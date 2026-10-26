//! Durable FIFO acquisition. Notification and timer adapters are hints; every
//! decision uses the same transaction and lease grant transition as TryAcquire.
use super::*;
pub const MAX_PENDING: usize = 16;
pub const MAX_WAIT_MS: u64 = 60_000;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum State {
    Pending,
    Cancelled,
    Expired,
    Complete,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Wait {
    duration: u64,
    deadline: u64,
    ticket: String,
    pub(super) state: State,
}
impl Wait {
    pub(super) fn valid(&self, c: &Cached) -> bool {
        (1..=MAX_WAIT_MS).contains(&self.duration)
            && self.deadline <= lease::MAX_TIME_MS
            && self.deadline == c.created.saturating_add(self.duration)
            && counter(&self.ticket).is_ok_and(|n| n > 0)
            && matches!(c.command, Command::Acquire { .. })
            && (self.state == State::Complete || matches!(c.reply, Reply::Contended))
    }
}
fn pending(tx: &impl Transaction) -> Result<Vec<(u64, usize, u64)>, String> {
    let mut list = Vec::new();
    let allocated = counter(&tx.get("wait_counter")?.unwrap_or_else(|| "0".into()))?;
    for slot in 0..MAX_SESSIONS {
        if let Some(s) = load(tx, slot)? {
            for (seq, c) in &s.entries {
                if let Some(w) = &c.waiting
                    && counter(&w.ticket)? > allocated
                {
                    return Err("queue allocation counter regressed".into());
                }
                if let Some(w) = &c.waiting
                    && w.state == State::Pending
                {
                    list.push((counter(&w.ticket)?, slot, *seq));
                }
            }
        }
    }
    if list.len() > MAX_PENDING {
        return Err("pending capacity corrupt".into());
    }
    list.sort_unstable();
    if list.windows(2).any(|w| w[0].0 == w[1].0) {
        return Err("duplicate queue ticket".into());
    }
    Ok(list)
}
/// Expire deadlines, then grant the oldest eligible waiter. The caller wraps
/// this and the triggering mutation in ONE transaction. At most one grant can
/// occur per drive, so one fresh random capability suffices.
pub fn drive(
    tx: &mut impl Transaction,
    now: u64,
    token: &str,
) -> Result<Option<Rejection>, String> {
    if let Some(r) = cleanup(tx, now)? {
        return Ok(Some(r));
    }
    let list = pending(tx)?;
    for (_, slot, seq) in &list {
        let mut s = load(tx, *slot)?.ok_or("missing queued session")?;
        let c = s.entries.get_mut(seq).ok_or("missing queued entry")?;
        let w = c.waiting.as_mut().ok_or("missing queue metadata")?;
        if now >= w.deadline {
            w.state = State::Expired;
            save(tx, *slot, &s)?;
        }
    }
    for (_, slot, seq) in list {
        let mut s = load(tx, slot)?.ok_or("missing queued session")?;
        let c = s.entries.get_mut(&seq).ok_or("missing queued entry")?;
        if c.waiting.as_ref().is_none_or(|w| w.state != State::Pending) {
            continue;
        }
        let reply = lease::execute(
            &mut Ledger { tx, receipt: None },
            &s.subject,
            &c.command,
            now,
            token,
        )?;
        if matches!(reply, Reply::Contended) {
            break;
        }
        let granted = matches!(reply, Reply::Applied { .. });
        c.reply = reply;
        c.waiting.as_mut().ok_or("missing queue metadata")?.state = State::Complete;
        save(tx, slot, &s)?;
        if granted {
            break;
        }
    }
    Ok(None)
}
/// Register/resume or cancel the exact acquisition. Cancellation also records a
/// terminal result before registration, preventing a delayed request from joining.
#[allow(clippy::too_many_arguments)]
pub fn acquire(
    tx: &mut impl Transaction,
    subject: &str,
    frame: &Frame,
    command: &Command,
    duration: u64,
    cancel: bool,
    now: u64,
    token: &str,
) -> Result<Output, String> {
    if !command.valid()
        || !matches!(command, Command::Acquire { .. })
        || !(1..=MAX_WAIT_MS).contains(&duration)
        || frame.sequence == 0
        || counter(&frame.session_id).is_err()
    {
        return reject(Rejection::InvalidArgument);
    }
    if let Some(r) = cleanup(tx, now)? {
        return reject(r);
    }
    let Some((slot, mut s)) = find(tx, &frame.session_id)? else {
        return reject(Rejection::SessionExpired);
    };
    if s.subject != subject {
        return reject(Rejection::PermissionDenied);
    }
    if !acknowledge(&mut s, frame.acknowledged) {
        return reject(Rejection::InvalidArgument);
    }
    if frame.sequence <= s.info.acknowledged {
        return reject(Rejection::Acknowledged);
    }
    if frame.sequence - s.info.acknowledged >= WINDOW {
        return reject(Rejection::Exhausted);
    }
    if let Some(c) = s.entries.get_mut(&frame.sequence) {
        if c.command != *command || c.waiting.as_ref().is_none_or(|w| w.duration != duration) {
            return reject(Rejection::Conflict);
        }
        let w = c.waiting.as_mut().ok_or("missing queue metadata")?;
        if w.state == State::Pending {
            if now >= w.deadline {
                w.state = State::Expired;
            } else if cancel {
                w.state = State::Cancelled;
            }
        }
    } else {
        if !cancel && pending(tx)?.len() >= MAX_PENDING {
            return reject(Rejection::Exhausted);
        }
        let Some(ticket) =
            counter(&tx.get("wait_counter")?.unwrap_or_else(|| "0".into()))?.checked_add(1)
        else {
            return reject(Rejection::Exhausted);
        };
        let Some(deadline) = now
            .checked_add(duration)
            .filter(|n| *n <= lease::MAX_TIME_MS)
        else {
            return reject(Rejection::Exhausted);
        };
        tx.put("wait_counter", &ticket.to_string())?;
        s.entries.insert(
            frame.sequence,
            Cached {
                command: command.clone(),
                reply: Reply::Contended,
                created: now,
                waiting: Some(Wait {
                    duration,
                    deadline,
                    ticket: ticket.to_string(),
                    state: if cancel {
                        State::Cancelled
                    } else {
                        State::Pending
                    },
                }),
            },
        );
    }
    touch(&mut s, now)?;
    save(tx, slot, &s)?;
    if let Some(r) = drive(tx, now, token)? {
        return reject(r);
    }
    execute(
        tx,
        subject,
        frame,
        &Command::Status {
            id: command.id().into(),
        },
        now,
        token,
    )
}
/// Next time-driven transition, including the owner's expiry only while there
/// is a queue. No periodic polling is needed while waiting for a release.
pub fn next_wakeup(tx: &impl Transaction) -> Result<Option<u64>, String> {
    let mut next = next_expiry(tx)?;
    let list = pending(tx)?;
    for (_, slot, seq) in &list {
        let s = load(tx, *slot)?.ok_or("missing queued session")?;
        let deadline = s.entries[seq]
            .waiting
            .as_ref()
            .ok_or("missing queue metadata")?
            .deadline;
        next = Some(next.map_or(deadline, |n| n.min(deadline)));
    }
    if !list.is_empty() {
        let owner: Option<lease::Lease> =
            serde_json::from_str(&tx.get("owner")?.unwrap_or_else(|| "null".into()))
                .map_err(|e| e.to_string())?;
        let expiry = owner
            .map(|o| counter(&o.expires_at_ms))
            .transpose()?
            .unwrap_or(0);
        next = Some(next.map_or(expiry, |n| n.min(expiry)));
    }
    Ok(next)
}
/// One authoritative service transition. Adapters select an action but never
/// implement queue ordering or grant/release arbitration.
pub enum Action<'a> {
    Open,
    Acknowledge {
        id: &'a str,
        through: u64,
    },
    Execute {
        frame: &'a Frame,
        command: &'a Command,
    },
    Acquire {
        frame: &'a Frame,
        command: &'a Command,
        duration: u64,
        cancel: bool,
    },
}
pub fn apply(
    tx: &mut impl Transaction,
    subject: &str,
    action: Action<'_>,
    now: u64,
    token: &str,
) -> Result<Output, String> {
    let acquisition = matches!(action, Action::Acquire { .. });
    if !matches!(action, Action::Acquire { cancel: true, .. })
        && let Some(r) = drive(tx, now, token)?
    {
        return reject(r);
    }
    let result = match action {
        Action::Open => open(tx, subject, now)?,
        Action::Acknowledge { id, through } => ack(tx, subject, id, through, now)?,
        Action::Execute { frame, command } => execute(tx, subject, frame, command, now, token)?,
        Action::Acquire {
            frame,
            command,
            duration,
            cancel,
        } => acquire(tx, subject, frame, command, duration, cancel, now, token)?,
    };
    if !acquisition && let Some(r) = drive(tx, now, token)? {
        return reject(r);
    }
    Ok(result)
}
