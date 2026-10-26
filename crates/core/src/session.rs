//! Resource-local bounded recovery sessions. All operations, including cleanup,
//! run inside the same transaction as lease state. No platform clock or scheduler.
use crate::{
    Transaction, counter,
    lease::{self, Command, Receipt, Rejection, Reply},
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub mod waiting;

pub const MAX_SESSIONS: usize = 16;
pub const WINDOW: u64 = 32;
pub const IDLE_MS: u64 = 300_000;
pub const RECOVERY_MS: u64 = 600_000;
pub const MAX_SESSION_BYTES: usize = 65_536;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Frame {
    pub session_id: String,
    pub sequence: u64,
    pub acknowledged: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Info {
    pub id: String,
    pub acknowledged: u64,
    pub expires_at_ms: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cached {
    command: Command,
    reply: Reply,
    created: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    waiting: Option<waiting::Wait>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Session {
    info: Info,
    subject: String,
    entries: BTreeMap<u64, Cached>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Output {
    Session(Info),
    Operation(Reply),
    Rejected(Rejection),
    Waiting(waiting::State),
}
fn reject(r: Rejection) -> Result<Output, String> {
    Ok(Output::Rejected(r))
}
fn key(slot: usize) -> String {
    format!("session/{slot}")
}
fn load(tx: &impl Transaction, slot: usize) -> Result<Option<Session>, String> {
    let Some(raw) = tx.get(&key(slot))? else {
        return Ok(None);
    };
    if raw.len() > MAX_SESSION_BYTES {
        return Err("oversized session record".into());
    }
    let s: Option<Session> = serde_json::from_str(&raw).map_err(|e| e.to_string())?;
    if let Some(s) = &s
        && (!lease::identifier(&s.subject, 32)
            || counter(&s.info.id)? == 0
            || s.info.expires_at_ms > lease::MAX_TIME_MS
            || s.entries.len() > WINDOW as usize
            || s.entries.iter().any(|(n, c)| {
                *n <= s.info.acknowledged
                    || n.checked_sub(s.info.acknowledged)
                        .is_none_or(|d| d > WINDOW)
                    || c.created > s.info.expires_at_ms
                    || !c.command.valid()
                    || c.waiting.as_ref().is_some_and(|w| !w.valid(c))
            }))
    {
        return Err("invalid session record".into());
    }
    Ok(s)
}
fn save(tx: &mut impl Transaction, slot: usize, s: &Session) -> Result<(), String> {
    let raw = serde_json::to_string(s).map_err(|e| e.to_string())?;
    if raw.len() > MAX_SESSION_BYTES {
        return Err("session byte budget exceeded".into());
    }
    tx.put(&key(slot), &raw)
}
fn clock(tx: &mut impl Transaction, now: u64) -> Result<Option<Rejection>, String> {
    if now > lease::MAX_TIME_MS {
        return Ok(Some(Rejection::InvalidArgument));
    }
    let previous = counter(&tx.get("clock")?.unwrap_or_else(|| "0".into()))?;
    if previous > lease::MAX_TIME_MS {
        return Err("invalid clock".into());
    }
    if now < previous {
        return Ok(Some(Rejection::ClockRegression));
    }
    tx.put("clock", &now.to_string())?;
    Ok(None)
}
/// Physical reclamation uses a fixed set of reusable slots; owner and fencing
/// state are never removed. Expiry checks also happen on every request.
pub fn cleanup(tx: &mut impl Transaction, now: u64) -> Result<Option<Rejection>, String> {
    if let Some(r) = clock(tx, now)? {
        return Ok(Some(r));
    }
    for slot in 0..MAX_SESSIONS {
        if load(tx, slot)?.is_some_and(|s| now >= s.info.expires_at_ms) {
            tx.put(&key(slot), "null")?;
        }
    }
    Ok(None)
}
pub fn next_expiry(tx: &impl Transaction) -> Result<Option<u64>, String> {
    let mut next = None;
    for slot in 0..MAX_SESSIONS {
        if let Some(s) = load(tx, slot)? {
            next = Some(next.map_or(s.info.expires_at_ms, |n: u64| n.min(s.info.expires_at_ms)));
        }
    }
    Ok(next)
}
fn touch(s: &mut Session, now: u64) -> Result<(), String> {
    let idle = now
        .checked_add(IDLE_MS)
        .filter(|n| *n <= lease::MAX_TIME_MS)
        .ok_or("session time exhausted")?;
    s.info.expires_at_ms = s
        .entries
        .values()
        .map(|c| c.created.saturating_add(RECOVERY_MS))
        .min()
        .map_or(idle, |n| n.min(idle));
    Ok(())
}
pub fn open(tx: &mut impl Transaction, subject: &str, now: u64) -> Result<Output, String> {
    if !lease::identifier(subject, 32) {
        return reject(Rejection::InvalidArgument);
    }
    if let Some(r) = cleanup(tx, now)? {
        return reject(r);
    }
    let mut free = None;
    let mut used = 0;
    for slot in 0..MAX_SESSIONS {
        if load(tx, slot)?.is_some() {
            used += 1
        } else if free.is_none() {
            free = Some(slot);
        }
    }
    // Keep one session slot for the current authenticated owner's recovery.
    if used >= MAX_SESSIONS - 1 {
        let owner: Option<lease::Lease> =
            serde_json::from_str(&tx.get("owner")?.unwrap_or_else(|| "null".into()))
                .map_err(|e| e.to_string())?;
        if !owner.is_some_and(|l| {
            l.subject == subject && counter(&l.expires_at_ms).is_ok_and(|n| now < n)
        }) {
            return reject(Rejection::Exhausted);
        }
    }
    let Some(slot) = free else {
        return reject(Rejection::Exhausted);
    };
    let Some(id) =
        counter(&tx.get("session_counter")?.unwrap_or_else(|| "0".into()))?.checked_add(1)
    else {
        return reject(Rejection::Exhausted);
    };
    let mut s = Session {
        info: Info {
            id: id.to_string(),
            acknowledged: 0,
            expires_at_ms: 0,
        },
        subject: subject.into(),
        entries: BTreeMap::new(),
    };
    touch(&mut s, now)?;
    tx.put("session_counter", &id.to_string())?;
    save(tx, slot, &s)?;
    Ok(Output::Session(s.info))
}
fn find(tx: &impl Transaction, id: &str) -> Result<Option<(usize, Session)>, String> {
    for slot in 0..MAX_SESSIONS {
        if let Some(s) = load(tx, slot)?
            && s.info.id == id
        {
            return Ok(Some((slot, s)));
        }
    }
    Ok(None)
}
fn acknowledge(s: &mut Session, through: u64) -> bool {
    if through <= s.info.acknowledged {
        return true;
    }
    if through - s.info.acknowledged > WINDOW {
        return false;
    }
    // Never acknowledge past an unexecuted / unresolved sequence gap.
    if (s.info.acknowledged + 1..=through).any(|n| {
        s.entries.get(&n).is_none_or(|c| {
            c.waiting
                .as_ref()
                .is_some_and(|w| w.state == waiting::State::Pending)
        })
    }) {
        return false;
    }
    s.entries.retain(|n, _| *n > through);
    s.info.acknowledged = through;
    true
}
pub fn ack(
    tx: &mut impl Transaction,
    subject: &str,
    id: &str,
    through: u64,
    now: u64,
) -> Result<Output, String> {
    if let Some(r) = cleanup(tx, now)? {
        return reject(r);
    }
    let Some((slot, mut s)) = find(tx, id)? else {
        return reject(Rejection::SessionExpired);
    };
    if s.subject != subject {
        return reject(Rejection::PermissionDenied);
    }
    if !acknowledge(&mut s, through) {
        return reject(Rejection::InvalidArgument);
    }
    touch(&mut s, now)?;
    save(tx, slot, &s)?;
    Ok(Output::Session(s.info))
}
// The existing lease transition owns all ownership decisions. This view replaces
// its legacy permanent receipt ledger with the bounded session's cached receipt.
// It does not virtualize owner, clock, revision, or fencing writes.
struct Ledger<'a, T> {
    tx: &'a mut T,
    receipt: Option<String>,
}
impl<T: Transaction> Transaction for Ledger<'_, T> {
    fn get(&self, key: &str) -> Result<Option<String>, String> {
        if key.starts_with("request/") {
            return Ok(self.receipt.clone());
        }
        if key == "count" {
            return Ok(Some("0".into()));
        }
        self.tx.get(key)
    }
    fn put(&mut self, key: &str, value: &str) -> Result<(), String> {
        if key.starts_with("request/") || key == "count" {
            return Ok(());
        }
        self.tx.put(key, value)
    }
}
pub fn execute(
    tx: &mut impl Transaction,
    subject: &str,
    frame: &Frame,
    command: &Command,
    now: u64,
    token: &str,
) -> Result<Output, String> {
    if !command.valid() || counter(&frame.session_id).is_err() || frame.sequence == 0 {
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
    let is_status = matches!(command, Command::Status { .. });
    let distance = frame.sequence - s.info.acknowledged;
    if distance > WINDOW {
        return reject(Rejection::Exhausted);
    }
    let cached = s.entries.get(&frame.sequence);
    let result = if let Some(c) = cached {
        if c.command.id() != command.id() || (!is_status && c.command != *command) {
            return reject(Rejection::Conflict);
        }
        if let Some(w) = &c.waiting {
            if !is_status {
                return reject(Rejection::Conflict);
            }
            if w.state != waiting::State::Complete {
                return Ok(Output::Waiting(w.state.clone()));
            }
        }
        if let Reply::Applied { receipt, .. } = &c.reply {
            let receipt: &Receipt = receipt;
            lease::execute(
                &mut Ledger {
                    tx,
                    receipt: Some(serde_json::to_string(receipt).map_err(|e| e.to_string())?),
                },
                subject,
                command,
                now,
                token,
            )?
        } else {
            c.reply.clone()
        }
    } else {
        if is_status {
            return reject(Rejection::NotFound);
        }
        // Final window position reserved for Release, not more acquisition/renewal.
        if distance == WINDOW && !matches!(command, Command::Release { .. }) {
            return reject(Rejection::Exhausted);
        }
        let reply = lease::execute(
            &mut Ledger { tx, receipt: None },
            subject,
            command,
            now,
            token,
        )?;
        s.entries.insert(
            frame.sequence,
            Cached {
                command: command.clone(),
                reply: reply.clone(),
                created: now,
                waiting: None,
            },
        );
        reply
    };
    touch(&mut s, now)?;
    save(tx, slot, &s)?;
    Ok(Output::Operation(result))
}
