//! P1 lease transitions. The adapter supplies one atomic transaction, an
//! authority-time sample, and a fresh cryptographic capability. No cloud APIs.
use crate::{Transaction, counter};
use serde::{Deserialize, Serialize};
use subtle::ConstantTimeEq;

pub const MIN_TTL_MS: u64 = 100;
pub const MAX_TTL_MS: u64 = 60_000;
pub const MAX_RECEIPTS: u64 = 1024;
pub const MAX_TIME_MS: u64 = (1 << 53) - 1;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "method", deny_unknown_fields)]
pub enum Command {
    Acquire {
        id: String,
        holder: String,
        ttl_ms: u64,
    },
    Renew {
        id: String,
        token: String,
        ttl_ms: u64,
    },
    Release {
        id: String,
        token: String,
    },
    Status {
        id: String,
    },
}
impl Command {
    pub fn id(&self) -> &str {
        match self {
            Self::Acquire { id, .. }
            | Self::Renew { id, .. }
            | Self::Release { id, .. }
            | Self::Status { id } => id,
        }
    }
    fn ttl(&self) -> Option<u64> {
        match self {
            Self::Acquire { ttl_ms, .. } | Self::Renew { ttl_ms, .. } => Some(*ttl_ms),
            _ => None,
        }
    }
    fn token(&self) -> Option<&str> {
        match self {
            Self::Renew { token, .. } | Self::Release { token, .. } => Some(token),
            _ => None,
        }
    }
    pub(crate) fn valid(&self) -> bool {
        identifier(self.id(), 128)
            && self
                .ttl()
                .is_none_or(|t| (MIN_TTL_MS..=MAX_TTL_MS).contains(&t))
            && self.token().is_none_or(capability)
            && match self {
                Self::Acquire { holder, .. } => identifier(holder, 64),
                _ => true,
            }
    }
}
pub fn identifier(value: &str, max: usize) -> bool {
    !value.is_empty()
        && value.len() <= max
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._:-".contains(&b))
}
pub fn capability(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Lease {
    pub acquisition_id: String,
    pub subject: String,
    pub holder: String,
    pub token: String,
    pub fence: String,
    pub revision: String,
    pub expires_at_ms: String,
}
impl Lease {
    fn valid(&self) -> bool {
        identifier(&self.acquisition_id, 128)
            && identifier(&self.subject, 32)
            && identifier(&self.holder, 64)
            && capability(&self.token)
            && counter(&self.fence).is_ok_and(|n| n > 0)
            && counter(&self.revision).is_ok_and(|n| n > 0)
            && counter(&self.expires_at_ms).is_ok_and(|n| n > 0 && n <= MAX_TIME_MS)
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Mutation {
    Acquired(Lease),
    Renewed(Lease),
    Released { token: String, revision: String },
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Receipt {
    pub subject: String,
    pub command: Command,
    pub mutation: Mutation,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum LeaseState {
    Active,
    Expired,
    Released,
    Superseded,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Observation {
    pub state: LeaseState,
    pub current: Option<Lease>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Rejection {
    SessionExpired,
    Acknowledged,
    InvalidArgument,
    Conflict,
    NotOwner,
    LeaseLost,
    Exhausted,
    ClockRegression,
    NotFound,
    PermissionDenied,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Reply {
    Applied {
        receipt: Box<Receipt>,
        replayed: bool,
        observation: Option<Observation>,
    },
    Contended,
    Rejected(Rejection),
}
fn rejected(reason: Rejection) -> Result<Reply, String> {
    Ok(Reply::Rejected(reason))
}
fn read_counter(tx: &impl Transaction, key: &str) -> Result<u64, String> {
    counter(&tx.get(key)?.unwrap_or_else(|| "0".into()))
}
fn observe(
    receipt: &Receipt,
    current: &Option<Lease>,
    fence: u64,
    now: u64,
) -> Option<Observation> {
    let original = match &receipt.mutation {
        Mutation::Acquired(l) | Mutation::Renewed(l) => l,
        _ => return None,
    };
    let (state, visible) = match current {
        Some(l) if l.token == original.token => {
            if now < counter(&l.expires_at_ms).expect("validated lease") {
                (LeaseState::Active, Some(l.clone()))
            } else {
                (LeaseState::Expired, None)
            }
        }
        _ if fence > counter(&original.fence).expect("validated receipt") => {
            (LeaseState::Superseded, None)
        }
        _ => (LeaseState::Released, None),
    };
    Some(Observation {
        state,
        current: visible,
    })
}
fn valid_receipt(r: &Receipt, id: &str, fence: u64, revision: u64) -> bool {
    if r.command.id() != id || !r.command.valid() || !identifier(&r.subject, 32) {
        return false;
    }
    match (&r.command, &r.mutation) {
        (Command::Acquire { id, holder, .. }, Mutation::Acquired(l)) => {
            l.valid()
                && l.subject == r.subject
                && &l.acquisition_id == id
                && &l.holder == holder
                && counter(&l.fence).unwrap() <= fence
                && counter(&l.revision).unwrap() <= revision
        }
        (Command::Renew { token, .. }, Mutation::Renewed(l)) => {
            l.valid()
                && l.subject == r.subject
                && &l.token == token
                && counter(&l.fence).unwrap() <= fence
                && counter(&l.revision).unwrap() <= revision
        }
        (
            Command::Release { token, .. },
            Mutation::Released {
                token: t,
                revision: r,
            },
        ) => token == t && counter(r).is_ok_and(|n| n > 0 && n <= revision),
        _ => false,
    }
}

/// Expected business rejections are values so an observed clock high-water mark
/// can commit. Storage/corruption errors are Err and MUST roll back every write.
/// new_token is used only for a fresh Acquire; retries never generate a new lease.
pub fn execute(
    tx: &mut impl Transaction,
    subject: &str,
    command: &Command,
    now: u64,
    new_token: &str,
) -> Result<Reply, String> {
    if !identifier(subject, 32) || !command.valid() || now > MAX_TIME_MS {
        return rejected(Rejection::InvalidArgument);
    }
    let clock = read_counter(tx, "clock")?;
    if clock > MAX_TIME_MS {
        return Err("invalid stored clock".into());
    }
    if now < clock {
        return rejected(Rejection::ClockRegression);
    }
    let fence = read_counter(tx, "fence")?;
    let revision = read_counter(tx, "revision")?;
    let count = read_counter(tx, "count")?;
    if count > MAX_RECEIPTS {
        return Err("invalid receipt count".into());
    }
    let owner: Option<Lease> = match tx.get("owner")? {
        None => None,
        Some(s) => serde_json::from_str(&s).map_err(|e| e.to_string())?,
    };
    if owner.as_ref().is_some_and(|l| {
        !l.valid()
            || counter(&l.fence).unwrap() != fence
            || counter(&l.revision).unwrap() != revision
    }) {
        return Err("invalid stored owner".into());
    }
    let key = format!("request/{}", command.id());
    let old = tx.get(&key)?;
    // Persist observations, including expiry and contention, so a later backward
    // clock reading cannot resurrect a lease already observed as expired.
    tx.put("clock", &now.to_string())?;
    if let Some(json) = old {
        let receipt: Receipt = serde_json::from_str(&json).map_err(|e| e.to_string())?;
        if !valid_receipt(&receipt, command.id(), fence, revision) {
            return Err("invalid stored receipt".into());
        }
        if receipt.subject != subject {
            return rejected(Rejection::PermissionDenied);
        }
        if !matches!(command, Command::Status { .. }) && receipt.command != *command {
            return rejected(Rejection::Conflict);
        }
        let observation = observe(&receipt, &owner, fence, now);
        return Ok(Reply::Applied {
            receipt: Box::new(receipt),
            replayed: true,
            observation,
        });
    }
    if matches!(command, Command::Status { .. }) {
        return rejected(Rejection::NotFound);
    }
    let next_revision = match revision.checked_add(1) {
        Some(r) => r,
        None => return rejected(Rejection::Exhausted),
    };
    let limit = if matches!(command, Command::Release { .. }) {
        MAX_RECEIPTS
    } else {
        MAX_RECEIPTS - 1
    };
    // Keep one receipt slot reserved for the current owner's Release.
    if count >= limit {
        return rejected(Rejection::Exhausted);
    }
    let mutation = match command {
        Command::Acquire { id, holder, ttl_ms } => {
            if owner
                .as_ref()
                .is_some_and(|l| now < counter(&l.expires_at_ms).expect("validated owner"))
            {
                return Ok(Reply::Contended);
            }
            if !capability(new_token) {
                return Err("invalid generated capability".into());
            }
            let Some(next_fence) = fence.checked_add(1) else {
                return rejected(Rejection::Exhausted);
            };
            let Some(expires) = now.checked_add(*ttl_ms).filter(|n| *n <= MAX_TIME_MS) else {
                return rejected(Rejection::Exhausted);
            };
            let l = Lease {
                acquisition_id: id.clone(),
                subject: subject.into(),
                holder: holder.clone(),
                token: new_token.into(),
                fence: next_fence.to_string(),
                revision: next_revision.to_string(),
                expires_at_ms: expires.to_string(),
            };
            tx.put(
                "owner",
                &serde_json::to_string(&Some(&l)).map_err(|e| e.to_string())?,
            )?;
            tx.put("fence", &l.fence)?;
            Mutation::Acquired(l)
        }
        Command::Renew { token, .. } | Command::Release { token, .. } => {
            let Some(mut l) = owner else {
                return rejected(Rejection::LeaseLost);
            };
            if l.subject != subject || !bool::from(l.token.as_bytes().ct_eq(token.as_bytes())) {
                return rejected(Rejection::NotOwner);
            }
            if now >= counter(&l.expires_at_ms)? {
                return rejected(Rejection::LeaseLost);
            }
            if matches!(command, Command::Renew { .. }) {
                let ttl = command.ttl().expect("renew ttl");
                let Some(expires) = now.checked_add(ttl).filter(|n| *n <= MAX_TIME_MS) else {
                    return rejected(Rejection::Exhausted);
                };
                l.expires_at_ms = expires.max(counter(&l.expires_at_ms)?).to_string();
                l.revision = next_revision.to_string();
                tx.put(
                    "owner",
                    &serde_json::to_string(&Some(&l)).map_err(|e| e.to_string())?,
                )?;
                Mutation::Renewed(l)
            } else {
                tx.put("owner", "null")?;
                Mutation::Released {
                    token: token.clone(),
                    revision: next_revision.to_string(),
                }
            }
        }
        Command::Status { .. } => unreachable!(),
    };
    tx.put("revision", &next_revision.to_string())?;
    tx.put("count", &(count + 1).to_string())?;
    let receipt = Receipt {
        subject: subject.into(),
        command: command.clone(),
        mutation,
    };
    tx.put(
        &key,
        &serde_json::to_string(&receipt).map_err(|e| e.to_string())?,
    )?;
    let current = match &receipt.mutation {
        Mutation::Acquired(l) | Mutation::Renewed(l) => Some(l.clone()),
        _ => None,
    };
    let observation = observe(&receipt, &current, read_counter(tx, "fence")?, now);
    Ok(Reply::Applied {
        receipt: Box::new(receipt),
        replayed: false,
        observation,
    })
}
