//! Unweighted, fixed-capacity semaphore ownership. The lease transition, bounded
//! recovery ledger and FIFO queue are shared with mutexes; no backend APIs here.
use crate::{
    Transaction, counter,
    lease::{Command, Lease, Mutation, Receipt},
};
use serde::{Deserialize, Serialize};
use subtle::ConstantTimeEq;

pub const MAX_CAPACITY: u32 = 64;

/// Fencing is scoped to (resource, slot). A newer grant in a different slot
/// must not invalidate a concurrently valid permit.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Permit {
    pub slot: u32,
    pub capacity: u32,
}
impl Permit {
    pub(crate) fn valid(&self) -> bool {
        (1..=MAX_CAPACITY).contains(&self.capacity) && (1..=self.capacity).contains(&self.slot)
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Slot {
    fence: String,
    owner: Option<Lease>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct State {
    // Empty until the first successful grant. Once committed, slots never resize.
    slots: Vec<Slot>,
}
/// Bind a fresh authority to the semaphore primitive, including before any
/// permits are granted so alarms and session-only calls use the same mode.
pub fn initialize(tx: &mut impl Transaction) -> Result<(), String> {
    if tx.get("semaphore")?.is_none() {
        if tx.get("owner")?.is_some() || tx.get("fence")?.is_some() {
            return Err("cannot reinterpret mutex state as semaphore".into());
        }
        tx.put("semaphore", "{\"slots\":[]}")?;
    }
    Ok(())
}
pub fn is_semaphore(tx: &impl Transaction) -> Result<bool, String> {
    Ok(tx.get("semaphore")?.is_some())
}

pub(crate) struct Owners {
    semaphore: bool,
    slots: Vec<Slot>,
}
impl Owners {
    pub(crate) fn load(tx: &impl Transaction, fence: u64, revision: u64) -> Result<Self, String> {
        let (semaphore, slots) = if let Some(raw) = tx.get("semaphore")? {
            if raw.len() > 65_536 || tx.get("owner")?.is_some() {
                return Err("invalid semaphore storage".into());
            }
            let state: State = serde_json::from_str(&raw).map_err(|e| e.to_string())?;
            (true, state.slots)
        } else {
            let owner = match tx.get("owner")? {
                Some(raw) => serde_json::from_str(&raw).map_err(|e| e.to_string())?,
                None if fence == 0 && revision == 0 => None,
                None => return Err("missing owner record for established mutex".into()),
            };
            (
                false,
                vec![Slot {
                    fence: fence.to_string(),
                    owner,
                }],
            )
        };
        if slots.len() > MAX_CAPACITY as usize
            || slots
                .iter()
                .map(|s| counter(&s.fence))
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .max()
                .unwrap_or(0)
                != fence
        {
            return Err("invalid slot fences".into());
        }
        for (i, slot) in slots.iter().enumerate() {
            if let Some(l) = &slot.owner {
                let expected = semaphore.then_some(Permit {
                    slot: i as u32 + 1,
                    capacity: slots.len() as u32,
                });
                if !l.valid()
                    || l.permit != expected
                    || l.fence != slot.fence
                    || counter(&l.revision)? > revision
                    || (!semaphore && counter(&l.revision)? != revision)
                {
                    return Err("invalid stored owner".into());
                }
            }
        }
        Ok(Self { semaphore, slots })
    }
    pub(crate) fn accepts(&self, command: &Command) -> bool {
        match command {
            Command::Acquire { .. } => !self.semaphore,
            Command::AcquirePermit { .. } => self.semaphore,
            _ => true,
        }
    }
    pub(crate) fn configure(&mut self, capacity: u32) -> bool {
        if self.slots.is_empty() {
            self.slots = vec![
                Slot {
                    fence: "0".into(),
                    owner: None
                };
                capacity as usize
            ];
        }
        self.slots.len() == capacity as usize
    }
    pub(crate) fn available(&self, now: u64) -> Option<usize> {
        self.slots.iter().position(|s| {
            s.owner
                .as_ref()
                .is_none_or(|l| now >= counter(&l.expires_at_ms).expect("validated owner"))
        })
    }
    pub(crate) fn permit(&self, slot: usize) -> Option<Permit> {
        self.semaphore.then_some(Permit {
            slot: slot as u32 + 1,
            capacity: self.slots.len() as u32,
        })
    }
    pub(crate) fn find(&self, token: &str) -> Option<(usize, Lease)> {
        self.slots.iter().enumerate().find_map(|(i, s)| {
            s.owner
                .as_ref()
                .filter(|l| bool::from(l.token.as_bytes().ct_eq(token.as_bytes())))
                .map(|l| (i, l.clone()))
        })
    }
    pub(crate) fn empty(&self) -> bool {
        self.slots.iter().all(|s| s.owner.is_none())
    }
    pub(crate) fn contains_token(&self, token: &str) -> bool {
        self.find(token).is_some()
    }
    pub(crate) fn set(
        &mut self,
        tx: &mut impl Transaction,
        slot: usize,
        owner: Option<Lease>,
        fence: u64,
    ) -> Result<(), String> {
        self.slots[slot] = Slot {
            fence: fence.to_string(),
            owner,
        };
        if self.semaphore {
            tx.put(
                "semaphore",
                &serde_json::to_string(&State {
                    slots: self.slots.clone(),
                })
                .map_err(|e| e.to_string())?,
            )
        } else {
            tx.put(
                "owner",
                &serde_json::to_string(&self.slots[0].owner).map_err(|e| e.to_string())?,
            )
        }
    }
    pub(crate) fn observe(&self, r: &Receipt, fence: u64) -> Result<(Option<Lease>, u64), String> {
        let l = match &r.mutation {
            Mutation::Acquired(l) | Mutation::Renewed(l) => l,
            _ => return Ok((None, fence)),
        };
        if self.semaphore {
            let p = l.permit.as_ref().ok_or("missing permit identity")?;
            if !p.valid() || p.capacity as usize != self.slots.len() {
                return Err("invalid receipt permit".into());
            }
            let slot = &self.slots[p.slot as usize - 1];
            if counter(&slot.fence)? < counter(&l.fence)? {
                return Err("slot fence regressed".into());
            }
            Ok((slot.owner.clone(), counter(&slot.fence)?))
        } else if l.permit.is_some() {
            Err("permit in mutex receipt".into())
        } else {
            Ok((self.slots[0].owner.clone(), fence))
        }
    }
}
fn owners(tx: &impl Transaction) -> Result<Owners, String> {
    Owners::load(
        tx,
        counter(&tx.get("fence")?.unwrap_or_else(|| "0".into()))?,
        counter(&tx.get("revision")?.unwrap_or_else(|| "0".into()))?,
    )
}
/// An initialized semaphore has no configured capacity until its first grant.
pub(crate) fn configured_capacity(tx: &impl Transaction) -> Result<Option<u32>, String> {
    let owners = owners(tx)?;
    Ok((owners.semaphore && !owners.slots.is_empty()).then_some(owners.slots.len() as u32))
}
pub(crate) fn has_owner(tx: &impl Transaction, subject: &str, now: u64) -> Result<bool, String> {
    Ok(owners(tx)?.slots.iter().any(|s| {
        s.owner.as_ref().is_some_and(|l| {
            l.subject == subject && counter(&l.expires_at_ms).is_ok_and(|n| now < n)
        })
    }))
}
pub(crate) fn next_available(tx: &impl Transaction) -> Result<u64, String> {
    Ok(owners(tx)?
        .slots
        .iter()
        .map(|s| {
            s.owner
                .as_ref()
                .map_or(0, |l| counter(&l.expires_at_ms).expect("validated owner"))
        })
        .min()
        .unwrap_or(0))
}
