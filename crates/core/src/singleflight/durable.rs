//! Durable admission receipts for a single, externally scoped resource.
//! The generation is part of every join attempt, so discarding expired receipts
//! can never turn a delayed retry into admission to a later generation.
use super::*;
use crate::lease::{capability, identifier};
use subtle::ConstantTimeEq;

#[derive(Clone)]
pub enum Action {
    Generation,
    Join { generation: u64, request_id: String },
    Observe(String),
    Cancel(String),
    Complete { ticket: String, outcome: Outcome },
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum Reply {
    Generation(u64),
    Joined { ticket: String, producer: bool },
    Observation(Observation),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum Rejection {
    Core(Error),
    ClosedGeneration,
    Conflict,
    Invalid,
}
impl From<Error> for Rejection {
    fn from(e: Error) -> Self {
        Self::Core(e)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Member {
    subject: String,
    request_id: String,
    capability: String,
}

#[derive(Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Authority {
    group: Group,
    // Entries are in admission order: producer, then waiter index + 1.
    members: BTreeMap<u64, Vec<Member>>,
}

impl Authority {
    pub fn restore(raw: &str) -> Result<Self, String> {
        // Eight generations of 4 KiB decimal JSON byte arrays plus at most
        // 136 bounded subject/request/capability records fit below 256 KiB.
        if raw.len() > 256 * 1024 {
            return Err("oversized flight snapshot".into());
        }
        let state: Self = serde_json::from_str(raw).map_err(|_| "invalid flight record")?;
        let g = &state.group;
        if g.clock > MAX_TIME_MS
            || g.flights.len() > MAX_GENERATIONS
            || g.flights
                .last_key_value()
                .is_some_and(|(&id, _)| id != g.last_generation)
            || !g.flights.keys().eq(state.members.keys())
        {
            return Err("inconsistent flight record".into());
        }
        let mut capabilities = std::collections::BTreeSet::new();
        for (&id, f) in &g.flights {
            let members = &state.members[&id];
            let mut attempts = std::collections::BTreeSet::new();
            if id == 0
                || id > g.last_generation
                || f.cancelled_waiters.len() > MAX_WAITERS
                || members.len() != f.cancelled_waiters.len() + 1
                || members.iter().any(|m| {
                    !identifier(&m.subject, 32)
                        || !identifier(&m.request_id, 128)
                        || !capability(&m.capability)
                        || !capabilities.insert(&m.capability)
                        || !attempts.insert((&m.subject, &m.request_id))
                })
            {
                return Err("inconsistent flight membership".into());
            }
            match &f.phase {
                Phase::Running { deadline }
                    if id != g.last_generation
                        || *deadline <= g.clock
                        || *deadline > MAX_TIME_MS =>
                {
                    return Err("invalid flight deadline".into());
                }
                Phase::Finished { at, outcome }
                    if *at > g.clock
                        || g.clock - at >= RECOVERY_MS
                        || matches!(outcome, Outcome::Value(v) if v.len() > MAX_RESULT_BYTES) =>
                {
                    return Err("invalid flight result".into());
                }
                _ => {}
            }
        }
        Ok(state)
    }

    fn generation(&self) -> Result<u64, Error> {
        if self
            .group
            .flights
            .get(&self.group.last_generation)
            .is_some_and(|f| matches!(f.phase, Phase::Running { .. }))
        {
            Ok(self.group.last_generation)
        } else {
            self.group
                .last_generation
                .checked_add(1)
                .ok_or(Error::GenerationExhausted)
        }
    }

    fn ticket(&self, subject: &str, token: &str) -> Result<Ticket, Rejection> {
        if !capability(token) {
            return Err(Rejection::Invalid);
        }
        for (&generation, members) in &self.members {
            for (i, m) in members.iter().enumerate() {
                if m.subject == subject
                    && bool::from(m.capability.as_bytes().ct_eq(token.as_bytes()))
                {
                    let role = if i == 0 {
                        Role::Leader
                    } else {
                        Role::Waiter(i - 1)
                    };
                    return Ok(Ticket { generation, role });
                }
            }
        }
        Err(Error::UnknownTicket.into())
    }

    pub fn apply(
        &mut self,
        now: u64,
        subject: &str,
        action: Action,
        nonce: &str,
    ) -> Result<Reply, Rejection> {
        if !identifier(subject, 32) {
            return Err(Rejection::Invalid);
        }
        self.group.advance(now)?;
        self.members
            .retain(|id, _| self.group.flights.contains_key(id));
        match action {
            Action::Generation => Ok(Reply::Generation(self.generation()?)),
            Action::Join {
                generation,
                request_id,
            } => {
                if generation == 0 || !identifier(&request_id, 128) {
                    return Err(Rejection::Invalid);
                }
                if let Some((i, m)) = self.members.get(&generation).and_then(|members| {
                    members
                        .iter()
                        .enumerate()
                        .find(|(_, m)| m.subject == subject && m.request_id == request_id)
                }) {
                    return Ok(Reply::Joined {
                        ticket: m.capability.clone(),
                        producer: i == 0,
                    });
                }
                if generation != self.generation()? {
                    return Err(Rejection::ClosedGeneration);
                }
                if !capability(nonce)
                    || self
                        .members
                        .values()
                        .flatten()
                        .any(|m| m.capability == nonce)
                {
                    return Err(Error::GenerationExhausted.into());
                }
                let ticket = self.group.join(now)?;
                self.members.entry(generation).or_default().push(Member {
                    subject: subject.into(),
                    request_id,
                    capability: nonce.into(),
                });
                Ok(Reply::Joined {
                    ticket: nonce.into(),
                    producer: ticket.is_leader(),
                })
            }
            Action::Observe(token) => {
                let t = self.ticket(subject, &token)?;
                Ok(Reply::Observation(self.group.observe(now, t)?))
            }
            Action::Cancel(token) => {
                let t = self.ticket(subject, &token)?;
                Ok(Reply::Observation(self.group.cancel_waiter(now, t)?))
            }
            Action::Complete { ticket, outcome } => {
                let t = self.ticket(subject, &ticket)?;
                let completion = match &outcome {
                    Outcome::Value(v) if v.len() <= MAX_RESULT_BYTES => Completion::Value(v),
                    Outcome::FillFailed => Completion::Failed,
                    Outcome::ResultTooLarge => Completion::TooLarge,
                    _ => return Err(Rejection::Invalid),
                };
                let settled = self.group.complete(now, t, completion)?;
                if settled != outcome && settled != Outcome::LeaderExpired {
                    return Err(Rejection::Conflict);
                }
                Ok(Reply::Observation(Observation::Finished(settled)))
            }
        }
    }
}
