//! Native cache-fill transitions for one authenticated resource. The adapter must
//! serialize calls, deduplicate admissions and bind tickets to their resource and
//! principal. A ticket is a core-local index, never a wire ownership capability.
use crate::lease::MAX_TIME_MS;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub mod durable;

pub const MAX_RESULT_BYTES: usize = 4096;
pub const FILL_TIMEOUT_MS: u64 = 30_000;
pub const RECOVERY_MS: u64 = 60_000;
pub const MAX_WAITERS: usize = 16;
pub const MAX_GENERATIONS: usize = 8;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Ticket {
    generation: u64,
    role: Role,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Role {
    Leader,
    Waiter(usize),
}

impl Ticket {
    pub fn is_leader(self) -> bool {
        self.role == Role::Leader
    }
}

pub enum Completion<'a> {
    Value(&'a [u8]),
    Failed,
    /// Lets the producer report the size failure without sending the value.
    TooLarge,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Outcome {
    Value(Vec<u8>),
    FillFailed,
    LeaderExpired,
    ResultTooLarge,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Observation {
    Pending,
    Cancelled,
    Finished(Outcome),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Error {
    Clock,
    Capacity,
    GenerationExhausted,
    UnknownTicket,
    WrongRole,
}

#[derive(Serialize, Deserialize)]
enum Phase {
    Running { deadline: u64 },
    Finished { at: u64, outcome: Outcome },
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Flight {
    phase: Phase,
    cancelled_waiters: Vec<bool>,
}

#[derive(Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Group {
    clock: u64,
    last_generation: u64,
    flights: BTreeMap<u64, Flight>,
}

impl Group {
    fn advance(&mut self, now: u64) -> Result<(), Error> {
        if !(self.clock..=MAX_TIME_MS).contains(&now) {
            return Err(Error::Clock);
        }
        self.clock = now;
        self.flights.retain(|_, flight| {
            if let Phase::Running { deadline } = flight.phase
                && now >= deadline
            {
                flight.phase = Phase::Finished {
                    at: deadline,
                    outcome: Outcome::LeaderExpired,
                };
            }
            !matches!(flight.phase, Phase::Finished { at, .. } if now - at >= RECOVERY_MS)
        });
        Ok(())
    }

    /// Admit one fresh logical request. A retried admission needs its original
    /// ticket from the adapter's recovery session.
    pub fn join(&mut self, now: u64) -> Result<Ticket, Error> {
        self.advance(now)?;
        if let Some(flight) = self.flights.get_mut(&self.last_generation)
            && matches!(flight.phase, Phase::Running { .. })
        {
            let index = flight.cancelled_waiters.len();
            if index >= MAX_WAITERS {
                return Err(Error::Capacity);
            }
            flight.cancelled_waiters.push(false);
            return Ok(Ticket {
                generation: self.last_generation,
                role: Role::Waiter(index),
            });
        }
        if self.flights.len() >= MAX_GENERATIONS {
            return Err(Error::Capacity);
        }
        let deadline = now + FILL_TIMEOUT_MS;
        if deadline > MAX_TIME_MS {
            return Err(Error::Clock);
        }
        let generation = self
            .last_generation
            .checked_add(1)
            .ok_or(Error::GenerationExhausted)?;
        self.flights.insert(
            generation,
            Flight {
                phase: Phase::Running { deadline },
                cancelled_waiters: Vec::new(),
            },
        );
        self.last_generation = generation;
        Ok(Ticket {
            generation,
            role: Role::Leader,
        })
    }

    fn flight(&mut self, now: u64, ticket: Ticket) -> Result<&mut Flight, Error> {
        self.advance(now)?;
        self.flights
            .get_mut(&ticket.generation)
            .ok_or(Error::UnknownTicket)
    }

    /// First terminal outcome wins, including when the producer retries after
    /// losing the completion reply.
    pub fn complete(
        &mut self,
        now: u64,
        ticket: Ticket,
        result: Completion<'_>,
    ) -> Result<Outcome, Error> {
        if !ticket.is_leader() {
            return Err(Error::WrongRole);
        }
        let flight = self.flight(now, ticket)?;
        if let Phase::Finished { outcome, .. } = &flight.phase {
            return Ok(outcome.clone());
        }
        let outcome = match result {
            Completion::Value(bytes) if bytes.len() <= MAX_RESULT_BYTES => {
                Outcome::Value(bytes.to_vec())
            }
            Completion::Value(_) | Completion::TooLarge => Outcome::ResultTooLarge,
            Completion::Failed => Outcome::FillFailed,
        };
        flight.phase = Phase::Finished {
            at: now,
            outcome: outcome.clone(),
        };
        Ok(outcome)
    }

    pub fn observe(&mut self, now: u64, ticket: Ticket) -> Result<Observation, Error> {
        self.flight(now, ticket)?.observation(ticket.role)
    }

    pub fn cancel_waiter(&mut self, now: u64, ticket: Ticket) -> Result<Observation, Error> {
        let Role::Waiter(index) = ticket.role else {
            return Err(Error::WrongRole);
        };
        let flight = self.flight(now, ticket)?;
        if matches!(flight.phase, Phase::Running { .. }) {
            *flight
                .cancelled_waiters
                .get_mut(index)
                .ok_or(Error::UnknownTicket)? = true;
        }
        flight.observation(ticket.role)
    }
}

impl Flight {
    fn observation(&self, role: Role) -> Result<Observation, Error> {
        if let Role::Waiter(index) = role
            && *self
                .cancelled_waiters
                .get(index)
                .ok_or(Error::UnknownTicket)?
        {
            return Ok(Observation::Cancelled);
        }
        Ok(match &self.phase {
            Phase::Running { .. } => Observation::Pending,
            Phase::Finished { outcome, .. } => Observation::Finished(outcome.clone()),
        })
    }
}
