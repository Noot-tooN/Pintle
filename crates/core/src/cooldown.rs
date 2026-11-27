//! Admission-time cooldown for one externally scoped key. The authority must
//! serialize transitions and commit the state before replying to a caller.
use crate::lease::{MAX_TIME_MS, identifier};

use serde::{Deserialize, Serialize};

pub const MAX_INTERVAL_MS: u64 = 86_400_000;

/// Keep all three fields stable across a transport retry.
#[derive(Clone, Copy, Debug)]
pub struct Attempt<'a> {
    pub window_id: u64,
    pub request_id: &'a str,
    pub interval_ms: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Decision {
    Admitted {
        admitted_at_ms: u64,
        retry_at_ms: u64,
    },
    CoolingDown {
        retry_at_ms: u64,
    },
    /// A newer window has committed. The original outcome is no longer retained.
    Superseded {
        retry_at_ms: u64,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Error {
    Clock,
    InvalidArgument,
    RequestConflict,
    InvalidWindow,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Admission {
    window_id: u64,
    subject: String,
    request_id: String,
    admitted_at_ms: u64,
    retry_at_ms: u64,
}

impl Admission {
    fn decision(&self) -> Decision {
        Decision::Admitted {
            admitted_at_ms: self.admitted_at_ms,
            retry_at_ms: self.retry_at_ms,
        }
    }
}

#[derive(Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Cooldown {
    clock_ms: u64,
    #[serde(deserialize_with = "Deserialize::deserialize")]
    latest: Option<Admission>,
}

impl Cooldown {
    /// Restore a bounded snapshot from continuous storage. Reject malformed
    /// records before they can turn an established key back into a fresh one.
    pub fn restore(raw: &str) -> Result<Self, String> {
        if raw.len() > 1024 {
            return Err("oversized cooldown record".into());
        }
        let state: Self = serde_json::from_str(raw).map_err(|_| "invalid cooldown record")?;
        if state.clock_ms > MAX_TIME_MS
            || state.latest.as_ref().is_some_and(|last| {
                !identifier(&last.subject, 32)
                    || !identifier(&last.request_id, 128)
                    || last.window_id > last.admitted_at_ms
                    || last.admitted_at_ms > state.clock_ms
                    || last.retry_at_ms > MAX_TIME_MS
                    || !last
                        .retry_at_ms
                        .checked_sub(last.admitted_at_ms)
                        .is_some_and(|ms| (1..=MAX_INTERVAL_MS).contains(&ms))
            })
        {
            return Err("inconsistent cooldown record".into());
        }
        Ok(state)
    }

    /// Read the window for a fresh attempt without consuming the opportunity.
    pub fn prepare(&mut self, now_ms: u64) -> Result<u64, Error> {
        if !(self.clock_ms..=MAX_TIME_MS).contains(&now_ms) {
            return Err(Error::Clock);
        }
        self.clock_ms = now_ms;
        Ok(match &self.latest {
            None => 0,
            Some(last) if now_ms < last.retry_at_ms => last.window_id,
            Some(last) => last.retry_at_ms,
        })
    }

    /// Decide a prepared attempt. A replay can return the historical admission
    /// after its deadline. Starting a new window requires a fresh attempt.
    pub fn try_admit(
        &mut self,
        now_ms: u64,
        subject: &str,
        attempt: Attempt<'_>,
    ) -> Result<Decision, Error> {
        if !identifier(subject, 32)
            || !identifier(attempt.request_id, 128)
            || !(1..=MAX_INTERVAL_MS).contains(&attempt.interval_ms)
        {
            return Err(Error::InvalidArgument);
        }
        let eligible_window = self.prepare(now_ms)?;
        if let Some(last) = &self.latest {
            let retry_at_ms = last.retry_at_ms;
            if attempt.window_id < last.window_id {
                return Ok(Decision::Superseded { retry_at_ms });
            }
            if attempt.window_id == last.window_id {
                if subject != last.subject || attempt.request_id != last.request_id {
                    return Ok(Decision::CoolingDown { retry_at_ms });
                }
                if attempt.interval_ms != retry_at_ms - last.admitted_at_ms {
                    return Err(Error::RequestConflict);
                }
                return Ok(last.decision());
            }
        }
        if attempt.window_id != eligible_window {
            return Err(Error::InvalidWindow);
        }
        let retry_at_ms = now_ms
            .checked_add(attempt.interval_ms)
            .filter(|&end| end <= MAX_TIME_MS)
            .ok_or(Error::InvalidArgument)?;
        let admission = Admission {
            window_id: attempt.window_id,
            subject: subject.into(),
            request_id: attempt.request_id.into(),
            admitted_at_ms: now_ms,
            retry_at_ms,
        };
        let decision = admission.decision();
        self.latest = Some(admission);
        Ok(decision)
    }
}
