//! P0's non-expiring grant transition. This is not a mutex/lease API.
//! The caller MUST execute the entire function in one serializable transaction.
//! No clock, Cloudflare types, networking, or platform storage dependencies.
pub mod lease;
pub mod session;

use serde::{Deserialize, Serialize};

/// Provisional synchronous, single-authority transaction view. Implementations
/// must roll back all writes when the callback returns an error. No awaits here.
pub trait Transaction {
    fn get(&self, key: &str) -> Result<Option<String>, String>;
    fn put(&mut self, key: &str, value: &str) -> Result<(), String>;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub request_id: String,
    pub owner: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Grant {
    pub request: Request,
    // Canonical decimal strings at every serialization/FFI boundary.
    pub fence: String,
    pub revision: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum Outcome {
    Granted { grant: Grant, replayed: bool },
    Contended,
    RequestIdConflict,
    Exhausted,
}

pub fn counter(value: &str) -> Result<u64, String> {
    let number = value.parse::<u64>().map_err(|_| "invalid counter")?;
    if number.to_string() != value {
        return Err("non-canonical counter".into());
    }
    Ok(number)
}

pub fn try_grant(tx: &mut impl Transaction, request: &Request) -> Result<Outcome, String> {
    if request.request_id.is_empty()
        || request.request_id.len() > 128
        || request.owner.is_empty()
        || request.owner.len() > 128
    {
        return Err("request_id and owner must be 1..128 bytes".into());
    }
    let dedupe_key = format!("request/{}", request.request_id);
    if let Some(value) = tx.get(&dedupe_key)? {
        let grant: Grant = serde_json::from_str(&value).map_err(|e| e.to_string())?;
        // Persisted receipts are untrusted storage input too. Never acknowledge
        // a malformed token or a receipt stored under a different request ID.
        if grant.request.request_id != request.request_id
            || grant.request.owner.is_empty()
            || grant.request.owner.len() > 128
            || counter(&grant.fence)? == 0
            || counter(&grant.revision)? == 0
        {
            return Err("invalid persisted grant receipt".into());
        }

        return Ok(if grant.request == *request {
            Outcome::Granted {
                grant,
                replayed: true,
            }
        } else {
            Outcome::RequestIdConflict
        });
    }
    if tx.get("owner")?.is_some() {
        return Ok(Outcome::Contended);
    }
    let fence = counter(&tx.get("fence")?.unwrap_or_else(|| "0".into()))?;
    let revision = counter(&tx.get("revision")?.unwrap_or_else(|| "0".into()))?;
    let (Some(fence), Some(revision)) = (fence.checked_add(1), revision.checked_add(1)) else {
        return Ok(Outcome::Exhausted);
    };
    let grant = Grant {
        request: request.clone(),
        fence: fence.to_string(),
        revision: revision.to_string(),
    };
    let value = serde_json::to_string(&grant).map_err(|e| e.to_string())?;
    tx.put("owner", &value)?;
    tx.put("fence", &grant.fence)?;
    tx.put("revision", &grant.revision)?;
    tx.put(&dedupe_key, &value)?;
    Ok(Outcome::Granted {
        grant,
        replayed: false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    #[derive(Default)]
    struct Memory(BTreeMap<String, String>);
    impl Transaction for Memory {
        fn get(&self, key: &str) -> Result<Option<String>, String> {
            Ok(self.0.get(key).cloned())
        }
        fn put(&mut self, key: &str, value: &str) -> Result<(), String> {
            self.0.insert(key.into(), value.into());
            Ok(())
        }
    }
    fn request(id: &str) -> Request {
        Request {
            request_id: id.into(),
            owner: "alice".into(),
        }
    }
    #[test]
    fn grant_contention_replay_and_payload_conflict() {
        let mut tx = Memory::default();
        let a = request("a");
        let Outcome::Granted {
            grant,
            replayed: false,
        } = try_grant(&mut tx, &a).unwrap()
        else {
            panic!()
        };
        assert_eq!(grant.fence, "1");
        assert_eq!(tx.0.len(), 4);
        let before = tx.0.clone();
        assert_eq!(
            try_grant(&mut tx, &request("b")).unwrap(),
            Outcome::Contended
        );
        assert_eq!(
            try_grant(&mut tx, &a).unwrap(),
            Outcome::Granted {
                grant,
                replayed: true
            }
        );
        assert_eq!(
            try_grant(
                &mut tx,
                &Request {
                    owner: "bob".into(),
                    ..a
                }
            )
            .unwrap(),
            Outcome::RequestIdConflict
        );
        assert_eq!(tx.0, before);
    }
    #[test]
    fn exact_counters_and_overflow_fail_closed() {
        for start in [0, (1 << 53) - 1, 1 << 53, (1 << 53) + 1, u64::MAX - 1] {
            let mut tx = Memory::default();
            tx.put("fence", &start.to_string()).unwrap();
            let Outcome::Granted { grant, .. } = try_grant(&mut tx, &request("a")).unwrap() else {
                panic!()
            };
            assert_eq!(counter(&grant.fence).unwrap(), start + 1);
        }
        for key in ["fence", "revision"] {
            let mut tx = Memory::default();
            tx.put(key, &u64::MAX.to_string()).unwrap();
            let before = tx.0.clone();
            assert_eq!(
                try_grant(&mut tx, &request("a")).unwrap(),
                Outcome::Exhausted
            );
            assert_eq!(tx.0, before);
        }
    }
    #[test]
    fn historical_replay_never_grants_again() {
        let mut tx = Memory::default();
        let a = try_grant(&mut tx, &request("a")).unwrap();
        tx.0.remove("owner"); // fixture availability, not Release semantics
        let b = try_grant(&mut tx, &request("b")).unwrap();
        let before = tx.0.clone();
        let Outcome::Granted { grant: first, .. } = a else {
            panic!()
        };
        let Outcome::Granted { grant: second, .. } = b else {
            panic!()
        };
        assert_eq!(second.fence, "2");
        assert_eq!(
            try_grant(&mut tx, &request("a")).unwrap(),
            Outcome::Granted {
                grant: first,
                replayed: true
            }
        );
        assert_eq!(tx.0, before);
    }
    #[test]
    fn malformed_counters_and_requests_are_rejected() {
        for value in ["01", "-1", "+1", "1.0", "18446744073709551616", ""] {
            assert!(counter(value).is_err());
        }
        let mut tx = Memory::default();
        assert!(try_grant(&mut tx, &request("")).is_err());
        tx.put("fence", "corrupt").unwrap();
        assert!(try_grant(&mut tx, &request("a")).is_err());
        assert!(tx.get("owner").unwrap().is_none());
    }
}
