//! Bounded exhaustive traces. No randomness, sleep, clock, or cloud API.
use pintle_core::{Grant, Outcome, Request, Transaction, try_grant};
use std::{cell::Cell, collections::BTreeMap};

#[derive(Clone, Default)]
struct Store(BTreeMap<String, String>);
impl Transaction for Store {
    fn get(&self, key: &str) -> Result<Option<String>, String> {
        Ok(self.0.get(key).cloned())
    }
    fn put(&mut self, key: &str, value: &str) -> Result<(), String> {
        self.0.insert(key.into(), value.into());
        Ok(())
    }
}

// Independent abstract state: numeric counters and receipts, no storage algorithm.
#[derive(Clone, Debug)]
struct Model {
    fence: u64,
    revision: u64,
    owner: Option<Grant>,
    receipts: BTreeMap<String, Grant>,
}
impl Model {
    fn grant(&mut self, r: &Request) -> Outcome {
        if let Some(g) = self.receipts.get(&r.request_id) {
            return if g.request.owner == r.owner {
                Outcome::Granted {
                    grant: g.clone(),
                    replayed: true,
                }
            } else {
                Outcome::RequestIdConflict
            };
        }
        if self.owner.is_some() {
            return Outcome::Contended;
        }
        if self.fence == u64::MAX || self.revision == u64::MAX {
            return Outcome::Exhausted;
        }
        self.fence += 1;
        self.revision += 1;
        let grant = Grant {
            request: r.clone(),
            fence: self.fence.to_string(),
            revision: self.revision.to_string(),
        };
        self.receipts.insert(r.request_id.clone(), grant.clone());
        self.owner = Some(grant.clone());
        Outcome::Granted {
            grant,
            replayed: false,
        }
    }
    fn rows(&self) -> BTreeMap<String, String> {
        let mut rows = BTreeMap::from([
            ("fence".into(), self.fence.to_string()),
            ("revision".into(), self.revision.to_string()),
        ]);
        if let Some(owner) = &self.owner {
            rows.insert("owner".into(), serde_json::to_string(owner).unwrap());
        }
        for (id, grant) in &self.receipts {
            rows.insert(
                format!("request/{id}"),
                serde_json::to_string(grant).unwrap(),
            );
        }
        rows
    }
}
fn request(id: &str, owner: &str) -> Request {
    Request {
        request_id: id.into(),
        owner: owner.into(),
    }
}

#[derive(Clone, Copy, Debug)]
enum Event {
    Alice,
    BobSameId,
    OtherId,
    Vacate,
    Restart,
    Abort,
    LostResponse,
    UnknownAck,
}
const EVENTS: [Event; 8] = [
    Event::Alice,
    Event::BobSameId,
    Event::OtherId,
    Event::Vacate,
    Event::Restart,
    Event::Abort,
    Event::LostResponse,
    Event::UnknownAck,
];

fn explore(model: Model, store: Store, trace: &mut Vec<Event>, count: &mut usize) {
    if trace.len() == 5 {
        return;
    }
    for event in EVENTS {
        let mut model = model.clone();
        let mut store = store.clone();
        trace.push(event);
        *count += 1;
        match event {
            Event::Vacate => {
                // P0 fixture operation only, not a production Release model.
                if model.revision != u64::MAX {
                    model.revision += 1;
                    model.owner = None;
                    store.0.remove("owner");
                    let revision = store.0["revision"].parse::<u64>().unwrap();
                    store
                        .0
                        .insert("revision".into(), (revision + 1).to_string());
                }
            }
            Event::Restart => {
                store.0 = serde_json::from_str(&serde_json::to_string(&store.0).unwrap()).unwrap();
            }
            Event::Abort => {
                // Deliberately discard a successful candidate transaction.
                let mut candidate = store.clone();
                let _ = try_grant(&mut candidate, &request("aborted", "alice")).unwrap();
            }
            _ => {
                let req = match event {
                    Event::BobSameId => request("a", "bob"),
                    Event::OtherId => request("b", "alice"),
                    _ => request("a", "alice"),
                };
                let expected = model.grant(&req);
                let actual = try_grant(&mut store, &req).unwrap();
                assert_eq!(actual, expected, "trace={trace:?}");
                if matches!(event, Event::LostResponse | Event::UnknownAck) {
                    // Commit retained, caller retries the SAME logical request.
                    let replay = try_grant(&mut store, &req).unwrap();
                    assert_eq!(replay, model.grant(&req), "retry trace={trace:?}");
                }
            }
        }
        assert_eq!(store.0, model.rows(), "state trace={trace:?}");
        explore(model, store, trace, count);
        trace.pop();
    }
}
#[test]
fn exhaustive_five_event_traces_against_independent_model() {
    let mut count = 0;
    for (fence, revision) in [
        (0, 0),
        ((1u64 << 53) - 1, 1u64 << 53),
        (u64::MAX - 1, u64::MAX - 1),
        (u64::MAX, 0),
        (0, u64::MAX),
    ] {
        let model = Model {
            fence,
            revision,
            owner: None,
            receipts: BTreeMap::new(),
        };
        let store = Store(model.rows());
        explore(model, store, &mut Vec::new(), &mut count);
    }
    assert_eq!(count, 187240); // five initial states * sum(8^1 .. 8^5)
    println!("validated {count} trace prefixes; depth=5 alphabet=8 initial_states=5");
}

struct FaultTx {
    store: Store,
    read: Cell<usize>,
    write: usize,
    fail_read: usize,
    fail_write: usize,
}
impl Transaction for FaultTx {
    fn get(&self, key: &str) -> Result<Option<String>, String> {
        let n = self.read.get() + 1;
        self.read.set(n);
        if n == self.fail_read {
            return Err("injected read error".into());
        }
        self.store.get(key)
    }
    fn put(&mut self, key: &str, value: &str) -> Result<(), String> {
        self.store.put(key, value)?;
        self.write += 1;
        if self.write == self.fail_write {
            return Err("injected write error".into());
        }
        Ok(())
    }
}
#[test]
fn every_read_and_write_failure_aborts_and_same_id_can_retry() {
    for read_failure in [true, false] {
        for position in 1..=4 {
            let mut durable = Store::default();
            let before = durable.0.clone();
            let mut tx = FaultTx {
                store: durable.clone(),
                read: Cell::new(0),
                write: 0,
                fail_read: if read_failure { position } else { 0 },
                fail_write: if read_failure { 0 } else { position },
            };
            let result = try_grant(&mut tx, &request("a", "alice"));
            assert!(result.is_err(), "read={read_failure} position={position}");
            // The adapter contract publishes only successful transactions.
            if result.is_ok() {
                durable = tx.store;
            }
            assert_eq!(durable.0, before);
            assert!(matches!(
                try_grant(&mut durable, &request("a", "alice")).unwrap(),
                Outcome::Granted {
                    replayed: false,
                    ..
                }
            ));
        }
    }
}
#[test]
fn input_lengths_and_invalid_counters_leave_state_unchanged() {
    for len in [0, 1, 127, 128, 129] {
        for field in ["request_id", "owner"] {
            let mut req = request("a", "alice");
            if field == "request_id" {
                req.request_id = "x".repeat(len);
            } else {
                req.owner = "x".repeat(len);
            }
            let mut tx = Store::default();
            let result = try_grant(&mut tx, &req);
            assert_eq!(result.is_ok(), (1..=128).contains(&len), "{field}={len}");
            if result.is_err() {
                assert!(tx.0.is_empty());
            }
        }
    }
    for field in ["fence", "revision"] {
        for value in [
            "",
            "00",
            "01",
            "+1",
            "-1",
            "1.0",
            "1e3",
            " 1",
            "1 ",
            "١",
            "18446744073709551616",
        ] {
            let mut tx = Store(BTreeMap::from([(field.into(), value.into())]));
            let before = tx.0.clone();
            assert!(
                try_grant(&mut tx, &request("a", "alice")).is_err(),
                "{field}={value}"
            );
            assert_eq!(tx.0, before);
        }
    }
}
#[test]
fn malformed_dedupe_never_returns_a_successful_receipt() {
    let good = Grant {
        request: request("a", "alice"),
        fence: "1".into(),
        revision: "1".into(),
    };
    let mut bad = vec!["{".into(), "{}".into()];
    for (fence, revision) in [
        ("01", "1"),
        ("0", "1"),
        ("1", "0"),
        ("18446744073709551616", "1"),
    ] {
        bad.push(
            serde_json::to_string(&Grant {
                fence: fence.into(),
                revision: revision.into(),
                ..good.clone()
            })
            .unwrap(),
        );
    }
    bad.push(
        serde_json::to_string(&Grant {
            request: request("wrong", "alice"),
            ..good
        })
        .unwrap(),
    );
    for value in bad {
        let mut tx = Store(BTreeMap::from([("request/a".into(), value)]));
        let before = tx.0.clone();
        assert!(try_grant(&mut tx, &request("a", "alice")).is_err());
        assert_eq!(tx.0, before);
    }
}
