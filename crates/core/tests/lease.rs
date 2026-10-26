use pintle_core::{Transaction, lease::*};
use std::collections::BTreeMap;
#[derive(Clone, Default)]
struct Store {
    rows: BTreeMap<String, String>,
    writes: usize,
    fail: usize,
}
impl Transaction for Store {
    fn get(&self, key: &str) -> Result<Option<String>, String> {
        Ok(self.rows.get(key).cloned())
    }
    fn put(&mut self, key: &str, value: &str) -> Result<(), String> {
        self.rows.insert(key.into(), value.into());
        self.writes += 1;
        if self.writes == self.fail {
            Err("injected".into())
        } else {
            Ok(())
        }
    }
}
fn token(n: u64) -> String {
    format!("{n:064x}")
}
fn acquire(id: &str, ttl: u64) -> Command {
    Command::Acquire {
        id: id.into(),
        holder: "worker".into(),
        ttl_ms: ttl,
    }
}
fn renew(id: &str, t: &str, ttl: u64) -> Command {
    Command::Renew {
        id: id.into(),
        token: t.into(),
        ttl_ms: ttl,
    }
}
fn release(id: &str, t: &str) -> Command {
    Command::Release {
        id: id.into(),
        token: t.into(),
    }
}
fn run(s: &mut Store, subject: &str, c: &Command, now: u64, n: u64) -> Reply {
    let mut tx = s.clone();
    tx.writes = 0;
    let result = execute(&mut tx, subject, c, now, &token(n)).unwrap();
    *s = tx;
    result
}
fn applied(r: Reply) -> (Receipt, bool, Option<Observation>) {
    match r {
        Reply::Applied {
            receipt,
            replayed,
            observation,
        } => (*receipt, replayed, observation),
        _ => panic!("{r:?}"),
    }
}
fn lease(r: Reply) -> Lease {
    let (r, _, _) = applied(r);
    match r.mutation {
        Mutation::Acquired(l) | Mutation::Renewed(l) => l,
        _ => panic!(),
    }
}
fn reject(r: Reply, why: Rejection) {
    assert_eq!(r, Reply::Rejected(why))
}
#[test]
fn exact_expiry_renew_release_and_stale_owners() {
    for delta in [99, 100, 101] {
        let mut s = Store::default();
        let a = lease(run(&mut s, "alice", &acquire("a", 100), 1000, 1));
        let r = run(&mut s, "alice", &renew("r", &a.token, 100), 1000 + delta, 2);
        if delta < 100 {
            assert_eq!(lease(r).expires_at_ms, "1199")
        } else {
            reject(r, Rejection::LeaseLost)
        }
    }
    let mut s = Store::default();
    let a = lease(run(&mut s, "alice", &acquire("a", 100), 1000, 1));
    assert_eq!(
        run(&mut s, "bob", &acquire("b", 100), 1099, 2),
        Reply::Contended
    );
    let b = lease(run(&mut s, "bob", &acquire("b", 100), 1100, 2));
    assert_eq!(b.fence, "2");
    for cmd in [
        renew("bad-renew", &a.token, 100),
        release("bad-release", &a.token),
    ] {
        reject(run(&mut s, "alice", &cmd, 1100, 3), Rejection::NotOwner)
    }
    reject(
        run(&mut s, "alice", &renew("stolen", &b.token, 100), 1100, 3),
        Rejection::NotOwner,
    );
    let (_, _, obs) = applied(run(&mut s, "alice", &acquire("a", 100), 1100, 3));
    assert_eq!(obs.unwrap().state, LeaseState::Superseded);
    let (_, replayed, _) = applied(run(&mut s, "bob", &release("release-b", &b.token), 1100, 3));
    assert!(!replayed);
    let c = lease(run(&mut s, "alice", &acquire("c", 100), 1100, 3));
    assert_eq!(c.fence, "3");
    let before = s.rows["owner"].clone();
    let (_, replayed, _) = applied(run(&mut s, "bob", &release("release-b", &b.token), 1100, 4));
    assert!(replayed);
    assert_eq!(s.rows["owner"], before);
}
#[test]
fn immutable_replay_observes_renewed_expired_and_released_leases() {
    let mut s = Store::default();
    let a = lease(run(&mut s, "alice", &acquire("a", 100), 1000, 1));
    let renewed = lease(run(&mut s, "alice", &renew("r", &a.token, 200), 1050, 2));
    assert_eq!(renewed.expires_at_ms, "1250");
    let (original, replayed, obs) = applied(run(&mut s, "alice", &acquire("a", 100), 1100, 3));
    assert!(replayed);
    assert_eq!(original.mutation, Mutation::Acquired(a.clone()));
    assert_eq!(obs.unwrap().current.unwrap(), renewed);
    let (original, _, obs) = applied(run(&mut s, "alice", &renew("r", &a.token, 200), 1250, 3));
    assert_eq!(original.mutation, Mutation::Renewed(renewed));
    assert_eq!(obs.unwrap().state, LeaseState::Expired);
    reject(
        run(&mut s, "alice", &renew("r", &a.token, 300), 1250, 3),
        Rejection::Conflict,
    );
    reject(
        run(&mut s, "bob", &Command::Status { id: "a".into() }, 1250, 3),
        Rejection::PermissionDenied,
    );
    reject(
        run(
            &mut s,
            "alice",
            &Command::Status {
                id: "unknown".into(),
            },
            1250,
            3,
        ),
        Rejection::NotFound,
    );
    reject(
        run(&mut s, "alice", &release("expired", &a.token), 1250, 3),
        Rejection::LeaseLost,
    );
}
#[test]
fn clock_regression_cannot_resurrect_observed_expiry_even_after_restart() {
    let mut s = Store::default();
    run(&mut s, "alice", &acquire("a", 100), 1000, 1);
    let (_, _, obs) = applied(run(
        &mut s,
        "alice",
        &Command::Status { id: "a".into() },
        1100,
        2,
    ));
    assert_eq!(obs.unwrap().state, LeaseState::Expired);
    s.rows = serde_json::from_str(&serde_json::to_string(&s.rows).unwrap()).unwrap();
    let before = s.rows.clone();
    reject(
        run(&mut s, "alice", &renew("r", &token(1), 100), 1099, 2),
        Rejection::ClockRegression,
    );
    assert_eq!(s.rows, before);
    assert_eq!(
        lease(run(&mut s, "bob", &acquire("b", 100), 1100, 2)).fence,
        "2"
    );
}
#[test]
fn limits_overflow_and_release_reservation() {
    for ttl in [0, 99, 100, 60000, 60001, u64::MAX] {
        let mut s = Store::default();
        let r = run(&mut s, "alice", &acquire("a", ttl), 1000, 1);
        if (MIN_TTL_MS..=MAX_TTL_MS).contains(&ttl) {
            lease(r);
        } else {
            reject(r, Rejection::InvalidArgument);
            assert!(s.rows.is_empty())
        }
    }
    for field in ["fence", "revision"] {
        let mut s = Store::default();
        s.rows.insert(field.into(), u64::MAX.to_string());
        reject(
            run(&mut s, "alice", &acquire("a", 100), 1000, 1),
            Rejection::Exhausted,
        );
        assert!(!s.rows.contains_key("owner"));
    }
    let mut s = Store::default();
    s.rows
        .insert("fence".into(), ((1u64 << 53) + 1).to_string());
    assert_eq!(
        lease(run(&mut s, "alice", &acquire("a", 100), 1000, 1)).fence,
        "9007199254740994"
    );
    let mut s = Store::default();
    run(&mut s, "alice", &acquire("a", 60000), 1000, 1);
    for i in 0..MAX_RECEIPTS - 2 {
        lease(run(
            &mut s,
            "alice",
            &renew(&format!("r{i}"), &token(1), 60000),
            1000,
            2,
        ));
    }
    reject(
        run(&mut s, "alice", &renew("full", &token(1), 100), 1000, 2),
        Rejection::Exhausted,
    );
    let (_, again, _) = applied(run(
        &mut s,
        "alice",
        &release("last-slot", &token(1)),
        1000,
        2,
    ));
    assert!(!again);
    assert_eq!(s.rows["count"], MAX_RECEIPTS.to_string());
    applied(run(
        &mut s,
        "alice",
        &Command::Status { id: "a".into() },
        1000,
        3,
    ));
    assert!(
        applied(run(
            &mut s,
            "alice",
            &release("last-slot", &token(1)),
            1000,
            3
        ))
        .1
    );
    reject(
        run(&mut s, "alice", &acquire("new", 100), 1000, 3),
        Rejection::Exhausted,
    );
}
#[test]
fn every_lifecycle_write_rolls_back_then_retries() {
    for method in ["acquire", "renew", "release"] {
        let mut base = Store::default();
        if method != "acquire" {
            run(&mut base, "alice", &acquire("a", 100), 1000, 1);
        }
        let cmd = match method {
            "acquire" => acquire("a", 100),
            "renew" => renew("r", &token(1), 100),
            _ => release("x", &token(1)),
        };
        let writes = if method == "acquire" { 6 } else { 5 };
        for position in 1..=writes {
            let mut candidate = base.clone();
            candidate.writes = 0;
            candidate.fail = position;
            assert!(
                execute(&mut candidate, "alice", &cmd, 1000, &token(1)).is_err(),
                "{method} write {position}"
            );
            // Same contract as real adapter: discard the whole failed candidate.
            let mut restored = base.clone();
            let result = run(&mut restored, "alice", &cmd, 1000, 1);
            assert!(!applied(result).1);
        }
    }
}

// Independent reference state, expressed as ownership + numeric time/counters
// and immutable operation receipts, not reads/writes through Transaction.
#[derive(Clone, Default)]
struct Reference {
    owner: Option<Lease>,
    fence: u64,
    revision: u64,
    clock: u64,
    receipts: BTreeMap<String, Receipt>,
}
impl Reference {
    fn observation(&self, receipt: &Receipt, now: u64) -> Option<Observation> {
        let original = match &receipt.mutation {
            Mutation::Acquired(l) | Mutation::Renewed(l) => l,
            _ => return None,
        };
        let (state, current) = match &self.owner {
            Some(l) if l.token == original.token && now < l.expires_at_ms.parse().unwrap() => {
                (LeaseState::Active, Some(l.clone()))
            }
            Some(l) if l.token == original.token => (LeaseState::Expired, None),
            _ if self.fence > original.fence.parse::<u64>().unwrap() => {
                (LeaseState::Superseded, None)
            }
            _ => (LeaseState::Released, None),
        };
        Some(Observation { state, current })
    }
    fn apply(&mut self, subject: &str, cmd: &Command, now: u64, nonce: u64) -> Reply {
        if now < self.clock {
            return Reply::Rejected(Rejection::ClockRegression);
        }
        self.clock = now;
        if let Some(r) = self.receipts.get(cmd.id()) {
            if r.subject != subject {
                return Reply::Rejected(Rejection::PermissionDenied);
            }
            if !matches!(cmd, Command::Status { .. }) && r.command != *cmd {
                return Reply::Rejected(Rejection::Conflict);
            }
            return Reply::Applied {
                receipt: Box::new(r.clone()),
                replayed: true,
                observation: self.observation(r, now),
            };
        }
        let mutation = match cmd {
            Command::Status { .. } => return Reply::Rejected(Rejection::NotFound),
            Command::Acquire { id, holder, ttl_ms } => {
                if self
                    .owner
                    .as_ref()
                    .is_some_and(|l| now < l.expires_at_ms.parse().unwrap())
                {
                    return Reply::Contended;
                }
                self.fence += 1;
                self.revision += 1;
                let l = Lease {
                    acquisition_id: id.clone(),
                    subject: subject.into(),
                    holder: holder.clone(),
                    token: token(nonce),
                    fence: self.fence.to_string(),
                    revision: self.revision.to_string(),
                    expires_at_ms: (now + ttl_ms).to_string(),
                };
                self.owner = Some(l.clone());
                Mutation::Acquired(l)
            }
            Command::Renew { token, .. } | Command::Release { token, .. } => {
                let Some(mut l) = self.owner.clone() else {
                    return Reply::Rejected(Rejection::LeaseLost);
                };
                if l.subject != subject || &l.token != token {
                    return Reply::Rejected(Rejection::NotOwner);
                }
                if now >= l.expires_at_ms.parse().unwrap() {
                    return Reply::Rejected(Rejection::LeaseLost);
                }
                self.revision += 1;
                if let Command::Renew { ttl_ms, .. } = cmd {
                    l.expires_at_ms = (now + ttl_ms)
                        .max(l.expires_at_ms.parse().unwrap())
                        .to_string();
                    l.revision = self.revision.to_string();
                    self.owner = Some(l.clone());
                    Mutation::Renewed(l)
                } else {
                    self.owner = None;
                    Mutation::Released {
                        token: token.clone(),
                        revision: self.revision.to_string(),
                    }
                }
            }
        };
        let r = Receipt {
            subject: subject.into(),
            command: cmd.clone(),
            mutation,
        };
        self.receipts.insert(cmd.id().into(), r.clone());
        Reply::Applied {
            receipt: Box::new(r.clone()),
            replayed: false,
            observation: self.observation(&r, now),
        }
    }
}
fn explore_leases(
    reference: Reference,
    store: Store,
    now: u64,
    trace: &mut Vec<usize>,
    visited: &mut usize,
) {
    if trace.len() == 5 {
        return;
    }
    for event in 0..12 {
        trace.push(event);
        *visited += 1;
        let mut model = reference.clone();
        let mut implementation = store.clone();
        let mut time = now;
        if event == 8 {
            time += 50
        } else if event == 9 {
            time += 100
        } else if event == 10 {
            time = time.saturating_sub(50)
        } else if event == 11 {
            implementation.rows =
                serde_json::from_str(&serde_json::to_string(&implementation.rows).unwrap())
                    .unwrap();
        } else {
            let (subject, cmd, nonce) = match event {
                0 => ("alice", acquire("a", 100), 1),
                1 => ("bob", acquire("b", 100), 2),
                2 => ("alice", renew("ra", &token(1), 100), 3),
                3 => ("alice", release("xa", &token(1)), 3),
                4 => ("bob", renew("rb", &token(2), 100), 3),
                5 => ("bob", release("xb", &token(2)), 3),
                6 => ("alice", Command::Status { id: "a".into() }, 3),
                _ => ("bob", Command::Status { id: "b".into() }, 3),
            };
            assert_eq!(
                run(&mut implementation, subject, &cmd, time, nonce),
                model.apply(subject, &cmd, time, nonce),
                "trace={trace:?}"
            );
        }
        for (key, value) in [
            ("clock", model.clock),
            ("fence", model.fence),
            ("revision", model.revision),
            ("count", model.receipts.len() as u64),
        ] {
            assert_eq!(
                implementation
                    .rows
                    .get(key)
                    .map(|s| s.parse::<u64>().unwrap())
                    .unwrap_or(0),
                value,
                "{key} trace={trace:?}"
            );
        }
        let actual: Option<Lease> = implementation
            .rows
            .get("owner")
            .map(|s| serde_json::from_str(s).unwrap())
            .unwrap_or(None);
        assert_eq!(actual, model.owner, "owner trace={trace:?}");
        for (id, expected) in &model.receipts {
            let actual: Receipt =
                serde_json::from_str(&implementation.rows[&format!("request/{id}")]).unwrap();
            assert_eq!(&actual, expected, "receipt trace={trace:?}");
        }
        assert_eq!(
            implementation
                .rows
                .keys()
                .filter(|k| k.starts_with("request/"))
                .count(),
            model.receipts.len()
        );
        explore_leases(model, implementation, time, trace, visited);
        trace.pop();
    }
}
#[test]
fn exhaustive_lease_lifecycle_time_and_restart_traces() {
    let mut visited = 0;
    explore_leases(
        Reference::default(),
        Store::default(),
        1000,
        &mut Vec::new(),
        &mut visited,
    );
    assert_eq!(visited, 271452);
    println!("P1 reference model: {visited} trace prefixes; 12 events; depth 5");
}

#[test]
fn time_overflow_and_corrupt_persisted_leases_fail_closed() {
    let mut s = Store::default();
    reject(
        run(&mut s, "alice", &acquire("a", 100), MAX_TIME_MS - 99, 1),
        Rejection::Exhausted,
    );
    assert!(!s.rows.contains_key("owner"));
    let mut s = Store::default();
    reject(
        run(&mut s, "alice", &acquire("a", 100), MAX_TIME_MS + 1, 1),
        Rejection::InvalidArgument,
    );
    assert!(s.rows.is_empty());
    let mut base = Store::default();
    run(&mut base, "alice", &acquire("a", 100), 1000, 1);
    for (row, field, value) in [
        ("owner", "expires_at_ms", "01"),
        ("owner", "token", "bad"),
        ("owner", "fence", "2"),
        ("owner", "revision", "0"),
    ] {
        let mut corrupted = base.clone();
        let mut json: serde_json::Value = serde_json::from_str(&corrupted.rows[row]).unwrap();
        json[field] = serde_json::Value::String(value.into());
        corrupted.rows.insert(row.into(), json.to_string());
        let before = corrupted.rows.clone();
        let mut candidate = corrupted.clone();
        assert!(
            execute(
                &mut candidate,
                "alice",
                &Command::Status { id: "a".into() },
                1000,
                &token(2)
            )
            .is_err()
        );
        assert_eq!(corrupted.rows, before);
    }
    let mut corrupted = base;
    let mut json: serde_json::Value = serde_json::from_str(&corrupted.rows["request/a"]).unwrap();
    json["mutation"]["Acquired"]["fence"] = serde_json::Value::String("0".into());
    corrupted.rows.insert("request/a".into(), json.to_string());
    assert!(
        execute(
            &mut corrupted,
            "alice",
            &Command::Status { id: "a".into() },
            1000,
            &token(2)
        )
        .is_err()
    );
}
