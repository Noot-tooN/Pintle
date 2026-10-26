use pintle_core::{
    Transaction,
    lease::{Command, Rejection, Reply},
    session::{self, Frame, Output},
};
use std::collections::BTreeMap;
#[derive(Clone, Default)]
struct Store {
    rows: BTreeMap<String, String>,
    writes: usize,
    fail: usize,
}
impl Transaction for Store {
    fn get(&self, k: &str) -> Result<Option<String>, String> {
        Ok(self.rows.get(k).cloned())
    }
    fn put(&mut self, k: &str, v: &str) -> Result<(), String> {
        self.rows.insert(k.into(), v.into());
        self.writes += 1;
        if self.writes == self.fail {
            Err("fault".into())
        } else {
            Ok(())
        }
    }
}
fn atomic<T>(s: &mut Store, f: impl FnOnce(&mut Store) -> Result<T, String>) -> Result<T, String> {
    let mut t = s.clone();
    t.writes = 0;
    let r = f(&mut t)?;
    *s = t;
    Ok(r)
}
fn open(s: &mut Store, now: u64) -> String {
    match atomic(s, |t| session::open(t, "alice", now)).unwrap() {
        Output::Session(i) => i.id,
        _ => panic!(),
    }
}
fn frame(id: &str, n: u64, ack: u64) -> Frame {
    Frame {
        session_id: id.into(),
        sequence: n,
        acknowledged: ack,
    }
}
fn acquire(id: &str) -> Command {
    Command::Acquire {
        id: id.into(),
        holder: "worker".into(),
        ttl_ms: 100,
    }
}
fn run(s: &mut Store, f: &Frame, c: &Command, now: u64) -> Output {
    atomic(s, |t| {
        session::execute(t, "alice", f, c, now, &format!("{now:064x}"))
    })
    .unwrap()
}
fn reject(r: Output, expected: Rejection) {
    assert!(matches!(r,Output::Rejected(r) if r==expected));
}
#[test]
fn recycled_history_exceeds_lifetime_cap_and_preserves_fences() {
    let mut s = Store::default();
    let id = open(&mut s, 1000);
    for n in 1..=4096 {
        let r = run(
            &mut s,
            &frame(&id, n, n - 1),
            &acquire("same-label"),
            1000 + n * 100,
        );
        assert!(matches!(
            r,
            Output::Operation(Reply::Applied {
                replayed: false,
                ..
            })
        ));
        assert_eq!(s.rows.get("fence"), Some(&n.to_string()));
        assert!(s.rows.len() <= 6); // clock, counter, session, owner, fence, revision
    }
}
#[test]
fn acknowledgement_is_not_release_and_late_duplicates_never_execute() {
    let mut s = Store::default();
    let id = open(&mut s, 1000);
    let c = acquire("a");
    let f = frame(&id, 1, 0);
    let r = run(&mut s, &f, &c, 1000);
    assert!(matches!(
        r,
        Output::Operation(Reply::Applied {
            replayed: false,
            ..
        })
    ));
    assert!(matches!(
        run(&mut s, &f, &c, 1001),
        Output::Operation(Reply::Applied { replayed: true, .. })
    ));
    let owner = s.rows["owner"].clone();
    atomic(&mut s, |t| session::ack(t, "alice", &id, 1, 1002)).unwrap();
    assert_eq!(s.rows["owner"], owner);
    reject(run(&mut s, &f, &c, 1100), Rejection::Acknowledged);
    assert_eq!(s.rows["fence"], "1");
    reject(
        run(
            &mut s,
            &frame(&id, 2, 1),
            &Command::Status {
                id: "missing".into(),
            },
            1100,
        ),
        Rejection::NotFound,
    );
}
#[test]
fn gaps_conflicts_limits_and_expiry_are_explicit() {
    let mut s = Store::default();
    let id = open(&mut s, 1000);
    run(&mut s, &frame(&id, 2, 0), &acquire("two"), 1000);
    reject(
        atomic(&mut s, |t| session::ack(t, "alice", &id, 2, 1001)).unwrap(),
        Rejection::InvalidArgument,
    );
    reject(
        run(&mut s, &frame(&id, 2, 0), &acquire("changed"), 1001),
        Rejection::Conflict,
    );
    reject(
        run(&mut s, &frame(&id, 33, 0), &acquire("outside"), 1001),
        Rejection::Exhausted,
    );
    run(&mut s, &frame(&id, 1, 0), &acquire("one"), 1001);
    atomic(&mut s, |t| session::ack(t, "alice", &id, 2, 1002)).unwrap();
    reject(
        atomic(&mut s, |t| session::ack(t, "bob", &id, 2, 1003)).unwrap(),
        Rejection::PermissionDenied,
    );
    reject(
        run(&mut s, &frame(&id, 3, 2), &acquire("late"), 301002),
        Rejection::SessionExpired,
    );
    assert_eq!(s.rows["session/0"], "null");
    reject(
        run(&mut s, &frame(&id, 3, 2), &acquire("backward"), 301001),
        Rejection::ClockRegression,
    );
    let next = open(&mut s, 301002);
    assert_ne!(id, next);
    reject(
        run(&mut s, &frame(&id, 3, 2), &acquire("late"), 301002),
        Rejection::SessionExpired,
    );
}
#[test]
fn stalled_receipts_cannot_be_kept_forever_by_heartbeats() {
    let mut s = Store::default();
    let id = open(&mut s, 1000);
    run(&mut s, &frame(&id, 1, 0), &acquire("a"), 1000);
    for now in [201000, 401000, 601000 - 1] {
        assert!(matches!(
            atomic(&mut s, |t| session::ack(t, "alice", &id, 0, now)).unwrap(),
            Output::Session(_)
        ));
    }
    reject(
        atomic(&mut s, |t| session::ack(t, "alice", &id, 0, 601000)).unwrap(),
        Rejection::SessionExpired,
    );
}
#[test]
fn session_churn_is_bounded_and_ids_never_repeat() {
    let mut s = Store::default();
    for round in 0..40 {
        let now = 1000 + round * session::IDLE_MS;
        for _ in 0..session::MAX_SESSIONS - 1 {
            open(&mut s, now);
        }
        reject(
            atomic(&mut s, |t| session::open(t, "bob", now)).unwrap(),
            Rejection::Exhausted,
        );
        assert!(s.rows.len() <= session::MAX_SESSIONS + 2);
    }
    assert_eq!(
        s.rows["session_counter"],
        (40 * (session::MAX_SESSIONS - 1)).to_string()
    );
}
#[test]
fn every_write_failure_rolls_back_cleanup_ack_and_ownership() {
    let mut base = Store::default();
    let id = open(&mut base, 1000);
    run(&mut base, &frame(&id, 1, 0), &acquire("a"), 1000);
    for mode in 0..3 {
        let f = |t: &mut Store| match mode {
            0 => session::execute(
                t,
                "alice",
                &frame(&id, 2, 1),
                &acquire("b"),
                1100,
                &"b".repeat(64),
            ),
            1 => session::ack(t, "alice", &id, 1, 1100),
            _ => session::cleanup(t, 301000).map(|_| Output::Rejected(Rejection::NotFound)),
        };
        let mut successful = base.clone();
        atomic(&mut successful, f).unwrap();
        let writes = successful.writes;
        for fail in 1..=writes {
            let mut s = base.clone();
            s.fail = fail;
            let before = s.rows.clone();
            assert!(atomic(&mut s, f).is_err());
            assert_eq!(s.rows, before);
        }
    }
}

// Independent finite session model: two sequence slots, ACK gaps, contention,
// expiry, clock regression, cleanup, and serialization/restart. No implementation
// helper is used to predict transitions.
#[test]
fn exhaustive_session_histories() {
    #[derive(Clone)]
    struct Model {
        now: u64,
        clock: u64,
        deadline: u64,
        active: bool,
        ack: u64,
        entries: BTreeMap<u64, (bool, u64)>,
        owner_until: u64,
        fence: u64,
    }
    #[derive(Debug, PartialEq)]
    enum Expected {
        Grant(bool),
        Contended,
        Ack,
        Reject(Rejection),
    }
    fn step(m: &mut Model, event: usize) -> Expected {
        if m.now < m.clock {
            return Expected::Reject(Rejection::ClockRegression);
        }
        m.clock = m.now;
        if m.now >= m.deadline {
            m.active = false;
            m.entries.clear();
        }
        if event == 6 {
            return Expected::Ack;
        }
        if !m.active {
            return Expected::Reject(Rejection::SessionExpired);
        }
        let result = if event < 2 {
            let seq = event as u64 + 1;
            if seq <= m.ack {
                return Expected::Reject(Rejection::Acknowledged);
            }
            if let Some((grant, _)) = m.entries.get(&seq) {
                if *grant {
                    Expected::Grant(true)
                } else {
                    Expected::Contended
                }
            } else {
                let grant = m.now >= m.owner_until;
                if grant {
                    m.owner_until = m.now + 100;
                    m.fence += 1;
                }
                m.entries.insert(seq, (grant, m.now));
                if grant {
                    Expected::Grant(false)
                } else {
                    Expected::Contended
                }
            }
        } else {
            let through = (event - 1) as u64;
            if through > m.ack {
                if (m.ack + 1..=through).any(|n| !m.entries.contains_key(&n)) {
                    return Expected::Reject(Rejection::InvalidArgument);
                }
                m.ack = through;
                m.entries.retain(|n, _| *n > through);
            }
            Expected::Ack
        };
        m.deadline = (m.now + 300000).min(
            m.entries
                .values()
                .map(|(_, t)| t + 600000)
                .min()
                .unwrap_or(u64::MAX),
        );
        result
    }
    fn classify(o: Output) -> Expected {
        match o {
            Output::Session(_) => Expected::Ack,
            Output::Rejected(r) => Expected::Reject(r),
            Output::Operation(Reply::Applied { replayed, .. }) => Expected::Grant(replayed),
            Output::Operation(Reply::Contended) => Expected::Contended,
            o => panic!("unexpected {o:?}"),
        }
    }
    fn walk(s: Store, m: Model, depth: usize, count: &mut usize) {
        if depth == 0 {
            return;
        }
        for event in 0..8 {
            let mut s = s.clone();
            let mut m = m.clone();
            *count += 1;
            match event {
                4 => m.now += 300000,
                5 => m.now = m.now.saturating_sub(1),
                7 => {
                    s.rows =
                        serde_json::from_str(&serde_json::to_string(&s.rows).unwrap()).unwrap();
                }
                _ => {
                    let expected = step(&mut m, event);
                    let actual = match event {
                        0 | 1 => classify(run(
                            &mut s,
                            &frame("1", event as u64 + 1, 0),
                            &acquire(&format!("r{event}")),
                            m.now,
                        )),
                        2 | 3 => classify(
                            atomic(&mut s, |t| {
                                session::ack(t, "alice", "1", (event - 1) as u64, m.now)
                            })
                            .unwrap(),
                        ),
                        6 => match atomic(&mut s, |t| session::cleanup(t, m.now)).unwrap() {
                            None => Expected::Ack,
                            Some(r) => Expected::Reject(r),
                        },
                        _ => unreachable!(),
                    };
                    assert_eq!(actual, expected, "event {event}, depth {depth}");
                    assert_eq!(
                        s.rows.get("fence").cloned().unwrap_or_else(|| "0".into()),
                        m.fence.to_string()
                    );
                    let stored: serde_json::Value =
                        serde_json::from_str(&s.rows["session/0"]).unwrap();
                    if m.active {
                        assert_eq!(stored["info"]["acknowledged"].as_u64(), Some(m.ack));
                        assert_eq!(
                            stored["entries"].as_object().unwrap().len(),
                            m.entries.len()
                        );
                    } else {
                        assert!(stored.is_null());
                    }
                }
            }
            walk(s, m, depth - 1, count);
        }
    }
    let mut s = Store::default();
    open(&mut s, 1000);
    let model = Model {
        now: 1000,
        clock: 1000,
        deadline: 301000,
        active: true,
        ack: 0,
        entries: BTreeMap::new(),
        owner_until: 0,
        fence: 0,
    };
    let mut count = 0;
    walk(s, model, 5, &mut count);
    assert_eq!(count, 37448);
    eprintln!("verified {count} bounded session trace prefixes");
}

#[test]
fn final_window_and_session_slots_are_reserved_for_owner_release() {
    let mut s = Store::default();
    let id = open(&mut s, 1000);
    let token = format!("{:064x}", 1000);
    run(
        &mut s,
        &frame(&id, 1, 0),
        &Command::Acquire {
            id: "a".into(),
            holder: "worker".into(),
            ttl_ms: 60000,
        },
        1000,
    );
    for n in 2..32 {
        run(
            &mut s,
            &frame(&id, n, 0),
            &Command::Renew {
                id: format!("r{n}"),
                token: token.clone(),
                ttl_ms: 60000,
            },
            1000,
        );
    }
    reject(
        run(&mut s, &frame(&id, 32, 0), &acquire("overflow"), 1000),
        Rejection::Exhausted,
    );
    assert!(matches!(
        run(
            &mut s,
            &frame(&id, 32, 0),
            &Command::Release {
                id: "release".into(),
                token
            },
            1000
        ),
        Output::Operation(Reply::Applied { .. })
    ));
    // Open's reservation admits the current owner when ordinary callers are full.
    let mut s = Store::default();
    let id = open(&mut s, 1000);
    run(&mut s, &frame(&id, 1, 0), &acquire("a"), 1000);
    for _ in 1..15 {
        open(&mut s, 1000);
    }
    reject(
        atomic(&mut s, |t| session::open(t, "bob", 1000)).unwrap(),
        Rejection::Exhausted,
    );
    open(&mut s, 1000);
    reject(
        atomic(&mut s, |t| session::open(t, "alice", 1000)).unwrap(),
        Rejection::Exhausted,
    );
}

#[test]
fn session_counter_precision_and_corrupt_records_fail_closed() {
    for initial in [1_u64 << 53, (1_u64 << 53) + 1, u64::MAX - 1] {
        let mut s = Store::default();
        s.rows.insert("session_counter".into(), initial.to_string());
        assert_eq!(open(&mut s, 1000), (initial + 1).to_string());
    }
    let mut s = Store::default();
    s.rows
        .insert("session_counter".into(), u64::MAX.to_string());
    reject(
        atomic(&mut s, |t| session::open(t, "alice", 1000)).unwrap(),
        Rejection::Exhausted,
    );
    let mut s = Store::default();
    open(&mut s, 1000);
    s.rows.insert("session/0".into(), "{}".into());
    let before = s.rows.clone();
    assert!(atomic(&mut s, |t| session::cleanup(t, 301000)).is_err());
    assert_eq!(s.rows, before);
}
