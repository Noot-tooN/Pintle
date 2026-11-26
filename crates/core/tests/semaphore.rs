use pintle_core::{
    Transaction,
    lease::{self, Command, Lease, LeaseState, Mutation, Rejection, Reply},
    semaphore,
    session::{
        self, Frame, Output,
        waiting::{self, Action, State},
    },
};
use std::collections::BTreeMap;

#[derive(Clone, Default)]
struct Store {
    rows: BTreeMap<String, String>,
    writes: usize,
    fail: usize,
    nonce: u64,
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
    let mut tx = s.clone();
    tx.writes = 0;
    let r = f(&mut tx)?;
    *s = tx;
    Ok(r)
}
fn setup() -> Store {
    let mut s = Store::default();
    semaphore::initialize(&mut s).unwrap();
    s
}
fn token(n: u64) -> String {
    format!("{n:064x}")
}
fn cmd(id: &str, capacity: u32) -> Command {
    Command::AcquirePermit {
        id: id.into(),
        holder: "worker".into(),
        ttl_ms: 100,
        capacity,
    }
}
fn run(s: &mut Store, c: &Command, now: u64, n: u64) -> Reply {
    atomic(s, |tx| lease::execute(tx, "alice", c, now, &token(n))).unwrap()
}
fn grant(r: Reply) -> Lease {
    match r {
        Reply::Applied { receipt, .. } => match receipt.mutation {
            Mutation::Acquired(l) | Mutation::Renewed(l) => l,
            _ => panic!(),
        },
        _ => panic!("{r:?}"),
    }
}
fn observed(r: Reply) -> LeaseState {
    match r {
        Reply::Applied {
            observation: Some(o),
            replayed: true,
            ..
        } => o.state,
        _ => panic!("{r:?}"),
    }
}
fn frame(n: u64) -> Frame {
    Frame {
        session_id: "1".into(),
        sequence: n,
        acknowledged: 0,
    }
}
fn apply(s: &mut Store, a: Action<'_>, now: u64) -> Result<Output, String> {
    let mut nonce = s.nonce;
    let result = atomic(s, |tx| {
        waiting::apply_with_tokens(tx, "alice", a, now, &mut || {
            nonce += 1;
            Ok(token(nonce))
        })
    });
    if result.is_ok() {
        s.nonce = nonce;
    }
    result
}
fn status(s: &mut Store, n: u64, now: u64) -> Output {
    atomic(s, |tx| {
        session::execute(
            tx,
            "alice",
            &frame(n),
            &Command::Status {
                id: format!("a{n}"),
            },
            now,
            "",
        )
    })
    .unwrap()
}
fn output_lease(o: Output) -> Lease {
    match o {
        Output::Operation(r) => grant(r),
        _ => panic!("{o:?}"),
    }
}
fn queued() -> Store {
    let mut s = setup();
    session::open(&mut s, "alice", 1000).unwrap();
    for n in 1..=4 {
        let r = apply(
            &mut s,
            Action::Acquire {
                frame: &frame(n),
                command: &cmd(&format!("a{n}"), 2),
                duration: 500,
                cancel: false,
            },
            1000,
        )
        .unwrap();
        if n <= 2 {
            output_lease(r);
        } else {
            assert!(matches!(r, Output::Waiting(State::Pending)));
        }
    }
    s
}
#[test]
fn capacity_slot_fencing_and_independent_lifecycles() {
    let mut s = setup();
    let a = grant(run(&mut s, &cmd("a", 2), 1000, 1));
    let b = grant(run(&mut s, &cmd("b", 2), 1000, 2));
    assert_eq!(
        (
            a.permit.as_ref().unwrap().slot,
            b.permit.as_ref().unwrap().slot
        ),
        (1, 2)
    );
    assert_eq!(run(&mut s, &cmd("full", 2), 1000, 3), Reply::Contended);
    assert_eq!(
        run(&mut s, &cmd("resize", 3), 1000, 3),
        Reply::Rejected(Rejection::CapacityMismatch)
    );
    assert_eq!(
        observed(run(&mut s, &cmd("a", 2), 1000, 99)),
        LeaseState::Active
    );
    let release = Command::Release {
        id: "release".into(),
        token: a.token.clone(),
    };
    run(&mut s, &release, 1000, 0);
    assert_eq!(
        observed(run(&mut s, &cmd("a", 2), 1000, 0)),
        LeaseState::Released
    );
    let c = grant(run(&mut s, &cmd("c", 2), 1000, 3));
    assert_eq!(c.permit, a.permit);
    assert_eq!(c.fence, "3");
    assert_eq!(
        observed(run(&mut s, &cmd("a", 2), 1000, 0)),
        LeaseState::Superseded
    );
    assert_eq!(
        observed(run(&mut s, &cmd("b", 2), 1000, 0)),
        LeaseState::Active
    );
    assert!(matches!(
        run(&mut s, &release, 1000, 0),
        Reply::Applied { replayed: true, .. }
    ));
    assert_eq!(
        observed(run(&mut s, &cmd("c", 2), 1000, 0)),
        LeaseState::Active
    );
    let stale = Command::Release {
        id: "stale".into(),
        token: a.token,
    };
    assert_eq!(
        run(&mut s, &stale, 1000, 0),
        Reply::Rejected(Rejection::NotOwner)
    );
    let renew = Command::Renew {
        id: "renew".into(),
        token: b.token.clone(),
        ttl_ms: 200,
    };
    let renewed = grant(run(&mut s, &renew, 1099, 0));
    assert_eq!(renewed.fence, b.fence);
    assert_eq!(renewed.permit, b.permit);
    assert_eq!(renewed.expires_at_ms, "1299");
    assert_eq!(
        observed(run(&mut s, &cmd("c", 2), 1100, 0)),
        LeaseState::Expired
    );
    assert_eq!(
        observed(run(&mut s, &cmd("b", 2), 1100, 0)),
        LeaseState::Active
    );
    assert_eq!(
        run(&mut s, &cmd("backwards", 2), 1099, 4),
        Reply::Rejected(Rejection::ClockRegression)
    );
    assert_eq!(
        lease::execute(&mut s, "bob", &renew, 1100, "").unwrap(),
        Reply::Rejected(Rejection::PermissionDenied)
    );
    let stolen = Command::Release {
        id: "stolen".into(),
        token: b.token,
    };
    assert_eq!(
        lease::execute(&mut s, "bob", &stolen, 1100, "").unwrap(),
        Reply::Rejected(Rejection::NotOwner)
    );
}
#[test]
fn exact_expiry_and_renew_release_orders() {
    for at in [1099, 1100, 1101] {
        for renew in [true, false] {
            let mut s = setup();
            let l = grant(run(&mut s, &cmd("a", 2), 1000, 1));
            let c = if renew {
                Command::Renew {
                    id: "change".into(),
                    token: l.token,
                    ttl_ms: 100,
                }
            } else {
                Command::Release {
                    id: "change".into(),
                    token: l.token,
                }
            };
            let r = run(&mut s, &c, at, 2);
            if at < 1100 {
                assert!(matches!(r, Reply::Applied { .. }));
            } else {
                assert_eq!(r, Reply::Rejected(Rejection::LeaseLost));
            }
        }
    }
    for release_first in [true, false] {
        let mut s = setup();
        let a = grant(run(&mut s, &cmd("a", 1), 1000, 1));
        let release = Command::Release {
            id: "r".into(),
            token: a.token,
        };
        let renew = Command::Renew {
            id: "n".into(),
            token: token(1),
            ttl_ms: 200,
        };
        let (first, second) = if release_first {
            (release, renew)
        } else {
            (renew, release)
        };
        assert!(matches!(
            run(&mut s, &first, 1001, 2),
            Reply::Applied { .. }
        ));
        assert_eq!(
            matches!(run(&mut s, &second, 1001, 3), Reply::Applied { .. }),
            !release_first
        );
    }
}
#[test]
fn fifo_batch_grants_distinct_capabilities_and_no_barging() {
    let mut s = queued();
    assert_eq!(waiting::next_wakeup(&s).unwrap(), Some(1100));
    let r = apply(
        &mut s,
        Action::Execute {
            frame: &frame(5),
            command: &cmd("barge", 2),
        },
        1100,
    )
    .unwrap();
    assert!(matches!(r, Output::Operation(Reply::Contended)));
    let a = output_lease(status(&mut s, 3, 1100));
    let b = output_lease(status(&mut s, 4, 1100));
    assert_eq!((&a.fence, &b.fence), (&"3".to_owned(), &"4".to_owned()));
    assert_ne!(a.token, b.token);
    assert_ne!(a.permit, b.permit);
    assert_eq!(waiting::next_wakeup(&s).unwrap(), Some(301100));
    let retry = apply(
        &mut s,
        Action::Acquire {
            frame: &frame(3),
            command: &cmd("a3", 2),
            duration: 500,
            cancel: true,
        },
        1100,
    )
    .unwrap();
    assert_eq!(output_lease(retry), a);
    assert_eq!(s.rows["fence"], "4");
}
#[test]
fn cancellation_deadlines_ack_and_restart_do_not_revive_intents() {
    for cancel in [true, false] {
        let mut s = queued();
        if cancel {
            assert!(matches!(
                apply(
                    &mut s,
                    Action::Acquire {
                        frame: &frame(3),
                        command: &cmd("a3", 2),
                        duration: 500,
                        cancel: true
                    },
                    1000
                )
                .unwrap(),
                Output::Waiting(State::Cancelled)
            ));
        }
        let now = if cancel { 1100 } else { 1500 };
        // Persisted records alone reconstruct all coordination state.
        let raw = serde_json::to_string(&s.rows).unwrap();
        s.rows = serde_json::from_str(&raw).unwrap();
        apply(
            &mut s,
            Action::Acknowledge {
                id: "1",
                through: 0,
            },
            now,
        )
        .unwrap();
        assert!(matches!(
            status(&mut s, 3, now),
            Output::Waiting(State::Cancelled | State::Expired)
        ));
        if cancel {
            output_lease(status(&mut s, 4, now));
        } else {
            assert_eq!(s.rows["fence"], "2");
        }
        apply(
            &mut s,
            Action::Acknowledge {
                id: "1",
                through: 4,
            },
            now,
        )
        .unwrap();
        assert!(matches!(
            status(&mut s, 3, now),
            Output::Rejected(Rejection::Acknowledged)
        ));
    }
}
#[test]
fn batch_grant_rolls_back_at_every_write_and_retries() {
    let baseline = queued();
    let mut success = baseline.clone();
    apply(
        &mut success,
        Action::Acknowledge {
            id: "1",
            through: 0,
        },
        1100,
    )
    .unwrap();
    for n in 1..=success.writes {
        let mut s = baseline.clone();
        s.fail = n;
        assert!(
            apply(
                &mut s,
                Action::Acknowledge {
                    id: "1",
                    through: 0
                },
                1100
            )
            .is_err(),
            "write {n}"
        );
        assert_eq!(s.rows, baseline.rows);
        s.fail = 0;
        apply(
            &mut s,
            Action::Acknowledge {
                id: "1",
                through: 0,
            },
            1100,
        )
        .unwrap();
        assert_eq!(s.rows, success.rows);
    }
    let mut s = baseline.clone();
    let before = s.rows.clone();
    assert!(
        atomic(&mut s, |tx| waiting::drive_with_tokens(
            tx,
            1100,
            &mut || Ok(token(42))
        ))
        .is_err()
    );
    assert_eq!(
        s.rows, before,
        "duplicate generated capability must roll back both grants"
    );
}
#[test]
fn capacity_bounds_precision_overflow_and_corruption() {
    for capacity in [0, 65, u32::MAX] {
        let mut s = setup();
        assert_eq!(
            run(&mut s, &cmd("a", capacity), 1000, 1),
            Reply::Rejected(Rejection::InvalidArgument)
        );
    }
    for capacity in [1, 2, 64] {
        let mut s = setup();
        for n in 1..=capacity {
            grant(run(
                &mut s,
                &cmd(&format!("a{n}"), capacity),
                1000,
                n.into(),
            ));
        }
        assert_eq!(
            run(&mut s, &cmd("full", capacity), 1000, 99),
            Reply::Contended
        );
    }
    for fence in [(1_u64 << 53) - 1, 1 << 53, (1 << 53) + 1, u64::MAX] {
        let mut s = setup();
        s.rows.insert("fence".into(), fence.to_string());
        s.rows.insert(
            "semaphore".into(),
            format!("{{\"slots\":[{{\"fence\":\"{fence}\",\"owner\":null}}]}}"),
        );
        let r = run(&mut s, &cmd("a", 1), 1000, 1);
        if fence == u64::MAX {
            assert_eq!(r, Reply::Rejected(Rejection::Exhausted));
        } else {
            assert_eq!(grant(r).fence, (fence + 1).to_string());
        }
    }
    let mut s = setup();
    grant(run(&mut s, &cmd("a", 2), 1000, 1));
    for bad in [
        "{}",
        "null",
        "{\"slots\":[]}",
        "{\"slots\":[{\"fence\":\"01\",\"owner\":null}]}",
    ] {
        let mut broken = s.clone();
        broken.rows.insert("semaphore".into(), bad.into());
        let before = broken.rows.clone();
        assert!(
            atomic(&mut broken, |tx| lease::execute(
                tx,
                "alice",
                &cmd("b", 2),
                1000,
                &token(2)
            ))
            .is_err()
        );
        assert_eq!(broken.rows, before);
    }
    let mut mutex = Store::default();
    lease::execute(
        &mut mutex,
        "alice",
        &Command::Acquire {
            id: "m".into(),
            holder: "w".into(),
            ttl_ms: 100,
        },
        1000,
        &token(1),
    )
    .unwrap();
    assert!(semaphore::initialize(&mut mutex).is_err());
    assert_eq!(
        run(
            &mut s,
            &Command::Acquire {
                id: "wrong".into(),
                holder: "w".into(),
                ttl_ms: 100
            },
            1000,
            2
        ),
        Reply::Rejected(Rejection::InvalidArgument)
    );
}

// Independent admission/expiry model: exhaustive prefixes over acquire, renew,
// release and three time advances. No engine storage helpers decide expectations.
#[derive(Clone)]
struct Model {
    slots: Vec<Option<(u64, u64)>>, // capability number and expiry
    now: u64,
    fence: u64,
}
fn model_walk(s: Store, m: Model, depth: usize, path: u64, count: &mut usize) {
    if depth == 0 {
        return;
    }
    for action in 0..6 {
        let mut s = s.clone();
        let mut m = m.clone();
        let nonce = path * 6 + action + 1;
        match action {
            0 => {
                let free = m
                    .slots
                    .iter()
                    .position(|p| p.is_none_or(|(_, expiry)| m.now >= expiry));
                let r = run(
                    &mut s,
                    &cmd(&format!("a{nonce}"), m.slots.len() as u32),
                    m.now,
                    nonce,
                );
                if let Some(slot) = free {
                    m.fence += 1;
                    let l = grant(r);
                    assert_eq!(l.fence, m.fence.to_string());
                    assert_eq!(l.permit.unwrap().slot, slot as u32 + 1);
                    m.slots[slot] = Some((nonce, m.now + 100));
                } else {
                    assert_eq!(r, Reply::Contended)
                }
            }
            1 | 2 => {
                let owner = m.slots[0];
                let capability = token(owner.map_or(u64::MAX, |p| p.0));
                let c = if action == 1 {
                    Command::Release {
                        id: format!("r{nonce}"),
                        token: capability,
                    }
                } else {
                    Command::Renew {
                        id: format!("n{nonce}"),
                        token: capability,
                        ttl_ms: 100,
                    }
                };
                let r = run(&mut s, &c, m.now, nonce);
                match owner {
                    None => assert_eq!(
                        r,
                        Reply::Rejected(if m.slots.iter().all(Option::is_none) {
                            Rejection::LeaseLost
                        } else {
                            Rejection::NotOwner
                        })
                    ),
                    Some((_, expiry)) if m.now >= expiry => {
                        assert_eq!(r, Reply::Rejected(Rejection::LeaseLost))
                    }
                    Some((owner, _)) => {
                        assert!(matches!(r, Reply::Applied { .. }));
                        m.slots[0] = if action == 1 {
                            None
                        } else {
                            Some((owner, m.now + 100))
                        };
                    }
                }
            }
            3 => m.now += 99,
            4 => m.now += 1,
            _ => m.now += 100,
        }
        *count += 1;
        model_walk(s, m, depth - 1, nonce, count);
    }
}
#[test]
fn exhaustive_semaphore_admission_and_time_model() {
    let mut count = 0;
    for capacity in [1, 2, 3] {
        model_walk(
            setup(),
            Model {
                slots: vec![None; capacity],
                now: 1000,
                fence: 0,
            },
            6,
            0,
            &mut count,
        );
    }
    assert_eq!(count, 167_958);
}

#[test]
fn capacity_mismatch_behind_fifo_head_is_terminal_and_recoverable() {
    let mut s = queued(); // Both slots occupied, with requests 3 and 4 pending.
    let mismatched = cmd("a5", 3);
    let attempt = frame(5);
    let wait = |command| Action::Acquire {
        frame: &attempt,
        command,
        duration: 10,
        cancel: false,
    };
    for now in [1001, 1011] {
        assert!(matches!(
            apply(&mut s, wait(&mismatched), now).unwrap(),
            Output::Operation(Reply::Rejected(Rejection::CapacityMismatch))
        ));
        assert!(matches!(
            status(&mut s, 5, now),
            Output::Operation(Reply::Rejected(Rejection::CapacityMismatch))
        ));
    }
    let corrected = cmd("a5", 2);
    assert!(matches!(
        apply(&mut s, wait(&corrected), 1011).unwrap(),
        Output::Rejected(Rejection::Conflict)
    ));
    assert!(matches!(
        status(&mut s, 3, 1011),
        Output::Waiting(State::Pending)
    ));
}

#[test]
fn drive_settles_incompatible_waiters_persisted_before_admission_check() {
    let mut s = queued();
    // A pre-fix authority could persist this second pending waiter behind a valid head.
    let mut session: serde_json::Value = serde_json::from_str(&s.rows["session/0"]).unwrap();
    session["entries"]["4"]["command"]["capacity"] = 3.into();
    s.rows.insert("session/0".into(), session.to_string());

    apply(&mut s, Action::Open, 1001).unwrap();
    assert!(matches!(
        status(&mut s, 4, 1001),
        Output::Operation(Reply::Rejected(Rejection::CapacityMismatch))
    ));
    assert!(matches!(
        status(&mut s, 3, 1001),
        Output::Waiting(State::Pending)
    ));
    assert_eq!(s.rows["fence"], "2");
}
