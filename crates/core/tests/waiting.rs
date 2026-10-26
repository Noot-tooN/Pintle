use pintle_core::{
    Transaction,
    lease::{Command, Mutation, Rejection, Reply},
    session::{
        self, Frame, Output,
        waiting::{self, State},
    },
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
fn cmd(id: &str) -> Command {
    Command::Acquire {
        id: id.into(),
        holder: "worker".into(),
        ttl_ms: 100,
    }
}
fn frame(seq: u64) -> Frame {
    Frame {
        session_id: "1".into(),
        sequence: seq,
        acknowledged: 0,
    }
}
fn token(n: u64) -> String {
    format!("{n:064x}")
}
fn setup() -> Store {
    let mut s = Store::default();
    atomic(&mut s, |t| session::open(t, "alice", 1000)).unwrap();
    atomic(&mut s, |t| {
        session::execute(t, "alice", &frame(1), &cmd("owner"), 1000, &token(1))
    })
    .unwrap();
    s
}
fn wait(s: &mut Store, n: u64, cancel: bool, now: u64) -> Output {
    atomic(s, |t| {
        waiting::apply(
            t,
            "alice",
            waiting::Action::Acquire {
                frame: &frame(n),
                command: &cmd(&format!("wait{n}")),
                duration: 200,
                cancel,
            },
            now,
            &token(n),
        )
    })
    .unwrap()
}
fn status(s: &mut Store, n: u64, now: u64) -> Output {
    atomic(s, |t| {
        session::execute(
            t,
            "alice",
            &frame(n),
            &Command::Status {
                id: format!("wait{n}"),
            },
            now,
            "",
        )
    })
    .unwrap()
}
fn drive(s: &mut Store, now: u64) {
    assert_eq!(
        atomic(s, |t| waiting::drive(t, now, &token(99))).unwrap(),
        None
    );
}
fn release(s: &mut Store, now: u64) {
    atomic(s, |t| {
        let r = session::execute(
            t,
            "alice",
            &frame(20),
            &Command::Release {
                id: "release".into(),
                token: token(1),
            },
            now,
            "",
        )?;
        waiting::drive(t, now, &token(98))?;
        Ok(r)
    })
    .unwrap();
}
fn state(o: Output) -> &'static str {
    match o {
        Output::Waiting(State::Pending) => "pending",
        Output::Waiting(State::Cancelled) => "cancelled",
        Output::Waiting(State::Expired) => "expired",
        Output::Operation(Reply::Applied { .. }) => "granted",
        Output::Rejected(Rejection::NotFound) => "absent",
        other => panic!("unexpected {other:?}"),
    }
}
#[test]
fn fifo_release_cancel_and_no_barging() {
    let mut s = setup();
    assert_eq!(state(wait(&mut s, 2, false, 1000)), "pending");
    assert_eq!(state(wait(&mut s, 3, false, 1000)), "pending");
    assert!(matches!(
        atomic(&mut s, |t| session::ack(t, "alice", "1", 2, 1000)).unwrap(),
        Output::Rejected(Rejection::InvalidArgument)
    ));
    release(&mut s, 1001);
    assert_eq!(state(status(&mut s, 2, 1001)), "granted");
    assert_eq!(state(status(&mut s, 3, 1001)), "pending");
    assert_eq!(state(wait(&mut s, 2, true, 1001)), "granted"); // cancel never releases a grant
    drive(&mut s, 1101);
    assert_eq!(state(status(&mut s, 3, 1101)), "granted");
    assert_eq!(s.rows["fence"], "3");
    let mut s = setup();
    assert_eq!(state(wait(&mut s, 2, true, 1000)), "cancelled");
    release(&mut s, 1001);
    assert_eq!(state(wait(&mut s, 2, false, 1001)), "cancelled");
    assert_eq!(s.rows["fence"], "1");
}
#[test]
fn exact_deadlines_sessions_conflicts_and_quota() {
    for now in [1199, 1200, 1201] {
        let mut s = setup();
        wait(&mut s, 2, false, 1000);
        drive(&mut s, now);
        assert_eq!(
            state(status(&mut s, 2, now)),
            if now < 1200 { "granted" } else { "expired" }
        );
    }
    let mut s = setup();
    wait(&mut s, 2, false, 1000);
    assert!(matches!(
        atomic(&mut s, |t| waiting::acquire(
            t,
            "bob",
            &frame(2),
            &cmd("wait2"),
            200,
            true,
            1000,
            ""
        ))
        .unwrap(),
        Output::Rejected(Rejection::PermissionDenied)
    ));
    assert!(matches!(
        atomic(&mut s, |t| waiting::acquire(
            t,
            "alice",
            &frame(2),
            &cmd("changed"),
            200,
            false,
            1000,
            ""
        ))
        .unwrap(),
        Output::Rejected(Rejection::Conflict)
    ));
    assert!(matches!(
        atomic(&mut s, |t| session::execute(
            t,
            "alice",
            &frame(2),
            &cmd("wait2"),
            1000,
            ""
        ))
        .unwrap(),
        Output::Rejected(Rejection::Conflict)
    ));
    for n in 3..=17 {
        assert_eq!(state(wait(&mut s, n, false, 1000)), "pending");
    }
    assert!(matches!(
        wait(&mut s, 18, false, 1000),
        Output::Rejected(Rejection::Exhausted)
    ));
    assert_eq!(state(wait(&mut s, 18, true, 1000)), "cancelled");
    drive(&mut s, 301000);
    assert!(matches!(
        status(&mut s, 2, 301000),
        Output::Rejected(Rejection::SessionExpired)
    ));
    assert_eq!(s.rows["fence"], "1");
}
#[test]
fn rollback_registration_cancellation_and_grant_every_write() {
    for operation in 0..3 {
        let mut base = setup();
        if operation != 0 {
            wait(&mut base, 2, false, 1000);
        }
        let transition = |s: &mut Store| -> Result<(), String> {
            match operation {
                0 => {
                    waiting::acquire(
                        s,
                        "alice",
                        &frame(2),
                        &cmd("wait2"),
                        200,
                        false,
                        1000,
                        &token(2),
                    )?;
                }
                1 => {
                    waiting::acquire(
                        s,
                        "alice",
                        &frame(2),
                        &cmd("wait2"),
                        200,
                        true,
                        1000,
                        &token(2),
                    )?;
                }
                _ => {
                    waiting::drive(s, 1100, &token(2))?;
                }
            }
            Ok(())
        };
        let mut success = base.clone();
        atomic(&mut success, transition).unwrap();
        for n in 1..=success.writes {
            let mut s = base.clone();
            s.fail = n;
            assert!(atomic(&mut s, transition).is_err());
            assert_eq!(s.rows, base.rows);
        }
    }
}
#[test]
fn precise_queue_tickets_and_overflow() {
    let mut s = setup();
    s.rows
        .insert("wait_counter".into(), (1u64 << 53).to_string());
    wait(&mut s, 2, false, 1000);
    assert_eq!(s.rows["wait_counter"], ((1u64 << 53) + 1).to_string());
    s.rows.insert("wait_counter".into(), u64::MAX.to_string());
    assert!(matches!(
        wait(&mut s, 3, false, 1000),
        Output::Rejected(Rejection::Exhausted)
    ));
    let before = s.rows.clone();
    assert_eq!(
        atomic(&mut s, |t| waiting::drive(t, 999, &token(2))).unwrap(),
        Some(Rejection::ClockRegression)
    );
    assert_eq!(s.rows, before);
}
#[derive(Clone)]
struct Model {
    now: u64,
    owner_until: u64,
    fence: u64,
    state: &'static str,
    deadline: u64,
}
fn explore(s: Store, m: Model, depth: usize, count: &mut usize) {
    if depth == 0 {
        return;
    }
    for event in 0..6 {
        let mut s = s.clone();
        let mut m = m.clone();
        match event {
            0 | 1 => {
                if m.state == "absent" {
                    m.deadline = m.now + 200;
                    m.state = if event == 1 { "cancelled" } else { "pending" };
                } else if m.state == "pending" && event == 1 {
                    m.state = if m.now >= m.deadline {
                        "expired"
                    } else {
                        "cancelled"
                    };
                }
                wait(&mut s, 2, event == 1, m.now);
            }
            2 => {
                if m.now < m.owner_until && m.fence == 1 {
                    m.owner_until = 0;
                }
                release(&mut s, m.now);
            }
            3 => {
                m.now += 100;
                drive(&mut s, m.now);
            }
            4 => {
                drive(&mut s, m.now);
            }
            _ => {
                s.rows = serde_json::from_str(&serde_json::to_string(&s.rows).unwrap()).unwrap();
            }
        }
        // All operation/notification paths drive; restart alone does not create work.
        if event != 5 && m.state == "pending" {
            if m.now >= m.deadline {
                m.state = "expired";
            } else if m.now >= m.owner_until {
                m.state = "granted";
                m.fence += 1;
                m.owner_until = m.now + 100;
            }
        }
        assert_eq!(state(status(&mut s, 2, m.now)), m.state, "event {event}");
        assert_eq!(s.rows["fence"], m.fence.to_string());
        if let Output::Operation(Reply::Applied { receipt, .. }) = status(&mut s, 2, m.now) {
            assert!(matches!(receipt.mutation, Mutation::Acquired(_)));
        }
        *count += 1;
        explore(s, m, depth - 1, count);
    }
}
#[test]
fn independent_wait_model() {
    let mut count = 0;
    explore(
        setup(),
        Model {
            now: 1000,
            owner_until: 1100,
            fence: 1,
            state: "absent",
            deadline: 0,
        },
        5,
        &mut count,
    );
    assert_eq!(count, 9330);
    println!("P2 independent model: {count} trace prefixes; depth 5; six events");
}

#[test]
fn renewal_reschedules_expiry_and_pending_cannot_be_acknowledged() {
    let mut s = setup();
    wait(&mut s, 2, false, 1000);
    assert_eq!(waiting::next_wakeup(&s).unwrap(), Some(1100));
    let renew = Command::Renew {
        id: "renew".into(),
        token: token(1),
        ttl_ms: 200,
    };
    let result = atomic(&mut s, |t| {
        waiting::apply(
            t,
            "alice",
            waiting::Action::Execute {
                frame: &frame(3),
                command: &renew,
            },
            1050,
            &token(3),
        )
    })
    .unwrap();
    assert!(matches!(result, Output::Operation(Reply::Applied { .. })));
    assert_eq!(waiting::next_wakeup(&s).unwrap(), Some(1200));
    drive(&mut s, 1100);
    assert_eq!(state(status(&mut s, 2, 1100)), "pending");
    assert!(matches!(
        atomic(&mut s, |t| waiting::apply(
            t,
            "alice",
            waiting::Action::Acknowledge {
                id: "1",
                through: 3
            },
            1100,
            &token(3)
        ))
        .unwrap(),
        Output::Rejected(Rejection::InvalidArgument)
    ));
    drive(&mut s, 1200);
    assert_eq!(state(status(&mut s, 2, 1200)), "expired");
    assert_eq!(s.rows["fence"], "1");
    assert!(matches!(
        atomic(&mut s, |t| waiting::apply(
            t,
            "alice",
            waiting::Action::Acknowledge {
                id: "1",
                through: 3
            },
            1200,
            &token(3)
        ))
        .unwrap(),
        Output::Session(_)
    ));
    assert!(matches!(
        wait(&mut s, 2, false, 1200),
        Output::Rejected(Rejection::Acknowledged)
    ));
}

#[derive(Clone, Debug, PartialEq)]
enum ExpectedWait {
    Absent,
    Pending { ticket: u64, deadline: u64 },
    Granted,
    Cancelled,
    Expired,
}
#[derive(Clone)]
struct QueueModel {
    now: u64,
    fence: u64,
    owner: Option<(u64, u64)>,
    waits: [ExpectedWait; 2],
    ticket: u64,
    step: u64,
}
impl QueueModel {
    fn drive(&mut self) {
        for w in &mut self.waits {
            if matches!(w,ExpectedWait::Pending{deadline,..} if self.now>=*deadline) {
                *w = ExpectedWait::Expired
            }
        }
        if self.owner.is_some_and(|(_, expiry)| self.now < expiry) {
            return;
        }
        let first = self
            .waits
            .iter()
            .enumerate()
            .filter_map(|(i, w)| {
                if let ExpectedWait::Pending { ticket, .. } = w {
                    Some((*ticket, i))
                } else {
                    None
                }
            })
            .min();
        if let Some((_, i)) = first {
            self.waits[i] = ExpectedWait::Granted;
            self.fence += 1;
            self.owner = Some((self.step + 2, self.now + 100));
        }
    }
}
fn explore_queue(s: Store, m: QueueModel, depth: usize, count: &mut usize) {
    if depth == 0 {
        return;
    }
    for event in 0..8 {
        let mut s = s.clone();
        let mut m = m.clone();
        m.step += 1;
        let old_owner = m.owner;
        if event == 5 {
            m.now += 100;
        }
        if event != 2 && event != 3 && event != 7 {
            m.drive();
        }
        match event {
            0..=3 => {
                let i = event % 2;
                let cancel = event >= 2;
                if m.waits[i] == ExpectedWait::Absent {
                    m.ticket += 1;
                    m.waits[i] = if cancel {
                        ExpectedWait::Cancelled
                    } else {
                        ExpectedWait::Pending {
                            ticket: m.ticket,
                            deadline: m.now + 300,
                        }
                    };
                } else if cancel && matches!(m.waits[i], ExpectedWait::Pending { .. }) {
                    m.waits[i] = ExpectedWait::Cancelled;
                }
                let result = atomic(&mut s, |t| {
                    waiting::apply(
                        t,
                        "alice",
                        waiting::Action::Acquire {
                            frame: &frame(i as u64 + 2),
                            command: &cmd(&format!("wait{}", i + 2)),
                            duration: 300,
                            cancel,
                        },
                        m.now,
                        &token(m.step + 2),
                    )
                });
                assert!(result.is_ok());
                m.drive();
            }
            4 => {
                let old_token = old_owner.map(|(token, _)| token).unwrap_or(1);
                if m.owner
                    .is_some_and(|(tok, expiry)| tok == old_token && m.now < expiry)
                {
                    m.owner = None;
                }
                atomic(&mut s, |t| {
                    waiting::apply(
                        t,
                        "alice",
                        waiting::Action::Execute {
                            frame: &frame(10 + m.step),
                            command: &Command::Release {
                                id: format!("release{}", m.step),
                                token: token(old_token),
                            },
                        },
                        m.now,
                        &token(m.step + 2),
                    )
                })
                .unwrap();
                m.drive();
            }
            5 | 6 => {
                atomic(&mut s, |t| waiting::drive(t, m.now, &token(m.step + 2))).unwrap();
            }
            _ => {
                s.rows = serde_json::from_str(&serde_json::to_string(&s.rows).unwrap()).unwrap();
            }
        }
        for i in 0..2 {
            let expected = match m.waits[i] {
                ExpectedWait::Absent => "absent",
                ExpectedWait::Pending { .. } => "pending",
                ExpectedWait::Granted => "granted",
                ExpectedWait::Cancelled => "cancelled",
                ExpectedWait::Expired => "expired",
            };
            assert_eq!(
                state(status(&mut s, i as u64 + 2, m.now)),
                expected,
                "event {event} step {}",
                m.step
            );
        }
        assert_eq!(s.rows["fence"], m.fence.to_string());
        let actual: Option<pintle_core::lease::Lease> =
            serde_json::from_str(&s.rows["owner"]).unwrap();
        assert_eq!(
            actual
                .as_ref()
                .map(|l| (l.token.clone(), l.expires_at_ms.clone())),
            m.owner.map(|(n, expiry)| (token(n), expiry.to_string()))
        );
        *count += 1;
        explore_queue(s, m, depth - 1, count);
    }
}
#[test]
fn independent_two_waiter_model() {
    let mut count = 0;
    explore_queue(
        setup(),
        QueueModel {
            now: 1000,
            fence: 1,
            owner: Some((1, 1100)),
            waits: [ExpectedWait::Absent, ExpectedWait::Absent],
            ticket: 0,
            step: 0,
        },
        5,
        &mut count,
    );
    assert_eq!(count, 37448);
    println!("P3 two-waiter model: {count} trace prefixes; eight events; depth five");
}
#[test]
fn regressed_queue_counter_aborts_without_partial_grant() {
    let mut s = setup();
    wait(&mut s, 2, false, 1000);
    s.rows.insert("wait_counter".into(), "0".into());
    let before = s.rows.clone();
    assert!(atomic(&mut s, |t| waiting::drive(t, 1100, &token(2))).is_err());
    assert_eq!(s.rows, before);
}
