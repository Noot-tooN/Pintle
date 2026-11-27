use pintle_core::{
    cooldown::{Attempt, Cooldown, Decision::*, Error},
    lease::MAX_TIME_MS,
};

fn attempt<'a>(window_id: u64, request_id: &'a str, interval_ms: u64) -> Attempt<'a> {
    Attempt {
        window_id,
        request_id,
        interval_ms,
    }
}

#[test]
fn simultaneous_refreshes_take_the_first_callers_interval_in_either_order() {
    for (first_ms, second_ms) in [(50, 500), (500, 50)] {
        let mut index = Cooldown::default();
        // Both workers observe the same key before either admission commits.
        let first = attempt(index.prepare(1000).unwrap(), "refresh", first_ms);
        let second = attempt(index.prepare(1000).unwrap(), "refresh", second_ms);
        let retry_at_ms = 1000 + first_ms;
        assert_eq!(
            index.try_admit(1000, "worker-a", first),
            Ok(Admitted {
                admitted_at_ms: 1000,
                retry_at_ms
            })
        );
        assert_eq!(
            index.try_admit(1000, "worker-b", second),
            Ok(CoolingDown { retry_at_ms })
        );
        // A's process can disappear here. Only authority time advances.
        let just_before = attempt(index.prepare(retry_at_ms - 1).unwrap(), "early", second_ms);
        assert_eq!(
            index.try_admit(retry_at_ms - 1, "worker-b", just_before),
            Ok(CoolingDown { retry_at_ms })
        );
        let fresh = attempt(index.prepare(retry_at_ms).unwrap(), "next", second_ms);
        assert_eq!(
            index.try_admit(retry_at_ms, "worker-b", fresh),
            Ok(Admitted {
                admitted_at_ms: retry_at_ms,
                retry_at_ms: retry_at_ms + second_ms
            })
        );
    }
}

#[test]
fn lost_replies_recover_original_decisions_until_a_successor_commits() {
    let mut index = Cooldown::default();
    let window = index.prepare(10).unwrap();
    let winner = attempt(window, "a", 100);
    let loser = attempt(window, "b", 500);
    // Admission time (20), not prepare time (10), starts the cooldown.
    let admitted = Ok(Admitted {
        admitted_at_ms: 20,
        retry_at_ms: 120,
    });
    let denied = Ok(CoolingDown { retry_at_ms: 120 });
    for now in [20, 119, 120, 121] {
        assert_eq!(index.try_admit(now, "worker", winner), admitted);
        assert_eq!(index.try_admit(now, "worker", loser), denied);
        assert_eq!(
            index.try_admit(
                now,
                "worker",
                Attempt {
                    interval_ms: 1,
                    ..winner
                }
            ),
            Err(Error::RequestConflict)
        );
    }
    let next = attempt(index.prepare(130).unwrap(), "next", 40);
    assert_eq!(
        index.try_admit(130, "worker", next),
        Ok(Admitted {
            admitted_at_ms: 130,
            retry_at_ms: 170
        })
    );
    for now in [131, 170, 180] {
        for old in [winner, loser] {
            assert_eq!(
                index.try_admit(now, "worker", old),
                Ok(Superseded { retry_at_ms: 170 })
            );
        }
    }
}

#[test]
fn tenant_indexes_have_independent_windows_and_attempts() {
    let mut tenant_a = Cooldown::default();
    let mut tenant_b = Cooldown::default();
    let a = attempt(tenant_a.prepare(1).unwrap(), "refresh", 10);
    let b = attempt(tenant_b.prepare(1).unwrap(), "refresh", 99);
    assert_eq!(
        tenant_a.try_admit(1, "worker", a),
        Ok(Admitted {
            admitted_at_ms: 1,
            retry_at_ms: 11
        })
    );
    assert_eq!(
        tenant_b.try_admit(1, "worker", b),
        Ok(Admitted {
            admitted_at_ms: 1,
            retry_at_ms: 100
        })
    );
    let next = attempt(tenant_a.prepare(11).unwrap(), "next", 10);
    assert_eq!(
        tenant_a.try_admit(11, "worker", next),
        Ok(Admitted {
            admitted_at_ms: 11,
            retry_at_ms: 21
        })
    );
    let next = attempt(tenant_b.prepare(11).unwrap(), "next", 10);
    assert_eq!(
        tenant_b.try_admit(11, "worker", next),
        Ok(CoolingDown { retry_at_ms: 100 })
    );
}

#[test]
fn invalid_attempts_cannot_open_or_replace_a_window() {
    let mut index = Cooldown::default();
    let start = MAX_TIME_MS - 10;
    let window = index.prepare(start).unwrap();
    assert_eq!(index.prepare(start - 1), Err(Error::Clock));
    for interval in [0, 11, u64::MAX] {
        assert_eq!(
            index.try_admit(start, "worker", attempt(window, "bad", interval)),
            Err(Error::InvalidArgument)
        );
    }
    let valid = attempt(window, "ok", 10);
    let admitted = Ok(Admitted {
        admitted_at_ms: start,
        retry_at_ms: MAX_TIME_MS,
    });
    assert_eq!(index.try_admit(start, "worker", valid), admitted);
    // Guessing the next window ID cannot bypass active suppression.
    assert_eq!(
        index.try_admit(start, "worker", attempt(MAX_TIME_MS, "early", 1)),
        Err(Error::InvalidWindow)
    );
    assert_eq!(index.prepare(MAX_TIME_MS + 1), Err(Error::Clock));
    assert_eq!(index.try_admit(MAX_TIME_MS, "worker", valid), admitted);
}

#[test]
fn one_millisecond_and_one_day_are_inclusive_and_survive_restore() {
    use pintle_core::cooldown::MAX_INTERVAL_MS;

    let mut index = Cooldown::default();
    for (now, interval) in [(0, 1), (1, MAX_INTERVAL_MS)] {
        let window = index.prepare(now).unwrap();
        assert_eq!(
            index.try_admit(
                now,
                "worker",
                attempt(window, "too-long", MAX_INTERVAL_MS + 1)
            ),
            Err(Error::InvalidArgument)
        );
        let req = attempt(window, "ok", interval);
        let decision = index.try_admit(now, "worker", req).unwrap();
        assert_eq!(
            decision,
            Admitted {
                admitted_at_ms: now,
                retry_at_ms: now + interval
            }
        );
        index = Cooldown::restore(&serde_json::to_string(&index).unwrap()).unwrap();
        assert_eq!(index.try_admit(now, "worker", req), Ok(decision));
    }
    assert!(Cooldown::restore(r#"{"clock_ms":10}"#).is_err());
    let mut corrupt = serde_json::to_value(&index).unwrap();
    corrupt["latest"]["retry_at_ms"] = (MAX_INTERVAL_MS + 2).into();
    assert!(Cooldown::restore(&corrupt.to_string()).is_err());
}
