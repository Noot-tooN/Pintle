use pintle_core::singleflight::{
    Completion, Error, FILL_TIMEOUT_MS, Group, MAX_GENERATIONS, MAX_RESULT_BYTES, MAX_WAITERS,
    Observation::{Cancelled, Finished, Pending},
    Outcome, RECOVERY_MS,
};

#[test]
fn crashed_producer_expires_at_deadline_and_cannot_publish_to_successor() {
    let mut group = Group::default();
    let old = group.join(0).unwrap();
    let waiting = group.join(1).unwrap();
    assert_eq!(group.observe(FILL_TIMEOUT_MS - 1, waiting), Ok(Pending));
    let next = group.join(FILL_TIMEOUT_MS).unwrap();
    assert!(next.is_leader());
    assert_eq!(
        group.observe(FILL_TIMEOUT_MS, waiting),
        Ok(Finished(Outcome::LeaderExpired))
    );
    assert_eq!(
        group.complete(FILL_TIMEOUT_MS, old, Completion::Value(b"stale")),
        Ok(Outcome::LeaderExpired)
    );
    assert_eq!(group.observe(FILL_TIMEOUT_MS, next), Ok(Pending));
    let fresh = group.join(FILL_TIMEOUT_MS).unwrap();
    let value = Outcome::Value(b"fresh".to_vec());
    assert_eq!(
        group.complete(FILL_TIMEOUT_MS, next, Completion::Value(b"fresh")),
        Ok(value.clone())
    );
    assert_eq!(group.observe(FILL_TIMEOUT_MS, fresh), Ok(Finished(value)));
}

#[test]
fn committed_result_survives_producer_disappearance_and_lost_reply_in_model() {
    let mut group = Group::default();
    let leader = group.join(0).unwrap();
    let waiter = group.join(1).unwrap();
    let value = Outcome::Value(b"original".to_vec());
    assert_eq!(
        group.complete(2, leader, Completion::Value(b"original")),
        Ok(value.clone())
    );
    assert!(group.join(3).unwrap().is_leader());
    assert_eq!(
        group.complete(4, leader, Completion::Value(b"original")),
        Ok(value.clone())
    );
    assert_eq!(
        group.complete(4, leader, Completion::Failed),
        Ok(value.clone())
    );
    assert_eq!(
        group.observe(2 + RECOVERY_MS - 1, waiter),
        Ok(Finished(value))
    );
    assert_eq!(
        group.observe(2 + RECOVERY_MS, waiter),
        Err(Error::UnknownTicket)
    );
}

#[test]
fn completion_at_the_deadline_expires_even_without_another_join() {
    let mut group = Group::default();
    let leader = group.join(0).unwrap();
    let waiter = group.join(0).unwrap();
    assert_eq!(
        group.complete(FILL_TIMEOUT_MS, leader, Completion::Value(b"too late")),
        Ok(Outcome::LeaderExpired)
    );
    assert_eq!(
        group.cancel_waiter(FILL_TIMEOUT_MS, waiter),
        Ok(Finished(Outcome::LeaderExpired))
    );
}

#[test]
fn cancellation_and_completion_use_commit_order_for_each_waiter() {
    let mut group = Group::default();
    let leader = group.join(0).unwrap();
    let early = group.join(1).unwrap();
    let late = group.join(1).unwrap();
    assert_eq!(
        group.complete(2, early, Completion::Failed),
        Err(Error::WrongRole)
    );
    assert_eq!(group.cancel_waiter(2, leader), Err(Error::WrongRole));
    assert_eq!(group.cancel_waiter(2, early), Ok(Cancelled));
    assert_eq!(group.observe(2, late), Ok(Pending));
    assert_eq!(
        group.complete(3, leader, Completion::Failed),
        Ok(Outcome::FillFailed)
    );
    assert_eq!(
        group.cancel_waiter(4, late),
        Ok(Finished(Outcome::FillFailed))
    );
    assert_eq!(group.cancel_waiter(4, early), Ok(Cancelled));
    assert_eq!(group.observe(4, early), Ok(Cancelled));
}

#[test]
fn losing_the_last_waiter_does_not_stop_the_fill() {
    let mut group = Group::default();
    let leader = group.join(0).unwrap();
    let only_waiter = group.join(1).unwrap();
    assert_eq!(group.cancel_waiter(1, only_waiter), Ok(Cancelled));
    let later = group.join(2).unwrap();
    assert!(!later.is_leader());
    let empty = Outcome::Value(vec![]);
    assert_eq!(
        group.complete(3, leader, Completion::Value(&[])),
        Ok(empty.clone())
    );
    assert_eq!(group.observe(3, later), Ok(Finished(empty)));
}

#[test]
fn exact_byte_limit_and_both_oversize_reports_reach_all_attached_waiters() {
    let at_limit = vec![0xff; MAX_RESULT_BYTES];
    let over = vec![0xff; MAX_RESULT_BYTES + 1];
    for (completion, expected) in [
        (
            Completion::Value(&at_limit),
            Outcome::Value(at_limit.clone()),
        ),
        (Completion::Value(&over), Outcome::ResultTooLarge),
        (Completion::TooLarge, Outcome::ResultTooLarge),
    ] {
        let mut group = Group::default();
        let leader = group.join(0).unwrap();
        let a = group.join(0).unwrap();
        let b = group.join(0).unwrap();
        assert_eq!(group.complete(1, leader, completion), Ok(expected.clone()));
        for ticket in [a, b] {
            assert_eq!(group.observe(1, ticket), Ok(Finished(expected.clone())));
        }
    }
}

#[test]
fn bounded_recovery_preserves_receipts_under_pressure_and_reclaims_at_equality() {
    let mut group = Group::default();
    let first = group.join(0).unwrap();
    for _ in 0..MAX_WAITERS {
        let w = group.join(0).unwrap();
        assert_eq!(group.cancel_waiter(0, w), Ok(Cancelled));
    }
    assert_eq!(group.join(0), Err(Error::Capacity));
    group.complete(1, first, Completion::Failed).unwrap();
    for _ in 1..MAX_GENERATIONS {
        let l = group.join(1).unwrap();
        group.complete(1, l, Completion::Failed).unwrap();
    }
    assert_eq!(group.join(1), Err(Error::Capacity));
    assert_eq!(group.observe(0, first), Err(Error::Clock));
    assert_eq!(
        group.observe(RECOVERY_MS, first),
        Ok(Finished(Outcome::FillFailed))
    );
    let next = group.join(1 + RECOVERY_MS).unwrap();
    assert_ne!(next, first);
    assert_eq!(
        group.complete(1 + RECOVERY_MS, first, Completion::Value(b"late")),
        Err(Error::UnknownTicket)
    );
}
