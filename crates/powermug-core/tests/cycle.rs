//! Cycle supervision on a simulated monotonic clock (SPECIFICATIONS §7.8).

use std::time::Duration;

use powermug_core::cycle::{CycleState, CycleSupervisor, Supervision};
use powermug_pivot::PointIndex;

const SEC: u64 = 1_000_000_000;

fn period(seconds: u64) -> Supervision {
    Supervision {
        period: Duration::from_secs(seconds),
        grace: Duration::ZERO,
    }
}

fn points(expired: &[powermug_core::cycle::Expired]) -> Vec<u32> {
    expired.iter().map(|e| e.point.0).collect()
}

#[test]
fn expires_exactly_at_the_deadline_not_before() {
    let mut sup = CycleSupervisor::new(1, 0);
    sup.supervise(PointIndex(0), period(1)).expect("in range");
    sup.accepted(PointIndex(0), 0).expect("in range");

    assert_eq!(sup.expire(SEC - 1), Vec::new());
    assert_eq!(sup.state(PointIndex(0)), Some(CycleState::Fresh));

    let expired = sup.expire(SEC);
    assert_eq!(points(&expired), vec![0]);
    assert_eq!(expired[0].deadline_ns, SEC);
    assert_eq!(sup.state(PointIndex(0)), Some(CycleState::Stale));
}

#[test]
fn a_stale_point_is_reported_only_once() {
    // PM-CYC-002: exactly one stale event, then no re-arming.
    let mut sup = CycleSupervisor::new(1, 0);
    sup.supervise(PointIndex(0), period(1)).expect("in range");
    sup.accepted(PointIndex(0), 0).expect("in range");
    assert_eq!(points(&sup.expire(SEC)), vec![0]);

    assert_eq!(sup.expire(5 * SEC), Vec::new());
    assert_eq!(sup.expire(60 * SEC), Vec::new());
}

#[test]
fn grace_extends_the_deadline() {
    let mut sup = CycleSupervisor::new(1, 0);
    sup.supervise(
        PointIndex(0),
        Supervision {
            period: Duration::from_secs(1),
            grace: Duration::from_millis(500),
        },
    )
    .expect("in range");
    sup.accepted(PointIndex(0), 0).expect("in range");

    assert_eq!(sup.expire(SEC + 400_000_000), Vec::new());
    assert_eq!(points(&sup.expire(SEC + 500_000_000)), vec![0]);
}

#[test]
fn a_newer_value_replaces_the_deadline() {
    // PM-CYC-001 and the generation counter: the first deadline is ignored.
    let mut sup = CycleSupervisor::new(1, 0);
    sup.supervise(PointIndex(0), period(1)).expect("in range");
    sup.accepted(PointIndex(0), 0).expect("in range");
    sup.accepted(PointIndex(0), SEC / 2).expect("in range");

    assert_eq!(sup.expire(SEC + 200_000_000), Vec::new());
    let expired = sup.expire(SEC + SEC / 2);
    assert_eq!(points(&expired), vec![0]);
    assert_eq!(expired[0].deadline_ns, SEC + SEC / 2);
}

#[test]
fn a_fresh_value_after_stale_rearms_the_point() {
    // PM-CYC-003: the next accepted value re-arms the deadline.
    let mut sup = CycleSupervisor::new(1, 0);
    sup.supervise(PointIndex(0), period(1)).expect("in range");
    sup.accepted(PointIndex(0), 0).expect("in range");
    assert_eq!(points(&sup.expire(SEC)), vec![0]);

    sup.accepted(PointIndex(0), SEC + SEC / 2)
        .expect("in range");
    assert_eq!(sup.state(PointIndex(0)), Some(CycleState::Fresh));
    assert_eq!(sup.expire(2 * SEC + SEC / 4), Vec::new());
    assert_eq!(points(&sup.expire(5 * SEC / 2)), vec![0]);
}

#[test]
fn a_point_without_a_value_never_expires() {
    let mut sup = CycleSupervisor::new(1, 0);
    sup.supervise(PointIndex(0), period(1)).expect("in range");

    assert_eq!(sup.expire(100 * SEC), Vec::new());
    assert_eq!(sup.state(PointIndex(0)), Some(CycleState::NoValue));
}

#[test]
fn an_unsupervised_point_is_never_armed() {
    let mut sup = CycleSupervisor::new(1, 0);
    sup.accepted(PointIndex(0), 0).expect("in range");

    assert_eq!(sup.expire(100 * SEC), Vec::new());
    assert_eq!(sup.state(PointIndex(0)), Some(CycleState::Unsupervised));
}

#[test]
fn a_suspended_point_writes_no_stale_event() {
    // PM-CYC-004: the session is lost, so the point waits for resynchronization.
    let mut sup = CycleSupervisor::new(1, 0);
    sup.supervise(PointIndex(0), period(1)).expect("in range");
    sup.accepted(PointIndex(0), 0).expect("in range");
    sup.suspend(PointIndex(0)).expect("in range");

    assert_eq!(sup.expire(10 * SEC), Vec::new());
    assert_eq!(sup.state(PointIndex(0)), Some(CycleState::Suspended));
}

#[test]
fn the_first_fresh_value_after_suspension_rearms_the_point() {
    let mut sup = CycleSupervisor::new(1, 0);
    sup.supervise(PointIndex(0), period(1)).expect("in range");
    sup.accepted(PointIndex(0), 0).expect("in range");
    sup.suspend(PointIndex(0)).expect("in range");

    sup.accepted(PointIndex(0), 10 * SEC).expect("in range");
    assert_eq!(sup.state(PointIndex(0)), Some(CycleState::Fresh));
    assert_eq!(sup.expire(10 * SEC + SEC / 2), Vec::new());
    assert_eq!(points(&sup.expire(11 * SEC)), vec![0]);
}

#[test]
fn a_deadline_beyond_one_wheel_lap_fires_at_the_right_time() {
    // The wheel spans about 410 s. A 1000 s period wraps more than twice.
    let mut sup = CycleSupervisor::new(1, 0);
    sup.supervise(PointIndex(0), period(1000))
        .expect("in range");
    sup.accepted(PointIndex(0), 0).expect("in range");

    assert_eq!(sup.expire(999 * SEC), Vec::new());
    assert_eq!(points(&sup.expire(1000 * SEC)), vec![0]);
}

#[test]
fn a_jump_of_hours_reports_every_expired_point_once() {
    let mut sup = CycleSupervisor::new(3, 0);
    for index in 0..3 {
        sup.supervise(PointIndex(index), period(1))
            .expect("in range");
    }
    sup.accepted(PointIndex(0), 0).expect("in range");
    sup.accepted(PointIndex(1), 0).expect("in range");
    sup.accepted(PointIndex(2), 5 * SEC).expect("in range");

    let mut expired = points(&sup.expire(2 * 3600 * SEC));
    expired.sort_unstable();
    assert_eq!(expired, vec![0, 1, 2]);
    assert_eq!(sup.expire(2 * 3600 * SEC + 1), Vec::new());
}

#[test]
fn points_outside_the_table_are_refused() {
    let mut sup = CycleSupervisor::new(2, 0);
    assert!(sup.supervise(PointIndex(9), period(1)).is_err());
    assert!(sup.accepted(PointIndex(9), 0).is_err());
    assert!(sup.suspend(PointIndex(9)).is_err());
    assert_eq!(sup.state(PointIndex(9)), None);
}
