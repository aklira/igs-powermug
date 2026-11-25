//! The saturation policy of the telemetry (SPECIFICATIONS 7.5, PM8): the level follows the journal
//! usage against the quota, and each class is shed or refused at the levels the policy names.

use powermug_core::saturation::{decide, level, Decision, Level, Thresholds, DEFAULT_QUOTA};
use powermug_store::EventClass;

#[test]
fn the_level_rises_with_the_usage_at_the_thresholds() {
    let t = Thresholds::default();
    let quota = 10_000;
    assert_eq!(level(0, quota, t), Level::Normal);
    assert_eq!(level(6_999, quota, t), Level::Normal);
    assert_eq!(level(7_000, quota, t), Level::Warning);
    assert_eq!(level(8_000, quota, t), Level::Throttle);
    assert_eq!(level(9_000, quota, t), Level::Shed);
    assert_eq!(level(9_999, quota, t), Level::Shed);
    assert_eq!(level(10_000, quota, t), Level::Stop);
    assert_eq!(level(20_000, quota, t), Level::Stop);
}

#[test]
fn a_zero_quota_is_full_so_nothing_is_kept_beyond_it() {
    assert_eq!(level(0, 0, Thresholds::default()), Level::Stop);
}

#[test]
fn the_levels_are_ordered_from_normal_to_stop() {
    assert!(Level::Normal < Level::Warning);
    assert!(Level::Shed < Level::Stop);
}

#[test]
fn class_c_is_shed_from_the_shed_level_and_kept_below_it() {
    assert_eq!(decide(EventClass::C, Level::Throttle), Decision::Keep);
    assert_eq!(decide(EventClass::C, Level::Shed), Decision::Shed);
    assert_eq!(decide(EventClass::C, Level::Stop), Decision::Shed);
}

#[test]
fn class_a_and_b_are_kept_until_stop_and_refused_at_stop() {
    for class in [EventClass::A, EventClass::B] {
        assert_eq!(decide(class, Level::Shed), Decision::Keep);
        assert_eq!(decide(class, Level::Stop), Decision::Refuse);
    }
}

#[test]
fn the_critical_class_is_never_shed_nor_refused_by_the_telemetry_policy() {
    for level in [Level::Normal, Level::Shed, Level::Stop] {
        assert_eq!(decide(EventClass::Critical, level), Decision::Keep);
    }
}

#[test]
fn the_proposed_default_quota_is_ten_thousand_events() {
    assert_eq!(DEFAULT_QUOTA, 10_000);
    assert_eq!(
        level(DEFAULT_QUOTA, DEFAULT_QUOTA, Thresholds::default()),
        Level::Stop
    );
}
