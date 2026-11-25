//! The saturation policy of the telemetry (SPECIFICATIONS 7.5, PM-TEL-006, PM8): the journal usage
//! of a class is compared to its quota, and the usage level decides what the class may keep. The
//! four levels are `warning`, `throttle`, `shed` and `stop`; the thresholds are percentages of the
//! quota. The quotas and the thresholds are parameters of the cell (owner's decision D4); the
//! defaults here are the values proposed for the first campaigns, to confirm on the bench.

use powermug_store::EventClass;

/// The journal quota of each telemetry class, in events (proposed: 10,000; to confirm, D4).
pub const DEFAULT_QUOTA: u64 = 10_000;

/// The percentages of the quota at which each level starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Thresholds {
    pub warning: u8,
    pub throttle: u8,
    pub shed: u8,
    pub stop: u8,
}

impl Default for Thresholds {
    fn default() -> Self {
        Self {
            warning: 70,
            throttle: 80,
            shed: 90,
            stop: 100,
        }
    }
}

/// How full the journal of a class is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Level {
    Normal,
    Warning,
    Throttle,
    Shed,
    Stop,
}

/// The level of a class holding `used` events of its `quota`. A zero quota is full: nothing may be
/// kept beyond it.
#[must_use]
pub fn level(used: u64, quota: u64, thresholds: Thresholds) -> Level {
    let reached = |percent: u8| u128::from(used) * 100 >= u128::from(quota) * u128::from(percent);
    if quota == 0 || reached(thresholds.stop) {
        Level::Stop
    } else if reached(thresholds.shed) {
        Level::Shed
    } else if reached(thresholds.throttle) {
        Level::Throttle
    } else if reached(thresholds.warning) {
        Level::Warning
    } else {
        Level::Normal
    }
}

/// What a batch of a class does at a level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    /// The events are kept (journaled or published, as the class prescribes).
    Keep,
    /// The events are dropped and counted: a sacrificable class, shed under saturation.
    Shed,
    /// The events are refused and counted: the acquisition stops for the class.
    Refuse,
}

/// The decision for a class at a level. Class C is sacrificable: it is shed from `shed` on. The
/// critical class is never shed nor refused by the telemetry policy (it has its own store). The
/// other classes keep their events up to `shed` and refuse them at `stop`.
#[must_use]
pub fn decide(class: EventClass, level: Level) -> Decision {
    match class {
        EventClass::C if level >= Level::Shed => Decision::Shed,
        EventClass::A | EventClass::B if level == Level::Stop => Decision::Refuse,
        _ => Decision::Keep,
    }
}
