//! Neutral indicators computed locally (SPECIFICATIONS §11.3, `indicators.v1`).
//!
//! This first slice reports the two indicators the cell can measure today:
//! `telemetry_last_seen_seconds` (scope `target`) and `iec104_connection_up` (scope
//! `site`, one per South session). The others of the catalogue need sources that do
//! not exist yet (cell health, host statistics, destinations, commands), and they are
//! not reported rather than estimated.

use std::sync::{Mutex, PoisonError};

use crate::lane::Lane;

/// One measured indicator, with its scope, unit and measurement time.
#[derive(Debug, Clone, PartialEq)]
pub struct Indicator {
    pub name: &'static str,
    pub scope: &'static str,
    /// The South session, for an indicator of scope `site`.
    pub session: Option<u32>,
    /// `None` when the value cannot be measured yet (no event accepted so far).
    pub value: Option<f64>,
    pub unit: &'static str,
    /// Wall-clock nanoseconds since the Unix epoch.
    pub measured_at_ns: i64,
}

/// Something that can sample the indicators at a given time.
pub trait IndicatorSource: Send + Sync {
    /// The indicators at `now_wall_ns`.
    fn sample(&self, now_wall_ns: i64) -> Vec<Indicator>;
}

impl IndicatorSource for Mutex<Lane> {
    fn sample(&self, now_wall_ns: i64) -> Vec<Indicator> {
        let lane = self.lock().unwrap_or_else(PoisonError::into_inner);
        let mut indicators = vec![Indicator {
            name: "telemetry_last_seen_seconds",
            scope: "target",
            session: None,
            value: lane
                .last_accepted_wall_ns()
                .map(|accepted| seconds_between(accepted, now_wall_ns)),
            unit: "seconds",
            measured_at_ns: now_wall_ns,
        }];
        for (session, up) in lane.session_states() {
            indicators.push(Indicator {
                name: "iec104_connection_up",
                scope: "site",
                session: Some(session.0),
                value: Some(if up { 1.0 } else { 0.0 }),
                unit: "boolean",
                measured_at_ns: now_wall_ns,
            });
        }
        indicators
    }
}

/// Seconds from `earlier` to `later`, never negative.
#[allow(
    clippy::cast_precision_loss,
    reason = "a duration for display, to sub-second precision"
)]
fn seconds_between(earlier: i64, later: i64) -> f64 {
    (later - earlier).max(0) as f64 / 1_000_000_000.0
}
