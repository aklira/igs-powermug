//! Neutral indicators computed locally (SPECIFICATIONS §11.3, `indicators.v1`).
//!
//! This first slice reports the two indicators the cell can measure today:
//! `telemetry_last_seen_seconds` (scope `target`) and `iec104_connection_up` (scope
//! `site`, one per South session). The others of the catalogue need sources that do
//! not exist yet (cell health, host statistics, destinations, commands), and they are
//! not reported rather than estimated.

use std::fmt::Write as _;
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
        for (session, state) in lane.session_states() {
            // Unknown until an adapter has reported the session: not up, not down.
            indicators.push(Indicator {
                name: "iec104_connection_up",
                scope: "site",
                session: Some(session.0),
                value: state.map(|up| if up { 1.0 } else { 0.0 }),
                unit: "boolean",
                measured_at_ns: now_wall_ns,
            });
            indicators.push(Indicator {
                name: "iec104_connection_event_count",
                scope: "site",
                session: Some(session.0),
                value: state.map(|_| f64::from(lane.connection_events(session))),
                unit: "count",
                measured_at_ns: now_wall_ns,
            });
        }
        indicators
    }
}

/// The indicators as `OpenMetrics` text (gauges, one sample per indicator and session). An
/// indicator not measured yet is left out rather than estimated. The series depend on the
/// indicators and the sessions, never on the number of points (PM-IND-001).
#[must_use]
pub fn openmetrics(indicators: &[Indicator]) -> String {
    let mut text = String::new();
    let mut declared: Vec<&str> = Vec::new();
    for indicator in indicators {
        let Some(value) = indicator.value else {
            continue;
        };
        if !declared.contains(&indicator.name) {
            declared.push(indicator.name);
            let _ = writeln!(text, "# TYPE {} gauge", indicator.name);
        }
        let labels = match indicator.session {
            Some(session) => format!("scope=\"{}\",session=\"{session}\"", indicator.scope),
            None => format!("scope=\"{}\"", indicator.scope),
        };
        let seconds = indicator.measured_at_ns / 1_000_000_000;
        let nanos = indicator.measured_at_ns % 1_000_000_000;
        let _ = writeln!(
            text,
            "{}{{{labels}}} {value} {seconds}.{nanos:09}",
            indicator.name
        );
    }
    text.push_str("# EOF\n");
    text
}

/// Seconds from `earlier` to `later`, never negative.
#[allow(
    clippy::cast_precision_loss,
    reason = "a duration for display, to sub-second precision"
)]
fn seconds_between(earlier: i64, later: i64) -> f64 {
    (later - earlier).max(0) as f64 / 1_000_000_000.0
}
