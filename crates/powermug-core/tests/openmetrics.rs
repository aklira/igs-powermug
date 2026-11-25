//! The indicators of the lane as `OpenMetrics` (SPECIFICATIONS 11.3, PM-IND-001): connection events
//! are counted per session, a value not measured yet is left out, and the series depend on the
//! sessions, never on the number of points.

use powermug_core::indicators::{openmetrics, IndicatorSource};
use powermug_core::lane::{Lane, PointSpec, PointTable, SessionId};
use powermug_core::Times;
use powermug_store::{EventClass, TelemetryLog};
use std::sync::Mutex;

const SEC: u64 = 1_000_000_000;
/// The instant the indicators are sampled at: 1,000 s after the epoch.
const NOW_NS: i64 = 1_000_000_000_000;

fn lane_with(points: usize, dir: &std::path::Path) -> Mutex<Lane> {
    let specs = (0..points)
        .map(|index| PointSpec {
            site_id: "S3".into(),
            point_id: format!("S3/TM_{index:05}"),
            class: EventClass::A,
            session: Some(SessionId(1)),
            supervision: None,
        })
        .collect();
    let table = PointTable::new(7, specs).expect("distinct");
    let log = TelemetryLog::open(dir.join("telemetry.db")).expect("opens");
    Mutex::new(Lane::new(table, log, 0))
}

fn at(seconds: u64) -> Times {
    Times {
        monotonic_ns: seconds * SEC,
        wall_ns: i64::try_from(1_000 * SEC + seconds * SEC).expect("in range"),
    }
}

fn connection_events(lane: &Mutex<Lane>) -> Option<f64> {
    lane.sample(NOW_NS)
        .into_iter()
        .find(|indicator| indicator.name == "iec104_connection_event_count")
        .and_then(|indicator| indicator.value)
}

#[test]
fn connection_changes_are_counted_per_session_from_the_first_report() {
    let dir = tempfile::tempdir().expect("temp dir");
    let lane = lane_with(2, dir.path());
    {
        let mut guard = lane.lock().expect("lock");
        guard
            .session_restored(SessionId(1), "connected", at(1))
            .expect("restored");
        guard
            .session_lost(SessionId(1), "link cut", at(2))
            .expect("lost");
        guard
            .session_restored(SessionId(1), "connected", at(3))
            .expect("restored");
    }
    // The first report is the session coming up, not a change: two changes since.
    assert_eq!(connection_events(&lane), Some(2.0));
}

#[test]
fn the_text_names_each_indicator_once_with_its_session_and_ends_with_eof() {
    let dir = tempfile::tempdir().expect("temp dir");
    let lane = lane_with(2, dir.path());
    {
        let mut guard = lane.lock().expect("lock");
        guard
            .session_restored(SessionId(1), "connected", at(1))
            .expect("restored");
        guard
            .session_lost(SessionId(1), "link cut", at(2))
            .expect("lost");
    }
    let text = openmetrics(&lane.sample(NOW_NS));
    assert_eq!(
        text.matches("# TYPE iec104_connection_event_count gauge")
            .count(),
        1
    );
    assert!(text.contains("iec104_connection_event_count{scope=\"site\",session=\"1\"} 1 "));
    assert!(text.contains("iec104_connection_up{scope=\"site\",session=\"1\"} 0 "));
    assert!(text.ends_with("# EOF\n"));
}

#[test]
fn a_value_not_measured_yet_is_left_out_rather_than_estimated() {
    let dir = tempfile::tempdir().expect("temp dir");
    let lane = lane_with(2, dir.path());
    // Nothing was reported: no telemetry seen, no session known.
    let text = openmetrics(&lane.sample(NOW_NS));
    assert_eq!(text, "# EOF\n");
}

#[test]
fn the_series_do_not_grow_with_the_number_of_points() {
    let dir_small = tempfile::tempdir().expect("temp dir");
    let dir_large = tempfile::tempdir().expect("temp dir");
    let small = lane_with(10, dir_small.path());
    let large = lane_with(10_000, dir_large.path());
    for lane in [&small, &large] {
        lane.lock()
            .expect("lock")
            .session_restored(SessionId(1), "connected", at(1))
            .expect("restored");
    }
    let series = |lane: &Mutex<Lane>| {
        openmetrics(&lane.sample(NOW_NS))
            .lines()
            .filter(|line| !line.starts_with('#'))
            .count()
    };
    assert_eq!(series(&small), series(&large));
}
