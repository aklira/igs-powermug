//! The saturation policy at the lane (SPECIFICATIONS 7.5, PM8): a batch of a class at `stop` is
//! refused with nothing written, class C is shed at `shed` and never published, and every loss is
//! counted by class.

use powermug_core::lane::{Lane, LaneError, PointSpec, PointTable, SessionId};
use powermug_core::Times;
use powermug_pivot::{EventId, PointEvent, PointIndex, Quality, Value};
use powermug_store::{EventClass, TelemetryLog};

const SEC: u64 = 1_000_000_000;
/// The wall-clock time of `at(0)`: 1,000 s after the epoch.
const WALL_EPOCH_NS: i64 = 1_000_000_000_000;

fn table() -> PointTable {
    PointTable::new(
        7,
        vec![
            PointSpec {
                site_id: "S3".into(),
                point_id: "S3/TM_001".into(),
                class: EventClass::A,
                session: Some(SessionId(1)),
                supervision: None,
            },
            PointSpec {
                site_id: "S3".into(),
                point_id: "S3/TD_002".into(),
                class: EventClass::C,
                session: Some(SessionId(1)),
                supervision: None,
            },
        ],
    )
    .expect("distinct")
}

fn event(point: u32, id: u32) -> PointEvent {
    let mut bytes = [0u8; 16];
    bytes[6] = 0x70;
    bytes[8] = 0x80;
    bytes[12..16].copy_from_slice(&id.to_be_bytes());
    PointEvent {
        point: PointIndex(point),
        value: Value::Int(i64::from(id)),
        quality: Quality::GOOD,
        source_time_ns: 10,
        receive_time_ns: 20,
        process_time_ns: 30,
        event_id: EventId::from_bytes(bytes).expect("v7"),
        revision_index: 7,
    }
}

fn at(seconds: u64) -> Times {
    Times {
        monotonic_ns: seconds * SEC,
        wall_ns: i64::try_from(1_000 * SEC + seconds * SEC).expect("in range"),
    }
}

fn open(dir: &std::path::Path) -> (Lane, TelemetryLog) {
    let path = dir.join("telemetry.db");
    let lane = Lane::new(table(), TelemetryLog::open(&path).expect("opens"), 0);
    (lane, TelemetryLog::open(&path).expect("opens"))
}

#[test]
fn a_batch_of_a_class_at_stop_is_refused_with_nothing_written() {
    let dir = tempfile::tempdir().expect("temp dir");
    let (mut lane, reader) = open(dir.path());
    lane.set_quota(EventClass::A, 3);
    for id in 1..=3 {
        lane.ingest(&[event(0, id)], at(u64::from(id)))
            .expect("admitted below stop");
    }

    let refused = lane.ingest(&[event(0, 4)], at(4));
    assert!(matches!(refused, Err(LaneError::Saturated(EventClass::A))));
    assert_eq!(
        reader.usage(EventClass::A).expect("reads"),
        3,
        "nothing was written"
    );
    assert_eq!(lane.losses(EventClass::A), (0, 1));
}

#[test]
fn class_c_is_shed_at_shed_and_never_published() {
    let dir = tempfile::tempdir().expect("temp dir");
    let (mut lane, _reader) = open(dir.path());
    lane.set_quota(EventClass::A, 10);
    for id in 1..=9 {
        lane.ingest(&[event(0, id)], at(u64::from(id)))
            .expect("admitted");
    }
    // 9 of 10: the telemetry is at shed, so class C is dropped and counted.
    lane.ingest(&[event(1, 100)], at(10))
        .expect("admitted: only class C is shed");

    assert_eq!(lane.losses(EventClass::C), (1, 0));
    let published = lane.take_published();
    assert!(
        published
            .iter()
            .all(|envelope| envelope.point_id != "S3/TD_002"),
        "no class C envelope is published while shed"
    );
}

#[test]
fn nothing_is_lost_below_the_thresholds() {
    let dir = tempfile::tempdir().expect("temp dir");
    let (mut lane, _reader) = open(dir.path());
    for id in 1..=5 {
        lane.ingest(&[event(0, id), event(1, 100 + id)], at(u64::from(id)))
            .expect("admitted");
    }
    assert_eq!(lane.losses(EventClass::A), (0, 0));
    assert_eq!(lane.losses(EventClass::C), (0, 0));
}

#[test]
fn a_forced_shedding_window_sheds_class_c_until_it_ends() {
    let dir = tempfile::tempdir().expect("temp dir");
    let (mut lane, _reader) = open(dir.path());
    // The order runs for five seconds from the epoch of the test's clock (wall time 1,000 s).
    lane.force_shed(WALL_EPOCH_NS + 5_000_000_000);

    lane.ingest(&[event(1, 100)], at(1)).expect("admitted");
    assert_eq!(
        lane.losses(EventClass::C),
        (1, 0),
        "shed while the window lasts"
    );
    assert!(lane
        .take_published()
        .iter()
        .all(|e| e.point_id != "S3/TD_002"));

    lane.ingest(&[event(1, 200)], at(6)).expect("admitted");
    assert_eq!(
        lane.losses(EventClass::C),
        (1, 0),
        "kept once the window has ended"
    );
    assert!(lane
        .take_published()
        .iter()
        .any(|e| e.point_id == "S3/TD_002"));
}
