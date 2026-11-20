//! A lane on a real telemetry log: the journal and the image move together.

use powermug_core::lane::{Lane, LaneError, PointSpec, PointTable};
use powermug_core::Times;
use powermug_pivot::proto::v1 as pb;
use powermug_pivot::{EventId, PointEvent, PointIndex, Quality, Value};
use powermug_store::{EventClass, StoreError, TelemetryLog};
use prost::Message;

fn spec(point_id: &str, class: EventClass) -> PointSpec {
    PointSpec {
        site_id: "S3".into(),
        point_id: point_id.into(),
        class,
        session: None,
        supervision: None,
    }
}

fn table() -> PointTable {
    PointTable::new(
        4,
        vec![
            spec("S3/TM_001", EventClass::A),
            spec("S3/TM_002", EventClass::B),
            spec("S3/CLASS_C", EventClass::C),
        ],
    )
    .expect("distinct point ids")
}

/// A `UUIDv7` whose last byte is `n`, so that each event id is distinct.
fn event_id(n: u8) -> EventId {
    let mut bytes = [0u8; 16];
    bytes[6] = 0x70;
    bytes[8] = 0x80;
    bytes[15] = n;
    EventId::from_bytes(bytes).expect("v7")
}

fn event(point: u32, id: u8, value: Value) -> PointEvent {
    PointEvent {
        point: PointIndex(point),
        value,
        quality: Quality::GOOD,
        source_time_ns: 10,
        receive_time_ns: 20,
        process_time_ns: 30,
        event_id: event_id(id),
        revision_index: 4,
    }
}

fn at() -> Times {
    Times {
        monotonic_ns: 0,
        wall_ns: 0,
    }
}

fn lane(dir: &tempfile::TempDir) -> Lane {
    let log = TelemetryLog::open(dir.path().join("telemetry.db")).expect("opens");
    Lane::new(table(), log, 0)
}

#[test]
fn journaled_events_reach_the_log_and_the_image() {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut lane = lane(&dir);

    let committed = lane
        .ingest(
            &[event(0, 1, Value::Int(5)), event(1, 2, Value::Double(2.5))],
            at(),
        )
        .expect("ingests");

    let committed = committed.expect("two journaled events");
    assert_eq!((committed.first_seq, committed.last_seq), (1, 2));

    let stored = lane.log().read_from(1, 10).expect("reads");
    assert_eq!(stored.len(), 2);
    let envelope = pb::Envelope::decode(stored[0].payload.as_slice()).expect("decodes");
    assert_eq!(envelope.point_id, "S3/TM_001");
    assert_eq!(envelope.mapping_revision, "4");
    assert_eq!(envelope.value, Some(Value::Int(5).into()));

    assert_eq!(
        lane.image().get(PointIndex(0)).map(|s| s.value),
        Some(Value::Int(5))
    );
    assert_eq!(
        lane.image().get(PointIndex(1)).map(|s| s.value),
        Some(Value::Double(2.5))
    );
}

#[test]
fn class_c_updates_the_image_without_a_journal_row() {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut lane = lane(&dir);

    let committed = lane
        .ingest(&[event(2, 1, Value::Bool(true))], at())
        .expect("ingests");

    assert_eq!(committed, None);
    assert_eq!(lane.log().last_seq().expect("last"), None);
    assert_eq!(
        lane.image().get(PointIndex(2)).map(|s| s.value),
        Some(Value::Bool(true))
    );
}

#[test]
fn unknown_point_refuses_the_whole_batch() {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut lane = lane(&dir);

    let err = lane
        .ingest(
            &[event(0, 1, Value::Int(1)), event(9, 2, Value::Int(2))],
            at(),
        )
        .expect_err("refused");

    assert!(matches!(err, LaneError::UnknownPoint(9)));
    assert_eq!(lane.log().last_seq().expect("last"), None);
    assert!(lane.image().get(PointIndex(0)).is_none());
}

#[test]
fn replayed_event_leaves_the_image_unchanged() {
    // PM-TEL-001: the image never gets ahead of the journal. A batch the
    // journal refuses must not reach the image.
    let dir = tempfile::tempdir().expect("temp dir");
    let mut lane = lane(&dir);
    lane.ingest(&[event(0, 1, Value::Int(1))], at())
        .expect("first");

    let err = lane
        .ingest(&[event(0, 1, Value::Int(99))], at())
        .expect_err("replay refused");

    assert!(matches!(err, LaneError::Store(StoreError::DuplicateEvent)));
    assert_eq!(
        lane.image().get(PointIndex(0)).map(|s| s.value),
        Some(Value::Int(1))
    );
}

#[test]
fn later_value_replaces_the_slot() {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut lane = lane(&dir);
    lane.ingest(&[event(0, 1, Value::Int(1))], at())
        .expect("first");
    lane.ingest(&[event(0, 2, Value::Int(2))], at())
        .expect("second");

    let slot = lane.image().get(PointIndex(0)).expect("has a value");
    assert_eq!(slot.value, Value::Int(2));
    assert_eq!(slot.revision_index, 4);
    assert_eq!(slot.source_time_ns, 10);
    assert_eq!(slot.receive_time_ns, 20);
}
