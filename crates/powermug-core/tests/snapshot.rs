//! Snapshots of the image and recovery from them (SPECIFICATIONS §7.2).

use powermug_core::lane::{Lane, PointSpec, PointTable, SessionId, TableError};
use powermug_core::Times;
use powermug_pivot::proto::v1 as pb;
use powermug_pivot::{EventId, PointEvent, PointIndex, Quality, Value};
use powermug_store::{EventClass, NewEvent, TelemetryLog};
use prost::Message;

const SEC: u64 = 1_000_000_000;

fn spec(point_id: &str, class: EventClass) -> PointSpec {
    PointSpec {
        site_id: "S3".into(),
        point_id: point_id.into(),
        class,
        session: Some(SessionId(1)),
        supervision: None,
    }
}

fn table() -> PointTable {
    PointTable::new(
        7,
        vec![
            spec("S3/TM_001", EventClass::A),
            spec("S3/TM_002", EventClass::B),
        ],
    )
    .expect("distinct")
}

fn event_id(n: u32) -> EventId {
    let mut bytes = [0u8; 16];
    bytes[6] = 0x70;
    bytes[8] = 0x80;
    bytes[12..16].copy_from_slice(&n.to_be_bytes());
    EventId::from_bytes(bytes).expect("v7")
}

fn event(point: u32, id: u32, value: i64) -> PointEvent {
    PointEvent {
        point: PointIndex(point),
        value: Value::Int(value),
        quality: Quality::GOOD,
        source_time_ns: 10,
        receive_time_ns: 20,
        process_time_ns: 30,
        event_id: event_id(id),
        revision_index: 7,
    }
}

fn at(monotonic_ns: u64) -> Times {
    Times {
        monotonic_ns,
        wall_ns: i64::try_from(monotonic_ns).expect("small test time"),
    }
}

fn image_of(lane: &Lane) -> Vec<(PointIndex, powermug_core::Slot)> {
    lane.image()
        .iter()
        .map(|(index, slot)| (index, *slot))
        .collect()
}

#[test]
fn recovery_from_a_snapshot_does_not_read_the_rows_before_it() {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("telemetry.db");
    let before_image;
    {
        let mut lane = Lane::new(table(), TelemetryLog::open(&path).expect("opens"), 0);
        lane.ingest(&[event(0, 1, 5), event(1, 2, 6)], at(0))
            .expect("first");

        // A row that does not replay, written before the snapshot: it must not
        // be read again once the snapshot covers it.
        let payload = pb::Envelope {
            event_id: event_id(3).as_bytes().to_vec(),
            site_id: "S9".into(),
            point_id: "S9/UNKNOWN".into(),
            value: Some(Value::Int(1).into()),
            quality: Some(Quality::GOOD.into()),
            mapping_revision: "7".into(),
            ..Default::default()
        }
        .encode_to_vec();
        let mut raw = TelemetryLog::open(&path).expect("opens");
        powermug_store::Journal::append(
            &mut raw,
            &[NewEvent {
                partition: "S9",
                point: Some(0),
                class: EventClass::A,
                event_id: *event_id(3).as_bytes(),
                revision: 7,
                ts_receive_ns: 0,
                payload: &payload,
            }],
        )
        .expect("appends the bad row");
        drop(raw);

        let taken = lane.snapshot().expect("snapshot");
        assert_eq!(taken, Some(3), "the snapshot covers the bad row");
        lane.ingest(&[event(0, 4, 9)], at(SEC))
            .expect("after the snapshot");
        before_image = image_of(&lane);
    }

    let lane = Lane::open(table(), TelemetryLog::open(&path).expect("opens"), 0)
        .expect("recovers from the snapshot");

    let values: Vec<_> = lane.image().iter().map(|(i, s)| (i, s.value)).collect();
    assert_eq!(
        values,
        vec![
            (PointIndex(0), Value::Int(9)),
            (PointIndex(1), Value::Int(6))
        ]
    );
    assert_eq!(
        before_image
            .iter()
            .map(|(i, s)| (*i, s.value))
            .collect::<Vec<_>>(),
        values
    );
}

#[test]
fn snapshot_is_taken_once_per_interval() {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut lane = Lane::new(
        table(),
        TelemetryLog::open(dir.path().join("t.db")).expect("opens"),
        0,
    );
    lane.ingest(&[event(0, 1, 5)], at(0)).expect("value");

    assert!(!lane.snapshot_if_due(at(59 * SEC)).expect("not yet"));
    assert!(lane.snapshot_if_due(at(60 * SEC)).expect("due"));
    assert!(!lane.snapshot_if_due(at(90 * SEC)).expect("again too soon"));
    assert!(lane.snapshot_if_due(at(120 * SEC)).expect("due again"));
}

#[test]
fn an_empty_journal_has_nothing_to_snapshot() {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut lane = Lane::new(
        table(),
        TelemetryLog::open(dir.path().join("t.db")).expect("opens"),
        0,
    );
    assert_eq!(lane.snapshot().expect("snapshot"), None);
}

#[test]
fn a_lane_holds_one_site() {
    let err = PointTable::new(
        7,
        vec![
            spec("S3/TM_001", EventClass::A),
            PointSpec {
                site_id: "S4".into(),
                point_id: "S4/TM_001".into(),
                class: EventClass::A,
                session: None,
                supervision: None,
            },
        ],
    )
    .expect_err("mixed sites refused");
    assert!(matches!(err, TableError::MixedSites { .. }), "{err:?}");
}
