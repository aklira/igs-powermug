//! Rebuilding the image from the journal after a restart (PM-TEL-003, FUN-IMG-004).

use std::time::Duration;

use powermug_core::cycle::{CycleState, Supervision};
use powermug_core::lane::{Lane, LaneError, PointSpec, PointTable, SessionId};
use powermug_core::Times;
use powermug_pivot::proto::v1 as pb;
use powermug_pivot::{EventId, PointEvent, PointIndex, Quality, Validity, Value};
use powermug_store::{EventClass, NewEvent, TelemetryLog};
use prost::Message;

const SEC: u64 = 1_000_000_000;

fn table() -> PointTable {
    PointTable::new(
        7,
        vec![
            PointSpec {
                site_id: "S3".into(),
                point_id: "S3/TM_001".into(),
                class: EventClass::A,
                session: Some(SessionId(1)),
                supervision: Some(Supervision {
                    period: Duration::from_secs(1),
                    grace: Duration::ZERO,
                }),
            },
            PointSpec {
                site_id: "S3".into(),
                point_id: "S3/TM_002".into(),
                class: EventClass::B,
                session: Some(SessionId(1)),
                supervision: None,
            },
            PointSpec {
                site_id: "S3".into(),
                point_id: "S3/CLASS_C".into(),
                class: EventClass::C,
                session: Some(SessionId(1)),
                supervision: Some(Supervision {
                    period: Duration::from_secs(1),
                    grace: Duration::ZERO,
                }),
            },
        ],
    )
    .expect("distinct")
}

/// A `UUIDv7` whose last four bytes are `n`, so that ids are distinct.
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

fn at(seconds_tenths: u64) -> Times {
    let monotonic_ns = seconds_tenths * SEC / 10;
    Times {
        monotonic_ns,
        wall_ns: i64::try_from(monotonic_ns).expect("small test time"),
    }
}

fn open_log(path: &std::path::Path) -> TelemetryLog {
    TelemetryLog::open(path).expect("opens")
}

#[test]
fn restart_restores_the_last_values_as_degraded() {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("telemetry.db");
    {
        let mut lane = Lane::new(table(), open_log(&path), 0);
        lane.ingest(&[event(0, 1, 5), event(1, 2, 6), event(2, 3, 7)], at(0))
            .expect("first batch");
        // Point 0 goes stale at 1 s, then a new value arrives at 2.5 s.
        lane.ingest(&[event(1, 4, 8)], at(20))
            .expect("reveals the stale point");
        lane.ingest(&[event(0, 5, 9)], at(25)).expect("fresh value");
    }

    let lane = Lane::open(table(), open_log(&path), 0).expect("recovers");

    let p0 = lane.image().get(PointIndex(0)).expect("recovered");
    assert_eq!(p0.value, Value::Int(9));
    assert_eq!(p0.quality, Quality::new(Validity::Invalid));
    assert_eq!(p0.revision_index, 7);
    assert_eq!(
        lane.image().get(PointIndex(1)).map(|s| s.value),
        Some(Value::Int(8))
    );
    assert!(
        lane.image().get(PointIndex(2)).is_none(),
        "class C is not journaled, so it is not recovered"
    );
    assert_eq!(
        lane.supervisor_state(PointIndex(0)),
        Some(CycleState::Suspended)
    );
}

#[test]
fn a_fresh_value_after_restart_makes_the_point_valid_again() {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("telemetry.db");
    {
        let mut lane = Lane::new(table(), open_log(&path), 0);
        lane.ingest(&[event(0, 1, 5)], at(0)).expect("value");
    }

    let mut lane = Lane::open(table(), open_log(&path), 0).expect("recovers");
    lane.ingest(&[event(0, 2, 10)], at(100)).expect("fresh");

    let slot = lane.image().get(PointIndex(0)).expect("value");
    assert_eq!((slot.value, slot.quality), (Value::Int(10), Quality::GOOD));
    assert_eq!(
        lane.supervisor_state(PointIndex(0)),
        Some(CycleState::Fresh)
    );
}

#[test]
fn a_recovered_point_writes_no_stale_event_until_it_is_fresh() {
    // PM-TEL-003 with PM-CYC-004: nothing old is reported stale after a restart.
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("telemetry.db");
    {
        let mut lane = Lane::new(table(), open_log(&path), 0);
        lane.ingest(&[event(0, 1, 5)], at(0)).expect("value");
    }

    let mut lane = Lane::open(table(), open_log(&path), 0).expect("recovers");
    let rows_before = lane.log().last_seq().expect("last");
    lane.ingest(&[event(1, 2, 1)], at(600))
        .expect("unrelated batch, a minute later");

    assert_eq!(
        lane.log().last_seq().expect("last"),
        rows_before.map(|s| s + 1)
    );
    assert_eq!(
        lane.supervisor_state(PointIndex(0)),
        Some(CycleState::Suspended)
    );
}

#[test]
fn a_journal_record_that_does_not_replay_stops_the_recovery() {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("telemetry.db");
    {
        let mut log = open_log(&path);
        let envelope = pb::Envelope {
            event_id: event_id(1).as_bytes().to_vec(),
            site_id: "S9".into(),
            point_id: "S9/UNKNOWN".into(),
            value: Some(Value::Int(1).into()),
            quality: Some(Quality::GOOD.into()),
            mapping_revision: "7".into(),
            ..Default::default()
        };
        let payload = envelope.encode_to_vec();
        log.append(&[NewEvent {
            partition: "S9",
            point: Some(0),
            class: EventClass::A,
            event_id: *event_id(1).as_bytes(),
            revision: 7,
            ts_receive_ns: 0,
            payload: &payload,
        }])
        .expect("appends");
    }

    let err = Lane::open(table(), open_log(&path), 0).expect_err("refused");
    assert!(matches!(err, LaneError::Replay { .. }), "{err:?}");
}

#[test]
fn recovery_reads_more_than_one_page_of_the_journal() {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("telemetry.db");
    {
        let mut lane = Lane::new(table(), open_log(&path), 0);
        let events: Vec<PointEvent> = (1..=2_500u32)
            .map(|id| event(1, id, i64::from(id)))
            .collect();
        lane.ingest(&events, at(0)).expect("batch of 2500");
    }

    let lane = Lane::open(table(), open_log(&path), 0).expect("recovers");
    assert_eq!(
        lane.image().get(PointIndex(1)).map(|s| s.value),
        Some(Value::Int(2_500))
    );
}
