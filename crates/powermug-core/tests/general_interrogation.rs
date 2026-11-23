//! A general interrogation after a restart and a reconnection (ACC-014, FUN-IMG-004,
//! PM-TEL-003): every point of the session is answered, and a point not yet resynchronised
//! is answered as invalid, until a fresh value arrives.

use powermug_core::lane::{Lane, PointSpec, PointTable, SessionId};
use powermug_core::Times;
use powermug_pivot::proto::v1 as pb;
use powermug_pivot::{EventId, PointEvent, PointIndex, Quality, Value};
use powermug_store::{EventClass, TelemetryLog};

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
                supervision: None,
            },
            PointSpec {
                site_id: "S3".into(),
                point_id: "S3/TM_002".into(),
                class: EventClass::B,
                session: Some(SessionId(1)),
                supervision: None,
            },
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

fn at(tenths: u64) -> Times {
    let monotonic_ns = tenths * SEC / 10;
    Times {
        monotonic_ns,
        wall_ns: i64::try_from(monotonic_ns).expect("small test time"),
    }
}

fn open_log(path: &std::path::Path) -> TelemetryLog {
    TelemetryLog::open(path).expect("opens")
}

/// The validity answered for `point` by a general interrogation of session 1.
fn answered_validity(lane: &Lane, point: &str) -> i32 {
    let envelope = lane
        .session_state(SessionId(1))
        .into_iter()
        .find(|envelope: &pb::Envelope| envelope.point_id == point)
        .unwrap_or_else(|| panic!("{point} is not answered"));
    envelope.quality.expect("quality").validity
}

#[test]
fn an_interrogation_after_a_restart_answers_every_point_as_not_yet_resynchronised() {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("telemetry.db");
    {
        let mut lane = Lane::new(table(), open_log(&path), 0);
        lane.ingest(&[event(0, 1, 5), event(1, 2, 6)], at(0))
            .expect("values");
        lane.snapshot().expect("snapshot");
    }

    // Restart: the image is restored from the snapshot, and every point is degraded.
    let mut lane = Lane::open(table(), open_log(&path), 0).expect("recovers");
    let invalid = pb::Validity::Invalid as i32;
    assert_eq!(answered_validity(&lane, "S3/TM_001"), invalid);
    assert_eq!(answered_validity(&lane, "S3/TM_002"), invalid);

    // The station is reconnected and sends a fresh value for one point only.
    lane.ingest(&[event(0, 3, 9)], at(10)).expect("fresh value");
    assert_eq!(
        answered_validity(&lane, "S3/TM_001"),
        pb::Validity::Good as i32
    );
    assert_eq!(answered_validity(&lane, "S3/TM_002"), invalid);

    // The answer carries the values of the image, for every point of the session.
    let answer = lane.session_state(SessionId(1));
    assert_eq!(answer.len(), 2);
    assert!(answer.iter().all(|envelope| envelope.value.is_some()));
}
