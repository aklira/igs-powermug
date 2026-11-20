//! Loss and restoration of a South session in a lane (SPECIFICATIONS §7.6,
//! PM-TEL-007, PM-CYC-004).

use std::time::Duration;

use powermug_core::cycle::{CycleState, Supervision};
use powermug_core::lane::{Lane, LaneError, PointSpec, PointTable, SessionId};
use powermug_core::Times;
use powermug_pivot::proto::v1 as pb;
use powermug_pivot::{EventId, PointEvent, PointIndex, Quality, Validity, Value};
use powermug_store::{EventClass, TelemetryLog};
use prost::Message;

const SEC: u64 = 1_000_000_000;

fn point(point_id: &str, class: EventClass, session: Option<u32>, supervised: bool) -> PointSpec {
    PointSpec {
        site_id: "S3".into(),
        point_id: point_id.into(),
        class,
        session: session.map(SessionId),
        supervision: supervised.then_some(Supervision {
            period: Duration::from_secs(1),
            grace: Duration::ZERO,
        }),
    }
}

/// Point 0: class A, session 1, supervised. Point 1: class B, session 1.
/// Point 2: class A, session 2. Point 3: class C, session 1.
fn lane(dir: &tempfile::TempDir) -> Lane {
    let table = PointTable::new(
        7,
        vec![
            point("S3/TM_001", EventClass::A, Some(1), true),
            point("S3/TM_002", EventClass::B, Some(1), false),
            point("S3/TM_003", EventClass::A, Some(2), false),
            point("S3/CLASS_C", EventClass::C, Some(1), false),
        ],
    )
    .expect("distinct");
    let log = TelemetryLog::open(dir.path().join("telemetry.db")).expect("opens");
    Lane::new(table, log, 0)
}

/// A `UUIDv7` whose last byte is `n`.
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

/// Every row of the log, decoded as an envelope when it has a point.
fn rows(lane: &Lane) -> Vec<(Option<u32>, Vec<u8>)> {
    lane.log()
        .read_from(1, 100)
        .expect("reads")
        .into_iter()
        .map(|row| (row.point, row.payload))
        .collect()
}

#[test]
fn loss_journals_one_state_event_and_one_quality_event_per_valued_point() {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut lane = lane(&dir);
    lane.ingest(
        &[
            event(0, 1, Value::Int(5)),
            event(1, 2, Value::Int(6)),
            event(2, 3, Value::Int(7)),
            event(3, 4, Value::Bool(true)),
        ],
        at(0),
    )
    .expect("values");
    let before = lane.log().last_seq().expect("last");
    assert_eq!(before, Some(3), "class C is not journaled");

    lane.session_lost(SessionId(1), "keepalive timeout", at(10))
        .expect("loss");

    let all = rows(&lane);
    // One state row, then one quality row for point 0 and one for point 1.
    // Point 3 has a value but is class C: it stays out of the journal.
    assert_eq!(all.len(), 6);
    assert_eq!(all[3].0, None, "the state event concerns no point");
    let state = pb::CommunicationState::decode(all[3].1.as_slice()).expect("decodes");
    assert_eq!(state.session_id, "1");
    assert!(!state.connected);
    assert_eq!(state.reason, "keepalive timeout");
    assert_eq!(all[4].0, Some(0));
    assert_eq!(all[5].0, Some(1));
    let quality = pb::Envelope::decode(all[4].1.as_slice()).expect("decodes");
    assert_eq!(
        quality.quality.as_ref().map(|q| q.validity),
        Some(pb::Validity::Invalid as i32)
    );
    assert_eq!(
        quality.cause.as_ref().map(|c| c.normalized.as_str()),
        Some("communication_loss")
    );

    let degraded = lane.image().get(PointIndex(0)).expect("value");
    assert_eq!(degraded.value, Value::Int(5));
    assert_eq!(degraded.quality, Quality::new(Validity::Invalid));
    assert_eq!(
        lane.image().get(PointIndex(3)).map(|s| s.quality),
        Some(Quality::new(Validity::Invalid)),
        "class C points still change in the image"
    );
    // Session 2 is untouched.
    assert_eq!(
        lane.image().get(PointIndex(2)).map(|s| s.quality),
        Some(Quality::GOOD)
    );
}

#[test]
fn a_session_already_lost_is_not_journaled_again() {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut lane = lane(&dir);
    lane.ingest(&[event(0, 1, Value::Int(5))], at(0))
        .expect("value");
    lane.session_lost(SessionId(1), "timeout", at(10))
        .expect("loss");
    let after_loss = lane.log().last_seq().expect("last");

    let again = lane
        .session_lost(SessionId(1), "timeout", at(20))
        .expect("second loss");

    assert_eq!(again, None);
    assert_eq!(lane.log().last_seq().expect("last"), after_loss);
}

#[test]
fn a_lost_session_writes_no_stale_event_for_its_points() {
    // PM-CYC-004: the point is suspended, so its period (1 s) does not expire.
    let dir = tempfile::tempdir().expect("temp dir");
    let mut lane = lane(&dir);
    lane.ingest(&[event(0, 1, Value::Int(5))], at(0))
        .expect("value");
    lane.session_lost(SessionId(1), "timeout", at(5))
        .expect("loss");

    lane.ingest(&[event(2, 2, Value::Int(1))], at(50))
        .expect("unrelated batch");

    assert_eq!(
        lane.supervisor_state(PointIndex(0)),
        Some(CycleState::Suspended)
    );
    for (_, payload) in rows(&lane) {
        if let Ok(envelope) = pb::Envelope::decode(payload.as_slice()) {
            assert!(envelope.provenance.is_empty(), "no stale event expected");
        }
    }
}

#[test]
fn restored_session_keeps_its_points_degraded_until_a_fresh_value() {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut lane = lane(&dir);
    lane.ingest(&[event(0, 1, Value::Int(5))], at(0))
        .expect("value");
    lane.session_lost(SessionId(1), "timeout", at(5))
        .expect("loss");

    lane.session_restored(SessionId(1), "link up", at(60))
        .expect("restored");

    let last = rows(&lane).pop().expect("a row");
    assert_eq!(last.0, None);
    let state = pb::CommunicationState::decode(last.1.as_slice()).expect("decodes");
    assert!(state.connected);
    assert_eq!(
        lane.image().get(PointIndex(0)).map(|s| s.quality),
        Some(Quality::new(Validity::Invalid)),
        "still degraded after reconnection"
    );

    lane.ingest(&[event(0, 2, Value::Int(9))], at(70))
        .expect("fresh value");

    assert_eq!(
        lane.image()
            .get(PointIndex(0))
            .map(|s| (s.value, s.quality)),
        Some((Value::Int(9), Quality::GOOD))
    );
    assert_eq!(
        lane.supervisor_state(PointIndex(0)),
        Some(CycleState::Fresh)
    );
}

#[test]
fn restoring_a_session_that_is_not_lost_does_nothing() {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut lane = lane(&dir);
    let committed = lane
        .session_restored(SessionId(2), "link up", at(0))
        .expect("no-op");
    assert_eq!(committed, None);
    assert_eq!(lane.log().last_seq().expect("last"), None);
}

#[test]
fn a_session_without_points_is_refused() {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut lane = lane(&dir);
    let err = lane
        .session_lost(SessionId(9), "timeout", at(0))
        .expect_err("refused");
    assert!(matches!(err, LaneError::UnknownSession(9)));
    assert!(!lane.is_lost(SessionId(9)));
}
