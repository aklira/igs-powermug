//! Cycle supervision inside a lane: stale events go through the same commit as
//! the batch that reveals them, and an unsaved expiry is undone.

use std::time::Duration;

use powermug_core::cycle::{CycleState, Supervision};
use powermug_core::lane::{Lane, LaneError, PointSpec, PointTable};
use powermug_core::Times;
use powermug_pivot::proto::v1 as pb;
use powermug_pivot::{Detail, EventId, PointEvent, PointIndex, Quality, Validity, Value};
use powermug_store::{EventClass, StoreError, TelemetryLog};
use prost::Message;

const SEC: u64 = 1_000_000_000;
const SEC_I: i64 = 1_000_000_000;

fn supervised(point_id: &str, class: EventClass) -> PointSpec {
    PointSpec {
        site_id: "S3".into(),
        point_id: point_id.into(),
        class,
        session: None,
        supervision: Some(Supervision {
            period: Duration::from_secs(1),
            grace: Duration::ZERO,
        }),
    }
}

fn lane(dir: &tempfile::TempDir) -> Lane {
    let table = PointTable::new(
        7,
        vec![
            supervised("S3/TM_001", EventClass::A),
            supervised("S3/TM_002", EventClass::A),
            supervised("S3/CLASS_C", EventClass::C),
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

fn event(point: u32, id: u8, value: i64, received_ns: i64) -> PointEvent {
    PointEvent {
        point: PointIndex(point),
        value: Value::Int(value),
        quality: Quality::GOOD,
        source_time_ns: received_ns,
        receive_time_ns: received_ns,
        process_time_ns: received_ns,
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

fn stored_envelopes(lane: &Lane) -> Vec<pb::Envelope> {
    lane.log()
        .read_from(1, 100)
        .expect("reads")
        .iter()
        .map(|row| pb::Envelope::decode(row.payload.as_slice()).expect("decodes"))
        .collect()
}

#[test]
fn stale_event_is_committed_with_the_batch_that_reveals_it() {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut lane = lane(&dir);
    lane.ingest(&[event(0, 1, 5, 0)], at(0))
        .expect("first value");

    // Point 0 has no value for two seconds: its deadline (1 s) has passed.
    lane.ingest(&[event(1, 2, 9, 2 * SEC_I)], at(2 * SEC))
        .expect("second batch");

    let envelopes = stored_envelopes(&lane);
    assert_eq!(envelopes.len(), 3);
    let stale = &envelopes[1];
    assert_eq!(stale.point_id, "S3/TM_001");
    assert_eq!(stale.provenance, vec!["cycle_check@7".to_owned()]);
    assert_eq!(
        stale.cause.as_ref().map(|c| c.normalized.as_str()),
        Some("supervision")
    );
    assert_eq!(envelopes[2].point_id, "S3/TM_002");

    let slot = lane.image().get(PointIndex(0)).expect("has a value");
    assert_eq!(slot.value, Value::Int(5));
    assert_eq!(
        slot.quality,
        Quality::new(Validity::Questionable).with_detail(Detail::OldData)
    );
    assert_eq!(
        lane.supervisor_state(PointIndex(0)),
        Some(CycleState::Stale)
    );
}

#[test]
fn a_new_value_after_stale_delivers_the_value_and_rearms() {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut lane = lane(&dir);
    lane.ingest(&[event(0, 1, 5, 0)], at(0)).expect("first");
    lane.ingest(&[event(1, 2, 0, 2 * SEC_I)], at(2 * SEC))
        .expect("reveal");

    lane.ingest(&[event(0, 3, 8, 3 * SEC_I)], at(3 * SEC))
        .expect("fresh");

    let slot = lane.image().get(PointIndex(0)).expect("value");
    assert_eq!(slot.value, Value::Int(8));
    assert_eq!(slot.quality, Quality::GOOD);
    assert_eq!(
        lane.supervisor_state(PointIndex(0)),
        Some(CycleState::Fresh)
    );
    // Re-armed: stale again one period after the new value, and only then.
    lane.ingest(&[event(1, 4, 0, 4 * SEC_I)], at(4 * SEC))
        .expect("later");
    assert_eq!(
        lane.supervisor_state(PointIndex(0)),
        Some(CycleState::Stale)
    );
}

#[test]
fn class_c_point_goes_stale_in_the_image_without_a_journal_row() {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut lane = lane(&dir);
    lane.ingest(&[event(2, 1, 4, 0)], at(0))
        .expect("class C value");
    assert_eq!(lane.log().last_seq().expect("last"), None);

    lane.ingest(&[event(1, 2, 0, 2 * SEC_I)], at(2 * SEC))
        .expect("reveal");

    // Only the class A value of point 1 was journaled: no row for class C.
    assert_eq!(lane.log().last_seq().expect("last"), Some(1));
    let slot = lane.image().get(PointIndex(2)).expect("value");
    assert_eq!(
        slot.quality,
        Quality::new(Validity::Questionable).with_detail(Detail::OldData)
    );
}

#[test]
fn a_failed_commit_undoes_the_expiry() {
    // A replayed event id makes the journal refuse the batch. The point must not
    // stay Stale without its stale event, so the next batch reports it again.
    let dir = tempfile::tempdir().expect("temp dir");
    let mut lane = lane(&dir);
    lane.ingest(&[event(0, 1, 5, 0)], at(0)).expect("first");

    let replay = lane
        .ingest(&[event(0, 1, 99, 2 * SEC_I)], at(2 * SEC))
        .expect_err("replay refused");
    assert!(matches!(
        replay,
        LaneError::Store(StoreError::DuplicateEvent)
    ));
    assert_eq!(lane.log().last_seq().expect("last"), Some(1));
    assert_eq!(
        lane.supervisor_state(PointIndex(0)),
        Some(CycleState::Fresh)
    );

    lane.ingest(&[event(1, 2, 0, 3 * SEC_I)], at(3 * SEC))
        .expect("next");

    let envelopes = stored_envelopes(&lane);
    assert_eq!(envelopes.len(), 3);
    assert_eq!(envelopes[1].point_id, "S3/TM_001");
    assert_eq!(envelopes[1].provenance, vec!["cycle_check@7".to_owned()]);
}
