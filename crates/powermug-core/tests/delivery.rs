//! The delivery of journaled events to a flux destination (SPECIFICATIONS 10.3, FUN-SAF-002 to
//! FUN-SAF-005): a caught-up destination is served from the live ring, a destination behind it is
//! served from the journal, marked as a replay and capped per call, and nothing is lost. A
//! reduced ACC-001: every journaled event reaches the destination.

use powermug_core::delivery::{self, Batch, DeliveryError};
use powermug_core::lane::{Lane, PointSpec, PointTable, SessionId};
use powermug_core::Times;
use powermug_pivot::proto::v1 as pb;
use powermug_pivot::{EventId, PointEvent, PointIndex, Quality, Value};
use powermug_store::{EventClass, Start, TelemetryLog};

const SEC: u64 = 1_000_000_000;
const DESTINATION: &str = "kafka-test";

fn table() -> PointTable {
    PointTable::new(
        7,
        vec![PointSpec {
            site_id: "S3".into(),
            point_id: "S3/TM_001".into(),
            class: EventClass::A,
            session: Some(SessionId(1)),
            supervision: None,
        }],
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

fn event(id: u32) -> PointEvent {
    PointEvent {
        point: PointIndex(0),
        value: Value::Int(i64::from(id)),
        quality: Quality::GOOD,
        source_time_ns: 10,
        receive_time_ns: 20,
        process_time_ns: 30,
        event_id: event_id(id),
        revision_index: 7,
    }
}

fn at(seconds: u64) -> Times {
    Times {
        monotonic_ns: seconds * SEC,
        wall_ns: i64::try_from(1_000 * SEC + seconds * SEC).expect("in range"),
    }
}

/// A lane writing `critical`-style events into `dir`, and a second handle of the same journal for
/// the destination's reads and acknowledgements.
fn open(dir: &std::path::Path) -> (Lane, TelemetryLog) {
    let path = dir.join("telemetry.db");
    let lane = Lane::new(table(), TelemetryLog::open(&path).expect("opens"), 0);
    let mut reader = TelemetryLog::open(&path).expect("opens");
    reader
        .declare_cursor(DESTINATION, true, Start::OldestRetained, 1)
        .expect("declares the destination");
    (lane, reader)
}

/// The next batch for the destination, from its cursor: what the cell serves it.
fn feed(
    lane: &Lane,
    reader: &TelemetryLog,
    destination: &str,
    limit: u32,
) -> Result<Batch, DeliveryError> {
    let cursor = reader
        .cursor(destination)?
        .ok_or_else(|| powermug_store::StoreError::UnknownCursor(destination.to_owned()))?
        .last_seq;
    delivery::after(lane, cursor, limit)
}

/// The envelopes of a batch, in order.
fn envelopes(batch: &Batch) -> Vec<pb::Envelope> {
    batch
        .items
        .iter()
        .map(|(_, envelope)| envelope.clone())
        .collect()
}

#[test]
fn a_caught_up_destination_is_served_from_the_live_ring() {
    let dir = tempfile::tempdir().expect("temp dir");
    let (mut lane, reader) = open(dir.path());
    for id in 1..=3 {
        lane.ingest(&[event(id)], at(u64::from(id)))
            .expect("ingests");
    }

    let fed = feed(&lane, &reader, DESTINATION, 10).expect("feeds");
    assert!(!fed.replay, "served from the ring, not the journal");
    let ids: Vec<Vec<u8>> = envelopes(&fed).iter().map(|e| e.event_id.clone()).collect();
    assert_eq!(ids.len(), 3);
    assert_eq!(ids[0], event_id(1).as_bytes().to_vec());
    assert_eq!(ids[2], event_id(3).as_bytes().to_vec());
}

#[test]
fn a_destination_behind_the_ring_is_replayed_from_the_journal_without_loss() {
    let dir = tempfile::tempdir().expect("temp dir");
    let (mut lane, reader) = open(dir.path());
    lane.set_live_capacity(2);
    for id in 1..=5 {
        lane.ingest(&[event(id)], at(u64::from(id)))
            .expect("ingests");
    }

    // The ring holds the last two only: the destination must read the journal.
    let fed = feed(&lane, &reader, DESTINATION, 10).expect("feeds");
    assert!(fed.replay, "marked as a replay");
    let ids: Vec<Vec<u8>> = envelopes(&fed).iter().map(|e| e.event_id.clone()).collect();
    let expected: Vec<Vec<u8>> = (1..=5).map(|id| event_id(id).as_bytes().to_vec()).collect();
    assert_eq!(
        ids, expected,
        "every journaled event is delivered, in order"
    );
}

#[test]
fn a_replay_is_capped_per_call() {
    let dir = tempfile::tempdir().expect("temp dir");
    let (mut lane, reader) = open(dir.path());
    lane.set_live_capacity(2);
    for id in 1..=5 {
        lane.ingest(&[event(id)], at(u64::from(id)))
            .expect("ingests");
    }

    let fed = feed(&lane, &reader, DESTINATION, 2).expect("feeds");
    assert!(fed.replay);
    assert_eq!(envelopes(&fed).len(), 2, "at most the limit per call");
}

#[test]
fn an_acknowledged_destination_is_fed_only_what_follows_its_cursor() {
    let dir = tempfile::tempdir().expect("temp dir");
    let (mut lane, mut reader) = open(dir.path());
    lane.set_live_capacity(2);
    for id in 1..=5 {
        lane.ingest(&[event(id)], at(u64::from(id)))
            .expect("ingests");
    }
    reader.acknowledge(DESTINATION, 5, 2).expect("acknowledges");

    let fed = feed(&lane, &reader, DESTINATION, 10).expect("feeds");
    assert!(envelopes(&fed).is_empty(), "nothing after the cursor");

    lane.ingest(&[event(6)], at(6)).expect("ingests");
    let fed = feed(&lane, &reader, DESTINATION, 10).expect("feeds");
    assert!(!fed.replay, "the ring covers the cursor again");
    assert_eq!(envelopes(&fed).len(), 1);
    assert_eq!(envelopes(&fed)[0].event_id, event_id(6).as_bytes().to_vec());
}

#[test]
fn an_undeclared_destination_is_an_error() {
    let dir = tempfile::tempdir().expect("temp dir");
    let (lane, reader) = open(dir.path());
    assert!(feed(&lane, &reader, "nobody", 10).is_err());
}
