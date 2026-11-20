//! The single writer of a cell: batches from several threads, grouped and
//! committed with their own ranges and savepoints.

use powermug_store::{
    Committed, EventClass, GroupJournal, Journal, NewEvent, StoreError, TelemetryLog,
};

fn id(n: u32) -> [u8; 16] {
    let mut bytes = [0u8; 16];
    bytes[6] = 0x70;
    bytes[8] = 0x80;
    bytes[12..16].copy_from_slice(&n.to_be_bytes());
    bytes
}

fn event(n: u32, payload: &[u8]) -> (String, [u8; 16], Vec<u8>) {
    ("S3".to_owned(), id(n), payload.to_vec())
}

fn batch(events: &[(String, [u8; 16], Vec<u8>)], class: EventClass) -> Vec<NewEvent<'_>> {
    events
        .iter()
        .map(|(partition, event_id, payload)| NewEvent {
            partition,
            point: Some(0),
            class,
            event_id: *event_id,
            revision: 1,
            ts_receive_ns: 0,
            payload,
        })
        .collect()
}

#[test]
fn batches_from_several_threads_get_their_own_ranges() {
    let dir = tempfile::tempdir().expect("temp dir");
    let (journal, writer) = GroupJournal::spawn(dir.path().join("telemetry.db")).expect("spawns");

    let handles: Vec<_> = (0..4u32)
        .map(|thread| {
            let mut journal = journal.clone();
            std::thread::spawn(move || {
                let events: Vec<_> = (0..3u32).map(|i| event(thread * 100 + i, b"p")).collect();
                journal.append(&batch(&events, EventClass::A))
            })
        })
        .collect();
    let mut ranges: Vec<Committed> = handles
        .into_iter()
        .map(|handle| {
            handle
                .join()
                .expect("joins")
                .expect("committed")
                .expect("non-empty")
        })
        .collect();
    ranges.sort_by_key(|range| range.first_seq);

    assert_eq!(ranges.first().map(|r| r.first_seq), Some(1));
    assert_eq!(ranges.last().map(|r| r.last_seq), Some(12));
    for pair in ranges.windows(2) {
        assert_eq!(
            pair[0].last_seq + 1,
            pair[1].first_seq,
            "ranges are contiguous"
        );
    }
    drop(journal);
    writer.join();
}

#[test]
fn a_duplicate_does_not_take_the_other_batches_of_its_group_with_it() {
    let dir = tempfile::tempdir().expect("temp dir");
    let (journal, writer) = GroupJournal::spawn(dir.path().join("telemetry.db")).expect("spawns");
    let mut first = journal.clone();
    let first_events = [event(1, b"a")];
    first
        .append(&batch(&first_events, EventClass::A))
        .expect("first");

    let mut duplicate = journal.clone();
    let duplicate_events = [event(1, b"again")];
    let mut fresh = journal.clone();
    let fresh_events = [event(2, b"b")];
    let refused =
        std::thread::spawn(move || duplicate.append(&batch(&duplicate_events, EventClass::A)));
    let accepted = std::thread::spawn(move || fresh.append(&batch(&fresh_events, EventClass::A)));

    let refused = refused.join().expect("joins");
    let accepted = accepted.join().expect("joins");
    assert!(
        matches!(refused, Err(StoreError::DuplicateEvent)),
        "{refused:?}"
    );
    assert!(matches!(accepted, Ok(Some(_))), "{accepted:?}");
    assert_eq!(journal.last_seq().expect("last"), Some(2));
    drop(first);
    drop(journal);
    writer.join();
}

#[test]
fn reads_go_through_the_writer() {
    let dir = tempfile::tempdir().expect("temp dir");
    let (mut journal, writer) =
        GroupJournal::spawn(dir.path().join("telemetry.db")).expect("spawns");
    let events = [event(1, b"one"), event(2, b"two")];
    journal
        .append(&batch(&events, EventClass::B))
        .expect("appends");

    let rows = journal.read_from(2, 10).expect("reads");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].payload, b"two");
    assert_eq!(rows[0].class, EventClass::B);
    drop(journal);
    writer.join();
}

#[test]
fn class_c_is_refused_before_it_reaches_the_writer() {
    let dir = tempfile::tempdir().expect("temp dir");
    let (mut journal, writer) =
        GroupJournal::spawn(dir.path().join("telemetry.db")).expect("spawns");
    let events = [event(1, b"c")];

    let err = journal
        .append(&batch(&events, EventClass::C))
        .expect_err("refused");

    assert!(matches!(err, StoreError::ClassNotJournaled));
    assert_eq!(journal.last_seq().expect("last"), None);
    drop(journal);
    writer.join();
}

#[test]
fn committed_rows_survive_the_writer_and_a_reopen() {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("telemetry.db");
    let (mut journal, writer) = GroupJournal::spawn(&path).expect("spawns");
    let events = [event(7, b"kept")];
    journal
        .append(&batch(&events, EventClass::A))
        .expect("appends");
    drop(journal);
    writer.join();

    let log = TelemetryLog::open(&path).expect("reopens");
    let rows = log.read_from(1, 10).expect("reads");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].payload, b"kept");
}
