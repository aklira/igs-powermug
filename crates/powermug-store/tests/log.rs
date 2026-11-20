//! The telemetry log on a real database file.

use powermug_store::{Committed, EventClass, NewEvent, StoreError, TelemetryLog};

fn event(id: u8, class: EventClass) -> NewEvent<'static> {
    NewEvent {
        partition: "S3",
        point: u32::from(id),
        class,
        event_id: [id; 16],
        revision: 7,
        ts_receive_ns: 1_000 + i64::from(id),
        payload: b"encoded",
    }
}

fn open(dir: &tempfile::TempDir) -> TelemetryLog {
    TelemetryLog::open(dir.path().join("telemetry.db")).expect("opens")
}

#[test]
fn committed_batch_is_read_back_in_order_and_survives_reopen() {
    let dir = tempfile::tempdir().expect("temp dir");
    {
        let mut log = open(&dir);
        let committed = log
            .append(&[event(1, EventClass::A), event(2, EventClass::B)])
            .expect("appends");
        assert_eq!(
            committed,
            Some(Committed {
                first_seq: 1,
                last_seq: 2
            })
        );
    }

    let log = open(&dir);
    let events = log.read_from(1, 10).expect("reads");
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].seq, 1);
    assert_eq!(events[0].class, EventClass::A);
    assert_eq!(events[0].event_id, [1; 16]);
    assert_eq!(events[1].class, EventClass::B);
    assert_eq!(events[1].payload, b"encoded");
    assert_eq!(log.last_seq().expect("last"), Some(2));
}

#[test]
fn duplicate_event_id_writes_nothing_from_the_batch() {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut log = open(&dir);
    log.append(&[event(1, EventClass::A)]).expect("first batch");

    let err = log
        .append(&[event(2, EventClass::A), event(1, EventClass::A)])
        .expect_err("duplicate refused");

    assert!(matches!(err, StoreError::DuplicateEvent));
    // Event 2 was in the same batch and must not be there.
    assert_eq!(log.last_seq().expect("last"), Some(1));
    assert_eq!(log.read_from(1, 10).expect("reads").len(), 1);
}

#[test]
fn class_c_is_refused_and_nothing_is_written() {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut log = open(&dir);

    let err = log
        .append(&[event(1, EventClass::A), event(2, EventClass::C)])
        .expect_err("refused");

    assert!(matches!(err, StoreError::ClassNotJournaled));
    assert_eq!(log.last_seq().expect("last"), None);
}

#[test]
fn empty_batch_commits_nothing() {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut log = open(&dir);
    assert_eq!(log.append(&[]).expect("empty is fine"), None);
    assert_eq!(log.last_seq().expect("last"), None);
}

#[test]
fn read_respects_the_starting_sequence_and_the_limit() {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut log = open(&dir);
    let batch: Vec<NewEvent<'_>> = (1..=5).map(|i| event(i, EventClass::Critical)).collect();
    log.append(&batch).expect("appends");

    let slice = log.read_from(2, 2).expect("reads");
    let seqs: Vec<i64> = slice.iter().map(|e| e.seq).collect();
    assert_eq!(seqs, vec![2, 3]);
}

#[test]
fn database_is_in_wal_mode_on_disk() {
    let dir = tempfile::tempdir().expect("temp dir");
    let _log = open(&dir);
    // The journal mode is a property of the file, so a second connection sees it.
    let raw = rusqlite::Connection::open(dir.path().join("telemetry.db")).expect("opens");
    let mode: String = raw
        .query_row("PRAGMA journal_mode", [], |row| row.get(0))
        .expect("reads mode");
    assert_eq!(mode, "wal");
}
