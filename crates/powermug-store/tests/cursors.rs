//! Destination cursors and retention of the telemetry log (SPECIFICATIONS §7.2, §7.4,
//! PM-TEL-004, FUN-SAF-007).

use powermug_store::{EventClass, NewEvent, Policy, Start, StoreError, TelemetryLog};

const NOW: i64 = 1_000_000_000_000;

fn log(dir: &tempfile::TempDir) -> TelemetryLog {
    TelemetryLog::open(dir.path().join("telemetry.db")).expect("opens")
}

/// Appends one event per `(class, age_ns)`: the age sets its receive time before `NOW`.
fn append(log: &mut TelemetryLog, events: &[(EventClass, i64)]) {
    let mut ids = Vec::new();
    let base = log.last_seq().expect("seq").unwrap_or(0);
    for (index, (class, age)) in events.iter().enumerate() {
        let mut id = [0u8; 16];
        id[..8]
            .copy_from_slice(&(base + i64::try_from(index).expect("few events") + 1).to_be_bytes());
        id[8] = 0x70;
        id[15] = 0x80;
        ids.push((*class, *age, id));
    }
    let batch: Vec<NewEvent<'_>> = ids
        .iter()
        .map(|(class, age, id)| NewEvent {
            partition: "S3",
            point: Some(1),
            class: *class,
            event_id: *id,
            revision: 1,
            ts_receive_ns: NOW - age,
            payload: b"x",
        })
        .collect();
    log.append(&batch).expect("appends");
}

#[test]
fn a_destination_starts_where_it_declares() {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut log = log(&dir);
    append(
        &mut log,
        &[
            (EventClass::A, 300),
            (EventClass::A, 200),
            (EventClass::B, 100),
        ],
    );

    // Now: the events journaled from here on; nothing is delivered before.
    let now = log
        .declare_cursor("now", true, Start::Now, NOW)
        .expect("declares");
    assert_eq!(now.last_seq, 3);
    // Oldest retained: everything is delivered.
    let oldest = log
        .declare_cursor("oldest", true, Start::OldestRetained, NOW)
        .expect("declares");
    assert_eq!(oldest.last_seq, 0);
    // Timestamp: from the first event received at or after the time.
    let at = log
        .declare_cursor("at", false, Start::Timestamp(NOW - 250), NOW)
        .expect("declares");
    assert_eq!(at.last_seq, 1);
}

#[test]
fn a_cursor_never_moves_back_and_cannot_pass_the_log() {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut log = log(&dir);
    append(&mut log, &[(EventClass::B, 0), (EventClass::B, 0)]);
    log.declare_cursor("flux", true, Start::OldestRetained, NOW)
        .expect("declares");

    assert_eq!(log.acknowledge("flux", 2, NOW).expect("acks").last_seq, 2);
    assert_eq!(
        log.acknowledge("flux", 1, NOW).expect("older ack").last_seq,
        2
    );
    assert!(matches!(
        log.acknowledge("flux", 5, NOW),
        Err(StoreError::AheadOfLog { seq: 5, last: 2 })
    ));
    assert!(matches!(
        log.acknowledge("missing", 1, NOW),
        Err(StoreError::UnknownCursor(name)) if name == "missing"
    ));
}

#[test]
fn a_destination_declared_with_another_kind_is_refused() {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut log = log(&dir);
    log.declare_cursor("dest", true, Start::Now, NOW)
        .expect("declares");
    assert!(matches!(
        log.declare_cursor("dest", false, Start::Now, NOW),
        Err(StoreError::CursorConflict(name)) if name == "dest"
    ));
}

#[test]
fn replay_reads_after_the_cursor_and_reports_what_retention_removed() {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut log = log(&dir);
    append(
        &mut log,
        &[
            (EventClass::B, 900),
            (EventClass::B, 800),
            (EventClass::B, 10),
        ],
    );
    log.declare_cursor("image", false, Start::OldestRetained, NOW)
        .expect("declares");
    log.acknowledge("image", 1, NOW).expect("acks");

    let replay = log.replay("image", 10).expect("replays");
    assert_eq!(
        replay.events.iter().map(|e| e.seq).collect::<Vec<_>>(),
        vec![2, 3]
    );
    assert_eq!(replay.skipped_before, None);

    // Age 500 ns: the two old class B events go; the destination was behind them.
    log.set_retention(
        EventClass::B,
        Policy {
            max_events: None,
            max_age_ns: Some(500),
        },
    )
    .expect("sets");
    let enforced = log.enforce_retention(NOW).expect("enforces");
    assert_eq!(enforced[0].class, EventClass::B);
    assert_eq!(enforced[0].deleted, 2);
    assert_eq!(enforced[0].oldest_seq, Some(3));

    let replay = log.replay("image", 10).expect("replays");
    assert_eq!(
        replay.events.iter().map(|e| e.seq).collect::<Vec<_>>(),
        vec![3]
    );
    assert_eq!(replay.skipped_before, Some(3));
    assert_eq!(log.dropped(EventClass::B).expect("counted"), 2);
}

#[test]
fn quota_keeps_the_newest_events_of_the_class() {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut log = log(&dir);
    append(&mut log, &[(EventClass::B, 0); 5]);
    log.set_retention(
        EventClass::B,
        Policy {
            max_events: Some(2),
            max_age_ns: None,
        },
    )
    .expect("sets");
    let enforced = log.enforce_retention(NOW).expect("enforces");
    assert_eq!(enforced[0].deleted, 3);
    assert_eq!(enforced[0].oldest_seq, Some(4));
    // A second pass has nothing more to do.
    assert_eq!(log.enforce_retention(NOW).expect("again")[0].deleted, 0);
}

#[test]
fn class_a_is_kept_until_every_flux_cursor_has_passed_it() {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut log = log(&dir);
    append(&mut log, &[(EventClass::A, 0); 4]);
    log.set_retention(
        EventClass::A,
        Policy {
            max_events: Some(1),
            max_age_ns: None,
        },
    )
    .expect("sets");

    // No flux cursor: nothing of class A is deleted by retention.
    assert_eq!(log.enforce_retention(NOW).expect("enforces")[0].deleted, 0);

    log.declare_cursor("scada", true, Start::OldestRetained, NOW)
        .expect("declares");
    log.declare_cursor("slow", true, Start::OldestRetained, NOW)
        .expect("declares");
    log.acknowledge("scada", 4, NOW).expect("acks");
    log.acknowledge("slow", 2, NOW).expect("acks");

    // The slowest flux cursor is at 2: events 1 and 2 may go, 3 and 4 stay.
    let enforced = log.enforce_retention(NOW).expect("enforces");
    assert_eq!(enforced[0].deleted, 2);
    assert_eq!(enforced[0].oldest_seq, Some(3));

    log.acknowledge("slow", 4, NOW).expect("acks");
    // Quota 1 keeps the newest, 4; event 3 is now passed by every flux cursor.
    assert_eq!(log.enforce_retention(NOW).expect("enforces")[0].deleted, 1);
}

#[test]
fn an_image_cursor_does_not_protect_class_a() {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut log = log(&dir);
    append(&mut log, &[(EventClass::A, 0); 2]);
    log.set_retention(
        EventClass::A,
        Policy {
            max_events: Some(1),
            max_age_ns: None,
        },
    )
    .expect("sets");
    log.declare_cursor("view", false, Start::OldestRetained, NOW)
        .expect("declares");
    // Still no flux cursor: class A is not deleted.
    assert_eq!(log.enforce_retention(NOW).expect("enforces")[0].deleted, 0);
}

#[test]
fn critical_and_class_c_are_never_subject_to_retention() {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut log = log(&dir);
    let policy = Policy {
        max_events: Some(0),
        max_age_ns: Some(0),
    };
    assert!(matches!(
        log.set_retention(EventClass::Critical, policy),
        Err(StoreError::ProtectedClass(_))
    ));
    assert!(matches!(
        log.set_retention(EventClass::C, policy),
        Err(StoreError::ProtectedClass(_))
    ));
}

#[test]
fn removing_a_cursor_releases_the_events_it_protected() {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut log = log(&dir);
    append(&mut log, &[(EventClass::A, 0); 3]);
    log.set_retention(
        EventClass::A,
        Policy {
            max_events: Some(0),
            max_age_ns: None,
        },
    )
    .expect("sets");
    log.declare_cursor("scada", true, Start::OldestRetained, NOW)
        .expect("declares");
    log.acknowledge("scada", 1, NOW).expect("acks");
    assert_eq!(log.enforce_retention(NOW).expect("enforces")[0].deleted, 1);
    assert!(log.remove_cursor("scada").expect("removes"));
    // Without its cursor, the remaining class A events are no longer protected at all.
    assert_eq!(log.enforce_retention(NOW).expect("enforces")[0].deleted, 0);
}
