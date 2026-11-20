//! Telemetry log: the `events` table of `telemetry.db` (SPECIFICATIONS §7.2).
//!
//! Every batch is written in one transaction with `synchronous=FULL` on a WAL
//! database, so a committed batch survives a power cut. The caller decides how
//! batches are grouped (the group commit of §7.2 is one call per group).

use std::path::Path;

use rusqlite::{params, Connection, ErrorCode, OptionalExtension, Transaction};

use crate::journal::OwnedEvent;
use thiserror::Error;

/// Class of an event (SPECIFICATIONS §7.5, [GW] §10.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventClass {
    A,
    B,
    /// Not journaled: class C updates the image directly ([GW] §10.1).
    C,
    Critical,
}

impl EventClass {
    pub(crate) const fn code(self) -> i64 {
        match self {
            Self::A => 1,
            Self::B => 2,
            Self::C => 3,
            Self::Critical => 4,
        }
    }

    fn from_code(code: i64) -> Option<Self> {
        match code {
            1 => Some(Self::A),
            2 => Some(Self::B),
            3 => Some(Self::C),
            4 => Some(Self::Critical),
            _ => None,
        }
    }
}

/// An event to append. `payload` is the encoded event, opaque to the store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NewEvent<'a> {
    pub partition: &'a str,
    /// `None` for an event that concerns a session, not a point.
    pub point: Option<u32>,
    pub class: EventClass,
    pub event_id: [u8; 16],
    pub revision: u32,
    /// Nanoseconds since the Unix epoch.
    pub ts_receive_ns: i64,
    pub payload: &'a [u8],
}

/// An event read back from the log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredEvent {
    pub seq: i64,
    pub partition: String,
    pub point: Option<u32>,
    pub class: EventClass,
    pub event_id: [u8; 16],
    pub revision: u32,
    pub ts_receive_ns: i64,
    pub payload: Vec<u8>,
}

/// A stored snapshot of an image: the journal sequence it was taken at, and its
/// encoded content.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    pub seq: i64,
    pub payload: Vec<u8>,
}

/// Proof that a batch is committed: its sequence numbers, inclusive.
///
/// The image may only be updated from a committed batch (PM-TEL-001), and the
/// `DURABLE` acknowledgement is sent only after this value exists (PM-TEL-002).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Committed {
    pub first_seq: i64,
    pub last_seq: i64,
}

/// Errors of the telemetry log.
#[derive(Debug, Error)]
pub enum StoreError {
    #[error("class C events are not journaled")]
    ClassNotJournaled,
    #[error("an event id of the batch is already in the log; nothing was written")]
    DuplicateEvent,
    #[error("stored event has an unknown class number {0}")]
    UnknownClass(i64),
    #[error("database error: {0}")]
    Database(#[from] rusqlite::Error),
    #[error("the transaction could not be committed: {0}")]
    CommitFailed(String),
    #[error("the writer has stopped")]
    WriterStopped,
    #[error("destination {0} has no cursor")]
    UnknownCursor(String),
    #[error("destination {0} exists with another kind (flux or image)")]
    CursorConflict(String),
    #[error("sequence {seq} is not journaled yet: the log ends at {last}")]
    AheadOfLog { seq: i64, last: i64 },
    #[error("retention refused: {0}")]
    ProtectedClass(&'static str),
}

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS snapshots (
    id          INTEGER PRIMARY KEY,
    partition   TEXT    NOT NULL,
    seq         INTEGER NOT NULL,
    payload     BLOB    NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
    seq         INTEGER PRIMARY KEY,
    partition   TEXT    NOT NULL,
    point       INTEGER,
    class       INTEGER NOT NULL,
    event_id    BLOB    NOT NULL UNIQUE CHECK (length(event_id) = 16),
    revision    INTEGER NOT NULL,
    ts_receive  INTEGER NOT NULL,
    payload     BLOB    NOT NULL
);
";

/// The telemetry log of one cell.
#[derive(Debug)]
pub struct TelemetryLog {
    pub(crate) conn: Connection,
}

impl TelemetryLog {
    /// Opens or creates the log at `path` in WAL mode with `synchronous=FULL`.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Database`] when the file cannot be opened or migrated.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let conn = Connection::open(path)?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "FULL")?;
        conn.execute_batch(SCHEMA)?;
        conn.execute_batch(crate::cursors::SCHEMA)?;
        Ok(Self { conn })
    }

    /// Appends a batch in one transaction. Returns `None` for an empty batch.
    ///
    /// Either every event of the batch is written, or none is. Class C events
    /// and an event id already in the log are refused before anything is
    /// written.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::ClassNotJournaled`] for a class C event,
    /// [`StoreError::DuplicateEvent`] for an id already in the log, and
    /// [`StoreError::Database`] on any other failure.
    pub fn append(&mut self, batch: &[NewEvent<'_>]) -> Result<Option<Committed>, StoreError> {
        if batch.iter().any(|e| e.class == EventClass::C) {
            return Err(StoreError::ClassNotJournaled);
        }
        if batch.is_empty() {
            return Ok(None);
        }
        let tx = self.conn.transaction()?;
        let committed = insert_all(&tx, batch)?;
        tx.commit()?;
        Ok(Some(committed))
    }

    /// Reads up to `limit` events with a sequence number of at least `seq`.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Database`] on a read failure, or
    /// [`StoreError::UnknownClass`] for a row of an unknown class.
    pub fn read_from(&self, seq: i64, limit: u32) -> Result<Vec<StoredEvent>, StoreError> {
        let mut statement = self.conn.prepare_cached(
            "SELECT seq, partition, point, class, event_id, revision, ts_receive, payload
             FROM events WHERE seq >= ?1 ORDER BY seq LIMIT ?2",
        )?;
        let rows = statement.query_map(params![seq, i64::from(limit)], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<i64>>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, Vec<u8>>(4)?,
                row.get::<_, i64>(5)?,
                row.get::<_, i64>(6)?,
                row.get::<_, Vec<u8>>(7)?,
            ))
        })?;

        let mut events = Vec::new();
        for row in rows {
            let (seq, partition, point, class, event_id, revision, ts, payload) = row?;
            let class = EventClass::from_code(class).ok_or(StoreError::UnknownClass(class))?;
            events.push(StoredEvent {
                seq,
                partition,
                point: point.map(|point| u32::try_from(point).unwrap_or(u32::MAX)),
                class,
                event_id: event_id.try_into().unwrap_or([0; 16]),
                revision: u32::try_from(revision).unwrap_or(u32::MAX),
                ts_receive_ns: ts,
                payload,
            });
        }
        Ok(events)
    }

    /// Commits several batches in one transaction. Each batch is atomic on its
    /// own: a batch that is refused is rolled back to its savepoint and the others
    /// are still committed. Returns one result per batch, in order.
    ///
    /// If the transaction itself cannot be committed, every batch fails with
    /// [`StoreError::CommitFailed`].
    pub fn append_groups(
        &mut self,
        groups: &[Vec<OwnedEvent>],
    ) -> Vec<Result<Option<Committed>, StoreError>> {
        let mut tx = match self.conn.transaction() {
            Ok(tx) => tx,
            Err(err) => {
                return groups
                    .iter()
                    .map(|_| Err(StoreError::CommitFailed(err.to_string())))
                    .collect()
            }
        };
        let mut results = Vec::with_capacity(groups.len());
        for group in groups {
            results.push(append_group(&mut tx, group));
        }
        match tx.commit() {
            Ok(()) => results,
            Err(err) => groups
                .iter()
                .map(|_| Err(StoreError::CommitFailed(err.to_string())))
                .collect(),
        }
    }

    /// Stores a snapshot of the image of `partition`, taken at journal sequence
    /// `seq`. The newest snapshot of a partition is the one that counts.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Database`] when the row cannot be written.
    pub fn put_snapshot(
        &mut self,
        partition: &str,
        seq: i64,
        payload: &[u8],
    ) -> Result<(), StoreError> {
        self.conn.execute(
            "INSERT INTO snapshots (partition, seq, payload) VALUES (?1, ?2, ?3)",
            params![partition, seq, payload],
        )?;
        Ok(())
    }

    /// The newest snapshot of `partition`, if any.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Database`] when the read fails.
    pub fn latest_snapshot(&self, partition: &str) -> Result<Option<Snapshot>, StoreError> {
        let row = self
            .conn
            .query_row(
                "SELECT seq, payload FROM snapshots WHERE partition = ?1 ORDER BY id DESC LIMIT 1",
                params![partition],
                |row| {
                    Ok(Snapshot {
                        seq: row.get(0)?,
                        payload: row.get(1)?,
                    })
                },
            )
            .optional()?;
        Ok(row)
    }

    /// The highest sequence number in the log, if any.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Database`] on a read failure.
    pub fn last_seq(&self) -> Result<Option<i64>, StoreError> {
        let seq = self
            .conn
            .query_row("SELECT MAX(seq) FROM events", [], |row| {
                row.get::<_, Option<i64>>(0)
            })
            .optional()?
            .flatten();
        Ok(seq)
    }
}

fn insert_all(conn: &Connection, batch: &[NewEvent<'_>]) -> Result<Committed, StoreError> {
    let mut statement = conn.prepare_cached(
        "INSERT INTO events (partition, point, class, event_id, revision, ts_receive, payload)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
    )?;
    let mut first_seq = None;
    let mut last_seq = 0;
    for event in batch {
        let inserted = statement.execute(params![
            event.partition,
            event.point.map(i64::from),
            event.class.code(),
            event.event_id.as_slice(),
            i64::from(event.revision),
            event.ts_receive_ns,
            event.payload,
        ]);
        match inserted {
            Ok(_) => {
                let seq = conn.last_insert_rowid();
                first_seq.get_or_insert(seq);
                last_seq = seq;
            }
            Err(rusqlite::Error::SqliteFailure(failure, _))
                if failure.code == ErrorCode::ConstraintViolation =>
            {
                return Err(StoreError::DuplicateEvent);
            }
            Err(err) => return Err(err.into()),
        }
    }
    Ok(Committed {
        first_seq: first_seq.unwrap_or(last_seq),
        last_seq,
    })
}

/// One batch inside a shared transaction, under its own savepoint.
fn append_group(
    tx: &mut Transaction<'_>,
    group: &[OwnedEvent],
) -> Result<Option<Committed>, StoreError> {
    if group.iter().any(|e| e.class == EventClass::C) {
        return Err(StoreError::ClassNotJournaled);
    }
    if group.is_empty() {
        return Ok(None);
    }
    let savepoint = tx.savepoint()?;
    let borrowed: Vec<NewEvent<'_>> = group.iter().map(OwnedEvent::as_new).collect();
    let committed = insert_all(&savepoint, &borrowed)?;
    savepoint.commit()?;
    Ok(Some(committed))
}
