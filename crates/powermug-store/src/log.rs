//! Telemetry log: the `events` table of `telemetry.db` (SPECIFICATIONS §7.2).
//!
//! Every batch is written in one transaction with `synchronous=FULL` on a WAL
//! database, so a committed batch survives a power cut. The caller decides how
//! batches are grouped (the group commit of §7.2 is one call per group).

use std::path::Path;

use rusqlite::{params, Connection, ErrorCode, OptionalExtension, Transaction};
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
    const fn code(self) -> i64 {
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
    pub point: u32,
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
    pub point: u32,
    pub class: EventClass,
    pub event_id: [u8; 16],
    pub revision: u32,
    pub ts_receive_ns: i64,
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
}

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS events (
    seq         INTEGER PRIMARY KEY,
    partition   TEXT    NOT NULL,
    point       INTEGER NOT NULL,
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
    conn: Connection,
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
                row.get::<_, i64>(2)?,
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
                point: u32::try_from(point).unwrap_or(u32::MAX),
                class,
                event_id: event_id.try_into().unwrap_or([0; 16]),
                revision: u32::try_from(revision).unwrap_or(u32::MAX),
                ts_receive_ns: ts,
                payload,
            });
        }
        Ok(events)
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

fn insert_all(tx: &Transaction<'_>, batch: &[NewEvent<'_>]) -> Result<Committed, StoreError> {
    let mut statement = tx.prepare_cached(
        "INSERT INTO events (partition, point, class, event_id, revision, ts_receive, payload)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
    )?;
    let mut first_seq = None;
    let mut last_seq = 0;
    for event in batch {
        let inserted = statement.execute(params![
            event.partition,
            i64::from(event.point),
            event.class.code(),
            event.event_id.as_slice(),
            i64::from(event.revision),
            event.ts_receive_ns,
            event.payload,
        ]);
        match inserted {
            Ok(_) => {
                let seq = tx.last_insert_rowid();
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
