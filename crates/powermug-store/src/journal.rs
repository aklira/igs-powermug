//! The journal as the core uses it: one trait, implemented by a file opened by
//! the cell, and by a handle onto the single writer of the cell (§7.2).

use crate::log::{
    Committed, EventClass, NewEvent, Snapshot, StoreError, StoredEvent, TelemetryLog,
};

/// An event to commit, owning its data so that it can cross a thread.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnedEvent {
    pub partition: String,
    pub point: Option<u32>,
    pub class: EventClass,
    pub event_id: [u8; 16],
    pub revision: u32,
    pub ts_receive_ns: i64,
    pub payload: Vec<u8>,
}

impl OwnedEvent {
    /// The borrowed form, for the insertion.
    #[must_use]
    pub fn as_new(&self) -> NewEvent<'_> {
        NewEvent {
            partition: &self.partition,
            point: self.point,
            class: self.class,
            event_id: self.event_id,
            revision: self.revision,
            ts_receive_ns: self.ts_receive_ns,
            payload: &self.payload,
        }
    }
}

impl From<&NewEvent<'_>> for OwnedEvent {
    fn from(event: &NewEvent<'_>) -> Self {
        Self {
            partition: event.partition.to_owned(),
            point: event.point,
            class: event.class,
            event_id: event.event_id,
            revision: event.revision,
            ts_receive_ns: event.ts_receive_ns,
            payload: event.payload.to_vec(),
        }
    }
}

/// The operations the core needs from its journal.
pub trait Journal: Send + std::fmt::Debug {
    /// Appends a batch atomically. Returns `None` for an empty batch.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::ClassNotJournaled`], [`StoreError::DuplicateEvent`]
    /// or a database error; nothing of the batch is then written.
    fn append(&mut self, batch: &[NewEvent<'_>]) -> Result<Option<Committed>, StoreError>;

    /// Reads up to `limit` events with a sequence number of at least `seq`.
    ///
    /// # Errors
    ///
    /// Returns a database error when the read fails.
    fn read_from(&self, seq: i64, limit: u32) -> Result<Vec<StoredEvent>, StoreError>;

    /// The highest sequence number of the journal, if any.
    ///
    /// # Errors
    ///
    /// Returns a database error when the read fails.
    fn last_seq(&self) -> Result<Option<i64>, StoreError>;

    /// The number of events of `class` the journal retains: its usage against the quota.
    ///
    /// # Errors
    ///
    /// Returns a database error when the read fails.
    fn usage(&self, class: EventClass) -> Result<u64, StoreError>;

    /// Stores a snapshot of the image of `partition` at journal sequence `seq`.
    ///
    /// # Errors
    ///
    /// Returns a database error when the snapshot cannot be written.
    fn put_snapshot(&mut self, partition: &str, seq: i64, payload: &[u8])
        -> Result<(), StoreError>;

    /// The newest snapshot of `partition`, if any.
    ///
    /// # Errors
    ///
    /// Returns a database error when the read fails.
    fn latest_snapshot(&self, partition: &str) -> Result<Option<Snapshot>, StoreError>;
}

impl Journal for TelemetryLog {
    fn append(&mut self, batch: &[NewEvent<'_>]) -> Result<Option<Committed>, StoreError> {
        TelemetryLog::append(self, batch)
    }

    fn read_from(&self, seq: i64, limit: u32) -> Result<Vec<StoredEvent>, StoreError> {
        TelemetryLog::read_from(self, seq, limit)
    }

    fn last_seq(&self) -> Result<Option<i64>, StoreError> {
        TelemetryLog::last_seq(self)
    }

    fn usage(&self, class: EventClass) -> Result<u64, StoreError> {
        TelemetryLog::usage(self, class)
    }

    fn put_snapshot(
        &mut self,
        partition: &str,
        seq: i64,
        payload: &[u8],
    ) -> Result<(), StoreError> {
        TelemetryLog::put_snapshot(self, partition, seq, payload)
    }

    fn latest_snapshot(&self, partition: &str) -> Result<Option<Snapshot>, StoreError> {
        TelemetryLog::latest_snapshot(self, partition)
    }
}
