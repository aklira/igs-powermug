//! Where the core puts a batch it accepted (PM-TEL-002).
//!
//! The contract does not know the journal. The core implements [`BatchSink`]:
//! `commit` returns `Ok` only once the batch is durable, and the service sends
//! `DURABLE` only then.

use crate::proto::v1::EventBatch;
use powermug_pivot::proto::v1::{CommunicationState, Envelope};
use tokio::sync::broadcast;

/// What the core did with an accepted batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durability {
    /// The batch is in the journal: its acknowledgement is `DURABLE`.
    Durable,
    /// The batch held only class C events, which skip the journal (§7.1). Its
    /// final acknowledgement is `RECEIVED`: it is in memory only.
    NotJournaled,
}

/// The batch was not accepted. The reason is bounded text sent to the adapter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Refusal {
    pub reason: String,
}

impl Refusal {
    #[must_use]
    pub fn new(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
        }
    }
}

/// The North feed of one destination: the cell's events after a journal sequence, and the
/// acknowledged position of the destination (SPECIFICATIONS 10.3). Events are read from the
/// journal when the live ring no longer holds them, so a slow destination loses nothing.
pub trait NorthFeed: Send {
    /// The acknowledged position: the journal sequence of the last event the destination has
    /// acknowledged, zero before any.
    ///
    /// # Errors
    ///
    /// Returns a [`Refusal`] when the cursor cannot be read.
    fn cursor(&mut self) -> Result<i64, Refusal>;

    /// Up to `limit` events after the journal sequence `after`, each with its own sequence.
    ///
    /// # Errors
    ///
    /// Returns a [`Refusal`] when the events cannot be read.
    fn next(&mut self, after: i64, limit: u32) -> Result<Vec<(i64, Envelope)>, Refusal>;

    /// Records that the destination has acknowledged the events up to `seq`.
    ///
    /// # Errors
    ///
    /// Returns a [`Refusal`] when the acknowledgement cannot be recorded.
    fn acknowledge(&mut self, seq: i64) -> Result<(), Refusal>;
}

/// Commits the batches of one adapter instance, in the order they arrive.
pub trait BatchSink: Send {
    /// The North feed of `destination`, when the sink can serve one. `None` serves the live
    /// broadcast only.
    ///
    /// # Errors
    ///
    /// Returns a [`Refusal`] when the destination is not declared.
    fn north_feed(&mut self, destination: &str) -> Result<Option<Box<dyn NorthFeed>>, Refusal> {
        let _ = destination;
        Ok(None)
    }

    /// Commits `batch`. Returns `Ok` only when the batch is durable.
    ///
    /// # Errors
    ///
    /// Returns [`Durability::Durable`] when the batch was journaled, and
    /// [`Durability::NotJournaled`] when it held only class C events.
    ///
    /// # Errors
    ///
    /// Returns a [`Refusal`] when the batch cannot be committed; nothing of the
    /// batch is then visible to the core.
    fn commit(&mut self, batch: &EventBatch) -> Result<Durability, Refusal>;

    /// Records that a session was lost or is back. Returns `Ok` only when the
    /// change is committed.
    ///
    /// # Errors
    ///
    /// Returns a [`Refusal`] when the change cannot be committed.
    fn communication(&mut self, state: &CommunicationState) -> Result<(), Refusal>;

    /// The current state of the points of `session_id` that hold a value.
    ///
    /// # Errors
    ///
    /// Returns a [`Refusal`] when the session is unknown to the core.
    fn snapshot(&mut self, session_id: &str) -> Result<Vec<Envelope>, Refusal>;

    /// A receiver of the events committed from now on. Events committed before
    /// the call are not delivered to it.
    fn subscribe(&mut self) -> broadcast::Receiver<Envelope>;
}
