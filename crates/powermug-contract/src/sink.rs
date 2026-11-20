//! Where the core puts a batch it accepted (PM-TEL-002).
//!
//! The contract does not know the journal. The core implements [`BatchSink`]:
//! `commit` returns `Ok` only once the batch is durable, and the service sends
//! `DURABLE` only then.

use crate::proto::v1::EventBatch;
use powermug_pivot::proto::v1::CommunicationState;

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

/// Commits the batches of one adapter instance, in the order they arrive.
pub trait BatchSink: Send {
    /// Commits `batch`. Returns `Ok` only when the batch is durable.
    ///
    /// # Errors
    ///
    /// Returns a [`Refusal`] when the batch cannot be committed; nothing of the
    /// batch is then visible to the core.
    fn commit(&mut self, batch: &EventBatch) -> Result<(), Refusal>;

    /// Records that a session was lost or is back. Returns `Ok` only when the
    /// change is committed.
    ///
    /// # Errors
    ///
    /// Returns a [`Refusal`] when the change cannot be committed.
    fn communication(&mut self, state: &CommunicationState) -> Result<(), Refusal>;
}
