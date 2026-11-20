//! Durable stores of Powermug (SPECIFICATIONS §7.2).
//!
//! This crate holds the telemetry log and the critical store: the revisions and
//! the generation pointer. The cursors and the audit records are not written yet.

mod generation;
mod journal;
mod log;
mod writer;

pub use generation::{CriticalStore, GenerationError, Outcome, RevisionStatus, ScopeState};
pub use journal::{Journal, OwnedEvent};
pub use log::{Committed, EventClass, NewEvent, Snapshot, StoreError, StoredEvent, TelemetryLog};
pub use writer::{GroupJournal, Writer, GROUP_MAX_EVENTS, GROUP_WINDOW};
