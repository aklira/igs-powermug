//! Durable stores of Powermug (SPECIFICATIONS §7.2).
//!
//! This crate holds the telemetry log, with its destination cursors and retention, and the
//! critical store: the revisions and the generation pointer. The audit records are not
//! written yet.

mod cursors;
mod generation;
mod journal;
mod log;
mod writer;

pub use cursors::{Cursor, Enforced, Policy, Replay, Start};
pub use generation::{CriticalStore, GenerationError, Outcome, RevisionStatus, ScopeState};
pub use journal::{Journal, OwnedEvent};
pub use log::{Committed, EventClass, NewEvent, Snapshot, StoreError, StoredEvent, TelemetryLog};
pub use writer::{GroupJournal, Writer, GROUP_MAX_EVENTS, GROUP_WINDOW};
