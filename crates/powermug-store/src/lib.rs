//! Durable stores of Powermug (SPECIFICATIONS §7.2).
//!
//! This crate holds the telemetry log. The critical store, the snapshots and
//! the cursors are not written yet.

mod log;

pub use log::{Committed, EventClass, NewEvent, StoreError, StoredEvent, TelemetryLog};
