//! The common part of the North producers of Powermug (SPECIFICATIONS 7.4, FUN-SAF-002 to 005).
//!
//! A North producer is a separate executable: each one links its own client library (librdkafka for
//! Kafka, a pure Rust client for MQTT 5), so the libraries never meet in the core (PM-PROC-001). This
//! crate holds what they share: the subscription to a flux destination of the core, the delivery loop
//! that acknowledges a batch only once the destination has acknowledged every record, and the mapping
//! of an envelope to a record.

pub mod record;
pub mod run;
pub mod sink;

pub use record::{record_of, Record};
pub use run::{capabilities, offer, run, Report, RetryPolicy, RunError, Subscription};
pub use sink::{DeliveryError, RecordSink};
