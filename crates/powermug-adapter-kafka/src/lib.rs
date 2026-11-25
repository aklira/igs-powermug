//! North Kafka producer of Powermug (SPECIFICATIONS 7.4, FUN-SAF-002 to 005, ADR-18).
//!
//! The producer is a separate executable, because librdkafka is C code: it is the subscriber of a
//! flux destination of the core. It produces each delivered batch to a topic, and acknowledges the
//! batch once the broker has acknowledged every record. The core moves the destination's cursor only
//! over acknowledged batches, so a producer that stops resumes where the broker last confirmed it.

pub mod record;
pub mod run;
pub mod settings;
pub mod sink;

pub use record::{record_of, Record};
pub use run::{capabilities, offer, run, Report, RetryPolicy, RunError};
pub use settings::{Settings, SettingsError};
pub use sink::{DeliveryError, KafkaSink, RecordSink};

/// The producer could not start or stop cleanly.
#[derive(Debug, thiserror::Error)]
pub enum ProducerError {
    #[error(transparent)]
    Settings(#[from] SettingsError),
    #[error("the Kafka producer cannot be created: {0}")]
    Kafka(#[from] rdkafka::error::KafkaError),
    #[error(transparent)]
    Run(#[from] RunError),
    #[error("the runtime cannot be started: {0}")]
    Runtime(#[from] std::io::Error),
}
