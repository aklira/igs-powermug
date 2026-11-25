//! North Kafka producer of Powermug (SPECIFICATIONS 7.4, ADR-18).
//!
//! The producer is a separate executable, because librdkafka is C code. The subscription, the delivery
//! loop and the record mapping are in `powermug-north`; this crate holds the Kafka sink and its
//! settings.

pub mod settings;
pub mod sink;

pub use settings::{Settings, SettingsError};
pub use sink::KafkaSink;

/// The protocol the producer declares to the core.
pub const PROTOCOL: &str = "kafka";

/// The producer could not start or stop cleanly.
#[derive(Debug, thiserror::Error)]
pub enum ProducerError {
    #[error(transparent)]
    Settings(#[from] SettingsError),
    #[error("the Kafka producer cannot be created: {0}")]
    Kafka(#[from] rdkafka::error::KafkaError),
    #[error(transparent)]
    Run(#[from] powermug_north::RunError),
    #[error("the runtime cannot be started: {0}")]
    Runtime(#[from] std::io::Error),
}
