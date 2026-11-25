//! North MQTT 5 client of Powermug (SPECIFICATIONS 7.4).
//!
//! The client is a separate executable, like the Kafka producer, so that its protocol library never
//! links into the core. It subscribes to a flux destination of the core, publishes each delivered
//! record at `QoS` 1, and acknowledges a batch only when the broker has acknowledged every publication.

pub mod settings;
pub mod sink;

pub use settings::{Settings, SettingsError};
pub use sink::MqttSink;

/// The protocol the client declares to the core.
pub const PROTOCOL: &str = "mqtt5";

/// The client could not start or stop cleanly.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error(transparent)]
    Settings(#[from] SettingsError),
    #[error(transparent)]
    Run(#[from] powermug_north::RunError),
    #[error("the runtime cannot be started: {0}")]
    Runtime(#[from] std::io::Error),
}
