//! The settings of one North Kafka instance, written by the cell and read by the producer at start.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// What the producer needs to serve one flux destination.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Settings {
    /// The socket the core serves this instance on.
    pub core_socket: PathBuf,
    /// The flux destination: the name the cell configures, whose cursor the core keeps.
    pub destination: String,
    /// The bootstrap servers, as librdkafka reads them: `host:port,host:port`.
    pub brokers: String,
    /// The topic the events are produced to.
    pub topic: String,
    /// SHA-256 of the producer artifact, lowercase hex.
    pub artifact_sha256: String,
}

/// The settings file is not usable.
#[derive(Debug, Error)]
pub enum SettingsError {
    #[error("the settings file cannot be read: {0}")]
    Read(#[from] std::io::Error),
    #[error("the settings are not valid: {0}")]
    Syntax(#[from] serde_json::Error),
    #[error("the setting {0} is empty")]
    Empty(&'static str),
}

impl Settings {
    /// Reads and checks the settings file.
    ///
    /// # Errors
    ///
    /// Returns [`SettingsError`] when the file cannot be read, is not the expected shape, or
    /// leaves a required setting empty.
    pub fn load(path: &Path) -> Result<Self, SettingsError> {
        let text = std::fs::read_to_string(path)?;
        let settings: Self = serde_json::from_str(&text)?;
        for (name, value) in [
            ("destination", &settings.destination),
            ("brokers", &settings.brokers),
            ("topic", &settings.topic),
            ("artifact_sha256", &settings.artifact_sha256),
        ] {
            if value.trim().is_empty() {
                return Err(SettingsError::Empty(name));
            }
        }
        Ok(settings)
    }
}
