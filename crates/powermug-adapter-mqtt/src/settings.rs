//! The settings of one North MQTT 5 instance, written by the cell and read by the client at start.

use std::path::{Path, PathBuf};

use powermug_north::Subscription;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::PROTOCOL;

/// What the client needs to serve one flux destination.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Settings {
    /// The socket the core serves this instance on.
    pub core_socket: PathBuf,
    /// The flux destination: the name the cell configures, whose cursor the core keeps.
    pub destination: String,
    /// The host name or address of the MQTT 5 broker.
    pub broker_host: String,
    /// The port of the broker, 1883 for plain TCP by convention.
    pub broker_port: u16,
    /// The client identifier presented to the broker.
    pub client_id: String,
    /// The topic prefix: the record of point `S3/TM_001` goes to `<prefix>/S3/TM_001`.
    pub topic_prefix: String,
    /// SHA-256 of the client artifact, lowercase hex.
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
    #[error("the setting {0} is not valid: {1}")]
    Invalid(&'static str, &'static str),
}

impl Settings {
    /// Reads and checks the settings file.
    ///
    /// # Errors
    ///
    /// Returns [`SettingsError`] when the file cannot be read, is not the expected shape, or leaves a
    /// setting empty or invalid.
    pub fn load(path: &Path) -> Result<Self, SettingsError> {
        Self::parse(&std::fs::read_to_string(path)?)
    }

    /// Parses and checks the settings.
    ///
    /// # Errors
    ///
    /// Returns [`SettingsError`] when the text is not the expected shape, or a setting is empty or
    /// invalid.
    pub fn parse(text: &str) -> Result<Self, SettingsError> {
        let settings: Self = serde_json::from_str(text)?;
        for (name, value) in [
            ("destination", &settings.destination),
            ("broker_host", &settings.broker_host),
            ("client_id", &settings.client_id),
            ("topic_prefix", &settings.topic_prefix),
            ("artifact_sha256", &settings.artifact_sha256),
        ] {
            if value.trim().is_empty() {
                return Err(SettingsError::Empty(name));
            }
        }
        if settings.broker_port == 0 {
            return Err(SettingsError::Invalid(
                "broker_port",
                "port 0 cannot be dialled",
            ));
        }
        if settings.topic_prefix.contains(['+', '#', '\0']) {
            return Err(SettingsError::Invalid(
                "topic_prefix",
                "a publication topic cannot hold a wildcard",
            ));
        }
        Ok(settings)
    }

    /// The subscription the client makes to the core, from these settings.
    #[must_use]
    pub fn subscription(&self) -> Subscription {
        Subscription {
            core_socket: self.core_socket.clone(),
            destination: self.destination.clone(),
            protocol: PROTOCOL,
            artifact_sha256: self.artifact_sha256.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text(prefix: &str, port: u16) -> String {
        format!(
            r#"{{"core_socket":"/run/core.sock","destination":"mqtt-dc1","broker_host":"broker","broker_port":{port},"client_id":"powermug","topic_prefix":"{prefix}","artifact_sha256":"{}"}}"#,
            "ab".repeat(32)
        )
    }

    #[test]
    fn valid_settings_make_the_subscription() {
        let settings = Settings::parse(&text("powermug/dc1", 1883)).expect("parses");
        let subscription = settings.subscription();
        assert_eq!(subscription.destination, "mqtt-dc1");
        assert_eq!(subscription.protocol, "mqtt5");
    }

    #[test]
    fn a_wildcard_in_the_prefix_is_refused() {
        assert!(matches!(
            Settings::parse(&text("powermug/#", 1883)),
            Err(SettingsError::Invalid("topic_prefix", _))
        ));
    }

    #[test]
    fn port_zero_is_refused() {
        assert!(matches!(
            Settings::parse(&text("powermug", 0)),
            Err(SettingsError::Invalid("broker_port", _))
        ));
    }
}
