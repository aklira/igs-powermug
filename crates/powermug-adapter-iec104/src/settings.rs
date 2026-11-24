//! The settings of one South instance, written by the cell and read by the adapter at start.
//!
//! The cell derives them from its configuration: the stations of the redundancy group, the
//! common address, the socket of the instance, and the points of the session with their
//! object addresses. The file is closed: unknown members are refused, as in the cell's own
//! configuration.

use std::net::SocketAddr;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::mapping::{DuplicateObject, Mapping};
use crate::run::Options;

/// What the adapter needs to run one session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Settings {
    /// The controlled stations of the redundancy group, in priority order.
    pub stations: Vec<SocketAddr>,
    /// The common address of the stations.
    pub common_address: u16,
    /// The socket the core serves this instance on.
    pub core_socket: PathBuf,
    /// The site of the partition, prefix of the point identifiers.
    pub site_id: String,
    /// The session of the core.
    pub session_id: u32,
    /// The partition revision the points come from.
    pub revision: u32,
    /// The points of the session, by object address.
    pub points: Vec<MappedPoint>,
    /// SHA-256 of the adapter artifact, lowercase hex.
    pub artifact_sha256: String,
    /// Credits of the stream: events in flight before acknowledgements.
    pub window_events: u32,
    /// The owner epoch the session was activated at (PM-CMD-005).
    #[serde(default)]
    pub owner_epoch: u64,
}

/// One point of the session: its object address on the station, and its identifier.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MappedPoint {
    pub address: u32,
    pub point: String,
}

/// The settings are not usable.
#[derive(Debug, Error)]
pub enum SettingsError {
    #[error("the settings are not valid JSON of this shape: {0}")]
    Syntax(String),
    #[error(transparent)]
    Mapping(#[from] DuplicateObject),
}

impl Settings {
    /// Reads settings from their JSON text.
    ///
    /// # Errors
    ///
    /// Returns [`SettingsError::Syntax`] for text that is not JSON of this shape.
    pub fn from_json(text: &str) -> Result<Self, SettingsError> {
        serde_json::from_str(text).map_err(|err| SettingsError::Syntax(err.to_string()))
    }

    /// The adapter options of these settings.
    ///
    /// # Errors
    ///
    /// Returns [`SettingsError::Mapping`] when two points share an object address.
    pub fn into_options(self) -> Result<Options, SettingsError> {
        let common_address = self.common_address;
        let mapping = Mapping::new(
            self.points
                .into_iter()
                .map(|point| (common_address, point.address, point.point)),
        )?;
        Ok(Options {
            stations: self.stations,
            common_address,
            core_socket: self.core_socket,
            site_id: self.site_id,
            session_id: self.session_id,
            revision: self.revision,
            mapping,
            artifact_sha256: self.artifact_sha256,
            window_events: self.window_events,
            owner_epoch: self.owner_epoch,
        })
    }
}
