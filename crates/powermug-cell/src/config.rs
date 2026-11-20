//! The cell's configuration file (JSON). Unknown members are refused.
//!
//! The file names the telemetry log and the critical store, the management interface
//! with its TLS material and its trusted key, and one lane per partition with its
//! points. A South session belongs to one lane only, so that the indicators of the
//! cell name each session once. The optional `adapters` section names the South
//! instances the cell starts: one per session, each in a sandbox.

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use powermug_core::lane::{PointSpec, PointTable, SessionId};
use powermug_store::EventClass;
use serde::Deserialize;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CellConfig {
    pub cell: String,
    pub telemetry_db: PathBuf,
    pub critical_db: PathBuf,
    pub management: ManagementConfig,
    pub lanes: Vec<LaneConfig>,
    #[serde(default)]
    pub adapters: Option<AdaptersConfig>,
}

/// The South instances the cell starts (§6.1, §6.2).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdaptersConfig {
    /// The `powermug` binary: its `spawner` role sandboxes each instance, its `adapter`
    /// role runs it. Absolute path.
    pub binary: PathBuf,
    /// Directory of the instances. Each keeps its settings and its socket in a
    /// subdirectory named after its session. Absolute path.
    pub directory: PathBuf,
    pub instances: Vec<InstanceConfig>,
}

/// One South instance. Its points are the points of its session in the lanes.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstanceConfig {
    /// The session the instance serves. It must be the session of points of a lane.
    pub session: u32,
    /// The protocol: only `iec104` exists.
    pub kind: String,
    /// The controlled stations of the redundancy group, in priority order.
    pub stations: Vec<SocketAddr>,
    /// The common address of the stations; 0 is not an address.
    pub common_address: u16,
    /// Credits of the stream (§6.5, default 4,096 events).
    #[serde(default = "default_window_events")]
    pub window_events: u32,
}

/// The credit window of the stream when the configuration does not set one (§6.5).
pub const DEFAULT_WINDOW_EVENTS: u32 = 4096;

fn default_window_events() -> u32 {
    DEFAULT_WINDOW_EVENTS
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagementConfig {
    pub listen: SocketAddr,
    /// Certificate chain of the server, PEM.
    pub certificate: PathBuf,
    /// Private key of the server, PEM.
    pub key: PathBuf,
    /// The CA that issues the client certificates, PEM.
    pub client_ca: PathBuf,
    /// The public key that signed plans must verify with, PEM.
    pub trusted_key: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LaneConfig {
    pub site: String,
    /// The partition revision the points come from.
    pub revision: u32,
    pub points: Vec<PointConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PointConfig {
    /// Prefixed by the site, for example `S3/TM_001`.
    pub id: String,
    pub session: u32,
    pub address: u32,
    /// `A`, `B`, `C` or `Critical`.
    pub class: String,
}

/// The configuration is not usable.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ConfigError {
    #[error("the configuration is not valid JSON or has an unknown member: {0}")]
    Syntax(String),
    #[error("the cell has no name")]
    NoCellName,
    #[error("the cell has no lane")]
    NoLane,
    #[error("lane {0} has no point")]
    NoPoint(String),
    #[error("point {point} has class {class}, which is not A, B, C or Critical")]
    UnknownClass { point: String, class: String },
    #[error(
        "session {session} belongs to lanes {first} and {second}; a session belongs to one lane"
    )]
    SessionInTwoLanes {
        session: u32,
        first: String,
        second: String,
    },
    #[error("point {0} is listed twice")]
    DuplicatePoint(String),
    #[error("adapter for session {0} names a session with no point in any lane")]
    AdapterWithoutLane(u32),
    #[error("session {0} has two adapter instances")]
    DuplicateAdapter(u32),
    #[error("adapter of session {session} has kind {kind}, and only iec104 exists")]
    UnknownAdapterKind { session: u32, kind: String },
    #[error("adapter of session {0} has no station")]
    AdapterWithoutStation(u32),
    #[error("adapter of session {0} has common address 0, which is not an address")]
    ZeroCommonAddress(u32),
    #[error("adapter of session {session} has window {window}; it must be between 1 and {DEFAULT_WINDOW_EVENTS}")]
    BadWindow { session: u32, window: u32 },
    #[error("{0} must be an absolute path")]
    RelativePath(&'static str),
    #[error("session {session} has two points at object address {address}")]
    DuplicateAddress { session: u32, address: u32 },
}

impl CellConfig {
    /// Reads and checks a configuration.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] for a file that is not JSON of this shape, or that
    /// breaks one of the rules checked here.
    pub fn from_json(text: &str) -> Result<Self, ConfigError> {
        let config: Self =
            serde_json::from_str(text).map_err(|err| ConfigError::Syntax(err.to_string()))?;
        config.check()?;
        Ok(config)
    }

    fn check(&self) -> Result<(), ConfigError> {
        if self.cell.trim().is_empty() {
            return Err(ConfigError::NoCellName);
        }
        if self.lanes.is_empty() {
            return Err(ConfigError::NoLane);
        }
        let mut lane_of_session: HashMap<u32, &str> = HashMap::new();
        for lane in &self.lanes {
            if lane.points.is_empty() {
                return Err(ConfigError::NoPoint(lane.site.clone()));
            }
            let mut ids = std::collections::HashSet::new();
            for point in &lane.points {
                parse_class(&point.class).ok_or_else(|| ConfigError::UnknownClass {
                    point: point.id.clone(),
                    class: point.class.clone(),
                })?;
                if !ids.insert(point.id.clone()) {
                    return Err(ConfigError::DuplicatePoint(point.id.clone()));
                }
                match lane_of_session.get(&point.session) {
                    Some(first) if *first != lane.site => {
                        return Err(ConfigError::SessionInTwoLanes {
                            session: point.session,
                            first: (*first).to_owned(),
                            second: lane.site.clone(),
                        })
                    }
                    _ => {
                        lane_of_session.insert(point.session, &lane.site);
                    }
                }
            }
        }
        if let Some(adapters) = &self.adapters {
            check_adapters(adapters, &self.lanes, &lane_of_session)?;
        }
        Ok(())
    }
}

fn check_adapters(
    adapters: &AdaptersConfig,
    lanes: &[LaneConfig],
    lane_of_session: &HashMap<u32, &str>,
) -> Result<(), ConfigError> {
    if !adapters.binary.is_absolute() {
        return Err(ConfigError::RelativePath("adapters.binary"));
    }
    if !adapters.directory.is_absolute() {
        return Err(ConfigError::RelativePath("adapters.directory"));
    }
    let mut seen = HashSet::new();
    for instance in &adapters.instances {
        if !seen.insert(instance.session) {
            return Err(ConfigError::DuplicateAdapter(instance.session));
        }
        if instance.kind != "iec104" {
            return Err(ConfigError::UnknownAdapterKind {
                session: instance.session,
                kind: instance.kind.clone(),
            });
        }
        if !lane_of_session.contains_key(&instance.session) {
            return Err(ConfigError::AdapterWithoutLane(instance.session));
        }
        if instance.stations.is_empty() {
            return Err(ConfigError::AdapterWithoutStation(instance.session));
        }
        if instance.common_address == 0 {
            return Err(ConfigError::ZeroCommonAddress(instance.session));
        }
        if instance.window_events == 0 || instance.window_events > DEFAULT_WINDOW_EVENTS {
            return Err(ConfigError::BadWindow {
                session: instance.session,
                window: instance.window_events,
            });
        }
        // The adapter maps objects by address: two points of its session may not share one.
        let mut addresses = HashSet::new();
        for point in lanes.iter().flat_map(|lane| &lane.points) {
            if point.session == instance.session && !addresses.insert(point.address) {
                return Err(ConfigError::DuplicateAddress {
                    session: instance.session,
                    address: point.address,
                });
            }
        }
    }
    Ok(())
}

/// The directory of the instance of `session` under the adapters directory.
#[must_use]
pub fn instance_directory(adapters: &AdaptersConfig, session: u32) -> PathBuf {
    Path::new(&adapters.directory).join(session.to_string())
}

/// The class of a configured point, if it names one.
#[must_use]
pub fn parse_class(name: &str) -> Option<EventClass> {
    match name {
        "A" => Some(EventClass::A),
        "B" => Some(EventClass::B),
        "C" => Some(EventClass::C),
        "Critical" => Some(EventClass::Critical),
        _ => None,
    }
}

impl LaneConfig {
    /// The point table of this lane.
    ///
    /// # Errors
    ///
    /// Returns the table error when the points cannot form a table (a duplicate
    /// identifier, or points of two sites).
    pub fn table(&self) -> Result<PointTable, powermug_core::lane::TableError> {
        let specs = self
            .points
            .iter()
            .map(|point| PointSpec {
                site_id: self.site.clone(),
                point_id: point.id.clone(),
                class: parse_class(&point.class).unwrap_or(EventClass::A),
                session: Some(SessionId(point.session)),
                supervision: None,
            })
            .collect();
        PointTable::new(self.revision, specs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text(lanes: &str) -> String {
        format!(
            r#"{{
  "cell": "CELL_DC1",
  "telemetry_db": "telemetry.db",
  "critical_db": "critical.db",
  "management": {{
    "listen": "127.0.0.1:8443",
    "certificate": "server.pem",
    "key": "server.key",
    "client_ca": "ca.pem",
    "trusted_key": "trusted.pub.pem"
  }},
  "lanes": [{lanes}]
}}"#
        )
    }

    const LANE_S3: &str = r#"{"site": "S3", "revision": 1, "points": [
        {"id": "S3/TM_001", "session": 1, "address": 1, "class": "A"},
        {"id": "S3/TM_002", "session": 1, "address": 2, "class": "B"}]}"#;

    #[test]
    fn a_valid_configuration_is_read() {
        let config = CellConfig::from_json(&text(LANE_S3)).expect("valid");
        assert_eq!(config.cell, "CELL_DC1");
        assert_eq!(config.lanes[0].table().expect("table").len(), 2);
    }

    #[test]
    fn an_unknown_member_is_refused() {
        let broken = text(LANE_S3).replace(
            "\"cell\": \"CELL_DC1\",",
            "\"cell\": \"CELL_DC1\", \"extra\": 1,",
        );
        assert!(matches!(
            CellConfig::from_json(&broken),
            Err(ConfigError::Syntax(_))
        ));
    }

    #[test]
    fn an_unknown_class_is_refused() {
        let lane = LANE_S3.replace("\"class\": \"B\"", "\"class\": \"Z\"");
        assert_eq!(
            CellConfig::from_json(&text(&lane)),
            Err(ConfigError::UnknownClass {
                point: "S3/TM_002".into(),
                class: "Z".into()
            })
        );
    }

    #[test]
    fn a_session_in_two_lanes_is_refused() {
        let other = r#"{"site": "S4", "revision": 1, "points": [
            {"id": "S4/TM_001", "session": 1, "address": 1, "class": "A"}]}"#;
        let lanes = format!("{LANE_S3}, {other}");
        assert!(matches!(
            CellConfig::from_json(&text(&lanes)),
            Err(ConfigError::SessionInTwoLanes { session: 1, .. })
        ));
    }

    #[test]
    fn a_lane_without_points_and_a_cell_without_lanes_are_refused() {
        let empty = r#"{"site": "S3", "revision": 1, "points": []}"#;
        assert_eq!(
            CellConfig::from_json(&text(empty)),
            Err(ConfigError::NoPoint("S3".into()))
        );
        assert_eq!(CellConfig::from_json(&text("")), Err(ConfigError::NoLane));
    }

    fn with_adapters(instance: &str) -> String {
        text(LANE_S3).replacen(
            "\n}",
            &format!(
                ",\n  \"adapters\": {{\"binary\": \"/usr/bin/powermug\", \"directory\": \"/run/powermug/adapters\", \"instances\": [{instance}]}}\n}}"
            ),
            1,
        )
    }

    const GOOD_INSTANCE: &str =
        r#"{"session": 1, "kind": "iec104", "stations": ["127.0.0.1:2404"], "common_address": 1}"#;

    #[test]
    fn a_valid_adapter_instance_is_read_with_the_default_window() {
        let config = CellConfig::from_json(&with_adapters(GOOD_INSTANCE)).expect("valid");
        let adapters = config.adapters.expect("section");
        assert_eq!(adapters.instances[0].window_events, DEFAULT_WINDOW_EVENTS);
    }

    #[test]
    fn an_adapter_is_checked_against_the_lanes_and_its_own_fields() {
        let cases = [
            (
                r#"{"session": 9, "kind": "iec104", "stations": ["127.0.0.1:2404"], "common_address": 1}"#,
                ConfigError::AdapterWithoutLane(9),
            ),
            (
                r#"{"session": 1, "kind": "dnp3", "stations": ["127.0.0.1:2404"], "common_address": 1}"#,
                ConfigError::UnknownAdapterKind {
                    session: 1,
                    kind: "dnp3".into(),
                },
            ),
            (
                r#"{"session": 1, "kind": "iec104", "stations": [], "common_address": 1}"#,
                ConfigError::AdapterWithoutStation(1),
            ),
            (
                r#"{"session": 1, "kind": "iec104", "stations": ["127.0.0.1:2404"], "common_address": 0}"#,
                ConfigError::ZeroCommonAddress(1),
            ),
            (
                r#"{"session": 1, "kind": "iec104", "stations": ["127.0.0.1:2404"], "common_address": 1, "window_events": 5000}"#,
                ConfigError::BadWindow {
                    session: 1,
                    window: 5000,
                },
            ),
        ];
        for (instance, expected) in cases {
            assert_eq!(
                CellConfig::from_json(&with_adapters(instance)),
                Err(expected),
                "{instance}"
            );
        }
    }

    #[test]
    fn two_adapters_for_one_session_and_relative_paths_are_refused() {
        let twice = format!("{GOOD_INSTANCE}, {GOOD_INSTANCE}");
        assert_eq!(
            CellConfig::from_json(&with_adapters(&twice)),
            Err(ConfigError::DuplicateAdapter(1))
        );
        let relative = with_adapters(GOOD_INSTANCE).replace("/usr/bin/powermug", "powermug");
        assert_eq!(
            CellConfig::from_json(&relative),
            Err(ConfigError::RelativePath("adapters.binary"))
        );
    }

    #[test]
    fn a_point_listed_twice_is_refused() {
        let lane = r#"{"site": "S3", "revision": 1, "points": [
            {"id": "S3/TM_001", "session": 1, "address": 1, "class": "A"},
            {"id": "S3/TM_001", "session": 1, "address": 2, "class": "A"}]}"#;
        assert_eq!(
            CellConfig::from_json(&text(lane)),
            Err(ConfigError::DuplicatePoint("S3/TM_001".into()))
        );
    }
}
