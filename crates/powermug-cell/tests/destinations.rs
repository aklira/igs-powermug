//! The flux destinations of the North side (SPECIFICATIONS 10.3, PM-TEL-004, FUN-SAF-004): each
//! declares its starting point, its name is unique, and the cell declares its cursor at opening.

use powermug_cell::config::{CellConfig, ConfigError};
use powermug_cell::Cell;
use powermug_store::TelemetryLog;

fn text(dir: &std::path::Path, destinations: &serde_json::Value) -> String {
    serde_json::json!({
        "cell": "CELL_DC1",
        "telemetry_db": dir.join("telemetry.db"),
        "critical_db": dir.join("critical.db"),
        "management": {
            "listen": "127.0.0.1:0",
            "certificate": "server.pem",
            "key": "server.key",
            "client_ca": "ca.pem",
            "trusted_key": "trusted.pub.pem"
        },
        "lanes": [{"site": "S3", "revision": 1, "points": [
            {"id": "S3/TM_001", "session": 1, "address": 1, "class": "A"}]}],
        "storage": {"shared_quota": true},
        "destinations": destinations.clone()
    })
    .to_string()
}

fn trusted_key() -> ed25519_dalek::VerifyingKey {
    ed25519_dalek::SigningKey::from_bytes(&[7; 32]).verifying_key()
}

#[test]
fn a_destination_names_its_starting_point_and_the_names_are_unique() {
    let dir = tempfile::tempdir().expect("temp dir");
    let valid = text(
        dir.path(),
        &serde_json::json!([
            {"name": "kafka-prod", "start": "oldest_retained"},
            {"name": "mqtt-lab", "start": "now"}
        ]),
    );
    assert!(CellConfig::from_json(&valid).is_ok());

    let missing = text(dir.path(), &serde_json::json!([{"name": "kafka-prod"}]));
    assert!(matches!(
        CellConfig::from_json(&missing),
        Err(ConfigError::Syntax(_))
    ));

    let unknown = text(
        dir.path(),
        &serde_json::json!([{"name": "kafka-prod", "start": "timestamp"}]),
    );
    assert!(matches!(
        CellConfig::from_json(&unknown),
        Err(ConfigError::Syntax(_))
    ));

    let twice = text(
        dir.path(),
        &serde_json::json!([
            {"name": "kafka-prod", "start": "now"},
            {"name": "kafka-prod", "start": "oldest_retained"}
        ]),
    );
    assert!(matches!(
        CellConfig::from_json(&twice),
        Err(ConfigError::BadDestination(_))
    ));
}

#[test]
fn opening_the_cell_declares_the_cursor_of_each_destination() {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = CellConfig::from_json(&text(
        dir.path(),
        &serde_json::json!([{"name": "kafka-prod", "start": "oldest_retained"}]),
    ))
    .expect("valid");
    let cell = Cell::open(&config, trusted_key()).expect("opens");
    cell.close();

    let log = TelemetryLog::open(dir.path().join("telemetry.db")).expect("opens");
    let cursor = log.cursor("kafka-prod").expect("reads").expect("declared");
    assert!(cursor.flux, "a destination is a flux destination");
    assert_eq!(cursor.last_seq, 0, "nothing acknowledged yet");
}
