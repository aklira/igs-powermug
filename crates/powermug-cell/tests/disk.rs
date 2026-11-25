//! The disk indicators (catalogue: `disk_free_percent` for the telemetry volume,
//! `cell_disk_free_percent` for the critical volume): the free share of each store's filesystem,
//! in percent, measured when the cell is opened.

use powermug_cell::config::CellConfig;
use powermug_cell::Cell;

fn trusted_key() -> ed25519_dalek::VerifyingKey {
    ed25519_dalek::SigningKey::from_bytes(&[7; 32]).verifying_key()
}

#[test]
fn the_free_share_of_each_store_volume_is_measured_in_percent() {
    let dir = tempfile::tempdir().expect("temp dir");
    let text = serde_json::json!({
        "cell": "CELL_DC1",
        "telemetry_db": dir.path().join("telemetry.db"),
        "critical_db": dir.path().join("critical.db"),
        "management": {
            "listen": "127.0.0.1:0",
            "certificate": "server.pem",
            "key": "server.key",
            "client_ca": "ca.pem",
            "trusted_key": "trusted.pub.pem"
        },
        "lanes": [{"site": "S3", "revision": 1, "points": [
            {"id": "S3/TM_001", "session": 1, "address": 1, "class": "A"}]}],
        "storage": {"shared_quota": true}
    });
    let config = CellConfig::from_json(&text.to_string()).expect("valid");
    let cell = Cell::open(&config, trusted_key()).expect("opens");
    let now = 1_000_000_000_000_i64;
    let indicators = cell.api.indicator_source().sample(now);
    for name in ["disk_free_percent", "cell_disk_free_percent"] {
        let value = indicators
            .iter()
            .find(|indicator| indicator.name == name)
            .and_then(|indicator| indicator.value)
            .unwrap_or_else(|| panic!("{name} is measured"));
        assert!((0.0..=100.0).contains(&value), "{name} is {value}");
    }
    cell.close();
}
