//! The telemetry and the critical stores on separate volumes (PM-TEL-006): on a shared filesystem
//! the cell refuses to open unless the operator declares the quotas.

use std::os::unix::fs::MetadataExt;

use powermug_cell::config::CellConfig;
use powermug_cell::{Cell, CellError};

fn config(
    telemetry: &std::path::Path,
    critical: &std::path::Path,
    shared_quota: bool,
) -> CellConfig {
    let text = serde_json::json!({
        "cell": "CELL_DC1",
        "telemetry_db": telemetry,
        "critical_db": critical,
        "management": {
            "listen": "127.0.0.1:0",
            "certificate": "server.pem",
            "key": "server.key",
            "client_ca": "ca.pem",
            "trusted_key": "trusted.pub.pem"
        },
        "lanes": [{"site": "S3", "revision": 1, "points": [{"id": "S3/TM_001", "session": 1, "address": 1, "class": "A"}]}],
        "storage": {"shared_quota": shared_quota}
    });
    CellConfig::from_json(&text.to_string()).expect("valid")
}

fn trusted_key() -> ed25519_dalek::VerifyingKey {
    ed25519_dalek::SigningKey::from_bytes(&[7; 32]).verifying_key()
}

#[test]
fn stores_on_a_shared_filesystem_are_refused_without_a_declared_quota() {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = config(
        &dir.path().join("telemetry.db"),
        &dir.path().join("critical.db"),
        false,
    );
    assert!(matches!(
        Cell::open(&config, trusted_key()),
        Err(CellError::SharedVolume)
    ));
}

#[test]
fn stores_on_a_shared_filesystem_open_once_the_quota_is_declared() {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = config(
        &dir.path().join("telemetry.db"),
        &dir.path().join("critical.db"),
        true,
    );
    let cell = Cell::open(&config, trusted_key()).expect("opens");
    cell.close();
}

#[test]
fn stores_on_different_filesystems_open_without_a_declaration() {
    let shared = std::path::Path::new("/dev/shm");
    if !shared.is_dir() {
        eprintln!("skipped: no /dev/shm to place a second filesystem");
        return;
    }
    let dir = tempfile::tempdir().expect("temp dir");
    let other = tempfile::tempdir_in(shared).expect("temp dir on /dev/shm");
    let same = std::fs::metadata(dir.path()).expect("metadata").dev()
        == std::fs::metadata(other.path()).expect("metadata").dev();
    if same {
        eprintln!("skipped: /dev/shm is on the same filesystem as the temporary directory");
        return;
    }
    let config = config(
        &other.path().join("telemetry.db"),
        &dir.path().join("critical.db"),
        false,
    );
    let cell = Cell::open(&config, trusted_key()).expect("opens");
    cell.close();
}
