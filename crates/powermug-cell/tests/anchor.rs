//! The audit head is anchored outside the critical store (NFR-SEC-008): the anchor is a JSON
//! object with the head of the chain, written at start and at stop, and the configuration keeps
//! it out of the store's directory.

use std::time::Duration;

use powermug_cell::anchor::{run, write_anchor};
use powermug_cell::config::{CellConfig, ConfigError};
use powermug_command::audit::{to_hex, GENESIS};
use powermug_command::{CommandWriter, Granularity, NewCommand, State};
use tokio::sync::watch;

fn command(id: &str) -> NewCommand {
    NewCommand {
        command_id: id.into(),
        identity: "scada-demo".into(),
        identity_level: Granularity::Client,
        point: "S3/TC_101".into(),
        operation: "direct_execute".into(),
        value: "true".into(),
        expires_ns: 10_000_000_000,
        expected_revision: "r1".into(),
        owner_epoch: 1,
        ha_term: 1,
    }
}

#[test]
fn an_anchor_is_a_json_object_with_the_head_and_its_time() {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("head.json");
    let head = [7_u8; 32];

    write_anchor(&path, &head, 42).expect("writes");

    let anchor: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&path).expect("reads")).expect("json");
    assert_eq!(anchor["head"], to_hex(&head));
    assert_eq!(anchor["at_ns"], 42);
    assert!(
        !dir.path().join("head.json.tmp").exists(),
        "the temporary file is renamed"
    );
}

#[tokio::test]
async fn the_head_is_anchored_at_start_and_at_stop_with_the_chain_at_that_time() {
    let dir = tempfile::tempdir().expect("temp dir");
    let store = dir.path().join("critical.db");
    let (client, writer) = CommandWriter::spawn(&store).expect("spawns");
    let path = dir.path().join("anchor").join("head.json");
    std::fs::create_dir(path.parent().expect("parent")).expect("creates the anchor directory");

    client.submit(command("c1"), 1_000).expect("submits");
    client
        .move_to("c1", State::Validated, 1_000, "")
        .expect("validated");
    let head = client.audit_head().expect("head");
    assert_ne!(head, GENESIS);

    // The period is far longer than the test: only the start and the stop anchor.
    let (stop, stopped) = watch::channel(false);
    let anchoring = tokio::spawn(run(
        client.clone(),
        path.clone(),
        Duration::from_secs(3600),
        stopped,
    ));
    tokio::time::sleep(Duration::from_millis(100)).await;
    let at_start: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&path).expect("reads")).expect("json");
    assert_eq!(at_start["head"], to_hex(&head), "anchored at start");
    client
        .move_to("c1", State::DurablyAccepted, 2_000, "")
        .expect("accepted");
    let head_at_stop = client.audit_head().expect("head");
    stop.send(true).expect("signals");
    anchoring.await.expect("anchoring ends");

    let anchor: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&path).expect("reads")).expect("json");
    assert_eq!(anchor["head"], to_hex(&head_at_stop));

    drop(client);
    writer.join();
}

const CONFIG: &str = r#"{
  "cell": "CELL_DC1",
  "telemetry_db": "telemetry.db",
  "critical_db": "critical.db",
  "management": {
    "listen": "127.0.0.1:8443",
    "certificate": "server.pem",
    "key": "server.key",
    "client_ca": "ca.pem",
    "trusted_key": "trusted.pub.pem"
  },
  "lanes": [{"site": "S3", "revision": 1, "points": [
    {"id": "S3/TC_101", "session": 1, "address": 5, "class": "A"}]}],
  "commands": {"acl": []}
}"#;

fn with_anchor(anchor: &str) -> String {
    CONFIG.replace(
        "\"commands\": {\"acl\": []}",
        &format!("\"commands\": {{\"audit_anchor\": {anchor}, \"acl\": []}}"),
    )
}

#[test]
fn an_anchor_outside_the_store_directory_is_accepted_with_a_default_period() {
    let config = CellConfig::from_json(&with_anchor(
        r#"{"path": "/var/lib/powermug/anchor/head.json"}"#,
    ))
    .expect("valid");
    let anchor = config.commands.audit_anchor.expect("configured");
    assert_eq!(anchor.every_s, 60);
}

#[test]
fn an_anchor_is_refused_when_relative_unperiodic_or_beside_the_store() {
    let relative = CellConfig::from_json(&with_anchor(r#"{"path": "head.json"}"#));
    assert!(matches!(relative, Err(ConfigError::BadAuditAnchor(_))));

    let no_period = CellConfig::from_json(&with_anchor(
        r#"{"path": "/var/lib/powermug/anchor/head.json", "every_s": 0}"#,
    ));
    assert!(matches!(no_period, Err(ConfigError::BadAuditAnchor(_))));

    let beside = CellConfig::from_json(
        &with_anchor(r#"{"path": "/var/lib/powermug/head.json"}"#).replace(
            "\"critical_db\": \"critical.db\"",
            "\"critical_db\": \"/var/lib/powermug/critical.db\"",
        ),
    );
    assert!(matches!(beside, Err(ConfigError::BadAuditAnchor(_))));
}
