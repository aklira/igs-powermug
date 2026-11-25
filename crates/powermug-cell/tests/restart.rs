//! The restart rules run when the cell opens, before any South session is activated (SPECIFICATIONS
//! 9.7): a command left ready by an earlier run is decided, and its outcome is kept by the cell.

use powermug_cell::config::CellConfig;
use powermug_cell::Cell;
use powermug_command::{CommandWriter, Granularity, NewCommand, State};

fn config(dir: &std::path::Path) -> CellConfig {
    let text = format!(
        r#"{{
  "cell": "CELL_DC1",
  "telemetry_db": "{telemetry}",
  "critical_db": "{critical}",
  "management": {{
    "listen": "127.0.0.1:8443",
    "certificate": "server.pem",
    "key": "server.key",
    "client_ca": "ca.pem",
    "trusted_key": "trusted.pub.pem"
  }},
  "lanes": [{{"site": "S3", "revision": 1, "points": [
    {{"id": "S3/TC_101", "session": 1, "address": 5, "class": "A"}}]}}],
  "commands": {{}}
}}"#,
        telemetry = dir.join("telemetry.db").display(),
        critical = dir.join("critical.db").display(),
    );
    CellConfig::from_json(&text).expect("valid")
}

fn trusted_key() -> ed25519_dalek::VerifyingKey {
    ed25519_dalek::SigningKey::from_bytes(&[7; 32]).verifying_key()
}

#[test]
fn a_command_left_ready_by_an_earlier_run_is_decided_when_the_cell_opens() {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = config(dir.path());

    // An earlier run wrote a command, owned at epoch 1, and stopped before it was emitted.
    let (client, writer) = CommandWriter::spawn(&config.critical_db).expect("spawns");
    client
        .submit(
            NewCommand {
                command_id: "c1".into(),
                identity: "scada-demo".into(),
                identity_level: Granularity::Client,
                point: "S3/TC_101".into(),
                operation: "direct_execute".into(),
                value: "true".into(),
                expires_ns: i64::MAX,
                expected_revision: String::new(),
                owner_epoch: 1,
                ha_term: 1,
            },
            1_000,
        )
        .expect("submits");
    for next in [State::Validated, State::DurablyAccepted, State::Ready] {
        client.move_to("c1", next, 1_000, "").expect("moves");
    }
    drop(client);
    writer.join();

    // The cell opens: it has no owner after a restart, so the command's epoch no longer holds.
    let cell = Cell::open(&config, trusted_key()).expect("opens");
    assert_eq!(cell.recovery.len(), 1);
    assert_eq!(cell.recovery[0].command_id, "c1");
    assert_eq!(cell.recovery[0].to, State::Failed);
    cell.close();

    let (client, writer) = CommandWriter::spawn(&config.critical_db).expect("spawns");
    assert_eq!(client.state("c1").expect("reads"), Some(State::Failed));
    drop(client);
    writer.join();
}

#[test]
fn a_command_whose_execution_was_sent_before_the_stop_is_in_doubt_when_the_cell_opens() {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = config(dir.path());

    // An earlier run opened the intent of an execution and stopped before its result was written.
    let (client, writer) = CommandWriter::spawn(&config.critical_db).expect("spawns");
    client
        .submit(
            NewCommand {
                command_id: "c2".into(),
                identity: "scada-demo".into(),
                identity_level: Granularity::Client,
                point: "S3/TC_101".into(),
                operation: "direct_execute".into(),
                value: "true".into(),
                expires_ns: i64::MAX,
                expected_revision: String::new(),
                owner_epoch: 1,
                ha_term: 1,
            },
            1_000,
        )
        .expect("submits");
    for next in [State::Validated, State::DurablyAccepted, State::Ready] {
        client.move_to("c2", next, 1_000, "").expect("moves");
    }
    client
        .record_intent("c2", State::ExecuteSent, 1_000)
        .expect("intent");
    drop(client);
    writer.join();

    // The frame may have left: the command is in doubt, and nothing is replayed.
    let cell = Cell::open(&config, trusted_key()).expect("opens");
    let outcome = cell
        .recovery
        .iter()
        .find(|recovery| recovery.command_id == "c2")
        .expect("decided");
    assert_eq!(outcome.to, State::InDoubt);
    assert_eq!(outcome.action, powermug_command::Action::Nothing);
    cell.close();
}

#[test]
fn a_single_node_is_the_confirmed_owner_of_its_partition_when_the_cell_opens() {
    let dir = tempfile::tempdir().expect("temp dir");
    let with_adapter = format!(
        r#"{{
  "cell": "CELL_DC1",
  "telemetry_db": "{telemetry}",
  "critical_db": "{critical}",
  "management": {{
    "listen": "127.0.0.1:8443",
    "certificate": "server.pem",
    "key": "server.key",
    "client_ca": "ca.pem",
    "trusted_key": "trusted.pub.pem"
  }},
  "lanes": [{{"site": "S3", "revision": 1, "points": [
    {{"id": "S3/TC_101", "session": 1, "address": 5, "class": "Critical"}}]}}],
  "adapters": {{
    "binary": "/bin/true",
    "directory": "{adapters}",
    "instances": [{{"session": 1, "kind": "iec104", "stations": ["127.0.0.1:2404"], "common_address": 1}}]
  }},
  "commands": {{}}
}}"#,
        telemetry = dir.path().join("telemetry.db").display(),
        critical = dir.path().join("critical.db").display(),
        adapters = dir.path().join("adapters").display(),
    );
    let config = CellConfig::from_json(&with_adapter).expect("valid");
    let cell = Cell::open(&config, trusted_key()).expect("opens");
    let link = cell
        .commands_by_session
        .get(&1)
        .expect("the session has a command link");
    // Owner epoch 1 from the opening: the commands are accepted, and the adapter is activated at
    // the same epoch.
    assert_eq!(
        link.dispatcher.lock().expect("lock").activation_epoch("S3"),
        1
    );
    cell.close();
}
