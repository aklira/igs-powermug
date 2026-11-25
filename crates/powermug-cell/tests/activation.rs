//! An activation interrupted at each of its persistent steps (SPECIFICATIONS 10.5, PM-CFG-005,
//! ACC-008, reduced). The switch of the active revision is one transaction of the critical store:
//! a stop before it leaves the previous revision, and a stop after it leaves the new one. A cell
//! opened afterwards reads its rules and its active revision from the store, so it is on exactly
//! one of the two, never a mix.

use std::path::Path;

use powermug_cell::commands::Request;
use powermug_cell::config::CellConfig;
use powermug_cell::Cell;
use powermug_command::{Granularity, State};
use powermug_contract::proto::v1 as pb;
use powermug_revision::canonical::canonical_bytes;
use powermug_revision::partition::partition_content_id;
use powermug_store::CriticalStore;
use serde_json::{json, Value};

fn document(parent: Option<&str>, granted: bool) -> Value {
    let commands = if granted {
        json!([{
            "point": "S3/TC_101", "issuer": "scada-demo", "operations": ["select"],
            "values": "any", "min_level": "link"
        }])
    } else {
        json!([])
    };
    let mut document = json!({
        "apiVersion": "powermug/v1",
        "kind": "PartitionRevision",
        "metadata": {"cell": "CELL_DC1", "site": "S3", "id": "placeholder"},
        "spec": {
            "requires": {"contract": "1.0", "capabilities": ["south.iec104"]},
            "sessions": [{
                "id": "s3-ca1", "adapter": "iec104-client", "common_address": 1,
                "endpoint": {"host": "127.0.0.1", "port": 2404},
                "transport_security": {"mode": "none", "justification": "loopback test"}
            }],
            "points": [{
                "id": "S3/TC_101", "session": "s3-ca1", "address": 5,
                "class": "Critical", "commandable": granted
            }],
            "commands": commands
        }
    });
    if let Some(parent) = parent {
        document["metadata"]["parent"] = json!(parent);
    }
    let id = partition_content_id(&document).expect("canonical");
    document["metadata"]["id"] = json!(id);
    document
}

fn config(dir: &Path) -> CellConfig {
    let text = json!({
        "cell": "CELL_DC1",
        "telemetry_db": dir.join("telemetry.db"),
        "critical_db": dir.join("critical.db"),
        "management": {
            "listen": "127.0.0.1:8443",
            "certificate": "server.pem",
            "key": "server.key",
            "client_ca": "ca.pem",
            "trusted_key": "trusted.pub.pem"
        },
        "lanes": [{"site": "S3", "revision": 1, "points": [
            {"id": "S3/TC_101", "session": 1, "address": 5, "class": "Critical"}]}],
        "adapters": {
            "binary": "/bin/true",
            "directory": dir.join("adapters"),
            "instances": [{"session": 1, "kind": "iec104", "stations": ["127.0.0.1:2404"],
                "common_address": 1}]
        },
        "commands": {}
    });
    CellConfig::from_json(&text.to_string()).expect("valid")
}

fn trusted_key() -> ed25519_dalek::VerifyingKey {
    ed25519_dalek::SigningKey::from_bytes(&[7; 32]).verifying_key()
}

/// Prepares a revision in the store, and activates it when `activate` is set.
fn stage(
    store: &mut CriticalStore,
    document: &Value,
    parent: Option<&str>,
    activate: bool,
) -> String {
    let id = document["metadata"]["id"].as_str().expect("id").to_owned();
    let body = canonical_bytes(document).expect("canonical");
    store.prepare(&id, "S3", parent, &body).expect("prepares");
    if activate {
        store.activate(&id, "S3").expect("activates");
    }
    id
}

/// What the reopened cell answers to a selection that cites `revision`.
fn answer(cell: &Cell, revision: &str) -> State {
    let link = cell.commands_by_session.get(&1).expect("a command link");
    let request = Request {
        command_id: format!("probe-{revision}"),
        identity: "scada-demo".into(),
        identity_level: Granularity::Link,
        point: "S3/TC_101".into(),
        operation: "select".into(),
        partition: "S3".into(),
        address: 5,
        kind: pb::CommandKind::Single,
        on: true,
        select: true,
        expires_ns: i64::MAX,
        expected_revision: revision.to_owned(),
    };
    link.dispatcher
        .lock()
        .expect("lock")
        .submit(&request, 1_000)
        .expect("submits")
}

#[test]
fn a_stop_before_the_switch_leaves_the_previous_revision() {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = config(dir.path());
    let previous = document(None, false);
    let next = document(Some(previous["metadata"]["id"].as_str().expect("id")), true);
    {
        let mut store = CriticalStore::open(&config.critical_db).expect("opens");
        let previous_id = stage(&mut store, &previous, None, true);
        // Prepared, not switched: the stop comes before the switch.
        stage(&mut store, &next, Some(&previous_id), false);
    }

    let cell = Cell::open(&config, trusted_key()).expect("opens");
    let previous_id = previous["metadata"]["id"].as_str().expect("id").to_owned();
    let next_id = next["metadata"]["id"].as_str().expect("id").to_owned();
    // The previous revision grants nothing: refused on its authorization, under its own id.
    assert_eq!(answer(&cell, &previous_id), State::RejectedAuth);
    // The prepared revision is not active: a command citing it is refused.
    assert_eq!(answer(&cell, &next_id), State::RejectedInvalid);
    cell.close();
}

#[test]
fn a_stop_after_the_switch_leaves_the_new_revision_whole() {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = config(dir.path());
    let previous = document(None, false);
    let next = document(Some(previous["metadata"]["id"].as_str().expect("id")), true);
    {
        let mut store = CriticalStore::open(&config.critical_db).expect("opens");
        let previous_id = stage(&mut store, &previous, None, true);
        // Switched, and the stop comes before the notifications of the gateway.
        stage(&mut store, &next, Some(&previous_id), true);
    }

    let cell = Cell::open(&config, trusted_key()).expect("opens");
    let previous_id = previous["metadata"]["id"].as_str().expect("id").to_owned();
    let next_id = next["metadata"]["id"].as_str().expect("id").to_owned();
    // The new revision is active, and its rules are the ones that grant the selection: authorized
    // and dispatched (here, without a stream: failed, which is past the authorization).
    assert_eq!(answer(&cell, &next_id), State::Failed);
    assert_eq!(answer(&cell, &previous_id), State::RejectedInvalid);
    cell.close();
}
