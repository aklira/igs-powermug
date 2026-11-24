//! The cell's command rules and gateway (SPECIFICATIONS 9.3, [GW] 11.1): the rules of the
//! configuration are checked, and a command reaches the dispatcher of its point only when the
//! rule, the owner and the adapter's stream allow it.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use powermug_cell::commands::{channel, CommandRoute, Dispatcher, Gateway, Route};
use powermug_cell::config::{acl_of, CellConfig, ConfigError};
use powermug_command::ownership::Ownership;
use powermug_command::{CommandWriter, Granularity, State};
use powermug_mgmt::{CommandGateway, CommandRequest, GatewayError};

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
  "commands": {"acl": [
    {"point": "S3/TC_101", "issuer": "scada-demo", "operations": ["direct_execute", "select"], "values": "bool"}
  ]}
}"#;

fn with_rule(rule: &str) -> String {
    CONFIG.replace(
        "{\"point\": \"S3/TC_101\", \"issuer\": \"scada-demo\", \"operations\": [\"direct_execute\", \"select\"], \"values\": \"bool\"}",
        rule,
    )
}

#[test]
fn a_valid_rule_is_read_and_an_invalid_one_is_refused() {
    let config = CellConfig::from_json(CONFIG).expect("valid");
    assert_eq!(config.commands.acl.len(), 1);
    assert_eq!(config.commands.acl[0].min_level, "link");

    assert_eq!(
        CellConfig::from_json(&with_rule(
            r#"{"point": "S3/TC_999", "issuer": "x", "operations": ["select"]}"#
        )),
        Err(ConfigError::UnknownCommandPoint("S3/TC_999".into()))
    );
    assert!(matches!(
        CellConfig::from_json(&with_rule(
            r#"{"point": "S3/TC_101", "issuer": "x", "operations": ["execute"]}"#
        )),
        Err(ConfigError::BadCommandRule { .. })
    ));
    assert!(matches!(
        CellConfig::from_json(&with_rule(
            r#"{"point": "S3/TC_101", "issuer": "x", "operations": ["select"], "min_level": "root"}"#
        )),
        Err(ConfigError::BadCommandRule { .. })
    ));
}

fn command(identity: &str, point: &str, command_id: &str) -> CommandRequest {
    CommandRequest {
        command_id: command_id.into(),
        identity: identity.into(),
        identity_level: Granularity::Link,
        point: point.into(),
        operation: "direct_execute".into(),
        on: true,
        expires_ns: i64::MAX,
        expected_revision: String::new(),
    }
}

#[test]
fn the_gateway_routes_a_command_through_the_rule_the_owner_and_the_stream() {
    let config = CellConfig::from_json(CONFIG).expect("valid");
    let dir = tempfile::tempdir().expect("temp dir");
    let (journal, writer) = CommandWriter::spawn(dir.path().join("critical.db")).expect("spawns");
    let mut ownership = Ownership::new();
    ownership.takeover("S3");
    // The adapter's stream is not attached: an authorized command is failed, never queued.
    let route = CommandRoute::new();
    let (to_adapter, _queued) = channel(8);
    route.attach(to_adapter);
    route.detach();
    let dispatcher = Arc::new(Mutex::new(Dispatcher::new(
        journal.clone(),
        acl_of(&config.commands),
        ownership,
        route,
    )));
    let mut routes = BTreeMap::new();
    routes.insert(
        "S3/TC_101".to_owned(),
        Route {
            dispatcher: Arc::clone(&dispatcher),
            partition: "S3".into(),
            address: 5,
        },
    );
    let gateway = Gateway::new(routes);

    assert_eq!(
        gateway.submit(&command("scada-demo", "S3/TC_101", "g1"), 10),
        Ok(State::Failed)
    );
    assert_eq!(
        gateway.submit(&command("intruder", "S3/TC_101", "g2"), 10),
        Ok(State::RejectedAuth)
    );
    assert!(matches!(
        gateway.submit(&command("scada-demo", "S3/TC_999", "g3"), 10),
        Err(GatewayError::UnknownPoint(_))
    ));

    // A command that was failed cannot be executed; an unknown one is not known.
    assert!(matches!(
        gateway.execute("g1", "S3/TC_101", 10),
        Err(GatewayError::Refused(_))
    ));
    assert!(matches!(
        gateway.execute("zz", "S3/TC_101", 10),
        Err(GatewayError::UnknownCommand(_))
    ));

    drop(gateway);
    drop(dispatcher);
    drop(journal);
    writer.join();
}
