//! The cell's command gateway (SPECIFICATIONS 9.3, [GW] 11.1): the rules come from the active
//! partition revision, and a command reaches the dispatcher of its point only when the rule, the
//! owner and the adapter's stream allow it.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use powermug_cell::commands::{channel, CommandRoute, Dispatcher, Gateway, Route};
use powermug_cell::config::{acl_of_rules, CellConfig, ConfigError};
use powermug_command::authorization::Acl;
use powermug_command::ownership::Ownership;
use powermug_command::{CommandWriter, Granularity, State};
use powermug_mgmt::{CommandGateway, CommandRequest, GatewayError};
use powermug_revision::partition::CommandRule;

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
  "commands": {}
}"#;

/// The rule of the partition revision that grants `scada-demo` the commands of `S3/TC_101`.
fn rule() -> CommandRule {
    CommandRule {
        point: "S3/TC_101".into(),
        issuer: "scada-demo".into(),
        operations: vec!["direct_execute".into(), "select".into()],
        values: "bool".into(),
        min_level: "link".into(),
    }
}

/// The access control list of the partition revision with that rule.
fn acl() -> Acl {
    acl_of_rules(&[rule()])
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
        expected_revision: "r1".into(),
    }
}

#[test]
fn the_configuration_holds_no_command_rules_of_its_own() {
    assert!(CellConfig::from_json(CONFIG).is_ok());
    let with_rules = CONFIG.replace("\"commands\": {}", "\"commands\": {\"acl\": []}");
    assert!(matches!(
        CellConfig::from_json(&with_rules),
        Err(ConfigError::Syntax(_))
    ));
}

#[test]
fn the_gateway_routes_a_command_through_the_rule_the_owner_and_the_stream() {
    let dir = tempfile::tempdir().expect("temp dir");
    let (journal, writer) = CommandWriter::spawn(dir.path().join("critical.db")).expect("spawns");
    let mut ownership = Ownership::new();
    ownership.takeover("S3");
    // The adapter's stream is not attached: an authorized command is failed, never queued.
    let route = CommandRoute::new();
    let (to_adapter, _queued) = channel(8);
    route.attach(to_adapter);
    route.detach();
    let mut dispatcher = Dispatcher::new(journal.clone(), acl(), ownership, route);
    dispatcher.set_active_revision(Some("r1".into()));
    let dispatcher = Arc::new(Mutex::new(dispatcher));
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
        gateway.execute("g1", "S3/TC_101", "scada-demo", 10),
        Err(GatewayError::Refused(_))
    ));
    assert!(matches!(
        gateway.execute("zz", "S3/TC_101", "scada-demo", 10),
        Err(GatewayError::UnknownCommand(_))
    ));

    drop(gateway);
    drop(dispatcher);
    drop(journal);
    writer.join();
}

#[test]
fn an_activation_moves_the_partition_to_the_revision_its_commands_cite() {
    let dir = tempfile::tempdir().expect("temp dir");
    let (journal, writer) = CommandWriter::spawn(dir.path().join("critical.db")).expect("spawns");
    let mut ownership = Ownership::new();
    ownership.takeover("S3");
    let route = CommandRoute::new();
    let (to_adapter, _queued) = channel(8);
    route.attach(to_adapter);
    let dispatcher = Arc::new(Mutex::new(Dispatcher::new(
        journal.clone(),
        acl(),
        ownership,
        route.clone(),
    )));
    let mut routes = BTreeMap::new();
    routes.insert(
        "S3/TC_101".to_owned(),
        Route {
            dispatcher,
            partition: "S3".to_owned(),
            address: 5,
        },
    );
    let gateway = Gateway::new(routes);

    // No revision is active yet: the command is refused, whatever it cites.
    assert_eq!(
        gateway
            .submit(&command("scada-demo", "S3/TC_101", "c1"), 10)
            .expect("submits"),
        State::RejectedInvalid
    );

    gateway.active_revision_changed("S3", Some("r1"));
    assert_eq!(
        gateway
            .submit(&command("scada-demo", "S3/TC_101", "c2"), 20)
            .expect("submits"),
        State::Ready
    );

    // A rollback to no revision refuses the commands again.
    gateway.active_revision_changed("S3", None);
    assert_eq!(
        gateway
            .submit(&command("scada-demo", "S3/TC_101", "c3"), 30)
            .expect("submits"),
        State::RejectedInvalid
    );

    drop(gateway);
    drop(route);
    drop(journal);
    writer.join();
}

#[test]
fn a_critical_change_blocks_the_partition_until_it_takes_effect_or_is_aborted() {
    let dir = tempfile::tempdir().expect("temp dir");
    let (journal, writer) = CommandWriter::spawn(dir.path().join("critical.db")).expect("spawns");
    let mut ownership = Ownership::new();
    ownership.takeover("S3");
    let route = CommandRoute::new();
    let (to_adapter, _queued) = channel(8);
    route.attach(to_adapter);
    let mut dispatcher = Dispatcher::new(journal.clone(), acl(), ownership, route.clone());
    dispatcher.set_active_revision(Some("r1".into()));
    let mut routes = BTreeMap::new();
    routes.insert(
        "S3/TC_101".to_owned(),
        Route {
            dispatcher: Arc::new(Mutex::new(dispatcher)),
            partition: "S3".to_owned(),
            address: 5,
        },
    );
    let gateway = Gateway::new(routes);

    gateway.begin_critical_change("S3");
    assert_eq!(
        gateway
            .submit(&command("scada-demo", "S3/TC_101", "c1"), 10)
            .expect("submits"),
        State::RejectedUnavailable
    );

    gateway.abort_critical_change("S3");
    assert_eq!(
        gateway
            .submit(&command("scada-demo", "S3/TC_101", "c2"), 20)
            .expect("submits"),
        State::Ready
    );

    // The change takes effect: the new revision is active and resumes the commands.
    gateway.begin_critical_change("S3");
    gateway.active_revision_changed("S3", Some("r2"));
    let mut citing_new = command("scada-demo", "S3/TC_101", "c3");
    citing_new.expected_revision = "r2".into();
    assert_eq!(
        gateway.submit(&citing_new, 30).expect("submits"),
        State::Ready
    );

    drop(gateway);
    drop(route);
    drop(journal);
    writer.join();
}

#[test]
fn the_rules_of_the_active_revision_grant_the_commands_of_its_partition() {
    let dir = tempfile::tempdir().expect("temp dir");
    let (journal, writer) = CommandWriter::spawn(dir.path().join("critical.db")).expect("spawns");
    let mut ownership = Ownership::new();
    ownership.takeover("S3");
    let route = CommandRoute::new();
    let (to_adapter, _queued) = channel(8);
    route.attach(to_adapter);
    let mut dispatcher = Dispatcher::new(journal.clone(), Acl::new(), ownership, route.clone());
    dispatcher.set_active_revision(Some("r1".into()));
    let mut routes = BTreeMap::new();
    routes.insert(
        "S3/TC_101".to_owned(),
        Route {
            dispatcher: Arc::new(Mutex::new(dispatcher)),
            partition: "S3".to_owned(),
            address: 5,
        },
    );
    let gateway = Gateway::new(routes);

    // The revision grants nothing yet: the command is refused on its authorization.
    assert_eq!(
        gateway
            .submit(&command("scada-demo", "S3/TC_101", "c1"), 10)
            .expect("submits"),
        State::RejectedAuth
    );

    gateway.active_rules_changed("S3", &[rule()]);
    assert_eq!(
        gateway
            .submit(&command("scada-demo", "S3/TC_101", "c2"), 20)
            .expect("submits"),
        State::Ready
    );

    drop(gateway);
    drop(route);
    drop(journal);
    writer.join();
}
