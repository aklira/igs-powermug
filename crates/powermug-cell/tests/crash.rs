//! A crash on the core's command path (SPECIFICATIONS 9.7, PM-CMD-002): a process submits a
//! direct command through the dispatcher, the intent of its emission is on disk, and the process
//! exits before the adapter answers. A cell opened afterwards classes the command in doubt, and
//! never sends it again.

use powermug_cell::commands::{channel, CommandRoute, Dispatcher, Request};
use powermug_cell::config::{acl_of_rules, CellConfig};
use powermug_cell::Cell;
use powermug_command::ownership::Ownership;
use powermug_command::{CommandWriter, Granularity, State};
use powermug_contract::proto::v1 as pb;
use powermug_revision::partition::CommandRule;

const CHILD_TEST: &str = "crash_child";
const DIR: &str = "POWERMUG_CRASH_DIR";
const CRASH_EXIT: i32 = 17;

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
    {{"id": "S3/TC_101", "session": 1, "address": 5, "class": "Critical"}}]}}],
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

fn rule() -> CommandRule {
    CommandRule {
        point: "S3/TC_101".into(),
        issuer: "scada-demo".into(),
        operations: vec!["direct_execute".into()],
        values: "bool".into(),
        min_level: "link".into(),
    }
}

fn request() -> Request {
    Request {
        command_id: "c1".into(),
        identity: "scada-demo".into(),
        identity_level: Granularity::Link,
        point: "S3/TC_101".into(),
        operation: "direct_execute".into(),
        partition: "S3".into(),
        address: 5,
        kind: pb::CommandKind::Single,
        on: true,
        select: false,
        expires_ns: i64::MAX,
        expected_revision: "r1".into(),
    }
}

/// The child: submits the command, and exits as a crash would, before any answer comes back.
#[test]
fn crash_child() {
    let Ok(dir) = std::env::var(DIR) else {
        return;
    };
    let (client, _writer) =
        CommandWriter::spawn(std::path::Path::new(&dir).join("critical.db")).expect("spawns");
    let mut ownership = Ownership::new();
    ownership.takeover("S3");
    let (to_adapter, _queued) = channel(8);
    let route = CommandRoute::new();
    route.attach(to_adapter);
    let mut dispatcher = Dispatcher::new(client, acl_of_rules(&[rule()]), ownership, route);
    dispatcher.set_active_revision(Some("r1".into()));
    let state = dispatcher.submit(&request(), 1_000).expect("submits");
    assert_eq!(
        state,
        State::Ready,
        "the dispatch is sent, not yet answered"
    );
    std::process::exit(CRASH_EXIT);
}

#[test]
fn a_command_sent_before_a_crash_is_in_doubt_when_the_cell_reopens() {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = config(dir.path());

    let status = std::process::Command::new(std::env::current_exe().expect("test binary"))
        .args(["--exact", CHILD_TEST, "--test-threads=1", "--nocapture"])
        .env(DIR, dir.path())
        .status()
        .expect("runs the child");
    assert_eq!(status.code(), Some(CRASH_EXIT));

    let cell = Cell::open(&config, trusted_key()).expect("opens");
    let outcome = cell
        .recovery
        .iter()
        .find(|outcome| outcome.command_id == "c1")
        .expect("decided");
    assert_eq!(outcome.to, State::InDoubt);
    assert_eq!(outcome.action, powermug_command::Action::Nothing);
    cell.close();
}
