//! An activation that changes the commands of a partition critically (FUN-CFG-007, FUN-CMD-011)
//! blocks them before it takes effect, and resumes them when it takes effect or fails. A change
//! that is not critical for commands does not block them.

mod common;

use std::sync::{Arc, Mutex};

use common::*;
use powermug_command::State;
use powermug_mgmt::{CommandGateway, CommandRequest, GatewayError};
use powermug_revision::partition::partition_content_id;
use serde_json::{json, Value};

/// Records what the gateway is told, in order.
#[derive(Default)]
struct Events(Mutex<Vec<String>>);

impl CommandGateway for Events {
    fn submit(&self, _request: &CommandRequest, _now_ns: i64) -> Result<State, GatewayError> {
        Err(GatewayError::UnknownPoint("no point".into()))
    }

    fn execute(
        &self,
        _command_id: &str,
        _point: &str,
        _identity: &str,
        _now_ns: i64,
    ) -> Result<State, GatewayError> {
        Err(GatewayError::UnknownPoint("no point".into()))
    }

    fn active_revision_changed(&self, partition: &str, revision: Option<&str>) {
        self.0.lock().expect("lock").push(format!(
            "changed:{partition}:{}",
            revision.unwrap_or("none")
        ));
    }

    fn active_rules_changed(
        &self,
        partition: &str,
        rules: &[powermug_revision::partition::CommandRule],
    ) {
        self.0
            .lock()
            .expect("lock")
            .push(format!("rules:{partition}:{}", rules.len()));
    }

    fn begin_critical_change(&self, partition: &str) {
        self.0
            .lock()
            .expect("lock")
            .push(format!("begin:{partition}"));
    }

    fn abort_critical_change(&self, partition: &str) {
        self.0
            .lock()
            .expect("lock")
            .push(format!("abort:{partition}"));
    }
}

/// A partition document of S3 with the given command rules, as a child of `parent`.
fn with_rules(parent: Option<&str>, rules: Value) -> Value {
    let mut document = sealed_document("S3", parent, "A");
    document["spec"]["commands"] = rules;
    let id = partition_content_id(&document).expect("canonical");
    document["metadata"]["id"] = json!(id);
    document
}

fn rule(operations: &[&str]) -> Value {
    json!({
        "point": "S3/TM_001",
        "issuer": "scada-demo",
        "operations": operations,
        "values": "any",
        "min_level": "link"
    })
}

fn plan_body(document: &Value) -> String {
    let artifact = revision_artifact(document);
    let plan = signed_plan(&signing_key(1), std::slice::from_ref(&artifact));
    json!({
        "signed_plan": plan,
        "revision_id": artifact["id"].as_str().expect("id")
    })
    .to_string()
}

async fn prepare_and_activate(address: std::net::SocketAddr, pki: &Pki, document: &Value) {
    let body = plan_body(document);
    for path in ["/revisions/prepare", "/revisions/activate"] {
        let (status, answer) = request(address, client_with_certificate(pki), "POST", path, &body)
            .await
            .expect("answers");
        assert_eq!(status, 200, "{path}: {answer}");
    }
}

#[tokio::test]
async fn only_an_activation_critical_for_commands_blocks_them_until_it_takes_effect() {
    let pki = pki();
    let dir = tempfile::tempdir().expect("temp dir");
    let events = Arc::new(Events::default());
    let address = serve_with_gateway(
        &pki,
        empty_store(&dir),
        signing_key(1).verifying_key(),
        events.clone(),
    )
    .await;

    // The first revision: nothing was active, so nothing is blocked.
    let first = with_rules(None, json!([]));
    prepare_and_activate(address, &pki, &first).await;
    let first_id = first["metadata"]["id"].as_str().expect("id").to_owned();

    // The same commands and points: not critical for commands.
    let second = with_rules(Some(&first_id), json!([]));
    prepare_and_activate(address, &pki, &second).await;
    let second_id = second["metadata"]["id"].as_str().expect("id").to_owned();

    // A new command rule: critical, so the commands are blocked around the activation.
    let third = with_rules(Some(&second_id), json!([rule(&["select"])]));
    prepare_and_activate(address, &pki, &third).await;
    let third_id = third["metadata"]["id"].as_str().expect("id").to_owned();

    // A critical activation that is refused (not prepared): the commands resume.
    let fourth = with_rules(
        Some(&third_id),
        json!([rule(&["select", "direct_execute"])]),
    );
    let (status, answer) = request(
        address,
        client_with_certificate(&pki),
        "POST",
        "/revisions/activate",
        &plan_body(&fourth),
    )
    .await
    .expect("answers");
    assert_eq!(status, 409, "{answer}");

    let recorded = events.0.lock().expect("lock").clone();
    assert_eq!(
        recorded,
        vec![
            "rules:S3:0".to_owned(),
            format!("changed:S3:{first_id}"),
            "rules:S3:0".to_owned(),
            format!("changed:S3:{second_id}"),
            "begin:S3".to_owned(),
            "rules:S3:1".to_owned(),
            format!("changed:S3:{third_id}"),
            "begin:S3".to_owned(),
            "abort:S3".to_owned(),
        ]
    );
}

#[tokio::test]
async fn a_declared_class_below_the_computed_one_is_refused_at_prepare() {
    let pki = pki();
    let dir = tempfile::tempdir().expect("temp dir");
    let events = Arc::new(Events::default());
    let address = serve_with_gateway(
        &pki,
        empty_store(&dir),
        signing_key(1).verifying_key(),
        events,
    )
    .await;
    let first = with_rules(None, json!([]));
    prepare_and_activate(address, &pki, &first).await;
    let first_id = first["metadata"]["id"].as_str().expect("id").to_owned();

    let mut critical = with_rules(Some(&first_id), json!([rule(&["select"])]));
    critical["spec"]["change_class"] = json!("HOT");
    critical["metadata"]["id"] = json!(partition_content_id(&critical).expect("canonical"));
    let (status, answer) = request(
        address,
        client_with_certificate(&pki),
        "POST",
        "/revisions/prepare",
        &plan_body(&critical),
    )
    .await
    .expect("answers");
    assert_eq!(status, 400, "{answer}");
    assert!(answer.contains("PMV-CEL-007"), "{answer}");

    // Declared as the change computes to: prepared and activated.
    critical["spec"]["change_class"] = json!("COMMAND_CRITICAL");
    critical["metadata"]["id"] = json!(partition_content_id(&critical).expect("canonical"));
    prepare_and_activate(address, &pki, &critical).await;
}
