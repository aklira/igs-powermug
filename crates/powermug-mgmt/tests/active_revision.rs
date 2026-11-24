//! An activation and a rollback tell the command gateway the partition's active revision, so the
//! commands that cite it are the ones accepted (SPECIFICATIONS 10.5, 9).

mod common;

use std::sync::{Arc, Mutex};

use common::*;
use powermug_command::State;
use powermug_mgmt::{CommandGateway, CommandRequest, GatewayError};
use serde_json::{json, Value};

/// Records the changes of the active revision it is told about.
#[derive(Default)]
struct Changes(Mutex<Vec<(String, Option<String>)>>);

impl CommandGateway for Changes {
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
        self.0
            .lock()
            .expect("lock")
            .push((partition.to_owned(), revision.map(str::to_owned)));
    }
}

fn plan_body(plan: &Value, revision_id: &str) -> String {
    json!({ "signed_plan": plan, "revision_id": revision_id }).to_string()
}

#[tokio::test]
async fn an_activation_and_a_rollback_tell_the_gateway_the_active_revision() {
    let pki = pki();
    let dir = tempfile::tempdir().expect("temp dir");
    let changes = Arc::new(Changes::default());
    let address = serve_with_gateway(
        &pki,
        empty_store(&dir),
        signing_key(1).verifying_key(),
        changes.clone(),
    )
    .await;
    let client = || client_with_certificate(&pki);

    let first = sealed_document("S3", None, "A");
    let first_artifact = revision_artifact(&first);
    let first_plan = signed_plan(&signing_key(1), std::slice::from_ref(&first_artifact));
    let first_body = plan_body(&first_plan, first_artifact["id"].as_str().expect("id"));
    for path in ["/revisions/prepare", "/revisions/activate"] {
        let (status, answer) = request(address, client(), "POST", path, &first_body)
            .await
            .expect("answers");
        assert_eq!(status, 200, "{answer}");
    }
    let first_id = first["metadata"]["id"].as_str().expect("id").to_owned();

    let second = sealed_document("S3", Some(&first_id), "A");
    let second_artifact = revision_artifact(&second);
    let second_plan = signed_plan(&signing_key(1), std::slice::from_ref(&second_artifact));
    let second_body = plan_body(&second_plan, second_artifact["id"].as_str().expect("id"));
    for path in ["/revisions/prepare", "/revisions/activate"] {
        let (status, answer) = request(address, client(), "POST", path, &second_body)
            .await
            .expect("answers");
        assert_eq!(status, 200, "{answer}");
    }
    let second_id = second["metadata"]["id"].as_str().expect("id").to_owned();

    let (status, answer) = request(
        address,
        client(),
        "POST",
        "/revisions/rollback",
        r#"{"site": "S3"}"#,
    )
    .await
    .expect("answers");
    assert_eq!(status, 200, "{answer}");

    let changes = changes.0.lock().expect("lock").clone();
    assert_eq!(
        changes,
        vec![
            ("S3".to_owned(), Some(first_id.clone())),
            ("S3".to_owned(), Some(second_id)),
            ("S3".to_owned(), Some(first_id)),
        ]
    );
}
