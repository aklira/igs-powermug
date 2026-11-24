//! The North entry point of commands over mTLS (SPECIFICATIONS 9, [GW] 11.1): a command is
//! handed to the cell's gateway, and its state comes back; a malformed command is refused
//! before it reaches the cell.

mod common;

use std::sync::{Arc, Mutex};

use common::*;
use powermug_command::{Granularity, State};
use powermug_mgmt::{CommandGateway, CommandRequest, GatewayError};
use serde_json::Value as Json;

/// Records the commands it receives. It routes one point only.
#[derive(Default)]
struct Recorder {
    received: Mutex<Vec<CommandRequest>>,
    executed_by: Mutex<Vec<String>>,
}

impl CommandGateway for Recorder {
    fn submit(&self, request: &CommandRequest, _now_ns: i64) -> Result<State, GatewayError> {
        self.received.lock().expect("lock").push(request.clone());
        if request.point == "S3/TC_101" {
            Ok(State::Ready)
        } else {
            Err(GatewayError::UnknownPoint(request.point.clone()))
        }
    }

    fn execute(
        &self,
        command_id: &str,
        point: &str,
        identity: &str,
        _now_ns: i64,
    ) -> Result<State, GatewayError> {
        self.executed_by
            .lock()
            .expect("lock")
            .push(identity.to_owned());
        if point != "S3/TC_101" {
            return Err(GatewayError::UnknownPoint(point.to_owned()));
        }
        match command_id {
            "c1" => Ok(State::ExecuteSent),
            "refused" => Err(GatewayError::Refused("the command is not selected".into())),
            other => Err(GatewayError::UnknownCommand(other.to_owned())),
        }
    }
}

impl Recorder {
    fn executed_by(&self) -> Vec<String> {
        self.executed_by.lock().expect("lock").clone()
    }
}

fn body(text: &str) -> Json {
    serde_json::from_str(text).expect("json")
}

#[tokio::test]
async fn a_command_is_handed_to_the_gateway_and_its_state_comes_back() {
    let pki = pki();
    let dir = tempfile::tempdir().expect("temp dir");
    let gateway = Arc::new(Recorder::default());
    let address = serve_with_gateway(
        &pki,
        empty_store(&dir),
        signing_key(1).verifying_key(),
        gateway.clone(),
    )
    .await;
    let client = || client_with_certificate(&pki);

    let (status, text) = request(
        address,
        client(),
        "POST",
        "/commands",
        r#"{"command_id": "c1", "point": "S3/TC_101", "operation": "direct_execute", "value": true}"#,
    )
    .await
    .expect("answers");
    assert_eq!(status, 200);
    assert_eq!(body(&text)["state"], "READY");
    assert_eq!(body(&text)["command_id"], "c1");

    let received = gateway.received.lock().expect("lock");
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].identity_level, Granularity::Client);
    assert!(received[0].on);
    assert_eq!(received[0].operation, "direct_execute");
}

#[tokio::test]
async fn a_malformed_command_never_reaches_the_gateway_and_an_unknown_point_is_refused_by_it() {
    let pki = pki();
    let dir = tempfile::tempdir().expect("temp dir");
    let gateway = Arc::new(Recorder::default());
    let address = serve_with_gateway(
        &pki,
        empty_store(&dir),
        signing_key(1).verifying_key(),
        gateway.clone(),
    )
    .await;
    let client = || client_with_certificate(&pki);

    let (status, text) = request(
        address,
        client(),
        "POST",
        "/commands",
        r#"{"command_id": "c2", "point": "S3/TC_999", "operation": "direct_execute", "value": true}"#,
    )
    .await
    .expect("answers");
    assert_eq!(status, 404);
    assert_eq!(body(&text)["error"], "unknown_point");

    let (status, text) = request(
        address,
        client(),
        "POST",
        "/commands",
        r#"{"command_id": "c3", "point": "S3/TC_101", "operation": "execute", "value": true}"#,
    )
    .await
    .expect("answers");
    assert_eq!(status, 400);
    assert_eq!(body(&text)["error"], "unknown_operation");

    let (status, text) = request(
        address,
        client(),
        "POST",
        "/commands",
        r#"{"command_id": " ", "point": "S3/TC_101", "operation": "select", "value": true}"#,
    )
    .await
    .expect("answers");
    assert_eq!(status, 400);
    assert_eq!(body(&text)["error"], "command_id_required");

    // Only the first command reached the gateway, and it was the unknown point's: the two
    // malformed commands never did.
    assert_eq!(gateway.received.lock().expect("lock").len(), 1);
}

#[tokio::test]
async fn a_cell_without_a_gateway_serves_no_commands() {
    let pki = pki();
    let dir = tempfile::tempdir().expect("temp dir");
    let address = serve(&pki, empty_store(&dir), signing_key(1).verifying_key()).await;
    let (status, text) = request(
        address,
        client_with_certificate(&pki),
        "POST",
        "/commands",
        r#"{"command_id": "c1", "point": "S3/TC_101", "operation": "select", "value": true}"#,
    )
    .await
    .expect("answers");
    assert_eq!(status, 503);
    assert_eq!(body(&text)["error"], "no_command_path");
}

#[tokio::test]
async fn a_selected_command_is_executed_through_the_gateway_and_refusals_are_answered() {
    let pki = pki();
    let dir = tempfile::tempdir().expect("temp dir");
    let gateway = Arc::new(Recorder::default());
    let address = serve_with_gateway(
        &pki,
        empty_store(&dir),
        signing_key(1).verifying_key(),
        gateway.clone(),
    )
    .await;
    let client = || client_with_certificate(&pki);

    let (status, text) = request(
        address,
        client(),
        "POST",
        "/commands/c1/execute",
        r#"{"point": "S3/TC_101"}"#,
    )
    .await
    .expect("answers");
    assert_eq!(status, 200);
    assert_eq!(body(&text)["state"], "EXECUTE_SENT");

    let (status, text) = request(
        address,
        client(),
        "POST",
        "/commands/refused/execute",
        r#"{"point": "S3/TC_101"}"#,
    )
    .await
    .expect("answers");
    assert_eq!(status, 409);
    assert_eq!(body(&text)["error"], "not_executable");

    let (status, text) = request(
        address,
        client(),
        "POST",
        "/commands/c9/execute",
        r#"{"point": "S3/TC_101"}"#,
    )
    .await
    .expect("answers");
    assert_eq!(status, 404);
    assert_eq!(body(&text)["error"], "unknown_command");

    // The execution is issued under the common name of the client certificate, for each call
    // that reached the gateway.
    assert_eq!(gateway.executed_by(), vec!["client"; 3]);
}

#[tokio::test]
async fn a_command_is_issued_under_the_common_name_of_the_client_certificate() {
    let pki = pki();
    let dir = tempfile::tempdir().expect("temp dir");
    let gateway = Arc::new(Recorder::default());
    let address = serve_with_gateway(
        &pki,
        empty_store(&dir),
        signing_key(1).verifying_key(),
        gateway.clone(),
    )
    .await;
    let (status, _) = request(
        address,
        client_with_certificate(&pki),
        "POST",
        "/commands",
        r#"{"command_id": "c2", "point": "S3/TC_101", "operation": "direct_execute", "value": false}"#,
    )
    .await
    .expect("answers");
    assert_eq!(status, 200);
    assert_eq!(gateway.received.lock().expect("lock")[0].identity, "client");
}

#[tokio::test]
async fn a_client_certificate_without_a_common_name_cannot_issue_a_command() {
    let pki = pki_with_client_subject(None);
    let dir = tempfile::tempdir().expect("temp dir");
    let gateway = Arc::new(Recorder::default());
    let address = serve_with_gateway(
        &pki,
        empty_store(&dir),
        signing_key(1).verifying_key(),
        gateway.clone(),
    )
    .await;
    let (status, text) = request(
        address,
        client_with_certificate(&pki),
        "POST",
        "/commands",
        r#"{"command_id": "c3", "point": "S3/TC_101", "operation": "direct_execute", "value": true}"#,
    )
    .await
    .expect("answers");
    assert_eq!(status, 401);
    assert_eq!(body(&text)["error"], "unidentified_client");
    assert!(gateway.received.lock().expect("lock").is_empty());
}
