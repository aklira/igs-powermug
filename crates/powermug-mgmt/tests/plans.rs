//! `prepare` and `activate` on a signed plan (§10.6, §10.5): the plan is verified, its
//! partition document is checked, and the critical store switches the active revision.

mod common;

use common::*;
use serde_json::{json, Value};

fn plan_body(plan: &Value, revision_id: &str) -> String {
    json!({ "signed_plan": plan, "revision_id": revision_id }).to_string()
}

fn trusted() -> ed25519_dalek::VerifyingKey {
    signing_key(1).verifying_key()
}

#[tokio::test]
async fn prepare_then_activate_makes_the_partition_revision_active() {
    let pki = pki();
    let dir = tempfile::tempdir().expect("temp dir");
    let address = serve(&pki, empty_store(&dir), trusted()).await;
    let document = sealed_document("S3", None, "A");
    let artifact = revision_artifact(&document);
    let plan = signed_plan(&signing_key(1), std::slice::from_ref(&artifact));
    let body = plan_body(&plan, artifact["id"].as_str().expect("id"));

    let (status, answer) = request(
        address,
        client_with_certificate(&pki),
        "POST",
        "/revisions/prepare",
        &body,
    )
    .await
    .expect("answers");
    assert_eq!(status, 200, "{answer}");
    assert!(answer.contains("PREPARED"), "{answer}");

    let (status, answer) = request(
        address,
        client_with_certificate(&pki),
        "POST",
        "/revisions/activate",
        &body,
    )
    .await
    .expect("answers");
    assert_eq!(status, 200, "{answer}");
    assert!(answer.contains("\"ACTIVE\""), "{answer}");

    let (_, state) = request(address, client_with_certificate(&pki), "GET", "/state", "")
        .await
        .expect("answers");
    let state: Value = serde_json::from_str(&state).expect("json");
    assert_eq!(state["scopes"][0]["active"], document["metadata"]["id"]);

    let (status, answer) = request(
        address,
        client_with_certificate(&pki),
        "POST",
        "/revisions/prepare",
        &body,
    )
    .await
    .expect("answers");
    assert_eq!(status, 200);
    assert!(answer.contains("ALREADY_ACTIVE"), "{answer}");
}

#[tokio::test]
async fn activation_without_preparation_is_a_conflict() {
    let pki = pki();
    let dir = tempfile::tempdir().expect("temp dir");
    let address = serve(&pki, empty_store(&dir), trusted()).await;
    let artifact = revision_artifact(&sealed_document("S3", None, "A"));
    let plan = signed_plan(&signing_key(1), std::slice::from_ref(&artifact));
    let body = plan_body(&plan, artifact["id"].as_str().expect("id"));

    let (status, answer) = request(
        address,
        client_with_certificate(&pki),
        "POST",
        "/revisions/activate",
        &body,
    )
    .await
    .expect("answers");

    assert_eq!(status, 409, "{answer}");
    assert!(answer.contains("not_prepared"), "{answer}");
}

#[tokio::test]
async fn a_plan_signed_by_another_key_is_refused() {
    let pki = pki();
    let dir = tempfile::tempdir().expect("temp dir");
    let address = serve(&pki, empty_store(&dir), trusted()).await;
    let artifact = revision_artifact(&sealed_document("S3", None, "A"));
    let plan = signed_plan(&signing_key(2), std::slice::from_ref(&artifact));
    let body = plan_body(&plan, artifact["id"].as_str().expect("id"));

    let (status, answer) = request(
        address,
        client_with_certificate(&pki),
        "POST",
        "/revisions/prepare",
        &body,
    )
    .await
    .expect("answers");

    assert_eq!(status, 400, "{answer}");
    assert!(answer.contains("invalid_signed_plan"), "{answer}");
}

#[tokio::test]
async fn a_revision_without_a_powermug_document_is_refused() {
    let pki = pki();
    let dir = tempfile::tempdir().expect("temp dir");
    let address = serve(&pki, empty_store(&dir), trusted()).await;
    let mut artifact = json!({ "artifact_type": "ibtnm.target-revision" });
    artifact["id"] =
        json!(powermug_revision::canonical::content_id("revision", &artifact).expect("canonical"));
    let plan = signed_plan(&signing_key(1), std::slice::from_ref(&artifact));
    let body = plan_body(&plan, artifact["id"].as_str().expect("id"));

    let (status, answer) = request(
        address,
        client_with_certificate(&pki),
        "POST",
        "/revisions/prepare",
        &body,
    )
    .await
    .expect("answers");

    assert_eq!(status, 400, "{answer}");
    assert!(answer.contains("no_powermug_document"), "{answer}");
}

#[tokio::test]
async fn an_invalid_partition_document_is_refused_with_its_findings() {
    let pki = pki();
    let dir = tempfile::tempdir().expect("temp dir");
    let address = serve(&pki, empty_store(&dir), trusted()).await;
    let artifact = revision_artifact(&sealed_document("S3", None, "Z"));
    let plan = signed_plan(&signing_key(1), std::slice::from_ref(&artifact));
    let body = plan_body(&plan, artifact["id"].as_str().expect("id"));

    let (status, answer) = request(
        address,
        client_with_certificate(&pki),
        "POST",
        "/revisions/prepare",
        &body,
    )
    .await
    .expect("answers");

    assert_eq!(status, 400, "{answer}");
    let answer: Value = serde_json::from_str(&answer).expect("json");
    assert_eq!(answer["error"], "invalid_document");
    assert!(answer["findings"]
        .as_array()
        .expect("array")
        .iter()
        .any(|f| f["code"] == "PMV-SEM-005"));
}

#[tokio::test]
async fn a_revision_prepared_against_an_old_parent_is_a_conflict() {
    let pki = pki();
    let dir = tempfile::tempdir().expect("temp dir");
    let address = serve(&pki, empty_store(&dir), trusted()).await;

    let first = revision_artifact(&sealed_document("S3", None, "A"));
    let first_plan = signed_plan(&signing_key(1), std::slice::from_ref(&first));
    let first_body = plan_body(&first_plan, first["id"].as_str().expect("id"));
    request(
        address,
        client_with_certificate(&pki),
        "POST",
        "/revisions/prepare",
        &first_body,
    )
    .await
    .expect("prepares the first");
    request(
        address,
        client_with_certificate(&pki),
        "POST",
        "/revisions/activate",
        &first_body,
    )
    .await
    .expect("activates the first");

    // Prepared against no parent, while the first one is active: a stale parent.
    let second = revision_artifact(&sealed_document("S3", None, "B"));
    let second_plan = signed_plan(&signing_key(1), std::slice::from_ref(&second));
    let second_body = plan_body(&second_plan, second["id"].as_str().expect("id"));

    let (status, answer) = request(
        address,
        client_with_certificate(&pki),
        "POST",
        "/revisions/prepare",
        &second_body,
    )
    .await
    .expect("answers");

    assert_eq!(status, 409, "{answer}");
    assert!(answer.contains("parent_mismatch"), "{answer}");
}
