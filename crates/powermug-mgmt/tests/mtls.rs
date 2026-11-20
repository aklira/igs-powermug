//! The management API over mutual TLS, on loopback: a client without a certificate
//! that chains to the client CA is refused; with one, the routes answer from a real
//! critical store.

mod common;

use common::*;
use ed25519_dalek::VerifyingKey;
use powermug_store::CriticalStore;

/// The key the API trusts in the tests of this file.
fn trusted() -> VerifyingKey {
    signing_key(1).verifying_key()
}

/// The store with two revisions of partition S3, the second one active.
fn store(dir: &tempfile::TempDir) -> CriticalStore {
    let mut store = CriticalStore::open(dir.path().join("critical.db")).expect("opens");
    store.prepare("r1", "S3", None, b"1").expect("prepares r1");
    store.activate("r1", "S3").expect("activates r1");
    store
        .prepare("r2", "S3", Some("r1"), b"2")
        .expect("prepares r2");
    store.activate("r2", "S3").expect("activates r2");
    store
}

#[tokio::test]
async fn a_client_without_a_certificate_is_served_nothing() {
    let pki = pki();
    let dir = tempfile::tempdir().expect("temp dir");
    let address = serve(&pki, store(&dir), trusted()).await;

    let answer = request(
        address,
        client_without_certificate(&pki),
        "GET",
        "/capabilities",
        "",
    )
    .await;

    match answer {
        Err(_) => {}
        Ok((status, body)) => panic!("served without a certificate: {status} {body}"),
    }
}

#[tokio::test]
async fn capabilities_name_the_profile_and_do_not_claim_the_sandbox() {
    let pki = pki();
    let dir = tempfile::tempdir().expect("temp dir");
    let address = serve(&pki, store(&dir), trusted()).await;

    let (status, body) = request(
        address,
        client_with_certificate(&pki),
        "GET",
        "/capabilities",
        "",
    )
    .await
    .expect("answers");

    assert_eq!(status, 200);
    let document: serde_json::Value = serde_json::from_str(&body).expect("json");
    assert_eq!(document["target_profile"], "powermug@1");
    assert_eq!(document["sandbox"], "unverified");
    assert_eq!(document["capabilities"]["south.iec104"], "1.0");
}

#[tokio::test]
async fn state_shows_the_active_and_the_previous_revision() {
    let pki = pki();
    let dir = tempfile::tempdir().expect("temp dir");
    let address = serve(&pki, store(&dir), trusted()).await;

    let (status, body) = request(address, client_with_certificate(&pki), "GET", "/state", "")
        .await
        .expect("answers");

    assert_eq!(status, 200);
    let document: serde_json::Value = serde_json::from_str(&body).expect("json");
    assert_eq!(document["scopes"][0]["scope"], "S3");
    assert_eq!(document["scopes"][0]["active"], "r2");
    assert_eq!(document["scopes"][0]["previous"], "r1");
}

#[tokio::test]
async fn rollback_restores_the_previous_revision_and_unknown_ids_are_404() {
    let pki = pki();
    let dir = tempfile::tempdir().expect("temp dir");
    let address = serve(&pki, store(&dir), trusted()).await;

    let (status, body) = request(
        address,
        client_with_certificate(&pki),
        "POST",
        "/revisions/rollback",
        r#"{"site":"S3"}"#,
    )
    .await
    .expect("answers");
    assert_eq!(status, 200, "{body}");
    let document: serde_json::Value = serde_json::from_str(&body).expect("json");
    assert_eq!(document["active"], "r1");

    let (status, _) = request(
        address,
        client_with_certificate(&pki),
        "GET",
        "/revisions/r1",
        "",
    )
    .await
    .expect("answers");
    assert_eq!(status, 200);
    let (status, body) = request(
        address,
        client_with_certificate(&pki),
        "GET",
        "/revisions/nope",
        "",
    )
    .await
    .expect("answers");
    assert_eq!(status, 404, "{body}");
}
