//! The operating mode over the management API (FUN-OPS-006, SPECIFICATIONS 9.3): read, set,
//! audited under the client certificate, refused for an unknown mode or another actor.

mod common;

use common::*;
use serde_json::Value as Json;

fn body(text: &str) -> Json {
    serde_json::from_str(text).expect("json")
}

#[tokio::test]
async fn the_mode_is_read_and_set_under_the_certificate_and_other_actors_are_refused() {
    let pki = pki();
    let dir = tempfile::tempdir().expect("temp dir");
    let address = serve(&pki, empty_store(&dir), signing_key(1).verifying_key()).await;
    let client = || client_with_certificate(&pki);

    let (status, text) = request(address, client(), "GET", "/mode", "")
        .await
        .expect("answers");
    assert_eq!(status, 200);
    assert_eq!(body(&text)["mode"], "normal");

    let (status, text) = request(
        address,
        client(),
        "POST",
        "/mode",
        r#"{"mode": "maintenance"}"#,
    )
    .await
    .expect("answers");
    assert_eq!(status, 200);
    assert_eq!(body(&text)["mode"], "maintenance");
    assert_eq!(body(&text)["changed"], true);

    let (_, text) = request(address, client(), "GET", "/mode", "")
        .await
        .expect("answers");
    assert_eq!(body(&text)["mode"], "maintenance");

    // The same mode again changes nothing.
    let (_, text) = request(
        address,
        client(),
        "POST",
        "/mode",
        r#"{"mode": "maintenance", "actor": "client"}"#,
    )
    .await
    .expect("answers");
    assert_eq!(body(&text)["changed"], false);

    let (status, text) = request(address, client(), "POST", "/mode", r#"{"mode": "debug"}"#)
        .await
        .expect("answers");
    assert_eq!(status, 400);
    assert_eq!(body(&text)["error"], "unknown_mode");

    let (status, text) = request(
        address,
        client(),
        "POST",
        "/mode",
        r#"{"mode": "shadow", "actor": "ops-jdoe"}"#,
    )
    .await
    .expect("answers");
    assert_eq!(status, 400);
    assert_eq!(body(&text)["error"], "actor_mismatch");
}

#[tokio::test]
async fn a_mode_change_by_a_certificate_without_a_common_name_is_refused() {
    let pki = pki_with_client_subject(None);
    let dir = tempfile::tempdir().expect("temp dir");
    let address = serve(&pki, empty_store(&dir), signing_key(1).verifying_key()).await;

    let (status, text) = request(
        address,
        client_with_certificate(&pki),
        "POST",
        "/mode",
        r#"{"mode": "maintenance"}"#,
    )
    .await
    .expect("answers");
    assert_eq!(status, 401);
    assert_eq!(body(&text)["error"], "unidentified_client");

    let (_, text) = request(address, client_with_certificate(&pki), "GET", "/mode", "")
        .await
        .expect("answers");
    assert_eq!(body(&text)["mode"], "normal");
}
