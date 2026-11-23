//! The operating mode over the management API (FUN-OPS-006, SPECIFICATIONS 9.3): read, set,
//! audited, refused for an unknown mode or an unnamed actor.

mod common;

use common::*;
use serde_json::Value as Json;

fn body(text: &str) -> Json {
    serde_json::from_str(text).expect("json")
}

#[tokio::test]
async fn the_mode_is_read_set_and_refused_when_unknown_or_unattributed() {
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
        r#"{"mode": "maintenance", "actor": "ops-jdoe"}"#,
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
        r#"{"mode": "maintenance", "actor": "ops-other"}"#,
    )
    .await
    .expect("answers");
    assert_eq!(body(&text)["changed"], false);

    let (status, text) = request(
        address,
        client(),
        "POST",
        "/mode",
        r#"{"mode": "debug", "actor": "ops-jdoe"}"#,
    )
    .await
    .expect("answers");
    assert_eq!(status, 400);
    assert_eq!(body(&text)["error"], "unknown_mode");

    let (status, text) = request(
        address,
        client(),
        "POST",
        "/mode",
        r#"{"mode": "shadow", "actor": "  "}"#,
    )
    .await
    .expect("answers");
    assert_eq!(status, 400);
    assert_eq!(body(&text)["error"], "actor_required");
}
