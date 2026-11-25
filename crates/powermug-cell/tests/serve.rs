//! The cell as it runs (`serve`): a command left ready by an earlier run is decided when the cell
//! opens, and the head of the audit chain is anchored outside the critical store before the cell
//! stops (SPECIFICATIONS 9.7, NFR-SEC-008).

use std::path::Path;
use std::time::Duration;

use ed25519_dalek::pkcs8::{spki::der::pem::LineEnding, EncodePublicKey};
use powermug_cell::config::CellConfig;
use powermug_cell::serve;
use powermug_command::audit::to_hex;
use powermug_command::{CommandWriter, Granularity, NewCommand, State};
use rcgen::{BasicConstraints, CertificateParams, CertifiedIssuer, IsCa, KeyPair};

/// The PKI of the management interface, and the trusted key of the signed plans, in `dir`.
fn write_material(dir: &Path) {
    let ca_key = KeyPair::generate().expect("key");
    let mut ca_params = CertificateParams::new(vec!["powermug-test-ca".into()]).expect("params");
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca = CertifiedIssuer::self_signed(ca_params, ca_key).expect("ca");
    let server_key = KeyPair::generate().expect("key");
    let server = CertificateParams::new(vec!["localhost".into()])
        .expect("params")
        .signed_by(&server_key, &ca)
        .expect("server certificate");
    std::fs::write(dir.join("server.pem"), server.pem()).expect("writes");
    std::fs::write(dir.join("server.key"), server_key.serialize_pem()).expect("writes");
    std::fs::write(dir.join("ca.pem"), ca.pem()).expect("writes");
    let trusted = ed25519_dalek::SigningKey::from_bytes(&[7; 32]).verifying_key();
    std::fs::write(
        dir.join("trusted.pub.pem"),
        trusted
            .to_public_key_pem(LineEnding::default())
            .expect("pem"),
    )
    .expect("writes");
}

fn config(dir: &Path) -> CellConfig {
    let text = format!(
        r#"{{
  "cell": "CELL_DC1",
  "telemetry_db": "{telemetry}",
  "critical_db": "{critical}",
  "storage": {{"shared_quota": true}},
  "management": {{
    "listen": "127.0.0.1:0",
    "certificate": "{certificate}",
    "key": "{key}",
    "client_ca": "{client_ca}",
    "trusted_key": "{trusted}"
  }},
  "lanes": [{{"site": "S3", "revision": 1, "points": [
    {{"id": "S3/TC_101", "session": 1, "address": 5, "class": "Critical"}}]}}],
  "commands": {{"audit_anchor": {{"path": "{anchor}", "every_s": 3600}}}}
}}"#,
        telemetry = dir.join("telemetry.db").display(),
        critical = dir.join("critical.db").display(),
        certificate = dir.join("server.pem").display(),
        key = dir.join("server.key").display(),
        client_ca = dir.join("ca.pem").display(),
        trusted = dir.join("trusted.pub.pem").display(),
        anchor = dir.join("anchor").join("head.json").display(),
    );
    CellConfig::from_json(&text).expect("valid")
}

#[tokio::test]
async fn the_cell_decides_its_ready_commands_at_start_and_anchors_its_head_at_stop() {
    let dir = tempfile::tempdir().expect("temp dir");
    write_material(dir.path());
    std::fs::create_dir(dir.path().join("anchor")).expect("the anchor directory");
    let config = config(dir.path());

    // An earlier run left a command ready, owned at epoch 1.
    let (client, writer) = CommandWriter::spawn(&config.critical_db).expect("spawns");
    client
        .submit(
            NewCommand {
                command_id: "c1".into(),
                identity: "scada-demo".into(),
                identity_level: Granularity::Client,
                point: "S3/TC_101".into(),
                operation: "direct_execute".into(),
                value: "true".into(),
                expires_ns: i64::MAX,
                expected_revision: String::new(),
                owner_epoch: 1,
                ha_term: 1,
            },
            1_000,
        )
        .expect("submits");
    for next in [State::Validated, State::DurablyAccepted, State::Ready] {
        client.move_to("c1", next, 1_000, "").expect("moves");
    }
    drop(client);
    writer.join();

    serve(config.clone(), async {
        tokio::time::sleep(Duration::from_millis(500)).await;
    })
    .await
    .expect("serves until its shutdown");

    let (client, writer) = CommandWriter::spawn(&config.critical_db).expect("spawns");
    assert_eq!(client.state("c1").expect("reads"), Some(State::Failed));

    let anchor: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(dir.path().join("anchor").join("head.json")).expect("reads"),
    )
    .expect("json");
    let head = client.audit_head().expect("head");
    assert_eq!(anchor["head"], to_hex(&head));
    drop(client);
    writer.join();
}
