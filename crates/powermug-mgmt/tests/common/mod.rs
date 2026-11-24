//! Helpers shared by the management tests: a certificate authority, mutual TLS
//! clients and servers on loopback, a minimal HTTP request over TLS, and signed plans.

#![allow(dead_code, reason = "each test binary uses a subset of these helpers")]

use std::net::SocketAddr;
use std::sync::Arc;

use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use ed25519_dalek::{Signer, SigningKey, VerifyingKey};
use powermug_mgmt::{router, server_config, Api, TlsListener};
use powermug_revision::canonical::{canonical_bytes, content_id, sha256_hex};
use powermug_revision::partition::partition_content_id;
use powermug_revision::profile::powermug_v1;
use powermug_revision::signed::key_id;
use powermug_store::CriticalStore;
use rcgen::{BasicConstraints, CertificateParams, CertifiedIssuer, IsCa, KeyPair};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::{ClientConfig, RootCertStore};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::TlsConnector;

/// Certificates of a test: a client CA, a server certificate for `localhost`, and a
/// client certificate signed by that CA.
pub struct Pki {
    pub ca_der: CertificateDer<'static>,
    pub server_pem: (String, String),
    pub client_chain: Vec<CertificateDer<'static>>,
    pub client_key: PrivateKeyDer<'static>,
    pub client_ca_pem: String,
}

pub fn pki() -> Pki {
    let ca_key = KeyPair::generate().expect("key");
    let mut ca_params = CertificateParams::new(vec!["powermug-test-ca".into()]).expect("params");
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca = CertifiedIssuer::self_signed(ca_params, ca_key).expect("ca");

    let server_key = KeyPair::generate().expect("key");
    let server = CertificateParams::new(vec!["localhost".into()])
        .expect("params")
        .signed_by(&server_key, &ca)
        .expect("server certificate");

    let client_key = KeyPair::generate().expect("key");
    let client = CertificateParams::new(vec!["client".into()])
        .expect("params")
        .signed_by(&client_key, &ca)
        .expect("client certificate");

    Pki {
        ca_der: ca.der().clone(),
        server_pem: (server.pem(), server_key.serialize_pem()),
        client_chain: vec![client.der().clone()],
        client_key: PrivateKeyDer::try_from(client_key.serialize_der()).expect("client key"),
        client_ca_pem: ca.pem(),
    }
}

fn roots(ca: &CertificateDer<'static>) -> Arc<RootCertStore> {
    let mut roots = RootCertStore::empty();
    roots.add(ca.clone()).expect("the CA is accepted");
    Arc::new(roots)
}

pub fn client_with_certificate(pki: &Pki) -> Arc<ClientConfig> {
    Arc::new(
        ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
            .with_root_certificates(roots(&pki.ca_der))
            .with_client_auth_cert(pki.client_chain.clone(), pki.client_key.clone_key())
            .expect("client certificate"),
    )
}

pub fn client_without_certificate(pki: &Pki) -> Arc<ClientConfig> {
    Arc::new(
        ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
            .with_root_certificates(roots(&pki.ca_der))
            .with_no_client_auth(),
    )
}

/// Indicators that report nothing: the source of the tests that do not look at them.
pub struct NoIndicators;

impl powermug_core::IndicatorSource for NoIndicators {
    fn sample(&self, _now_wall_ns: i64) -> Vec<powermug_core::Indicator> {
        Vec::new()
    }
}

/// Starts the API on a free loopback port, over `store`, trusting `trusted_key`.
pub async fn serve(pki: &Pki, store: CriticalStore, trusted_key: VerifyingKey) -> SocketAddr {
    serve_with(pki, store, trusted_key, std::sync::Arc::new(NoIndicators)).await
}

/// As [`serve`], with the cell's command gateway.
pub async fn serve_with_gateway(
    pki: &Pki,
    store: CriticalStore,
    trusted_key: VerifyingKey,
    gateway: std::sync::Arc<dyn powermug_mgmt::CommandGateway>,
) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("binds");
    let address = listener.local_addr().expect("address");
    let config = server_config(
        pki.server_pem.0.as_bytes(),
        pki.server_pem.1.as_bytes(),
        pki.client_ca_pem.as_bytes(),
    )
    .expect("server TLS");
    let api = Arc::new(
        Api::new(
            store,
            powermug_v1(),
            trusted_key,
            std::sync::Arc::new(NoIndicators),
            command_client(),
        )
        .with_commands(gateway),
    );
    tokio::spawn(async move {
        let _ = axum::serve(TlsListener::new(listener, config), router(api)).await;
    });
    address
}

/// A command journal for the API, in a directory kept for the test run.
pub fn command_client() -> powermug_command::CommandClient {
    let dir = tempfile::tempdir().expect("temp dir").keep();
    let (client, writer) =
        powermug_command::CommandWriter::spawn(dir.join("critical.db")).expect("spawns");
    drop(writer);
    client
}

/// As [`serve`], with the indicators of `source`.
pub async fn serve_with(
    pki: &Pki,
    store: CriticalStore,
    trusted_key: VerifyingKey,
    source: std::sync::Arc<dyn powermug_core::IndicatorSource>,
) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("binds");
    let address = listener.local_addr().expect("address");
    let config = server_config(
        pki.server_pem.0.as_bytes(),
        pki.server_pem.1.as_bytes(),
        pki.client_ca_pem.as_bytes(),
    )
    .expect("server TLS");
    let api = Arc::new(Api::new(
        store,
        powermug_v1(),
        trusted_key,
        source,
        command_client(),
    ));
    tokio::spawn(async move {
        let _ = axum::serve(TlsListener::new(listener, config), router(api)).await;
    });
    address
}

/// One request over TLS. Returns the status and the body, or the error of the connection.
pub async fn request(
    address: SocketAddr,
    client: Arc<ClientConfig>,
    method: &str,
    path: &str,
    body: &str,
) -> Result<(u16, String), String> {
    let tcp = TcpStream::connect(address)
        .await
        .map_err(|e| e.to_string())?;
    let name = ServerName::try_from("localhost").map_err(|e| e.to_string())?;
    let mut tls = TlsConnector::from(client)
        .connect(name, tcp)
        .await
        .map_err(|e| e.to_string())?;
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    tls.write_all(request.as_bytes())
        .await
        .map_err(|e| e.to_string())?;
    let mut response = Vec::new();
    tls.read_to_end(&mut response)
        .await
        .map_err(|e| e.to_string())?;
    let text = String::from_utf8_lossy(&response).into_owned();
    let status = text
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse::<u16>().ok())
        .ok_or_else(|| "no status line".to_owned())?;
    let body = text
        .split_once("\r\n\r\n")
        .map(|(_, body)| body.to_owned())
        .unwrap_or_default();
    Ok((status, body))
}

/// A signing key from a seed, for the tests only.
pub fn signing_key(seed: u8) -> SigningKey {
    SigningKey::from_bytes(&[seed; 32])
}

/// A partition document of §10.3 with its content identifier, as a producer writes it.
pub fn sealed_document(site: &str, parent: Option<&str>, class: &str) -> Value {
    let mut document = json!({
        "apiVersion": "powermug/v1",
        "kind": "PartitionRevision",
        "metadata": { "cell": "CELL_DC1", "site": site, "id": "placeholder" },
        "spec": {
            "requires": { "contract": "1.0", "capabilities": ["south.iec104"] },
            "sessions": [{
                "id": format!("{}-ca3", site.to_lowercase()),
                "adapter": "iec104-client",
                "common_address": 3,
                "endpoint": { "host": "10.3.0.3", "port": 2404 },
                "transport_security": { "mode": "none", "justification": "isolated process network" }
            }],
            "points": [{
                "id": format!("{site}/TM_001"),
                "session": format!("{}-ca3", site.to_lowercase()),
                "address": 101,
                "class": class
            }]
        }
    });
    if let Some(parent) = parent {
        document["metadata"]["parent"] = json!(parent);
    }
    let id = partition_content_id(&document).expect("canonical");
    document["metadata"]["id"] = json!(id);
    document
}

/// A `ibtnm.target-revision` artifact carrying the Powermug document, with its content
/// identifier as gridloop computes it (the artifact without its `id`).
pub fn revision_artifact(document: &Value) -> Value {
    let mut artifact = json!({
        "artifact_type": "ibtnm.target-revision",
        "powermug_document": document,
    });
    let id = content_id("revision", &artifact).expect("canonical");
    artifact["id"] = json!(id);
    artifact
}

/// A signed plan (§10.6) carrying `artifacts`, signed with `key`.
pub fn signed_plan(key: &SigningKey, artifacts: &[Value]) -> Value {
    let mut payload = json!({ "artifacts": artifacts });
    let plan_id = content_id("plan", &payload).expect("canonical");
    payload["id"] = json!(plan_id);

    let mut listed: Vec<Value> = artifacts
        .iter()
        .map(|artifact| {
            let digest = sha256_hex(&canonical_bytes(artifact).expect("canonical"));
            json!({ "id": artifact["id"], "sha256": digest })
        })
        .collect();
    listed.sort_by(|a, b| a["id"].as_str().cmp(&b["id"].as_str()));

    let manifest = json!({
        "algorithm": "Ed25519",
        "key_id": key_id(&key.verifying_key()),
        "plan_id": plan_id,
        "plan_sha256": sha256_hex(&canonical_bytes(&payload).expect("canonical")),
        "artifacts": listed,
    });
    let signature = key.sign(&canonical_bytes(&manifest).expect("canonical"));
    json!({
        "artifact_type": "ibtnm.signed-plan",
        "schema_version": "1.0.0",
        "manifest": manifest,
        "payload": payload,
        "signature": STANDARD.encode(signature.to_bytes()),
    })
}

/// An empty store in `dir`.
pub fn empty_store(dir: &tempfile::TempDir) -> CriticalStore {
    CriticalStore::open(dir.path().join("critical.db")).expect("opens")
}
