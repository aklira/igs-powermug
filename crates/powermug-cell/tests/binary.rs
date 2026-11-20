//! The cell binary, run as a process: it opens its stores, serves the management API
//! over mutual TLS, and stops on interruption.

use std::net::{SocketAddr, TcpListener as StdListener};
use std::path::Path;
use std::process::{Child, Command};
use std::sync::Arc;
use std::time::{Duration, Instant};

use ed25519_dalek::pkcs8::{spki::der::pem::LineEnding, EncodePublicKey};
use ed25519_dalek::SigningKey;
use rcgen::{BasicConstraints, CertificateParams, CertifiedIssuer, IsCa, KeyPair};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::{ClientConfig, RootCertStore};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

struct Material {
    ca_der: CertificateDer<'static>,
    client_chain: Vec<CertificateDer<'static>>,
    client_key: PrivateKeyDer<'static>,
}

/// Writes the TLS and trusted-key material of a cell into `dir`, and returns the client side.
fn material(dir: &Path) -> Material {
    let ca_key = KeyPair::generate().expect("key");
    let mut ca_params = CertificateParams::new(vec!["powermug-cell-ca".into()]).expect("params");
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca = CertifiedIssuer::self_signed(ca_params, ca_key).expect("ca");

    let server_key = KeyPair::generate().expect("key");
    let server = CertificateParams::new(vec!["localhost".into()])
        .expect("params")
        .signed_by(&server_key, &ca)
        .expect("server");
    let client_key = KeyPair::generate().expect("key");
    let client = CertificateParams::new(vec!["client".into()])
        .expect("params")
        .signed_by(&client_key, &ca)
        .expect("client");

    std::fs::write(dir.join("server.pem"), server.pem()).expect("writes");
    std::fs::write(dir.join("server.key"), server_key.serialize_pem()).expect("writes");
    std::fs::write(dir.join("ca.pem"), ca.pem()).expect("writes");
    let trusted = SigningKey::from_bytes(&[9u8; 32]).verifying_key();
    std::fs::write(
        dir.join("trusted.pub.pem"),
        trusted.to_public_key_pem(LineEnding::LF).expect("encodes"),
    )
    .expect("writes");

    Material {
        ca_der: ca.der().clone(),
        client_chain: vec![client.der().clone()],
        client_key: PrivateKeyDer::try_from(client_key.serialize_der()).expect("client key"),
    }
}

fn free_address() -> SocketAddr {
    let listener = StdListener::bind("127.0.0.1:0").expect("binds");
    listener.local_addr().expect("address")
}

fn write_config(dir: &Path, listen: SocketAddr) -> std::path::PathBuf {
    let config = format!(
        r#"{{
  "cell": "CELL_TEST",
  "telemetry_db": "{}",
  "critical_db": "{}",
  "management": {{
    "listen": "{listen}",
    "certificate": "{}",
    "key": "{}",
    "client_ca": "{}",
    "trusted_key": "{}"
  }},
  "lanes": [{{"site": "S3", "revision": 1, "points": [
    {{"id": "S3/TM_001", "session": 1, "address": 1, "class": "A"}},
    {{"id": "S3/TM_002", "session": 1, "address": 2, "class": "B"}}
  ]}}]
}}"#,
        dir.join("telemetry.db").display(),
        dir.join("critical.db").display(),
        dir.join("server.pem").display(),
        dir.join("server.key").display(),
        dir.join("ca.pem").display(),
        dir.join("trusted.pub.pem").display(),
    );
    let path = dir.join("cell.json");
    std::fs::write(&path, config).expect("writes");
    path
}

async fn get(
    address: SocketAddr,
    client: Arc<ClientConfig>,
    path: &str,
) -> Result<(u16, String), String> {
    let tcp = TcpStream::connect(address)
        .await
        .map_err(|e| e.to_string())?;
    let name = ServerName::try_from("localhost").map_err(|e| e.to_string())?;
    let mut tls = TlsConnector::from(client)
        .connect(name, tcp)
        .await
        .map_err(|e| e.to_string())?;
    let request = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
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
        .map(|(_, b)| b.to_owned())
        .unwrap_or_default();
    Ok((status, body))
}

fn client(material: &Material) -> Arc<ClientConfig> {
    let mut roots = RootCertStore::empty();
    roots
        .add(material.ca_der.clone())
        .expect("the CA is accepted");
    Arc::new(
        ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
            .with_root_certificates(Arc::new(roots))
            .with_client_auth_cert(
                material.client_chain.clone(),
                material.client_key.clone_key(),
            )
            .expect("client certificate"),
    )
}

/// Waits until the cell answers, or the time is up.
async fn wait_for_cell(
    address: SocketAddr,
    client: Arc<ClientConfig>,
) -> Result<(u16, String), String> {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        match get(address, Arc::clone(&client), "/capabilities").await {
            Ok(answer) => return Ok(answer),
            Err(err) if Instant::now() >= deadline => return Err(err),
            Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
}

struct Running(Child);

impl Drop for Running {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test]
async fn the_cell_serves_its_management_api_over_mutual_tls() {
    let dir = tempfile::tempdir().expect("temp dir");
    let material = material(dir.path());
    let listen = free_address();
    let config = write_config(dir.path(), listen);

    let child = Command::new(env!("CARGO_BIN_EXE_powermug-cell"))
        .arg("--config")
        .arg(&config)
        .spawn()
        .expect("the cell starts");
    let _running = Running(child);

    let (status, body) = wait_for_cell(listen, client(&material))
        .await
        .expect("the cell answers");
    assert_eq!(status, 200);
    assert!(body.contains("powermug@1"), "{body}");

    // Nothing is reported yet: the lane was opened on an empty journal, no adapter has
    // reported a session, so the indicators are unknown rather than up.
    let (status, body) = get(listen, client(&material), "/indicators")
        .await
        .expect("answers");
    assert_eq!(status, 200);
    let document: serde_json::Value = serde_json::from_str(&body).expect("json");
    let connection = document["indicators"]
        .as_array()
        .expect("array")
        .iter()
        .find(|i| i["name"] == "iec104_connection_up")
        .expect("the session is listed");
    assert!(connection["value"].is_null(), "{body}");
}

#[tokio::test]
async fn a_client_without_a_certificate_is_refused_by_the_cell() {
    let dir = tempfile::tempdir().expect("temp dir");
    let material = material(dir.path());
    let listen = free_address();
    let config = write_config(dir.path(), listen);
    let _running = Running(
        Command::new(env!("CARGO_BIN_EXE_powermug-cell"))
            .arg("--config")
            .arg(&config)
            .spawn()
            .expect("the cell starts"),
    );
    wait_for_cell(listen, client(&material))
        .await
        .expect("the cell answers");

    let mut roots = RootCertStore::empty();
    roots
        .add(material.ca_der.clone())
        .expect("the CA is accepted");
    let anonymous = Arc::new(
        ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
            .with_root_certificates(Arc::new(roots))
            .with_no_client_auth(),
    );
    assert!(get(listen, anonymous, "/capabilities").await.is_err());
}

#[test]
fn a_configuration_that_does_not_load_stops_the_cell_with_status_two() {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("bad.json");
    std::fs::write(&path, "{}").expect("writes");
    let status = Command::new(env!("CARGO_BIN_EXE_powermug-cell"))
        .arg("--config")
        .arg(&path)
        .status()
        .expect("runs");
    assert_eq!(status.code(), Some(2));
}
