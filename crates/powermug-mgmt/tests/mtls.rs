//! The management API over mutual TLS, on loopback: a client without a certificate
//! that chains to the client CA is refused; with one, the routes answer from a real
//! critical store.

use std::net::SocketAddr;
use std::sync::Arc;

use powermug_mgmt::{router, server_config, Api, TlsListener};
use powermug_revision::profile::powermug_v1;
use powermug_store::CriticalStore;
use rcgen::{BasicConstraints, CertificateParams, CertifiedIssuer, IsCa, KeyPair};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::{ClientConfig, RootCertStore};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::TlsConnector;

/// Certificates of the test: a client CA, a server certificate for `localhost`,
/// and a client certificate signed by that CA.
struct Pki {
    ca_der: CertificateDer<'static>,
    server_pem: (String, String),
    client_chain: Vec<CertificateDer<'static>>,
    client_key: PrivateKeyDer<'static>,
    client_ca_pem: String,
}

fn pki() -> Pki {
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

fn client_with_certificate(pki: &Pki) -> Arc<ClientConfig> {
    Arc::new(
        ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
            .with_root_certificates(roots(&pki.ca_der))
            .with_client_auth_cert(pki.client_chain.clone(), pki.client_key.clone_key())
            .expect("client certificate"),
    )
}

fn client_without_certificate(pki: &Pki) -> Arc<ClientConfig> {
    Arc::new(
        ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
            .with_root_certificates(roots(&pki.ca_der))
            .with_no_client_auth(),
    )
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

/// Starts the API on a free loopback port, and returns its address.
async fn serve(pki: &Pki, store: CriticalStore) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("binds");
    let address = listener.local_addr().expect("address");
    let config = server_config(
        pki.server_pem.0.as_bytes(),
        pki.server_pem.1.as_bytes(),
        pki.client_ca_pem.as_bytes(),
    )
    .expect("server TLS");
    let api = Arc::new(Api::new(store, powermug_v1()));
    tokio::spawn(async move {
        let _ = axum::serve(TlsListener::new(listener, config), router(api)).await;
    });
    address
}

/// One request over TLS. Returns the status and the body, or the error of the connection.
async fn request(
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

#[tokio::test]
async fn a_client_without_a_certificate_is_served_nothing() {
    let pki = pki();
    let dir = tempfile::tempdir().expect("temp dir");
    let address = serve(&pki, store(&dir)).await;

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
    let address = serve(&pki, store(&dir)).await;

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
    let address = serve(&pki, store(&dir)).await;

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
    let address = serve(&pki, store(&dir)).await;

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
