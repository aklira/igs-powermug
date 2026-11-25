//! MVP scenario S17 (commands), through the cell as it runs: a selection and an execution sent
//! over the management API (mutual TLS) reach a real station through the cell and its IEC 104
//! adapter. The station confirms and terminates them; the journal records both within two seconds.
//! The partition revision that grants the command is active in the store, as an activation would
//! have left it. The effect on the telemetry (TS and TM of S3) is not checked here.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use ed25519_dalek::pkcs8::{spki::der::pem::LineEnding, EncodePublicKey};
use igs_iec104::process_image::PointValue;
use igs_iec104::server::{Handler, Operation, Refusal, Server, ServerConfig};
use igs_iec104_codec::elements::{QualityFlags, Siq};
use igs_iec104_codec::header::{CommonAddress, InformationObjectAddress};
use powermug_cell::config::CellConfig;
use powermug_cell::serve;
use powermug_command::{CommandWriter, State};
use powermug_revision::canonical::canonical_bytes;
use powermug_revision::partition::partition_content_id;
use powermug_store::CriticalStore;
use rcgen::{BasicConstraints, CertificateParams, CertifiedIssuer, IsCa, KeyPair};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::{ClientConfig, RootCertStore};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

const BIN: &str = env!("CARGO_BIN_EXE_powermug");
const POINT: &str = "S3/TC_005";
const COMMAND_IOA: u32 = 5;
const ISSUER: &str = "scada-demo";

/// Accepts every selection and execution, at once.
struct Accepting;

impl Handler for Accepting {
    fn select(&self, _operation: &Operation) -> Result<(), Refusal> {
        Ok(())
    }

    fn execute(&self, _operation: &Operation) -> Result<(), Refusal> {
        Ok(())
    }
}

/// A station in this process, accepting every selection and execution at once. It runs as a task,
/// so the test binary stays the only process of its cgroup (see `cgroup_delegated`).
async fn start_station() -> (
    SocketAddr,
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<()>,
) {
    let server = Server::bind(
        ServerConfig::new(
            "127.0.0.1:0".parse::<SocketAddr>().expect("addr"),
            CommonAddress::new(1),
        ),
        Accepting,
    )
    .await
    .expect("station binds");
    let addr = server.local_addr().expect("address");
    server
        .handle()
        .add_point(
            InformationObjectAddress::new(COMMAND_IOA).expect("address"),
            PointValue::Single(Siq {
                on: false,
                quality: QualityFlags::default(),
            }),
            false,
        )
        .expect("commandable point");
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let task = tokio::spawn(async move {
        let _ = server
            .run_until(
                async {
                    let _ = stopped.await;
                },
                Duration::ZERO,
            )
            .await;
    });
    (addr, stop, task)
}

/// The PKI of the management interface: a CA, the server certificate, and the client certificate
/// whose common name is the issuer of the commands.
struct Pki {
    ca_pem: String,
    server_pem: String,
    server_key_pem: String,
    client_chain: Vec<CertificateDer<'static>>,
    client_key: PrivateKeyDer<'static>,
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
    let mut client_params = CertificateParams::new(vec!["client".into()]).expect("params");
    client_params.distinguished_name = rcgen::DistinguishedName::new();
    client_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, ISSUER);
    let client = client_params
        .signed_by(&client_key, &ca)
        .expect("client certificate");
    Pki {
        ca_pem: ca.pem(),
        server_pem: server.pem(),
        server_key_pem: server_key.serialize_pem(),
        client_chain: vec![client.der().clone()],
        client_key: PrivateKeyDer::try_from(client_key.serialize_der()).expect("client key"),
    }
}

fn client(pki: &Pki) -> Arc<ClientConfig> {
    let mut roots = RootCertStore::empty();
    for certificate in CertificateDer::pem_slice_iter(pki.ca_pem.as_bytes()) {
        roots
            .add(certificate.expect("a CA certificate"))
            .expect("accepted");
    }
    Arc::new(
        ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
            .with_root_certificates(roots)
            .with_client_auth_cert(pki.client_chain.clone(), pki.client_key.clone_key())
            .expect("client certificate"),
    )
}

async fn request(
    address: SocketAddr,
    client: Arc<ClientConfig>,
    method: &str,
    path: &str,
    body: &str,
) -> Result<(u16, String), String> {
    tokio::time::timeout(
        Duration::from_secs(5),
        exchange(address, client, method, path, body),
    )
    .await
    .map_err(|_| "the request timed out".to_owned())?
}

async fn exchange(
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

/// The partition revision of S3: the session of the station, the commandable point, and the rule
/// that lets `ISSUER` select on it.
fn revision(station: SocketAddr) -> Value {
    let mut document = json!({
        "apiVersion": "powermug/v1",
        "kind": "PartitionRevision",
        "metadata": {"cell": "CELL_DC1", "site": "S3", "id": "placeholder"},
        "spec": {
            "requires": {"contract": "1.0", "capabilities": ["south.iec104"]},
            "sessions": [{
                "id": "s3-ca1",
                "adapter": "iec104-client",
                "common_address": 1,
                "endpoint": {"host": station.ip().to_string(), "port": station.port()},
                "transport_security": {"mode": "none", "justification": "loopback test"}
            }],
            "points": [{
                "id": POINT, "session": "s3-ca1", "address": COMMAND_IOA,
                "class": "Critical", "commandable": true
            }],
            "commands": [{
                "point": POINT, "issuer": ISSUER, "operations": ["select"],
                "values": "any", "min_level": "client"
            }]
        }
    });
    let id = partition_content_id(&document).expect("canonical");
    document["metadata"]["id"] = json!(id);
    document
}

fn config(dir: &std::path::Path, station: SocketAddr, listen: SocketAddr) -> CellConfig {
    let text = json!({
        "cell": "CELL_DC1",
        "telemetry_db": dir.join("telemetry.db"),
        "critical_db": dir.join("critical.db"),
        "management": {
            "listen": listen.to_string(),
            "certificate": dir.join("server.pem"),
            "key": dir.join("server.key"),
            "client_ca": dir.join("ca.pem"),
            "trusted_key": dir.join("trusted.pub.pem")
        },
        "lanes": [{
            "site": "S3",
            "revision": 1,
            "points": [{"id": POINT, "session": 1, "address": COMMAND_IOA, "class": "Critical"}]
        }],
        "adapters": {
            "binary": BIN,
            "directory": dir.join("adapters"),
            "instances": [{
                "session": 1,
                "kind": "iec104",
                "stations": [station.to_string()],
                "common_address": 1
            }]
        },
        "commands": {}
    });
    CellConfig::from_json(&text.to_string()).expect("valid configuration")
}

/// A free loopback port for the management API.
fn free_port() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("binds");
    listener.local_addr().expect("address")
}

/// The cell runs its South instances in cgroups, which need a delegated cgroup v2 subtree. Run the
/// test binary as `systemd-run --user --scope -q -p Delegate=yes -- <binary>`. Elsewhere the test is
/// skipped with a message, or fails when `POWERMUG_REQUIRE_SANDBOX` is set.
fn cgroup_delegated() -> bool {
    let features = powermug_sandbox::features::probe();
    if features.cgroup_delegated && features.memory_controller {
        return true;
    }
    assert!(
        std::env::var_os("POWERMUG_REQUIRE_SANDBOX").is_none(),
        "POWERMUG_REQUIRE_SANDBOX is set but the cgroup subtree is not delegated"
    );
    eprintln!(
        "skipped: needs a delegated cgroup v2 subtree (systemd-run --user --scope -p Delegate=yes)"
    );
    false
}

/// Writes the management PKI and the trusted key of the signed plans into `dir`.
fn write_material(dir: &std::path::Path, pki: &Pki) {
    std::fs::write(dir.join("server.pem"), &pki.server_pem).expect("writes");
    std::fs::write(dir.join("server.key"), &pki.server_key_pem).expect("writes");
    std::fs::write(dir.join("ca.pem"), &pki.ca_pem).expect("writes");
    let trusted = ed25519_dalek::SigningKey::from_bytes(&[7; 32]).verifying_key();
    std::fs::write(
        dir.join("trusted.pub.pem"),
        trusted
            .to_public_key_pem(LineEnding::default())
            .expect("pem"),
    )
    .expect("writes");
}

/// Sends a selection until the cell accepts it into its journal. Returns its command id.
async fn select(listen: SocketAddr, pki: &Pki, revision_id: &str) -> String {
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut attempt = 0_u32;
    while Instant::now() < deadline {
        attempt += 1;
        let id = format!("s17-{attempt}");
        let body = json!({
            "command_id": id, "point": POINT, "operation": "select",
            "value": true, "expected_revision": revision_id
        })
        .to_string();
        let (status, answer) = request(listen, client(pki), "POST", "/commands", &body)
            .await
            .expect("answers");
        if status == 200 && answer.contains("\"READY\"") {
            return id;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    panic!("the selection was not accepted");
}

/// Sends the execution once the station has confirmed the selection. Returns when it was sent.
async fn execute(listen: SocketAddr, pki: &Pki, selected_id: &str) -> Instant {
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        let (status, _) = request(
            listen,
            client(pki),
            "POST",
            &format!("/commands/{selected_id}/execute"),
            &json!({"point": POINT}).to_string(),
        )
        .await
        .expect("answers");
        if status == 200 {
            return Instant::now();
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("the execution was not sent");
}

/// Waits, in the journal, for the command to be terminated, at most two seconds after `since`.
async fn terminated_within_two_seconds(
    dir: &std::path::Path,
    command_id: &str,
    since: Instant,
) -> Option<Duration> {
    let (journal, writer) = CommandWriter::spawn(dir.join("critical.db")).expect("spawns");
    let mut found = None;
    while found.is_none() && since.elapsed() < Duration::from_secs(2) {
        if journal.state(command_id).expect("reads") == Some(State::SuccessUnverified) {
            found = Some(since.elapsed());
        } else {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    drop(journal);
    writer.join();
    found
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_selection_and_an_execution_over_the_api_reach_the_station_and_are_terminated_in_two_seconds(
) {
    if !cgroup_delegated() {
        return;
    }
    let dir = tempfile::tempdir().expect("temp dir");
    let pki = pki();
    write_material(dir.path(), &pki);

    let (station, station_stop, station_task) = start_station().await;

    // The active revision of S3 grants the selection, as an activation would have left it.
    let document = revision(station);
    let revision_id = document["metadata"]["id"].as_str().expect("id").to_owned();
    {
        let mut store = CriticalStore::open(dir.path().join("critical.db")).expect("opens");
        let body = canonical_bytes(&document).expect("canonical");
        store
            .prepare(&revision_id, "S3", None, &body)
            .expect("prepares");
        store.activate(&revision_id, "S3").expect("activates");
    }

    let listen = free_port();
    let config = config(dir.path(), station, listen);
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let cell = tokio::spawn(serve(config, async {
        let _ = stopped.await;
    }));

    let api_ready = {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if request(listen, client(&pki), "GET", "/capabilities", "")
                .await
                .is_ok()
            {
                break true;
            }
            if Instant::now() >= deadline || cell.is_finished() {
                break false;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    };
    if !api_ready {
        let result = cell.await;
        panic!("the cell did not answer its API: {result:?}");
    }

    let selected_id = select(listen, &pki, &revision_id).await;
    let executed_at = execute(listen, &pki, &selected_id).await;
    let terminated_after =
        terminated_within_two_seconds(dir.path(), &selected_id, executed_at).await;

    let _ = stop.send(());
    let outcome = tokio::time::timeout(Duration::from_secs(20), cell)
        .await
        .expect("the cell stops")
        .expect("joins");
    let _ = station_stop.send(());
    let _ = station_task.await;

    assert!(
        terminated_after.is_some(),
        "the execution was not terminated within two seconds"
    );
    assert!(outcome.is_ok(), "{outcome:?}");
}
