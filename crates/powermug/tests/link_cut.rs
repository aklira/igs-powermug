//! MVP scenario S19 and ACC-003 at the link: the connection between the adapter and the station is
//! cut after the execution left and before the station's answer. The adapter reports the command
//! in doubt, the core classes it `IN_DOUBT`, and nothing is sent again. A proxy between the adapter
//! and the station carries the traffic; it hides the answers, then cuts the connection.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use igs_iec104::process_image::PointValue;
use igs_iec104::server::{Handler, Operation, Refusal, Server, ServerConfig};
use igs_iec104_codec::elements::{QualityFlags, Siq};
use igs_iec104_codec::header::{CommonAddress, InformationObjectAddress};
use powermug_cell::adapters::{instances, supervise, CommandLink, Outcome};
use powermug_cell::commands::{CommandRoute, Dispatcher, Request};
use powermug_cell::config::acl_of_rules;
use powermug_cell::CellConfig;
use powermug_command::ownership::Ownership;
use powermug_command::{CommandWriter, Granularity, State};
use powermug_contract::proto::v1 as pb;
use powermug_core::supervisor::Policy;
use powermug_core::Lane;
use powermug_revision::partition::CommandRule;
use powermug_store::TelemetryLog;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

const BIN: &str = env!("CARGO_BIN_EXE_powermug");
const COMMAND_IOA: u32 = 5;

/// Accepts every selection. It records the executions it receives; it answers them at once.
struct Recording {
    entered: Arc<AtomicBool>,
    executions: Arc<AtomicUsize>,
}

impl Handler for Recording {
    fn select(&self, _operation: &Operation) -> Result<(), Refusal> {
        Ok(())
    }

    fn execute(&self, _operation: &Operation) -> Result<(), Refusal> {
        self.executions.fetch_add(1, Ordering::SeqCst);
        self.entered.store(true, Ordering::SeqCst);
        Ok(())
    }
}

/// A TCP proxy between the adapter and the station. While `hide` is set, the station's bytes do
/// not reach the adapter. `cut` drops every connection and stops accepting.
struct Proxy {
    address: SocketAddr,
    hide: Arc<AtomicBool>,
    accepting: tokio::task::JoinHandle<()>,
    connections: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
}

impl Proxy {
    fn cut(&self) {
        self.accepting.abort();
        for connection in self.connections.lock().expect("lock").drain(..) {
            connection.abort();
        }
    }
}

async fn start_proxy(station: SocketAddr) -> Proxy {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("binds");
    let address = listener.local_addr().expect("address");
    let hide = Arc::new(AtomicBool::new(false));
    let connections: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>> =
        Arc::new(Mutex::new(Vec::new()));
    let accepting = {
        let hide = Arc::clone(&hide);
        let connections = Arc::clone(&connections);
        tokio::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                let Ok(server) = TcpStream::connect(station).await else {
                    continue;
                };
                let (mut from_adapter, mut to_adapter) = client.into_split();
                let (mut from_station, mut to_station) = server.into_split();
                let hide = Arc::clone(&hide);
                let connection = tokio::spawn(async move {
                    let up = async {
                        let mut buffer = [0_u8; 4096];
                        while let Ok(read) = from_adapter.read(&mut buffer).await {
                            if read == 0 || to_station.write_all(&buffer[..read]).await.is_err() {
                                break;
                            }
                        }
                    };
                    let down = async {
                        let mut buffer = [0_u8; 4096];
                        while let Ok(read) = from_station.read(&mut buffer).await {
                            if read == 0 {
                                break;
                            }
                            if !hide.load(Ordering::SeqCst)
                                && to_adapter.write_all(&buffer[..read]).await.is_err()
                            {
                                break;
                            }
                        }
                    };
                    tokio::join!(up, down);
                });
                connections.lock().expect("lock").push(connection);
            }
        })
    };
    Proxy {
        address,
        hide,
        accepting,
        connections,
    }
}

fn start_station(
    entered: Arc<AtomicBool>,
    executions: Arc<AtomicUsize>,
) -> (
    SocketAddr,
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<()>,
) {
    let runtime = tokio::runtime::Handle::current();
    let (addr_tx, addr_rx) = std::sync::mpsc::channel();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let task = runtime.spawn(async move {
        let server = Server::bind(
            ServerConfig::new(
                "127.0.0.1:0".parse::<SocketAddr>().expect("addr"),
                CommonAddress::new(1),
            ),
            Recording {
                entered,
                executions,
            },
        )
        .await
        .expect("station binds");
        addr_tx
            .send(server.local_addr().expect("address"))
            .expect("sends");
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
        let _ = server
            .run_until(
                async {
                    let _ = stopped.await;
                },
                Duration::ZERO,
            )
            .await;
    });
    let addr = addr_rx.recv().expect("the station's address");
    (addr, stop, task)
}

fn configuration(dir: &std::path::Path, station: SocketAddr) -> CellConfig {
    let text = serde_json::json!({
        "cell": "CELL_TEST",
        "telemetry_db": dir.join("telemetry.db"),
        "critical_db": dir.join("critical.db"),
        "management": {
            "listen": "127.0.0.1:0",
            "certificate": "unused.pem",
            "key": "unused.key",
            "client_ca": "unused.pem",
            "trusted_key": "unused.pub.pem"
        },
        "lanes": [{
            "site": "S3",
            "revision": 7,
            "points": [{"id": "S3/TC_005", "session": 1, "address": COMMAND_IOA, "class": "Critical"}]
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
        }
    });
    CellConfig::from_json(&text.to_string()).expect("valid configuration")
}

fn rule() -> CommandRule {
    CommandRule {
        point: "S3/TC_005".into(),
        issuer: "scada-demo".into(),
        operations: vec!["select".into(), "direct_execute".into()],
        values: "any".into(),
        min_level: "link".into(),
    }
}

fn request(command_id: &str, operation: &str, select: bool) -> Request {
    Request {
        command_id: command_id.into(),
        identity: "scada-demo".into(),
        identity_level: Granularity::Link,
        point: "S3/TC_005".into(),
        operation: operation.into(),
        partition: "S3".into(),
        address: COMMAND_IOA,
        kind: pb::CommandKind::Single,
        on: true,
        select,
        expires_ns: i64::MAX,
        expected_revision: "r1".into(),
    }
}

async fn wait_until(mut condition: impl FnMut() -> bool, seconds: u64) -> bool {
    let deadline = Instant::now() + Duration::from_secs(seconds);
    while Instant::now() < deadline {
        if condition() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    condition()
}

fn now_ns() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_nanos()),
    )
    .unwrap_or(i64::MAX)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_link_cut_after_the_execution_left_puts_the_command_in_doubt_and_never_replays_it() {
    let dir = tempfile::tempdir().expect("temp dir");
    let entered = Arc::new(AtomicBool::new(false));
    let executions = Arc::new(AtomicUsize::new(0));
    let (station, station_stop, station_task) =
        start_station(Arc::clone(&entered), Arc::clone(&executions));
    let proxy = start_proxy(station).await;
    let config = configuration(dir.path(), proxy.address);
    let lane = Arc::new(Mutex::new(Lane::new(
        config.lanes[0].table().expect("table"),
        TelemetryLog::open(dir.path().join("lane-telemetry.db")).expect("opens"),
        0,
    )));
    let instance = instances(&config).expect("instances").remove(0);

    let (client, writer) = CommandWriter::spawn(dir.path().join("critical.db")).expect("spawns");
    let mut ownership = Ownership::new();
    ownership.takeover("S3");
    let route = CommandRoute::new();
    let mut dispatcher = Dispatcher::new(
        client.clone(),
        acl_of_rules(&[rule()]),
        ownership,
        route.clone(),
    );
    dispatcher.set_active_revision(Some("r1".into()));
    let dispatcher = Arc::new(Mutex::new(dispatcher));
    let link = CommandLink {
        dispatcher: Arc::clone(&dispatcher),
        route,
    };
    let (stop, stopped) = watch::channel(false);
    let supervisor = tokio::spawn(supervise(
        instance,
        Arc::clone(&lane),
        Policy::default(),
        stopped,
        Some(link),
    ));

    let watched = Arc::clone(&lane);
    assert!(
        wait_until(
            || watched
                .lock()
                .expect("lock")
                .image()
                .get(powermug_pivot::PointIndex(0))
                .is_some(),
            30,
        )
        .await,
        "the adapter did not connect through the proxy"
    );

    // The selection is accepted by the station and confirmed to the core.
    dispatcher
        .lock()
        .expect("lock")
        .submit(&request("c1", "select", true), now_ns())
        .expect("submits");
    assert!(
        wait_until(
            || client.state("c1").expect("reads") == Some(State::SelectConfirmed),
            30
        )
        .await,
        "the selection was not confirmed"
    );

    // The execution leaves, and its answer is hidden from the adapter.
    proxy.hide.store(true, Ordering::SeqCst);
    dispatcher
        .lock()
        .expect("lock")
        .execute("c1", "scada-demo", now_ns())
        .expect("executes");
    assert!(
        wait_until(|| entered.load(Ordering::SeqCst), 30).await,
        "the execution did not reach the station"
    );

    // The link is cut before the answer.
    proxy.cut();
    let in_doubt = wait_until(
        || client.state("c1").expect("reads") == Some(State::InDoubt),
        30,
    )
    .await;

    // Nothing is sent again: the station received the execution once.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let received = executions.load(Ordering::SeqCst);

    let _ = stop.send(true);
    let outcome = tokio::time::timeout(Duration::from_secs(20), supervisor)
        .await
        .expect("the supervisor stops with the cell")
        .expect("joins");
    let _ = station_stop.send(());
    let _ = station_task.await;

    assert!(in_doubt, "the command was not classed in doubt");
    assert_eq!(received, 1, "the execution was received once, never again");
    assert_eq!(outcome, Outcome::Stopped);
    drop(dispatcher);
    drop(client);
    writer.join();
}
