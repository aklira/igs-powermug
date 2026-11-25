//! The adapter process dies while a command is in flight (SPECIFICATIONS 9.7, PM-CMD-002). The
//! station has accepted the selection and has received the execution, but never answers it: the
//! adapter is killed at that moment. The core must classify the command in doubt, never send it
//! again, and keep the adapter's restart from repeating it.

use std::net::SocketAddr;
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
use tokio::sync::watch;

const BIN: &str = env!("CARGO_BIN_EXE_powermug");
const COMMAND_IOA: u32 = 3;
const STATION_DIR: &str = "POWERMUG_STATION_DIR";

/// The station, in its own process: it accepts every selection, and takes the execution, records
/// it, and blocks for four seconds before it would answer. The test kills the adapter meanwhile.
struct StallingExecution {
    dir: std::path::PathBuf,
}

impl Handler for StallingExecution {
    fn select(&self, _operation: &Operation) -> Result<(), Refusal> {
        Ok(())
    }

    fn execute(&self, _operation: &Operation) -> Result<(), Refusal> {
        record(&self.dir, "executions", "x");
        record(&self.dir, "entered", "x");
        std::thread::sleep(Duration::from_secs(4));
        Ok(())
    }
}

fn record(dir: &std::path::Path, name: &str, line: &str) {
    use std::io::Write;
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(dir.join(name))
        .expect("opens the record");
    writeln!(file, "{line}").expect("writes the record");
}

fn count(dir: &std::path::Path, name: &str) -> usize {
    std::fs::read_to_string(dir.join(name)).map_or(0, |text| text.lines().count())
}

/// The child: the station of the test, run in its own process so that its stall cannot block the
/// core of the test. It writes its address, and serves until the test kills it.
#[test]
fn station_child() {
    let Ok(dir) = std::env::var(STATION_DIR) else {
        return;
    };
    let dir = std::path::PathBuf::from(dir);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let server = Server::bind(
            ServerConfig::new(
                "127.0.0.1:0".parse::<SocketAddr>().expect("addr"),
                CommonAddress::new(1),
            ),
            StallingExecution { dir: dir.clone() },
        )
        .await
        .expect("station binds");
        let addr = server.local_addr().expect("address");
        let handle = server.handle();
        handle
            .add_point(
                InformationObjectAddress::new(1).expect("address"),
                PointValue::Single(Siq {
                    on: true,
                    quality: QualityFlags::default(),
                }),
                false,
            )
            .expect("status point");
        handle
            .add_point(
                InformationObjectAddress::new(COMMAND_IOA).expect("address"),
                PointValue::Single(Siq {
                    on: false,
                    quality: QualityFlags::default(),
                }),
                false,
            )
            .expect("commandable point");
        std::fs::write(dir.join("station.addr"), addr.to_string()).expect("writes the address");
        let _ = server
            .run_until(std::future::pending::<()>(), Duration::ZERO)
            .await;
    });
}

/// Starts the station in a child process and returns its address and the child. The child is
/// killed when the station does not start in time.
fn start_station(dir: &std::path::Path) -> (SocketAddr, std::process::Child) {
    let mut child = std::process::Command::new(std::env::current_exe().expect("test binary"))
        .args([
            "--exact",
            "station_child",
            "--test-threads=1",
            "--nocapture",
        ])
        .env(STATION_DIR, dir)
        .spawn()
        .expect("starts the station");
    let deadline = Instant::now() + Duration::from_secs(20);
    let address = loop {
        let found = std::fs::read_to_string(dir.join("station.addr"))
            .ok()
            .and_then(|text| text.parse::<SocketAddr>().ok());
        if let Some(address) = found {
            break Some(address);
        }
        if Instant::now() >= deadline {
            break None;
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    let Some(address) = address else {
        let _ = child.kill();
        let _ = child.wait();
        panic!("the station did not start");
    };
    (address, child)
}

fn configuration(dir: &std::path::Path, station: SocketAddr) -> CellConfig {
    let text = serde_json::json!({
        "cell": "CELL_TEST",
        "telemetry_db": dir.join("telemetry.db"),
        "critical_db": dir.join("critical.db"),
        "storage": {"shared_quota": true},
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
            "points": [
                {"id": "S3/TM_001", "session": 1, "address": 1, "class": "A"},
                {"id": "S3/TC_003", "session": 1, "address": COMMAND_IOA, "class": "Critical"}
            ]
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

fn adapter_pids(directory: &std::path::Path) -> Vec<u32> {
    let marker = directory.display().to_string();
    let me = std::process::id();
    let Ok(entries) = std::fs::read_dir("/proc") else {
        return Vec::new();
    };
    entries
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let pid: u32 = entry.file_name().to_str()?.parse().ok()?;
            let stat = std::fs::read_to_string(entry.path().join("stat")).ok()?;
            let after_name = stat.rsplit_once(')')?.1;
            let parent: u32 = after_name.split_whitespace().nth(1)?.parse().ok()?;
            if parent != me {
                return None;
            }
            let cmdline = std::fs::read(entry.path().join("cmdline")).ok()?;
            let args: Vec<&[u8]> = cmdline.split(|byte| *byte == 0).collect();
            let adapter = args
                .windows(2)
                .any(|pair| pair == [b"adapter".as_slice(), b"iec104".as_slice()]);
            let ours = String::from_utf8_lossy(&cmdline).contains(&marker);
            (adapter && ours).then_some(pid)
        })
        .collect()
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

fn rule() -> CommandRule {
    CommandRule {
        point: "S3/TC_003".into(),
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
        point: "S3/TC_003".into(),
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_command_in_flight_when_the_adapter_dies_is_in_doubt_and_never_sent_again() {
    let dir = tempfile::tempdir().expect("temp dir");
    let (station_addr, mut station) = start_station(dir.path());
    let config = configuration(dir.path(), station_addr);
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
        std::sync::Arc::new(tokio::sync::Notify::new()),
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
        "the adapter did not connect"
    );

    // The selection is accepted by the station and confirmed to the core.
    let selected = dispatcher
        .lock()
        .expect("lock")
        .submit(&request("c1", "select", true), now_ns())
        .expect("submits");
    // The emission is open until the adapter answers: the command stays READY meanwhile.
    assert_eq!(selected, State::Ready);
    assert!(
        wait_until(
            || client.state("c1").expect("reads") == Some(State::SelectConfirmed),
            30
        )
        .await,
        "the selection was not confirmed"
    );

    // The execution reaches the station, which takes it and never answers.
    dispatcher
        .lock()
        .expect("lock")
        .execute("c1", "scada-demo", now_ns())
        .expect("executes");
    assert!(
        wait_until(|| count(dir.path(), "entered") > 0, 30).await,
        "the execution did not reach the station"
    );

    let pids = adapter_pids(dir.path());
    assert_eq!(pids.len(), 1, "one adapter process expected: {pids:?}");
    let killed = std::process::Command::new("kill")
        .arg("-9")
        .arg(pids[0].to_string())
        .status()
        .expect("kill runs");
    assert!(killed.success());

    let in_doubt = wait_until(
        || client.state("c1").expect("reads") == Some(State::InDoubt),
        30,
    )
    .await;

    // Let the station's execution finish, and the restarted adapter run: nothing is sent again.
    tokio::time::sleep(Duration::from_secs(5)).await;
    let executions = count(dir.path(), "executions");

    let _ = stop.send(true);
    let outcome = tokio::time::timeout(Duration::from_secs(20), supervisor)
        .await
        .expect("the supervisor stops with the cell")
        .expect("joins");
    let _ = station.kill();
    let _ = station.wait();

    assert!(in_doubt, "the command in flight was not classed in doubt");
    assert_eq!(executions, 1, "the execution was sent once, never again");
    assert_eq!(client.state("c1").expect("reads"), Some(State::InDoubt));
    assert_eq!(outcome, Outcome::Stopped);

    drop(dispatcher);
    drop(client);
    writer.join();
}

fn now_ns() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_nanos()),
    )
    .unwrap_or(i64::MAX)
}
