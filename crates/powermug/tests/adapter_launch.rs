//! A South instance started by the cell (§6.1 to §6.4, PM-ADP-003), against a real
//! controlled station of `igs-iec104` on loopback.
//!
//! The cell configuration names the instance; `powermug_cell::adapters` starts the real
//! `powermug` binary as spawner, which applies the protocol-adapter sandbox and runs the
//! IEC 104 adapter. The values must reach the lane through the contract.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use igs_iec104::process_image::PointValue;
use igs_iec104::server::{Handler, Operation, Refusal, Server, ServerConfig, ServerHandle};
use igs_iec104_codec::elements::{Qds, QualityFlags, ShortFloat, Siq};
use igs_iec104_codec::header::{CommonAddress, InformationObjectAddress};
use powermug_cell::adapters::{instances, provision, supervise, Outcome};
use powermug_cell::config::InstanceLimits;
use powermug_cell::CellConfig;
use powermug_core::supervisor::Policy;
use powermug_core::{Lane, SessionId};
use powermug_pivot::{PointIndex, Value};
use powermug_sandbox::features::probe;
use powermug_store::TelemetryLog;
use tokio::sync::watch;

const BIN: &str = env!("CARGO_BIN_EXE_powermug");

/// Refuses every command: the test only reads the station.
struct ReadOnly;

impl Handler for ReadOnly {
    fn select(&self, _: &Operation) -> Result<(), Refusal> {
        Err(Refusal::UnknownAddress)
    }

    fn execute(&self, _: &Operation) -> Result<(), Refusal> {
        Err(Refusal::UnknownAddress)
    }
}

/// A controlled station with a status point (ON) and a measurement (0.25), until stopped.
struct Station {
    addr: SocketAddr,
    stop: tokio::sync::oneshot::Sender<()>,
    task: tokio::task::JoinHandle<()>,
}

async fn start_station() -> Station {
    let server = Server::bind(
        ServerConfig::new(
            "127.0.0.1:0".parse::<SocketAddr>().expect("addr"),
            CommonAddress::new(1),
        ),
        ReadOnly,
    )
    .await
    .expect("station binds");
    let addr = server.local_addr().expect("address");
    let handle: ServerHandle = server.handle();
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
            InformationObjectAddress::new(2).expect("address"),
            PointValue::Float(ShortFloat::from_f32(0.25), Qds::default()),
            false,
        )
        .expect("measurement");
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
    Station { addr, stop, task }
}

/// The configuration of a cell with one lane of two points and one IEC 104 instance.
fn configuration(dir: &std::path::Path, station: SocketAddr, binary: &str) -> CellConfig {
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
            "points": [
                {"id": "S3/TM_001", "session": 1, "address": 1, "class": "A"},
                {"id": "S3/TM_002", "session": 1, "address": 2, "class": "B"}
            ]
        }],
        "adapters": {
            "binary": binary,
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

fn lane_of(config: &CellConfig, dir: &std::path::Path) -> Arc<Mutex<Lane>> {
    let log = TelemetryLog::open(dir.join("lane-telemetry.db")).expect("opens");
    let table = config.lanes[0].table().expect("table");
    Arc::new(Mutex::new(Lane::new(table, log, 0)))
}

async fn wait_until(mut condition: impl FnMut() -> bool, seconds: u64) -> bool {
    let deadline = Instant::now() + Duration::from_secs(seconds);
    while Instant::now() < deadline {
        if condition() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    condition()
}

#[tokio::test]
async fn an_instance_started_by_the_cell_feeds_its_lane() {
    let dir = tempfile::tempdir().expect("temp dir");
    let station = start_station().await;
    let config = configuration(dir.path(), station.addr, BIN);
    let lane = lane_of(&config, dir.path());
    let instance = instances(&config).expect("instances").remove(0);

    let (stop, stopped) = watch::channel(false);
    let supervisor = tokio::spawn(supervise(
        instance,
        Arc::clone(&lane),
        Policy::default(),
        stopped,
        None,
    ));

    let watched = Arc::clone(&lane);
    let fed = wait_until(
        || {
            let guard = watched.lock().expect("lock");
            guard.image().get(PointIndex(0)).is_some() && guard.image().get(PointIndex(1)).is_some()
        },
        30,
    )
    .await;

    let _ = stop.send(true);
    let outcome = tokio::time::timeout(Duration::from_secs(15), supervisor)
        .await
        .expect("the supervisor stops with the cell")
        .expect("joins");
    let _ = station.stop.send(());
    let _ = station.task.await;

    assert!(fed, "the lane did not receive both points");
    assert_eq!(outcome, Outcome::Stopped);
    let guard = lane.lock().expect("lock");
    let status = guard.image().get(PointIndex(0)).expect("status");
    assert_eq!(status.value, Value::Bool(true));
    let measurement = guard.image().get(PointIndex(1)).expect("measurement");
    assert_eq!(measurement.value, Value::Double(0.25));
    assert!(!guard.is_lost(SessionId(1)));
}

#[tokio::test]
async fn an_instance_that_cannot_start_degrades_its_session_and_is_quarantined() {
    let dir = tempfile::tempdir().expect("temp dir");
    let station = start_station().await;
    // The binary does not exist: every start fails, and the budget runs out.
    let missing = dir.path().join("no-such-powermug");
    let config = configuration(dir.path(), station.addr, missing.to_str().expect("utf-8"));
    let lane = lane_of(&config, dir.path());
    let instance = instances(&config).expect("instances").remove(0);
    let policy = Policy {
        initial_backoff: Duration::from_millis(10),
        max_backoff: Duration::from_millis(10),
        budget: 2,
        budget_window: Duration::from_secs(600),
        stable_after: Duration::from_secs(60),
    };

    let (_stop, stopped) = watch::channel(false);
    let outcome = tokio::time::timeout(
        Duration::from_secs(30),
        supervise(instance, Arc::clone(&lane), policy, stopped, None),
    )
    .await
    .expect("the budget runs out");

    let _ = station.stop.send(());
    let _ = station.task.await;

    assert_eq!(outcome, Outcome::Quarantined);
    assert!(lane.lock().expect("lock").is_lost(SessionId(1)));
}

/// The process ids of the adapter processes of one test. The tests share this process, and run
/// in parallel: the parent is this process, and the command line names the test's directory.
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
            // /proc/<pid>/stat: `pid (name) state ppid …`; the name may contain spaces.
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

#[tokio::test]
async fn a_killed_adapter_degrades_its_session_and_is_restarted() {
    let dir = tempfile::tempdir().expect("temp dir");
    let station = start_station().await;
    let config = configuration(dir.path(), station.addr, BIN);
    let lane = lane_of(&config, dir.path());
    let instance = instances(&config).expect("instances").remove(0);

    let (stop, stopped) = watch::channel(false);
    let supervisor = tokio::spawn(supervise(
        instance,
        Arc::clone(&lane),
        Policy::default(),
        stopped,
        None,
    ));

    let watched = Arc::clone(&lane);
    let fed = wait_until(
        || {
            watched
                .lock()
                .expect("lock")
                .image()
                .get(PointIndex(0))
                .is_some()
        },
        30,
    )
    .await;
    assert!(fed, "the lane did not receive a value before the crash");

    // The adapter dies by SIGKILL: the core must degrade its session, then restart it.
    let pids = adapter_pids(dir.path());
    assert_eq!(pids.len(), 1, "one adapter process expected: {pids:?}");
    let killed = std::process::Command::new("kill")
        .arg("-9")
        .arg(pids[0].to_string())
        .status()
        .expect("kill runs");
    assert!(killed.success());

    let degraded = wait_until(|| watched.lock().expect("lock").is_lost(SessionId(1)), 20).await;
    assert!(degraded, "the session was not degraded after the crash");

    // A restarted adapter reports its session and interrogates the station again.
    let recovered = wait_until(
        || {
            let guard = watched.lock().expect("lock");
            !guard.is_lost(SessionId(1))
                && guard
                    .image()
                    .get(PointIndex(0))
                    .is_some_and(|slot| slot.quality == powermug_pivot::Quality::GOOD)
        },
        30,
    )
    .await;

    let _ = stop.send(true);
    let outcome = tokio::time::timeout(Duration::from_secs(15), supervisor)
        .await
        .expect("the supervisor stops with the cell")
        .expect("joins");
    let _ = station.stop.send(());
    let _ = station.task.await;

    let state = {
        let guard = lane.lock().expect("lock");
        format!(
            "lost={} quality={:?} adapters={:?}",
            guard.is_lost(SessionId(1)),
            guard.image().get(PointIndex(0)).map(|slot| slot.quality),
            adapter_pids(dir.path())
        )
    };
    assert!(
        recovered,
        "the restarted adapter did not restore the values: {state}"
    );
    assert_eq!(outcome, Outcome::Stopped);
}

/// The cgroup checks need a delegated cgroup v2 subtree. Run the test binary as
/// `systemd-run --user --scope -q -p Delegate=yes -- <binary>`. Elsewhere the test is
/// skipped with a message, or fails when `POWERMUG_REQUIRE_SANDBOX` is set.
fn cgroup_delegated() -> bool {
    let features = probe();
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

#[tokio::test]
async fn an_instance_runs_in_its_cgroup_under_its_limits() {
    if !cgroup_delegated() {
        return;
    }
    let dir = tempfile::tempdir().expect("temp dir");
    let station = start_station().await;
    let config = configuration(dir.path(), station.addr, BIN);
    let lane = lane_of(&config, dir.path());
    let mut planned = instances(&config).expect("instances");
    let limits = InstanceLimits {
        memory_bytes: 64 * 1024 * 1024,
        pids: 32,
        cpu_percent: 50,
    };
    provision(&mut planned, limits).expect("cgroups created");
    let instance = planned.remove(0);
    let group = instance.cgroup.clone().expect("the instance has a cgroup");

    let (stop, stopped) = watch::channel(false);
    let supervisor = tokio::spawn(supervise(
        instance,
        Arc::clone(&lane),
        Policy::default(),
        stopped,
        None,
    ));
    let watched = Arc::clone(&lane);
    let fed = wait_until(
        || {
            watched
                .lock()
                .expect("lock")
                .image()
                .get(PointIndex(0))
                .is_some()
        },
        30,
    )
    .await;
    let pids = adapter_pids(dir.path());
    let read = |file: &str| {
        std::fs::read_to_string(group.join(file))
            .unwrap_or_default()
            .trim()
            .to_owned()
    };
    let members = read("cgroup.procs");
    let checks = (
        read("memory.max"),
        read("memory.swap.max"),
        read("pids.max"),
        read("cpu.max"),
    );

    let _ = stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(15), supervisor).await;
    let _ = station.stop.send(());
    let _ = station.task.await;

    assert!(fed, "the lane did not receive a value");
    assert_eq!(pids.len(), 1, "one adapter process expected: {pids:?}");
    assert!(
        members.lines().any(|pid| pid == pids[0].to_string()),
        "the adapter is not in its cgroup: {members:?}"
    );
    assert_eq!(
        checks,
        (
            (64 * 1024 * 1024u64).to_string(),
            "0".to_owned(),
            "32".to_owned(),
            "50000 100000".to_owned()
        ),
        "memory.max, memory.swap.max, pids.max, cpu.max"
    );
}

#[tokio::test]
async fn a_healthy_instance_keeps_running_past_the_health_window() {
    let dir = tempfile::tempdir().expect("temp dir");
    let station = start_station().await;
    let config = configuration(dir.path(), station.addr, BIN);
    let lane = lane_of(&config, dir.path());
    let instance = instances(&config).expect("instances").remove(0);

    let (stop, stopped) = watch::channel(false);
    let supervisor = tokio::spawn(supervise(
        instance,
        Arc::clone(&lane),
        Policy::default(),
        stopped,
        None,
    ));
    let watched = Arc::clone(&lane);
    let fed = wait_until(
        || {
            watched
                .lock()
                .expect("lock")
                .image()
                .get(PointIndex(0))
                .is_some()
        },
        30,
    )
    .await;
    let first = adapter_pids(dir.path());

    // Well past the three-second window: the reports keep the instance alive.
    tokio::time::sleep(Duration::from_secs(5)).await;
    let later = adapter_pids(dir.path());
    let lost = lane.lock().expect("lock").is_lost(SessionId(1));

    let _ = stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(15), supervisor).await;
    let _ = station.stop.send(());
    let _ = station.task.await;

    assert!(fed, "the lane did not receive a value");
    assert_eq!(first.len(), 1, "{first:?}");
    assert_eq!(first, later, "the healthy instance was restarted");
    assert!(!lost, "the session was lost while the instance was healthy");
}

#[tokio::test]
async fn a_silent_instance_is_killed_and_restarted() {
    let dir = tempfile::tempdir().expect("temp dir");
    let station = start_station().await;
    let config = configuration(dir.path(), station.addr, BIN);
    let lane = lane_of(&config, dir.path());
    let instance = instances(&config).expect("instances").remove(0);

    let (stop, stopped) = watch::channel(false);
    let supervisor = tokio::spawn(supervise(
        instance,
        Arc::clone(&lane),
        Policy::default(),
        stopped,
        None,
    ));
    let watched = Arc::clone(&lane);
    let fed = wait_until(
        || {
            watched
                .lock()
                .expect("lock")
                .image()
                .get(PointIndex(0))
                .is_some()
        },
        30,
    )
    .await;
    assert!(fed, "the lane did not receive a value before the freeze");
    let frozen = adapter_pids(dir.path());
    assert_eq!(frozen.len(), 1, "{frozen:?}");

    // A stopped process sends no health report: after the window the cell kills it.
    let halted = std::process::Command::new("kill")
        .arg("-STOP")
        .arg(frozen[0].to_string())
        .status()
        .expect("kill runs");
    assert!(halted.success());

    let degraded = wait_until(|| watched.lock().expect("lock").is_lost(SessionId(1)), 20).await;
    let recovered = wait_until(
        || {
            let guard = watched.lock().expect("lock");
            !guard.is_lost(SessionId(1))
                && guard
                    .image()
                    .get(PointIndex(0))
                    .is_some_and(|slot| slot.quality == powermug_pivot::Quality::GOOD)
        },
        30,
    )
    .await;
    let replacement = adapter_pids(dir.path());

    let _ = stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(15), supervisor).await;
    let _ = station.stop.send(());
    let _ = station.task.await;

    assert!(degraded, "the silent instance's session was not degraded");
    assert!(
        recovered,
        "the restarted instance did not restore the values"
    );
    assert_eq!(replacement.len(), 1, "{replacement:?}");
    assert_ne!(replacement, frozen, "the frozen process was not replaced");
}

#[tokio::test]
async fn a_station_that_stops_degrades_its_points_in_under_two_seconds() {
    // ACC-013: the loss of a South session degrades every attached point in less than 2 s.
    let dir = tempfile::tempdir().expect("temp dir");
    let station = start_station().await;
    let config = configuration(dir.path(), station.addr, BIN);
    let lane = lane_of(&config, dir.path());
    let instance = instances(&config).expect("instances").remove(0);

    let (stop, stopped) = watch::channel(false);
    let supervisor = tokio::spawn(supervise(
        instance,
        Arc::clone(&lane),
        Policy::default(),
        stopped,
        None,
    ));
    let watched = Arc::clone(&lane);
    let fed = wait_until(
        || {
            watched
                .lock()
                .expect("lock")
                .image()
                .get(PointIndex(0))
                .is_some()
        },
        30,
    )
    .await;
    assert!(fed, "the lane did not receive a value before the stop");

    let started = Instant::now();
    let _ = station.stop.send(());
    let _ = station.task.await;
    let mut elapsed = None;
    while started.elapsed() < Duration::from_secs(10) {
        if watched.lock().expect("lock").is_lost(SessionId(1)) {
            elapsed = Some(started.elapsed());
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let _ = stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(15), supervisor).await;

    let elapsed = elapsed.expect("the session was not degraded within 10 s");
    eprintln!("session degraded {elapsed:?} after the station stopped");
    assert!(
        elapsed < Duration::from_secs(2),
        "degraded after {elapsed:?}"
    );
}

#[tokio::test]
async fn a_takeover_restarts_the_instance_with_the_new_epoch() {
    // A takeover of the partition while the instance runs: the cell replaces the process, so
    // that the adapter is activated at the new owner epoch (PM-CMD-005).
    use powermug_cell::adapters::CommandLink;
    use powermug_cell::commands::{CommandRoute, Dispatcher};
    use powermug_command::authorization::Acl;
    use powermug_command::ownership::Ownership;
    use powermug_command::CommandWriter;

    let dir = tempfile::tempdir().expect("temp dir");
    let station = start_station().await;
    let config = configuration(dir.path(), station.addr, BIN);
    let lane = lane_of(&config, dir.path());
    let instance = instances(&config).expect("instances").remove(0);

    let (commands, writer) =
        CommandWriter::spawn(dir.path().join("critical.db")).expect("journal spawns");
    let route = CommandRoute::new();
    let link = CommandLink {
        dispatcher: std::sync::Arc::new(std::sync::Mutex::new(Dispatcher::new(
            commands.clone(),
            Acl::new(),
            Ownership::new(),
            route.clone(),
        ))),
        route,
    };

    let (stop, stopped) = watch::channel(false);
    let supervisor = tokio::spawn(supervise(
        instance,
        Arc::clone(&lane),
        Policy::default(),
        stopped,
        Some(link.clone()),
    ));
    let watched = Arc::clone(&lane);
    let fed = wait_until(
        || {
            watched
                .lock()
                .expect("lock")
                .image()
                .get(PointIndex(0))
                .is_some()
        },
        30,
    )
    .await;
    assert!(fed, "the lane did not receive a value before the takeover");
    let before = adapter_pids(dir.path());
    assert_eq!(before.len(), 1, "{before:?}");

    let epoch = link.dispatcher.lock().expect("lock").takeover("S3");
    assert_eq!(epoch, 1);
    // The partition of the instance is its site: the takeover is of that partition.
    let restarted = wait_until(
        || {
            let now = adapter_pids(dir.path());
            now.len() == 1 && now != before
        },
        30,
    )
    .await;
    let after = adapter_pids(dir.path());

    let _ = stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(15), supervisor).await;
    let _ = station.stop.send(());
    let _ = station.task.await;
    drop(link);
    drop(commands);
    writer.join();

    assert!(restarted, "the instance was not restarted: {after:?}");
}
