//! South instances run by the cell (§6.1 to §6.4, PM-ADP-003).
//!
//! Each configured instance is supervised by one task. A cycle binds the instance's socket
//! and writes its settings, then starts `powermug spawner --protocol`, which applies the
//! protocol-adapter sandbox and replaces itself with `powermug adapter iec104`. The core
//! serves the instance's contract on the socket until the process exits. An exit degrades
//! the session on the lane, and the supervisor restarts the instance under its policy;
//! once the restart budget is spent the instance is quarantined.
//!
//! Not yet: the instance's own uid and gid (PM-ADP-007), so the process keeps the cell's
//! identity, and its cgroup (§6.4). The socket directory is not handed to another uid
//! either. These need the decision recorded in PROGRESS.md.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use powermug_adapter_iec104::{MappedPoint, Settings};
use powermug_contract::handshake::{Offer, Requirement, Version};
use powermug_contract::peer::ExpectedPeer;
use powermug_contract::proto::v1 as pb;
use powermug_contract::server::{serve_instance_commanded, serve_instance_observed};
use powermug_contract::service::CoreConfig;
use powermug_core::supervisor::{Decision, Policy, Supervisor};
use powermug_core::{Clock, Lane, LaneSink, SessionId};
use sha2::{Digest, Sha256};
use tokio::net::UnixListener;
use tokio::process::Command;
use tokio::sync::{mpsc, oneshot, watch, Notify};

use powermug_sandbox::cgroup::{own_cgroup, Cgroup, CpuMax, Limits};
use powermug_sandbox::SandboxError;

use crate::commands::{channel as command_channel, CommandRoute, Dispatcher};
use crate::config::{instance_directory, CellConfig, ConfigError, InstanceLimits};

/// One South instance, as the configuration describes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Instance {
    /// The session the instance serves.
    pub session: u32,
    /// The `powermug` binary.
    pub binary: PathBuf,
    /// The instance's directory: its settings and its socket.
    pub directory: PathBuf,
    /// Index of the lane that holds the session's points, in the configuration.
    pub lane: usize,
    /// The settings written for the adapter. The artifact digest is filled at start.
    pub settings: Settings,
    /// The instance's cgroup, once [`provision`] has created it.
    pub cgroup: Option<PathBuf>,
}

/// Creates the cgroup of each instance under the cell's delegated subtree (§6.4).
///
/// The cell moves itself into a leaf of its subtree, so that the subtree can hold the
/// controllers, then creates one group per instance with its limits. A cell whose subtree
/// is not delegated refuses to run the instances (PM-ADP-005, production profile).
///
/// # Errors
///
/// Returns [`SandboxError`] when the subtree is not delegated or a limit file is refused.
pub fn provision(instances: &mut [Instance], limits: InstanceLimits) -> Result<(), SandboxError> {
    if instances.is_empty() {
        return Ok(());
    }
    let cell = Cgroup::delegate_subtree(&own_cgroup()?, &["cpu", "memory", "pids"])?;
    for instance in instances {
        let group = cell.create_child(
            &format!("adapter-{}", instance.session),
            Limits {
                memory_max: Some(limits.memory_bytes),
                pids_max: Some(limits.pids),
                no_swap: true,
                cpu_max: Some(CpuMax {
                    // One core is 100 000 µs of CPU per 100 000 µs period.
                    quota_us: u64::from(limits.cpu_percent) * 1000,
                    period_us: 100_000,
                }),
            },
        )?;
        instance.cgroup = Some(group.path().to_path_buf());
    }
    Ok(())
}

/// How the supervision of an instance ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// The cell is stopping.
    Stopped,
    /// The restart budget is spent: the instance stays down until a new revision.
    Quarantined,
}

/// The instances of a configuration, in the order of the configuration.
///
/// # Errors
///
/// Returns [`ConfigError::AdapterWithoutLane`] when a session names no lane. A checked
/// configuration never has one.
pub fn instances(config: &CellConfig) -> Result<Vec<Instance>, ConfigError> {
    let Some(adapters) = &config.adapters else {
        return Ok(Vec::new());
    };
    adapters
        .instances
        .iter()
        .map(|item| {
            let (index, lane) = config
                .lanes
                .iter()
                .enumerate()
                .find(|(_, lane)| lane.points.iter().any(|p| p.session == item.session))
                .ok_or(ConfigError::AdapterWithoutLane(item.session))?;
            let directory = instance_directory(adapters, item.session);
            let points = lane
                .points
                .iter()
                .filter(|p| p.session == item.session)
                .map(|p| MappedPoint {
                    address: p.address,
                    point: p.id.clone(),
                })
                .collect();
            Ok(Instance {
                session: item.session,
                binary: adapters.binary.clone(),
                directory: directory.clone(),
                lane: index,
                settings: Settings {
                    // Not yet taken from the partition's ownership: see PROGRESS.md.
                    owner_epoch: 0,
                    stations: item.stations.clone(),
                    common_address: item.common_address,
                    core_socket: directory.join("core.sock"),
                    site_id: lane.site.clone(),
                    session_id: item.session,
                    revision: lane.revision,
                    points,
                    artifact_sha256: String::new(),
                    window_events: item.window_events,
                },
                cgroup: None,
            })
        })
        .collect()
}

/// What the core requires of an IEC 60870-5-104 instance: a controlling station that does
/// general interrogation, on pivot 1.0.
fn offer() -> Offer {
    Offer {
        pivot: Version { major: 1, minor: 0 },
        requirements: vec![Requirement {
            protocol: "iec104".into(),
            role: pb::Role::Client,
            function: "general_interrogation".into(),
        }],
        command_variants: Vec::new(),
    }
}

/// Supervises one instance until `stop` is set or its budget is spent.
/// The command path of one session: the dispatcher that owns its commands, and the route to the
/// adapter's command stream, attached while the stream is open.
#[derive(Clone, Debug)]
pub struct CommandLink {
    pub dispatcher: Arc<Mutex<Dispatcher>>,
    pub route: CommandRoute,
}

pub async fn supervise(
    instance: Instance,
    lane: Arc<Mutex<Lane>>,
    policy: Policy,
    mut stop: watch::Receiver<bool>,
    commands: Option<CommandLink>,
) -> Outcome {
    let clock = Clock::new();
    let mut supervisor = Supervisor::new(policy);
    loop {
        if *stop.borrow() || supervisor.started(Instant::now()).is_err() {
            return Outcome::Stopped;
        }
        let reason = match run_once(&instance, &lane, &clock, &mut stop, commands.as_ref()).await {
            Cycle::Stopped => return Outcome::Stopped,
            Cycle::Ended(reason) => reason,
        };
        eprintln!("adapter of session {}: {reason}", instance.session);
        // PM-ADP-003 step 2: the session's points degrade at once.
        let _ = lane
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .session_lost(SessionId(instance.session), &reason, clock.now());
        match supervisor.crashed(Instant::now(), jitter()) {
            Ok(Decision::RestartAt(at)) => {
                tokio::select! {
                    () = tokio::time::sleep_until(tokio::time::Instant::from_std(at)) => {}
                    _ = stop.changed() => return Outcome::Stopped,
                }
                if supervisor.restart_due(at).is_err() {
                    return Outcome::Stopped;
                }
            }
            Ok(Decision::Quarantine) => {
                eprintln!("adapter of session {}: quarantined", instance.session);
                return Outcome::Quarantined;
            }
            Err(_) => return Outcome::Stopped,
        }
    }
}

enum Cycle {
    Stopped,
    Ended(String),
}

/// Silence after which an instance is dead: three missed reports of one second (§6.3).
const HEALTH_WINDOW: std::time::Duration = std::time::Duration::from_secs(3);

/// How one start of the process ended.
enum End {
    /// The process exited, or could not be waited for.
    Exited(String),
    /// No health report within the window: the process is killed (§6.3).
    Silent,
    /// The core's server ended before the process did.
    ServerFailed(String),
    /// The cell is stopping.
    Stopped,
}

/// One start of the process and its contract, until the process ends or the cell stops.
async fn run_once(
    instance: &Instance,
    lane: &Arc<Mutex<Lane>>,
    clock: &Clock,
    stop: &mut watch::Receiver<bool>,
    commands: Option<&CommandLink>,
) -> Cycle {
    let digest = match artifact_sha256(&instance.binary) {
        Ok(digest) => digest,
        Err(err) => {
            return Cycle::Ended(format!("cannot read {}: {err}", instance.binary.display()))
        }
    };
    let mut settings = instance.settings.clone();
    settings.artifact_sha256 = digest;
    let settings_path = instance.directory.join("settings.json");
    let written = std::fs::create_dir_all(&instance.directory).and_then(|()| {
        let text = serde_json::to_string_pretty(&settings).map_err(std::io::Error::other)?;
        std::fs::write(&settings_path, text)
    });
    if let Err(err) = written {
        return Cycle::Ended(format!("cannot write the settings: {err}"));
    }

    // The socket is bound before the process starts: its connection waits in the backlog.
    let socket = &settings.core_socket;
    let _ = std::fs::remove_file(socket);
    let listener = match UnixListener::bind(socket) {
        Ok(listener) => listener,
        Err(err) => return Cycle::Ended(format!("cannot bind {}: {err}", socket.display())),
    };

    let mut command = Command::new(&instance.binary);
    command.arg("spawner").arg("--protocol");
    if let Some(cgroup) = &instance.cgroup {
        command.arg("--cgroup").arg(cgroup);
    }
    for path in read_paths(&instance.binary) {
        command.arg("--read").arg(path);
    }
    command
        .arg("--write")
        .arg(&instance.directory)
        .arg("--")
        .arg(&instance.binary)
        .args(["adapter", "iec104", "--settings"])
        .arg(&settings_path)
        .kill_on_drop(true);
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(err) => return Cycle::Ended(format!("cannot start the spawner: {err}")),
    };
    let Some(pid) = child.id().and_then(|pid| i32::try_from(pid).ok()) else {
        let _ = child.start_kill();
        return Cycle::Ended("the spawner has no process id".into());
    };

    // The adapter execs from the spawner, so it keeps this process id.
    let config = CoreConfig {
        offer: offer(),
        expected_peer: ExpectedPeer {
            uid: nix::unistd::getuid().as_raw(),
            pid,
        },
    };
    let sink = Box::new(LaneSink::new(Arc::clone(lane), *clock));
    let heartbeat = Arc::new(Notify::new());
    let (done, done_rx) = oneshot::channel::<()>();
    let shutdown = async move {
        let _ = done_rx.await;
    };
    let (mut server, pump) = start_server(config, sink, listener, &heartbeat, shutdown, commands);

    // The silence window restarts with each health report (§6.3). Three missed reports
    // make the instance a crash, and the process is killed.
    let silence = tokio::time::sleep(HEALTH_WINDOW);
    tokio::pin!(silence);
    let end = loop {
        tokio::select! {
            status = child.wait() => break End::Exited(match status {
                Ok(status) => format!("the adapter process ended ({status})"),
                Err(err) => format!("the adapter process cannot be waited for: {err}"),
            }),
            _ = stop.changed() => break End::Stopped,
            finished = &mut server => break End::ServerFailed(match finished {
                Ok(Ok(())) => "the core's server ended".to_owned(),
                Ok(Err(err)) => format!("the core's server failed: {err}"),
                Err(err) => format!("the core's server task failed: {err}"),
            }),
            () = heartbeat.notified() => silence.as_mut().reset(tokio::time::Instant::now() + HEALTH_WINDOW),
            () = &mut silence => break End::Silent,
        }
    };
    // The process goes with its contract: a stopped server leaves no channel behind.
    let _ = child.start_kill();
    let _ = child.wait().await;
    if !matches!(end, End::ServerFailed(_)) {
        let _ = done.send(());
        let _ = server.await;
    }
    release_commands(commands, pump).await;
    match end {
        End::Exited(reason) | End::ServerFailed(reason) => Cycle::Ended(reason),
        End::Silent => Cycle::Ended("no health report for 3 s: the process is killed".into()),
        End::Stopped => Cycle::Stopped,
    }
}

/// The directories the spawner grants read access to: the binary and the system libraries.
fn read_paths(binary: &Path) -> Vec<PathBuf> {
    let mut paths: Vec<PathBuf> = binary.parent().map(Path::to_path_buf).into_iter().collect();
    paths.extend(
        ["/usr", "/lib", "/lib64", "/etc/ld.so.cache", "/proc"]
            .into_iter()
            .map(PathBuf::from),
    );
    paths.retain(|path| path.exists());
    paths
}

/// The SHA-256 of the adapter binary, lowercase hex: the artifact the instance runs.
fn artifact_sha256(binary: &Path) -> std::io::Result<String> {
    let bytes = std::fs::read(binary)?;
    let mut hex = String::with_capacity(64);
    for byte in Sha256::digest(&bytes) {
        let _ = write!(hex, "{byte:02x}");
    }
    Ok(hex)
}

/// A jitter in `[0, 1]` for the restart delay. The sub-second clock is enough to spread
/// restarts of different instances apart; it is not a security value.
fn jitter() -> f64 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.subsec_nanos());
    f64::from(nanos % 1000) / 1000.0
}

/// Starts the server of one cycle. With a command path, the adapter's command stream is attached
/// for this cycle only, and its updates are applied to the dispatcher by a pump that ends when
/// the server ends.
fn start_server(
    config: CoreConfig,
    sink: Box<dyn powermug_contract::sink::BatchSink>,
    listener: UnixListener,
    heartbeat: &Arc<Notify>,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
    commands: Option<&CommandLink>,
) -> (
    tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
    Option<tokio::task::JoinHandle<()>>,
) {
    match commands {
        Some(link) => {
            let (dispatch_tx, dispatch_rx) = command_channel(COMMAND_QUEUE);
            let (update_tx, mut update_rx) = mpsc::unbounded_channel::<pb::CommandUpdate>();
            link.route.attach(dispatch_tx);
            let dispatcher = Arc::clone(&link.dispatcher);
            let pump = tokio::spawn(async move {
                while let Some(update) = update_rx.recv().await {
                    let mut guard = dispatcher.lock().unwrap_or_else(PoisonError::into_inner);
                    let _ = guard.update(&update, now_ns());
                }
            });
            let server = tokio::spawn(serve_instance_commanded(
                config,
                sink,
                listener,
                dispatch_rx,
                update_tx,
                Arc::clone(heartbeat),
                shutdown,
            ));
            (server, Some(pump))
        }
        None => (
            tokio::spawn(serve_instance_observed(
                config,
                sink,
                listener,
                Arc::clone(heartbeat),
                shutdown,
            )),
            None,
        ),
    }
}

/// The adapter's stream of this cycle is gone: nothing more is sent on it, the pump has applied
/// its last updates, and what the stream left unanswered is in doubt (PM-CMD-002).
async fn release_commands(
    commands: Option<&CommandLink>,
    pump: Option<tokio::task::JoinHandle<()>>,
) {
    if let Some(link) = commands {
        link.route.detach();
        if let Some(pump) = pump {
            let _ = pump.await;
        }
        let mut guard = link
            .dispatcher
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let _ = guard.stream_ended(now_ns());
    }
}

/// Commands a dispatch can wait for in the queue of its stream.
const COMMAND_QUEUE: usize = 64;

/// The wall clock in nanoseconds, for the expiry and the audit of the command path.
fn now_ns() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_nanos()),
    )
    .unwrap_or(i64::MAX)
}
