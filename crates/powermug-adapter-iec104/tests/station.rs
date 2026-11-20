//! The adapter against a real controlled station of `igs-iec104`, feeding a
//! core lane over a Unix socket. Everything runs in this process, on loopback.

use std::net::SocketAddr;
use std::os::unix::fs::MetadataExt;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use igs_iec104::process_image::PointValue;
use igs_iec104::server::{Handler, Operation, Refusal, Server, ServerConfig, ServerHandle};
use igs_iec104_codec::elements::{Qds, QualityFlags, ShortFloat, Siq};
use igs_iec104_codec::header::{CommonAddress, InformationObjectAddress};
use powermug_adapter_iec104::{run, Mapping, Options, Report};
use powermug_contract::handshake::{Offer, Requirement, Version};
use powermug_contract::peer::ExpectedPeer;
use powermug_contract::proto::v1 as pb;
use powermug_contract::server::serve_instance;
use powermug_contract::service::CoreConfig;
use powermug_core::lane::{Lane, PointSpec, PointTable, SessionId};
use powermug_core::{Clock, LaneSink};
use powermug_pivot::{PointIndex, Quality, Value};
use powermug_store::{EventClass, TelemetryLog};
use tokio::net::UnixListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

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

/// A controlled station with a status point and a measurement, stopped on demand.
struct Station {
    addr: SocketAddr,
    stop: oneshot::Sender<()>,
    task: JoinHandle<()>,
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
    let (stop, stopped) = oneshot::channel::<()>();
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

fn point_spec(point_id: &str, class: EventClass) -> PointSpec {
    PointSpec {
        site_id: "S3".into(),
        point_id: point_id.into(),
        class,
        session: Some(SessionId(1)),
        supervision: None,
    }
}

/// A core with one lane of two points, served on a socket of this process.
struct Core {
    lane: Arc<Mutex<Lane>>,
    socket: std::path::PathBuf,
    stop: oneshot::Sender<()>,
    task: JoinHandle<()>,
    _dir: tempfile::TempDir,
}

fn start_core() -> Core {
    let dir = tempfile::tempdir().expect("temp dir");
    let table = PointTable::new(
        7,
        vec![
            point_spec("S3/TM_001", EventClass::A),
            point_spec("S3/TM_002", EventClass::B),
        ],
    )
    .expect("distinct");
    let log = TelemetryLog::open(dir.path().join("telemetry.db")).expect("opens");
    let lane = Arc::new(Mutex::new(Lane::new(table, log, 0)));

    let socket = dir.path().join("adapter.sock");
    let listener = UnixListener::bind(&socket).expect("bind");
    let uid = std::fs::metadata(dir.path()).expect("metadata").uid();
    let config = CoreConfig {
        offer: Offer {
            pivot: Version { major: 1, minor: 0 },
            requirements: vec![Requirement {
                protocol: "iec104".into(),
                role: pb::Role::Client,
                function: "general_interrogation".into(),
            }],
            command_variants: Vec::new(),
        },
        expected_peer: ExpectedPeer {
            uid,
            pid: i32::try_from(std::process::id()).expect("pid fits i32"),
        },
    };
    let (stop, stopped) = oneshot::channel::<()>();
    let sink_lane = Arc::clone(&lane);
    let task = tokio::spawn(async move {
        let sink = Box::new(LaneSink::new(sink_lane, Clock::new()));
        let _ = serve_instance(config, sink, listener, async {
            let _ = stopped.await;
        })
        .await;
    });
    Core {
        lane,
        socket,
        stop,
        task,
        _dir: dir,
    }
}

fn options(station: SocketAddr, socket: std::path::PathBuf) -> Options {
    Options {
        station,
        common_address: 1,
        core_socket: socket,
        site_id: "S3".into(),
        session_id: 1,
        revision: 7,
        mapping: Mapping::new(vec![
            (1, 1, "S3/TM_001".to_owned()),
            (1, 2, "S3/TM_002".to_owned()),
        ])
        .expect("distinct"),
        artifact_sha256: "ab".repeat(32),
        window_events: 64,
    }
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
async fn station_values_reach_the_lane_with_their_quality() {
    let station = start_station().await;
    let core = start_core();
    let (adapter_stop, adapter_stopped) = oneshot::channel::<()>();
    let adapter = tokio::spawn(run(options(station.addr, core.socket.clone()), async {
        let _ = adapter_stopped.await;
    }));

    let lane = Arc::clone(&core.lane);
    let received = wait_until(
        || {
            let guard = lane.lock().expect("lock");
            guard.image().get(PointIndex(0)).is_some() && guard.image().get(PointIndex(1)).is_some()
        },
        20,
    )
    .await;
    let _ = adapter_stop.send(());
    let report: Report = adapter.await.expect("joins").expect("runs");
    let _ = core.stop.send(());
    let _ = core.task.await;
    let _ = station.stop.send(());
    let _ = station.task.await;

    assert!(
        received,
        "the image did not receive both points: {report:?}"
    );
    assert!(
        report.connections >= 1 && report.batches >= 1 && report.events >= 2,
        "{report:?}"
    );
    let guard = lane.lock().expect("lock");
    let status = guard.image().get(PointIndex(0)).expect("status");
    assert_eq!(
        (status.value, status.quality),
        (Value::Bool(true), Quality::GOOD)
    );
    let measurement = guard.image().get(PointIndex(1)).expect("measurement");
    assert_eq!(measurement.value, Value::Double(0.25));
}

#[tokio::test]
async fn station_loss_reaches_the_lane_as_a_lost_session() {
    let station = start_station().await;
    let core = start_core();
    let (adapter_stop, adapter_stopped) = oneshot::channel::<()>();
    let adapter = tokio::spawn(run(options(station.addr, core.socket.clone()), async {
        let _ = adapter_stopped.await;
    }));

    let lane = Arc::clone(&core.lane);
    let fed = wait_until(
        || {
            lane.lock()
                .expect("lock")
                .image()
                .get(PointIndex(0))
                .is_some()
        },
        20,
    )
    .await;
    assert!(fed, "no value before the loss");

    // The station goes away: the adapter reports the lost session.
    let _ = station.stop.send(());
    let _ = station.task.await;
    let lost = wait_until(|| lane.lock().expect("lock").is_lost(SessionId(1)), 30).await;

    let _ = adapter_stop.send(());
    let report = adapter.await.expect("joins").expect("runs");
    let _ = core.stop.send(());
    let _ = core.task.await;

    assert!(lost, "the session was not reported lost: {report:?}");
    assert_eq!(report.refused_reports, 0);
    let guard = lane.lock().expect("lock");
    assert_eq!(
        guard.image().get(PointIndex(0)).map(|s| s.quality),
        Some(Quality::new(powermug_pivot::Validity::Invalid))
    );
}
