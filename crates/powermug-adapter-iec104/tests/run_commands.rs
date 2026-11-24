//! The adapter's loop of `run` with its command stream (SPECIFICATIONS 6.1, 9.4): the core
//! dispatches a selected command, the loop emits it through the redundancy group to a real
//! station, and the station's answers come back to the core.

use std::net::SocketAddr;
use std::os::unix::fs::MetadataExt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use igs_iec104::process_image::PointValue;
use igs_iec104::server::{CommandValue, Handler, Operation, Refusal, Server, ServerConfig};
use igs_iec104_codec::elements::{QualityFlags, Siq};
use igs_iec104_codec::header::{CommonAddress, InformationObjectAddress};
use powermug_adapter_iec104::{run, Mapping, Options};
use powermug_contract::handshake::{Offer, Requirement, Version};
use powermug_contract::peer::ExpectedPeer;
use powermug_contract::proto::v1 as pb;
use powermug_contract::server::serve_instance_commanded;
use powermug_contract::service::CoreConfig;
use powermug_contract::sink::{BatchSink, Durability, Refusal as SinkRefusal};
use powermug_pivot::proto::v1::{CommunicationState, Envelope};
use tokio::net::UnixListener;
use tokio::sync::{mpsc, oneshot, Notify};

struct AcceptAll;

impl BatchSink for AcceptAll {
    fn commit(&mut self, _batch: &pb::EventBatch) -> Result<Durability, SinkRefusal> {
        Ok(Durability::Durable)
    }

    fn communication(&mut self, _state: &CommunicationState) -> Result<(), SinkRefusal> {
        Ok(())
    }

    fn snapshot(&mut self, _session_id: &str) -> Result<Vec<Envelope>, SinkRefusal> {
        Ok(Vec::new())
    }

    fn subscribe(&mut self) -> tokio::sync::broadcast::Receiver<Envelope> {
        let (_tx, rx) = tokio::sync::broadcast::channel(1);
        rx
    }
}

/// Records the single commands it executes, and answers the station's usual way.
struct Recorder {
    executed: Arc<Mutex<Vec<(u32, bool)>>>,
}

impl Handler for Recorder {
    fn select(&self, _operation: &Operation) -> Result<(), Refusal> {
        Ok(())
    }

    fn execute(&self, operation: &Operation) -> Result<(), Refusal> {
        if let CommandValue::Single(sco) = operation.value {
            self.executed
                .lock()
                .expect("lock")
                .push((operation.address.value(), sco.on));
        }
        Ok(())
    }
}

fn dispatch_of(command_id: &str, select: bool) -> pb::CommandDispatch {
    pb::CommandDispatch {
        command_id: command_id.into(),
        address: 5,
        kind: pb::CommandKind::Single as i32,
        on: true,
        select,
        owner_epoch: 3,
        expires_ns: i64::MAX,
    }
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "an end-to-end scenario, read from top to bottom"
)]
async fn a_dispatch_of_the_core_is_executed_by_the_group_and_its_answers_reach_the_core() {
    // The station: a commandable point at address 5, and the points of the mapping.
    let executed = Arc::new(Mutex::new(Vec::new()));
    let station = Server::bind(
        ServerConfig::new(
            "127.0.0.1:0".parse::<SocketAddr>().expect("addr"),
            CommonAddress::new(1),
        ),
        Recorder {
            executed: Arc::clone(&executed),
        },
    )
    .await
    .expect("station binds");
    let station_addr: SocketAddr = station.local_addr().expect("address");
    let handle = station.handle();
    for address in [1, 2, 5] {
        handle
            .add_point(
                InformationObjectAddress::new(address).expect("address"),
                PointValue::Single(Siq {
                    on: false,
                    quality: QualityFlags::default(),
                }),
                false,
            )
            .expect("point");
    }
    let (station_stop, station_stopped) = oneshot::channel::<()>();
    let station_task = tokio::spawn(async move {
        let _ = station
            .run_until(
                async {
                    let _ = station_stopped.await;
                },
                Duration::ZERO,
            )
            .await;
    });

    // The core: a commanded instance on a socket.
    let dir = tempfile::tempdir().expect("temp dir");
    let socket = dir.path().join("core.sock");
    let listener = UnixListener::bind(&socket).expect("bind");
    let config = CoreConfig {
        offer: Offer {
            pivot: Version { major: 1, minor: 0 },
            requirements: vec![Requirement {
                protocol: "iec104".into(),
                role: pb::Role::Client,
                function: "general_interrogation".into(),
            }],
            command_variants: vec!["single".into()],
        },
        expected_peer: ExpectedPeer {
            uid: std::fs::metadata(dir.path()).expect("metadata").uid(),
            pid: i32::try_from(std::process::id()).expect("pid fits i32"),
        },
    };
    let (dispatch_tx, dispatch_rx) = mpsc::channel(8);
    let (update_tx, mut updates) = mpsc::unbounded_channel();
    let (core_stop, core_stopped) = oneshot::channel::<()>();
    let core = tokio::spawn(serve_instance_commanded(
        config,
        Box::new(AcceptAll),
        listener,
        dispatch_rx,
        update_tx,
        Arc::new(Notify::new()),
        async {
            let _ = core_stopped.await;
        },
    ));

    // The adapter: the loop of `run`, with the group of the station.
    let mapping = Mapping::new(vec![
        (1, 1, "S3/TM_001".to_owned()),
        (1, 2, "S3/TM_002".to_owned()),
    ])
    .expect("distinct");
    let options = Options {
        stations: vec![station_addr],
        common_address: 1,
        core_socket: socket.clone(),
        site_id: "S3".into(),
        session_id: 1,
        revision: 7,
        mapping,
        artifact_sha256: "ab".repeat(32),
        window_events: 64,
        owner_epoch: 3,
    };
    let (adapter_stop, adapter_stopped) = oneshot::channel::<()>();
    let adapter = tokio::spawn(run(options, async {
        let _ = adapter_stopped.await;
    }));

    // A selected command, then its execution: the station answers, and the core hears it.
    // The group starts its connection first: the dispatches go out once it is up.
    tokio::time::sleep(Duration::from_secs(2)).await;
    dispatch_tx
        .send(dispatch_of("c1", true))
        .await
        .expect("queued");
    dispatch_tx
        .send(dispatch_of("c1", false))
        .await
        .expect("queued");
    let mut received = Vec::new();
    while received.len() < 3 {
        let update = tokio::time::timeout(Duration::from_secs(30), updates.recv())
            .await
            .expect("an update in time")
            .expect("an update");
        received.push((update.command_id, update.status));
    }

    let _ = adapter_stop.send(());
    let report = adapter.await.expect("joins").expect("runs");
    let _ = core_stop.send(());
    let _ = core.await;
    let _ = station_stop.send(());
    let _ = station_task.await;

    let confirmed = pb::CommandStatus::Confirmed as i32;
    let terminated = pb::CommandStatus::Terminated as i32;
    assert_eq!(
        received,
        vec![
            ("c1".to_owned(), confirmed),
            ("c1".to_owned(), confirmed),
            ("c1".to_owned(), terminated),
        ]
    );
    assert_eq!(*executed.lock().expect("lock"), vec![(5, true)]);
    assert!(report.refused_reports == 0, "{report:?}");
    let _: Option<mpsc::Sender<pb::CommandUpdate>> = None;
}
