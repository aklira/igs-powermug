//! The whole command path in one process: a core serving the command stream on a Unix socket,
//! the adapter's dispatch loop, and a real controlling station of `igs-iec104`. A selected
//! command is confirmed, executed and terminated, a dispatch with another owner epoch is not
//! sent, and an expired command is never emitted.

use std::net::SocketAddr;
use std::os::unix::fs::MetadataExt;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use igs_iec104::client::{Client, ClientConfig, Event};
use igs_iec104::process_image::PointValue;
use igs_iec104::server::{CommandValue, Handler, Operation, Refusal, Server, ServerConfig};
use igs_iec104::Delivery;
use igs_iec104_codec::elements::{QualityFlags, Siq};
use igs_iec104_codec::header::{CommonAddress, InformationObjectAddress};
use igs_iec104_link::TransferState;
use powermug_adapter_iec104::dispatch;
use powermug_contract::client as contract;
use powermug_contract::handshake::{Offer, Requirement, Version};
use powermug_contract::peer::ExpectedPeer;
use powermug_contract::proto::v1 as pb;
use powermug_contract::server::serve_instance_commanded;
use powermug_contract::service::CoreConfig;
use powermug_contract::sink::{BatchSink, Durability, Refusal as SinkRefusal};
use powermug_pivot::proto::v1::{CommunicationState, Envelope};
use tokio::net::UnixListener;
use tokio::sync::{mpsc, oneshot};

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

/// Records the single commands it executes.
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

fn offer() -> Offer {
    Offer {
        pivot: Version { major: 1, minor: 0 },
        requirements: vec![Requirement {
            protocol: "iec104".into(),
            role: pb::Role::Client,
            function: "general_interrogation".into(),
        }],
        command_variants: vec!["single".into()],
    }
}

fn capabilities() -> pb::Capabilities {
    pb::Capabilities {
        api_version: Some(pb::Version { major: 1, minor: 0 }),
        pivot_versions: vec![pb::Version { major: 1, minor: 0 }],
        protocols: vec![pb::Protocol {
            name: "iec104".into(),
            role: pb::Role::Client as i32,
            functions: vec!["general_interrogation".into()],
        }],
        command_variants: vec!["single".into()],
        limits: Some(pb::Limits {
            max_batch_events: 1_024,
            max_batch_bytes: 1 << 20,
            max_command_bytes: 16 << 10,
            window_events: 64,
            max_events_per_second: 10_000,
        }),
        crash_recovery: true,
        hardware_dependencies: vec![],
        artifact_sha256: "ab".repeat(32),
        protocol_library_version: "0.1.0".into(),
        settings_schema: "{}".into(),
    }
}

fn dispatch_of(command_id: &str, select: bool, epoch: u64, expires_ns: i64) -> pb::CommandDispatch {
    pb::CommandDispatch {
        command_id: command_id.into(),
        address: 5,
        kind: pb::CommandKind::Single as i32,
        on: true,
        select,
        owner_epoch: epoch,
        expires_ns,
    }
}

async fn next_update(
    updates: &mut mpsc::UnboundedReceiver<pb::CommandUpdate>,
) -> pb::CommandUpdate {
    tokio::time::timeout(Duration::from_secs(20), updates.recv())
        .await
        .expect("an update in time")
        .expect("an update")
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "an end-to-end scenario, read from top to bottom"
)]
async fn a_dispatch_is_emitted_to_the_station_and_its_outcome_reaches_the_core() {
    // The station, with a commandable point at address 5.
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
    station
        .handle()
        .add_point(
            InformationObjectAddress::new(5).expect("address"),
            PointValue::Single(Siq {
                on: false,
                quality: QualityFlags::default(),
            }),
            false,
        )
        .expect("commandable point");
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

    // The core, serving the command stream on a socket.
    let dir = tempfile::tempdir().expect("temp dir");
    let socket = dir.path().join("adapter.sock");
    let listener = UnixListener::bind(&socket).expect("bind");
    let config = CoreConfig {
        offer: offer(),
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
        std::sync::Arc::new(tokio::sync::Notify::new()),
        async {
            let _ = core_stopped.await;
        },
    ));

    // The adapter: the handshake, the station's data transfer, then the dispatch loop.
    let channel = contract::connect(&socket).await.expect("connects");
    contract::describe_capabilities(channel.clone(), capabilities())
        .await
        .expect("accepted");
    let mut controlling = Client::connect(ClientConfig::new(station_addr, CommonAddress::new(1)));
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        assert!(Instant::now() < deadline, "the data transfer did not start");
        match tokio::time::timeout(Duration::from_millis(200), controlling.next_event()).await {
            Ok(Some(Event::Connected)) => {
                controlling.start_data_transfer().await.expect("starts");
            }
            Ok(Some(Event::Delivery(Delivery::Transfer(TransferState::Started)))) => break,
            _ => {}
        }
    }
    let (adapter_stop, adapter_stopped) = oneshot::channel::<()>();
    let adapter = tokio::spawn(async move {
        dispatch::serve_single(channel, &mut controlling, 3, 1, async {
            let _ = adapter_stopped.await;
        })
        .await
    });

    // A selected command, then its execution: confirmed, then confirmed and terminated.
    let far = i64::MAX;
    dispatch_tx
        .send(dispatch_of("c1", true, 3, far))
        .await
        .expect("queued");
    assert_eq!(
        next_update(&mut updates).await.status,
        pb::CommandStatus::Confirmed as i32
    );
    dispatch_tx
        .send(dispatch_of("c1", false, 3, far))
        .await
        .expect("queued");
    let confirmed = next_update(&mut updates).await;
    assert_eq!(confirmed.status, pb::CommandStatus::Confirmed as i32);
    let terminated = next_update(&mut updates).await;
    assert_eq!(terminated.status, pb::CommandStatus::Terminated as i32);
    assert_eq!(terminated.command_id, "c1");

    // Another owner epoch: not sent, reported as not sent.
    dispatch_tx
        .send(dispatch_of("c2", true, 9, far))
        .await
        .expect("queued");
    let other_epoch = next_update(&mut updates).await;
    assert_eq!(other_epoch.status, pb::CommandStatus::NotSent as i32);
    assert_eq!(other_epoch.command_id, "c2");

    // Expired before emission: never sent.
    dispatch_tx
        .send(dispatch_of("c3", false, 3, 1))
        .await
        .expect("queued");
    let expired = next_update(&mut updates).await;
    assert_eq!(expired.status, pb::CommandStatus::Expired as i32);

    // The core ends the stream: the adapter's loop ends with its report.
    drop(dispatch_tx);
    let report = tokio::time::timeout(Duration::from_secs(10), adapter)
        .await
        .expect("the loop ends in time")
        .expect("joins")
        .expect("serves");
    let _ = adapter_stop.send(());
    let _ = core_stop.send(());
    let _ = tokio::time::timeout(Duration::from_secs(5), core).await;
    let _ = station_stop.send(());
    let _ = station_task.await;

    assert_eq!(report, 4);
    assert_eq!(*executed.lock().expect("lock"), vec![(5, true)]);
}
