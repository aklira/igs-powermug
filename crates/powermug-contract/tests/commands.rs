//! The command stream over a Unix socket (SPECIFICATIONS 6.1, PM-CMD-005): the core sends a
//! dispatch to the adapter, the adapter's update comes back to the core, and a second command
//! stream on the same instance is refused. The core and the adapter run in this process.

use std::os::unix::fs::MetadataExt;
use std::time::Duration;

use powermug_contract::client;
use powermug_contract::handshake::{Offer, Requirement, Version};
use powermug_contract::peer::ExpectedPeer;
use powermug_contract::proto::v1 as pb;
use powermug_contract::server::serve_instance_commanded;
use powermug_contract::service::CoreConfig;
use powermug_contract::sink::{BatchSink, Durability, Refusal};
use powermug_pivot::proto::v1::{CommunicationState, Envelope};
use tokio::net::UnixListener;
use tokio::sync::{mpsc, oneshot};
use tonic::Code;

struct AcceptAll;

impl BatchSink for AcceptAll {
    fn commit(&mut self, _batch: &pb::EventBatch) -> Result<Durability, Refusal> {
        Ok(Durability::Durable)
    }

    fn communication(&mut self, _state: &CommunicationState) -> Result<(), Refusal> {
        Ok(())
    }

    fn snapshot(&mut self, _session_id: &str) -> Result<Vec<Envelope>, Refusal> {
        Ok(Vec::new())
    }

    fn subscribe(&mut self) -> tokio::sync::broadcast::Receiver<Envelope> {
        let (_tx, rx) = tokio::sync::broadcast::channel(1);
        rx
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

fn dispatch(command_id: &str) -> pb::CommandDispatch {
    pb::CommandDispatch {
        command_id: command_id.into(),
        address: 5,
        kind: pb::CommandKind::Single as i32,
        on: true,
        select: false,
        owner_epoch: 3,
        expires_ns: i64::MAX,
    }
}

#[tokio::test]
async fn a_dispatch_reaches_the_adapter_and_its_update_reaches_the_core() {
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
    let (update_tx, mut update_rx) = mpsc::unbounded_channel();
    let (stop, stopped) = oneshot::channel::<()>();
    let core = tokio::spawn(serve_instance_commanded(
        config,
        Box::new(AcceptAll),
        listener,
        dispatch_rx,
        update_tx,
        async {
            let _ = stopped.await;
        },
    ));

    let channel = client::connect(&socket).await.expect("connects");
    client::describe_capabilities(channel.clone(), capabilities())
        .await
        .expect("accepted");
    let (updates, mut dispatches) = client::execute_commands(channel)
        .await
        .expect("stream opens");

    dispatch_tx.send(dispatch("c1")).await.expect("queued");
    let received = tokio::time::timeout(Duration::from_secs(5), dispatches.message())
        .await
        .expect("in time")
        .expect("stream")
        .expect("a dispatch");
    assert_eq!(received.command_id, "c1");
    assert_eq!(received.owner_epoch, 3);

    updates
        .send(pb::CommandUpdate {
            command_id: "c1".into(),
            status: pb::CommandStatus::Confirmed as i32,
            reason: String::new(),
        })
        .await
        .expect("sent");
    let update = tokio::time::timeout(Duration::from_secs(5), update_rx.recv())
        .await
        .expect("in time")
        .expect("an update");
    assert_eq!(update.command_id, "c1");
    assert_eq!(update.status, pb::CommandStatus::Confirmed as i32);

    drop(updates);
    drop(dispatches);
    let _ = stop.send(());
    tokio::time::timeout(Duration::from_secs(5), core)
        .await
        .expect("the core stops in time")
        .expect("joins")
        .expect("serves");
}

#[tokio::test]
async fn a_command_stream_before_the_handshake_is_refused() {
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
    let (_dispatch_tx, dispatch_rx) = mpsc::channel(8);
    let (update_tx, _update_rx) = mpsc::unbounded_channel();
    let (stop, stopped) = oneshot::channel::<()>();
    let core = tokio::spawn(serve_instance_commanded(
        config,
        Box::new(AcceptAll),
        listener,
        dispatch_rx,
        update_tx,
        async {
            let _ = stopped.await;
        },
    ));

    let channel = client::connect(&socket).await.expect("connects");
    let refused = client::execute_commands(channel).await;
    match refused {
        Err(powermug_contract::client::ClientError::Status(status)) => {
            assert_eq!(status.code(), Code::FailedPrecondition);
        }
        other => panic!("the stream must be refused, got {:?}", other.map(|_| ())),
    }

    let _ = stop.send(());
    tokio::time::timeout(Duration::from_secs(5), core)
        .await
        .expect("the core stops in time")
        .expect("joins")
        .expect("serves");
}
