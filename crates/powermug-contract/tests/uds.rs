//! End to end over a Unix socket: a core serving one adapter instance and an
//! adapter client, both in this process. The instance's uid is the uid of this
//! process, and its pid is this process's pid.

use std::os::unix::fs::MetadataExt;

use powermug_contract::bounds::MAX_BATCH_EVENTS;
use powermug_contract::client::{self, ClientError};
use powermug_contract::handshake::{Offer, Requirement};
use powermug_contract::peer::ExpectedPeer;
use powermug_contract::proto::v1 as pb;
use powermug_contract::server::serve_instance;
use powermug_contract::service::CoreConfig;
use powermug_pivot::proto::v1::Envelope;
use tokio::net::UnixListener;
use tokio::sync::oneshot;
use tonic::Code;

/// A running core. Dropping it does not stop the server; `stop` does.
struct Core {
    _dir: tempfile::TempDir,
    socket: std::path::PathBuf,
    stop: oneshot::Sender<()>,
    task: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
}

impl Core {
    fn start(expected_pid: i32) -> Self {
        let dir = tempfile::tempdir().expect("temp dir");
        let socket = dir.path().join("adapter.sock");
        let listener = UnixListener::bind(&socket).expect("bind");
        let uid = std::fs::metadata(dir.path()).expect("metadata").uid();
        let config = CoreConfig {
            offer: offer(),
            expected_peer: ExpectedPeer {
                uid,
                pid: expected_pid,
            },
        };
        let (stop, stopped) = oneshot::channel::<()>();
        let task = tokio::spawn(serve_instance(config, listener, async {
            let _ = stopped.await;
        }));
        Self {
            _dir: dir,
            socket,
            stop,
            task,
        }
    }

    async fn shutdown(self) {
        let _ = self.stop.send(());
        let _ = self.task.await;
    }
}

fn own_pid() -> i32 {
    i32::try_from(std::process::id()).expect("pid fits i32")
}

fn offer() -> Offer {
    Offer {
        pivot: powermug_contract::handshake::Version { major: 1, minor: 0 },
        requirements: vec![Requirement {
            protocol: "iec104".into(),
            role: pb::Role::Client,
            function: "general_interrogation".into(),
        }],
        command_variants: vec!["single".into()],
    }
}

fn capabilities(window_events: u32) -> pb::Capabilities {
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
            window_events,
            max_events_per_second: 10_000,
        }),
        crash_recovery: true,
        hardware_dependencies: vec![],
        artifact_sha256: "ab".repeat(32),
        protocol_library_version: "0.1.0".into(),
        settings_schema: "{}".into(),
    }
}

fn batch(sequence: u64, events: usize) -> pb::EventBatch {
    pb::EventBatch {
        source_epoch: 1,
        batch_sequence: sequence,
        events: (0..events)
            .map(|i| Envelope {
                site_id: "S3".into(),
                point_id: format!("S3/TM_{i:04}"),
                ..Default::default()
            })
            .collect(),
    }
}

async fn connect(core: &Core) -> tonic::transport::Channel {
    client::connect(&core.socket).await.expect("connects")
}

fn status_of(err: ClientError) -> tonic::Status {
    match err {
        ClientError::Status(status) => status,
        other => panic!("expected a gRPC status, got {other:?}"),
    }
}

#[tokio::test]
async fn handshake_is_accepted_and_returns_the_core_versions() {
    let core = Core::start(own_pid());
    let channel = connect(&core).await;

    let answer = client::describe_capabilities(channel, capabilities(4))
        .await
        .expect("accepted");

    assert_eq!(answer.api_version, Some(pb::Version { major: 1, minor: 0 }));
    assert_eq!(
        answer.pivot_version,
        Some(pb::Version { major: 1, minor: 0 })
    );
    core.shutdown().await;
}

#[tokio::test]
async fn handshake_refused_on_api_major_carries_the_typed_reason() {
    let core = Core::start(own_pid());
    let channel = connect(&core).await;
    let mut caps = capabilities(4);
    caps.api_version = Some(pb::Version { major: 2, minor: 0 });

    let status = status_of(
        client::describe_capabilities(channel, caps)
            .await
            .expect_err("refused"),
    );

    assert_eq!(status.code(), Code::FailedPrecondition);
    assert!(
        status.message().contains("API major"),
        "{}",
        status.message()
    );
    core.shutdown().await;
}

#[tokio::test]
async fn peer_with_another_pid_is_refused_on_every_call() {
    let core = Core::start(own_pid() + 1);
    let channel = connect(&core).await;

    let status = status_of(
        client::describe_capabilities(channel, capabilities(4))
            .await
            .expect_err("refused"),
    );

    assert_eq!(status.code(), Code::PermissionDenied);
    core.shutdown().await;
}

#[tokio::test]
async fn stream_before_handshake_is_refused() {
    let core = Core::start(own_pid());
    let channel = connect(&core).await;

    let status = status_of(
        client::stream_events(channel, vec![batch(1, 1)], 4)
            .await
            .expect_err("refused"),
    );

    assert_eq!(status.code(), Code::FailedPrecondition);
    core.shutdown().await;
}

#[tokio::test]
async fn batches_are_acknowledged_in_order_within_the_credit_window() {
    let core = Core::start(own_pid());
    let channel = connect(&core).await;
    client::describe_capabilities(channel.clone(), capabilities(4))
        .await
        .expect("accepted");

    // Six batches of two events: 12 events through a window of 4 credits.
    let batches = (1..=6).map(|seq| batch(seq, 2)).collect();
    let acks = client::stream_events(channel, batches, 4)
        .await
        .expect("streamed");

    let sequences: Vec<u64> = acks.iter().map(|a| a.batch_sequence).collect();
    assert_eq!(sequences, vec![1, 2, 3, 4, 5, 6]);
    assert!(acks
        .iter()
        .all(|a| a.status == pb::AckStatus::Received as i32));
    let credits: u32 = acks.iter().map(|a| a.credits_granted).sum();
    assert_eq!(credits, 12);
    core.shutdown().await;
}

#[tokio::test]
async fn batch_larger_than_the_window_is_refused_by_the_adapter() {
    let core = Core::start(own_pid());
    let channel = connect(&core).await;
    client::describe_capabilities(channel.clone(), capabilities(4))
        .await
        .expect("accepted");

    let err = client::stream_events(channel, vec![batch(1, 6)], 4)
        .await
        .expect_err("refused");

    assert!(matches!(
        err,
        ClientError::BatchExceedsWindow {
            events: 6,
            window: 4
        }
    ));
    core.shutdown().await;
}

#[tokio::test]
async fn batch_above_the_event_bound_is_refused_by_the_core() {
    let core = Core::start(own_pid());
    let channel = connect(&core).await;
    let window = 2_048;
    client::describe_capabilities(channel.clone(), capabilities(window))
        .await
        .expect("accepted");

    let too_many = MAX_BATCH_EVENTS + 1;
    let status = status_of(
        client::stream_events(channel, vec![batch(1, too_many)], window)
            .await
            .expect_err("refused"),
    );

    assert_eq!(status.code(), Code::InvalidArgument);
    core.shutdown().await;
}

#[tokio::test]
async fn frame_above_the_byte_bound_is_refused_before_decoding() {
    let core = Core::start(own_pid());
    let channel = connect(&core).await;
    client::describe_capabilities(channel.clone(), capabilities(4))
        .await
        .expect("accepted");

    let mut oversize = batch(1, 1);
    oversize.events[0].point_id = "x".repeat(1 << 20);

    let status = status_of(
        client::stream_events(channel, vec![oversize], 4)
            .await
            .expect_err("refused"),
    );

    // tonic refuses the frame from its length prefix, before decoding it.
    assert_eq!(status.code(), Code::OutOfRange);
    assert!(
        status.message().contains("too large"),
        "{}",
        status.message()
    );
    core.shutdown().await;
}

#[tokio::test]
async fn adapter_that_overspends_its_window_gets_a_rejection() {
    let core = Core::start(own_pid());
    let channel = connect(&core).await;
    client::describe_capabilities(channel.clone(), capabilities(4))
        .await
        .expect("accepted");

    // A raw client bypasses the adapter's own credit check.
    let mut raw = pb::adapter_data_client::AdapterDataClient::new(channel);
    let mut acks = raw
        .stream_events(tokio_stream::iter(vec![batch(1, 6)]))
        .await
        .expect("stream opens")
        .into_inner();

    let ack = acks.message().await.expect("ok").expect("one ack");
    assert_eq!(ack.status, pb::AckStatus::Rejected as i32);
    assert_eq!(ack.credits_granted, 0);
    assert!(ack.reason.contains("exceeds"), "{}", ack.reason);
    core.shutdown().await;
}
