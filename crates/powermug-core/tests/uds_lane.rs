//! The adapter stream into a lane, over a Unix socket. The core serves one
//! instance; the adapter client sends batches; the lane's journal and image are
//! read back through the shared lane.

use std::sync::{Arc, Mutex};

use powermug_contract::client::{self};
use powermug_contract::handshake::{Offer, Requirement, Version};
use powermug_contract::peer::ExpectedPeer;
use powermug_contract::proto::v1 as pb;
use powermug_contract::server::serve_instance;
use powermug_contract::service::CoreConfig;
use powermug_core::{Clock, Lane, LaneSink, PointSpec, PointTable, SessionId};
use powermug_pivot::proto::v1 as pv;
use powermug_pivot::{PointIndex, Value};
use powermug_store::{EventClass, TelemetryLog};
use std::os::unix::fs::MetadataExt;
use tokio::net::UnixListener;
use tokio::sync::oneshot;

struct Setup {
    _dir: tempfile::TempDir,
    socket: std::path::PathBuf,
    lane: Arc<Mutex<Lane>>,
    stop: oneshot::Sender<()>,
    task: tokio::task::JoinHandle<()>,
}

fn point(point_id: &str, class: EventClass) -> PointSpec {
    PointSpec {
        site_id: "S3".into(),
        point_id: point_id.into(),
        class,
        session: Some(SessionId(1)),
        supervision: None,
    }
}

/// A lane of two points, on a new telemetry log, served on a new socket.
fn setup() -> Setup {
    let dir = tempfile::tempdir().expect("temp dir");
    let log = TelemetryLog::open(dir.path().join("telemetry.db")).expect("opens");
    let table = PointTable::new(
        7,
        vec![
            point("S3/TM_001", EventClass::A),
            point("S3/TM_002", EventClass::B),
        ],
    )
    .expect("distinct");
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
            command_variants: vec!["single".into()],
        },
        expected_peer: ExpectedPeer {
            uid,
            pid: i32::try_from(std::process::id()).expect("pid fits i32"),
        },
    };
    let (stop, stopped) = oneshot::channel::<()>();
    let sink = Box::new(LaneSink::new(Arc::clone(&lane), Clock::new()));
    let task = tokio::spawn(async move {
        let _ = serve_instance(config, sink, listener, async {
            let _ = stopped.await;
        })
        .await;
    });
    Setup {
        _dir: dir,
        socket,
        lane,
        stop,
        task,
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

/// An envelope with a `UUIDv7` whose last byte is `id`.
fn envelope(point_id: &str, id: u8, value: pv::Value) -> pv::Envelope {
    let mut event_id = vec![0u8; 16];
    event_id[6] = 0x70;
    event_id[8] = 0x80;
    event_id[15] = id;
    pv::Envelope {
        event_id,
        site_id: "S3".into(),
        point_id: point_id.into(),
        value: Some(value),
        quality: Some(pv::Quality {
            validity: pv::Validity::Good as i32,
            source: pv::Source::Process as i32,
            ..Default::default()
        }),
        source_time_ns: 10,
        receive_time_ns: 20,
        ..Default::default()
    }
}

fn int(value: i64) -> pv::Value {
    pv::Value {
        kind: Some(pv::value::Kind::IntValue(value)),
    }
}

fn batch(sequence: u64, events: Vec<pv::Envelope>) -> pb::EventBatch {
    pb::EventBatch {
        source_epoch: 1,
        batch_sequence: sequence,
        events,
    }
}

/// Reads the lane without holding its lock across an await point.
fn read<T>(lane: &Arc<Mutex<Lane>>, look: impl FnOnce(&Lane) -> T) -> T {
    let guard = lane.lock().expect("lock");
    look(&guard)
}

impl Setup {
    async fn shutdown(self) {
        let _ = self.stop.send(());
        let _ = self.task.await;
    }
}

#[tokio::test]
async fn committed_batch_is_acknowledged_durable_and_reaches_the_image() {
    let setup = setup();
    let channel = client::connect(&setup.socket).await.expect("connects");
    client::describe_capabilities(channel.clone(), capabilities())
        .await
        .expect("accepted");

    let acks = client::stream_events(
        channel,
        vec![batch(
            1,
            vec![
                envelope("S3/TM_001", 1, int(5)),
                envelope("S3/TM_002", 2, int(6)),
            ],
        )],
        64,
    )
    .await
    .expect("streamed");

    let statuses: Vec<i32> = acks.iter().map(|a| a.status).collect();
    assert_eq!(
        statuses,
        vec![
            pb::AckStatus::Received as i32,
            pb::AckStatus::Durable as i32
        ]
    );
    read(&setup.lane, |lane| {
        assert_eq!(lane.log().last_seq().expect("last"), Some(2));
        assert_eq!(
            lane.image().get(PointIndex(0)).map(|s| s.value),
            Some(Value::Int(5))
        );
        assert_eq!(
            lane.image().get(PointIndex(0)).map(|s| s.revision_index),
            Some(7)
        );
    });
    setup.shutdown().await;
}

#[tokio::test]
async fn batch_with_an_unknown_point_is_rejected_and_writes_nothing() {
    let setup = setup();
    let channel = client::connect(&setup.socket).await.expect("connects");
    client::describe_capabilities(channel.clone(), capabilities())
        .await
        .expect("accepted");

    let acks = client::stream_events(
        channel,
        vec![batch(
            1,
            vec![
                envelope("S3/TM_001", 1, int(1)),
                envelope("S3/NOPE", 2, int(2)),
            ],
        )],
        64,
    )
    .await
    .expect("streamed");

    let rejected = acks
        .iter()
        .find(|a| a.status == pb::AckStatus::Rejected as i32)
        .expect("rejected");
    assert!(rejected.reason.contains("S3/NOPE"), "{}", rejected.reason);
    read(&setup.lane, |lane| {
        assert_eq!(lane.log().last_seq().expect("last"), None);
        assert!(lane.image().get(PointIndex(0)).is_none());
    });
    setup.shutdown().await;
}

#[tokio::test]
async fn replayed_event_is_rejected_and_the_image_keeps_the_first_value() {
    let setup = setup();
    let channel = client::connect(&setup.socket).await.expect("connects");
    client::describe_capabilities(channel.clone(), capabilities())
        .await
        .expect("accepted");

    client::stream_events(
        channel.clone(),
        vec![batch(1, vec![envelope("S3/TM_001", 1, int(1))])],
        64,
    )
    .await
    .expect("first");
    let acks = client::stream_events(
        channel,
        vec![batch(2, vec![envelope("S3/TM_001", 1, int(99))])],
        64,
    )
    .await
    .expect("replay");

    let rejected = acks
        .iter()
        .find(|a| a.status == pb::AckStatus::Rejected as i32)
        .expect("rejected");
    assert!(rejected.reason.contains("already"), "{}", rejected.reason);
    read(&setup.lane, |lane| {
        assert_eq!(
            lane.image().get(PointIndex(0)).map(|s| s.value),
            Some(Value::Int(1))
        );
    });
    setup.shutdown().await;
}

#[tokio::test]
async fn lost_session_is_journaled_and_degrades_the_image_until_restored() {
    let setup = setup();
    let channel = client::connect(&setup.socket).await.expect("connects");
    client::describe_capabilities(channel.clone(), capabilities())
        .await
        .expect("accepted");
    client::stream_events(
        channel.clone(),
        vec![batch(1, vec![envelope("S3/TM_001", 1, int(5))])],
        64,
    )
    .await
    .expect("value");

    client::report_communication(
        channel.clone(),
        pv::CommunicationState {
            site_id: "S3".into(),
            session_id: "1".into(),
            connected: false,
            reason: "keepalive timeout".into(),
        },
    )
    .await
    .expect("loss committed");

    read(&setup.lane, |lane| {
        // One value, one state row, one quality row for the point with a value.
        assert_eq!(lane.log().last_seq().expect("last"), Some(3));
        assert_eq!(
            lane.image().get(PointIndex(0)).map(|s| s.quality),
            Some(powermug_pivot::Quality::new(
                powermug_pivot::Validity::Invalid
            ))
        );
    });

    client::report_communication(
        channel,
        pv::CommunicationState {
            site_id: "S3".into(),
            session_id: "1".into(),
            connected: true,
            reason: "link up".into(),
        },
    )
    .await
    .expect("restore committed");

    read(&setup.lane, |lane| {
        assert_eq!(lane.log().last_seq().expect("last"), Some(4));
        // Restored, but still degraded until a fresh value.
        assert_eq!(
            lane.image().get(PointIndex(0)).map(|s| s.quality),
            Some(powermug_pivot::Quality::new(
                powermug_pivot::Validity::Invalid
            ))
        );
    });
    setup.shutdown().await;
}
