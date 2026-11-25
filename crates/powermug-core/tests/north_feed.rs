//! The North feed of a declared destination (SPECIFICATIONS 10.3, FUN-SAF-002 to FUN-SAF-005): the
//! events come from the live ring or the journal, the cursor follows the acknowledgements, and a
//! destination that is not declared is refused.

use std::os::unix::fs::MetadataExt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use powermug_contract::client;
use powermug_contract::handshake::{Offer, Requirement, Version};
use powermug_contract::peer::ExpectedPeer;
use powermug_contract::proto::v1 as pb;
use powermug_contract::server::serve_instance;
use powermug_contract::service::{CoreConfig, PUBLISH_BATCH};
use powermug_contract::sink::BatchSink;
use powermug_core::lane::{Lane, PointSpec, PointTable, SessionId};
use powermug_core::{Clock, LaneSink};
use powermug_pivot::proto::v1 as pivot;
use powermug_store::{EventClass, Start, TelemetryLog};
use tokio::net::UnixListener;
use tokio::sync::oneshot;

const DESTINATION: &str = "kafka";

fn envelope(id: u32) -> pivot::Envelope {
    let mut event_id = vec![0u8; 16];
    event_id[6] = 0x70;
    event_id[8] = 0x80;
    event_id[12..16].copy_from_slice(&id.to_be_bytes());
    pivot::Envelope {
        event_id,
        site_id: "S3".into(),
        point_id: "S3/TM_001".into(),
        value: Some(pivot::Value {
            kind: Some(pivot::value::Kind::IntValue(i64::from(id))),
        }),
        quality: Some(pivot::Quality {
            validity: pivot::Validity::Good as i32,
            source: pivot::Source::Process as i32,
            ..Default::default()
        }),
        source_time_ns: 1,
        receive_time_ns: 1,
        ..Default::default()
    }
}

/// One committed batch of the events `ids`. The value of each event carries its number, which is
/// its journal sequence in these tests.
fn batch(ids: std::ops::RangeInclusive<u32>) -> pb::EventBatch {
    pb::EventBatch {
        source_epoch: 1,
        batch_sequence: u64::from(*ids.start()),
        events: ids.map(envelope).collect(),
    }
}

fn capabilities(destination: &str) -> pb::Capabilities {
    pb::Capabilities {
        destination: destination.into(),
        api_version: Some(pb::Version { major: 1, minor: 0 }),
        pivot_versions: vec![pb::Version { major: 1, minor: 0 }],
        protocols: vec![pb::Protocol {
            name: "iec104".into(),
            role: pb::Role::Client as i32,
            functions: vec!["general_interrogation".into()],
        }],
        command_variants: Vec::new(),
        limits: Some(pb::Limits {
            max_batch_events: 1_024,
            max_batch_bytes: 1 << 20,
            max_command_bytes: 16 << 10,
            window_events: 64,
            max_events_per_second: 10_000,
        }),
        crash_recovery: false,
        hardware_dependencies: Vec::new(),
        artifact_sha256: "ab".repeat(32),
        protocol_library_version: "0.1.0".into(),
        settings_schema: "{}".into(),
    }
}

/// A running core with one lane, and the instance accepted under `destination`.
struct Harness {
    reader: TelemetryLog,
    sink: LaneSink,
    channel: tonic::transport::Channel,
    stop: oneshot::Sender<()>,
    core: tokio::task::JoinHandle<()>,
    _dir: tempfile::TempDir,
}

/// Starts a core whose lane journals into a fresh store. `declare` declares `destination` with a
/// cursor at the oldest retained event, as the cell does for each configured destination.
async fn harness(destination: &str, declare: bool) -> Harness {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("telemetry.db");
    let mut reader = TelemetryLog::open(&path).expect("opens");
    if declare {
        reader
            .declare_cursor(destination, true, Start::OldestRetained, 1)
            .expect("declares the destination");
    }
    let table = PointTable::new(
        1,
        vec![PointSpec {
            site_id: "S3".into(),
            point_id: "S3/TM_001".into(),
            class: EventClass::A,
            session: Some(SessionId(1)),
            supervision: None,
        }],
    )
    .expect("distinct");
    let lane = Arc::new(Mutex::new(Lane::new(
        table,
        TelemetryLog::open(&path).expect("opens"),
        0,
    )));
    let sink = LaneSink::new(lane, Clock::new());

    let socket = dir.path().join("core.sock");
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
    let served = Box::new(sink.clone());
    let core = tokio::spawn(async move {
        let _ = serve_instance(config, served, listener, async {
            let _ = stopped.await;
        })
        .await;
    });

    let channel = client::connect(&socket).await.expect("connects");
    client::describe_capabilities(channel.clone(), capabilities(destination))
        .await
        .expect("accepted");
    Harness {
        reader,
        sink,
        channel,
        stop,
        core,
        _dir: dir,
    }
}

/// Ends the core. The subscriptions must be dropped before.
async fn finish(harness: Harness) {
    let Harness {
        stop,
        core,
        channel,
        ..
    } = harness;
    drop(channel);
    let _ = stop.send(());
    let _ = core.await;
}

async fn next_delivery(batches: &mut tonic::Streaming<pb::DeliveryBatch>) -> pb::DeliveryBatch {
    tokio::time::timeout(Duration::from_secs(10), batches.message())
        .await
        .expect("no stall")
        .expect("stream open")
        .expect("a batch")
}

/// Waits until the store holds the cursor of [`DESTINATION`] at `expected`.
async fn wait_for_cursor(reader: &TelemetryLog, expected: i64) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let last = reader
            .cursor(DESTINATION)
            .expect("reads")
            .expect("declared")
            .last_seq;
        if last == expected {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the cursor is at {last}, expected {expected}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// The event value carries the journal sequence of the event.
fn sequence_of(envelope: &pivot::Envelope) -> u64 {
    match envelope.value.as_ref().and_then(|v| v.kind.as_ref()) {
        Some(pivot::value::Kind::IntValue(number)) => u64::try_from(*number).unwrap_or(0),
        _ => 0,
    }
}

/// Receives and acknowledges batches until `total` events have arrived, in order.
async fn receive_all(
    acks: &tokio::sync::mpsc::Sender<pb::DeliveryAck>,
    batches: &mut tonic::Streaming<pb::DeliveryBatch>,
    total: u64,
) -> Vec<u64> {
    let mut received = Vec::new();
    while (received.len() as u64) < total {
        let delivery = next_delivery(batches).await;
        let ids: Vec<u64> = delivery.events.iter().map(sequence_of).collect();
        let last = *ids.last().expect("a batch has events");
        assert_eq!(
            delivery.last_seq,
            i64::try_from(last).expect("fits i64"),
            "the batch states the journal position of its last event"
        );
        received.extend(ids);
        acks.send(pb::DeliveryAck {
            sequence: delivery.sequence,
        })
        .await
        .expect("acked");
    }
    received
}

#[tokio::test]
async fn a_declared_destination_receives_every_event_in_order_and_its_cursor_follows_the_acks() {
    let harness = harness(DESTINATION, true).await;
    let mut committer = harness.sink.clone();
    for first in [1u32, 51, 101] {
        committer
            .commit(&batch(first..=first + 49))
            .expect("committed");
    }

    let (acks, mut batches) = client::publish_events(harness.channel.clone())
        .await
        .expect("subscribes");
    let received = receive_all(&acks, &mut batches, 150).await;
    assert_eq!(received, (1..=150).collect::<Vec<u64>>());
    wait_for_cursor(&harness.reader, 150).await;

    drop(acks);
    drop(batches);
    finish(harness).await;
}

#[tokio::test]
async fn a_destination_behind_the_live_ring_is_replayed_from_the_journal_in_order() {
    // The live ring holds 4096 events: the first 904 of these 5000 are only in the journal.
    let harness = harness(DESTINATION, true).await;
    let mut committer = harness.sink.clone();
    for first in (1..=5_000u32).step_by(1_000) {
        committer
            .commit(&batch(first..=first + 999))
            .expect("committed");
    }

    let (acks, mut batches) = client::publish_events(harness.channel.clone())
        .await
        .expect("subscribes");
    let received = receive_all(&acks, &mut batches, 5_000).await;
    assert_eq!(received, (1..=5_000).collect::<Vec<u64>>());
    wait_for_cursor(&harness.reader, 5_000).await;

    drop(acks);
    drop(batches);
    finish(harness).await;
}

#[tokio::test]
async fn a_new_subscription_resumes_after_the_acknowledged_position() {
    let harness = harness(DESTINATION, true).await;
    let mut committer = harness.sink.clone();
    committer.commit(&batch(1..=100)).expect("committed");

    let (acks, mut batches) = client::publish_events(harness.channel.clone())
        .await
        .expect("subscribes");
    let first = next_delivery(&mut batches).await;
    assert_eq!(first.events.len(), PUBLISH_BATCH);
    acks.send(pb::DeliveryAck {
        sequence: first.sequence,
    })
    .await
    .expect("acked");
    let acknowledged = i64::try_from(PUBLISH_BATCH).expect("fits i64");
    wait_for_cursor(&harness.reader, acknowledged).await;
    drop(acks);
    drop(batches);

    // The events the first subscription sent but did not get acknowledged come again.
    let (acks, mut batches) = client::publish_events(harness.channel.clone())
        .await
        .expect("subscribes again");
    let resumed = next_delivery(&mut batches).await;
    let ids: Vec<u64> = resumed.events.iter().map(sequence_of).collect();
    assert_eq!(ids, (65..=100).collect::<Vec<u64>>());

    drop(acks);
    drop(batches);
    finish(harness).await;
}

#[tokio::test]
async fn an_undeclared_destination_is_refused() {
    let harness = harness("ghost", false).await;
    assert!(client::publish_events(harness.channel.clone())
        .await
        .is_err());
    finish(harness).await;
}
