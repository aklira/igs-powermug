//! The North subscription: the core keeps at most one window of unacknowledged
//! batches, and sends everything once the subscriber acknowledges (SPECIFICATIONS §6.1).

use std::os::unix::fs::MetadataExt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use powermug_contract::client;
use powermug_contract::handshake::{Offer, Requirement, Version};
use powermug_contract::peer::ExpectedPeer;
use powermug_contract::proto::v1 as pb;
use powermug_contract::server::serve_instance;
use powermug_contract::service::{CoreConfig, PUBLISH_WINDOW};
use powermug_contract::sink::BatchSink;
use powermug_core::lane::{Lane, PointSpec, PointTable, SessionId};
use powermug_core::{Clock, LaneSink};
use powermug_pivot::proto::v1 as pivot;
use powermug_store::{EventClass, TelemetryLog};
use tokio::net::UnixListener;
use tokio::sync::oneshot;

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

fn batch(id: u32) -> pb::EventBatch {
    pb::EventBatch {
        source_epoch: 1,
        batch_sequence: u64::from(id),
        events: vec![envelope(id)],
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

#[tokio::test]
async fn the_core_never_holds_more_than_one_window_unacknowledged() {
    let dir = tempfile::tempdir().expect("temp dir");
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
    let log = TelemetryLog::open(dir.path().join("telemetry.db")).expect("opens");
    let lane = Arc::new(Mutex::new(Lane::new(table, log, 0)));
    let sink = LaneSink::new(Arc::clone(&lane), Clock::new());

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
    let mut committer = sink.clone();
    let core = tokio::spawn(async move {
        let _ = serve_instance(config, Box::new(sink), listener, async {
            let _ = stopped.await;
        })
        .await;
    });

    let channel = client::connect(&socket).await.expect("connects");
    client::describe_capabilities(channel.clone(), capabilities())
        .await
        .expect("accepted");
    let (acks, mut batches) = client::publish_events(channel).await.expect("subscribes");

    // More events than one window of batches, each committed on its own.
    let events = u32::try_from(PUBLISH_WINDOW).expect("window fits u32") + 40;
    for id in 1..=events {
        committer.commit(&batch(id)).expect("committed");
    }

    // The subscriber does not acknowledge yet: the core must stop at one window.
    let mut held = Vec::new();
    while let Ok(Ok(Some(delivery))) =
        tokio::time::timeout(Duration::from_millis(500), batches.message()).await
    {
        held.push(delivery);
    }
    assert_ne!(held.len(), 0);
    assert!(
        held.len() <= PUBLISH_WINDOW,
        "{} batches are unacknowledged, the window is {PUBLISH_WINDOW}",
        held.len()
    );

    // Acknowledging opens the window: everything arrives, in order, without a gap.
    let mut delivered: Vec<u64> = held
        .iter()
        .flat_map(|d| d.events.iter().map(sequence_of))
        .collect();
    for delivery in &held {
        acks.send(pb::DeliveryAck {
            sequence: delivery.sequence,
        })
        .await
        .expect("acked");
    }
    while delivered.len() < usize::try_from(events).expect("fits usize") {
        let next = tokio::time::timeout(Duration::from_secs(10), batches.message())
            .await
            .expect("no stall after the acknowledgements")
            .expect("stream open")
            .expect("a batch");
        for event in &next.events {
            delivered.push(sequence_of(event));
        }
        acks.send(pb::DeliveryAck {
            sequence: next.sequence,
        })
        .await
        .expect("acked");
    }
    assert_eq!(delivered, (1..=u64::from(events)).collect::<Vec<_>>());

    drop(acks);
    let _ = stop.send(());
    let _ = core.await;
}

/// The event value carries the commit number, which is the sequence the test checks.
fn sequence_of(envelope: &pivot::Envelope) -> u64 {
    match envelope.value.as_ref().and_then(|v| v.kind.as_ref()) {
        Some(pivot::value::Kind::IntValue(number)) => u64::try_from(*number).unwrap_or(0),
        _ => 0,
    }
}
