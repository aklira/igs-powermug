//! The producer binary against a real Kafka-compatible broker. The events of a destination reach the
//! topic in order, each with its `event_id` header, and the cursor of the destination follows the
//! broker's acknowledgements.
//!
//! The test needs a broker, so it is ignored by default. Set `POWERMUG_KAFKA_BROKERS` (for example
//! `127.0.0.1:19092`) and run `cargo test -p powermug-adapter-kafka --test broker -- --ignored`.

use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use powermug_adapter_kafka::record::{EVENT_ID, REPLAY};
use powermug_adapter_kafka::Settings;
use powermug_contract::peer::ExpectedPeer;
use powermug_contract::proto::v1 as pb;
use powermug_contract::server::serve_instance;
use powermug_contract::service::CoreConfig;
use powermug_contract::sink::BatchSink;
use powermug_core::lane::{Lane, PointSpec, PointTable, SessionId};
use powermug_core::{Clock, LaneSink};
use powermug_pivot::proto::v1 as pivot;
use powermug_store::{EventClass, Start, TelemetryLog};
use prost::Message;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::{Headers, Message as _};
use rdkafka::ClientConfig;
use tokio::net::UnixListener;
use tokio::sync::oneshot;

const DESTINATION: &str = "kafka-dc1";
const EVENTS: u32 = 100;

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

/// The value of an event carries its journal sequence in these tests.
fn sequence_of(envelope: &pivot::Envelope) -> u64 {
    match envelope
        .value
        .as_ref()
        .and_then(|value| value.kind.as_ref())
    {
        Some(pivot::value::Kind::IntValue(number)) => u64::try_from(*number).unwrap_or(0),
        _ => 0,
    }
}

fn unique_name(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("after the epoch")
        .as_nanos();
    format!("{prefix}-{nanos}")
}

/// One record as the consumer reads it back.
#[derive(Debug)]
struct Received {
    key: String,
    payload: Vec<u8>,
    event_id: Option<String>,
    replay: Option<String>,
}

/// Reads `count` records of `topic` from its beginning, or gives up after `timeout`.
fn consume(brokers: &str, topic: &str, count: usize, timeout: Duration) -> Vec<Received> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", unique_name("powermug-test"))
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .expect("consumer");
    consumer.subscribe(&[topic]).expect("subscribes");
    let deadline = std::time::Instant::now() + timeout;
    let mut received = Vec::new();
    while received.len() < count && std::time::Instant::now() < deadline {
        let Some(Ok(message)) = consumer.poll(Duration::from_millis(500)) else {
            continue;
        };
        let mut event_id = None;
        let mut replay = None;
        if let Some(headers) = message.headers() {
            for index in 0..headers.count() {
                let header = headers.get(index);
                let value = header
                    .value
                    .map(|bytes| String::from_utf8_lossy(bytes).into_owned());
                if header.key == EVENT_ID {
                    event_id = value;
                } else if header.key == REPLAY {
                    replay = value;
                }
            }
        }
        received.push(Received {
            key: String::from_utf8_lossy(message.key().unwrap_or_default()).into_owned(),
            payload: message.payload().unwrap_or_default().to_vec(),
            event_id,
            replay,
        });
    }
    received
}

/// The store, with the destination declared as the cell declares it, and a lane that holds the
/// events. Returns the reader of the store, and the sink that commits to the lane.
fn store_with_events(dir: &Path) -> (TelemetryLog, LaneSink) {
    let path = dir.join("telemetry.db");
    let mut reader = TelemetryLog::open(&path).expect("opens");
    reader
        .declare_cursor(DESTINATION, true, Start::OldestRetained, 1)
        .expect("declares the destination");
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
    let mut sink = LaneSink::new(lane, Clock::new());
    sink.commit(&pb::EventBatch {
        source_epoch: 1,
        batch_sequence: 1,
        events: (1..=EVENTS).map(envelope).collect(),
    })
    .expect("committed");
    (reader, sink)
}

/// Waits until the store holds the cursor of the destination at `expected`.
async fn wait_for_cursor(reader: &TelemetryLog, expected: i64) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
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
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Checks the records of the topic: every event, in order, with its headers.
fn check_records(received: &[Received]) {
    assert_eq!(
        received.len(),
        EVENTS as usize,
        "every event reaches the topic"
    );
    assert!(received.iter().all(|record| record.key == "S3/TM_001"));
    let ids: Vec<u64> = received
        .iter()
        .map(|record| {
            let envelope = pivot::Envelope::decode(record.payload.as_slice()).expect("decodes");
            sequence_of(&envelope)
        })
        .collect();
    assert_eq!(
        ids,
        (1..=u64::from(EVENTS)).collect::<Vec<u64>>(),
        "in order"
    );
    assert!(received
        .iter()
        .all(|record| record.event_id.as_deref().is_some_and(|id| id.len() == 32)));
    assert!(received
        .iter()
        .all(|record| record.replay.as_deref() == Some("false")));
}

/// Writes the settings the producer reads, and starts it as its own process.
fn start_producer(dir: &Path, settings: &Settings) -> Child {
    let settings_path: PathBuf = dir.join("settings.json");
    std::fs::write(
        &settings_path,
        serde_json::to_string(settings).expect("serializes"),
    )
    .expect("writes the settings");
    Command::new(env!("CARGO_BIN_EXE_powermug-kafka-producer"))
        .arg("--settings")
        .arg(&settings_path)
        .spawn()
        .expect("the producer starts")
}

#[tokio::test]
#[ignore = "needs a Kafka-compatible broker: set POWERMUG_KAFKA_BROKERS and run with --ignored"]
async fn the_events_reach_the_topic_in_order_and_the_cursor_follows_the_broker() {
    let brokers = std::env::var("POWERMUG_KAFKA_BROKERS")
        .expect("POWERMUG_KAFKA_BROKERS names the broker, for example 127.0.0.1:19092");
    let topic = unique_name("powermug-test");
    let dir = tempfile::tempdir().expect("temp dir");
    let (reader, sink) = store_with_events(dir.path());

    // The producer runs as its own process, as the cell starts it: the core expects its process.
    let settings = Settings {
        core_socket: dir.path().join("core.sock"),
        destination: DESTINATION.into(),
        brokers: brokers.clone(),
        topic: topic.clone(),
        artifact_sha256: "ab".repeat(32),
    };
    let listener = UnixListener::bind(&settings.core_socket).expect("bind");
    let mut producer = start_producer(dir.path(), &settings);
    let uid = std::fs::metadata(dir.path()).expect("metadata").uid();
    let config = CoreConfig {
        offer: powermug_adapter_kafka::offer(),
        expected_peer: ExpectedPeer {
            uid,
            pid: i32::try_from(producer.id()).expect("pid fits i32"),
        },
    };
    let (stop, stopped) = oneshot::channel::<()>();
    let core = tokio::spawn(async move {
        let _ = serve_instance(config, Box::new(sink), listener, async {
            let _ = stopped.await;
        })
        .await;
    });

    // The consumer blocks, so it runs on the blocking pool while the core serves the producer.
    let received = tokio::task::spawn_blocking(move || {
        consume(&brokers, &topic, EVENTS as usize, Duration::from_secs(90))
    })
    .await
    .expect("the consumer finishes");
    check_records(&received);

    // The broker has acknowledged every record, so the cursor has moved to the last event.
    wait_for_cursor(&reader, i64::from(EVENTS)).await;

    let _ = producer.kill();
    let _ = producer.wait();
    let _ = stop.send(());
    let _ = core.await;
}
