//! The MQTT client binary against a real MQTT 5 broker. The records of a destination reach the broker
//! in order, each under its point's topic, with its `event_id` as a user property, and the cursor of
//! the destination follows the broker's acknowledgements.
//!
//! The test needs a broker, so it is ignored by default. Set `POWERMUG_MQTT_BROKER` to `host:port` (for
//! example `127.0.0.1:11883`) and run `cargo test -p powermug-adapter-mqtt --test broker -- --ignored`.

use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::process::{Child, Command};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use powermug_adapter_mqtt::Settings;
use powermug_contract::peer::ExpectedPeer;
use powermug_contract::proto::v1 as pb;
use powermug_contract::server::serve_instance;
use powermug_contract::service::CoreConfig;
use powermug_contract::sink::BatchSink;
use powermug_core::lane::{Lane, PointSpec, PointTable, SessionId};
use powermug_core::{Clock, LaneSink};
use powermug_north::record::{EVENT_ID, REPLAY};
use powermug_pivot::proto::v1 as pivot;
use powermug_store::{EventClass, Start, TelemetryLog};
use prost::Message;
use rumqttc::v5::mqttbytes::QoS;
use rumqttc::v5::{AsyncClient, Event, Incoming, MqttOptions};
use tokio::net::UnixListener;
use tokio::sync::oneshot;

const DESTINATION: &str = "mqtt-dc1";
const POINT: &str = "S3/TM_001";
const EVENTS: u32 = 100;

fn envelope(id: u32) -> pivot::Envelope {
    let mut event_id = vec![0u8; 16];
    event_id[6] = 0x70;
    event_id[8] = 0x80;
    event_id[12..16].copy_from_slice(&id.to_be_bytes());
    pivot::Envelope {
        event_id,
        site_id: "S3".into(),
        point_id: POINT.into(),
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

/// One publication as the subscriber reads it back.
#[derive(Debug)]
struct Received {
    topic: String,
    payload: Vec<u8>,
    properties: Vec<(String, String)>,
}

impl Received {
    fn property(&self, name: &str) -> Option<&str> {
        self.properties
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    }
}

/// Subscribes to `filter` and collects `count` publications. `ready` fires once the broker has
/// confirmed the subscription, so no publication can precede it.
async fn subscribe_and_collect(
    host: String,
    port: u16,
    filter: String,
    count: usize,
    ready: oneshot::Sender<()>,
) -> Vec<Received> {
    let options = MqttOptions::new(unique_name("powermug-test-subscriber"), host, port);
    let (client, mut events) = AsyncClient::new(options, 64);
    client
        .subscribe(filter, QoS::AtLeastOnce)
        .await
        .expect("subscribes");
    let mut ready = Some(ready);
    let mut received = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    while received.len() < count {
        let event = tokio::time::timeout_at(deadline, events.poll())
            .await
            .expect("the publications arrive")
            .expect("the subscriber stays connected");
        match event {
            Event::Incoming(Incoming::SubAck(_)) => {
                if let Some(ready) = ready.take() {
                    let _ = ready.send(());
                }
            }
            Event::Incoming(Incoming::Publish(publish)) => received.push(Received {
                topic: String::from_utf8_lossy(&publish.topic).into_owned(),
                payload: publish.payload.to_vec(),
                properties: publish
                    .properties
                    .map(|properties| properties.user_properties)
                    .unwrap_or_default(),
            }),
            _ => {}
        }
    }
    received
}

/// The store, with the destination declared as the cell declares it, and a lane that holds the events.
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
            point_id: POINT.into(),
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

fn start_client(dir: &Path, settings: &Settings) -> Child {
    let settings_path = dir.join("settings.json");
    std::fs::write(
        &settings_path,
        serde_json::to_string(settings).expect("serializes"),
    )
    .expect("writes the settings");
    Command::new(env!("CARGO_BIN_EXE_powermug-mqtt-client"))
        .arg("--settings")
        .arg(&settings_path)
        .spawn()
        .expect("the client starts")
}

#[tokio::test]
#[ignore = "needs an MQTT 5 broker: set POWERMUG_MQTT_BROKER to host:port and run with --ignored"]
async fn the_records_reach_the_broker_in_order_and_the_cursor_follows() {
    let broker = std::env::var("POWERMUG_MQTT_BROKER")
        .expect("POWERMUG_MQTT_BROKER names the broker, for example 127.0.0.1:11883");
    let (host, port) = broker.rsplit_once(':').expect("the broker is host:port");
    let port: u16 = port.parse().expect("the port is a number");
    let prefix = unique_name("powermug-test");
    let dir = tempfile::tempdir().expect("temp dir");
    let (reader, sink) = store_with_events(dir.path());

    // The subscriber is confirmed before the client starts, so no publication is missed.
    let (ready, confirmed) = oneshot::channel();
    let subscriber = tokio::spawn(subscribe_and_collect(
        host.to_owned(),
        port,
        format!("{prefix}/#"),
        EVENTS as usize,
        ready,
    ));
    confirmed.await.expect("the subscription is confirmed");

    // The client runs as its own process, as the cell starts it: the core expects its process.
    let settings = Settings {
        core_socket: dir.path().join("core.sock"),
        destination: DESTINATION.into(),
        broker_host: host.to_owned(),
        broker_port: port,
        client_id: unique_name("powermug-test-client"),
        topic_prefix: prefix.clone(),
        artifact_sha256: "ab".repeat(32),
    };
    let listener = UnixListener::bind(&settings.core_socket).expect("bind");
    let mut client = start_client(dir.path(), &settings);
    let uid = std::fs::metadata(dir.path()).expect("metadata").uid();
    let config = CoreConfig {
        offer: powermug_north::offer(&settings.subscription()),
        expected_peer: ExpectedPeer {
            uid,
            pid: i32::try_from(client.id()).expect("pid fits i32"),
        },
    };
    let (stop, stopped) = oneshot::channel::<()>();
    let core = tokio::spawn(async move {
        let _ = serve_instance(config, Box::new(sink), listener, async {
            let _ = stopped.await;
        })
        .await;
    });

    let received = subscriber.await.expect("the subscriber finishes");
    let topic = format!("{prefix}/{POINT}");
    assert_eq!(
        received.len(),
        EVENTS as usize,
        "every record reaches the broker"
    );
    assert!(received
        .iter()
        .all(|publication| publication.topic == topic));
    let ids: Vec<u64> = received
        .iter()
        .map(|publication| {
            let envelope =
                pivot::Envelope::decode(publication.payload.as_slice()).expect("decodes");
            sequence_of(&envelope)
        })
        .collect();
    assert_eq!(
        ids,
        (1..=u64::from(EVENTS)).collect::<Vec<u64>>(),
        "in order"
    );
    assert!(received.iter().all(|publication| publication
        .property(EVENT_ID)
        .is_some_and(|id| id.len() == 32)));
    assert!(received
        .iter()
        .all(|publication| publication.property(REPLAY) == Some("false")));

    // The broker has acknowledged every publication, so the cursor has moved to the last event.
    wait_for_cursor(&reader, i64::from(EVENTS)).await;

    let _ = client.kill();
    let _ = client.wait();
    let _ = stop.send(());
    let _ = core.await;
}
