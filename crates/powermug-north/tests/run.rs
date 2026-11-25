//! The producer against a running core, with a sink that stands for the broker. The batches are
//! produced in order and acknowledged, so the cursor of the destination follows them; a batch that
//! keeps failing is never acknowledged; an undeclared destination is refused.

use std::future::Future;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use powermug_contract::peer::ExpectedPeer;
use powermug_contract::proto::v1 as pb;
use powermug_contract::server::serve_instance;
use powermug_contract::service::CoreConfig;
use powermug_contract::sink::BatchSink;
use powermug_core::lane::{Lane, PointSpec, PointTable, SessionId};
use powermug_core::{Clock, LaneSink};
use powermug_north::record::EVENT_ID;
use powermug_north::{
    offer, run, DeliveryError, Record, RecordSink, RetryPolicy, RunError, Subscription,
};
use powermug_pivot::proto::v1 as pivot;
use powermug_store::{EventClass, Start, TelemetryLog};
use prost::Message;
use tokio::net::UnixListener;
use tokio::sync::oneshot;

const DESTINATION: &str = "kafka-dc1";

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
    match envelope.value.as_ref().and_then(|v| v.kind.as_ref()) {
        Some(pivot::value::Kind::IntValue(number)) => u64::try_from(*number).unwrap_or(0),
        _ => 0,
    }
}

fn capabilities_of(destination: &str) -> pb::Capabilities {
    powermug_north::capabilities(&subscription(Path::new("/unused"), destination))
}

/// The subscription of the test producer. The core offers the same protocol.
fn subscription(socket: &Path, destination: &str) -> Subscription {
    Subscription {
        core_socket: socket.to_path_buf(),
        destination: destination.into(),
        protocol: "test",
        artifact_sha256: "ab".repeat(32),
    }
}

/// A core with one lane, whose destination `DESTINATION` is declared when `declare` is set.
struct Core {
    reader: TelemetryLog,
    sink: LaneSink,
    socket: PathBuf,
    stop: oneshot::Sender<()>,
    task: tokio::task::JoinHandle<()>,
    _dir: tempfile::TempDir,
}

fn core(declare: bool) -> Core {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("telemetry.db");
    let mut reader = TelemetryLog::open(&path).expect("opens");
    if declare {
        reader
            .declare_cursor(DESTINATION, true, Start::OldestRetained, 1)
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
        offer: offer_of_core(),
        expected_peer: ExpectedPeer {
            uid,
            pid: i32::try_from(std::process::id()).expect("pid fits i32"),
        },
    };
    let (stop, stopped) = oneshot::channel::<()>();
    let served = Box::new(sink.clone());
    let task = tokio::spawn(async move {
        let _ = serve_instance(config, served, listener, async {
            let _ = stopped.await;
        })
        .await;
    });
    Core {
        reader,
        sink,
        socket,
        stop,
        task,
        _dir: dir,
    }
}

/// The core's offer for the test producer.
fn offer_of_core() -> powermug_contract::handshake::Offer {
    offer(&subscription(Path::new("/unused"), DESTINATION))
}

impl Core {
    async fn finish(self) {
        let Core { stop, task, .. } = self;
        let _ = stop.send(());
        let _ = task.await;
    }

    fn commit(&self, ids: std::ops::RangeInclusive<u32>) {
        let mut committer = self.sink.clone();
        committer
            .commit(&pb::EventBatch {
                source_epoch: 1,
                batch_sequence: u64::from(*ids.start()),
                events: ids.map(envelope).collect(),
            })
            .expect("committed");
    }
}

/// What the sink received, and how many times it was asked.
#[derive(Debug, Default)]
struct Seen {
    records: Vec<Record>,
    attempts: u32,
}

/// A broker stand-in: it refuses its first `refusals` attempts, then keeps what it is given.
#[derive(Debug)]
struct FakeBroker {
    seen: Arc<Mutex<Seen>>,
    refusals: u32,
}

impl RecordSink for FakeBroker {
    fn deliver<'a>(
        &'a mut self,
        records: &'a [Record],
    ) -> impl Future<Output = Result<(), DeliveryError>> + Send + 'a {
        let mut seen = self.seen.lock().expect("not poisoned");
        seen.attempts += 1;
        let outcome = if self.refusals > 0 {
            self.refusals -= 1;
            Err(DeliveryError::Refused("the broker is busy".into()))
        } else {
            seen.records.extend_from_slice(records);
            Ok(())
        };
        drop(seen);
        std::future::ready(outcome)
    }
}

/// A copy of what the broker received, taken without holding its lock across an await.
fn snapshot(seen: &Arc<Mutex<Seen>>) -> Seen {
    let guard = seen.lock().expect("not poisoned");
    Seen {
        records: guard.records.clone(),
        attempts: guard.attempts,
    }
}

/// Completes once the broker holds `count` records, or fails the test.
async fn until_delivered(seen: Arc<Mutex<Seen>>, count: usize) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if seen.lock().expect("not poisoned").records.len() >= count {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the broker did not receive {count} records"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// Waits until the store holds the cursor of the destination at `expected`.
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

#[tokio::test]
async fn every_event_is_produced_in_order_and_acknowledged_so_the_cursor_follows() {
    let core = core(true);
    core.commit(1..=50);
    core.commit(51..=100);
    core.commit(101..=150);

    let seen = Arc::new(Mutex::new(Seen::default()));
    let mut broker = FakeBroker {
        seen: Arc::clone(&seen),
        refusals: 0,
    };
    let report = run(
        &subscription(&core.socket, DESTINATION),
        &mut broker,
        RetryPolicy::default(),
        until_delivered(Arc::clone(&seen), 150),
    )
    .await
    .expect("the producer runs");

    assert_eq!(report.records, 150);
    assert_eq!(report.batches, 3, "64, 64 and 22 events");
    let received = snapshot(&seen);
    let ids: Vec<u64> = received
        .records
        .iter()
        .map(|record| {
            let envelope = pivot::Envelope::decode(record.payload.as_slice()).expect("decodes");
            sequence_of(&envelope)
        })
        .collect();
    assert_eq!(ids, (1..=150).collect::<Vec<u64>>());
    assert!(received
        .records
        .iter()
        .all(|record| record.key == "S3/TM_001"));
    let event_ids: std::collections::BTreeSet<&str> = received
        .records
        .iter()
        .filter_map(|record| {
            record
                .headers
                .iter()
                .find(|(name, _)| *name == EVENT_ID)
                .map(|(_, value)| value.as_str())
        })
        .collect();
    assert_eq!(event_ids.len(), 150, "every event carries its own event_id");

    wait_for_cursor(&core.reader, 150).await;
    core.finish().await;
}

#[tokio::test]
async fn a_refused_batch_is_delivered_again_and_acknowledged_once() {
    let core = core(true);
    core.commit(1..=10);

    let seen = Arc::new(Mutex::new(Seen::default()));
    let mut broker = FakeBroker {
        seen: Arc::clone(&seen),
        refusals: 2,
    };
    let policy = RetryPolicy {
        attempts: 4,
        first_delay: Duration::from_millis(1),
        max_delay: Duration::from_millis(2),
    };
    let report = run(
        &subscription(&core.socket, DESTINATION),
        &mut broker,
        policy,
        until_delivered(Arc::clone(&seen), 10),
    )
    .await
    .expect("the producer runs");

    assert_eq!(report.records, 10);
    let received = snapshot(&seen);
    assert_eq!(received.attempts, 3, "two refusals, then the delivery");
    assert_eq!(
        received.records.len(),
        10,
        "the refused attempts delivered nothing"
    );

    wait_for_cursor(&core.reader, 10).await;
    core.finish().await;
}

#[tokio::test]
async fn a_batch_that_keeps_failing_stops_the_producer_and_is_never_acknowledged() {
    let core = core(true);
    core.commit(1..=10);

    let seen = Arc::new(Mutex::new(Seen::default()));
    let mut broker = FakeBroker {
        seen: Arc::clone(&seen),
        refusals: u32::MAX,
    };
    let policy = RetryPolicy {
        attempts: 3,
        first_delay: Duration::from_millis(1),
        max_delay: Duration::from_millis(2),
    };
    let result = run(
        &subscription(&core.socket, DESTINATION),
        &mut broker,
        policy,
        std::future::pending::<()>(),
    )
    .await;

    match result {
        Err(RunError::Delivery { attempts, .. }) => assert_eq!(attempts, 3),
        other => panic!("expected a delivery failure after 3 attempts, got {other:?}"),
    }
    assert_eq!(seen.lock().expect("not poisoned").attempts, 3);
    let cursor = core
        .reader
        .cursor(DESTINATION)
        .expect("reads")
        .expect("declared")
        .last_seq;
    assert_eq!(cursor, 0, "nothing was acknowledged, so the cursor stays");
    core.finish().await;
}

#[tokio::test]
async fn an_undeclared_destination_is_refused_by_the_core() {
    let core = core(false);
    let seen = Arc::new(Mutex::new(Seen::default()));
    let mut broker = FakeBroker {
        seen: Arc::clone(&seen),
        refusals: 0,
    };
    let result = run(
        &subscription(&core.socket, DESTINATION),
        &mut broker,
        RetryPolicy::default(),
        std::future::pending::<()>(),
    )
    .await;
    assert!(
        matches!(result, Err(RunError::Core(_))),
        "the core refuses the subscription, got {result:?}"
    );
    assert_eq!(seen.lock().expect("not poisoned").attempts, 0);
    core.finish().await;
}

#[test]
fn the_declared_capabilities_name_the_destination_and_the_protocol() {
    let capabilities = capabilities_of(DESTINATION);
    assert_eq!(capabilities.destination, DESTINATION);
    assert_eq!(capabilities.protocols[0].name, "test");
    assert_eq!(capabilities.protocols[0].functions, vec!["publish_events"]);
}
