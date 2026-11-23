//! A general interrogation over IEC 104 after a restart (ACC-014): the station adapter
//! answers from the core's image, and a point not yet resynchronised reaches the controlling
//! station with its invalid flag set. A fresh value clears the flag of that point only.
//! Everything runs in this process, on loopback.

use std::net::{SocketAddr, TcpListener};
use std::os::unix::fs::MetadataExt;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use igs_iec104::client::{Client, ClientConfig, Event};
use igs_iec104::Delivery;
use igs_iec104_codec::asdu::{Asdu, Body, Objects};
use igs_iec104_codec::elements::Qoi;
use igs_iec104_codec::header::CommonAddress;
use igs_iec104_link::TransferState;
use powermug_adapter_iec104::{serve, Mapping, ServeOptions};
use powermug_contract::handshake::{Offer, Requirement, Version};
use powermug_contract::peer::ExpectedPeer;
use powermug_contract::proto::v1 as pb;
use powermug_contract::server::serve_instance;
use powermug_contract::service::CoreConfig;
use powermug_contract::sink::BatchSink;
use powermug_core::lane::{Lane, PointSpec, PointTable, SessionId};
use powermug_core::{Clock, LaneSink};
use powermug_pivot::proto::v1 as pivot;
use powermug_store::{EventClass, TelemetryLog};
use tokio::net::UnixListener;
use tokio::sync::oneshot;

fn table() -> PointTable {
    PointTable::new(
        7,
        vec![
            point("S3/TM_001", EventClass::A),
            point("S3/TM_002", EventClass::B),
        ],
    )
    .expect("distinct")
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

fn envelope(point_id: &str, id: u8, value: pivot::value::Kind) -> pivot::Envelope {
    let mut event_id = vec![0u8; 16];
    event_id[6] = 0x70;
    event_id[8] = 0x80;
    event_id[15] = id;
    pivot::Envelope {
        event_id,
        site_id: "S3".into(),
        point_id: point_id.into(),
        value: Some(pivot::Value { kind: Some(value) }),
        quality: Some(pivot::Quality {
            validity: pivot::Validity::Good as i32,
            source: pivot::Source::Process as i32,
            ..Default::default()
        }),
        source_time_ns: 10,
        receive_time_ns: 20,
        ..Default::default()
    }
}

fn batch(sequence: u64, events: Vec<pivot::Envelope>) -> pb::EventBatch {
    pb::EventBatch {
        source_epoch: 1,
        batch_sequence: sequence,
        events,
    }
}

fn free_address() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("binds");
    let address = listener.local_addr().expect("address");
    drop(listener);
    address
}

/// The object address and invalid flag of each point of an interrogation answer.
fn invalid_flags(asdu: &Asdu) -> Vec<(u32, bool)> {
    fn single<T>(objects: &Objects<T>) -> Vec<(u32, &T)> {
        match objects {
            Objects::Individual(items) => items
                .iter()
                .map(|i| (i.address.value(), &i.value))
                .collect(),
            Objects::Sequence { address, values } => values
                .iter()
                .enumerate()
                .map(|(index, value)| (address.value() + u32::try_from(index).unwrap_or(0), value))
                .collect(),
        }
    }
    match &asdu.body {
        Body::M_SP_NA_1(objects) => single(objects)
            .into_iter()
            .map(|(address, siq)| (address, siq.quality.invalid))
            .collect(),
        Body::M_ME_NC_1(objects) => single(objects)
            .into_iter()
            .map(|(address, (_, qds))| (address, qds.quality.invalid))
            .collect(),
        _ => Vec::new(),
    }
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "an end-to-end scenario, read from top to bottom"
)]
async fn an_interrogation_after_a_restart_flags_the_points_not_yet_resynchronised() {
    let dir = tempfile::tempdir().expect("temp dir");
    let journal = dir.path().join("telemetry.db");

    // Before the restart: both points hold values, and a snapshot is taken.
    {
        let lane = Arc::new(Mutex::new(Lane::new(
            table(),
            TelemetryLog::open(&journal).expect("opens"),
            0,
        )));
        let mut before = LaneSink::new(Arc::clone(&lane), Clock::new());
        before
            .commit(&batch(
                1,
                vec![
                    envelope("S3/TM_001", 1, pivot::value::Kind::BoolValue(true)),
                    envelope("S3/TM_002", 2, pivot::value::Kind::DoubleValue(0.25)),
                ],
            ))
            .expect("values committed");
        lane.lock().expect("lock").snapshot().expect("snapshot");
    }

    // After the restart: the image is restored, and every point is degraded.
    let lane = Arc::new(Mutex::new(
        Lane::open(table(), TelemetryLog::open(&journal).expect("opens"), 0).expect("recovers"),
    ));
    let socket = dir.path().join("core.sock");
    let listener = UnixListener::bind(&socket).expect("bind");
    let config = CoreConfig {
        offer: Offer {
            pivot: Version { major: 1, minor: 0 },
            requirements: vec![Requirement {
                protocol: "iec104".into(),
                role: pb::Role::Server,
                function: "general_interrogation".into(),
            }],
            command_variants: Vec::new(),
        },
        expected_peer: ExpectedPeer {
            uid: std::fs::metadata(dir.path()).expect("metadata").uid(),
            pid: i32::try_from(std::process::id()).expect("pid fits i32"),
        },
    };
    let (core_stop, core_stopped) = oneshot::channel::<()>();
    // The committer shares the served sink: a sink publishes on its own channel, and only
    // the adapter's subscription on that channel receives the changes.
    let sink = LaneSink::new(Arc::clone(&lane), Clock::new());
    let mut committer = sink.clone();
    let core = tokio::spawn(async move {
        let _ = serve_instance(config, Box::new(sink), listener, async {
            let _ = core_stopped.await;
        })
        .await;
    });

    let station = free_address();
    let (serve_stop, serve_stopped) = oneshot::channel::<()>();
    let adapter = tokio::spawn(serve(
        ServeOptions {
            listen: station,
            common_address: 1,
            core_socket: socket,
            session_id: "1".into(),
            mapping: Mapping::new(vec![
                (1, 1, "S3/TM_001".to_owned()),
                (1, 2, "S3/TM_002".to_owned()),
            ])
            .expect("distinct"),
            artifact_sha256: "ab".repeat(32),
        },
        async {
            let _ = serve_stopped.await;
        },
    ));

    // The controlling station interrogates twice: once after the restart, and once after a
    // fresh value has arrived for the first point only.
    let mut controlling = Client::connect(ClientConfig::new(station, CommonAddress::new(1)));
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut first_answer: Vec<(u32, bool)> = Vec::new();
    let mut latest: std::collections::BTreeMap<u32, bool> = std::collections::BTreeMap::new();
    let mut second_requested = false;
    let mut fresh_committed = false;
    let mut done = false;
    while Instant::now() < deadline && !done {
        let next = tokio::time::timeout(Duration::from_millis(200), controlling.next_event()).await;
        let Ok(Some(event)) = next else {
            continue;
        };
        match event {
            Event::Connected => {
                controlling.start_data_transfer().await.expect("starts");
            }
            Event::Delivery(Delivery::Transfer(TransferState::Started)) => {
                controlling
                    .interrogate(Qoi::STATION)
                    .await
                    .expect("interrogates");
            }
            Event::Delivery(Delivery::Asdu(asdu)) => {
                for (address, invalid) in invalid_flags(&asdu) {
                    latest.insert(address, invalid);
                    if !second_requested {
                        first_answer.push((address, invalid));
                    }
                }
                if !second_requested && first_answer.len() >= 2 {
                    second_requested = true;
                    committer
                        .commit(&batch(
                            2,
                            vec![envelope(
                                "S3/TM_001",
                                3,
                                pivot::value::Kind::BoolValue(false),
                            )],
                        ))
                        .expect("fresh value committed");
                    fresh_committed = true;
                    controlling
                        .interrogate(Qoi::STATION)
                        .await
                        .expect("interrogates again");
                }
                if fresh_committed
                    && latest.get(&1) == Some(&false)
                    && latest.get(&2) == Some(&true)
                {
                    done = true;
                }
            }
            _ => {}
        }
    }

    let _ = serve_stop.send(());
    let _ = adapter.await.expect("joins");
    let _ = core_stop.send(());
    let _ = core.await;

    assert_eq!(
        first_answer,
        vec![(1, true), (2, true)],
        "after the restart both points are answered invalid"
    );
    assert!(
        done,
        "after the fresh value, point 1 is valid and point 2 stays invalid: {latest:?}"
    );
}
