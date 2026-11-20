//! The station adapter against a real controlling station of `igs-iec104`.
//!
//! The core holds two points. A controlling station interrogates the adapter's
//! station, then receives a change that is committed to the core afterwards.
//! Everything runs in this process, on loopback.

use std::net::{SocketAddr, TcpListener};
use std::os::unix::fs::MetadataExt;
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

/// The single-point and float values of an ASDU, by object address.
fn values(asdu: &Asdu) -> Vec<(u32, String)> {
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
            .map(|(address, siq)| (address, format!("on={}", siq.on)))
            .collect(),
        Body::M_ME_NC_1(objects) => single(objects)
            .into_iter()
            .map(|(address, (value, _))| (address, format!("float={}", value.as_f32())))
            .collect(),
        _ => Vec::new(),
    }
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "an end-to-end scenario, read from top to bottom"
)]
async fn controlling_station_interrogates_the_core_image_and_gets_changes() {
    let dir = tempfile::tempdir().expect("temp dir");
    let table = PointTable::new(
        7,
        vec![
            point("S3/TM_001", EventClass::A),
            point("S3/TM_002", EventClass::B),
        ],
    )
    .expect("distinct");
    let lane = std::sync::Arc::new(std::sync::Mutex::new(Lane::new(
        table,
        TelemetryLog::open(dir.path().join("telemetry.db")).expect("opens"),
        0,
    )));
    let sink = LaneSink::new(std::sync::Arc::clone(&lane), Clock::new());

    let socket = dir.path().join("core.sock");
    let listener = UnixListener::bind(&socket).expect("bind");
    let uid = std::fs::metadata(dir.path()).expect("metadata").uid();
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
            uid,
            pid: i32::try_from(std::process::id()).expect("pid fits i32"),
        },
    };
    let (core_stop, core_stopped) = oneshot::channel::<()>();
    let mut committer = sink.clone();
    let core = tokio::spawn(async move {
        let _ = serve_instance(config, Box::new(sink), listener, async {
            let _ = core_stopped.await;
        })
        .await;
    });

    // The core already holds the two values when the station starts.
    committer
        .commit(&batch(
            1,
            vec![
                envelope("S3/TM_001", 1, pivot::value::Kind::BoolValue(true)),
                envelope("S3/TM_002", 2, pivot::value::Kind::DoubleValue(0.25)),
            ],
        ))
        .expect("first values committed");

    let station = free_address();
    let (serve_stop, serve_stopped) = oneshot::channel::<()>();
    let options = ServeOptions {
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
    };
    let adapter = tokio::spawn(serve(options, async {
        let _ = serve_stopped.await;
    }));

    // The controlling station: start the transfer, interrogate, then wait for a change.
    let mut controlling = Client::connect(ClientConfig::new(station, CommonAddress::new(1)));
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut interrogated = Vec::new();
    let mut changed_to_off = false;
    let mut committed_change = false;
    while Instant::now() < deadline && !changed_to_off {
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
                let found = values(&asdu);
                if found.iter().any(|(_, v)| v == "on=false") {
                    changed_to_off = true;
                }
                interrogated.extend(found);
                if !committed_change && interrogated.len() >= 2 {
                    // The general interrogation has answered: now change a value in the core.
                    committed_change = true;
                    committer
                        .commit(&batch(
                            2,
                            vec![envelope(
                                "S3/TM_001",
                                3,
                                pivot::value::Kind::BoolValue(false),
                            )],
                        ))
                        .expect("change committed");
                }
            }
            _ => {}
        }
    }

    let _ = serve_stop.send(());
    let report = adapter.await.expect("joins").expect("serves");
    let _ = core_stop.send(());
    let _ = core.await;

    assert!(
        interrogated.contains(&(1, "on=true".to_owned())),
        "{interrogated:?}"
    );
    assert!(
        interrogated
            .iter()
            .any(|(address, v)| *address == 2 && v.starts_with("float=0.25")),
        "{interrogated:?}"
    );
    assert!(changed_to_off, "the change did not reach the station: committed={committed_change} {report:?} seen={interrogated:?}");
    assert!(report.points_added >= 2, "{report:?}");
    assert!(report.updates >= 1, "{report:?}");
}
