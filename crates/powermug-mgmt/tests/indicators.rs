//! `GET /indicators` over real TLS, from a lane of a real telemetry log (§11.3).

mod common;

use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use common::*;
use powermug_core::lane::{Lane, PointSpec, PointTable, SessionId};
use powermug_core::IndicatorSource;
use powermug_core::Times;
use powermug_pivot::{EventId, PointEvent, PointIndex, Quality, Value};
use powermug_store::{EventClass, TelemetryLog};
use serde_json::Value as Json;

fn now_ns() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos(),
    )
    .expect("fits i64")
}

fn lane(dir: &tempfile::TempDir) -> Lane {
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
    Lane::new(
        table,
        TelemetryLog::open(dir.path().join("t.db")).expect("opens"),
        0,
    )
}

fn event(id: u8, now: i64) -> PointEvent {
    let mut bytes = [0u8; 16];
    bytes[6] = 0x70;
    bytes[8] = 0x80;
    bytes[15] = id;
    PointEvent {
        point: PointIndex(0),
        value: Value::Int(1),
        quality: Quality::GOOD,
        source_time_ns: now,
        receive_time_ns: now,
        process_time_ns: now,
        event_id: EventId::from_bytes(bytes).expect("v7"),
        revision_index: 1,
    }
}

fn indicator<'a>(document: &'a Json, name: &str, session: Option<u32>) -> &'a Json {
    document["indicators"]
        .as_array()
        .expect("array")
        .iter()
        .find(|i| i["name"] == name && i["session"] == session.map_or(Json::Null, Json::from))
        .unwrap_or_else(|| panic!("no indicator {name} for {session:?}"))
}

#[tokio::test]
async fn an_empty_lane_reports_no_last_seen_value_and_its_session_up() {
    let pki = pki();
    let dir = tempfile::tempdir().expect("temp dir");
    let source: Arc<dyn IndicatorSource> = Arc::new(Mutex::new(lane(&dir)));
    let address = serve_with(
        &pki,
        empty_store(&dir),
        signing_key(1).verifying_key(),
        source,
    )
    .await;

    let (status, body) = request(
        address,
        client_with_certificate(&pki),
        "GET",
        "/indicators",
        "",
    )
    .await
    .expect("answers");

    assert_eq!(status, 200);
    let document: Json = serde_json::from_str(&body).expect("json");
    assert!(indicator(&document, "telemetry_last_seen_seconds", None)["value"].is_null());
    assert_eq!(
        indicator(&document, "iec104_connection_up", Some(1))["value"],
        1.0
    );
}

#[tokio::test]
async fn last_seen_and_connection_follow_the_lane() {
    let pki = pki();
    let dir = tempfile::tempdir().expect("temp dir");
    let now = now_ns();
    let shared = Arc::new(Mutex::new(lane(&dir)));
    shared
        .lock()
        .expect("lock")
        .ingest(
            &[event(1, now - 5_000_000_000)],
            Times {
                monotonic_ns: 0,
                wall_ns: now - 5_000_000_000,
            },
        )
        .expect("accepts an event five seconds ago");
    let source: Arc<dyn IndicatorSource> = Arc::clone(&shared) as Arc<dyn IndicatorSource>;
    let address = serve_with(
        &pki,
        empty_store(&dir),
        signing_key(1).verifying_key(),
        source,
    )
    .await;

    let (_, body) = request(
        address,
        client_with_certificate(&pki),
        "GET",
        "/indicators",
        "",
    )
    .await
    .expect("answers");
    let document: Json = serde_json::from_str(&body).expect("json");
    let seen = indicator(&document, "telemetry_last_seen_seconds", None)["value"]
        .as_f64()
        .expect("measured");
    assert!((5.0..10.0).contains(&seen), "last seen {seen}");
    assert_eq!(
        indicator(&document, "telemetry_last_seen_seconds", None)["scope"],
        "target"
    );

    shared
        .lock()
        .expect("lock")
        .session_lost(
            SessionId(1),
            "keepalive timeout",
            Times {
                monotonic_ns: 1,
                wall_ns: now,
            },
        )
        .expect("loss");
    let (_, body) = request(
        address,
        client_with_certificate(&pki),
        "GET",
        "/indicators",
        "",
    )
    .await
    .expect("answers");
    let document: Json = serde_json::from_str(&body).expect("json");
    assert_eq!(
        indicator(&document, "iec104_connection_up", Some(1))["value"],
        0.0
    );
    assert_eq!(
        indicator(&document, "iec104_connection_up", Some(1))["scope"],
        "site"
    );
}
