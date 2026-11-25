//! The indicators as `OpenMetrics` text on `GET /metrics`, behind the same mutual TLS as the rest of
//! the management API (SPECIFICATIONS 11.3, PM-IND-001).

mod common;

use std::sync::Arc;

use common::*;
use powermug_core::{Indicator, IndicatorSource};

/// One indicator of the site S3, and one that has not been measured yet.
struct Fixed;

impl IndicatorSource for Fixed {
    fn sample(&self, now_wall_ns: i64) -> Vec<Indicator> {
        vec![
            Indicator {
                name: "command_in_doubt_count",
                scope: "site",
                site: Some("S3".into()),
                session: None,
                value: Some(2.0),
                unit: "count",
                measured_at_ns: now_wall_ns,
            },
            Indicator {
                name: "iec104_connection_up",
                scope: "site",
                site: Some("S3".into()),
                session: Some(1),
                value: None,
                unit: "boolean",
                measured_at_ns: now_wall_ns,
            },
        ]
    }
}

#[tokio::test]
async fn the_indicators_are_served_as_openmetrics_behind_mutual_tls() {
    let pki = pki();
    let dir = tempfile::tempdir().expect("temp dir");
    let address = serve_with(
        &pki,
        empty_store(&dir),
        signing_key(1).verifying_key(),
        Arc::new(Fixed),
    )
    .await;

    let (status, text) = request(
        address,
        client_with_certificate(&pki),
        "GET",
        "/metrics",
        "",
    )
    .await
    .expect("answers");
    assert_eq!(status, 200);
    assert!(
        text.contains("# TYPE command_in_doubt_count gauge"),
        "{text}"
    );
    assert!(
        text.contains("command_in_doubt_count{scope=\"site\",site=\"S3\"} 2 "),
        "{text}"
    );
    assert!(
        !text.contains("iec104_connection_up"),
        "a value not measured is left out"
    );
    assert!(text.ends_with("# EOF\n"), "{text}");

    assert!(
        request(
            address,
            client_without_certificate(&pki),
            "GET",
            "/metrics",
            ""
        )
        .await
        .is_err(),
        "no client certificate, no metrics"
    );
}
