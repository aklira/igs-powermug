//! The identity of the issuer and its granularity (SPECIFICATIONS 9.3, GW 11.6, FUN-CMD-003).

use std::net::SocketAddr;

use powermug_command::identity::{iec104_plain, iec104_tls, mqtt5, opc_ua, rest};
use powermug_command::{
    authorize, Acl, CommandJournal, Grant, Granularity, Mode, NewCommand, Request, Value, ValueRule,
};

#[test]
fn each_protocol_gives_the_granularity_of_the_table() {
    let peer: SocketAddr = "192.0.2.10:2404".parse().expect("address");
    assert_eq!(iec104_plain(peer, 7).granularity, Granularity::Link);
    assert_eq!(iec104_plain(peer, 7).subject, "192.0.2.10:2404 ca=7");
    assert_eq!(
        iec104_tls("CN=upstream-a").granularity,
        Granularity::Upstream
    );
    assert_eq!(mqtt5("CN=client-1").granularity, Granularity::Client);
    assert_eq!(opc_ua("CN=app", None).granularity, Granularity::Application);
    assert_eq!(
        opc_ua("CN=app", Some("operator")).granularity,
        Granularity::User
    );
    assert_eq!(rest("CN=gateway", None).granularity, Granularity::Client);
    assert_eq!(
        rest("CN=gateway", Some("operator")).granularity,
        Granularity::User
    );
}

#[test]
fn granularities_are_ordered_from_the_coarsest_to_the_finest_and_round_trip() {
    let levels = [
        Granularity::Link,
        Granularity::Upstream,
        Granularity::Client,
        Granularity::Application,
        Granularity::User,
    ];
    assert!(levels.windows(2).all(|pair| pair[0] < pair[1]));
    for level in levels {
        assert_eq!(Granularity::from_code(level.code()), Some(level));
    }
    assert_eq!(Granularity::from_code("anonymous"), None);
}

fn acl_per_user() -> Acl {
    let mut acl = Acl::new();
    acl.grant(
        "S3/TC_101",
        Grant {
            issuer: "operator".into(),
            operations: vec!["direct_execute".into()],
            values: ValueRule::Bool,
            min_level: Granularity::User,
        },
    );
    acl
}

fn request(level: Granularity) -> Request<'static> {
    Request {
        issuer: "operator",
        identity_level: level,
        point: "S3/TC_101",
        operation: "direct_execute",
        value: Value::Bool(true),
    }
}

#[test]
fn a_grant_per_user_is_not_satisfied_by_a_link_identity() {
    let acl = acl_per_user();
    let refusal =
        authorize(&acl, Mode::Normal, &request(Granularity::Link)).expect_err("too coarse");
    assert_eq!(refusal.reason, "identity level too coarse");
    assert!(authorize(&acl, Mode::Normal, &request(Granularity::User)).is_ok());
}

#[test]
fn the_journal_records_the_identity_and_its_level() {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut journal = CommandJournal::open(dir.path().join("critical.db")).expect("opens");
    journal
        .submit(
            &NewCommand {
                command_id: "c1".into(),
                identity: "192.0.2.10:2404 ca=7".into(),
                identity_level: Granularity::Link,
                point: "S3/TC_101".into(),
                operation: "direct_execute".into(),
                value: "true".into(),
                expires_ns: i64::MAX,
                expected_revision: "r1".into(),
                owner_epoch: 1,
                ha_term: 1,
            },
            1,
        )
        .expect("submits");
    assert_eq!(
        journal.identity("c1").expect("reads"),
        Some(("192.0.2.10:2404 ca=7".to_owned(), Granularity::Link))
    );
    assert_eq!(journal.identity("missing").expect("reads"), None);
}
