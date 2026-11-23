//! Authorization of a command (SPECIFICATIONS 9.3, FUN-CMD-003, FUN-OPS-006, PM-CMD-005).

use powermug_command::{
    authorize, Acl, CommandJournal, Grant, Mode, NewCommand, Refusal, Request, State, Value,
    ValueRule,
};

fn acl() -> Acl {
    let mut acl = Acl::new();
    acl.grant(
        "S3/TC_101",
        Grant {
            issuer: "scada-demo".into(),
            operations: vec!["direct_execute".into(), "select".into()],
            values: ValueRule::Bool,
        },
    );
    acl.grant(
        "S3/TSP_201",
        Grant {
            issuer: "scada-demo".into(),
            operations: vec!["setpoint".into()],
            values: ValueRule::Range {
                min: 0.0,
                max: 100.0,
            },
        },
    );
    acl
}

fn request<'a>(issuer: &'a str, point: &'a str, operation: &'a str, value: Value) -> Request<'a> {
    Request {
        issuer,
        point,
        operation,
        value,
    }
}

fn refusal(result: Result<(), Refusal>) -> (State, &'static str) {
    let refusal = result.expect_err("refused");
    (refusal.state, refusal.reason)
}

#[test]
fn a_granted_issuer_operation_and_value_are_accepted() {
    let acl = acl();
    assert_eq!(
        authorize(
            &acl,
            Mode::Normal,
            &request(
                "scada-demo",
                "S3/TC_101",
                "direct_execute",
                Value::Bool(true)
            )
        ),
        Ok(())
    );
    assert_eq!(
        authorize(
            &acl,
            Mode::Normal,
            &request("scada-demo", "S3/TSP_201", "setpoint", Value::Number(42.5))
        ),
        Ok(())
    );
}

#[test]
fn an_unknown_issuer_an_unknown_operation_and_an_ungranted_point_are_unauthorized() {
    let acl = acl();
    assert_eq!(
        refusal(authorize(
            &acl,
            Mode::Normal,
            &request("intruder", "S3/TC_101", "direct_execute", Value::Bool(true))
        )),
        (State::RejectedAuth, "issuer not granted")
    );
    assert_eq!(
        refusal(authorize(
            &acl,
            Mode::Normal,
            &request("scada-demo", "S3/TC_101", "setpoint", Value::Bool(true))
        )),
        (State::RejectedAuth, "operation not granted")
    );
    assert_eq!(
        refusal(authorize(
            &acl,
            Mode::Normal,
            &request(
                "scada-demo",
                "S3/TC_999",
                "direct_execute",
                Value::Bool(true)
            )
        )),
        (State::RejectedAuth, "not commandable")
    );
}

#[test]
fn a_value_outside_the_granted_rule_is_unauthorized() {
    let acl = acl();
    assert_eq!(
        refusal(authorize(
            &acl,
            Mode::Normal,
            &request("scada-demo", "S3/TSP_201", "setpoint", Value::Number(120.0))
        )),
        (State::RejectedAuth, "value not granted")
    );
    assert_eq!(
        refusal(authorize(
            &acl,
            Mode::Normal,
            &request("scada-demo", "S3/TSP_201", "setpoint", Value::Bool(true))
        )),
        (State::RejectedAuth, "value not granted")
    );
    assert_eq!(
        refusal(authorize(
            &acl,
            Mode::Normal,
            &request(
                "scada-demo",
                "S3/TC_101",
                "direct_execute",
                Value::Number(1.0)
            )
        )),
        (State::RejectedAuth, "value not granted")
    );
}

#[test]
fn maintenance_and_shadow_refuse_every_command_as_unavailable() {
    let acl = acl();
    let valid = request(
        "scada-demo",
        "S3/TC_101",
        "direct_execute",
        Value::Bool(true),
    );
    assert_eq!(
        refusal(authorize(&acl, Mode::Maintenance, &valid)),
        (State::RejectedUnavailable, "maintenance")
    );
    assert_eq!(
        refusal(authorize(&acl, Mode::Shadow, &valid)),
        (State::RejectedUnavailable, "shadow")
    );
}

#[test]
fn the_acl_is_explicit_for_every_commandable_point() {
    let acl = acl();
    assert_eq!(
        acl.missing(&["S3/TC_101", "S3/TSP_201"]),
        Vec::<&str>::new()
    );
    assert_eq!(acl.missing(&["S3/TC_101", "S3/TC_102"]), vec!["S3/TC_102"]);
}

#[test]
fn a_refusal_is_recorded_as_a_terminal_state_in_the_journal() {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut journal = CommandJournal::open(dir.path().join("critical.db")).expect("opens");
    journal
        .submit(
            &NewCommand {
                command_id: "c1".into(),
                identity: "intruder".into(),
                point: "S3/TC_101".into(),
                operation: "direct_execute".into(),
                value: "true".into(),
                expires_ns: i64::MAX,
                expected_revision: "r1".into(),
                owner_epoch: 3,
                ha_term: 1,
            },
            1,
        )
        .expect("submits");
    let refused = authorize(
        &acl(),
        Mode::Normal,
        &request("intruder", "S3/TC_101", "direct_execute", Value::Bool(true)),
    )
    .expect_err("refused");
    journal
        .move_to("c1", refused.state, 2, refused.reason)
        .expect("recorded");
    assert_eq!(
        journal.state("c1").expect("reads"),
        Some(State::RejectedAuth)
    );
    assert!(refused.state.is_terminal());
}
