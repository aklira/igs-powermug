//! The core's dispatcher (SPECIFICATIONS 9.1 to 9.4): a command is journaled, authorized,
//! admitted by the partition's owner, dispatched, and followed by the adapter's answers. A
//! fake adapter reads the dispatches and sends the answers.

use powermug_cell::commands::{channel, Dispatcher, Request};
use powermug_command::authorization::{Acl, Grant, ValueRule};
use powermug_command::ownership::Ownership;
use powermug_command::{CommandWriter, Granularity, State};
use powermug_contract::proto::v1 as pb;

fn acl() -> Acl {
    let mut acl = Acl::new();
    for operation in ["direct_execute", "select"] {
        acl.grant(
            "S3/TC_101",
            Grant {
                issuer: "scada-demo".into(),
                operations: vec![operation.into()],
                values: ValueRule::Any,
                min_level: Granularity::Link,
            },
        );
    }
    acl
}

fn request(command_id: &str, select: bool) -> Request {
    Request {
        command_id: command_id.into(),
        identity: "scada-demo".into(),
        identity_level: Granularity::Link,
        point: "S3/TC_101".into(),
        operation: if select { "select" } else { "direct_execute" }.into(),
        partition: "S3".into(),
        address: 5,
        kind: pb::CommandKind::Single,
        on: true,
        select,
        expires_ns: i64::MAX,
        expected_revision: "r1".into(),
    }
}

fn answer(command_id: &str, status: pb::CommandStatus) -> pb::CommandUpdate {
    pb::CommandUpdate {
        command_id: command_id.into(),
        status: status as i32,
        reason: String::new(),
    }
}

fn owned() -> Ownership {
    let mut ownership = Ownership::new();
    ownership.takeover("S3");
    ownership
}

struct Harness {
    dispatcher: Dispatcher,
    dispatches: tokio::sync::mpsc::Receiver<pb::CommandDispatch>,
    writer: CommandWriter,
    _dir: tempfile::TempDir,
}

impl Harness {
    /// Closes the dispatcher's journal client, then waits for the writer: it ends only when
    /// every client is gone.
    fn finish(self) {
        let Harness {
            dispatcher, writer, ..
        } = self;
        drop(dispatcher);
        writer.join();
    }
}

fn harness(ownership: Ownership) -> Harness {
    let dir = tempfile::tempdir().expect("temp dir");
    let (client, writer) = CommandWriter::spawn(dir.path().join("critical.db")).expect("spawns");
    let (to_adapter, dispatches) = channel(8);
    Harness {
        dispatcher: Dispatcher::new(client, acl(), ownership, to_adapter),
        dispatches,
        writer,
        _dir: dir,
    }
}

#[test]
fn a_direct_command_is_dispatched_confirmed_and_terminated() {
    let mut h = harness(owned());
    let state = h
        .dispatcher
        .submit(&request("c1", false), 10)
        .expect("submits");
    // The emission is open until the adapter answers: the command stays READY meanwhile.
    assert_eq!(state, State::Ready);
    let dispatched = h.dispatches.try_recv().expect("dispatched");
    assert_eq!(
        (
            dispatched.command_id.as_str(),
            dispatched.select,
            dispatched.owner_epoch
        ),
        ("c1", false, 1)
    );

    assert_eq!(
        h.dispatcher
            .update(&answer("c1", pb::CommandStatus::Confirmed), 20)
            .expect("updates"),
        Some(State::ExecuteConfirmed)
    );
    assert_eq!(
        h.dispatcher
            .update(&answer("c1", pb::CommandStatus::Terminated), 30)
            .expect("updates"),
        Some(State::SuccessUnverified)
    );
    h.finish();
}

#[test]
fn a_selected_command_waits_for_its_execution_and_is_then_terminated() {
    let mut h = harness(owned());
    assert_eq!(
        h.dispatcher
            .submit(&request("c1", true), 10)
            .expect("submits"),
        State::Ready
    );
    assert!(h.dispatches.try_recv().expect("select dispatched").select);

    assert_eq!(
        h.dispatcher
            .update(&answer("c1", pb::CommandStatus::Confirmed), 20)
            .expect("updates"),
        Some(State::SelectConfirmed)
    );
    assert_eq!(
        h.dispatcher.execute("c1", 30).expect("executes"),
        State::SelectConfirmed
    );
    assert!(!h.dispatches.try_recv().expect("execute dispatched").select);
    assert_eq!(
        h.dispatcher
            .update(&answer("c1", pb::CommandStatus::Confirmed), 40)
            .expect("updates"),
        Some(State::ExecuteConfirmed)
    );
    assert_eq!(
        h.dispatcher
            .update(&answer("c1", pb::CommandStatus::Terminated), 50)
            .expect("updates"),
        Some(State::SuccessUnverified)
    );
    h.finish();
}

#[test]
fn a_command_from_an_identity_without_a_grant_is_refused_and_never_dispatched() {
    let mut h = harness(owned());
    let mut intruder = request("c1", false);
    intruder.identity = "intruder".into();
    assert_eq!(
        h.dispatcher.submit(&intruder, 10).expect("submits"),
        State::RejectedAuth
    );
    assert!(h.dispatches.try_recv().is_err());
    h.finish();
}

#[test]
fn a_partition_without_an_owner_refuses_the_command() {
    let mut h = harness(Ownership::new());
    assert_eq!(
        h.dispatcher
            .submit(&request("c1", false), 10)
            .expect("submits"),
        State::RejectedUnavailable
    );
    assert!(h.dispatches.try_recv().is_err());
    h.finish();
}

#[test]
fn a_refused_execution_is_equipment_rejected() {
    let mut h = harness(owned());
    h.dispatcher
        .submit(&request("c1", false), 10)
        .expect("submits");
    assert_eq!(
        h.dispatcher
            .update(&answer("c1", pb::CommandStatus::Refused), 20)
            .expect("updates"),
        Some(State::EquipmentRejected)
    );
    h.finish();
}

#[test]
fn a_command_in_flight_when_the_stream_ends_is_in_doubt_and_not_replayed() {
    let mut h = harness(owned());
    h.dispatcher
        .submit(&request("c1", false), 10)
        .expect("submits");
    h.dispatches.try_recv().expect("dispatched");

    assert_eq!(
        h.dispatcher.stream_ended(20).expect("closes"),
        vec![State::InDoubt]
    );
    assert!(h.dispatches.try_recv().is_err(), "nothing is replayed");
    h.finish();
}

#[test]
fn a_known_command_id_is_answered_with_its_state_and_not_dispatched_again() {
    let mut h = harness(owned());
    h.dispatcher
        .submit(&request("c1", false), 10)
        .expect("submits");
    h.dispatches.try_recv().expect("dispatched");
    assert_eq!(
        h.dispatcher
            .submit(&request("c1", false), 20)
            .expect("again"),
        State::Ready
    );
    assert!(h.dispatches.try_recv().is_err());
    h.finish();
}
