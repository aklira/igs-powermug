//! The emission of a command is bracketed by the journal (PM-CMD-002): the intent is written
//! before the call, the result right after, and an emission that is never finished leaves the
//! command in doubt, never silently lost.

use powermug_command::{CommandWriter, Emission, NewCommand, Outcome, Phase, State, Submission};

const T0: i64 = 1_000;
const FAR: i64 = 10_000_000_000;

fn command(id: &str) -> NewCommand {
    NewCommand {
        command_id: id.into(),
        identity: "scada-demo".into(),
        identity_level: powermug_command::Granularity::User,
        point: "S3/TC_101".into(),
        operation: "direct_execute".into(),
        value: "true".into(),
        expires_ns: FAR,
        expected_revision: "r1".into(),
        owner_epoch: 1,
        ha_term: 1,
    }
}

/// A command ready to be emitted, in a journal of its own.
fn ready(client: &powermug_command::CommandClient, id: &str) {
    assert_eq!(
        client.submit(command(id), T0).expect("submits"),
        Submission::Created
    );
    client
        .move_to(id, State::Validated, T0, "")
        .expect("validated");
    client
        .move_to(id, State::DurablyAccepted, T0, "")
        .expect("accepted");
    client.move_to(id, State::Ready, T0, "").expect("ready");
}

fn open(dir: &tempfile::TempDir) -> (powermug_command::CommandClient, CommandWriter) {
    CommandWriter::spawn(dir.path().join("critical.db")).expect("spawns")
}

#[test]
fn a_sent_call_is_recorded_with_its_intent_closed_and_its_result() {
    let dir = tempfile::tempdir().expect("temp dir");
    let (client, writer) = open(&dir);
    ready(&client, "c1");

    let emission = Emission::begin(&client, "c1", State::ExecuteSent, T0 + 1).expect("intent");
    // The state does not change before the result.
    assert_eq!(client.state("c1"), Ok(Some(State::Ready)));
    let state = emission
        .finish(Outcome::Sent("accepted by the peer".into()), T0 + 2)
        .expect("result");

    assert_eq!(state, State::ExecuteSent);
    let history = client.history("c1").expect("history");
    let intent = history
        .iter()
        .find(|t| t.phase == Phase::Intent)
        .expect("intent");
    assert!(intent.closed);
    drop(client);
    writer.join();
}

#[test]
fn a_call_that_did_not_leave_keeps_the_command_where_it_was() {
    let dir = tempfile::tempdir().expect("temp dir");
    let (client, writer) = open(&dir);
    ready(&client, "c1");

    let emission = Emission::begin(&client, "c1", State::ExecuteSent, T0 + 1).expect("intent");
    let state = emission
        .finish(Outcome::NotSent("connection down".into()), T0 + 2)
        .expect("closed");

    assert_eq!(state, State::Ready);
    assert_eq!(client.state("c1"), Ok(Some(State::Ready)));
    drop(client);
    writer.join();
}

#[test]
fn a_call_of_unknown_outcome_puts_the_command_in_doubt() {
    let dir = tempfile::tempdir().expect("temp dir");
    let (client, writer) = open(&dir);
    ready(&client, "c1");

    let emission = Emission::begin(&client, "c1", State::ExecuteSent, T0 + 1).expect("intent");
    let state = emission
        .finish(Outcome::Unknown("timeout".into()), T0 + 2)
        .expect("closed");

    assert_eq!(state, State::InDoubt);
    drop(client);
    writer.join();
}

#[test]
fn an_emission_dropped_before_its_result_puts_the_command_in_doubt() {
    let dir = tempfile::tempdir().expect("temp dir");
    let (client, writer) = open(&dir);
    ready(&client, "c1");

    {
        let _emission = Emission::begin(&client, "c1", State::ExecuteSent, T0 + 1).expect("intent");
        // The caller panics or returns before the call's outcome is known.
    }

    assert_eq!(client.state("c1"), Ok(Some(State::InDoubt)));
    drop(client);
    writer.join();
}

#[test]
fn an_intent_outside_the_table_is_refused_before_anything_is_sent() {
    let dir = tempfile::tempdir().expect("temp dir");
    let (client, writer) = open(&dir);
    assert_eq!(
        client.submit(command("c1"), T0).expect("submits"),
        Submission::Created
    );

    // From RECEIVED, the table allows no execution: nothing is emitted.
    assert!(Emission::begin(&client, "c1", State::ExecuteSent, T0).is_err());
    assert_eq!(client.state("c1"), Ok(Some(State::Received)));
    drop(client);
    writer.join();
}
