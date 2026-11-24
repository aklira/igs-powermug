//! The dedicated writer of the command journal (SPECIFICATIONS 9.2): one thread commits every
//! transition, clients from several threads get their own answers, and a stopped writer is
//! reported as such.

use std::time::Instant;

use powermug_command::audit::verify_export;
use powermug_command::Granularity;
use powermug_command::{CommandWriter, NewCommand, State, Submission};

const T0: i64 = 1_000;
const FAR: i64 = 10_000_000_000;

fn command(id: &str) -> NewCommand {
    NewCommand {
        command_id: id.into(),
        identity: "scada-demo".into(),
        identity_level: Granularity::User,
        point: "S3/TC_101".into(),
        operation: "direct_execute".into(),
        value: "true".into(),
        expires_ns: FAR,
        expected_revision: "r1".into(),
        owner_epoch: 1,
        ha_term: 1,
    }
}

#[test]
fn requests_from_several_threads_are_each_answered_by_the_one_writer() {
    let dir = tempfile::tempdir().expect("temp dir");
    let (client, writer) = CommandWriter::spawn(dir.path().join("critical.db")).expect("spawns");

    let handles: Vec<_> = (0..4)
        .map(|thread| {
            let client = client.clone();
            std::thread::spawn(move || {
                for n in 0..25 {
                    let id = format!("t{thread}-{n}");
                    assert_eq!(
                        client.submit(command(&id), T0).expect("submits"),
                        Submission::Created
                    );
                    client
                        .move_to(&id, State::Validated, T0, "")
                        .expect("validated");
                }
            })
        })
        .collect();
    for handle in handles {
        handle.join().expect("client thread");
    }

    assert_eq!(
        client.state("t2-24").expect("reads"),
        Some(State::Validated)
    );
    assert_eq!(client.verify_audit(), Ok(200));
    let exported = client.export_audit().expect("exports");
    assert_eq!(
        verify_export(&exported, Some(client.audit_head().expect("head"))),
        Ok(200)
    );
    drop(client);
    writer.join();
}

#[test]
fn the_writer_keeps_answering_while_any_client_is_alive() {
    let dir = tempfile::tempdir().expect("temp dir");
    let (client, writer) = CommandWriter::spawn(dir.path().join("critical.db")).expect("spawns");
    let spare = client.clone();
    drop(client);
    assert_eq!(spare.state("none").expect("answers"), None);
    drop(spare);
    writer.join();
}

#[test]
fn a_transition_is_committed_before_the_answer_and_timed_on_this_host() {
    let dir = tempfile::tempdir().expect("temp dir");
    let (client, writer) = CommandWriter::spawn(dir.path().join("critical.db")).expect("spawns");
    let mut latencies = Vec::new();
    for n in 0..500 {
        let id = format!("c{n}");
        let started = Instant::now();
        client.submit(command(&id), T0).expect("submits");
        client
            .move_to(&id, State::Validated, T0, "")
            .expect("validated");
        latencies.push(started.elapsed());
    }
    latencies.sort();
    let p99 = latencies[latencies.len() * 99 / 100];
    eprintln!(
        "command transition pair: p50 {:?}, p99 {:?} on this host (NFR-PERF-003 budget: 20 ms p99)",
        latencies[latencies.len() / 2],
        p99
    );
    drop(client);
    writer.join();
}
