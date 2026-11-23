//! Partition epochs and admission to dispatch (SPECIFICATIONS 9.4, PM-CMD-004, PM-CMD-005,
//! FUN-MGT-009, FUN-MGT-010).

use powermug_command::dispatch::{admit_submission, Dispatch, EpochGuard};
use powermug_command::ownership::{Owner, Ownership};
use powermug_command::{CommandJournal, NewCommand, State};

const NOW: i64 = 1_000;
const LATER: i64 = 10_000;

#[test]
fn a_takeover_increases_the_epoch_and_the_previous_owner_is_refused() {
    let mut ownership = Ownership::new();
    assert_eq!(ownership.owner("S3"), Owner::Unowned);
    let first = ownership.takeover("S3");
    assert_eq!(first, 1);
    assert_eq!(ownership.owner("S3"), Owner::Confirmed { epoch: 1 });

    // Cession, then a takeover by another node: the epoch goes up.
    ownership.release("S3", Owner::Ceding);
    assert_eq!(ownership.last_epoch("S3"), 1);
    let second = ownership.takeover("S3");
    assert_eq!(second, 2);

    // The adapter activated at epoch 1 refuses a dispatch carrying epoch 2.
    let old_guard = EpochGuard::activated_at(first);
    let refusal = old_guard
        .admit(Dispatch { epoch: second }, LATER, NOW)
        .expect_err("refused");
    assert_eq!(refusal.state, State::Failed);
}

#[test]
fn a_partition_without_a_confirmed_owner_refuses_every_command_as_unavailable() {
    for owner in [Owner::Ceding, Owner::Shadow, Owner::Unowned] {
        let refusal = admit_submission(owner, LATER, NOW).expect_err("refused");
        assert_eq!(refusal.state, State::RejectedUnavailable, "{owner:?}");
    }
    assert_eq!(
        admit_submission(Owner::Confirmed { epoch: 4 }, LATER, NOW),
        Ok(Dispatch { epoch: 4 })
    );
}

#[test]
fn a_command_expired_at_submission_is_rejected_as_invalid() {
    let refusal = admit_submission(Owner::Confirmed { epoch: 1 }, NOW, NOW).expect_err("expired");
    assert_eq!(refusal.state, State::RejectedInvalid);
}

#[test]
fn a_command_that_expires_before_emission_is_never_sent() {
    let guard = EpochGuard::activated_at(1);
    let dispatch = Dispatch { epoch: 1 };
    assert_eq!(guard.admit(dispatch, LATER, NOW), Ok(()));
    let refusal = guard.admit(dispatch, NOW, NOW).expect_err("expired");
    assert_eq!(refusal.state, State::Expired);
}

#[test]
fn a_rejected_submission_is_recorded_in_the_journal() {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut journal = CommandJournal::open(dir.path().join("critical.db")).expect("opens");
    journal
        .submit(
            &NewCommand {
                command_id: "c1".into(),
                identity: "scada-demo".into(),
                point: "S3/TC_101".into(),
                operation: "direct_execute".into(),
                value: "true".into(),
                expires_ns: LATER,
                expected_revision: "r1".into(),
                owner_epoch: 0,
                ha_term: 1,
            },
            NOW,
        )
        .expect("submits");
    let refusal = admit_submission(Owner::Shadow, LATER, NOW).expect_err("refused");
    journal
        .move_to("c1", refusal.state, NOW, refusal.reason)
        .expect("recorded");
    assert_eq!(
        journal.state("c1").expect("reads"),
        Some(State::RejectedUnavailable)
    );
}
