//! The emission of a command, bracketed by the journal (PM-CMD-002, SPECIFICATIONS 9.1).
//!
//! The intent of a transition is written before the call, and its result right after. An
//! [`Emission`] that is dropped before its result is recorded marks the command `IN_DOUBT`: a
//! call that may have left is never silently lost, and it is never replayed.

use crate::journal::CommandError;
use crate::state::State;
use crate::writer::CommandClient;

/// What the call did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// The call left and was accepted by the peer: the detail is kept in the journal.
    Sent(String),
    /// The call did not leave: the command stays where it was, with the reason.
    NotSent(String),
    /// The call may or may not have left: the command is `IN_DOUBT`.
    Unknown(String),
}

/// An open emission. Finish it with [`Emission::finish`] once the call has been made.
#[derive(Debug)]
pub struct Emission {
    client: CommandClient,
    command_id: String,
    to: State,
    begun_ns: i64,
    finished: bool,
}

impl Emission {
    /// Writes the intent of the transition `to` for `command_id`. Nothing may be sent before
    /// this returns.
    ///
    /// # Errors
    ///
    /// Returns the journal's refusal: a transition outside the table, or an intent already open.
    pub fn begin(
        client: &CommandClient,
        command_id: &str,
        to: State,
        at_ns: i64,
    ) -> Result<Self, CommandError> {
        client.record_intent(command_id, to, at_ns)?;
        Ok(Self {
            client: client.clone(),
            command_id: command_id.to_owned(),
            to,
            begun_ns: at_ns,
            finished: false,
        })
    }

    /// Records the outcome of the call, and returns the state of the command after it.
    ///
    /// # Errors
    ///
    /// Returns the journal's error; the intent then stays open, and the command is marked
    /// `IN_DOUBT` when this emission is dropped.
    pub fn finish(mut self, outcome: Outcome, at_ns: i64) -> Result<State, CommandError> {
        let state = match outcome {
            Outcome::Sent(detail) => {
                self.client
                    .record_result(&self.command_id, self.to, at_ns, &detail)?
            }
            Outcome::NotSent(reason) => {
                self.client
                    .close_intent_not_sent(&self.command_id, at_ns, &reason)?
            }
            Outcome::Unknown(reason) => {
                self.client
                    .close_intent_unknown(&self.command_id, at_ns, &reason)?
            }
        };
        self.finished = true;
        Ok(state)
    }
}

impl Drop for Emission {
    fn drop(&mut self) {
        if !self.finished {
            // The call's outcome is not known: the command is in doubt. A failure to record
            // this is reported by the journal's open intent at the next restart.
            let _ = self.client.close_intent_unknown(
                &self.command_id,
                self.begun_ns,
                "emission dropped before its result",
            );
        }
    }
}
