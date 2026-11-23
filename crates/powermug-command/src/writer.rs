//! The dedicated writer of the command journal (SPECIFICATIONS 9.2).
//!
//! One thread owns the journal. Every command transition is committed on its own, never
//! batched with telemetry. Callers hold a [`CommandClient`] and wait for each answer.

use std::path::Path;
use std::sync::mpsc::{self, Sender};
use std::thread::JoinHandle;

use crate::journal::{
    CommandError, CommandJournal, NewCommand, Recovery, RecoveryContext, Submission,
};
use crate::state::State;

enum Request {
    Submit(NewCommand, i64, Sender<Result<Submission, CommandError>>),
    MoveTo(
        String,
        State,
        i64,
        String,
        Sender<Result<State, CommandError>>,
    ),
    RecordIntent(String, State, i64, Sender<Result<(), CommandError>>),
    RecordResult(
        String,
        State,
        i64,
        String,
        Sender<Result<State, CommandError>>,
    ),
    Recover(RecoveryContext, Sender<Result<Vec<Recovery>, CommandError>>),
    State(String, Sender<Result<Option<State>, CommandError>>),
    VerifyAudit(Sender<Result<u64, CommandError>>),
    AuditHead(Sender<Result<[u8; 32], CommandError>>),
}

/// The handle used to send requests to the writer. Cloneable; every clone talks to the same
/// thread.
#[derive(Debug, Clone)]
pub struct CommandClient {
    tx: Sender<Request>,
}

/// The writer thread. Joining it waits for every request sent before the last client is
/// dropped.
#[derive(Debug)]
pub struct CommandWriter {
    thread: JoinHandle<()>,
}

impl CommandWriter {
    /// Opens the journal at `path` and starts the writer thread.
    ///
    /// # Errors
    ///
    /// Returns the open error of the journal; no thread is started then.
    pub fn spawn(path: impl AsRef<Path>) -> Result<(CommandClient, Self), CommandError> {
        let mut journal = CommandJournal::open(path)?;
        let (tx, rx) = mpsc::channel::<Request>();
        let thread = std::thread::spawn(move || {
            for request in rx {
                match request {
                    Request::Submit(command, at, reply) => {
                        let _ = reply.send(journal.submit(&command, at));
                    }
                    Request::MoveTo(id, to, at, detail, reply) => {
                        let _ = reply.send(journal.move_to(&id, to, at, &detail));
                    }
                    Request::RecordIntent(id, to, at, reply) => {
                        let _ = reply.send(journal.record_intent(&id, to, at));
                    }
                    Request::RecordResult(id, to, at, detail, reply) => {
                        let _ = reply.send(journal.record_result(&id, to, at, &detail));
                    }
                    Request::Recover(context, reply) => {
                        let _ = reply.send(journal.recover(&context));
                    }
                    Request::State(id, reply) => {
                        let _ = reply.send(journal.state(&id));
                    }
                    Request::VerifyAudit(reply) => {
                        let _ = reply.send(journal.verify_audit());
                    }
                    Request::AuditHead(reply) => {
                        let _ = reply.send(journal.audit_head());
                    }
                }
            }
        });
        Ok((CommandClient { tx }, Self { thread }))
    }

    /// Waits for the thread to end. It ends once every client is dropped.
    pub fn join(self) {
        let _ = self.thread.join();
    }
}

impl CommandClient {
    fn call<T>(
        &self,
        make: impl FnOnce(Sender<Result<T, CommandError>>) -> Request,
    ) -> Result<T, CommandError> {
        let (reply, answer) = mpsc::channel();
        self.tx
            .send(make(reply))
            .map_err(|_| CommandError::WriterStopped)?;
        answer.recv().map_err(|_| CommandError::WriterStopped)?
    }

    /// See [`CommandJournal::submit`].
    ///
    /// # Errors
    ///
    /// Returns the journal's error, or [`CommandError::WriterStopped`].
    pub fn submit(&self, command: NewCommand, at_ns: i64) -> Result<Submission, CommandError> {
        self.call(|reply| Request::Submit(command, at_ns, reply))
    }

    /// See [`CommandJournal::move_to`].
    ///
    /// # Errors
    ///
    /// Returns the journal's error, or [`CommandError::WriterStopped`].
    pub fn move_to(
        &self,
        command_id: &str,
        to: State,
        at_ns: i64,
        detail: &str,
    ) -> Result<State, CommandError> {
        self.call(|reply| {
            Request::MoveTo(command_id.to_owned(), to, at_ns, detail.to_owned(), reply)
        })
    }

    /// See [`CommandJournal::record_intent`].
    ///
    /// # Errors
    ///
    /// Returns the journal's error, or [`CommandError::WriterStopped`].
    pub fn record_intent(
        &self,
        command_id: &str,
        to: State,
        at_ns: i64,
    ) -> Result<(), CommandError> {
        self.call(|reply| Request::RecordIntent(command_id.to_owned(), to, at_ns, reply))
    }

    /// See [`CommandJournal::record_result`].
    ///
    /// # Errors
    ///
    /// Returns the journal's error, or [`CommandError::WriterStopped`].
    pub fn record_result(
        &self,
        command_id: &str,
        to: State,
        at_ns: i64,
        detail: &str,
    ) -> Result<State, CommandError> {
        self.call(|reply| {
            Request::RecordResult(command_id.to_owned(), to, at_ns, detail.to_owned(), reply)
        })
    }

    /// See [`CommandJournal::recover`].
    ///
    /// # Errors
    ///
    /// Returns the journal's error, or [`CommandError::WriterStopped`].
    pub fn recover(&self, context: RecoveryContext) -> Result<Vec<Recovery>, CommandError> {
        self.call(|reply| Request::Recover(context, reply))
    }

    /// See [`CommandJournal::state`].
    ///
    /// # Errors
    ///
    /// Returns the journal's error, or [`CommandError::WriterStopped`].
    pub fn state(&self, command_id: &str) -> Result<Option<State>, CommandError> {
        self.call(|reply| Request::State(command_id.to_owned(), reply))
    }

    /// See [`CommandJournal::verify_audit`].
    ///
    /// # Errors
    ///
    /// Returns the journal's error, or [`CommandError::WriterStopped`].
    pub fn verify_audit(&self) -> Result<u64, CommandError> {
        self.call(Request::VerifyAudit)
    }

    /// See [`CommandJournal::audit_head`].
    ///
    /// # Errors
    ///
    /// Returns the journal's error, or [`CommandError::WriterStopped`].
    pub fn audit_head(&self) -> Result<[u8; 32], CommandError> {
        self.call(Request::AuditHead)
    }
}
