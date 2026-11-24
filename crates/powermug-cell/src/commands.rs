//! The core's side of a command (SPECIFICATIONS 9.1 to 9.4, [GW] 11.1): a submission is
//! journaled, authorized, admitted by the partition's owner, and dispatched to the South
//! adapter; the adapter's answers move it on, and an unanswered command is in doubt when the
//! stream ends.
//!
//! The dispatcher owns the journal client and the open emissions. The adapter's channel is
//! given by the caller; the caller reads the updates and passes them to [`Dispatcher::update`].

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};

use powermug_command::authorization::{authorize, Acl};
use powermug_command::authorization::{Request as AclRequest, Value};
use powermug_command::dispatch::admit_submission;
use powermug_command::ownership::Ownership;
use powermug_command::{
    CommandClient, CommandError, Emission, Granularity, NewCommand, Outcome, State, Submission,
};
use powermug_contract::proto::v1 as pb;
use tokio::sync::mpsc;

/// One command as the North side gives it.
#[derive(Debug, Clone, PartialEq)]
pub struct Request {
    pub command_id: String,
    pub identity: String,
    pub identity_level: Granularity,
    /// The commandable point, for the ACL.
    pub point: String,
    /// The operation, for the ACL: for example `direct_execute` or `select`.
    pub operation: String,
    /// The partition the point belongs to.
    pub partition: String,
    /// The object address on the station.
    pub address: u32,
    pub kind: pb::CommandKind,
    pub on: bool,
    /// Select-before-operate: the select comes first, then the execution.
    pub select: bool,
    pub expires_ns: i64,
    pub expected_revision: String,
}

/// The way to the adapter's command stream of one session. It is attached while the adapter's
/// stream is open, and detached when it ends: a dispatch with no stream attached is refused as
/// not sent, never queued where it would wait for an adapter that is not there.
#[derive(Debug, Clone, Default)]
pub struct CommandRoute {
    sender: Arc<Mutex<Option<mpsc::Sender<pb::CommandDispatch>>>>,
}

impl CommandRoute {
    /// A route with no stream attached.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Attaches the adapter's stream.
    pub fn attach(&self, sender: mpsc::Sender<pb::CommandDispatch>) {
        *self.sender.lock().unwrap_or_else(PoisonError::into_inner) = Some(sender);
    }

    /// Detaches the stream: nothing is sent until another one is attached.
    pub fn detach(&self) {
        *self.sender.lock().unwrap_or_else(PoisonError::into_inner) = None;
    }

    fn send(&self, dispatch: pb::CommandDispatch) -> Result<(), &'static str> {
        let guard = self.sender.lock().unwrap_or_else(PoisonError::into_inner);
        match guard.as_ref() {
            Some(sender) => sender
                .try_send(dispatch)
                .map_err(|_| "the adapter command stream is full or closed"),
            None => Err("no adapter command stream is attached"),
        }
    }
}

/// What the dispatcher knows of a command it dispatched, to send its execution later.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Known {
    address: u32,
    kind: pb::CommandKind,
    on: bool,
    expires_ns: i64,
    owner_epoch: u64,
}

/// An emission waiting for the adapter's answer.
#[derive(Debug)]
struct Open {
    emission: Emission,
    select: bool,
}

/// The core's dispatcher for one South session.
#[derive(Debug)]
pub struct Dispatcher {
    journal: CommandClient,
    acl: Acl,
    ownership: Ownership,
    route: CommandRoute,
    known: BTreeMap<String, Known>,
    open: BTreeMap<String, Open>,
}

impl Dispatcher {
    /// A dispatcher over `journal`, with the ACL, the partition ownership, and the channel to
    /// the adapter's command stream.
    #[must_use]
    pub fn new(
        journal: CommandClient,
        acl: Acl,
        ownership: Ownership,
        route: CommandRoute,
    ) -> Self {
        Self {
            journal,
            acl,
            ownership,
            route,
            known: BTreeMap::new(),
            open: BTreeMap::new(),
        }
    }

    /// Submits a command: journals it, authorizes it, admits it under the partition's owner,
    /// and dispatches it. Returns the state after this step. A command known already returns
    /// its state, and nothing is written (PM-CMD-003).
    ///
    /// # Errors
    ///
    /// Returns the journal's error.
    pub fn submit(&mut self, request: &Request, now_ns: i64) -> Result<State, CommandError> {
        let epoch = self.ownership.last_epoch(&request.partition);
        let recorded = NewCommand {
            command_id: request.command_id.clone(),
            identity: request.identity.clone(),
            identity_level: request.identity_level,
            point: request.point.clone(),
            operation: request.operation.clone(),
            value: request.on.to_string(),
            expires_ns: request.expires_ns,
            expected_revision: request.expected_revision.clone(),
            owner_epoch: epoch,
            ha_term: 1,
        };
        if let Submission::Existing(state) = self.journal.submit(recorded, now_ns)? {
            return Ok(state);
        }

        let mode = self.journal.mode()?;
        let acl_request = AclRequest {
            issuer: &request.identity,
            identity_level: request.identity_level,
            point: &request.point,
            operation: &request.operation,
            value: Value::Bool(request.on),
        };
        if let Err(refusal) = authorize(&self.acl, mode, &acl_request) {
            return self.journal.move_to(
                &request.command_id,
                refusal.state,
                now_ns,
                refusal.reason,
            );
        }
        let admitted = match admit_submission(
            self.ownership.owner(&request.partition),
            request.expires_ns,
            now_ns,
        ) {
            Ok(admitted) => admitted,
            Err(refusal) => {
                return self.journal.move_to(
                    &request.command_id,
                    refusal.state,
                    now_ns,
                    refusal.reason,
                );
            }
        };

        for next in [State::Validated, State::DurablyAccepted, State::Ready] {
            self.journal
                .move_to(&request.command_id, next, now_ns, "")?;
        }
        self.known.insert(
            request.command_id.clone(),
            Known {
                address: request.address,
                kind: request.kind,
                on: request.on,
                expires_ns: request.expires_ns,
                owner_epoch: admitted.epoch,
            },
        );
        let to = if request.select {
            State::SelectSent
        } else {
            State::ExecuteSent
        };
        self.emit(&request.command_id, to, request.select, now_ns)
    }

    /// Sends the execution of a selected command, once its selection is confirmed. Returns the
    /// state of the command until the adapter answers: `SELECT_CONFIRMED`.
    ///
    /// # Errors
    ///
    /// Returns the journal's error: the command must be `SELECT_CONFIRMED`.
    pub fn execute(&mut self, command_id: &str, now_ns: i64) -> Result<State, CommandError> {
        if !self.known.contains_key(command_id) {
            return Err(CommandError::UnknownCommand(command_id.to_owned()));
        }
        self.emit(command_id, State::ExecuteSent, false, now_ns)
    }

    /// Opens the emission of `command_id` toward `to`, and sends the dispatch. The state
    /// changes when the adapter answers.
    fn emit(
        &mut self,
        command_id: &str,
        to: State,
        select: bool,
        now_ns: i64,
    ) -> Result<State, CommandError> {
        let known = *self
            .known
            .get(command_id)
            .ok_or_else(|| CommandError::UnknownCommand(command_id.to_owned()))?;
        let emission = Emission::begin(&self.journal, command_id, to, now_ns)?;
        let message = pb::CommandDispatch {
            command_id: command_id.to_owned(),
            address: known.address,
            kind: known.kind as i32,
            on: known.on,
            select,
            owner_epoch: known.owner_epoch,
            expires_ns: known.expires_ns,
        };
        match self.route.send(message) {
            Ok(()) => {
                self.open
                    .insert(command_id.to_owned(), Open { emission, select });
                self.journal
                    .state(command_id)
                    .map(|state| state.unwrap_or(to))
            }
            Err(reason) => {
                // Not dispatched, and not retried by the dispatcher: the command is failed.
                emission.finish(Outcome::NotSent(reason.into()), now_ns)?;
                self.journal
                    .move_to(command_id, State::Failed, now_ns, reason)
            }
        }
    }

    /// Applies one answer of the adapter to its command.
    ///
    /// # Errors
    ///
    /// Returns the journal's error.
    pub fn update(
        &mut self,
        update: &pb::CommandUpdate,
        now_ns: i64,
    ) -> Result<Option<State>, CommandError> {
        let id = update.command_id.clone();
        let status =
            pb::CommandStatus::try_from(update.status).unwrap_or(pb::CommandStatus::Unspecified);
        let Some(open) = self.open.remove(&id) else {
            // Terminated after the execution was confirmed: the command has no open emission.
            if status == pb::CommandStatus::Terminated
                && self.journal.state(&id)? == Some(State::ExecuteConfirmed)
            {
                self.journal
                    .move_to(&id, State::TerminationConfirmed, now_ns, "")?;
                return self
                    .journal
                    .move_to(&id, State::SuccessUnverified, now_ns, "")
                    .map(Some);
            }
            return Ok(None);
        };
        let Open { emission, select } = open;
        let state = match status {
            pb::CommandStatus::Confirmed => {
                emission.finish(Outcome::Sent(String::new()), now_ns)?;
                let next = if select {
                    State::SelectConfirmed
                } else {
                    State::ExecuteConfirmed
                };
                self.journal.move_to(&id, next, now_ns, "")?
            }
            pb::CommandStatus::Refused => {
                emission.finish(Outcome::Sent(String::new()), now_ns)?;
                let refused = if select {
                    State::Failed
                } else {
                    State::EquipmentRejected
                };
                self.journal.move_to(&id, refused, now_ns, &update.reason)?
            }
            pb::CommandStatus::NotSent => {
                emission.finish(Outcome::NotSent(update.reason.clone()), now_ns)?;
                self.journal
                    .move_to(&id, State::Failed, now_ns, &update.reason)?
            }
            pb::CommandStatus::Expired => {
                emission.finish(Outcome::NotSent(update.reason.clone()), now_ns)?;
                self.journal
                    .move_to(&id, State::Expired, now_ns, &update.reason)?
            }
            pb::CommandStatus::Terminated | pb::CommandStatus::Unspecified => emission.finish(
                Outcome::Unknown(format!("unexpected answer {status:?}")),
                now_ns,
            )?,
        };
        Ok(Some(state))
    }

    /// The adapter's stream ended: every command still open is in doubt. None is replayed
    /// (PM-CMD-002).
    ///
    /// # Errors
    ///
    /// Returns the journal's error for the first command it cannot close.
    pub fn stream_ended(&mut self, now_ns: i64) -> Result<Vec<State>, CommandError> {
        let open = std::mem::take(&mut self.open);
        let mut states = Vec::new();
        for (_, Open { emission, .. }) in open {
            states.push(emission.finish(Outcome::Unknown("command stream ended".into()), now_ns)?);
        }
        Ok(states)
    }
}

/// A channel of dispatches: the route takes the sender, and the adapter's stream reads the receiver.
#[must_use]
pub fn channel(
    capacity: usize,
) -> (
    mpsc::Sender<pb::CommandDispatch>,
    mpsc::Receiver<pb::CommandDispatch>,
) {
    mpsc::channel(capacity)
}
