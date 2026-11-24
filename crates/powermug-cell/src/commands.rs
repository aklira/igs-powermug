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
use powermug_command::ownership::{Owner, Ownership};
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
#[derive(Debug, Clone, PartialEq, Eq)]
struct Known {
    identity: String,
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
    blocked: bool,
    /// The revision the partition is active at: a command must cite it (SPECIFICATIONS 9, the
    /// content identifier). `None` when the partition has no active revision: nothing is accepted.
    active_revision: Option<String>,
    /// The selections confirmed and not yet executed: a critical change fails them.
    awaiting: std::collections::BTreeSet<String>,
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
            blocked: false,
            active_revision: None,
            awaiting: std::collections::BTreeSet::new(),
        }
    }

    /// Sets the revision the commands of this session must cite: the partition's active one, or
    /// `None` when there is none.
    pub fn set_active_revision(&mut self, revision: Option<String>) {
        self.active_revision = revision;
    }

    /// Blocks the commands of this session for a critical change (FUN-CFG-007, FUN-CMD-011):
    /// no new command is accepted and no selected command is executed until
    /// [`Dispatcher::unblock`]. The emissions still open are classified `IN_DOUBT`, since
    /// the adapter may or may not have sent them; none is replayed. A confirmed selection is
    /// `FAILED`: it cannot be attributed to the new revision, and it is never executed. Returns
    /// the states of the commands the block moved.
    ///
    /// # Errors
    ///
    /// Returns the journal's error for the first command it cannot close.
    pub fn block(&mut self, now_ns: i64) -> Result<Vec<State>, CommandError> {
        self.blocked = true;
        let open = std::mem::take(&mut self.open);
        let mut states = Vec::new();
        for (_, Open { emission, .. }) in open {
            states.push(emission.finish(
                Outcome::Unknown("blocked for a critical change before activation".into()),
                now_ns,
            )?);
        }
        for command_id in std::mem::take(&mut self.awaiting) {
            states.push(self.journal.move_to(
                &command_id,
                State::Failed,
                now_ns,
                "blocked by a critical change before activation",
            )?);
        }
        Ok(states)
    }

    /// Lifts the block of [`Dispatcher::block`] once the change is active.
    pub fn unblock(&mut self) {
        self.blocked = false;
    }

    /// Whether the commands of this session are blocked for a critical change.
    #[must_use]
    pub fn is_blocked(&self) -> bool {
        self.blocked
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
        if self.blocked {
            return self.journal.move_to(
                &request.command_id,
                State::RejectedUnavailable,
                now_ns,
                "a critical change is being activated",
            );
        }
        if self.active_revision.as_deref() != Some(request.expected_revision.as_str()) {
            return self.journal.move_to(
                &request.command_id,
                State::RejectedInvalid,
                now_ns,
                "the command does not cite the active revision",
            );
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
                identity: request.identity.clone(),
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

    /// Takes the partition over: its epoch increases, and this node is the confirmed owner.
    /// Returns the new epoch.
    pub fn takeover(&mut self, partition: &str) -> u64 {
        self.ownership.takeover(partition)
    }

    /// The owner epoch a South session is activated at: the confirmed owner's epoch for the
    /// partition, or 0 when the partition has no confirmed owner (PM-CMD-005). A dispatch of
    /// another epoch is refused by the adapter.
    #[must_use]
    pub fn activation_epoch(&self, partition: &str) -> u64 {
        match self.ownership.owner(partition) {
            Owner::Confirmed { epoch } => epoch,
            Owner::Ceding | Owner::Shadow | Owner::Unowned => 0,
        }
    }

    /// Sends the execution of a selected command, once its selection is confirmed. Returns the
    /// state of the command until the adapter answers: `SELECT_CONFIRMED`.
    ///
    /// # Errors
    ///
    /// Returns the journal's error: the command must be `SELECT_CONFIRMED`.
    ///
    /// Only the identity that submitted the command may execute it: the execution is not a new
    /// authorization, so another identity is refused (SPECIFICATIONS 9.3).
    ///
    /// # Errors
    ///
    /// Returns [`CommandError::UnknownCommand`] for a command this dispatcher does not know,
    /// [`CommandError::OtherIdentity`] for one submitted by another identity, and the
    /// journal's error otherwise.
    pub fn execute(
        &mut self,
        command_id: &str,
        identity: &str,
        now_ns: i64,
    ) -> Result<State, CommandError> {
        let known = self
            .known
            .get(command_id)
            .ok_or_else(|| CommandError::UnknownCommand(command_id.to_owned()))?;
        if known.identity != identity {
            return Err(CommandError::OtherIdentity(command_id.to_owned()));
        }
        if self.blocked {
            return Err(CommandError::Blocked(command_id.to_owned()));
        }
        let state = self.emit(command_id, State::ExecuteSent, false, now_ns)?;
        self.awaiting.remove(command_id);
        Ok(state)
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
        let known = self
            .known
            .get(command_id)
            .cloned()
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
                if select {
                    self.awaiting.insert(id.clone());
                }
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

/// Where a commandable point goes: the dispatcher of its session, the partition it belongs to,
/// and its object address on the station.
#[derive(Clone, Debug)]
pub struct Route {
    pub dispatcher: Arc<Mutex<Dispatcher>>,
    pub partition: String,
    pub address: u32,
}

/// The North entry point of the cell: it hands each command to the dispatcher of its point.
#[derive(Debug)]
pub struct Gateway {
    routes: std::collections::BTreeMap<String, Route>,
}

impl Gateway {
    /// A gateway over the routes of the commandable points.
    #[must_use]
    pub fn new(routes: std::collections::BTreeMap<String, Route>) -> Self {
        Self { routes }
    }
}

impl Gateway {
    /// The routes of a partition: each commandable point of it.
    fn partition_routes<'a>(&'a self, partition: &'a str) -> impl Iterator<Item = &'a Route> + 'a {
        self.routes
            .values()
            .filter(move |route| route.partition == partition)
    }
}

impl powermug_mgmt::CommandGateway for Gateway {
    fn active_revision_changed(&self, partition: &str, revision: Option<&str>) {
        for route in self.partition_routes(partition) {
            let mut dispatcher = route
                .dispatcher
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            dispatcher.set_active_revision(revision.map(str::to_owned));
            dispatcher.unblock();
        }
    }

    fn begin_critical_change(&self, partition: &str) {
        let now = now_ns();
        for route in self.partition_routes(partition) {
            let mut dispatcher = route
                .dispatcher
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if let Err(err) = dispatcher.block(now) {
                eprintln!("powermug-cell: the commands of {partition} are not blocked: {err}");
            }
        }
    }

    fn abort_critical_change(&self, partition: &str) {
        for route in self.partition_routes(partition) {
            route
                .dispatcher
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .unblock();
        }
    }

    fn submit(
        &self,
        request: &powermug_mgmt::CommandRequest,
        now_ns: i64,
    ) -> Result<State, powermug_mgmt::GatewayError> {
        let route = self
            .routes
            .get(&request.point)
            .ok_or_else(|| powermug_mgmt::GatewayError::UnknownPoint(request.point.clone()))?;
        let dispatch = Request {
            command_id: request.command_id.clone(),
            identity: request.identity.clone(),
            identity_level: request.identity_level,
            point: request.point.clone(),
            operation: request.operation.clone(),
            partition: route.partition.clone(),
            address: route.address,
            kind: pb::CommandKind::Single,
            on: request.on,
            select: request.operation == "select",
            expires_ns: request.expires_ns,
            expected_revision: request.expected_revision.clone(),
        };
        let mut dispatcher = route
            .dispatcher
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        dispatcher
            .submit(&dispatch, now_ns)
            .map_err(|err| powermug_mgmt::GatewayError::Journal(err.to_string()))
    }

    fn execute(
        &self,
        command_id: &str,
        point: &str,
        identity: &str,
        now_ns: i64,
    ) -> Result<State, powermug_mgmt::GatewayError> {
        let route = self
            .routes
            .get(point)
            .ok_or_else(|| powermug_mgmt::GatewayError::UnknownPoint(point.to_owned()))?;
        let mut dispatcher = route
            .dispatcher
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        dispatcher
            .execute(command_id, identity, now_ns)
            .map_err(|err| match err {
                CommandError::UnknownCommand(id) => powermug_mgmt::GatewayError::UnknownCommand(id),
                CommandError::IllegalTransition { .. }
                | CommandError::NoOpenIntent(..)
                | CommandError::IntentOpen(_)
                | CommandError::OtherIdentity(_)
                | CommandError::Blocked(_) => powermug_mgmt::GatewayError::Refused(err.to_string()),
                other => powermug_mgmt::GatewayError::Journal(other.to_string()),
            })
    }
}

/// The wall-clock time in nanoseconds since the epoch.
fn now_ns() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_nanos()),
    )
    .unwrap_or(i64::MAX)
}
