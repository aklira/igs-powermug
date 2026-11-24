//! The critical journal of commands (SPECIFICATIONS 9.2): the `commands`,
//! `command_transitions` and `idempotency` tables of `critical.db`.
//!
//! Every transition is checked against the table in [`crate::state`]. Before a network call
//! the journal records the intent of the transition, and after it the result (PM-CMD-002).
//! A submission with a known `command_id` returns the existing state (PM-CMD-003).

use rusqlite::{params, Connection, OptionalExtension, Transaction};
use thiserror::Error;

use crate::audit;
use crate::authorization::Mode;
use crate::identity::Granularity;
use crate::state::State;

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS commands (
    command_id        TEXT    PRIMARY KEY,
    state             TEXT    NOT NULL,
    identity          TEXT    NOT NULL,
    identity_level    TEXT    NOT NULL,
    point             TEXT    NOT NULL,
    operation         TEXT    NOT NULL,
    value             TEXT    NOT NULL,
    expires_ns        INTEGER NOT NULL,
    expected_revision TEXT    NOT NULL,
    owner_epoch       INTEGER NOT NULL,
    ha_term           INTEGER NOT NULL,
    created_ns        INTEGER NOT NULL,
    updated_ns        INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS command_transitions (
    id          INTEGER PRIMARY KEY,
    command_id  TEXT    NOT NULL REFERENCES commands (command_id),
    phase       TEXT    NOT NULL CHECK (phase IN ('intent', 'result')),
    from_state  TEXT,
    to_state    TEXT    NOT NULL,
    at_ns       INTEGER NOT NULL,
    closed      INTEGER NOT NULL DEFAULT 0 CHECK (closed IN (0, 1)),
    detail      TEXT    NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS mode (
    id          INTEGER PRIMARY KEY CHECK (id = 1),
    mode        TEXT    NOT NULL,
    changed_ns  INTEGER NOT NULL,
    actor       TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS command_transitions_by_command
    ON command_transitions (command_id, id);
CREATE TABLE IF NOT EXISTS idempotency (
    key         TEXT    PRIMARY KEY,
    command_id  TEXT    NOT NULL,
    created_ns  INTEGER NOT NULL
);
";

/// A command to record, as it was received.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewCommand {
    pub command_id: String,
    pub identity: String,
    /// The granularity of the identity the protocol gave (GW 11.6).
    pub identity_level: crate::identity::Granularity,
    pub point: String,
    pub operation: String,
    /// The value, as the North adapter gave it (opaque to the journal).
    pub value: String,
    pub expires_ns: i64,
    pub expected_revision: String,
    pub owner_epoch: u64,
    pub ha_term: u64,
}

/// What a submission found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Submission {
    /// The command is new and is recorded in `RECEIVED`.
    Created,
    /// The `command_id` is known: its current state is returned, nothing is written.
    Existing(State),
}

/// One row of a command's history.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Transition {
    pub phase: Phase,
    pub from: Option<State>,
    pub to: State,
    pub at_ns: i64,
    /// For an intent: whether its result was recorded.
    pub closed: bool,
    pub detail: String,
}

/// Whether a row is the intent of a transition or its result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    Intent,
    Result,
}

/// The state of a journal row that the journal cannot read.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum CommandError {
    #[error("command {0} is not in the journal")]
    UnknownCommand(String),
    #[error("transition from {from} to {to} is not in the table")]
    IllegalTransition {
        from: &'static str,
        to: &'static str,
    },
    #[error("command {0} has an intent that is not closed yet")]
    IntentOpen(String),
    #[error("command {0} has no open intent to {1}")]
    NoOpenIntent(String, &'static str),
    #[error("command {0} was submitted by another identity")]
    OtherIdentity(String),
    #[error("command {0} waits for a critical change to be activated")]
    Blocked(String),
    #[error("the journal holds an unknown state {0}")]
    UnknownState(String),
    #[error("the audit chain breaks at record {0}")]
    AuditBroken(i64),
    #[error("the command writer has stopped")]
    WriterStopped,
    #[error("database error: {0}")]
    Database(String),
}

impl From<rusqlite::Error> for CommandError {
    fn from(err: rusqlite::Error) -> Self {
        Self::Database(err.to_string())
    }
}

/// The context of a restart: what the node must match before it emits a command again.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryContext {
    pub now_ns: i64,
    pub active_revision: String,
    pub owner_epoch: u64,
    /// Whether this node is confirmed as the active HA node.
    pub ha_active: bool,
}

/// What recovery decided for one command (SPECIFICATIONS 9.7, [GW] 11.5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    /// Emit now: the gates pass and the command was never sent.
    Emit,
    /// Hold: the gates do not pass yet (not the active HA node).
    Hold,
    /// The selection was sent: wait for the expiry before any new selection.
    AwaitExpiry,
    /// The execution was confirmed: wait for the termination.
    AwaitTermination,
    /// The termination was confirmed: verify the field effect if the policy asks it.
    VerifyIfRequired,
    /// Nothing to do.
    Nothing,
}

/// One command's outcome at recovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Recovery {
    pub command_id: String,
    pub from: State,
    pub to: State,
    pub action: Action,
}

/// The journal of commands, in `critical.db`.
#[derive(Debug)]
pub struct CommandJournal {
    conn: Connection,
}

fn read_state(code: &str) -> Result<State, CommandError> {
    State::from_code(code).ok_or_else(|| CommandError::UnknownState(code.to_owned()))
}

fn current(tx: &Transaction<'_>, command_id: &str) -> Result<State, CommandError> {
    let code: Option<String> = tx
        .query_row(
            "SELECT state FROM commands WHERE command_id = ?1",
            params![command_id],
            |row| row.get(0),
        )
        .optional()?;
    read_state(&code.ok_or_else(|| CommandError::UnknownCommand(command_id.to_owned()))?)
}

fn check(from: State, to: State) -> Result<(), CommandError> {
    if from.may_move_to(to) {
        Ok(())
    } else {
        Err(CommandError::IllegalTransition {
            from: from.code(),
            to: to.code(),
        })
    }
}

fn set_state(
    tx: &Transaction<'_>,
    command_id: &str,
    to: State,
    at_ns: i64,
) -> Result<(), CommandError> {
    tx.execute(
        "UPDATE commands SET state = ?2, updated_ns = ?3 WHERE command_id = ?1",
        params![command_id, to.code(), at_ns],
    )?;
    Ok(())
}

fn open_intent(tx: &Transaction<'_>, command_id: &str) -> Result<Option<State>, CommandError> {
    let to: Option<String> = tx
        .query_row(
            "SELECT to_state FROM command_transitions
             WHERE command_id = ?1 AND phase = 'intent' AND closed = 0
             ORDER BY id DESC LIMIT 1",
            params![command_id],
            |row| row.get(0),
        )
        .optional()?;
    to.map(|code| read_state(&code)).transpose()
}

impl CommandJournal {
    /// Opens or creates the journal at `path`, in WAL mode with `synchronous=FULL`.
    ///
    /// # Errors
    ///
    /// Returns [`CommandError::Database`] when the file cannot be opened or migrated.
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self, CommandError> {
        let conn = Connection::open(path)?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "FULL")?;
        conn.execute_batch(SCHEMA)?;
        conn.execute_batch(audit::SCHEMA)?;
        Ok(Self { conn })
    }

    /// Records a command in `RECEIVED`, in one transaction with its idempotency key. A known
    /// `command_id` returns its current state and writes nothing (PM-CMD-003).
    ///
    /// # Errors
    ///
    /// Returns [`CommandError::Database`] on a failure; nothing is written then.
    pub fn submit(&mut self, command: &NewCommand, at_ns: i64) -> Result<Submission, CommandError> {
        let tx = self.conn.transaction()?;
        let existing: Option<String> = tx
            .query_row(
                "SELECT state FROM commands WHERE command_id = ?1",
                params![command.command_id],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(code) = existing {
            let state = read_state(&code)?;
            tx.commit()?;
            return Ok(Submission::Existing(state));
        }
        let epoch = i64::try_from(command.owner_epoch).unwrap_or(i64::MAX);
        let term = i64::try_from(command.ha_term).unwrap_or(i64::MAX);
        tx.execute(
            "INSERT INTO commands (command_id, state, identity, identity_level, point, operation,
                                   value, expires_ns, expected_revision, owner_epoch, ha_term,
                                   created_ns, updated_ns)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?12)",
            params![
                command.command_id,
                State::Received.code(),
                command.identity,
                command.identity_level.code(),
                command.point,
                command.operation,
                command.value,
                command.expires_ns,
                command.expected_revision,
                epoch,
                term,
                at_ns,
            ],
        )?;
        tx.execute(
            "INSERT INTO idempotency (key, command_id, created_ns) VALUES (?1, ?1, ?2)",
            params![command.command_id, at_ns],
        )?;
        tx.execute(
            "INSERT INTO command_transitions (command_id, phase, from_state, to_state, at_ns)
             VALUES (?1, 'result', NULL, ?2, ?3)",
            params![command.command_id, State::Received.code(), at_ns],
        )?;
        audit::append(
            &tx,
            "command.submitted",
            &command.command_id,
            State::Received.code(),
            at_ns,
        )?;
        tx.commit()?;
        Ok(Submission::Created)
    }

    /// The operating mode. A cell that never set one runs in `normal`.
    ///
    /// # Errors
    ///
    /// Returns [`CommandError::Database`] on a read failure, and
    /// [`CommandError::UnknownState`] for a stored mode it does not know.
    pub fn mode(&self) -> Result<Mode, CommandError> {
        let code: Option<String> = self
            .conn
            .query_row("SELECT mode FROM mode WHERE id = 1", [], |row| row.get(0))
            .optional()?;
        match code {
            None => Ok(Mode::Normal),
            Some(code) => Mode::from_code(&code).ok_or(CommandError::UnknownState(code)),
        }
    }

    /// Sets the operating mode, on behalf of `actor`. A change is written with its audit
    /// record in one transaction; setting the mode it already has writes nothing. Returns
    /// whether the mode changed.
    ///
    /// # Errors
    ///
    /// Returns [`CommandError::Database`] on a failure; nothing is changed then.
    pub fn set_mode(&mut self, mode: Mode, actor: &str, at_ns: i64) -> Result<bool, CommandError> {
        let tx = self.conn.transaction()?;
        let current: Option<String> = tx
            .query_row("SELECT mode FROM mode WHERE id = 1", [], |row| row.get(0))
            .optional()?;
        let previous = match current {
            None => Mode::Normal,
            Some(code) => Mode::from_code(&code).ok_or(CommandError::UnknownState(code))?,
        };
        if previous == mode {
            tx.commit()?;
            return Ok(false);
        }
        tx.execute(
            "INSERT INTO mode (id, mode, changed_ns, actor) VALUES (1, ?1, ?2, ?3)
             ON CONFLICT(id) DO UPDATE SET mode = excluded.mode,
                                           changed_ns = excluded.changed_ns,
                                           actor = excluded.actor",
            params![mode.code(), at_ns, actor],
        )?;
        audit::append(
            &tx,
            "mode.set",
            actor,
            &format!("{} -> {}", previous.code(), mode.code()),
            at_ns,
        )?;
        tx.commit()?;
        Ok(true)
    }

    /// The identity a command was recorded with, and its granularity.
    ///
    /// # Errors
    ///
    /// Returns [`CommandError::Database`] on a read failure.
    pub fn identity(
        &self,
        command_id: &str,
    ) -> Result<Option<(String, Granularity)>, CommandError> {
        let row: Option<(String, String)> = self
            .conn
            .query_row(
                "SELECT identity, identity_level FROM commands WHERE command_id = ?1",
                params![command_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        row.map(|(subject, code)| {
            Granularity::from_code(&code)
                .map(|level| (subject, level))
                .ok_or(CommandError::UnknownState(code))
        })
        .transpose()
    }

    /// The state of a command, if it is in the journal.
    ///
    /// # Errors
    ///
    /// Returns [`CommandError::Database`] on a read failure.
    pub fn state(&self, command_id: &str) -> Result<Option<State>, CommandError> {
        let code: Option<String> = self
            .conn
            .query_row(
                "SELECT state FROM commands WHERE command_id = ?1",
                params![command_id],
                |row| row.get(0),
            )
            .optional()?;
        code.map(|code| read_state(&code)).transpose()
    }

    /// Moves a command to `to`, when the table allows it. Writes the result in the same
    /// transaction as the new state.
    ///
    /// # Errors
    ///
    /// Returns [`CommandError::IllegalTransition`] when the table refuses the move,
    /// [`CommandError::UnknownCommand`] for an unknown id, and [`CommandError::IntentOpen`]
    /// while an intent of this command is not closed.
    pub fn move_to(
        &mut self,
        command_id: &str,
        to: State,
        at_ns: i64,
        detail: &str,
    ) -> Result<State, CommandError> {
        let tx = self.conn.transaction()?;
        let from = current(&tx, command_id)?;
        if open_intent(&tx, command_id)?.is_some() {
            return Err(CommandError::IntentOpen(command_id.to_owned()));
        }
        check(from, to)?;
        set_state(&tx, command_id, to, at_ns)?;
        tx.execute(
            "INSERT INTO command_transitions (command_id, phase, from_state, to_state, at_ns, detail)
             VALUES (?1, 'result', ?2, ?3, ?4, ?5)",
            params![command_id, from.code(), to.code(), at_ns, detail],
        )?;
        audit::append(
            &tx,
            "command.transition",
            command_id,
            &format!("{} -> {} {detail}", from.code(), to.code()),
            at_ns,
        )?;
        tx.commit()?;
        Ok(to)
    }

    /// Writes the intent of a transition before the network call (PM-CMD-002). The state does
    /// not change until the result is recorded.
    ///
    /// # Errors
    ///
    /// Returns [`CommandError::IllegalTransition`] when the table refuses the move, and
    /// [`CommandError::IntentOpen`] when an intent of this command is still open.
    pub fn record_intent(
        &mut self,
        command_id: &str,
        to: State,
        at_ns: i64,
    ) -> Result<(), CommandError> {
        let tx = self.conn.transaction()?;
        let from = current(&tx, command_id)?;
        if open_intent(&tx, command_id)?.is_some() {
            return Err(CommandError::IntentOpen(command_id.to_owned()));
        }
        check(from, to)?;
        tx.execute(
            "INSERT INTO command_transitions (command_id, phase, from_state, to_state, at_ns, closed)
             VALUES (?1, 'intent', ?2, ?3, ?4, 0)",
            params![command_id, from.code(), to.code(), at_ns],
        )?;
        audit::append(
            &tx,
            "command.intent",
            command_id,
            &format!("{} -> {}", from.code(), to.code()),
            at_ns,
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Writes the result of an intent, right after the network call (PM-CMD-002), and moves
    /// the command to `to`.
    ///
    /// # Errors
    ///
    /// Returns [`CommandError::NoOpenIntent`] when no intent to `to` is open, and
    /// [`CommandError::IllegalTransition`] when the table refuses the move.
    pub fn record_result(
        &mut self,
        command_id: &str,
        to: State,
        at_ns: i64,
        detail: &str,
    ) -> Result<State, CommandError> {
        let tx = self.conn.transaction()?;
        let from = current(&tx, command_id)?;
        if open_intent(&tx, command_id)? != Some(to) {
            return Err(CommandError::NoOpenIntent(command_id.to_owned(), to.code()));
        }
        check(from, to)?;
        tx.execute(
            "UPDATE command_transitions SET closed = 1
             WHERE command_id = ?1 AND phase = 'intent' AND closed = 0",
            params![command_id],
        )?;
        set_state(&tx, command_id, to, at_ns)?;
        tx.execute(
            "INSERT INTO command_transitions (command_id, phase, from_state, to_state, at_ns, detail)
             VALUES (?1, 'result', ?2, ?3, ?4, ?5)",
            params![command_id, from.code(), to.code(), at_ns, detail],
        )?;
        audit::append(
            &tx,
            "command.result",
            command_id,
            &format!("{} -> {} {detail}", from.code(), to.code()),
            at_ns,
        )?;
        tx.commit()?;
        Ok(to)
    }

    /// Closes the open intent of a command whose call was not sent: the state does not change,
    /// and the result says why.
    ///
    /// # Errors
    ///
    /// Returns [`CommandError::NoOpenIntent`] when no intent is open.
    pub fn close_intent_not_sent(
        &mut self,
        command_id: &str,
        at_ns: i64,
        reason: &str,
    ) -> Result<State, CommandError> {
        let tx = self.conn.transaction()?;
        let from = current(&tx, command_id)?;
        let Some(intent) = open_intent(&tx, command_id)? else {
            return Err(CommandError::NoOpenIntent(command_id.to_owned(), "any"));
        };
        tx.execute(
            "UPDATE command_transitions SET closed = 1
             WHERE command_id = ?1 AND phase = 'intent' AND closed = 0",
            params![command_id],
        )?;
        tx.execute(
            "INSERT INTO command_transitions (command_id, phase, from_state, to_state, at_ns, detail)
             VALUES (?1, 'result', ?2, ?2, ?3, ?4)",
            params![command_id, from.code(), at_ns, format!("not sent toward {}: {reason}", intent.code())],
        )?;
        audit::append(
            &tx,
            "command.not_sent",
            command_id,
            &format!("{} {reason}", intent.code()),
            at_ns,
        )?;
        tx.commit()?;
        Ok(from)
    }

    /// Closes the open intent of a command whose call may or may not have left: the command is
    /// `IN_DOUBT`, and it is never replayed (PM-CMD-002).
    ///
    /// # Errors
    ///
    /// Returns [`CommandError::NoOpenIntent`] when no intent is open, and
    /// [`CommandError::IllegalTransition`] when `IN_DOUBT` is not reachable from the state.
    pub fn close_intent_unknown(
        &mut self,
        command_id: &str,
        at_ns: i64,
        reason: &str,
    ) -> Result<State, CommandError> {
        let tx = self.conn.transaction()?;
        let from = current(&tx, command_id)?;
        let Some(intent) = open_intent(&tx, command_id)? else {
            return Err(CommandError::NoOpenIntent(command_id.to_owned(), "any"));
        };
        check(from, State::InDoubt)?;
        tx.execute(
            "UPDATE command_transitions SET closed = 1
             WHERE command_id = ?1 AND phase = 'intent' AND closed = 0",
            params![command_id],
        )?;
        set_state(&tx, command_id, State::InDoubt, at_ns)?;
        tx.execute(
            "INSERT INTO command_transitions (command_id, phase, from_state, to_state, at_ns, detail)
             VALUES (?1, 'result', ?2, 'IN_DOUBT', ?3, ?4)",
            params![command_id, from.code(), at_ns, format!("unknown after intent {}: {reason}", intent.code())],
        )?;
        audit::append(&tx, "command.in_doubt", command_id, reason, at_ns)?;
        tx.commit()?;
        Ok(State::InDoubt)
    }

    /// The history of a command, oldest first.
    ///
    /// # Errors
    ///
    /// Returns [`CommandError::Database`] on a read failure.
    pub fn history(&self, command_id: &str) -> Result<Vec<Transition>, CommandError> {
        let mut statement = self.conn.prepare(
            "SELECT phase, from_state, to_state, at_ns, closed, detail
             FROM command_transitions WHERE command_id = ?1 ORDER BY id",
        )?;
        let rows = statement.query_map(params![command_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, String>(5)?,
            ))
        })?;
        let mut history = Vec::new();
        for row in rows {
            let (phase, from, to, at_ns, closed, detail) = row?;
            history.push(Transition {
                phase: if phase == "intent" {
                    Phase::Intent
                } else {
                    Phase::Result
                },
                from: from.map(|code| read_state(&code)).transpose()?,
                to: read_state(&to)?,
                at_ns,
                closed: closed == 1,
                detail,
            });
        }
        Ok(history)
    }

    /// Applies the restart rules to every command that is not terminal (SPECIFICATIONS 9.7,
    /// [GW] 11.5), in one transaction. Nothing is sent here: the caller emits the commands
    /// marked [`Action::Emit`], through [`CommandJournal::record_intent`].
    ///
    /// # Errors
    ///
    /// Returns [`CommandError::Database`] on a failure; nothing is changed then.
    pub fn recover(&mut self, context: &RecoveryContext) -> Result<Vec<Recovery>, CommandError> {
        let tx = self.conn.transaction()?;
        let pending = pending_commands(&tx)?;
        let mut outcome = Vec::new();
        for command in pending {
            let (next, action) = decide(&command, context);
            // An intent written before the network call and never closed: the frame may or may
            // not have left. The command is in doubt, and it is never replayed (PM-CMD-002).
            let (next, action) = match open_intent(&tx, &command.id)? {
                Some(_) if !matches!(command.state, State::InDoubt | State::Reconciling) => (
                    Some((State::InDoubt, "intent without result at restart")),
                    Action::Nothing,
                ),
                _ => (next, action),
            };
            let to = match next {
                Some((next, reason)) => {
                    check(command.state, next)?;
                    set_state(&tx, &command.id, next, context.now_ns)?;
                    tx.execute(
                        "INSERT INTO command_transitions
                            (command_id, phase, from_state, to_state, at_ns, detail)
                         VALUES (?1, 'result', ?2, ?3, ?4, ?5)",
                        params![
                            command.id,
                            command.state.code(),
                            next.code(),
                            context.now_ns,
                            reason
                        ],
                    )?;
                    audit::append(
                        &tx,
                        "command.recovered",
                        &command.id,
                        &format!("{} -> {reason}", command.state.code()),
                        context.now_ns,
                    )?;
                    next
                }
                None => command.state,
            };
            outcome.push(Recovery {
                command_id: command.id,
                from: command.state,
                to,
                action,
            });
        }
        tx.commit()?;
        Ok(outcome)
    }
}

/// A command that is not terminal, as the restart rules read it.
struct Pending {
    id: String,
    state: State,
    expires_ns: i64,
    owner_epoch: i64,
    expected_revision: String,
}

fn pending_commands(tx: &Transaction<'_>) -> Result<Vec<Pending>, CommandError> {
    let mut statement = tx.prepare(
        "SELECT command_id, state, expires_ns, owner_epoch, expected_revision
         FROM commands ORDER BY created_ns, command_id",
    )?;
    let rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, i64>(3)?,
            row.get::<_, String>(4)?,
        ))
    })?;
    let mut pending = Vec::new();
    for row in rows {
        let (id, code, expires_ns, owner_epoch, expected_revision) = row?;
        let state = read_state(&code)?;
        if !state.is_terminal() {
            pending.push(Pending {
                id,
                state,
                expires_ns,
                owner_epoch,
                expected_revision,
            });
        }
    }
    Ok(pending)
}

/// The restart decision for one command: the next state, if it changes, and the action.
/// Pure: it reads only the command and the context.
fn decide(command: &Pending, context: &RecoveryContext) -> (Option<(State, &'static str)>, Action) {
    match command.state {
        State::Received | State::Validated => (
            Some((State::Failed, "interrupted before durable acceptance")),
            Action::Nothing,
        ),
        State::DurablyAccepted | State::Ready => {
            if command.expires_ns <= context.now_ns {
                (
                    Some((State::Expired, "expired before emission")),
                    Action::Nothing,
                )
            } else if command.expected_revision != context.active_revision {
                (
                    Some((State::Failed, "revision changed before emission")),
                    Action::Nothing,
                )
            } else if command.owner_epoch != i64::try_from(context.owner_epoch).unwrap_or(i64::MAX)
            {
                (
                    Some((State::Failed, "owner epoch changed before emission")),
                    Action::Nothing,
                )
            } else if !context.ha_active {
                (None, Action::Hold)
            } else {
                (None, Action::Emit)
            }
        }
        State::SelectSent => (None, Action::AwaitExpiry),
        State::SelectConfirmed => (
            Some((State::InDoubt, "selection not attributable to this session")),
            Action::Nothing,
        ),
        State::ExecuteSent => (
            Some((State::InDoubt, "execution sent without confirmation")),
            Action::Nothing,
        ),
        State::ExecuteConfirmed => (None, Action::AwaitTermination),
        State::TerminationConfirmed => (None, Action::VerifyIfRequired),
        _ => (None, Action::Nothing),
    }
}

impl CommandJournal {
    /// Checks the audit chain. Returns the number of records.
    ///
    /// # Errors
    ///
    /// Returns [`CommandError::AuditBroken`] at the first altered record.
    pub fn verify_audit(&self) -> Result<u64, CommandError> {
        audit::verify(&self.conn).map_err(|err| match err {
            audit::VerifyError::Broken(broken) => CommandError::AuditBroken(broken.seq),
            audit::VerifyError::Database(err) => CommandError::Database(err.to_string()),
        })
    }

    /// The hash of the last audit record, to anchor outside the store.
    ///
    /// # Errors
    ///
    /// Returns [`CommandError::Database`] on a read failure.
    pub fn audit_head(&self) -> Result<[u8; 32], CommandError> {
        audit::head(&self.conn)
    }
}
