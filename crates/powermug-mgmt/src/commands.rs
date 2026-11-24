//! The North entry point of commands (SPECIFICATIONS 9, [GW] 11.1): a command handed to the
//! cell is journaled, authorized and dispatched by the cell. The cell implements
//! [`CommandGateway`]; the management crate does not depend on the cell.

use powermug_command::{Granularity, State};

/// A command as the North side hands it to the cell.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandRequest {
    pub command_id: String,
    /// The identity of the issuer, as the protocol gives it.
    pub identity: String,
    pub identity_level: Granularity,
    /// The commandable point.
    pub point: String,
    /// `direct_execute` or `select`.
    pub operation: String,
    pub on: bool,
    /// When the command expires, in nanoseconds since the epoch.
    pub expires_ns: i64,
    pub expected_revision: String,
}

/// The gateway refused to take the command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GatewayError {
    /// No commandable point of that name is routed by this cell.
    UnknownPoint(String),
    /// The command journal failed.
    Journal(String),
    /// No command of that identifier is known to the cell.
    UnknownCommand(String),
    /// The command cannot take that step now (for example, it is not selected and confirmed).
    Refused(String),
}

/// The cell's side of the North entry point.
pub trait CommandGateway: Send + Sync {
    /// Hands a command to the cell, which returns its state after its first steps.
    ///
    /// # Errors
    ///
    /// Returns [`GatewayError::UnknownPoint`] for a point the cell does not route, and
    /// [`GatewayError::Journal`] when the journal fails.
    fn submit(&self, request: &CommandRequest, now_ns: i64) -> Result<State, GatewayError>;

    /// Executes a command selected earlier, once its selection is confirmed.
    ///
    /// # Errors
    ///
    /// Returns [`GatewayError::UnknownPoint`] for a point the cell does not route,
    /// [`GatewayError::UnknownCommand`] for an unknown command, and
    /// [`GatewayError::Refused`] when the command cannot be executed now.
    fn execute(&self, command_id: &str, point: &str, now_ns: i64) -> Result<State, GatewayError>;
}
