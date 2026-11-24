//! Single commands over IEC 60870-5-104 (SPECIFICATIONS 9, GW 11): the adapter sends a
//! single command to the station, and reads the station's answer back: the activation
//! confirmation, the activation termination, or a negative confirmation (refusal).
//!
//! The adapter is the controlling side here. It never repeats a command on its own: a command
//! that was sent and not answered is in doubt, and the core decides (PM-CMD-002).
//!
//! Known limit: the station of `igs-iec104` refuses an execution that has no prior selection,
//! so a direct execute (S/E = 0) is refused by it. Select-before-operate is the path tested
//! here. The direct path needs a change in `igs-iec104`, not in this adapter.

use igs_iec104::client::{Client, ClientError};
use igs_iec104_codec::asdu::{Asdu, Body, Objects};
use igs_iec104_codec::elements::{
    Dco, DoubleCommandState, Nva, Qoc, Qos, Rco, RegulatingStep, Sco, ShortFloat, Sva,
};
use igs_iec104_codec::header::{cause, InformationObjectAddress};
use thiserror::Error;

/// A single command to one object of the station.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SingleCommand {
    /// The information object address of the command.
    pub address: u32,
    /// ON (`true`) or OFF (`false`).
    pub on: bool,
    /// Select (`true`) or execute (`false`), for select-before-operate.
    pub select: bool,
}

/// A double command to one object of the station.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DoubleCommand {
    /// The information object address of the command.
    pub address: u32,
    /// ON (`true`) or OFF (`false`).
    pub on: bool,
    /// Select (`true`) or execute (`false`), for select-before-operate.
    pub select: bool,
}

/// A set point with a short floating point value, to one object of the station.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SetPointFloat {
    /// The information object address of the set point.
    pub address: u32,
    /// The value to set.
    pub value: f32,
    /// Select (`true`) or execute (`false`), for select-before-operate.
    pub select: bool,
}

/// A normalized set point: a value in -1 to +1 (`C_SE_NA_1`).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SetPointNormalized {
    /// The information object address of the set point.
    pub address: u32,
    /// The value, on the F16 grid: a multiple of 2^-15 in [-1, 1 - 2^-15].
    pub value: f32,
    /// Select (`true`) or execute (`false`), for select-before-operate.
    pub select: bool,
}

/// A scaled set point: a 16-bit signed integer (`C_SE_NB_1`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SetPointScaled {
    /// The information object address of the set point.
    pub address: u32,
    /// The value.
    pub value: i16,
    /// Select (`true`) or execute (`false`), for select-before-operate.
    pub select: bool,
}

/// A regulating step (raise or lower), to one object of the station (`C_RC_NA_1`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegulatingStepCommand {
    /// The information object address of the command.
    pub address: u32,
    /// Higher (`true`) or lower (`false`).
    pub higher: bool,
    /// Select (`true`) or execute (`false`), for select-before-operate.
    pub select: bool,
}

/// What the station answered to a command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Answer {
    /// The activation is confirmed: the station will carry the command out.
    Confirmed,
    /// The activation is terminated: the command is over.
    Terminated,
    /// The station refused the command (a negative confirmation).
    Refused,
}

/// The command could not be sent.
#[derive(Debug, Error)]
pub enum CommandError {
    #[error("address {0} is not a valid information object address")]
    Address(u32),
    #[error("the qualifier of the command is not valid")]
    Qualifier,
    #[error("the value {0} is not representable in the format of the set point")]
    Value(f32),
    #[error("the client could not send the command: {0}")]
    Client(#[from] ClientError),
    #[error("the command expired at {expires_ns} ns, before its emission at {now_ns} ns")]
    Expired { expires_ns: i64, now_ns: i64 },
}

/// Sends a single command. Returns once the request is queued on the connection; the answer
/// comes back as an ASDU, read with [`answer`].
///
/// # Errors
///
/// Returns [`CommandError::Address`] or [`CommandError::Qualifier`] for a command that cannot
/// be built, and [`CommandError::Client`] when the client has stopped.
pub async fn send(client: &Client, command: SingleCommand) -> Result<(), CommandError> {
    let address = InformationObjectAddress::new(command.address)
        .ok_or(CommandError::Address(command.address))?;
    let qoc =
        Qoc::new(Qoc::NO_ADDITIONAL_DEFINITION, command.select).ok_or(CommandError::Qualifier)?;
    client
        .single_command(
            address,
            Sco {
                on: command.on,
                qoc,
            },
            None,
        )
        .await
        .map_err(CommandError::from)
}

/// Sends a normalized set point. Like [`send`], it returns once the request is queued.
///
/// # Errors
///
/// Returns [`CommandError::Value`] for a value the format cannot carry, and as [`send`].
pub async fn send_setpoint_normalized(
    client: &Client,
    command: SetPointNormalized,
) -> Result<(), CommandError> {
    let address = InformationObjectAddress::new(command.address)
        .ok_or(CommandError::Address(command.address))?;
    let value = Nva::try_from_f32(command.value).ok_or(CommandError::Value(command.value))?;
    // QL 0: the default qualifier, no additional definition.
    let qualifier = Qos::new(0, command.select).ok_or(CommandError::Qualifier)?;
    client
        .set_point_normalized(address, value, qualifier, None)
        .await
        .map_err(CommandError::from)
}

/// Sends a scaled set point. Like [`send`], it returns once the request is queued.
///
/// # Errors
///
/// As [`send`].
pub async fn send_setpoint_scaled(
    client: &Client,
    command: SetPointScaled,
) -> Result<(), CommandError> {
    let address = InformationObjectAddress::new(command.address)
        .ok_or(CommandError::Address(command.address))?;
    // QL 0: the default qualifier, no additional definition.
    let qualifier = Qos::new(0, command.select).ok_or(CommandError::Qualifier)?;
    client
        .set_point_scaled(address, Sva::new(command.value), qualifier, None)
        .await
        .map_err(CommandError::from)
}

/// Sends a set point with a short floating point value. Like [`send`], it returns once the
/// request is queued.
///
/// # Errors
///
/// As [`send`].
pub async fn send_setpoint_float(
    client: &Client,
    command: SetPointFloat,
) -> Result<(), CommandError> {
    let address = InformationObjectAddress::new(command.address)
        .ok_or(CommandError::Address(command.address))?;
    // QL 0: the default qualifier, no additional definition.
    let qualifier = Qos::new(0, command.select).ok_or(CommandError::Qualifier)?;
    client
        .set_point_float(
            address,
            ShortFloat::from_f32(command.value),
            qualifier,
            None,
        )
        .await
        .map_err(CommandError::from)
}

/// Sends a regulating step. Like [`send`], it returns once the request is queued.
///
/// # Errors
///
/// As [`send`].
pub async fn send_regulating_step(
    client: &Client,
    command: RegulatingStepCommand,
) -> Result<(), CommandError> {
    let address = InformationObjectAddress::new(command.address)
        .ok_or(CommandError::Address(command.address))?;
    let qoc =
        Qoc::new(Qoc::NO_ADDITIONAL_DEFINITION, command.select).ok_or(CommandError::Qualifier)?;
    let step = if command.higher {
        RegulatingStep::Higher
    } else {
        RegulatingStep::Lower
    };
    client
        .regulating_step(address, Rco { step, qoc }, None)
        .await
        .map_err(CommandError::from)
}

/// Sends a double command. Like [`send`], it returns once the request is queued.
///
/// # Errors
///
/// As [`send`].
pub async fn send_double(client: &Client, command: DoubleCommand) -> Result<(), CommandError> {
    let address = InformationObjectAddress::new(command.address)
        .ok_or(CommandError::Address(command.address))?;
    let qoc =
        Qoc::new(Qoc::NO_ADDITIONAL_DEFINITION, command.select).ok_or(CommandError::Qualifier)?;
    let state = if command.on {
        DoubleCommandState::On
    } else {
        DoubleCommandState::Off
    };
    client
        .double_command(address, Dco { state, qoc }, None)
        .await
        .map_err(CommandError::from)
}

/// Sends a single command only if it has not expired at `now_ns` (PM-CMD-004): an expired
/// command is never emitted, and the check is made just before the emission.
///
/// # Errors
///
/// Returns [`CommandError::Expired`] past `expires_ns`, and otherwise as [`send`].
pub async fn send_before_expiry(
    client: &Client,
    command: SingleCommand,
    expires_ns: i64,
    now_ns: i64,
) -> Result<(), CommandError> {
    if expires_ns <= now_ns {
        return Err(CommandError::Expired { expires_ns, now_ns });
    }
    send(client, command).await
}

/// The answer of the station to a single or double command, with the address it concerns, if
/// `asdu` is such an answer.
#[must_use]
pub fn answer(asdu: &Asdu) -> Option<(u32, Answer)> {
    let address = match &asdu.body {
        Body::C_SC_NA_1(objects) => first_address(objects)?,
        Body::C_DC_NA_1(objects) => first_address(objects)?,
        Body::C_RC_NA_1(objects) => first_address(objects)?,
        Body::C_SE_NA_1(objects) => first_address(objects)?,
        Body::C_SE_NB_1(objects) => first_address(objects)?,
        Body::C_SE_NC_1(objects) => first_address(objects)?,
        _ => return None,
    };
    let answer = match (asdu.cot.cause(), asdu.cot.negative) {
        (cause::ACTIVATION_CONFIRMATION, false) => Answer::Confirmed,
        (cause::ACTIVATION_CONFIRMATION | cause::ACTIVATION_TERMINATION, true) => Answer::Refused,
        (cause::ACTIVATION_TERMINATION, false) => Answer::Terminated,
        _ => return None,
    };
    Some((address, answer))
}

/// The address of the first object of an ASDU.
fn first_address<T>(objects: &Objects<T>) -> Option<u32> {
    match objects {
        Objects::Individual(items) => Some(items.first()?.address.value()),
        Objects::Sequence { address, .. } => Some(address.value()),
    }
}
