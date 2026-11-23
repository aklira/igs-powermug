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
use igs_iec104_codec::elements::{Qoc, Sco};
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
    #[error("the client could not send the command: {0}")]
    Client(#[from] ClientError),
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

/// The answer of the station to a single command, with the address it concerns, if `asdu` is
/// such an answer.
#[must_use]
pub fn answer(asdu: &Asdu) -> Option<(u32, Answer)> {
    let Body::C_SC_NA_1(objects) = &asdu.body else {
        return None;
    };
    let address = match objects {
        Objects::Individual(items) => items.first()?.address.value(),
        Objects::Sequence { address, .. } => address.value(),
    };
    let answer = match (asdu.cot.cause(), asdu.cot.negative) {
        (cause::ACTIVATION_CONFIRMATION, false) => Answer::Confirmed,
        (cause::ACTIVATION_CONFIRMATION | cause::ACTIVATION_TERMINATION, true) => Answer::Refused,
        (cause::ACTIVATION_TERMINATION, false) => Answer::Terminated,
        _ => return None,
    };
    Some((address, answer))
}
