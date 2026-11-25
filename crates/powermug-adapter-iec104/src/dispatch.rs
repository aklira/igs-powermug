//! The adapter's side of the command stream (SPECIFICATIONS 6.1, 9.4): the dispatches of the
//! core are checked, built into station ASDUs, emitted, and their outcomes reported back.
//!
//! [`CommandPort`] holds the decisions and the pending commands; the caller sends the ASDUs and
//! reads the station's answers, so that the same port serves the redundancy group of [`run`]
//! and a single connection.
//!
//! A dispatch is refused, and reported as not sent, when its owner epoch differs from the one
//! the session was activated at (PM-CMD-005). A command whose expiry has passed is reported as
//! expired and never sent (PM-CMD-004). A command emitted and not yet answered is not reported
//! here: when the stream ends, the core puts it in doubt (PM-CMD-002).
//!
//! [`run`]: crate::run::run

use std::collections::VecDeque;
use std::time::{SystemTime, UNIX_EPOCH};

use igs_iec104::client::Client;
use igs_iec104::procedures;
use igs_iec104::redundancy::RedundantClient;
use igs_iec104::Delivery;
use igs_iec104_codec::asdu::Asdu;
use igs_iec104_codec::elements::{Dco, DoubleCommandState, Qoc, Rco, RegulatingStep, Sco};
use igs_iec104_codec::header::{CommonAddress, InformationObjectAddress};
use powermug_contract::client as contract;
use powermug_contract::proto::v1 as pb;
use thiserror::Error;
use tonic::transport::Channel;

use crate::command::{self, Answer};

/// Nanoseconds since the Unix epoch, for the expiry checks.
#[must_use]
pub fn now_ns() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_nanos()),
    )
    .unwrap_or(i64::MAX)
}

/// An outcome to report to the core for one command.
#[must_use]
pub fn update(command_id: &str, status: pb::CommandStatus, reason: &str) -> pb::CommandUpdate {
    pb::CommandUpdate {
        command_id: command_id.to_owned(),
        status: status as i32,
        reason: reason.to_owned(),
    }
}

/// What the port decided for a dispatch.
#[derive(Debug)]
pub enum Prepared {
    /// The ASDU to send to the station. The caller reports [`CommandPort::sent`] or a failure.
    Emit(Asdu),
    /// The outcome is already known: report it to the core, and nothing is sent.
    Done(pb::CommandUpdate),
}

/// A command emitted to the station and not yet finished.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Pending {
    command_id: String,
    address: u32,
    select: bool,
}

/// The adapter's decisions about commands, for one session.
#[derive(Debug)]
pub struct CommandPort {
    owner_epoch: u64,
    common_address: CommonAddress,
    pending: VecDeque<Pending>,
}

impl CommandPort {
    /// A port for a session activated at `owner_epoch`, on the stations' common address.
    #[must_use]
    pub fn new(owner_epoch: u64, common_address: u16) -> Self {
        Self {
            owner_epoch,
            common_address: CommonAddress::new(common_address),
            pending: VecDeque::new(),
        }
    }

    /// Checks a dispatch and builds its station ASDU, at `now_ns`.
    #[must_use]
    pub fn prepare(&self, dispatch: &pb::CommandDispatch, now_ns: i64) -> Prepared {
        if dispatch.owner_epoch != self.owner_epoch {
            return Prepared::Done(update(
                &dispatch.command_id,
                pb::CommandStatus::NotSent,
                "owner epoch differs from the activation",
            ));
        }
        if dispatch.expires_ns <= now_ns {
            return Prepared::Done(update(
                &dispatch.command_id,
                pb::CommandStatus::Expired,
                "expired before emission",
            ));
        }
        match self.build(dispatch) {
            Ok(asdu) => Prepared::Emit(asdu),
            Err(reason) => Prepared::Done(update(
                &dispatch.command_id,
                pb::CommandStatus::NotSent,
                &reason,
            )),
        }
    }

    fn build(&self, dispatch: &pb::CommandDispatch) -> Result<Asdu, String> {
        let address = InformationObjectAddress::new(dispatch.address)
            .ok_or_else(|| format!("address {} is not valid", dispatch.address))?;
        let qoc = Qoc::new(Qoc::NO_ADDITIONAL_DEFINITION, dispatch.select)
            .ok_or_else(|| "the qualifier of the command is not valid".to_owned())?;
        let built = match pb::CommandKind::try_from(dispatch.kind)
            .unwrap_or(pb::CommandKind::Unspecified)
        {
            pb::CommandKind::Single => procedures::single_command(
                self.common_address,
                address,
                Sco {
                    on: dispatch.on,
                    qoc,
                },
                None,
            ),
            pb::CommandKind::Double => {
                let state = if dispatch.on {
                    DoubleCommandState::On
                } else {
                    DoubleCommandState::Off
                };
                procedures::double_command(self.common_address, address, Dco { state, qoc }, None)
            }
            pb::CommandKind::RegulatingStep => {
                let step = if dispatch.on {
                    RegulatingStep::Higher
                } else {
                    RegulatingStep::Lower
                };
                procedures::regulating_step(self.common_address, address, Rco { step, qoc }, None)
            }
            pb::CommandKind::Unspecified => return Err("the command kind is not known".to_owned()),
        };
        built.map_err(|err| err.to_string())
    }

    /// Records that the ASDU of `dispatch` was handed to the station.
    pub fn sent(&mut self, dispatch: &pb::CommandDispatch) {
        self.pending.push_back(Pending {
            command_id: dispatch.command_id.clone(),
            address: dispatch.address,
            select: dispatch.select,
        });
    }

    /// Applies a station answer to the pending command of its address. Returns the update to
    /// report, if the answer finishes a step of a command.
    pub fn answer(&mut self, asdu: &Asdu) -> Option<pb::CommandUpdate> {
        let (address, answer) = command::answer(asdu)?;
        let index = self.pending.iter().position(|p| p.address == address)?;
        let pending = self.pending[index].clone();
        let finished = match answer {
            Answer::Confirmed => pending.select,
            Answer::Terminated | Answer::Refused => true,
        };
        if finished {
            self.pending.remove(index);
        }
        let status = match answer {
            Answer::Confirmed => pb::CommandStatus::Confirmed,
            Answer::Terminated => pb::CommandStatus::Terminated,
            Answer::Refused => pb::CommandStatus::Refused,
        };
        Some(update(&pending.command_id, status, ""))
    }

    /// The link to the station is lost: every command that left and has no answer yet is in
    /// doubt. The pending commands are forgotten, so that a late answer is never applied to them.
    pub fn lost(&mut self) -> Vec<pb::CommandUpdate> {
        self.pending
            .drain(..)
            .map(|pending| {
                update(
                    &pending.command_id,
                    pb::CommandStatus::InDoubt,
                    "the link to the station was lost before its answer",
                )
            })
            .collect()
    }
}

/// The command stream could not be opened.
#[derive(Debug, Error)]
pub enum DispatchError {
    #[error("the command stream could not be opened: {0}")]
    Stream(String),
}

/// Serves the dispatches of the core on `channel` through a single connection, until `stop`
/// completes or the core ends the stream. The caller starts the data transfer of `client`
/// beforehand.
///
/// # Errors
///
/// Returns [`DispatchError::Stream`] when the core refuses the command stream.
pub async fn serve_single(
    channel: Channel,
    client: &mut Client,
    owner_epoch: u64,
    common_address: u16,
    stop: impl std::future::Future<Output = ()>,
) -> Result<u64, DispatchError> {
    let (updates, mut dispatches) = contract::execute_commands(channel)
        .await
        .map_err(|err| DispatchError::Stream(err.to_string()))?;
    let mut port = CommandPort::new(owner_epoch, common_address);
    let mut count = 0u64;
    tokio::pin!(stop);
    loop {
        tokio::select! {
            () = &mut stop => break,
            dispatch = dispatches.message() => match dispatch {
                Ok(Some(dispatch)) => {
                    count += 1;
                    let outcome = match port.prepare(&dispatch, now_ns()) {
                        Prepared::Done(outcome) => outcome,
                        Prepared::Emit(asdu) => match client.send_asdu(asdu).await {
                            Ok(()) => {
                                port.sent(&dispatch);
                                continue;
                            }
                            Err(err) => update(&dispatch.command_id, pb::CommandStatus::NotSent, &err.to_string()),
                        },
                    };
                    if updates.send(outcome).await.is_err() {
                        break;
                    }
                }
                Ok(None) | Err(_) => break,
            },
            event = client.next_event() => match event {
                Some(igs_iec104::client::Event::Delivery(Delivery::Asdu(asdu))) => {
                    if let Some(outcome) = port.answer(&asdu) {
                        if updates.send(outcome).await.is_err() {
                            break;
                        }
                    }
                }
                Some(_) => {}
                None => break,
            },
        }
    }
    Ok(count)
}

/// The group's side of the same port: the ASDUs go to the started connection, and the answers
/// come from the group's events.
pub async fn send_group(
    group: &RedundantClient,
    port: &mut CommandPort,
    dispatch: &pb::CommandDispatch,
) -> Option<pb::CommandUpdate> {
    match port.prepare(dispatch, now_ns()) {
        Prepared::Done(outcome) => Some(outcome),
        Prepared::Emit(asdu) => match group.send_asdu(asdu).await {
            Ok(()) => {
                port.sent(dispatch);
                None
            }
            Err(err) => Some(update(
                &dispatch.command_id,
                pb::CommandStatus::NotSent,
                &err.to_string(),
            )),
        },
    }
}
