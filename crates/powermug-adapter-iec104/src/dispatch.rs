//! The adapter's side of the command stream (SPECIFICATIONS 6.1, 9.4): receives the dispatches
//! of the core, emits each one to the station, and reports its outcome back to the core.
//!
//! A dispatch is refused, and reported as not sent, when its owner epoch differs from the one
//! the session was activated at (PM-CMD-005). A command whose expiry has passed is reported as
//! expired and never sent (PM-CMD-004). A command that was emitted and is not yet answered is
//! not reported here: when the stream ends, the core puts it in doubt (PM-CMD-002).

use std::collections::VecDeque;
use std::time::{SystemTime, UNIX_EPOCH};

use igs_iec104::client::{Client, Event};
use igs_iec104::Delivery;
use powermug_contract::client as contract;
use powermug_contract::proto::v1 as pb;
use thiserror::Error;
use tonic::transport::Channel;

use crate::command::{
    self, check_live, Answer, CommandError, DoubleCommand, RegulatingStepCommand, SingleCommand,
};

/// What the loop did, when it stopped.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct DispatchReport {
    /// Dispatches received from the core.
    pub dispatched: u64,
    /// Station answers reported to the core.
    pub outcomes: u64,
}

/// The command stream could not be opened.
#[derive(Debug, Error)]
pub enum DispatchError {
    #[error("the command stream could not be opened: {0}")]
    Stream(String),
}

/// A command emitted to the station and not yet finished.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Pending {
    command_id: String,
    address: u32,
    select: bool,
}

fn now_ns() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_nanos()),
    )
    .unwrap_or(i64::MAX)
}

fn update(command_id: &str, status: pb::CommandStatus, reason: &str) -> pb::CommandUpdate {
    pb::CommandUpdate {
        command_id: command_id.to_owned(),
        status: status as i32,
        reason: reason.to_owned(),
    }
}

/// Emits one dispatch. Returns the update to report now, if the outcome is already known.
async fn emit(
    client: &Client,
    dispatch: &pb::CommandDispatch,
    epoch: u64,
    pending: &mut VecDeque<Pending>,
) -> Option<pb::CommandUpdate> {
    if dispatch.owner_epoch != epoch {
        return Some(update(
            &dispatch.command_id,
            pb::CommandStatus::NotSent,
            "owner epoch differs from the activation",
        ));
    }
    if let Err(CommandError::Expired { .. }) = check_live(dispatch.expires_ns, now_ns()) {
        return Some(update(
            &dispatch.command_id,
            pb::CommandStatus::Expired,
            "expired before emission",
        ));
    }
    let sent =
        match pb::CommandKind::try_from(dispatch.kind).unwrap_or(pb::CommandKind::Unspecified) {
            pb::CommandKind::Single => {
                command::send(
                    client,
                    SingleCommand {
                        address: dispatch.address,
                        on: dispatch.on,
                        select: dispatch.select,
                    },
                )
                .await
            }
            pb::CommandKind::Double => {
                command::send_double(
                    client,
                    DoubleCommand {
                        address: dispatch.address,
                        on: dispatch.on,
                        select: dispatch.select,
                    },
                )
                .await
            }
            pb::CommandKind::RegulatingStep => {
                command::send_regulating_step(
                    client,
                    RegulatingStepCommand {
                        address: dispatch.address,
                        higher: dispatch.on,
                        select: dispatch.select,
                    },
                )
                .await
            }
            pb::CommandKind::Unspecified => Err(CommandError::Qualifier),
        };
    match sent {
        Ok(()) => {
            pending.push_back(Pending {
                command_id: dispatch.command_id.clone(),
                address: dispatch.address,
                select: dispatch.select,
            });
            None
        }
        Err(err) => Some(update(
            &dispatch.command_id,
            pb::CommandStatus::NotSent,
            &err.to_string(),
        )),
    }
}

/// Serves the dispatches of the core on `channel`, emitting them through `client`, until `stop`
/// completes or the core ends the stream. `epoch` is the owner epoch of the session's
/// activation (PM-CMD-005). The caller starts the data transfer of `client` beforehand.
///
/// # Errors
///
/// Returns [`DispatchError::Stream`] when the core refuses the command stream.
pub async fn serve(
    channel: Channel,
    client: &mut Client,
    epoch: u64,
    stop: impl std::future::Future<Output = ()>,
) -> Result<DispatchReport, DispatchError> {
    let (updates, mut dispatches) = contract::execute_commands(channel)
        .await
        .map_err(|err| DispatchError::Stream(err.to_string()))?;
    let mut report = DispatchReport::default();
    let mut pending: VecDeque<Pending> = VecDeque::new();
    tokio::pin!(stop);
    loop {
        tokio::select! {
            () = &mut stop => break,
            dispatch = dispatches.message() => match dispatch {
                Ok(Some(dispatch)) => {
                    report.dispatched += 1;
                    if let Some(outcome) = emit(client, &dispatch, epoch, &mut pending).await {
                        report.outcomes += 1;
                        if updates.send(outcome).await.is_err() {
                            break;
                        }
                    }
                }
                Ok(None) | Err(_) => break,
            },
            event = client.next_event() => match event {
                Some(Event::Delivery(Delivery::Asdu(asdu))) => {
                    let Some((address, answer)) = command::answer(&asdu) else {
                        continue;
                    };
                    let Some(index) = pending.iter().position(|p| p.address == address) else {
                        continue;
                    };
                    let finished = match answer {
                        Answer::Confirmed => pending[index].select,
                        Answer::Terminated | Answer::Refused => true,
                    };
                    let command_id = pending[index].command_id.clone();
                    if finished {
                        pending.remove(index);
                    }
                    let status = match answer {
                        Answer::Confirmed => pb::CommandStatus::Confirmed,
                        Answer::Terminated => pb::CommandStatus::Terminated,
                        Answer::Refused => pb::CommandStatus::Refused,
                    };
                    report.outcomes += 1;
                    if updates.send(update(&command_id, status, "")).await.is_err() {
                        break;
                    }
                }
                Some(_) => {}
                None => break,
            },
        }
    }
    Ok(report)
}
