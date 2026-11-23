//! Admission of a command to dispatch (SPECIFICATIONS 9.4, PM-CMD-004, PM-CMD-005).
//!
//! The core checks the partition at submission, and the South adapter checks the epoch it was
//! given at activation, and the expiry, just before it emits the command.

use crate::authorization::Refusal;
use crate::ownership::Owner;
use crate::state::State;

/// The pair a dispatch carries (PM-CMD-005).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Dispatch {
    pub epoch: u64,
}

/// Core side, at submission: the command is admitted only when the partition has a confirmed
/// owner and the command has not expired. Returns the dispatch, with the epoch of the owner.
///
/// # Errors
///
/// Returns `REJECTED_INVALID` for a command already expired, and `REJECTED_UNAVAILABLE` for a
/// partition in cession, in shadow mode, or without an owner.
pub fn admit_submission(owner: Owner, expires_ns: i64, now_ns: i64) -> Result<Dispatch, Refusal> {
    if expires_ns <= now_ns {
        return Err(Refusal {
            state: State::RejectedInvalid,
            reason: "expired at submission",
        });
    }
    match owner {
        Owner::Confirmed { epoch } => Ok(Dispatch { epoch }),
        Owner::Ceding => Err(Refusal {
            state: State::RejectedUnavailable,
            reason: "partition in cession",
        }),
        Owner::Shadow => Err(Refusal {
            state: State::RejectedUnavailable,
            reason: "partition in shadow mode",
        }),
        Owner::Unowned => Err(Refusal {
            state: State::RejectedUnavailable,
            reason: "partition without a confirmed owner",
        }),
    }
}

/// South side: the epoch the adapter was given when its session was activated (PM-CMD-005).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EpochGuard {
    epoch: u64,
}

impl EpochGuard {
    /// The guard of a session activated at `epoch`.
    #[must_use]
    pub const fn activated_at(epoch: u64) -> Self {
        Self { epoch }
    }

    /// Admits a dispatch just before emission. The epoch must be the one of activation, and
    /// the command must not have expired (PM-CMD-004).
    ///
    /// # Errors
    ///
    /// Returns `FAILED` for another epoch, and `EXPIRED` for a command past its expiry.
    pub fn admit(&self, dispatch: Dispatch, expires_ns: i64, now_ns: i64) -> Result<(), Refusal> {
        if dispatch.epoch != self.epoch {
            return Err(Refusal {
                state: State::Failed,
                reason: "epoch differs from the activation",
            });
        }
        if expires_ns <= now_ns {
            return Err(Refusal {
                state: State::Expired,
                reason: "expired before emission",
            });
        }
        Ok(())
    }
}
