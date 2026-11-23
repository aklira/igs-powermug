//! The states of a command and the transitions between them ([GW] §11.3, SPECIFICATIONS 9.1).
//!
//! The table is the only place that says which transition is legal. The journal checks every
//! transition against it, and the tests check the whole table.

/// The state of a command.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum State {
    Received,
    RejectedAuth,
    RejectedPolicy,
    RejectedInvalid,
    RejectedUnavailable,
    Validated,
    DurablyAccepted,
    Ready,
    SelectSent,
    SelectConfirmed,
    ExecuteSent,
    EquipmentRejected,
    ExecuteConfirmed,
    TerminationConfirmed,
    VerifiedSuccess,
    SuccessUnverified,
    Expired,
    Cancelled,
    Failed,
    InDoubt,
    Reconciling,
}

impl State {
    /// Every state, in the order of the specification.
    pub const ALL: [State; 21] = [
        State::Received,
        State::RejectedAuth,
        State::RejectedPolicy,
        State::RejectedInvalid,
        State::RejectedUnavailable,
        State::Validated,
        State::DurablyAccepted,
        State::Ready,
        State::SelectSent,
        State::SelectConfirmed,
        State::ExecuteSent,
        State::EquipmentRejected,
        State::ExecuteConfirmed,
        State::TerminationConfirmed,
        State::VerifiedSuccess,
        State::SuccessUnverified,
        State::Expired,
        State::Cancelled,
        State::Failed,
        State::InDoubt,
        State::Reconciling,
    ];

    /// The name stored in the journal, as in the specification.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            State::Received => "RECEIVED",
            State::RejectedAuth => "REJECTED_AUTH",
            State::RejectedPolicy => "REJECTED_POLICY",
            State::RejectedInvalid => "REJECTED_INVALID",
            State::RejectedUnavailable => "REJECTED_UNAVAILABLE",
            State::Validated => "VALIDATED",
            State::DurablyAccepted => "DURABLY_ACCEPTED",
            State::Ready => "READY",
            State::SelectSent => "SELECT_SENT",
            State::SelectConfirmed => "SELECT_CONFIRMED",
            State::ExecuteSent => "EXECUTE_SENT",
            State::EquipmentRejected => "EQUIPMENT_REJECTED",
            State::ExecuteConfirmed => "EXECUTE_CONFIRMED",
            State::TerminationConfirmed => "TERMINATION_CONFIRMED",
            State::VerifiedSuccess => "VERIFIED_SUCCESS",
            State::SuccessUnverified => "SUCCESS_UNVERIFIED",
            State::Expired => "EXPIRED",
            State::Cancelled => "CANCELLED",
            State::Failed => "FAILED",
            State::InDoubt => "IN_DOUBT",
            State::Reconciling => "RECONCILING",
        }
    }

    /// The state named by a journal code, if any.
    #[must_use]
    pub fn from_code(code: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|state| state.code() == code)
    }

    /// A terminal state is absorbing: no transition leaves it.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            State::RejectedAuth
                | State::RejectedPolicy
                | State::RejectedInvalid
                | State::RejectedUnavailable
                | State::EquipmentRejected
                | State::VerifiedSuccess
                | State::SuccessUnverified
                | State::Expired
                | State::Cancelled
                | State::Failed
        )
    }

    /// Whether the table allows the transition from `self` to `next`.
    #[must_use]
    pub fn may_move_to(self, next: State) -> bool {
        transitions(self).contains(&next)
    }
}

/// The states reachable in one transition from `from`.
///
/// Read from the diagram of [GW] §11.3, with the recovery edges of §11.5 made explicit.
/// `Ready` and `SelectConfirmed` may go to `InDoubt` at recovery, when an intent to send was
/// journaled and its result was not: the frame may or may not have left.
#[must_use]
pub fn transitions(from: State) -> &'static [State] {
    #[allow(clippy::enum_glob_use, reason = "a transition table, read row by row")]
    use State::*;
    match from {
        Received => &[
            RejectedAuth,
            RejectedPolicy,
            RejectedInvalid,
            RejectedUnavailable,
            Validated,
            Failed,
        ],
        Validated => &[DurablyAccepted, Failed],
        DurablyAccepted => &[Ready, Expired, Cancelled, Failed],
        Ready => &[SelectSent, ExecuteSent, Expired, Cancelled, Failed, InDoubt],
        SelectSent => &[SelectConfirmed, Failed, InDoubt, Expired],
        SelectConfirmed => &[ExecuteSent, Expired, Cancelled, Failed, InDoubt],
        ExecuteSent => &[EquipmentRejected, ExecuteConfirmed, InDoubt, Failed],
        ExecuteConfirmed => &[TerminationConfirmed, InDoubt, Failed],
        TerminationConfirmed => &[VerifiedSuccess, SuccessUnverified, Failed],
        InDoubt => &[Reconciling, Failed],
        Reconciling => &[
            ExecuteConfirmed,
            TerminationConfirmed,
            VerifiedSuccess,
            SuccessUnverified,
            EquipmentRejected,
            Failed,
        ],
        RejectedAuth | RejectedPolicy | RejectedInvalid | RejectedUnavailable
        | EquipmentRejected | VerifiedSuccess | SuccessUnverified | Expired | Cancelled
        | Failed => &[],
    }
}
