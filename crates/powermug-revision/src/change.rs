//! The change class of a partition revision against the one it replaces (FUN-CFG-007,
//! SPECIFICATIONS 10.5). The classes are ordered by their cost: a change of a command rule, of a
//! commandable point's address or identity, or of the partition of its session is
//! `COMMAND_CRITICAL`; a change of a session's endpoint or contract is `SESSION_RESTART`; the rest
//! is `HOT`. `SYSTEM_REBOOT` is not produced by this subset: no change in it needs a reboot yet.

use std::collections::BTreeSet;

use crate::partition::{PartitionRevision, Point};

/// The class of a change, from the cheapest to the most disruptive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ChangeClass {
    /// Processing only: no South session restarts.
    Hot,
    /// A session restarts to take its new endpoint or contract.
    SessionRestart,
    /// New commands are blocked and in-flight ones are classified before activation.
    CommandCritical,
}

impl ChangeClass {
    /// The name of the class in the documents and the journal.
    #[must_use]
    pub fn code(self) -> &'static str {
        match self {
            Self::Hot => "HOT",
            Self::SessionRestart => "SESSION_RESTART",
            Self::CommandCritical => "COMMAND_CRITICAL",
        }
    }
}

/// The class of the change from `previous` to `next`.
#[must_use]
pub fn change_class(previous: &PartitionRevision, next: &PartitionRevision) -> ChangeClass {
    if command_critical(previous, next) {
        ChangeClass::CommandCritical
    } else if previous.spec.sessions != next.spec.sessions {
        ChangeClass::SessionRestart
    } else {
        ChangeClass::Hot
    }
}

fn command_critical(previous: &PartitionRevision, next: &PartitionRevision) -> bool {
    if previous.metadata.site != next.metadata.site || previous.spec.commands != next.spec.commands
    {
        return true;
    }
    let commandable: BTreeSet<&str> = previous
        .spec
        .commands
        .iter()
        .map(|rule| rule.point.as_str())
        .collect();
    commandable
        .into_iter()
        .any(|id| moved(point(previous, id), point(next, id)))
}

fn point<'a>(revision: &'a PartitionRevision, id: &str) -> Option<&'a Point> {
    revision.spec.points.iter().find(|point| point.id == id)
}

/// Whether a commandable point changed its session or its address, or appeared or disappeared.
fn moved(before: Option<&Point>, after: Option<&Point>) -> bool {
    match (before, after) {
        (Some(before), Some(after)) => {
            before.session != after.session || before.address != after.address
        }
        (None, None) => false,
        _ => true,
    }
}
