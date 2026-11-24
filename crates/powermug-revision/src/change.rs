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
    /// The cell restarts. Declared only: no change of this subset computes to it.
    SystemReboot,
}

impl ChangeClass {
    /// The name of the class in the documents and the journal.
    #[must_use]
    pub fn code(self) -> &'static str {
        match self {
            Self::Hot => "HOT",
            Self::SessionRestart => "SESSION_RESTART",
            Self::CommandCritical => "COMMAND_CRITICAL",
            Self::SystemReboot => "SYSTEM_REBOOT",
        }
    }

    /// The class named by a document, if it names one.
    #[must_use]
    pub fn from_code(code: &str) -> Option<Self> {
        match code {
            "HOT" => Some(Self::Hot),
            "SESSION_RESTART" => Some(Self::SessionRestart),
            "COMMAND_CRITICAL" => Some(Self::CommandCritical),
            "SYSTEM_REBOOT" => Some(Self::SystemReboot),
            _ => None,
        }
    }
}

/// The findings on the class a revision declares (PMV-CEL-007): a declared class lower than the
/// computed one is refused. `previous` is the revision active in the scope, if any; without one,
/// nothing runs yet and the computed class is `HOT`.
#[must_use]
pub fn declared_class_findings(
    next: &PartitionRevision,
    previous: Option<&PartitionRevision>,
) -> Vec<crate::validate::Finding> {
    let Some(declared) = next
        .spec
        .change_class
        .as_deref()
        .and_then(ChangeClass::from_code)
    else {
        return Vec::new();
    };
    let computed = previous.map_or(ChangeClass::Hot, |previous| change_class(previous, next));
    if declared >= computed {
        return Vec::new();
    }
    vec![crate::validate::Finding {
        code: "PMV-CEL-007",
        pointer: "/spec/change_class".to_owned(),
        message: format!(
            "the declared class {} is lower than the computed class {}",
            declared.code(),
            computed.code()
        ),
    }]
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
    // A point is commandable when it is declared so in either revision, or named by a rule.
    let commandable: BTreeSet<&str> = [previous, next]
        .into_iter()
        .flat_map(|revision| {
            revision
                .spec
                .points
                .iter()
                .filter(|point| point.commandable)
                .map(|point| point.id.as_str())
                .chain(
                    revision
                        .spec
                        .commands
                        .iter()
                        .map(|rule| rule.point.as_str()),
                )
        })
        .collect();
    commandable.into_iter().any(|id| {
        moved(point(previous, id), point(next, id)) || declares_differently(previous, next, id)
    })
}

/// Whether a point changed its commandability: declared commandable in one revision only.
fn declares_differently(previous: &PartitionRevision, next: &PartitionRevision, id: &str) -> bool {
    let commandable =
        |revision: &PartitionRevision| point(revision, id).is_some_and(|point| point.commandable);
    commandable(previous) != commandable(next)
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
