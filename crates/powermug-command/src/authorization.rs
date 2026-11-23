//! Authorization of a command (SPECIFICATIONS 9.3, FUN-CMD-003, FUN-OPS-006, PM-CMD-005).
//!
//! The rule matches on the issuer, the point, the operation, the value and the operating mode.
//! It runs in the core, never in an adapter. The ACL is explicit for every commandable point:
//! a point without a grant is refused.

use std::collections::BTreeMap;

use crate::identity::Granularity;
use crate::state::State;

/// The operating mode of the cell.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Normal,
    /// Set by the management API, audited: refuses new commands, telemetry continues.
    Maintenance,
    /// A partition that is not the active owner: refuses commands (PM-CMD-005).
    Shadow,
}

impl Mode {
    /// The name stored in the journal and sent by the management API.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Mode::Normal => "normal",
            Mode::Maintenance => "maintenance",
            Mode::Shadow => "shadow",
        }
    }

    /// The mode named by its code, if any.
    #[must_use]
    pub fn from_code(code: &str) -> Option<Self> {
        [Mode::Normal, Mode::Maintenance, Mode::Shadow]
            .into_iter()
            .find(|mode| mode.code() == code)
    }
}

/// The values a grant accepts.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ValueRule {
    /// Any value.
    Any,
    /// A boolean value only (single commands).
    Bool,
    /// A number inside `[min, max]` (set points). Booleans are refused.
    Range { min: f64, max: f64 },
}

/// What one issuer may do with one point.
#[derive(Debug, Clone, PartialEq)]
pub struct Grant {
    pub issuer: String,
    pub operations: Vec<String>,
    pub values: ValueRule,
    /// The finest identity the grant can be checked against: an identity coarser than this
    /// cannot prove the grant (GW 11.6).
    pub min_level: Granularity,
}

/// The command value of a request.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Value {
    Bool(bool),
    Number(f64),
}

/// A command as it is authorized: the issuer it actually had, the point, the operation and
/// the value.
#[derive(Debug, Clone, Copy)]
pub struct Request<'a> {
    pub issuer: &'a str,
    /// The granularity of the identity the protocol gave for this command.
    pub identity_level: Granularity,
    pub point: &'a str,
    pub operation: &'a str,
    pub value: Value,
}

/// Why a command is refused, with the state the journal records for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Refusal {
    pub state: State,
    pub reason: &'static str,
}

/// The explicit ACL of the commandable points of a revision.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Acl {
    points: BTreeMap<String, Vec<Grant>>,
}

impl Acl {
    /// An empty ACL: no point is commandable.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a grant to a point. A point may have several grants, one per issuer.
    pub fn grant(&mut self, point: &str, grant: Grant) {
        self.points.entry(point.to_owned()).or_default().push(grant);
    }

    /// The commandable points that have no grant. An empty list means the ACL is explicit
    /// for every one of them (PMV-ACL-001).
    #[must_use]
    pub fn missing<'a>(&self, commandable: &[&'a str]) -> Vec<&'a str> {
        commandable
            .iter()
            .copied()
            .filter(|point| self.points.get(*point).is_none_or(Vec::is_empty))
            .collect()
    }
}

/// Decides whether a command may be recorded and dispatched.
///
/// # Errors
///
/// Returns the [`Refusal`] to record: `REJECTED_UNAVAILABLE` in maintenance and shadow
/// modes, `REJECTED_AUTH` for a point without a grant, an issuer or operation not granted, or
/// a value the grant does not accept.
pub fn authorize(acl: &Acl, mode: Mode, request: &Request<'_>) -> Result<(), Refusal> {
    match mode {
        Mode::Normal => {}
        Mode::Maintenance => {
            return Err(Refusal {
                state: State::RejectedUnavailable,
                reason: "maintenance",
            })
        }
        Mode::Shadow => {
            return Err(Refusal {
                state: State::RejectedUnavailable,
                reason: "shadow",
            })
        }
    }
    let Some(grants) = acl.points.get(request.point) else {
        return Err(Refusal {
            state: State::RejectedAuth,
            reason: "not commandable",
        });
    };
    let Some(grant) = grants.iter().find(|grant| grant.issuer == request.issuer) else {
        return Err(Refusal {
            state: State::RejectedAuth,
            reason: "issuer not granted",
        });
    };
    if request.identity_level < grant.min_level {
        return Err(Refusal {
            state: State::RejectedAuth,
            reason: "identity level too coarse",
        });
    }
    if !grant
        .operations
        .iter()
        .any(|operation| operation == request.operation)
    {
        return Err(Refusal {
            state: State::RejectedAuth,
            reason: "operation not granted",
        });
    }
    let accepted = match (grant.values, request.value) {
        (ValueRule::Any, _) | (ValueRule::Bool, Value::Bool(_)) => true,
        (ValueRule::Range { min, max }, Value::Number(number)) => (min..=max).contains(&number),
        (ValueRule::Bool | ValueRule::Range { .. }, _) => false,
    };
    if accepted {
        Ok(())
    } else {
        Err(Refusal {
            state: State::RejectedAuth,
            reason: "value not granted",
        })
    }
}
