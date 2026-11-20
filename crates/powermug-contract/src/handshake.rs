//! Handshake decision: what the core accepts from an adapter (PM-ADP-001 and
//! §8.2 of [GW]).
//!
//! A major API mismatch, an unaccepted pivot version or a missing capability
//! stops the instance with a typed error. Nothing is degraded silently.

use thiserror::Error;

use crate::bounds::{MAX_BATCH_BYTES, MAX_BATCH_EVENTS, MAX_COMMAND_BYTES};
use crate::proto::v1 as pb;

/// API version spoken by this core.
pub const API_VERSION: Version = Version { major: 1, minor: 0 };

/// A version of the API or of the pivot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Version {
    pub major: u32,
    pub minor: u32,
}

/// A capability the core needs from the adapter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Requirement {
    pub protocol: String,
    pub role: pb::Role,
    pub function: String,
}

/// What the core asks for: the pivot it emits, and what the adapter must offer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Offer {
    pub pivot: Version,
    pub requirements: Vec<Requirement>,
    pub command_variants: Vec<String>,
}

/// The adapter's handshake was refused.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum HandshakeError {
    #[error("adapter declares no API version")]
    MissingApiVersion,
    #[error("API major {adapter} does not match the core's major {core}")]
    ApiMajorMismatch { core: u32, adapter: u32 },
    #[error("adapter does not accept pivot {major}.{minor}")]
    PivotNotAccepted { major: u32, minor: u32 },
    #[error("protocol {name} declares no role")]
    UnspecifiedRole { name: String },
    #[error("missing capability: {protocol} {role:?} {function}")]
    MissingCapability {
        protocol: String,
        role: pb::Role,
        function: String,
    },
    #[error("missing command variant {0}")]
    MissingCommandVariant(String),
    #[error("adapter declares no limits")]
    MissingLimits,
    #[error("limit {field} is zero")]
    ZeroLimit { field: &'static str },
    #[error("limit {field} = {requested} exceeds the core maximum {maximum}")]
    LimitAboveCore {
        field: &'static str,
        requested: u32,
        maximum: u64,
    },
    #[error("adapter declares no artifact hash")]
    MissingArtifactHash,
}

/// Checks the handshake of an adapter against the core's offer.
///
/// # Errors
///
/// Returns the first [`HandshakeError`] found, in the order of the checks of §8.2.
pub fn check(capabilities: &pb::Capabilities, offer: &Offer) -> Result<(), HandshakeError> {
    let api = capabilities
        .api_version
        .as_ref()
        .ok_or(HandshakeError::MissingApiVersion)?;
    if api.major != API_VERSION.major {
        return Err(HandshakeError::ApiMajorMismatch {
            core: API_VERSION.major,
            adapter: api.major,
        });
    }

    let accepts_pivot = capabilities
        .pivot_versions
        .iter()
        .any(|v| v.major == offer.pivot.major && v.minor == offer.pivot.minor);
    if !accepts_pivot {
        return Err(HandshakeError::PivotNotAccepted {
            major: offer.pivot.major,
            minor: offer.pivot.minor,
        });
    }

    for protocol in &capabilities.protocols {
        if !matches!(pb::Role::try_from(protocol.role), Ok(role) if role != pb::Role::Unspecified) {
            return Err(HandshakeError::UnspecifiedRole {
                name: protocol.name.clone(),
            });
        }
    }

    for required in &offer.requirements {
        let present = capabilities.protocols.iter().any(|p| {
            p.name == required.protocol
                && pb::Role::try_from(p.role) == Ok(required.role)
                && p.functions.contains(&required.function)
        });
        if !present {
            return Err(HandshakeError::MissingCapability {
                protocol: required.protocol.clone(),
                role: required.role,
                function: required.function.clone(),
            });
        }
    }

    for variant in &offer.command_variants {
        if !capabilities.command_variants.contains(variant) {
            return Err(HandshakeError::MissingCommandVariant(variant.clone()));
        }
    }

    check_limits(capabilities.limits.as_ref())?;

    if capabilities.artifact_sha256.is_empty() {
        return Err(HandshakeError::MissingArtifactHash);
    }
    Ok(())
}

fn check_limits(limits: Option<&pb::Limits>) -> Result<(), HandshakeError> {
    let limits = limits.ok_or(HandshakeError::MissingLimits)?;
    check_ceiling(
        "max_batch_events",
        limits.max_batch_events,
        MAX_BATCH_EVENTS,
    )?;
    check_ceiling("max_batch_bytes", limits.max_batch_bytes, MAX_BATCH_BYTES)?;
    check_ceiling(
        "max_command_bytes",
        limits.max_command_bytes,
        MAX_COMMAND_BYTES,
    )?;
    check_non_zero("window_events", limits.window_events)?;
    check_non_zero("max_events_per_second", limits.max_events_per_second)?;
    Ok(())
}

fn check_non_zero(field: &'static str, declared: u32) -> Result<(), HandshakeError> {
    if declared == 0 {
        Err(HandshakeError::ZeroLimit { field })
    } else {
        Ok(())
    }
}

fn check_ceiling(field: &'static str, declared: u32, maximum: usize) -> Result<(), HandshakeError> {
    check_non_zero(field, declared)?;
    let maximum = u64::try_from(maximum).unwrap_or(u64::MAX);
    if u64::from(declared) > maximum {
        Err(HandshakeError::LimitAboveCore {
            field,
            requested: declared,
            maximum,
        })
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn capabilities() -> pb::Capabilities {
        pb::Capabilities {
            api_version: Some(pb::Version { major: 1, minor: 3 }),
            pivot_versions: vec![pb::Version { major: 1, minor: 0 }],
            protocols: vec![pb::Protocol {
                name: "iec104".into(),
                role: pb::Role::Client as i32,
                functions: vec!["general_interrogation".into(), "clock_sync".into()],
            }],
            command_variants: vec!["single".into()],
            limits: Some(pb::Limits {
                max_batch_events: 512,
                max_batch_bytes: 512 << 10,
                max_command_bytes: 8 << 10,
                window_events: 2_048,
                max_events_per_second: 10_000,
            }),
            crash_recovery: true,
            hardware_dependencies: vec![],
            artifact_sha256: "ab".repeat(32),
            protocol_library_version: "0.1.0".into(),
            settings_schema: "{}".into(),
        }
    }

    fn offer() -> Offer {
        Offer {
            pivot: Version { major: 1, minor: 0 },
            requirements: vec![Requirement {
                protocol: "iec104".into(),
                role: pb::Role::Client,
                function: "general_interrogation".into(),
            }],
            command_variants: vec!["single".into()],
        }
    }

    #[test]
    fn compatible_adapter_is_accepted() {
        assert_eq!(check(&capabilities(), &offer()), Ok(()));
    }

    #[test]
    fn minor_difference_in_api_is_accepted_both_ways() {
        // The core's API minor is 0 and the adapter's is 3: accepted.
        let mut caps = capabilities();
        caps.api_version = Some(pb::Version { major: 1, minor: 0 });
        assert_eq!(check(&caps, &offer()), Ok(()));
    }

    #[test]
    fn major_api_mismatch_stops_the_instance() {
        let mut caps = capabilities();
        caps.api_version = Some(pb::Version { major: 2, minor: 0 });
        assert_eq!(
            check(&caps, &offer()),
            Err(HandshakeError::ApiMajorMismatch {
                core: 1,
                adapter: 2
            })
        );
    }

    #[test]
    fn missing_api_version_is_refused() {
        let mut caps = capabilities();
        caps.api_version = None;
        assert_eq!(
            check(&caps, &offer()),
            Err(HandshakeError::MissingApiVersion)
        );
    }

    #[test]
    fn pivot_must_be_accepted_exactly() {
        let mut caps = capabilities();
        caps.pivot_versions = vec![pb::Version { major: 2, minor: 0 }];
        assert_eq!(
            check(&caps, &offer()),
            Err(HandshakeError::PivotNotAccepted { major: 1, minor: 0 })
        );
    }

    #[test]
    fn missing_capability_is_a_typed_error() {
        let mut off = offer();
        off.requirements[0].function = "sbo".into();
        assert_eq!(
            check(&capabilities(), &off),
            Err(HandshakeError::MissingCapability {
                protocol: "iec104".into(),
                role: pb::Role::Client,
                function: "sbo".into(),
            })
        );
    }

    #[test]
    fn role_must_match_the_capability() {
        let mut off = offer();
        off.requirements[0].role = pb::Role::Server;
        assert!(matches!(
            check(&capabilities(), &off),
            Err(HandshakeError::MissingCapability { .. })
        ));
    }

    #[test]
    fn protocol_without_role_is_refused() {
        let mut caps = capabilities();
        caps.protocols[0].role = pb::Role::Unspecified as i32;
        assert_eq!(
            check(&caps, &offer()),
            Err(HandshakeError::UnspecifiedRole {
                name: "iec104".into()
            })
        );
    }

    #[test]
    fn missing_command_variant_is_refused() {
        let mut off = offer();
        off.command_variants = vec!["select_before_operate".into()];
        assert_eq!(
            check(&capabilities(), &off),
            Err(HandshakeError::MissingCommandVariant(
                "select_before_operate".into()
            ))
        );
    }

    #[test]
    fn limit_above_the_core_maximum_is_refused() {
        let mut caps = capabilities();
        if let Some(limits) = caps.limits.as_mut() {
            limits.max_batch_events = 2_000;
        }
        assert_eq!(
            check(&caps, &offer()),
            Err(HandshakeError::LimitAboveCore {
                field: "max_batch_events",
                requested: 2_000,
                maximum: 1_024,
            })
        );
    }

    #[test]
    fn zero_limit_is_refused() {
        let mut caps = capabilities();
        if let Some(limits) = caps.limits.as_mut() {
            limits.window_events = 0;
        }
        assert_eq!(
            check(&caps, &offer()),
            Err(HandshakeError::ZeroLimit {
                field: "window_events"
            })
        );
    }

    #[test]
    fn missing_limits_and_missing_hash_are_refused() {
        let mut no_limits = capabilities();
        no_limits.limits = None;
        assert_eq!(
            check(&no_limits, &offer()),
            Err(HandshakeError::MissingLimits)
        );

        let mut no_hash = capabilities();
        no_hash.artifact_sha256.clear();
        assert_eq!(
            check(&no_hash, &offer()),
            Err(HandshakeError::MissingArtifactHash)
        );
    }
}
