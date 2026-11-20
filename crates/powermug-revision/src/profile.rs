//! The target profile `powermug@1` (SPECIFICATIONS §11.2): the capabilities the
//! core declares and the limits of its hardware profile. Stage 3 of the validation
//! (§10.4) checks a revision against it.

use std::collections::BTreeMap;

/// The sizing limits of the profile (FUN-SCA-003). The values are those of §11.2.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Limits {
    pub max_partitions: u64,
    pub max_rtu: u64,
    pub max_points: u64,
    pub max_events_per_second: u64,
    pub max_sessions: u64,
    pub max_processors: u64,
}

/// A target profile: the contract version, the capabilities and the limits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Profile {
    pub name: String,
    pub contract_version: String,
    /// Capability name to version, for example `south.iec104` to `1.0`.
    pub capabilities: BTreeMap<String, String>,
    pub limits: Limits,
}

/// The profile `powermug@1` of §11.2.
#[must_use]
pub fn powermug_v1() -> Profile {
    let capabilities = [
        ("south.iec104", "1.0"),
        ("north.iec104", "1.0"),
        ("north.kafka", "1.0"),
        ("north.mqtt5", "1.0"),
        ("dest.image", "1.0"),
        ("dest.flux", "1.0"),
        ("cmd.direct", "1.0"),
        ("cmd.sbo", "1.0"),
        ("cmd.setpoint", "1.0"),
        ("indicators.v1", "1.0"),
        ("local_actions.v1", "1.0"),
        ("target.state.rest", "1.0"),
        ("target.revision.rest", "1.0"),
        ("transforms.catalog.v1", "1.0"),
        ("expr.cel.v1", "1.0"),
        ("processing.v1", "1.0"),
        ("scope.cell", "1.0"),
    ]
    .into_iter()
    .map(|(name, version)| (name.to_owned(), version.to_owned()))
    .collect();
    Profile {
        name: "powermug@1".to_owned(),
        contract_version: "1.0.0".to_owned(),
        capabilities,
        limits: Limits {
            max_partitions: 70,
            max_rtu: 70,
            max_points: 200_000,
            max_events_per_second: 50_000,
            max_sessions: 500,
            max_processors: 16,
        },
    }
}
