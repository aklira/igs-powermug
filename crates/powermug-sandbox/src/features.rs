//! Probing of the kernel features the sandbox needs (PM-ADP-005).
//!
//! The Landlock ABI is asked through its syscall, because `securityfs` is not always
//! mounted (it is not on the development host, see `docs/reports/kernel-features.md`).

use std::fs;
use std::path::Path;

use crate::cgroup::own_cgroup;

/// What the running kernel offers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Features {
    /// Landlock ABI version, `None` when Landlock is absent.
    pub landlock_abi: Option<u32>,
    /// The calling process sits in a cgroup v2 subtree its user can write to.
    pub cgroup_delegated: bool,
    /// The memory controller is available in that subtree.
    pub memory_controller: bool,
    /// The seccomp filter mode is supported by the kernel.
    pub seccomp: bool,
}

impl Features {
    /// Whether an adapter may run under the production profile (PM-ADP-005).
    #[must_use]
    pub fn sufficient_for_production(&self) -> bool {
        self.landlock_abi.is_some_and(|v| v >= 1) && self.cgroup_delegated && self.seccomp
    }
}

/// Probes the running kernel.
#[must_use]
pub fn probe() -> Features {
    let cgroup = own_cgroup().ok();
    let cgroup_delegated = cgroup
        .as_deref()
        .is_some_and(|dir| writable(&dir.join("cgroup.subtree_control")));
    let memory_controller = cgroup.as_deref().is_some_and(|dir| {
        fs::read_to_string(dir.join("cgroup.controllers"))
            .is_ok_and(|c| c.split_whitespace().any(|x| x == "memory"))
    });
    Features {
        landlock_abi: landlock_abi(),
        cgroup_delegated,
        memory_controller,
        seccomp: fs::read_to_string("/proc/self/status")
            .is_ok_and(|s| s.lines().any(|l| l.starts_with("Seccomp:"))),
    }
}

fn writable(path: &Path) -> bool {
    fs::OpenOptions::new().write(true).open(path).is_ok()
}

/// The Landlock ABI version of the kernel.
///
/// The `landlock` crate has no probe of its own, so the newest ABI is requested with a hard
/// requirement and the request steps down until the kernel accepts it. This uses no unsafe
/// code and no `securityfs`.
fn landlock_abi() -> Option<u32> {
    use landlock::{Access, AccessFs, CompatLevel, Compatible, Ruleset, RulesetAttr, ABI};
    let candidates = [
        (ABI::V7, 7),
        (ABI::V6, 6),
        (ABI::V5, 5),
        (ABI::V4, 4),
        (ABI::V3, 3),
        (ABI::V2, 2),
        (ABI::V1, 1),
    ];
    candidates.iter().find_map(|(abi, number)| {
        Ruleset::default()
            .set_compatibility(CompatLevel::HardRequirement)
            .handle_access(AccessFs::from_all(*abi))
            .and_then(Ruleset::create)
            .ok()
            .map(|_| *number)
    })
}
