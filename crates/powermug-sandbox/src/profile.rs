//! The sandbox profile of an adapter and the function that applies it.

use std::path::PathBuf;

use landlock::{
    path_beneath_rules, Access, AccessFs, Ruleset, RulesetAttr, RulesetCreatedAttr, ABI,
};
use nix::sys::prctl;

use crate::{seccomp, SandboxError};

/// What an adapter may touch. Everything not listed is refused.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Profile {
    /// Paths the adapter may read and execute (its binary, its libraries, its config).
    pub read_paths: Vec<PathBuf>,
    /// Paths the adapter may read and write (its work directory only).
    pub write_paths: Vec<PathBuf>,
    /// System calls allowed; see [`seccomp::adapter_default`].
    pub syscalls: Vec<i64>,
}

impl Profile {
    /// A profile with the default system-call list and the given paths.
    #[must_use]
    pub fn adapter(read_paths: Vec<PathBuf>, write_paths: Vec<PathBuf>) -> Self {
        Self {
            read_paths,
            write_paths,
            syscalls: seccomp::adapter_default(),
        }
    }
}

/// Applies the profile to the calling process: `no_new_privs`, then Landlock, then seccomp.
/// The restrictions survive `exec`, so the caller must `exec` the adapter afterwards.
///
/// # Errors
///
/// Returns the first layer that refuses (see [`SandboxError`]).
pub fn apply(profile: &Profile) -> Result<(), SandboxError> {
    prctl::set_no_new_privs().map_err(SandboxError::NoNewPrivs)?;
    restrict_files(profile)?;
    seccomp::install(&profile.syscalls)?;
    Ok(())
}

fn restrict_files(profile: &Profile) -> Result<(), SandboxError> {
    let abi = ABI::V3;
    let all = AccessFs::from_all(abi);
    let read = AccessFs::from_read(abi);
    let write = AccessFs::from_all(abi);
    let landlock_err = |e: &dyn std::fmt::Display| SandboxError::Landlock(e.to_string());

    let ruleset = Ruleset::default()
        .handle_access(all)
        .map_err(|e| landlock_err(&e))?
        .create()
        .map_err(|e| landlock_err(&e))?
        .add_rules(path_beneath_rules(&profile.read_paths, read))
        .map_err(|e| landlock_err(&e))?
        .add_rules(path_beneath_rules(&profile.write_paths, write))
        .map_err(|e| landlock_err(&e))?;
    let status = ruleset.restrict_self().map_err(|e| landlock_err(&e))?;
    if status.ruleset == landlock::RulesetStatus::NotEnforced {
        return Err(SandboxError::MissingFeature("landlock enforcement"));
    }
    Ok(())
}
