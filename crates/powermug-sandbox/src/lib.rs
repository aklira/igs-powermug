//! Process sandbox for Powermug adapters (§6.4, PM-PROC-001, PM-ADP-005).
//!
//! The sandbox has three layers, each applied by the process that will run the adapter,
//! just before `exec`:
//!
//! - cgroup v2 (`cgroup`): the process joins a child group with memory, pids and
//!   swap limits. The core creates the group; the spawner joins it.
//! - `no_new_privs` and Landlock (`landlock`): filesystem access is limited to the
//!   declared paths. The restriction survives `exec`.
//! - seccomp (`seccomp`): a system-call allow-list. A forbidden call kills the process
//!   with `SIGSYS`.
//!
//! Everything here uses safe APIs. The crate therefore keeps the workspace rule
//! `unsafe_code = "forbid"`.

#![forbid(unsafe_code)]

pub mod cgroup;
pub mod features;
pub mod profile;
pub mod seccomp;

pub use cgroup::{Cgroup, Limits};
pub use features::{probe, Features};
pub use profile::{apply, Profile};

use thiserror::Error;

/// Errors of the sandbox. Each names the layer that refused.
#[derive(Debug, Error)]
pub enum SandboxError {
    /// A cgroup file could not be read or written.
    #[error("cgroup {path}: {source}")]
    Cgroup {
        /// The file or directory.
        path: String,
        /// The underlying error.
        #[source]
        source: std::io::Error,
    },
    /// A required kernel feature is missing (PM-ADP-005).
    #[error("missing kernel feature: {0}")]
    MissingFeature(&'static str),
    /// `no_new_privs` could not be set.
    #[error("no_new_privs: {0}")]
    NoNewPrivs(nix::errno::Errno),
    /// The Landlock ruleset could not be built or applied.
    #[error("landlock: {0}")]
    Landlock(String),
    /// The seccomp filter could not be compiled or installed.
    #[error("seccomp: {0}")]
    Seccomp(String),
    /// An I/O error outside the cgroup files.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}
