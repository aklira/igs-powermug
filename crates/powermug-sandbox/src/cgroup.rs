//! cgroup v2 children for adapter instances (§6.4: cgroup CPU, memory, IO and PID).
//!
//! The core only creates groups under a subtree it has been delegated (for example the
//! scope of a `systemd-run --user --scope -p Delegate=yes` or the Quadlet unit). Writes
//! are plain file operations on `/sys/fs/cgroup`, so no unsafe code is needed.

use std::fs;
use std::path::{Path, PathBuf};

use crate::SandboxError;

/// Limits applied to an adapter group. `None` leaves the controller's default.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Limits {
    /// `memory.max` in bytes.
    pub memory_max: Option<u64>,
    /// `pids.max`.
    pub pids_max: Option<u32>,
    /// `memory.swap.max = 0` when true: the instance cannot swap.
    pub no_swap: bool,
    /// `cpu.max`: CPU time per period.
    pub cpu_max: Option<CpuMax>,
}

/// `cpu.max`: at most `quota_us` microseconds of CPU in each `period_us`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CpuMax {
    pub quota_us: u64,
    pub period_us: u64,
}

/// A cgroup directory owned by the sandbox.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cgroup {
    path: PathBuf,
}

/// The cgroup v2 path of the calling process, from `/proc/self/cgroup`.
///
/// # Errors
///
/// Returns an error if the file is unreadable or has no cgroup v2 line.
pub fn own_cgroup() -> Result<PathBuf, SandboxError> {
    let text = fs::read_to_string("/proc/self/cgroup")?;
    let rel = text
        .lines()
        .find_map(|line| line.strip_prefix("0::"))
        .ok_or(SandboxError::MissingFeature("cgroup v2 membership"))?;
    Ok(Path::new("/sys/fs/cgroup").join(rel.trim_start_matches('/')))
}

fn io(path: &Path, source: std::io::Error) -> SandboxError {
    SandboxError::Cgroup {
        path: path.display().to_string(),
        source,
    }
}

impl Cgroup {
    /// Wraps an existing cgroup directory.
    #[must_use]
    pub fn at(path: PathBuf) -> Self {
        Self { path }
    }

    /// The directory of the group.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Moves the calling process into a sub-group, so that its parent can hold controllers
    /// (the cgroup v2 "no internal processes" rule), then enables `controllers` on `parent`.
    ///
    /// # Errors
    ///
    /// Returns an error if the subtree is not delegated to this user.
    pub fn delegate_subtree(parent: &Path, controllers: &[&str]) -> Result<Self, SandboxError> {
        let leaf = parent.join("core");
        create_dir(&leaf)?;
        write(&leaf.join("cgroup.procs"), &std::process::id().to_string())?;
        let enable = controllers.iter().fold(String::new(), |mut acc, c| {
            acc.push('+');
            acc.push_str(c);
            acc.push(' ');
            acc
        });
        write(&parent.join("cgroup.subtree_control"), enable.trim_end())?;
        Ok(Self::at(parent.to_path_buf()))
    }

    /// Creates a child group `name` under this group, with `limits`.
    ///
    /// # Errors
    ///
    /// Returns an error if the directory or a limit file cannot be written.
    pub fn create_child(&self, name: &str, limits: Limits) -> Result<Self, SandboxError> {
        let path = self.path.join(name);
        create_dir(&path)?;
        if let Some(bytes) = limits.memory_max {
            write(&path.join("memory.max"), &bytes.to_string())?;
        }
        if let Some(pids) = limits.pids_max {
            write(&path.join("pids.max"), &pids.to_string())?;
        }
        if limits.no_swap {
            write(&path.join("memory.swap.max"), "0")?;
        }
        if let Some(cpu) = limits.cpu_max {
            write(
                &path.join("cpu.max"),
                &format!("{} {}", cpu.quota_us, cpu.period_us),
            )?;
        }
        Ok(Self::at(path))
    }

    /// Number of OOM kills recorded in `memory.events`.
    ///
    /// # Errors
    ///
    /// Returns an error if the file is unreadable or has no `oom_kill` field.
    pub fn oom_kills(&self) -> Result<u64, SandboxError> {
        let path = self.path.join("memory.events");
        let text = fs::read_to_string(&path).map_err(|e| io(&path, e))?;
        text.lines()
            .find_map(|l| l.strip_prefix("oom_kill "))
            .and_then(|v| v.trim().parse().ok())
            .ok_or(SandboxError::MissingFeature("memory.events oom_kill"))
    }

    /// Removes the group. It must hold no process.
    ///
    /// # Errors
    ///
    /// Returns an error if processes remain or the directory cannot be removed.
    pub fn remove(&self) -> Result<(), SandboxError> {
        fs::remove_dir(&self.path).map_err(|e| io(&self.path, e))
    }
}

/// Joins the process `pid` to the group `dir`.
///
/// # Errors
///
/// Returns an error if `cgroup.procs` cannot be written.
pub fn join(dir: &Path, pid: u32) -> Result<(), SandboxError> {
    write(&dir.join("cgroup.procs"), &pid.to_string())
}

fn create_dir(path: &Path) -> Result<(), SandboxError> {
    match fs::create_dir(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
        Err(e) => Err(io(path, e)),
    }
}

fn write(path: &Path, value: &str) -> Result<(), SandboxError> {
    fs::write(path, value).map_err(|e| io(path, e))
}
