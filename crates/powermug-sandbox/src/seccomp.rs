//! seccomp allow-lists (§8.3). A call outside the list kills the process with `SIGSYS`.

use std::collections::BTreeMap;

use seccompiler::{BpfProgram, SeccompAction, SeccompFilter, SeccompRule, TargetArch};

use crate::SandboxError;

/// The system calls an adapter needs for its own runtime: memory, threads, time, I/O on
/// files it already holds, signals and exit. Network and process creation are absent.
///
/// The list is the minimum for the Rust standard library. Protocol adapters add their own
/// entries through a profile, and each addition is reviewed.
#[must_use]
pub fn adapter_default() -> Vec<i64> {
    let calls = [
        libc::SYS_read,
        libc::SYS_write,
        libc::SYS_close,
        libc::SYS_openat,
        libc::SYS_fstat,
        libc::SYS_newfstatat,
        libc::SYS_statx,
        libc::SYS_lseek,
        libc::SYS_pread64,
        libc::SYS_readv,
        libc::SYS_writev,
        libc::SYS_mmap,
        libc::SYS_munmap,
        libc::SYS_mprotect,
        libc::SYS_mremap,
        libc::SYS_madvise,
        libc::SYS_brk,
        libc::SYS_futex,
        libc::SYS_set_robust_list,
        libc::SYS_set_tid_address,
        libc::SYS_rseq,
        libc::SYS_clock_gettime,
        libc::SYS_clock_nanosleep,
        libc::SYS_nanosleep,
        libc::SYS_sched_yield,
        libc::SYS_sched_getaffinity,
        libc::SYS_getrandom,
        libc::SYS_rt_sigaction,
        libc::SYS_rt_sigprocmask,
        libc::SYS_rt_sigreturn,
        libc::SYS_sigaltstack,
        libc::SYS_getpid,
        libc::SYS_gettid,
        libc::SYS_tgkill,
        libc::SYS_prlimit64,
        libc::SYS_uname,
        libc::SYS_getcwd,
        libc::SYS_exit,
        libc::SYS_exit_group,
        // The spawner runs `execve` after the filter is installed, and the filter is inherited
        // by the adapter. Execution is therefore limited by Landlock to the declared read
        // paths. Removing this entry needs a launcher that execs before filtering (review
        // item of P1, see PROGRESS.md).
        libc::SYS_execve,
        // glibc start-up: thread-local storage and dynamic loader lookups.
        libc::SYS_arch_prctl,
        libc::SYS_readlink,
        libc::SYS_readlinkat,
        libc::SYS_faccessat,
        libc::SYS_faccessat2,
        libc::SYS_fcntl,
        libc::SYS_getdents64,
        libc::SYS_access,
        libc::SYS_poll,
    ];
    calls.to_vec()
}

/// [`adapter_default`] plus the calls of a protocol adapter that talks over TCP and over its
/// core's Unix socket: sockets, `epoll`, timers and pipes of the async runtime. The adapter
/// runs a single-threaded runtime, so no thread-creation call is added. `ioctl` is needed by
/// the standard library to set non-blocking mode; the filter cannot restrict its arguments
/// here, which is a review item (PROGRESS.md).
#[must_use]
pub fn protocol_adapter() -> Vec<i64> {
    let mut calls: std::collections::BTreeSet<i64> = adapter_default().into_iter().collect();
    calls.extend([
        libc::SYS_socket,
        libc::SYS_socketpair,
        libc::SYS_connect,
        libc::SYS_getsockopt,
        libc::SYS_setsockopt,
        libc::SYS_getsockname,
        libc::SYS_getpeername,
        libc::SYS_sendto,
        libc::SYS_recvfrom,
        libc::SYS_sendmsg,
        libc::SYS_recvmsg,
        libc::SYS_shutdown,
        libc::SYS_epoll_create1,
        libc::SYS_epoll_ctl,
        libc::SYS_epoll_pwait,
        libc::SYS_epoll_wait,
        libc::SYS_eventfd2,
        libc::SYS_timerfd_create,
        libc::SYS_timerfd_settime,
        libc::SYS_pipe2,
        libc::SYS_ioctl,
    ]);
    calls.into_iter().collect()
}

/// Installs an allow-list filter on the calling thread's process.
///
/// # Errors
///
/// Returns [`SandboxError::Seccomp`] if the filter cannot be compiled or installed.
/// `no_new_privs` must be set first, or the kernel refuses the filter without privilege.
pub fn install(allowed: &[i64]) -> Result<(), SandboxError> {
    let arch = target_arch()?;
    let rules: BTreeMap<i64, Vec<SeccompRule>> =
        allowed.iter().map(|nr| (*nr, Vec::new())).collect();
    let filter = SeccompFilter::new(
        rules,
        SeccompAction::KillProcess,
        SeccompAction::Allow,
        arch,
    )
    .map_err(|e| SandboxError::Seccomp(e.to_string()))?;
    let program: BpfProgram = filter
        .try_into()
        .map_err(|e: seccompiler::BackendError| SandboxError::Seccomp(e.to_string()))?;
    seccompiler::apply_filter(&program).map_err(|e| SandboxError::Seccomp(e.to_string()))
}

fn target_arch() -> Result<TargetArch, SandboxError> {
    match std::env::consts::ARCH {
        "x86_64" => Ok(TargetArch::x86_64),
        "aarch64" => Ok(TargetArch::aarch64),
        other => Err(SandboxError::Seccomp(format!(
            "unsupported architecture {other}"
        ))),
    }
}
