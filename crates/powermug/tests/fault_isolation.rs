//! Fault isolation of adapter instances (FUN-ACQ-001/002, PM-ACC-004, PM-PROC-001).
//!
//! Two healthy fake adapters run next to five faulty ones, each in its own sandbox and its
//! own cgroup. Each fault must end the faulty instance in the expected way, and the healthy
//! instances must keep running. The test process itself is the "core": it survives every
//! fault and asserts the result.
//!
//! The test needs a delegated cgroup v2 subtree with the memory controller, and Landlock.
//! Without them it is skipped with a message. On the development host, run it as:
//!
//! ```text
//! systemd-run --user --scope -q -p Delegate=yes -- cargo test -p powermug --test fault_isolation
//! ```
//!
//! `cargo test` runs the test binary inside the scope together with `cargo` itself, and the
//! cgroup rule "no internal processes" then refuses the subtree. Build the binary first and run
//! it under `systemd-run` (see PROGRESS.md). Set `POWERMUG_REQUIRE_SANDBOX=1` on a runner that
//! must test the sandbox, so that a missing prerequisite fails instead of skipping.

use std::fs;
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus};
use std::thread::sleep;
use std::time::{Duration, Instant};

use powermug_sandbox::cgroup::{own_cgroup, Cgroup, Limits};
use powermug_sandbox::features::probe;

const BIN: &str = env!("CARGO_BIN_EXE_powermug");
const SIGABRT: i32 = 6;
const SIGKILL: i32 = 9;
const SIGSYS: i32 = 31;

/// Paths a fake adapter may read: its own binary and the system libraries. `/etc/hostname`
/// is deliberately absent, so the `read-forbidden` fault is refused by Landlock.
fn read_paths() -> Vec<PathBuf> {
    let exe_dir = Path::new(BIN).parent().expect("binary has a directory");
    [
        exe_dir,
        Path::new("/usr"),
        Path::new("/lib"),
        Path::new("/lib64"),
        Path::new("/etc/ld.so.cache"),
        Path::new("/proc"),
    ]
    .into_iter()
    .filter(|p| p.exists())
    .map(Path::to_path_buf)
    .collect()
}

fn spawn_instance(group: &Cgroup, fault: &str, seconds: u64, workdir: &Path) -> Child {
    let mut command = Command::new(BIN);
    command.arg("spawner").arg("--cgroup").arg(group.path());
    for path in read_paths() {
        command.arg("--read").arg(path);
    }
    command.arg("--write").arg(workdir);
    command
        .arg("--")
        .arg(BIN)
        .args(["adapter", "fake", "--fault", fault])
        .arg("--seconds")
        .arg(seconds.to_string());
    command.spawn().expect("spawner starts")
}

fn wait_for(child: &mut Child, limit: Duration) -> Option<ExitStatus> {
    let deadline = Instant::now() + limit;
    loop {
        if let Some(status) = child.try_wait().expect("try_wait") {
            return Some(status);
        }
        if Instant::now() > deadline {
            return None;
        }
        sleep(Duration::from_millis(20));
    }
}

#[test]
fn faulty_adapters_die_alone_and_healthy_ones_keep_running() {
    let features = probe();
    if !features.cgroup_delegated || !features.memory_controller || features.landlock_abi.is_none()
    {
        let message = "needs a delegated cgroup v2 subtree with memory, and Landlock \
             (run under `systemd-run --user --scope -p Delegate=yes`)";
        // A runner that claims to test the sandbox must not skip silently: set
        // POWERMUG_REQUIRE_SANDBOX=1 there, and a missing prerequisite fails the test.
        assert!(
            std::env::var_os("POWERMUG_REQUIRE_SANDBOX").is_none(),
            "POWERMUG_REQUIRE_SANDBOX is set but the sandbox {message}"
        );
        eprintln!("skipped: {message}");
        return;
    }
    let own = own_cgroup().expect("cgroup v2 membership");
    let parent = Cgroup::delegate_subtree(&own, &["memory", "pids"]).expect("subtree delegated");

    let workdir = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("fault-isolation");
    fs::create_dir_all(&workdir).expect("work directory");

    let limits = Limits {
        memory_max: Some(64 * 1024 * 1024),
        pids_max: Some(32),
        no_swap: true,
    };

    // Healthy instances run for 6 s; the faults happen meanwhile.
    let healthy_group_a = parent.create_child("healthy-a", limits).expect("group");
    let healthy_group_b = parent.create_child("healthy-b", limits).expect("group");
    let mut healthy_a = spawn_instance(&healthy_group_a, "run", 6, &workdir);
    let mut healthy_b = spawn_instance(&healthy_group_b, "run", 6, &workdir);

    let faults = ["abort", "panic", "oom", "syscall", "read-forbidden"];
    let mut instances: Vec<(&str, Cgroup, Child)> = faults
        .iter()
        .map(|fault| {
            let group = parent
                .create_child(&format!("fault-{fault}"), limits)
                .expect("group");
            let child = spawn_instance(&group, fault, 6, &workdir);
            (*fault, group, child)
        })
        .collect();

    let mut outcomes = Vec::new();
    for (fault, group, child) in &mut instances {
        let status = wait_for(child, Duration::from_secs(20))
            .unwrap_or_else(|| panic!("{fault} did not end within 20 s"));
        outcomes.push((*fault, status, group.oom_kills().unwrap_or(0)));
    }

    // The healthy instances must still be running after every fault has ended.
    assert!(
        healthy_a.try_wait().expect("try_wait").is_none(),
        "healthy instance A stopped during the faults"
    );
    assert!(
        healthy_b.try_wait().expect("try_wait").is_none(),
        "healthy instance B stopped during the faults"
    );

    for (fault, status, oom_kills) in &outcomes {
        match *fault {
            "abort" => assert_eq!(status.signal(), Some(SIGABRT), "abort: {status:?}"),
            "panic" => assert_eq!(status.code(), Some(101), "panic: {status:?}"),
            "oom" => {
                assert_eq!(status.signal(), Some(SIGKILL), "oom: {status:?}");
                assert!(*oom_kills >= 1, "oom: the cgroup recorded no OOM kill");
            }
            "syscall" => assert_eq!(status.signal(), Some(SIGSYS), "syscall: {status:?}"),
            "read-forbidden" => {
                assert_eq!(status.code(), Some(42), "read-forbidden: {status:?}");
            }
            other => panic!("unexpected fault {other}"),
        }
    }

    for (name, child) in [("healthy-a", &mut healthy_a), ("healthy-b", &mut healthy_b)] {
        let status = wait_for(child, Duration::from_secs(20))
            .unwrap_or_else(|| panic!("{name} did not finish"));
        assert_eq!(status.code(), Some(0), "{name} ended badly: {status:?}");
    }

    // Every group is empty now, so it can be removed.
    for (_, group, _) in &instances {
        group.remove().expect("group empty");
    }
    healthy_group_a.remove().expect("group empty");
    healthy_group_b.remove().expect("group empty");
}
