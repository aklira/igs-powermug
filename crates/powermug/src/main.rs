//! The `powermug` binary. Roles: revision tools, the spawner, a fake adapter for tests,
//! and diagnostics. The core and the protocol adapters arrive with later phases.

#![forbid(unsafe_code)]

use std::fs;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use ed25519_dalek::pkcs8::DecodePublicKey;
use ed25519_dalek::VerifyingKey;
use powermug_revision::canonical::content_id;
use powermug_revision::profile::powermug_v1;
use powermug_revision::{
    parse_strict_json, validate_stage1, validate_stage2, validate_stage3, verify_signed_plan,
};
use powermug_sandbox::{cgroup, features, profile, Profile};

#[derive(Debug, Parser)]
#[command(name = "powermug", version, about = "Powermug gateway")]
struct Cli {
    #[command(subcommand)]
    command: Cmd,
}

#[derive(Debug, Subcommand)]
enum Cmd {
    /// Tools on revisions. They run offline and never touch a cell.
    Revision {
        #[command(subcommand)]
        action: RevisionAction,
    },
    /// Applies the sandbox to this process, then replaces it with the adapter (§6.4).
    Spawner {
        /// The cgroup the process joins before the restrictions apply.
        #[arg(long)]
        cgroup: Option<PathBuf>,
        /// Paths the adapter may read and execute.
        #[arg(long = "read")]
        read: Vec<PathBuf>,
        /// Paths the adapter may read and write.
        #[arg(long = "write")]
        write: Vec<PathBuf>,
        /// The adapter binary and its arguments.
        #[arg(required = true, trailing_var_arg = true, allow_hyphen_values = true)]
        command: Vec<String>,
    },
    /// Adapter roles. `fake` exists for the fault-isolation tests only.
    Adapter {
        #[command(subcommand)]
        kind: AdapterKind,
    },
    /// Diagnostics.
    Diag {
        #[command(subcommand)]
        action: DiagAction,
    },
}

#[derive(Debug, Subcommand)]
enum RevisionAction {
    /// Print the content identifier of a revision document (RFC 8785, `id` excluded).
    Hash {
        /// The revision file, strict JSON.
        file: PathBuf,
    },
    /// Stage 1 of the validation (§10.4): envelope, schema and content identifier. Prints the
    /// findings as canonical JSON, and exits with 1 when there is any.
    Validate {
        /// The revision file, strict JSON.
        file: PathBuf,
    },
    /// Verify a signed-plan envelope and print the revision it carries.
    VerifyPlan {
        /// The envelope file, strict JSON.
        file: PathBuf,
        /// The trusted public key, PEM (`SubjectPublicKeyInfo`).
        #[arg(long)]
        public_key: PathBuf,
        /// The identifier of the revision to extract, `revision:sha256:…`.
        #[arg(long)]
        revision: String,
    },
}

#[derive(Debug, Subcommand)]
enum AdapterKind {
    /// A fake adapter that misbehaves on request.
    Fake {
        /// What the fake does.
        #[arg(long, value_enum, default_value_t = Fault::Run)]
        fault: Fault,
        /// For `run`: how long it keeps running, in seconds.
        #[arg(long, default_value_t = 3)]
        seconds: u64,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Fault {
    /// Runs normally until the deadline, then exits with 0.
    Run,
    /// Aborts: the process dies by SIGABRT, as after a fatal runtime error.
    Abort,
    /// Panics (exit code 101).
    Panic,
    /// Allocates memory until the cgroup limit kills it.
    Oom,
    /// Calls a system call outside its allow-list (ptrace), killed with SIGSYS.
    Syscall,
    /// Reads a file that its profile does not allow; exits with 42 when refused.
    ReadForbidden,
}

#[derive(Debug, Subcommand)]
enum DiagAction {
    /// Print the kernel features the sandbox depends on (PM-ADP-005).
    Features,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Cmd::Revision { action } => match action {
            RevisionAction::Hash { file } => revision_hash(&file),
            RevisionAction::Validate { file } => revision_validate(&file),
            RevisionAction::VerifyPlan {
                file,
                public_key,
                revision,
            } => revision_verify_plan(&file, &public_key, &revision),
        },
        Cmd::Spawner {
            cgroup,
            read,
            write,
            command,
        } => spawner(cgroup.as_deref(), read, write, &command),
        Cmd::Adapter { kind } => match kind {
            AdapterKind::Fake { fault, seconds } => fake_adapter(fault, seconds),
        },
        Cmd::Diag { action } => match action {
            DiagAction::Features => {
                println!("{:#?}", features::probe());
                Ok(())
            }
        },
    }
}

fn read_strict(path: &Path) -> Result<serde_json::Value> {
    let text = fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    parse_strict_json(&text).with_context(|| format!("loading {}", path.display()))
}

fn revision_hash(file: &Path) -> Result<()> {
    let document = read_strict(file)?;
    println!("{}", content_id("revision", &document)?);
    Ok(())
}

fn revision_validate(file: &Path) -> Result<()> {
    let document = read_strict(file)?;
    let mut findings = validate_stage1(&document);
    // Stage 2 covers the partition revisions of the P2 subset (§10.3).
    if document.get("kind").and_then(serde_json::Value::as_str) == Some("PartitionRevision") {
        findings.extend(validate_stage2(&document));
        findings.extend(validate_stage3(&document, &powermug_v1()));
        findings.sort();
    }
    let bytes = powermug_revision::canonical::canonical_bytes(&serde_json::to_value(&findings)?)?;
    println!("{}", String::from_utf8(bytes)?);
    if findings.is_empty() {
        Ok(())
    } else {
        std::process::exit(1);
    }
}

fn revision_verify_plan(file: &Path, public_key: &Path, revision: &str) -> Result<()> {
    let envelope = read_strict(file)?;
    let pem = fs::read_to_string(public_key)
        .with_context(|| format!("reading {}", public_key.display()))?;
    let key = VerifyingKey::from_public_key_pem(&pem)
        .context("public key is not an Ed25519 key in PEM form")?;
    let plan = verify_signed_plan(&envelope, &key)?;
    let Ok(selected) = plan.revision(revision) else {
        bail!("revision {revision} is not in the plan exactly once");
    };
    println!("{}", serde_json::to_string(selected)?);
    Ok(())
}

/// The spawner: joins the cgroup, applies the profile, then replaces itself with the adapter.
/// The restrictions survive `exec`; a failure before `exec` exits with status 111.
fn spawner(
    cgroup_dir: Option<&Path>,
    read: Vec<PathBuf>,
    write: Vec<PathBuf>,
    command: &[String],
) -> Result<()> {
    if let Some(dir) = cgroup_dir {
        cgroup::join(dir, std::process::id()).context("joining the cgroup")?;
    }
    let profile = Profile::adapter(read, write);
    if let Err(e) = profile::apply(&profile) {
        eprintln!("spawner: sandbox not applied: {e}");
        std::process::exit(111);
    }
    let (program, args) = command.split_first().context("no adapter command")?;
    // `exec` replaces this process: it returns only on failure.
    let err = Command::new(program).args(args).exec();
    eprintln!("spawner: exec {program} failed: {err}");
    std::process::exit(111);
}

fn fake_adapter(fault: Fault, seconds: u64) -> Result<()> {
    match fault {
        Fault::Run => {
            let deadline = Instant::now() + Duration::from_secs(seconds);
            while Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(50));
            }
            Ok(())
        }
        Fault::Abort => std::process::abort(),
        Fault::Panic => panic!("injected fault: panic in a fake adapter"),
        Fault::Oom => {
            // Each chunk is written, so its pages are resident. The cgroup limit must stop
            // this before the 1 GiB ceiling below is reached.
            let mut chunks: Vec<Vec<u8>> = Vec::new();
            for _ in 0..1024 {
                chunks.push(vec![1u8; 1024 * 1024]);
            }
            std::process::exit(3);
        }
        Fault::Syscall => {
            // ptrace is not in the allow-list: the kernel kills the process with SIGSYS.
            let _ = nix::sys::ptrace::traceme();
            std::process::exit(4);
        }
        Fault::ReadForbidden => match fs::read("/etc/hostname") {
            Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => std::process::exit(42),
            Err(_) => std::process::exit(44),
            Ok(_) => std::process::exit(43),
        },
    }
}
