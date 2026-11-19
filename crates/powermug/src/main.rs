//! The `powermug` binary. Roles grow with the phases; P1 provides the revision tools.

#![forbid(unsafe_code)]

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use ed25519_dalek::pkcs8::DecodePublicKey;
use ed25519_dalek::VerifyingKey;
use powermug_revision::canonical::content_id;
use powermug_revision::{parse_strict_json, verify_signed_plan};

#[derive(Debug, Parser)]
#[command(name = "powermug", version, about = "Powermug gateway")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Tools on revisions. They run offline and never touch a cell.
    Revision {
        #[command(subcommand)]
        action: RevisionAction,
    },
}

#[derive(Debug, Subcommand)]
enum RevisionAction {
    /// Print the content identifier of a revision document (RFC 8785, `id` excluded).
    Hash {
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

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Revision { action } => match action {
            RevisionAction::Hash { file } => revision_hash(&file),
            RevisionAction::VerifyPlan {
                file,
                public_key,
                revision,
            } => revision_verify_plan(&file, &public_key, &revision),
        },
    }
}

fn read_strict(path: &Path) -> Result<serde_json::Value> {
    let text = fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    parse_strict_json(&text).with_context(|| format!("loading {}", path.display()))
}

fn revision_hash(file: &Path) -> Result<()> {
    let document = read_strict(file)?;
    let id = content_id("revision", &document)?;
    println!("{id}");
    Ok(())
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
