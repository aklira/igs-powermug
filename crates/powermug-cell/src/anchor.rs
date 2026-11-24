//! Anchoring of the command audit head outside the critical store (NFR-SEC-008). The head is the
//! hash of the last record of the chain: a copy kept outside the store shows that records were not
//! removed from the end of the chain. The anchor is written at start, every period, and at stop.

use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use powermug_command::audit::to_hex;
use powermug_command::CommandClient;
use tokio::sync::watch;

/// Writes the anchor to `path`: the head and the time, as one JSON object. The file is written
/// beside it and renamed over it, so a reader sees the old anchor or the new one, never half.
///
/// # Errors
///
/// Returns the error of the write, the flush to disk, or the rename.
pub fn write_anchor(path: &Path, head: &[u8; 32], at_ns: i64) -> io::Result<()> {
    let mut temporary = path.as_os_str().to_owned();
    temporary.push(".tmp");
    let temporary = PathBuf::from(temporary);
    {
        let mut file = std::fs::File::create(&temporary)?;
        let line = serde_json::json!({ "head": to_hex(head), "at_ns": at_ns });
        writeln!(file, "{line}")?;
        file.sync_all()?;
    }
    std::fs::rename(&temporary, path)
}

/// Anchors the head of the chain of `client` to `path` at start, every `every`, and when
/// `stopped` changes. A failure is reported on the standard error and the anchoring goes on.
pub async fn run(
    client: CommandClient,
    path: PathBuf,
    every: Duration,
    mut stopped: watch::Receiver<bool>,
) {
    let mut ticker = tokio::time::interval(every);
    loop {
        tokio::select! {
            _ = ticker.tick() => anchor_once(&client, &path).await,
            _ = stopped.changed() => break,
        }
    }
    anchor_once(&client, &path).await;
}

async fn anchor_once(client: &CommandClient, path: &Path) {
    let client = client.clone();
    let path = path.to_owned();
    let outcome = tokio::task::spawn_blocking(move || -> Result<(), String> {
        let head = client.audit_head().map_err(|err| err.to_string())?;
        write_anchor(&path, &head, now_ns()).map_err(|err| err.to_string())
    })
    .await;
    match outcome {
        Ok(Ok(())) => {}
        Ok(Err(reason)) => eprintln!("powermug-cell: the audit head is not anchored: {reason}"),
        Err(err) => eprintln!("powermug-cell: the audit anchoring stopped: {err}"),
    }
}

fn now_ns() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_nanos()),
    )
    .unwrap_or(i64::MAX)
}
