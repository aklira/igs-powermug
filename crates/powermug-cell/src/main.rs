//! `powermug-cell --config <file>`: runs one cell until interrupted.

use std::path::PathBuf;
use std::process::ExitCode;

use powermug_cell::{serve, CellConfig};

fn config_path() -> Result<PathBuf, String> {
    let mut arguments = std::env::args().skip(1);
    match (
        arguments.next().as_deref(),
        arguments.next(),
        arguments.next(),
    ) {
        (Some("--config"), Some(path), None) => Ok(PathBuf::from(path)),
        _ => Err("usage: powermug-cell --config <file>".to_owned()),
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let path = match config_path() {
        Ok(path) => path,
        Err(message) => {
            eprintln!("{message}");
            return ExitCode::from(2);
        }
    };
    let text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(err) => {
            eprintln!("cannot read {}: {err}", path.display());
            return ExitCode::from(2);
        }
    };
    let config = match CellConfig::from_json(&text) {
        Ok(config) => config,
        Err(err) => {
            eprintln!("{}: {err}", path.display());
            return ExitCode::from(2);
        }
    };
    let shutdown = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    match serve(config, shutdown).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("{err}");
            ExitCode::FAILURE
        }
    }
}
