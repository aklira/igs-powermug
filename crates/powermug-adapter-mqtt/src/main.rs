//! `powermug-mqtt-client --settings <file>`: the North MQTT 5 instance of the cell. The cell writes the
//! settings file, then starts the client under the spawner, as it starts the South adapters.

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::Parser;
use powermug_adapter_mqtt::{ClientError, MqttSink, Settings};
use powermug_north::{run, Report, RetryPolicy};

#[derive(Debug, Parser)]
#[command(
    name = "powermug-mqtt-client",
    version,
    about = "North MQTT 5 client of Powermug"
)]
struct Cli {
    /// The settings file written by the cell.
    #[arg(long)]
    settings: PathBuf,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match execute(&cli.settings) {
        Ok(report) => {
            eprintln!("mqtt client stopped: {report:?}");
            ExitCode::SUCCESS
        }
        Err(err) => {
            eprintln!("mqtt client failed: {err}");
            ExitCode::FAILURE
        }
    }
}

fn execute(settings_path: &Path) -> Result<Report, ClientError> {
    let settings = Settings::load(settings_path)?;
    let mut sink = MqttSink::new(&settings);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    // The client runs until the core ends its stream, or until a batch cannot be delivered.
    let report = runtime.block_on(run(
        &settings.subscription(),
        &mut sink,
        RetryPolicy::default(),
        std::future::pending::<()>(),
    ))?;
    Ok(report)
}
