//! `powermug-kafka-producer --settings <file>`: the North Kafka instance of the cell. The cell writes
//! the settings file, then starts the producer under the spawner, as it starts the South adapters.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use powermug_adapter_kafka::{run, KafkaSink, ProducerError, RetryPolicy, Settings};

#[derive(Debug, Parser)]
#[command(
    name = "powermug-kafka-producer",
    version,
    about = "North Kafka producer of Powermug"
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
            eprintln!("kafka producer stopped: {report:?}");
            ExitCode::SUCCESS
        }
        Err(err) => {
            eprintln!("kafka producer failed: {err}");
            ExitCode::FAILURE
        }
    }
}

fn execute(
    settings_path: &std::path::Path,
) -> Result<powermug_adapter_kafka::Report, ProducerError> {
    let settings = Settings::load(settings_path)?;
    let mut sink = KafkaSink::new(&settings.brokers, &settings.topic)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    // The producer runs until the core ends its stream, or until a batch cannot be delivered.
    let report = runtime.block_on(run(
        &settings,
        &mut sink,
        RetryPolicy::default(),
        std::future::pending::<()>(),
    ))?;
    Ok(report)
}
