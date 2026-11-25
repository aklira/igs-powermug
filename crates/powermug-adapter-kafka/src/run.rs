//! The North loop of the producer: it subscribes to its flux destination on the core, produces each
//! delivered batch, and acknowledges it once the sink has delivered every record of it.

use std::future::Future;
use std::time::Duration;

use powermug_contract::client::{self, ClientError};
use powermug_contract::handshake::{Offer, Requirement, Version};
use powermug_contract::proto::v1 as pb;
use thiserror::Error;

use crate::record::record_of;
use crate::settings::Settings;
use crate::sink::{DeliveryError, RecordSink};

/// The attempts a batch gets before the producer stops, and the pause between them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    pub attempts: u32,
    pub first_delay: Duration,
    pub max_delay: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            attempts: 8,
            first_delay: Duration::from_millis(200),
            max_delay: Duration::from_secs(5),
        }
    }
}

/// What the producer did before it stopped.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Report {
    pub batches: u64,
    pub records: u64,
}

/// The producer could not carry on.
#[derive(Debug, Error)]
pub enum RunError {
    #[error(transparent)]
    Core(#[from] ClientError),
    #[error("a batch was not delivered after {attempts} attempts: {reason}")]
    Delivery { attempts: u32, reason: String },
}

/// The capabilities the producer declares at the handshake.
#[must_use]
pub fn capabilities(settings: &Settings) -> pb::Capabilities {
    pb::Capabilities {
        destination: settings.destination.clone(),
        api_version: Some(pb::Version { major: 1, minor: 0 }),
        pivot_versions: vec![pb::Version { major: 1, minor: 0 }],
        protocols: vec![pb::Protocol {
            name: "kafka".into(),
            role: pb::Role::Client as i32,
            functions: vec!["publish_events".into()],
        }],
        command_variants: Vec::new(),
        limits: Some(pb::Limits {
            max_batch_events: 1_024,
            max_batch_bytes: 1 << 20,
            max_command_bytes: 16 << 10,
            window_events: 64,
            max_events_per_second: 10_000,
        }),
        crash_recovery: false,
        hardware_dependencies: Vec::new(),
        artifact_sha256: settings.artifact_sha256.clone(),
        protocol_library_version: env!("CARGO_PKG_VERSION").into(),
        settings_schema: "{}".into(),
    }
}

/// The offer the core makes to a North Kafka instance: the instance must provide the publication.
#[must_use]
pub fn offer() -> Offer {
    Offer {
        pivot: Version { major: 1, minor: 0 },
        requirements: vec![Requirement {
            protocol: "kafka".into(),
            role: pb::Role::Client,
            function: "publish_events".into(),
        }],
        command_variants: Vec::new(),
    }
}

/// Runs the producer until `stop` completes or the core ends the stream.
///
/// # Errors
///
/// Returns [`RunError::Core`] when the core cannot be reached or refuses the instance, and
/// [`RunError::Delivery`] when a batch is still not delivered after the attempts of `policy`. A batch
/// that fails is never acknowledged, so the core keeps its cursor behind it.
pub async fn run<S: RecordSink>(
    settings: &Settings,
    sink: &mut S,
    policy: RetryPolicy,
    stop: impl Future<Output = ()>,
) -> Result<Report, RunError> {
    let channel = client::connect(&settings.core_socket).await?;
    client::describe_capabilities(channel.clone(), capabilities(settings)).await?;
    let (acks, mut batches) = client::publish_events(channel).await?;
    tokio::pin!(stop);
    let mut report = Report::default();
    loop {
        let next = tokio::select! {
            () = &mut stop => break,
            next = batches.message() => next.map_err(|status| RunError::Core(ClientError::from(status)))?,
        };
        let Some(batch) = next else {
            break;
        };
        let records: Vec<_> = batch.events.iter().map(record_of).collect();
        deliver(sink, &records, policy).await?;
        acks.send(pb::DeliveryAck {
            sequence: batch.sequence,
        })
        .await
        .map_err(|_| RunError::Core(ClientError::StreamClosed))?;
        report.batches += 1;
        report.records += u64::try_from(records.len()).unwrap_or(u64::MAX);
    }
    Ok(report)
}

async fn deliver<S: RecordSink>(
    sink: &mut S,
    records: &[crate::record::Record],
    policy: RetryPolicy,
) -> Result<(), RunError> {
    let mut delay = policy.first_delay;
    let mut attempt = 1;
    loop {
        match sink.deliver(records).await {
            Ok(()) => return Ok(()),
            Err(DeliveryError::Produce(reason)) if attempt >= policy.attempts => {
                return Err(RunError::Delivery {
                    attempts: attempt,
                    reason,
                });
            }
            Err(_) => {
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(policy.max_delay);
                attempt += 1;
            }
        }
    }
}
