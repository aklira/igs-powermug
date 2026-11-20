//! The adapter's loop: a controlling station on `igs-iec104`, feeding the core
//! over the adapter contract (SPECIFICATIONS §6.2, §7.1, §7.6).
//!
//! The loop connects to the core and makes the handshake, then opens one
//! `StreamEvents` call. Each time the station's data transfer starts, it asks
//! for a general interrogation. Monitoring ASDUs are converted and sent as
//! batches. A lost connection is reported to the core as a lost session, and
//! its return as a restored one.

use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use igs_iec104::client::{Client, ClientConfig, Event};
use igs_iec104::Delivery;
use igs_iec104_codec::elements::Qoi;
use igs_iec104_codec::header::CommonAddress;
use igs_iec104_link::TransferState;
use powermug_contract::client::{self, ClientError, EventStream};
use powermug_contract::proto::v1 as pb;
use powermug_pivot::proto::v1 as pivot;
use uuid::Uuid;

use crate::convert::{asdu as convert_asdu, Context, ConvertError};
use crate::mapping::Mapping;

/// Settings of one adapter instance.
#[derive(Debug, Clone)]
pub struct Options {
    /// The controlled station, to connect to.
    pub station: SocketAddr,
    /// The common address of the station.
    pub common_address: u16,
    /// The socket the core serves this instance on.
    pub core_socket: PathBuf,
    /// The site of the partition, prefix of the point identifiers.
    pub site_id: String,
    /// The session of the core, numbered by the revision.
    pub session_id: u32,
    /// The partition revision the mapping comes from.
    pub revision: u32,
    pub mapping: Mapping,
    /// SHA-256 of the adapter artifact, lowercase hex.
    pub artifact_sha256: String,
    /// Credits of the stream: events in flight before acknowledgements.
    pub window_events: u32,
}

/// What the loop did, returned when it stops.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Report {
    /// Connections made to the station.
    pub connections: u64,
    /// Batches sent to the core.
    pub batches: u64,
    /// Events sent to the core.
    pub events: u64,
    /// Objects of the station that are not in the mapping.
    pub unmapped: u64,
    /// ASDUs whose type the adapter does not convert.
    pub unsupported: u64,
    /// Batches the core rejected.
    pub rejected: u64,
    /// Session changes the core refused.
    pub refused_reports: u64,
}

/// The capabilities the adapter declares at the handshake.
#[must_use]
pub fn capabilities(options: &Options) -> pb::Capabilities {
    pb::Capabilities {
        api_version: Some(pb::Version { major: 1, minor: 0 }),
        pivot_versions: vec![pb::Version { major: 1, minor: 0 }],
        protocols: vec![pb::Protocol {
            name: "iec104".into(),
            role: pb::Role::Client as i32,
            functions: vec!["general_interrogation".into()],
        }],
        command_variants: Vec::new(),
        limits: Some(pb::Limits {
            max_batch_events: 1_024,
            max_batch_bytes: 1 << 20,
            max_command_bytes: 16 << 10,
            window_events: options.window_events,
            max_events_per_second: 10_000,
        }),
        crash_recovery: false,
        hardware_dependencies: Vec::new(),
        artifact_sha256: options.artifact_sha256.clone(),
        protocol_library_version: "0.1.0".into(),
        settings_schema: "{}".into(),
    }
}

fn now_ns() -> i64 {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    i64::try_from(elapsed.as_nanos()).unwrap_or(i64::MAX)
}

fn event_id() -> [u8; 16] {
    *Uuid::now_v7().as_bytes()
}

/// Runs until `stop` completes, or the station client ends.
///
/// # Errors
///
/// Returns [`ClientError`] when the core cannot be reached, refuses the
/// handshake, or refuses the stream.
pub async fn run(options: Options, stop: impl Future<Output = ()>) -> Result<Report, ClientError> {
    let channel = client::connect(&options.core_socket).await?;
    client::describe_capabilities(channel.clone(), capabilities(&options)).await?;
    let mut stream = EventStream::open(channel.clone(), options.window_events).await?;

    let mut station = Client::connect(ClientConfig::new(
        options.station,
        CommonAddress::new(options.common_address),
    ));
    let mut report = Report::default();
    let mut lost = false;
    let mut epoch: u64 = 0;
    let mut sequence: u64 = 0;
    tokio::pin!(stop);

    loop {
        tokio::select! {
            () = &mut stop => break,
            Some(ack) = stream.next_ack() => {
                if ack.status == pb::AckStatus::Rejected as i32 {
                    report.rejected += 1;
                }
            }
            event = station.next_event() => {
                let Some(event) = event else { break };
                match event {
                    Event::Connected => {
                        epoch += 1;
                        report.connections += 1;
                        if lost {
                            lost = false;
                            session_change(&channel, &options, true, "connected", &mut report).await;
                        }
                        station.start_data_transfer().await.ok();
                    }
                    Event::Disconnected(error) | Event::ConnectFailed(error) => {
                        if !lost {
                            lost = true;
                            let reason = error.to_string();
                            session_change(&channel, &options, false, &reason, &mut report).await;
                        }
                    }
                    Event::Delivery(Delivery::Transfer(TransferState::Started)) => {
                        station.interrogate(Qoi::STATION).await.ok();
                    }
                    Event::Delivery(Delivery::Asdu(asdu)) => {
                        let context = Context {
                            site_id: &options.site_id,
                            mapping: &options.mapping,
                            revision: options.revision,
                            received_ns: now_ns(),
                        };
                        match convert_asdu(&asdu, &context, &mut event_id) {
                            Ok(converted) => {
                                report.unmapped += converted.unmapped.len() as u64;
                                if converted.envelopes.is_empty() {
                                    continue;
                                }
                                sequence += 1;
                                let events = converted.envelopes;
                                let count = events.len() as u64;
                                let batch = pb::EventBatch {
                                    source_epoch: epoch,
                                    batch_sequence: sequence,
                                    events,
                                };
                                stream.send(batch).await?;
                                report.batches += 1;
                                report.events += count;
                            }
                            Err(ConvertError::UnsupportedType(_)) => report.unsupported += 1,
                        }
                    }
                    Event::Closed => break,
                    _ => {}
                }
            }
        }
    }
    stream.finish().await?;
    Ok(report)
}

async fn session_change(
    channel: &tonic::transport::Channel,
    options: &Options,
    connected: bool,
    reason: &str,
    report: &mut Report,
) {
    let state = pivot::CommunicationState {
        site_id: options.site_id.clone(),
        session_id: options.session_id.to_string(),
        connected,
        reason: reason.to_owned(),
    };
    if client::report_communication(channel.clone(), state)
        .await
        .is_err()
    {
        report.refused_reports += 1;
    }
}
