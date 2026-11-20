//! The controlled-station side: an `igs-iec104` station that answers the
//! controlling stations from the core's image (SPECIFICATIONS §6.2, FUN-IMG-003).
//!
//! At start the adapter asks the core for the state of its session (the
//! general-interrogation answer), and adds each point to the station's image.
//! Then it follows the events the core commits, and updates the station's image
//! with each of them, so that the controlling stations get spontaneous changes.
//! The station refuses every command: the commands are not in this slice.

use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use igs_iec104::clock;
use igs_iec104::process_image::PointValue;
use igs_iec104::server::{Handler, Operation, Refusal, Server, ServerConfig};
use igs_iec104_codec::elements::{Diq, DoublePoint, Qds, QualityFlags, ShortFloat, Siq, Sva};
use igs_iec104_codec::header::{CommonAddress, InformationObjectAddress};
use powermug_contract::client::{self, ClientError};
use powermug_contract::proto::v1 as pb;
use powermug_pivot::proto::v1 as pivot;
use powermug_pivot::{Detail, Quality, Validity, Value};
use thiserror::Error;

use crate::mapping::Mapping;

/// Settings of the station adapter.
#[derive(Debug, Clone)]
pub struct ServeOptions {
    /// The address the station listens on.
    pub listen: SocketAddr,
    /// The common address of the station.
    pub common_address: u16,
    /// The socket the core serves this instance on.
    pub core_socket: PathBuf,
    /// The session of the core whose points the station serves.
    pub session_id: String,
    /// The objects of the station, mapped to points.
    pub mapping: Mapping,
    /// SHA-256 of the adapter artifact, lowercase hex.
    pub artifact_sha256: String,
}

/// What the station adapter did, returned when it stops.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ServeReport {
    /// Points added to the station's image at start.
    pub points_added: u64,
    /// Changes of points written to the station's image.
    pub updates: u64,
    /// Values the station cannot carry (bitstrings, unsigned or out-of-range numbers).
    pub unsupported: u64,
    /// Events of points that are not in the mapping.
    pub unmapped: u64,
}

/// The station adapter could not run.
#[derive(Debug, Error)]
pub enum ServeError {
    #[error(transparent)]
    Core(#[from] ClientError),
    #[error("the station cannot start: {0}")]
    Station(String),
    #[error("the core ended the event stream: {0}")]
    Stream(String),
}

/// The station refuses every command: this slice does not carry commands.
struct ReadOnly;

impl Handler for ReadOnly {
    fn select(&self, _: &Operation) -> Result<(), Refusal> {
        Err(Refusal::UnknownAddress)
    }

    fn execute(&self, _: &Operation) -> Result<(), Refusal> {
        Err(Refusal::UnknownAddress)
    }
}

/// The capabilities the station adapter declares at the handshake.
#[must_use]
pub fn capabilities(options: &ServeOptions) -> pb::Capabilities {
    pb::Capabilities {
        api_version: Some(pb::Version { major: 1, minor: 0 }),
        pivot_versions: vec![pb::Version { major: 1, minor: 0 }],
        protocols: vec![pb::Protocol {
            name: "iec104".into(),
            role: pb::Role::Server as i32,
            functions: vec!["general_interrogation".into()],
        }],
        command_variants: Vec::new(),
        limits: Some(pb::Limits {
            max_batch_events: 1_024,
            max_batch_bytes: 1 << 20,
            max_command_bytes: 16 << 10,
            window_events: 1,
            max_events_per_second: 10_000,
        }),
        crash_recovery: false,
        hardware_dependencies: Vec::new(),
        artifact_sha256: options.artifact_sha256.clone(),
        protocol_library_version: "0.1.0".into(),
        settings_schema: "{}".into(),
    }
}

/// The station value of a pivot value and quality, or `None` when the station
/// cannot carry it.
#[must_use]
pub fn point_value(value: Value, quality: Quality) -> Option<PointValue> {
    let flags = QualityFlags {
        blocked: quality.is_operator_blocked(),
        substituted: quality.is_substituted(),
        not_topical: quality.validity() == Validity::Questionable,
        invalid: quality.validity() == Validity::Invalid,
    };
    let qds = Qds {
        overflow: quality.has_detail(Detail::Overflow),
        quality: flags,
    };
    match value {
        Value::Bool(on) => Some(PointValue::Single(Siq { on, quality: flags })),
        Value::Enum(code) => {
            let state = match code {
                0 => DoublePoint::IndeterminateOrIntermediate,
                1 => DoublePoint::Off,
                2 => DoublePoint::On,
                3 => DoublePoint::Indeterminate,
                _ => return None,
            };
            Some(PointValue::Double(Diq {
                state,
                quality: flags,
            }))
        }
        Value::Int(number) => i16::try_from(number)
            .ok()
            .map(|number| PointValue::Scaled(Sva::new(number), qds)),
        #[allow(
            clippy::cast_possible_truncation,
            reason = "IEC 60870-5-104 short floats are IEEE 754 single precision"
        )]
        Value::Double(number) => Some(PointValue::Float(ShortFloat::from_f32(number as f32), qds)),
        Value::UInt(_) | Value::Bits { .. } => None,
    }
}

fn station_value(envelope: &pivot::Envelope) -> Option<PointValue> {
    let value = Value::try_from(envelope.value.clone()?).ok()?;
    let quality = Quality::try_from(envelope.quality.clone()?).ok()?;
    point_value(value, quality)
}

/// Serves the station until `stop` completes, or the core ends the event stream.
///
/// # Errors
///
/// Returns [`ServeError::Core`] when the core refuses the handshake or the
/// state request, [`ServeError::Station`] when the station cannot bind, and
/// [`ServeError::Stream`] when the core ends the event stream with an error.
pub async fn serve(
    options: ServeOptions,
    stop: impl Future<Output = ()>,
) -> Result<ServeReport, ServeError> {
    let channel = client::connect(&options.core_socket).await?;
    client::describe_capabilities(channel.clone(), capabilities(&options)).await?;
    let state = client::interrogate_state(channel.clone(), &options.session_id).await?;
    let mut events = client::subscribe_events(channel).await?;

    let station = Server::bind(
        ServerConfig::new(options.listen, CommonAddress::new(options.common_address)),
        ReadOnly,
    )
    .await
    .map_err(|err| ServeError::Station(err.to_string()))?;
    let handle = station.handle();
    let (station_stop, station_stopped) = tokio::sync::oneshot::channel::<()>();
    let station_task = tokio::spawn(async move {
        let _ = station
            .run_until(
                async {
                    let _ = station_stopped.await;
                },
                Duration::ZERO,
            )
            .await;
    });

    let mut report = ServeReport::default();
    for envelope in &state {
        let Some((common, address)) = options.mapping.object_of(&envelope.point_id) else {
            report.unmapped += 1;
            continue;
        };
        if common != options.common_address {
            report.unmapped += 1;
            continue;
        }
        let (Some(object), Some(value)) = (
            InformationObjectAddress::new(address),
            station_value(envelope),
        ) else {
            report.unsupported += 1;
            continue;
        };
        if handle.add_point(object, value, false).is_ok() {
            report.points_added += 1;
        } else {
            report.unsupported += 1;
        }
    }

    tokio::pin!(stop);
    let outcome = loop {
        tokio::select! {
            () = &mut stop => break Ok(()),
            message = events.message() => match message {
                Ok(Some(envelope)) => {
                    let Some((common, address)) = options.mapping.object_of(&envelope.point_id) else {
                        report.unmapped += 1;
                        continue;
                    };
                    if common != options.common_address {
                        report.unmapped += 1;
                        continue;
                    }
                    let (Some(object), Some(value)) = (
                        InformationObjectAddress::new(address),
                        station_value(&envelope),
                    ) else {
                        report.unsupported += 1;
                        continue;
                    };
                    if let Ok(time) = clock::now_utc() {
                        if handle.update(object, value, time).is_ok() {
                            report.updates += 1;
                        } else {
                            report.unsupported += 1;
                        }
                    }
                }
                Ok(None) => break Ok(()),
                Err(status) => break Err(ServeError::Stream(status.to_string())),
            },
        }
    };
    let _ = station_stop.send(());
    let _ = station_task.await;
    outcome.map(|()| report)
}
