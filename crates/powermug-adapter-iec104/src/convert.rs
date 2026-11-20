//! From an ASDU to pivot envelopes (§5.2 for the quality, §9.4 for the matrix).
//!
//! Each object of a monitoring ASDU becomes one envelope, when its address is
//! mapped. Objects without a point are counted in [`Converted::unmapped`]; they
//! are not dropped silently. Type identifications this adapter does not
//! convert are refused as a whole.
//!
//! The fidelity of the quality, from IEC 104 to the normalized quality:
//! - `IV` gives `invalid`; `NT` gives `questionable` (the detail `old_data` is
//!   not set, since NT covers every case of "not updated in time");
//! - `SB` gives `substituted`; `BL` gives `operator_blocked`; `OV` gives the
//!   detail `overflow`.
//!
//! Monitoring types without a time tag carry no source time: the envelope takes
//! the reception time as its source time, and the core stamps the process time.

use igs_iec104_codec::asdu::{Asdu, Body, Objects};
use igs_iec104_codec::elements::{Diq, DoublePoint, Nva, Qds, QualityFlags, ShortFloat, Siq, Sva};
use powermug_pivot::proto::v1 as pb;
use powermug_pivot::{Detail, Quality, Validity, Value};
use thiserror::Error;

use crate::mapping::Mapping;

/// What a conversion needs from the adapter.
#[derive(Debug, Clone, Copy)]
pub struct Context<'a> {
    pub site_id: &'a str,
    pub mapping: &'a Mapping,
    /// The partition revision the mapping came from.
    pub revision: u32,
    /// Reception time of the ASDU, in nanoseconds since the Unix epoch.
    pub received_ns: i64,
}

/// An object of the ASDU that has no point in the mapping.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Unmapped {
    pub common_address: u16,
    pub address: u32,
}

/// The result of converting one ASDU.
#[derive(Debug, Default, PartialEq)]
pub struct Converted {
    pub envelopes: Vec<pb::Envelope>,
    pub unmapped: Vec<Unmapped>,
}

/// The ASDU cannot be converted by this adapter.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ConvertError {
    #[error("type identification {0} is not converted by this adapter")]
    UnsupportedType(String),
}

/// Converts one ASDU. `next_id` gives each envelope its event id.
///
/// # Errors
///
/// Returns [`ConvertError::UnsupportedType`] for a type outside the monitoring
/// types this adapter converts; no envelope is then produced.
pub fn asdu(
    asdu: &Asdu,
    context: &Context<'_>,
    next_id: &mut dyn FnMut() -> [u8; 16],
) -> Result<Converted, ConvertError> {
    let common_address = asdu.common_address.value();
    let mut emitter = Emitter {
        context,
        common_address,
        next_id,
        converted: Converted::default(),
    };
    match &asdu.body {
        Body::M_SP_NA_1(objects) => emitter.emit(single(objects, |siq: &Siq| {
            (Value::Bool(siq.on), quality(siq.quality, false))
        })),
        Body::M_DP_NA_1(objects) => emitter.emit(single(objects, |diq: &Diq| {
            (
                Value::Enum(double_code(diq.state)),
                quality(diq.quality, false),
            )
        })),
        Body::M_ME_NA_1(objects) => emitter.emit(single(objects, |(value, qds): &(Nva, Qds)| {
            (
                Value::Double(f64::from(value.as_f32())),
                quality(qds.quality, qds.overflow),
            )
        })),
        Body::M_ME_NB_1(objects) => emitter.emit(single(objects, |(value, qds): &(Sva, Qds)| {
            (
                Value::Int(i64::from(value.value())),
                quality(qds.quality, qds.overflow),
            )
        })),
        Body::M_ME_NC_1(objects) => {
            emitter.emit(single(objects, |(value, qds): &(ShortFloat, Qds)| {
                (
                    Value::Double(f64::from(value.as_f32())),
                    quality(qds.quality, qds.overflow),
                )
            }));
        }
        other => {
            return Err(ConvertError::UnsupportedType(format!(
                "{:?}",
                other.type_id()
            )))
        }
    }
    Ok(emitter.converted)
}

/// Each object of `objects` with its address, as the (address, value) pairs.
fn single<'a, T>(
    objects: &'a Objects<T>,
    to_value: impl Fn(&T) -> (Value, Quality) + 'a,
) -> impl Iterator<Item = (u32, Value, Quality)> + 'a {
    let pairs: Vec<(u32, &'a T)> = match objects {
        Objects::Individual(items) => items
            .iter()
            .map(|item| (item.address.value(), &item.value))
            .collect(),
        Objects::Sequence { address, values } => values
            .iter()
            .enumerate()
            .filter_map(|(index, value)| {
                let offset = u32::try_from(index).ok()?;
                Some((address.value().checked_add(offset)?, value))
            })
            .collect(),
    };
    pairs.into_iter().map(move |(address, value)| {
        let (typed, quality) = to_value(value);
        (address, typed, quality)
    })
}

/// The normalized quality of IEC 104 quality flags and the overflow bit.
fn quality(flags: QualityFlags, overflow: bool) -> Quality {
    let validity = if flags.invalid {
        Validity::Invalid
    } else if flags.not_topical {
        Validity::Questionable
    } else {
        Validity::Good
    };
    let mut quality = Quality::new(validity);
    if flags.substituted {
        quality = quality.with_substituted(true);
    }
    if flags.blocked {
        quality = quality.with_operator_blocked(true);
    }
    if overflow {
        quality = quality.with_detail(Detail::Overflow);
    }
    quality
}

/// The code of a double-point state, as the revision names it.
fn double_code(state: DoublePoint) -> u32 {
    match state {
        DoublePoint::IndeterminateOrIntermediate => 0,
        DoublePoint::Off => 1,
        DoublePoint::On => 2,
        DoublePoint::Indeterminate => 3,
    }
}

struct Emitter<'a, 'b> {
    context: &'a Context<'a>,
    common_address: u16,
    next_id: &'b mut dyn FnMut() -> [u8; 16],
    converted: Converted,
}

impl Emitter<'_, '_> {
    fn emit(&mut self, items: impl Iterator<Item = (u32, Value, Quality)>) {
        for (address, value, quality) in items {
            match self.context.mapping.point_of(self.common_address, address) {
                Some(point_id) => {
                    let envelope = pb::Envelope {
                        event_id: (self.next_id)().to_vec(),
                        site_id: self.context.site_id.to_owned(),
                        point_id: point_id.to_owned(),
                        value: Some(value.into()),
                        quality: Some(quality.into()),
                        source_time_ns: self.context.received_ns,
                        receive_time_ns: self.context.received_ns,
                        mapping_revision: self.context.revision.to_string(),
                        ..Default::default()
                    };
                    self.converted.envelopes.push(envelope);
                }
                None => self.converted.unmapped.push(Unmapped {
                    common_address: self.common_address,
                    address,
                }),
            }
        }
    }
}
