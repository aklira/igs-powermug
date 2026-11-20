//! Conversions between the hot-path types and the messages of
//! `powermug.pivot.v1`.
//!
//! Decoding is strict. A message that leaves validity, source or value unset,
//! or that carries an unknown enum number, is refused with a typed error and
//! never silently defaulted (PM-ADP-001 applies the same rule to the contract).

use thiserror::Error;

use crate::event::{Value, ValueError};
use crate::proto::v1 as pb;
use crate::quality::{Detail, Quality, Validity};

/// Upper bound of a text value, in bytes (FUN-PIV-002).
pub const MAX_TEXT_BYTES: usize = 256;

/// Errors when converting a Protobuf message to a hot-path type.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ConvertError {
    #[error("validity is not set")]
    UnspecifiedValidity,
    #[error("source is not set")]
    UnspecifiedSource,
    #[error("unknown validity number {0}")]
    UnknownValidity(i32),
    #[error("unknown source number {0}")]
    UnknownSource(i32),
    #[error("unknown or unset detail number {0}")]
    UnknownDetail(i32),
    #[error("value is not set")]
    EmptyValue,
    #[error("text values are not representable on the hot path")]
    TextOnHotPath,
    #[error("text value of {0} bytes exceeds {MAX_TEXT_BYTES}")]
    TextTooLong(usize),
    #[error("bitstring length {0} exceeds 64 bits")]
    BitsTooLong(u32),
    #[error(transparent)]
    Value(#[from] ValueError),
}

const fn validity_to_pb(validity: Validity) -> pb::Validity {
    match validity {
        Validity::Good => pb::Validity::Good,
        Validity::Invalid => pb::Validity::Invalid,
        Validity::Questionable => pb::Validity::Questionable,
    }
}

const fn detail_to_pb(detail: Detail) -> pb::Detail {
    match detail {
        Detail::Overflow => pb::Detail::Overflow,
        Detail::OutOfRange => pb::Detail::OutOfRange,
        Detail::BadReference => pb::Detail::BadReference,
        Detail::Oscillatory => pb::Detail::Oscillatory,
        Detail::Failure => pb::Detail::Failure,
        Detail::OldData => pb::Detail::OldData,
        Detail::Inconsistent => pb::Detail::Inconsistent,
        Detail::Inaccurate => pb::Detail::Inaccurate,
    }
}

const fn detail_from_pb(detail: pb::Detail) -> Option<Detail> {
    match detail {
        pb::Detail::Unspecified => None,
        pb::Detail::Overflow => Some(Detail::Overflow),
        pb::Detail::OutOfRange => Some(Detail::OutOfRange),
        pb::Detail::BadReference => Some(Detail::BadReference),
        pb::Detail::Oscillatory => Some(Detail::Oscillatory),
        pb::Detail::Failure => Some(Detail::Failure),
        pb::Detail::OldData => Some(Detail::OldData),
        pb::Detail::Inconsistent => Some(Detail::Inconsistent),
        pb::Detail::Inaccurate => Some(Detail::Inaccurate),
    }
}

impl From<Quality> for pb::Quality {
    fn from(quality: Quality) -> Self {
        Self {
            validity: validity_to_pb(quality.validity()) as i32,
            details: quality.details().map(|d| detail_to_pb(d) as i32).collect(),
            source: if quality.is_substituted() {
                pb::Source::Substituted
            } else {
                pb::Source::Process
            } as i32,
            test: quality.is_test(),
            operator_blocked: quality.is_operator_blocked(),
            disabled: quality.is_disabled(),
        }
    }
}

impl TryFrom<pb::Quality> for Quality {
    type Error = ConvertError;

    fn try_from(message: pb::Quality) -> Result<Self, Self::Error> {
        let validity = match pb::Validity::try_from(message.validity) {
            Ok(pb::Validity::Unspecified) => return Err(ConvertError::UnspecifiedValidity),
            Ok(pb::Validity::Good) => Validity::Good,
            Ok(pb::Validity::Invalid) => Validity::Invalid,
            Ok(pb::Validity::Questionable) => Validity::Questionable,
            Err(_) => return Err(ConvertError::UnknownValidity(message.validity)),
        };

        let mut quality = Self::new(validity);
        for number in message.details {
            let detail = pb::Detail::try_from(number)
                .ok()
                .and_then(detail_from_pb)
                .ok_or(ConvertError::UnknownDetail(number))?;
            quality = quality.with_detail(detail);
        }

        quality = match pb::Source::try_from(message.source) {
            Ok(pb::Source::Unspecified) => return Err(ConvertError::UnspecifiedSource),
            Ok(pb::Source::Process) => quality,
            Ok(pb::Source::Substituted) => quality.with_substituted(true),
            Err(_) => return Err(ConvertError::UnknownSource(message.source)),
        };

        Ok(quality
            .with_test(message.test)
            .with_operator_blocked(message.operator_blocked)
            .with_disabled(message.disabled))
    }
}

impl From<Value> for pb::Value {
    fn from(value: Value) -> Self {
        use pb::value::Kind;
        let kind = match value {
            Value::Bool(b) => Kind::BoolValue(b),
            Value::Int(i) => Kind::IntValue(i),
            Value::UInt(u) => Kind::UintValue(u),
            Value::Double(d) => Kind::DoubleValue(d),
            Value::Enum(e) => Kind::EnumValue(e),
            Value::Bits { value, len } => Kind::BitsValue(pb::Bits {
                value,
                len: u32::from(len),
            }),
        };
        Self { kind: Some(kind) }
    }
}

impl TryFrom<pb::Value> for Value {
    type Error = ConvertError;

    fn try_from(message: pb::Value) -> Result<Self, Self::Error> {
        use pb::value::Kind;
        match message.kind.ok_or(ConvertError::EmptyValue)? {
            Kind::BoolValue(b) => Ok(Self::Bool(b)),
            Kind::IntValue(i) => Ok(Self::Int(i)),
            Kind::UintValue(u) => Ok(Self::UInt(u)),
            Kind::DoubleValue(d) => Ok(Self::Double(d)),
            Kind::EnumValue(e) => Ok(Self::Enum(e)),
            Kind::BitsValue(bits) => {
                let len = u8::try_from(bits.len)
                    .ok()
                    .filter(|len| *len <= 64)
                    .ok_or(ConvertError::BitsTooLong(bits.len))?;
                Ok(Self::bits(bits.value, len)?)
            }
            Kind::TextValue(text) => {
                if text.len() > MAX_TEXT_BYTES {
                    Err(ConvertError::TextTooLong(text.len()))
                } else {
                    Err(ConvertError::TextOnHotPath)
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use prost::Message;

    use super::*;

    fn wire_round_trip(quality: Quality) -> Quality {
        let message: pb::Quality = quality.into();
        let bytes = message.encode_to_vec();
        let decoded = pb::Quality::decode(bytes.as_slice()).expect("decodes");
        Quality::try_from(decoded).expect("valid")
    }

    #[test]
    fn every_valid_quality_survives_protobuf() {
        for bits in 0..0x4000u16 {
            let Ok(quality) = Quality::from_bits(bits) else {
                continue;
            };
            assert_eq!(wire_round_trip(quality), quality, "bits {bits:#06x}");
        }
    }

    #[test]
    fn quality_refuses_unset_validity_and_source() {
        let unset = pb::Quality {
            validity: pb::Validity::Unspecified as i32,
            source: pb::Source::Process as i32,
            ..Default::default()
        };
        assert_eq!(
            Quality::try_from(unset),
            Err(ConvertError::UnspecifiedValidity)
        );

        let no_source = pb::Quality {
            validity: pb::Validity::Good as i32,
            ..Default::default()
        };
        assert_eq!(
            Quality::try_from(no_source),
            Err(ConvertError::UnspecifiedSource)
        );
    }

    #[test]
    fn quality_refuses_unknown_numbers() {
        let bad_validity = pb::Quality {
            validity: 42,
            source: pb::Source::Process as i32,
            ..Default::default()
        };
        assert_eq!(
            Quality::try_from(bad_validity),
            Err(ConvertError::UnknownValidity(42))
        );

        let bad_detail = pb::Quality {
            validity: pb::Validity::Good as i32,
            details: vec![pb::Detail::Unspecified as i32],
            source: pb::Source::Process as i32,
            ..Default::default()
        };
        assert_eq!(
            Quality::try_from(bad_detail),
            Err(ConvertError::UnknownDetail(0))
        );
    }

    #[test]
    fn value_round_trips_for_scalar_cases() {
        let cases = [
            Value::Bool(true),
            Value::Int(i64::MIN),
            Value::UInt(u64::MAX),
            Value::Double(-0.5),
            Value::Enum(3),
            Value::bits(0b101, 3).expect("valid"),
            Value::bits(u64::MAX, 64).expect("valid"),
        ];
        for value in cases {
            let message: pb::Value = value.into();
            let bytes = message.encode_to_vec();
            let decoded = pb::Value::decode(bytes.as_slice()).expect("decodes");
            assert_eq!(Value::try_from(decoded), Ok(value));
        }
    }

    #[test]
    fn value_refuses_empty_text_and_oversize_bits() {
        assert_eq!(
            Value::try_from(pb::Value { kind: None }),
            Err(ConvertError::EmptyValue)
        );

        let text = pb::Value {
            kind: Some(pb::value::Kind::TextValue("ok".into())),
        };
        assert_eq!(Value::try_from(text), Err(ConvertError::TextOnHotPath));

        let long_text = pb::Value {
            kind: Some(pb::value::Kind::TextValue("x".repeat(MAX_TEXT_BYTES + 1))),
        };
        assert_eq!(
            Value::try_from(long_text),
            Err(ConvertError::TextTooLong(MAX_TEXT_BYTES + 1))
        );

        let too_long_bits = pb::Value {
            kind: Some(pb::value::Kind::BitsValue(pb::Bits { value: 0, len: 65 })),
        };
        assert_eq!(
            Value::try_from(too_long_bits),
            Err(ConvertError::BitsTooLong(65))
        );

        let stray_bits = pb::Value {
            kind: Some(pb::value::Kind::BitsValue(pb::Bits {
                value: 0b1000,
                len: 3,
            })),
        };
        assert_eq!(
            Value::try_from(stray_bits),
            Err(ConvertError::Value(ValueError::BitsOutOfRange(3)))
        );
    }
}
