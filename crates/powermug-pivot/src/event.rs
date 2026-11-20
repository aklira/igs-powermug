//! Compact point event for the hot path (PM-PIV-001).
//!
//! The core works on [`PointEvent`] only. It holds no heap data: text values
//! are not representable here, and Protobuf encoding happens at process
//! boundaries and in the journal.

use thiserror::Error;

use crate::quality::Quality;

/// Index of a point, interned when a revision is activated (PM-PIV-001).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PointIndex(pub u32);

/// Value of a point as a tagged union.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Value {
    Bool(bool),
    Int(i64),
    UInt(u64),
    Double(f64),
    /// Enumeration value; the mapping to labels is declared in the revision.
    Enum(u32),
    /// Bitstring of `len` bits, `len` at most 64 (FUN-PIV-002).
    Bits {
        value: u64,
        len: u8,
    },
}

/// Errors when building a [`Value`].
#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum ValueError {
    #[error("bitstring length {0} exceeds 64 bits")]
    BitsTooLong(u8),
    #[error("bitstring value has bits set above its length {0}")]
    BitsOutOfRange(u8),
}

impl Value {
    /// Bitstring of `len` bits. Bits above `len` must be zero.
    ///
    /// # Errors
    ///
    /// Returns [`ValueError`] when `len` exceeds 64 or `value` has bits above `len`.
    pub const fn bits(value: u64, len: u8) -> Result<Self, ValueError> {
        if len > 64 {
            return Err(ValueError::BitsTooLong(len));
        }
        let mask = if len == 64 {
            u64::MAX
        } else {
            (1u64 << len) - 1
        };
        if value & !mask != 0 {
            return Err(ValueError::BitsOutOfRange(len));
        }
        Ok(Self::Bits { value, len })
    }
}

/// Event identifier: a `UUIDv7` (FUN-SAF-001), assigned by the core.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EventId([u8; 16]);

/// Errors when decoding an [`EventId`].
#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum EventIdError {
    #[error("not a UUIDv7: version nibble is {0}")]
    NotVersion7(u8),
    #[error("not an RFC 9562 variant")]
    BadVariant,
}

impl EventId {
    /// Accepts a `UUIDv7` in network byte order and refuses any other version or
    /// variant.
    ///
    /// # Errors
    ///
    /// Returns [`EventIdError`] when the version or the variant is not RFC 9562's.
    pub const fn from_bytes(bytes: [u8; 16]) -> Result<Self, EventIdError> {
        let version = bytes[6] >> 4;
        if version != 7 {
            return Err(EventIdError::NotVersion7(version));
        }
        if bytes[8] & 0b1100_0000 != 0b1000_0000 {
            return Err(EventIdError::BadVariant);
        }
        Ok(Self(bytes))
    }

    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

/// One point event on the hot path (PM-PIV-001).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PointEvent {
    pub point: PointIndex,
    pub value: Value,
    pub quality: Quality,
    /// Nanoseconds since the Unix epoch.
    pub source_time_ns: i64,
    /// Nanoseconds since the Unix epoch.
    pub receive_time_ns: i64,
    /// Nanoseconds since the Unix epoch.
    pub process_time_ns: i64,
    pub event_id: EventId,
    /// Index of the partition revision that mapped the point.
    pub revision_index: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    // Example from RFC 9562, Appendix A, as a UUIDv7.
    const UUID_V7: [u8; 16] = [
        0x01, 0x8f, 0x3c, 0x2a, 0x5b, 0x7e, 0x7c, 0x3d, 0x8a, 0x9f, 0x01, 0x23, 0x45, 0x67, 0x89,
        0xab,
    ];

    #[test]
    fn event_id_accepts_uuid_v7() {
        let id = EventId::from_bytes(UUID_V7).expect("v7");
        assert_eq!(id.as_bytes(), &UUID_V7);
    }

    #[test]
    fn event_id_refuses_other_versions_and_variants() {
        let mut v4 = UUID_V7;
        v4[6] = 0x40;
        assert_eq!(EventId::from_bytes(v4), Err(EventIdError::NotVersion7(4)));

        let mut bad_variant = UUID_V7;
        bad_variant[8] = 0xc0;
        assert_eq!(
            EventId::from_bytes(bad_variant),
            Err(EventIdError::BadVariant)
        );
    }

    #[test]
    fn bitstring_length_and_range_are_checked() {
        assert_eq!(
            Value::bits(0b101, 3),
            Ok(Value::Bits {
                value: 0b101,
                len: 3
            })
        );
        assert_eq!(Value::bits(0b1000, 3), Err(ValueError::BitsOutOfRange(3)));
        assert_eq!(Value::bits(0, 65), Err(ValueError::BitsTooLong(65)));
        assert_eq!(
            Value::bits(u64::MAX, 64),
            Ok(Value::Bits {
                value: u64::MAX,
                len: 64
            })
        );
    }

    #[test]
    fn point_event_is_copy_and_fixed_size() {
        let event = PointEvent {
            point: PointIndex(7),
            value: Value::Int(-3),
            quality: Quality::GOOD,
            source_time_ns: 1,
            receive_time_ns: 2,
            process_time_ns: 3,
            event_id: EventId::from_bytes(UUID_V7).expect("v7"),
            revision_index: 1,
        };
        let copy = event;
        assert_eq!(copy, event);
        // The hot path must stay small enough to move by value between lanes.
        assert!(std::mem::size_of::<PointEvent>() <= 96);
    }
}
