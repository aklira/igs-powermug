//! Normalized quality (SPECIFICATIONS §5.2).
//!
//! The quality is a 16-bit field. Bits 0 and 1 hold the validity; code 3 is
//! reserved and refused. The other bits are the detail set, the source and the
//! Powermug flags. Bits 14 and 15 are unused and refused.

use thiserror::Error;

/// Validity of a value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Validity {
    Good,
    Invalid,
    Questionable,
}

impl Validity {
    const fn code(self) -> u16 {
        match self {
            Self::Good => 0,
            Self::Invalid => 1,
            Self::Questionable => 2,
        }
    }
}

/// Members of the detail set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Detail {
    Overflow,
    OutOfRange,
    BadReference,
    Oscillatory,
    Failure,
    OldData,
    Inconsistent,
    Inaccurate,
}

impl Detail {
    /// Every member of the detail set, in bit order.
    pub const ALL: [Self; 8] = [
        Self::Overflow,
        Self::OutOfRange,
        Self::BadReference,
        Self::Oscillatory,
        Self::Failure,
        Self::OldData,
        Self::Inconsistent,
        Self::Inaccurate,
    ];

    const fn bit(self) -> u16 {
        let shift: u32 = match self {
            Self::Overflow => 2,
            Self::OutOfRange => 3,
            Self::BadReference => 4,
            Self::Oscillatory => 5,
            Self::Failure => 6,
            Self::OldData => 7,
            Self::Inconsistent => 8,
            Self::Inaccurate => 9,
        };
        1 << shift
    }
}

const VALIDITY_MASK: u16 = 0b11;
const VALIDITY_RESERVED: u16 = 3;
const SUBSTITUTED: u16 = 1 << 10;
const TEST: u16 = 1 << 11;
const OPERATOR_BLOCKED: u16 = 1 << 12;
const DISABLED: u16 = 1 << 13;
const KNOWN_MASK: u16 = (1 << 14) - 1;

// IEC 60870-5-104 quality descriptor (QDS) bits.
const QDS_OV: u8 = 0x01;
const QDS_BL: u8 = 0x10;
const QDS_SB: u8 = 0x20;
const QDS_NT: u8 = 0x40;
const QDS_IV: u8 = 0x80;

/// Errors when decoding a raw quality field.
#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum QualityError {
    #[error("validity code 3 is reserved")]
    ReservedValidity,
    #[error("unknown quality bits {0:#06x}")]
    UnknownBits(u16),
}

/// Normalized quality of a point value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Quality(u16);

/// The IEC 104 QDS byte that carries a quality, and whether the mapping lost
/// information (FUN-PIV-006: a lossy mapping must be counted, not silent).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Iec104Qds {
    pub qds: u8,
    /// `false` when at least one part of the normalized quality has no IEC 104
    /// representation and was dropped.
    pub lossless: bool,
}

impl Quality {
    /// Good quality, no detail, process source, not test, not blocked.
    pub const GOOD: Self = Self(0);

    /// Quality with the given validity and no flag set.
    #[must_use]
    pub const fn new(validity: Validity) -> Self {
        Self(validity.code())
    }

    /// Decodes a raw quality field, refusing reserved and unknown bits.
    ///
    /// # Errors
    ///
    /// Returns [`QualityError`] for validity code 3 or for bits 14 and 15.
    pub fn from_bits(bits: u16) -> Result<Self, QualityError> {
        let unknown = bits & !KNOWN_MASK;
        if unknown != 0 {
            return Err(QualityError::UnknownBits(unknown));
        }
        if bits & VALIDITY_MASK == VALIDITY_RESERVED {
            return Err(QualityError::ReservedValidity);
        }
        Ok(Self(bits))
    }

    /// The raw quality field.
    #[must_use]
    pub const fn bits(self) -> u16 {
        self.0
    }

    #[must_use]
    pub const fn validity(self) -> Validity {
        match self.0 & VALIDITY_MASK {
            1 => Validity::Invalid,
            2 => Validity::Questionable,
            _ => Validity::Good,
        }
    }

    #[must_use]
    pub const fn with_validity(self, validity: Validity) -> Self {
        Self((self.0 & !VALIDITY_MASK) | validity.code())
    }

    #[must_use]
    pub const fn has_detail(self, detail: Detail) -> bool {
        self.0 & detail.bit() != 0
    }

    #[must_use]
    pub const fn with_detail(self, detail: Detail) -> Self {
        Self(self.0 | detail.bit())
    }

    /// Details set on this quality, in the order of [`Detail::ALL`].
    pub fn details(self) -> impl Iterator<Item = Detail> {
        Detail::ALL.into_iter().filter(move |d| self.has_detail(*d))
    }

    /// `source = substituted` when set, `source = process` otherwise.
    #[must_use]
    pub const fn is_substituted(self) -> bool {
        self.0 & SUBSTITUTED != 0
    }

    #[must_use]
    pub const fn with_substituted(self, on: bool) -> Self {
        self.with_bit(SUBSTITUTED, on)
    }

    #[must_use]
    pub const fn is_test(self) -> bool {
        self.0 & TEST != 0
    }

    #[must_use]
    pub const fn with_test(self, on: bool) -> Self {
        self.with_bit(TEST, on)
    }

    #[must_use]
    pub const fn is_operator_blocked(self) -> bool {
        self.0 & OPERATOR_BLOCKED != 0
    }

    #[must_use]
    pub const fn with_operator_blocked(self, on: bool) -> Self {
        self.with_bit(OPERATOR_BLOCKED, on)
    }

    /// Powermug extension: the producing processor is disabled (§8.6).
    #[must_use]
    pub const fn is_disabled(self) -> bool {
        self.0 & DISABLED != 0
    }

    #[must_use]
    pub const fn with_disabled(self, on: bool) -> Self {
        self.with_bit(DISABLED, on)
    }

    /// Default mapping to the IEC 104 QDS (§5.2 table).
    ///
    /// `OV`, `BL`, `SB`, `NT` and `IV` are set from the corresponding parts.
    /// `old_data` is carried by `NT` only when the validity is questionable,
    /// as in the table. Everything else has no QDS bit and makes the mapping
    /// lossy: the other details, `test` and `disabled`.
    #[must_use]
    pub const fn to_iec104_qds(self) -> Iec104Qds {
        let mut qds = 0u8;
        match self.validity() {
            Validity::Good => {}
            Validity::Invalid => qds |= QDS_IV,
            Validity::Questionable => qds |= QDS_NT,
        }
        if self.has_detail(Detail::Overflow) {
            qds |= QDS_OV;
        }
        if self.is_substituted() {
            qds |= QDS_SB;
        }
        if self.is_operator_blocked() {
            qds |= QDS_BL;
        }

        let old_data_carried = matches!(self.validity(), Validity::Questionable);
        let lost_detail = self.0 & DETAIL_MASK_WITHOUT_QDS != 0
            || (self.has_detail(Detail::OldData) && !old_data_carried);
        let lossless = !lost_detail && !self.is_test() && !self.is_disabled();

        Iec104Qds { qds, lossless }
    }

    const fn with_bit(self, bit: u16, on: bool) -> Self {
        if on {
            Self(self.0 | bit)
        } else {
            Self(self.0 & !bit)
        }
    }
}

/// Detail bits with no QDS representation. `overflow` has a QDS bit and
/// `old_data` is handled separately, so neither is in this mask.
const DETAIL_MASK_WITHOUT_QDS: u16 = Detail::OutOfRange.bit()
    | Detail::BadReference.bit()
    | Detail::Oscillatory.bit()
    | Detail::Failure.bit()
    | Detail::Inconsistent.bit()
    | Detail::Inaccurate.bit();

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_good() {
        let q = Quality::default();
        assert_eq!(q, Quality::GOOD);
        assert_eq!(q.validity(), Validity::Good);
        assert!(
            !q.is_substituted() && !q.is_test() && !q.is_operator_blocked() && !q.is_disabled()
        );
    }

    #[test]
    fn from_bits_refuses_reserved_validity_and_unknown_bits() {
        assert_eq!(
            Quality::from_bits(0b11),
            Err(QualityError::ReservedValidity)
        );
        assert_eq!(
            Quality::from_bits(1 << 14),
            Err(QualityError::UnknownBits(1 << 14))
        );
        assert_eq!(
            Quality::from_bits(1 << 15),
            Err(QualityError::UnknownBits(1 << 15))
        );
    }

    #[test]
    fn every_valid_pattern_round_trips() {
        for bits in 0..=KNOWN_MASK {
            if bits & VALIDITY_MASK == VALIDITY_RESERVED {
                continue;
            }
            let q = Quality::from_bits(bits).expect("valid pattern");
            assert_eq!(q.bits(), bits);
        }
    }

    #[test]
    fn with_validity_keeps_flags() {
        let q = Quality::new(Validity::Good)
            .with_detail(Detail::Overflow)
            .with_test(true);
        let changed = q.with_validity(Validity::Questionable);
        assert_eq!(changed.validity(), Validity::Questionable);
        assert!(changed.has_detail(Detail::Overflow));
        assert!(changed.is_test());
    }

    #[test]
    fn flags_can_be_set_and_cleared() {
        let q = Quality::GOOD
            .with_substituted(true)
            .with_operator_blocked(true);
        assert!(q.is_substituted() && q.is_operator_blocked());
        let cleared = q.with_substituted(false);
        assert!(!cleared.is_substituted() && cleared.is_operator_blocked());
    }

    #[test]
    fn iec104_default_mapping_rows() {
        let row = |q: Quality| q.to_iec104_qds();

        assert_eq!(
            row(Quality::GOOD),
            Iec104Qds {
                qds: 0x00,
                lossless: true
            }
        );

        assert_eq!(
            row(Quality::new(Validity::Invalid)),
            Iec104Qds {
                qds: 0x80,
                lossless: true
            }
        );

        let questionable_old = Quality::new(Validity::Questionable).with_detail(Detail::OldData);
        assert_eq!(
            row(questionable_old),
            Iec104Qds {
                qds: 0x40,
                lossless: true
            }
        );

        assert_eq!(
            row(Quality::new(Validity::Questionable)),
            Iec104Qds {
                qds: 0x40,
                lossless: true
            }
        );

        assert_eq!(
            row(Quality::GOOD.with_substituted(true)),
            Iec104Qds {
                qds: 0x20,
                lossless: true
            }
        );

        assert_eq!(
            row(Quality::GOOD.with_detail(Detail::Overflow)),
            Iec104Qds {
                qds: 0x01,
                lossless: true
            }
        );

        assert_eq!(
            row(Quality::GOOD.with_operator_blocked(true)),
            Iec104Qds {
                qds: 0x10,
                lossless: true
            }
        );
    }

    #[test]
    fn iec104_mapping_reports_loss() {
        // No QDS bit for these: the mapping must say so.
        assert!(
            !Quality::GOOD
                .with_detail(Detail::OutOfRange)
                .to_iec104_qds()
                .lossless
        );
        assert!(!Quality::GOOD.with_test(true).to_iec104_qds().lossless);
        assert!(!Quality::GOOD.with_disabled(true).to_iec104_qds().lossless);
        // old_data is only carried by NT under questionable validity.
        let invalid_old = Quality::new(Validity::Invalid).with_detail(Detail::OldData);
        assert!(!invalid_old.to_iec104_qds().lossless);
        // Invalid combined with a supported flag keeps both bits.
        let invalid_overflow = Quality::new(Validity::Invalid).with_detail(Detail::Overflow);
        assert_eq!(
            invalid_overflow.to_iec104_qds(),
            Iec104Qds {
                qds: 0x81,
                lossless: true
            }
        );
    }
}
