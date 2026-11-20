//! Message bounds (PM-ADP-002).
//!
//! The byte bound is checked on the raw frame, before decoding (FUN-ACQ-007).
//! The event count is checked after decoding, because prost decodes a whole
//! message at once.

use thiserror::Error;

/// Events per batch.
pub const MAX_BATCH_EVENTS: usize = 1_024;
/// Bytes per batch: 1 MiB.
pub const MAX_BATCH_BYTES: usize = 1 << 20;
/// Bytes per command: 16 KiB.
pub const MAX_COMMAND_BYTES: usize = 16 << 10;
/// Bytes per handshake: 256 KiB.
pub const MAX_HANDSHAKE_BYTES: usize = 256 << 10;

/// Kinds of messages on the contract, each with its own bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageKind {
    Handshake,
    Command,
    EventBatch,
}

impl MessageKind {
    #[must_use]
    pub const fn max_bytes(self) -> usize {
        match self {
            Self::Handshake => MAX_HANDSHAKE_BYTES,
            Self::Command => MAX_COMMAND_BYTES,
            Self::EventBatch => MAX_BATCH_BYTES,
        }
    }
}

/// A message exceeds a bound.
#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum BoundError {
    #[error("{kind:?} of {len} bytes exceeds {max} bytes")]
    TooLarge {
        kind: MessageKind,
        len: usize,
        max: usize,
    },
    #[error("batch of {0} events exceeds {MAX_BATCH_EVENTS}")]
    TooManyEvents(usize),
}

/// Refuses a frame larger than the bound of its kind. Call it before decoding.
///
/// # Errors
///
/// Returns [`BoundError::TooLarge`] when `len` exceeds the bound of `kind`.
pub const fn check_frame_len(kind: MessageKind, len: usize) -> Result<(), BoundError> {
    let max = kind.max_bytes();
    if len > max {
        Err(BoundError::TooLarge { kind, len, max })
    } else {
        Ok(())
    }
}

/// Refuses a batch with more events than the bound. Call it after decoding.
///
/// # Errors
///
/// Returns [`BoundError::TooManyEvents`] when `count` exceeds the bound.
pub const fn check_batch_events(count: usize) -> Result<(), BoundError> {
    if count > MAX_BATCH_EVENTS {
        Err(BoundError::TooManyEvents(count))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_at_the_bound_is_accepted_and_one_more_byte_is_refused() {
        for kind in [
            MessageKind::Handshake,
            MessageKind::Command,
            MessageKind::EventBatch,
        ] {
            let max = kind.max_bytes();
            assert_eq!(check_frame_len(kind, max), Ok(()));
            assert_eq!(
                check_frame_len(kind, max + 1),
                Err(BoundError::TooLarge {
                    kind,
                    len: max + 1,
                    max
                })
            );
        }
    }

    #[test]
    fn command_bound_is_tighter_than_batch_bound() {
        assert!(check_frame_len(MessageKind::Command, MAX_COMMAND_BYTES + 1).is_err());
        assert!(check_frame_len(MessageKind::EventBatch, MAX_COMMAND_BYTES + 1).is_ok());
    }

    #[test]
    fn batch_event_count_is_bounded() {
        assert_eq!(check_batch_events(MAX_BATCH_EVENTS), Ok(()));
        assert_eq!(
            check_batch_events(MAX_BATCH_EVENTS + 1),
            Err(BoundError::TooManyEvents(MAX_BATCH_EVENTS + 1))
        );
    }
}
