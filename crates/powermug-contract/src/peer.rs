//! Identity of the peer on the Unix socket (§8.4 of [GW], PM-ADP-007).
//!
//! The core reads the peer's credentials with `SO_PEERCRED` and checks them
//! here. The system call is made by the transport. This module holds the
//! decision only, so that it can be tested without a socket.

use thiserror::Error;

/// Credentials the core read from the socket.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeerCredentials {
    pub uid: u32,
    pub pid: i32,
}

/// Identity of the instance the core spawned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExpectedPeer {
    pub uid: u32,
    pub pid: i32,
}

/// The peer is not the instance the core spawned.
#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum PeerError {
    #[error("peer uid {actual} is not the instance uid {expected}")]
    UidMismatch { expected: u32, actual: u32 },
    #[error("peer pid {actual} is not the instance pid {expected}")]
    PidMismatch { expected: i32, actual: i32 },
}

/// Both the uid and the pid must match (§8.4 of [GW]).
///
/// # Errors
///
/// Returns [`PeerError`] for the first of uid or pid that differs.
pub const fn authenticate(
    expected: ExpectedPeer,
    actual: PeerCredentials,
) -> Result<(), PeerError> {
    if expected.uid != actual.uid {
        return Err(PeerError::UidMismatch {
            expected: expected.uid,
            actual: actual.uid,
        });
    }
    if expected.pid != actual.pid {
        return Err(PeerError::PidMismatch {
            expected: expected.pid,
            actual: actual.pid,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const EXPECTED: ExpectedPeer = ExpectedPeer {
        uid: 10_000,
        pid: 4_242,
    };

    #[test]
    fn matching_uid_and_pid_pass() {
        let actual = PeerCredentials {
            uid: 10_000,
            pid: 4_242,
        };
        assert_eq!(authenticate(EXPECTED, actual), Ok(()));
    }

    #[test]
    fn wrong_uid_is_refused_even_with_the_right_pid() {
        let actual = PeerCredentials {
            uid: 1_000,
            pid: 4_242,
        };
        assert_eq!(
            authenticate(EXPECTED, actual),
            Err(PeerError::UidMismatch {
                expected: 10_000,
                actual: 1_000
            })
        );
    }

    #[test]
    fn wrong_pid_is_refused_with_the_right_uid() {
        let actual = PeerCredentials {
            uid: 10_000,
            pid: 99,
        };
        assert_eq!(
            authenticate(EXPECTED, actual),
            Err(PeerError::PidMismatch {
                expected: 4_242,
                actual: 99
            })
        );
    }
}
