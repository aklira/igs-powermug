//! The identity of the issuer of a command, as the North protocol carries it ([GW] 11.6,
//! FUN-CMD-003).
//!
//! The authorization can be no finer than the identity the protocol gives. The granularity of
//! an identity is ordered: a grant that requires a granularity is satisfied only by an identity
//! at least that fine. The journal records the identity a command actually had, and its level.

use std::net::SocketAddr;

/// How finely an identity names its issuer, from the coarsest to the finest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Granularity {
    /// The link: IP address, port and common address. Usable on isolated networks only.
    Link,
    /// The upstream system, by its client certificate (IEC 62351-3).
    Upstream,
    /// The client, by its certificate (MQTT 5).
    Client,
    /// The application, by its certificate (OPC UA).
    Application,
    /// The user, by a token or the protocol's user identity.
    User,
}

impl Granularity {
    /// The name stored in the journal.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Granularity::Link => "link",
            Granularity::Upstream => "upstream",
            Granularity::Client => "client",
            Granularity::Application => "application",
            Granularity::User => "user",
        }
    }

    /// The granularity named by a journal code, if any.
    #[must_use]
    pub fn from_code(code: &str) -> Option<Self> {
        [
            Granularity::Link,
            Granularity::Upstream,
            Granularity::Client,
            Granularity::Application,
            Granularity::User,
        ]
        .into_iter()
        .find(|level| level.code() == code)
    }
}

/// The identity of an issuer: what the protocol gave, and its granularity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identity {
    pub subject: String,
    pub granularity: Granularity,
}

/// IEC 104 without TLS: the address and port of the link, and the common address of the ASDUs.
#[must_use]
pub fn iec104_plain(peer: SocketAddr, common_address: u16) -> Identity {
    Identity {
        subject: format!("{peer} ca={common_address}"),
        granularity: Granularity::Link,
    }
}

/// IEC 104 with TLS (IEC 62351-3): the client certificate, per upstream system.
#[must_use]
pub fn iec104_tls(certificate_subject: &str) -> Identity {
    Identity {
        subject: certificate_subject.to_owned(),
        granularity: Granularity::Upstream,
    }
}

/// MQTT 5: the client certificate, per client.
#[must_use]
pub fn mqtt5(certificate_subject: &str) -> Identity {
    Identity {
        subject: certificate_subject.to_owned(),
        granularity: Granularity::Client,
    }
}

/// OPC UA: the application certificate, or the user when the protocol provides one.
#[must_use]
pub fn opc_ua(certificate_subject: &str, user: Option<&str>) -> Identity {
    match user {
        Some(user) => Identity {
            subject: user.to_owned(),
            granularity: Granularity::User,
        },
        None => Identity {
            subject: certificate_subject.to_owned(),
            granularity: Granularity::Application,
        },
    }
}

/// REST: the mTLS client certificate and the user token; per user when the token is present,
/// otherwise per client.
#[must_use]
pub fn rest(certificate_subject: &str, user: Option<&str>) -> Identity {
    match user {
        Some(user) => Identity {
            subject: user.to_owned(),
            granularity: Granularity::User,
        },
        None => Identity {
            subject: certificate_subject.to_owned(),
            granularity: Granularity::Client,
        },
    }
}
