//! Management API of Powermug: the target API v1 subset, served over mutual TLS
//! (SPECIFICATIONS §11.1, NFR-SEC-001).

pub mod api;
pub mod tls;

pub use api::{router, Api, ApiError};
pub use tls::{server_config, TlsError, TlsListener, HANDSHAKE_TIMEOUT};
