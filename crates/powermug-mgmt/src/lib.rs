//! Management API of Powermug: the target API v1 subset, served over mutual TLS
//! (SPECIFICATIONS §11.1, NFR-SEC-001).

pub mod api;
pub mod commands;
pub mod tls;

pub use api::{router, service, Api, ApiError};
pub use commands::{CommandGateway, CommandRequest, GatewayError};
pub use tls::{
    load_trusted_key, server_config, ClientIdentity, KeyError, TlsError, TlsListener,
    HANDSHAKE_TIMEOUT,
};
