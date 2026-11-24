//! Mutual TLS for the management interface (NFR-SEC-001, §11.1): TLS 1.3 with rustls,
//! and a client certificate that must chain to the configured client CA.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::connect_info::Connected;
use axum::serve::IncomingStream;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::{RootCertStore, ServerConfig, ServerConnection};
use thiserror::Error;
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::server::TlsStream;
use tokio_rustls::TlsAcceptor;

/// A client that does not finish its handshake in this time is dropped.
pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

/// The TLS material could not be used.
#[derive(Debug, Error)]
pub enum TlsError {
    #[error("cannot read the PEM material: {0}")]
    Pem(String),
    #[error("the TLS configuration is refused: {0}")]
    Config(String),
}

/// The server configuration: TLS 1.3 only, and a client certificate is required and
/// must chain to `client_ca_pem`.
///
/// # Errors
///
/// Returns [`TlsError`] when the PEM material cannot be read or the configuration is
/// refused.
pub fn server_config(
    certificate_pem: &[u8],
    key_pem: &[u8],
    client_ca_pem: &[u8],
) -> Result<Arc<ServerConfig>, TlsError> {
    let chain = CertificateDer::pem_slice_iter(certificate_pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| TlsError::Pem(err.to_string()))?;
    let key =
        PrivateKeyDer::from_pem_slice(key_pem).map_err(|err| TlsError::Pem(err.to_string()))?;
    let mut roots = RootCertStore::empty();
    for certificate in CertificateDer::pem_slice_iter(client_ca_pem) {
        let certificate = certificate.map_err(|err| TlsError::Pem(err.to_string()))?;
        roots
            .add(certificate)
            .map_err(|err| TlsError::Config(err.to_string()))?;
    }
    let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
        .build()
        .map_err(|err| TlsError::Config(err.to_string()))?;
    let config = ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
        .with_client_cert_verifier(verifier)
        .with_single_cert(chain, key)
        .map_err(|err| TlsError::Config(err.to_string()))?;
    Ok(Arc::new(config))
}

/// A TCP listener that completes the TLS handshake before handing a connection over.
/// A handshake that fails or stalls is dropped, and the listener keeps accepting.
pub struct TlsListener {
    tcp: TcpListener,
    acceptor: TlsAcceptor,
}

impl TlsListener {
    #[must_use]
    pub fn new(tcp: TcpListener, config: Arc<ServerConfig>) -> Self {
        Self {
            tcp,
            acceptor: TlsAcceptor::from(config),
        }
    }
}

impl std::fmt::Debug for TlsListener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsListener")
            .field("tcp", &self.tcp)
            .finish_non_exhaustive()
    }
}

/// The identity of the client of a connection: the common name of the certificate it
/// presented in the handshake. A request reads it through `ConnectInfo<ClientIdentity>`.
/// The common name is `None` when the certificate has none, or when no certificate was
/// presented (the client verifier requires one, so that is not expected).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientIdentity(Option<String>);

impl ClientIdentity {
    /// The common name of the client's certificate.
    #[must_use]
    pub fn common_name(&self) -> Option<&str> {
        self.0.as_deref()
    }

    fn from_connection(connection: &ServerConnection) -> Self {
        let common_name = connection
            .peer_certificates()
            .and_then(|chain| chain.first())
            .and_then(|leaf| common_name_of(leaf.as_ref()));
        Self(common_name)
    }
}

/// The first common name of a DER certificate's subject.
fn common_name_of(der: &[u8]) -> Option<String> {
    let (_, certificate) = x509_parser::parse_x509_certificate(der).ok()?;
    let name = certificate
        .subject()
        .iter_common_name()
        .next()?
        .as_str()
        .ok()?;
    Some(name.to_owned())
}

impl Connected<IncomingStream<'_, TlsListener>> for ClientIdentity {
    fn connect_info(stream: IncomingStream<'_, TlsListener>) -> Self {
        let (_, connection) = stream.io().get_ref();
        Self::from_connection(connection)
    }
}

impl axum::serve::Listener for TlsListener {
    type Io = TlsStream<TcpStream>;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        loop {
            let Ok((tcp, address)) = self.tcp.accept().await else {
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            };
            if let Ok(Ok(tls)) =
                tokio::time::timeout(HANDSHAKE_TIMEOUT, self.acceptor.accept(tcp)).await
            {
                return (tls, address);
            }
        }
    }

    fn local_addr(&self) -> io::Result<Self::Addr> {
        self.tcp.local_addr()
    }
}

/// The trusted public key of signed plans, read from a PEM file (SPKI, Ed25519).
///
/// # Errors
///
/// Returns [`KeyError`] when the file cannot be read or is not an Ed25519 public key.
pub fn load_trusted_key(path: &std::path::Path) -> Result<ed25519_dalek::VerifyingKey, KeyError> {
    use ed25519_dalek::pkcs8::DecodePublicKey;
    let pem = std::fs::read_to_string(path).map_err(|err| KeyError::Read(err.to_string()))?;
    ed25519_dalek::VerifyingKey::from_public_key_pem(&pem)
        .map_err(|err| KeyError::Pem(err.to_string()))
}

/// The trusted key could not be loaded.
#[derive(Debug, Error)]
pub enum KeyError {
    #[error("cannot read the trusted key: {0}")]
    Read(String),
    #[error("the trusted key is not an Ed25519 public key in PEM form: {0}")]
    Pem(String),
}
