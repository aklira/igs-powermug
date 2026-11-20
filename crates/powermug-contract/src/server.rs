//! Serves the core's side of the contract on a Unix domain socket.

use std::future::Future;

use tokio::net::UnixListener;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::Server;

use crate::bounds::{MAX_BATCH_BYTES, MAX_HANDSHAKE_BYTES};
use crate::proto::v1::{
    adapter_control_server::AdapterControlServer, adapter_data_server::AdapterDataServer,
};
use crate::service::{CoreConfig, CoreService};

/// Serves one adapter instance on `listener` until `shutdown` completes.
///
/// Each message is bounded before it is decoded: tonic refuses a frame above
/// `max_decoding_message_size` from its length prefix (PM-ADP-002).
///
/// # Errors
///
/// Returns the transport error when the server stops abnormally.
pub async fn serve_instance(
    config: CoreConfig,
    listener: UnixListener,
    shutdown: impl Future<Output = ()>,
) -> Result<(), tonic::transport::Error> {
    let service = CoreService::new(config);
    Server::builder()
        .add_service(
            AdapterControlServer::new(service.clone())
                .max_decoding_message_size(MAX_HANDSHAKE_BYTES),
        )
        .add_service(AdapterDataServer::new(service).max_decoding_message_size(MAX_BATCH_BYTES))
        .serve_with_incoming_shutdown(UnixListenerStream::new(listener), shutdown)
        .await
}
