//! Adapter side of `powermug.adapter.v1`: the gRPC client over a Unix socket.
//!
//! The acknowledgements are read in their own task, so the sender never waits
//! on a stream the core is blocked writing to. The sender waits only for
//! credits.

use std::path::Path;

use hyper_util::rt::TokioIo;
use thiserror::Error;
use tokio::net::UnixStream;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Channel, Endpoint, Uri};
use tonic::Status;
use tower::service_fn;

use crate::credit::{CreditError, CreditWindow};
use crate::proto::v1::{
    adapter_control_client::AdapterControlClient, adapter_data_client::AdapterDataClient, AckBatch,
    Capabilities, EventBatch, HandshakeResponse,
};

/// The adapter could not complete a call.
#[derive(Debug, Error)]
pub enum ClientError {
    #[error("cannot connect to the core socket: {0}")]
    Connect(#[from] tonic::transport::Error),
    #[error("core refused the call: {0}")]
    Status(#[from] Status),
    #[error("batch of {events} events exceeds the window of {window}")]
    BatchExceedsWindow { events: u32, window: u32 },
    #[error(transparent)]
    Credit(#[from] CreditError),
    #[error("the core closed the stream before all batches were acknowledged")]
    StreamClosed,
}

/// Connects to the core's socket.
///
/// # Errors
///
/// Returns [`ClientError::Connect`] when the socket cannot be opened.
pub async fn connect(socket: impl AsRef<Path>) -> Result<Channel, ClientError> {
    let socket = socket.as_ref().to_path_buf();
    // The authority is not used: the connector always opens the socket.
    let channel = Endpoint::try_from("http://powermug.local")?
        .connect_with_connector(service_fn(move |_: Uri| {
            let socket = socket.clone();
            async move { UnixStream::connect(socket).await.map(TokioIo::new) }
        }))
        .await?;
    Ok(channel)
}

/// Sends the handshake and returns the core's answer.
///
/// # Errors
///
/// Returns [`ClientError::Status`] when the core refuses the handshake.
pub async fn describe_capabilities(
    channel: Channel,
    capabilities: Capabilities,
) -> Result<HandshakeResponse, ClientError> {
    let response = AdapterControlClient::new(channel)
        .describe_capabilities(capabilities)
        .await?;
    Ok(response.into_inner())
}

/// Sends `batches` on one stream, never more events than `window` allows
/// before credits come back. Returns every acknowledgement, in arrival order.
///
/// # Errors
///
/// Returns [`ClientError::BatchExceedsWindow`] for a batch larger than the
/// window, [`ClientError::Status`] when the core ends the stream with an error,
/// and [`ClientError::StreamClosed`] when the core closes it early.
pub async fn stream_events(
    channel: Channel,
    batches: Vec<EventBatch>,
    window: u32,
) -> Result<Vec<AckBatch>, ClientError> {
    let (tx, rx) = mpsc::channel::<EventBatch>(1);
    let response = AdapterDataClient::new(channel)
        .stream_events(ReceiverStream::new(rx))
        .await?;
    let mut acks = response.into_inner();

    let (credit_tx, mut credit_rx) = mpsc::unbounded_channel::<u32>();
    let reader = tokio::spawn(async move {
        let mut received = Vec::new();
        while let Some(ack) = acks.message().await? {
            // A closed credit channel only means the sender has finished.
            let _ = credit_tx.send(ack.credits_granted);
            received.push(ack);
        }
        Ok::<_, Status>(received)
    });

    let sent = send_within_window(&tx, &mut credit_rx, batches, window).await;
    drop(tx);

    let received = reader.await.map_err(|_| ClientError::StreamClosed)??;
    sent?;
    Ok(received)
}

async fn send_within_window(
    tx: &mpsc::Sender<EventBatch>,
    credits: &mut mpsc::UnboundedReceiver<u32>,
    batches: Vec<EventBatch>,
    window: u32,
) -> Result<(), ClientError> {
    let mut available = CreditWindow::new(window);
    available.grant(window)?;
    for batch in batches {
        let events = u32::try_from(batch.events.len()).unwrap_or(u32::MAX);
        if events > window {
            return Err(ClientError::BatchExceedsWindow { events, window });
        }
        while available.consume(events).is_err() {
            let returned = credits.recv().await.ok_or(ClientError::StreamClosed)?;
            available.grant(returned)?;
        }
        tx.send(batch)
            .await
            .map_err(|_| ClientError::StreamClosed)?;
    }
    Ok(())
}
