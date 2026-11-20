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
    Capabilities, CommunicationAck, EventBatch, HandshakeResponse,
};
use powermug_pivot::proto::v1::CommunicationState;

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

/// Reports that a session was lost or is back. Returns once the core has
/// committed the change.
///
/// # Errors
///
/// Returns [`ClientError::Status`] when the core refuses the change.
pub async fn report_communication(
    channel: Channel,
    state: CommunicationState,
) -> Result<CommunicationAck, ClientError> {
    let response = AdapterDataClient::new(channel)
        .report_communication(state)
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

/// An open `StreamEvents` call, fed batch by batch. The adapter sends a batch
/// when the window allows it, and reads the acknowledgements as they come.
#[derive(Debug)]
pub struct EventStream {
    batches: mpsc::Sender<EventBatch>,
    credits: mpsc::UnboundedReceiver<u32>,
    acks: mpsc::UnboundedReceiver<AckBatch>,
    available: CreditWindow,
    window: u32,
    reader: tokio::task::JoinHandle<Result<(), Status>>,
}

impl EventStream {
    /// Opens the stream with a window of `window` events.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Status`] when the core refuses the stream, for
    /// example before the handshake.
    pub async fn open(channel: Channel, window: u32) -> Result<Self, ClientError> {
        let (batches, rx) = mpsc::channel::<EventBatch>(1);
        let response = AdapterDataClient::new(channel)
            .stream_events(ReceiverStream::new(rx))
            .await?;
        let mut inbound = response.into_inner();
        let (credit_tx, credits) = mpsc::unbounded_channel::<u32>();
        let (ack_tx, acks) = mpsc::unbounded_channel::<AckBatch>();
        let reader = tokio::spawn(async move {
            while let Some(ack) = inbound.message().await? {
                let _ = credit_tx.send(ack.credits_granted);
                let _ = ack_tx.send(ack);
            }
            Ok::<_, Status>(())
        });
        let mut available = CreditWindow::new(window);
        available.grant(window)?;
        Ok(Self {
            batches,
            credits,
            acks,
            available,
            window,
            reader,
        })
    }

    /// Sends a batch, waiting for the credits it needs.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::BatchExceedsWindow`] for a batch larger than the
    /// window, and [`ClientError::StreamClosed`] when the core ends the stream.
    pub async fn send(&mut self, batch: EventBatch) -> Result<(), ClientError> {
        let events = u32::try_from(batch.events.len()).unwrap_or(u32::MAX);
        if events > self.window {
            return Err(ClientError::BatchExceedsWindow {
                events,
                window: self.window,
            });
        }
        while self.available.consume(events).is_err() {
            let returned = self.credits.recv().await.ok_or(ClientError::StreamClosed)?;
            self.available.grant(returned)?;
        }
        self.batches
            .send(batch)
            .await
            .map_err(|_| ClientError::StreamClosed)
    }

    /// The next acknowledgement, or `None` when the stream has ended.
    pub async fn next_ack(&mut self) -> Option<AckBatch> {
        self.acks.recv().await
    }

    /// Ends the stream and returns the status the core ended it with, if any.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Status`] when the core ended the stream with an error.
    pub async fn finish(self) -> Result<(), ClientError> {
        drop(self.batches);
        self.reader.await.map_err(|_| ClientError::StreamClosed)??;
        Ok(())
    }
}
