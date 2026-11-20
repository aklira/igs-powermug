//! Core side of `powermug.adapter.v1`: the services the core serves to one
//! adapter instance.
//!
//! One socket serves one instance (SPECIFICATIONS §6.1). Every RPC checks the
//! peer's credentials, read by tonic from `SO_PEERCRED`. A `StreamEvents` call
//! is refused until the instance has been accepted by `DescribeCapabilities`.
//!
//! Each accepted batch gets `RECEIVED`, then either `DURABLE` once the sink has
//! committed it, or `REJECTED` with the reason. Its credits return with that
//! last acknowledgement, so a refused batch does not shrink the window.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex, PoisonError};

use tokio::sync::{mpsc, Notify};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::server::UdsConnectInfo;
use tonic::{Request, Response, Status, Streaming};

use crate::bounds;
use crate::credit::CreditWindow;
use crate::handshake::{self, Offer, API_VERSION};
use crate::peer::{self, ExpectedPeer, PeerCredentials};
use crate::proto::v1::{
    adapter_control_server::AdapterControl, adapter_data_server::AdapterData, AckBatch, AckStatus,
    Capabilities, CommunicationAck, DeliveryAck, DeliveryBatch, EventBatch, HandshakeResponse,
    HealthAck, HealthReport, InterrogateRequest,
};
use crate::sink::{BatchSink, Durability, Refusal};
use powermug_pivot::proto::v1::CommunicationState;

/// Batches a North subscriber may hold unacknowledged.
pub const PUBLISH_WINDOW: usize = 256;
/// Events per published batch, at most.
pub const PUBLISH_BATCH: usize = 64;

/// What the core requires of the instance it spawned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoreConfig {
    pub offer: Offer,
    pub expected_peer: ExpectedPeer,
}

/// State of the instance once its handshake is accepted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Accepted {
    window_events: u32,
}

/// The core's service for one adapter instance.
///
/// The sink is locked for the duration of one commit only. It is a blocking
/// call run on the async task; a dedicated writer thread replaces it when the
/// group commit arrives.
#[derive(Clone)]
pub struct CoreService {
    config: Arc<CoreConfig>,
    accepted: Arc<Mutex<Option<Accepted>>>,
    sink: Arc<Mutex<Box<dyn BatchSink>>>,
    /// Notified on every accepted health report, if the owner watches them.
    heartbeat: Option<Arc<Notify>>,
}

impl CoreService {
    #[must_use]
    pub fn new(config: CoreConfig, sink: Box<dyn BatchSink>) -> Self {
        Self {
            config: Arc::new(config),
            accepted: Arc::new(Mutex::new(None)),
            sink: Arc::new(Mutex::new(sink)),
            heartbeat: None,
        }
    }

    /// Notifies `heartbeat` on every health report of the instance (§6.3).
    #[must_use]
    pub fn with_heartbeat(mut self, heartbeat: Arc<Notify>) -> Self {
        self.heartbeat = Some(heartbeat);
        self
    }

    fn accepted(&self) -> Option<Accepted> {
        *self.accepted.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn set_accepted(&self, accepted: Accepted) {
        *self.accepted.lock().unwrap_or_else(PoisonError::into_inner) = Some(accepted);
    }
}

impl std::fmt::Debug for CoreService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CoreService")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

/// Checks the peer of a request against the instance the core spawned.
fn authenticate<T>(request: &Request<T>, expected: ExpectedPeer) -> Result<(), Status> {
    let info = request
        .extensions()
        .get::<UdsConnectInfo>()
        .ok_or_else(|| Status::unauthenticated("no connection information"))?;
    let cred = info
        .peer_cred
        .ok_or_else(|| Status::unauthenticated("no peer credentials on the socket"))?;
    let pid = cred
        .pid()
        .ok_or_else(|| Status::unauthenticated("no peer pid on the socket"))?;
    peer::authenticate(
        expected,
        PeerCredentials {
            uid: cred.uid(),
            pid,
        },
    )
    .map_err(|err| Status::permission_denied(err.to_string()))
}

fn version(major: u32, minor: u32) -> crate::proto::v1::Version {
    crate::proto::v1::Version { major, minor }
}

#[tonic::async_trait]
impl AdapterControl for CoreService {
    async fn describe_capabilities(
        &self,
        request: Request<Capabilities>,
    ) -> Result<Response<HandshakeResponse>, Status> {
        authenticate(&request, self.config.expected_peer)?;
        let capabilities = request.into_inner();
        handshake::check(&capabilities, &self.config.offer)
            .map_err(|err| Status::failed_precondition(err.to_string()))?;

        let window_events = capabilities
            .limits
            .as_ref()
            .map_or(0, |limits| limits.window_events);
        self.set_accepted(Accepted { window_events });

        Ok(Response::new(HandshakeResponse {
            api_version: Some(version(API_VERSION.major, API_VERSION.minor)),
            pivot_version: Some(version(
                self.config.offer.pivot.major,
                self.config.offer.pivot.minor,
            )),
        }))
    }

    async fn report_health(
        &self,
        request: Request<HealthReport>,
    ) -> Result<Response<HealthAck>, Status> {
        authenticate(&request, self.config.expected_peer)?;
        self.accepted().ok_or_else(|| {
            Status::failed_precondition("no accepted handshake for this instance")
        })?;
        if let Some(heartbeat) = &self.heartbeat {
            heartbeat.notify_one();
        }
        Ok(Response::new(HealthAck {}))
    }
}

fn ack(batch_sequence: u64, status: AckStatus, credits_granted: u32, reason: &str) -> AckBatch {
    AckBatch {
        batch_sequence,
        status: status as i32,
        credits_granted,
        reason: reason.to_owned(),
    }
}

fn commit(sink: &Mutex<Box<dyn BatchSink>>, batch: &EventBatch) -> Result<Durability, Refusal> {
    let mut sink = sink.lock().unwrap_or_else(PoisonError::into_inner);
    sink.commit(batch)
}

#[tonic::async_trait]
impl AdapterData for CoreService {
    type StreamEventsStream = ReceiverStream<Result<AckBatch, Status>>;
    type PublishEventsStream = ReceiverStream<Result<DeliveryBatch, Status>>;
    type InterrogateStateStream =
        tokio_stream::Iter<std::vec::IntoIter<Result<powermug_pivot::proto::v1::Envelope, Status>>>;

    async fn interrogate_state(
        &self,
        request: Request<InterrogateRequest>,
    ) -> Result<Response<Self::InterrogateStateStream>, Status> {
        authenticate(&request, self.config.expected_peer)?;
        self.accepted().ok_or_else(|| {
            Status::failed_precondition("no accepted handshake for this instance")
        })?;
        let session = request.into_inner().session_id;
        let envelopes = {
            let mut sink = self.sink.lock().unwrap_or_else(PoisonError::into_inner);
            sink.snapshot(&session)
                .map_err(|refusal| Status::invalid_argument(refusal.reason))?
        };
        let items: Vec<Result<_, Status>> = envelopes.into_iter().map(Ok).collect();
        Ok(Response::new(tokio_stream::iter(items)))
    }

    async fn publish_events(
        &self,
        request: Request<Streaming<DeliveryAck>>,
    ) -> Result<Response<Self::PublishEventsStream>, Status> {
        authenticate(&request, self.config.expected_peer)?;
        self.accepted().ok_or_else(|| {
            Status::failed_precondition("no accepted handshake for this instance")
        })?;
        let mut acks = request.into_inner();
        let mut events = {
            let mut sink = self.sink.lock().unwrap_or_else(PoisonError::into_inner);
            sink.subscribe()
        };
        let (tx, rx) = mpsc::channel(PUBLISH_WINDOW);
        tokio::spawn(async move {
            let mut next_sequence = 1u64;
            let mut unacknowledged = BTreeSet::new();
            loop {
                let may_send = unacknowledged.len() < PUBLISH_WINDOW;
                tokio::select! {
                    ack = acks.message() => match ack {
                        Ok(Some(ack)) => {
                            unacknowledged.remove(&ack.sequence);
                        }
                        Ok(None) | Err(_) => return,
                    },
                    event = events.recv(), if may_send => {
                        let first = match event {
                            Ok(envelope) => envelope,
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                                // A lost event is reported, never skipped silently.
                                let _ = tx
                                    .send(Err(Status::data_loss(format!(
                                        "the subscriber fell behind: {skipped} events were skipped"
                                    ))))
                                    .await;
                                return;
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                        };
                        let mut batch = vec![first];
                        while batch.len() < PUBLISH_BATCH {
                            match events.try_recv() {
                                Ok(envelope) => batch.push(envelope),
                                Err(_) => break,
                            }
                        }
                        let sequence = next_sequence;
                        next_sequence += 1;
                        unacknowledged.insert(sequence);
                        let delivery = DeliveryBatch { sequence, events: batch };
                        if tx.send(Ok(delivery)).await.is_err() {
                            return;
                        }
                    }
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn report_communication(
        &self,
        request: Request<CommunicationState>,
    ) -> Result<Response<CommunicationAck>, Status> {
        authenticate(&request, self.config.expected_peer)?;
        self.accepted().ok_or_else(|| {
            Status::failed_precondition("no accepted handshake for this instance")
        })?;
        let state = request.into_inner();
        let mut sink = self.sink.lock().unwrap_or_else(PoisonError::into_inner);
        sink.communication(&state)
            .map_err(|refusal| Status::invalid_argument(refusal.reason))?;
        Ok(Response::new(CommunicationAck {}))
    }

    async fn stream_events(
        &self,
        request: Request<Streaming<EventBatch>>,
    ) -> Result<Response<Self::StreamEventsStream>, Status> {
        authenticate(&request, self.config.expected_peer)?;
        let accepted = self.accepted().ok_or_else(|| {
            Status::failed_precondition("no accepted handshake for this instance")
        })?;

        let (tx, rx) = mpsc::channel(16);
        let mut inbound = request.into_inner();
        let sink = Arc::clone(&self.sink);
        tokio::spawn(async move {
            // The window starts full: the adapter may send one window at once.
            let mut window = CreditWindow::new(accepted.window_events);
            if window.grant(accepted.window_events).is_err() {
                return;
            }
            loop {
                let batch = match inbound.message().await {
                    Ok(Some(batch)) => batch,
                    Ok(None) => return,
                    Err(status) => {
                        let _ = tx.send(Err(status)).await;
                        return;
                    }
                };
                if let Err(err) = bounds::check_batch_events(batch.events.len()) {
                    let _ = tx
                        .send(Err(Status::invalid_argument(err.to_string())))
                        .await;
                    return;
                }
                // Batch sizes are bounded by MAX_BATCH_EVENTS, so this fits.
                let events = u32::try_from(batch.events.len()).unwrap_or(u32::MAX);
                let sequence = batch.batch_sequence;

                if let Err(err) = window.consume(events) {
                    let reply = ack(
                        sequence,
                        AckStatus::Rejected,
                        0,
                        &format!("batch exceeds the credit window: {err}"),
                    );
                    if tx.send(Ok(reply)).await.is_err() {
                        return;
                    }
                    continue;
                }

                if tx
                    .send(Ok(ack(sequence, AckStatus::Received, 0, "")))
                    .await
                    .is_err()
                {
                    return;
                }
                let reply = match commit(&sink, &batch) {
                    Ok(Durability::Durable) => ack(sequence, AckStatus::Durable, events, ""),
                    Ok(Durability::NotJournaled) => ack(sequence, AckStatus::Received, events, ""),
                    Err(refusal) => ack(sequence, AckStatus::Rejected, events, &refusal.reason),
                };
                // The batch is no longer in flight: its credits return now.
                let _ = window.grant(events);
                if tx.send(Ok(reply)).await.is_err() {
                    return;
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
