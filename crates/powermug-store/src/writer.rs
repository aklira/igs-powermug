//! The single writer of a cell: one thread owns the telemetry log, and takes
//! batches from all the lanes (SPECIFICATIONS §7.2).
//!
//! The writer waits up to [`GROUP_WINDOW`] after the first batch, or until
//! [`GROUP_MAX_EVENTS`] events are waiting, then commits them in one transaction
//! with `synchronous=FULL`. Each batch keeps its own savepoint, so a refused
//! batch does not affect the others of its group. Reads go through the same
//! thread, so that the writer is the only connection to the file.

use std::path::Path;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender, SyncSender};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::cursors::Cursor;
use crate::journal::{Journal, OwnedEvent};
use crate::log::{
    Committed, EventClass, NewEvent, Snapshot, StoreError, StoredEvent, TelemetryLog,
};

/// How long the writer waits for more batches after the first one.
pub const GROUP_WINDOW: Duration = Duration::from_millis(5);
/// Events that close a group early.
pub const GROUP_MAX_EVENTS: usize = 4_096;

enum Request {
    Append {
        records: Vec<OwnedEvent>,
        reply: SyncSender<Result<Option<Committed>, StoreError>>,
    },
    Read {
        seq: i64,
        limit: u32,
        reply: SyncSender<Result<Vec<StoredEvent>, StoreError>>,
    },
    Last {
        reply: SyncSender<Result<Option<i64>, StoreError>>,
    },
    Usage {
        class: EventClass,
        reply: SyncSender<Result<u64, StoreError>>,
    },
    DestinationCursor {
        destination: String,
        reply: SyncSender<Result<Option<Cursor>, StoreError>>,
    },
    Acknowledge {
        destination: String,
        seq: i64,
        now_ns: i64,
        reply: SyncSender<Result<Cursor, StoreError>>,
    },
    PutSnapshot {
        partition: String,
        seq: i64,
        payload: Vec<u8>,
        reply: SyncSender<Result<(), StoreError>>,
    },
    LatestSnapshot {
        partition: String,
        reply: SyncSender<Result<Option<Snapshot>, StoreError>>,
    },
}

/// A handle onto the writer of a cell. Clone it for each lane.
#[derive(Debug, Clone)]
pub struct GroupJournal {
    requests: Sender<Request>,
}

/// The writer thread of a cell. It stops once every handle is dropped.
#[derive(Debug)]
pub struct Writer {
    thread: JoinHandle<()>,
}

impl Writer {
    /// Waits for the writer thread to finish, after every handle is dropped.
    pub fn join(self) {
        let _ = self.thread.join();
    }
}

impl GroupJournal {
    /// Opens the telemetry log at `path` and starts its writer thread.
    ///
    /// # Errors
    ///
    /// Returns a store error when the file cannot be opened.
    pub fn spawn(path: impl AsRef<Path>) -> Result<(Self, Writer), StoreError> {
        let log = TelemetryLog::open(path)?;
        let (requests, inbox) = mpsc::channel();
        let thread = std::thread::Builder::new()
            .name("powermug-writer".into())
            .spawn(move || serve(log, &inbox))
            .map_err(|err| StoreError::CommitFailed(err.to_string()))?;
        Ok((Self { requests }, Writer { thread }))
    }

    fn send(&self, request: Request) -> Result<(), StoreError> {
        self.requests
            .send(request)
            .map_err(|_| StoreError::WriterStopped)
    }
}

fn serve(mut log: TelemetryLog, inbox: &Receiver<Request>) {
    while let Ok(first) = inbox.recv() {
        match first {
            Request::Append { records, reply } => {
                let mut pending = vec![(records, reply)];
                let mut events = pending[0].0.len();
                let mut other = None;
                let deadline = Instant::now() + GROUP_WINDOW;
                while events < GROUP_MAX_EVENTS {
                    let wait = deadline.saturating_duration_since(Instant::now());
                    if wait.is_zero() {
                        break;
                    }
                    match inbox.recv_timeout(wait) {
                        Ok(Request::Append { records, reply }) => {
                            events += records.len();
                            pending.push((records, reply));
                        }
                        Ok(request) => {
                            other = Some(request);
                            break;
                        }
                        Err(RecvTimeoutError::Timeout | RecvTimeoutError::Disconnected) => break,
                    }
                }
                commit_group(&mut log, pending);
                if let Some(request) = other {
                    answer(&mut log, request);
                }
            }
            request => answer(&mut log, request),
        }
    }
}

type Pending = (
    Vec<OwnedEvent>,
    SyncSender<Result<Option<Committed>, StoreError>>,
);

fn commit_group(log: &mut TelemetryLog, pending: Vec<Pending>) {
    let (groups, replies): (Vec<_>, Vec<_>) = pending.into_iter().unzip();
    let results = log.append_groups(&groups);
    for (reply, result) in replies.into_iter().zip(results) {
        // The lane may have stopped waiting: its result is then not needed.
        let _ = reply.send(result);
    }
}

fn answer(log: &mut TelemetryLog, request: Request) {
    match request {
        Request::Append { records, reply } => commit_group(log, vec![(records, reply)]),
        Request::Read { seq, limit, reply } => {
            let _ = reply.send(log.read_from(seq, limit));
        }
        Request::Last { reply } => {
            let _ = reply.send(log.last_seq());
        }
        Request::Usage { class, reply } => {
            let _ = reply.send(log.usage(class));
        }
        Request::DestinationCursor { destination, reply } => {
            let _ = reply.send(log.cursor(&destination));
        }
        Request::Acknowledge {
            destination,
            seq,
            now_ns,
            reply,
        } => {
            let _ = reply.send(log.acknowledge(&destination, seq, now_ns));
        }
        Request::PutSnapshot {
            partition,
            seq,
            payload,
            reply,
        } => {
            let _ = reply.send(log.put_snapshot(&partition, seq, &payload));
        }
        Request::LatestSnapshot { partition, reply } => {
            let _ = reply.send(log.latest_snapshot(&partition));
        }
    }
}

impl Journal for GroupJournal {
    fn append(&mut self, batch: &[NewEvent<'_>]) -> Result<Option<Committed>, StoreError> {
        if batch.iter().any(|event| event.class == EventClass::C) {
            return Err(StoreError::ClassNotJournaled);
        }
        if batch.is_empty() {
            return Ok(None);
        }
        let (reply, waiting) = mpsc::sync_channel(1);
        let records = batch.iter().map(OwnedEvent::from).collect();
        self.send(Request::Append { records, reply })?;
        waiting.recv().map_err(|_| StoreError::WriterStopped)?
    }

    fn read_from(&self, seq: i64, limit: u32) -> Result<Vec<StoredEvent>, StoreError> {
        let (reply, waiting) = mpsc::sync_channel(1);
        self.send(Request::Read { seq, limit, reply })?;
        waiting.recv().map_err(|_| StoreError::WriterStopped)?
    }

    fn last_seq(&self) -> Result<Option<i64>, StoreError> {
        let (reply, waiting) = mpsc::sync_channel(1);
        self.send(Request::Last { reply })?;
        waiting.recv().map_err(|_| StoreError::WriterStopped)?
    }

    fn usage(&self, class: EventClass) -> Result<u64, StoreError> {
        let (reply, waiting) = mpsc::sync_channel(1);
        self.send(Request::Usage { class, reply })?;
        waiting.recv().map_err(|_| StoreError::WriterStopped)?
    }

    fn cursor(&self, destination: &str) -> Result<Option<Cursor>, StoreError> {
        let (reply, waiting) = mpsc::sync_channel(1);
        self.send(Request::DestinationCursor {
            destination: destination.to_owned(),
            reply,
        })?;
        waiting.recv().map_err(|_| StoreError::WriterStopped)?
    }

    fn acknowledge(
        &mut self,
        destination: &str,
        seq: i64,
        now_ns: i64,
    ) -> Result<Cursor, StoreError> {
        let (reply, waiting) = mpsc::sync_channel(1);
        self.send(Request::Acknowledge {
            destination: destination.to_owned(),
            seq,
            now_ns,
            reply,
        })?;
        waiting.recv().map_err(|_| StoreError::WriterStopped)?
    }

    fn put_snapshot(
        &mut self,
        partition: &str,
        seq: i64,
        payload: &[u8],
    ) -> Result<(), StoreError> {
        let (reply, waiting) = mpsc::sync_channel(1);
        self.send(Request::PutSnapshot {
            partition: partition.to_owned(),
            seq,
            payload: payload.to_vec(),
            reply,
        })?;
        waiting.recv().map_err(|_| StoreError::WriterStopped)?
    }

    fn latest_snapshot(&self, partition: &str) -> Result<Option<Snapshot>, StoreError> {
        let (reply, waiting) = mpsc::sync_channel(1);
        self.send(Request::LatestSnapshot {
            partition: partition.to_owned(),
            reply,
        })?;
        waiting.recv().map_err(|_| StoreError::WriterStopped)?
    }
}
