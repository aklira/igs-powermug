//! The cell: one process that opens the telemetry log, the critical store and one
//! lane per partition, and serves the management API over mutual TLS.
//!
//! The telemetry log has a single writer for the whole cell (§7.2): every lane holds
//! a handle onto it. The lanes are rebuilt from the journal at start (§7.2, PM-TEL-003),
//! so that nothing is valid until a fresh value. The South instances of the configuration
//! are started and supervised by the cell (see [`adapters`]); their values reach the lanes
//! only through the adapter contract.

pub mod adapters;
pub mod commands;
pub mod config;

use std::future::Future;
use std::sync::{Arc, Mutex};

use powermug_core::lane::{Lane, LaneError, TableError};
use powermug_core::supervisor::Policy;
use powermug_core::{Clock, Indicator, IndicatorSource};
use powermug_mgmt::{load_trusted_key, server_config, Api, KeyError, TlsError, TlsListener};
use powermug_revision::profile::powermug_v1;
use powermug_store::{CriticalStore, GenerationError, GroupJournal, StoreError, Writer};
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::sync::watch;

pub use config::{CellConfig, ConfigError};

/// The cell could not start or run.
#[derive(Debug, Error)]
pub enum CellError {
    #[error("cannot read {path}: {message}")]
    File { path: String, message: String },
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Generation(#[from] GenerationError),
    #[error(transparent)]
    Table(#[from] TableError),
    #[error(transparent)]
    Lane(#[from] LaneError),
    #[error(transparent)]
    Key(#[from] KeyError),
    #[error(transparent)]
    Tls(#[from] TlsError),
    #[error("cannot listen on the management address: {0}")]
    Listen(String),
    #[error(transparent)]
    Sandbox(#[from] powermug_sandbox::SandboxError),
    #[error(transparent)]
    Command(#[from] powermug_command::CommandError),
}

/// The indicators of the cell: those of every lane, measured together.
#[derive(Debug)]
pub struct CellIndicators {
    lanes: Vec<Arc<Mutex<Lane>>>,
}

impl IndicatorSource for CellIndicators {
    fn sample(&self, now_wall_ns: i64) -> Vec<Indicator> {
        self.lanes
            .iter()
            .flat_map(|lane| lane.sample(now_wall_ns))
            .collect()
    }
}

/// The opened cell: its lanes, its management API, and the writer of its telemetry log.
#[derive(Debug)]
pub struct Cell {
    pub api: Arc<Api>,
    pub lanes: Vec<Arc<Mutex<Lane>>>,
    /// The command path of each South session that has an adapter instance.
    pub commands_by_session: std::collections::BTreeMap<u32, adapters::CommandLink>,
    writer: Writer,
    commands: powermug_command::CommandWriter,
}

impl Cell {
    /// Opens the stores and the lanes of `config`, trusting `trusted_key` for signed plans.
    ///
    /// # Errors
    ///
    /// Returns [`CellError`] when a store cannot be opened, a lane cannot be rebuilt
    /// from its journal, or the points do not form a table.
    pub fn open(
        config: &CellConfig,
        trusted_key: ed25519_dalek::VerifyingKey,
    ) -> Result<Self, CellError> {
        let (journal, writer) = GroupJournal::spawn(&config.telemetry_db)?;
        let clock = Clock::new();
        let mut lanes = Vec::with_capacity(config.lanes.len());
        for lane in &config.lanes {
            let table = lane.table()?;
            // The lane's monotonic clock starts at zero, as the clock that feeds it.
            let opened = Lane::open(table, journal.clone(), clock.now().monotonic_ns)?;
            lanes.push(Arc::new(Mutex::new(opened)));
        }
        drop(journal);
        let critical = CriticalStore::open(&config.critical_db)?;
        let indicators: Arc<dyn IndicatorSource> = Arc::new(CellIndicators {
            lanes: lanes.clone(),
        });
        // The command journal lives in the critical store's file, on its own connection.
        let (commands, command_writer) =
            powermug_command::CommandWriter::spawn(&config.critical_db)?;
        // Each session's dispatcher has the empty ACL and no owner: until the configuration
        // grants a command and the partition has an owner, every command is refused.
        let mut commands_by_session = std::collections::BTreeMap::new();
        for instance in adapters::instances(config)? {
            let route = commands::CommandRoute::new();
            let dispatcher = commands::Dispatcher::new(
                commands.clone(),
                powermug_command::authorization::Acl::new(),
                powermug_command::ownership::Ownership::new(),
                route.clone(),
            );
            commands_by_session.insert(
                instance.session,
                adapters::CommandLink {
                    dispatcher: Arc::new(Mutex::new(dispatcher)),
                    route,
                },
            );
        }
        let api = Arc::new(Api::new(
            critical,
            powermug_v1(),
            trusted_key,
            indicators,
            commands,
        ));
        Ok(Self {
            api,
            lanes,
            commands_by_session,
            writer,
            commands: command_writer,
        })
    }

    /// Releases the lanes and waits for the telemetry writer to stop.
    pub fn close(self) {
        let Self {
            api,
            lanes,
            commands_by_session,
            writer,
            commands,
        } = self;
        drop(api);
        drop(lanes);
        // The dispatchers hold journal clients: they go before the writer is waited for.
        drop(commands_by_session);
        writer.join();
        commands.join();
    }
}

/// Reads a file the configuration names.
fn read(path: &std::path::Path) -> Result<Vec<u8>, CellError> {
    std::fs::read(path).map_err(|err| CellError::File {
        path: path.display().to_string(),
        message: err.to_string(),
    })
}

/// Opens the cell of `config` and serves its management API until `shutdown` completes.
///
/// # Errors
///
/// Returns [`CellError`] when the cell cannot be opened, the TLS material is unusable,
/// or the management address cannot be bound.
pub async fn serve(
    config: CellConfig,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), CellError> {
    let management = &config.management;
    let tls = server_config(
        &read(&management.certificate)?,
        &read(&management.key)?,
        &read(&management.client_ca)?,
    )?;
    let trusted_key = load_trusted_key(&management.trusted_key)?;
    let cell = Cell::open(&config, trusted_key)?;

    let listener = TcpListener::bind(management.listen)
        .await
        .map_err(|err| CellError::Listen(err.to_string()))?;
    let address = listener
        .local_addr()
        .map_err(|err| CellError::Listen(err.to_string()))?;
    println!(
        "powermug-cell {}: management API on https://{address}",
        config.cell
    );

    // The South instances stop with the cell: a stop signal reaches each supervisor.
    let (stop, stopped) = watch::channel(false);
    let mut instances = adapters::instances(&config)?;
    let limits = config
        .adapters
        .as_ref()
        .map(|adapters| adapters.limits)
        .unwrap_or_default();
    adapters::provision(&mut instances, limits)?;
    let supervisors: Vec<_> = instances
        .into_iter()
        .map(|instance| {
            let lane = Arc::clone(&cell.lanes[instance.lane]);
            let commands = cell.commands_by_session.get(&instance.session).cloned();
            tokio::spawn(adapters::supervise(
                instance,
                lane,
                Policy::default(),
                stopped.clone(),
                commands,
            ))
        })
        .collect();

    let app = powermug_mgmt::router(Arc::clone(&cell.api));
    let stopping = stop.clone();
    let served = axum::serve(TlsListener::new(listener, tls), app)
        .with_graceful_shutdown(async move {
            shutdown.await;
            let _ = stopping.send(true);
        })
        .await;
    let _ = stop.send(true);
    for supervisor in supervisors {
        let _ = supervisor.await;
    }
    cell.close();
    served.map_err(|err| CellError::Listen(err.to_string()))
}
