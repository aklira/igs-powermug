//! The cell: one process that opens the telemetry log, the critical store and one
//! lane per partition, and serves the management API over mutual TLS.
//!
//! The telemetry log has a single writer for the whole cell (§7.2): every lane holds
//! a handle onto it. The lanes are rebuilt from the journal at start (§7.2, PM-TEL-003),
//! so that nothing is valid until a fresh value. The South instances of the configuration
//! are started and supervised by the cell (see [`adapters`]); their values reach the lanes
//! only through the adapter contract.

pub mod actions;
pub mod adapters;
pub mod anchor;
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
use tokio::sync::{watch, Notify};

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
    #[error("a stored revision cannot be read: {0}")]
    StoredRevision(String),
    #[error("the telemetry and critical stores share a filesystem without a declared quota (PM-TEL-006): separate them, or set the quotas and declare storage.shared_quota")]
    SharedVolume,
}

/// The telemetry and the critical stores must not share a filesystem unless each has a quota
/// (PM-TEL-006): they are compared by device.
fn check_volumes(config: &CellConfig) -> Result<(), CellError> {
    use std::os::unix::fs::MetadataExt;
    if config.storage.shared_quota {
        return Ok(());
    }
    let device = |path: &std::path::Path| -> Result<u64, CellError> {
        let parent = match path.parent() {
            Some(parent) if !parent.as_os_str().is_empty() => parent,
            _ => std::path::Path::new("."),
        };
        std::fs::metadata(parent)
            .map(|metadata| metadata.dev())
            .map_err(|err| CellError::File {
                path: parent.display().to_string(),
                message: err.to_string(),
            })
    };
    if device(&config.telemetry_db)? == device(&config.critical_db)? {
        return Err(CellError::SharedVolume);
    }
    Ok(())
}

/// The indicators of the cell: those of every lane, measured together.
#[derive(Debug)]
pub struct CellIndicators {
    lanes: Vec<Arc<Mutex<Lane>>>,
    commands: powermug_command::CommandClient,
    sites: Vec<String>,
    /// The directories of the telemetry and the critical stores: each volume is measured.
    telemetry_dir: std::path::PathBuf,
    critical_dir: std::path::PathBuf,
}

/// Declares the cursor of each flux destination with its starting point (PM-TEL-004).
fn declare_destinations(config: &CellConfig, now_ns: i64) -> Result<(), CellError> {
    if config.destinations.is_empty() {
        return Ok(());
    }
    let mut log = powermug_store::TelemetryLog::open(&config.telemetry_db)?;
    for destination in &config.destinations {
        let start = match destination.start {
            config::DestinationStart::Now => powermug_store::Start::Now,
            config::DestinationStart::OldestRetained => powermug_store::Start::OldestRetained,
        };
        log.declare_cursor(&destination.name, true, start, now_ns)?;
    }
    Ok(())
}

/// The source of the indicators of the cell: the lanes, the commands in doubt per site, and the
/// free share of the volumes of the two stores.
fn indicator_source(
    config: &CellConfig,
    lanes: &[Arc<Mutex<Lane>>],
    commands: &powermug_command::CommandClient,
) -> Arc<dyn IndicatorSource> {
    let mut sites: Vec<String> = config.lanes.iter().map(|lane| lane.site.clone()).collect();
    sites.dedup();
    let dir_of = |path: &std::path::Path| {
        path.parent()
            .filter(|dir| !dir.as_os_str().is_empty())
            .unwrap_or(std::path::Path::new("."))
            .to_path_buf()
    };
    Arc::new(CellIndicators {
        lanes: lanes.to_vec(),
        commands: commands.clone(),
        sites,
        telemetry_dir: dir_of(&config.telemetry_db),
        critical_dir: dir_of(&config.critical_db),
    })
}

/// The free share of the filesystem holding `dir`, in percent, when it can be read.
fn free_percent(dir: &std::path::Path) -> Option<f64> {
    let stat = nix::sys::statvfs::statvfs(dir).ok()?;
    let blocks = stat.blocks();
    if blocks == 0 {
        return None;
    }
    let percent = stat.blocks_available().saturating_mul(100) / blocks;
    Some(f64::from(u32::try_from(percent).unwrap_or(u32::MAX)))
}

impl IndicatorSource for CellIndicators {
    fn sample(&self, now_wall_ns: i64) -> Vec<Indicator> {
        let mut indicators: Vec<Indicator> = self
            .lanes
            .iter()
            .flat_map(|lane| lane.sample(now_wall_ns))
            .collect();
        // The commands in doubt, per site: the journal answers for each site it holds.
        for site in &self.sites {
            let value = self
                .commands
                .count_in_doubt(site)
                .ok()
                .map(|count| f64::from(u32::try_from(count).unwrap_or(u32::MAX)));
            indicators.push(Indicator {
                name: "command_in_doubt_count",
                scope: "site",
                site: Some(site.clone()),
                session: None,
                value,
                unit: "count",
                measured_at_ns: now_wall_ns,
            });
        }
        // The free share of each store's volume (scope target): not measured when unreadable.
        for (name, dir) in [
            ("disk_free_percent", &self.telemetry_dir),
            ("cell_disk_free_percent", &self.critical_dir),
        ] {
            indicators.push(Indicator {
                name,
                scope: "target",
                site: None,
                session: None,
                value: free_percent(dir),
                unit: "percent",
                measured_at_ns: now_wall_ns,
            });
        }
        indicators
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
    /// A journal client for the audit anchoring, which reads the head of the chain.
    audit: powermug_command::CommandClient,
    /// The commands the restart rules decided when the cell opened, by partition.
    pub recovery: Vec<powermug_command::Recovery>,
    /// The restart request of each South session: a local action asks its supervisor through it.
    pub restarts: std::collections::BTreeMap<u32, Arc<Notify>>,
    /// The site of each lane, in the order of `lanes`.
    sites: Vec<String>,
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
        check_volumes(config)?;
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
        declare_destinations(config, clock.now().wall_ns)?;
        // The command journal lives in the critical store's file, on its own connection.
        let (commands, command_writer) =
            powermug_command::CommandWriter::spawn(&config.critical_db)?;
        let audit = commands.clone();
        let indicators = indicator_source(config, &lanes, &commands);
        // Each session's dispatcher has the empty ACL and no owner: until the configuration
        // grants a command and the partition has an owner, every command is refused. Its
        // commands must cite the partition's active revision, read from the critical store.
        let mut commands_by_session = std::collections::BTreeMap::new();
        // The rules are the command rules of the active revision of the instance's partition.
        let mut active_of_session = std::collections::BTreeMap::new();
        for instance in adapters::instances(config)? {
            let site = &config.lanes[instance.lane].site;
            let revision = critical.active(site)?;
            let rules = match &revision {
                Some(id) => stored_rules(&critical, id)?,
                None => Vec::new(),
            };
            active_of_session.insert(instance.session, (revision, rules));
        }
        // The restart rules of the commands run before any South session is activated
        // (SPECIFICATIONS 9.7, GW 11.5). Ownership is not kept across a restart, so the owner
        // epoch is 0: a command emitted under an earlier owner fails rather than being sent again.
        let mut recovery = Vec::new();
        let sites: std::collections::BTreeSet<&str> =
            config.lanes.iter().map(|lane| lane.site.as_str()).collect();
        for site in sites {
            let revision = critical.active(site)?;
            recovery.extend(commands.recover_partition(
                site,
                powermug_command::RecoveryContext {
                    now_ns: commands::now_ns(),
                    active_revision: revision.unwrap_or_default(),
                    owner_epoch: 0,
                    ha_active: true,
                },
            )?);
        }
        for instance in adapters::instances(config)? {
            let route = commands::CommandRoute::new();
            let (revision, rules) = active_of_session
                .remove(&instance.session)
                .unwrap_or_default();
            // A single node is the confirmed owner of its partition from its opening (decision of
            // the owner, 2026-11-25): commands are accepted from then on, at owner epoch 1.
            let mut ownership = powermug_command::ownership::Ownership::new();
            ownership.takeover(&config.lanes[instance.lane].site);
            let mut dispatcher = commands::Dispatcher::new(
                commands.clone(),
                config::acl_of_rules(&rules),
                ownership,
                route.clone(),
            );
            dispatcher.set_active_revision(revision);
            commands_by_session.insert(
                instance.session,
                adapters::CommandLink {
                    dispatcher: Arc::new(Mutex::new(dispatcher)),
                    route,
                },
            );
        }
        // A commandable point is routed to the dispatcher of its session.
        let mut routes = std::collections::BTreeMap::new();
        for lane in &config.lanes {
            for point in &lane.points {
                if let Some(link) = commands_by_session.get(&point.session) {
                    routes.insert(
                        point.id.clone(),
                        commands::Route {
                            dispatcher: Arc::clone(&link.dispatcher),
                            partition: lane.site.clone(),
                            address: point.address,
                        },
                    );
                }
            }
        }
        let api = Arc::new(
            Api::new(critical, powermug_v1(), trusted_key, indicators, commands)
                .with_commands(Arc::new(commands::Gateway::new(routes))),
        );
        let restarts = adapters::instances(config)?
            .iter()
            .map(|instance| (instance.session, Arc::new(Notify::new())))
            .collect();
        let sites = config.lanes.iter().map(|lane| lane.site.clone()).collect();
        Ok(Self {
            api,
            lanes,
            commands_by_session,
            writer,
            commands: command_writer,
            audit,
            recovery,
            restarts,
            sites,
        })
    }

    /// The host of the local actions, which shed the lanes of a site for `shed_window`.
    #[must_use]
    pub fn action_host(&self, shed_window: std::time::Duration) -> actions::CellHost {
        let lanes = self
            .sites
            .iter()
            .cloned()
            .zip(self.lanes.iter().cloned())
            .collect();
        actions::CellHost::new(self.restarts.clone(), lanes, shed_window)
    }

    /// A journal client for anchoring the audit head. It must be dropped before the cell closes.
    #[must_use]
    pub fn audit_client(&self) -> powermug_command::CommandClient {
        self.audit.clone()
    }

    /// Releases the lanes and waits for the telemetry writer to stop.
    pub fn close(self) {
        let Self {
            api,
            lanes,
            commands_by_session,
            writer,
            commands,
            audit,
            recovery: _,
            restarts: _,
            sites: _,
        } = self;
        drop(api);
        drop(lanes);
        // The dispatchers and the anchoring hold journal clients: they go before the command
        // writer is waited for.
        drop(commands_by_session);
        drop(audit);
        writer.join();
        commands.join();
    }
}

/// The command rules of a stored revision.
fn stored_rules(
    critical: &CriticalStore,
    id: &str,
) -> Result<Vec<powermug_revision::partition::CommandRule>, CellError> {
    let body = critical.body(id)?.ok_or_else(|| {
        CellError::StoredRevision(format!("the active revision {id} has no stored body"))
    })?;
    let document: serde_json::Value =
        serde_json::from_slice(&body).map_err(|err| CellError::StoredRevision(err.to_string()))?;
    let revision = powermug_revision::partition::parse(&document)
        .map_err(|finding| CellError::StoredRevision(finding.message))?;
    Ok(revision.spec.commands)
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
    // The audit head is anchored outside the store until the stop signal (NFR-SEC-008).
    let anchoring = config.commands.audit_anchor.as_ref().map(|anchor| {
        tokio::spawn(anchor::run(
            cell.audit_client(),
            anchor.path.clone(),
            std::time::Duration::from_secs(anchor.every_s),
            stopped.clone(),
        ))
    });
    let supervisors: Vec<_> = instances
        .into_iter()
        .map(|instance| {
            let lane = Arc::clone(&cell.lanes[instance.lane]);
            let commands = cell.commands_by_session.get(&instance.session).cloned();
            let restart = cell
                .restarts
                .get(&instance.session)
                .map_or_else(|| Arc::new(Notify::new()), Arc::clone);
            tokio::spawn(adapters::supervise(
                instance,
                lane,
                Policy::default(),
                stopped.clone(),
                commands,
                restart,
            ))
        })
        .collect();

    let app = powermug_mgmt::service(Arc::clone(&cell.api));
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
    if let Some(anchoring) = anchoring {
        let _ = anchoring.await;
    }
    cell.close();
    served.map_err(|err| CellError::Listen(err.to_string()))
}
