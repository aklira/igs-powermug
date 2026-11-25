//! The local actions as the cell carries them out (SPECIFICATIONS 11.4, PM-ACT-001 to 003): a restart
//! request reaches the supervisor of its session, a shedding order sheds the lanes of its site only,
//! the actions the cell cannot carry out yet are refused, and the executor bounds and audits them.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use powermug_cell::actions::CellHost;
use powermug_core::actions::{
    Action, ActionHost, AuditSink, Budget, Executor, Outcome, Record, Request,
};
use powermug_core::lane::{Lane, PointSpec, PointTable, SessionId};
use powermug_core::{Clock, Times};
use powermug_pivot::{EventId, PointEvent, PointIndex, Quality, Value};
use powermug_store::{EventClass, TelemetryLog};
use tokio::sync::Notify;

const MINUTE_NS: i64 = 60_000_000_000;

fn lane_of(dir: &std::path::Path, name: &str, site: &str) -> Arc<Mutex<Lane>> {
    let table = PointTable::new(
        1,
        vec![PointSpec {
            site_id: site.into(),
            point_id: format!("{site}/TD_002"),
            class: EventClass::C,
            session: Some(SessionId(1)),
            supervision: None,
        }],
    )
    .expect("distinct");
    let log = TelemetryLog::open(dir.join(name)).expect("opens");
    Arc::new(Mutex::new(Lane::new(table, log, 0)))
}

fn class_c_event(id: u32) -> PointEvent {
    let mut bytes = [0u8; 16];
    bytes[6] = 0x70;
    bytes[8] = 0x80;
    bytes[12..16].copy_from_slice(&id.to_be_bytes());
    PointEvent {
        point: PointIndex(0),
        value: Value::Int(i64::from(id)),
        quality: Quality::GOOD,
        source_time_ns: 10,
        receive_time_ns: 20,
        process_time_ns: 30,
        event_id: EventId::from_bytes(bytes).expect("v7"),
        revision_index: 1,
    }
}

fn now() -> Times {
    Clock::new().now()
}

#[tokio::test]
async fn a_restart_request_reaches_the_supervisor_of_its_session() {
    let restart = Arc::new(Notify::new());
    let mut host = CellHost::new(
        BTreeMap::from([(7, Arc::clone(&restart))]),
        Vec::new(),
        Duration::from_secs(60),
    );
    host.run(Action::RestartAdapter, "7").expect("requested");
    tokio::time::timeout(Duration::from_secs(1), restart.notified())
        .await
        .expect("the supervisor is told to restart the process");
}

#[test]
fn a_restart_of_an_unknown_session_or_a_bad_target_is_refused() {
    let mut host = CellHost::new(BTreeMap::new(), Vec::new(), Duration::from_secs(60));
    assert!(host.run(Action::RestartAdapter, "9").is_err());
    assert!(host.run(Action::RestartAdapter, "S3").is_err());
}

#[test]
fn shedding_class_c_sheds_the_lanes_of_its_site_only() {
    let dir = tempfile::tempdir().expect("temp dir");
    let s3 = lane_of(dir.path(), "s3.db", "S3");
    let s4 = lane_of(dir.path(), "s4.db", "S4");
    let mut host = CellHost::new(
        BTreeMap::new(),
        vec![
            ("S3".into(), Arc::clone(&s3)),
            ("S4".into(), Arc::clone(&s4)),
        ],
        Duration::from_secs(60),
    );
    host.run(Action::ShedClassC, "S3").expect("shed");

    let times = now();
    s3.lock()
        .expect("not poisoned")
        .ingest(&[class_c_event(1)], times)
        .expect("admitted");
    s4.lock()
        .expect("not poisoned")
        .ingest(&[class_c_event(2)], times)
        .expect("admitted");

    assert_eq!(
        s3.lock().expect("not poisoned").losses(EventClass::C),
        (1, 0)
    );
    assert_eq!(
        s4.lock().expect("not poisoned").losses(EventClass::C),
        (0, 0)
    );
}

#[test]
fn shedding_an_unknown_site_is_refused() {
    let mut host = CellHost::new(BTreeMap::new(), Vec::new(), Duration::from_secs(60));
    assert!(host.run(Action::ShedClassC, "S9").is_err());
}

#[test]
fn the_actions_the_contract_cannot_carry_yet_are_refused() {
    let mut host = CellHost::new(BTreeMap::new(), Vec::new(), Duration::from_secs(60));
    assert!(host.run(Action::GeneralInterrogation, "S3").is_err());
    assert!(host.run(Action::SwitchRedundantConnection, "S3").is_err());
    assert!(host.run(Action::PurgeTmp, "S3").is_err());
}

/// Records what the executor writes to the audit, in order.
#[derive(Debug, Default)]
struct Trail {
    kinds: Vec<String>,
}

impl AuditSink for Trail {
    fn record(&mut self, kind: &str, _record: &Record) -> Result<(), String> {
        self.kinds.push(kind.to_owned());
        Ok(())
    }
}

#[test]
fn the_executor_bounds_a_restart_and_audits_each_request() {
    let restart = Arc::new(Notify::new());
    let mut host = CellHost::new(
        BTreeMap::from([(7, restart)]),
        Vec::new(),
        Duration::from_secs(60),
    );
    let mut executor = Executor::new(BTreeMap::from([(
        Action::RestartAdapter,
        Budget {
            max: 1,
            window_ns: MINUTE_NS,
        },
    )]));
    let mut trail = Trail::default();
    let request = |id: &str| Request {
        action_id: id.into(),
        action: "restart_adapter".into(),
        target: "7".into(),
        trigger: "test".into(),
    };

    let first = executor.execute(&mut host, &mut trail, &request("a-1"), 0);
    let second = executor.execute(&mut host, &mut trail, &request("a-2"), 1_000);
    assert_eq!(first, Outcome::Done);
    assert!(matches!(second, Outcome::Refused(_)), "{second:?}");
    assert_eq!(
        trail.kinds,
        vec![
            "action.requested",
            "action.finished",
            "action.requested",
            "action.finished"
        ]
    );
    assert_eq!(executor.audit().len(), 2);
}
