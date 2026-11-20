//! Two partition lanes of one cell share the single writer (SPECIFICATIONS §7.2).

use powermug_core::lane::{Lane, PointSpec, PointTable, SessionId};
use powermug_core::Times;
use powermug_pivot::{EventId, PointEvent, PointIndex, Quality, Value};
use powermug_store::{EventClass, GroupJournal};

fn table(site: &str) -> PointTable {
    PointTable::new(
        1,
        vec![PointSpec {
            site_id: site.into(),
            point_id: format!("{site}/TM_001"),
            class: EventClass::A,
            session: Some(SessionId(1)),
            supervision: None,
        }],
    )
    .expect("distinct")
}

fn event(id: u32, value: i64) -> PointEvent {
    let mut bytes = [0u8; 16];
    bytes[6] = 0x70;
    bytes[8] = 0x80;
    bytes[12..16].copy_from_slice(&id.to_be_bytes());
    PointEvent {
        point: PointIndex(0),
        value: Value::Int(value),
        quality: Quality::GOOD,
        source_time_ns: 1,
        receive_time_ns: 1,
        process_time_ns: 1,
        event_id: EventId::from_bytes(bytes).expect("v7"),
        revision_index: 1,
    }
}

#[test]
fn two_lanes_commit_through_one_writer_and_keep_their_own_images() {
    let dir = tempfile::tempdir().expect("temp dir");
    let (journal, writer) = GroupJournal::spawn(dir.path().join("telemetry.db")).expect("spawns");
    let mut lane_a = Lane::new(table("S1"), journal.clone(), 0);
    let mut lane_b = Lane::new(table("S2"), journal.clone(), 0);
    drop(journal);

    let at = Times {
        monotonic_ns: 0,
        wall_ns: 0,
    };
    let first = lane_a
        .ingest(&[event(1, 10)], at)
        .expect("lane A")
        .expect("journaled");
    let second = lane_b
        .ingest(&[event(2, 20)], at)
        .expect("lane B")
        .expect("journaled");
    let third = lane_a
        .ingest(&[event(3, 30)], at)
        .expect("lane A again")
        .expect("journaled");

    assert_eq!((first.first_seq, first.last_seq), (1, 1));
    assert_eq!((second.first_seq, second.last_seq), (2, 2));
    assert_eq!((third.first_seq, third.last_seq), (3, 3));
    assert_eq!(
        lane_a.image().get(PointIndex(0)).map(|s| s.value),
        Some(Value::Int(30))
    );
    assert_eq!(
        lane_b.image().get(PointIndex(0)).map(|s| s.value),
        Some(Value::Int(20))
    );
    assert_eq!(lane_a.log().last_seq().expect("last"), Some(3));
    drop(lane_a);
    drop(lane_b);
    writer.join();
}
