//! A single command from the controlling side to a real station of `igs-iec104`: the station
//! executes what it accepts, and answers with the activation confirmation and termination.
//! A refusal reaches the controlling side as a negative answer. Loopback, one process.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use igs_iec104::client::{Client, ClientConfig, Event};
use igs_iec104::process_image::PointValue;
use igs_iec104::server::{CommandValue, Handler, Operation, Refusal, Server, ServerConfig};
use igs_iec104::Delivery;
use igs_iec104_codec::elements::{DoubleCommandState, QualityFlags, Siq};
use igs_iec104_codec::header::{CommonAddress, InformationObjectAddress};
use igs_iec104_link::TransferState;
use powermug_adapter_iec104::command::{
    answer, send, send_before_expiry, send_double, Answer, CommandError, DoubleCommand,
    SingleCommand,
};

/// Records the executions it accepts, and refuses every command when `refuse` is set.
struct Recorder {
    executed: Arc<Mutex<Vec<(u32, bool)>>>,
    refuse: bool,
}

impl Handler for Recorder {
    fn select(&self, _operation: &Operation) -> Result<(), Refusal> {
        if self.refuse {
            Err(Refusal::Rejected)
        } else {
            Ok(())
        }
    }

    fn execute(&self, operation: &Operation) -> Result<(), Refusal> {
        if self.refuse {
            return Err(Refusal::Rejected);
        }
        let on = match operation.value {
            CommandValue::Single(sco) => sco.on,
            CommandValue::Double(dco) => matches!(dco.state, DoubleCommandState::On),
            _ => return Ok(()),
        };
        self.executed
            .lock()
            .expect("lock")
            .push((operation.address.value(), on));
        Ok(())
    }
}

async fn start_station(
    refuse: bool,
) -> (
    SocketAddr,
    Arc<Mutex<Vec<(u32, bool)>>>,
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<()>,
) {
    let executed = Arc::new(Mutex::new(Vec::new()));
    let server = Server::bind(
        ServerConfig::new(
            "127.0.0.1:0".parse::<SocketAddr>().expect("addr"),
            CommonAddress::new(1),
        ),
        Recorder {
            executed: Arc::clone(&executed),
            refuse,
        },
    )
    .await
    .expect("station binds");
    let addr = server.local_addr().expect("address");
    server
        .handle()
        .add_point(
            InformationObjectAddress::new(5).expect("address"),
            PointValue::Single(Siq {
                on: false,
                quality: QualityFlags::default(),
            }),
            false,
        )
        .expect("commandable point");
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let task = tokio::spawn(async move {
        let _ = server
            .run_until(
                async {
                    let _ = stopped.await;
                },
                Duration::ZERO,
            )
            .await;
    });
    (addr, executed, stop, task)
}

/// Connects a controlling client and waits until its data transfer is started.
async fn controlling(addr: SocketAddr) -> Client {
    let mut client = Client::connect(ClientConfig::new(addr, CommonAddress::new(1)));
    let deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(200), client.next_event()).await {
            Ok(Some(Event::Connected)) => {
                client.start_data_transfer().await.expect("starts");
            }
            Ok(Some(Event::Delivery(Delivery::Transfer(TransferState::Started)))) => return client,
            _ => {}
        }
    }
    panic!("the data transfer did not start");
}

/// Collects the answers to commands until `count` have arrived or the time is up.
async fn answers(client: &mut Client, count: usize) -> Vec<(u32, Answer)> {
    let mut found = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < deadline && found.len() < count {
        if let Ok(Some(Event::Delivery(Delivery::Asdu(asdu)))) =
            tokio::time::timeout(Duration::from_millis(200), client.next_event()).await
        {
            if let Some(found_answer) = answer(&asdu) {
                found.push(found_answer);
            }
        }
    }
    found
}

#[tokio::test]
async fn a_selected_command_is_confirmed_executed_and_terminated() {
    let (addr, executed, stop, task) = start_station(false).await;
    let mut client = controlling(addr).await;

    // Select-before-operate: the select is confirmed first, then the execution.
    send(
        &client,
        SingleCommand {
            address: 5,
            on: true,
            select: true,
        },
    )
    .await
    .expect("selected");
    let selected = answers(&mut client, 1).await;
    send(
        &client,
        SingleCommand {
            address: 5,
            on: true,
            select: false,
        },
    )
    .await
    .expect("executed");
    let executed_answers = answers(&mut client, 2).await;

    let _ = stop.send(());
    let _ = task.await;

    assert_eq!(selected, vec![(5, Answer::Confirmed)]);
    assert_eq!(
        executed_answers,
        vec![(5, Answer::Confirmed), (5, Answer::Terminated)]
    );
    assert_eq!(*executed.lock().expect("lock"), vec![(5, true)]);
}

#[tokio::test]
async fn a_refused_command_reaches_the_controlling_side_as_a_negative_answer() {
    let (addr, executed, stop, task) = start_station(true).await;
    let mut client = controlling(addr).await;

    send(
        &client,
        SingleCommand {
            address: 5,
            on: false,
            select: false,
        },
    )
    .await
    .expect("sent");
    let received = answers(&mut client, 1).await;

    let _ = stop.send(());
    let _ = task.await;

    assert_eq!(received.first(), Some(&(5, Answer::Refused)));
    assert!(executed.lock().expect("lock").is_empty());
}

#[tokio::test]
async fn a_selected_double_command_is_confirmed_executed_and_terminated() {
    let (addr, executed, stop, task) = start_station(false).await;
    let mut client = controlling(addr).await;

    send_double(
        &client,
        DoubleCommand {
            address: 5,
            on: false,
            select: true,
        },
    )
    .await
    .expect("selected");
    let selected = answers(&mut client, 1).await;
    send_double(
        &client,
        DoubleCommand {
            address: 5,
            on: false,
            select: false,
        },
    )
    .await
    .expect("executed");
    let executed_answers = answers(&mut client, 2).await;

    let _ = stop.send(());
    let _ = task.await;

    assert_eq!(selected, vec![(5, Answer::Confirmed)]);
    assert_eq!(
        executed_answers,
        vec![(5, Answer::Confirmed), (5, Answer::Terminated)]
    );
    assert_eq!(*executed.lock().expect("lock"), vec![(5, false)]);
}

#[tokio::test]
async fn an_expired_command_is_never_emitted() {
    let (addr, executed, stop, task) = start_station(false).await;
    let client = controlling(addr).await;

    let refused = send_before_expiry(
        &client,
        SingleCommand {
            address: 5,
            on: true,
            select: true,
        },
        1_000,
        2_000,
    )
    .await;
    assert!(matches!(refused, Err(CommandError::Expired { .. })));
    tokio::time::sleep(Duration::from_millis(300)).await;

    let _ = stop.send(());
    let _ = task.await;

    assert!(executed.lock().expect("lock").is_empty());
}

#[tokio::test]
async fn a_live_command_is_emitted_before_its_expiry() {
    let (addr, executed, stop, task) = start_station(false).await;
    let mut client = controlling(addr).await;

    send_before_expiry(
        &client,
        SingleCommand {
            address: 5,
            on: true,
            select: true,
        },
        i64::MAX,
        1_000,
    )
    .await
    .expect("sent");
    let selected = answers(&mut client, 1).await;

    let _ = stop.send(());
    let _ = task.await;

    assert_eq!(selected, vec![(5, Answer::Confirmed)]);
    assert!(executed.lock().expect("lock").is_empty());
}
