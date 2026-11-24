//! A regulating step (raise or lower) from the controlling side to a real station of
//! `igs-iec104`: selected, then executed; the station records the step it accepted, and
//! answers with the activation confirmation and termination. Loopback, one process.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use igs_iec104::client::{Client, ClientConfig, Event};
use igs_iec104::process_image::PointValue;
use igs_iec104::server::{CommandValue, Handler, Operation, Refusal, Server, ServerConfig};
use igs_iec104::Delivery;
use igs_iec104_codec::elements::{Qds, RegulatingStep, ShortFloat};
use igs_iec104_codec::header::{CommonAddress, InformationObjectAddress};
use igs_iec104_link::TransferState;
use powermug_adapter_iec104::command::{
    answer, send_regulating_step, Answer, RegulatingStepCommand,
};

/// Records the steps it accepts.
struct Recorder {
    steps: Arc<Mutex<Vec<(u32, bool)>>>,
}

impl Handler for Recorder {
    fn select(&self, _operation: &Operation) -> Result<(), Refusal> {
        Ok(())
    }

    fn execute(&self, operation: &Operation) -> Result<(), Refusal> {
        if let CommandValue::Step(rco) = operation.value {
            self.steps.lock().expect("lock").push((
                operation.address.value(),
                matches!(rco.step, RegulatingStep::Higher),
            ));
        }
        Ok(())
    }
}

#[tokio::test]
async fn a_selected_regulating_step_is_executed_and_terminated() {
    let steps = Arc::new(Mutex::new(Vec::new()));
    let server = Server::bind(
        ServerConfig::new(
            "127.0.0.1:0".parse::<SocketAddr>().expect("addr"),
            CommonAddress::new(1),
        ),
        Recorder {
            steps: Arc::clone(&steps),
        },
    )
    .await
    .expect("station binds");
    let addr = server.local_addr().expect("address");
    server
        .handle()
        .add_point(
            InformationObjectAddress::new(6).expect("address"),
            PointValue::Float(ShortFloat::from_f32(0.0), Qds::default()),
            false,
        )
        .expect("step point");
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

    let mut client = Client::connect(ClientConfig::new(addr, CommonAddress::new(1)));
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        assert!(Instant::now() < deadline, "the data transfer did not start");
        match tokio::time::timeout(Duration::from_millis(200), client.next_event()).await {
            Ok(Some(Event::Connected)) => {
                client.start_data_transfer().await.expect("starts");
            }
            Ok(Some(Event::Delivery(Delivery::Transfer(TransferState::Started)))) => break,
            _ => {}
        }
    }

    send_regulating_step(
        &client,
        RegulatingStepCommand {
            address: 6,
            higher: true,
            select: true,
        },
    )
    .await
    .expect("selected");
    send_regulating_step(
        &client,
        RegulatingStepCommand {
            address: 6,
            higher: true,
            select: false,
        },
    )
    .await
    .expect("executed");

    let mut received = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < deadline && received.len() < 3 {
        if let Ok(Some(Event::Delivery(Delivery::Asdu(asdu)))) =
            tokio::time::timeout(Duration::from_millis(200), client.next_event()).await
        {
            if let Some(found) = answer(&asdu) {
                received.push(found);
            }
        }
    }

    let _ = stop.send(());
    let _ = task.await;

    assert_eq!(
        received,
        vec![
            (6, Answer::Confirmed),
            (6, Answer::Confirmed),
            (6, Answer::Terminated)
        ]
    );
    assert_eq!(*steps.lock().expect("lock"), vec![(6, true)]);
}
