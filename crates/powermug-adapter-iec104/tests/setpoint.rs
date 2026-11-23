//! A float set point from the controlling side to a real station of `igs-iec104`: selected,
//! then executed; the station records the value it accepted, and answers with the activation
//! confirmation and termination. Loopback, one process.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use igs_iec104::client::{Client, ClientConfig, Event};
use igs_iec104::process_image::PointValue;
use igs_iec104::server::{CommandValue, Handler, Operation, Refusal, Server, ServerConfig};
use igs_iec104::Delivery;
use igs_iec104_codec::elements::{Qds, ShortFloat};
use igs_iec104_codec::header::{CommonAddress, InformationObjectAddress};
use igs_iec104_link::TransferState;
use powermug_adapter_iec104::command::{answer, send_setpoint_float, Answer, SetPointFloat};

/// Records the float set points it accepts.
struct Recorder {
    values: Arc<Mutex<Vec<(u32, f32)>>>,
}

impl Handler for Recorder {
    fn select(&self, _operation: &Operation) -> Result<(), Refusal> {
        Ok(())
    }

    fn execute(&self, operation: &Operation) -> Result<(), Refusal> {
        if let CommandValue::Float(value, _) = operation.value {
            self.values
                .lock()
                .expect("lock")
                .push((operation.address.value(), value.as_f32()));
        }
        Ok(())
    }
}

#[tokio::test]
async fn a_selected_float_setpoint_is_accepted_with_its_value_and_terminated() {
    let values = Arc::new(Mutex::new(Vec::new()));
    let server = Server::bind(
        ServerConfig::new(
            "127.0.0.1:0".parse::<SocketAddr>().expect("addr"),
            CommonAddress::new(1),
        ),
        Recorder {
            values: Arc::clone(&values),
        },
    )
    .await
    .expect("station binds");
    let addr = server.local_addr().expect("address");
    server
        .handle()
        .add_point(
            InformationObjectAddress::new(7).expect("address"),
            PointValue::Float(ShortFloat::from_f32(0.0), Qds::default()),
            false,
        )
        .expect("setpoint point");
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

    send_setpoint_float(
        &client,
        SetPointFloat {
            address: 7,
            value: 42.5,
            select: true,
        },
    )
    .await
    .expect("selected");
    send_setpoint_float(
        &client,
        SetPointFloat {
            address: 7,
            value: 42.5,
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
            (7, Answer::Confirmed),
            (7, Answer::Confirmed),
            (7, Answer::Terminated)
        ]
    );
    assert_eq!(*values.lock().expect("lock"), vec![(7, 42.5)]);
}
