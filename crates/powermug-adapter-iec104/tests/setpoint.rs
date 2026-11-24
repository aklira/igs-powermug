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
use powermug_adapter_iec104::command::{
    answer, send_setpoint_float, send_setpoint_normalized, send_setpoint_scaled, Answer,
    CommandError, SetPointFloat, SetPointNormalized, SetPointScaled,
};

/// Records the float set points it accepts.
struct Recorder {
    values: Arc<Mutex<Vec<(u32, f32)>>>,
}

impl Handler for Recorder {
    fn select(&self, _operation: &Operation) -> Result<(), Refusal> {
        Ok(())
    }

    fn execute(&self, operation: &Operation) -> Result<(), Refusal> {
        let value = match operation.value {
            CommandValue::Float(value, _) => value.as_f32(),
            CommandValue::Normalized(value, _) => value.as_f32(),
            CommandValue::Scaled(value, _) => f32::from(value.value()),
            _ => return Ok(()),
        };
        self.values
            .lock()
            .expect("lock")
            .push((operation.address.value(), value));
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

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "an end-to-end scenario, read from top to bottom"
)]
async fn a_selected_normalized_and_scaled_setpoint_are_accepted_with_their_values() {
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
    for address in [8, 9] {
        server
            .handle()
            .add_point(
                InformationObjectAddress::new(address).expect("address"),
                PointValue::Float(ShortFloat::from_f32(0.0), Qds::default()),
                false,
            )
            .expect("setpoint point");
    }
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

    // 1.0 is outside the normalized range: refused before anything is sent.
    assert!(matches!(
        send_setpoint_normalized(
            &client,
            SetPointNormalized {
                address: 8,
                value: 1.0,
                select: true
            }
        )
        .await,
        Err(CommandError::Value(_))
    ));

    // Normalized 0.5 (exact on the F16 grid), scaled -1200.
    send_setpoint_normalized(
        &client,
        SetPointNormalized {
            address: 8,
            value: 0.5,
            select: true,
        },
    )
    .await
    .expect("normalized selected");
    send_setpoint_normalized(
        &client,
        SetPointNormalized {
            address: 8,
            value: 0.5,
            select: false,
        },
    )
    .await
    .expect("normalized executed");
    send_setpoint_scaled(
        &client,
        SetPointScaled {
            address: 9,
            value: -1200,
            select: true,
        },
    )
    .await
    .expect("scaled selected");
    send_setpoint_scaled(
        &client,
        SetPointScaled {
            address: 9,
            value: -1200,
            select: false,
        },
    )
    .await
    .expect("scaled executed");

    let mut received = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < deadline && received.len() < 6 {
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
            (8, Answer::Confirmed),
            (8, Answer::Confirmed),
            (8, Answer::Terminated),
            (9, Answer::Confirmed),
            (9, Answer::Confirmed),
            (9, Answer::Terminated),
        ]
    );
    assert_eq!(*values.lock().expect("lock"), vec![(8, 0.5), (9, -1200.0)]);
}
