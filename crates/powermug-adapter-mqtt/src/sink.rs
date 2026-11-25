//! The MQTT 5 sink. Each record is published at `QoS` 1 to `<prefix>/<key>`, its headers as user
//! properties. A batch is delivered when the broker has acknowledged every publication (PUBACK) with
//! an accepting reason. A connection that fails or stalls is replaced before the next attempt, so no
//! publication of an earlier attempt is counted by a later one.

use std::collections::HashSet;
use std::fmt;
use std::time::Duration;

use powermug_north::{DeliveryError, Record, RecordSink};
use rumqttc::v5::mqttbytes::v5::{PubAckReason, PublishProperties};
use rumqttc::v5::mqttbytes::QoS;
use rumqttc::v5::{AsyncClient, Event, EventLoop, Incoming, MqttOptions};
use rumqttc::Outgoing;

use crate::settings::Settings;

/// Publications the client may queue before they are sent. A batch of the core holds at most 1,024
/// events, so a batch always fits.
pub const QUEUE: usize = 1_024;

/// How long a batch may wait for the broker's acknowledgements before the connection is replaced.
pub const ACK_TIMEOUT: Duration = Duration::from_secs(30);

/// The sink of an MQTT 5 broker.
pub struct MqttSink {
    options: MqttOptions,
    topic_prefix: String,
    client: AsyncClient,
    events: EventLoop,
    broken: bool,
}

impl fmt::Debug for MqttSink {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MqttSink")
            .field("topic_prefix", &self.topic_prefix)
            .field("broken", &self.broken)
            .finish_non_exhaustive()
    }
}

impl MqttSink {
    /// A sink for the broker named in `settings`. The connection opens with the first delivery.
    #[must_use]
    pub fn new(settings: &Settings) -> Self {
        let options = MqttOptions::new(
            settings.client_id.clone(),
            settings.broker_host.clone(),
            settings.broker_port,
        );
        let (client, events) = AsyncClient::new(options.clone(), QUEUE);
        Self {
            options,
            topic_prefix: settings.topic_prefix.clone(),
            client,
            events,
            broken: false,
        }
    }

    fn replace_connection(&mut self) {
        let (client, events) = AsyncClient::new(self.options.clone(), QUEUE);
        self.client = client;
        self.events = events;
        self.broken = false;
    }

    async fn publish_all(&mut self, records: &[Record]) -> Result<(), DeliveryError> {
        for record in records {
            let topic = topic_of(&self.topic_prefix, &record.key)?;
            let properties = PublishProperties {
                user_properties: record
                    .headers
                    .iter()
                    .map(|(name, value)| ((*name).to_owned(), value.clone()))
                    .collect(),
                ..Default::default()
            };
            if let Err(err) = self
                .client
                .publish_with_properties(
                    topic,
                    QoS::AtLeastOnce,
                    false,
                    record.payload.clone(),
                    properties,
                )
                .await
            {
                self.broken = true;
                return Err(DeliveryError::Refused(format!(
                    "the client cannot queue a publication: {err}"
                )));
            }
        }

        // Drive the connection until every publication is sent and acknowledged. The packet
        // identifiers of the sent publications are known from the outgoing events.
        let mut outstanding = HashSet::new();
        let mut sent = 0_usize;
        let mut refusal = None;
        while sent < records.len() || !outstanding.is_empty() {
            match self.events.poll().await {
                Ok(Event::Outgoing(Outgoing::Publish(pkid))) if pkid != 0 => {
                    sent += 1;
                    outstanding.insert(pkid);
                }
                Ok(Event::Incoming(Incoming::PubAck(ack))) => {
                    if outstanding.remove(&ack.pkid) && !accepted(ack.reason) {
                        refusal = Some(format!("{:?}", ack.reason));
                    }
                }
                Ok(_) => {}
                Err(err) => {
                    self.broken = true;
                    return Err(DeliveryError::Refused(format!(
                        "the connection to the broker failed: {err}"
                    )));
                }
            }
        }
        match refusal {
            Some(reason) => Err(DeliveryError::Refused(format!(
                "the broker refused a publication: {reason}"
            ))),
            None => Ok(()),
        }
    }
}

/// Whether the broker's reason on a PUBACK accepts the publication.
fn accepted(reason: PubAckReason) -> bool {
    matches!(
        reason,
        PubAckReason::Success | PubAckReason::NoMatchingSubscribers
    )
}

/// The topic of a record: the prefix, then the key, which is a point identifier such as `S3/TM_001`.
fn topic_of(prefix: &str, key: &str) -> Result<String, DeliveryError> {
    if key.is_empty() || key.contains(['+', '#', '\0']) {
        return Err(DeliveryError::Refused(format!(
            "{key:?} is not a valid topic level"
        )));
    }
    Ok(format!("{prefix}/{key}"))
}

impl RecordSink for MqttSink {
    async fn deliver(&mut self, records: &[Record]) -> Result<(), DeliveryError> {
        if self.broken {
            self.replace_connection();
        }
        if let Ok(outcome) = tokio::time::timeout(ACK_TIMEOUT, self.publish_all(records)).await {
            return outcome;
        }
        // A stalled connection is not trusted with the next attempt.
        self.broken = true;
        Err(DeliveryError::Refused(format!(
            "the broker did not acknowledge within {ACK_TIMEOUT:?}"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_topic_is_the_prefix_then_the_point() {
        assert_eq!(
            topic_of("powermug/dc1", "S3/TM_001").expect("valid"),
            "powermug/dc1/S3/TM_001"
        );
    }

    #[test]
    fn a_wildcard_in_the_key_is_refused() {
        assert!(topic_of("powermug", "S3/+").is_err());
        assert!(topic_of("powermug", "S3/#").is_err());
        assert!(topic_of("powermug", "").is_err());
    }

    #[test]
    fn only_accepting_reasons_acknowledge_a_publication() {
        assert!(accepted(PubAckReason::Success));
        assert!(accepted(PubAckReason::NoMatchingSubscribers));
        assert!(!accepted(PubAckReason::NotAuthorized));
        assert!(!accepted(PubAckReason::QuotaExceeded));
    }
}
