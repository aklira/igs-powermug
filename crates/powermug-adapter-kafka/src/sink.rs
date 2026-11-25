//! Where the records of a batch go. The Kafka sink produces them with an idempotent producer and
//! acknowledges a batch only when the broker has acknowledged every record of it.

use std::future::Future;
use std::time::Duration;

use futures_util::future::try_join_all;
use rdkafka::config::ClientConfig;
use rdkafka::error::KafkaError;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;

use powermug_north::{DeliveryError, Record, RecordSink};

/// How long one record may wait for the broker before it is reported as not delivered.
pub const DELIVERY_TIMEOUT: Duration = Duration::from_secs(30);

/// The sink of a Kafka topic.
pub struct KafkaSink {
    producer: FutureProducer,
    topic: String,
}

impl std::fmt::Debug for KafkaSink {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("KafkaSink")
            .field("topic", &self.topic)
            .finish_non_exhaustive()
    }
}

impl KafkaSink {
    /// Creates the producer. Idempotence and `acks=all` keep a retried record from being written
    /// twice by the producer itself.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError`] when librdkafka cannot create the producer from this configuration.
    pub fn new(brokers: &str, topic: &str) -> Result<Self, KafkaError> {
        let producer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("enable.idempotence", "true")
            .set("acks", "all")
            .set(
                "message.timeout.ms",
                DELIVERY_TIMEOUT.as_millis().to_string(),
            )
            .create()?;
        Ok(Self {
            producer,
            topic: topic.to_owned(),
        })
    }
}

impl RecordSink for KafkaSink {
    fn deliver<'a>(
        &'a mut self,
        records: &'a [Record],
    ) -> impl Future<Output = Result<(), DeliveryError>> + Send + 'a {
        let producer: &FutureProducer = &self.producer;
        let topic: &str = &self.topic;
        async move {
            // The records are queued together, so the broker receives them in a pipeline.
            let sends = records.iter().map(|record| {
                let mut headers = OwnedHeaders::new();
                for (name, value) in &record.headers {
                    headers = headers.insert(Header {
                        key: name,
                        value: Some(value.as_bytes()),
                    });
                }
                producer.send(
                    FutureRecord::to(topic)
                        .key(record.key.as_str())
                        .payload(record.payload.as_slice())
                        .headers(headers),
                    Timeout::After(DELIVERY_TIMEOUT),
                )
            });
            try_join_all(sends)
                .await
                .map_err(|(err, _)| DeliveryError::Refused(err.to_string()))?;
            Ok(())
        }
    }
}
