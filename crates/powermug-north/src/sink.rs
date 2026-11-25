//! Where the records of a batch go. Each North protocol implements the trait: Kafka with its producer,
//! MQTT 5 with its client.

use std::future::Future;

use thiserror::Error;

use crate::record::Record;

/// A batch was not delivered to the destination.
#[derive(Debug, Error)]
pub enum DeliveryError {
    #[error("the destination did not acknowledge a record: {0}")]
    Refused(String),
}

/// Where the records of a batch go.
pub trait RecordSink {
    /// Delivers `records`. Returns `Ok` only when the destination has acknowledged every record.
    ///
    /// # Errors
    ///
    /// Returns [`DeliveryError`] when a record is not acknowledged. The batch is then delivered again,
    /// so a record may reach its destination twice; its `event_id` tells the copies apart.
    fn deliver<'a>(
        &'a mut self,
        records: &'a [Record],
    ) -> impl Future<Output = Result<(), DeliveryError>> + Send + 'a;
}
