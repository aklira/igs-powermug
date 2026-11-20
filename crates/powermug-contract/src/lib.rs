//! Adapter contract of Powermug: `powermug.adapter.v1` (SPECIFICATIONS §6,
//! [GW] §8).
//!
//! This crate holds the generated messages and the decisions taken on them:
//! the handshake, the message bounds, the credit window and the identity of
//! the peer. The gRPC transport over a Unix socket is not written yet.

pub mod bounds;
pub mod client;
pub mod credit;
pub mod handshake;
pub mod peer;
pub mod server;
pub mod service;

/// Generated messages of `powermug.adapter.v1`.
#[allow(
    clippy::all,
    clippy::pedantic,
    missing_docs,
    missing_debug_implementations
)]
pub mod proto {
    pub mod v1 {
        include!(concat!(env!("OUT_DIR"), "/powermug.adapter.v1.rs"));
    }
}

#[cfg(test)]
mod tests {
    use prost::Message;

    use crate::proto::v1 as pb;

    /// The pivot `Envelope` is the one of `powermug-pivot`, not a copy.
    #[test]
    fn event_batch_carries_the_pivot_envelope() {
        let envelope: powermug_pivot::proto::v1::Envelope = powermug_pivot::proto::v1::Envelope {
            site_id: "S3".into(),
            point_id: "S3/TM_001".into(),
            ..Default::default()
        };
        let batch = pb::EventBatch {
            source_epoch: 7,
            batch_sequence: 42,
            events: vec![envelope.clone()],
        };
        let bytes = batch.encode_to_vec();
        let decoded = pb::EventBatch::decode(bytes.as_slice()).expect("decodes");
        assert_eq!(decoded.events, vec![envelope]);
        assert_eq!(decoded.batch_sequence, 42);
    }
}
