//! The mapping of a delivered envelope to a Kafka record (SPECIFICATIONS 7.4, FUN-SAF-003).
//!
//! The payload is the envelope in Protobuf, as the journal holds it. The key is the point, so the
//! events of one point stay in order within a partition. The headers carry what a consumer needs to
//! recognise a duplicate (`event_id`, which is unique per event) and to tell a replay from a live
//! value.

use std::fmt::Write;

use powermug_pivot::proto::v1::Envelope;
use prost::Message;

/// The `UUIDv7` of the event, in lowercase hex. Unique per event: a consumer drops a repeat.
pub const EVENT_ID: &str = "powermug.event_id";
/// The partition revision that mapped the point.
pub const MAPPING_REVISION: &str = "powermug.mapping_revision";
/// `true` for an event replayed from the journal, `false` for a live one.
pub const REPLAY: &str = "powermug.replay";

/// One record of a Kafka topic, before it is sent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub key: String,
    pub payload: Vec<u8>,
    pub headers: Vec<(&'static str, String)>,
}

/// The record of one delivered envelope.
#[must_use]
pub fn record_of(envelope: &Envelope) -> Record {
    let key = if envelope.point_id.is_empty() {
        envelope.site_id.clone()
    } else {
        envelope.point_id.clone()
    };
    let mut headers = vec![
        (EVENT_ID, to_hex(&envelope.event_id)),
        (REPLAY, envelope.replay.to_string()),
    ];
    if !envelope.mapping_revision.is_empty() {
        headers.push((MAPPING_REVISION, envelope.mapping_revision.clone()));
    }
    Record {
        key,
        payload: envelope.encode_to_vec(),
        headers,
    }
}

fn to_hex(bytes: &[u8]) -> String {
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut out, byte| {
            let _ = write!(out, "{byte:02x}");
            out
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn envelope() -> Envelope {
        Envelope {
            event_id: vec![0x01, 0xab, 0xff],
            site_id: "S3".into(),
            point_id: "S3/TM_001".into(),
            mapping_revision: "rev-7".into(),
            replay: true,
            ..Default::default()
        }
    }

    #[test]
    fn the_key_is_the_point_and_the_payload_is_the_envelope() {
        let source = envelope();
        let record = record_of(&source);
        assert_eq!(record.key, "S3/TM_001");
        assert_eq!(
            Envelope::decode(record.payload.as_slice()).expect("decodes"),
            source
        );
    }

    #[test]
    fn the_headers_identify_the_event_and_its_origin() {
        let record = record_of(&envelope());
        assert!(record.headers.contains(&(EVENT_ID, "01abff".into())));
        assert!(record.headers.contains(&(REPLAY, "true".into())));
        assert!(record.headers.contains(&(MAPPING_REVISION, "rev-7".into())));
    }

    #[test]
    fn an_event_without_a_point_is_keyed_by_its_site() {
        let source = Envelope {
            point_id: String::new(),
            ..envelope()
        };
        assert_eq!(record_of(&source).key, "S3");
    }

    #[test]
    fn an_empty_mapping_revision_is_not_sent() {
        let source = Envelope {
            mapping_revision: String::new(),
            ..envelope()
        };
        assert!(record_of(&source)
            .headers
            .iter()
            .all(|(name, _)| *name != MAPPING_REVISION));
    }
}
