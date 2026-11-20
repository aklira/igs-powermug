//! ASDUs as they arrive on the wire, decoded by the codec, converted to envelopes.

use igs_iec104_codec::asdu::Asdu;
use powermug_adapter_iec104::{convert_asdu, Context, ConvertError, Mapping, Unmapped};
use powermug_pivot::proto::v1 as pb;

fn mapping() -> Mapping {
    Mapping::new(vec![
        (1, 1, "S3/TM_001".to_owned()),
        (1, 2, "S3/TM_002".to_owned()),
        (1, 5, "S3/TM_005".to_owned()),
        (1, 0x11, "S3/TM_011".to_owned()),
    ])
    .expect("distinct")
}

fn convert(
    octets: &[u8],
    mapping: &Mapping,
) -> Result<powermug_adapter_iec104::Converted, ConvertError> {
    let asdu = Asdu::decode(octets).expect("decodes");
    let context = Context {
        site_id: "S3",
        mapping,
        revision: 7,
        received_ns: 1_000,
    };
    let mut next = 0u8;
    let mut next_id = move || {
        next += 1;
        [next; 16]
    };
    convert_asdu(&asdu, &context, &mut next_id)
}

fn value_of(envelope: &pb::Envelope) -> Option<pb::value::Kind> {
    envelope.value.as_ref().and_then(|v| v.kind.clone())
}

fn validity_of(envelope: &pb::Envelope) -> i32 {
    envelope.quality.as_ref().map_or(0, |q| q.validity)
}

#[test]
fn single_points_keep_their_state_and_invalid_quality() {
    // M_SP_NA_1, two objects, spontaneous. Second object: ON with IV set.
    let octets = [1, 2, 3, 0, 1, 0, 1, 0, 0, 0x01, 2, 0, 0, 0x81];
    let converted = convert(&octets, &mapping()).expect("converts");

    assert_eq!(converted.envelopes.len(), 2);
    let first = &converted.envelopes[0];
    assert_eq!(first.point_id, "S3/TM_001");
    assert_eq!(value_of(first), Some(pb::value::Kind::BoolValue(true)));
    assert_eq!(validity_of(first), pb::Validity::Good as i32);
    let second = &converted.envelopes[1];
    assert_eq!(second.point_id, "S3/TM_002");
    assert_eq!(validity_of(second), pb::Validity::Invalid as i32);
    assert_eq!(first.mapping_revision, "7");
    assert_eq!(first.source_time_ns, 1_000);
}

#[test]
fn scaled_value_with_overflow_and_not_topical_quality() {
    // M_ME_NB_1, value -2, QDS = OV | NT.
    let octets = [11, 1, 3, 0, 1, 0, 5, 0, 0, 0xFE, 0xFF, 0x41];
    let converted = convert(&octets, &mapping()).expect("converts");

    let envelope = &converted.envelopes[0];
    assert_eq!(envelope.point_id, "S3/TM_005");
    assert_eq!(value_of(envelope), Some(pb::value::Kind::IntValue(-2)));
    assert_eq!(validity_of(envelope), pb::Validity::Questionable as i32);
    let overflow = envelope
        .quality
        .as_ref()
        .is_some_and(|q| q.details.contains(&(pb::Detail::Overflow as i32)));
    assert!(overflow);
}

#[test]
fn normalized_value_with_operator_blocked_quality() {
    // M_ME_NA_1, raw 0x4000 = 0.5, QDS = BL.
    let octets = [9, 1, 3, 0, 1, 0, 1, 0, 0, 0x00, 0x40, 0x10];
    let map = Mapping::new(vec![(1, 1, "S3/TM_001".to_owned())]).expect("distinct");
    let converted = convert(&octets, &map).expect("converts");

    let envelope = &converted.envelopes[0];
    assert_eq!(value_of(envelope), Some(pb::value::Kind::DoubleValue(0.5)));
    assert!(envelope
        .quality
        .as_ref()
        .is_some_and(|q| q.operator_blocked));
}

#[test]
fn sequence_objects_take_consecutive_addresses() {
    // M_SP_NA_1 with SQ = 1: one address 0x10, then 0x11, both ON.
    let octets = [1, 0x82, 3, 0, 1, 0, 0x10, 0, 0, 0x01, 0x01];
    let converted = convert(&octets, &mapping()).expect("converts");

    assert_eq!(converted.envelopes.len(), 1);
    assert_eq!(converted.envelopes[0].point_id, "S3/TM_011");
}

#[test]
fn unmapped_objects_are_counted_not_dropped_silently() {
    // M_SP_NA_1 for address 9, which is not in the table.
    let octets = [1, 1, 3, 0, 1, 0, 9, 0, 0, 0x01];
    let converted = convert(&octets, &mapping()).expect("converts");

    assert_eq!(converted.envelopes, Vec::new());
    assert_eq!(
        converted.unmapped,
        vec![Unmapped {
            common_address: 1,
            address: 9
        }]
    );
}

#[test]
fn a_command_type_is_refused_as_a_whole() {
    // C_IC_NA_1, general interrogation: not a monitoring type.
    let octets = [0x64, 1, 6, 0, 1, 0, 0, 0, 0, 0x14];
    let err = convert(&octets, &mapping()).expect_err("refused");
    assert!(matches!(err, ConvertError::UnsupportedType(_)));
}
