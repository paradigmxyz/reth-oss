//! Exact field widths, empty request EOF, framing, checksum and bounded IO controls.

use super::*;
use futures::io::Cursor;
use proptest::prelude::*;

const PROTOCOLS: [ControlProtocol; 4] = [
    ControlProtocol::Ping,
    ControlProtocol::MetadataV1,
    ControlProtocol::MetadataV2,
    ControlProtocol::MetadataV3,
];

#[tokio::test]
async fn all_four_protocols_round_trip_exact_requests_and_single_response_widths() {
    for protocol in PROTOCOLS {
        let id = StreamProtocol::new(protocol.id());
        let request = if protocol == ControlProtocol::Ping {
            ControlRequest::Ping(u64::MAX)
        } else {
            ControlRequest::Metadata
        };
        let reply = if protocol == ControlProtocol::Ping {
            ControlReply::Ping(u64::MAX)
        } else {
            let mut metadata = LocalMetadata::default().snapshot(protocol);
            metadata.seq_number = u64::MAX;
            metadata.attnets = [0xaa; 8];
            if metadata.syncnets.is_some() {
                metadata.syncnets = Some(0xf);
            }
            if metadata.custody_group_count.is_some() {
                metadata.custody_group_count = Some(128);
            }
            ControlReply::Metadata(metadata)
        };
        let mut encoded = Cursor::new(Vec::new());
        ControlCodec.write_request(&id, &mut encoded, request).await.unwrap();
        let bytes = encoded.into_inner();
        if protocol != ControlProtocol::Ping {
            assert!(bytes.is_empty());
        }
        assert_eq!(ControlCodec.read_request(&id, &mut Cursor::new(bytes)).await.unwrap(), request);
        let mut encoded = Cursor::new(Vec::new());
        ControlCodec.write_response(&id, &mut encoded, reply).await.unwrap();
        let bytes = encoded.into_inner();
        assert_eq!(bytes[0], 0);
        assert_eq!(
            decode_request(&bytes[1..], CONTROL_LIMITS).unwrap().len(),
            protocol.response_bytes()
        );
        assert_eq!(ControlCodec.read_response(&id, &mut Cursor::new(bytes)).await.unwrap(), reply);
    }
}

#[tokio::test]
async fn metadata_requires_empty_eof_and_rejects_framed_empty_or_any_content() {
    for protocol in PROTOCOLS.into_iter().filter(|p| *p != ControlProtocol::Ping) {
        let id = StreamProtocol::new(protocol.id());
        for bytes in [vec![0], encode_request(&[], CONTROL_LIMITS).unwrap(), vec![1; 342]] {
            assert_eq!(
                ControlCodec.read_request(&id, &mut Cursor::new(bytes)).await.unwrap_err().kind(),
                io::ErrorKind::InvalidData
            );
        }
        let mut out = Cursor::new(Vec::new());
        assert!(ControlCodec.write_request(&id, &mut out, ControlRequest::Ping(0)).await.is_err());
        assert!(out.into_inner().is_empty());
    }
}

#[tokio::test]
async fn no_context_or_second_chunk_or_trailing_bytes_are_accepted() {
    for protocol in PROTOCOLS {
        let id = StreamProtocol::new(protocol.id());
        let payload = vec![0; protocol.response_bytes()];
        let mut good = vec![0];
        good.extend_from_slice(&encode_request(&payload, CONTROL_LIMITS).unwrap());
        let mut trailing = good.clone();
        trailing.push(0);
        let mut duplicate = good.clone();
        duplicate.extend_from_slice(&good);
        let mut context = vec![0, 1, 2, 3, 4];
        context.extend_from_slice(&good[1..]);
        for bad in [trailing, duplicate, context] {
            assert!(ControlCodec.read_response(&id, &mut Cursor::new(bad)).await.is_err());
        }
        assert!(ControlCodec.read_response(&id, &mut Cursor::new(good)).await.is_ok());
    }
}

#[tokio::test]
async fn incomplete_fields_invalid_bits_and_excess_custody_reject_exact_schema() {
    for protocol in PROTOCOLS {
        let id = StreamProtocol::new(protocol.id());
        for length in [0, protocol.response_bytes() - 1, protocol.response_bytes() + 1, 256] {
            let mut bytes = vec![0];
            bytes.extend_from_slice(&encode_request(&vec![0; length], CONTROL_LIMITS).unwrap());
            assert!(ControlCodec.read_response(&id, &mut Cursor::new(bytes)).await.is_err());
        }
        if protocol == ControlProtocol::MetadataV2 || protocol == ControlProtocol::MetadataV3 {
            let mut payload = vec![0; protocol.response_bytes()];
            payload[16] = 16;
            let mut bytes = vec![0];
            bytes.extend_from_slice(&encode_request(&payload, CONTROL_LIMITS).unwrap());
            assert!(ControlCodec.read_response(&id, &mut Cursor::new(bytes)).await.is_err());
        }
    }
    let id = StreamProtocol::new(METADATA_V3);
    let mut metadata = LocalMetadata::default().snapshot(ControlProtocol::MetadataV3);
    metadata.custody_group_count = Some(129);
    assert!(ControlCodec
        .write_response(&id, &mut Cursor::new(Vec::new()), ControlReply::Metadata(metadata))
        .await
        .is_err());
    metadata.custody_group_count = None;
    assert!(ControlCodec
        .write_response(&id, &mut Cursor::new(Vec::new()), ControlReply::Metadata(metadata))
        .await
        .is_err());
}

#[tokio::test]
async fn response_code_rules_preserve_unavailable_as_failure_and_discard_remote_text() {
    let id = StreamProtocol::new(PING_V1);
    for code in [1, 2, 3, 128, 255] {
        let mut bytes = vec![code];
        bytes.extend_from_slice(
            &encode_request(b"private remote error bytes", CONTROL_LIMITS).unwrap(),
        );
        assert_eq!(
            ControlCodec.read_response(&id, &mut Cursor::new(bytes)).await.unwrap(),
            ControlReply::Error(code)
        );
    }
    for code in [4, 127] {
        let mut bytes = vec![code];
        bytes.extend_from_slice(&encode_request(&[], CONTROL_LIMITS).unwrap());
        assert!(ControlCodec.read_response(&id, &mut Cursor::new(bytes)).await.is_err());
    }
}

#[tokio::test]
async fn checksum_corruption_and_all_truncated_prefixes_fail() {
    let id = StreamProtocol::new(PING_V1);
    let mut bytes = vec![0];
    bytes.extend_from_slice(&encode_request(&73u64.to_le_bytes(), CONTROL_LIMITS).unwrap());
    for length in 0..bytes.len() {
        assert!(ControlCodec
            .read_response(&id, &mut Cursor::new(bytes[..length].to_vec()))
            .await
            .is_err());
    }
    bytes[16] ^= 1;
    assert!(ControlCodec.read_response(&id, &mut Cursor::new(bytes)).await.is_err());
}

#[tokio::test]
async fn every_oversized_io_path_and_unknown_protocol_fails_closed() {
    assert!(ControlCodec
        .read_response(
            &StreamProtocol::new(PING_V1),
            &mut Cursor::new(vec![0; MAX_RESPONSE_BYTES + 1])
        )
        .await
        .is_err());
    assert!(ControlCodec
        .read_request(&StreamProtocol::new(PING_V1), &mut Cursor::new(vec![0; 52]))
        .await
        .is_err());
    assert!(ControlCodec
        .read_request(
            &StreamProtocol::new("/eth2/beacon_chain/req/metadata/4/ssz_snappy"),
            &mut Cursor::new(Vec::new())
        )
        .await
        .is_err());
    assert!(ControlCodec
        .write_response(
            &StreamProtocol::new(METADATA_V1),
            &mut Cursor::new(Vec::new()),
            ControlReply::Ping(0)
        )
        .await
        .is_err());
}

#[test]
fn local_metadata_does_not_invent_subscriptions_custody_or_enr_sequence() {
    for protocol in PROTOCOLS {
        let local = LocalMetadata::default().snapshot(protocol);
        assert_eq!(local.seq_number, 0);
        assert_eq!(local.attnets, [0; 8]);
        assert!(local.syncnets.is_none_or(|bits| bits == 0));
        assert!(local.custody_group_count.is_none_or(|groups| groups == 0));
    }
}

proptest! {
    #[test]
    fn arbitrary_wire_bytes_are_bounded_and_never_panic(bytes in prop::collection::vec(any::<u8>(), 0..400)) {
        let _ = decode_request(&bytes, CONTROL_LIMITS);
        if let Some((&code, framed)) = bytes.split_first() && (code == 0 || valid_error_code(code)) {
            let _ = decode_request(framed, CONTROL_LIMITS);
        }
    }
}

#[tokio::test]
async fn custody_metadata_sequence_tracks_only_actual_custody_changes_and_roundtrips_v3() {
    let zero = LocalMetadata::default();
    let four = zero.next_custody(4).unwrap();
    assert_eq!(four.seq_number(), 1);
    assert_eq!(four.next_custody(4).unwrap().seq_number(), 1);
    assert_eq!(four.next_custody(0).unwrap().seq_number(), 2);
    assert!(four.next_custody(5).is_none());
    assert!(LocalMetadata { sequence: u64::MAX, custody: 0 }.next_custody(4).is_none());
    let original = four.snapshot(ControlProtocol::MetadataV3);
    let id = StreamProtocol::new(METADATA_V3);
    let mut wire = Cursor::new(Vec::new());
    ControlCodec.write_response(&id, &mut wire, ControlReply::Metadata(original)).await.unwrap();
    wire.set_position(0);
    assert_eq!(
        ControlCodec.read_response(&id, &mut wire).await.unwrap(),
        ControlReply::Metadata(original)
    );
    assert_eq!(original.custody_group_count, Some(4));
    assert_eq!(four.snapshot(ControlProtocol::MetadataV1).custody_group_count, None);
    assert_eq!(four.snapshot(ControlProtocol::MetadataV2).custody_group_count, None);
}
