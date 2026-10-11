//! Independently constructed protocol vectors for consensus-specs v1.7.0-beta.3.
//! Constants use fixed SSZ field bytes, literal-only raw Snappy blocks and a separate
//! bitwise CRC32C encoder (polynomial 0x82f63b78, mask rotate-right 15 + 0xa282ead8).
//! They are wire vectors, not a capture or a public-network interoperability claim.

use super::*;
use proptest::prelude::*;
use reth_embedded_beacon_config::{BlobParameters, ChainConfig, ForkSchedule};

const STATUS_V1_SSZ: &str = "010203041111111111111111111111111111111111111111111111111111111111111111080706050403020122222222222222222222222222222222222222222222222222222222222222221817161514131211";
const STATUS_V1_FRAME: &str = "54ff060000734e61507059005b0000519e1d8654f053010203041111111111111111111111111111111111111111111111111111111111111111080706050403020122222222222222222222222222222222222222222222222222222222222222221817161514131211";
const STATUS_V2_SSZ: &str = "0102030411111111111111111111111111111111111111111111111111111111111111110807060504030201222222222222222222222222222222222222222222222222222222222222222218171615141312112827262524232221";
const STATUS_V2_FRAME: &str = "5cff060000734e6150705900630000b5c5717f5cf05b0102030411111111111111111111111111111111111111111111111111111111111111110807060504030201222222222222222222222222222222222222222222222222222222222222222218171615141312112827262524232221";
const LIMITS: Limits = Limits { max_payload: 200000, max_compressed: 240000 };

fn hex(bytes: &str) -> Vec<u8> {
    bytes
        .as_bytes()
        .chunks_exact(2)
        .map(|b| {
            let digit = |b: u8| if b <= b'9' { b - b'0' } else { b - b'a' + 10 };
            digit(b[0]) * 16 + digit(b[1])
        })
        .collect()
}

fn chunk(config: &ChainConfig, epoch: u64, payload: &[u8]) -> Vec<u8> {
    let mut out = vec![0];
    out.extend_from_slice(&config.fork_digest(epoch).unwrap());
    out.extend_from_slice(&encode_request(payload, LIMITS).unwrap());
    out
}

#[test]
fn independent_literal_status_vectors_decode_exact_fields() {
    let payload = hex(STATUS_V1_SSZ);
    assert_eq!(decode_request(&hex(STATUS_V1_FRAME), LIMITS).unwrap(), payload);
    assert_eq!(
        Status::decode(&payload).unwrap(),
        Status {
            fork_digest: [1, 2, 3, 4],
            finalized_root: [0x11; 32],
            finalized_epoch: 0x0102030405060708,
            head_root: [0x22; 32],
            head_slot: 0x1112131415161718
        }
    );
    let payload = hex(STATUS_V2_SSZ);
    assert_eq!(decode_request(&hex(STATUS_V2_FRAME), LIMITS).unwrap(), payload);
    assert_eq!(StatusV2::decode(&payload).unwrap().earliest_available_slot, 0x2122232425262728);
}

#[test]
fn byte_fragmentation_never_releases_request_before_eof() {
    for (protocol, wire, expected) in [
        (Protocol::StatusV1, STATUS_V1_FRAME, STATUS_V1_SSZ),
        (Protocol::StatusV2, STATUS_V2_FRAME, STATUS_V2_SSZ),
    ] {
        let mut decoder = StreamDecoder::request(protocol, LIMITS, 1).unwrap();
        for byte in hex(wire) {
            decoder.push(&[byte]).unwrap();
            assert_eq!(decoder.next_message().unwrap(), DecodeProgress::NeedMore);
        }
        decoder.finish_input();
        assert_eq!(
            decoder.next_message().unwrap(),
            DecodeProgress::Message(WireMessage::Request(hex(expected)))
        );
        assert_eq!(decoder.next_message().unwrap(), DecodeProgress::Finished);
        assert_eq!(decoder.push(&[0]), Err(WireError::Closed));
    }
}

#[test]
fn every_truncated_golden_prefix_and_trailing_request_is_rejected() {
    let wire = hex(STATUS_V2_FRAME);
    for end in 0..wire.len() {
        let mut decoder = StreamDecoder::request(Protocol::StatusV2, LIMITS, 1).unwrap();
        decoder.push(&wire[..end]).unwrap();
        decoder.finish_input();
        assert_eq!(decoder.next_message(), Err(WireError::Truncated), "prefix {end}");
        assert_eq!(decoder.next_message(), Err(WireError::Closed));
    }
    let mut decoder = StreamDecoder::request(Protocol::StatusV2, LIMITS, 1).unwrap();
    decoder.push(&wire).unwrap();
    decoder.push(&[0]).unwrap();
    assert_eq!(decoder.next_message(), Err(WireError::TrailingBytes));
}

#[test]
fn golden_checksum_and_reserved_frame_corruption_fail() {
    let mut wire = hex(STATUS_V1_FRAME);
    wire[15] ^= 1;
    let mut decoder = StreamDecoder::request(Protocol::StatusV1, LIMITS, 1).unwrap();
    decoder.push(&wire).unwrap();
    decoder.finish_input();
    assert_eq!(decoder.next_message(), Err(WireError::Compression));
    wire = hex(STATUS_V1_FRAME);
    wire[11] = 0x7f;
    let mut decoder = StreamDecoder::request(Protocol::StatusV1, LIMITS, 1).unwrap();
    decoder.push(&wire).unwrap();
    assert_eq!(decoder.next_message(), Err(WireError::Compression));
}

#[test]
fn declared_size_bounds_apply_before_waiting_for_skippable_frame() {
    // Ping declares 8 bytes, so max_compressed_len is 41, despite a much larger global limit.
    let mut wire = vec![8];
    wire.extend_from_slice(&[0xff, 6, 0, 0, 115, 78, 97, 80, 112, 89]);
    wire.extend_from_slice(&[0x80, 255, 255, 255]);
    let mut decoder = StreamDecoder::request(Protocol::PingV1, LIMITS, 1).unwrap();
    decoder.push(&wire).unwrap();
    assert_eq!(decoder.next_message(), Err(WireError::Oversized));
    assert_eq!(decode_request(&wire, LIMITS), Err(WireError::Oversized));
    let mut decoder = StreamDecoder::request(Protocol::StatusV2, LIMITS, 1).unwrap();
    decoder.push(&[93]).unwrap();
    assert_eq!(decoder.next_message(), Err(WireError::Ssz));
    assert_eq!(max_compressed_len(usize::MAX), Err(WireError::Oversized));
}

#[test]
fn custom_errors_are_bounded_context_free_and_terminal() {
    let config = ChainConfig::hoodi();
    for code in [1, 2, 128, 200, 255] {
        let mut wire = vec![code];
        wire.extend_from_slice(&encode_request(b"busy", LIMITS).unwrap());
        let mut decoder = StreamDecoder::response(Protocol::StatusV2, LIMITS, 1, &config).unwrap();
        for byte in wire {
            decoder.push(&[byte]).unwrap();
        }
        assert_eq!(
            decoder.next_message().unwrap(),
            DecodeProgress::Message(WireMessage::Response(ResponseChunk {
                code,
                context: None,
                payload: b"busy".to_vec()
            }))
        );
        assert_eq!(decoder.next_message().unwrap(), DecodeProgress::Finished);
    }
    for code in [4, 127, 3] {
        let mut decoder = StreamDecoder::response(Protocol::StatusV2, LIMITS, 1, &config).unwrap();
        decoder.push(&[code]).unwrap();
        assert_eq!(decoder.next_message(), Err(WireError::ResponseCode));
    }
    let mut wire = vec![200];
    wire.extend(encode_request(&[0; 257], LIMITS).unwrap());
    let mut decoder = StreamDecoder::response(Protocol::StatusV2, LIMITS, 1, &config).unwrap();
    decoder.push(&wire).unwrap();
    assert_eq!(decoder.next_message(), Err(WireError::Oversized));
}

#[test]
fn response_chunks_are_incremental_and_count_is_not_unbounded() {
    let config = ChainConfig::hoodi();
    let mut wire = chunk(&config, 0, b"first");
    wire.extend(chunk(&config, 2048, b"second"));
    let mut decoder =
        StreamDecoder::response(Protocol::BlocksByRangeV2, LIMITS, 2, &config).unwrap();
    let mut received = Vec::new();
    for byte in wire {
        decoder.push(&[byte]).unwrap();
        match decoder.next_message().unwrap() {
            DecodeProgress::Message(WireMessage::Response(chunk)) => received.push(chunk.payload),
            DecodeProgress::NeedMore => {}
            progress => panic!("unexpected progress {progress:?}"),
        }
    }
    assert_eq!(received, [b"first".to_vec(), b"second".to_vec()]);
    assert_eq!(decoder.next_message().unwrap(), DecodeProgress::Finished);
    let mut decoder =
        StreamDecoder::response(Protocol::BlocksByRootV2, LIMITS, 1, &config).unwrap();
    let mut wire = chunk(&config, 0, b"first");
    wire.extend(chunk(&config, 0, b"extra"));
    decoder.push(&wire).unwrap();
    assert_eq!(decoder.next_message(), Err(WireError::TrailingBytes));
    assert!(matches!(
        StreamDecoder::response(Protocol::StatusV2, LIMITS, 2, &config),
        Err(WireError::Protocol)
    ));
}

#[test]
fn historical_context_and_blob_schedule_require_exact_object_slot() {
    let mut config = ChainConfig::hoodi();
    config.forks = ForkSchedule::new(config.forks.activations()[..7].to_vec())
        .unwrap()
        .with_gloas(60000, [0x80, 0, 9, 0x10])
        .unwrap();
    config.blob_schedule.push(BlobParameters { epoch: 61000, max_blobs_per_block: 30 });
    config.validate().unwrap();
    let response = ResponseChunk {
        code: 0,
        context: Some(config.fork_digest(60000).unwrap()),
        payload: vec![],
    };
    assert_eq!(
        validate_response_context(
            &config,
            Protocol::PayloadEnvelopesByRootV1,
            &response,
            60000 * 32
        ),
        Ok(())
    );
    assert_eq!(
        validate_response_context(
            &config,
            Protocol::PayloadEnvelopesByRootV1,
            &response,
            59999 * 32
        ),
        Err(WireError::ForkContext)
    );
    assert_eq!(
        validate_response_context(
            &config,
            Protocol::PayloadEnvelopesByRootV1,
            &response,
            61000 * 32
        ),
        Err(WireError::ForkContext)
    );
    let mut decoder =
        StreamDecoder::response(Protocol::PayloadEnvelopesByRootV1, LIMITS, 1, &config).unwrap();
    decoder.push(&chunk(&config, 61000, b"envelope")).unwrap();
    assert!(matches!(decoder.next_message(), Ok(DecodeProgress::Message(_))));
    let mut decoder =
        StreamDecoder::response(Protocol::PayloadEnvelopesByRootV1, LIMITS, 1, &config).unwrap();
    decoder.push(&chunk(&config, 50688, b"wrong fork")).unwrap();
    assert_eq!(decoder.next_message(), Err(WireError::ForkContext));
    let mut other_network = config.clone();
    other_network.genesis_validators_root = [9; 32];
    let mut decoder =
        StreamDecoder::response(Protocol::BlocksByRootV2, LIMITS, 1, &config).unwrap();
    decoder.push(&chunk(&other_network, 60000, b"wrong genesis")).unwrap();
    assert_eq!(decoder.next_message(), Err(WireError::ForkContext));
}

#[test]
fn range_and_root_request_ssz_contracts_are_checked_before_admission() {
    let mut request = blocks_by_range(10, 2, 1, 1024).unwrap();
    request[8..16].copy_from_slice(&3u64.to_le_bytes());
    let mut decoder = StreamDecoder::request(Protocol::BlocksByRangeV2, LIMITS, 2).unwrap();
    decoder.push(&encode_request(&request, LIMITS).unwrap()).unwrap();
    decoder.finish_input();
    assert_eq!(decoder.next_message(), Err(WireError::Range));
    let mut decoder = StreamDecoder::request(Protocol::BlocksByRootV2, LIMITS, 2).unwrap();
    decoder.push(&encode_request(&[0; 33], LIMITS).unwrap()).unwrap();
    decoder.finish_input();
    assert_eq!(decoder.next_message(), Err(WireError::Ssz));
    assert_eq!(
        Protocol::from_id("/eth2/beacon_chain/req/status/3/ssz_snappy"),
        Err(WireError::Protocol)
    );
}

#[test]
fn multi_frame_message_and_missing_single_response_eof_are_checked() {
    let config = ChainConfig::hoodi();
    let payload = vec![7; 150000];
    let wire = chunk(&config, 0, &payload);
    let mut decoder =
        StreamDecoder::response(Protocol::BlocksByRootV2, LIMITS, 1, &config).unwrap();
    for bytes in wire.chunks(19) {
        decoder.push(bytes).unwrap();
        if let DecodeProgress::Message(WireMessage::Response(response)) =
            decoder.next_message().unwrap()
        {
            assert_eq!(response.payload, payload);
        }
    }
    assert_eq!(decoder.next_message().unwrap(), DecodeProgress::Finished);
    let mut decoder = StreamDecoder::response(Protocol::StatusV2, LIMITS, 1, &config).unwrap();
    decoder.finish_input();
    assert_eq!(decoder.next_message(), Err(WireError::Truncated));
    let mut decoder =
        StreamDecoder::response(Protocol::BlocksByRootV2, LIMITS, 1, &config).unwrap();
    decoder.finish_input();
    assert_eq!(decoder.next_message().unwrap(), DecodeProgress::Finished);
}

proptest! {
    #[test]
    fn arbitrary_fragmented_request_streams_are_bounded(bytes in proptest::collection::vec(any::<u8>(), 0..500),
        split in 1usize..32)
    {
        let mut decoder = StreamDecoder::request(Protocol::StatusV2,
            Limits { max_payload: 1024, max_compressed: 2048 }, 1).unwrap();
        for piece in bytes.chunks(split) {
            if decoder.push(piece).is_err() || decoder.next_message().is_err() { return Ok(()); }
        }
        decoder.finish_input();
        let _ = decoder.next_message();
    }
}
