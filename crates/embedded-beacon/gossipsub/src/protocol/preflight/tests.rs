// Copyright 2020 Sigma Prime Pty Ltd.
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the "Software"),
// to deal in the Software without restriction, including without limitation
// the rights to use, copy, modify, merge, publish, distribute, sublicense,
// and/or sell copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
// DEALINGS IN THE SOFTWARE.

use super::*;
use crate::{
    config::ValidationMode, handler::HandlerEvent, protocol::GossipsubCodec, Config, ConfigBuilder,
};
use asynchronous_codec::Decoder;
use bytes::BytesMut;
use std::collections::HashMap;

const LIMITS: IngressLimits = IngressLimits {
    max_published_messages: 16,
    max_subscriptions: 256,
    max_controls: 16,
    max_message_ids: 1024,
    max_prune_peers: 16,
};
const MAX_FRAME: usize = 12 * 1024 * 1024 + 1024;

fn length_prefix(mut value: usize) -> Vec<u8> {
    let mut result = Vec::new();
    loop {
        let low = (value & 0x7f) as u8;
        value >>= 7;
        result.push(if value == 0 { low } else { low | 0x80 });
        if value == 0 {
            return result;
        }
    }
}
fn record(tag: u8, data: &[u8]) -> Vec<u8> {
    let mut result = vec![tag];
    result.extend_from_slice(&length_prefix(data.len()));
    result.extend_from_slice(data);
    result
}
fn framed(payload: &[u8]) -> BytesMut {
    let mut result = BytesMut::new();
    result.extend_from_slice(&length_prefix(payload.len()));
    result.extend_from_slice(payload);
    result
}
fn codec(limits: Option<IngressLimits>) -> GossipsubCodec {
    GossipsubCodec::new(MAX_FRAME, ValidationMode::Anonymous, HashMap::new())
        .with_ingress_limits(limits)
}
fn capacity(payload: &[u8]) {
    let mut source = framed(payload);
    let original = source.clone();
    let pointer = source.as_ptr();
    let error: io::Error = codec(Some(LIMITS)).decode(&mut source).unwrap_err().into();
    assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
    assert_eq!(source, original, "capacity must precede owned decoder consumption");
    assert_eq!(source.as_ptr(), pointer, "capacity must preserve the original buffer");
}
fn same_decode(payload: &[u8]) {
    let mut guarded_source = framed(payload);
    let mut native_source = guarded_source.clone();
    let guarded = codec(Some(LIMITS)).decode(&mut guarded_source).unwrap().unwrap();
    let native = codec(None).decode(&mut native_source).unwrap().unwrap();
    let (
        HandlerEvent::Message { rpc: guarded_rpc, invalid_messages: guarded_invalid },
        HandlerEvent::Message { rpc: native_rpc, invalid_messages: native_invalid },
    ) = (guarded, native)
    else {
        panic!("expected messages")
    };
    assert_eq!(guarded_rpc, native_rpc);
    assert_eq!(format!("{guarded_invalid:?}"), format!("{native_invalid:?}"));
    assert_eq!(guarded_source, native_source);
}

#[test]
fn defaults_preserve_native_decoder_and_builder_opt_in() {
    assert_eq!(Config::default().protocol_config().ingress_limits, None);
    let opted_in = ConfigBuilder::default().ingress_limits(LIMITS).build().unwrap();
    assert_eq!(opted_in.protocol_config().ingress_limits, Some(LIMITS));
    assert_eq!(codec(None).ingress_limits, None);
    let mut source = framed(&[18, 0].repeat(17));
    let HandlerEvent::Message { rpc, .. } = codec(None).decode(&mut source).unwrap().unwrap()
    else {
        panic!("expected messages")
    };
    assert_eq!(rpc.messages.len(), 17, "unconfigured generic codec retains original behavior");
}

#[test]
fn admitted_messages_controls_and_opaque_data_match_native() {
    let mut publication = record(18, &[0xff, 0x00, 0x80, 0x81]);
    publication.extend(record(34, b"/eth2/example/beacon_block/ssz_snappy"));
    let mut payload = record(18, &publication);
    payload.extend(record(10, &[8, 1, 18, 1, b't']));
    let mut ids = record(10, &[0x42; 20]);
    ids.extend(record(10, &[0x12; 20]));
    let mut controls = record(18, &ids);
    controls.extend(record(42, &ids));
    controls.extend(record(26, &[10, 1, b't']));
    let mut peer = record(10, &[1, 2, 3]);
    peer.extend(record(18, &[9, 8, 7]));
    let mut prune = record(10, b"t");
    prune.extend(record(18, &peer));
    prune.extend([24, 60]);
    controls.extend(record(34, &prune));
    payload.extend(record(26, &controls));
    same_decode(&payload);
}

#[test]
fn unknown_fields_and_nonminimal_protobuf_varints_match_native() {
    let mut payload = record(10, &[0x88, 0x00, 0x81, 0x00, 18, 1, b't']);
    payload.extend(record(18, &[0xa2, 0x00, 0x81, 0x00, b't']));
    payload.extend([56, 1, 57]);
    payload.extend([0; 8]);
    payload.extend([58, 2, 0xfe, 0xff, 61]);
    payload.extend([0; 4]);
    same_decode(&payload);
}

#[test]
fn exact_publication_limit_is_admitted_and_next_record_is_capacity() {
    same_decode(&[]);
    same_decode(&[18, 0].repeat(16));
    capacity(&[18, 0].repeat(17));
}

#[test]
fn huge_empty_publication_list_is_rejected_before_owned_decode() {
    // This used to allocate 100,000 owned Message structs from only 200,000 wire bytes.
    capacity(&[18, 0].repeat(100_000));
}

#[test]
fn subscriptions_are_bounded_before_repeated_allocation() {
    same_decode(&[10, 0].repeat(256));
    capacity(&[10, 0].repeat(257));
}

#[test]
fn every_control_action_is_counted_before_allocation() {
    for tag in [10, 18, 26, 34, 42] {
        same_decode(&record(26, &[tag, 0].repeat(16)));
        capacity(&record(26, &[tag, 0].repeat(17)));
    }
}

#[test]
fn message_ids_are_aggregated_across_all_action_kinds() {
    for (action, id_tag) in [(10, 18), (18, 10), (42, 10)] {
        same_decode(&record(26, &record(action, &[id_tag, 0].repeat(1024))));
        capacity(&record(26, &record(action, &[id_tag, 0].repeat(1025))));
    }
    let mut controls = record(10, &[18, 0].repeat(512));
    controls.extend(record(18, &[10, 0].repeat(512)));
    same_decode(&record(26, &controls));
    controls.extend(record(42, &[10, 0]));
    capacity(&record(26, &controls));
}

#[test]
fn prune_peer_limit_is_per_action_and_preserves_backoff() {
    let mut prune = [18, 0].repeat(16);
    prune.extend([24, 60]);
    same_decode(&record(26, &record(34, &prune)));
    let mut two_prunes = record(34, &prune);
    two_prunes.extend(record(34, &prune));
    same_decode(&record(26, &two_prunes));
    capacity(&record(26, &record(34, &[18, 0].repeat(17))));
}

#[test]
fn repeated_optional_control_preserves_last_value_and_checks_discarded_envelopes() {
    let first = record(18, &[10, 0].repeat(1024));
    let second = record(42, &[10, 0].repeat(1024));
    let mut payload = record(26, &first);
    payload.extend(record(26, &second));
    same_decode(&payload);
    let mut oversized = record(26, &record(18, &[10, 0].repeat(1025)));
    oversized.extend(record(26, &[]));
    capacity(&oversized);
}

#[test]
fn malformed_fields_after_capacity_keep_invalid_data_classification() {
    let bad_fields: &[&[u8]] = &[
        &[18, 3, 34, 2, b't'], // truncated topic field in a complete publication
        &[18, 3, 34, 1, 0xff], // invalid UTF-8
        &[63],                 // unknown wire type 7
        &[56, 0x80],           // unterminated unknown varint
        &[58, 4, 1],           // truncated unknown length-delimited field
        &[57, 1],              // truncated unknown fixed64
    ];
    for bad in bad_fields {
        let mut payload = [18, 0].repeat(17);
        payload.extend_from_slice(bad);
        let mut guarded_source = framed(&payload);
        let original = guarded_source.clone();
        let mut native_source = original.clone();
        let guarded: io::Error =
            codec(Some(LIMITS)).decode(&mut guarded_source).unwrap_err().into();
        let native: io::Error = codec(None).decode(&mut native_source).unwrap_err().into();
        assert_eq!(guarded.kind(), io::ErrorKind::InvalidData);
        assert_eq!(guarded.kind(), native.kind());
        assert_eq!(guarded_source, original);
    }
}

#[test]
fn malformed_nested_boundary_cannot_borrow_sibling_bytes() {
    // The pinned reader can read a known field past a malformed nested length and then reset
    // its cursor. The opt-in preflight rejects that structural violation with InvalidData.
    let mut source = framed(&[18, 2, 34, 2, 56, 1]);
    let original = source.clone();
    let error: io::Error = codec(Some(LIMITS)).decode(&mut source).unwrap_err().into();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert_eq!(source, original);
}

#[test]
fn incomplete_and_oversized_frame_behavior_matches_native() {
    for partial in [&[][..], &[0x80], &[5, 18, 0]] {
        let mut guarded = BytesMut::from(partial);
        let mut native = guarded.clone();
        assert!(codec(Some(LIMITS)).decode(&mut guarded).unwrap().is_none());
        assert!(codec(None).decode(&mut native).unwrap().is_none());
        assert_eq!(guarded, native);
        assert_eq!(guarded.as_ref(), partial);
    }
    let prefix = length_prefix(MAX_FRAME + 1);
    let mut guarded = BytesMut::from(prefix.as_slice());
    let mut native = guarded.clone();
    let guarded_error: io::Error = codec(Some(LIMITS)).decode(&mut guarded).unwrap_err().into();
    let native_error: io::Error = codec(None).decode(&mut native).unwrap_err().into();
    assert_eq!(guarded_error.kind(), io::ErrorKind::PermissionDenied);
    assert_eq!(guarded_error.to_string(), native_error.to_string());
    assert_eq!(guarded, native);
}

#[test]
fn malformed_stream_prefix_is_not_capacity() {
    let mut source = BytesMut::from(&[0x80, 0x00][..]);
    let mut native = source.clone();
    let guarded: io::Error = codec(Some(LIMITS)).decode(&mut source).unwrap_err().into();
    let unguarded: io::Error = codec(None).decode(&mut native).unwrap_err().into();
    assert_eq!(guarded.kind(), io::ErrorKind::InvalidData);
    assert_eq!(guarded.kind(), unguarded.kind());
    assert_eq!(source, native);
}

#[test]
fn pointer_width_prefix_errors_match_original_codec() {
    let width = (usize::BITS as usize + 6) / 7;
    let mut prefixes = vec![vec![], vec![0x80], vec![0x80, 0], vec![0x81, 0]];
    for length in 1..width {
        prefixes.push(vec![0xff; length]); // incomplete at every supported continuation depth
    }
    prefixes.push(vec![0xff; width]); // continuation at final byte is overflow
    prefixes.push(vec![0xff; width + 1]);
    let mut nonminimal = vec![0x80; width - 1];
    nonminimal.push(0);
    prefixes.push(nonminimal);
    // Retain the exact native final-byte truncation rule instead of inventing stricter framing.
    for last in [1, 2, 0x7f] {
        let mut prefix = vec![0x80; width - 1];
        prefix.push(last);
        prefixes.push(prefix);
    }
    for prefix in prefixes {
        let mut guarded_source = BytesMut::from(prefix.as_slice());
        let mut native_source = guarded_source.clone();
        let guarded = codec(Some(LIMITS)).decode(&mut guarded_source);
        let native = codec(None).decode(&mut native_source);
        match (guarded, native) {
            (Ok(None), Ok(None)) => {}
            (
                Ok(Some(HandlerEvent::Message { rpc: left, .. })),
                Ok(Some(HandlerEvent::Message { rpc: right, .. })),
            ) => assert_eq!(left, right),
            (Err(left), Err(right)) => {
                let left: io::Error = left.into();
                let right: io::Error = right.into();
                assert_eq!(left.kind(), right.kind());
                assert_eq!(left.to_string(), right.to_string());
            }
            (left, right) => panic!("different prefix classification: {left:?} / {right:?}"),
        }
        assert_eq!(guarded_source, native_source);
    }
}

#[test]
fn valid_prefix_length_and_remainder_are_exact() {
    for value in [0, 1, 127, 128, 255, 16383, 16384, MAX_FRAME, usize::MAX] {
        let mut prefix = length_prefix(value);
        prefix.extend([0x42, 0x24]);
        let (decoded, remaining) = frame_length(&prefix).unwrap().unwrap();
        assert_eq!(decoded, value);
        assert_eq!(remaining, &[0x42, 0x24]);
    }
}
