//! Bounded consensus wire codecs. Transport and gossip scheduling are separate concerns.

use reth_embedded_beacon_primitives::{sha256, Root};
use snap::{read::FrameDecoder, write::FrameEncoder};
use std::io::{Cursor, Read, Write};

pub mod custody;
mod endpoint;
mod fork_id;
mod handshake;
mod stream;

#[cfg(feature = "transport")]
pub mod transport;

#[cfg(feature = "transport")]
pub mod block_rpc;

#[cfg(feature = "transport")]
pub mod envelope_rpc;

#[cfg(feature = "transport")]
pub mod availability_rpc;

#[cfg(feature = "transport")]
pub mod control_rpc;

#[cfg(feature = "transport")]
pub mod gossip;

#[cfg(feature = "discovery")]
pub mod discovery;

#[cfg(feature = "discovery")]
pub mod peer;

#[cfg(test)]
mod vectors;

#[cfg(all(test, feature = "transport"))]
mod test_blocks;

#[cfg(all(test, feature = "transport"))]
mod test_envelopes;

pub use endpoint::EndpointPolicy;
pub use fork_id::{
    decode_custody_group_count, encode_custody_group_count, DiscoveryCompatibility,
    DiscoveryFieldError, DiscoveryForkFields, EnrForkId,
};
pub use handshake::{validate_status, HandshakeError, StatusCompatibility};
pub use stream::{validate_response_context, DecodeProgress, Protocol, StreamDecoder, WireMessage};

/// Req/Resp protocol identifiers use exact version matching.
pub const STATUS_V1: &str = "/eth2/beacon_chain/req/status/1/ssz_snappy";
/// Blocks returned using fork digest response context.
pub const BLOCKS_BY_RANGE_V2: &str = "/eth2/beacon_chain/req/beacon_blocks_by_range/2/ssz_snappy";
/// Blocks requested by their consensus roots.
pub const BLOCKS_BY_ROOT_V2: &str = "/eth2/beacon_chain/req/beacon_blocks_by_root/2/ssz_snappy";
/// Current slot-independent ping protocol.
pub const PING_V1: &str = "/eth2/beacon_chain/req/ping/1/ssz_snappy";

/// Peer status in the Phase 0 status wire schema.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Status {
    /// Network digest including the active fork and any blob parameter upgrade.
    pub fork_digest: [u8; 4],
    /// Latest finalized consensus block root.
    pub finalized_root: Root,
    /// Latest finalized epoch.
    pub finalized_epoch: u64,
    /// Peer head consensus block root.
    pub head_root: Root,
    /// Peer head slot.
    pub head_slot: u64,
}

/// Limits are caller-supplied because fork-specific payload sizes vary.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    /// Maximum decompressed bytes.
    pub max_payload: usize,
    /// Maximum on-wire compressed bytes, including frame headers.
    pub max_compressed: usize,
}

/// Protocol rejection before any consensus import.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum WireError {
    /// The length prefix was not a canonical u64 unsigned varint.
    #[error("invalid length prefix")]
    LengthPrefix,
    /// Either the compressed or decompressed message exceeds its configured bound.
    #[error("message exceeds configured limit")]
    Oversized,
    /// Snappy framing, checksums or declared uncompressed length were invalid.
    #[error("invalid SSZ-snappy frame")]
    Compression,
    /// An SSZ fixed length or value was invalid.
    #[error("invalid SSZ message")]
    Ssz,
    /// Requested slots overflow or exceed the range limit.
    #[error("invalid block range")]
    Range,
    /// The response status byte is unknown.
    #[error("invalid response code")]
    ResponseCode,
    /// A stream ended before its complete message arrived.
    #[error("truncated protocol stream")]
    Truncated,
    /// Bytes followed a completed request or terminal response.
    #[error("bytes after terminal message")]
    TrailingBytes,
    /// The stream was already closed or rejected.
    #[error("protocol stream is closed")]
    Closed,
    /// The chunk uses an unknown or inappropriate historical fork digest.
    #[error("invalid response fork context")]
    ForkContext,
    /// The protocol or its stream budget is unsupported.
    #[error("invalid protocol configuration")]
    Protocol,
}

/// An individually framed response chunk; success context is fork-specific.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResponseChunk {
    /// Zero is success; 1 through 3 and 128 through 255 are protocol errors.
    pub code: u8,
    /// Only success chunks have a context digest.
    pub context: Option<[u8; 4]>,
    /// Decompressed SSZ bytes or the bounded error message.
    pub payload: Vec<u8>,
}

impl Status {
    /// Serialize this fixed-size SSZ container.
    pub fn encode(&self) -> [u8; 84] {
        let mut out = [0; 84];
        out[..4].copy_from_slice(&self.fork_digest);
        out[4..36].copy_from_slice(&self.finalized_root);
        out[36..44].copy_from_slice(&self.finalized_epoch.to_le_bytes());
        out[44..76].copy_from_slice(&self.head_root);
        out[76..84].copy_from_slice(&self.head_slot.to_le_bytes());
        out
    }

    /// Decode status without accepting trailing SSZ bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, WireError> {
        if bytes.len() != 84 {
            return Err(WireError::Ssz);
        }
        Ok(Self {
            fork_digest: bytes[..4].try_into().map_err(|_| WireError::Ssz)?,
            finalized_root: bytes[4..36].try_into().map_err(|_| WireError::Ssz)?,
            finalized_epoch: u64::from_le_bytes(
                bytes[36..44].try_into().map_err(|_| WireError::Ssz)?,
            ),
            head_root: bytes[44..76].try_into().map_err(|_| WireError::Ssz)?,
            head_slot: u64::from_le_bytes(bytes[76..84].try_into().map_err(|_| WireError::Ssz)?),
        })
    }
}

/// Compute the historical base `ForkData` digest. Current network digests must use
/// `ChainConfig::fork_digest` to include the blob schedule.
pub fn fork_digest(version: [u8; 4], genesis_validators_root: Root) -> [u8; 4] {
    let mut preimage = [0; 64];
    preimage[..4].copy_from_slice(&version);
    preimage[32..].copy_from_slice(&genesis_validators_root);
    sha256(&preimage)[..4].try_into().expect("fixed digest width")
}

/// Encode canonical unsigned varint and framed (not raw) Snappy.
pub fn encode_request(payload: &[u8], limits: Limits) -> Result<Vec<u8>, WireError> {
    if payload.len() > limits.max_payload {
        return Err(WireError::Oversized);
    }
    let mut out = Vec::new();
    write_varint(payload.len() as u64, &mut out);
    let mut encoder = FrameEncoder::new(Vec::new());
    encoder.write_all(payload).map_err(|_| WireError::Compression)?;
    let mut compressed = encoder.into_inner().map_err(|_| WireError::Compression)?;
    if compressed.is_empty() {
        compressed.extend_from_slice(&[0xff, 6, 0, 0, 115, 78, 97, 80, 112, 89]);
    }
    if compressed.len() > limits.max_compressed.min(max_compressed_len(payload.len())?) {
        return Err(WireError::Oversized);
    }
    out.extend_from_slice(&compressed);
    Ok(out)
}

/// Decode exactly one complete request with allocation bounds enforced first.
/// Use `decode_request_prefix` to delimit sequential response chunks.
pub fn decode_request(bytes: &[u8], limits: Limits) -> Result<Vec<u8>, WireError> {
    let (payload, consumed) = decode_request_prefix(bytes, limits)?;
    if consumed != bytes.len() {
        return Err(WireError::Compression);
    }
    Ok(payload)
}

/// Decode one chunk from a stream buffer, returning its exact consumed wire length.
/// Snappy frame boundaries are inspected without consuming bytes from the following response.
pub fn decode_request_prefix(bytes: &[u8], limits: Limits) -> Result<(Vec<u8>, usize), WireError> {
    const IDENTIFIER: [u8; 10] = [0xff, 6, 0, 0, 115, 78, 97, 80, 112, 89];
    let (declared, prefix) = read_varint(bytes)?;
    let declared = usize::try_from(declared).map_err(|_| WireError::Oversized)?;
    if declared > limits.max_payload {
        return Err(WireError::Oversized);
    }
    let compressed_limit = limits.max_compressed.min(max_compressed_len(declared)?);
    if !bytes[prefix..].starts_with(&IDENTIFIER) {
        return Err(WireError::Compression);
    }
    let mut consumed = prefix + IDENTIFIER.len();
    let mut out = Vec::with_capacity(declared);
    while out.len() < declared {
        let header_end = consumed.checked_add(4).ok_or(WireError::Oversized)?;
        let header = bytes.get(consumed..header_end).ok_or(WireError::Compression)?;
        let kind = header[0];
        let length =
            usize::from(header[1]) | (usize::from(header[2]) << 8) | (usize::from(header[3]) << 16);
        let end = header_end.checked_add(length).ok_or(WireError::Oversized)?;
        if end - prefix > compressed_limit {
            return Err(WireError::Oversized);
        }
        let data = bytes.get(header_end..end).ok_or(WireError::Compression)?;
        match kind {
            0 | 1 => {
                // Both data frame types carry a four-byte masked CRC32C.
                if data.len() < 4 {
                    return Err(WireError::Compression);
                }
                let size = if kind == 0 {
                    snap::raw::decompress_len(&data[4..]).map_err(|_| WireError::Compression)?
                } else {
                    data.len() - 4
                };
                if size > 65536 || size > declared - out.len() {
                    return Err(WireError::Compression);
                }
                let mut frame = Vec::with_capacity(IDENTIFIER.len() + 4 + data.len());
                frame.extend_from_slice(&IDENTIFIER);
                frame.extend_from_slice(&bytes[consumed..end]);
                let mut decoder = FrameDecoder::new(Cursor::new(frame));
                let before = out.len();
                decoder.read_to_end(&mut out).map_err(|_| WireError::Compression)?;
                if out.len() - before != size {
                    return Err(WireError::Compression);
                }
            }
            0xff if data == &IDENTIFIER[4..] => {}
            0x80..=0xfe => {}
            _ => return Err(WireError::Compression),
        }
        consumed = end;
    }
    if consumed - prefix > compressed_limit {
        return Err(WireError::Oversized);
    }
    Ok((out, consumed))
}

/// Decode a single delimited response chunk with optional fork context.
pub fn decode_response_chunk(
    bytes: &[u8],
    success_has_context: bool,
    limits: Limits,
) -> Result<ResponseChunk, WireError> {
    let Some((&code, rest)) = bytes.split_first() else { return Err(WireError::ResponseCode) };
    if (4..128).contains(&code) {
        return Err(WireError::ResponseCode);
    }
    let (context, rest) = if code == 0 && success_has_context {
        if rest.len() < 4 {
            return Err(WireError::Ssz);
        }
        (Some(rest[..4].try_into().map_err(|_| WireError::Ssz)?), &rest[4..])
    } else {
        (None, rest)
    };
    let limits = if code == 0 {
        limits
    } else {
        Limits { max_payload: limits.max_payload.min(256), ..limits }
    };
    Ok(ResponseChunk { code, context, payload: decode_request(rest, limits)? })
}

/// Validate range requests and encode their three fixed uint64 fields.
pub fn blocks_by_range(
    start_slot: u64,
    count: u64,
    step: u64,
    max_blocks: u64,
) -> Result<[u8; 24], WireError> {
    if count == 0 || count > max_blocks || step != 1 || start_slot.checked_add(count - 1).is_none()
    {
        return Err(WireError::Range);
    }
    let mut out = [0; 24];
    out[..8].copy_from_slice(&start_slot.to_le_bytes());
    out[8..16].copy_from_slice(&count.to_le_bytes());
    out[16..].copy_from_slice(&step.to_le_bytes());
    Ok(out)
}

fn write_varint(mut value: u64, out: &mut Vec<u8>) {
    while value >= 128 {
        out.push((value as u8 & 127) | 128);
        value >>= 7;
    }
    out.push(value as u8);
}

fn read_varint(bytes: &[u8]) -> Result<(u64, usize), WireError> {
    let mut value = 0;
    for (index, &byte) in bytes.iter().take(10).enumerate() {
        if index == 9 && byte > 1 {
            return Err(WireError::LengthPrefix);
        }
        value |= u64::from(byte & 127) << (index * 7);
        if byte & 128 == 0 {
            if index > 0 && byte == 0 {
                return Err(WireError::LengthPrefix);
            }
            return Ok((value, index + 1));
        }
    }
    Err(WireError::LengthPrefix)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    const LIMITS: Limits = Limits { max_payload: 65536, max_compressed: 100000 };

    #[test]
    fn status_rejects_extra_bytes_and_roundtrips() {
        let status = Status {
            fork_digest: [4; 4],
            finalized_root: [5; 32],
            finalized_epoch: 17,
            head_root: [6; 32],
            head_slot: 600,
        };
        assert_eq!(Status::decode(&status.encode()), Ok(status));
        assert_eq!(Status::decode(&[0; 85]), Err(WireError::Ssz));
    }

    #[test]
    fn malformed_lengths_and_compression_are_rejected() {
        assert_eq!(decode_request(&[0x80, 0], LIMITS), Err(WireError::LengthPrefix));
        assert_eq!(decode_request(&[0xff; 11], LIMITS), Err(WireError::LengthPrefix));
        assert_eq!(decode_request(&[1, 2, 3], LIMITS), Err(WireError::Compression));
        let mut encoded = encode_request(b"payload", LIMITS).unwrap();
        encoded[0] += 1;
        assert_eq!(decode_request(&encoded, LIMITS), Err(WireError::Compression));
        encoded[0] -= 2;
        assert_eq!(decode_request(&encoded, LIMITS), Err(WireError::Compression));
    }

    #[test]
    fn checks_compressed_and_decompressed_bounds() {
        let encoded = encode_request(&[0; 100], LIMITS).unwrap();
        assert_eq!(
            decode_request(&encoded, Limits { max_payload: 99, ..LIMITS }),
            Err(WireError::Oversized)
        );
        assert_eq!(
            decode_request(&encoded, Limits { max_compressed: 1, ..LIMITS }),
            Err(WireError::Oversized)
        );
        assert_eq!(blocks_by_range(u64::MAX, 2, 1, 1024), Err(WireError::Range));
        assert_eq!(blocks_by_range(0, 2, 2, 1024), Err(WireError::Range));
    }

    #[test]
    fn response_context_and_error_bounds() {
        let mut response = vec![0, 1, 2, 3, 4];
        response.extend(encode_request(b"block", LIMITS).unwrap());
        assert_eq!(
            decode_response_chunk(&response, true, LIMITS).unwrap(),
            ResponseChunk { code: 0, context: Some([1, 2, 3, 4]), payload: b"block".to_vec() }
        );
        let mut error = vec![2];
        error.extend(encode_request(&[0; 257], LIMITS).unwrap());
        assert_eq!(decode_response_chunk(&error, true, LIMITS), Err(WireError::Oversized));
    }

    proptest! {
        #[test]
        fn arbitrary_frames_do_not_panic(bytes in proptest::collection::vec(any::<u8>(), 0..300)) {
            let _ = decode_request(&bytes, LIMITS);
            let _ = decode_response_chunk(&bytes, true, LIMITS);
        }

        #[test]
        fn framed_roundtrip(bytes in proptest::collection::vec(any::<u8>(), 0..4096)) {
            let encoded = encode_request(&bytes, LIMITS).unwrap();
            prop_assert_eq!(decode_request(&encoded, LIMITS).unwrap(), bytes);
        }
    }
}

/// Status v2 advertises historical availability to avoid impossible backfill requests.
pub const STATUS_V2: &str = "/eth2/beacon_chain/req/status/2/ssz_snappy";
/// Gloas execution payload envelope range protocol.
pub const PAYLOAD_ENVELOPES_BY_RANGE_V1: &str =
    "/eth2/beacon_chain/req/execution_payload_envelopes_by_range/1/ssz_snappy";
/// Gloas execution payload envelope root protocol.
pub const PAYLOAD_ENVELOPES_BY_ROOT_V1: &str =
    "/eth2/beacon_chain/req/execution_payload_envelopes_by_root/1/ssz_snappy";
/// Extended data column range protocol.
pub const DATA_COLUMNS_BY_RANGE_V1: &str =
    "/eth2/beacon_chain/req/data_column_sidecars_by_range/1/ssz_snappy";

/// Status v2 adds the earliest available historical slot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StatusV2 {
    /// The original fixed status fields.
    pub status: Status,
    /// Earliest slot for which this peer can serve historical blocks and sidecars.
    pub earliest_available_slot: u64,
}

impl StatusV2 {
    /// Encode the complete v2 fixed-size SSZ status.
    pub fn encode(&self) -> [u8; 92] {
        let mut bytes = [0; 92];
        bytes[..84].copy_from_slice(&self.status.encode());
        bytes[84..].copy_from_slice(&self.earliest_available_slot.to_le_bytes());
        bytes
    }

    /// Decode a v2 status without trailing bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, WireError> {
        if bytes.len() != 92 {
            return Err(WireError::Ssz);
        }
        Ok(Self {
            status: Status::decode(&bytes[..84])?,
            earliest_available_slot: u64::from_le_bytes(
                bytes[84..].try_into().map_err(|_| WireError::Ssz)?,
            ),
        })
    }
}

/// Compute the actual network digest, including fork-aware scheduled blob parameters.
pub fn network_fork_digest(
    config: &reth_embedded_beacon_config::ChainConfig,
    epoch: u64,
) -> Result<[u8; 4], reth_embedded_beacon_config::ConfigError> {
    config.fork_digest(epoch)
}

/// Encode Gloas envelope range requests with their independent payload count limit.
pub fn payload_envelopes_by_range(
    start_slot: u64,
    count: u64,
    max_payloads: u64,
) -> Result<[u8; 16], WireError> {
    if count == 0 || count > max_payloads || start_slot.checked_add(count - 1).is_none() {
        return Err(WireError::Range);
    }
    let mut bytes = [0; 16];
    bytes[..8].copy_from_slice(&start_slot.to_le_bytes());
    bytes[8..].copy_from_slice(&count.to_le_bytes());
    Ok(bytes)
}

#[cfg(test)]
mod stream_tests {
    use super::*;

    #[test]
    fn sequential_chunks_and_multi_frame_payload() {
        let limits = Limits { max_payload: 200000, max_compressed: 300000 };
        let first = encode_request(&vec![7; 150000], limits).unwrap();
        let second = encode_request(b"next response", limits).unwrap();
        let mut stream = first.clone();
        stream.extend_from_slice(&second);
        let (decoded, consumed) = decode_request_prefix(&stream, limits).unwrap();
        assert_eq!(decoded, vec![7; 150000]);
        assert_eq!(consumed, first.len());
        assert_eq!(decode_request(&stream[consumed..], limits).unwrap(), b"next response");
        assert_eq!(decode_request(&stream, limits), Err(WireError::Compression));
    }

    #[test]
    fn empty_chunk_requires_identifier_and_is_delimited() {
        let limits = Limits { max_payload: 100, max_compressed: 100 };
        assert_eq!(decode_request(&[0], limits), Err(WireError::Compression));
        let first = encode_request(&[], limits).unwrap();
        let mut stream = first.clone();
        stream.extend_from_slice(&encode_request(b"next", limits).unwrap());
        assert_eq!(decode_request_prefix(&stream, limits).unwrap(), (Vec::new(), first.len()));
    }

    #[test]
    fn checksum_corruption_is_rejected() {
        let limits = Limits { max_payload: 100, max_compressed: 100 };
        let mut bytes = encode_request(b"checksum", limits).unwrap();
        bytes[15] ^= 1;
        assert_eq!(decode_request(&bytes, limits), Err(WireError::Compression));
    }

    #[test]
    fn current_status_and_payload_range_are_exact_ssz() {
        let status = StatusV2 {
            status: Status {
                fork_digest: [1; 4],
                finalized_root: [2; 32],
                finalized_epoch: 3,
                head_root: [4; 32],
                head_slot: 5,
            },
            earliest_available_slot: 6,
        };
        assert_eq!(StatusV2::decode(&status.encode()), Ok(status));
        assert_eq!(StatusV2::decode(&[0; 93]), Err(WireError::Ssz));
        assert_eq!(payload_envelopes_by_range(u64::MAX, 2, 128), Err(WireError::Range));
        assert_eq!(payload_envelopes_by_range(0, 129, 128), Err(WireError::Range));
        assert_eq!(payload_envelopes_by_range(12, 1, 128).unwrap()[..8], 12u64.to_le_bytes());
    }
}

/// Maximum permitted framed Snappy bytes for a declared SSZ length.
/// The bound is defined by the pinned consensus networking specification.
pub fn max_compressed_len(length: usize) -> Result<usize, WireError> {
    length.checked_add(length / 6).and_then(|n| n.checked_add(32)).ok_or(WireError::Oversized)
}

/// Legacy complete blob sidecars requested by beacon slot range.
pub const BLOB_SIDECARS_BY_RANGE_V1: &str =
    "/eth2/beacon_chain/req/blob_sidecars_by_range/1/ssz_snappy";
/// Legacy complete blob sidecars requested by message root and commitment index.
pub const BLOB_SIDECARS_BY_ROOT_V1: &str =
    "/eth2/beacon_chain/req/blob_sidecars_by_root/1/ssz_snappy";
/// Full data-column sidecars requested by message root and column indices.
pub const DATA_COLUMNS_BY_ROOT_V1: &str =
    "/eth2/beacon_chain/req/data_column_sidecars_by_root/1/ssz_snappy";
