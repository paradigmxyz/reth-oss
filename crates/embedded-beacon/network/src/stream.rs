//! Incremental, bounded Req/Resp decoding for the pinned SSZ-snappy protocols.
//! Request bytes are released only after EOF proves there is no trailing request.
//! Variable consensus objects still require full fork-specific SSZ and consensus validation.

use super::{
    blocks_by_range, decode_request_prefix, max_compressed_len, payload_envelopes_by_range,
    read_varint, Limits, ResponseChunk, Status, StatusV2, WireError, BLOB_SIDECARS_BY_RANGE_V1,
    BLOB_SIDECARS_BY_ROOT_V1, BLOCKS_BY_RANGE_V2, BLOCKS_BY_ROOT_V2, DATA_COLUMNS_BY_RANGE_V1,
    DATA_COLUMNS_BY_ROOT_V1, PAYLOAD_ENVELOPES_BY_RANGE_V1, PAYLOAD_ENVELOPES_BY_ROOT_V1, PING_V1,
    STATUS_V1, STATUS_V2,
};
use reth_embedded_beacon_config::ChainConfig;
use reth_embedded_beacon_primitives::Fork;

const IDENTIFIER: [u8; 10] = [0xff, 6, 0, 0, 115, 78, 97, 80, 112, 89];
const MAX_PAYLOAD_SIZE: usize = 10 * 1024 * 1024;
const MAX_CHUNKS: usize = 128 * 128;

/// Exact protocol versions supported by this codec.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Protocol {
    /// Original 84-byte status container.
    StatusV1,
    /// Fulu 92-byte status container with historical availability.
    StatusV2,
    /// Fixed uint64 metadata sequence number.
    PingV1,
    /// Fork-context beacon blocks requested by slot range.
    BlocksByRangeV2,
    /// Fork-context beacon blocks requested by root.
    BlocksByRootV2,
    /// Gloas execution payload envelopes requested by range.
    PayloadEnvelopesByRangeV1,
    /// Gloas execution payload envelopes requested by consensus root.
    PayloadEnvelopesByRootV1,
    /// Legacy complete blob sidecars requested by slot range.
    BlobSidecarsByRangeV1,
    /// Legacy complete blob sidecars requested by root and commitment index.
    BlobSidecarsByRootV1,
    /// Full data columns requested by slot range and column indices.
    DataColumnsByRangeV1,
    /// Full data columns requested by root and column indices.
    DataColumnsByRootV1,
}

impl Protocol {
    /// Return the exact multistream protocol identifier.
    pub const fn id(self) -> &'static str {
        match self {
            Self::StatusV1 => STATUS_V1,
            Self::StatusV2 => STATUS_V2,
            Self::PingV1 => PING_V1,
            Self::BlocksByRangeV2 => BLOCKS_BY_RANGE_V2,
            Self::BlocksByRootV2 => BLOCKS_BY_ROOT_V2,
            Self::PayloadEnvelopesByRangeV1 => PAYLOAD_ENVELOPES_BY_RANGE_V1,
            Self::PayloadEnvelopesByRootV1 => PAYLOAD_ENVELOPES_BY_ROOT_V1,
            Self::BlobSidecarsByRangeV1 => BLOB_SIDECARS_BY_RANGE_V1,
            Self::BlobSidecarsByRootV1 => BLOB_SIDECARS_BY_ROOT_V1,
            Self::DataColumnsByRangeV1 => DATA_COLUMNS_BY_RANGE_V1,
            Self::DataColumnsByRootV1 => DATA_COLUMNS_BY_ROOT_V1,
        }
    }

    /// Resolve an exact protocol ID without accepting a different version or codec.
    pub fn from_id(id: &str) -> Result<Self, WireError> {
        [
            Self::StatusV1,
            Self::StatusV2,
            Self::PingV1,
            Self::BlocksByRangeV2,
            Self::BlocksByRootV2,
            Self::PayloadEnvelopesByRangeV1,
            Self::PayloadEnvelopesByRootV1,
            Self::BlobSidecarsByRangeV1,
            Self::BlobSidecarsByRootV1,
            Self::DataColumnsByRangeV1,
            Self::DataColumnsByRootV1,
        ]
        .into_iter()
        .find(|p| p.id() == id)
        .ok_or(WireError::Protocol)
    }

    const fn has_context(self) -> bool {
        !matches!(self, Self::StatusV1 | Self::StatusV2 | Self::PingV1)
    }

    const fn is_envelope(self) -> bool {
        matches!(self, Self::PayloadEnvelopesByRangeV1 | Self::PayloadEnvelopesByRootV1)
    }

    const fn single_response(self) -> bool {
        !self.has_context()
    }

    fn validate_payload(
        self,
        payload: &[u8],
        request: bool,
        max_items: usize,
    ) -> Result<(), WireError> {
        match self {
            Self::StatusV1 => {
                Status::decode(payload)?;
            }
            Self::StatusV2 => {
                StatusV2::decode(payload)?;
            }
            Self::PingV1 if payload.len() != 8 => return Err(WireError::Ssz),
            Self::BlocksByRangeV2 if request => {
                if payload.len() != 24 {
                    return Err(WireError::Ssz);
                }
                blocks_by_range(
                    uint64(payload, 0)?,
                    uint64(payload, 8)?,
                    uint64(payload, 16)?,
                    max_items as u64,
                )?;
            }
            Self::PayloadEnvelopesByRangeV1 | Self::BlobSidecarsByRangeV1 if request => {
                if payload.len() != 16 {
                    return Err(WireError::Ssz);
                }
                payload_envelopes_by_range(
                    uint64(payload, 0)?,
                    uint64(payload, 8)?,
                    max_items as u64,
                )?;
            }
            Self::BlocksByRootV2 | Self::PayloadEnvelopesByRootV1
                if request &&
                    (!payload.len().is_multiple_of(32) || payload.len() / 32 > max_items) =>
            {
                return Err(WireError::Ssz);
            }
            Self::BlobSidecarsByRootV1
                if request &&
                    (!payload.len().is_multiple_of(40) || payload.len() / 40 > max_items) =>
            {
                return Err(WireError::Ssz)
            }
            Self::DataColumnsByRangeV1
                if request &&
                    (payload.len() < 20 ||
                        !((payload.len() - 20).is_multiple_of(8)) ||
                        payload.len() > 20 + 128 * 8) =>
            {
                return Err(WireError::Ssz)
            }
            Self::DataColumnsByRootV1 if request && !payload.is_empty() => {
                let first = payload.get(..4).ok_or(WireError::Ssz)?;
                let first =
                    u32::from_le_bytes(first.try_into().map_err(|_| WireError::Ssz)?) as usize;
                if first == 0 ||
                    !first.is_multiple_of(4) ||
                    first > payload.len() ||
                    first / 4 > 128
                {
                    return Err(WireError::Ssz)
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn check_declared(self, declared: usize, request: bool) -> Result<(), WireError> {
        let fixed = match self {
            Self::StatusV1 => Some(84),
            Self::StatusV2 => Some(92),
            Self::PingV1 => Some(8),
            Self::BlocksByRangeV2 if request => Some(24),
            Self::PayloadEnvelopesByRangeV1 | Self::BlobSidecarsByRangeV1 if request => Some(16),
            _ => None,
        };
        if fixed.is_some_and(|expected| declared != expected) {
            return Err(WireError::Ssz);
        }
        Ok(())
    }
}

/// A message whose framing and checksum have been validated.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WireMessage {
    /// A complete request followed by stream EOF.
    Request(Vec<u8>),
    /// One response chunk. Variable SSZ objects still require their importer.
    Response(ResponseChunk),
}

/// Result of polling an incremental decoder.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DecodeProgress {
    /// More wire bytes or request EOF are required.
    NeedMore,
    /// A fully framed message is available.
    Message(WireMessage),
    /// The stream has ended or its requested response budget is complete.
    Finished,
}

/// A decoder with per-message input, decompression and response-count budgets.
/// Feed bounded reads and drain messages between reads. No request is accepted until EOF.
/// An error poisons this decoder; its peer stream must then be closed.
#[derive(Debug)]
pub struct StreamDecoder {
    protocol: Protocol,
    limits: Limits,
    request: bool,
    max_items: usize,
    emitted: usize,
    buffer: Vec<u8>,
    scan: Option<Scan>,
    contexts: Vec<[u8; 4]>,
    eof: bool,
    terminal: bool,
    poisoned: bool,
}

#[derive(Debug)]
struct Scan {
    code: u8,
    context: Option<[u8; 4]>,
    payload_start: usize,
    prefix_end: usize,
    cursor: usize,
    produced: usize,
    declared: usize,
    compressed_limit: usize,
}

impl StreamDecoder {
    /// Decode one request, with a fork-specific maximum count for list/range requests.
    pub fn request(
        protocol: Protocol,
        limits: Limits,
        max_items: usize,
    ) -> Result<Self, WireError> {
        Self::new(protocol, limits, true, max_items, Vec::new())
    }

    /// Decode responses with an exact requested chunk budget and trusted network digests.
    /// The digest allow-list includes enabled historical forks and scheduled blob upgrades.
    pub fn response(
        protocol: Protocol,
        limits: Limits,
        max_chunks: usize,
        config: &ChainConfig,
    ) -> Result<Self, WireError> {
        config.validate().map_err(|_| WireError::Protocol)?;
        let mut contexts = Vec::new();
        for epoch in config
            .forks
            .activations()
            .iter()
            .map(|fork| fork.epoch)
            .chain(config.blob_schedule.iter().map(|blob| blob.epoch))
            .filter(|epoch| *epoch != u64::MAX)
        {
            let fork = config.forks.at_epoch(epoch).map_err(|_| WireError::Protocol)?.fork;
            if !protocol.is_envelope() || fork >= Fork::Gloas {
                let digest = config.fork_digest(epoch).map_err(|_| WireError::Protocol)?;
                if !contexts.contains(&digest) {
                    contexts.push(digest);
                }
            }
        }
        let limits =
            Limits { max_payload: limits.max_payload.min(config.max_payload_size), ..limits };
        Self::new(protocol, limits, false, max_chunks, contexts)
    }

    fn new(
        protocol: Protocol,
        limits: Limits,
        request: bool,
        max_items: usize,
        contexts: Vec<[u8; 4]>,
    ) -> Result<Self, WireError> {
        if limits.max_payload > MAX_PAYLOAD_SIZE ||
            limits.max_compressed < IDENTIFIER.len() ||
            max_items == 0 ||
            max_items > MAX_CHUNKS ||
            (!request && protocol.single_response() && max_items != 1)
        {
            return Err(WireError::Protocol);
        }
        let limits = Limits {
            max_compressed: limits.max_compressed.min(max_compressed_len(limits.max_payload)?),
            ..limits
        };
        Ok(Self {
            protocol,
            limits,
            request,
            max_items,
            emitted: 0,
            buffer: Vec::new(),
            scan: None,
            contexts,
            eof: false,
            terminal: false,
            poisoned: false,
        })
    }

    /// Append a bounded network read. Drain before appending more than one message budget.
    pub fn push(&mut self, bytes: &[u8]) -> Result<(), WireError> {
        if self.poisoned || self.eof || self.terminal {
            return Err(WireError::Closed);
        }
        let capacity = self.limits.max_compressed.checked_add(15).ok_or(WireError::Oversized)?;
        if self.buffer.len().checked_add(bytes.len()).is_none_or(|len| len > capacity) {
            self.poisoned = true;
            return Err(WireError::Oversized);
        }
        self.buffer.extend_from_slice(bytes);
        Ok(())
    }

    /// Signal orderly stream EOF. A partial message is rejected on the next poll.
    pub const fn finish_input(&mut self) {
        self.eof = true;
    }

    /// Poll a chunk without decompressing incomplete frames repeatedly.
    pub fn next_message(&mut self) -> Result<DecodeProgress, WireError> {
        if self.poisoned {
            return Err(WireError::Closed);
        }
        let result = self.poll();
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }

    const fn incomplete(&self) -> Result<DecodeProgress, WireError> {
        if self.eof {
            Err(WireError::Truncated)
        } else {
            Ok(DecodeProgress::NeedMore)
        }
    }

    fn poll(&mut self) -> Result<DecodeProgress, WireError> {
        if self.terminal {
            return if self.buffer.is_empty() {
                Ok(DecodeProgress::Finished)
            } else {
                Err(WireError::TrailingBytes)
            };
        }
        if self.buffer.is_empty() && self.eof {
            if self.request || (self.protocol.single_response() && self.emitted == 0) {
                return Err(WireError::Truncated);
            }
            self.terminal = true;
            return Ok(DecodeProgress::Finished);
        }
        if self.scan.is_none() {
            let (code, context, payload_start) = if self.request {
                (0, None, 0)
            } else {
                let Some(&code) = self.buffer.first() else { return self.incomplete() };
                if (4..128).contains(&code) || (code == 3 && self.protocol.single_response()) {
                    return Err(WireError::ResponseCode);
                }
                if code == 0 && self.protocol.has_context() {
                    let Some(context) = self.buffer.get(1..5) else { return self.incomplete() };
                    let digest = context.try_into().map_err(|_| WireError::ForkContext)?;
                    if !self.contexts.contains(&digest) {
                        return Err(WireError::ForkContext);
                    }
                    (code, Some(digest), 5)
                } else {
                    (code, None, 1)
                }
            };
            let prefix = &self.buffer[payload_start..];
            if !prefix.iter().take(10).any(|b| b & 128 == 0) {
                if prefix.len() >= 10 {
                    return Err(WireError::LengthPrefix);
                }
                return self.incomplete();
            }
            let (declared, prefix_len) = read_varint(prefix)?;
            let declared = usize::try_from(declared).map_err(|_| WireError::Oversized)?;
            let bound =
                if code == 0 { self.limits.max_payload } else { self.limits.max_payload.min(256) };
            if declared > bound {
                return Err(WireError::Oversized);
            }
            if code == 0 {
                self.protocol.check_declared(declared, self.request)?;
            }
            let prefix_end = payload_start + prefix_len;
            let cursor = prefix_end + IDENTIFIER.len();
            let Some(identifier) = self.buffer.get(prefix_end..cursor) else {
                return self.incomplete()
            };
            if identifier != IDENTIFIER {
                return Err(WireError::Compression);
            }
            let compressed_limit = self.limits.max_compressed.min(max_compressed_len(declared)?);
            if IDENTIFIER.len() > compressed_limit {
                return Err(WireError::Oversized);
            }
            self.scan = Some(Scan {
                code,
                context,
                payload_start,
                prefix_end,
                cursor,
                produced: 0,
                declared,
                compressed_limit,
            });
        }
        let scan = self.scan.as_mut().ok_or(WireError::Protocol)?;
        while scan.produced < scan.declared {
            let header_end = scan.cursor.checked_add(4).ok_or(WireError::Oversized)?;
            let Some(header) = self.buffer.get(scan.cursor..header_end) else {
                return self.incomplete()
            };
            let length = usize::from(header[1]) |
                (usize::from(header[2]) << 8) |
                (usize::from(header[3]) << 16);
            let end = header_end.checked_add(length).ok_or(WireError::Oversized)?;
            if end - scan.prefix_end > scan.compressed_limit {
                return Err(WireError::Oversized);
            }
            let Some(data) = self.buffer.get(header_end..end) else { return self.incomplete() };
            match header[0] {
                0 | 1 => {
                    if data.len() < 4 {
                        return Err(WireError::Compression);
                    }
                    let produced = if header[0] == 0 {
                        snap::raw::decompress_len(&data[4..]).map_err(|_| WireError::Compression)?
                    } else {
                        data.len() - 4
                    };
                    if produced > 65536 || produced > scan.declared - scan.produced {
                        return Err(WireError::Compression);
                    }
                    scan.produced += produced;
                }
                0xff if data == &IDENTIFIER[4..] => {}
                0x80..=0xfe => {}
                _ => return Err(WireError::Compression),
            }
            scan.cursor = end;
        }
        let end = scan.cursor;
        if self.request {
            if self.buffer.len() != end {
                return Err(WireError::TrailingBytes);
            }
            if !self.eof {
                return Ok(DecodeProgress::NeedMore);
            }
        }
        let scan = self.scan.take().ok_or(WireError::Protocol)?;
        let (payload, consumed) =
            decode_request_prefix(&self.buffer[scan.payload_start..end], self.limits)?;
        if consumed != end - scan.payload_start {
            return Err(WireError::Compression);
        }
        if scan.code == 0 {
            self.protocol.validate_payload(&payload, self.request, self.max_items)?;
        }
        self.buffer.drain(..end);
        self.emitted += 1;
        self.terminal = self.request || scan.code != 0 || self.emitted == self.max_items;
        if self.terminal && !self.buffer.is_empty() {
            return Err(WireError::TrailingBytes);
        }
        Ok(DecodeProgress::Message(if self.request {
            WireMessage::Request(payload)
        } else {
            WireMessage::Response(ResponseChunk { code: scan.code, context: scan.context, payload })
        }))
    }
}

/// Check the response digest against the slot obtained from a fully decoded consensus object.
/// Envelope context is the referenced beacon block's slot, not the envelope arrival slot.
pub fn validate_response_context(
    config: &ChainConfig,
    protocol: Protocol,
    chunk: &ResponseChunk,
    referenced_slot: u64,
) -> Result<(), WireError> {
    if chunk.code != 0 || !protocol.has_context() {
        return Err(WireError::ForkContext);
    }
    config.validate().map_err(|_| WireError::Protocol)?;
    let epoch = referenced_slot / config.slots_per_epoch;
    let fork = config.forks.at_epoch(epoch).map_err(|_| WireError::ForkContext)?.fork;
    if protocol.is_envelope() && fork < Fork::Gloas {
        return Err(WireError::ForkContext);
    }
    if matches!(protocol, Protocol::BlobSidecarsByRangeV1 | Protocol::BlobSidecarsByRootV1) &&
        !matches!(fork, Fork::Deneb | Fork::Electra) ||
        matches!(protocol, Protocol::DataColumnsByRangeV1 | Protocol::DataColumnsByRootV1) &&
            fork < Fork::Fulu
    {
        return Err(WireError::ForkContext);
    }
    let digest = config.fork_digest(epoch).map_err(|_| WireError::ForkContext)?;
    if chunk.context != Some(digest) {
        return Err(WireError::ForkContext);
    }
    Ok(())
}

fn uint64(bytes: &[u8], offset: usize) -> Result<u64, WireError> {
    Ok(u64::from_le_bytes(
        bytes
            .get(offset..offset + 8)
            .ok_or(WireError::Ssz)?
            .try_into()
            .map_err(|_| WireError::Ssz)?,
    ))
}
