//! Bounded block Req/Resp on the authenticated transport's existing identity and listeners.
//!
//! Returned blocks have complete SSZ and network-context checks, but still require consensus
//! import.

use crate::{
    blocks_by_range, encode_request, max_compressed_len, validate_response_context, DecodeProgress,
    Limits, Protocol, ResponseChunk, StreamDecoder, WireError, WireMessage,
};
use async_trait::async_trait;
use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use libp2p::{request_response::Codec, StreamProtocol};
use reth_embedded_beacon_config::{ChainConfig, Preset as ConfigPreset};
use reth_embedded_beacon_primitives::{Fork, Root, Slot};
use reth_embedded_beacon_state_transition::ssz::{Preset, SignedBeaconBlock};
use std::{collections::HashSet, io, sync::Arc};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Maximum number requested by this follower in one outbound batch.
pub const MAX_BLOCK_BATCH: usize = 16;
/// Independent aggregate decompressed-byte cap for one received or served batch.
pub const MAX_BATCH_BYTES: usize = 32 * 1024 * 1024;
const MAX_WIRE_ITEMS: usize = 1024;
const REQUEST_LIMITS: Limits = Limits { max_payload: 32 * 1024, max_compressed: 40 * 1024 };

/// A bounded range or list request. Missing slots or unavailable roots may have no response.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BlockRequest {
    /// Contiguous slot range; protocol step is always one.
    Range {
        /// First requested slot.
        start_slot: Slot,
        /// Number of requested slots.
        count: u64,
    },
    /// Consensus message roots in request order.
    Roots(Vec<Root>),
}

impl BlockRequest {
    /// Exact protocol negotiated for this request.
    pub const fn protocol(&self) -> Protocol {
        match self {
            Self::Range { .. } => Protocol::BlocksByRangeV2,
            Self::Roots(_) => Protocol::BlocksByRootV2,
        }
    }

    /// Admit the follower's smaller outbound resource budget before opening a stream.
    pub fn validate_outbound(&self) -> Result<(), WireError> {
        self.encode()?;
        match self {
            Self::Range { count, .. } if *count <= MAX_BLOCK_BATCH as u64 => Ok(()),
            Self::Roots(roots)
                if !roots.is_empty() &&
                    roots.len() <= MAX_BLOCK_BATCH &&
                    roots.iter().copied().collect::<HashSet<_>>().len() == roots.len() =>
            {
                Ok(())
            }
            _ => Err(WireError::Range),
        }
    }

    /// Check actual message roots, unique slots and range ancestry against the original request.
    pub fn matches(&self, blocks: &[WireBlock]) -> Result<(), WireError> {
        if blocks.len() > MAX_BLOCK_BATCH {
            return Err(WireError::Oversized);
        }
        let mut seen = HashSet::new();
        let mut prior_slot = None;
        let mut prior_root = None;
        let mut bytes = 0usize;
        for block in blocks {
            bytes = bytes.checked_add(block.ssz.len()).ok_or(WireError::Oversized)?;
            if bytes > MAX_BATCH_BYTES || !seen.insert(block.root) {
                return Err(WireError::Oversized);
            }
            match self {
                Self::Range { start_slot, count } => {
                    let offset = block.slot.checked_sub(*start_slot).ok_or(WireError::Range)?;
                    if offset >= *count || prior_slot.is_some_and(|slot| slot >= block.slot) {
                        return Err(WireError::Range);
                    }
                    if prior_root.is_some_and(|root| root != block.parent_root) {
                        return Err(WireError::Range);
                    }
                    prior_slot = Some(block.slot);
                    prior_root = Some(block.root);
                }
                Self::Roots(roots) => {
                    if blocks.len() > roots.len() || !roots.contains(&block.root) {
                        return Err(WireError::Ssz);
                    }
                }
            }
        }
        Ok(())
    }

    fn encode(&self) -> Result<Vec<u8>, WireError> {
        match self {
            Self::Range { start_slot, count } => {
                Ok(blocks_by_range(*start_slot, *count, 1, MAX_WIRE_ITEMS as u64)?.to_vec())
            }
            Self::Roots(roots) => {
                if roots.len() > MAX_WIRE_ITEMS {
                    return Err(WireError::Oversized);
                }
                Ok(roots.iter().flat_map(|root| root.iter().copied()).collect())
            }
        }
    }

    fn decode(protocol: Protocol, bytes: &[u8]) -> Result<Self, WireError> {
        match protocol {
            Protocol::BlocksByRangeV2 if bytes.len() == 24 => {
                let value = |offset| -> Result<u64, WireError> {
                    Ok(u64::from_le_bytes(
                        bytes[offset..offset + 8].try_into().map_err(|_| WireError::Ssz)?,
                    ))
                };
                blocks_by_range(value(0)?, value(8)?, value(16)?, MAX_WIRE_ITEMS as u64)?;
                Ok(Self::Range { start_slot: value(0)?, count: value(8)? })
            }
            Protocol::BlocksByRootV2
                if bytes.len().is_multiple_of(32) && bytes.len() / 32 <= MAX_WIRE_ITEMS =>
            {
                Ok(Self::Roots(
                    bytes
                        .chunks_exact(32)
                        .map(|root| root.try_into().expect("exact root width"))
                        .collect(),
                ))
            }
            _ => Err(WireError::Ssz),
        }
    }
}

/// A completely decoded signed block retained once as immutable SSZ bytes.
/// Construction grants no signature, state-transition, execution or availability authority.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WireBlock {
    slot: Slot,
    root: Root,
    parent_root: Root,
    fork: Fork,
    context: [u8; 4],
    ssz: Arc<[u8]>,
}

impl WireBlock {
    /// Validate complete schema, exact configured fork, body root and network digest.
    pub fn from_ssz(bytes: Vec<u8>, config: &ChainConfig) -> Result<Self, WireError> {
        config.validate().map_err(|_| WireError::Protocol)?;
        if bytes.len() > config.max_payload_size.min(10 * 1024 * 1024) {
            return Err(WireError::Oversized);
        }
        // The signed variable container's canonical message offset is checked again by full SSZ.
        let slot = u64::from_le_bytes(
            bytes.get(100..108).ok_or(WireError::Ssz)?.try_into().map_err(|_| WireError::Ssz)?,
        );
        let fork = config.fork_at_slot(slot).map_err(|_| WireError::ForkContext)?.fork;
        let preset = match config.preset {
            ConfigPreset::Mainnet => Preset::Mainnet,
            ConfigPreset::Minimal => Preset::Minimal,
        };
        let signed =
            SignedBeaconBlock::from_ssz(&bytes, fork, preset).map_err(|_| WireError::Ssz)?;
        let header = signed.header().map_err(|_| WireError::Ssz)?;
        let root = signed.message_root().map_err(|_| WireError::Ssz)?;
        let context = config
            .fork_digest(slot / config.slots_per_epoch)
            .map_err(|_| WireError::ForkContext)?;
        Ok(Self {
            slot: header.slot,
            root,
            parent_root: header.parent_root,
            fork,
            context,
            ssz: bytes.into(),
        })
    }

    /// Actual decoded slot.
    pub const fn slot(&self) -> Slot {
        self.slot
    }
    /// Actual consensus message root, excluding the outer signature.
    pub const fn root(&self) -> Root {
        self.root
    }
    /// Actual decoded parent consensus root.
    pub const fn parent_root(&self) -> Root {
        self.parent_root
    }
    /// Exact configured schema used for decoding.
    pub const fn fork(&self) -> Fork {
        self.fork
    }
    /// Exact validated network fork digest.
    pub const fn context(&self) -> [u8; 4] {
        self.context
    }
    /// Shared complete signed SSZ bytes.
    pub const fn ssz(&self) -> &Arc<[u8]> {
        &self.ssz
    }
}

/// A bounded immutable batch; retaining clones retains its global decoder reservation.
/// At most two response batches can be decoded or retained by one transport at a time.
#[derive(Clone, Debug)]
pub struct BlockBatch {
    blocks: Arc<[WireBlock]>,
    _reservation: Option<Arc<OwnedSemaphorePermit>>,
}

impl BlockBatch {
    /// Blocks in their checked wire order.
    pub fn blocks(&self) -> &[WireBlock] {
        &self.blocks
    }

    pub(crate) fn serving(blocks: Vec<WireBlock>) -> Self {
        Self { blocks: blocks.into(), _reservation: None }
    }
}

impl PartialEq for BlockBatch {
    fn eq(&self, other: &Self) -> bool {
        self.blocks == other.blocks
    }
}
impl Eq for BlockBatch {}

#[derive(Clone, Debug)]
pub(crate) enum BlockReply {
    Blocks(BlockBatch),
    Error(u8),
}

#[derive(Clone, Debug)]
pub(crate) struct BlockCodec {
    config: Arc<ChainConfig>,
    read_budget: Arc<Semaphore>,
    write_budget: Arc<Semaphore>,
}

impl BlockCodec {
    pub(crate) fn new(config: Arc<ChainConfig>) -> Self {
        Self {
            config,
            read_budget: Arc::new(Semaphore::new(2)),
            write_budget: Arc::new(Semaphore::new(2)),
        }
    }

    pub(crate) fn budgets(&self) -> (Arc<Semaphore>, Arc<Semaphore>) {
        (self.read_budget.clone(), self.write_budget.clone())
    }

    fn limits(&self) -> Result<Limits, WireError> {
        let max_payload = self.config.max_payload_size.min(10 * 1024 * 1024);
        Ok(Limits { max_payload, max_compressed: max_compressed_len(max_payload)? })
    }
}

#[async_trait]
impl Codec for BlockCodec {
    type Protocol = StreamProtocol;
    type Request = BlockRequest;
    type Response = BlockReply;

    async fn read_request<T>(
        &mut self,
        protocol: &StreamProtocol,
        io: &mut T,
    ) -> io::Result<BlockRequest>
    where
        T: AsyncRead + Unpin + Send,
    {
        let protocol = block_protocol(protocol)?;
        let mut decoder =
            StreamDecoder::request(protocol, REQUEST_LIMITS, MAX_WIRE_ITEMS).map_err(wire_io)?;
        let mut buffer = [0; 1024];
        loop {
            match decoder.next_message().map_err(wire_io)? {
                DecodeProgress::Message(WireMessage::Request(bytes)) => {
                    return BlockRequest::decode(protocol, &bytes).map_err(wire_io)
                }
                DecodeProgress::NeedMore => {}
                _ => return Err(wire_io(WireError::Protocol)),
            }
            let read = io.read(&mut buffer).await?;
            if read == 0 {
                decoder.finish_input();
            } else {
                decoder.push(&buffer[..read]).map_err(wire_io)?;
            }
        }
    }

    async fn read_response<T>(
        &mut self,
        protocol: &StreamProtocol,
        io: &mut T,
    ) -> io::Result<BlockReply>
    where
        T: AsyncRead + Unpin + Send,
    {
        let protocol = block_protocol(protocol)?;
        let reservation = Arc::new(
            self.read_budget
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| wire_io(WireError::Closed))?,
        );
        let mut decoder = StreamDecoder::response(
            protocol,
            self.limits().map_err(wire_io)?,
            MAX_BLOCK_BATCH,
            &self.config,
        )
        .map_err(wire_io)?;
        let mut buffer = vec![0; 64 * 1024];
        let mut blocks = Vec::new();
        let mut bytes = 0usize;
        let mut error_code = None;
        loop {
            match decoder.next_message().map_err(wire_io)? {
                DecodeProgress::Message(WireMessage::Response(chunk)) if chunk.code == 0 => {
                    bytes = bytes
                        .checked_add(chunk.payload.len())
                        .filter(|bytes| *bytes <= MAX_BATCH_BYTES)
                        .ok_or_else(|| wire_io(WireError::Oversized))?;
                    let config = self.config.clone();
                    let reservation = reservation.clone();
                    let block = tokio::task::spawn_blocking(move || {
                        // Cancellation cannot release the global budget while decoding continues.
                        let _reservation = reservation;
                        let block = WireBlock::from_ssz(chunk.payload, &config)?;
                        validate_response_context(
                            &config,
                            protocol,
                            &ResponseChunk { code: 0, context: chunk.context, payload: Vec::new() },
                            block.slot,
                        )?;
                        Ok::<_, WireError>(block)
                    })
                    .await
                    .map_err(|_| wire_io(WireError::Closed))?
                    .map_err(wire_io)?;
                    blocks.push(block);
                }
                DecodeProgress::Message(WireMessage::Response(chunk)) => {
                    if !blocks.is_empty() {
                        return Err(wire_io(WireError::Protocol));
                    }
                    error_code = Some(chunk.code);
                }
                DecodeProgress::Finished => {
                    // Count exhaustion is not stream EOF; reject any trailing response byte.
                    if io.read(&mut buffer[..1]).await? != 0 {
                        return Err(wire_io(WireError::TrailingBytes));
                    }
                    return Ok(match error_code {
                        Some(code) => BlockReply::Error(code),
                        None => BlockReply::Blocks(BlockBatch {
                            blocks: blocks.into(),
                            _reservation: Some(reservation),
                        }),
                    });
                }
                DecodeProgress::NeedMore => {
                    let read = io.read(&mut buffer).await?;
                    if read == 0 {
                        decoder.finish_input();
                    } else {
                        decoder.push(&buffer[..read]).map_err(wire_io)?;
                    }
                }
                _ => return Err(wire_io(WireError::Protocol)),
            }
        }
    }

    async fn write_request<T>(
        &mut self,
        protocol: &StreamProtocol,
        io: &mut T,
        request: BlockRequest,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        if block_protocol(protocol)? != request.protocol() {
            return Err(wire_io(WireError::Protocol));
        }
        io.write_all(
            &encode_request(&request.encode().map_err(wire_io)?, REQUEST_LIMITS)
                .map_err(wire_io)?,
        )
        .await?;
        io.close().await
    }

    async fn write_response<T>(
        &mut self,
        protocol: &StreamProtocol,
        io: &mut T,
        response: BlockReply,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        block_protocol(protocol)?;
        let reservation = Arc::new(
            self.write_budget
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| wire_io(WireError::Closed))?,
        );
        let limits = self.limits().map_err(wire_io)?;
        match response {
            BlockReply::Blocks(batch) => {
                let bytes = batch
                    .blocks
                    .iter()
                    .try_fold(0usize, |sum, block| sum.checked_add(block.ssz.len()))
                    .ok_or_else(|| wire_io(WireError::Oversized))?;
                if batch.blocks.len() > MAX_BLOCK_BATCH || bytes > MAX_BATCH_BYTES {
                    return Err(wire_io(WireError::Oversized));
                }
                for block in batch.blocks.iter() {
                    io.write_all(&[0]).await?;
                    io.write_all(&block.context).await?;
                    let ssz = block.ssz.clone();
                    let reservation = reservation.clone();
                    let wire = tokio::task::spawn_blocking(move || {
                        let _reservation = reservation;
                        encode_request(&ssz, limits)
                    })
                    .await
                    .map_err(|_| wire_io(WireError::Closed))?
                    .map_err(wire_io)?;
                    io.write_all(&wire).await?;
                }
            }
            BlockReply::Error(code @ (1..=3 | 128..=255)) => {
                io.write_all(&[code]).await?;
                io.write_all(
                    &encode_request(b"blocks unavailable", REQUEST_LIMITS).map_err(wire_io)?,
                )
                .await?;
            }
            BlockReply::Error(_) => return Err(wire_io(WireError::ResponseCode)),
        }
        io.close().await
    }
}

fn block_protocol(protocol: &StreamProtocol) -> io::Result<Protocol> {
    match Protocol::from_id(protocol.as_ref()).map_err(wire_io)? {
        protocol @ (Protocol::BlocksByRangeV2 | Protocol::BlocksByRootV2) => Ok(protocol),
        _ => Err(wire_io(WireError::Protocol)),
    }
}

fn wire_io(error: WireError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_blocks::{config, wire_block};
    use futures::{io::Cursor, FutureExt};

    async fn response_wire(codec: &mut BlockCodec, blocks: Vec<WireBlock>) -> Vec<u8> {
        let mut out = Cursor::new(Vec::new());
        codec
            .write_response(
                &StreamProtocol::new(Protocol::BlocksByRangeV2.id()),
                &mut out,
                BlockReply::Blocks(BlockBatch::serving(blocks)),
            )
            .await
            .unwrap();
        out.into_inner()
    }

    #[test]
    fn request_bounds_and_actual_range_ancestry_are_checked() {
        let a = wire_block(9, [1; 32]);
        let b = wire_block(12, a.root());
        let range = BlockRequest::Range { start_slot: 9, count: 4 };
        assert_eq!(range.matches(&[a.clone(), b.clone()]), Ok(()));
        assert_eq!(range.matches(&[b.clone(), a.clone()]), Err(WireError::Range));
        assert_eq!(range.matches(&[a.clone(), wire_block(12, [4; 32])]), Err(WireError::Range));
        assert_eq!(BlockRequest::Roots(vec![b.root(), a.root()]).matches(&[a.clone(), b]), Ok(()));
        assert_eq!(BlockRequest::Roots(vec![[9; 32]]).matches(&[a]), Err(WireError::Ssz));
        assert_eq!(
            BlockRequest::Range { start_slot: u64::MAX, count: 2 }.validate_outbound(),
            Err(WireError::Range)
        );
        assert_eq!(
            BlockRequest::Range { start_slot: 0, count: 17 }.validate_outbound(),
            Err(WireError::Range)
        );
    }

    #[test]
    fn full_signed_container_is_required_and_identity_excludes_signature() {
        let block = wire_block(9, [1; 32]);
        let mut changed = block.ssz().to_vec();
        changed[4] ^= 1;
        assert_eq!(WireBlock::from_ssz(changed, &config()).unwrap().root(), block.root());
        let mut malformed = block.ssz().to_vec();
        malformed[0] = 99;
        assert_eq!(WireBlock::from_ssz(malformed, &config()), Err(WireError::Ssz));
        let mut trailing = block.ssz().to_vec();
        trailing.push(0);
        assert_eq!(WireBlock::from_ssz(trailing, &config()), Err(WireError::Ssz));
    }

    #[tokio::test]
    async fn multi_chunk_and_empty_response_roundtrip() {
        let mut codec = BlockCodec::new(Arc::new(config()));
        let a = wire_block(9, [1; 32]);
        let b = wire_block(12, a.root());
        let protocol = StreamProtocol::new(Protocol::BlocksByRangeV2.id());
        let wire = response_wire(&mut codec, vec![a.clone(), b.clone()]).await;
        let BlockReply::Blocks(batch) =
            codec.read_response(&protocol, &mut Cursor::new(wire)).await.unwrap()
        else {
            panic!("expected blocks")
        };
        assert_eq!(batch.blocks(), [a, b]);
        let BlockReply::Blocks(empty) =
            codec.read_response(&protocol, &mut Cursor::new(Vec::new())).await.unwrap()
        else {
            panic!("expected empty blocks")
        };
        assert!(empty.blocks().is_empty());
    }

    #[tokio::test]
    async fn wrong_context_and_truncation_reject_whole_batch() {
        let mut codec = BlockCodec::new(Arc::new(config()));
        let protocol = StreamProtocol::new(Protocol::BlocksByRangeV2.id());
        let wire = response_wire(&mut codec, vec![wire_block(9, [1; 32])]).await;
        let mut wrong = wire.clone();
        wrong[1] ^= 1;
        assert_eq!(
            codec.read_response(&protocol, &mut Cursor::new(wrong)).await.unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        for end in [1, 4, 8, wire.len() - 1] {
            assert_eq!(
                codec
                    .read_response(&protocol, &mut Cursor::new(wire[..end].to_vec()))
                    .await
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::InvalidData
            );
        }
    }

    #[tokio::test]
    async fn retained_batch_clones_hold_global_decode_budget() {
        let mut codec = BlockCodec::new(Arc::new(config()));
        let protocol = StreamProtocol::new(Protocol::BlocksByRangeV2.id());
        let a = codec.read_response(&protocol, &mut Cursor::new(Vec::new())).await.unwrap();
        let b = codec.read_response(&protocol, &mut Cursor::new(Vec::new())).await.unwrap();
        assert_eq!(codec.read_budget.available_permits(), 0);
        assert!(codec
            .read_response(&protocol, &mut Cursor::new(Vec::new()))
            .now_or_never()
            .is_none());
        let retained = a.clone();
        drop(a);
        assert_eq!(codec.read_budget.available_permits(), 0);
        drop(retained);
        assert_eq!(codec.read_budget.available_permits(), 1);
        drop(b);
        assert_eq!(codec.read_budget.available_permits(), 2);
    }

    #[tokio::test]
    async fn exhausted_count_does_not_hide_trailing_response_bytes() {
        let mut codec = BlockCodec::new(Arc::new(config()));
        let mut blocks = Vec::new();
        let mut root = [1; 32];
        for slot in 0..MAX_BLOCK_BATCH as u64 {
            let block = wire_block(slot, root);
            root = block.root();
            blocks.push(block);
        }
        let mut wire = response_wire(&mut codec, blocks).await;
        wire.push(0);
        assert_eq!(
            codec
                .read_response(
                    &StreamProtocol::new(Protocol::BlocksByRangeV2.id()),
                    &mut Cursor::new(wire)
                )
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidData
        );
    }
}
