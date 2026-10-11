//! Bounded Gloas envelope retrieval on the authenticated block transport.
//!
//! Envelope SSZ has no beacon slot field. Context and range membership use metadata copied from the
//! original fully decoded beacon block, never a payload timestamp. Wire acceptance grants no
//! signature, execution, data-availability or consensus-import authority.

use crate::{
    block_rpc::{BlockCodec, WireBlock, MAX_BATCH_BYTES, MAX_BLOCK_BATCH},
    encode_request, max_compressed_len, payload_envelopes_by_range, validate_response_context,
    DecodeProgress, Limits, Protocol, ResponseChunk, StreamDecoder, WireError, WireMessage,
};
use async_trait::async_trait;
use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use libp2p::{request_response::Codec, StreamProtocol};
use reth_embedded_beacon_config::{ChainConfig, Preset as ConfigPreset};
use reth_embedded_beacon_primitives::{Fork, Root, Slot};
use reth_embedded_beacon_state_transition::ssz::{gloas, BeaconBlock, Preset};
use std::{collections::HashSet, io, sync::Arc};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Smaller local request and retained-response budget shared with block retrieval.
pub const MAX_ENVELOPE_BATCH: usize = MAX_BLOCK_BATCH;
const MAX_WIRE_ITEMS: usize = 128;
const REQUEST_LIMITS: Limits = Limits { max_payload: 4 * 1024, max_compressed: 5 * 1024 };

/// Beacon slots or beacon block message roots whose execution payloads are requested.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EnvelopeRequest {
    /// Contiguous beacon slots; skipped slots may yield no envelope.
    Range {
        /// First referenced beacon slot.
        start_slot: Slot,
        /// Number of referenced beacon slots.
        count: u64,
    },
    /// Beacon block message roots, excluding the outer block signature.
    Roots(Vec<Root>),
}

impl EnvelopeRequest {
    /// Exact negotiated Gloas protocol.
    pub const fn protocol(&self) -> Protocol {
        match self {
            Self::Range { .. } => Protocol::PayloadEnvelopesByRangeV1,
            Self::Roots(_) => Protocol::PayloadEnvelopesByRootV1,
        }
    }

    /// Reject oversized, empty, duplicate or overflowing requests before opening a stream.
    pub fn validate_outbound(&self) -> Result<(), WireError> {
        self.encode()?;
        match self {
            Self::Range { count, .. } if *count <= MAX_ENVELOPE_BATCH as u64 => Ok(()),
            Self::Roots(roots)
                if !roots.is_empty() &&
                    roots.len() <= MAX_ENVELOPE_BATCH &&
                    roots.iter().copied().collect::<HashSet<_>>().len() == roots.len() =>
            {
                Ok(())
            }
            _ => Err(WireError::Range),
        }
    }

    /// Resolve every envelope against exact metadata copied from original decoded blocks.
    /// A payload timestamp or EL slot number cannot select the response's consensus context.
    pub fn matches<'a>(
        &self,
        config: &ChainConfig,
        batch: &EnvelopeBatch,
        block: impl Fn(Root) -> Option<&'a EnvelopeBlockReference>,
    ) -> Result<(), WireError> {
        self.encode()?;
        if batch.envelopes.len() > MAX_ENVELOPE_BATCH {
            return Err(WireError::Oversized);
        }
        let mut seen = HashSet::new();
        let mut prior_slot = None;
        let mut prior_root = None;
        let mut bytes = 0usize;
        for envelope in batch.envelopes.iter() {
            bytes = bytes.checked_add(envelope.ssz.len()).ok_or(WireError::Oversized)?;
            if bytes > MAX_BATCH_BYTES || !seen.insert(envelope.beacon_block_root) {
                return Err(WireError::Oversized);
            }
            let referenced = block(envelope.beacon_block_root).ok_or(WireError::Ssz)?;
            envelope.validate_reference(config, referenced, self.protocol())?;
            match self {
                Self::Range { start_slot, count } => {
                    let offset =
                        referenced.slot.checked_sub(*start_slot).ok_or(WireError::Range)?;
                    if offset >= *count || prior_slot.is_some_and(|slot| slot >= referenced.slot) {
                        return Err(WireError::Range);
                    }
                    if let (Some(previous_root), Some(previous_slot)) = (prior_root, prior_slot) {
                        // EMPTY fork-choice nodes have blocks but no returned envelope. Follow
                        // their exact original metadata without assuming adjacent payloads.
                        let mut parent = referenced.parent_root;
                        let mut child_slot = referenced.slot;
                        let mut traversed = 0;
                        while parent != previous_root {
                            traversed += 1;
                            if traversed > MAX_ENVELOPE_BATCH {
                                return Err(WireError::Range);
                            }
                            let ancestor = block(parent).ok_or(WireError::Range)?;
                            validate_block_context(config, ancestor)?;
                            if ancestor.root != parent ||
                                ancestor.slot >= child_slot ||
                                ancestor.slot <= previous_slot
                            {
                                return Err(WireError::Range);
                            }
                            child_slot = ancestor.slot;
                            parent = ancestor.parent_root;
                        }
                    }
                    prior_slot = Some(referenced.slot);
                    prior_root = Some(referenced.root);
                }
                Self::Roots(roots) => {
                    if batch.envelopes.len() > roots.len() || !roots.contains(&referenced.root) {
                        return Err(WireError::Ssz);
                    }
                }
            }
        }
        Ok(())
    }

    pub(crate) fn validate_expected(
        &self,
        config: &ChainConfig,
        blocks: &[EnvelopeBlockReference],
    ) -> Result<(), WireError> {
        self.validate_outbound()?;
        self.validate_serving(config, blocks)?;
        if let Self::Roots(roots) = self &&
            roots.iter().any(|root| !blocks.iter().any(|block| block.root == *root))
        {
            return Err(WireError::Ssz);
        }
        Ok(())
    }

    pub(crate) fn validate_serving(
        &self,
        config: &ChainConfig,
        blocks: &[EnvelopeBlockReference],
    ) -> Result<(), WireError> {
        self.encode()?;
        if blocks.len() > MAX_ENVELOPE_BATCH {
            return Err(WireError::Oversized);
        }
        let mut seen = HashSet::new();
        let mut prior_slot = None;
        let mut prior_root = None;
        for block in blocks {
            validate_block_context(config, block)?;
            if !seen.insert(block.root) {
                return Err(WireError::Ssz);
            }
            match self {
                Self::Range { start_slot, count } => {
                    let offset = block.slot.checked_sub(*start_slot).ok_or(WireError::Range)?;
                    if offset >= *count ||
                        prior_slot.is_some_and(|slot| slot >= block.slot) ||
                        prior_root.is_some_and(|root| root != block.parent_root)
                    {
                        return Err(WireError::Range);
                    }
                    prior_slot = Some(block.slot);
                    prior_root = Some(block.root);
                }
                Self::Roots(roots) if !roots.contains(&block.root) => return Err(WireError::Ssz),
                Self::Roots(_) => {}
            }
        }

        Ok(())
    }

    fn encode(&self) -> Result<Vec<u8>, WireError> {
        match self {
            Self::Range { start_slot, count } => {
                Ok(payload_envelopes_by_range(*start_slot, *count, MAX_WIRE_ITEMS as u64)?.to_vec())
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
        let request = match protocol {
            Protocol::PayloadEnvelopesByRangeV1 if bytes.len() == 16 => {
                let start_slot =
                    u64::from_le_bytes(bytes[..8].try_into().map_err(|_| WireError::Ssz)?);
                let count = u64::from_le_bytes(bytes[8..].try_into().map_err(|_| WireError::Ssz)?);
                payload_envelopes_by_range(start_slot, count, MAX_WIRE_ITEMS as u64)?;
                Self::Range { start_slot, count }
            }
            Protocol::PayloadEnvelopesByRootV1
                if bytes.len().is_multiple_of(32) && bytes.len() / 32 <= MAX_WIRE_ITEMS =>
            {
                Self::Roots(
                    bytes
                        .chunks_exact(32)
                        .map(|root| root.try_into().expect("exact root width"))
                        .collect(),
                )
            }
            _ => return Err(WireError::Ssz),
        };
        // Admit the specification's wire request maximum; responses may be smaller.
        request.encode()?;
        Ok(request)
    }
}

/// Small immutable linkage metadata constructed only from a completely decoded signed block.
/// It retains no block SSZ or batch permit, allowing block reservations to be released before RPC.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EnvelopeBlockReference {
    root: Root,
    slot: Slot,
    parent_root: Root,
    fork: Fork,
    context: [u8; 4],
}

impl From<&WireBlock> for EnvelopeBlockReference {
    fn from(block: &WireBlock) -> Self {
        Self {
            root: block.root(),
            slot: block.slot(),
            parent_root: block.parent_root(),
            fork: block.fork(),
            context: block.context(),
        }
    }
}

impl EnvelopeBlockReference {
    /// Derive linkage from the actual native block, including an unsigned trusted anchor.
    /// This never manufactures a signed block, builder signature or execution authority.
    pub fn from_block(block: &BeaconBlock, config: &ChainConfig) -> Result<Self, WireError> {
        config.validate().map_err(|_| WireError::Protocol)?;
        let BeaconBlock::Gloas(block, preset) = block else { return Err(WireError::Ssz) };
        let expected = match config.preset {
            ConfigPreset::Mainnet => Preset::Mainnet,
            ConfigPreset::Minimal => Preset::Minimal,
        };
        if *preset != expected ||
            config.fork_at_slot(block.slot).map_err(|_| WireError::ForkContext)?.fork !=
                Fork::Gloas
        {
            return Err(WireError::ForkContext)
        }
        let reference = Self {
            root: block.hash_tree_root(*preset).map_err(|_| WireError::Ssz)?,
            slot: block.slot,
            parent_root: block.parent_root,
            fork: Fork::Gloas,
            context: config
                .fork_digest(block.slot / config.slots_per_epoch)
                .map_err(|_| WireError::ForkContext)?,
        };
        validate_block_context(config, &reference)?;
        Ok(reference)
    }

    /// Actual beacon message root from the original fully decoded block.
    pub const fn root(&self) -> Root {
        self.root
    }

    /// Exact checked network digest copied or derived from the genuine native block.
    pub const fn context(&self) -> [u8; 4] {
        self.context
    }

    /// Actual beacon slot from the original fully decoded block.
    pub const fn slot(&self) -> Slot {
        self.slot
    }
}

/// Complete signed Gloas envelope SSZ retained once.
/// Structural linkage does not verify the builder signature, EL status or availability.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WireEnvelope {
    beacon_block_root: Root,
    parent_beacon_block_root: Root,
    payload_slot_number: Slot,
    context: [u8; 4],
    ssz: Arc<[u8]>,
}

impl WireEnvelope {
    /// Decode full Gloas SSZ and bind context to an actual decoded referenced beacon block.
    pub fn from_ssz(
        bytes: Vec<u8>,
        referenced: &WireBlock,
        config: &ChainConfig,
    ) -> Result<Self, WireError> {
        let envelope = Self::decode(bytes, referenced.context(), config)?;
        envelope.validate_reference(
            config,
            &referenced.into(),
            Protocol::PayloadEnvelopesByRootV1,
        )?;
        Ok(envelope)
    }

    /// Decode exact signed envelope bytes against metadata derived from a genuine native block.
    /// Structural association remains separate from builder, execution and DA verification.
    pub fn from_reference(
        bytes: Vec<u8>,
        referenced: &EnvelopeBlockReference,
        config: &ChainConfig,
    ) -> Result<Self, WireError> {
        let envelope = Self::decode(bytes, referenced.context, config)?;
        envelope.validate_reference(config, referenced, Protocol::PayloadEnvelopesByRootV1)?;
        Ok(envelope)
    }

    fn decode(bytes: Vec<u8>, context: [u8; 4], config: &ChainConfig) -> Result<Self, WireError> {
        config.validate().map_err(|_| WireError::Protocol)?;
        if bytes.len() > config.max_payload_size.min(10 * 1024 * 1024) {
            return Err(WireError::Oversized);
        }
        let preset = match config.preset {
            ConfigPreset::Mainnet => Preset::Mainnet,
            ConfigPreset::Minimal => Preset::Minimal,
        };
        let signed = gloas::SignedExecutionPayloadEnvelope::from_ssz(&bytes, preset)
            .map_err(|_| WireError::Ssz)?;
        Ok(Self {
            beacon_block_root: signed.message.beacon_block_root,
            parent_beacon_block_root: signed.message.parent_beacon_block_root,
            payload_slot_number: signed.message.payload.slot_number,
            context,
            ssz: bytes.into(),
        })
    }

    fn validate_reference(
        &self,
        config: &ChainConfig,
        block: &EnvelopeBlockReference,
        protocol: Protocol,
    ) -> Result<(), WireError> {
        validate_block_context(config, block)?;
        if self.beacon_block_root != block.root ||
            self.parent_beacon_block_root != block.parent_root ||
            self.payload_slot_number != block.slot
        {
            return Err(WireError::Ssz);
        }
        validate_response_context(
            config,
            protocol,
            &ResponseChunk { code: 0, context: Some(self.context), payload: Vec::new() },
            block.slot,
        )
    }

    /// Beacon message root referenced by the decoded envelope.
    pub const fn beacon_block_root(&self) -> Root {
        self.beacon_block_root
    }

    /// Decoded parent beacon message root.
    pub const fn parent_beacon_block_root(&self) -> Root {
        self.parent_beacon_block_root
    }

    /// Received digest, requiring exact referenced-block context verification.
    pub const fn context(&self) -> [u8; 4] {
        self.context
    }

    /// Shared complete signed envelope bytes.
    pub const fn ssz(&self) -> &Arc<[u8]> {
        &self.ssz
    }
}

/// Immutable bounded envelopes; clones retain the shared block/envelope decode reservation.
#[derive(Clone, Debug)]
pub struct EnvelopeBatch {
    envelopes: Arc<[WireEnvelope]>,
    _reservation: Option<Arc<OwnedSemaphorePermit>>,
}

impl EnvelopeBatch {
    /// Complete envelopes in checked wire order; consensus import is still required.
    pub fn envelopes(&self) -> &[WireEnvelope] {
        &self.envelopes
    }

    pub(crate) fn serving(envelopes: Vec<WireEnvelope>) -> Self {
        Self { envelopes: envelopes.into(), _reservation: None }
    }
}

impl PartialEq for EnvelopeBatch {
    fn eq(&self, other: &Self) -> bool {
        self.envelopes == other.envelopes
    }
}
impl Eq for EnvelopeBatch {}

#[derive(Clone, Debug)]
pub(crate) enum EnvelopeReply {
    Envelopes(EnvelopeBatch),
    Error(u8),
}

#[derive(Clone, Debug)]
pub(crate) struct EnvelopeCodec {
    config: Arc<ChainConfig>,
    read_budget: Arc<Semaphore>,
    write_budget: Arc<Semaphore>,
}

impl EnvelopeCodec {
    pub(crate) fn new(config: Arc<ChainConfig>, blocks: &BlockCodec) -> Self {
        let (read_budget, write_budget) = blocks.budgets();
        Self { config, read_budget, write_budget }
    }

    fn limits(&self) -> Result<Limits, WireError> {
        let max_payload = self.config.max_payload_size.min(10 * 1024 * 1024);
        Ok(Limits { max_payload, max_compressed: max_compressed_len(max_payload)? })
    }
}

#[async_trait]
impl Codec for EnvelopeCodec {
    type Protocol = StreamProtocol;
    type Request = EnvelopeRequest;
    type Response = EnvelopeReply;

    async fn read_request<T>(
        &mut self,
        protocol: &StreamProtocol,
        io: &mut T,
    ) -> io::Result<EnvelopeRequest>
    where
        T: AsyncRead + Unpin + Send,
    {
        let protocol = envelope_protocol(protocol)?;
        let mut decoder =
            StreamDecoder::request(protocol, REQUEST_LIMITS, MAX_WIRE_ITEMS).map_err(wire_io)?;
        let mut buffer = [0; 1024];
        loop {
            match decoder.next_message().map_err(wire_io)? {
                DecodeProgress::Message(WireMessage::Request(bytes)) => {
                    return EnvelopeRequest::decode(protocol, &bytes).map_err(wire_io)
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
    ) -> io::Result<EnvelopeReply>
    where
        T: AsyncRead + Unpin + Send,
    {
        let protocol = envelope_protocol(protocol)?;
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
            MAX_ENVELOPE_BATCH,
            &self.config,
        )
        .map_err(wire_io)?;
        let mut buffer = vec![0; 64 * 1024];
        let mut envelopes = Vec::new();
        let mut bytes = 0usize;
        let mut error_code = None;
        loop {
            match decoder.next_message().map_err(wire_io)? {
                DecodeProgress::Message(WireMessage::Response(chunk)) if chunk.code == 0 => {
                    bytes = bytes
                        .checked_add(chunk.payload.len())
                        .filter(|bytes| *bytes <= MAX_BATCH_BYTES)
                        .ok_or_else(|| wire_io(WireError::Oversized))?;
                    let context = chunk.context.ok_or_else(|| wire_io(WireError::ForkContext))?;
                    let config = self.config.clone();
                    let reservation = reservation.clone();
                    let envelope = reserved_blocking(reservation, move || {
                        WireEnvelope::decode(chunk.payload, context, &config)
                    })
                    .await?;
                    // The matching transport owns original block metadata and completes context
                    // checks.
                    envelopes.push(envelope);
                }
                DecodeProgress::Message(WireMessage::Response(chunk)) => {
                    if !envelopes.is_empty() {
                        return Err(wire_io(WireError::Protocol));
                    }
                    error_code = Some(chunk.code);
                }
                DecodeProgress::Finished => {
                    if io.read(&mut buffer[..1]).await? != 0 {
                        return Err(wire_io(WireError::TrailingBytes));
                    }
                    return Ok(match error_code {
                        Some(code) => EnvelopeReply::Error(code),
                        None => EnvelopeReply::Envelopes(EnvelopeBatch {
                            envelopes: envelopes.into(),
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
        request: EnvelopeRequest,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        if envelope_protocol(protocol)? != request.protocol() {
            return Err(wire_io(WireError::Protocol));
        }
        request.validate_outbound().map_err(wire_io)?;
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
        response: EnvelopeReply,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        envelope_protocol(protocol)?;
        let reservation = Arc::new(
            self.write_budget
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| wire_io(WireError::Closed))?,
        );
        let limits = self.limits().map_err(wire_io)?;
        match response {
            EnvelopeReply::Envelopes(batch) => {
                let bytes = batch
                    .envelopes
                    .iter()
                    .try_fold(0usize, |sum, envelope| sum.checked_add(envelope.ssz.len()))
                    .ok_or_else(|| wire_io(WireError::Oversized))?;
                if batch.envelopes.len() > MAX_ENVELOPE_BATCH || bytes > MAX_BATCH_BYTES {
                    return Err(wire_io(WireError::Oversized));
                }
                for envelope in batch.envelopes.iter() {
                    io.write_all(&[0]).await?;
                    io.write_all(&envelope.context).await?;
                    let ssz = envelope.ssz.clone();
                    let reservation = reservation.clone();
                    let wire = reserved_blocking(reservation, move || encode_request(&ssz, limits))
                        .await?;
                    io.write_all(&wire).await?;
                }
            }
            EnvelopeReply::Error(code @ (1..=3 | 128..=255)) => {
                io.write_all(&[code]).await?;
                io.write_all(
                    &encode_request(b"envelopes unavailable", REQUEST_LIMITS).map_err(wire_io)?,
                )
                .await?;
            }
            EnvelopeReply::Error(_) => return Err(wire_io(WireError::ResponseCode)),
        }
        io.close().await
    }
}

async fn reserved_blocking<T: Send + 'static>(
    reservation: Arc<OwnedSemaphorePermit>,
    work: impl FnOnce() -> Result<T, WireError> + Send + 'static,
) -> io::Result<T> {
    tokio::task::spawn_blocking(move || {
        // Cancellation cannot return the permit while the worker retains bytes.
        let _reservation = reservation;
        work()
    })
    .await
    .map_err(|_| wire_io(WireError::Closed))?
    .map_err(wire_io)
}
fn validate_block_context(
    config: &ChainConfig,
    block: &EnvelopeBlockReference,
) -> Result<(), WireError> {
    config.validate().map_err(|_| WireError::Protocol)?;
    if block.fork != Fork::Gloas ||
        config.fork_at_slot(block.slot).map_err(|_| WireError::ForkContext)?.fork != block.fork ||
        config
            .fork_digest(block.slot / config.slots_per_epoch)
            .map_err(|_| WireError::ForkContext)? !=
            block.context
    {
        return Err(WireError::ForkContext);
    }
    Ok(())
}

fn envelope_protocol(protocol: &StreamProtocol) -> io::Result<Protocol> {
    match Protocol::from_id(protocol.as_ref()).map_err(wire_io)? {
        protocol @ (Protocol::PayloadEnvelopesByRangeV1 | Protocol::PayloadEnvelopesByRootV1) => {
            Ok(protocol)
        }
        _ => Err(wire_io(WireError::Protocol)),
    }
}

fn wire_io(error: WireError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}

#[cfg(test)]
mod tests;
