//! Bounded exact-wire blob and data-column retrieval using shared transport reservations.
//!
//! Complete SSZ, request membership and fork context are checked here. Original bytes remain
//! untrusted until the candidate's mandatory signature, inclusion and KZG checks complete.

use crate::{
    block_rpc::{BlockCodec, MAX_BATCH_BYTES},
    encode_request, max_compressed_len, payload_envelopes_by_range, validate_response_context,
    DecodeProgress, Limits, Protocol, ResponseChunk, StreamDecoder, WireError, WireMessage,
};
use async_trait::async_trait;
use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use libp2p::{request_response::Codec, StreamProtocol};
use reth_embedded_beacon_config::{ChainConfig, Preset as ConfigPreset};
use reth_embedded_beacon_primitives::{Fork, Root, Slot};
use reth_embedded_beacon_state_transition::ssz::{
    auxiliary::{deneb, electra, fulu, gloas},
    Preset,
};
use std::{collections::HashSet, io, sync::Arc};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Maximum requested or retained sidecars in one local RPC; partial responses are permitted.
pub const MAX_SIDECAR_BATCH: usize = 128;
const MAX_WIRE_ROOTS: usize = 128;
const MAX_WIRE_BLOB_IDS: usize = 1152;
const MAX_WIRE_COLUMNS: usize = 128 * 128;
const REQUEST_LIMITS: Limits = Limits { max_payload: 160 * 1024, max_compressed: 192 * 1024 };

/// One blob belonging to a particular consensus message root.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct BlobIdentifier {
    /// Consensus block message root.
    pub block_root: Root,
    /// Ordered commitment index within that block.
    pub index: u64,
}

/// Requested column indices belonging to one consensus message root.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnIdentifier {
    /// Consensus block message root.
    pub block_root: Root,
    /// Column indices in the 128-column extension.
    pub columns: Vec<u64>,
}

/// Exact pinned blob or column request without an asserted availability result.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SidecarRequest {
    /// Legacy blob slot range; post-Fulu missing blobs may yield an empty response.
    BlobRange {
        /// First requested beacon slot.
        start_slot: Slot,
        /// Number of requested beacon slots.
        count: u64,
    },
    /// Individual legacy blob identifiers.
    BlobRoots(Vec<BlobIdentifier>),
    /// Contiguous slot range with explicitly requested columns.
    ColumnRange {
        /// First requested beacon slot.
        start_slot: Slot,
        /// Number of requested beacon slots.
        count: u64,
        /// Exact column indices requested from each known block.
        columns: Vec<u64>,
    },
    /// Column groups bound to original beacon message roots.
    ColumnRoots(Vec<ColumnIdentifier>),
}

impl SidecarRequest {
    /// Exact negotiated protocol, including its version.
    pub const fn protocol(&self) -> Protocol {
        match self {
            Self::BlobRange { .. } => Protocol::BlobSidecarsByRangeV1,
            Self::BlobRoots(_) => Protocol::BlobSidecarsByRootV1,
            Self::ColumnRange { .. } => Protocol::DataColumnsByRangeV1,
            Self::ColumnRoots(_) => Protocol::DataColumnsByRootV1,
        }
    }

    /// Apply the follower's smaller work budget before retaining inputs or opening a stream.
    /// Blob range requests reserve nine items per slot, covering the legacy Electra maximum.
    pub fn validate_outbound(&self) -> Result<(), WireError> {
        self.encode()?;
        let count = match self {
            Self::BlobRange { count, .. } => usize::try_from(*count)
                .ok()
                .and_then(|n| n.checked_mul(9))
                .ok_or(WireError::Oversized)?,
            Self::BlobRoots(ids) => {
                if ids.iter().copied().collect::<HashSet<_>>().len() != ids.len() {
                    return Err(WireError::Ssz)
                }
                ids.len()
            }
            Self::ColumnRange { count, columns, .. } => {
                validate_unique_columns(columns)?;
                usize::try_from(*count)
                    .ok()
                    .and_then(|n| n.checked_mul(columns.len()))
                    .ok_or(WireError::Oversized)?
            }
            Self::ColumnRoots(ids) => {
                let mut seen = HashSet::new();
                for id in ids {
                    for column in &id.columns {
                        if !seen.insert((id.block_root, *column)) {
                            return Err(WireError::Ssz)
                        }
                    }
                }
                seen.len()
            }
        };
        if count == 0 || count > MAX_SIDECAR_BATCH {
            return Err(WireError::Oversized)
        }
        Ok(())
    }

    /// Match every returned original sidecar to the exact request and configured historical slot.
    /// A missing item stays unavailable; it cannot establish sampled availability.
    pub fn matches(&self, config: &ChainConfig, batch: &SidecarBatch) -> Result<(), WireError> {
        self.encode()?;
        if batch.sidecars.len() > MAX_SIDECAR_BATCH {
            return Err(WireError::Oversized)
        }
        let mut seen = HashSet::new();
        let mut prior = None;
        let mut bytes = 0usize;
        for sidecar in batch.sidecars.iter() {
            bytes = bytes
                .checked_add(sidecar.ssz.len())
                .filter(|n| *n <= MAX_BATCH_BYTES)
                .ok_or(WireError::Oversized)?;
            sidecar.validate_context(config, self.protocol())?;
            if !seen.insert((sidecar.block_root, sidecar.index)) {
                return Err(WireError::Ssz)
            }
            let membership = match self {
                Self::BlobRange { start_slot, count } => {
                    sidecar.slot.checked_sub(*start_slot).is_some_and(|n| n < *count)
                }
                Self::BlobRoots(ids) => ids.contains(&BlobIdentifier {
                    block_root: sidecar.block_root,
                    index: sidecar.index,
                }),
                Self::ColumnRange { start_slot, count, columns } => {
                    sidecar.slot.checked_sub(*start_slot).is_some_and(|n| n < *count) &&
                        columns.contains(&sidecar.index)
                }
                Self::ColumnRoots(ids) => ids.iter().any(|id| {
                    id.block_root == sidecar.block_root && id.columns.contains(&sidecar.index)
                }),
            };
            if !membership {
                return Err(WireError::Range)
            }
            if matches!(self, Self::BlobRange { .. } | Self::ColumnRange { .. }) {
                if prior.is_some_and(|(slot, index, root)| {
                    (slot, index) >= (sidecar.slot, sidecar.index) ||
                        slot == sidecar.slot && root != sidecar.block_root
                }) {
                    return Err(WireError::Range)
                }
                prior = Some((sidecar.slot, sidecar.index, sidecar.block_root));
            }
        }
        Ok(())
    }

    fn encode(&self) -> Result<Vec<u8>, WireError> {
        match self {
            Self::BlobRange { start_slot, count } => {
                Ok(payload_envelopes_by_range(*start_slot, *count, MAX_WIRE_ROOTS as u64)?.to_vec())
            }
            Self::BlobRoots(ids) => {
                if ids.len() > MAX_WIRE_BLOB_IDS {
                    return Err(WireError::Oversized)
                }
                let mut bytes = Vec::with_capacity(ids.len() * 40);
                for id in ids {
                    bytes.extend_from_slice(&id.block_root);
                    bytes.extend_from_slice(&id.index.to_le_bytes());
                }
                Ok(bytes)
            }
            Self::ColumnRange { start_slot, count, columns } => {
                let mut bytes =
                    payload_envelopes_by_range(*start_slot, *count, MAX_WIRE_ROOTS as u64)?
                        .to_vec();
                validate_columns(columns)?;
                bytes.extend_from_slice(&20u32.to_le_bytes());
                for index in columns {
                    bytes.extend_from_slice(&index.to_le_bytes());
                }
                Ok(bytes)
            }
            Self::ColumnRoots(ids) => {
                if ids.len() > MAX_WIRE_ROOTS {
                    return Err(WireError::Oversized)
                }
                let mut offset = ids.len() * 4;
                let mut bytes = Vec::with_capacity(offset);
                for id in ids {
                    validate_columns(&id.columns)?;
                    bytes.extend_from_slice(
                        &u32::try_from(offset).map_err(|_| WireError::Oversized)?.to_le_bytes(),
                    );
                    offset = offset
                        .checked_add(36 + id.columns.len() * 8)
                        .ok_or(WireError::Oversized)?;
                }
                for id in ids {
                    bytes.extend_from_slice(&id.block_root);
                    bytes.extend_from_slice(&36u32.to_le_bytes());
                    for index in &id.columns {
                        bytes.extend_from_slice(&index.to_le_bytes());
                    }
                }
                Ok(bytes)
            }
        }
    }

    fn decode(protocol: Protocol, bytes: &[u8]) -> Result<Self, WireError> {
        let request = match protocol {
            Protocol::BlobSidecarsByRangeV1 if bytes.len() == 16 => {
                Self::BlobRange { start_slot: uint64(bytes, 0)?, count: uint64(bytes, 8)? }
            }
            Protocol::BlobSidecarsByRootV1
                if bytes.len().is_multiple_of(40) && bytes.len() / 40 <= MAX_WIRE_BLOB_IDS =>
            {
                Self::BlobRoots(
                    bytes
                        .chunks_exact(40)
                        .map(|id| {
                            Ok(BlobIdentifier {
                                block_root: id[..32].try_into().map_err(|_| WireError::Ssz)?,
                                index: uint64(id, 32)?,
                            })
                        })
                        .collect::<Result<Vec<_>, WireError>>()?,
                )
            }
            Protocol::DataColumnsByRangeV1 if bytes.len() >= 20 && uint32(bytes, 16)? == 20 => {
                Self::ColumnRange {
                    start_slot: uint64(bytes, 0)?,
                    count: uint64(bytes, 8)?,
                    columns: decode_columns(&bytes[20..])?,
                }
            }
            Protocol::DataColumnsByRootV1 => {
                if bytes.is_empty() {
                    Self::ColumnRoots(Vec::new())
                } else {
                    let first = uint32(bytes, 0)?;
                    if first == 0 ||
                        !first.is_multiple_of(4) ||
                        first > bytes.len() ||
                        first / 4 > MAX_WIRE_ROOTS
                    {
                        return Err(WireError::Ssz)
                    }
                    let mut ids = Vec::with_capacity(first / 4);
                    for index in 0..first / 4 {
                        let start = uint32(bytes, index * 4)?;
                        let end = if index + 1 == first / 4 {
                            bytes.len()
                        } else {
                            uint32(bytes, (index + 1) * 4)?
                        };
                        if start < first || end < start || end > bytes.len() {
                            return Err(WireError::Ssz)
                        }
                        let id = bytes.get(start..end).ok_or(WireError::Ssz)?;
                        if id.len() < 36 || uint32(id, 32)? != 36 {
                            return Err(WireError::Ssz)
                        }
                        ids.push(ColumnIdentifier {
                            block_root: id[..32].try_into().map_err(|_| WireError::Ssz)?,
                            columns: decode_columns(&id[36..])?,
                        });
                    }
                    Self::ColumnRoots(ids)
                }
            }
            _ => return Err(WireError::Ssz),
        };
        // Re-encoding enforces canonical offsets, including the first list/container offset.
        if request.encode()? != bytes {
            return Err(WireError::Ssz)
        }
        Ok(request)
    }
}

/// Sidecar family decoded from complete SSZ, not inferred from a payload timestamp.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SidecarKind {
    /// Deneb/Electra legacy blob sidecar.
    Blob,
    /// Fulu/Gloas full data-column sidecar.
    Column,
}

/// Structurally checked original bytes; no signature or KZG validity is asserted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WireSidecar {
    block_root: Root,
    slot: Slot,
    index: u64,
    fork: Fork,
    kind: SidecarKind,
    context: [u8; 4],
    ssz: Arc<[u8]>,
}

impl WireSidecar {
    /// Completely decode the exact sidecar family and check its actual historical digest.
    pub fn from_ssz(
        bytes: Vec<u8>,
        context: [u8; 4],
        protocol: Protocol,
        config: &ChainConfig,
    ) -> Result<Self, WireError> {
        config.validate().map_err(|_| WireError::Protocol)?;
        if bytes.len() > config.max_payload_size.min(10 * 1024 * 1024) {
            return Err(WireError::Oversized)
        }
        let preset = match config.preset {
            ConfigPreset::Mainnet => Preset::Mainnet,
            ConfigPreset::Minimal => Preset::Minimal,
        };
        let fork = config
            .forks
            .activations()
            .iter()
            .map(|a| a.epoch)
            .chain(config.blob_schedule.iter().map(|a| a.epoch))
            .filter(|epoch| *epoch != u64::MAX)
            .find_map(|epoch| {
                (config.fork_digest(epoch).ok() == Some(context))
                    .then(|| config.forks.at_epoch(epoch).ok().map(|a| a.fork))
                    .flatten()
            })
            .ok_or(WireError::ForkContext)?;
        let (kind, slot, root, index) = if is_blob(protocol) {
            match fork {
                Fork::Deneb => {
                    let s =
                        deneb::BlobSidecar::from_ssz(&bytes, preset).map_err(|_| WireError::Ssz)?;
                    (
                        SidecarKind::Blob,
                        s.signed_block_header.message.slot,
                        s.signed_block_header.message.hash_tree_root(),
                        s.index,
                    )
                }
                Fork::Electra => {
                    let s = electra::BlobSidecar::from_ssz(&bytes, preset)
                        .map_err(|_| WireError::Ssz)?;
                    (
                        SidecarKind::Blob,
                        s.signed_block_header.message.slot,
                        s.signed_block_header.message.hash_tree_root(),
                        s.index,
                    )
                }
                _ => return Err(WireError::ForkContext),
            }
        } else if is_column(protocol) {
            match fork {
                Fork::Fulu => {
                    let s = fulu::DataColumnSidecar::from_ssz(&bytes, preset)
                        .map_err(|_| WireError::Ssz)?;
                    if s.column.values.is_empty() ||
                        s.column.values.len() != s.kzg_commitments.values.len() ||
                        s.kzg_proofs.values.len() != s.column.values.len() ||
                        s.column.values.len() >
                            config
                                .blob_parameters(
                                    s.signed_block_header.message.slot / config.slots_per_epoch,
                                )
                                .map_err(|_| WireError::ForkContext)?
                                .max_blobs_per_block as usize
                    {
                        return Err(WireError::Ssz)
                    }
                    (
                        SidecarKind::Column,
                        s.signed_block_header.message.slot,
                        s.signed_block_header.message.hash_tree_root(),
                        s.index,
                    )
                }
                Fork::Gloas => {
                    let s = gloas::DataColumnSidecar::from_ssz(&bytes, preset)
                        .map_err(|_| WireError::Ssz)?;
                    if s.column.values.is_empty() ||
                        s.column.values.len() != s.kzg_proofs.values.len() ||
                        s.column.values.len() >
                            config
                                .blob_parameters(s.slot / config.slots_per_epoch)
                                .map_err(|_| WireError::ForkContext)?
                                .max_blobs_per_block as usize
                    {
                        return Err(WireError::Ssz)
                    }
                    (SidecarKind::Column, s.slot, s.beacon_block_root, s.index)
                }
                _ => return Err(WireError::ForkContext),
            }
        } else {
            return Err(WireError::Protocol)
        };
        let result = Self { block_root: root, slot, index, fork, kind, context, ssz: bytes.into() };
        result.validate_context(config, protocol)?;
        Ok(result)
    }

    fn validate_context(&self, config: &ChainConfig, protocol: Protocol) -> Result<(), WireError> {
        let expected_kind = if is_blob(protocol) {
            SidecarKind::Blob
        } else if is_column(protocol) {
            SidecarKind::Column
        } else {
            return Err(WireError::Protocol)
        };
        if self.kind != expected_kind ||
            config.fork_at_slot(self.slot).map_err(|_| WireError::ForkContext)?.fork != self.fork
        {
            return Err(WireError::ForkContext)
        }
        let index_limit = match self.kind {
            SidecarKind::Blob => {
                config
                    .blob_parameters(self.slot / config.slots_per_epoch)
                    .map_err(|_| WireError::ForkContext)?
                    .max_blobs_per_block
            }
            SidecarKind::Column => 128,
        };
        if self.index >= index_limit {
            return Err(WireError::Ssz)
        }
        validate_response_context(
            config,
            protocol,
            &ResponseChunk { code: 0, context: Some(self.context), payload: Vec::new() },
            self.slot,
        )
    }

    /// Exact consensus message root referenced by the sidecar.
    pub const fn block_root(&self) -> Root {
        self.block_root
    }
    /// Actual decoded beacon slot.
    pub const fn slot(&self) -> Slot {
        self.slot
    }
    /// Original commitment or column index.
    pub const fn index(&self) -> u64 {
        self.index
    }
    /// Exact schema selected by the checked digest and actual slot.
    pub const fn fork(&self) -> Fork {
        self.fork
    }
    /// Structurally decoded family.
    pub const fn kind(&self) -> SidecarKind {
        self.kind
    }
    /// Original validated historical response digest.
    pub const fn context(&self) -> [u8; 4] {
        self.context
    }
    /// Shared complete original SSZ bytes.
    pub const fn ssz(&self) -> &Arc<[u8]> {
        &self.ssz
    }
}

/// Immutable batch whose clones retain the shared decoder/retention reservation.
#[derive(Clone, Debug)]
pub struct SidecarBatch {
    sidecars: Arc<[WireSidecar]>,
    _reservation: Option<Arc<OwnedSemaphorePermit>>,
}
impl SidecarBatch {
    /// Complete structurally checked original sidecars.
    pub fn sidecars(&self) -> &[WireSidecar] {
        &self.sidecars
    }
    pub(crate) fn serving(sidecars: Vec<WireSidecar>) -> Self {
        Self { sidecars: sidecars.into(), _reservation: None }
    }
}
impl PartialEq for SidecarBatch {
    fn eq(&self, other: &Self) -> bool {
        self.sidecars == other.sidecars
    }
}
impl Eq for SidecarBatch {}

#[derive(Clone, Debug)]
pub(crate) enum SidecarReply {
    Sidecars(SidecarBatch),
    Error(u8),
}

#[derive(Clone, Debug)]
pub(crate) struct SidecarCodec {
    config: Arc<ChainConfig>,
    read_budget: Arc<Semaphore>,
    write_budget: Arc<Semaphore>,
}
impl SidecarCodec {
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
impl Codec for SidecarCodec {
    type Protocol = StreamProtocol;
    type Request = SidecarRequest;
    type Response = SidecarReply;

    async fn read_request<T: AsyncRead + Unpin + Send>(
        &mut self,
        protocol: &StreamProtocol,
        io: &mut T,
    ) -> io::Result<SidecarRequest> {
        let protocol = sidecar_protocol(protocol)?;
        let mut decoder =
            StreamDecoder::request(protocol, REQUEST_LIMITS, MAX_WIRE_COLUMNS).map_err(wire_io)?;
        let mut buffer = [0; 4096];
        loop {
            match decoder.next_message().map_err(wire_io)? {
                DecodeProgress::Message(WireMessage::Request(bytes)) => {
                    return SidecarRequest::decode(protocol, &bytes).map_err(wire_io)
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

    async fn read_response<T: AsyncRead + Unpin + Send>(
        &mut self,
        protocol: &StreamProtocol,
        io: &mut T,
    ) -> io::Result<SidecarReply> {
        let protocol = sidecar_protocol(protocol)?;
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
            MAX_SIDECAR_BATCH,
            &self.config,
        )
        .map_err(wire_io)?;
        let mut buffer = vec![0; 64 * 1024];
        let mut sidecars = Vec::new();
        let mut bytes = 0usize;
        let mut error_code = None;
        loop {
            match decoder.next_message().map_err(wire_io)? {
                DecodeProgress::Message(WireMessage::Response(chunk)) if chunk.code == 0 => {
                    bytes = bytes
                        .checked_add(chunk.payload.len())
                        .filter(|n| *n <= MAX_BATCH_BYTES)
                        .ok_or_else(|| wire_io(WireError::Oversized))?;
                    let context = chunk.context.ok_or_else(|| wire_io(WireError::ForkContext))?;
                    let config = self.config.clone();
                    let worker_reservation = reservation.clone();
                    sidecars.push(
                        reserved_blocking(worker_reservation, move || {
                            WireSidecar::from_ssz(chunk.payload, context, protocol, &config)
                        })
                        .await?,
                    );
                }
                DecodeProgress::Message(WireMessage::Response(chunk)) => {
                    if !sidecars.is_empty() {
                        return Err(wire_io(WireError::Protocol))
                    }
                    error_code = Some(chunk.code);
                }
                DecodeProgress::Finished => {
                    if io.read(&mut buffer[..1]).await? != 0 {
                        return Err(wire_io(WireError::TrailingBytes))
                    }
                    return Ok(match error_code {
                        Some(code) => SidecarReply::Error(code),
                        None => SidecarReply::Sidecars(SidecarBatch {
                            sidecars: sidecars.into(),
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

    async fn write_request<T: AsyncWrite + Unpin + Send>(
        &mut self,
        protocol: &StreamProtocol,
        io: &mut T,
        request: SidecarRequest,
    ) -> io::Result<()> {
        if sidecar_protocol(protocol)? != request.protocol() {
            return Err(wire_io(WireError::Protocol))
        }
        request.validate_outbound().map_err(wire_io)?;
        io.write_all(
            &encode_request(&request.encode().map_err(wire_io)?, REQUEST_LIMITS)
                .map_err(wire_io)?,
        )
        .await?;
        io.close().await
    }

    async fn write_response<T: AsyncWrite + Unpin + Send>(
        &mut self,
        protocol: &StreamProtocol,
        io: &mut T,
        response: SidecarReply,
    ) -> io::Result<()> {
        let protocol = sidecar_protocol(protocol)?;
        let reservation = Arc::new(
            self.write_budget
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| wire_io(WireError::Closed))?,
        );
        let limits = self.limits().map_err(wire_io)?;
        match response {
            SidecarReply::Sidecars(batch) => {
                let bytes = batch
                    .sidecars
                    .iter()
                    .try_fold(0usize, |sum, sidecar| sum.checked_add(sidecar.ssz.len()))
                    .ok_or_else(|| wire_io(WireError::Oversized))?;
                if batch.sidecars.len() > MAX_SIDECAR_BATCH || bytes > MAX_BATCH_BYTES {
                    return Err(wire_io(WireError::Oversized))
                }
                for sidecar in batch.sidecars.iter() {
                    sidecar.validate_context(&self.config, protocol).map_err(wire_io)?;
                }
                for sidecar in batch.sidecars.iter() {
                    io.write_all(&[0]).await?;
                    io.write_all(&sidecar.context).await?;
                    let ssz = sidecar.ssz.clone();
                    let worker_reservation = reservation.clone();
                    let wire =
                        reserved_blocking(worker_reservation, move || encode_request(&ssz, limits))
                            .await?;
                    io.write_all(&wire).await?;
                }
            }
            SidecarReply::Error(code @ (1..=3 | 128..=255)) => {
                io.write_all(&[code]).await?;
                io.write_all(
                    &encode_request(b"availability data unavailable", REQUEST_LIMITS)
                        .map_err(wire_io)?,
                )
                .await?;
            }
            SidecarReply::Error(_) => return Err(wire_io(WireError::ResponseCode)),
        }
        io.close().await
    }
}

async fn reserved_blocking<T: Send + 'static>(
    reservation: Arc<OwnedSemaphorePermit>,
    work: impl FnOnce() -> Result<T, WireError> + Send + 'static,
) -> io::Result<T> {
    tokio::task::spawn_blocking(move || {
        let _reservation = reservation;
        work()
    })
    .await
    .map_err(|_| wire_io(WireError::Closed))?
    .map_err(wire_io)
}
fn validate_columns(columns: &[u64]) -> Result<(), WireError> {
    if columns.len() > 128 || columns.iter().any(|index| *index >= 128) {
        return Err(WireError::Ssz)
    }
    Ok(())
}
fn validate_unique_columns(columns: &[u64]) -> Result<(), WireError> {
    validate_columns(columns)?;
    if columns.iter().copied().collect::<HashSet<_>>().len() != columns.len() {
        return Err(WireError::Ssz)
    }
    Ok(())
}
fn decode_columns(bytes: &[u8]) -> Result<Vec<u64>, WireError> {
    if !bytes.len().is_multiple_of(8) || bytes.len() / 8 > 128 {
        return Err(WireError::Ssz)
    }
    let columns =
        bytes.chunks_exact(8).map(|part| uint64(part, 0)).collect::<Result<Vec<_>, _>>()?;
    validate_columns(&columns)?;
    Ok(columns)
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
fn uint32(bytes: &[u8], offset: usize) -> Result<usize, WireError> {
    Ok(u32::from_le_bytes(
        bytes
            .get(offset..offset + 4)
            .ok_or(WireError::Ssz)?
            .try_into()
            .map_err(|_| WireError::Ssz)?,
    ) as usize)
}
const fn is_blob(protocol: Protocol) -> bool {
    matches!(protocol, Protocol::BlobSidecarsByRangeV1 | Protocol::BlobSidecarsByRootV1)
}
const fn is_column(protocol: Protocol) -> bool {
    matches!(protocol, Protocol::DataColumnsByRangeV1 | Protocol::DataColumnsByRootV1)
}
fn sidecar_protocol(protocol: &StreamProtocol) -> io::Result<Protocol> {
    let protocol = Protocol::from_id(protocol.as_ref()).map_err(wire_io)?;
    if !is_blob(protocol) && !is_column(protocol) {
        return Err(wire_io(WireError::Protocol))
    }
    Ok(protocol)
}
fn wire_io(error: WireError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}

#[cfg(test)]
pub(crate) mod tests;
