//! Read-only bounded original-wire serving from actual native receipts and selected ancestry.
//! Saved records alone cannot select roots or branches; unsigned anchors never become signed
//! blocks.

use super::*;
use reth_embedded_beacon_network::{
    block_rpc::{BlockRequest, MAX_BATCH_BYTES},
    envelope_rpc::EnvelopeRequest,
};
use reth_embedded_beacon_primitives::BeaconBlockHeader;

const MAX_SERVING_ANCESTRY: usize = 2048;

/// Exact peer request served through the sole graph owner and its bounded command admission.
#[derive(Clone, Debug)]
pub enum ServingRequest {
    /// Original signed blocks in requested root order or actual selected slot ancestry.
    Blocks(BlockRequest),
    /// Original admitted Gloas envelopes and actual native linkage metadata.
    Envelopes(EnvelopeRequest),
}

/// Immutable original bytes bounded independently of transport response reservations.
#[derive(Clone, Debug)]
pub enum ServingBatch {
    /// Original signed blocks; actual unsigned checkpoint anchors are omitted.
    Blocks(Vec<WireBlock>),
    /// Original retained sidecars after fresh mathematical checks against owner-selected blocks.
    Sidecars(Vec<reth_embedded_beacon_network::availability_rpc::WireSidecar>),
    /// Signed envelopes with exact retained native block metadata, including missing payload
    /// slots.
    Envelopes {
        /// Complete original signed envelope encodings.
        envelopes: Vec<WireEnvelope>,
        /// Metadata from actual native graph blocks, never an advertised root hint.
        blocks: Vec<EnvelopeBlockReference>,
    },
}

impl ImportCoordinator {
    /// Read immutable original peer responses through the existing immediately bounded queue.
    /// None means the request exceeds local history or serving limits, without peer invalidity.
    pub fn serve(
        &self,
        request: ServingRequest,
    ) -> impl Future<Output = Result<Option<ServingBatch>, ImportError>> + Send + 'static {
        let admitted = self.try_serve(request);
        async move { admitted?.await.map_err(|_| ImportError::Closed)? }
    }

    fn try_serve(
        &self,
        request: ServingRequest,
    ) -> Result<oneshot::Receiver<Result<Option<ServingBatch>, ImportError>>, ImportError> {
        if self.is_halted() {
            return Err(ImportError::Halted)
        }
        match &request {
            ServingRequest::Blocks(request) => request.validate_outbound(),
            ServingRequest::Envelopes(request) => request.validate_outbound(),
        }
        .map_err(|_| ImportError::Bounds)?;
        let slot = self.commands.clone().try_acquire_owned().map_err(|error| match error {
            tokio::sync::TryAcquireError::Closed => ImportError::Halted,
            tokio::sync::TryAcquireError::NoPermits => ImportError::Backpressure,
        })?;
        let queue = self.sender.try_reserve().map_err(|error| match error {
            mpsc::error::TrySendError::Closed(()) => ImportError::Closed,
            mpsc::error::TrySendError::Full(()) => ImportError::Backpressure,
        })?;
        let (reply, receive) = oneshot::channel();
        self.observe_queue_admission();
        queue.send(Command::Serve { request, reply, _slot: slot });
        Ok(receive)
    }
}

impl Graph {
    pub(super) fn serving_header(
        &self,
        root: Root,
    ) -> Result<Option<BeaconBlockHeader>, ImportError> {
        match self {
            Self::Classic { store, .. } => store
                .block(root)
                .map(BeaconBlock::header)
                .transpose()
                .map_err(|_| ImportError::Wire),
            Self::Gloas { store, .. } => {
                if let Some(block) = store.block(root) {
                    return block
                        .header(ReadGloasGossipView::store_parameters(store).phase0.preset)
                        .map(Some)
                        .map_err(|_| ImportError::Wire)
                }
                ReadGloasGossipView::historical_block(store, root)
                    .map(BeaconBlock::header)
                    .transpose()
                    .map_err(|_| ImportError::Wire)
            }
        }
    }

    fn serving_range(&self, start: u64, count: u64) -> Result<Option<Vec<Root>>, ImportError> {
        let Some(end) = start.checked_add(count) else { return Ok(None) };
        let mut root = self.snapshot()?.head.root;
        let mut roots = Vec::new();
        // Walk only the bounded hot selected ancestry. The source independently contributes
        // original finalized ancestors authenticated by actual committed native receipts.
        for _ in 0..MAX_SERVING_ANCESTRY {
            let Some(header) = self.serving_header(root)? else { break };
            if header.slot < start {
                roots.reverse();
                return Ok(Some(roots))
            }
            if header.slot < end {
                roots.push(root);
            }
            if header.parent_root == [0; 32] {
                roots.reverse();
                return Ok(Some(roots))
            }
            if header.parent_root == root {
                return Err(ImportError::Wire)
            }
            root = header.parent_root;
        }
        roots.reverse();
        Ok(Some(roots))
    }

    fn serving_gloas_reference(
        &self,
        root: Root,
        config: &ChainConfig,
    ) -> Result<Option<EnvelopeBlockReference>, ImportError> {
        let Self::Gloas { store, .. } = self else { return Ok(None) };
        store
            .block(root)
            .map(|block| {
                EnvelopeBlockReference::from_block(
                    &BeaconBlock::Gloas(
                        block.clone(),
                        ReadGloasGossipView::store_parameters(store).phase0.preset,
                    ),
                    config,
                )
            })
            .transpose()
            .map_err(|_| ImportError::Wire)
    }
}

impl<E, C, G> Worker<E, C, G>
where
    E: ExecutionEngine<
            Payload = ExecutionData,
            PayloadReply = PayloadReply,
            Forkchoice = ForkchoiceState,
            ForkchoiceReply = ForkchoiceReply,
        > + Clone,
    E::Error: std::error::Error + Send + Sync + 'static,
    C: ClassicAvailability,
    G: GloasAvailability,
{
    pub(super) fn serve(
        &self,
        request: ServingRequest,
    ) -> Result<Option<ServingBatch>, ImportError> {
        match request {
            ServingRequest::Blocks(request) => {
                if request.validate_outbound().is_err() {
                    return Ok(None)
                }
                let roots = match &request {
                    BlockRequest::Roots(roots) => roots.clone(),
                    BlockRequest::Range { start_slot, count } => {
                        self.graph.serving_range(*start_slot, *count)?.unwrap_or_default()
                    }
                };
                // Original signed ancestors remain serviceable under their independent owner
                // after hot-state pruning. This getter rechecks exact native proposer BLS.
                let mut by_root = self
                    .source
                    .serving_contexts(&request)
                    .map_err(|error| ImportError::Persistence(error.to_string()))?
                    .unwrap_or_default()
                    .into_iter()
                    .map(|wire| (wire.root(), wire))
                    .collect::<std::collections::BTreeMap<_, _>>();
                let mut retained = by_root.values().try_fold(0usize, |total, wire| {
                    total.checked_add(wire.ssz().len()).ok_or(ImportError::Bounds)
                })?;
                for root in roots {
                    if by_root.contains_key(&root) || self.graph.serving_header(root)?.is_none() {
                        continue
                    }
                    let Some((format, bytes)) = self
                        .repository
                        .original_block(root)
                        .map_err(|error| ImportError::Persistence(error.to_string()))?
                    else {
                        continue
                    };
                    if format != CheckpointBlockFormat::Signed {
                        continue
                    }
                    retained = retained.checked_add(bytes.len()).ok_or(ImportError::Bounds)?;
                    if retained > MAX_BATCH_BYTES {
                        return Ok(None)
                    }
                    let wire = WireBlock::from_ssz(bytes.to_vec(), &self.config)
                        .map_err(|_| ImportError::Wire)?;
                    if wire.root() != root {
                        return Err(ImportError::Wire)
                    }
                    by_root.insert(root, wire);
                }
                let blocks = match &request {
                    BlockRequest::Roots(roots) => {
                        roots.iter().filter_map(|root| by_root.remove(root)).collect::<Vec<_>>()
                    }
                    BlockRequest::Range { .. } => {
                        let mut blocks = by_root.into_values().collect::<Vec<_>>();
                        blocks.sort_by_key(WireBlock::slot);
                        if blocks.windows(2).any(|pair| pair[0].slot() == pair[1].slot()) {
                            return Err(ImportError::Wire)
                        }
                        blocks
                    }
                };
                request.matches(&blocks).map_err(|_| ImportError::Wire)?;
                Ok(Some(ServingBatch::Blocks(blocks)))
            }
            ServingRequest::Envelopes(request) => {
                if request.validate_outbound().is_err() {
                    return Ok(None)
                }
                let roots = match &request {
                    EnvelopeRequest::Roots(roots) => roots.clone(),
                    EnvelopeRequest::Range { start_slot, count } => {
                        let Some(roots) = self.graph.serving_range(*start_slot, *count)? else {
                            return Ok(None)
                        };
                        roots
                    }
                };
                let mut blocks = Vec::new();
                let mut envelopes = Vec::new();
                let mut retained = 0usize;
                for root in roots {
                    let Some(reference) = self.graph.serving_gloas_reference(root, &self.config)?
                    else {
                        continue
                    };
                    if let Some(bytes) = self
                        .repository
                        .original_envelope(root)
                        .map_err(|error| ImportError::Persistence(error.to_string()))?
                    {
                        retained = retained.checked_add(bytes.len()).ok_or(ImportError::Bounds)?;
                        if retained > MAX_BATCH_BYTES {
                            return Ok(None)
                        }
                        let wire = WireEnvelope::from_reference(bytes, &reference, &self.config)
                            .map_err(|_| ImportError::Wire)?;
                        envelopes.push(wire);
                    }
                    blocks.push(reference);
                }
                Ok(Some(ServingBatch::Envelopes { envelopes, blocks }))
            }
        }
    }
}
