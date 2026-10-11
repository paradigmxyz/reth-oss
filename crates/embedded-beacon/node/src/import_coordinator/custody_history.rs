//! Original historical contexts are acquired only along the actual finalized parent chain.
//!
//! Fresh native-registry BLS and exact committed root linkage grant sidecar context only.
//! These data receipts never admit fork-choice blocks, post-states or execution payloads.

use super::*;

#[derive(Clone, Copy, Debug)]
pub(crate) struct HistoricalDependency {
    root: Root,
    upper_slot: u64,
    boundary: Root,
}
impl HistoricalDependency {
    pub(crate) const fn root(self) -> Root {
        self.root
    }
    pub(crate) const fn upper_slot(self) -> u64 {
        self.upper_slot
    }
}
#[derive(Debug)]
pub(crate) struct HistoricalContextReceipt {
    original: WireBlock,
    key: [u8; 48],
    boundary: Root,
    digest: Root,
}
impl HistoricalContextReceipt {
    pub(crate) const fn original(&self) -> &WireBlock {
        &self.original
    }
    pub(crate) const fn key(&self) -> [u8; 48] {
        self.key
    }
    pub(crate) const fn boundary(&self) -> Root {
        self.boundary
    }
    pub(crate) const fn digest(&self) -> Root {
        self.digest
    }
}
impl ImportCoordinator {
    pub(crate) fn custody_dependency(
        &self,
    ) -> impl Future<Output = Result<Option<HistoricalDependency>, ImportError>> + Send + 'static
    {
        let admitted = self.try_custody_dependency();
        async move { admitted?.await.map_err(|_| ImportError::Closed)? }
    }
    fn try_custody_dependency(
        &self,
    ) -> Result<oneshot::Receiver<Result<Option<HistoricalDependency>, ImportError>>, ImportError>
    {
        if self.is_halted() {
            return Err(ImportError::Halted)
        }
        let slot =
            self.commands.clone().try_acquire_owned().map_err(|_| ImportError::Backpressure)?;
        let queue = self.sender.try_reserve().map_err(|_| ImportError::Backpressure)?;
        let (reply, receive) = oneshot::channel();
        self.observe_queue_admission();
        queue.send(Command::CustodyDependency { reply, _slot: slot });
        Ok(receive)
    }
    pub(crate) fn retain_custody_context(
        &self,
        block: WireBlock,
    ) -> impl Future<Output = Result<(), ImportError>> + Send + 'static {
        let admitted = self.try_custody_context(block);
        async move { admitted?.await.map_err(|_| ImportError::Closed)? }
    }
    fn try_custody_context(
        &self,
        block: WireBlock,
    ) -> Result<oneshot::Receiver<Result<(), ImportError>>, ImportError> {
        if self.is_halted() {
            return Err(ImportError::Halted)
        }
        let count = block.ssz().len();
        if count == 0 || count > self.limits.max_input_bytes {
            return Err(ImportError::Bounds)
        }
        let bytes = self
            .bytes
            .clone()
            .try_acquire_many_owned(u32::try_from(count).map_err(|_| ImportError::Bounds)?)
            .map_err(|_| ImportError::Backpressure)?;
        let slot =
            self.commands.clone().try_acquire_owned().map_err(|_| ImportError::Backpressure)?;
        let queue = self.sender.try_reserve().map_err(|_| ImportError::Backpressure)?;
        let (reply, receive) = oneshot::channel();
        self.observe_queue_admission();
        queue.send(Command::CustodyContext { block, reply, _bytes: bytes, _slot: slot });
        Ok(receive)
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
    pub(super) fn custody_dependency(&self) -> Result<Option<HistoricalDependency>, ImportError> {
        let snapshot = self.graph.snapshot()?;
        let floor = (self.graph.current_slot() / self.config.slots_per_epoch).saturating_sub(4096) *
            self.config.slots_per_epoch;
        let missing_anchor = self
            .unsigned_custody_context
            .as_ref()
            .map(|context| self.source.custody_missing_signed_anchor(snapshot, context, floor))
            .transpose()
            .map_err(|error| ImportError::Persistence(error.to_string()))?
            .flatten();
        let dependency = match missing_anchor {
            Some(dependency) => Some(dependency),
            None => self
                .source
                .custody_missing_parent(snapshot, floor)
                .map_err(|error| ImportError::Persistence(error.to_string()))?,
        };
        let Some((root, upper_slot)) = dependency else { return Ok(None) };
        // The native genesis origin has no produced signed original. Its immutable parent
        // commitment ends this historical chain; never ask peers to invent a genesis signature.
        if root == [0; 32] || self.origin_genesis == Some(root) || upper_slot <= floor {
            return Ok(None)
        }
        Ok(Some(HistoricalDependency {
            root,
            upper_slot,
            boundary: snapshot.finalized_checkpoint.root,
        }))
    }
    pub(super) fn retain_custody_context(&self, original: WireBlock) -> Result<(), ImportError> {
        let dependency = self
            .custody_dependency()?
            .ok_or(ImportError::GossipIgnore("finalized historical dependency unavailable"))?;
        if original.root() != dependency.root || original.slot() >= dependency.upper_slot {
            return Err(ImportError::GossipReject(
                "historical original does not match committed parent",
            ))
        }
        let signed =
            SignedBeaconBlock::from_ssz(original.ssz(), original.fork(), preset(&self.config))
                .map_err(|_| ImportError::Wire)?;
        let header = signed.header().map_err(|_| ImportError::Wire)?;
        let index = usize::try_from(header.proposer_index).map_err(|_| ImportError::Wire)?;
        let head = self.graph.snapshot()?.head.root;
        let key = match &self.graph {
            Graph::Classic { store, .. } => {
                let state = store
                    .read_block_state(head)?
                    .ok_or(ImportError::GossipIgnore("actual native head state unavailable"))?;
                super::custody_anchor::native_key(&state, index)?
            }
            Graph::Gloas { store, .. } => {
                let state = ReadGloasGossipView::read_block_state(store, head)?
                    .ok_or(ImportError::GossipIgnore("actual native head state unavailable"))?;
                state
                    .validators
                    .values
                    .get(index)
                    .map(|validator| validator.pubkey)
                    .ok_or(ImportError::Wire)?
            }
        };
        let version = self
            .config
            .fork_at_slot(original.slot())
            .map_err(|_| ImportError::Configuration)?
            .version;
        let signature: [u8; 96] = original
            .ssz()
            .get(4..100)
            .ok_or(ImportError::Wire)?
            .try_into()
            .map_err(|_| ImportError::Wire)?;
        verify_signature(
            &key,
            &compute_signing_root(
                original.root(),
                compute_domain([0; 4], version, self.config.genesis_validators_root),
            ),
            &signature,
        )
        .map_err(|_| ImportError::GossipReject("invalid historical proposer signature"))?;
        let receipt = HistoricalContextReceipt {
            original,
            key,
            boundary: dependency.boundary,
            digest: checked_config_digest(&self.config)?,
        };
        self.source
            .retain_historical_context(&receipt)
            .map_err(|error| ImportError::Persistence(error.to_string()))
    }
}
