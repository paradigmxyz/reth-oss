//! Read-only actual unsigned bootstrap context, without synthesized signatures.

use super::*;

/// Original unsigned bytes bound to an actual retained native anchor by its sole owner.
/// This context carries neither an execution receipt nor a sidecar proof.
#[derive(Clone, Debug)]
pub struct UnsignedCustodyAnchor {
    original: Arc<[u8]>,
    root: Root,
    slot: u64,
    fork: Fork,
    preset: Preset,
    proposer_key: Option<[u8; 48]>,
    config_digest: Root,
    genuine_genesis: bool,
}
impl UnsignedCustodyAnchor {
    /// Only the dedicated genesis owner calls this with freshly reauthenticated originals.
    /// The seal retains serving bytes independently of later graph selection or pruning.
    pub(super) fn from_genuine_genesis(
        genesis: &crate::VerifiedGenesis,
        config: &ChainConfig,
    ) -> Result<Self, ImportError> {
        let config_digest =
            checked_config_digest(config).map_err(|_| ImportError::Configuration)?;
        if genesis.config_digest() != config_digest ||
            config.fork_at_slot(0).map_err(|_| ImportError::Configuration)?.fork !=
                genesis.fork()
        {
            return Err(ImportError::Configuration)
        }
        let context = Self {
            original: genesis.block_ssz(),
            root: genesis.block_root(),
            slot: 0,
            fork: genesis.fork(),
            preset: preset(config),
            proposer_key: None,
            config_digest,
            genuine_genesis: true,
        };
        let header = context.block()?.header().map_err(|_| ImportError::Wire)?;
        if header.slot != 0 ||
            header.parent_root != [0; 32] ||
            header.proposer_index != 0 ||
            header.state_root != genesis.state_root()
        {
            return Err(ImportError::Wire)
        }
        Ok(context)
    }
    pub(crate) fn original(&self) -> &[u8] {
        &self.original
    }
    pub(crate) fn original_len(&self) -> usize {
        self.original.len()
    }
    pub(crate) const fn root(&self) -> Root {
        self.root
    }
    pub(crate) const fn slot(&self) -> u64 {
        self.slot
    }
    pub(crate) const fn proposer_key(&self) -> Option<[u8; 48]> {
        self.proposer_key
    }
    pub(crate) const fn config_digest(&self) -> Root {
        self.config_digest
    }
    pub(crate) const fn is_genuine_genesis(&self) -> bool {
        self.genuine_genesis
    }

    pub(crate) fn block(&self) -> Result<BeaconBlock, ImportError> {
        let block = BeaconBlock::from_ssz(&self.original, self.fork, self.preset)
            .map_err(|_| ImportError::Wire)?;
        if block.hash_tree_root().map_err(|_| ImportError::Wire)? != self.root ||
            block.header().map_err(|_| ImportError::Wire)?.slot != self.slot
        {
            return Err(ImportError::Wire)
        }
        Ok(block)
    }
}

impl ImportCoordinator {
    /// Read only original unsigned data from actual retained graph membership.
    pub fn unsigned_custody_anchor(
        &self,
        root: Root,
    ) -> impl Future<Output = Result<Option<UnsignedCustodyAnchor>, ImportError>> + Send + 'static
    {
        let admitted = self.try_custody_anchor(Some(root));
        async move { admitted?.await.map_err(|_| ImportError::Closed)? }
    }
    pub(crate) fn retained_unsigned_custody_anchor(
        &self,
    ) -> impl Future<Output = Result<Option<UnsignedCustodyAnchor>, ImportError>> + Send + 'static
    {
        let admitted = self.try_custody_anchor(None);
        async move { admitted?.await.map_err(|_| ImportError::Closed)? }
    }
    fn try_custody_anchor(
        &self,
        root: Option<Root>,
    ) -> Result<oneshot::Receiver<Result<Option<UnsignedCustodyAnchor>, ImportError>>, ImportError>
    {
        if self.is_halted() {
            return Err(ImportError::Halted)
        }
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
        queue.send(Command::CustodyAnchor { root, reply, _slot: slot });
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
    pub(super) fn custody_anchor(
        &self,
        root: Root,
    ) -> Result<Option<UnsignedCustodyAnchor>, ImportError> {
        if let Some(context) = &self.unsigned_custody_context &&
            root == context.root()
        {
            self.source
                .ensure_unsigned_context(context)
                .map_err(|error| ImportError::Persistence(error.to_string()))?;
            return Ok(Some(context.clone()))
        }
        let Some(header) = self.graph.serving_header(root)? else { return Ok(None) };
        let Some((format, original)) = self
            .repository
            .original_block(root)
            .map_err(|error| ImportError::Persistence(error.to_string()))?
        else {
            return Ok(None)
        };
        if format != CheckpointBlockFormat::Unsigned {
            return Ok(None)
        }
        let fork = self.config.fork_at_slot(header.slot).map_err(|_| ImportError::Wire)?.fork;
        let anchor = UnsignedCustodyAnchor {
            original: Arc::from(original.as_ref()),
            root,
            slot: header.slot,
            fork,
            preset: preset(&self.config),
            proposer_key: if fork == Fork::Fulu {
                self.anchor_proposer_key(root, header.proposer_index)?
            } else {
                None
            },
            config_digest: checked_config_digest(&self.config)
                .map_err(|_| ImportError::Configuration)?,
            // Only the dedicated owner reauthenticates complete VerifiedGenesis inputs.
            // Slot zero or an unsigned checkpoint alone cannot mint this provenance.
            genuine_genesis: self.origin_genesis == Some(root) &&
                header.slot == 0 &&
                header.parent_root == [0; 32],
        };
        if anchor.block()?.header().map_err(|_| ImportError::Wire)? != header {
            return Err(ImportError::Wire)
        }
        Ok(Some(anchor))
    }
    pub(super) fn retained_custody_anchor(
        &self,
    ) -> Result<Option<UnsignedCustodyAnchor>, ImportError> {
        let Some(context) = &self.unsigned_custody_context else { return Ok(None) };
        self.source
            .ensure_unsigned_context(context)
            .map_err(|error| ImportError::Persistence(error.to_string()))?;
        Ok(Some(context.clone()))
    }
    fn anchor_proposer_key(
        &self,
        root: Root,
        proposer: u64,
    ) -> Result<Option<[u8; 48]>, ImportError> {
        let index = usize::try_from(proposer).map_err(|_| ImportError::Wire)?;
        match &self.graph {
            Graph::Classic { store, .. } => {
                let Some(state) = store.read_block_state(root)? else { return Ok(None) };
                native_key(&state, index).map(Some)
            }
            Graph::Gloas { store, .. } => {
                if let Some(state) = ReadGloasGossipView::read_block_state(store, root)? {
                    return state
                        .validators
                        .values
                        .get(index)
                        .map(|validator| Some(validator.pubkey))
                        .ok_or(ImportError::Wire)
                }
                let Some(state) = ReadGloasGossipView::read_historical_block_state(store, root)?
                else {
                    return Ok(None)
                };
                native_key(&state, index).map(Some)
            }
        }
    }
}

pub(super) fn native_key(state: &BeaconState, index: usize) -> Result<[u8; 48], ImportError> {
    let validators = match state {
        BeaconState::Phase0(state, _) => &state.validators.values,
        BeaconState::Altair(state, _) => &state.validators.values,
        BeaconState::Bellatrix(state, _) => &state.validators.values,
        BeaconState::Capella(state, _) => &state.validators.values,
        BeaconState::Deneb(state, _) => &state.validators.values,
        BeaconState::Electra(state, _) => &state.validators.values,
        BeaconState::Fulu(state, _) => &state.validators.values,
        BeaconState::Gloas(state, _) => &state.validators.values,
    };
    validators.get(index).map(|validator| validator.pubkey).ok_or(ImportError::Wire)
}
