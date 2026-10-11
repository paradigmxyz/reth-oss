//! Sealed Gloas checkpoint execution-base associations above strict fork-choice queries.
//!
//! A trusted complete checkpoint plus this exact adapter's VALID target acknowledgement can
//! authenticate the original bid's parent execution base. It never supplies the pending bid's
//! payload, FULL delivery, gossip execution status, an envelope, or a mutable core-store override.
//! Construction performs complete original SSZ/WS checks on the dedicated consensus owner.

use crate::{
    bootstrap::checked_config_digest,
    execution_bootstrap::{BootstrapProofError, ReconciledCheckpoint},
    import_coordinator::ImportAnchor,
    runtime_engine::RuntimeEngineAdapter,
    BootstrapError, CheckpointBlockFormat, SszCheckpointVerifier,
};
use alloy_rpc_types_engine::{ExecutionData, ForkchoiceState};
use reth_embedded_beacon_config::{ChainConfig, Preset as ConfigPreset};
use reth_embedded_beacon_execution_proxy::{ExecutionEngine, ForkchoiceReply, PayloadReply};
use reth_embedded_beacon_fork_choice::{
    compact::{CompactGloasStore, GloasStateRead, ReadGloasGossipView},
    gloas::GloasStore,
    ForkChoiceNode, PayloadStatus, StoreError,
};
use reth_embedded_beacon_primitives::{Checkpoint, Fork, Root};
use reth_embedded_beacon_state_transition::ssz::{
    BeaconBlock, BeaconState, Preset, SignedBeaconBlock,
};
use std::sync::Arc;
use thiserror::Error;

/// One immutable base receipt bound to the original trusted Gloas anchor and captured adapter.
/// The coordinator retains it privately; no public hash/status setter exists.
#[derive(Debug)]
pub(crate) struct ExecutionCheckpointBoundary {
    root: Root,
    slot: u64,
    parent_beacon_root: Root,
    post_state_root: Root,
    pending_payload_hash: Root,
    base_hash: Option<Root>,
    config_digest: Root,
    engine_binding: Arc<ChainConfig>,
}
impl ExecutionCheckpointBoundary {
    /// Reauthenticate original checkpoint bytes, then bind a genuine store's exact original data.
    /// Recovered later heads must use their own signed replay; this never rewinds the EL to an
    /// anchor.
    pub(crate) fn authenticate<E>(
        receipt: ReconciledCheckpoint,
        config: &ChainConfig,
        anchor: &ImportAnchor,
        now_unix_ms: u64,
        store: &impl ExecutionBoundaryView,
        engine: &RuntimeEngineAdapter<E>,
    ) -> Result<Self, ExecutionBoundaryError>
    where
        E: ExecutionEngine<
                Payload = ExecutionData,
                PayloadReply = PayloadReply,
                Forkchoice = ForkchoiceState,
                ForkchoiceReply = ForkchoiceReply,
            > + Clone,
        E::Error: std::error::Error + Send + Sync + 'static,
    {
        receipt.check_binding(config, anchor, engine)?;
        let config_digest = checked_config_digest(config)?;
        let verifier =
            SszCheckpointVerifier::new(config.clone(), anchor.decode_limits, anchor.block_format)?;
        let verified = if let Some(replay) = &anchor.replay {
            if !replay.proof.matches_inputs(
                &anchor.state_ssz,
                &replay.post_block_state_ssz,
                &anchor.block_ssz,
                anchor.block_format,
            ) {
                return Err(ExecutionBoundaryError::Binding)
            }
            verifier
                .verify_checkpoint_replay(
                    &anchor.state_ssz,
                    &replay.post_block_state_ssz,
                    &anchor.block_ssz,
                    anchor.verified.checkpoint(),
                    now_unix_ms,
                    anchor.max_age_epochs,
                    replay.proof.max_replay_slots(),
                )?
                .verified()
                .clone()
        } else {
            verifier.verify_checkpoint(
                &anchor.state_ssz,
                &anchor.block_ssz,
                anchor.verified.checkpoint(),
                now_unix_ms,
                anchor.max_age_epochs,
            )?
        };
        if verified.checkpoint() != receipt.checkpoint().checkpoint() ||
            verified.state_root() != receipt.checkpoint().state_root() ||
            verified.slot() != receipt.checkpoint().slot() ||
            verified.config_digest() != Some(config_digest) ||
            store.boundary_finalized() != verified.checkpoint()
        {
            return Err(ExecutionBoundaryError::Binding)
        }
        let fork = anchor
            .replay
            .as_ref()
            .map_or_else(
                || config.fork_at_slot(verified.slot()).map(|active| active.fork),
                |replay| Ok(replay.proof.post_block_fork()),
            )
            .map_err(|_| ExecutionBoundaryError::Binding)?;
        if fork != Fork::Gloas {
            return Err(ExecutionBoundaryError::HistoricalSchema)
        }
        let preset = match config.preset {
            ConfigPreset::Mainnet => Preset::Mainnet,
            ConfigPreset::Minimal => Preset::Minimal,
        };
        let original_state = anchor.replay.as_ref().map_or_else(
            || anchor.state_ssz.as_ref(),
            |replay| replay.post_block_state_ssz.as_ref(),
        );
        let BeaconState::Gloas(state, _) = BeaconState::from_ssz(original_state, fork, preset)
            .map_err(|error| ExecutionBoundaryError::Schema(error.to_string()))?
        else {
            return Err(ExecutionBoundaryError::HistoricalSchema)
        };
        let block = match anchor.block_format {
            CheckpointBlockFormat::Unsigned => {
                BeaconBlock::from_ssz(&anchor.block_ssz, fork, preset)
            }
            CheckpointBlockFormat::Signed => {
                SignedBeaconBlock::from_ssz(&anchor.block_ssz, fork, preset)
                    .map(|signed| signed.message())
            }
        }
        .map_err(|error| ExecutionBoundaryError::Schema(error.to_string()))?;
        let BeaconBlock::Gloas(block, _) = block else {
            return Err(ExecutionBoundaryError::HistoricalSchema)
        };
        let root = block
            .hash_tree_root(preset)
            .map_err(|error| ExecutionBoundaryError::Schema(error.to_string()))?;
        let state_root = state
            .hash_tree_root(preset)
            .map_err(|error| ExecutionBoundaryError::Schema(error.to_string()))?;
        let bid = &block.body.signed_execution_payload_bid.message;
        let base_hash = (bid.parent_block_hash != [0; 32]).then_some(bid.parent_block_hash);
        let retained_state = store.boundary_state(root)?.ok_or(StoreError::UnknownRoot)?;
        if root != verified.checkpoint().root ||
            block.state_root != state_root ||
            state.latest_execution_payload_bid != *bid ||
            state.latest_block_hash != bid.parent_block_hash ||
            state.slot != block.slot ||
            receipt.execution_hash() != base_hash ||
            store.boundary_block(root) != Some(&block) ||
            *retained_state != state ||
            store.boundary_genesis_time_ms() !=
                config
                    .genesis_time
                    .checked_mul(1000)
                    .ok_or(ExecutionBoundaryError::Binding)? ||
            store.boundary_slot_duration_ms() !=
                config
                    .seconds_per_slot
                    .checked_mul(1000)
                    .ok_or(ExecutionBoundaryError::Binding)? ||
            store.boundary_slots_per_epoch() != config.slots_per_epoch
        {
            return Err(ExecutionBoundaryError::Binding)
        }
        Ok(Self {
            root,
            slot: block.slot,
            parent_beacon_root: block.parent_root,
            post_state_root: state_root,
            pending_payload_hash: bid.block_hash,
            base_hash,
            config_digest,
            engine_binding: engine.bootstrap_binding(),
        })
    }

    /// Recheck the exact captured adapter's actual health and immutable identity before FCU.
    /// Original bytes were already concretely reauthenticated and matched private immutable maps.
    pub(crate) fn check_adapter<E>(
        &self,
        config: &ChainConfig,
        engine: &RuntimeEngineAdapter<E>,
    ) -> Result<(), ExecutionBoundaryError>
    where
        E: ExecutionEngine<
                Payload = ExecutionData,
                PayloadReply = PayloadReply,
                Forkchoice = ForkchoiceState,
                ForkchoiceReply = ForkchoiceReply,
            > + Clone,
        E::Error: std::error::Error + Send + Sync + 'static,
    {
        if engine.is_closed() {
            return Err(ExecutionBoundaryError::ReconciliationRequired)
        }
        if checked_config_digest(config)? != self.config_digest ||
            !Arc::ptr_eq(&self.engine_binding, &engine.bootstrap_binding())
        {
            return Err(ExecutionBoundaryError::Binding)
        }
        Ok(())
    }

    /// Resolve only actual retained EMPTY carry into this exact reconciled anchor base.
    /// FULL and PENDING preserve the strict core query unchanged. Every traversed original
    /// block/post-state and selected parent relation is checked with bounded decreasing slots.
    pub(crate) fn verified_execution_block_hash(
        &self,
        store: &impl ExecutionBoundaryView,
        current: ForkChoiceNode,
    ) -> Result<Option<Root>, ExecutionBoundaryError> {
        if current.payload_status != Some(PayloadStatus::Empty) {
            return store.boundary_verified(current).map_err(Into::into)
        }
        let parameters = store.boundary_parameters();
        // This bound comes only from the genuine store constructor, which checked its policy.
        let mut root = current.root;
        let first = store.boundary_block(root).ok_or(StoreError::UnknownRoot)?;
        let expected = first.body.signed_execution_payload_bid.message.parent_block_hash;
        for _ in 0..parameters.max_blocks {
            let block = store.boundary_block(root).ok_or(StoreError::UnknownRoot)?;
            let state = store.boundary_state(root)?.ok_or(StoreError::UnknownRoot)?;
            let bid = &block.body.signed_execution_payload_bid.message;
            if state.slot != block.slot ||
                state.latest_block_header.slot != block.slot ||
                state.latest_block_header.parent_root != block.parent_root ||
                state.latest_block_header.proposer_index != block.proposer_index ||
                state.latest_execution_payload_bid != *bid ||
                state.latest_block_hash != bid.parent_block_hash ||
                bid.parent_block_hash != expected
            {
                return Err(ExecutionBoundaryError::Binding)
            }
            if root == self.root {
                let mismatched_parent_beacon_root = block.parent_root != self.parent_beacon_root;
                if block.slot != self.slot ||
                    mismatched_parent_beacon_root ||
                    block.state_root != self.post_state_root ||
                    bid.block_hash != self.pending_payload_hash ||
                    expected != self.base_hash.unwrap_or([0; 32])
                {
                    return Err(ExecutionBoundaryError::Binding)
                }
                return Ok(self.base_hash)
            }
            let parent_node = store.boundary_node(block.parent_root)?;
            if parent_node.payload_status.is_none() {
                return store.boundary_verified(current).map_err(Into::into)
            }
            let parent = store.boundary_block(block.parent_root).ok_or(StoreError::UnknownRoot)?;
            if parent.slot >= block.slot {
                return Err(StoreError::InvalidBlockTime.into())
            }
            let parent_bid = &parent.body.signed_execution_payload_bid.message;
            if expected == parent_bid.block_hash {
                let hash = store.boundary_verified(ForkChoiceNode {
                    root: block.parent_root,
                    payload_status: Some(PayloadStatus::Full),
                })?;
                if hash != Some(expected) {
                    return Err(ExecutionBoundaryError::Binding)
                }
                return Ok(hash)
            }
            if expected != parent_bid.parent_block_hash {
                return Err(ExecutionBoundaryError::Binding)
            }
            root = block.parent_root;
        }
        Err(StoreError::ResourceLimit.into())
    }

    /// Finalized association remains the actual finalized bid/historical schema commitment.
    /// Require a locally verified EMPTY base before exposing it to a real Engine finality call.
    pub(crate) fn finalized_execution_block_hash(
        &self,
        store: &impl ExecutionBoundaryView,
    ) -> Result<Option<Root>, ExecutionBoundaryError> {
        let committed = store.boundary_finalized_hash()?;
        let mut node = store.boundary_node(store.boundary_finalized().root)?;
        if node.payload_status.is_some() {
            node.payload_status = Some(PayloadStatus::Empty)
        }
        let verified = self.verified_execution_block_hash(store, node)?;
        if committed != verified {
            return Err(ExecutionBoundaryError::Binding)
        }
        Ok(verified)
    }
}

/// A sealed base receipt cannot substitute for another adapter, graph or payload branch.
#[derive(Debug, Error)]
pub(crate) enum ExecutionBoundaryError {
    /// Exact original authenticated anchor commitments differ.
    #[error("checkpoint execution boundary binding mismatch")]
    Binding,
    /// Original earlier schemas require the genuine history/migration path.
    #[error("checkpoint execution boundary requires original Gloas schema")]
    HistoricalSchema,
    /// Actual shared proxy closed after reconciliation.
    #[error("checkpoint execution boundary requires fresh reconciliation")]
    ReconciliationRequired,
    /// Concrete complete checkpoint/WS authentication failed.
    #[error(transparent)]
    Bootstrap(#[from] BootstrapError),
    /// Exact sealed original-data/adapter receipt binding failed.
    #[error(transparent)]
    Proof(#[from] BootstrapProofError),
    /// Strict graph ancestry/envelope/resource errors preserve their original type.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// Original complete schema could not be decoded.
    #[error("checkpoint execution boundary schema failed: {0}")]
    Schema(String),
}

// Only these two actual privately authenticated graph owners can satisfy this node-local view.
// Owned cold reads preserve known missing/corrupt/budget errors instead of borrowing hot-only data.
mod sealed {
    pub(crate) trait Sealed {}
    impl Sealed for reth_embedded_beacon_fork_choice::gloas::GloasStore {}
    impl Sealed for reth_embedded_beacon_fork_choice::compact::CompactGloasStore {}
}

pub(crate) trait ExecutionBoundaryView: sealed::Sealed {
    fn boundary_state(&self, root: Root) -> Result<Option<GloasStateRead>, StoreError>;
    fn boundary_block(
        &self,
        root: Root,
    ) -> Option<&reth_embedded_beacon_state_transition::ssz::gloas::BeaconBlock>;
    fn boundary_parameters(&self) -> reth_embedded_beacon_fork_choice::StoreParameters;
    fn boundary_node(&self, root: Root) -> Result<ForkChoiceNode, StoreError>;
    fn boundary_finalized(&self) -> Checkpoint;
    fn boundary_verified(&self, node: ForkChoiceNode) -> Result<Option<Root>, StoreError>;
    fn boundary_finalized_hash(&self) -> Result<Option<Root>, StoreError>;
    fn boundary_genesis_time_ms(&self) -> u64;
    fn boundary_slot_duration_ms(&self) -> u64;
    fn boundary_slots_per_epoch(&self) -> u64;
}

macro_rules! impl_boundary_view {
    ($owner:ty) => {
        impl ExecutionBoundaryView for $owner {
            fn boundary_state(&self, root: Root) -> Result<Option<GloasStateRead>, StoreError> {
                Self::read_block_state(self, root)
            }
            fn boundary_block(
                &self,
                root: Root,
            ) -> Option<&reth_embedded_beacon_state_transition::ssz::gloas::BeaconBlock> {
                ReadGloasGossipView::block(self, root)
            }
            fn boundary_parameters(&self) -> reth_embedded_beacon_fork_choice::StoreParameters {
                ReadGloasGossipView::store_parameters(self)
            }
            fn boundary_node(&self, root: Root) -> Result<ForkChoiceNode, StoreError> {
                ReadGloasGossipView::node_for_root(self, root)
            }
            fn boundary_finalized(&self) -> Checkpoint {
                ReadGloasGossipView::finalized_checkpoint(self)
            }
            fn boundary_verified(&self, node: ForkChoiceNode) -> Result<Option<Root>, StoreError> {
                Self::verified_execution_block_hash(self, node)
            }
            fn boundary_finalized_hash(&self) -> Result<Option<Root>, StoreError> {
                Self::finalized_execution_block_hash(self)
            }
            fn boundary_genesis_time_ms(&self) -> u64 {
                ReadGloasGossipView::genesis_time_ms(self)
            }
            fn boundary_slot_duration_ms(&self) -> u64 {
                ReadGloasGossipView::slot_duration_ms(self)
            }
            fn boundary_slots_per_epoch(&self) -> u64 {
                ReadGloasGossipView::slots_per_epoch(self)
            }
        }
    };
}
impl_boundary_view!(GloasStore);
impl_boundary_view!(CompactGloasStore);

#[cfg(test)]
mod tests;

pub(crate) mod recovery;
pub(crate) mod roll_forward;
