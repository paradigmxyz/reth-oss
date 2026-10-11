//! Private pre-cut preparation and post-FCU activation of genuine execution-base boundaries.
//!
//! Preparation checks actual finalized ancestry and original typed commitments while all parent
//! data remain retained. A consensus-only graph cut may then remove those parents without
//! changing strict core execution queries. The candidate base carries existing authenticated
//! authority only; activation additionally requires the actual same-adapter canonical Engine
//! acknowledgement and a matching concrete durable canonical snapshot.

use super::*;
use crate::{
    compact_persistence::{AdmittedStateRepository, PersistenceError},
    runtime_engine::forkchoice_receipt::VerifiedForkchoice,
    runtime_parameters::RuntimeParameters,
};
use alloy_primitives::B256;
use reth_embedded_beacon_fork_choice::{compact::FinalizedRetentionPolicy, StoreSnapshot};
use reth_embedded_beacon_state_transition::fork_processing::replay_slots;

#[derive(Debug)]
pub(crate) struct PreparedExecutionBoundary {
    candidate: ExecutionCheckpointBoundary,
    cut: Checkpoint,
    actual: StoreSnapshot,
    expected_execution: Option<ForkchoiceState>,
    boundary_state_root: Root,
    boundary_slot: u64,
}
impl PreparedExecutionBoundary {
    /// Exact canonical horizon selected from the unchanged actual finalized graph.
    #[cfg(test)]
    pub(crate) const fn checkpoint(&self) -> Checkpoint {
        self.cut
    }
    /// Use this already proven existing base only for candidate pre-publication queries.
    /// Reading it cannot activate a canonical boundary or mint an Engine receipt.
    pub(crate) const fn candidate(&self) -> &ExecutionCheckpointBoundary {
        &self.candidate
    }
    /// Bind the actual native finalized fallback after its optional confirmation anchor retires.
    /// Head/finalized authority and the captured consensus snapshot must remain exact; only
    /// the safe hash may become the independently authenticated finalized execution hash.
    pub(crate) fn rebind_finalized_confirmation<E>(
        &mut self,
        config: &ChainConfig,
        store: &CompactGloasStore,
        engine: &RuntimeEngineAdapter<E>,
        expected: Option<ForkchoiceState>,
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
        self.candidate.check_adapter(config, engine)?;
        if store.snapshot()? != self.actual {
            return Err(ExecutionBoundaryError::Binding)
        }
        let head = self.candidate.verified_execution_block_hash(store, self.actual.head)?;
        let finalized = self.candidate.finalized_execution_block_hash(store)?;
        match (self.expected_execution, expected, head) {
            (Some(previous), Some(actual), Some(head))
                if previous.head_block_hash == actual.head_block_hash &&
                    previous.finalized_block_hash == actual.finalized_block_hash &&
                    actual.head_block_hash == B256::from(head) &&
                    actual.finalized_block_hash == B256::from(finalized.unwrap_or([0; 32])) &&
                    actual.safe_block_hash == actual.finalized_block_hash => {}
            (None, None, None) if finalized.is_none() => {}
            _ => return Err(ExecutionBoundaryError::Binding),
        }
        self.expected_execution = expected;
        Ok(())
    }
    /// Consume preparation only after exact Engine acknowledgement and durable publication.
    /// A saved pointer alone never creates this proof; signed replay and preparation came first.
    pub(crate) fn commit<E>(
        self,
        config: &ChainConfig,
        engine: &RuntimeEngineAdapter<E>,
        receipt: Option<&VerifiedForkchoice>,
        repository: &AdmittedStateRepository,
    ) -> Result<ExecutionCheckpointBoundary, BoundaryCommitError>
    where
        E: ExecutionEngine<
                Payload = ExecutionData,
                PayloadReply = PayloadReply,
                Forkchoice = ForkchoiceState,
                ForkchoiceReply = ForkchoiceReply,
            > + Clone,
        E::Error: std::error::Error + Send + Sync + 'static,
    {
        self.candidate.check_adapter(config, engine)?;
        match (self.expected_execution, receipt) {
            (Some(expected), Some(actual)) if actual.matches(engine, expected) => {}
            (None, None) => {}
            _ => return Err(BoundaryCommitError::Binding),
        }
        if repository.consensus_config_digest() != self.candidate.config_digest {
            return Err(BoundaryCommitError::Binding)
        }
        if !repository.retained_execution_boundary(
            self.cut,
            self.candidate.post_state_root,
            self.candidate.slot,
            self.boundary_state_root,
            self.boundary_slot,
        )? {
            return Err(BoundaryCommitError::Binding)
        }
        let persisted = repository.current_snapshot()?.ok_or(BoundaryCommitError::Binding)?;
        if persisted.head() != self.actual.head ||
            persisted.checkpoints() !=
                (self.actual.justified_checkpoint, self.actual.finalized_checkpoint)
        {
            return Err(BoundaryCommitError::Binding)
        }
        let (format, original) =
            repository.original_block(self.cut.root)?.ok_or(BoundaryCommitError::Binding)?;
        let preset = match config.preset {
            ConfigPreset::Minimal => Preset::Minimal,
            ConfigPreset::Mainnet => Preset::Mainnet,
        };
        let block = match format {
            CheckpointBlockFormat::Unsigned => {
                BeaconBlock::from_ssz(&original, Fork::Gloas, preset)
            }
            CheckpointBlockFormat::Signed => {
                SignedBeaconBlock::from_ssz(&original, Fork::Gloas, preset)
                    .map(|signed| signed.message())
            }
        }
        .map_err(|_| BoundaryCommitError::Binding)?;
        let BeaconBlock::Gloas(block, _) = block else { return Err(BoundaryCommitError::Binding) };
        if block.hash_tree_root(preset).map_err(|_| BoundaryCommitError::Binding)? != self.cut.root ||
            block.state_root != self.candidate.post_state_root ||
            block.slot != self.candidate.slot ||
            block.parent_root != self.candidate.parent_beacon_root ||
            block.body.signed_execution_payload_bid.message.block_hash !=
                self.candidate.pending_payload_hash ||
            block.body.signed_execution_payload_bid.message.parent_block_hash !=
                self.candidate.base_hash.unwrap_or([0; 32])
        {
            return Err(BoundaryCommitError::Binding)
        }
        Ok(self.candidate)
    }
}

impl ExecutionCheckpointBoundary {
    /// Prepare the wrapper's actual checked canonical horizon before any parent deletion.
    /// The existing receipt or actual retained verified envelope proves the EMPTY base.
    pub(crate) fn prepare_finalized_roll_forward<E>(
        &self,
        config: &ChainConfig,
        store: &CompactGloasStore,
        policy: FinalizedRetentionPolicy,
        expected_execution: Option<ForkchoiceState>,
        engine: &RuntimeEngineAdapter<E>,
    ) -> Result<Option<PreparedExecutionBoundary>, ExecutionBoundaryError>
    where
        E: ExecutionEngine<
                Payload = ExecutionData,
                PayloadReply = PayloadReply,
                Forkchoice = ForkchoiceState,
                ForkchoiceReply = ForkchoiceReply,
            > + Clone,
        E::Error: std::error::Error + Send + Sync + 'static,
    {
        self.check_adapter(config, engine)?;
        let Some(cut) = store.finalized_retention_boundary(policy)? else { return Ok(None) };
        if store.boundary_block(cut.root).is_none() &&
            ReadGloasGossipView::historical_block(store, cut.root).is_some()
        {
            // Historical cuts retain their original native schema. The strict core keeps
            // their execution receipts; no Gloas wrapper may relabel them as bids.
            return Ok(None)
        }
        let actual = store.snapshot()?;
        let target = cut.epoch.checked_mul(config.slots_per_epoch).ok_or(StoreError::Overflow)?;
        let finalized = store.boundary_node(actual.finalized_checkpoint.root)?;
        if cut.epoch > actual.finalized_checkpoint.epoch ||
            ReadGloasGossipView::get_ancestor(store, finalized, target)?.root != cut.root
        {
            return Err(ExecutionBoundaryError::Binding)
        }
        let block = store.boundary_block(cut.root).ok_or(StoreError::UnknownRoot)?;
        let state = store.boundary_state(cut.root)?.ok_or(StoreError::UnknownRoot)?;
        let bid = &block.body.signed_execution_payload_bid.message;
        let base_hash = self.verified_execution_block_hash(
            store,
            ForkChoiceNode { root: cut.root, payload_status: Some(PayloadStatus::Empty) },
        )?;
        let state_root = state
            .hash_tree_root()
            .map_err(|error| ExecutionBoundaryError::Schema(error.to_string()))?;
        if block.slot < self.slot ||
            block.slot > target ||
            state.slot != block.slot ||
            state.latest_block_header.slot != block.slot ||
            state.latest_block_header.parent_root != block.parent_root ||
            state.latest_block_header.proposer_index != block.proposer_index ||
            state.latest_execution_payload_bid != *bid ||
            state.latest_block_hash != bid.parent_block_hash ||
            block.state_root != state_root ||
            block
                .hash_tree_root(state.preset())
                .map_err(|error| ExecutionBoundaryError::Schema(error.to_string()))? !=
                cut.root ||
            base_hash.unwrap_or([0; 32]) != bid.parent_block_hash
        {
            return Err(ExecutionBoundaryError::Binding)
        }
        // The epoch boundary is distinct from the original post-block commitment. Replay
        // only the actual scheduled gap under the already checked finite clock budget.
        let boundary_state_root = if target == block.slot {
            state_root
        } else {
            let parameters = RuntimeParameters::from_config(config)
                .map_err(|_| ExecutionBoundaryError::Binding)?;
            let original = BeaconState::Gloas((*state).clone(), state.preset());
            replay_slots(
                &original,
                target,
                &parameters.transitions,
                store.boundary_parameters().max_tick_slots,
            )
            .map_err(StoreError::Processing)?
            .hash_tree_root()
            .map_err(|error| ExecutionBoundaryError::Schema(error.to_string()))?
        };
        let head = self.verified_execution_block_hash(store, actual.head)?;
        let finalized = self.finalized_execution_block_hash(store)?;
        match (head, expected_execution) {
            (Some(head), Some(expected))
                if expected.head_block_hash == B256::from(head) &&
                    expected.finalized_block_hash ==
                        B256::from(finalized.unwrap_or([0; 32])) => {}
            (None, None) if finalized.is_none() && base_hash.is_none() => {}
            _ => return Err(ExecutionBoundaryError::Binding),
        }
        Ok(Some(PreparedExecutionBoundary {
            cut,
            actual,
            expected_execution,
            boundary_state_root,
            boundary_slot: target,
            candidate: Self {
                root: cut.root,
                slot: block.slot,
                parent_beacon_root: block.parent_root,
                post_state_root: state_root,
                pending_payload_hash: bid.block_hash,
                base_hash,
                config_digest: self.config_digest,
                engine_binding: self.engine_binding.clone(),
            },
        }))
    }
}

#[derive(Debug, Error)]
pub(crate) enum BoundaryCommitError {
    #[error("finalized execution boundary publication does not match prepared evidence")]
    Binding,
    #[error(transparent)]
    Boundary(#[from] ExecutionBoundaryError),
    #[error(transparent)]
    Persistence(#[from] PersistenceError),
}

#[cfg(test)]
mod tests;
