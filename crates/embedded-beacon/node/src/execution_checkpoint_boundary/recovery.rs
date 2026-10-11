//! Restart preparation cannot activate a saved execution association or rewind execution.
//! Original native cut commitments are used only to reconstruct the signed later graph. The
//! exact actual later-head VALID reply and canonical journal publication activate its base.

use super::*;
use crate::{
    compact_persistence::AdmittedStateRepository,
    runtime_engine::forkchoice_receipt::VerifiedForkchoice,
};
use alloy_primitives::B256;
use reth_embedded_beacon_fork_choice::StoreSnapshot;

#[derive(Debug)]
pub(crate) struct PreparedRecoveryExecutionBoundary {
    candidate: ExecutionCheckpointBoundary,
    checkpoint: Checkpoint,
}
impl PreparedRecoveryExecutionBoundary {
    pub(crate) const fn candidate(&self) -> &ExecutionCheckpointBoundary {
        &self.candidate
    }

    pub(crate) fn prepare<E>(
        config: &ChainConfig,
        store: &CompactGloasStore,
        repository: &AdmittedStateRepository,
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
        if engine.is_closed() ||
            checked_config_digest(config)? != repository.consensus_config_digest() ||
            checked_config_digest(engine.config())? != repository.consensus_config_digest()
        {
            return Err(ExecutionBoundaryError::Binding)
        }
        let checkpoint = store.boundary_finalized();
        let block = store.boundary_block(checkpoint.root).ok_or(StoreError::UnknownRoot)?;
        let state = store.boundary_state(checkpoint.root)?.ok_or(StoreError::UnknownRoot)?;
        let preset = state.preset();
        let bid = &block.body.signed_execution_payload_bid.message;
        let state_root = state
            .hash_tree_root()
            .map_err(|error| ExecutionBoundaryError::Schema(error.to_string()))?;
        if block
            .hash_tree_root(preset)
            .map_err(|error| ExecutionBoundaryError::Schema(error.to_string()))? !=
            checkpoint.root ||
            block.state_root != state_root ||
            state.slot != block.slot ||
            state.latest_block_header.slot != block.slot ||
            state.latest_block_header.proposer_index != block.proposer_index ||
            state.latest_block_header.parent_root != block.parent_root ||
            state.latest_execution_payload_bid != *bid ||
            state.latest_block_hash != bid.parent_block_hash ||
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
        repository
            .checked_recovery_seed(checkpoint, state_root, block.slot)
            .map_err(|error| ExecutionBoundaryError::Schema(error.to_string()))?;
        Ok(Self {
            checkpoint,
            candidate: ExecutionCheckpointBoundary {
                root: checkpoint.root,
                slot: block.slot,
                parent_beacon_root: block.parent_root,
                post_state_root: state_root,
                pending_payload_hash: bid.block_hash,
                base_hash: (bid.parent_block_hash != [0; 32]).then_some(bid.parent_block_hash),
                config_digest: repository.consensus_config_digest(),
                engine_binding: engine.bootstrap_binding(),
            },
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn commit<E>(
        self,
        config: &ChainConfig,
        store: &CompactGloasStore,
        actual: StoreSnapshot,
        expected: Option<ForkchoiceState>,
        receipt: Option<&VerifiedForkchoice>,
        repository: &AdmittedStateRepository,
        engine: &RuntimeEngineAdapter<E>,
    ) -> Result<ExecutionCheckpointBoundary, ExecutionBoundaryError>
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
        repository
            .checked_recovery_seed(
                self.checkpoint,
                self.candidate.post_state_root,
                self.candidate.slot,
            )
            .map_err(|error| ExecutionBoundaryError::Schema(error.to_string()))?;
        if store.snapshot()? != actual {
            return Err(ExecutionBoundaryError::Binding)
        }
        let head = self.candidate.verified_execution_block_hash(store, actual.head)?;
        let finalized = self.candidate.finalized_execution_block_hash(store)?;
        match (expected, receipt, head) {
            (Some(expected), Some(receipt), Some(head))
                if expected.head_block_hash == B256::from(head) &&
                    expected.finalized_block_hash ==
                        B256::from(finalized.unwrap_or([0; 32])) &&
                    receipt.matches(engine, expected) => {}
            (None, None, None) if finalized.is_none() && self.candidate.base_hash.is_none() => {}
            _ => return Err(ExecutionBoundaryError::Binding),
        }
        let persisted = repository
            .current_snapshot()
            .map_err(|error| ExecutionBoundaryError::Schema(error.to_string()))?
            .ok_or(ExecutionBoundaryError::Binding)?;
        if persisted.head() != actual.head ||
            persisted.checkpoints() != (actual.justified_checkpoint, actual.finalized_checkpoint)
        {
            return Err(ExecutionBoundaryError::Binding)
        }
        Ok(self.candidate)
    }
}
