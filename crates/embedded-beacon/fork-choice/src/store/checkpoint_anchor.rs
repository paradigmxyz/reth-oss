//! Explicit trusted checkpoint construction across real skipped slots and scheduled upgrades.

use super::*;

impl Store {
    /// Start at an explicitly trusted epoch boundary backed by an actual earlier beacon block.
    ///
    /// Bootstrap must authenticate the selected checkpoint before calling this constructor.
    /// This method independently replays the exact checked schedule and verifies the complete
    /// boundary SSZ. The original block and its committed post-state retain their original
    /// schema, root and slot; the replay result has a separate checkpoint-state entry.
    pub fn get_store_at_checkpoint(
        anchor_state: BeaconState,
        anchor_block: BeaconBlock,
        checkpoint: Checkpoint,
        boundary_state_ssz: &[u8],
        parameters: StoreParameters,
        transitions: TransitionParameters,
    ) -> Result<Self, StoreError> {
        if anchor_block.schema_fork() == Fork::Gloas {
            return Err(StoreError::ValidationUnavailable(Fork::Gloas))
        }
        if boundary_state_ssz.len() > parameters.max_retained_schema_bytes {
            return Err(StoreError::ResourceLimit)
        }
        let header = anchor_block.header()?;
        let mut latest = anchor_state.latest_block_header();
        if latest.state_root == [0; 32] {
            latest.state_root = header.state_root;
        }
        if latest != header || anchor_block.hash_tree_root()? != checkpoint.root {
            return Err(StoreError::AnchorMismatch)
        }
        let target = checkpoint
            .epoch
            .checked_mul(parameters.phase0.slots_per_epoch())
            .ok_or(StoreError::Overflow)?;
        if target < anchor_state.slot() {
            return Err(StoreError::AnchorMismatch)
        }
        if target - anchor_state.slot() > parameters.max_tick_slots {
            return Err(StoreError::ResourceLimit)
        }
        let mut store = Self::get_store(anchor_state, anchor_block, parameters)?;
        store.set_transition_parameters(transitions)?;
        let original = store.read_block_state(checkpoint.root)?.ok_or(StoreError::UnknownRoot)?;
        let boundary = if target == original.slot() {
            original.hot_arc().ok_or(crate::compact::StateSourceError::Unavailable)?
        } else {
            Arc::new(fork_processing::replay_slots(
                &original,
                target,
                store.transition_parameters.as_ref().ok_or(StoreError::InvalidParameters)?,
                parameters.max_tick_slots,
            )?)
        };
        let mut boundary_header = boundary.latest_block_header();
        if boundary_header.state_root == [0; 32] {
            boundary_header.state_root = header.state_root;
        }
        if boundary.slot() != target ||
            boundary.preset() != original.preset() ||
            boundary.genesis_time() != original.genesis_time() ||
            boundary.genesis_validators_root() != original.genesis_validators_root() ||
            boundary_header.hash_tree_root() != checkpoint.root ||
            boundary.to_ssz()?.as_slice() != boundary_state_ssz
        {
            return Err(StoreError::AnchorMismatch)
        }
        let record = BlockRecord {
            root: checkpoint.root,
            parent_root: header.parent_root,
            slot: header.slot,
            justified: checkpoint,
            unrealized_justified: checkpoint,
            execution_status: ExecutionStatus::Valid,
        };
        store.kernel = ForkChoice::new(
            record,
            checkpoint,
            state_weights(&boundary, checkpoint.epoch)?,
            store.slots_per_epoch(),
        )?;
        store.time_ms = target
            .checked_mul(store.slot_duration_ms())
            .and_then(|time| store.genesis_time_ms.checked_add(time))
            .ok_or(StoreError::Overflow)?;
        store.unrealized_justified = checkpoint;
        store.unrealized_finalized = checkpoint;
        store.checkpoint_states.insert((checkpoint.epoch, checkpoint.root), boundary.into());
        store.check_retention_budget()?;
        Ok(store)
    }
}

impl Store {
    /// Follow the actual admitted ancestry for optional shuffling-dependency evidence.
    /// A trusted constructor or authenticated finalized cut can retain a frontier newer than
    /// the requested slot. That explicit absence grants no proposer boost; unknown input roots
    /// and inconsistent parent ordering remain errors. Ordinary retained ancestry uses the same
    /// exact parent walk as the mandatory kernel ancestor query, without a whole-graph scan.
    pub(super) fn retained_proposer_dependency(
        &self,
        mut root: Root,
        slot: u64,
    ) -> Result<Option<Root>, StoreError> {
        loop {
            let block = self.kernel.blocks.get(&root).ok_or(ForkChoiceError::UnknownRoot)?;
            if block.slot <= slot {
                return Ok(Some(root))
            }
            let Some(parent) = self.kernel.blocks.get(&block.parent_root) else { return Ok(None) };
            if parent.slot >= block.slot {
                return Err(ForkChoiceError::InvalidBlockSlot.into())
            }
            root = parent.root;
        }
    }
}
