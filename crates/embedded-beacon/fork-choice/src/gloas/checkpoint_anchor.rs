//! Explicit trusted Gloas checkpoint construction retaining the actual original beacon block.

use super::*;
use reth_embedded_beacon_state_transition::gloas_processing::replay_slots;

impl GloasStore {
    /// Construct from an authenticated Gloas checkpoint and independently verified empty replay.
    ///
    /// The caller establishes checkpoint trust. Exact original block/post-state commitments
    /// remain unchanged; the independently replayed boundary has its own checkpoint entry.
    /// Earlier-schema origins must use the classic constructor and actual signed migration.
    pub fn get_store_at_checkpoint(
        anchor_state: BeaconState,
        anchor_block: BeaconBlock,
        checkpoint: Checkpoint,
        boundary_state_ssz: &[u8],
        parameters: StoreParameters,
        processing: GloasParameters,
    ) -> Result<Self, StoreError> {
        if boundary_state_ssz.len() > parameters.max_retained_schema_bytes {
            return Err(StoreError::ResourceLimit)
        }
        let preset = processing.preset();
        let mut latest = anchor_state.latest_block_header;
        if latest.state_root == [0; 32] {
            latest.state_root = anchor_block.state_root;
        }
        if latest != anchor_block.header(preset)? ||
            anchor_block.hash_tree_root(preset)? != checkpoint.root
        {
            return Err(StoreError::AnchorMismatch)
        }
        let target = checkpoint
            .epoch
            .checked_mul(processing.slots_per_epoch())
            .ok_or(StoreError::Overflow)?;
        if target < anchor_state.slot {
            return Err(StoreError::AnchorMismatch)
        }
        if target - anchor_state.slot > parameters.max_tick_slots {
            return Err(StoreError::ResourceLimit)
        }
        let mut store = Self::get_store(anchor_state, anchor_block, parameters, processing)?;
        let original = store.read_block_state(checkpoint.root)?.ok_or(StoreError::UnknownRoot)?;
        let boundary = if target == original.slot {
            original.hot_arc().ok_or(crate::compact::StateSourceError::Unavailable)?
        } else {
            Arc::new(replay_slots(&original, target, &store.processing, parameters.max_tick_slots)?)
        };
        let mut boundary_header = boundary.latest_block_header;
        if boundary_header.state_root == [0; 32] {
            boundary_header.state_root = store.block(checkpoint.root)?.state_root;
        }
        if boundary.slot != target ||
            boundary.genesis_time != original.genesis_time ||
            boundary.genesis_validators_root != original.genesis_validators_root ||
            boundary_header.hash_tree_root() != checkpoint.root ||
            boundary.to_ssz(preset)?.as_slice() != boundary_state_ssz
        {
            return Err(StoreError::AnchorMismatch)
        }
        store.time_ms = target
            .checked_mul(store.slot_duration_ms())
            .and_then(|time| store.genesis_time_ms.checked_add(time))
            .ok_or(StoreError::Overflow)?;
        store.justified = checkpoint;
        store.finalized = checkpoint;
        store.unrealized_justified = checkpoint;
        store.unrealized_finalized = checkpoint;
        store.unrealized_justifications.insert(checkpoint.root, checkpoint);
        store.checkpoint_states.insert((checkpoint.epoch, checkpoint.root), boundary.into());
        store.check_budget()?;
        Ok(store)
    }
}
