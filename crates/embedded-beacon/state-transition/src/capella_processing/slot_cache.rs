//! Incremental Capella root-ring updates for empty-slot replay.

use super::{position, BeaconState, Constants, ProcessingError};
use crate::{phase0_processing::slot_cache::RootTree, ssz::Ssz};
use reth_embedded_beacon_primitives::{merkleize, uint64_root, Root};

pub(super) struct SlotCache {
    fields: [Root; 28],
    block_roots: RootTree,
    state_roots: RootTree,
    randao_mixes: RootTree,
    constants: Constants,
}

impl SlotCache {
    pub(super) fn new(state: &BeaconState, constants: &Constants) -> Result<Self, ProcessingError> {
        let preset = constants.preset();
        let block_roots = RootTree::new(&state.block_roots.values)?;
        let state_roots = RootTree::new(&state.state_roots.values)?;
        let randao_mixes = RootTree::new(&state.randao_mixes.values)?;
        // Exact complete Capella field order; all other fields are rebuilt after every epoch.
        let fields = [
            uint64_root(state.genesis_time),
            state.genesis_validators_root,
            uint64_root(state.slot),
            state.fork.hash_tree_root(preset)?,
            state.latest_block_header.hash_tree_root(),
            block_roots.root(),
            state_roots.root(),
            state.historical_roots.hash_tree_root(preset)?,
            state.eth1_data.hash_tree_root(preset)?,
            state.eth1_data_votes.hash_tree_root(preset)?,
            uint64_root(state.eth1_deposit_index),
            state.validators.hash_tree_root(preset)?,
            state.balances.hash_tree_root(preset)?,
            randao_mixes.root(),
            state.slashings.hash_tree_root(preset)?,
            state.previous_epoch_participation.hash_tree_root(preset)?,
            state.current_epoch_participation.hash_tree_root(preset)?,
            state.justification_bits.hash_tree_root(preset)?,
            Ssz::hash_tree_root(&state.previous_justified_checkpoint, preset)?,
            Ssz::hash_tree_root(&state.current_justified_checkpoint, preset)?,
            Ssz::hash_tree_root(&state.finalized_checkpoint, preset)?,
            state.inactivity_scores.hash_tree_root(preset)?,
            state.current_sync_committee.hash_tree_root(preset)?,
            state.next_sync_committee.hash_tree_root(preset)?,
            state.latest_execution_payload_header.hash_tree_root(preset)?,
            uint64_root(state.next_withdrawal_index),
            uint64_root(state.next_withdrawal_validator_index),
            state.historical_summaries.hash_tree_root(preset)?,
        ];
        Ok(Self { fields, block_roots, state_roots, randao_mixes, constants: *constants })
    }

    pub(super) fn root(&self) -> Result<Root, ProcessingError> {
        Ok(merkleize(&self.fields, self.fields.len())?)
    }

    pub(super) fn process_slot(&mut self, state: &mut BeaconState) -> Result<(), ProcessingError> {
        let index = position(state.slot, self.constants.historical_slots)?;
        let previous_state_root = self.root()?;
        *state
            .state_roots
            .values
            .get_mut(index)
            .ok_or(ProcessingError::InvalidState("state-root ring is truncated"))? =
            previous_state_root;
        self.state_roots.update(index, previous_state_root)?;
        if state.latest_block_header.state_root == [0; 32] {
            state.latest_block_header.state_root = previous_state_root;
            self.fields[4] = state.latest_block_header.hash_tree_root();
        }
        let block_root = state.latest_block_header.hash_tree_root();
        *state
            .block_roots
            .values
            .get_mut(index)
            .ok_or(ProcessingError::InvalidState("block-root ring is truncated"))? = block_root;
        self.block_roots.update(index, block_root)?;
        self.fields[5] = self.block_roots.root();
        self.fields[6] = self.state_roots.root();
        Ok(())
    }

    pub(super) fn refresh_epoch(&mut self, state: &BeaconState) -> Result<(), ProcessingError> {
        let preset = self.constants.preset();
        // Capella epochs only overwrite the next RANDAO entry; unchanged roots remain shared.
        let next = super::add(state.slot / self.constants.slots_per_epoch, 1, "cached next epoch")?;
        let index = position(next, self.constants.historical_epochs)?;
        let mix = *state
            .randao_mixes
            .values
            .get(index)
            .ok_or(ProcessingError::InvalidState("RANDAO ring is truncated"))?;
        self.randao_mixes.update(index, mix)?;
        self.fields[27] = state.historical_summaries.hash_tree_root(preset)?;
        self.fields[9] = state.eth1_data_votes.hash_tree_root(preset)?;
        self.fields[11] = state.validators.hash_tree_root(preset)?;
        self.fields[12] = state.balances.hash_tree_root(preset)?;
        self.fields[13] = self.randao_mixes.root();
        self.fields[14] = state.slashings.hash_tree_root(preset)?;
        self.fields[15] = state.previous_epoch_participation.hash_tree_root(preset)?;
        self.fields[16] = state.current_epoch_participation.hash_tree_root(preset)?;
        self.fields[17] = state.justification_bits.hash_tree_root(preset)?;
        self.fields[18] = Ssz::hash_tree_root(&state.previous_justified_checkpoint, preset)?;
        self.fields[19] = Ssz::hash_tree_root(&state.current_justified_checkpoint, preset)?;
        self.fields[20] = Ssz::hash_tree_root(&state.finalized_checkpoint, preset)?;
        self.fields[21] = state.inactivity_scores.hash_tree_root(preset)?;
        self.fields[22] = state.current_sync_committee.hash_tree_root(preset)?;
        self.fields[23] = state.next_sync_committee.hash_tree_root(preset)?;
        Ok(())
    }

    pub(super) fn advance_slot(&mut self, slot: u64) {
        self.fields[2] = uint64_root(slot);
    }
}
