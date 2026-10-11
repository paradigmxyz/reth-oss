//! Incremental Phase0 root-ring updates for empty-slot replay.

use super::{position, BeaconState, Constants, ProcessingError};
use crate::ssz::Ssz;
use reth_embedded_beacon_primitives::{hash_pair, merkleize, uint64_root, Root};

pub(super) struct SlotCache {
    fields: [Root; 21],
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
        // Exact complete Phase0 field order; all other fields are rebuilt after every epoch.
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
            state.previous_epoch_attestations.hash_tree_root(preset)?,
            state.current_epoch_attestations.hash_tree_root(preset)?,
            state.justification_bits.hash_tree_root(preset)?,
            Ssz::hash_tree_root(&state.previous_justified_checkpoint, preset)?,
            Ssz::hash_tree_root(&state.current_justified_checkpoint, preset)?,
            Ssz::hash_tree_root(&state.finalized_checkpoint, preset)?,
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
        // Phase0 epochs only overwrite the next RANDAO entry; unchanged roots remain shared.
        let next = super::add(state.slot / self.constants.slots_per_epoch, 1, "cached next epoch")?;
        let index = position(next, self.constants.historical_epochs)?;
        let mix = *state
            .randao_mixes
            .values
            .get(index)
            .ok_or(ProcessingError::InvalidState("RANDAO ring is truncated"))?;
        self.randao_mixes.update(index, mix)?;
        self.fields[7] = state.historical_roots.hash_tree_root(preset)?;
        self.fields[9] = state.eth1_data_votes.hash_tree_root(preset)?;
        self.fields[11] = state.validators.hash_tree_root(preset)?;
        self.fields[12] = state.balances.hash_tree_root(preset)?;
        self.fields[13] = self.randao_mixes.root();
        self.fields[14] = state.slashings.hash_tree_root(preset)?;
        self.fields[15] = state.previous_epoch_attestations.hash_tree_root(preset)?;
        self.fields[16] = state.current_epoch_attestations.hash_tree_root(preset)?;
        self.fields[17] = state.justification_bits.hash_tree_root(preset)?;
        self.fields[18] = Ssz::hash_tree_root(&state.previous_justified_checkpoint, preset)?;
        self.fields[19] = Ssz::hash_tree_root(&state.current_justified_checkpoint, preset)?;
        self.fields[20] = Ssz::hash_tree_root(&state.finalized_checkpoint, preset)?;
        Ok(())
    }

    pub(super) fn advance_slot(&mut self, slot: u64) {
        self.fields[2] = uint64_root(slot);
    }
}

pub(crate) struct RootTree {
    nodes: Vec<Root>,
    count: usize,
}

impl RootTree {
    pub(crate) fn new(values: &[Root]) -> Result<Self, ProcessingError> {
        if values.is_empty() || !values.len().is_power_of_two() {
            return Err(ProcessingError::InvalidState(
                "root ring must have nonzero power-of-two length",
            ))
        }
        let count = values.len();
        let length =
            count.checked_mul(2).ok_or(ProcessingError::Arithmetic("root tree capacity"))?;
        let mut nodes = vec![[0; 32]; length];
        nodes[count..].copy_from_slice(values);
        for index in (1..count).rev() {
            nodes[index] = hash_pair(nodes[index * 2], nodes[index * 2 + 1]);
        }
        Ok(Self { nodes, count })
    }

    pub(crate) fn root(&self) -> Root {
        self.nodes[1]
    }

    pub(crate) fn update(&mut self, index: usize, root: Root) -> Result<(), ProcessingError> {
        if index >= self.count {
            return Err(ProcessingError::InvalidState("cached root-ring index is out of range"))
        }
        let mut index =
            self.count.checked_add(index).ok_or(ProcessingError::Arithmetic("root tree index"))?;
        self.nodes[index] = root;
        while index > 1 {
            index /= 2;
            self.nodes[index] = hash_pair(self.nodes[index * 2], self.nodes[index * 2 + 1]);
        }
        Ok(())
    }
}
