//! Strict graph bootstrap retaining the genuine origin post-state and separately replayed boundary.

use super::{Graph, ImportError};
use crate::runtime_parameters::RuntimeParameters;
use reth_embedded_beacon_fork_choice::{compact::CompactStateSource, gloas::GloasStore, Store};
use reth_embedded_beacon_primitives::{Checkpoint, Fork};
use reth_embedded_beacon_state_transition::ssz::{BeaconBlock, BeaconState};
use std::sync::Arc;

impl Graph {
    pub(super) fn new_at_checkpoint(
        state: BeaconState,
        block: BeaconBlock,
        checkpoint: Checkpoint,
        boundary_state_ssz: &[u8],
        parameters: &RuntimeParameters,
        source: Arc<dyn CompactStateSource>,
        fast_confirmation: bool,
    ) -> Result<Self, ImportError> {
        match (state, block) {
            (BeaconState::Gloas(state, _), BeaconBlock::Gloas(block, _)) => {
                let store = GloasStore::get_store_at_checkpoint(
                    state,
                    block,
                    checkpoint,
                    boundary_state_ssz,
                    parameters.fork_choice,
                    parameters.transitions.gloas.clone(),
                )?;
                Self::from_gloas(store, source, fast_confirmation)
            }
            (state, block)
                if state.schema_fork() < Fork::Gloas && block.schema_fork() < Fork::Gloas =>
            {
                let store = Store::get_store_at_checkpoint(
                    state,
                    block,
                    checkpoint,
                    boundary_state_ssz,
                    parameters.fork_choice,
                    parameters.transitions.clone(),
                )?;
                Self::from_classic(store, source, fast_confirmation)
            }
            _ => Err(ImportError::Configuration),
        }
    }
}
