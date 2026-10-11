//! Private runner-only choice between strict and explicitly artificial published-fixture graphs.

use reth_embedded_beacon_fork_choice::{
    ForkChoiceNode, SpecStore, Store, StoreError, StoreParameters,
};
use reth_embedded_beacon_primitives::{Checkpoint, Fork, Root};
use reth_embedded_beacon_state_transition::{
    fork_processing::{self, TransitionParameters},
    ssz::{BeaconBlock, BeaconState, SignedBeaconBlock},
};

/// No graph or validation result from this runner-only enum can enter runtime code.
pub(super) enum FixtureStore {
    Strict(Store),
    Artificial(SpecStore),
}

impl FixtureStore {
    pub(super) fn get_store(
        state: BeaconState,
        block: BeaconBlock,
        parameters: StoreParameters,
        artificial: bool,
    ) -> Result<Self, StoreError> {
        if artificial {
            Ok(Self::Artificial(SpecStore::get_store(state, block, parameters)?))
        } else {
            Ok(Self::Strict(Store::get_store(state, block, parameters)?))
        }
    }
    pub(super) fn set_transition_parameters(
        &mut self,
        parameters: TransitionParameters,
    ) -> Result<(), StoreError> {
        match self {
            Self::Strict(store) => store.set_transition_parameters(parameters),
            Self::Artificial(store) => store.set_transition_parameters(parameters),
        }
    }
    pub(super) fn on_tick(&mut self, time: u64) -> Result<(), StoreError> {
        match self {
            Self::Strict(store) => store.on_tick(time),
            Self::Artificial(store) => store.on_tick(time),
        }
    }
    pub(super) async fn on_block_and_contents_with_engine<
        E: fork_processing::ExecutionEngine,
        D: reth_embedded_beacon_fork_choice::data_availability::DataAvailability,
    >(
        &mut self,
        block: &SignedBeaconBlock,
        engine: &E,
        availability: &D,
    ) -> Result<(), StoreError> {
        match self {
            Self::Strict(store) => {
                store.on_block_and_contents_with_engine(block, engine, availability).await
            }
            Self::Artificial(store) => {
                store.on_block_and_contents_with_engine(block, engine, availability).await
            }
        }
    }
    pub(super) async fn on_compliance_block_and_contents<
        E: fork_processing::ExecutionEngine,
        D: reth_embedded_beacon_fork_choice::data_availability::DataAvailability,
    >(
        &mut self,
        block: &SignedBeaconBlock,
        engine: &E,
        availability: &D,
    ) -> Result<(), StoreError> {
        match self {
            Self::Artificial(store) => {
                store.on_compliance_block_and_contents(block, engine, availability).await
            }
            Self::Strict(_) => Err(StoreError::InvalidParameters),
        }
    }
    pub(super) fn on_attestation_wire(
        &mut self,
        bytes: &[u8],
        fork: Fork,
    ) -> Result<(), StoreError> {
        match self {
            Self::Strict(store) => store.on_attestation_wire(bytes, fork),
            Self::Artificial(store) => store.on_attestation_wire(bytes, fork),
        }
    }
    pub(super) fn on_attester_slashing_wire(
        &mut self,
        bytes: &[u8],
        fork: Fork,
    ) -> Result<(), StoreError> {
        match self {
            Self::Strict(store) => store.on_attester_slashing_wire(bytes, fork),
            Self::Artificial(store) => store.on_attester_slashing_wire(bytes, fork),
        }
    }
    pub(super) fn get_head(&self) -> Result<ForkChoiceNode, StoreError> {
        match self {
            Self::Strict(store) => store.get_head(),
            Self::Artificial(store) => store.get_head(),
        }
    }
    pub(super) fn get_proposer_head(&self) -> Result<ForkChoiceNode, StoreError> {
        match self {
            Self::Strict(store) => store.get_proposer_head(),
            Self::Artificial(store) => store.get_proposer_head(),
        }
    }
    pub(super) fn viable_leaf_weights(
        &self,
    ) -> Result<std::collections::BTreeMap<Root, u128>, StoreError> {
        match self {
            Self::Strict(store) => store.viable_leaf_weights(),
            Self::Artificial(store) => store.viable_leaf_weights(),
        }
    }
    pub(super) fn block_state(&self, root: Root) -> Option<&BeaconState> {
        match self {
            Self::Strict(store) => store.block_state(root),
            Self::Artificial(store) => store.block_state(root),
        }
    }
    pub(super) const fn checkpoints(&self) -> (Checkpoint, Checkpoint) {
        match self {
            Self::Strict(store) => store.checkpoints(),
            Self::Artificial(store) => store.checkpoints(),
        }
    }
    pub(super) const fn time_ms(&self) -> u64 {
        match self {
            Self::Strict(store) => store.time_ms(),
            Self::Artificial(store) => store.time_ms(),
        }
    }
    pub(super) const fn genesis_time_ms(&self) -> u64 {
        match self {
            Self::Strict(store) => store.genesis_time_ms(),
            Self::Artificial(store) => store.genesis_time_ms(),
        }
    }
    pub(super) fn proposer_boost_root(&self) -> Root {
        match self {
            Self::Strict(store) => store.proposer_boost_root(),
            Self::Artificial(store) => store.proposer_boost_root(),
        }
    }
}
