//! Isolated real-signature fixture context over a private production graph.
//! Declared failed blocks, execution statuses and finality are bounded observations only.
//! Neither the inner Store nor authoritative shared allocations can escape this wrapper.

use super::{block_preset, Store};
use crate::{
    gossip_view::{self, GossipView},
    ExecutionStatus, ForkChoiceNode, StoreError, StoreParameters,
};
use reth_embedded_beacon_primitives::{Checkpoint, Root};
use reth_embedded_beacon_state_transition::{
    fork_processing::{self, TransitionParameters},
    ssz::{BeaconBlock, BeaconState, SignedBeaconBlock},
};
use std::collections::BTreeMap;

/// A sealed fixture view with strict signed setup imports and no runtime graph conversion.
#[derive(Clone)]
pub struct StrictGossipStore {
    inner: Store,
    failed_blocks: BTreeMap<Root, BeaconBlock>,
    payload_statuses: BTreeMap<Root, ExecutionStatus>,
    finalized: Option<Checkpoint>,
}

impl StrictGossipStore {
    /// Check the complete unchanged anchor and capture its exact configured preset and clock.
    pub fn get_store(
        state: BeaconState,
        block: BeaconBlock,
        parameters: StoreParameters,
    ) -> Result<Self, StoreError> {
        Ok(Self {
            inner: Store::get_store(state, block, parameters)?,
            failed_blocks: BTreeMap::new(),
            payload_statuses: BTreeMap::new(),
            finalized: None,
        })
    }
    /// Bind the exact checked fixture schedule before signed setup replay.
    pub fn set_transition_parameters(
        &mut self,
        parameters: TransitionParameters,
    ) -> Result<(), StoreError> {
        self.inner.set_transition_parameters(parameters)
    }
    /// Advance the ordinary production clock without granting authority to observations.
    pub fn on_tick(&mut self, time_ms: u64) -> Result<(), StoreError> {
        self.inner.on_tick(time_ms)
    }
    /// Replay real proposer and operation signatures, complete post-state roots and callbacks.
    /// Execution and availability fixture mocks must be separately reported by the adapter.
    pub async fn on_block_and_contents_with_engine<
        E: fork_processing::ExecutionEngine,
        D: crate::data_availability::DataAvailability,
    >(
        &mut self,
        block: &SignedBeaconBlock,
        engine: &E,
        availability: &D,
    ) -> Result<(), StoreError> {
        let mut candidate = self.clone();
        candidate.inner.on_block_and_contents_with_engine(block, engine, availability).await?;
        candidate.check_budget()?;
        *self = candidate;
        Ok(())
    }
    /// Observe a complete explicitly failed parent without admitting it to the retained graph.
    /// Head, post-state, execution and checkpoint queries keep their original private graph.
    pub fn fixture_observe_block_without_state(
        &mut self,
        block: BeaconBlock,
    ) -> Result<Root, StoreError> {
        if block_preset(&block) != self.inner.parameters.phase0.preset {
            return Err(StoreError::AnchorMismatch)
        }
        let root = block.hash_tree_root()?;
        if self.inner.block(root).is_some() {
            return Err(StoreError::AnchorMismatch)
        }
        if let Some(previous) = self.failed_blocks.get(&root) {
            return if previous == &block { Ok(root) } else { Err(StoreError::AnchorMismatch) }
        }
        let mut candidate = self.clone();
        candidate.failed_blocks.insert(root, block);
        candidate.check_budget()?;
        *self = candidate;
        Ok(root)
    }
    /// Observe declared finalized timing without modifying a retained state or graph checkpoint.
    /// Unknown checkpoint roots are permitted only as explicitly isolated fixture observations.
    pub fn fixture_set_finalized_checkpoint(
        &mut self,
        checkpoint: Checkpoint,
    ) -> Result<(), StoreError> {
        checkpoint.epoch.checked_mul(self.inner.slots_per_epoch()).ok_or(StoreError::Overflow)?;
        self.finalized = Some(checkpoint);
        Ok(())
    }
    /// Observe a fixture execution-status declaration at an already known complete block root.
    /// This status is not an Engine result and is never written into the production graph.
    pub fn fixture_set_payload_status(
        &mut self,
        root: Root,
        status: ExecutionStatus,
    ) -> Result<(), StoreError> {
        if self.block(root).is_none() {
            return Err(StoreError::UnknownRoot)
        }
        if !self.payload_statuses.contains_key(&root) &&
            self.payload_statuses.len() >= self.inner.parameters.max_blocks
        {
            return Err(StoreError::ResourceLimit)
        }
        self.payload_statuses.insert(root, status);
        Ok(())
    }
    /// Read the checked fixture event clock.
    pub const fn time_ms(&self) -> u64 {
        self.inner.time_ms()
    }
    fn check_budget(&self) -> Result<(), StoreError> {
        if self
            .inner
            .blocks
            .len()
            .checked_add(self.failed_blocks.len())
            .ok_or(StoreError::Overflow)? >
            self.inner.parameters.max_blocks
        {
            return Err(StoreError::ResourceLimit)
        }
        let mut bytes = self.inner.retained_schema_bytes()?;
        for block in self.failed_blocks.values() {
            bytes = bytes
                .checked_add(block.heap_bytes()?)
                .and_then(|value| value.checked_add(std::mem::size_of::<BeaconBlock>()))
                .ok_or(StoreError::Overflow)?;
        }
        if bytes > self.inner.parameters.max_retained_schema_bytes {
            return Err(StoreError::ResourceLimit)
        }
        Ok(())
    }
}

impl std::fmt::Debug for StrictGossipStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StrictGossipStore")
            .field("retained_blocks", &self.inner.blocks.len())
            .field("observed_failed_blocks", &self.failed_blocks.len())
            .field("declared_payload_statuses", &self.payload_statuses.len())
            .field("declared_finalized", &self.finalized)
            .field("time_ms", &self.inner.time_ms())
            .finish()
    }
}

impl gossip_view::sealed::Sealed for StrictGossipStore {}
impl GossipView for StrictGossipStore {
    const VERIFY_SIGNATURES: bool = true;
    fn block(&self, root: Root) -> Option<&BeaconBlock> {
        self.inner.block(root).or_else(|| self.failed_blocks.get(&root))
    }
    fn block_state(&self, root: Root) -> Option<&BeaconState> {
        self.inner.block_state(root)
    }
    fn get_head(&self) -> Result<ForkChoiceNode, StoreError> {
        self.inner.get_head()
    }
    fn checkpoints(&self) -> (Checkpoint, Checkpoint) {
        let (justified, finalized) = self.inner.checkpoints();
        (justified, self.finalized.unwrap_or(finalized))
    }
    fn genesis_time_ms(&self) -> u64 {
        self.inner.genesis_time_ms()
    }
    fn slot_duration_ms(&self) -> u64 {
        self.inner.slot_duration_ms()
    }
    fn slots_per_epoch(&self) -> u64 {
        self.inner.slots_per_epoch()
    }
    fn transition_parameters(&self) -> Option<&TransitionParameters> {
        self.inner.transition_parameters()
    }
    fn store_parameters(&self) -> StoreParameters {
        self.inner.parameters
    }
    fn execution_status(&self, root: Root) -> ExecutionStatus {
        self.payload_statuses.get(&root).copied().unwrap_or(ExecutionStatus::Optimistic)
    }
}
