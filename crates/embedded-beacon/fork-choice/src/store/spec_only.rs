//! Isolated published-fixture graphs with a fixed artificial signature policy.
//!
//! This type cannot become a production Store or expose authoritative shared allocations.
//! Canonical schemas, committee membership, timing, post-state roots, execution callbacks and
//! availability proofs remain checked. Only BLS verification follows the artificial fixture mode.

use super::{
    BeaconBlock, BeaconState, ForkChoiceNode, Root, SignedBeaconBlock, Store, StoreError,
    StoreParameters, TransitionParameters,
};
use crate::gossip_view::{self, GossipView};
use reth_embedded_beacon_primitives::{Checkpoint, Fork};
use reth_embedded_beacon_state_transition::fork_processing;
use std::sync::Arc;

/// A sealed test graph that has no conversion or dereference into a production Store.
#[derive(Clone, Debug)]
pub struct SpecStore {
    inner: Store,
}

impl SpecStore {
    /// Check a complete fixture anchor and bind its clock/preset without claiming trust externally.
    pub fn get_store(
        anchor_state: BeaconState,
        anchor_block: BeaconBlock,
        parameters: StoreParameters,
    ) -> Result<Self, StoreError> {
        Ok(Self { inner: Store::get_store(anchor_state, anchor_block, parameters)? })
    }

    /// Bind the exact fixture fork schedule before any artificial block replay.
    pub fn set_transition_parameters(
        &mut self,
        parameters: TransitionParameters,
    ) -> Result<(), StoreError> {
        self.inner.set_transition_parameters(parameters)
    }

    /// Replay the monotonic fixture clock with the same fixed artificial signature policy.
    pub fn on_tick(&mut self, time_ms: u64) -> Result<(), StoreError> {
        self.inner.on_tick_inner::<false>(time_ms)
    }

    /// Replay actual signed schemas using fixture-only BLS rules and mandatory execution/DA checks.
    pub async fn on_block_and_contents_with_engine<
        E: fork_processing::ExecutionEngine,
        D: crate::data_availability::DataAvailability,
    >(
        &mut self,
        block: &SignedBeaconBlock,
        engine: &E,
        availability: &D,
    ) -> Result<(), StoreError> {
        self.inner
            .on_block_and_contents_inner::<false, false, E, D>(block, engine, availability)
            .await
    }

    /// Replay the pinned compliance block event, ignoring each assertion-invalid contained vote.
    /// The block transition, post-state root, execution callbacks and availability remain
    /// mandatory. Infrastructure failures are fatal and this fixed artificial context cannot
    /// become a Store.
    #[cfg(feature = "spec-tests")]
    pub async fn on_compliance_block_and_contents<
        E: fork_processing::ExecutionEngine,
        D: crate::data_availability::DataAvailability,
    >(
        &mut self,
        block: &SignedBeaconBlock,
        engine: &E,
        availability: &D,
    ) -> Result<(), StoreError> {
        self.inner
            .on_block_and_contents_inner::<false, true, E, D>(block, engine, availability)
            .await
    }

    /// Admit fixture attestations only after real ancestry, membership and timing checks.
    pub fn on_attestation_wire(&mut self, bytes: &[u8], fork: Fork) -> Result<(), StoreError> {
        self.inner.on_attestation_wire_inner::<false>(bytes, fork, false)
    }

    /// Admit fixture slashings only after both actual sorted bounded index lists are validated.
    pub fn on_attester_slashing_wire(
        &mut self,
        bytes: &[u8],
        fork: Fork,
    ) -> Result<(), StoreError> {
        self.inner.on_attester_slashing_wire_inner::<false>(bytes, fork)
    }

    /// Compute the fixture graph's ordinary head without exporting graph authority.
    pub fn get_head(&self) -> Result<ForkChoiceNode, StoreError> {
        self.inner.get_head()
    }
    /// Observe the complete fixture leaf set and exact weights without exporting graph authority.
    pub fn viable_leaf_weights(
        &self,
    ) -> Result<std::collections::BTreeMap<Root, u128>, StoreError> {
        self.inner.viable_leaf_weights()
    }
    /// Compute the fixture graph's optional proposer head.
    pub fn get_proposer_head(&self) -> Result<ForkChoiceNode, StoreError> {
        self.inner.get_proposer_head()
    }
    /// Borrow a fixture block; its presence alone never authenticates its state.
    pub fn block(&self, root: Root) -> Option<&BeaconBlock> {
        self.inner.block(root)
    }
    /// Borrow a state computed by the isolated fixture transition.
    pub fn block_state(&self, root: Root) -> Option<&BeaconState> {
        self.inner.block_state(root)
    }
    /// Fixture graph checkpoints, including explicitly declared artificial context overrides.
    pub const fn checkpoints(&self) -> (Checkpoint, Checkpoint) {
        self.inner.checkpoints()
    }
    /// Exact fixture clock in milliseconds.
    pub const fn time_ms(&self) -> u64 {
        self.inner.time_ms()
    }
    /// Exact fixture genesis clock in milliseconds.
    pub const fn genesis_time_ms(&self) -> u64 {
        self.inner.genesis_time_ms()
    }
    /// Exact fixture slot duration in milliseconds.
    pub const fn slot_duration_ms(&self) -> u64 {
        self.inner.slot_duration_ms()
    }
    /// Exact fixture epoch length.
    pub const fn slots_per_epoch(&self) -> u64 {
        self.inner.slots_per_epoch()
    }
    /// Current fixture slot.
    pub const fn current_slot(&self) -> u64 {
        self.inner.current_slot()
    }
    /// Current fixture proposer boost identity.
    pub fn proposer_boost_root(&self) -> Root {
        self.inner.proposer_boost_root()
    }
    /// Borrow the checked schedule privately bound to this fixture graph.
    pub const fn transition_parameters(&self) -> Option<&TransitionParameters> {
        self.inner.transition_parameters()
    }

    /// Record a declared checkpoint observation in this isolated graph only.
    /// This does not establish finality or authenticate any checkpoint state.
    pub fn fixture_set_finalized_checkpoint(
        &mut self,
        checkpoint: Checkpoint,
    ) -> Result<(), StoreError> {
        checkpoint.epoch.checked_mul(self.slots_per_epoch()).ok_or(StoreError::Overflow)?;
        self.inner.kernel.finalized = checkpoint;
        Ok(())
    }

    /// Retain a complete declared block observation without a post-state or graph admission.
    /// Gossip fixtures explicitly exercise pending and failed state-lookup paths this way.
    pub fn fixture_observe_block_without_state(
        &mut self,
        block: BeaconBlock,
    ) -> Result<(), StoreError> {
        if super::block_preset(&block) != self.inner.parameters.phase0.preset {
            return Err(StoreError::AnchorMismatch)
        }
        let root = block.hash_tree_root()?;
        if self.inner.blocks.contains_key(&root) {
            return Ok(())
        }
        if self.inner.blocks.len() >= self.inner.parameters.max_blocks {
            return Err(StoreError::ResourceLimit)
        }
        let mut candidate = self.inner.clone();
        candidate.blocks.insert(root, Arc::new(block));
        candidate.check_retention_budget()?;
        self.inner = candidate;
        Ok(())
    }
}

impl gossip_view::sealed::Sealed for SpecStore {}
impl GossipView for SpecStore {
    const VERIFY_SIGNATURES: bool = false;
    fn block(&self, root: Root) -> Option<&BeaconBlock> {
        Self::block(self, root)
    }
    fn block_state(&self, root: Root) -> Option<&BeaconState> {
        Self::block_state(self, root)
    }
    fn get_head(&self) -> Result<ForkChoiceNode, StoreError> {
        Self::get_head(self)
    }
    fn checkpoints(&self) -> (Checkpoint, Checkpoint) {
        Self::checkpoints(self)
    }
    fn genesis_time_ms(&self) -> u64 {
        Self::genesis_time_ms(self)
    }
    fn slot_duration_ms(&self) -> u64 {
        Self::slot_duration_ms(self)
    }
    fn slots_per_epoch(&self) -> u64 {
        Self::slots_per_epoch(self)
    }
    fn transition_parameters(&self) -> Option<&TransitionParameters> {
        Self::transition_parameters(self)
    }
    fn store_parameters(&self) -> StoreParameters {
        crate::fast_confirmation::FastConfirmationView::parameters(self)
    }
}

impl crate::fast_confirmation::sealed::Sealed for SpecStore {}
impl crate::fast_confirmation::FastConfirmationView for SpecStore {
    const VERIFY_SIGNATURES: bool = false;
    fn parameters(&self) -> crate::StoreParameters {
        crate::fast_confirmation::FastConfirmationView::parameters(&self.inner)
    }
    fn processing(&self) -> crate::fast_confirmation::ProcessingRef<'_> {
        crate::fast_confirmation::FastConfirmationView::processing(&self.inner)
    }
    fn current_slot(&self) -> u64 {
        crate::fast_confirmation::FastConfirmationView::current_slot(&self.inner)
    }
    fn time_ms(&self) -> u64 {
        crate::fast_confirmation::FastConfirmationView::time_ms(&self.inner)
    }
    fn genesis_time_ms(&self) -> u64 {
        crate::fast_confirmation::FastConfirmationView::genesis_time_ms(&self.inner)
    }
    fn checkpoints(
        &self,
    ) -> (reth_embedded_beacon_primitives::Checkpoint, reth_embedded_beacon_primitives::Checkpoint)
    {
        crate::fast_confirmation::FastConfirmationView::checkpoints(&self.inner)
    }
    fn unrealized_justified(&self) -> reth_embedded_beacon_primitives::Checkpoint {
        crate::fast_confirmation::FastConfirmationView::unrealized_justified(&self.inner)
    }
    fn head(&self) -> Result<crate::ForkChoiceNode, crate::StoreError> {
        crate::fast_confirmation::FastConfirmationView::head(&self.inner)
    }
    fn block_info(
        &self,
        root: reth_embedded_beacon_primitives::Root,
    ) -> Result<crate::fast_confirmation::BlockInfo, crate::StoreError> {
        crate::fast_confirmation::FastConfirmationView::block_info(&self.inner, root)
    }
    fn state(
        &self,
        root: reth_embedded_beacon_primitives::Root,
    ) -> Result<crate::fast_confirmation::StateRef<'_>, crate::StoreError> {
        crate::fast_confirmation::FastConfirmationView::state(&self.inner, root)
    }
    fn checkpoint_state(
        &self,
        checkpoint: reth_embedded_beacon_primitives::Checkpoint,
    ) -> Result<crate::fast_confirmation::StateRef<'_>, crate::StoreError> {
        crate::fast_confirmation::FastConfirmationView::checkpoint_state(&self.inner, checkpoint)
    }
    fn latest(
        &self,
        index: u64,
    ) -> Result<Option<crate::fast_confirmation::Message>, crate::StoreError> {
        crate::fast_confirmation::FastConfirmationView::latest(&self.inner, index)
    }
    fn equivocating(&self, index: u64) -> bool {
        crate::fast_confirmation::FastConfirmationView::equivocating(&self.inner, index)
    }
    fn is_ancestor(
        &self,
        node: crate::ForkChoiceNode,
        ancestor: crate::ForkChoiceNode,
    ) -> Result<bool, crate::StoreError> {
        crate::fast_confirmation::FastConfirmationView::is_ancestor(&self.inner, node, ancestor)
    }
    fn ancestor(
        &self,
        node: crate::ForkChoiceNode,
        slot: u64,
    ) -> Result<crate::ForkChoiceNode, crate::StoreError> {
        crate::fast_confirmation::FastConfirmationView::ancestor(&self.inner, node, slot)
    }
    fn voting_source(
        &self,
        root: reth_embedded_beacon_primitives::Root,
    ) -> Result<reth_embedded_beacon_primitives::Checkpoint, crate::StoreError> {
        crate::fast_confirmation::FastConfirmationView::voting_source(&self.inner, root)
    }
    fn unrealized_justification(
        &self,
        root: reth_embedded_beacon_primitives::Root,
    ) -> Result<reth_embedded_beacon_primitives::Checkpoint, crate::StoreError> {
        crate::fast_confirmation::FastConfirmationView::unrealized_justification(&self.inner, root)
    }
    fn is_gloas(&self) -> bool {
        crate::fast_confirmation::FastConfirmationView::is_gloas(&self.inner)
    }
}
