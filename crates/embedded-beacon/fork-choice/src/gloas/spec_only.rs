//! Isolated Gloas gossip fixture context with mandatory real signatures.
//! Declared checkpoint observations cannot escape into a production graph or shared allocation.
//! This is distinct from artificial BLS-disabled compliance processing.

use super::{
    envelope_gossip::{self, GossipEnvelopeError},
    gossip_view::{self, GloasGossipView},
    DataAvailability, EnvelopeError, ExecutionEngine, GloasStore,
};
use crate::{ExecutionStatus, ForkChoiceNode, StoreError, StoreParameters};
use reth_embedded_beacon_primitives::{Checkpoint, Root};
use reth_embedded_beacon_state_transition::{
    gloas_processing::GloasParameters,
    ssz::gloas::{
        BeaconBlock, BeaconState, ExecutionPayloadEnvelope, SignedBeaconBlock,
        SignedExecutionPayloadEnvelope,
    },
};
use std::{collections::BTreeMap, sync::Arc};

mod block_observations;

/// An isolated strict-signature fixture graph with no inner Store/Arc extraction or conversion.
#[derive(Clone)]
pub struct SpecStore {
    inner: GloasStore,
    gossip_processing: GloasParameters,
    replay_checkpoints: BTreeMap<Root, Checkpoint>,
    gossip_finalized: Option<Checkpoint>,
    observed_failed_blocks: BTreeMap<Root, BeaconBlock>,
    asserted_payload_observations: BTreeMap<Root, ExecutionPayloadEnvelope>,
    observed_payload_statuses: BTreeMap<Root, ExecutionStatus>,
}

impl SpecStore {
    /// Bind the exact complete fixture anchor, preset, fork and clock with normal checks.
    pub fn get_store(
        state: BeaconState,
        block: BeaconBlock,
        parameters: StoreParameters,
        processing: GloasParameters,
    ) -> Result<Self, StoreError> {
        processing.validate()?;
        let gossip_processing = processing.clone();
        // Published gossip cases may activate a typed state at genesis while declaring a future
        // gossip activation epoch. Bind internal transition checks to the actual stored fork;
        // keep that declared schedule separately in this fixture-only view.
        let mut anchor_processing = processing;
        anchor_processing.fork_epoch = state.fork.epoch;
        Ok(Self {
            inner: GloasStore::get_store(state, block, parameters, anchor_processing)?,
            gossip_processing,
            replay_checkpoints: BTreeMap::new(),
            gossip_finalized: None,
            observed_failed_blocks: BTreeMap::new(),
            asserted_payload_observations: BTreeMap::new(),
            observed_payload_statuses: BTreeMap::new(),
        })
    }
    /// Replay the ordinary monotonic event clock.
    pub fn on_tick(&mut self, time_ms: u64) -> Result<(), StoreError> {
        self.inner.on_tick(time_ms)
    }
    /// Require every ordinary signed transition, post-state root and included vote check.
    pub fn on_block_and_contents(&mut self, block: &SignedBeaconBlock) -> Result<(), StoreError> {
        self.inner.on_block_and_contents(block)
    }
    /// Require the unchanged signature, linkage, execution and availability callbacks.
    pub async fn on_execution_payload_envelope<E: ExecutionEngine, D: DataAvailability>(
        &mut self,
        envelope: &SignedExecutionPayloadEnvelope,
        engine: &E,
        availability: &D,
    ) -> Result<(), EnvelopeError> {
        self.inner.on_execution_payload_envelope(envelope, engine, availability).await
    }
    /// Declare an explicitly artificial finalized-checkpoint context only inside this wrapper.
    /// The block must be retained; no state-finality proof is asserted by this operation.
    pub fn fixture_set_finalized_checkpoint(
        &mut self,
        checkpoint: Checkpoint,
    ) -> Result<(), StoreError> {
        let head = self.inner.get_head()?.root;
        self.fixture_set_finalized_checkpoint_for_states(checkpoint, &[head])
    }
    /// Declare the published checkpoint context for selected temporary gossip replay states.
    /// Only scalar observations are retained; complete state Arcs and their roots stay unchanged.
    pub fn fixture_set_finalized_checkpoint_for_states(
        &mut self,
        checkpoint: Checkpoint,
        roots: &[Root],
    ) -> Result<(), StoreError> {
        let finalized_slot = checkpoint
            .epoch
            .checked_mul(self.inner.slots_per_epoch())
            .ok_or(StoreError::Overflow)?;
        self.inner.block(checkpoint.root)?;
        if roots.len() > self.inner.parameters.max_blocks {
            return Err(StoreError::ResourceLimit)
        }
        let mut candidate = self.clone();
        for &root in roots {
            if let Some(state) = candidate.inner.block_states.get(&root) &&
                state.slot(candidate.inner.processing.preset()) >= finalized_slot
            {
                candidate.replay_checkpoints.insert(root, checkpoint);
            }
        }
        if candidate.replay_checkpoints.len() > candidate.inner.parameters.max_blocks {
            return Err(StoreError::ResourceLimit)
        }
        candidate.inner.finalized = checkpoint;
        *self = candidate;
        Ok(())
    }
    /// Publish an explicitly asserted fixture FULL branch after real envelope gossip checks.
    /// This is no execution/state/DA proof; the unchanged strict import method remains separate.
    pub fn fixture_declare_gossip_payload(
        &mut self,
        signed: &SignedExecutionPayloadEnvelope,
    ) -> Result<(), GossipEnvelopeError> {
        envelope_gossip::validate(self, signed)?;
        let mut candidate = self.clone();
        candidate
            .inner
            .payloads
            .insert(signed.message.beacon_block_root, Arc::new(signed.message.clone()));
        candidate.inner.check_budget()?;
        *self = candidate;
        Ok(())
    }
    /// Declare finalized timing for propagation without changing head/finality graph queries.
    /// Published pre-finalized gossip cases use arbitrary roots; no checkpoint proof is asserted.
    pub fn fixture_set_gossip_finalized_checkpoint(
        &mut self,
        checkpoint: Checkpoint,
    ) -> Result<(), StoreError> {
        checkpoint.epoch.checked_mul(self.inner.slots_per_epoch()).ok_or(StoreError::Overflow)?;
        self.gossip_finalized = Some(checkpoint);
        Ok(())
    }
    /// Checked current fixture event clock; branch setup can advance without time regression.
    pub const fn time_ms(&self) -> u64 {
        self.inner.time_ms()
    }
    /// Borrow the exact locally retained block without exporting a shared authoritative Arc.
    pub fn block(&self, root: Root) -> Option<&BeaconBlock> {
        self.inner
            .blocks
            .get(&root)
            .map(AsRef::as_ref)
            .or_else(|| self.observed_failed_blocks.get(&root))
    }
    /// Checked genesis clock.
    pub const fn genesis_time_ms(&self) -> u64 {
        self.inner.genesis_time_ms()
    }
    /// Checked slot clock.
    pub const fn slot_duration_ms(&self) -> u64 {
        self.inner.slot_duration_ms()
    }
}

impl gossip_view::sealed::Sealed for SpecStore {}
impl GloasGossipView for SpecStore {
    fn store_parameters(&self) -> StoreParameters {
        self.inner.parameters
    }
    fn finalized_checkpoint(&self) -> Checkpoint {
        self.gossip_finalized.unwrap_or(self.inner.finalized)
    }
    fn replay_finalized_checkpoint(&self, root: Root) -> Option<Checkpoint> {
        self.replay_checkpoints.get(&root).copied()
    }
    fn processing_parameters(&self) -> &GloasParameters {
        &self.gossip_processing
    }
    fn block(&self, root: Root) -> Option<&BeaconBlock> {
        Self::block(self, root)
    }
    fn block_state(&self, root: Root) -> Option<&BeaconState> {
        self.inner.block_state(root)
    }
    fn payload(&self, root: Root) -> Option<&ExecutionPayloadEnvelope> {
        self.inner
            .payloads
            .get(&root)
            .map(AsRef::as_ref)
            .or_else(|| self.asserted_payload_observations.get(&root))
    }
    fn execution_status(&self, root: Root) -> ExecutionStatus {
        self.observed_payload_statuses.get(&root).copied().unwrap_or_else(|| {
            if self.inner.payloads.contains_key(&root) {
                ExecutionStatus::Valid
            } else {
                ExecutionStatus::Optimistic
            }
        })
    }
    fn get_head(&self) -> Result<ForkChoiceNode, StoreError> {
        self.inner.get_head()
    }
    fn get_ancestor(
        &self,
        current: ForkChoiceNode,
        slot: u64,
    ) -> Result<ForkChoiceNode, StoreError> {
        self.inner.get_ancestor(current, slot)
    }
    fn is_valid_dependent_root(&self, root: Root, slot: u64) -> Result<bool, StoreError> {
        self.inner.is_valid_dependent_root(root, slot)
    }
    fn should_build_on_full(&self, head: ForkChoiceNode, slot: u64) -> Result<bool, StoreError> {
        self.inner.should_build_on_full(head, slot)
    }
    fn genesis_time_ms(&self) -> u64 {
        Self::genesis_time_ms(self)
    }
    fn slot_duration_ms(&self) -> u64 {
        Self::slot_duration_ms(self)
    }
    fn slots_per_epoch(&self) -> u64 {
        self.inner.slots_per_epoch()
    }
}

impl crate::fast_confirmation::sealed::Sealed for SpecStore {}
impl crate::fast_confirmation::FastConfirmationView for SpecStore {
    const VERIFY_SIGNATURES: bool = true;
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

impl std::fmt::Debug for SpecStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SpecStore")
            .field("retained_blocks", &self.inner.blocks.len())
            .field("retained_payloads", &self.inner.payloads.len())
            .field("observed_failed_blocks", &self.observed_failed_blocks.len())
            .field("asserted_payloads", &self.asserted_payload_observations.len())
            .field("payload_status_observations", &self.observed_payload_statuses.len())
            .field("replay_checkpoint_count", &self.replay_checkpoints.len())
            .field("gossip_finalized", &self.gossip_finalized)
            .field("time_ms", &self.inner.time_ms())
            .finish()
    }
}
