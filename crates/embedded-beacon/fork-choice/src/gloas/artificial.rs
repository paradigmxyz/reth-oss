//! Isolated BLS-disabled Gloas compliance graphs for published fixtures only.
//! No conversion, dereference, inner graph or shared allocation can escape this namespace.
//! Canonical transition roots, membership, timing and mandatory Engine/DA callbacks stay checked.

use super::{DataAvailability, EnvelopeError, ExecutionEngine, GloasStore};
use crate::{ForkChoiceNode, StoreError, StoreParameters};
use reth_embedded_beacon_primitives::{Checkpoint, Root};
use reth_embedded_beacon_state_transition::{
    gloas_processing::GloasParameters,
    ssz::{
        auxiliary::gloas::PayloadAttestationMessage,
        gloas::{
            Attestation, AttesterSlashing, BeaconBlock, BeaconState, SignedBeaconBlock,
            SignedExecutionPayloadEnvelope,
        },
    },
};
use std::collections::BTreeSet;

mod compliance;

/// A sealed artificial graph with a fixed BLS-disabled fixture policy and no runtime extraction.
#[derive(Clone, Debug)]
pub struct ArtificialSpecStore {
    inner: GloasStore,
}

impl ArtificialSpecStore {
    /// Validate the exact full fixture anchor, fork, preset and clock before any replay.
    pub fn get_store(
        state: BeaconState,
        block: BeaconBlock,
        parameters: StoreParameters,
        processing: GloasParameters,
    ) -> Result<Self, StoreError> {
        Ok(Self { inner: GloasStore::get_store(state, block, parameters, processing)? })
    }
    /// Replay the same bounded monotonic clock under the fixed fixture signature policy.
    pub fn on_tick(&mut self, time_ms: u64) -> Result<(), StoreError> {
        self.inner.on_tick_inner::<false>(time_ms)
    }
    /// Replay complete signed schemas and verify exact post-state roots, skipping only fixture BLS.
    pub fn on_block_and_contents(&mut self, block: &SignedBeaconBlock) -> Result<(), StoreError> {
        self.inner.on_block_and_contents_inner::<false>(block)
    }
    /// Verify actual committee membership, ancestry and event timing before recording a vote.
    pub fn on_attestation(&mut self, attestation: &Attestation) -> Result<(), StoreError> {
        self.inner.on_attestation_inner::<false>(attestation)
    }
    /// Require an actual slashable pair with bounded sorted indices before excluding equivocators.
    pub fn on_attester_slashing(&mut self, slashing: &AttesterSlashing) -> Result<(), StoreError> {
        self.inner.on_attester_slashing_inner::<false>(slashing)
    }
    /// Require the exact current-slot PTC membership and retain every duplicate committee position.
    pub fn on_payload_attestation_message(
        &mut self,
        message: &PayloadAttestationMessage,
    ) -> Result<(), StoreError> {
        self.inner.on_payload_attestation_message_inner::<false>(message)
    }
    /// Require identical commitments and real Engine/DA callback results; only fixture BLS differs.
    pub async fn on_execution_payload_envelope<E: ExecutionEngine, D: DataAvailability>(
        &mut self,
        envelope: &SignedExecutionPayloadEnvelope,
        engine: &E,
        availability: &D,
    ) -> Result<(), EnvelopeError> {
        self.inner
            .on_execution_payload_envelope_inner::<false, E, D>(envelope, engine, availability)
            .await
    }
    /// Borrow the computed fixture post-state, without exporting an authoritative shared
    /// allocation.
    pub fn block_state(&self, root: Root) -> Option<&BeaconState> {
        self.inner.block_state(root)
    }
    /// Current realized and pulled-up fixture checkpoints.
    pub const fn checkpoints(&self) -> (Checkpoint, Checkpoint, Checkpoint, Checkpoint) {
        self.inner.checkpoints()
    }
    /// Exact checked fixture clock in milliseconds.
    pub const fn time_ms(&self) -> u64 {
        self.inner.time_ms()
    }
    /// Exact checked genesis clock in milliseconds.
    pub const fn genesis_time_ms(&self) -> u64 {
        self.inner.genesis_time_ms()
    }
    /// Exact checked fixture slot duration in milliseconds.
    pub const fn slot_duration_ms(&self) -> u64 {
        self.inner.slot_duration_ms()
    }
    /// Exact checked fixture epoch length.
    pub const fn slots_per_epoch(&self) -> u64 {
        self.inner.slots_per_epoch()
    }
    /// Current fixture proposer boost root.
    pub const fn proposer_boost_root(&self) -> Root {
        self.inner.proposer_boost_root()
    }
    /// Ordinary payload-aware head selection in the isolated graph.
    pub fn get_head(&self) -> Result<ForkChoiceNode, StoreError> {
        self.inner.get_head()
    }
    /// Ordinary payload-aware proposer head selection in the isolated graph.
    pub fn get_proposer_head(&self) -> Result<ForkChoiceNode, StoreError> {
        self.inner.get_proposer_head()
    }
    /// Compute the entire filtered viable fixture node set.
    pub fn get_filtered_node_tree(&self) -> Result<BTreeSet<ForkChoiceNode>, StoreError> {
        self.inner.get_filtered_node_tree()
    }
    /// Compute the exact graph children of this payload branch.
    pub fn get_node_children(
        &self,
        node: ForkChoiceNode,
    ) -> Result<Vec<ForkChoiceNode>, StoreError> {
        self.inner.get_node_children(node)
    }
    /// Exact LMD weight and proposer boost for a fixture payload branch.
    pub fn get_weight(&self, node: ForkChoiceNode) -> Result<u64, StoreError> {
        self.inner.get_weight(node)
    }
    /// Exact PTC timeliness position observations; these do not grant local payload admission.
    pub fn payload_timeliness_votes(&self, root: Root) -> Option<&[Option<bool>]> {
        self.inner.payload_timeliness_votes(root)
    }
    /// Exact PTC availability position observations; these do not substitute for DA verification.
    pub fn payload_data_availability_votes(&self, root: Root) -> Option<&[Option<bool>]> {
        self.inner.payload_data_availability_votes(root)
    }
}

impl crate::fast_confirmation::sealed::Sealed for ArtificialSpecStore {}
impl crate::fast_confirmation::FastConfirmationView for ArtificialSpecStore {
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
