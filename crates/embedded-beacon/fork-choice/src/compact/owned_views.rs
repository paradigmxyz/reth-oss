//! Checked owning gossip reads and prepared sealed fast-confirmation contexts.
//! Earlier borrowed views retain their exact signature policy; compact views own data leases.
use super::{GraphStateRead, OwnedStateRead, StateRead, StateSchema, StateSourceError};
use crate::{
    fast_confirmation::{self, BlockInfo, FastConfirmationView, Message, ProcessingRef, StateRef},
    gloas::gossip_view::GloasGossipView,
    gossip_view::GossipView,
    ExecutionStatus, ForkChoiceNode, Store, StoreError, StoreParameters,
};
use reth_embedded_beacon_primitives::{Checkpoint, Root};
use reth_embedded_beacon_state_transition::{
    fork_processing::TransitionParameters,
    gloas_processing::GloasParameters,
    ssz::{gloas, BeaconBlock, BeaconState},
};
use std::{collections::BTreeMap, ops::Deref};
pub(crate) mod sealed {
    pub trait Sealed {}
    pub trait GloasSealed {}
}
/// Borrowed ordinary data or an owning immutable lease, tied to this read's entire lifetime.
#[derive(Debug)]
pub enum StateAccess<'a, S: StateSchema> {
    /// Ordinary data retained by the immutable graph.
    Borrowed(&'a S),
    /// Owning cold/hot guard which survives cache eviction.
    Owned(OwnedStateRead<S>),
}
impl<S: StateSchema> Clone for StateAccess<'_, S> {
    fn clone(&self) -> Self {
        match self {
            Self::Borrowed(s) => Self::Borrowed(s),
            Self::Owned(s) => Self::Owned(s.clone()),
        }
    }
}
impl<S: StateSchema> Deref for StateAccess<'_, S> {
    type Target = S;
    fn deref(&self) -> &S {
        match self {
            Self::Borrowed(s) => s,
            Self::Owned(s) => s,
        }
    }
}
impl<S: StateSchema> AsRef<S> for StateAccess<'_, S> {
    fn as_ref(&self) -> &S {
        self
    }
}
/// Sealed classic graph with owned state access and explicit known-data errors.
pub trait ReadGossipView: sealed::Sealed {
    /// Fixed strict/isolated policy inherited from the graph type.
    const VERIFY_SIGNATURES: bool;
    /// Unknown graph roots are absent; a known unavailable/corrupt cold root is an error.
    fn read_block_state(
        &self,
        root: Root,
    ) -> Result<Option<StateAccess<'_, BeaconState>>, StoreError>;
    /// Exact private graph query.
    fn block(&self, root: Root) -> Option<&BeaconBlock>;
    /// Exact private graph query.
    fn get_head(&self) -> Result<ForkChoiceNode, StoreError>;
    /// Exact private graph query.
    fn checkpoints(&self) -> (Checkpoint, Checkpoint);
    /// Exact private graph query.
    fn genesis_time_ms(&self) -> u64;
    /// Exact private graph query.
    fn slot_duration_ms(&self) -> u64;
    /// Exact private graph query.
    fn slots_per_epoch(&self) -> u64;
    /// Exact private graph query.
    fn transition_parameters(&self) -> Option<&TransitionParameters>;
    /// Exact private graph query.
    fn store_parameters(&self) -> StoreParameters;
    /// Exact private graph query.
    fn execution_status(&self, root: Root) -> ExecutionStatus;
    /// Exact private graph query.
    fn get_ancestor(&self, root: Root, slot: u64) -> Result<Root, StoreError>;
    /// Exact private graph query.
    fn get_checkpoint_block(&self, root: Root, epoch: u64) -> Result<Root, StoreError>;
}
impl<T: GossipView> sealed::Sealed for T {}
impl<T: GossipView> ReadGossipView for T {
    const VERIFY_SIGNATURES: bool = T::VERIFY_SIGNATURES;
    fn read_block_state(
        &self,
        root: Root,
    ) -> Result<Option<StateAccess<'_, BeaconState>>, StoreError> {
        Ok(GossipView::block_state(self, root).map(StateAccess::Borrowed))
    }
    fn block(&self, root: Root) -> Option<&BeaconBlock> {
        GossipView::block(self, root)
    }
    fn get_head(&self) -> Result<ForkChoiceNode, StoreError> {
        GossipView::get_head(self)
    }
    fn checkpoints(&self) -> (Checkpoint, Checkpoint) {
        GossipView::checkpoints(self)
    }
    fn genesis_time_ms(&self) -> u64 {
        GossipView::genesis_time_ms(self)
    }
    fn slot_duration_ms(&self) -> u64 {
        GossipView::slot_duration_ms(self)
    }
    fn slots_per_epoch(&self) -> u64 {
        GossipView::slots_per_epoch(self)
    }
    fn transition_parameters(&self) -> Option<&TransitionParameters> {
        GossipView::transition_parameters(self)
    }
    fn store_parameters(&self) -> StoreParameters {
        GossipView::store_parameters(self)
    }
    fn execution_status(&self, root: Root) -> ExecutionStatus {
        GossipView::execution_status(self, root)
    }
    fn get_ancestor(&self, root: Root, slot: u64) -> Result<Root, StoreError> {
        GossipView::get_ancestor(self, root, slot)
    }
    fn get_checkpoint_block(&self, root: Root, epoch: u64) -> Result<Root, StoreError> {
        GossipView::get_checkpoint_block(self, root, epoch)
    }
}
/// Sealed Gloas/history graph preserving actual payload evidence and owning state reads.
pub trait ReadGloasGossipView: sealed::GloasSealed {
    /// Checked typed Gloas read; known failures never become unknown-parent absence.
    fn read_block_state(
        &self,
        root: Root,
    ) -> Result<Option<StateAccess<'_, gloas::BeaconState>>, StoreError>;
    /// Genuine earlier schema retained through actual migration.
    fn read_historical_block_state(
        &self,
        root: Root,
    ) -> Result<Option<StateAccess<'_, BeaconState>>, StoreError>;
    /// Exact private graph query.
    fn finalized_checkpoint(&self) -> Checkpoint;
    /// Exact private graph query.
    fn replay_finalized_checkpoint(&self, root: Root) -> Option<Checkpoint>;
    /// Exact private graph query.
    fn store_parameters(&self) -> StoreParameters;
    /// Exact private graph query.
    fn processing_parameters(&self) -> &GloasParameters;
    /// Exact private graph query.
    fn block(&self, root: Root) -> Option<&gloas::BeaconBlock>;
    /// Exact private graph query.
    fn historical_block(&self, root: Root) -> Option<&BeaconBlock>;
    /// Exact private graph query.
    fn historical_transition_parameters(&self) -> Option<&TransitionParameters>;
    /// Exact private graph query.
    fn node_for_root(&self, root: Root) -> Result<ForkChoiceNode, StoreError>;
    /// Exact private graph query.
    fn payload(&self, root: Root) -> Option<&gloas::ExecutionPayloadEnvelope>;
    /// Exact private graph query.
    fn execution_status(&self, root: Root) -> ExecutionStatus;
    /// Exact private graph query.
    fn get_head(&self) -> Result<ForkChoiceNode, StoreError>;
    /// Exact private graph query.
    fn get_ancestor(&self, node: ForkChoiceNode, slot: u64) -> Result<ForkChoiceNode, StoreError>;
    /// Exact private graph query.
    fn is_valid_dependent_root(&self, root: Root, slot: u64) -> Result<bool, StoreError>;
    /// Exact private graph query.
    fn should_build_on_full(&self, head: ForkChoiceNode, slot: u64) -> Result<bool, StoreError>;
    /// Exact private graph query.
    fn genesis_time_ms(&self) -> u64;
    /// Exact private graph query.
    fn slot_duration_ms(&self) -> u64;
    /// Exact private graph query.
    fn slots_per_epoch(&self) -> u64;
}
impl<T: GloasGossipView> sealed::GloasSealed for T {}
impl<T: GloasGossipView> ReadGloasGossipView for T {
    fn read_block_state(
        &self,
        root: Root,
    ) -> Result<Option<StateAccess<'_, gloas::BeaconState>>, StoreError> {
        Ok(GloasGossipView::block_state(self, root).map(StateAccess::Borrowed))
    }
    fn read_historical_block_state(
        &self,
        root: Root,
    ) -> Result<Option<StateAccess<'_, BeaconState>>, StoreError> {
        Ok(GloasGossipView::historical_block_state(self, root).map(StateAccess::Borrowed))
    }
    fn finalized_checkpoint(&self) -> Checkpoint {
        GloasGossipView::finalized_checkpoint(self)
    }
    fn replay_finalized_checkpoint(&self, root: Root) -> Option<Checkpoint> {
        GloasGossipView::replay_finalized_checkpoint(self, root)
    }
    fn store_parameters(&self) -> StoreParameters {
        GloasGossipView::store_parameters(self)
    }
    fn processing_parameters(&self) -> &GloasParameters {
        GloasGossipView::processing_parameters(self)
    }
    fn block(&self, root: Root) -> Option<&gloas::BeaconBlock> {
        GloasGossipView::block(self, root)
    }
    fn historical_block(&self, root: Root) -> Option<&BeaconBlock> {
        GloasGossipView::historical_block(self, root)
    }
    fn historical_transition_parameters(&self) -> Option<&TransitionParameters> {
        GloasGossipView::historical_transition_parameters(self)
    }
    fn node_for_root(&self, root: Root) -> Result<ForkChoiceNode, StoreError> {
        GloasGossipView::node_for_root(self, root)
    }
    fn payload(&self, root: Root) -> Option<&gloas::ExecutionPayloadEnvelope> {
        GloasGossipView::payload(self, root)
    }
    fn execution_status(&self, root: Root) -> ExecutionStatus {
        GloasGossipView::execution_status(self, root)
    }
    fn get_head(&self) -> Result<ForkChoiceNode, StoreError> {
        GloasGossipView::get_head(self)
    }
    fn get_ancestor(&self, node: ForkChoiceNode, slot: u64) -> Result<ForkChoiceNode, StoreError> {
        GloasGossipView::get_ancestor(self, node, slot)
    }
    fn is_valid_dependent_root(&self, root: Root, slot: u64) -> Result<bool, StoreError> {
        GloasGossipView::is_valid_dependent_root(self, root, slot)
    }
    fn should_build_on_full(&self, head: ForkChoiceNode, slot: u64) -> Result<bool, StoreError> {
        GloasGossipView::should_build_on_full(self, head, slot)
    }
    fn genesis_time_ms(&self) -> u64 {
        GloasGossipView::genesis_time_ms(self)
    }
    fn slot_duration_ms(&self) -> u64 {
        GloasGossipView::slot_duration_ms(self)
    }
    fn slots_per_epoch(&self) -> u64 {
        GloasGossipView::slots_per_epoch(self)
    }
}

/// Sealed immutable confirmation context owning all required complete reads.
#[derive(Clone, Debug)]
pub struct ClassicConfirmationView {
    pub(crate) graph: Store,
    pub(crate) states: BTreeMap<Root, StateRead>,
    pub(crate) checkpoints: BTreeMap<(u64, Root), StateRead>,
}
impl fast_confirmation::sealed::Sealed for ClassicConfirmationView {}
impl FastConfirmationView for ClassicConfirmationView {
    const VERIFY_SIGNATURES: bool = true;
    fn parameters(&self) -> StoreParameters {
        FastConfirmationView::parameters(&self.graph)
    }
    fn processing(&self) -> ProcessingRef<'_> {
        FastConfirmationView::processing(&self.graph)
    }
    fn current_slot(&self) -> u64 {
        FastConfirmationView::current_slot(&self.graph)
    }
    fn time_ms(&self) -> u64 {
        FastConfirmationView::time_ms(&self.graph)
    }
    fn genesis_time_ms(&self) -> u64 {
        FastConfirmationView::genesis_time_ms(&self.graph)
    }
    fn checkpoints(&self) -> (Checkpoint, Checkpoint) {
        FastConfirmationView::checkpoints(&self.graph)
    }
    fn unrealized_justified(&self) -> Checkpoint {
        FastConfirmationView::unrealized_justified(&self.graph)
    }
    fn head(&self) -> Result<ForkChoiceNode, StoreError> {
        FastConfirmationView::head(&self.graph)
    }
    fn block_info(&self, root: Root) -> Result<BlockInfo, StoreError> {
        let mut info = FastConfirmationView::block_info(&self.graph, root)?;
        if self.graph.execution_status(root) != ExecutionStatus::Valid {
            info.safe_execution_hash = None;
        }
        Ok(info)
    }
    fn latest(&self, index: u64) -> Result<Option<Message>, StoreError> {
        FastConfirmationView::latest(&self.graph, index)
    }
    fn equivocating(&self, index: u64) -> bool {
        FastConfirmationView::equivocating(&self.graph, index)
    }
    fn is_ancestor(
        &self,
        node: ForkChoiceNode,
        ancestor: ForkChoiceNode,
    ) -> Result<bool, StoreError> {
        FastConfirmationView::is_ancestor(&self.graph, node, ancestor)
    }
    fn ancestor(&self, node: ForkChoiceNode, slot: u64) -> Result<ForkChoiceNode, StoreError> {
        FastConfirmationView::ancestor(&self.graph, node, slot)
    }
    fn voting_source(&self, root: Root) -> Result<Checkpoint, StoreError> {
        FastConfirmationView::voting_source(&self.graph, root)
    }
    fn unrealized_justification(&self, root: Root) -> Result<Checkpoint, StoreError> {
        FastConfirmationView::unrealized_justification(&self.graph, root)
    }
    fn is_gloas(&self) -> bool {
        FastConfirmationView::is_gloas(&self.graph)
    }
    fn has_payload_branches(&self, root: Root) -> bool {
        FastConfirmationView::has_payload_branches(&self.graph, root)
    }
    fn state(&self, root: Root) -> Result<StateRef<'_>, StoreError> {
        let state = self.states.get(&root).ok_or(StateSourceError::Unavailable)?;
        Ok(StateRef::Classic(state))
    }
    fn checkpoint_state(&self, checkpoint: Checkpoint) -> Result<StateRef<'_>, StoreError> {
        let state = self
            .checkpoints
            .get(&(checkpoint.epoch, checkpoint.root))
            .ok_or(StateSourceError::Unavailable)?;
        Ok(StateRef::Classic(state))
    }
}

/// Sealed immutable confirmation context owning all required complete reads.
#[derive(Clone, Debug)]
pub struct GloasConfirmationView {
    pub(crate) graph: crate::gloas::GloasStore,
    pub(crate) states: BTreeMap<Root, GraphStateRead>,
    pub(crate) checkpoints: BTreeMap<(u64, Root), GraphStateRead>,
}
impl fast_confirmation::sealed::Sealed for GloasConfirmationView {}
impl FastConfirmationView for GloasConfirmationView {
    const VERIFY_SIGNATURES: bool = true;
    fn parameters(&self) -> StoreParameters {
        FastConfirmationView::parameters(&self.graph)
    }
    fn processing(&self) -> ProcessingRef<'_> {
        FastConfirmationView::processing(&self.graph)
    }
    fn current_slot(&self) -> u64 {
        FastConfirmationView::current_slot(&self.graph)
    }
    fn time_ms(&self) -> u64 {
        FastConfirmationView::time_ms(&self.graph)
    }
    fn genesis_time_ms(&self) -> u64 {
        FastConfirmationView::genesis_time_ms(&self.graph)
    }
    fn checkpoints(&self) -> (Checkpoint, Checkpoint) {
        FastConfirmationView::checkpoints(&self.graph)
    }
    fn unrealized_justified(&self) -> Checkpoint {
        FastConfirmationView::unrealized_justified(&self.graph)
    }
    fn head(&self) -> Result<ForkChoiceNode, StoreError> {
        FastConfirmationView::head(&self.graph)
    }
    fn block_info(&self, root: Root) -> Result<BlockInfo, StoreError> {
        FastConfirmationView::block_info(&self.graph, root)
    }
    fn latest(&self, index: u64) -> Result<Option<Message>, StoreError> {
        FastConfirmationView::latest(&self.graph, index)
    }
    fn equivocating(&self, index: u64) -> bool {
        FastConfirmationView::equivocating(&self.graph, index)
    }
    fn is_ancestor(
        &self,
        node: ForkChoiceNode,
        ancestor: ForkChoiceNode,
    ) -> Result<bool, StoreError> {
        FastConfirmationView::is_ancestor(&self.graph, node, ancestor)
    }
    fn ancestor(&self, node: ForkChoiceNode, slot: u64) -> Result<ForkChoiceNode, StoreError> {
        FastConfirmationView::ancestor(&self.graph, node, slot)
    }
    fn voting_source(&self, root: Root) -> Result<Checkpoint, StoreError> {
        FastConfirmationView::voting_source(&self.graph, root)
    }
    fn unrealized_justification(&self, root: Root) -> Result<Checkpoint, StoreError> {
        FastConfirmationView::unrealized_justification(&self.graph, root)
    }
    fn is_gloas(&self) -> bool {
        FastConfirmationView::is_gloas(&self.graph)
    }
    fn has_payload_branches(&self, root: Root) -> bool {
        FastConfirmationView::has_payload_branches(&self.graph, root)
    }
    fn state(&self, root: Root) -> Result<StateRef<'_>, StoreError> {
        let state = self.states.get(&root).ok_or(StateSourceError::Unavailable)?;
        Ok(state.view())
    }
    fn checkpoint_state(&self, checkpoint: Checkpoint) -> Result<StateRef<'_>, StoreError> {
        let state = self
            .checkpoints
            .get(&(checkpoint.epoch, checkpoint.root))
            .ok_or(StateSourceError::Unavailable)?;
        Ok(state.view())
    }
}
