//! Sealed borrowed Gloas views for shared bid/preference admission.
//! Runtime views retain mandatory signatures. Isolated fixture views cannot expose the inner
//! production graph, shared allocations or an implicit conversion into runtime authority.

use super::GloasStore;
use crate::{
    gossip_view::GossipView, ExecutionStatus, ForkChoiceNode, PayloadStatus, StoreError,
    StoreParameters,
};
use reth_embedded_beacon_primitives::{Checkpoint, Root};
use reth_embedded_beacon_state_transition::{
    fork_processing::TransitionParameters,
    gloas_processing::GloasParameters,
    ssz::{
        self,
        gloas::{BeaconBlock, BeaconState, ExecutionPayloadEnvelope},
    },
};

pub(super) mod sealed {
    pub trait Sealed {}
}

/// Borrowed checked graph queries; only this crate can implement the context.
/// Gossip acceptance never grants state-transition, execution or data-availability authority.
pub trait GloasGossipView: sealed::Sealed {
    /// Finalized context used by gossip timing. Fixture observations do not prove finality.
    fn finalized_checkpoint(&self) -> Checkpoint;
    /// Optional declared fixture finality for a temporary gossip replay copy only.
    /// Runtime views always return `None`; retained state commitments are never replaced.
    fn replay_finalized_checkpoint(&self, _root: Root) -> Option<Checkpoint> {
        None
    }
    /// Exact configured graph policy and limits, checked by its constructor.
    fn store_parameters(&self) -> StoreParameters;
    /// Exact processing parameters checked against the complete anchor.
    fn processing_parameters(&self) -> &GloasParameters;
    /// Retained block or explicitly isolated fixture observation.
    fn block(&self, root: Root) -> Option<&BeaconBlock>;
    /// Retained block post-state, without an extractable shared authoritative allocation.
    fn block_state(&self, root: Root) -> Option<&BeaconState>;
    /// Borrow a genuine earlier-schema block retained by authenticated migration.
    /// Isolated views and graphs without migration history return absence.
    fn historical_block(&self, _root: Root) -> Option<&ssz::BeaconBlock> {
        None
    }
    /// Borrow its exact original earlier-schema post-state, without relabeling or extracting an
    /// `Arc`.
    fn historical_block_state(&self, _root: Root) -> Option<&ssz::BeaconState> {
        None
    }
    /// Borrow the original checked full transition schedule retained by authenticated migration.
    fn historical_transition_parameters(&self) -> Option<&TransitionParameters> {
        None
    }
    /// Distinguish actual Gloas payload branches from genuine historical roots.
    fn node_for_root(&self, root: Root) -> Result<ForkChoiceNode, StoreError> {
        if self.block(root).is_some() {
            Ok(ForkChoiceNode { root, payload_status: Some(PayloadStatus::Pending) })
        } else if self.historical_block(root).is_some() {
            Ok(ForkChoiceNode { root, payload_status: None })
        } else {
            Err(StoreError::UnknownRoot)
        }
    }
    /// Retained runtime-verified envelope or explicitly declared isolated fixture context.
    fn payload(&self, root: Root) -> Option<&ExecutionPayloadEnvelope>;
    /// Actual verification result, or an explicit isolated fixture status observation.
    /// A status query does not establish payload verification or import authority.
    fn execution_status(&self, root: Root) -> ExecutionStatus;
    /// Actual graph head and its payload branch.
    fn get_head(&self) -> Result<ForkChoiceNode, StoreError>;
    /// Payload-aware ancestry at or before the supplied slot.
    fn get_ancestor(
        &self,
        current: ForkChoiceNode,
        slot: u64,
    ) -> Result<ForkChoiceNode, StoreError>;
    /// Whether a retained root can be the proposer lookahead dependency.
    fn is_valid_dependent_root(&self, root: Root, slot: u64) -> Result<bool, StoreError>;
    /// Exact graph branch decision for the intended proposal slot.
    fn should_build_on_full(&self, head: ForkChoiceNode, slot: u64) -> Result<bool, StoreError>;
    /// Already checked genesis clock.
    fn genesis_time_ms(&self) -> u64;
    /// Already checked configured slot clock.
    fn slot_duration_ms(&self) -> u64;
    /// Already checked configured epoch length.
    fn slots_per_epoch(&self) -> u64;
}

impl sealed::Sealed for GloasStore {}
impl GloasGossipView for GloasStore {
    fn store_parameters(&self) -> StoreParameters {
        self.parameters
    }
    fn finalized_checkpoint(&self) -> Checkpoint {
        self.finalized
    }
    fn processing_parameters(&self) -> &GloasParameters {
        Self::processing_parameters(self)
    }
    fn block(&self, root: Root) -> Option<&BeaconBlock> {
        self.blocks.get(&root).map(AsRef::as_ref)
    }
    fn block_state(&self, root: Root) -> Option<&BeaconState> {
        Self::block_state(self, root)
    }
    fn historical_block(&self, root: Root) -> Option<&ssz::BeaconBlock> {
        self.historical.as_ref()?.block(root)
    }
    fn historical_block_state(&self, root: Root) -> Option<&ssz::BeaconState> {
        self.historical.as_ref()?.block_state(root)
    }
    fn historical_transition_parameters(&self) -> Option<&TransitionParameters> {
        self.historical.as_ref()?.transition_parameters()
    }
    fn payload(&self, root: Root) -> Option<&ExecutionPayloadEnvelope> {
        self.payloads.get(&root).map(AsRef::as_ref)
    }
    fn execution_status(&self, root: Root) -> ExecutionStatus {
        if self.payloads.contains_key(&root) {
            ExecutionStatus::Valid
        } else if let Some(history) = &self.historical &&
            history.block(root).is_some()
        {
            GossipView::execution_status(history.as_ref(), root)
        } else {
            ExecutionStatus::Optimistic
        }
    }
    fn get_head(&self) -> Result<ForkChoiceNode, StoreError> {
        Self::get_head(self)
    }
    fn get_ancestor(
        &self,
        current: ForkChoiceNode,
        slot: u64,
    ) -> Result<ForkChoiceNode, StoreError> {
        Self::get_ancestor(self, current, slot)
    }
    fn is_valid_dependent_root(&self, root: Root, slot: u64) -> Result<bool, StoreError> {
        Self::is_valid_dependent_root(self, root, slot)
    }
    fn should_build_on_full(&self, head: ForkChoiceNode, slot: u64) -> Result<bool, StoreError> {
        Self::should_build_on_full(self, head, slot)
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
}
