//! Sealed borrowed consensus graph views for shared gossip algorithms.
//! The runtime view always requires signatures. A separately typed fixture-only view cannot
//! expose its inner Store, shared authoritative Arc or conversion into a runtime graph.

use crate::{ExecutionStatus, ForkChoiceNode, Store, StoreError, StoreParameters};
use reth_embedded_beacon_primitives::{Checkpoint, Root};
use reth_embedded_beacon_state_transition::{
    fork_processing::TransitionParameters,
    ssz::{BeaconBlock, BeaconState},
};

pub(crate) mod sealed {
    pub trait Sealed {}
}

/// Read-only graph context; only this crate can supply an implementation.
/// Runtime callers use the concrete Store, whose strict imports authenticate every post-state.
/// Fixture views expose schema observations only and confer no block or execution authority.
pub trait GossipView: sealed::Sealed {
    /// Fixed signature policy of this type; runtime Store always sets true.
    const VERIFY_SIGNATURES: bool;
    /// Borrow an actually retained block or an isolated fixture observation.
    fn block(&self, root: Root) -> Option<&BeaconBlock>;
    /// Borrow the exact retained post-state without extracting a shared authoritative Arc.
    fn block_state(&self, root: Root) -> Option<&BeaconState>;
    /// Compute the head from the actual private graph.
    fn get_head(&self) -> Result<ForkChoiceNode, StoreError>;
    /// Return the current justified and finalized checkpoints.
    fn checkpoints(&self) -> (Checkpoint, Checkpoint);
    /// Already checked genesis clock in milliseconds.
    fn genesis_time_ms(&self) -> u64;
    /// Already checked configured slot duration.
    fn slot_duration_ms(&self) -> u64;
    /// Exact configured preset's epoch length.
    fn slots_per_epoch(&self) -> u64;
    /// Borrow privately bound and checked transition parameters.
    fn transition_parameters(&self) -> Option<&TransitionParameters>;
    /// Copy the exact configured store limits and clock policy without exposing the graph.
    fn store_parameters(&self) -> StoreParameters;
    /// Locally known execution status; isolated fixture declarations confer no execution proof.
    fn execution_status(&self, root: Root) -> ExecutionStatus {
        if self.block_state(root).is_some() {
            ExecutionStatus::Valid
        } else {
            ExecutionStatus::Optimistic
        }
    }
    /// Walk actual complete block links with the privately bound graph retention budget.
    fn get_ancestor(&self, mut root: Root, slot: u64) -> Result<Root, StoreError> {
        for _ in 0..self.store_parameters().max_blocks {
            let (current_slot, parent) =
                block_link(self.block(root).ok_or(StoreError::UnknownRoot)?);
            if current_slot <= slot {
                return Ok(root)
            }
            let (parent_slot, _) = block_link(self.block(parent).ok_or(StoreError::UnknownRoot)?);
            if parent_slot >= current_slot {
                return Err(StoreError::InvalidBlockTime)
            }
            root = parent;
        }
        Err(StoreError::ResourceLimit)
    }
    /// Exact retained checkpoint ancestor at the first slot of the requested epoch.
    fn get_checkpoint_block(&self, root: Root, epoch: u64) -> Result<Root, StoreError> {
        self.get_ancestor(
            root,
            epoch.checked_mul(self.slots_per_epoch()).ok_or(StoreError::Overflow)?,
        )
    }
}

impl sealed::Sealed for Store {}
impl GossipView for Store {
    const VERIFY_SIGNATURES: bool = true;
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
    fn execution_status(&self, root: Root) -> ExecutionStatus {
        Self::execution_status(self, root)
    }
}

/// Read scalar links without rehashing or copying a complete block body.
pub(crate) const fn block_link(block: &BeaconBlock) -> (u64, Root) {
    match block {
        BeaconBlock::Phase0(value, _) => (value.slot, value.parent_root),
        BeaconBlock::Altair(value, _) => (value.slot, value.parent_root),
        BeaconBlock::Bellatrix(value, _) => (value.slot, value.parent_root),
        BeaconBlock::Capella(value, _) => (value.slot, value.parent_root),
        BeaconBlock::Deneb(value, _) => (value.slot, value.parent_root),
        BeaconBlock::Electra(value, _) => (value.slot, value.parent_root),
        BeaconBlock::Fulu(value, _) => (value.slot, value.parent_root),
        BeaconBlock::Gloas(value, _) => (value.slot, value.parent_root),
    }
}
