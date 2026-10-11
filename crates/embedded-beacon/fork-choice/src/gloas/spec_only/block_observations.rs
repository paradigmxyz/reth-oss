//! Explicit bounded block-gossip observations confined to the strict fixture wrapper.

use super::{SpecStore, StoreError};
use crate::{
    gloas::envelope_gossip::{self, GossipEnvelopeError},
    ExecutionStatus,
};
use reth_embedded_beacon_primitives::Root;
use reth_embedded_beacon_state_transition::ssz::{
    gloas::{BeaconBlock, SignedExecutionPayloadEnvelope},
    Ssz,
};

impl SpecStore {
    /// Retain a complete declared failed parent solely in the wrapper observation map.
    /// The private validated graph, its head and all post-state lookups remain unchanged.
    pub fn fixture_observe_block_without_state(
        &mut self,
        block: BeaconBlock,
    ) -> Result<Root, StoreError> {
        let root = block.hash_tree_root(self.inner.processing.preset())?;
        if self.inner.blocks.contains_key(&root) {
            return Err(StoreError::AnchorMismatch)
        }
        if let Some(previous) = self.observed_failed_blocks.get(&root) {
            return if previous == &block { Ok(root) } else { Err(StoreError::AnchorMismatch) }
        }
        let mut candidate = self.clone();
        candidate.observed_failed_blocks.insert(root, block);
        candidate.check_block_observation_budget()?;
        *self = candidate;
        Ok(root)
    }
    /// Record the published assertion that an anchor payload was already verified.
    /// Real complete linkage and signatures are checked, but no Engine/DA proof is inferred.
    /// This assertion stays outside the validated graph and must be reported by the adapter.
    pub fn fixture_assert_verified_payload_observation(
        &mut self,
        signed: &SignedExecutionPayloadEnvelope,
    ) -> Result<(), GossipEnvelopeError> {
        envelope_gossip::validate(self, signed)?;
        let root = signed.message.beacon_block_root;
        if let Some(retained) = self.inner.payloads.get(&root) {
            return if retained.as_ref() == &signed.message {
                Ok(())
            } else {
                Err(StoreError::AnchorMismatch.into())
            }
        }
        if let Some(previous) = self.asserted_payload_observations.get(&root) {
            return if previous == &signed.message {
                Ok(())
            } else {
                Err(StoreError::AnchorMismatch.into())
            }
        }
        let mut candidate = self.clone();
        candidate.asserted_payload_observations.insert(root, signed.message.clone());
        candidate.check_block_observation_budget()?;
        *self = candidate;
        Ok(())
    }
    /// Declare a processed execution result only in this isolated fixture wrapper.
    /// This does not admit an envelope, create a FULL branch or modify retained post-states.
    pub fn fixture_set_payload_status(
        &mut self,
        root: Root,
        status: ExecutionStatus,
    ) -> Result<(), StoreError> {
        if self.block(root).is_none() {
            return Err(StoreError::UnknownRoot)
        }
        let mut candidate = self.clone();
        candidate.observed_payload_statuses.insert(root, status);
        candidate.check_block_observation_budget()?;
        *self = candidate;
        Ok(())
    }
    fn check_block_observation_budget(&self) -> Result<(), StoreError> {
        if self
            .inner
            .blocks
            .len()
            .checked_add(self.observed_failed_blocks.len())
            .ok_or(StoreError::Overflow)? >
            self.inner.parameters.max_blocks ||
            self.asserted_payload_observations.len() > self.inner.parameters.max_blocks ||
            self.observed_payload_statuses.len() > self.inner.parameters.max_blocks
        {
            return Err(StoreError::ResourceLimit)
        }
        let mut bytes = self.inner.retained_schema_bytes()?;
        for block in self.observed_failed_blocks.values() {
            bytes = bytes
                .checked_add(block.heap_bytes()?)
                .and_then(|value| value.checked_add(std::mem::size_of_val(block)))
                .ok_or(StoreError::Overflow)?;
        }
        for payload in self.asserted_payload_observations.values() {
            bytes = bytes
                .checked_add(payload.heap_bytes()?)
                .and_then(|value| value.checked_add(std::mem::size_of_val(payload)))
                .ok_or(StoreError::Overflow)?;
        }
        bytes = bytes
            .checked_add(
                self.observed_payload_statuses
                    .len()
                    .checked_mul(std::mem::size_of::<(Root, ExecutionStatus)>())
                    .ok_or(StoreError::Overflow)?,
            )
            .ok_or(StoreError::Overflow)?;
        if bytes > self.inner.parameters.max_retained_schema_bytes {
            return Err(StoreError::ResourceLimit)
        }
        Ok(())
    }
}
