//! Ordered Gloas payload-attestation propagation with real BLS and bounded observations.
//! Successful gossip never records fork-choice votes, validates a payload or grants FULL.

use crate::gloas_bids::GossipContext;
use reth_embedded_beacon_crypto::{compute_domain, compute_signing_root, verify_signature};
use reth_embedded_beacon_fork_choice::{compact::ReadGloasGossipView, StoreError};
use reth_embedded_beacon_primitives::{Root, SszError};
use reth_embedded_beacon_state_transition::{
    gloas_processing,
    phase0_processing::ProcessingError,
    ssz::{auxiliary::gloas::PayloadAttestationMessage, gloas::BeaconBlock, Preset, Ssz},
};
use std::collections::{BTreeMap, BTreeSet};

/// Independent bounded collections of observed roots and accepted (slot, validator) identities.
#[derive(Clone, Debug)]
pub struct SeenPayloadAttestations {
    observed: BTreeMap<Root, u64>,
    validators: BTreeSet<(u64, u64)>,
    max_entries: usize,
    context: Option<GossipContext>,
}

/// Propagation classification remains distinct from local resource or infrastructure failures.
#[derive(Debug, thiserror::Error)]
pub enum PayloadAttestationGossipError {
    /// A duplicate, missing prerequisite or wrong assigned slot should not propagate.
    #[error("ignore payload-attestation gossip: {0}")]
    Ignore(&'static str),
    /// A structurally or cryptographically invalid message should not propagate.
    #[error("reject payload-attestation gossip: {0}")]
    Reject(&'static str),
    /// A bounded collection or object memory budget was exceeded.
    #[error("payload-attestation gossip resource limit exceeded")]
    ResourceLimit,
    /// Clock or index arithmetic overflowed.
    #[error("payload-attestation gossip arithmetic overflow")]
    Overflow,
    /// An authoritative graph query failed.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// The checked state failed a processing invariant.
    #[error(transparent)]
    Processing(#[from] ProcessingError),
    /// A candidate violated its complete typed SSZ structure.
    #[error(transparent)]
    Ssz(#[from] SszError),
}

impl SeenPayloadAttestations {
    /// Apply the same maximum independently to observations and accepted message identities.
    pub const fn new(max_entries: usize) -> Result<Self, PayloadAttestationGossipError> {
        if max_entries == 0 {
            return Err(PayloadAttestationGossipError::ResourceLimit)
        }
        Ok(Self {
            observed: BTreeMap::new(),
            validators: BTreeSet::new(),
            max_entries,
            context: None,
        })
    }
    /// Observe a complete block without asserting that its transition or signature is valid.
    pub fn observe_block(
        &mut self,
        block: &BeaconBlock,
        preset: Preset,
    ) -> Result<(), PayloadAttestationGossipError> {
        if block
            .heap_bytes()?
            .checked_add(std::mem::size_of_val(block))
            .ok_or(PayloadAttestationGossipError::Overflow)? >
            64 * 1024 * 1024
        {
            return Err(PayloadAttestationGossipError::ResourceLimit)
        }
        let root = block.hash_tree_root(preset)?;
        if !self.observed.contains_key(&root) && self.observed.len() == self.max_entries {
            return Err(PayloadAttestationGossipError::ResourceLimit)
        }
        self.observed.insert(root, block.slot);
        Ok(())
    }
    /// Prune old observations and identities using the node's required gossip horizon.
    pub fn retain_from_slot(&mut self, slot: u64) {
        self.observed.retain(|_, value| *value >= slot);
        self.validators.retain(|(value, _)| *value >= slot);
    }
    /// Require the actual head's PTC and domain, without mutating the graph or its vote maps.
    pub fn validate_message<V: ReadGloasGossipView>(
        &mut self,
        store: &V,
        message: &PayloadAttestationMessage,
        current_time_ms: u64,
    ) -> Result<(), PayloadAttestationGossipError> {
        self.validate_with_view(store, message, current_time_ms)
    }
    fn validate_with_view<V: ReadGloasGossipView>(
        &mut self,
        store: &V,
        message: &PayloadAttestationMessage,
        current_time_ms: u64,
    ) -> Result<(), PayloadAttestationGossipError> {
        if let Some(context) = &self.context &&
            !context.matches(store)?
        {
            return Err(PayloadAttestationGossipError::Reject(
                "gossip cache belongs to another chain or configuration",
            ))
        }
        let data = &message.data;
        let parameters = store.processing_parameters();
        let epoch = data.slot / store.slots_per_epoch();
        if epoch < parameters.fork_epoch {
            return Err(PayloadAttestationGossipError::Reject(
                "payload attestation's slot is pre-gloas",
            ))
        }
        let key = (data.slot, message.validator_index);
        if self.validators.contains(&key) {
            return Err(PayloadAttestationGossipError::Ignore(
                "already seen payload attestation from this validator",
            ))
        }
        let start = data
            .slot
            .checked_mul(store.slot_duration_ms())
            .and_then(|time| store.genesis_time_ms().checked_add(time))
            .ok_or(PayloadAttestationGossipError::Overflow)?;
        let end = start
            .checked_add(store.slot_duration_ms())
            .and_then(|time| time.checked_add(500))
            .ok_or(PayloadAttestationGossipError::Overflow)?;
        if current_time_ms.checked_add(500).ok_or(PayloadAttestationGossipError::Overflow)? < start ||
            current_time_ms > end
        {
            return Err(PayloadAttestationGossipError::Ignore(
                "payload attestation is not for the current slot",
            ))
        }
        let block = store.block(data.beacon_block_root);
        if block.is_none() && !self.observed.contains_key(&data.beacon_block_root) {
            return Err(PayloadAttestationGossipError::Ignore(
                "payload attestation's block has not been seen",
            ))
        }
        if store.read_block_state(data.beacon_block_root)?.is_none() {
            return Err(PayloadAttestationGossipError::Reject(
                "payload attestation's block failed validation",
            ))
        }
        if block.ok_or(StoreError::UnknownRoot)?.slot != data.slot {
            return Err(PayloadAttestationGossipError::Ignore(
                "payload attestation's block is not at the assigned slot",
            ))
        }
        let head = store.get_head()?;
        let state = store.read_block_state(head.root)?.ok_or(StoreError::UnknownRoot)?;
        let index = usize::try_from(message.validator_index)
            .map_err(|_| PayloadAttestationGossipError::Reject("validator index out of range"))?;
        let validator = state
            .validators
            .values
            .get(index)
            .ok_or(PayloadAttestationGossipError::Reject("validator index out of range"))?;
        if !gloas_processing::get_ptc(&state, data.slot, parameters)?
            .values
            .contains(&message.validator_index)
        {
            return Err(PayloadAttestationGossipError::Reject(
                "validator is not in the payload timeliness committee",
            ))
        }
        let version = if epoch < state.fork.epoch {
            state.fork.previous_version
        } else {
            state.fork.current_version
        };
        let signing_root = compute_signing_root(
            data.hash_tree_root(parameters.preset())?,
            compute_domain([12, 0, 0, 0], version, state.genesis_validators_root),
        );
        verify_signature(&validator.pubkey, &signing_root, &message.signature).map_err(|_| {
            PayloadAttestationGossipError::Reject("invalid payload attestation signature")
        })?;
        if self.validators.len() == self.max_entries {
            return Err(PayloadAttestationGossipError::ResourceLimit)
        }
        if self.context.is_none() {
            self.context = Some(GossipContext::capture(store)?);
        }
        self.validators.insert(key);
        Ok(())
    }
}

#[cfg(feature = "spec-tests")]
pub mod spec_only;

#[cfg(test)]
mod tests;
