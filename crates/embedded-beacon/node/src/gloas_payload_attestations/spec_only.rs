//! Sealed strict-signature fixture cache; no runtime cache or graph extraction is available.

use super::{PayloadAttestationGossipError, SeenPayloadAttestations};
use reth_embedded_beacon_fork_choice::gloas::spec_only::SpecStore;
use reth_embedded_beacon_state_transition::ssz::{
    auxiliary::gloas::PayloadAttestationMessage, gloas::BeaconBlock, Preset,
};

/// Fixed real-BLS gossip policy over the isolated asserted-context graph.
#[derive(Clone, Debug)]
pub struct SpecSeenPayloadAttestations {
    inner: SeenPayloadAttestations,
}
impl SpecSeenPayloadAttestations {
    /// Apply the unchanged independent retention budgets.
    pub fn new(max_entries: usize) -> Result<Self, PayloadAttestationGossipError> {
        Ok(Self { inner: SeenPayloadAttestations::new(max_entries)? })
    }
    /// Observe root and slot without asserting validation.
    pub fn observe_block(
        &mut self,
        block: &BeaconBlock,
        preset: Preset,
    ) -> Result<(), PayloadAttestationGossipError> {
        self.inner.observe_block(block, preset)
    }
    /// Prune old identities with the same runtime retention behavior.
    pub fn retain_from_slot(&mut self, slot: u64) {
        self.inner.retain_from_slot(slot);
    }
    /// Run unchanged ordered head-state, PTC, clock and real-signature checks.
    pub fn validate_message(
        &mut self,
        store: &SpecStore,
        message: &PayloadAttestationMessage,
        current_time_ms: u64,
    ) -> Result<(), PayloadAttestationGossipError> {
        self.inner.validate_with_view(store, message, current_time_ms)
    }
}
