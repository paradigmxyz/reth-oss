//! Sealed bid/preference fixture cache with no runtime cache extraction or conversion.
//! All admission checks and signatures remain mandatory. Any asserted checkpoint context stays
//! inside the separately typed fixture graph and never grants runtime import or payload authority.

use super::{BidGossipError, SeenBids};
use reth_embedded_beacon_fork_choice::gloas::spec_only::SpecStore;
use reth_embedded_beacon_state_transition::ssz::{
    auxiliary::gloas::SignedProposerPreferences,
    gloas::{BeaconBlock, SignedExecutionPayloadBid, SignedExecutionPayloadEnvelope},
    Preset,
};

/// A separately typed bounded fixture cache, without access to its inner runtime cache.
#[derive(Clone, Debug)]
pub struct SpecSeenBids {
    inner: SeenBids,
}

impl SpecSeenBids {
    /// Set the same independent cache and replay budgets as the runtime implementation.
    pub fn new(max_entries: usize, max_replay_slots: u64) -> Result<Self, BidGossipError> {
        Ok(Self { inner: SeenBids::new(max_entries, max_replay_slots)? })
    }
    /// Record a complete observed block identity without asserting successful transition.
    pub fn observe_block(
        &mut self,
        block: &BeaconBlock,
        preset: Preset,
    ) -> Result<(), BidGossipError> {
        self.inner.observe_block(block, preset)
    }
    /// Cache an envelope only after unchanged real-signature gossip propagation checks.
    /// This never invokes execution/DA admission or issues a runtime FULL token.
    pub fn validate_envelope(
        &mut self,
        store: &SpecStore,
        envelope: &SignedExecutionPayloadEnvelope,
    ) -> Result<(), BidGossipError> {
        self.inner.validate_envelope_with_view(store, envelope)
    }
    /// Run unchanged ordered bid checks against the isolated strict-signature graph.
    pub fn validate_bid(
        &mut self,
        store: &SpecStore,
        bid: &SignedExecutionPayloadBid,
        time_ms: u64,
    ) -> Result<(), BidGossipError> {
        self.inner.validate_bid_with_view(store, bid, time_ms)
    }
    /// Run unchanged proposer lookahead, dependent-root, timing and signature checks.
    pub fn validate_preferences(
        &mut self,
        store: &SpecStore,
        preferences: &SignedProposerPreferences,
        time_ms: u64,
    ) -> Result<(), BidGossipError> {
        self.inner.validate_preferences_with_view(store, preferences, time_ms)
    }
}
