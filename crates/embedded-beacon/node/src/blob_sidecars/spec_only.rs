//! Real-signature fixture gossip over an isolated declared graph context.
//! This cache cannot expose a runtime cache or accept a BLS-disabled graph view.

use super::{BlobGossipError, SeenBlobSidecars};
use c_kzg::KzgSettings;
use reth_embedded_beacon_fork_choice::store::strict_gossip_fixture::StrictGossipStore;
use reth_embedded_beacon_state_transition::ssz::auxiliary::{deneb, electra};

/// A separately typed fixture cache requiring real signatures and real KZG proofs.
#[derive(Clone, Debug)]
pub struct SpecSeenBlobSidecars {
    inner: SeenBlobSidecars,
}
impl SpecSeenBlobSidecars {
    /// Bind explicit local budgets without initializing any verification override.
    pub fn new(max_entries: usize, max_replay_slots: u64) -> Result<Self, BlobGossipError> {
        Ok(Self { inner: SeenBlobSidecars::new(max_entries, max_replay_slots)? })
    }
    /// Retire duplicate observations without unbinding the cache's chain identity.
    pub fn retain_from_slot(&mut self, slot: u64) {
        self.inner.retain_from_slot(slot);
    }
    /// Run unchanged Deneb production checks against a sealed strict fixture graph.
    pub fn validate_deneb(
        &mut self,
        store: &StrictGossipStore,
        sidecar: &deneb::BlobSidecar,
        time_ms: u64,
        subnet: u64,
        settings: &KzgSettings,
    ) -> Result<(), BlobGossipError> {
        self.inner.validate_deneb_inner(store, sidecar, time_ms, subnet, settings)
    }
    /// Run unchanged Electra production checks against a sealed strict fixture graph.
    pub fn validate_electra(
        &mut self,
        store: &StrictGossipStore,
        sidecar: &electra::BlobSidecar,
        time_ms: u64,
        subnet: u64,
        settings: &KzgSettings,
    ) -> Result<(), BlobGossipError> {
        self.inner.validate_electra_inner(store, sidecar, time_ms, subnet, settings)
    }
}
