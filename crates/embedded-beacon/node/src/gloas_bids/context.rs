//! Immutable successful-gossip context, shared without exposing graph or signature authority.

use reth_embedded_beacon_fork_choice::{compact::ReadGloasGossipView, StoreError, StoreParameters};
use reth_embedded_beacon_primitives::Root;
use reth_embedded_beacon_state_transition::gloas_processing::GloasParameters;

/// Concrete chain domain, clock and complete graph/processing policy for a bounded gossip cache.
#[derive(Clone, Debug)]
pub(crate) struct GossipContext {
    genesis_validators_root: Root,
    genesis_time_ms: u64,
    processing: GloasParameters,
    policy: StoreParameters,
}
impl GossipContext {
    pub(crate) fn capture<V: ReadGloasGossipView>(store: &V) -> Result<Self, StoreError> {
        let head = store.get_head()?;
        let state = store.read_block_state(head.root)?.ok_or(StoreError::UnknownRoot)?;
        Ok(Self {
            genesis_validators_root: state.genesis_validators_root,
            genesis_time_ms: store.genesis_time_ms(),
            processing: store.processing_parameters().clone(),
            policy: store.store_parameters(),
        })
    }
    pub(crate) fn matches<V: ReadGloasGossipView>(&self, store: &V) -> Result<bool, StoreError> {
        if self.processing != *store.processing_parameters() ||
            self.policy != store.store_parameters() ||
            self.genesis_time_ms != store.genesis_time_ms()
        {
            return Ok(false)
        }
        let head = store.get_head()?;
        let state = store.read_block_state(head.root)?.ok_or(StoreError::UnknownRoot)?;
        Ok(self.genesis_validators_root == state.genesis_validators_root)
    }
}
