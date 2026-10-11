//! Isolated declared Fulu gossip fixture contexts with fixed artificial BLS policy.
//! The private cache cannot become a runtime cache or grant block, execution or DA authority.

#![cfg(feature = "spec-tests")]

use super::{header_size, ColumnGossipError, SeenFuluColumns};
use c_kzg::KzgSettings;
use reth_embedded_beacon_fork_choice::store::spec_only::SpecStore;
use reth_embedded_beacon_primitives::Root;
use reth_embedded_beacon_state_transition::ssz::auxiliary::fulu::{
    DataColumnSidecar, PartialDataColumnGroupID, PartialDataColumnHeader, PartialDataColumnSidecar,
};
use std::sync::Arc;

/// Fixture-only bounded cache accepting only the sealed artificial `SpecStore` graph.
/// It has no conversion, dereference, runtime cache getter or runtime validation token.
#[derive(Clone, Debug)]
pub struct SpecSeenFuluColumns {
    inner: SeenFuluColumns,
}

impl SpecSeenFuluColumns {
    /// Bind exact retention and replay limits without exposing a runtime cache.
    pub fn new(
        max_columns: usize,
        max_headers: usize,
        max_header_bytes: usize,
        max_replay_slots: u64,
    ) -> Result<Self, ColumnGossipError> {
        Ok(Self {
            inner: SeenFuluColumns::new(
                max_columns,
                max_headers,
                max_header_bytes,
                max_replay_slots,
            )?,
        })
    }

    /// Declare a received fixture block without authenticating its post-state.
    pub fn observe_block(&mut self, root: Root) -> Result<(), ColumnGossipError> {
        self.inner.observe_block(root)
    }

    /// Add a bounded declared cached header solely for fixtures exercising cached lookup paths.
    /// The declaration is schema/root checked but intentionally confers no cryptographic authority.
    pub fn fixture_preload_partial_header(
        &mut self,
        root: Root,
        header: PartialDataColumnHeader,
    ) -> Result<(), ColumnGossipError> {
        if header.signed_block_header.message.hash_tree_root() != root {
            return Err(ColumnGossipError::Reject("declared cached header root mismatch"));
        }
        if let Some(existing) = self.inner.headers.get(&root) {
            return if existing.as_ref() == &header {
                Ok(())
            } else {
                Err(ColumnGossipError::Reject("declared cached header differs from prior header"))
            };
        }
        let bytes = header_size(&header)?;
        let total =
            self.inner.header_bytes.checked_add(bytes).ok_or(ColumnGossipError::ResourceLimit)?;
        if self.inner.headers.len() >= self.inner.max_headers || total > self.inner.max_header_bytes
        {
            return Err(ColumnGossipError::ResourceLimit);
        }
        self.inner.headers.insert(root, Arc::new(header));
        self.inner.header_bytes = total;
        Ok(())
    }

    /// Validate the exact complete-column rules with fixed fixture BLS policy and actual KZG.
    pub fn validate_complete(
        &mut self,
        store: &SpecStore,
        sidecar: &DataColumnSidecar,
        current_time_ms: u64,
        subnet_id: u64,
        settings: &KzgSettings,
    ) -> Result<(), ColumnGossipError> {
        self.inner.validate_complete_inner(store, sidecar, current_time_ms, subnet_id, settings)
    }

    /// Validate the exact partial-column rules with fixed fixture BLS policy and actual KZG.
    pub fn validate_partial(
        &mut self,
        store: &SpecStore,
        sidecar: &PartialDataColumnSidecar,
        current_time_ms: u64,
        group: &PartialDataColumnGroupID,
        column_index: u64,
        settings: &KzgSettings,
    ) -> Result<(), ColumnGossipError> {
        self.inner.validate_partial_inner(
            store,
            sidecar,
            current_time_ms,
            group,
            column_index,
            settings,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reth_embedded_beacon_state_transition::ssz::{Preset, Ssz};

    #[test]
    fn declared_headers_are_root_checked_and_bounded_atomically() {
        let mut seen = SpecSeenFuluColumns::new(2, 1, 1_048_576, 8).unwrap();
        let header = PartialDataColumnHeader::default_value(Preset::Minimal).unwrap();
        let root = header.signed_block_header.message.hash_tree_root();
        assert_eq!(
            seen.fixture_preload_partial_header([7; 32], header.clone()),
            Err(ColumnGossipError::Reject("declared cached header root mismatch")),
        );
        assert!(seen.inner.headers.is_empty());
        assert_eq!(seen.inner.header_bytes, 0);
        seen.fixture_preload_partial_header(root, header.clone()).unwrap();
        let bytes = seen.inner.header_bytes;
        seen.fixture_preload_partial_header(root, header.clone()).unwrap();
        assert_eq!(seen.inner.header_bytes, bytes);
        let mut other = header;
        other.signed_block_header.message.slot = 1;
        let other_root = other.signed_block_header.message.hash_tree_root();
        assert_eq!(
            seen.fixture_preload_partial_header(other_root, other),
            Err(ColumnGossipError::ResourceLimit)
        );
        assert_eq!(seen.inner.headers.len(), 1);
        assert_eq!(seen.inner.header_bytes, bytes);
    }

    #[test]
    fn declared_header_byte_budget_and_observations_grant_no_cached_proof() {
        let mut seen = SpecSeenFuluColumns::new(1, 1, 1, 8).unwrap();
        let header = PartialDataColumnHeader::default_value(Preset::Minimal).unwrap();
        let root = header.signed_block_header.message.hash_tree_root();
        assert_eq!(
            seen.fixture_preload_partial_header(root, header),
            Err(ColumnGossipError::ResourceLimit)
        );
        assert!(seen.inner.headers.is_empty());
        seen.observe_block(root).unwrap();
        assert!(seen.inner.headers.is_empty());
        assert!(seen.inner.columns.is_empty());
        assert_eq!(seen.observe_block([9; 32]), Err(ColumnGossipError::ResourceLimit));
        assert_eq!(seen.inner.observed_blocks.len(), 1);
    }
}
