//! Ordered Gloas complete and partial column gossip validation against actual retained blocks.
//! Accepted columns do not select a head, verify an execution envelope or prove full custody.

use c_kzg::{KzgSettings, CELLS_PER_EXT_BLOB};
use reth_embedded_beacon_data_availability::columns::{
    verify_gloas_column_kzg_proofs, verify_gloas_column_structure,
    verify_gloas_partial_column_kzg_proofs,
};
use reth_embedded_beacon_fork_choice::{
    compact::{ReadGloasGossipView, StateSourceError},
    StoreError,
};
use reth_embedded_beacon_primitives::Root;
use reth_embedded_beacon_state_transition::ssz::auxiliary::gloas::{
    DataColumnSidecar, PartialDataColumnGroupID, PartialDataColumnSidecar,
};
use std::collections::BTreeSet;

/// Bounded observed-block and cryptographically accepted-column identities.
#[derive(Clone, Debug)]
pub struct SeenColumns {
    observed_blocks: BTreeSet<Root>,
    columns: BTreeSet<(Root, u64)>,
    max_observed_blocks: usize,
    max_columns: usize,
}

/// Gossip decisions are separate from local resource and clock failures.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ColumnGossipError {
    /// A harmless duplicate, future slot or still unknown block is not propagated.
    #[error("ignore column gossip: {0}")]
    Ignore(&'static str),
    /// Normative structure, association, subnet or mathematical proof failed.
    #[error("reject column gossip: {0}")]
    Reject(&'static str),
    /// Admission storage is exhausted; this does not prove a peer sent invalid data.
    #[error("column gossip resource limit exceeded")]
    ResourceLimit,
    /// Checked local clock or slot arithmetic cannot represent this operation.
    #[error("column gossip clock overflow")]
    ClockOverflow,
    /// An admitted cold state failed its exact data-only binding or durable lease checks.
    #[error(transparent)]
    StateSource(#[from] StateSourceError),
    /// The checked graph could not produce a required authoritative read.
    #[error("column gossip authoritative graph is unavailable")]
    GraphUnavailable,
}

impl ColumnGossipError {
    pub(crate) fn from_store(error: StoreError) -> Self {
        match error {
            StoreError::CompactState(error) => Self::StateSource(error),
            StoreError::ResourceLimit => Self::ResourceLimit,
            StoreError::Overflow => Self::ClockOverflow,
            _ => Self::GraphUnavailable,
        }
    }
}

impl SeenColumns {
    /// Reserve bounded identity sets; caller retention must prune retired block identities.
    pub const fn new(
        max_observed_blocks: usize,
        max_columns: usize,
    ) -> Result<Self, ColumnGossipError> {
        if max_observed_blocks == 0 || max_columns == 0 {
            return Err(ColumnGossipError::ResourceLimit);
        }
        Ok(Self {
            observed_blocks: BTreeSet::new(),
            columns: BTreeSet::new(),
            max_observed_blocks,
            max_columns,
        })
    }

    /// Record the root of a received beacon block before or after strict import.
    /// Observation grants no validation authority. A received block absent from the strict
    /// store has no validated post-state and cannot authorize a column.
    pub fn observe_block(&mut self, root: Root) -> Result<(), ColumnGossipError> {
        if !self.observed_blocks.contains(&root) &&
            self.observed_blocks.len() == self.max_observed_blocks
        {
            return Err(ColumnGossipError::ResourceLimit);
        }
        self.observed_blocks.insert(root);
        Ok(())
    }

    /// Prune identities using a caller-selected set of actually retained block roots.
    pub fn retain_blocks(&mut self, retained: &BTreeSet<Root>) {
        self.observed_blocks.retain(|root| retained.contains(root));
        self.columns.retain(|(root, _)| retained.contains(root));
    }

    /// Apply complete-sidecar gossip checks in the pinned normative order.
    /// Expensive KZG work must run on the import coordinator's bounded blocking workers.
    pub fn validate_complete<V: ReadGloasGossipView>(
        &mut self,
        store: &V,
        sidecar: &DataColumnSidecar,
        current_time_ms: u64,
        subnet_id: u64,
        settings: &KzgSettings,
    ) -> Result<(), ColumnGossipError> {
        let key = (sidecar.beacon_block_root, sidecar.index);
        if self.columns.contains(&key) {
            return Err(ColumnGossipError::Ignore("duplicate block and column"));
        }
        if sidecar.index % CELLS_PER_EXT_BLOB as u64 != subnet_id {
            return Err(ColumnGossipError::Reject("wrong subnet"));
        }
        let slot_time = store
            .genesis_time_ms()
            .checked_add(
                sidecar
                    .slot
                    .checked_mul(store.slot_duration_ms())
                    .ok_or(ColumnGossipError::ClockOverflow)?,
            )
            .ok_or(ColumnGossipError::ClockOverflow)?;
        if current_time_ms.checked_add(500).ok_or(ColumnGossipError::ClockOverflow)? < slot_time {
            return Err(ColumnGossipError::Ignore("future slot"));
        }
        let Some(block) = store.block(sidecar.beacon_block_root) else {
            return Err(self.unknown_block(sidecar.beacon_block_root));
        };
        let _state = store
            .read_block_state(sidecar.beacon_block_root)
            .map_err(ColumnGossipError::from_store)?
            .ok_or(ColumnGossipError::Reject("block failed validation"))?;
        if sidecar.slot != block.slot {
            return Err(ColumnGossipError::Reject("slot differs from block"));
        }
        let commitments =
            &block.body.signed_execution_payload_bid.message.blob_kzg_commitments.values;
        verify_gloas_column_structure(sidecar, commitments)
            .map_err(|_| ColumnGossipError::Reject("invalid column structure"))?;
        if self.columns.len() == self.max_columns {
            return Err(ColumnGossipError::ResourceLimit);
        }
        verify_gloas_column_kzg_proofs(settings, sidecar, commitments)
            .map_err(|_| ColumnGossipError::Reject("invalid column KZG proof"))?;
        self.columns.insert(key);
        Ok(())
    }

    /// Apply partial-sidecar gossip checks against the actual block's committed blob order.
    /// Partial columns have no duplicate, future-time or subnet check in this helper.
    pub fn validate_partial<V: ReadGloasGossipView>(
        &self,
        store: &V,
        sidecar: &PartialDataColumnSidecar,
        group: &PartialDataColumnGroupID,
        column_index: u64,
        settings: &KzgSettings,
    ) -> Result<(), ColumnGossipError> {
        let cells_present = sidecar
            .cells_present_bitmap
            .bits
            .packed()
            .iter()
            .map(|byte| byte.count_ones() as usize)
            .sum::<usize>();
        if cells_present == 0 {
            return Err(ColumnGossipError::Reject("semantically empty partial column"));
        }
        if sidecar.partial_column.values.len() != cells_present {
            return Err(ColumnGossipError::Reject("cell count differs from bitmap"));
        }
        if sidecar.kzg_proofs.values.len() != cells_present {
            return Err(ColumnGossipError::Reject("proof count differs from bitmap"));
        }
        let Some(block) = store.block(group.beacon_block_root) else {
            return Err(self.unknown_block(group.beacon_block_root));
        };
        let _state = store
            .read_block_state(group.beacon_block_root)
            .map_err(ColumnGossipError::from_store)?
            .ok_or(ColumnGossipError::Reject("block failed validation"))?;
        if group.slot != block.slot {
            return Err(ColumnGossipError::Reject("group slot differs from block"));
        }
        let commitments =
            &block.body.signed_execution_payload_bid.message.blob_kzg_commitments.values;
        if sidecar.cells_present_bitmap.bits.len() != commitments.len() {
            return Err(ColumnGossipError::Reject("bitmap length differs from commitments"));
        }
        verify_gloas_partial_column_kzg_proofs(settings, sidecar, commitments, column_index)
            .map_err(|_| ColumnGossipError::Reject("invalid partial column KZG proof"))
    }

    fn unknown_block(&self, root: Root) -> ColumnGossipError {
        if self.observed_blocks.contains(&root) {
            ColumnGossipError::Reject("block failed validation")
        } else {
            ColumnGossipError::Ignore("unknown block")
        }
    }
}
