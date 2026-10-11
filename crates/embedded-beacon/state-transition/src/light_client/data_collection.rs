//! Bounded light-client serving cache over externally selected canonical blocks.
//!
//! Records retain derived proofs rather than complete states or execution payloads. The
//! caller supplies only committed full-node imports and the actual fork-choice head. This
//! serving cache performs no independent beacon/EL admission or head selection.

use super::{
    full_node, Bootstrap, FinalityUpdate, LightClientError, LightClientParameters,
    OptimisticUpdate, Update,
};
use crate::ssz::{BeaconState, SignedBeaconBlock};
use reth_embedded_beacon_primitives::{Checkpoint, Root};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

/// Hard retained-record and encoded-proof budgets; no unbounded full-state retention.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LightClientDataLimits {
    /// Maximum imported block records, including the anchor; imports fail closed at the cap.
    pub max_blocks: usize,
    /// Maximum sum of canonical encoded bootstrap/update payloads retained once per record.
    /// This is an encoded-data budget; map and allocation overhead are additionally bounded by
    /// `max_blocks`, and canonical collection bounds cap each record's overhead.
    pub max_encoded_bytes: usize,
}
impl Default for LightClientDataLimits {
    fn default() -> Self {
        Self { max_blocks: 256, max_encoded_bytes: 16 * 1024 * 1024 }
    }
}
#[derive(Debug)]
struct Record {
    parent: Root,
    slot: u64,
    finalized: Checkpoint,
    bootstrap: Arc<Bootstrap>,
    update: Option<Arc<Update>>,
}
/// Derived-data cache sharing proof allocations between records and canonical lookup maps.
#[derive(Debug)]
pub struct LightClientDataStore {
    parameters: LightClientParameters,
    limits: LightClientDataLimits,
    encoded_bytes: usize,
    anchor: Root,
    blocks: BTreeMap<Root, Record>,
    head: Root,
    latest_finalized_checkpoint: Checkpoint,
    bootstraps: BTreeMap<Root, Arc<Bootstrap>>,
    best_updates: BTreeMap<u64, Arc<Update>>,
    latest_finality_update: Option<FinalityUpdate>,
    latest_optimistic_update: Option<OptimisticUpdate>,
}
impl LightClientDataStore {
    /// Anchor serving metadata to a complete committed state/block pair.
    /// Full consensus/EL/availability admission must precede this cache operation.
    pub fn new(
        state: &BeaconState,
        block: &SignedBeaconBlock,
        params: &LightClientParameters,
        limits: LightClientDataLimits,
    ) -> Result<Self, LightClientError> {
        if limits.max_blocks == 0 || limits.max_blocks > 4096 || limits.max_encoded_bytes == 0 {
            return Err(LightClientError::Invalid("invalid light-client serving-cache limits"))
        }
        let bootstrap = full_node::create_light_client_bootstrap(state, block, params)?;
        let encoded_bytes = bootstrap.to_ssz(params.preset())?.len();
        if encoded_bytes > limits.max_encoded_bytes {
            return Err(LightClientError::Invalid(
                "light-client serving-cache encoded budget exceeded",
            ))
        }
        let anchor = block.message_root()?;
        let record = Record {
            parent: block.header()?.parent_root,
            slot: state.slot(),
            finalized: full_node::finalized_checkpoint(state),
            bootstrap: Arc::new(bootstrap),
            update: None,
        };
        let mut store = Self {
            parameters: params.clone(),
            limits,
            encoded_bytes,
            anchor,
            blocks: BTreeMap::from([(anchor, record)]),
            head: anchor,
            latest_finalized_checkpoint: full_node::finalized_checkpoint(state),
            bootstraps: BTreeMap::new(),
            best_updates: BTreeMap::new(),
            latest_finality_update: None,
            latest_optimistic_update: None,
        };
        store.on_head(anchor)?;
        Ok(store)
    }
    /// Retain only derived metadata from a complete committed import and its parent snapshot.
    /// Failed derivation, real signature verification, or resource checks retain no record.
    pub fn on_block(
        &mut self,
        state: &BeaconState,
        block: &SignedBeaconBlock,
        attested_state: &BeaconState,
        attested_block: &SignedBeaconBlock,
        finalized_block: Option<&SignedBeaconBlock>,
    ) -> Result<(), LightClientError> {
        let root = block.message_root()?;
        if self.blocks.contains_key(&root) {
            return Ok(())
        }
        if self.blocks.len() >= self.limits.max_blocks {
            return Err(LightClientError::Invalid(
                "light-client serving-cache record budget exceeded",
            ))
        }
        let parent = block.header()?.parent_root;
        let parent_record = self
            .blocks
            .get(&parent)
            .ok_or(LightClientError::Invalid("light-client serving parent unavailable"))?;
        if attested_block.message_root()? != parent || attested_state.slot() != parent_record.slot {
            return Err(LightClientError::Invalid(
                "serving parent snapshot does not match retained record",
            ))
        }
        let bootstrap = full_node::create_light_client_bootstrap(state, block, &self.parameters)?;
        full_node::validate_state_block_link(attested_state, attested_block, &self.parameters)?;
        let update = if full_node::has_sync_committee_participants(block)? {
            Some(full_node::create_serving_update(
                state,
                block,
                attested_state,
                attested_block,
                finalized_block,
                &self.parameters,
            )?)
        } else {
            None
        };
        let mut encoded_bytes = bootstrap.to_ssz(self.parameters.preset())?.len();
        if let Some(update) = &update {
            encoded_bytes = encoded_bytes
                .checked_add(update.to_ssz(self.parameters.preset())?.len())
                .ok_or(LightClientError::Invalid("encoded serving-cache size overflow"))?
        }
        let total = self
            .encoded_bytes
            .checked_add(encoded_bytes)
            .ok_or(LightClientError::Invalid("encoded serving-cache size overflow"))?;
        if total > self.limits.max_encoded_bytes {
            return Err(LightClientError::Invalid(
                "light-client serving-cache encoded budget exceeded",
            ))
        }
        self.blocks.insert(
            root,
            Record {
                parent,
                slot: state.slot(),
                finalized: full_node::finalized_checkpoint(state),
                bootstrap: Arc::new(bootstrap),
                update: update.map(Arc::new),
            },
        );
        self.encoded_bytes = total;
        Ok(())
    }
    /// Rebuild serving choices from the actual selected canonical ancestry, excluding orphans.
    /// The entire selection changes atomically after ancestry/finality/resource checks.
    pub fn on_head(&mut self, head: Root) -> Result<(), LightClientError> {
        let selected = self
            .blocks
            .get(&head)
            .ok_or(LightClientError::Invalid("selected serving head unavailable"))?;
        if selected.finalized.epoch < self.latest_finalized_checkpoint.epoch {
            return Err(LightClientError::Invalid("serving head decreases finalized epoch"))
        }
        let finalized = selected.finalized;
        let old_finalized_slot = if self.latest_finalized_checkpoint.root == [0; 32] {
            self.blocks[&self.anchor].slot
        } else {
            self.blocks
                .get(&self.latest_finalized_checkpoint.root)
                .ok_or(LightClientError::Invalid("previous finalized serving block unavailable"))?
                .slot
        };
        let low_slot = old_finalized_slot.max(self.blocks[&self.anchor].slot);
        let mut path = Vec::new();
        let mut seen = BTreeSet::new();
        let mut cursor = head;
        loop {
            if !seen.insert(cursor) {
                return Err(LightClientError::Invalid("serving ancestry contains a cycle"))
            }
            let record = self
                .blocks
                .get(&cursor)
                .ok_or(LightClientError::Invalid("canonical serving ancestry unavailable"))?;
            path.push(cursor);
            if cursor == self.anchor {
                break
            }
            cursor = record.parent;
        }
        path.reverse();
        if finalized.root != [0; 32] && !seen.contains(&finalized.root) {
            return Err(LightClientError::Invalid(
                "head's finalized root is outside canonical serving ancestry",
            ))
        }
        let finalized_slot = finalized
            .epoch
            .checked_mul(self.parameters.slots_per_epoch())
            .ok_or(LightClientError::Invalid("finalized boundary overflows"))?;
        let mut bootstraps = BTreeMap::new();
        let mut best_updates: BTreeMap<u64, Arc<Update>> = BTreeMap::new();
        let mut latest: Option<Arc<Update>> = None;
        for (position, root) in path.iter().enumerate() {
            let record = &self.blocks[root];
            let slots_per_epoch = self.parameters.slots_per_epoch();
            let next_boundary = record
                .slot
                .checked_add((slots_per_epoch - record.slot % slots_per_epoch) % slots_per_epoch)
                .ok_or(LightClientError::Invalid("serving checkpoint boundary overflows"))?;
            let boundary = record.slot.is_multiple_of(slots_per_epoch) ||
                path.get(position + 1)
                    .is_some_and(|next| self.blocks[next].slot > next_boundary);
            if *root == self.anchor || (record.slot <= finalized_slot && boundary) {
                bootstraps.insert(*root, Arc::clone(&record.bootstrap));
            }
            if let Some(update) = &record.update {
                let attested_period = super::compute_sync_committee_period_at_slot(
                    update.attested_header.beacon().slot,
                    &self.parameters,
                )?;
                let signature_period = super::compute_sync_committee_period_at_slot(
                    update.signature_slot,
                    &self.parameters,
                )?;
                if attested_period == signature_period &&
                    best_updates.get(&attested_period).map_or(Ok(true), |old| {
                        super::is_better_update(update, old, &self.parameters)
                    })?
                {
                    best_updates.insert(attested_period, Arc::clone(update));
                }
                if update.attested_header.beacon().slot >= low_slot &&
                    update.signature_slot > low_slot &&
                    latest.as_ref().is_none_or(|old| {
                        (update.attested_header.beacon().slot, update.signature_slot) >
                            (old.attested_header.beacon().slot, old.signature_slot)
                    })
                {
                    latest = Some(Arc::clone(update));
                }
            }
        }
        let finality =
            latest.as_ref().map(|update| full_node::create_light_client_finality_update(update));
        let optimistic =
            latest.as_ref().map(|update| full_node::create_light_client_optimistic_update(update));
        self.head = head;
        self.latest_finalized_checkpoint = finalized;
        self.bootstraps = bootstraps;
        self.best_updates = best_updates;
        self.latest_finality_update = finality;
        self.latest_optimistic_update = optimistic;
        Ok(())
    }
    /// Selected canonical head identity, never an independent head-selection result.
    pub const fn head(&self) -> Root {
        self.head
    }
    /// Exact checkpoint reported by the selected committed head state.
    pub const fn latest_finalized_checkpoint(&self) -> Checkpoint {
        self.latest_finalized_checkpoint
    }
    /// Finalized boundary/anchor bootstrap retained on canonical ancestry.
    pub fn bootstrap(&self, root: &Root) -> Option<&Bootstrap> {
        self.bootstraps.get(root).map(AsRef::as_ref)
    }
    /// Best canonical same-period update using the pinned ranking rules.
    pub fn best_update(&self, period: u64) -> Option<&Update> {
        self.best_updates.get(&period).map(AsRef::as_ref)
    }
    /// Latest canonical compact finality update, if a participating aggregate exists.
    pub const fn latest_finality_update(&self) -> Option<&FinalityUpdate> {
        self.latest_finality_update.as_ref()
    }
    /// Latest canonical compact optimistic update, if a participating aggregate exists.
    pub const fn latest_optimistic_update(&self) -> Option<&OptimisticUpdate> {
        self.latest_optimistic_update.as_ref()
    }
    /// Canonical encoded proof-data retained exactly once per imported record.
    pub const fn retained_encoded_bytes(&self) -> usize {
        self.encoded_bytes
    }
    /// Number of admitted lightweight records; complete states/payloads are never retained.
    pub fn retained_blocks(&self) -> usize {
        self.blocks.len()
    }
}
