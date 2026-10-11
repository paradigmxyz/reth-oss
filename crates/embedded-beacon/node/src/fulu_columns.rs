//! Fulu complete/partial column gossip with exact signatures, ancestry and proposer selection.
//! Proofs run on bounded workers; successful gossip cannot grant block or execution validity.

use super::gloas_columns::ColumnGossipError;
use c_kzg::{KzgSettings, CELLS_PER_EXT_BLOB};
use reth_embedded_beacon_crypto::{compute_domain, compute_signing_root, verify_signature};
use reth_embedded_beacon_data_availability::columns::{
    verify_fulu_column_inclusion_proof, verify_fulu_column_kzg_proofs,
    verify_fulu_column_structure, verify_fulu_partial_column_kzg_proofs,
    verify_fulu_partial_header_inclusion_proof,
};
use reth_embedded_beacon_fork_choice::compact::ReadGossipView;
use reth_embedded_beacon_primitives::{BeaconBlockHeader, Root};
use reth_embedded_beacon_state_transition::{
    fork_processing, fulu_processing,
    ssz::{
        auxiliary::fulu::{
            DataColumnSidecar, PartialDataColumnGroupID, PartialDataColumnHeader,
            PartialDataColumnSidecar,
        },
        phase0::Validator,
        BeaconState, Ssz,
    },
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

#[cfg(feature = "spec-tests")]
pub mod spec_only;

/// Bounded duplicate identities and immutable proof-checked partial headers.
#[derive(Clone, Debug)]
pub struct SeenFuluColumns {
    observed_blocks: BTreeSet<Root>,
    columns: BTreeSet<(u64, u64, u64)>,
    headers: BTreeMap<Root, Arc<PartialDataColumnHeader>>,
    header_bytes: usize,
    max_columns: usize,
    max_headers: usize,
    max_header_bytes: usize,
    max_replay_slots: u64,
}

impl SeenFuluColumns {
    /// Bind local retention and replay budgets before admitting untrusted gossip.
    pub const fn new(
        max_columns: usize,
        max_headers: usize,
        max_header_bytes: usize,
        max_replay_slots: u64,
    ) -> Result<Self, ColumnGossipError> {
        if max_columns == 0 || max_headers == 0 || max_header_bytes == 0 || max_replay_slots == 0 {
            return Err(ColumnGossipError::ResourceLimit);
        }
        Ok(Self {
            observed_blocks: BTreeSet::new(),
            columns: BTreeSet::new(),
            headers: BTreeMap::new(),
            header_bytes: 0,
            max_columns,
            max_headers,
            max_header_bytes,
            max_replay_slots,
        })
    }

    /// Observed blocks grant no authority. A received parent without a strict post-state rejects.
    pub fn observe_block(&mut self, root: Root) -> Result<(), ColumnGossipError> {
        if !self.observed_blocks.contains(&root) && self.observed_blocks.len() == self.max_columns {
            return Err(ColumnGossipError::ResourceLimit);
        }
        self.observed_blocks.insert(root);
        Ok(())
    }

    /// Prune finalized header/duplicate identities and actually retired observed blocks.
    pub fn prune(
        &mut self,
        finalized_slot: u64,
        retained: &BTreeSet<Root>,
    ) -> Result<(), ColumnGossipError> {
        self.columns.retain(|(slot, _, _)| *slot > finalized_slot);
        self.observed_blocks.retain(|root| retained.contains(root));
        self.headers.retain(|_, header| header.signed_block_header.message.slot > finalized_slot);
        self.header_bytes = self.headers.values().try_fold(0usize, |total, header| {
            total.checked_add(header_size(header)?).ok_or(ColumnGossipError::ResourceLimit)
        })?;
        Ok(())
    }

    /// Validate complete columns in normative order, recording only fully accepted messages.
    pub fn validate_complete<V: ReadGossipView>(
        &mut self,
        store: &V,
        sidecar: &DataColumnSidecar,
        current_time_ms: u64,
        subnet_id: u64,
        settings: &KzgSettings,
    ) -> Result<(), ColumnGossipError> {
        self.validate_complete_inner(store, sidecar, current_time_ms, subnet_id, settings)
    }

    fn validate_complete_inner<V: ReadGossipView>(
        &mut self,
        store: &V,
        sidecar: &DataColumnSidecar,
        current_time_ms: u64,
        subnet_id: u64,
        settings: &KzgSettings,
    ) -> Result<(), ColumnGossipError> {
        let header = &sidecar.signed_block_header.message;
        let key = (header.slot, header.proposer_index, sidecar.index);
        if self.columns.contains(&key) {
            return Err(ColumnGossipError::Ignore("duplicate proposer slot and column"));
        }
        let parameters = store
            .transition_parameters()
            .ok_or(ColumnGossipError::Ignore("configured transition schedule unavailable"))?;
        let max_blobs = usize::try_from(
            parameters
                .gloas
                .fulu
                .blob_parameters_at_epoch(header.slot / store.slots_per_epoch())
                .max_blobs_per_block,
        )
        .map_err(|_| ColumnGossipError::ResourceLimit)?;
        verify_fulu_column_structure(sidecar, max_blobs)
            .map_err(|_| ColumnGossipError::Reject("invalid column structure"))?;
        if sidecar.index % CELLS_PER_EXT_BLOB as u64 != subnet_id {
            return Err(ColumnGossipError::Reject("wrong subnet"));
        }
        self.validate_header_prefix(store, &sidecar.signed_block_header, current_time_ms)?;
        verify_fulu_column_inclusion_proof(sidecar, parameters.gloas.preset())
            .map_err(|_| ColumnGossipError::Reject("invalid commitments inclusion proof"))?;
        verify_fulu_column_kzg_proofs(settings, sidecar)
            .map_err(|_| ColumnGossipError::Reject("invalid column KZG proof"))?;
        self.validate_expected_proposer(store, header)?;
        if self.columns.len() == self.max_columns {
            return Err(ColumnGossipError::ResourceLimit);
        }
        self.columns.insert(key);
        Ok(())
    }

    /// A valid new header remains cached even if cells in the same message subsequently fail.
    /// This follows the separate normative header/cell validation stages.
    pub fn validate_partial<V: ReadGossipView>(
        &mut self,
        store: &V,
        sidecar: &PartialDataColumnSidecar,
        current_time_ms: u64,
        group: &PartialDataColumnGroupID,
        column_index: u64,
        settings: &KzgSettings,
    ) -> Result<(), ColumnGossipError> {
        self.validate_partial_inner(store, sidecar, current_time_ms, group, column_index, settings)
    }

    fn validate_partial_inner<V: ReadGossipView>(
        &mut self,
        store: &V,
        sidecar: &PartialDataColumnSidecar,
        current_time_ms: u64,
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
        if sidecar.header.values.len() > 1 ||
            (sidecar.header.values.is_empty() && cells_present == 0)
        {
            return Err(ColumnGossipError::Reject("semantically empty or malformed partial column"));
        }
        if sidecar.partial_column.values.len() != cells_present {
            return Err(ColumnGossipError::Reject("cell count differs from bitmap"));
        }
        if sidecar.kzg_proofs.values.len() != cells_present {
            return Err(ColumnGossipError::Reject("proof count differs from bitmap"));
        }
        if let Some(header) = sidecar.header.values.first() {
            if self
                .headers
                .get(&group.beacon_block_root)
                .is_some_and(|prior| prior.as_ref() != header)
            {
                return Err(ColumnGossipError::Reject(
                    "header differs from previously validated header",
                ));
            }
            if header.signed_block_header.message.hash_tree_root() != group.beacon_block_root {
                return Err(ColumnGossipError::Reject("header root differs from partial group"));
            }
            if header.kzg_commitments.values.is_empty() {
                return Err(ColumnGossipError::Reject("header commitments are empty"));
            }
            self.validate_header_prefix(store, &header.signed_block_header, current_time_ms)?;
            let parameters = store
                .transition_parameters()
                .ok_or(ColumnGossipError::Ignore("configured transition schedule unavailable"))?;
            verify_fulu_partial_header_inclusion_proof(header, parameters.gloas.preset())
                .map_err(|_| ColumnGossipError::Reject("invalid header inclusion proof"))?;
            self.validate_expected_proposer(store, &header.signed_block_header.message)?;
            if !self.headers.contains_key(&group.beacon_block_root) {
                let bytes = header_size(header)?;
                let total =
                    self.header_bytes.checked_add(bytes).ok_or(ColumnGossipError::ResourceLimit)?;
                if self.headers.len() == self.max_headers || total > self.max_header_bytes {
                    return Err(ColumnGossipError::ResourceLimit);
                }
                self.headers.insert(group.beacon_block_root, Arc::new(header.clone()));
                self.header_bytes = total;
            }
        }
        if cells_present > 0 {
            let header = self
                .headers
                .get(&group.beacon_block_root)
                .ok_or(ColumnGossipError::Ignore("validated partial header unavailable"))?;
            check_time(store, header.signed_block_header.message.slot, current_time_ms)?;
            if sidecar.cells_present_bitmap.bits.len() != header.kzg_commitments.values.len() {
                return Err(ColumnGossipError::Reject("bitmap length differs from commitments"));
            }
            verify_fulu_partial_column_kzg_proofs(
                settings,
                sidecar,
                &header.kzg_commitments.values,
                column_index,
            )
            .map_err(|_| ColumnGossipError::Reject("invalid partial column KZG proof"))?;
        }
        Ok(())
    }

    fn validate_header_prefix<V: ReadGossipView>(
        &self,
        store: &V,
        signed: &reth_embedded_beacon_state_transition::ssz::phase0::SignedBeaconBlockHeader,
        current_time_ms: u64,
    ) -> Result<(), ColumnGossipError> {
        let header = &signed.message;
        check_time(store, header.slot, current_time_ms)?;
        let Some(parent) = store.block(header.parent_root) else {
            return Err(if self.observed_blocks.contains(&header.parent_root) {
                ColumnGossipError::Reject("parent failed validation")
            } else {
                ColumnGossipError::Ignore("unknown parent")
            });
        };
        let _parent_state = store
            .read_block_state(header.parent_root)
            .map_err(ColumnGossipError::from_store)?
            .ok_or(ColumnGossipError::Reject("parent failed validation"))?;
        let head = store.get_head().map_err(ColumnGossipError::from_store)?;
        let state = store
            .read_block_state(head.root)
            .map_err(ColumnGossipError::from_store)?
            .ok_or(ColumnGossipError::Ignore("authoritative head state unavailable"))?;
        let proposer = usize::try_from(header.proposer_index)
            .ok()
            .and_then(|index| validators(&state).get(index))
            .ok_or(ColumnGossipError::Reject("proposer index out of range"))?;
        let epoch = header.slot / store.slots_per_epoch();
        let fork = state.fork_info();
        let version = if epoch < fork.epoch { fork.previous_version } else { fork.current_version };
        let signing_root = compute_signing_root(
            (*header).hash_tree_root(),
            compute_domain([0, 0, 0, 0], version, state.genesis_validators_root()),
        );
        if V::VERIFY_SIGNATURES {
            verify_signature(&proposer.pubkey, &signing_root, &signed.signature)
                .map_err(|_| ColumnGossipError::Reject("invalid proposer signature"))?;
        }
        if header.slot <=
            parent.header().map_err(|_| ColumnGossipError::Reject("invalid parent header"))?.slot
        {
            return Err(ColumnGossipError::Reject("header slot is not higher than parent"));
        }
        let (_, finalized) = store.checkpoints();
        let boundary = finalized
            .epoch
            .checked_mul(store.slots_per_epoch())
            .ok_or(ColumnGossipError::ClockOverflow)?;
        let mut root = header.parent_root;
        for _ in 0..4096 {
            let ancestor = store
                .block(root)
                .ok_or(ColumnGossipError::Ignore("retained ancestry unavailable"))?;
            let ancestor = ancestor
                .header()
                .map_err(|_| ColumnGossipError::Reject("invalid ancestor header"))?;
            if ancestor.slot <= boundary {
                return if root == finalized.root {
                    Ok(())
                } else {
                    Err(ColumnGossipError::Reject("finalized root is not an ancestor"))
                };
            }
            root = ancestor.parent_root;
        }
        Err(ColumnGossipError::ResourceLimit)
    }

    fn validate_expected_proposer<V: ReadGossipView>(
        &self,
        store: &V,
        header: &BeaconBlockHeader,
    ) -> Result<(), ColumnGossipError> {
        let parent = store
            .read_block_state(header.parent_root)
            .map_err(ColumnGossipError::from_store)?
            .ok_or(ColumnGossipError::Reject("parent failed validation"))?;
        if header.slot.saturating_sub(parent.slot()) > self.max_replay_slots {
            return Err(ColumnGossipError::Ignore("bounded proposer replay unavailable"));
        }
        let parameters = store
            .transition_parameters()
            .ok_or(ColumnGossipError::Ignore("configured transition schedule unavailable"))?;
        let mut state = parent.as_ref().clone();
        replay_slots_for_view::<V>(&mut state, header.slot, parameters)?;
        let BeaconState::Fulu(state, _) = state else {
            return Err(ColumnGossipError::Reject("column header does not belong to Fulu"));
        };
        let expected = fulu_processing::get_beacon_proposer_index(&state, &parameters.gloas.fulu)
            .map_err(|_| ColumnGossipError::Ignore("proposer shuffling unavailable"))?;
        if expected != header.proposer_index {
            return Err(ColumnGossipError::Reject("proposer differs from selected proposer"));
        }
        Ok(())
    }
}

fn header_size(header: &PartialDataColumnHeader) -> Result<usize, ColumnGossipError> {
    header
        .heap_bytes()
        .map_err(|_| ColumnGossipError::ResourceLimit)?
        .checked_add(std::mem::size_of::<PartialDataColumnHeader>())
        .and_then(|bytes| bytes.checked_add(2 * std::mem::size_of::<usize>()))
        .ok_or(ColumnGossipError::ResourceLimit)
}

fn check_time<V: ReadGossipView>(
    store: &V,
    slot: u64,
    current_time_ms: u64,
) -> Result<(), ColumnGossipError> {
    let time = store
        .genesis_time_ms()
        .checked_add(
            slot.checked_mul(store.slot_duration_ms()).ok_or(ColumnGossipError::ClockOverflow)?,
        )
        .ok_or(ColumnGossipError::ClockOverflow)?;
    if current_time_ms.checked_add(500).ok_or(ColumnGossipError::ClockOverflow)? < time {
        return Err(ColumnGossipError::Ignore("future slot"));
    }
    let (_, finalized) = store.checkpoints();
    if slot <=
        finalized
            .epoch
            .checked_mul(store.slots_per_epoch())
            .ok_or(ColumnGossipError::ClockOverflow)?
    {
        return Err(ColumnGossipError::Ignore("slot does not exceed finalized boundary"));
    }
    Ok(())
}

fn validators(state: &BeaconState) -> &[Validator] {
    match state {
        BeaconState::Phase0(state, _) => &state.validators.values,
        BeaconState::Altair(state, _) => &state.validators.values,
        BeaconState::Bellatrix(state, _) => &state.validators.values,
        BeaconState::Capella(state, _) => &state.validators.values,
        BeaconState::Deneb(state, _) => &state.validators.values,
        BeaconState::Electra(state, _) => &state.validators.values,
        BeaconState::Fulu(state, _) => &state.validators.values,
        BeaconState::Gloas(state, _) => &state.validators.values,
    }
}

fn replay_slots_for_view<V: ReadGossipView>(
    state: &mut BeaconState,
    slot: u64,
    parameters: &fork_processing::TransitionParameters,
) -> Result<(), ColumnGossipError> {
    let result = if V::VERIFY_SIGNATURES {
        fork_processing::process_slots(state, slot, parameters)
    } else {
        #[cfg(feature = "spec-tests")]
        {
            fork_processing::spec_only::process_slots(state, slot, parameters)
        }
        #[cfg(not(feature = "spec-tests"))]
        {
            return Err(ColumnGossipError::Reject("artificial graph requires spec-tests feature"));
        }
    };
    result.map_err(|_| ColumnGossipError::Ignore("proposer shuffling unavailable"))
}
