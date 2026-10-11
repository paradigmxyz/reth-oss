//! Deneb/Electra blob gossip with exact ancestry, signatures, inclusion and KZG proofs.
//! Gossip acceptance records only bounded duplicate observations, never a block/Engine/DA token.

use c_kzg::KzgSettings;
use reth_embedded_beacon_crypto::{compute_domain, compute_signing_root, verify_signature};
use reth_embedded_beacon_data_availability::admission::{verify_blob_batch, AdmissionError};
use reth_embedded_beacon_fork_choice::{compact::ReadGossipView, StoreError, StoreParameters};
use reth_embedded_beacon_primitives::{hash_pair, Fork, Root, SszError};
use reth_embedded_beacon_state_transition::{
    deneb_processing, electra_processing, fork_processing,
    phase0_processing::ProcessingError,
    ssz::{
        auxiliary::{deneb, electra},
        phase0::{SignedBeaconBlockHeader, Validator},
        BeaconState,
    },
};
use std::collections::{BTreeMap, BTreeSet};

#[cfg(feature = "spec-tests")]
pub mod spec_only;

/// Bound accepted-message identities and authenticated parent/slot proposer results to one chain.
#[derive(Clone)]
pub struct SeenBlobSidecars {
    seen: BTreeSet<(u64, u64, u64)>,
    proposers: BTreeMap<(Root, u64), u64>,
    max_entries: usize,
    max_replay_slots: u64,
    context: Option<NetworkContext>,
}

/// Normative propagation outcomes remain distinct from local infrastructure failures.
#[derive(Debug, thiserror::Error)]
pub enum BlobGossipError {
    /// The message may be queued/retried and must not be propagated now.
    #[error("ignore blob sidecar: {0}")]
    Ignore(&'static str),
    /// The message fails a mandatory consensus gossip check.
    #[error("reject blob sidecar: {0}")]
    Reject(&'static str),
    /// A cache is bound to another trusted chain or local validation policy.
    #[error("blob gossip context error: {0}")]
    Context(&'static str),
    /// A local allocation/retention bound cannot be satisfied.
    #[error("blob gossip resource limit")]
    ResourceLimit,
    /// The configured clock cannot represent this event.
    #[error("blob gossip clock overflow")]
    Overflow,
    /// The checked underlying graph is unavailable or inconsistent.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// A complete typed schema is malformed.
    #[error(transparent)]
    Ssz(#[from] SszError),
    /// Trusted transition parameters or the retained state are inconsistent.
    #[error(transparent)]
    Processing(#[from] ProcessingError),
}

#[derive(Clone, Debug)]
struct NetworkContext {
    genesis_validators_root: Root,
    genesis_time_ms: u64,
    slot_duration_ms: u64,
    store_parameters: StoreParameters,
    processing: fork_processing::TransitionParameters,
}

struct BlobParts<'a> {
    index: u64,
    blob: &'a deneb::Blob,
    commitment: &'a [u8; 48],
    proof: &'a [u8; 48],
    signed_header: &'a SignedBeaconBlockHeader,
    inclusion: &'a [Root],
}

impl std::fmt::Debug for SeenBlobSidecars {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SeenBlobSidecars")
            .field("seen", &self.seen.len())
            .field("proposers", &self.proposers.len())
            .field("max_entries", &self.max_entries)
            .field("max_replay_slots", &self.max_replay_slots)
            .field("context_bound", &self.context.is_some())
            .finish()
    }
}

impl SeenBlobSidecars {
    /// Set explicit duplicate and slot-replay bounds before receiving messages.
    pub const fn new(max_entries: usize, max_replay_slots: u64) -> Result<Self, BlobGossipError> {
        if max_entries == 0 || max_entries > 1_048_576 || max_replay_slots == 0 {
            return Err(BlobGossipError::ResourceLimit)
        }
        Ok(Self {
            seen: BTreeSet::new(),
            proposers: BTreeMap::new(),
            max_entries,
            max_replay_slots,
            context: None,
        })
    }

    /// Remove retired identities without permitting the cache to be reused for another chain.
    pub fn retain_from_slot(&mut self, slot: u64) {
        self.seen.retain(|(observed, _, _)| *observed >= slot);
        self.proposers.retain(|(_, observed), _| *observed >= slot);
    }

    /// Validate a canonical Deneb sidecar against the actual strictly imported graph.
    pub fn validate_deneb<V: ReadGossipView>(
        &mut self,
        store: &V,
        sidecar: &deneb::BlobSidecar,
        current_time_ms: u64,
        subnet_id: u64,
        settings: &KzgSettings,
    ) -> Result<(), BlobGossipError> {
        self.validate_deneb_inner(store, sidecar, current_time_ms, subnet_id, settings)
    }

    /// Validate a canonical Electra sidecar with its distinct blob/subnet limits.
    pub fn validate_electra<V: ReadGossipView>(
        &mut self,
        store: &V,
        sidecar: &electra::BlobSidecar,
        current_time_ms: u64,
        subnet_id: u64,
        settings: &KzgSettings,
    ) -> Result<(), BlobGossipError> {
        self.validate_electra_inner(store, sidecar, current_time_ms, subnet_id, settings)
    }

    fn validate_deneb_inner<V: ReadGossipView>(
        &mut self,
        store: &V,
        sidecar: &deneb::BlobSidecar,
        time_ms: u64,
        subnet: u64,
        settings: &KzgSettings,
    ) -> Result<(), BlobGossipError> {
        let parts = BlobParts {
            index: sidecar.index,
            blob: &sidecar.blob,
            commitment: &sidecar.kzg_commitment,
            proof: &sidecar.kzg_proof,
            signed_header: &sidecar.signed_block_header,
            inclusion: &sidecar.kzg_commitment_inclusion_proof.values,
        };
        self.validate_inner(store, parts, Fork::Deneb, time_ms, subnet, settings)
    }

    fn validate_electra_inner<V: ReadGossipView>(
        &mut self,
        store: &V,
        sidecar: &electra::BlobSidecar,
        time_ms: u64,
        subnet: u64,
        settings: &KzgSettings,
    ) -> Result<(), BlobGossipError> {
        let parts = BlobParts {
            index: sidecar.index,
            blob: &sidecar.blob,
            commitment: &sidecar.kzg_commitment,
            proof: &sidecar.kzg_proof,
            signed_header: &sidecar.signed_block_header,
            inclusion: &sidecar.kzg_commitment_inclusion_proof.values,
        };
        self.validate_inner(store, parts, Fork::Electra, time_ms, subnet, settings)
    }

    fn validate_inner<V: ReadGossipView>(
        &mut self,
        store: &V,
        sidecar: BlobParts<'_>,
        fork: Fork,
        current_time_ms: u64,
        subnet_id: u64,
        settings: &KzgSettings,
    ) -> Result<(), BlobGossipError> {
        if !V::VERIFY_SIGNATURES {
            return Err(BlobGossipError::Reject("blob sidecar requires a strict signature view"))
        }
        self.check_context(store)?;
        let header = &sidecar.signed_header.message;
        let key = (header.slot, header.proposer_index, sidecar.index);
        if self.seen.contains(&key) {
            return Err(BlobGossipError::Ignore(
                "already seen blob sidecar from this proposer for this slot and index",
            ))
        }
        let parameters = store.transition_parameters().ok_or(
            ProcessingError::InvalidParameters("trusted transition parameters are unavailable"),
        )?;
        let (max_blobs, subnets) = if fork == Fork::Deneb {
            (parameters.gloas.fulu.electra.deneb.max_blobs_per_block, 6)
        } else {
            (parameters.gloas.fulu.electra.max_blobs_per_block, 9)
        };
        if sidecar.index >= max_blobs {
            return Err(BlobGossipError::Reject("blob index out of range"))
        }
        if sidecar.index % subnets != subnet_id {
            return Err(BlobGossipError::Reject("blob sidecar is for wrong subnet"))
        }
        let start = header
            .slot
            .checked_mul(store.slot_duration_ms())
            .and_then(|time| store.genesis_time_ms().checked_add(time))
            .ok_or(BlobGossipError::Overflow)?;
        if current_time_ms.checked_add(500).ok_or(BlobGossipError::Overflow)? < start {
            return Err(BlobGossipError::Ignore("blob sidecar is from a future slot"))
        }
        let (_, finalized) = store.checkpoints();
        let finalized_slot = finalized
            .epoch
            .checked_mul(store.slots_per_epoch())
            .ok_or(BlobGossipError::Overflow)?;
        if header.slot <= finalized_slot {
            return Err(BlobGossipError::Ignore(
                "blob sidecar is not from a slot greater than the latest finalized slot",
            ))
        }
        let parent = store
            .block(header.parent_root)
            .ok_or(BlobGossipError::Ignore("blob sidecar's parent has not been seen"))?;
        let parent_state = store
            .read_block_state(header.parent_root)?
            .ok_or(BlobGossipError::Reject("blob sidecar's parent failed validation"))?;
        let head = store.get_head()?;
        let state = store.read_block_state(head.root)?.ok_or(StoreError::UnknownRoot)?;
        let proposer = usize::try_from(header.proposer_index)
            .ok()
            .and_then(|index| validators(&state).get(index))
            .ok_or(BlobGossipError::Reject("proposer index out of range"))?;
        let epoch = header.slot / store.slots_per_epoch();
        let state_fork = state.fork_info();
        let version = if epoch < state_fork.epoch {
            state_fork.previous_version
        } else {
            state_fork.current_version
        };
        let signing_root = compute_signing_root(
            header.hash_tree_root(),
            compute_domain([0; 4], version, state.genesis_validators_root()),
        );
        verify_signature(&proposer.pubkey, &signing_root, &sidecar.signed_header.signature)
            .map_err(|_| {
                BlobGossipError::Reject("invalid proposer signature on blob sidecar block header")
            })?;
        if header.slot <= parent.header()?.slot {
            return Err(BlobGossipError::Reject(
                "blob sidecar is not from a higher slot than its parent",
            ))
        }
        if store.get_checkpoint_block(header.parent_root, finalized.epoch)? != finalized.root {
            return Err(BlobGossipError::Reject(
                "finalized checkpoint is not an ancestor of blob sidecar's block",
            ))
        }
        verify_inclusion(&sidecar)?;
        if sidecar.blob.values.len() != c_kzg::BYTES_PER_BLOB {
            return Err(BlobGossipError::Ssz(SszError::InvalidLength {
                expected: c_kzg::BYTES_PER_BLOB,
                actual: sidecar.blob.values.len(),
            }))
        }
        match verify_blob_batch(
            settings,
            std::slice::from_ref(sidecar.commitment),
            std::slice::from_ref(sidecar.blob),
            std::slice::from_ref(sidecar.proof),
        ) {
            Ok(()) => {}
            Err(AdmissionError::ResourceLimit) => return Err(BlobGossipError::ResourceLimit),
            Err(_) => return Err(BlobGossipError::Reject("invalid blob kzg proof")),
        }
        let gap = header
            .slot
            .checked_sub(parent_state.slot())
            .ok_or(BlobGossipError::Reject("retained parent state is ahead of blob header"))?;
        if gap > self.max_replay_slots.min(store.store_parameters().max_tick_slots) {
            return Err(BlobGossipError::Ignore("proposer shuffling unavailable"))
        }
        if parameters.fork_at_slot(header.slot)? != fork {
            return Err(BlobGossipError::Reject(
                "blob sidecar schema disagrees with trusted fork schedule",
            ))
        }
        let replay_key = (header.parent_root, header.slot);
        let expected = if let Some(&proposer) = self.proposers.get(&replay_key) {
            proposer
        } else {
            check_replay_budget(&parent_state, store.store_parameters())?;
            let state = fork_processing::replay_slots(
                &parent_state,
                header.slot,
                parameters,
                self.max_replay_slots.min(store.store_parameters().max_tick_slots),
            )?;
            match (&state, fork) {
                (BeaconState::Deneb(state, _), Fork::Deneb) => {
                    deneb_processing::get_beacon_proposer_index(
                        state,
                        &parameters.gloas.fulu.electra.deneb,
                    )?
                }
                (BeaconState::Electra(state, _), Fork::Electra) => {
                    electra_processing::get_beacon_proposer_index(
                        state,
                        &parameters.gloas.fulu.electra,
                    )?
                }
                _ => {
                    return Err(BlobGossipError::Reject(
                        "blob sidecar schema disagrees with trusted fork schedule",
                    ))
                }
            }
        };
        if header.proposer_index != expected {
            return Err(BlobGossipError::Reject(
                "blob sidecar proposer_index does not match expected proposer",
            ))
        }
        if self.seen.len() >= self.max_entries {
            return Err(BlobGossipError::ResourceLimit)
        }
        if !self.proposers.contains_key(&replay_key) &&
            self.proposers.len() >= self.max_entries.min(store.store_parameters().max_blocks)
        {
            return Err(BlobGossipError::ResourceLimit)
        }
        if self.context.is_none() {
            self.context = Some(NetworkContext::capture(store)?);
        }
        self.proposers.entry(replay_key).or_insert(expected);
        self.seen.insert(key);
        Ok(())
    }

    fn check_context<V: ReadGossipView>(&self, store: &V) -> Result<(), BlobGossipError> {
        if self.context.as_ref().is_some_and(|context| !context.matches_configuration(store)) {
            return Err(BlobGossipError::Context("blob cache belongs to another chain or policy"))
        }
        if let Some(context) = &self.context {
            let head = store.get_head()?;
            let state = store.read_block_state(head.root)?.ok_or(StoreError::UnknownRoot)?;
            if state.genesis_validators_root() != context.genesis_validators_root {
                return Err(BlobGossipError::Context(
                    "blob cache belongs to another chain or policy",
                ))
            }
        }
        Ok(())
    }
}

impl NetworkContext {
    fn capture<V: ReadGossipView>(store: &V) -> Result<Self, BlobGossipError> {
        let head = store.get_head()?;
        let state = store.read_block_state(head.root)?.ok_or(StoreError::UnknownRoot)?;
        Ok(Self {
            genesis_validators_root: state.genesis_validators_root(),
            genesis_time_ms: store.genesis_time_ms(),
            slot_duration_ms: store.slot_duration_ms(),
            store_parameters: store.store_parameters(),
            processing: store
                .transition_parameters()
                .ok_or(ProcessingError::InvalidParameters(
                    "trusted transition parameters are unavailable",
                ))?
                .clone(),
        })
    }
    fn matches_configuration<V: ReadGossipView>(&self, store: &V) -> bool {
        self.genesis_time_ms == store.genesis_time_ms() &&
            self.slot_duration_ms == store.slot_duration_ms() &&
            self.store_parameters == store.store_parameters() &&
            store.transition_parameters() == Some(&self.processing)
    }
}

// The copied parent must fit its actual retained-store budget. The independent hard ceiling
// protects one configured worker while permitting current large mainnet validator registries.
const MAX_REPLAY_BYTES: u64 = 4 * 1024 * 1024 * 1024;
fn check_replay_budget(
    state: &BeaconState,
    policy: StoreParameters,
) -> Result<(), BlobGossipError> {
    let bytes = state
        .heap_bytes()?
        .checked_add(std::mem::size_of_val(state))
        .ok_or(BlobGossipError::ResourceLimit)?;
    let bytes = u64::try_from(bytes).map_err(|_| BlobGossipError::ResourceLimit)?;
    let retained = u64::try_from(policy.max_retained_schema_bytes)
        .map_err(|_| BlobGossipError::ResourceLimit)?;
    if bytes > retained.min(MAX_REPLAY_BYTES) {
        return Err(BlobGossipError::ResourceLimit)
    }
    Ok(())
}

fn verify_inclusion(sidecar: &BlobParts<'_>) -> Result<(), BlobGossipError> {
    if sidecar.inclusion.len() != 17 || sidecar.index >= 4096 {
        return Err(BlobGossipError::Reject("invalid blob sidecar inclusion proof"))
    }
    let mut first = [0; 32];
    first.copy_from_slice(&sidecar.commitment[..32]);
    let mut second = [0; 32];
    second[..16].copy_from_slice(&sidecar.commitment[32..]);
    let mut root = hash_pair(first, second);
    // Twelve list-element levels, one SSZ length mix-in, then body field11 in its four-level tree.
    let mut index = (11u64 << 13) | sidecar.index;
    for sibling in sidecar.inclusion {
        root = if index & 1 == 0 { hash_pair(root, *sibling) } else { hash_pair(*sibling, root) };
        index >>= 1;
    }
    if root != sidecar.signed_header.message.body_root {
        return Err(BlobGossipError::Reject("invalid blob sidecar inclusion proof"))
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

#[cfg(test)]
mod tests;
