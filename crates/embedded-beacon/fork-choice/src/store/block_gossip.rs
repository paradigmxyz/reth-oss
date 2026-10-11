//! Strict beacon block propagation across every configured fork.
//! Acceptance records only bounded proposer/slot observations. Signed state import,
//! execution, availability and authoritative graph admission remain separate operations.

use super::{validators, StoreError, StoreParameters};
use crate::{
    compact::{ReadGloasGossipView, ReadGossipView, StateAccess},
    gossip_view::block_link,
    ExecutionStatus,
};
use reth_embedded_beacon_crypto::{compute_domain, compute_signing_root, verify_signature};
use reth_embedded_beacon_primitives::{Fork, Root, SszError};
use reth_embedded_beacon_state_transition::{
    altair_processing, bellatrix_processing, capella_processing, deneb_processing,
    electra_processing,
    fork_processing::{self, TransitionParameters},
    fulu_processing,
    gloas_processing::{self, GloasParameters},
    phase0_processing::{self, ProcessingError},
    ssz::{gloas, phase0::ForkInfo, BeaconState, Preset, SignedBeaconBlock, Ssz},
};
use std::collections::BTreeSet;

/// Chain-bound finite duplicate observations; no entry can authorize a block or payload import.
#[derive(Clone, PartialEq, Eq)]
pub struct SeenBeaconBlocks {
    proposer_slots: BTreeSet<(u64, u64)>,
    maximum_entries: usize,
    genesis_validators_root: Root,
    genesis_time_ms: u64,
    store_parameters: StoreParameters,
    policy: Policy,
}

impl SeenBeaconBlocks {
    /// Bind observations to the exact retained classic graph and trusted network policy.
    pub fn new<V: ReadGossipView>(
        view: &V,
        maximum_entries: usize,
    ) -> Result<Self, GossipBlockError> {
        check_seen_limit(maximum_entries)?;
        let (head, parameters) = classic_context(view)?;
        Ok(Self {
            proposer_slots: BTreeSet::new(),
            maximum_entries,
            genesis_validators_root: head.genesis_validators_root(),
            genesis_time_ms: view.genesis_time_ms(),
            store_parameters: view.store_parameters(),
            policy: Policy::Classic(parameters.clone()),
        })
    }
    /// Bind observations to the exact retained Gloas graph and trusted network policy.
    pub fn new_gloas<V: ReadGloasGossipView>(
        view: &V,
        maximum_entries: usize,
    ) -> Result<Self, GossipBlockError> {
        check_seen_limit(maximum_entries)?;
        let head = view.read_block_state(view.get_head()?.root)?.ok_or(StoreError::UnknownRoot)?;
        view.processing_parameters().validate()?;
        Ok(Self {
            proposer_slots: BTreeSet::new(),
            maximum_entries,
            genesis_validators_root: head.genesis_validators_root,
            genesis_time_ms: view.genesis_time_ms(),
            store_parameters: view.store_parameters(),
            policy: Policy::Gloas(view.processing_parameters().clone()),
        })
    }
    /// Current number of bounded observations.
    pub fn entries(&self) -> usize {
        self.proposer_slots.len()
    }
    /// Explicitly prune obsolete slots without changing any consensus graph.
    pub fn prune_before_slot(&mut self, minimum_slot: u64) {
        self.proposer_slots.retain(|(slot, _)| *slot >= minimum_slot);
    }
    fn bind_classic<V: ReadGossipView>(
        &self,
        view: &V,
        head: &BeaconState,
        parameters: &TransitionParameters,
    ) -> Result<(), GossipBlockError> {
        if self.genesis_validators_root != head.genesis_validators_root() ||
            self.genesis_time_ms != view.genesis_time_ms() ||
            self.store_parameters != view.store_parameters() ||
            !matches!(&self.policy, Policy::Classic(bound) if bound == parameters)
        {
            return Err(StoreError::InvalidParameters.into())
        }
        Ok(())
    }
    fn bind_gloas<V: ReadGloasGossipView>(
        &self,
        view: &V,
        head: &gloas::BeaconState,
    ) -> Result<(), GossipBlockError> {
        if self.genesis_validators_root != head.genesis_validators_root ||
            self.genesis_time_ms != view.genesis_time_ms() ||
            self.store_parameters != view.store_parameters() ||
            !matches!(&self.policy, Policy::Gloas(bound) if bound == view.processing_parameters())
        {
            return Err(StoreError::InvalidParameters.into())
        }
        Ok(())
    }
    fn publish(&mut self, key: (u64, u64)) -> Result<(), GossipBlockError> {
        if self.proposer_slots.len() >= self.maximum_entries {
            return Err(GossipBlockError::Capacity)
        }
        self.proposer_slots.insert(key);
        Ok(())
    }
}

impl std::fmt::Debug for SeenBeaconBlocks {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SeenBeaconBlocks")
            .field("entries", &self.entries())
            .field("maximum_entries", &self.maximum_entries)
            .field("genesis_time_ms", &self.genesis_time_ms)
            .field("preset", &self.store_parameters.phase0.preset)
            .finish()
    }
}

/// Candidate outcomes distinguish protocol rejection from missing local infrastructure.
#[derive(Debug, thiserror::Error)]
pub enum GossipBlockError {
    /// Duplicate or unavailable propagation prerequisites.
    #[error("ignore beacon block gossip: {0}")]
    Ignore(&'static str),
    /// A concrete candidate failed an ordered propagation check.
    #[error("reject beacon block gossip: {0}")]
    Reject(&'static str),
    /// Duplicate observation budget is exhausted.
    #[error("beacon block gossip observation capacity exhausted")]
    Capacity,
    /// Local graph, clock, configuration or retention failure.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// Temporary replay failed a state/configuration invariant.
    #[error(transparent)]
    Processing(#[from] ProcessingError),
    /// Complete candidate structural encoding failed.
    #[error(transparent)]
    Ssz(#[from] SszError),
}

#[derive(Clone, PartialEq, Eq)]
enum Policy {
    Classic(TransitionParameters),
    Gloas(GloasParameters),
}

struct Candidate<'a> {
    slot: u64,
    proposer: u64,
    parent: Root,
    signature: &'a [u8; 96],
    preset: Preset,
}

/// Apply the ordered Phase0 through Fulu propagation rules with mandatory real BLS.
/// Signature policy is strict independently of any fixture view's associated policy constant.
pub fn validate<V: ReadGossipView>(
    view: &V,
    seen: &mut SeenBeaconBlocks,
    signed: &SignedBeaconBlock,
    current_time_ms: u64,
) -> Result<(), GossipBlockError> {
    let (head, parameters) = classic_context(view)?;
    seen.bind_classic(view, &head, parameters)?;
    let candidate = candidate(signed);
    let key = (candidate.slot, candidate.proposer);
    prefix(
        seen,
        key,
        view.genesis_time_ms(),
        view.slot_duration_ms(),
        current_time_ms,
        view.checkpoints().1.epoch,
        view.slots_per_epoch(),
    )?;
    let parent = view
        .block(candidate.parent)
        .ok_or(GossipBlockError::Ignore("block's parent has not been seen"))?;
    if signed.schema_fork() == Fork::Gloas || candidate.preset != parameters.preset() {
        return Err(StoreError::InvalidParameters.into())
    }
    let payload_status = view.execution_status(candidate.parent);
    let parent_state = match view.read_block_state(candidate.parent)? {
        Some(state) => state,
        None if signed.schema_fork() < Fork::Bellatrix => {
            return Err(GossipBlockError::Reject("block's parent is invalid"))
        }
        None if signed.schema_fork() == Fork::Bellatrix => {
            let justified =
                view.read_block_state(view.checkpoints().0.root)?.ok_or(StoreError::UnknownRoot)?;
            if execution_enabled(&justified, signed)? {
                return Err(if payload_status == ExecutionStatus::Optimistic {
                    GossipBlockError::Reject(
                        "block's parent is invalid and its payload is optimistic",
                    )
                } else {
                    GossipBlockError::Ignore(
                        "block's parent is invalid and its payload is processed",
                    )
                })
            }
            return Err(GossipBlockError::Reject(
                "block's parent is invalid and execution is not enabled",
            ))
        }
        None => {
            return Err(if payload_status == ExecutionStatus::Optimistic {
                GossipBlockError::Reject("block's parent is invalid and its payload is optimistic")
            } else {
                GossipBlockError::Ignore("block's parent is invalid and its payload is processed")
            })
        }
    };
    if signed.schema_fork() >= Fork::Capella && payload_status == ExecutionStatus::Invalid {
        return Err(GossipBlockError::Ignore("block's parent is valid and its payload is invalid"))
    }
    let proposer = usize::try_from(candidate.proposer)
        .ok()
        .and_then(|index| validators(&head).get(index))
        .ok_or(GossipBlockError::Reject("proposer index out of range"))?;
    check_candidate_bytes(signed.heap_bytes()?)?;
    signature(
        proposer.pubkey,
        head.fork_info(),
        head.genesis_validators_root(),
        candidate.slot,
        view.slots_per_epoch(),
        signed.message_root()?,
        candidate.signature,
    )?;
    if execution_enabled(&head, signed)? {
        let timestamp = payload_timestamp(signed).ok_or(StoreError::InvalidParameters)?;
        if timestamp !=
            head.genesis_time()
                .checked_add(
                    candidate
                        .slot
                        .checked_mul(view.slot_duration_ms() / 1000)
                        .ok_or(StoreError::Overflow)?,
                )
                .ok_or(StoreError::Overflow)?
        {
            return Err(GossipBlockError::Reject("incorrect execution payload timestamp"))
        }
        if signed.schema_fork() == Fork::Bellatrix && payload_status == ExecutionStatus::Invalid {
            return Err(GossipBlockError::Ignore(
                "block's parent is valid and its payload is invalid",
            ))
        }
    }
    if candidate.slot <= block_link(parent).0 {
        return Err(GossipBlockError::Reject("block is not from a higher slot than its parent"))
    }
    let finalized = view.checkpoints().1;
    if view.get_checkpoint_block(candidate.parent, finalized.epoch)? != finalized.root {
        return Err(GossipBlockError::Reject("finalized checkpoint is not an ancestor of block"))
    }
    let limit = match signed.schema_fork() {
        Fork::Deneb => Some(parameters.gloas.fulu.electra.deneb.max_blobs_per_block),
        Fork::Electra => Some(parameters.gloas.fulu.electra.max_blobs_per_block),
        Fork::Fulu => Some(
            parameters
                .gloas
                .fulu
                .blob_parameters_at_epoch(candidate.slot / view.slots_per_epoch())
                .max_blobs_per_block,
        ),
        _ => None,
    };
    if limit.is_some_and(|limit| commitment_count(signed) > limit as usize) {
        return Err(GossipBlockError::Reject("too many blob kzg commitments"))
    }
    let replay = fork_processing::replay_slots(
        &parent_state,
        candidate.slot,
        parameters,
        view.store_parameters().max_tick_slots,
    )?;
    if candidate.proposer != expected_proposer(&replay, parameters)? {
        return Err(GossipBlockError::Reject(
            "block proposer_index does not match expected proposer",
        ))
    }
    seen.publish(key)
}

/// Apply every Gloas operation/request, ancestry, bid and proposer propagation rule with real BLS.
pub fn validate_gloas<V: ReadGloasGossipView>(
    view: &V,
    seen: &mut SeenBeaconBlocks,
    signed: &gloas::SignedBeaconBlock,
    current_time_ms: u64,
) -> Result<(), GossipBlockError> {
    let head = view.read_block_state(view.get_head()?.root)?.ok_or(StoreError::UnknownRoot)?;
    seen.bind_gloas(view, &head)?;
    let block = &signed.message;
    let key = (block.slot, block.proposer_index);
    prefix(
        seen,
        key,
        view.genesis_time_ms(),
        view.slot_duration_ms(),
        current_time_ms,
        view.finalized_checkpoint().epoch,
        view.slots_per_epoch(),
    )?;
    operation_limits(&block.body)?;
    let parent = view
        .block(block.parent_root)
        .ok_or(GossipBlockError::Ignore("block's parent has not been seen"))?;
    let parent_state = view
        .read_block_state(block.parent_root)?
        .ok_or(GossipBlockError::Reject("block's parent is invalid"))?;
    let proposer = usize::try_from(block.proposer_index)
        .ok()
        .and_then(|index| head.validators.values.get(index))
        .ok_or(GossipBlockError::Reject("proposer index out of range"))?;
    check_candidate_bytes(signed.heap_bytes()?)?;
    signature(
        proposer.pubkey,
        head.fork.clone(),
        head.genesis_validators_root,
        block.slot,
        view.slots_per_epoch(),
        block.hash_tree_root(view.processing_parameters().preset())?,
        &signed.signature,
    )?;
    let bid = &block.body.signed_execution_payload_bid.message;
    let parent_full =
        bid.parent_block_hash == parent.body.signed_execution_payload_bid.message.block_hash;
    if parent_full && view.payload(block.parent_root).is_none() {
        return Err(GossipBlockError::Ignore("parent payload is not verified"))
    }
    if block.slot <= parent.slot {
        return Err(GossipBlockError::Reject("block is not from a higher slot than its parent"))
    }
    let finalized = view.finalized_checkpoint();
    let ancestor = view.get_ancestor(
        crate::ForkChoiceNode {
            root: block.parent_root,
            payload_status: Some(crate::PayloadStatus::Pending),
        },
        finalized.epoch.checked_mul(view.slots_per_epoch()).ok_or(StoreError::Overflow)?,
    )?;
    if ancestor.root != finalized.root {
        return Err(GossipBlockError::Reject("finalized checkpoint is not an ancestor of block"))
    }
    if bid.blob_kzg_commitments.values.len() >
        view.processing_parameters()
            .fulu
            .blob_parameters_at_epoch(block.slot / view.slots_per_epoch())
            .max_blobs_per_block as usize
    {
        return Err(GossipBlockError::Reject("too many blob kzg commitments"))
    }
    if bid.parent_block_root != block.parent_root {
        return Err(GossipBlockError::Reject("bid's parent does not equal block's parent"))
    }
    let replay = gloas_processing::replay_slots(
        &parent_state,
        block.slot,
        view.processing_parameters(),
        view.store_parameters().max_tick_slots,
    )?;
    if block.proposer_index !=
        gloas_processing::get_beacon_proposer_index(&replay, view.processing_parameters())?
    {
        return Err(GossipBlockError::Reject(
            "block proposer_index does not match expected proposer",
        ))
    }
    if !parent_full && bid.parent_block_hash != replay.latest_block_hash {
        return Err(GossipBlockError::Reject("bid does not build on the parent's execution head"))
    }
    seen.publish(key)
}

fn check_seen_limit(limit: usize) -> Result<(), GossipBlockError> {
    if limit == 0 || limit > 1_048_576 {
        return Err(StoreError::InvalidParameters.into())
    }
    Ok(())
}
fn classic_context<V: ReadGossipView>(
    view: &V,
) -> Result<(StateAccess<'_, BeaconState>, &TransitionParameters), GossipBlockError> {
    let head = view.read_block_state(view.get_head()?.root)?.ok_or(StoreError::UnknownRoot)?;
    let parameters = view
        .transition_parameters()
        .ok_or_else(|| StoreError::ValidationUnavailable(head.schema_fork()))?;
    parameters.validate()?;
    if view.slots_per_epoch() == 0 ||
        view.slot_duration_ms() == 0 ||
        !view.slot_duration_ms().is_multiple_of(1000)
    {
        return Err(StoreError::InvalidParameters.into())
    }
    Ok((head, parameters))
}
fn prefix(
    seen: &SeenBeaconBlocks,
    key: (u64, u64),
    genesis: u64,
    duration: u64,
    time: u64,
    finalized_epoch: u64,
    slots_per_epoch: u64,
) -> Result<(), GossipBlockError> {
    if seen.proposer_slots.contains(&key) {
        return Err(GossipBlockError::Ignore(
            "block is not the first valid block for this slot and proposer",
        ))
    }
    let start = u128::from(key.0)
        .checked_mul(u128::from(duration))
        .and_then(|value| value.checked_add(u128::from(genesis)))
        .ok_or(StoreError::Overflow)?;
    if start > u128::from(time) + 500 {
        return Err(GossipBlockError::Ignore("block is from a future slot"))
    }
    if key.0 <= finalized_epoch.checked_mul(slots_per_epoch).ok_or(StoreError::Overflow)? {
        return Err(GossipBlockError::Ignore(
            "block is not from a slot greater than the latest finalized slot",
        ))
    }
    Ok(())
}
fn signature(
    pubkey: [u8; 48],
    fork: ForkInfo,
    genesis_root: Root,
    slot: u64,
    slots_per_epoch: u64,
    root: Root,
    signature: &[u8; 96],
) -> Result<(), GossipBlockError> {
    let version = if slot / slots_per_epoch < fork.epoch {
        fork.previous_version
    } else {
        fork.current_version
    };
    let signing_root = compute_signing_root(root, compute_domain([0; 4], version, genesis_root));
    verify_signature(&pubkey, &signing_root, signature)
        .map_err(|_| GossipBlockError::Reject("invalid proposer signature"))?;
    Ok(())
}
fn check_candidate_bytes(bytes: usize) -> Result<(), GossipBlockError> {
    if bytes > 64 * 1024 * 1024 {
        return Err(StoreError::ResourceLimit.into())
    }
    Ok(())
}
const fn candidate(signed: &SignedBeaconBlock) -> Candidate<'_> {
    macro_rules! fields {
        ($block:expr, $preset:expr) => {
            Candidate {
                slot: $block.message.slot,
                proposer: $block.message.proposer_index,
                parent: $block.message.parent_root,
                signature: &$block.signature,
                preset: *$preset,
            }
        };
    }
    match signed {
        SignedBeaconBlock::Phase0(value, preset) => fields!(value, preset),
        SignedBeaconBlock::Altair(value, preset) => fields!(value, preset),
        SignedBeaconBlock::Bellatrix(value, preset) => fields!(value, preset),
        SignedBeaconBlock::Capella(value, preset) => fields!(value, preset),
        SignedBeaconBlock::Deneb(value, preset) => fields!(value, preset),
        SignedBeaconBlock::Electra(value, preset) => fields!(value, preset),
        SignedBeaconBlock::Fulu(value, preset) => fields!(value, preset),
        SignedBeaconBlock::Gloas(value, preset) => fields!(value, preset),
    }
}
fn execution_enabled(
    state: &BeaconState,
    signed: &SignedBeaconBlock,
) -> Result<bool, GossipBlockError> {
    Ok(match signed {
        SignedBeaconBlock::Phase0(_, _) | SignedBeaconBlock::Altair(_, _) => false,
        SignedBeaconBlock::Bellatrix(block, _) => {
            let BeaconState::Bellatrix(state, _) = state else {
                return Err(StoreError::InvalidParameters.into())
            };
            bellatrix_processing::is_execution_enabled(state, &block.message.body.execution_payload)
        }
        _ => true,
    })
}
const fn payload_timestamp(signed: &SignedBeaconBlock) -> Option<u64> {
    match signed {
        SignedBeaconBlock::Bellatrix(block, _) => {
            Some(block.message.body.execution_payload.timestamp)
        }
        SignedBeaconBlock::Capella(block, _) => {
            Some(block.message.body.execution_payload.timestamp)
        }
        SignedBeaconBlock::Deneb(block, _) => Some(block.message.body.execution_payload.timestamp),
        SignedBeaconBlock::Electra(block, _) => {
            Some(block.message.body.execution_payload.timestamp)
        }
        SignedBeaconBlock::Fulu(block, _) => Some(block.message.body.execution_payload.timestamp),
        _ => None,
    }
}
const fn commitment_count(signed: &SignedBeaconBlock) -> usize {
    match signed {
        SignedBeaconBlock::Deneb(block, _) => block.message.body.blob_kzg_commitments.values.len(),
        SignedBeaconBlock::Electra(block, _) => {
            block.message.body.blob_kzg_commitments.values.len()
        }
        SignedBeaconBlock::Fulu(block, _) => block.message.body.blob_kzg_commitments.values.len(),
        _ => 0,
    }
}
fn expected_proposer(
    state: &BeaconState,
    parameters: &TransitionParameters,
) -> Result<u64, ProcessingError> {
    let gloas = &parameters.gloas;
    match state {
        BeaconState::Phase0(state, _) => phase0_processing::get_beacon_proposer_index(
            state,
            &gloas.fulu.electra.deneb.capella.bellatrix.altair.phase0,
        ),
        BeaconState::Altair(state, _) => altair_processing::get_beacon_proposer_index(
            state,
            &gloas.fulu.electra.deneb.capella.bellatrix.altair,
        ),
        BeaconState::Bellatrix(state, _) => bellatrix_processing::get_beacon_proposer_index(
            state,
            &gloas.fulu.electra.deneb.capella.bellatrix,
        ),
        BeaconState::Capella(state, _) => {
            capella_processing::get_beacon_proposer_index(state, &gloas.fulu.electra.deneb.capella)
        }
        BeaconState::Deneb(state, _) => {
            deneb_processing::get_beacon_proposer_index(state, &gloas.fulu.electra.deneb)
        }
        BeaconState::Electra(state, _) => {
            electra_processing::get_beacon_proposer_index(state, &gloas.fulu.electra)
        }
        BeaconState::Fulu(state, _) => {
            fulu_processing::get_beacon_proposer_index(state, &gloas.fulu)
        }
        BeaconState::Gloas(state, _) => gloas_processing::get_beacon_proposer_index(state, gloas),
    }
}
fn operation_limits(body: &gloas::BeaconBlockBody) -> Result<(), GossipBlockError> {
    for (count, limit, reason) in [
        (body.proposer_slashings.values.len(), 16, "too many proposer slashings"),
        (body.attester_slashings.values.len(), 1, "too many attester slashings"),
        (body.attestations.values.len(), 8, "too many attestations"),
        (body.deposits.values.len(), 0, "block must not contain deposits"),
        (body.voluntary_exits.values.len(), 16, "too many voluntary exits"),
        (body.bls_to_execution_changes.values.len(), 16, "too many bls to execution changes"),
        (body.payload_attestations.values.len(), 4, "too many payload attestations"),
        (
            body.parent_execution_requests.withdrawals.values.len(),
            16,
            "too many withdrawal requests",
        ),
        (
            body.parent_execution_requests.consolidations.values.len(),
            2,
            "too many consolidation requests",
        ),
        (
            body.parent_execution_requests.builder_deposits.values.len(),
            64,
            "too many builder deposit requests",
        ),
        (
            body.parent_execution_requests.builder_exits.values.len(),
            16,
            "too many builder exit requests",
        ),
    ] {
        if count > limit {
            return Err(GossipBlockError::Reject(reason))
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
