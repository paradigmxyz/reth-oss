//! Atomic Altair upgrades, empty-slot replay and epoch processing.
//!
//! Callers must enforce the trusted fork schedule and slot-gap resource limits.
//! These kernels do not enable complete-chain readiness.

use crate::{
    compute_committee, compute_seed, compute_shuffled_index, integer_squareroot,
    phase0_processing::{
        add, is_active, mul, position, sub, Constants, Phase0Parameters, ProcessingError,
        RewardDeltas, BASE_REWARD_FACTOR, DOMAIN_BEACON_ATTESTER, EFFECTIVE_BALANCE_INCREMENT,
        MAX_EFFECTIVE_BALANCE, MAX_SEED_LOOKAHEAD, MIN_EPOCHS_TO_INACTIVITY_PENALTY,
        MIN_SEED_LOOKAHEAD,
    },
    ssz::{
        altair::{BeaconState, SyncCommittee},
        phase0, List, Preset, PresetBits, Ssz, Vector,
    },
};
use reth_embedded_beacon_crypto::aggregate_pubkeys;
use reth_embedded_beacon_primitives::{hash_pair, sha256, Checkpoint, Root};
use std::collections::BTreeMap;

pub(crate) mod block;
mod slot_cache;

#[cfg(feature = "spec-tests")]
pub use block::spec_only;

pub use block::{
    get_attesting_indices, get_beacon_proposer_index, get_indexed_attestation,
    is_valid_indexed_attestation, process_attestation, process_attester_slashing, process_block,
    process_block_header, process_deposit, process_eth1_data, process_operations,
    process_proposer_slashing, process_randao, process_signed_block, process_sync_aggregate,
    process_voluntary_exit, slash_validator, state_transition, verify_block_signature,
};

/// Trusted network configuration required by Altair processing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AltairParameters {
    /// Parameters inherited unchanged from Phase0.
    pub phase0: Phase0Parameters,
    /// Version written into the upgraded state at the scheduled boundary.
    pub fork_version: [u8; 4],
    /// Score added for missing timely target participation.
    pub inactivity_score_bias: u64,
    /// Score recovered per epoch outside an inactivity leak.
    pub inactivity_score_recovery_rate: u64,
}

impl AltairParameters {
    /// Exact pinned preset and its default network configuration.
    pub const fn for_preset(preset: Preset) -> Self {
        Self {
            phase0: Phase0Parameters::for_preset(preset),
            fork_version: match preset {
                Preset::Mainnet => [1, 0, 0, 0],
                Preset::Minimal => [1, 0, 0, 1],
            },
            inactivity_score_bias: 4,
            inactivity_score_recovery_rate: 16,
        }
    }

    /// Reject zero divisors and invalid inherited configuration.
    pub fn validate(&self) -> Result<(), ProcessingError> {
        self.phase0.validate()?;
        if self.inactivity_score_bias == 0 {
            return Err(ProcessingError::InvalidParameters("inactivity score bias must be nonzero"))
        }
        Ok(())
    }

    /// Exact epoch length under the selected preset.
    pub const fn slots_per_epoch(&self) -> u64 {
        self.phase0.slots_per_epoch()
    }
}

const FLAG_WEIGHTS: [u64; 3] = [14, 26, 14];
const WEIGHT_DENOMINATOR: u64 = 64;
const PROPOSER_WEIGHT: u64 = 8;
const DOMAIN_SYNC_COMMITTEE: [u8; 4] = [7, 0, 0, 0];

const fn constants_for_preset(preset: Preset) -> Constants {
    let mut constants = Constants::for_preset(preset);
    constants.proportional_slashing_multiplier = 2;
    constants.inactivity_penalty_quotient = 50_331_648;
    constants.minimum_slashing_penalty_quotient = 64;
    constants
}

const fn sync_committee_size(preset: Preset) -> usize {
    match preset {
        Preset::Mainnet => 512,
        Preset::Minimal => 32,
    }
}

const fn sync_period(preset: Preset) -> u64 {
    match preset {
        Preset::Mainnet => 256,
        Preset::Minimal => 8,
    }
}

fn checked_constants(
    state: &BeaconState,
    parameters: &AltairParameters,
) -> Result<Constants, ProcessingError> {
    parameters.validate()?;
    state.hash_tree_root(parameters.phase0.preset)?;
    let count = state.validators.values.len();
    if [
        state.balances.values.len(),
        state.previous_epoch_participation.values.len(),
        state.current_epoch_participation.values.len(),
        state.inactivity_scores.values.len(),
    ]
    .iter()
    .any(|&len| len != count)
    {
        return Err(ProcessingError::InvalidState("Altair validator-indexed list lengths differ"))
    }
    Ok(constants_for_preset(parameters.phase0.preset))
}

fn transact<F>(
    state: &mut BeaconState,
    parameters: &AltairParameters,
    operation: F,
) -> Result<(), ProcessingError>
where
    F: FnOnce(&mut BeaconState, &Constants) -> Result<(), ProcessingError>,
{
    let constants = checked_constants(state, parameters)?;
    let mut candidate = state.clone();
    operation(&mut candidate, &constants)?;
    checked_constants(&candidate, parameters)?;
    *state = candidate;
    Ok(())
}

const fn epoch(state: &BeaconState, constants: &Constants) -> u64 {
    state.slot / constants.slots_per_epoch
}

fn active_indices(state: &BeaconState, epoch: u64) -> Vec<u64> {
    state
        .validators
        .values
        .iter()
        .enumerate()
        .filter_map(|(index, validator)| is_active(validator, epoch).then_some(index as u64))
        .collect()
}

fn total_balance(state: &BeaconState, indices: &[bool]) -> Result<u64, ProcessingError> {
    state
        .validators
        .values
        .iter()
        .zip(indices)
        .filter(|(_, present)| **present)
        .try_fold(0u64, |sum, (validator, _)| {
            add(sum, validator.effective_balance, "total effective balance")
        })
        .map(|balance| balance.max(EFFECTIVE_BALANCE_INCREMENT))
}

pub(crate) fn active_balance(
    state: &BeaconState,
    constants: &Constants,
) -> Result<u64, ProcessingError> {
    let current = epoch(state, constants);
    state
        .validators
        .values
        .iter()
        .filter(|validator| is_active(validator, current))
        .try_fold(0u64, |sum, validator| {
            add(sum, validator.effective_balance, "total active balance")
        })
        .map(|balance| balance.max(EFFECTIVE_BALANCE_INCREMENT))
}

pub(crate) fn block_root_at_slot(
    state: &BeaconState,
    slot: u64,
    constants: &Constants,
) -> Result<Root, ProcessingError> {
    if slot >= state.slot || state.slot - slot > constants.historical_slots {
        return Err(ProcessingError::InvalidState("block root is outside historical slot window"))
    }
    state
        .block_roots
        .values
        .get(position(slot, constants.historical_slots)?)
        .copied()
        .ok_or(ProcessingError::InvalidState("block-root ring is truncated"))
}

pub(crate) fn block_root_at_epoch(
    state: &BeaconState,
    epoch: u64,
    constants: &Constants,
) -> Result<Root, ProcessingError> {
    block_root_at_slot(state, mul(epoch, constants.slots_per_epoch, "epoch start slot")?, constants)
}

pub(crate) fn committee(
    state: &BeaconState,
    slot: u64,
    index: u64,
    constants: &Constants,
) -> Result<Vec<u64>, ProcessingError> {
    let epoch = slot / constants.slots_per_epoch;
    let indices = active_indices(state, epoch);
    let committees = ((indices.len() as u64 / constants.slots_per_epoch) /
        constants.target_committee_size)
        .clamp(1, constants.maximum_committees);
    if index >= committees {
        return Err(ProcessingError::InvalidState("attestation committee index is out of range"))
    }
    let mix_epoch = sub(
        add(epoch, constants.historical_epochs, "seed lookback epoch")?,
        MIN_SEED_LOOKAHEAD + 1,
        "seed lookback epoch",
    )?;
    let mix = state
        .randao_mixes
        .values
        .get(position(mix_epoch, constants.historical_epochs)?)
        .copied()
        .ok_or(ProcessingError::InvalidState("RANDAO ring is truncated"))?;
    let seed = compute_seed(DOMAIN_BEACON_ATTESTER, epoch, mix);
    let committee_index = add(
        mul(slot % constants.slots_per_epoch, committees, "committee index")?,
        index,
        "committee index",
    )?;
    Ok(compute_committee(
        &indices,
        seed,
        committee_index,
        mul(committees, constants.slots_per_epoch, "committee count")?,
        constants.shuffle_rounds,
    )?)
}

fn slot_kernel(state: &mut BeaconState, constants: &Constants) -> Result<(), ProcessingError> {
    let state_root = state.hash_tree_root(constants.preset())?;
    let index = position(state.slot, constants.historical_slots)?;
    *state
        .state_roots
        .values
        .get_mut(index)
        .ok_or(ProcessingError::InvalidState("state-root ring is truncated"))? = state_root;
    if state.latest_block_header.state_root == [0; 32] {
        state.latest_block_header.state_root = state_root;
    }
    let block_root = state.latest_block_header.hash_tree_root();
    *state
        .block_roots
        .values
        .get_mut(index)
        .ok_or(ProcessingError::InvalidState("block-root ring is truncated"))? = block_root;
    Ok(())
}

fn slots_kernel(
    state: &mut BeaconState,
    target_slot: u64,
    parameters: &AltairParameters,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    if state.slot >= target_slot {
        return Err(ProcessingError::SlotNotIncreasing { current: state.slot, target: target_slot })
    }
    let mut cache = slot_cache::SlotCache::new(state, constants)?;
    while state.slot < target_slot {
        cache.process_slot(state)?;
        let next_slot = add(state.slot, 1, "next slot")?;
        if next_slot.is_multiple_of(constants.slots_per_epoch) {
            epoch_kernel(state, parameters, constants)?;
            cache.refresh_epoch(state)?;
        }
        state.slot = next_slot;
        cache.advance_slot(next_slot);
    }
    Ok(())
}

pub(crate) fn churn(
    state: &BeaconState,
    parameters: &AltairParameters,
    constants: &Constants,
) -> u64 {
    parameters.phase0.min_per_epoch_churn_limit.max(
        active_indices(state, epoch(state, constants)).len() as u64 /
            parameters.phase0.churn_limit_quotient,
    )
}

pub(crate) fn activation_exit_epoch(current: u64) -> Result<u64, ProcessingError> {
    add(add(current, 1, "activation/exit epoch")?, MAX_SEED_LOOKAHEAD, "activation/exit lookahead")
}

pub(crate) fn exit_kernel(
    state: &mut BeaconState,
    index: usize,
    parameters: &AltairParameters,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    let validator = state
        .validators
        .values
        .get(index)
        .ok_or(ProcessingError::InvalidState("exit validator index is out of range"))?;
    if validator.exit_epoch != u64::MAX {
        return Ok(())
    }
    let mut queue = activation_exit_epoch(epoch(state, constants))?;
    for validator in &state.validators.values {
        if validator.exit_epoch != u64::MAX {
            queue = queue.max(validator.exit_epoch);
        }
    }
    let queue_count =
        state.validators.values.iter().filter(|validator| validator.exit_epoch == queue).count()
            as u64;
    if queue_count >= churn(state, parameters, constants) {
        queue = add(queue, 1, "exit queue churn")?;
    }
    let withdrawable =
        add(queue, parameters.phase0.min_validator_withdrawability_delay, "withdrawability delay")?;
    let validator = &mut state.validators.values[index];
    validator.exit_epoch = queue;
    validator.withdrawable_epoch = withdrawable;
    Ok(())
}

pub(crate) fn registry_kernel(
    state: &mut BeaconState,
    parameters: &AltairParameters,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    let current = epoch(state, constants);
    for index in 0..state.validators.values.len() {
        let validator = &mut state.validators.values[index];
        if validator.activation_eligibility_epoch == u64::MAX &&
            validator.effective_balance == MAX_EFFECTIVE_BALANCE
        {
            validator.activation_eligibility_epoch =
                add(current, 1, "activation eligibility epoch")?;
        }
        if is_active(validator, current) &&
            validator.effective_balance <= parameters.phase0.ejection_balance
        {
            exit_kernel(state, index, parameters, constants)?;
        }
    }
    let mut queue = state
        .validators
        .values
        .iter()
        .enumerate()
        .filter_map(|(index, validator)| {
            (validator.activation_eligibility_epoch <= state.finalized_checkpoint.epoch &&
                validator.activation_epoch == u64::MAX)
                .then_some(index)
        })
        .collect::<Vec<_>>();
    queue.sort_unstable_by_key(|&index| {
        (state.validators.values[index].activation_eligibility_epoch, index)
    });
    let limit = churn(state, parameters, constants);
    let activation_epoch = activation_exit_epoch(current)?;
    for index in queue.into_iter().take(usize::try_from(limit).unwrap_or(usize::MAX)) {
        state.validators.values[index].activation_epoch = activation_epoch;
    }
    Ok(())
}

pub(crate) fn slashings_kernel(
    state: &mut BeaconState,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    let current = epoch(state, constants);
    let total = active_balance(state, constants)?;
    let slashings = state
        .slashings
        .values
        .iter()
        .try_fold(0u64, |sum, &value| add(sum, value, "total slashings"))?;
    let adjusted =
        mul(slashings, constants.proportional_slashing_multiplier, "adjusted slashings")?
            .min(total);
    let penalized_epoch = add(current, constants.slashing_epochs / 2, "slashing midpoint epoch")?;
    for (index, validator) in state.validators.values.iter().enumerate() {
        if validator.slashed && validator.withdrawable_epoch == penalized_epoch {
            let numerator = mul(
                validator.effective_balance / EFFECTIVE_BALANCE_INCREMENT,
                adjusted,
                "slashing penalty numerator",
            )?;
            let penalty = mul(numerator / total, EFFECTIVE_BALANCE_INCREMENT, "slashing penalty")?;
            state.balances.values[index] = state.balances.values[index].saturating_sub(penalty);
        }
    }
    Ok(())
}

pub(crate) fn eth1_reset_kernel(
    state: &mut BeaconState,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    if add(epoch(state, constants), 1, "next voting epoch")?
        .is_multiple_of(constants.eth1_voting_epochs)
    {
        state.eth1_data_votes.values.clear();
    }
    Ok(())
}

pub(crate) fn effective_balances_kernel(
    state: &mut BeaconState,
    _: &Constants,
) -> Result<(), ProcessingError> {
    let hysteresis_increment = EFFECTIVE_BALANCE_INCREMENT / 4;
    let downward = hysteresis_increment;
    let upward = mul(hysteresis_increment, 5, "upward hysteresis threshold")?;
    for (validator, &balance) in state.validators.values.iter_mut().zip(&state.balances.values) {
        if add(balance, downward, "downward hysteresis comparison")? < validator.effective_balance ||
            add(validator.effective_balance, upward, "upward hysteresis comparison")? < balance
        {
            validator.effective_balance =
                (balance - balance % EFFECTIVE_BALANCE_INCREMENT).min(MAX_EFFECTIVE_BALANCE);
        }
    }
    Ok(())
}

pub(crate) fn slashings_reset_kernel(
    state: &mut BeaconState,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    let next = add(epoch(state, constants), 1, "next slashing epoch")?;
    let index = position(next, constants.slashing_epochs)?;
    *state
        .slashings
        .values
        .get_mut(index)
        .ok_or(ProcessingError::InvalidState("slashings ring is truncated"))? = 0;
    Ok(())
}

pub(crate) fn randao_reset_kernel(
    state: &mut BeaconState,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    let current = epoch(state, constants);
    let next = add(current, 1, "next RANDAO epoch")?;
    let mix = state
        .randao_mixes
        .values
        .get(position(current, constants.historical_epochs)?)
        .copied()
        .ok_or(ProcessingError::InvalidState("RANDAO ring is truncated"))?;
    let index = position(next, constants.historical_epochs)?;
    *state
        .randao_mixes
        .values
        .get_mut(index)
        .ok_or(ProcessingError::InvalidState("RANDAO ring is truncated"))? = mix;
    Ok(())
}

pub(crate) fn historical_roots_kernel(
    state: &mut BeaconState,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    let next = add(epoch(state, constants), 1, "next historical epoch")?;
    if next.is_multiple_of(constants.historical_slots / constants.slots_per_epoch) {
        if state.historical_roots.values.len() >= 16_777_216 {
            return Err(ProcessingError::InvalidState("historical roots limit reached"))
        }
        let root = hash_pair(
            state.block_roots.hash_tree_root(constants.preset())?,
            state.state_roots.hash_tree_root(constants.preset())?,
        );
        state.historical_roots.values.push(root);
    }
    Ok(())
}

/// Upgrade an authenticated Phase0 state at the trusted scheduled boundary.
///
/// The caller enforces the fork epoch; this function performs the exact irregular upgrade.
pub fn upgrade_to_altair(
    pre: &phase0::BeaconState,
    parameters: &AltairParameters,
) -> Result<BeaconState, ProcessingError> {
    parameters.validate()?;
    let preset = parameters.phase0.preset;
    pre.hash_tree_root(preset)?;
    if pre.validators.values.len() != pre.balances.values.len() {
        return Err(ProcessingError::InvalidState("validator and balance lengths differ"))
    }
    let count = pre.validators.values.len();
    let empty_committee = SyncCommittee {
        pubkeys: Vector::from_vec(vec![[0; 48]; sync_committee_size(preset)]),
        aggregate_pubkey: [0; 48],
    };
    let mut post = BeaconState {
        genesis_time: pre.genesis_time,
        genesis_validators_root: pre.genesis_validators_root,
        slot: pre.slot,
        fork: phase0::ForkInfo {
            previous_version: pre.fork.current_version,
            current_version: parameters.fork_version,
            epoch: pre.slot / parameters.slots_per_epoch(),
        },
        latest_block_header: pre.latest_block_header,
        block_roots: pre.block_roots.clone(),
        state_roots: pre.state_roots.clone(),
        historical_roots: pre.historical_roots.clone(),
        eth1_data: pre.eth1_data.clone(),
        eth1_data_votes: pre.eth1_data_votes.clone(),
        eth1_deposit_index: pre.eth1_deposit_index,
        validators: pre.validators.clone(),
        balances: pre.balances.clone(),
        randao_mixes: pre.randao_mixes.clone(),
        slashings: pre.slashings.clone(),
        previous_epoch_participation: List::from_vec(vec![0; count]),
        current_epoch_participation: List::from_vec(vec![0; count]),
        justification_bits: PresetBits::from_ssz(&pre.justification_bits.to_ssz(preset)?, preset)?,
        previous_justified_checkpoint: pre.previous_justified_checkpoint,
        current_justified_checkpoint: pre.current_justified_checkpoint,
        finalized_checkpoint: pre.finalized_checkpoint,
        inactivity_scores: List::from_vec(vec![0; count]),
        current_sync_committee: empty_committee.clone(),
        next_sync_committee: empty_committee,
    };
    let constants = checked_constants(&post, parameters)?;
    let mut committees = BTreeMap::new();
    for pending in &pre.previous_epoch_attestations.values {
        let flags = participation_flags(&post, &pending.data, pending.inclusion_delay, &constants)?;
        let key = (pending.data.slot, pending.data.index);
        let committee = match committees.entry(key) {
            std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(committee(&post, key.0, key.1, &constants)?)
            }
        };
        if committee.len() != pending.aggregation_bits.bits.len() {
            return Err(ProcessingError::InvalidState(
                "pending attestation bit length differs from its committee",
            ))
        }
        for (position, &index) in committee.iter().enumerate() {
            if pending
                .aggregation_bits
                .bits
                .get(position)
                .ok_or(ProcessingError::InvalidState("attestation bits are truncated"))?
            {
                let index = usize::try_from(index).map_err(|_| {
                    ProcessingError::InvalidState("validator index does not fit platform")
                })?;
                *post.previous_epoch_participation.values.get_mut(index).ok_or(
                    ProcessingError::InvalidState("attesting validator index is out of range"),
                )? |= flags;
            }
        }
    }
    let committee = next_sync_committee(&post, &constants)?;
    post.current_sync_committee = committee.clone();
    post.next_sync_committee = committee;
    checked_constants(&post, parameters)?;
    Ok(post)
}

/// Cache the complete Altair pre-state root and latest header without advancing the slot.
pub fn process_slot(
    state: &mut BeaconState,
    parameters: &AltairParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, slot_kernel)
}

/// Replay Altair empty slots; callers bound the gap and scheduled fork boundary.
pub fn process_slots(
    state: &mut BeaconState,
    target_slot: u64,
    parameters: &AltairParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, constants| {
        slots_kernel(candidate, target_slot, parameters, constants)
    })
}

/// Apply every ordered Altair epoch kernel atomically.
pub fn process_epoch(
    state: &mut BeaconState,
    parameters: &AltairParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, constants| {
        epoch_kernel(candidate, parameters, constants)
    })
}

/// Compute the next weighted sync committee and its strictly validated aggregate public key.
pub fn get_next_sync_committee(
    state: &BeaconState,
    parameters: &AltairParameters,
) -> Result<SyncCommittee, ProcessingError> {
    let constants = checked_constants(state, parameters)?;
    next_sync_committee(state, &constants)
}

/// Derive the satisfied timely participation flag indexes for an attestation.
pub fn get_attestation_participation_flag_indices(
    state: &BeaconState,
    data: &phase0::AttestationData,
    inclusion_delay: u64,
    parameters: &AltairParameters,
) -> Result<Vec<usize>, ProcessingError> {
    let constants = checked_constants(state, parameters)?;
    let flags = participation_flags(state, data, inclusion_delay, &constants)?;
    Ok((0..3).filter(|index| flags & (1 << index) != 0).collect())
}

/// Compute exact shuffled committee membership from the authenticated Altair state.
pub fn get_beacon_committee(
    state: &BeaconState,
    slot: u64,
    index: u64,
    parameters: &AltairParameters,
) -> Result<Vec<u64>, ProcessingError> {
    let constants = checked_constants(state, parameters)?;
    committee(state, slot, index, &constants)
}

/// Compute one Altair flag reward and penalty component without mutating balances.
pub fn get_flag_index_deltas(
    state: &BeaconState,
    flag_index: usize,
    parameters: &AltairParameters,
) -> Result<RewardDeltas, ProcessingError> {
    let constants = checked_constants(state, parameters)?;
    flag_deltas(state, flag_index, &constants)
}

/// Compute the inactivity-score penalty component without changing state.
pub fn get_inactivity_penalty_deltas(
    state: &BeaconState,
    parameters: &AltairParameters,
) -> Result<RewardDeltas, ProcessingError> {
    let constants = checked_constants(state, parameters)?;
    inactivity_deltas(state, parameters, &constants)
}

fn next_sync_committee(
    state: &BeaconState,
    constants: &Constants,
) -> Result<SyncCommittee, ProcessingError> {
    let next = add(epoch(state, constants), 1, "next sync committee epoch")?;
    let active = active_indices(state, next);
    if active.is_empty() {
        return Err(ProcessingError::InvalidState("sync committee has no active validators"))
    }
    let mix_epoch = sub(
        add(next, constants.historical_epochs, "sync seed lookback epoch")?,
        MIN_SEED_LOOKAHEAD + 1,
        "sync seed lookback epoch",
    )?;
    let mix = *state
        .randao_mixes
        .values
        .get(position(mix_epoch, constants.historical_epochs)?)
        .ok_or(ProcessingError::InvalidState("RANDAO ring is truncated"))?;
    let seed = compute_seed(DOMAIN_SYNC_COMMITTEE, next, mix);
    let mut pubkeys = Vec::with_capacity(sync_committee_size(constants.preset()));
    let mut iteration = 0u64;
    let mut random = [0; 32];
    while pubkeys.len() < sync_committee_size(constants.preset()) {
        let candidate = compute_shuffled_index(
            iteration % (active.len() as u64),
            active.len() as u64,
            seed,
            constants.shuffle_rounds,
        )?;
        let index = usize::try_from(active[candidate as usize])
            .map_err(|_| ProcessingError::InvalidState("validator index does not fit platform"))?;
        if iteration.is_multiple_of(32) {
            let mut bytes = [0; 40];
            bytes[..32].copy_from_slice(&seed);
            bytes[32..].copy_from_slice(&(iteration / 32).to_le_bytes());
            random = sha256(&bytes);
        }
        let validator = &state.validators.values[index];
        if mul(validator.effective_balance, 255, "sync committee effective weight")? >=
            mul(
                MAX_EFFECTIVE_BALANCE,
                u64::from(random[(iteration % 32) as usize]),
                "sync committee random weight",
            )?
        {
            pubkeys.push(validator.pubkey);
        }
        iteration = add(iteration, 1, "sync committee selection iteration")?;
    }
    let aggregate_pubkey = aggregate_pubkeys(&pubkeys)?;
    Ok(SyncCommittee { pubkeys: Vector::from_vec(pubkeys), aggregate_pubkey })
}

fn participation_flags(
    state: &BeaconState,
    data: &phase0::AttestationData,
    inclusion_delay: u64,
    constants: &Constants,
) -> Result<u8, ProcessingError> {
    let current = epoch(state, constants);
    let justified = if data.target.epoch == current {
        state.current_justified_checkpoint
    } else {
        state.previous_justified_checkpoint
    };
    if data.source != justified {
        return Err(ProcessingError::InvalidState("attestation source checkpoint does not match"))
    }
    let target = data.target.root == block_root_at_epoch(state, data.target.epoch, constants)?;
    let head = target && data.beacon_block_root == block_root_at_slot(state, data.slot, constants)?;
    let mut flags = 0;
    if inclusion_delay <= integer_squareroot(constants.slots_per_epoch) {
        flags |= 1;
    }
    if target && inclusion_delay <= constants.slots_per_epoch {
        flags |= 2;
    }
    if head && inclusion_delay == 1 {
        flags |= 4;
    }
    Ok(flags)
}

fn unslashed(
    state: &BeaconState,
    flag: usize,
    target_epoch: u64,
    constants: &Constants,
) -> Result<Vec<bool>, ProcessingError> {
    if flag >= FLAG_WEIGHTS.len() {
        return Err(ProcessingError::InvalidParameters("participation flag index is out of range"))
    }
    let current = epoch(state, constants);
    let participation = if target_epoch == current {
        &state.current_epoch_participation.values
    } else if target_epoch == current.saturating_sub(1) {
        &state.previous_epoch_participation.values
    } else {
        return Err(ProcessingError::InvalidState(
            "participation epoch is outside current or previous epoch",
        ))
    };
    Ok(state
        .validators
        .values
        .iter()
        .zip(participation)
        .map(|(validator, &flags)| {
            is_active(validator, target_epoch) && !validator.slashed && flags & (1 << flag) != 0
        })
        .collect())
}

fn eligible(state: &BeaconState, constants: &Constants) -> Result<Vec<bool>, ProcessingError> {
    let previous = epoch(state, constants).saturating_sub(1);
    let next_previous = add(previous, 1, "eligible previous epoch")?;
    Ok(state
        .validators
        .values
        .iter()
        .map(|validator| {
            is_active(validator, previous) ||
                (validator.slashed && next_previous < validator.withdrawable_epoch)
        })
        .collect())
}

fn inactivity_leak(state: &BeaconState, constants: &Constants) -> Result<bool, ProcessingError> {
    Ok(sub(
        epoch(state, constants).saturating_sub(1),
        state.finalized_checkpoint.epoch,
        "finality delay",
    )? > MIN_EPOCHS_TO_INACTIVITY_PENALTY)
}

fn base_reward_per_increment(
    state: &BeaconState,
    constants: &Constants,
) -> Result<u64, ProcessingError> {
    Ok(mul(EFFECTIVE_BALANCE_INCREMENT, BASE_REWARD_FACTOR, "base reward numerator")? /
        integer_squareroot(active_balance(state, constants)?))
}

pub(crate) fn base_reward(
    state: &BeaconState,
    index: usize,
    constants: &Constants,
) -> Result<u64, ProcessingError> {
    mul(
        state.validators.values[index].effective_balance / EFFECTIVE_BALANCE_INCREMENT,
        base_reward_per_increment(state, constants)?,
        "Altair base reward",
    )
}

pub(crate) fn flag_deltas(
    state: &BeaconState,
    flag: usize,
    constants: &Constants,
) -> Result<RewardDeltas, ProcessingError> {
    crate::rewards::flag_deltas(state, flag, constants)
}

pub(crate) fn inactivity_deltas(
    state: &BeaconState,
    parameters: &AltairParameters,
    constants: &Constants,
) -> Result<RewardDeltas, ProcessingError> {
    crate::rewards::inactivity_deltas(state, parameters, constants)
}

pub(crate) fn inactivity_kernel(
    state: &mut BeaconState,
    parameters: &AltairParameters,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    if epoch(state, constants) == 0 {
        return Ok(())
    }
    let target = unslashed(state, 1, epoch(state, constants) - 1, constants)?;
    let eligible = eligible(state, constants)?;
    let leak = inactivity_leak(state, constants)?;
    for (index, is_eligible) in eligible.into_iter().enumerate() {
        if is_eligible {
            let score = &mut state.inactivity_scores.values[index];
            *score = if target[index] {
                score.saturating_sub(1)
            } else {
                add(*score, parameters.inactivity_score_bias, "inactivity score increase")?
            };
            if !leak {
                *score = score.saturating_sub(parameters.inactivity_score_recovery_rate);
            }
        }
    }
    Ok(())
}

pub(crate) fn rewards_kernel(
    state: &mut BeaconState,
    parameters: &AltairParameters,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    if epoch(state, constants) == 0 {
        return Ok(())
    }
    let deltas = [
        flag_deltas(state, 0, constants)?,
        flag_deltas(state, 1, constants)?,
        flag_deltas(state, 2, constants)?,
        inactivity_deltas(state, parameters, constants)?,
    ];
    for delta in deltas {
        for (index, (&reward, &penalty)) in delta.rewards.iter().zip(&delta.penalties).enumerate() {
            state.balances.values[index] =
                add(state.balances.values[index], reward, "reward balance")?
                    .saturating_sub(penalty);
        }
    }
    Ok(())
}

pub(crate) fn participation_kernel(
    state: &mut BeaconState,
    _: &Constants,
) -> Result<(), ProcessingError> {
    state.previous_epoch_participation = std::mem::replace(
        &mut state.current_epoch_participation,
        List::from_vec(vec![0; state.validators.values.len()]),
    );
    Ok(())
}

pub(crate) fn sync_committee_kernel(
    state: &mut BeaconState,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    if add(epoch(state, constants), 1, "next sync committee epoch")?
        .is_multiple_of(sync_period(constants.preset()))
    {
        state.current_sync_committee = state.next_sync_committee.clone();
        state.next_sync_committee = next_sync_committee(state, constants)?;
    }
    Ok(())
}

pub(crate) fn epoch_kernel(
    state: &mut BeaconState,
    parameters: &AltairParameters,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    justification_kernel(state, constants)?;
    inactivity_kernel(state, parameters, constants)?;
    rewards_kernel(state, parameters, constants)?;
    registry_kernel(state, parameters, constants)?;
    slashings_kernel(state, constants)?;
    eth1_reset_kernel(state, constants)?;
    effective_balances_kernel(state, constants)?;
    slashings_reset_kernel(state, constants)?;
    randao_reset_kernel(state, constants)?;
    historical_roots_kernel(state, constants)?;
    participation_kernel(state, constants)?;
    sync_committee_kernel(state, constants)
}

pub(crate) fn justification_kernel(
    state: &mut BeaconState,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    let current = epoch(state, constants);
    if current <= 1 {
        return Ok(())
    }
    let previous = current - 1;
    let previous_balance = total_balance(state, &unslashed(state, 1, previous, constants)?)?;
    let current_balance = total_balance(state, &unslashed(state, 1, current, constants)?)?;
    let total = active_balance(state, constants)?;
    let old_previous = state.previous_justified_checkpoint;
    let old_current = state.current_justified_checkpoint;
    let bits = state
        .justification_bits
        .bits
        .as_ssz()
        .first()
        .copied()
        .ok_or(ProcessingError::InvalidState("justification bitvector is truncated"))?;
    let mut bits = (bits << 1) & 15;
    state.previous_justified_checkpoint = old_current;
    if mul(previous_balance, 3, "justification participation")? >=
        mul(total, 2, "justification threshold")?
    {
        state.current_justified_checkpoint =
            Checkpoint { epoch: previous, root: block_root_at_epoch(state, previous, constants)? };
        bits |= 2;
    }
    if mul(current_balance, 3, "justification participation")? >=
        mul(total, 2, "justification threshold")?
    {
        state.current_justified_checkpoint =
            Checkpoint { epoch: current, root: block_root_at_epoch(state, current, constants)? };
        bits |= 1;
    }
    if bits & 14 == 14 && add(old_previous.epoch, 3, "previous checkpoint finality")? == current {
        state.finalized_checkpoint = old_previous;
    }
    if bits & 6 == 6 && add(old_previous.epoch, 2, "previous checkpoint finality")? == current {
        state.finalized_checkpoint = old_previous;
    }
    if bits & 7 == 7 && add(old_current.epoch, 2, "current checkpoint finality")? == current {
        state.finalized_checkpoint = old_current;
    }
    if bits & 3 == 3 && add(old_current.epoch, 1, "current checkpoint finality")? == current {
        state.finalized_checkpoint = old_current;
    }
    state.justification_bits = PresetBits::from_ssz(&[bits], constants.preset())?;
    Ok(())
}

/// Apply the exact `process_justification_and_finalization` Altair component atomically.
pub fn process_justification_and_finalization(
    state: &mut BeaconState,
    parameters: &AltairParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, justification_kernel)
}

/// Apply the exact `process_inactivity_updates` Altair component atomically.
pub fn process_inactivity_updates(
    state: &mut BeaconState,
    parameters: &AltairParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, constants| {
        inactivity_kernel(candidate, parameters, constants)
    })
}

/// Apply the exact `process_rewards_and_penalties` Altair component atomically.
pub fn process_rewards_and_penalties(
    state: &mut BeaconState,
    parameters: &AltairParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, constants| {
        rewards_kernel(candidate, parameters, constants)
    })
}

/// Apply the exact `process_registry_updates` Altair component atomically.
pub fn process_registry_updates(
    state: &mut BeaconState,
    parameters: &AltairParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, constants| {
        registry_kernel(candidate, parameters, constants)
    })
}

/// Apply the exact `process_slashings` Altair component atomically.
pub fn process_slashings(
    state: &mut BeaconState,
    parameters: &AltairParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, slashings_kernel)
}

/// Apply the exact `process_eth1_data_reset` Altair component atomically.
pub fn process_eth1_data_reset(
    state: &mut BeaconState,
    parameters: &AltairParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, eth1_reset_kernel)
}

/// Apply the exact `process_effective_balance_updates` Altair component atomically.
pub fn process_effective_balance_updates(
    state: &mut BeaconState,
    parameters: &AltairParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, constants| {
        effective_balances_kernel(candidate, constants)
    })
}

/// Apply the exact `process_slashings_reset` Altair component atomically.
pub fn process_slashings_reset(
    state: &mut BeaconState,
    parameters: &AltairParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, slashings_reset_kernel)
}

/// Apply the exact `process_randao_mixes_reset` Altair component atomically.
pub fn process_randao_mixes_reset(
    state: &mut BeaconState,
    parameters: &AltairParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, randao_reset_kernel)
}

/// Apply the exact `process_historical_roots_update` Altair component atomically.
pub fn process_historical_roots_update(
    state: &mut BeaconState,
    parameters: &AltairParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, constants| {
        historical_roots_kernel(candidate, constants)
    })
}

/// Apply the exact `process_participation_flag_updates` Altair component atomically.
pub fn process_participation_flag_updates(
    state: &mut BeaconState,
    parameters: &AltairParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, participation_kernel)
}

/// Apply the exact `process_sync_committee_updates` Altair component atomically.
pub fn process_sync_committee_updates(
    state: &mut BeaconState,
    parameters: &AltairParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, sync_committee_kernel)
}

pub(crate) fn validate_state(
    state: &BeaconState,
    parameters: &AltairParameters,
) -> Result<(), ProcessingError> {
    checked_constants(state, parameters).map(|_| ())
}

/// Replay into a private temporary candidate without allocating a second complete state copy.
/// Errors may modify this candidate; callers must discard it and never pass retained state.
pub(crate) fn process_slots_in_place(
    state: &mut BeaconState,
    target: u64,
    parameters: &AltairParameters,
) -> Result<(), ProcessingError> {
    let constants = checked_constants(state, parameters)?;
    slots_kernel(state, target, parameters, &constants)?;
    checked_constants(state, parameters)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ssz::{
        altair::{BeaconBlock, BeaconBlockBody, SignedBeaconBlock, SyncAggregate},
        phase0::{Eth1Data, ForkInfo, Validator},
        PresetBits,
    };
    use reth_embedded_beacon_primitives::BeaconBlockHeader;

    fn validator() -> Validator {
        Validator {
            pubkey: [
                0xa4, 0x91, 0xd1, 0xb0, 0xec, 0xd9, 0xbb, 0x91, 0x79, 0x89, 0xf0, 0xe7, 0x4f, 0x0d,
                0xea, 0x04, 0x22, 0xea, 0xc4, 0xa8, 0x73, 0xe5, 0xe2, 0x64, 0x4f, 0x36, 0x8d, 0xff,
                0xb9, 0xa6, 0xe2, 0x0f, 0xd6, 0xe1, 0x0c, 0x1b, 0x77, 0x65, 0x4d, 0x06, 0x7c, 0x06,
                0x18, 0xf6, 0xe5, 0xa7, 0xf7, 0x9a,
            ],
            withdrawal_credentials: [0; 32],
            effective_balance: 32_000_000_000,
            slashed: false,
            activation_eligibility_epoch: 0,
            activation_epoch: 0,
            exit_epoch: u64::MAX,
            withdrawable_epoch: u64::MAX,
        }
    }

    fn pre_state() -> phase0::BeaconState {
        phase0::BeaconState {
            genesis_time: 0,
            genesis_validators_root: [1; 32],
            slot: 0,
            fork: ForkInfo {
                previous_version: [0, 0, 0, 1],
                current_version: [0, 0, 0, 1],
                epoch: 0,
            },
            latest_block_header: BeaconBlockHeader::default(),
            block_roots: Vector::from_vec(vec![[0; 32]; 64]),
            state_roots: Vector::from_vec(vec![[0; 32]; 64]),
            historical_roots: List::default(),
            eth1_data: Eth1Data { deposit_root: [0; 32], deposit_count: 0, block_hash: [0; 32] },
            eth1_data_votes: List::default(),
            eth1_deposit_index: 0,
            validators: List::from_vec(vec![validator(); 32]),
            balances: List::from_vec(vec![32_000_000_000; 32]),
            randao_mixes: Vector::from_vec(vec![[0; 32]; 64]),
            slashings: Vector::from_vec(vec![0; 64]),
            previous_epoch_attestations: List::default(),
            current_epoch_attestations: List::default(),
            justification_bits: crate::ssz::Bits::from_ssz(&[0], Preset::Minimal).unwrap(),
            previous_justified_checkpoint: Checkpoint::default(),
            current_justified_checkpoint: Checkpoint::default(),
            finalized_checkpoint: Checkpoint::default(),
        }
    }

    fn state() -> BeaconState {
        let mut state =
            upgrade_to_altair(&pre_state(), &AltairParameters::for_preset(Preset::Minimal))
                .unwrap();
        state.slot = 7;
        state
    }

    fn body(state: &BeaconState) -> BeaconBlockBody {
        BeaconBlockBody {
            randao_reveal: [0; 96],
            eth1_data: state.eth1_data.clone(),
            graffiti: [0; 32],
            proposer_slashings: List::default(),
            attester_slashings: List::default(),
            attestations: List::default(),
            deposits: List::default(),
            voluntary_exits: List::default(),
            sync_aggregate: SyncAggregate {
                sync_committee_bits: PresetBits::from_ssz(&[0; 4], Preset::Minimal).unwrap(),
                sync_committee_signature: [0; 96],
            },
        }
    }

    #[test]
    fn upgrade_constructs_real_duplicate_committee_and_fork_binding() {
        let parameters = AltairParameters::for_preset(Preset::Minimal);
        let pre = pre_state();
        let post = upgrade_to_altair(&pre, &parameters).unwrap();
        assert_eq!(post.fork.previous_version, pre.fork.current_version);
        assert_eq!(post.fork.current_version, parameters.fork_version);
        assert_eq!(post.fork.epoch, 0);
        assert_eq!(post.current_sync_committee, post.next_sync_committee);
        assert_eq!(post.current_sync_committee.pubkeys.values, vec![validator().pubkey; 32]);
        assert_eq!(
            post.current_sync_committee.aggregate_pubkey,
            aggregate_pubkeys(&vec![validator().pubkey; 32]).unwrap()
        );
        assert_eq!(post.previous_epoch_participation.values, vec![0; 32]);
        assert_eq!(post.inactivity_scores.values, vec![0; 32]);
        let mut invalid = pre.clone();
        invalid.validators.values[0].pubkey = [0; 48];
        assert!(matches!(
            upgrade_to_altair(&invalid, &parameters),
            Err(ProcessingError::Crypto(_))
        ));
        assert_eq!(pre.slot, 0);
    }

    #[test]
    fn inactivity_scores_honor_leak_slashed_participants_and_atomic_overflow() {
        let parameters = AltairParameters::for_preset(Preset::Minimal);
        let mut state = state();
        state.slot = 63;
        state.inactivity_scores.values.fill(2);
        state.previous_epoch_participation.values[0] = 2;
        state.previous_epoch_participation.values[1] = 2;
        state.validators.values[1].slashed = true;
        process_inactivity_updates(&mut state, &parameters).unwrap();
        assert_eq!(state.inactivity_scores.values[0], 1);
        assert_eq!(state.inactivity_scores.values[1], 6);
        state.finalized_checkpoint.epoch = 6;
        process_inactivity_updates(&mut state, &parameters).unwrap();
        assert_eq!(state.inactivity_scores.values, vec![0; 32]);
        state.finalized_checkpoint.epoch = 0;
        state.inactivity_scores.values[1] = u64::MAX;
        let before = state.clone();
        assert!(matches!(
            process_inactivity_updates(&mut state, &parameters),
            Err(ProcessingError::Arithmetic(_))
        ));
        assert_eq!(state, before);
    }

    #[test]
    fn cached_slots_match_complete_schema_roots_across_sync_and_ring_boundaries() {
        let parameters = AltairParameters::for_preset(Preset::Minimal);
        let mut cached = state();
        let mut full = cached.clone();
        let constants = checked_constants(&cached, &parameters).unwrap();
        let mut cache = slot_cache::SlotCache::new(&cached, &constants).unwrap();
        for _ in 0..73 {
            assert_eq!(cache.root().unwrap(), cached.hash_tree_root(Preset::Minimal).unwrap());
            cache.process_slot(&mut cached).unwrap();
            slot_kernel(&mut full, &constants).unwrap();
            let next = cached.slot + 1;
            if next.is_multiple_of(constants.slots_per_epoch) {
                epoch_kernel(&mut cached, &parameters, &constants).unwrap();
                epoch_kernel(&mut full, &parameters, &constants).unwrap();
                cache.refresh_epoch(&cached).unwrap();
            }
            cached.slot = next;
            full.slot = next;
            cache.advance_slot(next);
            assert_eq!(cached, full);
        }
        assert_eq!(cache.root().unwrap(), cached.hash_tree_root(Preset::Minimal).unwrap());
        assert_eq!(cached.historical_roots.values.len(), 1);
    }

    #[test]
    fn nonempty_sync_signature_and_signed_import_are_strict_and_atomic() {
        let parameters = AltairParameters::for_preset(Preset::Minimal);
        let mut state = state();
        let before = state.clone();
        let aggregate = SyncAggregate {
            sync_committee_bits: PresetBits::from_ssz(&[255; 4], Preset::Minimal).unwrap(),
            sync_committee_signature: [0; 96],
        };
        assert!(matches!(
            process_sync_aggregate(&mut state, &aggregate, &parameters),
            Err(ProcessingError::Crypto(_))
        ));
        assert_eq!(state, before);
        let mut advanced = state.clone();
        process_slots(&mut advanced, 8, &parameters).unwrap();
        let block = SignedBeaconBlock {
            message: BeaconBlock {
                slot: 8,
                proposer_index: get_beacon_proposer_index(&advanced, &parameters).unwrap(),
                parent_root: advanced.latest_block_header.hash_tree_root(),
                state_root: [0; 32],
                body: body(&advanced),
            },
            signature: [0; 96],
        };
        assert!(matches!(
            state_transition(&mut state, &block, &parameters),
            Err(ProcessingError::Crypto(_))
        ));
        assert_eq!(state, before);
    }

    #[test]
    fn mismatched_validator_lists_fail_before_mutation() {
        let parameters = AltairParameters::for_preset(Preset::Minimal);
        let mut state = state();
        state.inactivity_scores.values.pop();
        let before = state.clone();
        assert!(matches!(
            process_slot(&mut state, &parameters),
            Err(ProcessingError::InvalidState("Altair validator-indexed list lengths differ"))
        ));
        assert_eq!(state, before);
    }
}
