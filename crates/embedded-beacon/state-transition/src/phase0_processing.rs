//! Atomic Phase0 slot and epoch processing from the pinned consensus specification.
//!
//! Signed Phase0 import verifies BLS and the committed post-state root.
//! Fork scheduling, upgrades and later fork transitions remain separate requirements.
//! Callers must select Phase0 correctly and impose input/slot-gap resource limits.

use crate::{
    compute_committee, compute_seed, integer_squareroot,
    ssz::{
        phase0::{BeaconState, PendingAttestation, Validator},
        Bits, Preset, Ssz,
    },
    TransitionError,
};
use reth_embedded_beacon_primitives::{hash_pair, Checkpoint, Root, SszError};
use std::collections::BTreeMap;
use thiserror::Error;

mod block;
pub mod genesis;
pub(crate) mod slot_cache;

#[cfg(feature = "spec-tests")]
pub use block::spec_only;

pub use block::{
    get_attesting_indices, get_beacon_proposer_index, get_indexed_attestation,
    is_valid_indexed_attestation, process_attestation, process_attester_slashing, process_block,
    process_block_header, process_deposit, process_eth1_data, process_operations,
    process_proposer_slashing, process_randao, process_signed_block, process_voluntary_exit,
    slash_validator, state_transition, verify_block_signature,
};

/// Runtime parameters not determined solely by the published SSZ preset.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Phase0Parameters {
    /// Exact published collection and algorithm preset.
    pub preset: Preset,
    /// Active validators at or below this balance enter the exit queue.
    pub ejection_balance: u64,
    /// Minimum validator activations and exits per epoch.
    pub min_per_epoch_churn_limit: u64,
    /// Active validator count divisor for epoch churn.
    pub churn_limit_quotient: u64,
    /// Delay between ordinary exit and withdrawal eligibility.
    pub min_validator_withdrawability_delay: u64,
    /// Genesis fork version used by fork-agnostic deposit signatures.
    pub genesis_fork_version: [u8; 4],
    /// Minimum active epochs before a voluntary exit is permitted.
    pub shard_committee_period: u64,
}

impl Phase0Parameters {
    /// Exact pinned preset and associated default configuration parameters.
    pub const fn for_preset(preset: Preset) -> Self {
        Self {
            preset,
            ejection_balance: 16_000_000_000,
            min_per_epoch_churn_limit: match preset {
                Preset::Mainnet => 4,
                Preset::Minimal => 2,
            },
            churn_limit_quotient: match preset {
                Preset::Mainnet => 65_536,
                Preset::Minimal => 32,
            },
            min_validator_withdrawability_delay: 256,
            genesis_fork_version: match preset {
                Preset::Mainnet => [0, 0, 0, 0],
                Preset::Minimal => [0, 0, 0, 1],
            },
            shard_committee_period: match preset {
                Preset::Mainnet => 256,
                Preset::Minimal => 64,
            },
        }
    }

    /// Reject division by zero and parameters outside the Phase0 domain.
    pub const fn validate(&self) -> Result<(), ProcessingError> {
        if self.ejection_balance > 32_000_000_000 {
            return Err(ProcessingError::InvalidParameters(
                "ejection balance exceeds maximum effective balance",
            ))
        }
        if self.min_per_epoch_churn_limit == 0 || self.churn_limit_quotient == 0 {
            return Err(ProcessingError::InvalidParameters(
                "validator churn parameters must be nonzero",
            ))
        }
        Ok(())
    }

    /// Exact number of slots per epoch under the selected preset.
    pub const fn slots_per_epoch(&self) -> u64 {
        match self.preset {
            Preset::Mainnet => 32,
            Preset::Minimal => 8,
        }
    }
}

/// Failed processing leaves the caller's state unchanged.
#[derive(Debug, Error)]
pub enum ProcessingError {
    /// Invalid runtime parameter configuration.
    #[error("invalid consensus parameters: {0}")]
    InvalidParameters(&'static str),
    /// Invalid state invariant or referenced index.
    #[error("invalid consensus state: {0}")]
    InvalidState(&'static str),
    /// Slot replay must advance strictly beyond the current slot.
    #[error("target slot {target} does not advance current slot {current}")]
    SlotNotIncreasing {
        /// Current pre-state slot.
        current: u64,
        /// Requested target slot.
        target: u64,
    },
    /// Consensus Uint64 arithmetic cannot overflow or underflow.
    #[error("consensus arithmetic overflow or underflow: {0}")]
    Arithmetic(&'static str),
    /// Noncanonical SSZ structure or declared bounds.
    #[error(transparent)]
    Ssz(#[from] SszError),
    /// Invalid deterministic committee operation.
    #[error(transparent)]
    Helper(#[from] TransitionError),
    /// Mandatory BLS verification failed.
    #[error(transparent)]
    Crypto(#[from] reth_embedded_beacon_crypto::CryptoError),
}

/// Per-validator deltas before saturating balance application.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RewardDeltas {
    /// Reward indexed by validator registry position.
    pub rewards: Vec<u64>,
    /// Penalty indexed by validator registry position.
    pub penalties: Vec<u64>,
}

/// Cache the current state and header roots without incrementing the slot.
pub fn process_slot(
    state: &mut BeaconState,
    parameters: &Phase0Parameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, slot_kernel)
}

/// Replay empty slots and Phase0 epochs up to a strictly greater target slot.
///
/// Callers must bound the slot gap and reject crossing a scheduled fork.
pub fn process_slots(
    state: &mut BeaconState,
    target_slot: u64,
    parameters: &Phase0Parameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, constants| {
        slots_kernel(candidate, target_slot, parameters, constants)
    })
}

/// Apply the complete ordered Phase0 epoch kernels atomically.
pub fn process_epoch(
    state: &mut BeaconState,
    parameters: &Phase0Parameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, constants| {
        epoch_kernel(candidate, parameters, constants)
    })
}

/// Update justification bits and finality from target participation.
pub fn process_justification_and_finalization(
    state: &mut BeaconState,
    parameters: &Phase0Parameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, justification_kernel)
}

/// Apply Phase0 attestation rewards and inactivity penalties.
pub fn process_rewards_and_penalties(
    state: &mut BeaconState,
    parameters: &Phase0Parameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, rewards_kernel)
}

/// Process activation eligibility, ejections and the finalized activation queue.
pub fn process_registry_updates(
    state: &mut BeaconState,
    parameters: &Phase0Parameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, constants| {
        registry_kernel(candidate, parameters, constants)
    })
}

/// Apply delayed correlated slashing penalties at the window midpoint.
pub fn process_slashings(
    state: &mut BeaconState,
    parameters: &Phase0Parameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, slashings_kernel)
}

/// Clear execution-data votes at a voting-period boundary.
pub fn process_eth1_data_reset(
    state: &mut BeaconState,
    parameters: &Phase0Parameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, eth1_reset_kernel)
}

/// Update effective balances with the specified hysteresis thresholds.
pub fn process_effective_balance_updates(
    state: &mut BeaconState,
    parameters: &Phase0Parameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, effective_balances_kernel)
}

/// Clear the slashing bucket for the next epoch.
pub fn process_slashings_reset(
    state: &mut BeaconState,
    parameters: &Phase0Parameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, slashings_reset_kernel)
}

/// Carry the current RANDAO mix into the next epoch's ring position.
pub fn process_randao_mixes_reset(
    state: &mut BeaconState,
    parameters: &Phase0Parameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, randao_reset_kernel)
}

/// Append a committed historical root when the root rings wrap.
pub fn process_historical_roots_update(
    state: &mut BeaconState,
    parameters: &Phase0Parameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, historical_roots_kernel)
}

/// Move current epoch pending attestations into the previous epoch record.
pub fn process_participation_record_updates(
    state: &mut BeaconState,
    parameters: &Phase0Parameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, _| {
        candidate.previous_epoch_attestations =
            std::mem::take(&mut candidate.current_epoch_attestations);
        Ok(())
    })
}

/// Compute every Phase0 attestation reward and penalty component.
pub fn get_attestation_deltas(
    state: &BeaconState,
    parameters: &Phase0Parameters,
) -> Result<RewardDeltas, ProcessingError> {
    let constants = checked_constants(state, parameters)?;
    attestation_deltas(state, &constants)
}

/// Compute source participation deltas for the previous epoch.
pub fn get_source_deltas(
    state: &BeaconState,
    parameters: &Phase0Parameters,
) -> Result<RewardDeltas, ProcessingError> {
    component_deltas(state, parameters, Component::Source)
}

/// Compute target participation deltas for the previous epoch.
pub fn get_target_deltas(
    state: &BeaconState,
    parameters: &Phase0Parameters,
) -> Result<RewardDeltas, ProcessingError> {
    component_deltas(state, parameters, Component::Target)
}

/// Compute head participation deltas for the previous epoch.
pub fn get_head_deltas(
    state: &BeaconState,
    parameters: &Phase0Parameters,
) -> Result<RewardDeltas, ProcessingError> {
    component_deltas(state, parameters, Component::Head)
}

/// Compute inclusion-delay and proposer rewards for previous epoch source votes.
pub fn get_inclusion_delay_deltas(
    state: &BeaconState,
    parameters: &Phase0Parameters,
) -> Result<RewardDeltas, ProcessingError> {
    let constants = checked_constants(state, parameters)?;
    let view = Participation::new(state, &constants, false)?;
    inclusion_deltas(state, &view)
}

/// Compute leak penalties with Phase0 proposer reward compensation.
pub fn get_inactivity_penalty_deltas(
    state: &BeaconState,
    parameters: &Phase0Parameters,
) -> Result<RewardDeltas, ProcessingError> {
    let constants = checked_constants(state, parameters)?;
    let view = Participation::new(state, &constants, false)?;
    inactivity_deltas(state, &constants, &view)
}

/// Compute the shuffled committee for an exact Phase0 slot and committee index.
pub fn get_beacon_committee(
    state: &BeaconState,
    slot: u64,
    index: u64,
    parameters: &Phase0Parameters,
) -> Result<Vec<u64>, ProcessingError> {
    let constants = checked_constants(state, parameters)?;
    committee(state, slot, index, &constants)
}

/// Initiate an ordinary validator exit without slashing or signature checks.
pub fn initiate_validator_exit(
    state: &mut BeaconState,
    index: u64,
    parameters: &Phase0Parameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, constants| {
        let index = usize::try_from(index)
            .map_err(|_| ProcessingError::InvalidState("validator index does not fit platform"))?;
        exit_kernel(candidate, index, parameters, constants)
    })
}

#[derive(Clone, Copy)]
pub(crate) struct Constants {
    pub(crate) slots_per_epoch: u64,
    pub(crate) historical_slots: u64,
    pub(crate) historical_epochs: u64,
    pub(crate) slashing_epochs: u64,
    pub(crate) eth1_voting_epochs: u64,
    pub(crate) maximum_committees: u64,
    pub(crate) target_committee_size: u64,
    pub(crate) shuffle_rounds: u8,
    pub(crate) proportional_slashing_multiplier: u64,
    pub(crate) inactivity_penalty_quotient: u64,
    pub(crate) minimum_slashing_penalty_quotient: u64,
}

impl Constants {
    pub(crate) const fn for_preset(preset: Preset) -> Self {
        match preset {
            Preset::Mainnet => Self {
                slots_per_epoch: 32,
                historical_slots: 8192,
                historical_epochs: 65536,
                slashing_epochs: 8192,
                eth1_voting_epochs: 64,
                maximum_committees: 64,
                target_committee_size: 128,
                shuffle_rounds: 90,
                proportional_slashing_multiplier: 1,
                inactivity_penalty_quotient: 67_108_864,
                minimum_slashing_penalty_quotient: 128,
            },
            Preset::Minimal => Self {
                slots_per_epoch: 8,
                historical_slots: 64,
                historical_epochs: 64,
                slashing_epochs: 64,
                eth1_voting_epochs: 4,
                maximum_committees: 4,
                target_committee_size: 4,
                shuffle_rounds: 10,
                proportional_slashing_multiplier: 2,
                inactivity_penalty_quotient: 33_554_432,
                minimum_slashing_penalty_quotient: 64,
            },
        }
    }

    pub(crate) const fn preset(self) -> Preset {
        if self.slots_per_epoch == 32 {
            Preset::Mainnet
        } else {
            Preset::Minimal
        }
    }
}

pub(crate) const EFFECTIVE_BALANCE_INCREMENT: u64 = 1_000_000_000;
pub(crate) const MAX_EFFECTIVE_BALANCE: u64 = 32_000_000_000;
pub(crate) const MIN_EPOCHS_TO_INACTIVITY_PENALTY: u64 = 4;
pub(crate) const BASE_REWARD_FACTOR: u64 = 64;
const BASE_REWARDS_PER_EPOCH: u64 = 4;
const PROPOSER_REWARD_QUOTIENT: u64 = 8;
pub(crate) const MIN_SEED_LOOKAHEAD: u64 = 1;
pub(crate) const MAX_SEED_LOOKAHEAD: u64 = 4;
pub(crate) const DOMAIN_BEACON_ATTESTER: [u8; 4] = [1, 0, 0, 0];

fn checked_constants(
    state: &BeaconState,
    parameters: &Phase0Parameters,
) -> Result<Constants, ProcessingError> {
    parameters.validate()?;
    // Validate every structural bound even for manually edited typed values.
    state.hash_tree_root(parameters.preset)?;
    if state.validators.values.len() != state.balances.values.len() {
        return Err(ProcessingError::InvalidState("validator and balance lengths differ"))
    }
    Ok(Constants::for_preset(parameters.preset))
}

fn transact<F>(
    state: &mut BeaconState,
    parameters: &Phase0Parameters,
    operation: F,
) -> Result<(), ProcessingError>
where
    F: FnOnce(&mut BeaconState, &Constants) -> Result<(), ProcessingError>,
{
    let constants = checked_constants(state, parameters)?;
    let mut candidate = state.clone();
    operation(&mut candidate, &constants)?;
    // Mutations must not publish data outside the complete Phase0 schema.
    candidate.hash_tree_root(parameters.preset)?;
    *state = candidate;
    Ok(())
}

pub(crate) fn add(a: u64, b: u64, context: &'static str) -> Result<u64, ProcessingError> {
    a.checked_add(b).ok_or(ProcessingError::Arithmetic(context))
}

pub(crate) fn mul(a: u64, b: u64, context: &'static str) -> Result<u64, ProcessingError> {
    a.checked_mul(b).ok_or(ProcessingError::Arithmetic(context))
}

pub(crate) fn sub(a: u64, b: u64, context: &'static str) -> Result<u64, ProcessingError> {
    a.checked_sub(b).ok_or(ProcessingError::Arithmetic(context))
}

pub(crate) fn position(value: u64, modulus: u64) -> Result<usize, ProcessingError> {
    usize::try_from(value % modulus)
        .map_err(|_| ProcessingError::InvalidState("ring index does not fit platform"))
}

const fn epoch(state: &BeaconState, constants: &Constants) -> u64 {
    state.slot / constants.slots_per_epoch
}

pub(crate) const fn is_active(validator: &Validator, epoch: u64) -> bool {
    validator.activation_epoch <= epoch && epoch < validator.exit_epoch
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

fn active_balance(state: &BeaconState, constants: &Constants) -> Result<u64, ProcessingError> {
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

fn block_root_at_slot(
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

fn block_root_at_epoch(
    state: &BeaconState,
    epoch: u64,
    constants: &Constants,
) -> Result<Root, ProcessingError> {
    block_root_at_slot(state, mul(epoch, constants.slots_per_epoch, "epoch start slot")?, constants)
}

fn committee(
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
    parameters: &Phase0Parameters,
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

struct Vote<'a> {
    attestation: &'a PendingAttestation,
    indices: Vec<usize>,
    target_matches: bool,
    head_matches: bool,
}

struct Participation<'a> {
    previous: Vec<Vote<'a>>,
    current: Vec<Vote<'a>>,
    total_active_balance: u64,
    eligible: Vec<bool>,
    finality_delay: u64,
}

impl<'a> Participation<'a> {
    fn new(
        state: &'a BeaconState,
        constants: &Constants,
        include_heads: bool,
    ) -> Result<Self, ProcessingError> {
        let current = epoch(state, constants);
        let previous = current.saturating_sub(1);
        let mut cache = BTreeMap::new();
        let previous_records = if previous == current {
            &state.current_epoch_attestations.values
        } else {
            &state.previous_epoch_attestations.values
        };
        let previous_votes =
            votes(state, previous_records, previous, constants, &mut cache, include_heads)?;
        let current_votes = votes(
            state,
            &state.current_epoch_attestations.values,
            current,
            constants,
            &mut cache,
            false,
        )?;
        let next_previous = add(previous, 1, "eligible previous epoch")?;
        let eligible = state
            .validators
            .values
            .iter()
            .map(|validator| {
                is_active(validator, previous) ||
                    (validator.slashed && next_previous < validator.withdrawable_epoch)
            })
            .collect();
        Ok(Self {
            previous: previous_votes,
            current: current_votes,
            total_active_balance: active_balance(state, constants)?,
            eligible,
            finality_delay: sub(previous, state.finalized_checkpoint.epoch, "finality delay")?,
        })
    }

    fn unslashed(&self, state: &BeaconState, component: Component, current: bool) -> Vec<bool> {
        let mut indices = vec![false; state.validators.values.len()];
        for vote in if current { &self.current } else { &self.previous } {
            if component.matches(vote) {
                for &index in &vote.indices {
                    if !state.validators.values[index].slashed {
                        indices[index] = true;
                    }
                }
            }
        }
        indices
    }
}

fn votes<'a>(
    state: &'a BeaconState,
    records: &'a [PendingAttestation],
    expected_epoch: u64,
    constants: &Constants,
    cache: &mut BTreeMap<(u64, u64), Vec<u64>>,
    include_heads: bool,
) -> Result<Vec<Vote<'a>>, ProcessingError> {
    let mut output = Vec::with_capacity(records.len());
    for attestation in records {
        if attestation.data.slot / constants.slots_per_epoch != expected_epoch ||
            attestation.data.target.epoch != expected_epoch
        {
            return Err(ProcessingError::InvalidState(
                "pending attestation is in the wrong epoch record",
            ))
        }
        if attestation.inclusion_delay == 0 {
            return Err(ProcessingError::InvalidState("pending attestation inclusion delay is zero"))
        }
        if attestation.proposer_index >= state.validators.values.len() as u64 {
            return Err(ProcessingError::InvalidState(
                "pending attestation proposer index is out of range",
            ))
        }
        let key = (attestation.data.slot, attestation.data.index);
        let committee = match cache.entry(key) {
            std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(committee(state, key.0, key.1, constants)?)
            }
        };
        if attestation.aggregation_bits.bits.len() != committee.len() {
            return Err(ProcessingError::InvalidState(
                "pending attestation bit length differs from its committee",
            ))
        }
        let mut indices = Vec::new();
        for (position, &index) in committee.iter().enumerate() {
            if attestation
                .aggregation_bits
                .bits
                .get(position)
                .ok_or(ProcessingError::InvalidState("pending attestation bits are truncated"))?
            {
                indices.push(usize::try_from(index).map_err(|_| {
                    ProcessingError::InvalidState("attesting index does not fit platform")
                })?);
            }
        }
        let target_matches =
            attestation.data.target.root == block_root_at_epoch(state, expected_epoch, constants)?;
        let head_matches = include_heads &&
            target_matches &&
            attestation.data.beacon_block_root ==
                block_root_at_slot(state, attestation.data.slot, constants)?;
        output.push(Vote { attestation, indices, target_matches, head_matches });
    }
    Ok(output)
}

#[derive(Clone, Copy)]
enum Component {
    Source,
    Target,
    Head,
}

impl Component {
    const fn matches(self, vote: &Vote<'_>) -> bool {
        match self {
            Self::Source => true,
            Self::Target => vote.target_matches,
            Self::Head => vote.head_matches,
        }
    }
}

fn justification_kernel(
    state: &mut BeaconState,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    let current = epoch(state, constants);
    if current <= 1 {
        return Ok(())
    }
    let previous = current - 1;
    let view = Participation::new(state, constants, false)?;
    let previous_balance = total_balance(state, &view.unslashed(state, Component::Target, false))?;
    let current_balance = total_balance(state, &view.unslashed(state, Component::Target, true))?;
    let total = view.total_active_balance;
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
    state.justification_bits = Bits::from_ssz(&[bits], constants.preset())?;
    Ok(())
}

fn empty_deltas(state: &BeaconState) -> RewardDeltas {
    RewardDeltas {
        rewards: vec![0; state.validators.values.len()],
        penalties: vec![0; state.validators.values.len()],
    }
}

fn base_reward(state: &BeaconState, index: usize, total: u64) -> Result<u64, ProcessingError> {
    let validator = state
        .validators
        .values
        .get(index)
        .ok_or(ProcessingError::InvalidState("reward validator index is out of range"))?;
    Ok(mul(validator.effective_balance, BASE_REWARD_FACTOR, "base reward numerator")? /
        integer_squareroot(total) /
        BASE_REWARDS_PER_EPOCH)
}

fn component_deltas(
    state: &BeaconState,
    parameters: &Phase0Parameters,
    component: Component,
) -> Result<RewardDeltas, ProcessingError> {
    let constants = checked_constants(state, parameters)?;
    let view = Participation::new(state, &constants, matches!(component, Component::Head))?;
    participation_deltas(state, &view, component)
}

fn participation_deltas(
    state: &BeaconState,
    view: &Participation<'_>,
    component: Component,
) -> Result<RewardDeltas, ProcessingError> {
    let mut deltas = empty_deltas(state);
    let indices = view.unslashed(state, component, false);
    let attesting_balance = total_balance(state, &indices)?;
    for (index, &eligible) in view.eligible.iter().enumerate() {
        if eligible {
            let base = base_reward(state, index, view.total_active_balance)?;
            if indices[index] {
                deltas.rewards[index] = if view.finality_delay > MIN_EPOCHS_TO_INACTIVITY_PENALTY {
                    base
                } else {
                    mul(
                        base,
                        attesting_balance / EFFECTIVE_BALANCE_INCREMENT,
                        "attestation reward numerator",
                    )? / (view.total_active_balance / EFFECTIVE_BALANCE_INCREMENT)
                };
            } else {
                deltas.penalties[index] = base;
            }
        }
    }
    Ok(deltas)
}

fn inclusion_deltas(
    state: &BeaconState,
    view: &Participation<'_>,
) -> Result<RewardDeltas, ProcessingError> {
    let mut deltas = empty_deltas(state);
    let mut earliest = vec![None::<&PendingAttestation>; state.validators.values.len()];
    for vote in &view.previous {
        for &index in &vote.indices {
            if !state.validators.values[index].slashed &&
                earliest[index].is_none_or(|previous| {
                    vote.attestation.inclusion_delay < previous.inclusion_delay
                })
            {
                earliest[index] = Some(vote.attestation);
            }
        }
    }
    for (index, attestation) in earliest.into_iter().enumerate() {
        if let Some(attestation) = attestation {
            let base = base_reward(state, index, view.total_active_balance)?;
            let proposer_reward = base / PROPOSER_REWARD_QUOTIENT;
            let proposer = usize::try_from(attestation.proposer_index).map_err(|_| {
                ProcessingError::InvalidState("proposer index does not fit platform")
            })?;
            deltas.rewards[proposer] =
                add(deltas.rewards[proposer], proposer_reward, "proposer inclusion reward")?;
            let reward = sub(base, proposer_reward, "attester inclusion reward")? /
                attestation.inclusion_delay;
            deltas.rewards[index] =
                add(deltas.rewards[index], reward, "attester inclusion reward")?;
        }
    }
    Ok(deltas)
}

fn inactivity_deltas(
    state: &BeaconState,
    constants: &Constants,
    view: &Participation<'_>,
) -> Result<RewardDeltas, ProcessingError> {
    let mut deltas = empty_deltas(state);
    if view.finality_delay > MIN_EPOCHS_TO_INACTIVITY_PENALTY {
        let target = view.unslashed(state, Component::Target, false);
        for (index, &eligible) in view.eligible.iter().enumerate() {
            if eligible {
                let base = base_reward(state, index, view.total_active_balance)?;
                let compensation = sub(
                    mul(BASE_REWARDS_PER_EPOCH, base, "inactivity base compensation")?,
                    base / PROPOSER_REWARD_QUOTIENT,
                    "inactivity proposer compensation",
                )?;
                deltas.penalties[index] = compensation;
                if !target[index] {
                    let penalty = mul(
                        state.validators.values[index].effective_balance,
                        view.finality_delay,
                        "inactivity effective balance penalty",
                    )? / constants.inactivity_penalty_quotient;
                    deltas.penalties[index] =
                        add(deltas.penalties[index], penalty, "total inactivity penalty")?;
                }
            }
        }
    }
    Ok(deltas)
}

fn attestation_deltas(
    state: &BeaconState,
    constants: &Constants,
) -> Result<RewardDeltas, ProcessingError> {
    let view = Participation::new(state, constants, true)?;
    let mut total = empty_deltas(state);
    let components = [
        participation_deltas(state, &view, Component::Source)?,
        participation_deltas(state, &view, Component::Target)?,
        participation_deltas(state, &view, Component::Head)?,
        inclusion_deltas(state, &view)?,
        inactivity_deltas(state, constants, &view)?,
    ];
    for component in components {
        for (index, (reward, penalty)) in
            component.rewards.into_iter().zip(component.penalties).enumerate()
        {
            total.rewards[index] =
                add(total.rewards[index], reward, "combined attestation rewards")?;
            total.penalties[index] =
                add(total.penalties[index], penalty, "combined attestation penalties")?;
        }
    }
    Ok(total)
}

fn rewards_kernel(state: &mut BeaconState, constants: &Constants) -> Result<(), ProcessingError> {
    if epoch(state, constants) == 0 {
        return Ok(())
    }
    let deltas = attestation_deltas(state, constants)?;
    for (index, balance) in state.balances.values.iter_mut().enumerate() {
        *balance = add(*balance, deltas.rewards[index], "reward balance application")?
            .saturating_sub(deltas.penalties[index]);
    }
    Ok(())
}

fn churn(state: &BeaconState, parameters: &Phase0Parameters, constants: &Constants) -> u64 {
    parameters.min_per_epoch_churn_limit.max(
        active_indices(state, epoch(state, constants)).len() as u64 /
            parameters.churn_limit_quotient,
    )
}

fn activation_exit_epoch(current: u64) -> Result<u64, ProcessingError> {
    add(add(current, 1, "activation/exit epoch")?, MAX_SEED_LOOKAHEAD, "activation/exit lookahead")
}

fn exit_kernel(
    state: &mut BeaconState,
    index: usize,
    parameters: &Phase0Parameters,
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
        add(queue, parameters.min_validator_withdrawability_delay, "withdrawability delay")?;
    let validator = &mut state.validators.values[index];
    validator.exit_epoch = queue;
    validator.withdrawable_epoch = withdrawable;
    Ok(())
}

fn registry_kernel(
    state: &mut BeaconState,
    parameters: &Phase0Parameters,
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
            validator.effective_balance <= parameters.ejection_balance
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

fn slashings_kernel(state: &mut BeaconState, constants: &Constants) -> Result<(), ProcessingError> {
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

fn eth1_reset_kernel(
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

fn effective_balances_kernel(
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

fn slashings_reset_kernel(
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

fn randao_reset_kernel(
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

fn historical_roots_kernel(
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

fn epoch_kernel(
    state: &mut BeaconState,
    parameters: &Phase0Parameters,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    justification_kernel(state, constants)?;
    rewards_kernel(state, constants)?;
    registry_kernel(state, parameters, constants)?;
    slashings_kernel(state, constants)?;
    eth1_reset_kernel(state, constants)?;
    effective_balances_kernel(state, constants)?;
    slashings_reset_kernel(state, constants)?;
    randao_reset_kernel(state, constants)?;
    historical_roots_kernel(state, constants)?;
    state.previous_epoch_attestations = std::mem::take(&mut state.current_epoch_attestations);
    Ok(())
}

/// Replay into a private temporary candidate without allocating a second complete state copy.
/// Errors may modify this candidate; callers must discard it and never pass retained state.
pub(crate) fn process_slots_in_place(
    state: &mut BeaconState,
    target: u64,
    parameters: &Phase0Parameters,
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
        phase0::{
            AttestationData, BeaconBlock, BeaconBlockBody, Eth1Data, ForkInfo, IndexedAttestation,
            SignedBeaconBlock,
        },
        BitListValue, List, Vector,
    };
    use reth_embedded_beacon_primitives::BeaconBlockHeader;

    fn state() -> BeaconState {
        BeaconState {
            genesis_time: 0,
            genesis_validators_root: [1; 32],
            slot: 7,
            fork: ForkInfo { previous_version: [0; 4], current_version: [0; 4], epoch: 0 },
            latest_block_header: BeaconBlockHeader::default(),
            block_roots: Vector::from_vec(vec![[0; 32]; 64]),
            state_roots: Vector::from_vec(vec![[0; 32]; 64]),
            historical_roots: List::default(),
            eth1_data: Eth1Data { deposit_root: [0; 32], deposit_count: 0, block_hash: [0; 32] },
            eth1_data_votes: List::default(),
            eth1_deposit_index: 0,
            validators: List::default(),
            balances: List::default(),
            randao_mixes: Vector::from_vec(vec![[0; 32]; 64]),
            slashings: Vector::from_vec(vec![0; 64]),
            previous_epoch_attestations: List::default(),
            current_epoch_attestations: List::default(),
            justification_bits: Bits::from_ssz(&[0], Preset::Minimal).unwrap(),
            previous_justified_checkpoint: Checkpoint::default(),
            current_justified_checkpoint: Checkpoint::default(),
            finalized_checkpoint: Checkpoint::default(),
        }
    }

    fn validator() -> Validator {
        Validator {
            pubkey: [0; 48],
            withdrawal_credentials: [0; 32],
            effective_balance: 32_000_000_000,
            slashed: false,
            activation_eligibility_epoch: 0,
            activation_epoch: 0,
            exit_epoch: u64::MAX,
            withdrawable_epoch: u64::MAX,
        }
    }

    #[test]
    fn slot_caches_pre_state_and_fills_header_once() {
        let parameters = Phase0Parameters::for_preset(Preset::Minimal);
        let mut state = state();
        let root = state.hash_tree_root(Preset::Minimal).unwrap();
        process_slot(&mut state, &parameters).unwrap();
        assert_eq!(state.slot, 7);
        assert_eq!(state.state_roots.values[7], root);
        assert_eq!(state.latest_block_header.state_root, root);
        assert_eq!(state.block_roots.values[7], state.latest_block_header.hash_tree_root());
        state.slot = 8;
        process_slot(&mut state, &parameters).unwrap();
        assert_eq!(state.latest_block_header.state_root, root);
        assert_eq!(state.block_roots.values[8], state.block_roots.values[7]);
    }

    #[test]
    fn slots_run_epoch_resets_before_advancing() {
        let parameters = Phase0Parameters::for_preset(Preset::Minimal);
        let mut state = state();
        state.slashings.values[1] = 77;
        state.randao_mixes.values[0] = [4; 32];
        process_slots(&mut state, 8, &parameters).unwrap();
        assert_eq!(state.slot, 8);
        assert_eq!(state.slashings.values[1], 0);
        assert_eq!(state.randao_mixes.values[1], [4; 32]);
        assert!(state.historical_roots.values.is_empty());
        assert!(matches!(
            process_slots(&mut state, 8, &parameters),
            Err(ProcessingError::SlotNotIncreasing { current: 8, target: 8 })
        ));
    }

    #[test]
    fn failed_epoch_replay_is_atomic() {
        let parameters = Phase0Parameters::for_preset(Preset::Minimal);
        let mut state = state();
        state.validators.values.push(validator());
        state.balances.values.push(u64::MAX);
        let before = state.clone();
        assert!(matches!(
            process_slots(&mut state, 8, &parameters),
            Err(ProcessingError::Arithmetic("downward hysteresis comparison"))
        ));
        assert_eq!(state, before);
    }

    #[test]
    fn registry_exit_queue_enforces_churn() {
        let parameters = Phase0Parameters::for_preset(Preset::Minimal);
        let mut state = state();
        let mut low = validator();
        low.effective_balance = 16_000_000_000;
        state.validators.values = vec![low; 4];
        state.balances.values = vec![16_000_000_000; 4];
        process_registry_updates(&mut state, &parameters).unwrap();
        assert_eq!(
            state.validators.values.iter().map(|v| v.exit_epoch).collect::<Vec<_>>(),
            [5, 5, 6, 6]
        );
        assert_eq!(
            state.validators.values.iter().map(|v| v.withdrawable_epoch).collect::<Vec<_>>(),
            [261, 261, 262, 262]
        );
    }

    #[test]
    fn malformed_participation_and_zero_churn_fail_without_panics() {
        let parameters = Phase0Parameters::for_preset(Preset::Minimal);
        let mut state = state();
        state.slot = 15;
        state.validators.values = vec![validator(); 8];
        state.balances.values = vec![32_000_000_000; 8];
        state.previous_epoch_attestations.values.push(PendingAttestation {
            aggregation_bits: BitListValue::from_ssz(&[1], Preset::Minimal).unwrap(),
            data: AttestationData {
                slot: 0,
                index: 0,
                beacon_block_root: [0; 32],
                source: Checkpoint::default(),
                target: Checkpoint::default(),
            },
            inclusion_delay: 1,
            proposer_index: 0,
        });
        let before = state.clone();
        assert!(matches!(
            process_epoch(&mut state, &parameters),
            Err(ProcessingError::InvalidState(
                "pending attestation bit length differs from its committee"
            ))
        ));
        assert_eq!(state, before);
        let invalid = Phase0Parameters { churn_limit_quotient: 0, ..parameters };
        assert!(matches!(
            process_registry_updates(&mut state, &invalid),
            Err(ProcessingError::InvalidParameters(_))
        ));
        assert_eq!(state, before);
    }

    #[test]
    fn justification_does_not_look_up_unused_same_slot_heads() {
        let parameters = Phase0Parameters::for_preset(Preset::Minimal);
        let mut state = state();
        state.slot = 47;
        state.validators.values = vec![validator(); 8];
        state.balances.values = vec![32_000_000_000; 8];
        state.block_roots.values[40] = [2; 32];
        state.current_epoch_attestations.values.push(PendingAttestation {
            aggregation_bits: BitListValue::from_ssz(&[3], Preset::Minimal).unwrap(),
            data: AttestationData {
                slot: 47,
                index: 0,
                beacon_block_root: [2; 32],
                source: Checkpoint::default(),
                target: Checkpoint { epoch: 5, root: [2; 32] },
            },
            inclusion_delay: 1,
            proposer_index: 0,
        });
        process_justification_and_finalization(&mut state, &parameters).unwrap();
        assert_eq!(state.slot, 47);
        assert_eq!(state.current_epoch_attestations.values.len(), 1);
    }

    #[test]
    fn cached_empty_replay_matches_full_schema_roots_across_ring_wraps() {
        let parameters = Phase0Parameters::for_preset(Preset::Minimal);
        let mut cached = state();
        cached.validators.values = vec![validator(); 8];
        cached.balances.values = vec![32_000_000_000; 8];
        cached.randao_mixes.values[0] = [9; 32];
        let mut full = cached.clone();
        let constants = checked_constants(&cached, &parameters).unwrap();
        let mut cache = slot_cache::SlotCache::new(&cached, &constants).unwrap();
        while cached.slot < 73 {
            assert_eq!(cache.root().unwrap(), full.hash_tree_root(Preset::Minimal).unwrap());
            cache.process_slot(&mut cached).unwrap();
            slot_kernel(&mut full, &constants).unwrap();
            assert_eq!(cached, full);
            let next = cached.slot + 1;
            if next.is_multiple_of(constants.slots_per_epoch) {
                epoch_kernel(&mut cached, &parameters, &constants).unwrap();
                epoch_kernel(&mut full, &parameters, &constants).unwrap();
                cache.refresh_epoch(&cached).unwrap();
            }
            cached.slot = next;
            full.slot = next;
            cache.advance_slot(next);
        }
        assert_eq!(cached, full);
        assert_eq!(cached.historical_roots.values.len(), 1);
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
        }
    }

    #[test]
    fn signed_import_requires_real_bls_and_rolls_back_replayed_slots() {
        let parameters = Phase0Parameters::for_preset(Preset::Minimal);
        let mut state = state();
        let mut active = validator();
        // This public key comes from the retained published aggregate verification fixture.
        active.pubkey = [
            0xa4, 0x91, 0xd1, 0xb0, 0xec, 0xd9, 0xbb, 0x91, 0x79, 0x89, 0xf0, 0xe7, 0x4f, 0x0d,
            0xea, 0x04, 0x22, 0xea, 0xc4, 0xa8, 0x73, 0xe5, 0xe2, 0x64, 0x4f, 0x36, 0x8d, 0xff,
            0xb9, 0xa6, 0xe2, 0x0f, 0xd6, 0xe1, 0x0c, 0x1b, 0x77, 0x65, 0x4d, 0x06, 0x7c, 0x06,
            0x18, 0xf6, 0xe5, 0xa7, 0xf7, 0x9a,
        ];
        state.validators.values = vec![active; 8];
        state.balances.values = vec![32_000_000_000; 8];
        let before = state.clone();
        let mut advanced = state.clone();
        process_slots(&mut advanced, 8, &parameters).unwrap();
        let signed = SignedBeaconBlock {
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
            process_signed_block(&mut state, &signed, &parameters),
            Err(ProcessingError::Crypto(
                reth_embedded_beacon_crypto::CryptoError::InvalidSignature
            ))
        ));
        assert_eq!(state, before);
    }

    #[test]
    fn indexed_participants_are_nonempty_sorted_unique_before_crypto() {
        let parameters = Phase0Parameters::for_preset(Preset::Minimal);
        let mut state = state();
        state.validators.values = vec![validator(); 2];
        state.balances.values = vec![32_000_000_000; 2];
        let data = AttestationData {
            slot: 0,
            index: 0,
            beacon_block_root: [0; 32],
            source: Checkpoint::default(),
            target: Checkpoint::default(),
        };
        for values in [vec![], vec![0, 0], vec![1, 0]] {
            let indexed = IndexedAttestation {
                attesting_indices: List::from_vec(values),
                data: data.clone(),
                signature: [0; 96],
            };
            assert!(matches!(
                is_valid_indexed_attestation(&state, &indexed, &parameters),
                Err(ProcessingError::InvalidState(
                    "indexed attestation participants are not nonempty sorted unique indexes"
                ))
            ));
        }
        let indexed = IndexedAttestation {
            attesting_indices: List::from_vec(vec![2]),
            data,
            signature: [0; 96],
        };
        assert!(matches!(
            is_valid_indexed_attestation(&state, &indexed, &parameters),
            Err(ProcessingError::InvalidState("validator index is out of range"))
        ));
    }

    #[test]
    fn required_deposit_count_failure_preserves_state() {
        let parameters = Phase0Parameters::for_preset(Preset::Minimal);
        let mut state = state();
        state.eth1_data.deposit_count = 1;
        let before = state.clone();
        assert!(matches!(
            process_operations(&mut state, &body(&before), &parameters),
            Err(ProcessingError::InvalidState(
                "block deposit count does not match outstanding deposits"
            ))
        ));
        assert_eq!(state, before);
    }
    #[cfg(feature = "spec-tests")]
    #[test]
    fn fixture_signature_mode_cannot_change_production_verification() {
        let parameters = Phase0Parameters::for_preset(Preset::Minimal);
        let mut state = state();
        state.validators.values = vec![validator(); 8];
        state.balances.values = vec![32_000_000_000; 8];
        let before = state.clone();
        let body = body(&state);
        spec_only::process_randao(&mut state, &body, &parameters).unwrap();
        assert!(matches!(
            process_randao(&mut state, &body, &parameters),
            Err(ProcessingError::Crypto(_))
        ));
        let mut production = before.clone();
        assert!(matches!(
            process_randao(&mut production, &body, &parameters),
            Err(ProcessingError::Crypto(_))
        ));
        assert_eq!(production, before);
    }
}
