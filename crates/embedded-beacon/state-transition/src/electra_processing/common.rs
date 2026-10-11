//! Exact balance-weighted Electra helpers shared by epochs and strict operations.

use super::{Constants, ElectraParameters, ProcessingError};
use crate::{
    compute_seed, compute_shuffled_index, integer_squareroot,
    phase0_processing::{add, is_active, mul, position, sub},
    ssz::{
        electra::{BeaconState, PendingDeposit, SyncAggregate, SyncCommittee, Validator},
        Preset, Vector,
    },
};
use reth_embedded_beacon_crypto::{
    aggregate_pubkeys, compute_domain, compute_signing_root, eth_fast_aggregate_verify,
    subtract_aggregate_pubkeys,
};
use reth_embedded_beacon_primitives::{sha256, Root};
use std::collections::BTreeMap;

pub(crate) const INCREMENT: u64 = 1_000_000_000;
pub(crate) const MIN_ACTIVATION: u64 = 32_000_000_000;
pub(crate) const MAX_EFFECTIVE: u64 = 2_048_000_000_000;

pub(crate) const fn has_compounding_withdrawal_credential(validator: &Validator) -> bool {
    validator.withdrawal_credentials[0] == 2
}
pub(crate) const fn has_execution_withdrawal_credential(validator: &Validator) -> bool {
    validator.withdrawal_credentials[0] == 1 || validator.withdrawal_credentials[0] == 2
}
pub(crate) const fn get_max_effective_balance(validator: &Validator) -> u64 {
    if has_compounding_withdrawal_credential(validator) {
        MAX_EFFECTIVE
    } else {
        MIN_ACTIVATION
    }
}
pub(crate) fn index(index: u64, length: usize) -> Result<usize, ProcessingError> {
    let index = usize::try_from(index)
        .map_err(|_| ProcessingError::InvalidState("validator index does not fit platform"))?;
    if index >= length {
        return Err(ProcessingError::InvalidState("validator index is out of range"))
    }
    Ok(index)
}
pub(crate) fn active_indices(state: &BeaconState, epoch: u64) -> Vec<u64> {
    state
        .validators
        .values
        .iter()
        .enumerate()
        .filter_map(|(index, v)| is_active(v, epoch).then_some(index as u64))
        .collect()
}
pub(crate) fn total_active_balance(
    state: &BeaconState,
    constants: &Constants,
) -> Result<u64, ProcessingError> {
    let epoch = state.slot / constants.slots_per_epoch;
    state
        .validators
        .values
        .iter()
        .filter(|v| is_active(v, epoch))
        .try_fold(0, |sum, v| add(sum, v.effective_balance, "total active balance"))
        .map(|sum| sum.max(INCREMENT))
}
pub(crate) fn get_balance_churn_limit(
    state: &BeaconState,
    parameters: &ElectraParameters,
    constants: &Constants,
) -> Result<u64, ProcessingError> {
    let total = total_active_balance(state, constants)?;
    let churn = parameters
        .min_per_epoch_churn_limit
        .max(total / parameters.deneb.capella.bellatrix.altair.phase0.churn_limit_quotient);
    Ok(churn - churn % INCREMENT)
}
pub(crate) fn get_activation_exit_churn_limit(
    state: &BeaconState,
    parameters: &ElectraParameters,
    constants: &Constants,
) -> Result<u64, ProcessingError> {
    Ok(get_balance_churn_limit(state, parameters, constants)?
        .min(parameters.max_per_epoch_activation_exit_churn_limit))
}
pub(crate) fn get_consolidation_churn_limit(
    state: &BeaconState,
    parameters: &ElectraParameters,
    constants: &Constants,
) -> Result<u64, ProcessingError> {
    sub(
        get_balance_churn_limit(state, parameters, constants)?,
        get_activation_exit_churn_limit(state, parameters, constants)?,
        "consolidation churn",
    )
}
pub(crate) fn get_pending_balance_to_withdraw(
    state: &BeaconState,
    validator_index: u64,
) -> Result<u64, ProcessingError> {
    index(validator_index, state.validators.values.len())?;
    state
        .pending_partial_withdrawals
        .values
        .iter()
        .filter(|w| w.validator_index == validator_index)
        .try_fold(0, |sum, w| add(sum, w.amount, "pending withdrawal balance"))
}
pub(crate) fn compute_exit_epoch_and_update_churn(
    state: &mut BeaconState,
    balance: u64,
    parameters: &ElectraParameters,
    constants: &Constants,
) -> Result<u64, ProcessingError> {
    let baseline =
        crate::altair_processing::activation_exit_epoch(state.slot / constants.slots_per_epoch)?;
    let earliest = state.earliest_exit_epoch.max(baseline);
    let churn = get_activation_exit_churn_limit(state, parameters, constants)?;
    let mut remaining =
        if state.earliest_exit_epoch < earliest { churn } else { state.exit_balance_to_consume };
    let mut earliest = earliest;
    if balance > remaining {
        let extra = sub(balance, remaining, "exit excess balance")?;
        let epochs =
            add(sub(extra, 1, "exit epoch rounding")? / churn, 1, "exit additional epochs")?;
        earliest = add(earliest, epochs, "exit epoch")?;
        remaining = add(
            remaining,
            mul(epochs, churn, "exit available balance")?,
            "exit available balance",
        )?;
    }
    state.exit_balance_to_consume = sub(remaining, balance, "exit remaining balance")?;
    state.earliest_exit_epoch = earliest;
    Ok(earliest)
}
pub(crate) fn compute_consolidation_epoch_and_update_churn(
    state: &mut BeaconState,
    balance: u64,
    parameters: &ElectraParameters,
    constants: &Constants,
) -> Result<u64, ProcessingError> {
    let baseline =
        crate::altair_processing::activation_exit_epoch(state.slot / constants.slots_per_epoch)?;
    let earliest = state.earliest_consolidation_epoch.max(baseline);
    let churn = get_consolidation_churn_limit(state, parameters, constants)?;
    let mut remaining = if state.earliest_consolidation_epoch < earliest {
        churn
    } else {
        state.consolidation_balance_to_consume
    };
    let mut earliest = earliest;
    if balance > remaining {
        if churn == 0 {
            return Err(ProcessingError::InvalidState("consolidation churn is zero"))
        }
        let extra = sub(balance, remaining, "consolidation excess balance")?;
        let epochs = add(
            sub(extra, 1, "consolidation epoch rounding")? / churn,
            1,
            "consolidation additional epochs",
        )?;
        earliest = add(earliest, epochs, "consolidation epoch")?;
        remaining = add(
            remaining,
            mul(epochs, churn, "consolidation available balance")?,
            "consolidation available balance",
        )?;
    }
    state.consolidation_balance_to_consume =
        sub(remaining, balance, "consolidation remaining balance")?;
    state.earliest_consolidation_epoch = earliest;
    Ok(earliest)
}
pub(crate) fn initiate_validator_exit(
    state: &mut BeaconState,
    validator_index: u64,
    parameters: &ElectraParameters,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    let index = index(validator_index, state.validators.values.len())?;
    let validator = &state.validators.values[index];
    if validator.exit_epoch != u64::MAX {
        return Ok(())
    }
    let effective = validator.effective_balance;
    let exit = compute_exit_epoch_and_update_churn(state, effective, parameters, constants)?;
    let withdrawable = add(
        exit,
        parameters.deneb.capella.bellatrix.altair.phase0.min_validator_withdrawability_delay,
        "withdrawability delay",
    )?;
    state.validators.values[index].exit_epoch = exit;
    state.validators.values[index].withdrawable_epoch = withdrawable;
    Ok(())
}
pub(crate) fn queue_excess_active_balance(
    state: &mut BeaconState,
    validator_index: u64,
) -> Result<(), ProcessingError> {
    let index = index(validator_index, state.validators.values.len())?;
    let balance = state.balances.values[index];
    if balance > MIN_ACTIVATION {
        if state.pending_deposits.values.len() >= 134_217_728 {
            return Err(ProcessingError::InvalidState("pending deposits limit reached"))
        }
        let validator = &state.validators.values[index];
        let mut infinity = [0; 96];
        infinity[0] = 0xc0;
        state.pending_deposits.values.push(PendingDeposit {
            pubkey: validator.pubkey,
            withdrawal_credentials: validator.withdrawal_credentials,
            amount: sub(balance, MIN_ACTIVATION, "queued excess balance")?,
            signature: infinity,
            slot: 0,
        });
        state.balances.values[index] = MIN_ACTIVATION;
    }
    Ok(())
}
pub(crate) fn switch_to_compounding_validator(
    state: &mut BeaconState,
    validator_index: u64,
    _parameters: &ElectraParameters,
    _constants: &Constants,
) -> Result<(), ProcessingError> {
    let index = index(validator_index, state.validators.values.len())?;
    state.validators.values[index].withdrawal_credentials[0] = 2;
    queue_excess_active_balance(state, validator_index)
}
pub(crate) fn domain(state: &BeaconState, domain_type: [u8; 4], epoch: u64) -> Root {
    let version = if epoch < state.fork.epoch {
        state.fork.previous_version
    } else {
        state.fork.current_version
    };
    compute_domain(domain_type, version, state.genesis_validators_root)
}
pub(crate) fn get_committee_count_per_slot(
    state: &BeaconState,
    epoch: u64,
    constants: &Constants,
) -> Result<u64, ProcessingError> {
    Ok(((active_indices(state, epoch).len() as u64 / constants.slots_per_epoch) /
        constants.target_committee_size)
        .clamp(1, constants.maximum_committees))
}
pub(crate) fn seed(
    state: &BeaconState,
    epoch: u64,
    domain_type: [u8; 4],
    constants: &Constants,
) -> Result<Root, ProcessingError> {
    let lookback = sub(add(epoch, constants.historical_epochs, "seed epoch")?, 2, "seed lookback")?;
    let mix = *state
        .randao_mixes
        .values
        .get(position(lookback, constants.historical_epochs)?)
        .ok_or(ProcessingError::InvalidState("RANDAO ring is truncated"))?;
    Ok(compute_seed(domain_type, epoch, mix))
}
fn weighted_candidate(
    state: &BeaconState,
    active: &[u64],
    seed: Root,
    iteration: u64,
    random: &[u8; 32],
    constants: &Constants,
) -> Result<Option<u64>, ProcessingError> {
    let shuffled = compute_shuffled_index(
        iteration % (active.len() as u64),
        active.len() as u64,
        seed,
        constants.shuffle_rounds,
    )?;
    let candidate = active[index(shuffled, active.len())?];
    let offset = position(iteration, 16)? * 2;
    let value = u64::from(u16::from_le_bytes([random[offset], random[offset + 1]]));
    let effective =
        state.validators.values[index(candidate, state.validators.values.len())?].effective_balance;
    if mul(effective, 65_535, "weighted effective balance")? >=
        mul(MAX_EFFECTIVE, value, "weighted random value")?
    {
        Ok(Some(candidate))
    } else {
        Ok(None)
    }
}
pub(crate) fn get_beacon_proposer_index(
    state: &BeaconState,
    _parameters: &ElectraParameters,
    constants: &Constants,
) -> Result<u64, ProcessingError> {
    let epoch = state.slot / constants.slots_per_epoch;
    let active = active_indices(state, epoch);
    if active.is_empty() {
        return Err(ProcessingError::InvalidState("no active proposer candidates"))
    }
    let seed = seed(state, epoch, [0; 4], constants)?;
    let mut bytes = [0; 40];
    bytes[..32].copy_from_slice(&seed);
    bytes[32..].copy_from_slice(&state.slot.to_le_bytes());
    let seed = sha256(&bytes);
    bytes[..32].copy_from_slice(&seed);
    let mut iteration = 0u64;
    let mut random = [0; 32];
    loop {
        if iteration.is_multiple_of(16) {
            bytes[32..].copy_from_slice(&(iteration / 16u64).to_le_bytes());
            random = sha256(&bytes);
        }
        if let Some(index) =
            weighted_candidate(state, &active, seed, iteration, &random, constants)?
        {
            return Ok(index)
        }
        iteration = add(iteration, 1, "proposer selection iteration")?;
    }
}
pub(crate) fn next_sync_committee(
    state: &BeaconState,
    constants: &Constants,
) -> Result<SyncCommittee, ProcessingError> {
    let epoch = add(state.slot / constants.slots_per_epoch, 1, "next sync epoch")?;
    let active = active_indices(state, epoch);
    if active.is_empty() {
        return Err(ProcessingError::InvalidState("sync committee has no active validators"))
    }
    let seed = seed(state, epoch, [7, 0, 0, 0], constants)?;
    let size = match constants.preset() {
        Preset::Mainnet => 512,
        Preset::Minimal => 32,
    };
    let mut pubkeys = Vec::with_capacity(size);
    let mut iteration = 0u64;
    let mut random = [0; 32];
    let mut bytes = [0; 40];
    bytes[..32].copy_from_slice(&seed);
    while pubkeys.len() < size {
        if iteration.is_multiple_of(16) {
            bytes[32..].copy_from_slice(&(iteration / 16u64).to_le_bytes());
            random = sha256(&bytes);
        }
        if let Some(index) =
            weighted_candidate(state, &active, seed, iteration, &random, constants)?
        {
            pubkeys.push(state.validators.values[index as usize].pubkey);
        }
        iteration = add(iteration, 1, "sync selection iteration")?;
    }
    let aggregate_pubkey = aggregate_pubkeys(&pubkeys)?;
    Ok(SyncCommittee { pubkeys: Vector::from_vec(pubkeys), aggregate_pubkey })
}
pub(crate) fn slash_validator_kernel(
    state: &mut BeaconState,
    validator_index: u64,
    whistleblower: Option<u64>,
    parameters: &ElectraParameters,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    let slashed_index = index(validator_index, state.validators.values.len())?;
    initiate_validator_exit(state, validator_index, parameters, constants)?;
    let epoch = state.slot / constants.slots_per_epoch;
    let effective = state.validators.values[slashed_index].effective_balance;
    state.validators.values[slashed_index].slashed = true;
    state.validators.values[slashed_index].withdrawable_epoch = state.validators.values
        [slashed_index]
        .withdrawable_epoch
        .max(add(epoch, constants.slashing_epochs, "slashing withdrawability epoch")?);
    let position = position(epoch, constants.slashing_epochs)?;
    state.slashings.values[position] =
        add(state.slashings.values[position], effective, "slashing bucket balance")?;
    state.balances.values[slashed_index] =
        state.balances.values[slashed_index].saturating_sub(effective / 4096);
    let proposer = get_beacon_proposer_index(state, parameters, constants)?;
    let proposer = index(proposer, state.validators.values.len())?;
    let whistleblower =
        index(whistleblower.unwrap_or(proposer as u64), state.validators.values.len())?;
    let reward = effective / 4096;
    let proposer_reward = mul(reward, 8, "slashing proposer reward")? / 64;
    state.balances.values[proposer] =
        add(state.balances.values[proposer], proposer_reward, "slashing proposer reward")?;
    state.balances.values[whistleblower] = add(
        state.balances.values[whistleblower],
        sub(reward, proposer_reward, "slashing whistleblower remainder")?,
        "slashing whistleblower reward",
    )?;
    Ok(())
}
pub(crate) fn sync_aggregate_kernel(
    state: &mut BeaconState,
    aggregate: &SyncAggregate,
    parameters: &ElectraParameters,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    sync_aggregate_kernel_with_policy::<true>(state, aggregate, parameters, constants)
}
pub(crate) fn sync_aggregate_kernel_with_policy<const VERIFY_BLS: bool>(
    state: &mut BeaconState,
    aggregate: &SyncAggregate,
    parameters: &ElectraParameters,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    crate::common::require_signature_policy::<VERIFY_BLS>()?;
    let keys = &state.current_sync_committee.pubkeys.values;
    let mut bits = Vec::with_capacity(keys.len());
    for index in 0..keys.len() {
        bits.push(
            aggregate
                .sync_committee_bits
                .bits
                .get(index)
                .ok_or(ProcessingError::InvalidState("sync committee bits are truncated"))?,
        );
    }
    let previous = state.slot.saturating_sub(1);
    if previous >= state.slot || state.slot - previous > constants.historical_slots {
        return Err(ProcessingError::InvalidState("block root is outside historical slot window"))
    }
    let root = state.block_roots.values[position(previous, constants.historical_slots)?];
    let root = compute_signing_root(
        root,
        domain(state, [7, 0, 0, 0], previous / constants.slots_per_epoch),
    );
    if VERIFY_BLS {
        let count = bits.iter().filter(|&&bit| bit).count();
        let participants = if count == keys.len() {
            vec![state.current_sync_committee.aggregate_pubkey]
        } else if count > keys.len() / 2 {
            let removed = keys
                .iter()
                .zip(&bits)
                .filter_map(|(key, &bit)| (!bit).then_some(*key))
                .collect::<Vec<_>>();
            vec![subtract_aggregate_pubkeys(
                &state.current_sync_committee.aggregate_pubkey,
                &removed,
            )?]
        } else {
            keys.iter().zip(&bits).filter_map(|(key, &bit)| bit.then_some(*key)).collect::<Vec<_>>()
        };
        eth_fast_aggregate_verify(&participants, &root, &aggregate.sync_committee_signature)?;
    }
    let active = total_active_balance(state, constants)?;
    let base_per_increment =
        mul(INCREMENT, 64, "base reward numerator")? / integer_squareroot(active);
    let total_base = mul(base_per_increment, active / INCREMENT, "sync total base rewards")?;
    let maximum =
        mul(total_base, 2, "sync maximum participant reward")? / 64 / constants.slots_per_epoch;
    let participant_reward = maximum / (keys.len() as u64);
    let proposer_reward = mul(participant_reward, 8, "sync proposer reward")? / 56;
    let proposer = index(
        get_beacon_proposer_index(state, parameters, constants)?,
        state.validators.values.len(),
    )?;
    let mut registry = BTreeMap::new();
    for (index, v) in state.validators.values.iter().enumerate() {
        registry.entry(v.pubkey).or_insert(index);
    }
    for (key, &bit) in keys.iter().zip(&bits) {
        let &index = registry.get(key).ok_or(ProcessingError::InvalidState(
            "sync committee public key is absent from validator registry",
        ))?;
        if bit {
            state.balances.values[index] =
                add(state.balances.values[index], participant_reward, "sync participant reward")?;
            state.balances.values[proposer] =
                add(state.balances.values[proposer], proposer_reward, "sync proposer reward")?;
        } else {
            state.balances.values[index] =
                state.balances.values[index].saturating_sub(participant_reward);
        }
    }
    Ok(())
}
