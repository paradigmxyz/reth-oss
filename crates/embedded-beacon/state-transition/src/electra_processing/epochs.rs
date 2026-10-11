//! Complete ordered Electra epoch processing, including balance churn and pending queues.

use super::{
    common, execution_requests, transact, with_altair, Constants, ElectraParameters,
    ProcessingError,
};
use crate::{
    altair_processing,
    phase0_processing::{add, is_active, mul, sub},
    ssz::{
        electra::{BeaconState, HistoricalSummary},
        Preset, Ssz,
    },
};

pub(super) fn epoch_kernel(
    state: &mut BeaconState,
    parameters: &ElectraParameters,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    epoch_kernel_with_policy::<true>(state, parameters, constants)
}

pub(super) fn epoch_kernel_with_policy<const VERIFY_BLS: bool>(
    state: &mut BeaconState,
    parameters: &ElectraParameters,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    crate::common::require_signature_policy::<VERIFY_BLS>()?;
    with_altair(state, |view| {
        altair_processing::justification_kernel(view, constants)?;
        altair_processing::inactivity_kernel(
            view,
            &parameters.deneb.capella.bellatrix.altair,
            constants,
        )?;
        altair_processing::rewards_kernel(
            view,
            &parameters.deneb.capella.bellatrix.altair,
            constants,
        )
    })?;
    registry_kernel(state, parameters, constants)?;
    slashings_kernel(state, constants)?;
    with_altair(state, |view| altair_processing::eth1_reset_kernel(view, constants))?;
    pending_deposits_kernel_with_policy::<VERIFY_BLS>(state, parameters, constants)?;
    pending_consolidations_kernel(state, constants)?;
    effective_balances_kernel(state)?;
    with_altair(state, |view| {
        altair_processing::slashings_reset_kernel(view, constants)?;
        altair_processing::randao_reset_kernel(view, constants)
    })?;
    historical_kernel(state, constants)?;
    with_altair(state, |view| altair_processing::participation_kernel(view, constants))?;
    sync_committee_kernel(state, constants)
}
fn registry_kernel(
    state: &mut BeaconState,
    parameters: &ElectraParameters,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    let current = state.slot / constants.slots_per_epoch;
    let activation = altair_processing::activation_exit_epoch(current)?;
    for index in 0..state.validators.values.len() {
        let validator = &state.validators.values[index];
        if validator.activation_eligibility_epoch == u64::MAX &&
            validator.effective_balance >= common::MIN_ACTIVATION
        {
            state.validators.values[index].activation_eligibility_epoch =
                add(current, 1, "activation eligibility epoch")?;
        } else if is_active(validator, current) &&
            validator.effective_balance <=
                parameters.deneb.capella.bellatrix.altair.phase0.ejection_balance
        {
            common::initiate_validator_exit(state, index as u64, parameters, constants)?;
        } else if validator.activation_eligibility_epoch <= state.finalized_checkpoint.epoch &&
            validator.activation_epoch == u64::MAX
        {
            state.validators.values[index].activation_epoch = activation;
        }
    }
    Ok(())
}
fn slashings_kernel(state: &mut BeaconState, constants: &Constants) -> Result<(), ProcessingError> {
    let epoch = state.slot / constants.slots_per_epoch;
    let total = common::total_active_balance(state, constants)?;
    let slashings = state
        .slashings
        .values
        .iter()
        .try_fold(0, |sum, &balance| add(sum, balance, "total slashings"))?;
    let adjusted = mul(slashings, 3, "adjusted slashings")?.min(total);
    let penalty_per_increment = adjusted / (total / common::INCREMENT);
    let midpoint = add(epoch, constants.slashing_epochs / 2, "slashing midpoint epoch")?;
    for (index, validator) in state.validators.values.iter().enumerate() {
        if validator.slashed && validator.withdrawable_epoch == midpoint {
            let penalty = mul(
                penalty_per_increment,
                validator.effective_balance / common::INCREMENT,
                "slashing correlation penalty",
            )?;
            state.balances.values[index] = state.balances.values[index].saturating_sub(penalty);
        }
    }
    Ok(())
}
fn pending_deposits_kernel(
    state: &mut BeaconState,
    parameters: &ElectraParameters,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    pending_deposits_kernel_with_policy::<true>(state, parameters, constants)
}

fn pending_deposits_kernel_with_policy<const VERIFY_BLS: bool>(
    state: &mut BeaconState,
    parameters: &ElectraParameters,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    crate::common::require_signature_policy::<VERIFY_BLS>()?;
    let next = add(state.slot / constants.slots_per_epoch, 1, "next deposit epoch")?;
    let available = add(
        state.deposit_balance_to_consume,
        common::get_activation_exit_churn_limit(state, parameters, constants)?,
        "pending deposit available churn",
    )?;
    let finalized =
        mul(state.finalized_checkpoint.epoch, constants.slots_per_epoch, "finalized deposit slot")?;
    let deposits = std::mem::take(&mut state.pending_deposits.values);
    let mut processed = 0;
    let mut count = 0;
    let mut postponed = Vec::new();
    let mut hit_churn = false;
    for deposit in &deposits {
        if (deposit.slot > 0 && state.eth1_deposit_index < state.deposit_requests_start_index) ||
            deposit.slot > finalized ||
            count >= 16
        {
            break
        }
        let validator = state.validators.values.iter().find(|v| v.pubkey == deposit.pubkey);
        let withdrawn = validator.is_some_and(|v| v.withdrawable_epoch < next);
        let exited = validator.is_some_and(|v| v.exit_epoch < u64::MAX);
        if withdrawn {
            execution_requests::apply_pending_deposit_kernel_with_policy::<VERIFY_BLS>(
                state, deposit, parameters,
            )?;
        } else if exited {
            postponed.push(deposit.clone());
        } else {
            let amount = add(processed, deposit.amount, "pending deposit processed amount")?;
            hit_churn = amount > available;
            if hit_churn {
                break
            }
            processed = amount;
            execution_requests::apply_pending_deposit_kernel_with_policy::<VERIFY_BLS>(
                state, deposit, parameters,
            )?;
        }
        count += 1;
    }
    state.pending_deposits.values = deposits.into_iter().skip(count).chain(postponed).collect();
    state.deposit_balance_to_consume =
        if hit_churn { sub(available, processed, "remaining deposit churn")? } else { 0 };
    Ok(())
}
fn pending_consolidations_kernel(
    state: &mut BeaconState,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    let next = add(state.slot / constants.slots_per_epoch, 1, "next consolidation epoch")?;
    let mut processed = 0;
    for consolidation in &state.pending_consolidations.values {
        let source = common::index(consolidation.source_index, state.validators.values.len())?;
        let target = common::index(consolidation.target_index, state.validators.values.len())?;
        let validator = &state.validators.values[source];
        if validator.slashed {
            processed += 1;
            continue
        }
        if validator.withdrawable_epoch > next {
            break
        }
        let amount = state.balances.values[source].min(validator.effective_balance);
        state.balances.values[source] = state.balances.values[source].saturating_sub(amount);
        state.balances.values[target] =
            add(state.balances.values[target], amount, "consolidated target balance")?;
        processed += 1;
    }
    state.pending_consolidations.values.drain(..processed);
    Ok(())
}
fn effective_balances_kernel(state: &mut BeaconState) -> Result<(), ProcessingError> {
    let down = common::INCREMENT / 4;
    let up = mul(down, 5, "upward hysteresis threshold")?;
    for (validator, &balance) in state.validators.values.iter_mut().zip(&state.balances.values) {
        if add(balance, down, "downward hysteresis comparison")? < validator.effective_balance ||
            add(validator.effective_balance, up, "upward hysteresis comparison")? < balance
        {
            validator.effective_balance = (balance - balance % common::INCREMENT)
                .min(common::get_max_effective_balance(validator));
        }
    }
    Ok(())
}
fn historical_kernel(
    state: &mut BeaconState,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    let next = add(state.slot / constants.slots_per_epoch, 1, "next historical epoch")?;
    if next.is_multiple_of(constants.historical_slots / constants.slots_per_epoch) {
        if state.historical_summaries.values.len() >= 16_777_216 {
            return Err(ProcessingError::InvalidState("historical summaries limit reached"))
        }
        state.historical_summaries.values.push(HistoricalSummary {
            block_summary_root: state.block_roots.hash_tree_root(constants.preset())?,
            state_summary_root: state.state_roots.hash_tree_root(constants.preset())?,
        });
    }
    Ok(())
}
fn sync_committee_kernel(
    state: &mut BeaconState,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    let next = add(state.slot / constants.slots_per_epoch, 1, "next sync period epoch")?;
    let period = match constants.preset() {
        Preset::Mainnet => 256,
        Preset::Minimal => 8,
    };
    if next.is_multiple_of(period) {
        state.current_sync_committee = state.next_sync_committee.clone();
        state.next_sync_committee = common::next_sync_committee(state, constants)?;
    }
    Ok(())
}
/// Apply the exact ordered Electra epoch transaction atomically.
pub fn process_epoch(
    state: &mut BeaconState,
    parameters: &ElectraParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, constants| {
        epoch_kernel(candidate, parameters, constants)
    })
}
/// Apply exact participation-based justification and finalization atomically.
pub fn process_justification_and_finalization(
    state: &mut BeaconState,
    parameters: &ElectraParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, constants| {
        with_altair(candidate, |view| altair_processing::justification_kernel(view, constants))
    })
}
/// Update inactivity scores according to participation and finality delay.
pub fn process_inactivity_updates(
    state: &mut BeaconState,
    parameters: &ElectraParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, constants| {
        with_altair(candidate, |view| {
            altair_processing::inactivity_kernel(
                view,
                &parameters.deneb.capella.bellatrix.altair,
                constants,
            )
        })
    })
}
/// Apply weighted participation and inactivity balance deltas atomically.
pub fn process_rewards_and_penalties(
    state: &mut BeaconState,
    parameters: &ElectraParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, constants| {
        with_altair(candidate, |view| {
            altair_processing::rewards_kernel(
                view,
                &parameters.deneb.capella.bellatrix.altair,
                constants,
            )
        })
    })
}
/// Apply exact eligibility, balance-based exits and finalized activations.
pub fn process_registry_updates(
    state: &mut BeaconState,
    parameters: &ElectraParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, constants| {
        registry_kernel(candidate, parameters, constants)
    })
}
/// Apply the fork's increment-factored correlated slashing penalties.
pub fn process_slashings(
    state: &mut BeaconState,
    parameters: &ElectraParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, slashings_kernel)
}
/// Reset execution-data voting at the exact period boundary.
pub fn process_eth1_data_reset(
    state: &mut BeaconState,
    parameters: &ElectraParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, constants| {
        with_altair(candidate, |view| altair_processing::eth1_reset_kernel(view, constants))
    })
}
/// Process finalized pending deposits with exact ordering, postponement and balance churn.
pub fn process_pending_deposits(
    state: &mut BeaconState,
    parameters: &ElectraParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, constants| {
        pending_deposits_kernel(candidate, parameters, constants)
    })
}
/// Transfer eligible pending consolidations in order and drop slashed sources.
pub fn process_pending_consolidations(
    state: &mut BeaconState,
    parameters: &ElectraParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, pending_consolidations_kernel)
}
/// Update effective balances with credential-specific limits and exact hysteresis.
pub fn process_effective_balance_updates(
    state: &mut BeaconState,
    parameters: &ElectraParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, _| effective_balances_kernel(candidate))
}
/// Reset the next slashing vector bucket atomically.
pub fn process_slashings_reset(
    state: &mut BeaconState,
    parameters: &ElectraParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, constants| {
        with_altair(candidate, |view| altair_processing::slashings_reset_kernel(view, constants))
    })
}
/// Copy the current RANDAO mix into the next epoch's ring bucket.
pub fn process_randao_mixes_reset(
    state: &mut BeaconState,
    parameters: &ElectraParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, constants| {
        with_altair(candidate, |view| altair_processing::randao_reset_kernel(view, constants))
    })
}
/// Append exact block/state summaries without extending legacy historical roots.
pub fn process_historical_summaries_update(
    state: &mut BeaconState,
    parameters: &ElectraParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, historical_kernel)
}
/// Rotate participation flags and append zero flags for the current registry length.
pub fn process_participation_flag_updates(
    state: &mut BeaconState,
    parameters: &ElectraParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, constants| {
        with_altair(candidate, |view| altair_processing::participation_kernel(view, constants))
    })
}
/// Rotate sync committees and select the next committee with exact16-bit weighting.
pub fn process_sync_committee_updates(
    state: &mut BeaconState,
    parameters: &ElectraParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, sync_committee_kernel)
}
