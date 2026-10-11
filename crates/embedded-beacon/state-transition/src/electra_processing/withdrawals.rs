//! Electra pending partial withdrawals and credential-specific validator sweeps.

use super::{checked_constants, common, transact, ElectraParameters};
use crate::{
    phase0_processing::{add, sub, ProcessingError},
    ssz::{
        electra::{BeaconState, ExecutionPayload, Validator, Withdrawal},
        Preset,
    },
};

const MIN_ACTIVATION_BALANCE: u64 = 32_000_000_000;

/// Exact ordered withdrawals plus consumed queue/sweep entries.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExpectedWithdrawals {
    /// Ordered pending partial withdrawals followed by the validator sweep.
    pub withdrawals: Vec<Withdrawal>,
    /// Queue entries consumed, including mature entries that produced no withdrawal.
    pub processed_partial_withdrawals_count: u64,
    /// Registry entries visited before reaching the sweep/output limit.
    pub processed_sweep_withdrawals_count: u64,
}

const fn payload_limit(preset: Preset) -> usize {
    match preset {
        Preset::Mainnet => 16,
        Preset::Minimal => 4,
    }
}
const fn partial_limit(preset: Preset) -> usize {
    match preset {
        Preset::Mainnet => 8,
        Preset::Minimal => 2,
    }
}
const fn sweep_limit(preset: Preset) -> u64 {
    match preset {
        Preset::Mainnet => 16_384,
        Preset::Minimal => 16,
    }
}

/// Eligibility for the entire balance, including compounding credentials.
pub const fn is_fully_withdrawable_validator(
    validator: &Validator,
    balance: u64,
    epoch: u64,
) -> bool {
    common::has_execution_withdrawal_credential(validator) &&
        validator.withdrawable_epoch <= epoch &&
        balance > 0
}
/// Eligibility for a sweep of excess over this credential's exact effective-balance maximum.
pub const fn is_partially_withdrawable_validator(validator: &Validator, balance: u64) -> bool {
    let maximum = common::get_max_effective_balance(validator);
    common::has_execution_withdrawal_credential(validator) &&
        validator.effective_balance == maximum &&
        balance > maximum
}
/// Eligibility for a queued partial withdrawal, independently of its requested amount.
pub const fn is_eligible_for_partial_withdrawals(validator: &Validator, balance: u64) -> bool {
    validator.exit_epoch == u64::MAX &&
        validator.effective_balance >= MIN_ACTIVATION_BALANCE &&
        balance > MIN_ACTIVATION_BALANCE
}

fn balance_after(
    state: &BeaconState,
    index: u64,
    withdrawals: &[Withdrawal],
) -> Result<u64, ProcessingError> {
    let position = usize::try_from(index)
        .map_err(|_| ProcessingError::InvalidState("withdrawal index does not fit platform"))?;
    let balance = *state
        .balances
        .values
        .get(position)
        .ok_or(ProcessingError::InvalidState("withdrawal balance index is out of range"))?;
    let withdrawn = withdrawals
        .iter()
        .filter(|item| item.validator_index == index)
        .try_fold(0, |sum, item| add(sum, item.amount, "prior withdrawals sum"))?;
    sub(balance, withdrawn, "balance after prior withdrawals")
}
fn append_withdrawal(
    output: &mut Vec<Withdrawal>,
    next: &mut u64,
    index: u64,
    validator: &Validator,
    amount: u64,
) -> Result<(), ProcessingError> {
    let mut address = [0; 20];
    address.copy_from_slice(&validator.withdrawal_credentials[12..]);
    output.push(Withdrawal { index: *next, validator_index: index, address, amount });
    *next = add(*next, 1, "withdrawal index")?;
    Ok(())
}

/// Derive mature pending partial withdrawals while reserving one validator-sweep position.
/// Prior withdrawals are accounted for without cloning the validator registry.
pub fn get_pending_partial_withdrawals(
    state: &BeaconState,
    withdrawal_index: u64,
    prior: &[Withdrawal],
    parameters: &ElectraParameters,
) -> Result<(Vec<Withdrawal>, u64, u64), ProcessingError> {
    checked_constants(state, parameters)?;
    pending_partials(state, withdrawal_index, prior, parameters)
}
fn pending_partials(
    state: &BeaconState,
    mut next: u64,
    prior: &[Withdrawal],
    parameters: &ElectraParameters,
) -> Result<(Vec<Withdrawal>, u64, u64), ProcessingError> {
    let limit = prior
        .len()
        .checked_add(partial_limit(parameters.preset()))
        .ok_or(ProcessingError::InvalidState("partial withdrawal limit overflow"))?
        .min(payload_limit(parameters.preset()) - 1);
    if prior.len() > limit {
        return Err(ProcessingError::InvalidState("no reserved validator withdrawal position"));
    }
    let epoch = state.slot / parameters.slots_per_epoch();
    let mut all = prior.to_vec();
    let mut processed = 0;
    for pending in &state.pending_partial_withdrawals.values {
        if pending.withdrawable_epoch > epoch || all.len() >= limit {
            break;
        }
        let index = usize::try_from(pending.validator_index).map_err(|_| {
            ProcessingError::InvalidState("partial withdrawal index does not fit platform")
        })?;
        let validator =
            state.validators.values.get(index).ok_or(ProcessingError::InvalidState(
                "partial withdrawal validator is out of range",
            ))?;
        let balance = balance_after(state, pending.validator_index, &all)?;
        if is_eligible_for_partial_withdrawals(validator, balance) {
            append_withdrawal(
                &mut all,
                &mut next,
                pending.validator_index,
                validator,
                (balance - MIN_ACTIVATION_BALANCE).min(pending.amount),
            )?;
        }
        processed = add(processed, 1, "processed partial withdrawals")?;
    }
    Ok((all.split_off(prior.len()), next, processed))
}

/// Derive the credential-specific sweep after accounting for prior pending withdrawals.
pub fn get_validators_sweep_withdrawals(
    state: &BeaconState,
    withdrawal_index: u64,
    prior: &[Withdrawal],
    parameters: &ElectraParameters,
) -> Result<(Vec<Withdrawal>, u64, u64), ProcessingError> {
    checked_constants(state, parameters)?;
    validators_sweep(state, withdrawal_index, prior, parameters)
}
fn validators_sweep(
    state: &BeaconState,
    mut next: u64,
    prior: &[Withdrawal],
    parameters: &ElectraParameters,
) -> Result<(Vec<Withdrawal>, u64, u64), ProcessingError> {
    let count = state.validators.values.len();
    let limit = payload_limit(parameters.preset());
    if count == 0 || prior.len() >= limit {
        return Err(ProcessingError::InvalidState(
            "withdrawal sweep requires a registry and reserved output position",
        ));
    }
    let mut cursor = usize::try_from(state.next_withdrawal_validator_index)
        .map_err(|_| ProcessingError::InvalidState("sweep index does not fit platform"))?;
    if cursor >= count {
        return Err(ProcessingError::InvalidState("withdrawal sweep cursor is out of range"));
    }
    let epoch = state.slot / parameters.slots_per_epoch();
    let mut all = prior.to_vec();
    let mut processed = 0;
    for _ in 0..count.min(sweep_limit(parameters.preset()) as usize) {
        if all.len() >= limit {
            break;
        }
        let validator = &state.validators.values[cursor];
        let balance = balance_after(state, cursor as u64, &all)?;
        let amount = if is_fully_withdrawable_validator(validator, balance, epoch) {
            Some(balance)
        } else if is_partially_withdrawable_validator(validator, balance) {
            Some(balance - common::get_max_effective_balance(validator))
        } else {
            None
        };
        if let Some(amount) = amount {
            append_withdrawal(&mut all, &mut next, cursor as u64, validator, amount)?;
        }
        cursor = (cursor + 1) % count;
        processed = add(processed, 1, "processed validator withdrawals")?;
    }
    Ok((all.split_off(prior.len()), next, processed))
}

pub(crate) fn expected_kernel(
    state: &BeaconState,
    parameters: &ElectraParameters,
) -> Result<ExpectedWithdrawals, ProcessingError> {
    let (mut withdrawals, next, partial_count) =
        pending_partials(state, state.next_withdrawal_index, &[], parameters)?;
    let (sweep, _, sweep_count) = validators_sweep(state, next, &withdrawals, parameters)?;
    withdrawals.extend(sweep);
    Ok(ExpectedWithdrawals {
        withdrawals,
        processed_partial_withdrawals_count: partial_count,
        processed_sweep_withdrawals_count: sweep_count,
    })
}
/// Derive exact queue-plus-sweep withdrawals without asserting execution validity.
pub fn get_expected_withdrawals(
    state: &BeaconState,
    parameters: &ElectraParameters,
) -> Result<ExpectedWithdrawals, ProcessingError> {
    checked_constants(state, parameters)?;
    expected_kernel(state, parameters)
}
pub(crate) fn withdrawals_kernel(
    state: &mut BeaconState,
    payload: &ExecutionPayload,
    parameters: &ElectraParameters,
) -> Result<(), ProcessingError> {
    let expected = expected_kernel(state, parameters)?;
    if payload.withdrawals.values != expected.withdrawals {
        return Err(ProcessingError::InvalidState(
            "execution withdrawals differ from exact Electra queue and sweep",
        ));
    }
    let next_index = match expected.withdrawals.last() {
        Some(last) => add(last.index, 1, "next withdrawal index")?,
        None => state.next_withdrawal_index,
    };
    let cursor = if expected.withdrawals.len() == payload_limit(parameters.preset()) {
        add(
            expected
                .withdrawals
                .last()
                .ok_or(ProcessingError::InvalidState("full withdrawal output is empty"))?
                .validator_index,
            1,
            "next withdrawal validator",
        )?
    } else {
        add(
            state.next_withdrawal_validator_index,
            sweep_limit(parameters.preset()),
            "next withdrawal sweep",
        )?
    } % state.validators.values.len() as u64;
    for withdrawal in &expected.withdrawals {
        let index = usize::try_from(withdrawal.validator_index)
            .map_err(|_| ProcessingError::InvalidState("withdrawal index does not fit platform"))?;
        let balance = state
            .balances
            .values
            .get_mut(index)
            .ok_or(ProcessingError::InvalidState("withdrawal balance index is out of range"))?;
        *balance = sub(*balance, withdrawal.amount, "applied withdrawal amount")?;
    }
    let processed = usize::try_from(expected.processed_partial_withdrawals_count)
        .map_err(|_| ProcessingError::InvalidState("partial queue cursor does not fit platform"))?;
    state.pending_partial_withdrawals.values.drain(..processed);
    state.next_withdrawal_index = next_index;
    state.next_withdrawal_validator_index = cursor;
    Ok(())
}
/// Atomically apply only the exact Electra withdrawal operation; complete import verifies EL.
pub fn process_withdrawals(
    state: &mut BeaconState,
    payload: &ExecutionPayload,
    parameters: &ElectraParameters,
) -> Result<(), ProcessingError> {
    payload.hash_tree_root(parameters.preset())?;
    transact(state, parameters, |candidate, _| withdrawals_kernel(candidate, payload, parameters))
}

#[cfg(test)]
mod tests {
    use super::{
        super::blocks::tests::{payload, state_and_keys},
        *,
    };
    use crate::ssz::{electra::PendingPartialWithdrawal, List};

    fn credentials(prefix: u8, address: u8) -> [u8; 32] {
        let mut value = [0; 32];
        value[0] = prefix;
        value[12..].fill(address);
        value
    }
    #[test]
    fn duplicate_partial_requests_account_for_prior_withdrawals_before_the_validator_sweep() {
        let (mut state, _, parameters) = state_and_keys();
        state.validators.values[0].withdrawal_credentials = credentials(2, 4);
        state.validators.values[0].effective_balance = 2_048_000_000_000;
        state.balances.values[0] = 100_000_000_000;
        state.validators.values[1].withdrawal_credentials = credentials(1, 5);
        state.balances.values[1] = 33_000_000_000;
        state.pending_partial_withdrawals = List::from_vec(vec![
            PendingPartialWithdrawal {
                validator_index: 0,
                amount: 60_000_000_000,
                withdrawable_epoch: 0,
            },
            PendingPartialWithdrawal {
                validator_index: 0,
                amount: 60_000_000_000,
                withdrawable_epoch: 0,
            },
        ]);
        let expected = get_expected_withdrawals(&state, &parameters).unwrap();
        assert_eq!(
            expected.withdrawals.iter().map(|w| (w.validator_index, w.amount)).collect::<Vec<_>>(),
            vec![(0, 60_000_000_000), (0, 8_000_000_000), (1, 1_000_000_000)]
        );
        assert_eq!(expected.processed_partial_withdrawals_count, 2);
        assert_eq!(expected.processed_sweep_withdrawals_count, 16);
        let correct = payload(&state, &parameters);
        let mut incorrect = correct.clone();
        incorrect.withdrawals.values[1].amount += 1;
        let before = state.clone();
        assert!(process_withdrawals(&mut state, &incorrect, &parameters).is_err());
        assert_eq!(state, before);
        process_withdrawals(&mut state, &correct, &parameters).unwrap();
        assert_eq!(state.balances.values[0], 32_000_000_000);
        assert_eq!(state.balances.values[1], 32_000_000_000);
        assert_eq!(state.next_withdrawal_index, 3);
        assert_eq!(state.next_withdrawal_validator_index, 16);
        assert!(state.pending_partial_withdrawals.values.is_empty());
    }
    #[test]
    fn mature_ineligible_queue_entries_are_consumed_but_future_entries_stop_processing() {
        let (mut state, _, parameters) = state_and_keys();
        state.validators.values[0].exit_epoch = 0;
        state.pending_partial_withdrawals = List::from_vec(vec![
            PendingPartialWithdrawal { validator_index: 0, amount: 5, withdrawable_epoch: 0 },
            PendingPartialWithdrawal { validator_index: 1, amount: 5, withdrawable_epoch: 1 },
            PendingPartialWithdrawal { validator_index: 2, amount: 5, withdrawable_epoch: 0 },
        ]);
        let expected = get_expected_withdrawals(&state, &parameters).unwrap();
        assert_eq!(expected.processed_partial_withdrawals_count, 1);
        assert!(expected.withdrawals.is_empty());
        let payload = payload(&state, &parameters);
        process_withdrawals(&mut state, &payload, &parameters).unwrap();
        assert_eq!(state.pending_partial_withdrawals.values.len(), 2);
        assert_eq!(state.pending_partial_withdrawals.values[0].validator_index, 1);
    }
    #[test]
    fn compounding_sweep_uses_2048_eth_maximum_and_pending_entries_reserve_sweep_space() {
        let (mut state, _, parameters) = state_and_keys();
        state.validators.values[0].withdrawal_credentials = credentials(2, 4);
        state.validators.values[0].effective_balance = 2_048_000_000_000;
        state.balances.values[0] = 2_048_000_000_007;
        state.pending_partial_withdrawals = List::from_vec(vec![
            PendingPartialWithdrawal {
                validator_index: 1,
                amount: 1,
                withdrawable_epoch: 0
            };
            3
        ]);
        state.balances.values[1] = 32_000_000_010;
        let expected = get_expected_withdrawals(&state, &parameters).unwrap();
        assert_eq!(expected.processed_partial_withdrawals_count, 2);
        assert_eq!(expected.withdrawals.len(), 3);
        assert_eq!(expected.withdrawals[2].validator_index, 0);
        assert_eq!(expected.withdrawals[2].amount, 7);
        let payload = payload(&state, &parameters);
        process_withdrawals(&mut state, &payload, &parameters).unwrap();
        assert_eq!(state.balances.values[0], 2_048_000_000_000);
        assert_eq!(state.pending_partial_withdrawals.values.len(), 1);
    }
}
