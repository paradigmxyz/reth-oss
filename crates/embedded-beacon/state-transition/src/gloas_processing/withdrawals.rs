//! Gloas immediate builder/validator withdrawal commitments and ordered sweeps.

use super::{builder, checked_constants, common, transact, GloasParameters};
use crate::{
    phase0_processing::{add, sub, ProcessingError},
    ssz::{
        gloas::{BeaconState, Validator, Withdrawal},
        Preset, ProgressiveList,
    },
};

const MIN_ACTIVATION_BALANCE: u64 = 32_000_000_000;

/// Exact ordered withdrawals plus consumed queue/sweep entries.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExpectedWithdrawals {
    /// Pending builder payment withdrawals consumed before the other sweeps.
    pub processed_builder_withdrawals_count: u64,
    /// Builder registry entries visited, including entries producing no withdrawal.
    pub processed_builders_sweep_count: u64,
    /// Ordered pending partial withdrawals followed by the validator sweep.
    pub withdrawals: Vec<Withdrawal>,
    /// Queue entries consumed, including mature entries that produced no withdrawal.
    pub processed_partial_withdrawals_count: u64,
    /// Registry entries visited before reaching the sweep/output limit.
    pub processed_validators_sweep_count: u64,
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
    parameters: &GloasParameters,
) -> Result<(Vec<Withdrawal>, u64, u64), ProcessingError> {
    checked_constants(state, parameters)?;
    pending_partials(state, withdrawal_index, prior, parameters)
}
fn pending_partials(
    state: &BeaconState,
    mut next: u64,
    prior: &[Withdrawal],
    parameters: &GloasParameters,
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
    parameters: &GloasParameters,
) -> Result<(Vec<Withdrawal>, u64, u64), ProcessingError> {
    checked_constants(state, parameters)?;
    validators_sweep(state, withdrawal_index, prior, parameters)
}
fn validators_sweep(
    state: &BeaconState,
    mut next: u64,
    prior: &[Withdrawal],
    parameters: &GloasParameters,
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

fn builder_withdrawals(
    state: &BeaconState,
    mut next: u64,
    prior: &[Withdrawal],
    parameters: &GloasParameters,
) -> Result<(Vec<Withdrawal>, u64, u64), ProcessingError> {
    let limit = payload_limit(parameters.preset()) - 1;
    if prior.len() > limit {
        return Err(ProcessingError::InvalidState(
            "builder withdrawals require a reserved validator position",
        ));
    }
    let mut output = Vec::new();
    for payment in &state.builder_pending_withdrawals.values {
        if prior.len() + output.len() >= limit {
            break;
        }
        output.push(Withdrawal {
            index: next,
            validator_index: builder::convert_builder_index_to_validator_index(
                payment.builder_index,
            ),
            address: payment.fee_recipient,
            amount: payment.amount,
        });
        next = add(next, 1, "builder withdrawal index")?;
    }
    let processed = output.len() as u64;
    Ok((output, next, processed))
}
fn builders_sweep(
    state: &BeaconState,
    mut next: u64,
    prior: &[Withdrawal],
    parameters: &GloasParameters,
) -> Result<(Vec<Withdrawal>, u64, u64), ProcessingError> {
    let limit = payload_limit(parameters.preset()) - 1;
    if prior.len() > limit {
        return Err(ProcessingError::InvalidState(
            "builder sweep requires a reserved validator position",
        ));
    }
    let count = state.builders.values.len();
    if count == 0 {
        return Ok((Vec::new(), next, 0));
    }
    let mut cursor = common::index(state.next_withdrawal_builder_index, count)?;
    let epoch = state.slot / parameters.slots_per_epoch();
    let mut all = prior.to_vec();
    let mut processed = 0;
    for _ in 0..count.min(sweep_limit(parameters.preset()) as usize) {
        if all.len() >= limit {
            break;
        }
        let builder = &state.builders.values[cursor];
        let flagged = builder::convert_builder_index_to_validator_index(cursor as u64);
        let balance = builder_balance_after_kernel(state, cursor as u64, &all)?;
        if builder.withdrawable_epoch <= epoch && balance > 0 {
            all.push(Withdrawal {
                index: next,
                validator_index: flagged,
                address: builder.execution_address,
                amount: balance,
            });
            next = add(next, 1, "builder sweep withdrawal index")?;
        }
        cursor = (cursor + 1) % count;
        processed = add(processed, 1, "processed builder sweep")?;
    }
    Ok((all.split_off(prior.len()), next, processed))
}
/// Derive queued builder withdrawals before partial and registry sweeps.
pub fn get_builder_withdrawals(
    state: &BeaconState,
    next: u64,
    prior: &[Withdrawal],
    parameters: &GloasParameters,
) -> Result<(Vec<Withdrawal>, u64, u64), ProcessingError> {
    checked_constants(state, parameters)?;
    builder_withdrawals(state, next, prior, parameters)
}
/// Derive mature builder balances while reserving one validator sweep position.
pub fn get_builders_sweep_withdrawals(
    state: &BeaconState,
    next: u64,
    prior: &[Withdrawal],
    parameters: &GloasParameters,
) -> Result<(Vec<Withdrawal>, u64, u64), ProcessingError> {
    checked_constants(state, parameters)?;
    builders_sweep(state, next, prior, parameters)
}
pub(crate) fn expected_kernel(
    state: &BeaconState,
    parameters: &GloasParameters,
) -> Result<ExpectedWithdrawals, ProcessingError> {
    let (mut withdrawals, next, builder_count) =
        builder_withdrawals(state, state.next_withdrawal_index, &[], parameters)?;
    let (partials, next, partial_count) = pending_partials(state, next, &withdrawals, parameters)?;
    withdrawals.extend(partials);
    let (builders, next, builder_sweep_count) =
        builders_sweep(state, next, &withdrawals, parameters)?;
    withdrawals.extend(builders);
    let (validators, _, validator_sweep_count) =
        validators_sweep(state, next, &withdrawals, parameters)?;
    withdrawals.extend(validators);
    Ok(ExpectedWithdrawals {
        withdrawals,
        processed_builder_withdrawals_count: builder_count,
        processed_partial_withdrawals_count: partial_count,
        processed_builders_sweep_count: builder_sweep_count,
        processed_validators_sweep_count: validator_sweep_count,
    })
}
/// Derive the complete ordered builder/partial/registry withdrawals without mutating the state.
pub fn get_expected_withdrawals(
    state: &BeaconState,
    parameters: &GloasParameters,
) -> Result<ExpectedWithdrawals, ProcessingError> {
    checked_constants(state, parameters)?;
    expected_kernel(state, parameters)
}
pub(crate) fn withdrawals_kernel(
    state: &mut BeaconState,
    parameters: &GloasParameters,
) -> Result<(), ProcessingError> {
    // Empty parent payload preserves the existing expected list and all withdrawal cursors.
    if state.latest_block_hash != state.latest_execution_payload_bid.block_hash {
        return Ok(());
    }
    let expected = expected_kernel(state, parameters)?;
    let next_index = match expected.withdrawals.last() {
        Some(last) => add(last.index, 1, "next withdrawal index")?,
        None => state.next_withdrawal_index,
    };
    let validator_cursor = if expected.withdrawals.len() == payload_limit(parameters.preset()) {
        add(
            expected
                .withdrawals
                .last()
                .ok_or(ProcessingError::InvalidState("full withdrawal output is empty"))?
                .validator_index,
            1,
            "next validator withdrawal",
        )?
    } else {
        add(
            state.next_withdrawal_validator_index,
            sweep_limit(parameters.preset()),
            "next validator sweep",
        )?
    } % state.validators.values.len() as u64;
    let builder_cursor = if state.builders.values.is_empty() {
        state.next_withdrawal_builder_index
    } else {
        add(
            state.next_withdrawal_builder_index,
            expected.processed_builders_sweep_count,
            "next builder sweep",
        )? % state.builders.values.len() as u64
    };
    for withdrawal in &expected.withdrawals {
        if builder::is_builder_index(withdrawal.validator_index) {
            let index = common::index(
                builder::convert_validator_index_to_builder_index(withdrawal.validator_index),
                state.builders.values.len(),
            )?;
            state.builders.values[index].balance =
                state.builders.values[index].balance.saturating_sub(withdrawal.amount);
        } else {
            let index = common::index(withdrawal.validator_index, state.balances.values.len())?;
            state.balances.values[index] =
                state.balances.values[index].saturating_sub(withdrawal.amount);
        }
    }
    let builders = usize::try_from(expected.processed_builder_withdrawals_count).map_err(|_| {
        ProcessingError::InvalidState("builder withdrawal queue cursor does not fit platform")
    })?;
    let partials = usize::try_from(expected.processed_partial_withdrawals_count)
        .map_err(|_| ProcessingError::InvalidState("partial queue cursor does not fit platform"))?;
    state.builder_pending_withdrawals.values.drain(..builders);
    state.pending_partial_withdrawals.values.drain(..partials);
    state.next_withdrawal_index = next_index;
    state.next_withdrawal_validator_index = validator_cursor;
    state.next_withdrawal_builder_index = builder_cursor;
    state.payload_expected_withdrawals = ProgressiveList::from_vec(expected.withdrawals);
    Ok(())
}
/// Atomically commit deterministic withdrawals immediately after a FULL parent payload.
/// The current envelope must honor this committed list; no EL data or external verdict is accepted.
pub fn process_withdrawals(
    state: &mut BeaconState,
    parameters: &GloasParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, _| withdrawals_kernel(candidate, parameters))
}

fn builder_balance_after_kernel(
    state: &BeaconState,
    index: u64,
    withdrawals: &[Withdrawal],
) -> Result<u64, ProcessingError> {
    let position = common::index(index, state.builders.values.len())?;
    let flagged = builder::convert_builder_index_to_validator_index(index);
    let withdrawn = withdrawals
        .iter()
        .filter(|w| w.validator_index == flagged)
        .try_fold(0, |sum, w| add(sum, w.amount, "prior builder withdrawals"))?;
    Ok(state.builders.values[position].balance.saturating_sub(withdrawn))
}
/// Account for all earlier withdrawals of this builder, saturating at its remaining balance.
pub fn get_builder_balance_after_withdrawals(
    state: &BeaconState,
    index: u64,
    withdrawals: &[Withdrawal],
    parameters: &GloasParameters,
) -> Result<u64, ProcessingError> {
    checked_constants(state, parameters)?;
    builder_balance_after_kernel(state, index, withdrawals)
}

#[cfg(test)]
mod tests {
    use super::{super::blocks::tests::state_and_keys, *};
    use crate::ssz::gloas::{Builder, BuilderPendingWithdrawal, PendingPartialWithdrawal};

    fn credentials(prefix: u8, address: u8) -> [u8; 32] {
        let mut value = [0; 32];
        value[0] = prefix;
        value[12..].fill(address);
        value
    }
    #[test]
    fn ordered_builder_partial_builder_sweep_and_validator_sweep_deduct_immediately() {
        let (mut state, keys, parameters) = state_and_keys();
        state.builders.values = vec![
            Builder {
                pubkey: keys[0].sk_to_pk().to_bytes(),
                version: 0,
                execution_address: [11; 20],
                balance: 100,
                deposit_epoch: 0,
                withdrawable_epoch: 0,
            },
            Builder {
                pubkey: keys[1].sk_to_pk().to_bytes(),
                version: 0,
                execution_address: [12; 20],
                balance: 200,
                deposit_epoch: 0,
                withdrawable_epoch: 0,
            },
        ];
        state.builder_pending_withdrawals.values.push(BuilderPendingWithdrawal {
            fee_recipient: [9; 20],
            amount: 25,
            builder_index: 0,
        });
        state.validators.values[0].withdrawal_credentials = credentials(2, 3);
        state.validators.values[0].effective_balance = MIN_ACTIVATION_BALANCE;
        state.balances.values[0] = MIN_ACTIVATION_BALANCE + 10;
        state.pending_partial_withdrawals.values.push(PendingPartialWithdrawal {
            validator_index: 0,
            amount: 7,
            withdrawable_epoch: 0,
        });
        state.validators.values[1].withdrawal_credentials = credentials(1, 4);
        state.validators.values[1].withdrawable_epoch = 0;
        state.balances.values[1] = 2;
        let expected = get_expected_withdrawals(&state, &parameters).unwrap();
        let ordered = vec![
            Withdrawal {
                index: 0,
                validator_index: builder::BUILDER_INDEX_FLAG,
                address: [9; 20],
                amount: 25,
            },
            Withdrawal { index: 1, validator_index: 0, address: [3; 20], amount: 7 },
            Withdrawal {
                index: 2,
                validator_index: builder::BUILDER_INDEX_FLAG,
                address: [11; 20],
                amount: 75,
            },
            Withdrawal { index: 3, validator_index: 1, address: [4; 20], amount: 2 },
        ];
        assert_eq!(expected.withdrawals, ordered);
        assert_eq!(expected.processed_builder_withdrawals_count, 1);
        assert_eq!(expected.processed_partial_withdrawals_count, 1);
        assert_eq!(expected.processed_builders_sweep_count, 1);
        assert_eq!(expected.processed_validators_sweep_count, 2);
        process_withdrawals(&mut state, &parameters).unwrap();
        assert_eq!(state.payload_expected_withdrawals.values, ordered);
        assert_eq!(state.builders.values[0].balance, 0);
        assert_eq!(state.builders.values[1].balance, 200);
        assert_eq!(state.balances.values[0], MIN_ACTIVATION_BALANCE + 3);
        assert_eq!(state.balances.values[1], 0);
        assert!(state.pending_partial_withdrawals.values.is_empty());
        assert!(state.builder_pending_withdrawals.values.is_empty());
        assert_eq!(state.next_withdrawal_index, 4);
        assert_eq!(state.next_withdrawal_validator_index, 2);
        assert_eq!(state.next_withdrawal_builder_index, 1);
    }
    #[test]
    fn empty_parent_preserves_committed_withdrawals_queues_balances_and_all_cursors() {
        let (mut state, _, parameters) = state_and_keys();
        state.latest_execution_payload_bid.block_hash = [22; 32];
        state.next_withdrawal_index = 42;
        state.next_withdrawal_validator_index = 7;
        state.next_withdrawal_builder_index = 99;
        state.builder_pending_withdrawals.values.push(BuilderPendingWithdrawal {
            fee_recipient: [8; 20],
            amount: 9,
            builder_index: 100,
        });
        state.payload_expected_withdrawals.values.push(Withdrawal {
            index: 8,
            validator_index: 0,
            address: [8; 20],
            amount: 7,
        });
        let before = state.clone();
        process_withdrawals(&mut state, &parameters).unwrap();
        assert_eq!(state, before);
    }
    #[test]
    fn unknown_builder_and_counter_overflow_fail_without_publishing_partial_deductions() {
        let (mut state, _, parameters) = state_and_keys();
        state.builder_pending_withdrawals.values.push(BuilderPendingWithdrawal {
            fee_recipient: [8; 20],
            amount: 9,
            builder_index: 100,
        });
        let before = state.clone();
        assert!(process_withdrawals(&mut state, &parameters).is_err());
        assert_eq!(state, before);
        state.next_withdrawal_index = u64::MAX;
        let before = state.clone();
        assert!(process_withdrawals(&mut state, &parameters).is_err());
        assert_eq!(state, before);
        assert_eq!(
            builder::convert_validator_index_to_builder_index(
                builder::convert_builder_index_to_validator_index(3)
            ),
            3
        );
        assert!(!builder::is_builder_index(3));
    }
}
