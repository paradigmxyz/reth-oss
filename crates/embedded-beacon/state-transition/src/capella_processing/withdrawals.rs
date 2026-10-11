//! Exact validator withdrawal sweep and mandatory credential-change signatures.

use super::{
    checked_constants, transact, with_altair, BeaconState, CapellaParameters, ProcessingError,
};
use crate::{
    phase0_processing::{add, sub, MAX_EFFECTIVE_BALANCE},
    ssz::{
        altair,
        capella::{ExecutionPayload, SignedBLSToExecutionChange, Withdrawal},
        phase0::Validator,
        Preset,
    },
};
use reth_embedded_beacon_crypto::{compute_domain, compute_signing_root, verify_signature};
use reth_embedded_beacon_primitives::sha256;

/// Exact ordered sweep output from the pinned specification.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExpectedWithdrawals {
    /// At most the preset withdrawal limit, preserving sweep order and full/partial amounts.
    pub withdrawals: Vec<Withdrawal>,
    /// Actual validators visited before reaching the output or sweep limit.
    pub processed_sweep_withdrawals_count: u64,
}

/// Whether a validator has an execution-address withdrawal credential.
pub const fn has_eth1_withdrawal_credential(validator: &Validator) -> bool {
    validator.withdrawal_credentials[0] == 1
}

/// Whether its entire nonzero balance is eligible at this exact epoch.
pub const fn is_fully_withdrawable_validator(
    validator: &Validator,
    balance: u64,
    epoch: u64,
) -> bool {
    has_eth1_withdrawal_credential(validator) &&
        validator.withdrawable_epoch <= epoch &&
        balance > 0
}

/// Whether its excess over the maximum effective balance is eligible.
pub const fn is_partially_withdrawable_validator(validator: &Validator, balance: u64) -> bool {
    has_eth1_withdrawal_credential(validator) &&
        validator.effective_balance == MAX_EFFECTIVE_BALANCE &&
        balance > MAX_EFFECTIVE_BALANCE
}

const fn withdrawals_limit(preset: Preset) -> usize {
    match preset {
        Preset::Mainnet => 16,
        Preset::Minimal => 4,
    }
}
const fn sweep_limit(preset: Preset) -> u64 {
    match preset {
        Preset::Mainnet => 16_384,
        Preset::Minimal => 16,
    }
}

fn expected(
    validators: &[Validator],
    balances: &[u64],
    epoch: u64,
    next_index: u64,
    next_validator: u64,
    parameters: &CapellaParameters,
) -> Result<ExpectedWithdrawals, ProcessingError> {
    let count = validators.len();
    if count == 0 || balances.len() != count {
        return Err(ProcessingError::InvalidState(
            "withdrawals require a nonempty aligned validator registry",
        ));
    }
    let mut validator_index = usize::try_from(next_validator).map_err(|_| {
        ProcessingError::InvalidState("withdrawal validator index does not fit platform")
    })?;
    if validator_index >= count {
        return Err(ProcessingError::InvalidState("withdrawal validator index is out of range"));
    }
    let limit = withdrawals_limit(parameters.preset());
    let sweep = count.min(usize::try_from(sweep_limit(parameters.preset())).map_err(|_| {
        ProcessingError::InvalidParameters("withdrawal sweep does not fit platform")
    })?);
    let mut output = ExpectedWithdrawals {
        withdrawals: Vec::with_capacity(limit),
        processed_sweep_withdrawals_count: 0,
    };
    let mut withdrawal_index = next_index;
    for _ in 0..sweep {
        if output.withdrawals.len() == limit {
            break;
        }
        let validator = &validators[validator_index];
        let balance = balances[validator_index];
        let amount = if is_fully_withdrawable_validator(validator, balance, epoch) {
            balance
        } else if is_partially_withdrawable_validator(validator, balance) {
            balance - MAX_EFFECTIVE_BALANCE
        } else {
            0
        };
        if amount != 0 {
            let mut address = [0; 20];
            address.copy_from_slice(&validator.withdrawal_credentials[12..]);
            output.withdrawals.push(Withdrawal {
                index: withdrawal_index,
                validator_index: validator_index as u64,
                address,
                amount,
            });
            withdrawal_index = add(withdrawal_index, 1, "withdrawal index")?;
        }
        validator_index = (validator_index + 1) % count;
        output.processed_sweep_withdrawals_count += 1;
    }
    Ok(output)
}

/// Derive the exact withdrawal list without mutating the state or asserting execution validity.
pub fn get_expected_withdrawals(
    state: &BeaconState,
    parameters: &CapellaParameters,
) -> Result<ExpectedWithdrawals, ProcessingError> {
    checked_constants(state, parameters)?;
    expected(
        &state.validators.values,
        &state.balances.values,
        state.slot / parameters.slots_per_epoch(),
        state.next_withdrawal_index,
        state.next_withdrawal_validator_index,
        parameters,
    )
}

/// Shared raw Capella/Deneb kernel; the caller owns the surrounding atomic transaction.
pub(crate) fn withdrawals_kernel(
    state: &mut altair::BeaconState,
    actual: &[Withdrawal],
    next_index: u64,
    next_validator: u64,
    parameters: &CapellaParameters,
) -> Result<(u64, u64), ProcessingError> {
    let expected = expected(
        &state.validators.values,
        &state.balances.values,
        state.slot / parameters.slots_per_epoch(),
        next_index,
        next_validator,
        parameters,
    )?;
    if actual != expected.withdrawals {
        return Err(ProcessingError::InvalidState(
            "execution payload withdrawals differ from the exact sweep",
        ));
    }
    let index = match actual.last() {
        Some(last) => add(last.index, 1, "next withdrawal index")?,
        None => next_index,
    };
    let validator = if actual.len() == withdrawals_limit(parameters.preset()) {
        add(
            actual
                .last()
                .ok_or(ProcessingError::InvalidState("full withdrawal list is empty"))?
                .validator_index,
            1,
            "next withdrawal validator",
        )?
    } else {
        add(next_validator, sweep_limit(parameters.preset()), "next withdrawal sweep")?
    } % state.validators.values.len() as u64;
    for withdrawal in actual {
        let position = usize::try_from(withdrawal.validator_index).map_err(|_| {
            ProcessingError::InvalidState("withdrawal validator index does not fit platform")
        })?;
        let balance = state
            .balances
            .values
            .get_mut(position)
            .ok_or(ProcessingError::InvalidState("withdrawal balance index is out of range"))?;
        *balance = sub(*balance, withdrawal.amount, "withdrawal balance")?;
    }
    Ok((index, validator))
}

/// Validate and atomically apply only the exact withdrawal sweep. Full import also verifies EL.
pub fn process_withdrawals(
    state: &mut BeaconState,
    payload: &ExecutionPayload,
    parameters: &CapellaParameters,
) -> Result<(), ProcessingError> {
    payload.hash_tree_root(parameters.preset())?;
    transact(state, parameters, |candidate, _| {
        let index = candidate.next_withdrawal_index;
        let validator = candidate.next_withdrawal_validator_index;
        let (index, validator) = with_altair(candidate, |common| {
            withdrawals_kernel(common, &payload.withdrawals.values, index, validator, parameters)
        })?;
        candidate.next_withdrawal_index = index;
        candidate.next_withdrawal_validator_index = validator;
        Ok(())
    })
}

/// Shared strict raw credential change; its BLS domain always uses the genesis fork version.
pub(crate) fn bls_to_execution_change_kernel(
    state: &mut altair::BeaconState,
    operation: &SignedBLSToExecutionChange,
    parameters: &CapellaParameters,
) -> Result<(), ProcessingError> {
    bls_to_execution_change_kernel_with_policy::<true>(state, operation, parameters)
}

pub(crate) fn bls_to_execution_change_kernel_with_policy<const VERIFY_BLS: bool>(
    state: &mut altair::BeaconState,
    operation: &SignedBLSToExecutionChange,
    parameters: &CapellaParameters,
) -> Result<(), ProcessingError> {
    operation.hash_tree_root(parameters.preset())?;
    let change = &operation.message;
    let index = usize::try_from(change.validator_index).map_err(|_| {
        ProcessingError::InvalidState("credential-change index does not fit platform")
    })?;
    let validator = state.validators.values.get(index).ok_or(ProcessingError::InvalidState(
        "credential-change validator index is out of range",
    ))?;
    if validator.withdrawal_credentials[0] != 0 ||
        validator.withdrawal_credentials[1..] != sha256(&change.from_bls_pubkey)[1..]
    {
        return Err(ProcessingError::InvalidState(
            "credential-change withdrawal key does not match BLS credentials",
        ));
    }
    let domain = compute_domain(
        [10, 0, 0, 0],
        parameters.bellatrix.altair.phase0.genesis_fork_version,
        state.genesis_validators_root,
    );
    let root = compute_signing_root(change.hash_tree_root(parameters.preset())?, domain);
    if VERIFY_BLS {
        verify_signature(&change.from_bls_pubkey, &root, &operation.signature)?;
    }
    let mut credentials = [0; 32];
    credentials[0] = 1;
    credentials[12..].copy_from_slice(&change.to_execution_address);
    state.validators.values[index].withdrawal_credentials = credentials;
    Ok(())
}

/// Require the withdrawal-key signature before atomically replacing BLS credentials.
pub fn process_bls_to_execution_change(
    state: &mut BeaconState,
    operation: &SignedBLSToExecutionChange,
    parameters: &CapellaParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, _| {
        with_altair(candidate, |common| {
            bls_to_execution_change_kernel(common, operation, parameters)
        })
    })
}
