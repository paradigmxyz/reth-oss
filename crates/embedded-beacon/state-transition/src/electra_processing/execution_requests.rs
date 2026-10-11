//! Electra execution requests, delayed deposits and authorized exit/consolidation queues.

use super::{checked_constants, common, transact, ElectraParameters};
use crate::{
    is_valid_merkle_branch,
    phase0_processing::{add, Constants, ProcessingError},
    ssz::{
        electra::{
            self, BeaconState, ConsolidationRequest, Deposit, DepositRequest, PendingDeposit,
            WithdrawalRequest,
        },
        Preset, Ssz,
    },
};
use reth_embedded_beacon_crypto::{compute_domain, compute_signing_root, verify_signature};
use reth_embedded_beacon_primitives::{bytes_root, merkleize, uint64_root, Root};

const MIN_ACTIVATION_BALANCE: u64 = 32_000_000_000;
const PENDING_DEPOSITS_LIMIT: usize = 134_217_728;
const VALIDATOR_REGISTRY_LIMIT: u64 = 1_099_511_627_776;
const fn partial_queue_limit(preset: Preset) -> usize {
    match preset {
        Preset::Mainnet => 134_217_728,
        Preset::Minimal => 64,
    }
}
const fn consolidation_queue_limit(preset: Preset) -> usize {
    match preset {
        Preset::Mainnet => 262_144,
        Preset::Minimal => 64,
    }
}
fn validator_index(state: &BeaconState, pubkey: &[u8; 48]) -> Option<usize> {
    state.validators.values.iter().position(|validator| &validator.pubkey == pubkey)
}
const fn active(validator: &electra::Validator, epoch: u64) -> bool {
    validator.activation_epoch <= epoch && epoch < validator.exit_epoch
}
const fn require_pending_capacity(state: &BeaconState) -> Result<(), ProcessingError> {
    if state.pending_deposits.values.len() >= PENDING_DEPOSITS_LIMIT {
        return Err(ProcessingError::InvalidState("pending deposit queue limit reached"));
    }
    Ok(())
}

/// Canonical EIP-7685 typed groups, omitting empty groups and preserving request order.
pub fn get_execution_requests_list(
    requests: &electra::ExecutionRequests,
    preset: Preset,
) -> Result<Vec<Vec<u8>>, ProcessingError> {
    requests.hash_tree_root(preset)?;
    let mut output = Vec::with_capacity(3);
    macro_rules! group {
        ($tag:expr,$values:expr) => {
            if !$values.values.is_empty() {
                let mut bytes = vec![$tag];
                $values.write_ssz(preset, &mut bytes)?;
                output.push(bytes);
            }
        };
    }
    group!(0, requests.deposits);
    group!(1, requests.withdrawals);
    group!(2, requests.consolidations);
    Ok(output)
}

/// Check only a new deposit's proof of possession using the fork-agnostic deposit domain.
pub fn is_valid_deposit_signature(
    pubkey: [u8; 48],
    credentials: Root,
    amount: u64,
    signature: [u8; 96],
    parameters: &ElectraParameters,
) -> Result<bool, ProcessingError> {
    parameters.validate()?;
    let message =
        merkleize(&[bytes_root(&pubkey, 48, false)?, credentials, uint64_root(amount)], 3)?;
    let domain = compute_domain(
        [3, 0, 0, 0],
        parameters.deneb.capella.bellatrix.altair.phase0.genesis_fork_version,
        [0; 32],
    );
    Ok(verify_signature(&pubkey, &compute_signing_root(message, domain), &signature).is_ok())
}
/// Build a validator with the correct credential-specific effective-balance maximum.
pub fn get_validator_from_deposit(
    pubkey: [u8; 48],
    credentials: Root,
    amount: u64,
) -> electra::Validator {
    let mut validator = electra::Validator {
        pubkey,
        withdrawal_credentials: credentials,
        effective_balance: 0,
        slashed: false,
        activation_eligibility_epoch: u64::MAX,
        activation_epoch: u64::MAX,
        exit_epoch: u64::MAX,
        withdrawable_epoch: u64::MAX,
    };
    validator.effective_balance =
        (amount - amount % 1_000_000_000).min(common::get_max_effective_balance(&validator));
    validator
}
fn add_validator(
    state: &mut BeaconState,
    pubkey: [u8; 48],
    credentials: Root,
    amount: u64,
) -> Result<(), ProcessingError> {
    if state.validators.values.len() as u64 >= VALIDATOR_REGISTRY_LIMIT {
        return Err(ProcessingError::InvalidState("validator registry limit reached"));
    }
    state.validators.values.push(get_validator_from_deposit(pubkey, credentials, amount));
    state.balances.values.push(amount);
    state.previous_epoch_participation.values.push(0);
    state.current_epoch_participation.values.push(0);
    state.inactivity_scores.values.push(0);
    Ok(())
}

/// Raw epoch helper: new keys require a proof of possession; existing keys receive a top-up.
pub(crate) fn apply_pending_deposit_kernel(
    state: &mut BeaconState,
    deposit: &PendingDeposit,
    parameters: &ElectraParameters,
) -> Result<(), ProcessingError> {
    apply_pending_deposit_kernel_with_policy::<true>(state, deposit, parameters)
}

pub(crate) fn apply_pending_deposit_kernel_with_policy<const VERIFY_BLS: bool>(
    state: &mut BeaconState,
    deposit: &PendingDeposit,
    parameters: &ElectraParameters,
) -> Result<(), ProcessingError> {
    if let Some(index) = validator_index(state, &deposit.pubkey) {
        state.balances.values[index] =
            add(state.balances.values[index], deposit.amount, "pending deposit balance")?;
    } else if !VERIFY_BLS ||
        is_valid_deposit_signature(
            deposit.pubkey,
            deposit.withdrawal_credentials,
            deposit.amount,
            deposit.signature,
            parameters,
        )?
    {
        add_validator(state, deposit.pubkey, deposit.withdrawal_credentials, deposit.amount)?;
    }
    Ok(())
}
/// Apply only one delayed deposit atomically, independently of epoch queue/churn eligibility.
pub fn apply_pending_deposit(
    state: &mut BeaconState,
    deposit: &PendingDeposit,
    parameters: &ElectraParameters,
) -> Result<(), ProcessingError> {
    deposit.hash_tree_root(parameters.preset())?;
    transact(state, parameters, |candidate, _| {
        apply_pending_deposit_kernel(candidate, deposit, parameters)
    })
}

pub(crate) fn apply_deposit_kernel(
    state: &mut BeaconState,
    pubkey: [u8; 48],
    credentials: Root,
    amount: u64,
    signature: [u8; 96],
    parameters: &ElectraParameters,
) -> Result<(), ProcessingError> {
    if validator_index(state, &pubkey).is_none() {
        if !is_valid_deposit_signature(pubkey, credentials, amount, signature, parameters)? {
            return Ok(());
        }
        require_pending_capacity(state)?;
        add_validator(state, pubkey, credentials, 0)?;
    }
    require_pending_capacity(state)?;
    state.pending_deposits.values.push(PendingDeposit {
        pubkey,
        withdrawal_credentials: credentials,
        amount,
        signature,
        slot: 0,
    });
    Ok(())
}
pub(crate) fn deposit_kernel(
    state: &mut BeaconState,
    deposit: &Deposit,
    parameters: &ElectraParameters,
) -> Result<(), ProcessingError> {
    if !is_valid_merkle_branch(
        deposit.data.hash_tree_root(parameters.preset())?,
        &deposit.proof.values,
        33,
        state.eth1_deposit_index,
        state.eth1_data.deposit_root,
    ) {
        return Err(ProcessingError::InvalidState("deposit Merkle proof is invalid"));
    }
    state.eth1_deposit_index = add(state.eth1_deposit_index, 1, "ordered deposit index")?;
    apply_deposit_kernel(
        state,
        deposit.data.pubkey,
        deposit.data.withdrawal_credentials,
        deposit.data.amount,
        deposit.data.signature,
        parameters,
    )
}
/// Verify a legacy deposit proof, consume its index and queue its delayed balance atomically.
pub fn process_deposit(
    state: &mut BeaconState,
    deposit: &Deposit,
    parameters: &ElectraParameters,
) -> Result<(), ProcessingError> {
    deposit.hash_tree_root(parameters.preset())?;
    transact(state, parameters, |candidate, _| deposit_kernel(candidate, deposit, parameters))
}

pub(crate) fn deposit_request_kernel(
    state: &mut BeaconState,
    request: &DepositRequest,
) -> Result<(), ProcessingError> {
    require_pending_capacity(state)?;
    if state.deposit_requests_start_index == u64::MAX {
        state.deposit_requests_start_index = request.index;
    }
    state.pending_deposits.values.push(PendingDeposit {
        pubkey: request.pubkey,
        withdrawal_credentials: request.withdrawal_credentials,
        amount: request.amount,
        signature: request.signature,
        slot: state.slot,
    });
    Ok(())
}
/// Queue an EL deposit request; signature validity is checked when its delayed deposit applies.
/// Calling this isolated operation does not establish that an engine authenticated the request.
pub fn process_deposit_request(
    state: &mut BeaconState,
    request: &DepositRequest,
    parameters: &ElectraParameters,
) -> Result<(), ProcessingError> {
    request.hash_tree_root(parameters.preset())?;
    transact(state, parameters, |candidate, _| deposit_request_kernel(candidate, request))
}

pub(crate) fn withdrawal_request_kernel(
    state: &mut BeaconState,
    request: &WithdrawalRequest,
    parameters: &ElectraParameters,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    let full = request.amount == 0;
    if state.pending_partial_withdrawals.values.len() >= partial_queue_limit(parameters.preset()) &&
        !full
    {
        return Ok(());
    }
    let Some(index) = validator_index(state, &request.validator_pubkey) else {
        return Ok(());
    };
    let validator = &state.validators.values[index];
    let epoch = state.slot / parameters.slots_per_epoch();
    if !common::has_execution_withdrawal_credential(validator) ||
        validator.withdrawal_credentials[12..] != request.source_address ||
        !active(validator, epoch) ||
        validator.exit_epoch != u64::MAX
    {
        return Ok(());
    }
    let maturity = add(
        validator.activation_epoch,
        parameters.deneb.capella.bellatrix.altair.phase0.shard_committee_period,
        "withdrawal request maturity",
    )?;
    if epoch < maturity {
        return Ok(());
    }
    let pending = common::get_pending_balance_to_withdraw(state, index as u64)?;
    if full {
        if pending == 0 {
            common::initiate_validator_exit(state, index as u64, parameters, constants)?;
        }
        return Ok(());
    }
    let reserved = add(MIN_ACTIVATION_BALANCE, pending, "pending withdrawal reserved balance")?;
    if common::has_compounding_withdrawal_credential(validator) &&
        validator.effective_balance >= MIN_ACTIVATION_BALANCE &&
        state.balances.values[index] > reserved
    {
        let amount = (state.balances.values[index] - reserved).min(request.amount);
        let exit_epoch =
            common::compute_exit_epoch_and_update_churn(state, amount, parameters, constants)?;
        let withdrawable_epoch = add(
            exit_epoch,
            parameters.deneb.capella.bellatrix.altair.phase0.min_validator_withdrawability_delay,
            "partial withdrawable epoch",
        )?;
        state.pending_partial_withdrawals.values.push(electra::PendingPartialWithdrawal {
            validator_index: index as u64,
            amount,
            withdrawable_epoch,
        });
    }
    Ok(())
}
/// Process an authorized EL withdrawal request; malformed authorization is ignored as specified.
pub fn process_withdrawal_request(
    state: &mut BeaconState,
    request: &WithdrawalRequest,
    parameters: &ElectraParameters,
) -> Result<(), ProcessingError> {
    request.hash_tree_root(parameters.preset())?;
    transact(state, parameters, |candidate, constants| {
        withdrawal_request_kernel(candidate, request, parameters, constants)
    })
}

/// Whether a self-consolidation request may switch an active execution validator to compounding.
pub fn is_valid_switch_to_compounding_request(
    state: &BeaconState,
    request: &ConsolidationRequest,
    parameters: &ElectraParameters,
) -> Result<bool, ProcessingError> {
    checked_constants(state, parameters)?;
    Ok(valid_switch(state, request, parameters))
}
fn valid_switch(
    state: &BeaconState,
    request: &ConsolidationRequest,
    parameters: &ElectraParameters,
) -> bool {
    if request.source_pubkey != request.target_pubkey {
        return false;
    }
    let Some(index) = validator_index(state, &request.source_pubkey) else {
        return false;
    };
    let source = &state.validators.values[index];
    source.withdrawal_credentials[0] == 1 &&
        source.withdrawal_credentials[12..] == request.source_address &&
        active(source, state.slot / parameters.slots_per_epoch()) &&
        source.exit_epoch == u64::MAX
}
pub(crate) fn consolidation_request_kernel(
    state: &mut BeaconState,
    request: &ConsolidationRequest,
    parameters: &ElectraParameters,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    if valid_switch(state, request, parameters) {
        let index = validator_index(state, &request.source_pubkey)
            .ok_or(ProcessingError::InvalidState("authorized compounding validator disappeared"))?;
        common::switch_to_compounding_validator(state, index as u64, parameters, constants)?;
        return Ok(());
    }
    if request.source_pubkey == request.target_pubkey ||
        state.pending_consolidations.values.len() >=
            consolidation_queue_limit(parameters.preset()) ||
        common::get_consolidation_churn_limit(state, parameters, constants)? <=
            MIN_ACTIVATION_BALANCE
    {
        return Ok(());
    }
    let (Some(source), Some(target)) = (
        validator_index(state, &request.source_pubkey),
        validator_index(state, &request.target_pubkey),
    ) else {
        return Ok(());
    };
    let validator = &state.validators.values[source];
    let target_validator = &state.validators.values[target];
    let epoch = state.slot / parameters.slots_per_epoch();
    if !common::has_execution_withdrawal_credential(validator) ||
        validator.withdrawal_credentials[12..] != request.source_address ||
        !common::has_compounding_withdrawal_credential(target_validator) ||
        !active(validator, epoch) ||
        !active(target_validator, epoch) ||
        validator.exit_epoch != u64::MAX ||
        target_validator.exit_epoch != u64::MAX
    {
        return Ok(());
    }
    if epoch <
        add(
            validator.activation_epoch,
            parameters.deneb.capella.bellatrix.altair.phase0.shard_committee_period,
            "consolidation maturity",
        )? ||
        common::get_pending_balance_to_withdraw(state, source as u64)? > 0
    {
        return Ok(());
    }
    let balance = validator.effective_balance;
    let exit_epoch = common::compute_consolidation_epoch_and_update_churn(
        state, balance, parameters, constants,
    )?;
    let withdrawable = add(
        exit_epoch,
        parameters.deneb.capella.bellatrix.altair.phase0.min_validator_withdrawability_delay,
        "consolidation withdrawable epoch",
    )?;
    state.validators.values[source].exit_epoch = exit_epoch;
    state.validators.values[source].withdrawable_epoch = withdrawable;
    state.pending_consolidations.values.push(electra::PendingConsolidation {
        source_index: source as u64,
        target_index: target as u64,
    });
    Ok(())
}
/// Process authorized self-switch or two-validator consolidation with balance-based churn.
pub fn process_consolidation_request(
    state: &mut BeaconState,
    request: &ConsolidationRequest,
    parameters: &ElectraParameters,
) -> Result<(), ProcessingError> {
    request.hash_tree_root(parameters.preset())?;
    transact(state, parameters, |candidate, constants| {
        consolidation_request_kernel(candidate, request, parameters, constants)
    })
}

#[cfg(test)]
mod tests {
    use super::{super::blocks::tests::state_and_keys, *};
    use crate::ssz::List;
    use blst::min_pk::SecretKey;
    use reth_embedded_beacon_crypto::ETHEREUM_BLS_DST;

    fn credentials(prefix: u8, address: u8) -> Root {
        let mut root = [0; 32];
        root[0] = prefix;
        root[12..].fill(address);
        root
    }
    fn mature(state: &mut BeaconState, parameters: &ElectraParameters) {
        state.slot = parameters.deneb.capella.bellatrix.altair.phase0.shard_committee_period *
            parameters.slots_per_epoch();
    }
    #[test]
    fn requests_are_encoded_as_ordered_nonempty_type_prefixed_fixed_width_groups() {
        let requests = electra::ExecutionRequests {
            deposits: List::default(),
            withdrawals: List::from_vec(vec![WithdrawalRequest {
                source_address: [3; 20],
                validator_pubkey: [4; 48],
                amount: 0x1122,
            }]),
            consolidations: List::from_vec(vec![ConsolidationRequest {
                source_address: [5; 20],
                source_pubkey: [6; 48],
                target_pubkey: [7; 48],
            }]),
        };
        let groups = get_execution_requests_list(&requests, Preset::Minimal).unwrap();
        let mut withdrawal = vec![1];
        withdrawal.extend_from_slice(&[3; 20]);
        withdrawal.extend_from_slice(&[4; 48]);
        withdrawal.extend_from_slice(&0x1122u64.to_le_bytes());
        let mut consolidation = vec![2];
        consolidation.extend_from_slice(&[5; 20]);
        consolidation.extend_from_slice(&[6; 48]);
        consolidation.extend_from_slice(&[7; 48]);
        assert_eq!(groups, vec![withdrawal, consolidation]);
        let mut invalid = requests;
        invalid.consolidations.values = vec![invalid.consolidations.values[0].clone(); 3];
        assert!(get_execution_requests_list(&invalid, Preset::Minimal).is_err());
    }
    #[test]
    fn partial_requests_reserve_previous_pending_balance_and_full_exit_waits_for_queue() {
        let (mut state, _, parameters) = state_and_keys();
        mature(&mut state, &parameters);
        state.validators.values[0].withdrawal_credentials = credentials(2, 10);
        state.balances.values[0] = 42_000_000_000;
        let request = WithdrawalRequest {
            source_address: [10; 20],
            validator_pubkey: state.validators.values[0].pubkey,
            amount: 8_000_000_000,
        };
        let mut unauthorized = request.clone();
        unauthorized.source_address = [11; 20];
        let before = state.clone();
        process_withdrawal_request(&mut state, &unauthorized, &parameters).unwrap();
        assert_eq!(state, before);
        process_withdrawal_request(&mut state, &request, &parameters).unwrap();
        assert_eq!(state.pending_partial_withdrawals.values[0].amount, 8_000_000_000);
        process_withdrawal_request(&mut state, &request, &parameters).unwrap();
        assert_eq!(state.pending_partial_withdrawals.values[1].amount, 2_000_000_000);
        let mut full = request;
        full.amount = 0;
        process_withdrawal_request(&mut state, &full, &parameters).unwrap();
        assert_eq!(state.validators.values[0].exit_epoch, u64::MAX);
        state.pending_partial_withdrawals.values.clear();
        process_withdrawal_request(&mut state, &full, &parameters).unwrap();
        assert_ne!(state.validators.values[0].exit_epoch, u64::MAX);
    }
    #[test]
    fn authorized_self_switch_queues_excess_and_consolidation_changes_only_source_exit() {
        let (mut state, _, mut parameters) = state_and_keys();
        mature(&mut state, &parameters);
        state.validators.values[0].withdrawal_credentials = credentials(1, 10);
        state.balances.values[0] = 40_000_000_000;
        let switch = ConsolidationRequest {
            source_address: [10; 20],
            source_pubkey: state.validators.values[0].pubkey,
            target_pubkey: state.validators.values[0].pubkey,
        };
        process_consolidation_request(&mut state, &switch, &parameters).unwrap();
        assert_eq!(state.validators.values[0].withdrawal_credentials[0], 2);
        assert_eq!(state.balances.values[0], 32_000_000_000);
        assert_eq!(state.pending_deposits.values[0].amount, 8_000_000_000);
        assert_eq!(state.pending_deposits.values[0].slot, 0);
        assert!(state.pending_consolidations.values.is_empty());
        parameters.min_per_epoch_churn_limit = 128_000_000_000;
        parameters.max_per_epoch_activation_exit_churn_limit = 64_000_000_000;
        state.validators.values[1].withdrawal_credentials = credentials(1, 12);
        state.validators.values[2].withdrawal_credentials = credentials(2, 13);
        let request = ConsolidationRequest {
            source_address: [12; 20],
            source_pubkey: state.validators.values[1].pubkey,
            target_pubkey: state.validators.values[2].pubkey,
        };
        let before_balances = state.balances.clone();
        process_consolidation_request(&mut state, &request, &parameters).unwrap();
        assert_eq!(
            state.pending_consolidations.values,
            vec![electra::PendingConsolidation { source_index: 1, target_index: 2 }]
        );
        assert_ne!(state.validators.values[1].exit_epoch, u64::MAX);
        assert_eq!(state.validators.values[2].exit_epoch, u64::MAX);
        assert_eq!(state.balances, before_balances);
    }
    #[test]
    fn delayed_deposits_verify_only_new_key_proofs_and_legacy_topups_queue_balances() {
        let (mut state, _, parameters) = state_and_keys();
        let key = SecretKey::key_gen(&[99; 32], &[]).unwrap();
        let pubkey = key.sk_to_pk().to_bytes();
        let credentials = credentials(2, 10);
        let amount = 64_000_000_000;
        let message = merkleize(
            &[bytes_root(&pubkey, 48, false).unwrap(), credentials, uint64_root(amount)],
            3,
        )
        .unwrap();
        let domain = compute_domain(
            [3, 0, 0, 0],
            parameters.deneb.capella.bellatrix.altair.phase0.genesis_fork_version,
            [0; 32],
        );
        let signature =
            key.sign(&compute_signing_root(message, domain), ETHEREUM_BLS_DST, &[]).to_bytes();
        let mut deposit = PendingDeposit {
            pubkey,
            withdrawal_credentials: credentials,
            amount,
            signature: [0; 96],
            slot: 0,
        };
        let before = state.clone();
        apply_pending_deposit(&mut state, &deposit, &parameters).unwrap();
        assert_eq!(state, before);
        deposit.signature = signature;
        apply_pending_deposit(&mut state, &deposit, &parameters).unwrap();
        assert_eq!(state.validators.values.len(), 33);
        assert_eq!(state.validators.values[32].effective_balance, 64_000_000_000);
        assert_eq!(state.balances.values[32], amount);
        let known = state.validators.values[0].pubkey;
        let balance = state.balances.values[0];
        apply_deposit_kernel(&mut state, known, [0; 32], 7, [0; 96], &parameters).unwrap();
        assert_eq!(state.balances.values[0], balance);
        assert_eq!(state.pending_deposits.values[0].amount, 7);
        assert_eq!(state.pending_deposits.values[0].slot, 0);
        let request = DepositRequest {
            pubkey: [22; 48],
            withdrawal_credentials: [0; 32],
            amount: 1,
            signature: [0; 96],
            index: 101,
        };
        process_deposit_request(&mut state, &request, &parameters).unwrap();
        assert_eq!(state.deposit_requests_start_index, 101);
        let mut next = request;
        next.index = 900;
        process_deposit_request(&mut state, &next, &parameters).unwrap();
        assert_eq!(state.deposit_requests_start_index, 101);
    }
}

#[cfg(feature = "spec-tests")]
#[path = "execution_requests_artificial.rs"]
pub(crate) mod artificial;
