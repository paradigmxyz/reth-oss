//! Artificial published-fixture operations, never compiled into runtime dependency graphs.
#![cfg(feature = "spec-tests")]
use super::*;
use crate::spec_crypto::skip_signature_for_artificial_fixture as verify_signature;

pub(crate) fn proposer_slashing_kernel(
    state: &mut BeaconState,
    operation: &ProposerSlashing,
    parameters: &ElectraParameters,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    let first = operation.signed_header_1.message;
    let second = operation.signed_header_2.message;
    if first.slot != second.slot || first.proposer_index != second.proposer_index || first == second
    {
        return Err(ProcessingError::InvalidState("proposer slashing headers do not conflict"));
    }
    let i = index(first.proposer_index, state.validators.values.len())?;
    if !slashable(&state.validators.values[i], state.slot / constants.slots_per_epoch) {
        return Err(ProcessingError::InvalidState("proposer is not slashable"));
    }
    let key = state.validators.values[i].pubkey;
    for signed in [&operation.signed_header_1, &operation.signed_header_2] {
        let root = compute_signing_root(
            signed.message.hash_tree_root(),
            common::domain(state, [0; 4], signed.message.slot / constants.slots_per_epoch),
        );
        verify_signature(&key, &root, &signed.signature)?;
    }
    common::slash_validator_kernel(state, first.proposer_index, None, parameters, constants)
}

pub(crate) fn attester_slashing_kernel(
    state: &mut BeaconState,
    operation: &AttesterSlashing,
    parameters: &ElectraParameters,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    let first = &operation.attestation_1;
    let second = &operation.attestation_2;
    if !((first.data != second.data && first.data.target.epoch == second.data.target.epoch) ||
        (first.data.source.epoch < second.data.source.epoch &&
            second.data.target.epoch < first.data.target.epoch))
    {
        return Err(ProcessingError::InvalidState("attester slashing data do not conflict"));
    }
    indexed_kernel::<false>(state, first, parameters)?;
    indexed_kernel::<false>(state, second, parameters)?;
    let mut any = false;
    for &index in &first.attesting_indices.values {
        if second.attesting_indices.values.binary_search(&index).is_ok() &&
            slashable(
                &state.validators.values[index as usize],
                state.slot / constants.slots_per_epoch,
            )
        {
            common::slash_validator_kernel(state, index, None, parameters, constants)?;
            any = true;
        }
    }
    if !any {
        return Err(ProcessingError::InvalidState(
            "attester slashing has no slashable intersection",
        ));
    }
    Ok(())
}

pub(crate) fn voluntary_exit_kernel(
    state: &mut BeaconState,
    operation: &SignedVoluntaryExit,
    parameters: &ElectraParameters,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    let exit = &operation.message;
    let i = index(exit.validator_index, state.validators.values.len())?;
    let validator = &state.validators.values[i];
    let current = state.slot / constants.slots_per_epoch;
    if !(validator.activation_epoch <= current && current < validator.exit_epoch) ||
        validator.exit_epoch != u64::MAX ||
        current < exit.epoch ||
        current <
            add(
                validator.activation_epoch,
                parameters.deneb.capella.bellatrix.altair.phase0.shard_committee_period,
                "voluntary exit maturity",
            )?
    {
        return Err(ProcessingError::InvalidState(
            "voluntary exit is inactive, exited or premature",
        ));
    }
    if common::get_pending_balance_to_withdraw(state, exit.validator_index)? != 0 {
        return Err(ProcessingError::InvalidState("voluntary exit has pending partial withdrawals"));
    }
    let domain = compute_domain(
        [4, 0, 0, 0],
        parameters.deneb.capella.fork_version,
        state.genesis_validators_root,
    );
    verify_signature(
        &validator.pubkey,
        &compute_signing_root(exit.hash_tree_root(parameters.preset())?, domain),
        &operation.signature,
    )?;
    common::initiate_validator_exit(state, exit.validator_index, parameters, constants)
}

pub(crate) fn bls_change_kernel(
    state: &mut BeaconState,
    operation: &SignedBLSToExecutionChange,
    parameters: &ElectraParameters,
) -> Result<(), ProcessingError> {
    with_altair(state, |common| {
        capella_processing::bls_to_execution_change_kernel_with_policy::<false>(
            common,
            &bls_change(operation),
            &parameters.deneb.capella,
        )
    })
}

pub(crate) fn operations_kernel(
    state: &mut BeaconState,
    body: &BeaconBlockBody,
    parameters: &ElectraParameters,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    let limit = state.eth1_data.deposit_count.min(state.deposit_requests_start_index);
    let expected = if state.eth1_deposit_index < limit {
        (limit - state.eth1_deposit_index).min(16)
    } else {
        0
    };
    if body.deposits.values.len() as u64 != expected {
        return Err(ProcessingError::InvalidState(
            "legacy deposit count differs from Electra cutover limit",
        ));
    }
    for value in &body.proposer_slashings.values {
        proposer_slashing_kernel(state, value, parameters, constants)?;
    }
    for value in &body.attester_slashings.values {
        attester_slashing_kernel(state, value, parameters, constants)?;
    }
    for value in &body.attestations.values {
        attestation_kernel::<false>(state, value, parameters, constants)?;
    }
    for value in &body.deposits.values {
        execution_requests::artificial::deposit_kernel(state, value, parameters)?;
    }
    for value in &body.voluntary_exits.values {
        voluntary_exit_kernel(state, value, parameters, constants)?;
    }
    for value in &body.bls_to_execution_changes.values {
        bls_change_kernel(state, value, parameters)?;
    }
    for value in &body.execution_requests.deposits.values {
        execution_requests::deposit_request_kernel(state, value)?;
    }
    for value in &body.execution_requests.withdrawals.values {
        execution_requests::withdrawal_request_kernel(state, value, parameters, constants)?;
    }
    for value in &body.execution_requests.consolidations.values {
        execution_requests::consolidation_request_kernel(state, value, parameters, constants)?;
    }
    Ok(())
}
