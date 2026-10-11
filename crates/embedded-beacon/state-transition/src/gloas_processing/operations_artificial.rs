//! Artificial published-fixture operations, never compiled into runtime dependency graphs.
#![cfg(feature = "spec-tests")]
use super::*;
use crate::spec_crypto::skip_signature_for_artificial_fixture as verify_signature;

pub(crate) fn proposer_slashing_kernel(
    state: &mut BeaconState,
    operation: &ProposerSlashing,
    parameters: &GloasParameters,
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
    let proposal_epoch = first.slot / constants.slots_per_epoch;
    let current = state.slot / constants.slots_per_epoch;
    let payment_index = if proposal_epoch == current {
        Some(constants.slots_per_epoch + first.slot % constants.slots_per_epoch)
    } else if proposal_epoch == current.saturating_sub(1) {
        Some(first.slot % constants.slots_per_epoch)
    } else {
        None
    };
    if let Some(payment_index) = payment_index {
        let payment_index = index(payment_index, state.builder_pending_payments.values.len())?;
        if state.builder_pending_payments.values[payment_index].proposer_index ==
            first.proposer_index
        {
            state.builder_pending_payments.values[payment_index] = builder::empty_payment();
        }
    }
    common::slash_validator_kernel(state, first.proposer_index, None, parameters, constants)
}

pub(crate) fn attester_slashing_kernel(
    state: &mut BeaconState,
    operation: &AttesterSlashing,
    parameters: &GloasParameters,
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
    parameters: &GloasParameters,
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
                parameters
                    .fulu
                    .electra
                    .deneb
                    .capella
                    .bellatrix
                    .altair
                    .phase0
                    .shard_committee_period,
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
        parameters.fulu.electra.deneb.capella.fork_version,
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
    parameters: &GloasParameters,
) -> Result<(), ProcessingError> {
    let message = &operation.message;
    let i = index(message.validator_index, state.validators.values.len())?;
    let validator = &state.validators.values[i];
    let digest = reth_embedded_beacon_primitives::sha256(&message.from_bls_pubkey);
    if validator.withdrawal_credentials[0] != 0 ||
        validator.withdrawal_credentials[1..] != digest[1..]
    {
        return Err(ProcessingError::InvalidState(
            "BLS withdrawal credentials do not match withdrawal key",
        ));
    }
    let domain = compute_domain(
        [10, 0, 0, 0],
        parameters.fulu.electra.deneb.capella.bellatrix.altair.phase0.genesis_fork_version,
        state.genesis_validators_root,
    );
    verify_signature(
        &message.from_bls_pubkey,
        &compute_signing_root(message.hash_tree_root(parameters.preset())?, domain),
        &operation.signature,
    )?;
    let mut credentials = [0; 32];
    credentials[0] = 1;
    credentials[12..].copy_from_slice(&message.to_execution_address);
    state.validators.values[i].withdrawal_credentials = credentials;
    Ok(())
}

pub(crate) fn operations_kernel(
    state: &mut BeaconState,
    body: &BeaconBlockBody,
    parameters: &GloasParameters,
    constants: &Constants,
    parent_slot: u64,
) -> Result<(), ProcessingError> {
    if !body.deposits.values.is_empty() ||
        body.proposer_slashings.values.len() > 16 ||
        body.attester_slashings.values.len() > 1 ||
        body.attestations.values.len() > 8 ||
        body.voluntary_exits.values.len() > 16 ||
        body.bls_to_execution_changes.values.len() > 16 ||
        body.payload_attestations.values.len() > 4
    {
        return Err(ProcessingError::InvalidState(
            "Gloas body operation count exceeds consensus limits",
        ));
    }
    for value in &body.proposer_slashings.values {
        proposer_slashing_kernel(state, value, parameters, constants)?;
    }
    for value in &body.attester_slashings.values {
        attester_slashing_kernel(state, value, parameters, constants)?;
    }
    for value in &body.attestations.values {
        attestation_kernel::<false>(state, value, parameters, constants, parent_slot)?;
    }
    for value in &body.voluntary_exits.values {
        voluntary_exit_kernel(state, value, parameters, constants)?;
    }
    for value in &body.bls_to_execution_changes.values {
        bls_change_kernel(state, value, parameters)?;
    }
    for value in &body.payload_attestations.values {
        ptc::artificial::payload_attestation_kernel(state, value, parameters)?;
    }
    Ok(())
}
