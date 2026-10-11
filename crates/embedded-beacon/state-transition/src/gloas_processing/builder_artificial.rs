//! Explicit artificial published-fixture kernels; absent from runtime builds.
#![cfg(feature = "spec-tests")]
use super::*;
use crate::spec_crypto::skip_signature_for_artificial_fixture as verify_signature;

pub(crate) fn is_valid_builder_deposit_signature(
    request: &BuilderDepositRequest,
    parameters: &GloasParameters,
) -> Result<bool, ProcessingError> {
    parameters.validate()?;
    let message = merkleize(
        &[
            bytes_root(&request.pubkey, 48, false)?,
            request.withdrawal_credentials,
            uint64_root(request.amount),
        ],
        3,
    )?;
    let version =
        parameters.fulu.electra.deneb.capella.bellatrix.altair.phase0.genesis_fork_version;
    let root = compute_signing_root(message, compute_domain([14, 0, 0, 0], version, [0; 32]));
    Ok(verify_signature(&request.pubkey, &root, &request.signature).is_ok())
}

pub(crate) fn is_pending_validator(
    deposits: &[gloas::PendingDeposit],
    pubkey: &[u8; 48],
    parameters: &GloasParameters,
) -> Result<bool, ProcessingError> {
    for deposit in deposits {
        if &deposit.pubkey == pubkey &&
            super::super::execution_requests::artificial::is_valid_deposit_signature(
                deposit.pubkey,
                deposit.withdrawal_credentials,
                deposit.amount,
                deposit.signature,
                parameters,
            )?
        {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(crate) fn builder_deposit_kernel(
    state: &mut BeaconState,
    request: &BuilderDepositRequest,
    parameters: &GloasParameters,
) -> Result<(), ProcessingError> {
    if !is_builder_withdrawal_credential(request.withdrawal_credentials) {
        return Ok(());
    }
    if let Some(index) =
        state.builders.values.iter().position(|builder| builder.pubkey == request.pubkey)
    {
        let builder = &mut state.builders.values[index];
        if builder.withdrawable_epoch != u64::MAX && builder.balance == 0 {
            builder.withdrawable_epoch = add(
                state.slot / parameters.slots_per_epoch(),
                parameters.min_builder_withdrawability_delay,
                "exited builder top-up maturity",
            )?;
        }
        builder.balance = add(builder.balance, request.amount, "builder deposit top-up")?;
    } else if is_valid_builder_deposit_signature(request, parameters)? {
        let builder = gloas::Builder {
            pubkey: request.pubkey,
            version: 0,
            execution_address: request.withdrawal_credentials[12..]
                .try_into()
                .map_err(|_| ProcessingError::InvalidState("builder execution address length"))?,
            balance: request.amount,
            deposit_epoch: state.slot / parameters.slots_per_epoch(),
            withdrawable_epoch: u64::MAX,
        };
        let index = new_builder_index_kernel(state, parameters);
        if index == state.builders.values.len() {
            state.builders.values.push(builder);
        } else {
            state.builders.values[index] = builder;
        }
    }
    Ok(())
}

pub(crate) fn verify_bid_signature_kernel(
    state: &BeaconState,
    signed: &SignedExecutionPayloadBid,
    parameters: &GloasParameters,
) -> Result<(), ProcessingError> {
    let builder = &state.builders.values
        [common::index(signed.message.builder_index, state.builders.values.len())?];
    let root = compute_signing_root(
        signed.message.hash_tree_root(parameters.preset())?,
        common::domain(state, [11, 0, 0, 0], state.slot / parameters.slots_per_epoch()),
    );
    Ok(verify_signature(&builder.pubkey, &root, &signed.signature)?)
}

pub(crate) fn bid_kernel(
    state: &mut BeaconState,
    signed: &SignedExecutionPayloadBid,
    parameters: &GloasParameters,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    let bid = &signed.message;
    if bid.builder_index == BUILDER_INDEX_SELF_BUILD {
        let mut infinity = [0; 96];
        infinity[0] = 0xc0;
        if bid.value != 0 || signed.signature != infinity {
            return Err(ProcessingError::InvalidState(
                "self-build bid requires zero value and infinity signature",
            ));
        }
    } else {
        if !active_builder_kernel(state, bid.builder_index)? {
            return Err(ProcessingError::InvalidState("bid builder is not finalized and active"));
        }
        let builder =
            &state.builders.values[common::index(bid.builder_index, state.builders.values.len())?];
        if builder.version != 0 || !can_cover_kernel(state, bid.builder_index, bid.value)? {
            return Err(ProcessingError::InvalidState(
                "builder version or unreserved bid balance is invalid",
            ));
        }
        verify_bid_signature_kernel(state, signed, parameters)?;
    }
    let epoch = state.slot / parameters.slots_per_epoch();
    if bid.blob_kzg_commitments.values.len() as u64 >
        parameters.fulu.blob_parameters_at_epoch(epoch).max_blobs_per_block
    {
        return Err(ProcessingError::InvalidState(
            "bid blob commitment count exceeds active schedule",
        ));
    }
    if state.slot == 0 || bid.slot != state.slot {
        return Err(ProcessingError::InvalidState("bid slot is invalid"));
    }
    if bid.parent_block_hash != state.latest_block_hash {
        return Err(ProcessingError::InvalidState("bid execution parent linkage is invalid"));
    }
    if bid.block_hash == bid.parent_block_hash {
        return Err(ProcessingError::InvalidState(
            "bid block hash must differ from execution parent",
        ));
    }
    let prior = state
        .block_roots
        .values
        .get(position(state.slot - 1, constants.historical_slots)?)
        .ok_or(ProcessingError::InvalidState("block root ring is truncated"))?;
    if bid.parent_block_root != *prior {
        return Err(ProcessingError::InvalidState("bid parent beacon root differs"));
    }
    let randao = state
        .randao_mixes
        .values
        .get(position(epoch, constants.historical_epochs)?)
        .ok_or(ProcessingError::InvalidState("RANDAO ring is truncated"))?;
    if bid.prev_randao != *randao {
        return Err(ProcessingError::InvalidState("bid RANDAO differs"));
    }
    if bid.value > 0 {
        let payment_index = parameters.slots_per_epoch() + bid.slot % parameters.slots_per_epoch();
        let index = common::index(payment_index, state.builder_pending_payments.values.len())?;
        state.builder_pending_payments.values[index] = BuilderPendingPayment {
            weight: 0,
            withdrawal: gloas::BuilderPendingWithdrawal {
                fee_recipient: bid.fee_recipient,
                amount: bid.value,
                builder_index: bid.builder_index,
            },
            proposer_index: common::get_beacon_proposer_index(state, parameters, constants)?,
        };
    }
    state.latest_execution_payload_bid = bid.clone();
    Ok(())
}

pub(crate) fn onboard_builders_from_pending_deposits_kernel(
    state: &mut BeaconState,
    parameters: &GloasParameters,
) -> Result<(), ProcessingError> {
    let deposits = std::mem::take(&mut state.pending_deposits.values);
    let mut retained = Vec::new();
    for deposit in deposits {
        if state.validators.values.iter().any(|validator| validator.pubkey == deposit.pubkey) {
            retained.push(deposit);
            continue;
        }
        if let Some(index) =
            state.builders.values.iter().position(|builder| builder.pubkey == deposit.pubkey)
        {
            state.builders.values[index].balance = add(
                state.builders.values[index].balance,
                deposit.amount,
                "fork builder deposit top-up",
            )?;
        } else if !is_builder_withdrawal_credential(deposit.withdrawal_credentials) ||
            is_pending_validator(&retained, &deposit.pubkey, parameters)?
        {
            retained.push(deposit);
        } else if super::super::execution_requests::artificial::is_valid_deposit_signature(
            deposit.pubkey,
            deposit.withdrawal_credentials,
            deposit.amount,
            deposit.signature,
            parameters,
        )? {
            let address = deposit.withdrawal_credentials[12..]
                .try_into()
                .map_err(|_| ProcessingError::InvalidState("builder execution address length"))?;
            add_builder_to_registry_kernel(
                state,
                deposit.pubkey,
                0,
                address,
                deposit.amount,
                deposit.slot,
                parameters,
            );
        }
    }
    state.pending_deposits.values = retained;
    Ok(())
}
