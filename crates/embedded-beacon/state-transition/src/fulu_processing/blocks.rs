//! Exact Fulu block sequencing and mandatory Engine V4 request verification.

use super::{
    checked_constants, common, execution_requests, operations, transact, with_altair, withdrawals,
    ExecutionContext, ExecutionEngine, FuluParameters, ImportError,
};
use crate::{
    altair_processing,
    phase0_processing::{add, mul, position, Constants, ProcessingError},
    ssz::{
        fulu::{
            BeaconBlock, BeaconBlockBody, BeaconState, ExecutionPayload, ExecutionPayloadHeader,
            SignedBeaconBlock, SyncAggregate,
        },
        Ssz,
    },
};
use reth_embedded_beacon_crypto::{compute_signing_root, verify_signature};
use reth_embedded_beacon_primitives::{sha256, uint64_root, BeaconBlockHeader, Root};

/// Derive the EIP-4844 versioned hash without asserting availability or KZG validity.
pub fn kzg_commitment_to_versioned_hash(commitment: &[u8; 48]) -> Root {
    let mut root = sha256(commitment);
    root[0] = 1;
    root
}
/// Select the Fulu balance-weighted proposer from the authenticated state.
pub fn get_beacon_proposer_index(
    state: &BeaconState,
    parameters: &FuluParameters,
) -> Result<u64, ProcessingError> {
    let constants = checked_constants(state, parameters)?;
    common::get_beacon_proposer_index(state, parameters, &constants)
}

pub(crate) fn verify_block_signature_kernel(
    state: &BeaconState,
    block: &SignedBeaconBlock,
    parameters: &FuluParameters,
) -> Result<(), ProcessingError> {
    let index = common::index(block.message.proposer_index, state.validators.values.len())?;
    let root = compute_signing_root(
        block.message.hash_tree_root(parameters.preset())?,
        common::domain(state, [0; 4], block.message.slot / parameters.slots_per_epoch()),
    );
    Ok(verify_signature(&state.validators.values[index].pubkey, &root, &block.signature)?)
}
/// Verify only the outer proposer signature; complete import also verifies state, EL and root.
pub fn verify_block_signature(
    state: &BeaconState,
    block: &SignedBeaconBlock,
    parameters: &FuluParameters,
) -> Result<(), ProcessingError> {
    checked_constants(state, parameters)?;
    block.hash_tree_root(parameters.preset())?;
    verify_block_signature_kernel(state, block, parameters)
}
pub(crate) fn header_kernel(
    state: &mut BeaconState,
    block: &BeaconBlock,
    parameters: &FuluParameters,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    if block.slot != state.slot || block.slot <= state.latest_block_header.slot {
        return Err(ProcessingError::InvalidState("block slot does not match a newer state slot"));
    }
    let proposer = common::get_beacon_proposer_index(state, parameters, constants)?;
    if block.proposer_index != proposer {
        return Err(ProcessingError::InvalidState("block proposer is not selected Fulu proposer"));
    }
    if block.parent_root != state.latest_block_header.hash_tree_root() {
        return Err(ProcessingError::InvalidState("block parent differs from latest beacon header"));
    }
    if state.validators.values[common::index(proposer, state.validators.values.len())?].slashed {
        return Err(ProcessingError::InvalidState("block proposer is slashed"));
    }
    state.latest_block_header = BeaconBlockHeader {
        slot: block.slot,
        proposer_index: proposer,
        parent_root: block.parent_root,
        state_root: [0; 32],
        body_root: block.body.hash_tree_root(parameters.preset())?,
    };
    Ok(())
}
/// Atomically process only the full Fulu body header.
pub fn process_block_header(
    state: &mut BeaconState,
    block: &BeaconBlock,
    parameters: &FuluParameters,
) -> Result<(), ProcessingError> {
    block.hash_tree_root(parameters.preset())?;
    transact(state, parameters, |candidate, constants| {
        header_kernel(candidate, block, parameters, constants)
    })
}

/// Pure payload consensus checks and complete header derivation; no EL verdict or import token.
pub fn validate_execution_payload_consensus(
    state: &BeaconState,
    body: &BeaconBlockBody,
    parameters: &FuluParameters,
) -> Result<ExecutionPayloadHeader, ProcessingError> {
    checked_constants(state, parameters)?;
    body.hash_tree_root(parameters.preset())?;
    payload_header(state, &body.execution_payload, &body.blob_kzg_commitments.values, parameters)
}
fn payload_header(
    state: &BeaconState,
    payload: &ExecutionPayload,
    commitments: &[[u8; 48]],
    parameters: &FuluParameters,
) -> Result<ExecutionPayloadHeader, ProcessingError> {
    if payload.parent_hash != state.latest_execution_payload_header.block_hash {
        return Err(ProcessingError::InvalidState("execution payload parent hash differs"));
    }
    let epoch = state.slot / parameters.slots_per_epoch();
    let mix = state
        .randao_mixes
        .values
        .get(position(
            epoch,
            match parameters.preset() {
                crate::ssz::Preset::Mainnet => 65_536,
                crate::ssz::Preset::Minimal => 64,
            },
        )?)
        .ok_or(ProcessingError::InvalidState("RANDAO ring is truncated"))?;
    if payload.prev_randao != *mix {
        return Err(ProcessingError::InvalidState("execution payload RANDAO differs"));
    }
    let timestamp = add(
        state.genesis_time,
        mul(
            state.slot,
            parameters.electra.deneb.capella.bellatrix.seconds_per_slot,
            "execution slot duration",
        )?,
        "execution timestamp",
    )?;
    if payload.timestamp != timestamp {
        return Err(ProcessingError::InvalidState("execution payload timestamp differs"));
    }
    if commitments.len() as u64 >
        parameters
            .blob_parameters_at_epoch(state.slot / parameters.slots_per_epoch())
            .max_blobs_per_block
    {
        return Err(ProcessingError::InvalidState(
            "Fulu blob commitment count exceeds network limit",
        ));
    }
    Ok(ExecutionPayloadHeader {
        parent_hash: payload.parent_hash,
        fee_recipient: payload.fee_recipient,
        state_root: payload.state_root,
        receipts_root: payload.receipts_root,
        logs_bloom: payload.logs_bloom.clone(),
        prev_randao: payload.prev_randao,
        block_number: payload.block_number,
        gas_limit: payload.gas_limit,
        gas_used: payload.gas_used,
        timestamp: payload.timestamp,
        extra_data: payload.extra_data.clone(),
        base_fee_per_gas: payload.base_fee_per_gas,
        block_hash: payload.block_hash,
        transactions_root: payload.transactions.hash_tree_root(parameters.preset())?,
        withdrawals_root: payload.withdrawals.hash_tree_root(parameters.preset())?,
        blob_gas_used: payload.blob_gas_used,
        excess_blob_gas: payload.excess_blob_gas,
    })
}
fn execution_context(
    state: &BeaconState,
    body: &BeaconBlockBody,
    parameters: &FuluParameters,
) -> Result<ExecutionContext, ProcessingError> {
    Ok(ExecutionContext {
        slot: state.slot,
        parent_beacon_block_root: state.latest_block_header.parent_root,
        previous_execution_payload_hash: (!crate::common::execution_header_is_empty!(
            &state.latest_execution_payload_header,
            blob
        ))
        .then_some(state.latest_execution_payload_header.block_hash),
        expected_blob_versioned_hashes: body
            .blob_kzg_commitments
            .values
            .iter()
            .map(kzg_commitment_to_versioned_hash)
            .collect(),
        execution_requests_list: execution_requests::get_execution_requests_list(
            &body.execution_requests,
            parameters.preset(),
        )?,
    })
}
/// Atomically verify only the exact payload, blob hashes, parent beacon root and EIP-7685 groups.
/// A successful isolated operation does not verify the beacon signature, withdrawals or post-root.
pub async fn process_execution_payload<E: ExecutionEngine>(
    state: &mut BeaconState,
    body: &BeaconBlockBody,
    parameters: &FuluParameters,
    engine: &E,
) -> Result<(), ImportError<E::Error>> {
    let header = validate_execution_payload_consensus(state, body, parameters)?;
    let context = execution_context(state, body, parameters)?;
    if context.previous_execution_payload_hash.is_none() {
        engine
            .verify_terminal_block(&body.execution_payload, context.clone())
            .await
            .map_err(ImportError::Execution)?;
    }
    engine
        .verify_payload(&body.execution_payload, context)
        .await
        .map_err(ImportError::Execution)?;
    state.latest_execution_payload_header = header;
    Ok(())
}

pub(crate) fn randao_kernel(
    state: &mut BeaconState,
    reveal: &[u8; 96],
    parameters: &FuluParameters,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    randao_kernel_with_policy::<true>(state, reveal, parameters, constants)
}

pub(crate) fn randao_kernel_with_policy<const VERIFY_BLS: bool>(
    state: &mut BeaconState,
    reveal: &[u8; 96],
    parameters: &FuluParameters,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    crate::common::require_signature_policy::<VERIFY_BLS>()?;
    let epoch = state.slot / constants.slots_per_epoch;
    let proposer = common::get_beacon_proposer_index(state, parameters, constants)?;
    let key =
        state.validators.values[common::index(proposer, state.validators.values.len())?].pubkey;
    if VERIFY_BLS {
        verify_signature(
            &key,
            &compute_signing_root(uint64_root(epoch), common::domain(state, [2, 0, 0, 0], epoch)),
            reveal,
        )?;
    }
    let mix = state
        .randao_mixes
        .values
        .get_mut(position(epoch, constants.historical_epochs)?)
        .ok_or(ProcessingError::InvalidState("RANDAO ring is truncated"))?;
    for (byte, hashed) in mix.iter_mut().zip(sha256(reveal)) {
        *byte ^= hashed;
    }
    Ok(())
}
/// Verify the Fulu selected proposer's RANDAO signature before atomically changing the mix.
pub fn process_randao(
    state: &mut BeaconState,
    body: &BeaconBlockBody,
    parameters: &FuluParameters,
) -> Result<(), ProcessingError> {
    body.hash_tree_root(parameters.preset())?;
    transact(state, parameters, |candidate, constants| {
        randao_kernel(candidate, &body.randao_reveal, parameters, constants)
    })
}
/// Apply only the unchanged Eth1 majority-vote operation using exact inherited constants.
pub fn process_eth1_data(
    state: &mut BeaconState,
    body: &BeaconBlockBody,
    parameters: &FuluParameters,
) -> Result<(), ProcessingError> {
    body.hash_tree_root(parameters.preset())?;
    transact(state, parameters, |candidate, constants| {
        with_altair(candidate, |view| {
            altair_processing::block::eth1_vote_kernel(view, &body.eth1_data, constants)
        })
    })
}
/// Verify the sync aggregate using Fulu proposer selection and exact rewards.
pub fn process_sync_aggregate(
    state: &mut BeaconState,
    aggregate: &SyncAggregate,
    parameters: &FuluParameters,
) -> Result<(), ProcessingError> {
    aggregate.hash_tree_root(parameters.preset())?;
    transact(state, parameters, |candidate, constants| {
        common::sync_aggregate_kernel(candidate, aggregate, parameters, constants)
    })
}

/// Raw complete block sequence; callers own candidate isolation, slots, outer BLS and post-root.
pub(crate) async fn process_block<E: ExecutionEngine>(
    state: &mut BeaconState,
    block: &BeaconBlock,
    parameters: &FuluParameters,
    constants: &Constants,
    engine: &E,
) -> Result<(), ImportError<E::Error>> {
    process_block_with_policy::<true, E>(state, block, parameters, constants, engine).await
}

pub(crate) async fn process_block_with_policy<const VERIFY_BLS: bool, E: ExecutionEngine>(
    state: &mut BeaconState,
    block: &BeaconBlock,
    parameters: &FuluParameters,
    constants: &Constants,
    engine: &E,
) -> Result<(), ImportError<E::Error>> {
    crate::common::require_signature_policy::<VERIFY_BLS>()?;
    header_kernel(state, block, parameters, constants)?;
    withdrawals::withdrawals_kernel(state, &block.body.execution_payload, parameters)?;
    let header = validate_execution_payload_consensus(state, &block.body, parameters)?;
    let context = execution_context(state, &block.body, parameters)?;
    if context.previous_execution_payload_hash.is_none() {
        engine
            .verify_terminal_block(&block.body.execution_payload, context.clone())
            .await
            .map_err(ImportError::Execution)?;
    }
    engine
        .verify_payload(&block.body.execution_payload, context)
        .await
        .map_err(ImportError::Execution)?;
    state.latest_execution_payload_header = header;
    randao_kernel_with_policy::<VERIFY_BLS>(
        state,
        &block.body.randao_reveal,
        parameters,
        constants,
    )?;
    with_altair(state, |view| {
        altair_processing::block::eth1_vote_kernel(view, &block.body.eth1_data, constants)
    })?;
    operations::operations_kernel_with_policy::<VERIFY_BLS>(
        state,
        &block.body,
        parameters,
        constants,
    )?;
    common::sync_aggregate_kernel_with_policy::<VERIFY_BLS>(
        state,
        &block.body.sync_aggregate,
        parameters,
        constants,
    )?;
    Ok(())
}
