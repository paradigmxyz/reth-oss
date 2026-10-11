//! Explicit artificial published-fixture kernels; absent from runtime builds.
#![cfg(feature = "spec-tests")]
use super::*;
use crate::spec_crypto::skip_signature_for_artificial_fixture as verify_signature;

pub(crate) fn randao_kernel(
    state: &mut BeaconState,
    reveal: &[u8; 96],
    parameters: &ElectraParameters,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    let epoch = state.slot / constants.slots_per_epoch;
    let proposer = common::get_beacon_proposer_index(state, parameters, constants)?;
    let key =
        state.validators.values[common::index(proposer, state.validators.values.len())?].pubkey;
    verify_signature(
        &key,
        &compute_signing_root(uint64_root(epoch), common::domain(state, [2, 0, 0, 0], epoch)),
        reveal,
    )?;
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

pub(crate) async fn process_block<E: ExecutionEngine>(
    state: &mut BeaconState,
    block: &BeaconBlock,
    parameters: &ElectraParameters,
    constants: &Constants,
    engine: &E,
) -> Result<(), ImportError<E::Error>> {
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
    randao_kernel(state, &block.body.randao_reveal, parameters, constants)?;
    with_altair(state, |view| {
        altair_processing::block::eth1_vote_kernel(view, &block.body.eth1_data, constants)
    })?;
    operations::artificial::operations_kernel(state, &block.body, parameters, constants)?;
    common::sync_aggregate_kernel_with_policy::<false>(
        state,
        &block.body.sync_aggregate,
        parameters,
        constants,
    )?;
    Ok(())
}
