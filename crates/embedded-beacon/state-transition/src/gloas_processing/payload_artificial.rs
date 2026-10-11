//! Explicit artificial published-fixture kernels; absent from runtime builds.
#![cfg(feature = "spec-tests")]
use super::*;

pub(crate) fn apply_parent_execution_payload_kernel(
    state: &mut BeaconState,
    requests: &ExecutionRequests,
    parameters: &GloasParameters,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    if requests.withdrawals.values.len() > 16 ||
        requests.consolidations.values.len() > 2 ||
        requests.builder_deposits.values.len() > 64 ||
        requests.builder_exits.values.len() > 16
    {
        return Err(ProcessingError::InvalidState(
            "parent execution request group exceeds consensus limit",
        ));
    }
    let parent_slot = state.latest_block_header.slot;
    let parent_epoch = parent_slot / parameters.slots_per_epoch();
    let current = state.slot / parameters.slots_per_epoch();
    let parent_hash = state.latest_execution_payload_bid.block_hash;
    if parent_epoch == current {
        builder::settle_builder_payment_kernel(
            state,
            parameters.slots_per_epoch() + parent_slot % parameters.slots_per_epoch(),
            parameters,
        )?;
    } else if parent_epoch == current.saturating_sub(1) {
        builder::settle_builder_payment_kernel(
            state,
            parent_slot % parameters.slots_per_epoch(),
            parameters,
        )?;
    } else if state.latest_execution_payload_bid.value > 0 {
        let bid = &state.latest_execution_payload_bid;
        state.builder_pending_withdrawals.values.push(
            crate::ssz::gloas::BuilderPendingWithdrawal {
                fee_recipient: bid.fee_recipient,
                amount: bid.value,
                builder_index: bid.builder_index,
            },
        );
    }
    for request in &requests.deposits.values {
        execution_requests::deposit_request_kernel(state, request)?;
    }
    for request in &requests.withdrawals.values {
        execution_requests::withdrawal_request_kernel(state, request, parameters, constants)?;
    }
    for request in &requests.consolidations.values {
        execution_requests::consolidation_request_kernel(state, request, parameters, constants)?;
    }
    for request in &requests.builder_deposits.values {
        builder::artificial::builder_deposit_kernel(state, request, parameters)?;
    }
    for request in &requests.builder_exits.values {
        builder::builder_exit_kernel(state, request, parameters)?;
    }
    state
        .execution_payload_availability
        .bits
        .set(position(parent_slot, constants.historical_slots)?, true)?;
    state.latest_block_hash = parent_hash;
    Ok(())
}

pub(crate) fn parent_execution_kernel(
    state: &mut BeaconState,
    block: &BeaconBlock,
    parameters: &GloasParameters,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    let requests = &block.body.parent_execution_requests;
    if block.body.signed_execution_payload_bid.message.parent_block_hash !=
        state.latest_execution_payload_bid.block_hash
    {
        if !requests.deposits.values.is_empty() ||
            !requests.withdrawals.values.is_empty() ||
            !requests.consolidations.values.is_empty() ||
            !requests.builder_deposits.values.is_empty() ||
            !requests.builder_exits.values.is_empty()
        {
            return Err(ProcessingError::InvalidState(
                "EMPTY parent cannot carry execution requests",
            ));
        }
        return Ok(());
    }
    if requests.hash_tree_root(parameters.preset())? !=
        state.latest_execution_payload_bid.execution_requests_root
    {
        return Err(ProcessingError::InvalidState(
            "parent execution requests differ from committed bid",
        ));
    }
    apply_parent_execution_payload_kernel(state, requests, parameters, constants)
}
