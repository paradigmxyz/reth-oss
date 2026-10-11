//! Atomic Capella processing pinned to consensus-specs v1.7.0-beta.3 with mandatory execution
//! verification during signed import.
//!
//! Callers enforce the trusted fork schedule, execution chain identity and resource
//! limits. Common epoch and operation data move through a restoring guard; complete state
//! commitments always use the actual Capella schema.

pub use crate::bellatrix_processing::{ExecutionContext, ImportError};
use crate::{
    altair_processing,
    common::with_altair,
    phase0_processing::{add, mul, position, Constants, ProcessingError},
    ssz::{
        bellatrix,
        capella::{BeaconState, ExecutionPayload, ExecutionPayloadHeader},
        List, Preset, Ssz,
    },
};
use std::future::Future;

mod block;
mod slot_cache;
mod withdrawals;
pub(crate) use withdrawals::{
    bls_to_execution_change_kernel, bls_to_execution_change_kernel_with_policy, withdrawals_kernel,
};
pub use withdrawals::{
    get_expected_withdrawals, has_eth1_withdrawal_credential, is_fully_withdrawable_validator,
    is_partially_withdrawable_validator, process_bls_to_execution_change, process_withdrawals,
    ExpectedWithdrawals,
};

#[cfg(feature = "spec-tests")]
pub use block::spec_only;

pub use block::{
    get_attesting_indices, get_beacon_proposer_index, get_indexed_attestation,
    is_valid_indexed_attestation, process_attestation, process_attester_slashing,
    process_block_header, process_deposit, process_eth1_data, process_execution_payload,
    process_operations, process_proposer_slashing, process_randao, process_signed_block,
    process_sync_aggregate, process_voluntary_exit, state_transition, verify_block_signature,
};

/// Trusted inherited network configuration and exact Capella upgrade version.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CapellaParameters {
    /// Bellatrix clock, participation, churn and genesis signature parameters.
    pub bellatrix: crate::bellatrix_processing::BellatrixParameters,
    /// Version written during the caller-scheduled Capella upgrade.
    pub fork_version: [u8; 4],
}
impl CapellaParameters {
    /// Exact pinned preset and associated default network configuration.
    pub const fn for_preset(preset: Preset) -> Self {
        Self {
            bellatrix: crate::bellatrix_processing::BellatrixParameters::for_preset(preset),
            fork_version: match preset {
                Preset::Mainnet => [3, 0, 0, 0],
                Preset::Minimal => [3, 0, 0, 1],
            },
        }
    }
    /// Reject inherited invalid clock and divisor parameters.
    pub fn validate(&self) -> Result<(), ProcessingError> {
        self.bellatrix.validate()
    }
    /// Exact number of slots per epoch.
    pub const fn slots_per_epoch(&self) -> u64 {
        self.bellatrix.slots_per_epoch()
    }
    /// Exact SSZ and deterministic algorithm preset.
    pub const fn preset(&self) -> Preset {
        self.bellatrix.preset()
    }
}

/// Mandatory verification of the exact Capella V2 candidate during atomic signed import.
/// `Ok(())` must mean the actual configured engine returned VALID and a matching latest valid
/// hash after checking this payload's computed block hash. ACCEPTED, SYNCING, timeouts and
/// unverified terminal/execution ancestry are errors. No external verdict flag authorizes import.
pub trait ExecutionEngine: Send + Sync {
    /// Engine rejection or transport/response error.
    type Error: std::error::Error + Send + Sync + 'static;
    /// Verify first-payload terminal eligibility against configured override activation or
    /// authenticated terminal block and parent total difficulties. New-payload VALID alone
    /// does not establish this proof; absent evidence must reject the first payload.
    fn verify_terminal_block<'a>(
        &'a self,
        payload: &'a ExecutionPayload,
        context: ExecutionContext,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;
    /// Verify the candidate and its inherited execution ancestry with the real engine.
    fn verify_payload<'a>(
        &'a self,
        payload: &'a ExecutionPayload,
        context: ExecutionContext,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;
}

const fn constants_for_preset(preset: Preset) -> Constants {
    let mut constants = Constants::for_preset(preset);
    constants.inactivity_penalty_quotient = 16_777_216;
    constants.minimum_slashing_penalty_quotient = 32;
    constants.proportional_slashing_multiplier = 3;
    constants
}

fn checked_constants(
    state: &BeaconState,
    parameters: &CapellaParameters,
) -> Result<Constants, ProcessingError> {
    parameters.validate()?;
    state.hash_tree_root(parameters.preset())?;
    let count = state.validators.values.len();
    if [
        state.balances.values.len(),
        state.previous_epoch_participation.values.len(),
        state.current_epoch_participation.values.len(),
        state.inactivity_scores.values.len(),
    ]
    .iter()
    .any(|&len| len != count)
    {
        return Err(ProcessingError::InvalidState("Capella validator-indexed list lengths differ"))
    }
    Ok(constants_for_preset(parameters.preset()))
}

fn transact<T>(
    state: &mut BeaconState,
    parameters: &CapellaParameters,
    operation: impl FnOnce(&mut BeaconState, &Constants) -> Result<T, ProcessingError>,
) -> Result<T, ProcessingError> {
    let constants = checked_constants(state, parameters)?;
    let mut candidate = state.clone();
    let result = operation(&mut candidate, &constants)?;
    checked_constants(&candidate, parameters)?;
    *state = candidate;
    Ok(result)
}

/// Exact irregular Bellatrix-to-Capella upgrade; the caller enforces the scheduled boundary.
pub fn upgrade_to_capella(
    pre: &bellatrix::BeaconState,
    parameters: &CapellaParameters,
) -> Result<BeaconState, ProcessingError> {
    parameters.validate()?;
    pre.hash_tree_root(parameters.preset())?;
    let count = pre.validators.values.len();
    if [
        pre.balances.values.len(),
        pre.previous_epoch_participation.values.len(),
        pre.current_epoch_participation.values.len(),
        pre.inactivity_scores.values.len(),
    ]
    .iter()
    .any(|&length| length != count)
    {
        return Err(ProcessingError::InvalidState("pre-upgrade validator-indexed lists differ"));
    }
    let post = BeaconState {
        genesis_time: pre.genesis_time,
        genesis_validators_root: pre.genesis_validators_root,
        slot: pre.slot,
        fork: crate::ssz::phase0::ForkInfo {
            previous_version: pre.fork.current_version,
            current_version: parameters.fork_version,
            epoch: pre.slot / parameters.slots_per_epoch(),
        },
        latest_block_header: pre.latest_block_header,
        block_roots: pre.block_roots.clone(),
        state_roots: pre.state_roots.clone(),
        historical_roots: pre.historical_roots.clone(),
        eth1_data: pre.eth1_data.clone(),
        eth1_data_votes: pre.eth1_data_votes.clone(),
        eth1_deposit_index: pre.eth1_deposit_index,
        validators: pre.validators.clone(),
        balances: pre.balances.clone(),
        randao_mixes: pre.randao_mixes.clone(),
        slashings: pre.slashings.clone(),
        previous_epoch_participation: pre.previous_epoch_participation.clone(),
        current_epoch_participation: pre.current_epoch_participation.clone(),
        justification_bits: pre.justification_bits.clone(),
        previous_justified_checkpoint: pre.previous_justified_checkpoint,
        current_justified_checkpoint: pre.current_justified_checkpoint,
        finalized_checkpoint: pre.finalized_checkpoint,
        inactivity_scores: pre.inactivity_scores.clone(),
        current_sync_committee: crate::ssz::capella::SyncCommittee {
            pubkeys: pre.current_sync_committee.pubkeys.clone(),
            aggregate_pubkey: pre.current_sync_committee.aggregate_pubkey,
        },
        next_sync_committee: crate::ssz::capella::SyncCommittee {
            pubkeys: pre.next_sync_committee.pubkeys.clone(),
            aggregate_pubkey: pre.next_sync_committee.aggregate_pubkey,
        },
        latest_execution_payload_header: upgrade_execution_header(
            &pre.latest_execution_payload_header,
        ),
        next_withdrawal_index: 0,
        next_withdrawal_validator_index: 0,
        historical_summaries: List::default(),
    };
    checked_constants(&post, parameters)?;
    Ok(post)
}

fn upgrade_execution_header(
    pre: &crate::ssz::bellatrix::ExecutionPayloadHeader,
) -> ExecutionPayloadHeader {
    ExecutionPayloadHeader {
        parent_hash: pre.parent_hash,
        fee_recipient: pre.fee_recipient,
        state_root: pre.state_root,
        receipts_root: pre.receipts_root,
        logs_bloom: pre.logs_bloom.clone(),
        prev_randao: pre.prev_randao,
        block_number: pre.block_number,
        gas_limit: pre.gas_limit,
        gas_used: pre.gas_used,
        timestamp: pre.timestamp,
        extra_data: pre.extra_data.clone(),
        base_fee_per_gas: pre.base_fee_per_gas,
        block_hash: pre.block_hash,
        transactions_root: pre.transactions_root,
        withdrawals_root: [0; 32],
    }
}

/// Check pure consensus payload invariants and derive its complete header.
///
/// This result does not establish execution validity or mutate the state. Complete import
/// must invoke the mandatory execution adapter before accepting the derived header.
pub fn validate_execution_payload_consensus(
    state: &BeaconState,
    payload: &ExecutionPayload,
    parameters: &CapellaParameters,
) -> Result<ExecutionPayloadHeader, ProcessingError> {
    let constants = checked_constants(state, parameters)?;
    payload.hash_tree_root(parameters.preset())?;
    if payload.parent_hash != state.latest_execution_payload_header.block_hash {
        return Err(ProcessingError::InvalidState("execution payload parent hash mismatch"))
    }
    let current = state.slot / constants.slots_per_epoch;
    let mix = state
        .randao_mixes
        .values
        .get(position(current, constants.historical_epochs)?)
        .ok_or(ProcessingError::InvalidState("RANDAO ring is truncated"))?;
    if payload.prev_randao != *mix {
        return Err(ProcessingError::InvalidState("execution payload RANDAO mismatch"))
    }
    let time = add(
        state.genesis_time,
        mul(
            state.slot,
            parameters.bellatrix.seconds_per_slot,
            "execution timestamp slot duration",
        )?,
        "execution timestamp",
    )?;
    if payload.timestamp != time {
        return Err(ProcessingError::InvalidState("execution payload timestamp mismatch"))
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
    })
}

/// Cache the complete Capella pre-state commitment without advancing the slot.
pub fn process_slot(
    state: &mut BeaconState,
    parameters: &CapellaParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, slot_kernel)
}

/// Replay empty Capella slots, including every ordered epoch kernel.
pub fn process_slots(
    state: &mut BeaconState,
    target_slot: u64,
    parameters: &CapellaParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, constants| {
        slots_kernel(candidate, target_slot, parameters, constants)
    })
}

/// Apply the complete ordered Capella epoch sequence atomically.
pub fn process_epoch(
    state: &mut BeaconState,
    parameters: &CapellaParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, constants| {
        epoch_kernel(candidate, parameters, constants)
    })
}

fn slot_kernel(state: &mut BeaconState, constants: &Constants) -> Result<(), ProcessingError> {
    let state_root = state.hash_tree_root(constants.preset())?;
    let index = position(state.slot, constants.historical_slots)?;
    *state
        .state_roots
        .values
        .get_mut(index)
        .ok_or(ProcessingError::InvalidState("state-root ring is truncated"))? = state_root;
    if state.latest_block_header.state_root == [0; 32] {
        state.latest_block_header.state_root = state_root;
    }
    *state
        .block_roots
        .values
        .get_mut(index)
        .ok_or(ProcessingError::InvalidState("block-root ring is truncated"))? =
        state.latest_block_header.hash_tree_root();
    Ok(())
}

fn slots_kernel(
    state: &mut BeaconState,
    target_slot: u64,
    parameters: &CapellaParameters,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    if state.slot >= target_slot {
        return Err(ProcessingError::SlotNotIncreasing { current: state.slot, target: target_slot })
    }
    let mut cache = slot_cache::SlotCache::new(state, constants)?;
    while state.slot < target_slot {
        cache.process_slot(state)?;
        let next = add(state.slot, 1, "next slot")?;
        if next.is_multiple_of(constants.slots_per_epoch) {
            epoch_kernel(state, parameters, constants)?;
            cache.refresh_epoch(state)?;
        }
        state.slot = next;
        cache.advance_slot(next);
    }
    Ok(())
}

fn epoch_kernel(
    state: &mut BeaconState,
    parameters: &CapellaParameters,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    with_altair(state, |common| {
        altair_processing::justification_kernel(common, constants)?;
        altair_processing::inactivity_kernel(common, &parameters.bellatrix.altair, constants)?;
        altair_processing::rewards_kernel(common, &parameters.bellatrix.altair, constants)?;
        altair_processing::registry_kernel(common, &parameters.bellatrix.altair, constants)?;
        altair_processing::slashings_kernel(common, constants)?;
        altair_processing::eth1_reset_kernel(common, constants)?;
        altair_processing::effective_balances_kernel(common, constants)?;
        altair_processing::slashings_reset_kernel(common, constants)?;
        altair_processing::randao_reset_kernel(common, constants)
    })?;
    historical_summaries_kernel(state, constants)?;
    with_altair(state, |common| {
        altair_processing::participation_kernel(common, constants)?;
        altair_processing::sync_committee_kernel(common, constants)
    })
}

/// Apply the exact `process_justification_and_finalization` Capella component atomically.
pub fn process_justification_and_finalization(
    state: &mut BeaconState,
    parameters: &CapellaParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, constants| {
        with_altair(candidate, |common| altair_processing::justification_kernel(common, constants))
    })
}

/// Apply the exact `process_inactivity_updates` Capella component atomically.
pub fn process_inactivity_updates(
    state: &mut BeaconState,
    parameters: &CapellaParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, constants| {
        with_altair(candidate, |common| {
            altair_processing::inactivity_kernel(common, &parameters.bellatrix.altair, constants)
        })
    })
}

/// Apply the exact `process_rewards_and_penalties` Capella component atomically.
pub fn process_rewards_and_penalties(
    state: &mut BeaconState,
    parameters: &CapellaParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, constants| {
        with_altair(candidate, |common| {
            altair_processing::rewards_kernel(common, &parameters.bellatrix.altair, constants)
        })
    })
}

/// Apply the exact `process_registry_updates` Capella component atomically.
pub fn process_registry_updates(
    state: &mut BeaconState,
    parameters: &CapellaParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, constants| {
        with_altair(candidate, |common| {
            altair_processing::registry_kernel(common, &parameters.bellatrix.altair, constants)
        })
    })
}

/// Apply the exact `process_slashings` Capella component atomically.
pub fn process_slashings(
    state: &mut BeaconState,
    parameters: &CapellaParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, constants| {
        with_altair(candidate, |common| altair_processing::slashings_kernel(common, constants))
    })
}

/// Apply the exact `process_eth1_data_reset` Capella component atomically.
pub fn process_eth1_data_reset(
    state: &mut BeaconState,
    parameters: &CapellaParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, constants| {
        with_altair(candidate, |common| altair_processing::eth1_reset_kernel(common, constants))
    })
}

/// Apply the exact `process_effective_balance_updates` Capella component atomically.
pub fn process_effective_balance_updates(
    state: &mut BeaconState,
    parameters: &CapellaParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, constants| {
        with_altair(candidate, |common| {
            altair_processing::effective_balances_kernel(common, constants)
        })
    })
}

/// Apply the exact `process_slashings_reset` Capella component atomically.
pub fn process_slashings_reset(
    state: &mut BeaconState,
    parameters: &CapellaParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, constants| {
        with_altair(candidate, |common| {
            altair_processing::slashings_reset_kernel(common, constants)
        })
    })
}

/// Apply the exact `process_randao_mixes_reset` Capella component atomically.
pub fn process_randao_mixes_reset(
    state: &mut BeaconState,
    parameters: &CapellaParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, constants| {
        with_altair(candidate, |common| altair_processing::randao_reset_kernel(common, constants))
    })
}

fn historical_summaries_kernel(
    state: &mut BeaconState,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    let next_epoch =
        add(state.slot / constants.slots_per_epoch, 1, "historical summaries next epoch")?;
    if next_epoch.is_multiple_of(constants.historical_slots / constants.slots_per_epoch) {
        if state.historical_summaries.values.len() >= 16_777_216 {
            return Err(ProcessingError::InvalidState("historical summaries limit reached"));
        }
        state.historical_summaries.values.push(crate::ssz::capella::HistoricalSummary {
            block_summary_root: state.block_roots.hash_tree_root(constants.preset())?,
            state_summary_root: state.state_roots.hash_tree_root(constants.preset())?,
        });
    }
    Ok(())
}

/// Append separate block/state historical summaries; legacy historical roots remain frozen.
pub fn process_historical_summaries_update(
    state: &mut BeaconState,
    parameters: &CapellaParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, historical_summaries_kernel)
}

/// Apply the exact `process_participation_flag_updates` Capella component atomically.
pub fn process_participation_flag_updates(
    state: &mut BeaconState,
    parameters: &CapellaParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, constants| {
        with_altair(candidate, |common| altair_processing::participation_kernel(common, constants))
    })
}

/// Apply the exact `process_sync_committee_updates` Capella component atomically.
pub fn process_sync_committee_updates(
    state: &mut BeaconState,
    parameters: &CapellaParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, constants| {
        with_altair(candidate, |common| altair_processing::sync_committee_kernel(common, constants))
    })
}

/// Replay into a private temporary candidate without allocating a second complete state copy.
/// Errors may modify this candidate; callers must discard it and never pass retained state.
pub(crate) fn process_slots_in_place(
    state: &mut BeaconState,
    target: u64,
    parameters: &CapellaParameters,
) -> Result<(), ProcessingError> {
    let constants = checked_constants(state, parameters)?;
    slots_kernel(state, target, parameters, &constants)?;
    checked_constants(state, parameters)?;
    Ok(())
}

#[cfg(test)]
mod tests;

/// Derive the exact committee from borrowed registry and RANDAO data without cloning state.
pub fn get_beacon_committee(
    state: &BeaconState,
    slot: u64,
    index: u64,
    parameters: &CapellaParameters,
) -> Result<Vec<u64>, ProcessingError> {
    let constants = checked_constants(state, parameters)?;
    crate::common::beacon_committee(state, slot, index, &constants)
}
