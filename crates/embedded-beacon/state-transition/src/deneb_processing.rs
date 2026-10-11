//! Atomic Deneb processing and versioned execution verification.
//!
//! Blob sidecar availability/proofs and trusted fork scheduling remain caller protocol
//! requirements; this state transition checks block commitment limits and passes every
//! versioned hash and parent beacon root to its mandatory execution adapter.

use crate::{
    altair_processing,
    capella_processing::CapellaParameters,
    common::with_altair,
    phase0_processing::{add, mul, position, Constants, ProcessingError},
    ssz::{
        altair, capella,
        deneb::{self, BeaconState, ExecutionPayload, ExecutionPayloadHeader},
        List, Preset, Ssz,
    },
};
use reth_embedded_beacon_primitives::{sha256, Root};
use std::future::Future;

mod block;
mod slot_cache;

#[cfg(feature = "spec-tests")]
pub use block::spec_only;

pub use block::{
    get_attesting_indices, get_beacon_proposer_index, get_indexed_attestation,
    is_valid_indexed_attestation, process_attestation, process_attester_slashing,
    process_block_header, process_bls_to_execution_change, process_deposit, process_eth1_data,
    process_execution_payload, process_operations, process_proposer_slashing, process_randao,
    process_signed_block, process_sync_aggregate, process_voluntary_exit, process_withdrawals,
    state_transition, verify_block_signature,
};

/// Trusted network configuration for exact Deneb processing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DenebParameters {
    /// Inherited clock, churn, participation and withdrawal configuration.
    pub capella: CapellaParameters,
    /// Version written at the scheduled Deneb upgrade.
    pub fork_version: [u8; 4],
    /// Consensus blob count limit, distinct from the SSZ commitment list bound.
    pub max_blobs_per_block: u64,
    /// Upper bound on activation churn; exit churn retains the ordinary formula.
    pub max_per_epoch_activation_churn_limit: u64,
}

impl DenebParameters {
    /// Exact pinned preset and default network configuration.
    pub const fn for_preset(preset: Preset) -> Self {
        Self {
            capella: CapellaParameters::for_preset(preset),
            fork_version: match preset {
                Preset::Mainnet => [4, 0, 0, 0],
                Preset::Minimal => [4, 0, 0, 1],
            },
            max_blobs_per_block: 6,
            max_per_epoch_activation_churn_limit: match preset {
                Preset::Mainnet => 8,
                Preset::Minimal => 4,
            },
        }
    }

    /// Reject unsupported blob capacities and inherited zero divisors.
    pub fn validate(&self) -> Result<(), ProcessingError> {
        self.capella.validate()?;
        if self.max_blobs_per_block == 0 || self.max_blobs_per_block > 4096 {
            return Err(ProcessingError::InvalidParameters(
                "Deneb blob limit is outside SSZ capacity",
            ))
        }
        if self.max_per_epoch_activation_churn_limit == 0 {
            return Err(ProcessingError::InvalidParameters("activation churn cap must be nonzero"))
        }
        Ok(())
    }

    /// Exact selected SSZ and deterministic algorithm preset.
    pub const fn preset(&self) -> Preset {
        self.capella.preset()
    }

    /// Exact slots per epoch for the selected preset.
    pub const fn slots_per_epoch(&self) -> u64 {
        self.capella.slots_per_epoch()
    }
}

/// Context bound to the exact versioned execution candidate during import.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutionContext {
    /// Slot of the candidate beacon block.
    pub slot: u64,
    /// Committed parent root passed to execution for EIP-4788.
    pub parent_beacon_block_root: Root,
    /// Committed execution parent hash, absent before the first merge payload.
    pub previous_execution_payload_hash: Option<Root>,
    /// Ordered EIP-4844 hashes derived from every block commitment.
    pub expected_blob_versioned_hashes: Vec<Root>,
}

/// Mandatory exact-candidate version-three execution verification.
///
/// Successful completion requires actual VALID with matching latest valid hash, correct
/// computed block hash, transaction versioned hashes and parent beacon root. Optimistic,
/// malformed and transport responses must fail. Private specification adapters do not
/// establish actual execution validity and cannot select a runtime launcher path.
pub trait ExecutionEngine: Send + Sync {
    /// Execution rejection, malformed response or transport failure.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Verify first-payload terminal eligibility against a configured activated override or
    /// authenticated terminal block and parent total difficulties. VALID alone is insufficient;
    /// missing evidence must reject, and this callback runs before new-payload verification.
    fn verify_terminal_block<'a>(
        &'a self,
        payload: &'a ExecutionPayload,
        context: ExecutionContext,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;
    /// Verify the exact candidate and its complete context before state publication.
    fn verify_payload<'a>(
        &'a self,
        payload: &'a ExecutionPayload,
        context: ExecutionContext,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;
}

pub use crate::bellatrix_processing::ImportError;

const fn constants_for_preset(preset: Preset) -> Constants {
    let mut constants = Constants::for_preset(preset);
    constants.inactivity_penalty_quotient = 16_777_216;
    constants.minimum_slashing_penalty_quotient = 32;
    constants.proportional_slashing_multiplier = 3;
    constants
}

pub(crate) fn checked_constants(
    state: &BeaconState,
    parameters: &DenebParameters,
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
        return Err(ProcessingError::InvalidState("Deneb validator-indexed list lengths differ"))
    }
    Ok(constants_for_preset(parameters.preset()))
}

fn transact<T>(
    state: &mut BeaconState,
    parameters: &DenebParameters,
    operation: impl FnOnce(&mut BeaconState, &Constants) -> Result<T, ProcessingError>,
) -> Result<T, ProcessingError> {
    let constants = checked_constants(state, parameters)?;
    let mut candidate = state.clone();
    let result = operation(&mut candidate, &constants)?;
    checked_constants(&candidate, parameters)?;
    *state = candidate;
    Ok(result)
}

/// Exact irregular Capella-to-Deneb upgrade at the caller's trusted scheduled boundary.
pub fn upgrade_to_deneb(
    pre: &capella::BeaconState,
    parameters: &DenebParameters,
) -> Result<BeaconState, ProcessingError> {
    parameters.validate()?;
    pre.hash_tree_root(parameters.preset())?;
    let header = &pre.latest_execution_payload_header;
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
        current_sync_committee: deneb::SyncCommittee {
            pubkeys: pre.current_sync_committee.pubkeys.clone(),
            aggregate_pubkey: pre.current_sync_committee.aggregate_pubkey,
        },
        next_sync_committee: deneb::SyncCommittee {
            pubkeys: pre.next_sync_committee.pubkeys.clone(),
            aggregate_pubkey: pre.next_sync_committee.aggregate_pubkey,
        },
        latest_execution_payload_header: ExecutionPayloadHeader {
            parent_hash: header.parent_hash,
            fee_recipient: header.fee_recipient,
            state_root: header.state_root,
            receipts_root: header.receipts_root,
            logs_bloom: header.logs_bloom.clone(),
            prev_randao: header.prev_randao,
            block_number: header.block_number,
            gas_limit: header.gas_limit,
            gas_used: header.gas_used,
            timestamp: header.timestamp,
            extra_data: header.extra_data.clone(),
            base_fee_per_gas: header.base_fee_per_gas,
            block_hash: header.block_hash,
            transactions_root: header.transactions_root,
            withdrawals_root: header.withdrawals_root,
            blob_gas_used: 0,
            excess_blob_gas: 0,
        },
        next_withdrawal_index: pre.next_withdrawal_index,
        next_withdrawal_validator_index: pre.next_withdrawal_validator_index,
        historical_summaries: List::from_vec(
            pre.historical_summaries
                .values
                .iter()
                .map(|summary| deneb::HistoricalSummary {
                    block_summary_root: summary.block_summary_root,
                    state_summary_root: summary.state_summary_root,
                })
                .collect(),
        ),
    };
    checked_constants(&post, parameters)?;
    Ok(post)
}

/// Derive the specified version prefix followed by the commitment hash's remaining bytes.
pub fn kzg_commitment_to_versioned_hash(commitment: &[u8; 48]) -> Root {
    let mut hash = sha256(commitment);
    hash[0] = 1;
    hash
}

/// Check pure execution consensus invariants and derive the exact committed header.
///
/// This does not establish execution validity or blob availability. Signed import must
/// invoke its execution adapter with the actual ordered versioned hashes before commit.
pub fn validate_execution_payload_consensus(
    state: &BeaconState,
    payload: &ExecutionPayload,
    commitments: &[[u8; 48]],
    parameters: &DenebParameters,
) -> Result<ExecutionPayloadHeader, ProcessingError> {
    let constants = checked_constants(state, parameters)?;
    payload.hash_tree_root(parameters.preset())?;
    if payload.parent_hash != state.latest_execution_payload_header.block_hash {
        return Err(ProcessingError::InvalidState("execution payload parent hash mismatch"))
    }
    let current = state.slot / constants.slots_per_epoch;
    if payload.prev_randao !=
        *state
            .randao_mixes
            .values
            .get(position(current, constants.historical_epochs)?)
            .ok_or(ProcessingError::InvalidState("RANDAO ring is truncated"))?
    {
        return Err(ProcessingError::InvalidState("execution payload RANDAO mismatch"))
    }
    let timestamp = add(
        state.genesis_time,
        mul(
            state.slot,
            parameters.capella.bellatrix.seconds_per_slot,
            "execution timestamp slot duration",
        )?,
        "execution timestamp",
    )?;
    if payload.timestamp != timestamp {
        return Err(ProcessingError::InvalidState("execution payload timestamp mismatch"))
    }
    if commitments.len() as u64 > parameters.max_blobs_per_block {
        return Err(ProcessingError::InvalidState("Deneb block exceeds configured blob limit"))
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

/// Cache the complete Deneb pre-state root and latest header without advancing the slot.
pub fn process_slot(
    state: &mut BeaconState,
    parameters: &DenebParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, slot_kernel)
}

/// Replay empty Deneb slots, with complete epoch processing and actual fork state roots.
pub fn process_slots(
    state: &mut BeaconState,
    target_slot: u64,
    parameters: &DenebParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, constants| {
        slots_kernel(candidate, target_slot, parameters, constants)
    })
}

/// Apply the exact ordered Deneb epoch kernels atomically.
pub fn process_epoch(
    state: &mut BeaconState,
    parameters: &DenebParameters,
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
    parameters: &DenebParameters,
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

fn registry_common(
    state: &mut altair::BeaconState,
    parameters: &DenebParameters,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    let current = state.slot / constants.slots_per_epoch;
    let inherited = &parameters.capella.bellatrix.altair;
    for index in 0..state.validators.values.len() {
        let validator = &mut state.validators.values[index];
        if validator.activation_eligibility_epoch == u64::MAX &&
            validator.effective_balance == 32_000_000_000
        {
            validator.activation_eligibility_epoch =
                add(current, 1, "activation eligibility epoch")?;
        }
        if crate::phase0_processing::is_active(validator, current) &&
            validator.effective_balance <= inherited.phase0.ejection_balance
        {
            altair_processing::exit_kernel(state, index, inherited, constants)?;
        }
    }
    let mut queue = state
        .validators
        .values
        .iter()
        .enumerate()
        .filter_map(|(index, validator)| {
            (validator.activation_eligibility_epoch <= state.finalized_checkpoint.epoch &&
                validator.activation_epoch == u64::MAX)
                .then_some(index)
        })
        .collect::<Vec<_>>();
    queue.sort_unstable_by_key(|&index| {
        (state.validators.values[index].activation_eligibility_epoch, index)
    });
    let limit = altair_processing::churn(state, inherited, constants)
        .min(parameters.max_per_epoch_activation_churn_limit);
    let activation = altair_processing::activation_exit_epoch(current)?;
    for index in queue.into_iter().take(usize::try_from(limit).unwrap_or(usize::MAX)) {
        state.validators.values[index].activation_epoch = activation;
    }
    Ok(())
}

fn historical_kernel(
    state: &mut BeaconState,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    let next = add(state.slot / constants.slots_per_epoch, 1, "next historical epoch")?;
    if next.is_multiple_of(constants.historical_slots / constants.slots_per_epoch) {
        state.historical_summaries.values.push(deneb::HistoricalSummary {
            block_summary_root: state.block_roots.hash_tree_root(constants.preset())?,
            state_summary_root: state.state_roots.hash_tree_root(constants.preset())?,
        });
    }
    Ok(())
}

fn epoch_kernel(
    state: &mut BeaconState,
    parameters: &DenebParameters,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    let inherited = &parameters.capella.bellatrix.altair;
    with_altair(state, |common| {
        altair_processing::justification_kernel(common, constants)?;
        altair_processing::inactivity_kernel(common, inherited, constants)?;
        altair_processing::rewards_kernel(common, inherited, constants)?;
        registry_common(common, parameters, constants)?;
        altair_processing::slashings_kernel(common, constants)?;
        altair_processing::eth1_reset_kernel(common, constants)?;
        altair_processing::effective_balances_kernel(common, constants)?;
        altair_processing::slashings_reset_kernel(common, constants)?;
        altair_processing::randao_reset_kernel(common, constants)
    })?;
    historical_kernel(state, constants)?;
    with_altair(state, |common| {
        altair_processing::participation_kernel(common, constants)?;
        altair_processing::sync_committee_kernel(common, constants)
    })
}

/// Apply `process_justification_and_finalization` with the exact Deneb parameters.
pub fn process_justification_and_finalization(
    state: &mut BeaconState,
    parameters: &DenebParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, constants| {
        with_altair(candidate, |common| altair_processing::justification_kernel(common, constants))
    })
}

/// Apply `process_inactivity_updates` with the exact Deneb parameters.
pub fn process_inactivity_updates(
    state: &mut BeaconState,
    parameters: &DenebParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, constants| {
        with_altair(candidate, |common| {
            altair_processing::inactivity_kernel(
                common,
                &parameters.capella.bellatrix.altair,
                constants,
            )
        })
    })
}

/// Apply `process_rewards_and_penalties` with the exact Deneb parameters.
pub fn process_rewards_and_penalties(
    state: &mut BeaconState,
    parameters: &DenebParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, constants| {
        with_altair(candidate, |common| {
            altair_processing::rewards_kernel(
                common,
                &parameters.capella.bellatrix.altair,
                constants,
            )
        })
    })
}

/// Apply `process_slashings` with the exact Deneb parameters.
pub fn process_slashings(
    state: &mut BeaconState,
    parameters: &DenebParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, constants| {
        with_altair(candidate, |common| altair_processing::slashings_kernel(common, constants))
    })
}

/// Apply `process_eth1_data_reset` with the exact Deneb parameters.
pub fn process_eth1_data_reset(
    state: &mut BeaconState,
    parameters: &DenebParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, constants| {
        with_altair(candidate, |common| altair_processing::eth1_reset_kernel(common, constants))
    })
}

/// Apply `process_effective_balance_updates` with the exact Deneb parameters.
pub fn process_effective_balance_updates(
    state: &mut BeaconState,
    parameters: &DenebParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, constants| {
        with_altair(candidate, |common| {
            altair_processing::effective_balances_kernel(common, constants)
        })
    })
}

/// Apply `process_slashings_reset` with the exact Deneb parameters.
pub fn process_slashings_reset(
    state: &mut BeaconState,
    parameters: &DenebParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, constants| {
        with_altair(candidate, |common| {
            altair_processing::slashings_reset_kernel(common, constants)
        })
    })
}

/// Apply `process_randao_mixes_reset` with the exact Deneb parameters.
pub fn process_randao_mixes_reset(
    state: &mut BeaconState,
    parameters: &DenebParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, constants| {
        with_altair(candidate, |common| altair_processing::randao_reset_kernel(common, constants))
    })
}

/// Apply `process_participation_flag_updates` with the exact Deneb parameters.
pub fn process_participation_flag_updates(
    state: &mut BeaconState,
    parameters: &DenebParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, constants| {
        with_altair(candidate, |common| altair_processing::participation_kernel(common, constants))
    })
}

/// Apply `process_sync_committee_updates` with the exact Deneb parameters.
pub fn process_sync_committee_updates(
    state: &mut BeaconState,
    parameters: &DenebParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, constants| {
        with_altair(candidate, |common| altair_processing::sync_committee_kernel(common, constants))
    })
}

/// Process activation/ejection queues with capped activation churn and unchanged exit churn.
pub fn process_registry_updates(
    state: &mut BeaconState,
    parameters: &DenebParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, constants| {
        with_altair(candidate, |common| registry_common(common, parameters, constants))
    })
}

/// Update historical summaries from the complete block/state root rings.
pub fn process_historical_summaries_update(
    state: &mut BeaconState,
    parameters: &DenebParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, historical_kernel)
}

/// Replay into a private temporary candidate without allocating a second complete state copy.
/// Errors may modify this candidate; callers must discard it and never pass retained state.
pub(crate) fn process_slots_in_place(
    state: &mut BeaconState,
    target: u64,
    parameters: &DenebParameters,
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
    parameters: &DenebParameters,
) -> Result<Vec<u64>, ProcessingError> {
    let constants = checked_constants(state, parameters)?;
    crate::common::beacon_committee(state, slot, index, &constants)
}
