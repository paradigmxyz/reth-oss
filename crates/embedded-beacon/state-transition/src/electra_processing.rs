//! Atomic Electra upgrades, epochs and strict signed import with required execution verification.
//!
//! Complete state roots use all 37 fields. Changed churn, weighting and pending queues
//! have dedicated kernels; unchanged participation routines use a restoring shared view.

use crate::{
    common::with_altair,
    deneb_processing::DenebParameters,
    phase0_processing::{add, position, Constants, ProcessingError},
    ssz::{
        electra::{BeaconState, ExecutionPayload, SignedBeaconBlock},
        Preset,
    },
};
use reth_embedded_beacon_primitives::Root;
use std::future::Future;

pub use crate::bellatrix_processing::ImportError;

mod blocks;
pub(crate) mod common;
mod epochs;
pub(crate) mod execution_requests;
mod operations;
mod slots;
mod upgrade;
mod withdrawals;

pub use blocks::{
    get_beacon_proposer_index, kzg_commitment_to_versioned_hash, process_block_header,
    process_eth1_data, process_execution_payload, process_randao, process_sync_aggregate,
    validate_execution_payload_consensus, verify_block_signature,
};
pub use epochs::{
    process_effective_balance_updates, process_epoch, process_eth1_data_reset,
    process_historical_summaries_update, process_inactivity_updates,
    process_justification_and_finalization, process_participation_flag_updates,
    process_pending_consolidations, process_pending_deposits, process_randao_mixes_reset,
    process_registry_updates, process_rewards_and_penalties, process_slashings,
    process_slashings_reset, process_sync_committee_updates,
};
pub use execution_requests::{
    apply_pending_deposit, get_execution_requests_list, get_validator_from_deposit,
    is_valid_deposit_signature, is_valid_switch_to_compounding_request,
    process_consolidation_request, process_deposit, process_deposit_request,
    process_withdrawal_request,
};
pub use operations::{
    get_attesting_indices, get_indexed_attestation, is_valid_indexed_attestation,
    process_attestation, process_attester_slashing, process_bls_to_execution_change,
    process_operations, process_proposer_slashing, process_voluntary_exit,
};
#[cfg(feature = "spec-tests")]
pub(crate) use slots::process_slots_with_policy;
pub use slots::{process_slot, process_slots};

/// Artificial published signed fixtures; no runtime authority or policy toggle.
#[cfg(feature = "spec-tests")]
pub mod spec_only {
    pub use super::operations::spec_only::*;
    use super::{
        blocks, checked_constants, slots, BeaconState, ElectraParameters, ExecutionEngine,
        ImportError, ProcessingError, SignedBeaconBlock,
    };
    /// Apply artificial signed data while preserving all non-BLS consensus checks and post roots.
    #[cfg(feature = "spec-tests")]
    pub async fn state_transition<E: ExecutionEngine>(
        state: &mut BeaconState,
        block: &SignedBeaconBlock,
        parameters: &ElectraParameters,
        engine: &E,
    ) -> Result<(), ImportError<E::Error>> {
        block.hash_tree_root(parameters.preset()).map_err(ProcessingError::from)?;
        let constants = checked_constants(state, parameters)?;
        let mut candidate = state.clone();
        if candidate.slot != block.message.slot {
            slots::slots_kernel_with_policy::<false>(
                &mut candidate,
                block.message.slot,
                parameters,
                &constants,
            )?;
        }
        blocks::artificial::process_block(
            &mut candidate,
            &block.message,
            parameters,
            &constants,
            engine,
        )
        .await?;
        checked_constants(&candidate, parameters)?;
        if candidate.hash_tree_root(parameters.preset()).map_err(ProcessingError::from)? !=
            block.message.state_root
        {
            return Err(ProcessingError::InvalidState("block post-state root mismatch").into())
        }
        *state = candidate;
        Ok(())
    }
    /// Replay artificial fixture slots, including delayed deposit signature policy.
    #[cfg(feature = "spec-tests")]
    pub fn process_slots(
        state: &mut BeaconState,
        target: u64,
        parameters: &ElectraParameters,
    ) -> Result<(), ProcessingError> {
        super::process_slots_with_policy::<false>(state, target, parameters)
    }
}
pub use upgrade::upgrade_to_electra;
pub use withdrawals::{
    get_expected_withdrawals, get_pending_partial_withdrawals, get_validators_sweep_withdrawals,
    is_eligible_for_partial_withdrawals, is_fully_withdrawable_validator,
    is_partially_withdrawable_validator, process_withdrawals, ExpectedWithdrawals,
};

/// Exact trusted configuration for Electra processing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ElectraParameters {
    /// Inherited clock, presets, genesis and fixed voluntary-exit signature version.
    pub deneb: DenebParameters,
    /// Version written at the trusted scheduled upgrade.
    pub fork_version: [u8; 4],
    /// Minimum balance-based churn per epoch, denominated in Gwei.
    pub min_per_epoch_churn_limit: u64,
    /// Maximum balance churn dedicated to activations and exits, in Gwei.
    pub max_per_epoch_activation_exit_churn_limit: u64,
    /// Maximum blob commitments permitted by consensus for this fork.
    pub max_blobs_per_block: u64,
}
impl ElectraParameters {
    /// Exact pinned preset and its default network values.
    pub const fn for_preset(preset: Preset) -> Self {
        Self {
            deneb: DenebParameters::for_preset(preset),
            fork_version: match preset {
                Preset::Mainnet => [5, 0, 0, 0],
                Preset::Minimal => [5, 0, 0, 1],
            },
            min_per_epoch_churn_limit: match preset {
                Preset::Mainnet => 128_000_000_000,
                Preset::Minimal => 64_000_000_000,
            },
            max_per_epoch_activation_exit_churn_limit: match preset {
                Preset::Mainnet => 256_000_000_000,
                Preset::Minimal => 128_000_000_000,
            },
            max_blobs_per_block: 9,
        }
    }
    /// Reject zero divisors, fractional churn increments and unsupported blob capacities.
    pub fn validate(&self) -> Result<(), ProcessingError> {
        self.deneb.validate()?;
        if self.min_per_epoch_churn_limit == 0 ||
            self.max_per_epoch_activation_exit_churn_limit == 0 ||
            !self.min_per_epoch_churn_limit.is_multiple_of(common::INCREMENT) ||
            !self.max_per_epoch_activation_exit_churn_limit.is_multiple_of(common::INCREMENT)
        {
            return Err(ProcessingError::InvalidParameters(
                "Electra churn must be nonzero whole balance increments",
            ))
        }
        if self.max_blobs_per_block == 0 || self.max_blobs_per_block > 4096 {
            return Err(ProcessingError::InvalidParameters(
                "Electra blob limit is outside SSZ capacity",
            ))
        }
        Ok(())
    }
    /// Selected canonical SSZ and algorithm preset.
    pub const fn preset(&self) -> Preset {
        self.deneb.preset()
    }
    /// Exact slots per epoch for this preset.
    pub const fn slots_per_epoch(&self) -> u64 {
        self.deneb.slots_per_epoch()
    }
}

/// Exact V4 candidate inputs bound inside the atomic state import.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutionContext {
    /// Candidate beacon slot.
    pub slot: u64,
    /// Parent beacon block root committed by the candidate.
    pub parent_beacon_block_root: Root,
    /// Committed execution parent hash, absent before the first merge payload.
    pub previous_execution_payload_hash: Option<Root>,
    /// Ordered versioned hashes derived from the blob commitments.
    pub expected_blob_versioned_hashes: Vec<Root>,
    /// Canonical type-prefixed nonempty execution request lists in ascending type order.
    pub execution_requests_list: Vec<Vec<u8>>,
}
/// Required exact-candidate V4 execution verification, including requests and blob context.
///
/// Success means actual VALID with matching latest valid hash for the correctly computed
/// execution block hash and every context input. Optimistic or malformed replies must fail.
pub trait ExecutionEngine: Send + Sync {
    /// Engine validation, transport or malformed-response error.
    type Error: std::error::Error + Send + Sync + 'static;
    /// Verify first-payload terminal eligibility against a configured activated override or
    /// authenticated terminal block and parent total difficulties. VALID alone is insufficient;
    /// missing evidence must reject, and this callback runs before new-payload verification.
    fn verify_terminal_block<'a>(
        &'a self,
        payload: &'a ExecutionPayload,
        context: ExecutionContext,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;
    /// Verify this candidate through the configured engine before any state is published.
    fn verify_payload<'a>(
        &'a self,
        payload: &'a ExecutionPayload,
        context: ExecutionContext,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;
}
pub(crate) const fn constants_for_preset(preset: Preset) -> Constants {
    let mut constants = Constants::for_preset(preset);
    constants.inactivity_penalty_quotient = 16_777_216;
    constants.proportional_slashing_multiplier = 3;
    constants.minimum_slashing_penalty_quotient = 4096;
    constants
}
pub(crate) fn checked_constants(
    state: &BeaconState,
    parameters: &ElectraParameters,
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
    .any(|&n| n != count)
    {
        return Err(ProcessingError::InvalidState("Electra validator-indexed list lengths differ"))
    }
    Ok(constants_for_preset(parameters.preset()))
}
pub(crate) fn transact<T>(
    state: &mut BeaconState,
    parameters: &ElectraParameters,
    operation: impl FnOnce(&mut BeaconState, &Constants) -> Result<T, ProcessingError>,
) -> Result<T, ProcessingError> {
    let constants = checked_constants(state, parameters)?;
    let mut candidate = state.clone();
    let result = operation(&mut candidate, &constants)?;
    checked_constants(&candidate, parameters)?;
    *state = candidate;
    Ok(result)
}
/// Replay and import a complete signed Electra block atomically with mandatory BLS and EL.
pub async fn state_transition<E: ExecutionEngine>(
    state: &mut BeaconState,
    block: &SignedBeaconBlock,
    parameters: &ElectraParameters,
    engine: &E,
) -> Result<(), ImportError<E::Error>> {
    block.hash_tree_root(parameters.preset()).map_err(ProcessingError::from)?;
    let constants = checked_constants(state, parameters)?;
    let mut candidate = state.clone();
    if candidate.slot != block.message.slot {
        slots::slots_kernel(&mut candidate, block.message.slot, parameters, &constants)?;
    }
    blocks::verify_block_signature_kernel(&candidate, block, parameters)?;
    blocks::process_block(&mut candidate, &block.message, parameters, &constants, engine).await?;
    checked_constants(&candidate, parameters)?;
    if candidate.hash_tree_root(parameters.preset()).map_err(ProcessingError::from)? !=
        block.message.state_root
    {
        return Err(ProcessingError::InvalidState("block post-state root mismatch").into())
    }
    *state = candidate;
    Ok(())
}
/// Strict signed import alias; runtime verification cannot be disabled.
pub async fn process_signed_block<E: ExecutionEngine>(
    state: &mut BeaconState,
    block: &SignedBeaconBlock,
    parameters: &ElectraParameters,
    engine: &E,
) -> Result<(), ImportError<E::Error>> {
    state_transition(state, block, parameters, engine).await
}
/// Compute the next exact weighted sync committee and real aggregate public key.
pub fn get_next_sync_committee(
    state: &BeaconState,
    parameters: &ElectraParameters,
) -> Result<crate::ssz::electra::SyncCommittee, ProcessingError> {
    let constants = checked_constants(state, parameters)?;
    common::next_sync_committee(state, &constants)
}
/// Compute the balance-based churn limit rounded down to whole effective increments.
pub fn get_balance_churn_limit(
    state: &BeaconState,
    parameters: &ElectraParameters,
) -> Result<u64, ProcessingError> {
    let constants = checked_constants(state, parameters)?;
    common::get_balance_churn_limit(state, parameters, &constants)
}
/// Compute balance churn dedicated to activations and exits.
pub fn get_activation_exit_churn_limit(
    state: &BeaconState,
    parameters: &ElectraParameters,
) -> Result<u64, ProcessingError> {
    let constants = checked_constants(state, parameters)?;
    common::get_activation_exit_churn_limit(state, parameters, &constants)
}
/// Compute balance churn dedicated to consolidations.
pub fn get_consolidation_churn_limit(
    state: &BeaconState,
    parameters: &ElectraParameters,
) -> Result<u64, ProcessingError> {
    let constants = checked_constants(state, parameters)?;
    common::get_consolidation_churn_limit(state, parameters, &constants)
}
/// Add exact validator balance to the exit queue atomically and return its epoch.
pub fn compute_exit_epoch_and_update_churn(
    state: &mut BeaconState,
    balance: u64,
    parameters: &ElectraParameters,
) -> Result<u64, ProcessingError> {
    transact(state, parameters, |candidate, constants| {
        common::compute_exit_epoch_and_update_churn(candidate, balance, parameters, constants)
    })
}
/// Add exact validator balance to the consolidation queue atomically and return its epoch.
pub fn compute_consolidation_epoch_and_update_churn(
    state: &mut BeaconState,
    balance: u64,
    parameters: &ElectraParameters,
) -> Result<u64, ProcessingError> {
    transact(state, parameters, |candidate, constants| {
        common::compute_consolidation_epoch_and_update_churn(
            candidate, balance, parameters, constants,
        )
    })
}
/// Place an unexited validator into the balance-based exit queue atomically.
pub fn initiate_validator_exit(
    state: &mut BeaconState,
    index: u64,
    parameters: &ElectraParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, constants| {
        common::initiate_validator_exit(candidate, index, parameters, constants)
    })
}
/// Apply the fork's immediate slashing penalty and exact proposer/whistleblower rewards.
pub fn slash_validator(
    state: &mut BeaconState,
    index: u64,
    whistleblower: Option<u64>,
    parameters: &ElectraParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, constants| {
        common::slash_validator_kernel(candidate, index, whistleblower, parameters, constants)
    })
}
/// Switch credentials and queue excess active balance atomically.
pub fn switch_to_compounding_validator(
    state: &mut BeaconState,
    index: u64,
    parameters: &ElectraParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, constants| {
        common::switch_to_compounding_validator(candidate, index, parameters, constants)
    })
}

#[cfg(test)]
pub(crate) mod tests;

/// Derive the exact committee from borrowed registry and RANDAO data without cloning state.
pub fn get_beacon_committee(
    state: &BeaconState,
    slot: u64,
    index: u64,
    parameters: &ElectraParameters,
) -> Result<Vec<u64>, ProcessingError> {
    let constants = checked_constants(state, parameters)?;
    crate::common::beacon_committee(state, slot, index, &constants)
}

/// Replay into a private temporary candidate without allocating a second complete state copy.
/// Errors may modify this candidate; callers must discard it and never pass retained state.
pub(crate) fn process_slots_in_place(
    state: &mut BeaconState,
    target: u64,
    parameters: &ElectraParameters,
) -> Result<(), ProcessingError> {
    let constants = checked_constants(state, parameters)?;
    slots::slots_kernel(state, target, parameters, &constants)?;
    checked_constants(state, parameters)?;
    Ok(())
}
