//! Atomic Fulu upgrades, epochs and strict signed import with required execution verification.
//!
//! Complete state roots use all 38 fields. Changed churn, weighting and pending queues
//! have dedicated kernels; unchanged participation routines use a restoring shared view.

use crate::{
    common::with_altair,
    electra_processing::ElectraParameters,
    phase0_processing::{add, position, Constants, ProcessingError},
    ssz::{
        fulu::{BeaconState, ExecutionPayload, SignedBeaconBlock},
        Preset, Ssz,
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
    get_unrealized_checkpoints, process_effective_balance_updates, process_epoch,
    process_eth1_data_reset, process_historical_summaries_update, process_inactivity_updates,
    process_justification_and_finalization, process_participation_flag_updates,
    process_pending_consolidations, process_pending_deposits, process_proposer_lookahead,
    process_randao_mixes_reset, process_registry_updates, process_rewards_and_penalties,
    process_slashings, process_slashings_reset, process_sync_committee_updates,
};
pub use execution_requests::{
    apply_pending_deposit, get_execution_requests_list, get_validator_from_deposit,
    is_valid_deposit_signature, is_valid_switch_to_compounding_request,
    process_consolidation_request, process_deposit, process_deposit_request,
    process_withdrawal_request,
};
pub use operations::{
    get_attesting_indices, get_indexed_attestation, get_validated_indexed_attestation,
    is_valid_indexed_attestation, process_attestation, process_attester_slashing,
    process_bls_to_execution_change, process_operations, process_proposer_slashing,
    process_voluntary_exit, CheckedAttestationState,
};
#[cfg(feature = "spec-tests")]
pub(crate) use slots::process_slots_with_policy;
pub use slots::{process_slot, process_slots};

/// Explicit artificial fixture operations; unavailable in runtime dependency graphs.
#[cfg(feature = "spec-tests")]
pub mod spec_only {
    /// Replay artificial fixture slots with the explicitly isolated delayed-deposit policy.
    #[cfg(feature = "spec-tests")]
    pub fn process_slots(
        state: &mut super::BeaconState,
        target: u64,
        parameters: &super::FuluParameters,
    ) -> Result<(), super::ProcessingError> {
        super::process_slots_with_policy::<false>(state, target, parameters)
    }
    pub use super::operations::spec_only::*;
    /// Artificial signed fixtures only; runtime dependencies never enable this feature.
    #[cfg(feature = "spec-tests")]
    pub async fn state_transition<E: super::ExecutionEngine>(
        state: &mut super::BeaconState,
        block: &super::SignedBeaconBlock,
        parameters: &super::FuluParameters,
        engine: &E,
    ) -> Result<(), super::ImportError<E::Error>> {
        super::state_transition_kernel::<false, E>(state, block, parameters, engine).await
    }
}
pub use upgrade::upgrade_to_fulu;
pub use withdrawals::{
    get_expected_withdrawals, get_pending_partial_withdrawals, get_validators_sweep_withdrawals,
    is_eligible_for_partial_withdrawals, is_fully_withdrawable_validator,
    is_partially_withdrawable_validator, process_withdrawals, ExpectedWithdrawals,
};

/// Exact proposer vector for all slots in one epoch.
pub type ProposerIndices = crate::ssz::Vector<u64, crate::ssz::PresetLength<32, 8>>;

/// Fork-specific blob parameter change, supplied by trusted network configuration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlobParameters {
    /// Activation epoch of this blob-only parameter change.
    pub epoch: u64,
    /// Consensus maximum blob commitments at this epoch.
    pub max_blobs_per_block: u64,
}
/// Exact trusted Fulu clock, inherited rules and epoch-indexed blob schedule.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FuluParameters {
    /// Inherited balance churn, clock and signature configuration.
    pub electra: ElectraParameters,
    /// Version written during the caller's scheduled upgrade.
    pub fork_version: [u8; 4],
    /// Trusted Fulu epoch; public presets retain their actual configured activation.
    pub fork_epoch: u64,
    /// Trusted Electra epoch used by the default blob-parameter digest.
    pub electra_fork_epoch: u64,
    /// Ordered, unique blob-only parameter changes at or after the Fulu fork.
    pub blob_schedule: Vec<BlobParameters>,
}
impl FuluParameters {
    /// Exact pinned preset and default network schedule.
    pub fn for_preset(preset: Preset) -> Self {
        Self {
            electra: ElectraParameters::for_preset(preset),
            fork_version: match preset {
                Preset::Mainnet => [6, 0, 0, 0],
                Preset::Minimal => [6, 0, 0, 1],
            },
            electra_fork_epoch: match preset {
                Preset::Mainnet => 364_032,
                Preset::Minimal => u64::MAX,
            },
            fork_epoch: match preset {
                Preset::Mainnet => 411_392,
                Preset::Minimal => u64::MAX,
            },
            blob_schedule: match preset {
                Preset::Mainnet => vec![
                    BlobParameters { epoch: 412_672, max_blobs_per_block: 15 },
                    BlobParameters { epoch: 419_072, max_blobs_per_block: 21 },
                ],
                Preset::Minimal => Vec::new(),
            },
        }
    }
    /// Reject invalid inherited divisors, duplicate/nonordered epochs and SSZ capacities.
    pub fn validate(&self) -> Result<(), ProcessingError> {
        self.electra.validate()?;
        let mut previous = None;
        for entry in &self.blob_schedule {
            if entry.epoch < self.fork_epoch ||
                previous.is_some_and(|epoch| entry.epoch <= epoch) ||
                entry.max_blobs_per_block == 0 ||
                entry.max_blobs_per_block > 4096
            {
                return Err(ProcessingError::InvalidParameters("invalid Fulu blob schedule"))
            }
            previous = Some(entry.epoch);
        }
        Ok(())
    }
    /// Exact canonical SSZ and algorithm preset.
    pub const fn preset(&self) -> Preset {
        self.electra.preset()
    }
    /// Exact slots per epoch for the selected preset.
    pub const fn slots_per_epoch(&self) -> u64 {
        self.electra.slots_per_epoch()
    }
    /// Select the latest effective blob parameters; input validity is checked by kernels.
    pub fn blob_parameters_at_epoch(&self, epoch: u64) -> BlobParameters {
        self.blob_schedule.iter().rev().find(|entry| epoch >= entry.epoch).copied().unwrap_or(
            BlobParameters {
                epoch: self.electra_fork_epoch,
                max_blobs_per_block: self.electra.max_blobs_per_block,
            },
        )
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
    parameters: &FuluParameters,
) -> Result<Constants, ProcessingError> {
    parameters.validate()?;
    // A validation guard needs every SSZ bound, not an otherwise discarded commitment.
    // Slot-root rings and the signed post-state root still hash the complete exact state.
    Ssz::validate_structure(state, parameters.preset())?;
    check_validator_lists(state)?;
    Ok(constants_for_preset(parameters.preset()))
}
/// Validate the complete state once and retain that exact immutable state's root.
fn checked_constants_with_root(
    state: &BeaconState,
    parameters: &FuluParameters,
) -> Result<(Constants, Root), ProcessingError> {
    parameters.validate()?;
    let root = state.hash_tree_root(parameters.preset())?;
    check_validator_lists(state)?;
    Ok((constants_for_preset(parameters.preset()), root))
}
fn check_validator_lists(state: &BeaconState) -> Result<(), ProcessingError> {
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
        return Err(ProcessingError::InvalidState("Fulu validator-indexed list lengths differ"))
    }
    Ok(())
}
pub(crate) fn transact<T>(
    state: &mut BeaconState,
    parameters: &FuluParameters,
    operation: impl FnOnce(&mut BeaconState, &Constants) -> Result<T, ProcessingError>,
) -> Result<T, ProcessingError> {
    let constants = checked_constants(state, parameters)?;
    let mut candidate = state.clone();
    let result = operation(&mut candidate, &constants)?;
    checked_constants(&candidate, parameters)?;
    *state = candidate;
    Ok(result)
}
/// Replay and import a complete signed Fulu block atomically with mandatory BLS and EL.
pub async fn state_transition<E: ExecutionEngine>(
    state: &mut BeaconState,
    block: &SignedBeaconBlock,
    parameters: &FuluParameters,
    engine: &E,
) -> Result<(), ImportError<E::Error>> {
    state_transition_kernel::<true, E>(state, block, parameters, engine).await
}

pub(crate) async fn state_transition_kernel<const VERIFY_BLS: bool, E: ExecutionEngine>(
    state: &mut BeaconState,
    block: &SignedBeaconBlock,
    parameters: &FuluParameters,
    engine: &E,
) -> Result<(), ImportError<E::Error>> {
    crate::common::require_signature_policy::<VERIFY_BLS>()?;
    block.hash_tree_root(parameters.preset()).map_err(ProcessingError::from)?;
    let constants = checked_constants(state, parameters)?;
    let mut candidate = state.clone();
    import_block_candidate::<VERIFY_BLS, E>(&mut candidate, block, parameters, &constants, engine)
        .await?;
    *state = candidate;
    Ok(())
}
/// Execute the same fully checked kernel on the dispatcher's disposable transaction copy.
/// This preserves every input hash, signature, engine check and output-root comparison.
/// An error may modify this copy, so callers must discard it without publishing it.
pub(crate) async fn state_transition_in_place<E: ExecutionEngine>(
    state: &mut BeaconState,
    block: &SignedBeaconBlock,
    parameters: &FuluParameters,
    engine: &E,
) -> Result<(), ImportError<E::Error>> {
    crate::common::require_signature_policy::<true>()?;
    block.hash_tree_root(parameters.preset()).map_err(ProcessingError::from)?;
    let constants = checked_constants(state, parameters)?;
    import_block_candidate::<true, E>(state, block, parameters, &constants, engine).await
}
async fn import_block_candidate<const VERIFY_BLS: bool, E: ExecutionEngine>(
    candidate: &mut BeaconState,
    block: &SignedBeaconBlock,
    parameters: &FuluParameters,
    constants: &Constants,
    engine: &E,
) -> Result<(), ImportError<E::Error>> {
    if candidate.slot != block.message.slot {
        slots::slots_kernel_with_policy::<VERIFY_BLS>(
            candidate,
            block.message.slot,
            parameters,
            constants,
        )?;
    }
    if VERIFY_BLS {
        blocks::verify_block_signature_kernel(candidate, block, parameters)?;
    }
    if VERIFY_BLS {
        blocks::process_block(candidate, &block.message, parameters, constants, engine).await?;
    } else {
        blocks::process_block_with_policy::<false, E>(
            candidate,
            &block.message,
            parameters,
            constants,
            engine,
        )
        .await?;
    }
    let (_, post_state_root) = checked_constants_with_root(candidate, parameters)?;
    if post_state_root != block.message.state_root {
        return Err(ProcessingError::InvalidState("block post-state root mismatch").into())
    }
    Ok(())
}
/// Strict signed import alias; runtime verification cannot be disabled.
pub async fn process_signed_block<E: ExecutionEngine>(
    state: &mut BeaconState,
    block: &SignedBeaconBlock,
    parameters: &FuluParameters,
    engine: &E,
) -> Result<(), ImportError<E::Error>> {
    state_transition(state, block, parameters, engine).await
}
/// Compute the next exact weighted sync committee and real aggregate public key.
pub fn get_next_sync_committee(
    state: &BeaconState,
    parameters: &FuluParameters,
) -> Result<crate::ssz::fulu::SyncCommittee, ProcessingError> {
    let constants = checked_constants(state, parameters)?;
    common::next_sync_committee(state, &constants)
}
/// Compute the balance-based churn limit rounded down to whole effective increments.
pub fn get_balance_churn_limit(
    state: &BeaconState,
    parameters: &FuluParameters,
) -> Result<u64, ProcessingError> {
    let constants = checked_constants(state, parameters)?;
    common::get_balance_churn_limit(state, parameters, &constants)
}
/// Compute balance churn dedicated to activations and exits.
pub fn get_activation_exit_churn_limit(
    state: &BeaconState,
    parameters: &FuluParameters,
) -> Result<u64, ProcessingError> {
    let constants = checked_constants(state, parameters)?;
    common::get_activation_exit_churn_limit(state, parameters, &constants)
}
/// Compute balance churn dedicated to consolidations.
pub fn get_consolidation_churn_limit(
    state: &BeaconState,
    parameters: &FuluParameters,
) -> Result<u64, ProcessingError> {
    let constants = checked_constants(state, parameters)?;
    common::get_consolidation_churn_limit(state, parameters, &constants)
}
/// Add exact validator balance to the exit queue atomically and return its epoch.
pub fn compute_exit_epoch_and_update_churn(
    state: &mut BeaconState,
    balance: u64,
    parameters: &FuluParameters,
) -> Result<u64, ProcessingError> {
    transact(state, parameters, |candidate, constants| {
        common::compute_exit_epoch_and_update_churn(candidate, balance, parameters, constants)
    })
}
/// Add exact validator balance to the consolidation queue atomically and return its epoch.
pub fn compute_consolidation_epoch_and_update_churn(
    state: &mut BeaconState,
    balance: u64,
    parameters: &FuluParameters,
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
    parameters: &FuluParameters,
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
    parameters: &FuluParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, constants| {
        common::slash_validator_kernel(candidate, index, whistleblower, parameters, constants)
    })
}
/// Switch credentials and queue excess active balance atomically.
pub fn switch_to_compounding_validator(
    state: &mut BeaconState,
    index: u64,
    parameters: &FuluParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, constants| {
        common::switch_to_compounding_validator(candidate, index, parameters, constants)
    })
}

/// Compute every proposer for a selected epoch using the exact16-bit balance weights.
pub fn get_beacon_proposer_indices(
    state: &BeaconState,
    epoch: u64,
    parameters: &FuluParameters,
) -> Result<ProposerIndices, ProcessingError> {
    let constants = checked_constants(state, parameters)?;
    Ok(crate::ssz::Vector::from_vec(common::proposer_indices(state, epoch, &constants)?))
}
/// Initialize the current and next epoch proposer lookahead from the authenticated state.
pub fn initialize_proposer_lookahead(
    state: &BeaconState,
    parameters: &FuluParameters,
) -> Result<crate::ssz::fulu::ProposerLookahead, ProcessingError> {
    let constants = checked_constants(state, parameters)?;
    let current = state.slot / constants.slots_per_epoch;
    let mut indices = common::proposer_indices(state, current, &constants)?;
    indices.extend(common::proposer_indices(
        state,
        add(current, 1, "next proposer epoch")?,
        &constants,
    )?);
    Ok(crate::ssz::Vector::from_vec(indices))
}

#[cfg(test)]
pub(crate) mod tests;

/// Derive the exact committee from borrowed registry and RANDAO data without cloning state.
pub fn get_beacon_committee(
    state: &BeaconState,
    slot: u64,
    index: u64,
    parameters: &FuluParameters,
) -> Result<Vec<u64>, ProcessingError> {
    let constants = checked_constants(state, parameters)?;
    crate::common::beacon_committee(state, slot, index, &constants)
}

/// Replay into a private temporary candidate without allocating a second complete state copy.
/// Errors may modify this candidate; callers must discard it and never pass retained state.
pub(crate) fn process_slots_in_place(
    state: &mut BeaconState,
    target: u64,
    parameters: &FuluParameters,
) -> Result<(), ProcessingError> {
    let constants = checked_constants(state, parameters)?;
    slots::slots_kernel(state, target, parameters, &constants)?;
    checked_constants(state, parameters)?;
    Ok(())
}
