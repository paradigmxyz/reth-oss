//! Exact progressive Gloas state transitions with separate execution envelope verification.
//!
//! Beacon block import commits only after slots, mandatory signatures and the complete
//! post-state root agree. Envelope validation is a separate immutable mandatory-engine
//! operation; runtime execution/full-data admission remains a separate responsibility.

use crate::{
    common::with_altair,
    fulu_processing::FuluParameters,
    phase0_processing::{add, position, Constants, ProcessingError},
    ssz::{
        gloas::{BeaconState, ExecutionPayload, SignedBeaconBlock},
        Preset, Ssz,
    },
};
use reth_embedded_beacon_primitives::Root;
use std::future::Future;

pub use crate::bellatrix_processing::ImportError;

pub mod blocks;
pub mod builder;
pub(crate) mod common;
mod epochs;
pub mod execution_requests;
pub mod operations;
pub mod payload;
pub mod ptc;
mod slots;
mod upgrade;
pub mod withdrawals;

pub use blocks::{
    get_beacon_proposer_index, process_block_header, process_eth1_data, process_randao,
    process_sync_aggregate, verify_block_signature,
};
pub use builder::{
    can_builder_cover_bid, get_index_for_new_builder, get_pending_balance_to_withdraw_for_builder,
    initiate_builder_exit, is_active_builder, is_pending_validator,
    is_valid_builder_deposit_signature, onboard_builders_from_pending_deposits,
    process_builder_deposit_request, process_builder_exit_request, process_execution_payload_bid,
    settle_builder_payment, verify_execution_payload_bid_signature,
};
pub use epochs::{
    get_unrealized_checkpoints, process_builder_pending_payments,
    process_effective_balance_updates, process_epoch, process_eth1_data_reset,
    process_historical_summaries_update, process_inactivity_updates,
    process_justification_and_finalization, process_participation_flag_updates,
    process_pending_consolidations, process_pending_deposits, process_proposer_lookahead,
    process_ptc_window, process_randao_mixes_reset, process_registry_updates,
    process_rewards_and_penalties, process_slashings, process_slashings_reset,
    process_sync_committee_updates,
};
pub use execution_requests::{
    apply_pending_deposit, get_execution_requests_list, get_validator_from_deposit,
    is_valid_deposit_signature, is_valid_switch_to_compounding_request,
    process_consolidation_request, process_deposit_request, process_withdrawal_request,
};
pub use operations::{
    get_attesting_indices, get_indexed_attestation, get_validated_indexed_attestation,
    is_attestation_same_slot, is_valid_indexed_attestation, process_attestation,
    process_attester_slashing, process_bls_to_execution_change, process_operations,
    process_proposer_slashing, process_voluntary_exit, CheckedAttestationState,
};
pub use payload::{
    apply_parent_execution_payload, kzg_commitment_to_versioned_hash,
    process_parent_execution_payload, verify_execution_payload_envelope,
    verify_execution_payload_envelope_signature,
};
pub use ptc::{
    compute_ptc, get_indexed_payload_attestation, get_ptc, is_valid_indexed_payload_attestation,
    process_payload_attestation, IndexedPayloadAttestation,
};
pub use withdrawals::{
    get_builder_balance_after_withdrawals, get_builder_withdrawals, get_builders_sweep_withdrawals,
    get_expected_withdrawals, get_pending_partial_withdrawals, get_validators_sweep_withdrawals,
    process_withdrawals, ExpectedWithdrawals,
};

#[cfg(feature = "spec-tests")]
pub(crate) use slots::process_slots_with_policy;
pub use slots::{process_slot, process_slots, replay_slots};
pub use upgrade::{initialize_ptc_window, upgrade_to_gloas};

#[cfg(feature = "spec-tests")]
mod spec_only_verification;

/// Artificial published fixtures only; absent from every runtime dependency graph.
#[cfg(feature = "spec-tests")]
pub mod spec_only {
    use super::{
        blocks, checked_constants, checked_constants_with_root, slots, BeaconState,
        GloasParameters, ProcessingError, SignedBeaconBlock,
    };
    pub use super::{
        operations::spec_only::*,
        spec_only_verification::{
            is_valid_indexed_attestation, is_valid_indexed_payload_attestation,
            verify_execution_payload_envelope,
        },
    };
    /// Artificial one-time onboarding; no runtime validation authority is constructed.
    #[cfg(feature = "spec-tests")]
    pub fn upgrade_to_gloas(
        pre: &crate::ssz::fulu::BeaconState,
        parameters: &GloasParameters,
    ) -> Result<BeaconState, ProcessingError> {
        super::upgrade::artificial::upgrade_to_gloas(pre, parameters)
    }
    /// Apply artificial signed data with exact non-BLS consensus checks and complete post root.
    #[cfg(feature = "spec-tests")]
    pub fn state_transition(
        state: &mut BeaconState,
        block: &SignedBeaconBlock,
        parameters: &GloasParameters,
    ) -> Result<(), ProcessingError> {
        block.hash_tree_root(parameters.preset())?;
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
        blocks::artificial::process_block(&mut candidate, &block.message, parameters, &constants)?;
        let (_, post_state_root) = checked_constants_with_root(&candidate, parameters)?;
        if post_state_root != block.message.state_root {
            return Err(ProcessingError::InvalidState("block post-state root mismatch"))
        }
        *state = candidate;
        Ok(())
    }
    /// Replay artificial fixture slots with the explicitly isolated delayed-deposit policy.
    #[cfg(feature = "spec-tests")]
    pub fn process_slots(
        state: &mut BeaconState,
        target: u64,
        parameters: &GloasParameters,
    ) -> Result<(), ProcessingError> {
        super::process_slots_with_policy::<false>(state, target, parameters)
    }
}

/// Exact proposer vector for one complete epoch.
pub type ProposerIndices = crate::ssz::Vector<u64, crate::ssz::PresetLength<32, 8>>;

/// Trusted Gloas fork configuration and independent balance churn budgets.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GloasParameters {
    /// Inherited clock, BPO schedule, signature and balance configuration.
    pub fulu: FuluParameters,
    /// Version written at the configured Gloas upgrade.
    pub fork_version: [u8; 4],
    /// Trusted activation epoch, binding cached PTC eligibility.
    pub fork_epoch: u64,
    /// Denominator of the uncapped exit and capped activation balance churn.
    pub churn_limit_quotient: u64,
    /// Separate denominator of the consolidation balance churn.
    pub consolidation_churn_limit_quotient: u64,
    /// Maximum activation balance per epoch; exits are not capped by this value.
    pub max_per_epoch_activation_churn_limit: u64,
    /// Epoch delay before an exiting builder can withdraw.
    pub min_builder_withdrawability_delay: u64,
}
impl GloasParameters {
    /// Exact pinned preset defaults; public Gloas activation remains disabled.
    pub fn for_preset(preset: Preset) -> Self {
        Self {
            fulu: FuluParameters::for_preset(preset),
            fork_version: match preset {
                Preset::Mainnet => [7, 0, 0, 0],
                Preset::Minimal => [7, 0, 0, 1],
            },
            fork_epoch: u64::MAX,
            churn_limit_quotient: match preset {
                Preset::Mainnet => 32768,
                Preset::Minimal => 16,
            },
            consolidation_churn_limit_quotient: match preset {
                Preset::Mainnet => 65536,
                Preset::Minimal => 32,
            },
            max_per_epoch_activation_churn_limit: match preset {
                Preset::Mainnet => 256_000_000_000,
                Preset::Minimal => 128_000_000_000,
            },
            min_builder_withdrawability_delay: match preset {
                Preset::Mainnet => 64,
                Preset::Minimal => 2,
            },
        }
    }
    /// Reject zero divisors and activation budgets below one effective increment.
    pub fn validate(&self) -> Result<(), ProcessingError> {
        self.fulu.validate()?;
        if self.churn_limit_quotient == 0 ||
            self.consolidation_churn_limit_quotient == 0 ||
            self.max_per_epoch_activation_churn_limit < common::INCREMENT ||
            !self.max_per_epoch_activation_churn_limit.is_multiple_of(common::INCREMENT)
        {
            return Err(ProcessingError::InvalidParameters("invalid Gloas churn parameters"))
        }
        Ok(())
    }
    /// Exact SSZ and algorithm preset.
    pub const fn preset(&self) -> Preset {
        self.fulu.preset()
    }
    /// Slots in one epoch for the configured preset.
    pub const fn slots_per_epoch(&self) -> u64 {
        self.fulu.slots_per_epoch()
    }
}
/// Exact immutable execution envelope context bound before the engine callback.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutionContext {
    /// Envelope and beacon slot.
    pub slot: u64,
    /// Parent beacon root committed by the envelope's bid.
    pub parent_beacon_block_root: Root,
    /// Previously applied execution block hash committed by the beacon state.
    pub previous_execution_payload_hash: Root,
    /// Ordered versioned blob commitment hashes from the authenticated bid.
    pub expected_blob_versioned_hashes: Vec<Root>,
    /// Canonical nonempty five-type execution request encodings.
    pub execution_requests_list: Vec<Vec<u8>>,
}
/// Required actual engine verification of the exact Gloas payload and context.
///
/// Success requires actual VALID and matching latest valid hash, including the exact
/// execution block hash, requests, block access list, slot and blob context. Optimistic,
/// accepted, syncing and malformed responses must fail. This interface creates no runtime
/// authority token; data availability and full fork-choice admission remain independent.
pub trait ExecutionEngine: Send + Sync {
    /// Engine transport, validation or malformed response error.
    type Error: std::error::Error + Send + Sync + 'static;
    /// Verify the exact immutable candidate through the configured execution engine.
    fn verify_payload<'a>(
        &'a self,
        payload: &'a ExecutionPayload,
        context: ExecutionContext,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;
}
pub(crate) const fn constants_for_preset(preset: Preset) -> Constants {
    crate::fulu_processing::constants_for_preset(preset)
}
pub(crate) fn checked_constants(
    state: &BeaconState,
    parameters: &GloasParameters,
) -> Result<Constants, ProcessingError> {
    parameters.validate()?;
    // Progressive root construction is mandatory at commitment boundaries; guard-only
    // validation instead checks every nested bound without rehashing unchanged fields.
    Ssz::validate_structure(state, parameters.preset())?;
    check_validator_lists(state)?;
    Ok(constants_for_preset(parameters.preset()))
}
/// Validate the complete state once and retain that exact immutable state's root.
fn checked_constants_with_root(
    state: &BeaconState,
    parameters: &GloasParameters,
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
        return Err(ProcessingError::InvalidState("Gloas validator-indexed list lengths differ"))
    }
    Ok(())
}
pub(crate) fn transact<T>(
    state: &mut BeaconState,
    parameters: &GloasParameters,
    operation: impl FnOnce(&mut BeaconState, &Constants) -> Result<T, ProcessingError>,
) -> Result<T, ProcessingError> {
    let constants = checked_constants(state, parameters)?;
    let mut candidate = state.clone();
    let result = operation(&mut candidate, &constants)?;
    checked_constants(&candidate, parameters)?;
    *state = candidate;
    Ok(result)
}
/// Import a signed beacon block atomically with mandatory BLS and exact progressive roots.
pub fn state_transition(
    state: &mut BeaconState,
    block: &SignedBeaconBlock,
    parameters: &GloasParameters,
) -> Result<(), ProcessingError> {
    block.hash_tree_root(parameters.preset())?;
    let constants = checked_constants(state, parameters)?;
    let mut candidate = state.clone();
    import_block_candidate(&mut candidate, block, parameters, &constants)?;
    *state = candidate;
    Ok(())
}
/// Consume a disposable Gloas state through the mandatory complete signed transition.
/// Only a fully verified post-state is returned; errors discard all candidate mutations.
pub fn state_transition_owned(
    mut state: BeaconState,
    block: &SignedBeaconBlock,
    parameters: &GloasParameters,
) -> Result<BeaconState, ProcessingError> {
    state_transition_in_place(&mut state, block, parameters)?;
    Ok(state)
}
/// Execute the same fully checked kernel on the dispatcher's disposable transaction copy.
/// This preserves every input hash, signature and output-root comparison.
/// An error may modify this copy, so callers must discard it without publishing it.
pub(crate) fn state_transition_in_place(
    candidate: &mut BeaconState,
    block: &SignedBeaconBlock,
    parameters: &GloasParameters,
) -> Result<(), ProcessingError> {
    block.hash_tree_root(parameters.preset())?;
    let constants = checked_constants(candidate, parameters)?;
    import_block_candidate(candidate, block, parameters, &constants)
}
fn import_block_candidate(
    candidate: &mut BeaconState,
    block: &SignedBeaconBlock,
    parameters: &GloasParameters,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    if candidate.slot != block.message.slot {
        slots::slots_kernel(candidate, block.message.slot, parameters, constants)?;
    }
    blocks::verify_block_signature_kernel(candidate, block, parameters)?;
    blocks::process_block(candidate, &block.message, parameters, constants)?;
    let (_, post_state_root) = checked_constants_with_root(candidate, parameters)?;
    if post_state_root != block.message.state_root {
        return Err(ProcessingError::InvalidState("block post-state root mismatch"))
    }
    Ok(())
}
/// Strict signed beacon import alias; this does not admit an execution payload envelope.
pub fn process_signed_block(
    state: &mut BeaconState,
    block: &SignedBeaconBlock,
    parameters: &GloasParameters,
) -> Result<(), ProcessingError> {
    state_transition(state, block, parameters)
}
/// Compute the next exact weighted sync committee and real aggregate public key.
pub fn get_next_sync_committee(
    state: &BeaconState,
    parameters: &GloasParameters,
) -> Result<crate::ssz::gloas::SyncCommittee, ProcessingError> {
    let constants = checked_constants(state, parameters)?;
    common::next_sync_committee(state, &constants)
}
/// Compute the capped activation churn, rounded to complete effective increments.
pub fn get_activation_churn_limit(
    state: &BeaconState,
    parameters: &GloasParameters,
) -> Result<u64, ProcessingError> {
    let constants = checked_constants(state, parameters)?;
    common::get_activation_churn_limit(state, parameters, &constants)
}
/// Compute the uncapped exit churn independently of activation limits.
pub fn get_exit_churn_limit(
    state: &BeaconState,
    parameters: &GloasParameters,
) -> Result<u64, ProcessingError> {
    let constants = checked_constants(state, parameters)?;
    common::get_exit_churn_limit(state, parameters, &constants)
}
/// Compute balance churn dedicated to consolidations.
pub fn get_consolidation_churn_limit(
    state: &BeaconState,
    parameters: &GloasParameters,
) -> Result<u64, ProcessingError> {
    let constants = checked_constants(state, parameters)?;
    common::get_consolidation_churn_limit(state, parameters, &constants)
}
/// Add exact validator balance to the exit queue atomically and return its epoch.
pub fn compute_exit_epoch_and_update_churn(
    state: &mut BeaconState,
    balance: u64,
    parameters: &GloasParameters,
) -> Result<u64, ProcessingError> {
    transact(state, parameters, |candidate, constants| {
        common::compute_exit_epoch_and_update_churn(candidate, balance, parameters, constants)
    })
}
/// Add exact validator balance to the consolidation queue atomically and return its epoch.
pub fn compute_consolidation_epoch_and_update_churn(
    state: &mut BeaconState,
    balance: u64,
    parameters: &GloasParameters,
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
    parameters: &GloasParameters,
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
    parameters: &GloasParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, constants| {
        common::slash_validator_kernel(candidate, index, whistleblower, parameters, constants)
    })
}
/// Switch credentials and queue excess active balance atomically.
pub fn switch_to_compounding_validator(
    state: &mut BeaconState,
    index: u64,
    parameters: &GloasParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, constants| {
        common::switch_to_compounding_validator(candidate, index, parameters, constants)
    })
}

/// Compute every proposer for a selected epoch using the exact16-bit balance weights.
pub fn get_beacon_proposer_indices(
    state: &BeaconState,
    epoch: u64,
    parameters: &GloasParameters,
) -> Result<ProposerIndices, ProcessingError> {
    let constants = checked_constants(state, parameters)?;
    Ok(crate::ssz::Vector::from_vec(common::proposer_indices(state, epoch, &constants)?))
}

/// Replay into a private temporary candidate without allocating a second complete state copy.
/// Errors may modify this candidate; callers must discard it and never pass retained state.
pub(crate) fn process_slots_in_place(
    state: &mut BeaconState,
    target: u64,
    parameters: &GloasParameters,
) -> Result<(), ProcessingError> {
    let constants = checked_constants(state, parameters)?;
    slots::slots_kernel(state, target, parameters, &constants)?;
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
    parameters: &GloasParameters,
) -> Result<Vec<u64>, ProcessingError> {
    let constants = checked_constants(state, parameters)?;
    crate::common::beacon_committee(state, slot, index, &constants)
}
