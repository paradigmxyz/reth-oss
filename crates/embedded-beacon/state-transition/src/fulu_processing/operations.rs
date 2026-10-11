//! Fulu multi-committee attestations, strict signatures and changed operation sequencing.

use super::{checked_constants, common, execution_requests, transact, with_altair, FuluParameters};
use crate::{
    capella_processing, compute_committee, compute_seed, integer_squareroot,
    phase0_processing::{add, mul, position, sub, Constants, ProcessingError},
    ssz::{
        capella,
        fulu::{
            self, Attestation, AttesterSlashing, BeaconBlockBody, BeaconState, IndexedAttestation,
            ProposerSlashing, SignedBLSToExecutionChange, SignedVoluntaryExit,
        },
        List,
    },
};
use reth_embedded_beacon_crypto::{
    compute_domain, compute_signing_root, fast_aggregate_verify, verify_signature,
};
use reth_embedded_beacon_primitives::Root;

fn index(index: u64, length: usize) -> Result<usize, ProcessingError> {
    common::index(index, length)
}
/// Committee inputs borrowed from one immutable state during one participant derivation.
struct CommitteeInputs {
    active: Vec<u64>,
    seed: Root,
    count: u64,
}

impl CommitteeInputs {
    fn new(state: &BeaconState, slot: u64, constants: &Constants) -> Result<Self, ProcessingError> {
        let epoch = slot / constants.slots_per_epoch;
        let active = common::active_indices(state, epoch);
        let count = ((active.len() as u64 / constants.slots_per_epoch) /
            constants.target_committee_size)
            .clamp(1, constants.maximum_committees);
        let mix_epoch = sub(
            add(epoch, constants.historical_epochs, "committee mix epoch")?,
            2,
            "committee mix lookback",
        )?;
        let mix = *state
            .randao_mixes
            .values
            .get(position(mix_epoch, constants.historical_epochs)?)
            .ok_or(ProcessingError::InvalidState("RANDAO ring is truncated"))?;
        Ok(Self { active, seed: compute_seed([1, 0, 0, 0], epoch, mix), count })
    }

    fn committee(
        &self,
        slot: u64,
        committee_index: u64,
        constants: &Constants,
    ) -> Result<Vec<u64>, ProcessingError> {
        if committee_index >= self.count {
            return Err(ProcessingError::InvalidState(
                "attestation committee index is out of range",
            ));
        }
        let committee_position = add(
            mul(slot % constants.slots_per_epoch, self.count, "slot committee position")?,
            committee_index,
            "committee position",
        )?;
        Ok(compute_committee(
            &self.active,
            self.seed,
            committee_position,
            mul(self.count, constants.slots_per_epoch, "committees per epoch")?,
            constants.shuffle_rounds,
        )?)
    }
}
/// Derive sorted unique multi-committee participants without verifying their aggregate signature.
pub fn get_attesting_indices(
    state: &BeaconState,
    attestation: &Attestation,
    parameters: &FuluParameters,
) -> Result<Vec<u64>, ProcessingError> {
    let constants = checked_constants(state, parameters)?;
    attestation.hash_tree_root(parameters.preset())?;
    attesting_indices(state, attestation, &constants, false)
}
fn attesting_indices(
    state: &BeaconState,
    attestation: &Attestation,
    constants: &Constants,
    require_each_nonempty: bool,
) -> Result<Vec<u64>, ProcessingError> {
    let mut committees = None;
    attesting_indices_with(attestation, require_each_nonempty, |committee_index| {
        if committees.is_none() {
            committees = Some(CommitteeInputs::new(state, attestation.data.slot, constants)?);
        }
        committees
            .as_ref()
            .expect("committee inputs initialized for a selected committee")
            .committee(attestation.data.slot, committee_index, constants)
    })
}

fn attesting_indices_with(
    attestation: &Attestation,
    require_each_nonempty: bool,
    mut committee: impl FnMut(u64) -> Result<Vec<u64>, ProcessingError>,
) -> Result<Vec<u64>, ProcessingError> {
    let mut offset = 0usize;
    let mut output = Vec::new();
    for committee_index in 0..attestation.committee_bits.bits.len() {
        if !attestation
            .committee_bits
            .bits
            .get(committee_index)
            .ok_or(ProcessingError::InvalidState("committee bitvector is truncated"))?
        {
            continue;
        }
        let committee = committee(committee_index as u64)?;
        let end = offset
            .checked_add(committee.len())
            .ok_or(ProcessingError::InvalidState("committee aggregation offset overflow"))?;
        if end > attestation.aggregation_bits.bits.len() {
            return Err(ProcessingError::InvalidState(
                "multi-committee aggregation bits are truncated",
            ));
        }
        let initial = output.len();
        for (position, index) in committee.into_iter().enumerate() {
            if attestation
                .aggregation_bits
                .bits
                .get(offset + position)
                .ok_or(ProcessingError::InvalidState("aggregation bitfield is truncated"))?
            {
                output.push(index);
            }
        }
        if require_each_nonempty && output.len() == initial {
            return Err(ProcessingError::InvalidState(
                "selected attestation committee has no participants",
            ));
        }
        offset = end;
    }
    if offset != attestation.aggregation_bits.bits.len() {
        return Err(ProcessingError::InvalidState(
            "aggregation length differs from selected committees",
        ));
    }
    output.sort_unstable();
    output.dedup();
    Ok(output)
}
/// Convert an Fulu aggregate to exact indexed form; signature validation remains separate.
pub fn get_indexed_attestation(
    state: &BeaconState,
    attestation: &Attestation,
    parameters: &FuluParameters,
) -> Result<IndexedAttestation, ProcessingError> {
    Ok(IndexedAttestation {
        attesting_indices: List::from_vec(get_attesting_indices(state, attestation, parameters)?),
        data: attestation.data.clone(),
        signature: attestation.signature,
    })
}
/// Derive indexed participants and require their strict aggregate signature against one
/// checked immutable state. All aggregate and indexed SSZ bounds remain mandatory.
pub fn get_validated_indexed_attestation(
    state: &BeaconState,
    attestation: &Attestation,
    parameters: &FuluParameters,
) -> Result<IndexedAttestation, ProcessingError> {
    validated_indexed_kernel::<true>(state, attestation, parameters)
}

fn validated_indexed_kernel<const VERIFY_BLS: bool>(
    state: &BeaconState,
    attestation: &Attestation,
    parameters: &FuluParameters,
) -> Result<IndexedAttestation, ProcessingError> {
    crate::common::require_signature_policy::<VERIFY_BLS>()?;
    let constants = checked_constants(state, parameters)?;
    attestation.hash_tree_root(parameters.preset())?;
    let indexed = IndexedAttestation {
        attesting_indices: List::from_vec(attesting_indices(
            state,
            attestation,
            &constants,
            false,
        )?),
        data: attestation.data.clone(),
        signature: attestation.signature,
    };
    indexed.hash_tree_root(parameters.preset())?;
    indexed_kernel::<VERIFY_BLS>(state, &indexed, parameters)?;
    Ok(indexed)
}
fn indexed_kernel<const VERIFY_BLS: bool>(
    state: &BeaconState,
    attestation: &IndexedAttestation,
    parameters: &FuluParameters,
) -> Result<(), ProcessingError> {
    let indices = &attestation.attesting_indices.values;
    if indices.is_empty() || indices.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(ProcessingError::InvalidState(
            "indexed participants are not nonempty sorted unique indexes",
        ));
    }
    let keys = indices
        .iter()
        .map(|&i| Ok(state.validators.values[index(i, state.validators.values.len())?].pubkey))
        .collect::<Result<Vec<_>, ProcessingError>>()?;
    let root = compute_signing_root(
        attestation.data.hash_tree_root(parameters.preset())?,
        common::domain(state, [1, 0, 0, 0], attestation.data.target.epoch),
    );
    if VERIFY_BLS {
        fast_aggregate_verify(&keys, &root, &attestation.signature)?;
    }
    Ok(())
}
/// Require the strict aggregate signature for sorted unique Fulu indexed participants.
pub fn is_valid_indexed_attestation(
    state: &BeaconState,
    attestation: &IndexedAttestation,
    parameters: &FuluParameters,
) -> Result<(), ProcessingError> {
    checked_constants(state, parameters)?;
    attestation.hash_tree_root(parameters.preset())?;
    indexed_kernel::<true>(state, attestation, parameters)
}
fn block_root(
    state: &BeaconState,
    slot: u64,
    constants: &Constants,
) -> Result<Root, ProcessingError> {
    if slot >= state.slot || state.slot - slot > constants.historical_slots {
        return Err(ProcessingError::InvalidState("block root is outside historical window"));
    }
    state
        .block_roots
        .values
        .get(position(slot, constants.historical_slots)?)
        .copied()
        .ok_or(ProcessingError::InvalidState("block root ring is truncated"))
}
fn flags(
    state: &BeaconState,
    data: &fulu::AttestationData,
    delay: u64,
    constants: &Constants,
) -> Result<u8, ProcessingError> {
    let justified = if data.target.epoch == state.slot / constants.slots_per_epoch {
        state.current_justified_checkpoint
    } else {
        state.previous_justified_checkpoint
    };
    if data.source != justified {
        return Err(ProcessingError::InvalidState("attestation source checkpoint differs"));
    }
    let target = data.target.root ==
        block_root(
            state,
            mul(data.target.epoch, constants.slots_per_epoch, "attestation target slot")?,
            constants,
        )?;
    let head = target && data.beacon_block_root == block_root(state, data.slot, constants)?;
    let mut flags = 0;
    if delay <= integer_squareroot(constants.slots_per_epoch) {
        flags |= 1;
    }
    if target {
        flags |= 2;
    }
    if head && delay == 1 {
        flags |= 4;
    }
    Ok(flags)
}
pub(crate) fn attestation_kernel<const VERIFY_BLS: bool>(
    state: &mut BeaconState,
    operation: &Attestation,
    parameters: &FuluParameters,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    attestation_kernel_with_inputs::<VERIFY_BLS>(
        state,
        operation,
        parameters,
        constants,
        &mut context::AttestationInputs::default(),
    )
}

fn attestation_kernel_with_inputs<const VERIFY_BLS: bool>(
    state: &mut BeaconState,
    operation: &Attestation,
    parameters: &FuluParameters,
    constants: &Constants,
    inputs: &mut context::AttestationInputs,
) -> Result<(), ProcessingError> {
    let data = &operation.data;
    let current = state.slot / constants.slots_per_epoch;
    if (data.target.epoch != current && data.target.epoch != current.saturating_sub(1)) ||
        data.target.epoch != data.slot / constants.slots_per_epoch
    {
        return Err(ProcessingError::InvalidState("attestation target epoch is inconsistent"));
    }
    if data.index != 0 {
        return Err(ProcessingError::InvalidState("Fulu attestation data index must be zero"));
    }
    if add(data.slot, 1, "minimum inclusion slot")? > state.slot {
        return Err(ProcessingError::InvalidState("attestation inclusion is premature"));
    }
    let indices = inputs.indices(state, operation, constants)?;
    let flags = flags(state, data, sub(state.slot, data.slot, "inclusion delay")?, constants)?;
    let indexed = IndexedAttestation {
        attesting_indices: List::from_vec(indices),
        data: data.clone(),
        signature: operation.signature,
    };
    indexed_kernel::<VERIFY_BLS>(state, &indexed, parameters)?;
    let proposer = index(
        common::get_beacon_proposer_index(state, parameters, constants)?,
        state.validators.values.len(),
    )?;
    let reward_per_increment = mul(1_000_000_000, 64, "base reward factor")? /
        integer_squareroot(inputs.active_balance(state, constants)?);
    let mut numerator = 0;
    for index in indexed.attesting_indices.values {
        let index = index as usize;
        let base = mul(
            state.validators.values[index].effective_balance / 1_000_000_000,
            reward_per_increment,
            "attestation base reward",
        )?;
        let participation = if data.target.epoch == current {
            &mut state.current_epoch_participation.values
        } else {
            &mut state.previous_epoch_participation.values
        };
        for (flag, weight) in [14, 26, 14].into_iter().enumerate() {
            let mask = 1 << flag;
            if flags & mask != 0 && participation[index] & mask == 0 {
                participation[index] |= mask;
                numerator = add(
                    numerator,
                    mul(base, weight, "proposer reward numerator")?,
                    "proposer reward numerator",
                )?;
            }
        }
    }
    state.balances.values[proposer] =
        add(state.balances.values[proposer], numerator / 448, "attestation proposer reward")?;
    Ok(())
}
/// Strict multi-committee attestation processing with exact participation and proposer rewards.
pub fn process_attestation(
    state: &mut BeaconState,
    operation: &Attestation,
    parameters: &FuluParameters,
) -> Result<(), ProcessingError> {
    operation.hash_tree_root(parameters.preset())?;
    transact(state, parameters, |candidate, constants| {
        attestation_kernel::<true>(candidate, operation, parameters, constants)
    })
}

const fn slashable(validator: &fulu::Validator, epoch: u64) -> bool {
    !validator.slashed &&
        validator.activation_epoch <= epoch &&
        epoch < validator.withdrawable_epoch
}
pub(crate) fn proposer_slashing_kernel(
    state: &mut BeaconState,
    operation: &ProposerSlashing,
    parameters: &FuluParameters,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    proposer_slashing_kernel_with_policy::<true>(state, operation, parameters, constants)
}

pub(crate) fn proposer_slashing_kernel_with_policy<const VERIFY_BLS: bool>(
    state: &mut BeaconState,
    operation: &ProposerSlashing,
    parameters: &FuluParameters,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    crate::common::require_signature_policy::<VERIFY_BLS>()?;
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
        if VERIFY_BLS {
            verify_signature(&key, &root, &signed.signature)?;
        }
    }
    common::slash_validator_kernel(state, first.proposer_index, None, parameters, constants)
}
/// Strict proposer equivocation with Fulu slashing penalties and balance-based exit churn.
pub fn process_proposer_slashing(
    state: &mut BeaconState,
    operation: &ProposerSlashing,
    parameters: &FuluParameters,
) -> Result<(), ProcessingError> {
    operation.hash_tree_root(parameters.preset())?;
    transact(state, parameters, |candidate, constants| {
        proposer_slashing_kernel(candidate, operation, parameters, constants)
    })
}
pub(crate) fn attester_slashing_kernel(
    state: &mut BeaconState,
    operation: &AttesterSlashing,
    parameters: &FuluParameters,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    attester_slashing_kernel_with_policy::<true>(state, operation, parameters, constants)
}

pub(crate) fn attester_slashing_kernel_with_policy<const VERIFY_BLS: bool>(
    state: &mut BeaconState,
    operation: &AttesterSlashing,
    parameters: &FuluParameters,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    crate::common::require_signature_policy::<VERIFY_BLS>()?;
    let first = &operation.attestation_1;
    let second = &operation.attestation_2;
    if !((first.data != second.data && first.data.target.epoch == second.data.target.epoch) ||
        (first.data.source.epoch < second.data.source.epoch &&
            second.data.target.epoch < first.data.target.epoch))
    {
        return Err(ProcessingError::InvalidState("attester slashing data do not conflict"));
    }
    indexed_kernel::<VERIFY_BLS>(state, first, parameters)?;
    indexed_kernel::<VERIFY_BLS>(state, second, parameters)?;
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
/// Strict extended indexed-attestation slashing with exact Fulu penalties.
pub fn process_attester_slashing(
    state: &mut BeaconState,
    operation: &AttesterSlashing,
    parameters: &FuluParameters,
) -> Result<(), ProcessingError> {
    operation.hash_tree_root(parameters.preset())?;
    transact(state, parameters, |candidate, constants| {
        attester_slashing_kernel(candidate, operation, parameters, constants)
    })
}
pub(crate) fn voluntary_exit_kernel(
    state: &mut BeaconState,
    operation: &SignedVoluntaryExit,
    parameters: &FuluParameters,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    voluntary_exit_kernel_with_policy::<true>(state, operation, parameters, constants)
}

pub(crate) fn voluntary_exit_kernel_with_policy<const VERIFY_BLS: bool>(
    state: &mut BeaconState,
    operation: &SignedVoluntaryExit,
    parameters: &FuluParameters,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    crate::common::require_signature_policy::<VERIFY_BLS>()?;
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
                parameters.electra.deneb.capella.bellatrix.altair.phase0.shard_committee_period,
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
        parameters.electra.deneb.capella.fork_version,
        state.genesis_validators_root,
    );
    if VERIFY_BLS {
        verify_signature(
            &validator.pubkey,
            &compute_signing_root(exit.hash_tree_root(parameters.preset())?, domain),
            &operation.signature,
        )?;
    }
    common::initiate_validator_exit(state, exit.validator_index, parameters, constants)
}
/// Require the static Capella signature domain and an empty pending-withdrawal balance.
pub fn process_voluntary_exit(
    state: &mut BeaconState,
    operation: &SignedVoluntaryExit,
    parameters: &FuluParameters,
) -> Result<(), ProcessingError> {
    operation.hash_tree_root(parameters.preset())?;
    transact(state, parameters, |candidate, constants| {
        voluntary_exit_kernel(candidate, operation, parameters, constants)
    })
}
const fn bls_change(operation: &SignedBLSToExecutionChange) -> capella::SignedBLSToExecutionChange {
    capella::SignedBLSToExecutionChange {
        message: capella::BLSToExecutionChange {
            validator_index: operation.message.validator_index,
            from_bls_pubkey: operation.message.from_bls_pubkey,
            to_execution_address: operation.message.to_execution_address,
        },
        signature: operation.signature,
    }
}
pub(crate) fn bls_change_kernel(
    state: &mut BeaconState,
    operation: &SignedBLSToExecutionChange,
    parameters: &FuluParameters,
) -> Result<(), ProcessingError> {
    bls_change_kernel_with_policy::<true>(state, operation, parameters)
}

pub(crate) fn bls_change_kernel_with_policy<const VERIFY_BLS: bool>(
    state: &mut BeaconState,
    operation: &SignedBLSToExecutionChange,
    parameters: &FuluParameters,
) -> Result<(), ProcessingError> {
    crate::common::require_signature_policy::<VERIFY_BLS>()?;
    with_altair(state, |common| {
        capella_processing::bls_to_execution_change_kernel_with_policy::<VERIFY_BLS>(
            common,
            &bls_change(operation),
            &parameters.electra.deneb.capella,
        )
    })
}
/// Require the exact withdrawal-key signature before replacing BLS credentials.
pub fn process_bls_to_execution_change(
    state: &mut BeaconState,
    operation: &SignedBLSToExecutionChange,
    parameters: &FuluParameters,
) -> Result<(), ProcessingError> {
    operation.hash_tree_root(parameters.preset())?;
    transact(state, parameters, |candidate, _| bls_change_kernel(candidate, operation, parameters))
}

pub(crate) fn operations_kernel(
    state: &mut BeaconState,
    body: &BeaconBlockBody,
    parameters: &FuluParameters,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    operations_kernel_with_policy::<true>(state, body, parameters, constants)
}

pub(crate) fn operations_kernel_with_policy<const VERIFY_BLS: bool>(
    state: &mut BeaconState,
    body: &BeaconBlockBody,
    parameters: &FuluParameters,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    crate::common::require_signature_policy::<VERIFY_BLS>()?;
    if !body.deposits.values.is_empty() {
        return Err(ProcessingError::InvalidState("Fulu blocks do not accept legacy deposits"))
    }
    for value in &body.proposer_slashings.values {
        proposer_slashing_kernel_with_policy::<VERIFY_BLS>(state, value, parameters, constants)?;
    }
    for value in &body.attester_slashings.values {
        attester_slashing_kernel_with_policy::<VERIFY_BLS>(state, value, parameters, constants)?;
    }
    {
        // Slashings are complete; discard preparation before exits can change the registry.
        let mut inputs = context::AttestationInputs::default();
        for value in &body.attestations.values {
            attestation_kernel_with_inputs::<VERIFY_BLS>(
                state,
                value,
                parameters,
                constants,
                &mut inputs,
            )?;
        }
    }
    for value in &body.voluntary_exits.values {
        voluntary_exit_kernel_with_policy::<VERIFY_BLS>(state, value, parameters, constants)?;
    }
    for value in &body.bls_to_execution_changes.values {
        bls_change_kernel_with_policy::<VERIFY_BLS>(state, value, parameters)?;
    }
    for value in &body.execution_requests.deposits.values {
        execution_requests::deposit_request_kernel(state, value)?;
    }
    for value in &body.execution_requests.withdrawals.values {
        execution_requests::withdrawal_request_kernel(state, value, parameters, constants)?;
    }
    for value in &body.execution_requests.consolidations.values {
        execution_requests::consolidation_request_kernel(state, value, parameters, constants)?;
    }
    Ok(())
}
/// Atomically process the exact Fulu operation order; it does not authorize an unverified EL
/// body.
pub fn process_operations(
    state: &mut BeaconState,
    body: &BeaconBlockBody,
    parameters: &FuluParameters,
) -> Result<(), ProcessingError> {
    body.hash_tree_root(parameters.preset())?;
    transact(state, parameters, |candidate, constants| {
        operations_kernel(candidate, body, parameters, constants)
    })
}

/// Published artificial attestation fixtures only; strict block import always verifies signatures.
#[cfg(feature = "spec-tests")]
pub(super) mod spec_only {
    use super::*;
    /// Artificial fixture indexing with all committee, registry and SSZ checks retained.
    pub fn get_validated_indexed_attestation(
        state: &BeaconState,
        attestation: &Attestation,
        parameters: &FuluParameters,
    ) -> Result<IndexedAttestation, ProcessingError> {
        validated_indexed_kernel::<false>(state, attestation, parameters)
    }
    /// Artificial fixture aggregation with all ordering, length, registry and SSZ checks.
    #[cfg(feature = "spec-tests")]
    pub fn is_valid_indexed_attestation(
        state: &BeaconState,
        operation: &IndexedAttestation,
        parameters: &FuluParameters,
    ) -> Result<(), ProcessingError> {
        checked_constants(state, parameters)?;
        operation.hash_tree_root(parameters.preset())?;
        indexed_kernel::<false>(state, operation, parameters)
    }

    /// Apply only a metadata-declared artificial attestation fixture with BLS disabled.
    pub fn process_attestation(
        state: &mut BeaconState,
        operation: &Attestation,
        parameters: &FuluParameters,
    ) -> Result<(), ProcessingError> {
        operation.hash_tree_root(parameters.preset())?;
        transact(state, parameters, |candidate, constants| {
            attestation_kernel::<false>(candidate, operation, parameters, constants)
        })
    }
}

#[cfg(test)]
mod tests;

mod checked;
pub use checked::CheckedAttestationState;

mod context;
