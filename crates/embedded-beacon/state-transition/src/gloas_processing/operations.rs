//! Gloas availability votes, builder payment weights and mandatory operation signatures.

use super::{builder, checked_constants, common, ptc, transact, GloasParameters};
use crate::{
    compute_committee, compute_seed, integer_squareroot,
    phase0_processing::{add, mul, position, sub, Constants, ProcessingError},
    ssz::{
        gloas::{
            self, Attestation, AttesterSlashing, BeaconBlockBody, BeaconState, IndexedAttestation,
            ProposerSlashing, SignedBLSToExecutionChange, SignedVoluntaryExit,
        },
        ProgressiveList,
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
pub(crate) fn committee(
    state: &BeaconState,
    slot: u64,
    committee_index: u64,
    constants: &Constants,
) -> Result<Vec<u64>, ProcessingError> {
    CommitteeInputs::new(state, slot, constants)?.committee(slot, committee_index, constants)
}
/// Derive sorted unique multi-committee participants without verifying their aggregate signature.
pub fn get_attesting_indices(
    state: &BeaconState,
    attestation: &Attestation,
    parameters: &GloasParameters,
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
/// Convert a Gloas aggregate to exact indexed form; signature validation remains separate.
pub fn get_indexed_attestation(
    state: &BeaconState,
    attestation: &Attestation,
    parameters: &GloasParameters,
) -> Result<IndexedAttestation, ProcessingError> {
    Ok(IndexedAttestation {
        attesting_indices: ProgressiveList::from_vec(get_attesting_indices(
            state,
            attestation,
            parameters,
        )?),
        data: attestation.data.clone(),
        signature: attestation.signature,
    })
}
/// Derive indexed participants and require their strict aggregate signature against one
/// checked immutable state. All aggregate and indexed SSZ bounds remain mandatory.
pub fn get_validated_indexed_attestation(
    state: &BeaconState,
    attestation: &Attestation,
    parameters: &GloasParameters,
) -> Result<IndexedAttestation, ProcessingError> {
    validated_indexed_kernel::<true>(state, attestation, parameters)
}

fn validated_indexed_kernel<const VERIFY_BLS: bool>(
    state: &BeaconState,
    attestation: &Attestation,
    parameters: &GloasParameters,
) -> Result<IndexedAttestation, ProcessingError> {
    crate::common::require_signature_policy::<VERIFY_BLS>()?;
    let constants = checked_constants(state, parameters)?;
    attestation.hash_tree_root(parameters.preset())?;
    let indexed = IndexedAttestation {
        attesting_indices: ProgressiveList::from_vec(attesting_indices(
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
    parameters: &GloasParameters,
) -> Result<(), ProcessingError> {
    let indices = &attestation.attesting_indices.values;
    if indices.is_empty() ||
        indices.len() >
            match parameters.preset() {
                crate::ssz::Preset::Mainnet => 131_072,
                crate::ssz::Preset::Minimal => 8_192,
            } ||
        indices.windows(2).any(|pair| pair[0] >= pair[1])
    {
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
/// Require the strict aggregate signature for sorted unique Gloas indexed participants.
pub fn is_valid_indexed_attestation(
    state: &BeaconState,
    attestation: &IndexedAttestation,
    parameters: &GloasParameters,
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
    data: &gloas::AttestationData,
    delay: u64,
    constants: &Constants,
    parent_slot: u64,
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
    let payload_matches = if same_slot_kernel(state, data, constants)? {
        if data.index != 0 {
            return Err(ProcessingError::InvalidState(
                "same-slot attestation payload index must be zero",
            ));
        }
        true
    } else {
        let available = state
            .execution_payload_availability
            .bits
            .get(position(parent_slot, constants.historical_slots)?)
            .ok_or(ProcessingError::InvalidState("payload availability ring is truncated"))?;
        data.index == u64::from(available)
    };
    let head = target &&
        data.beacon_block_root == block_root(state, data.slot, constants)? &&
        payload_matches;
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
    parameters: &GloasParameters,
    constants: &Constants,
    parent_slot: u64,
) -> Result<(), ProcessingError> {
    attestation_kernel_with_inputs::<VERIFY_BLS>(
        state,
        operation,
        parameters,
        constants,
        parent_slot,
        &mut context::AttestationInputs::default(),
    )
}

fn attestation_kernel_with_inputs<const VERIFY_BLS: bool>(
    state: &mut BeaconState,
    operation: &Attestation,
    parameters: &GloasParameters,
    constants: &Constants,
    parent_slot: u64,
    inputs: &mut context::AttestationInputs,
) -> Result<(), ProcessingError> {
    let data = &operation.data;
    let current = state.slot / constants.slots_per_epoch;
    if (data.target.epoch != current && data.target.epoch != current.saturating_sub(1)) ||
        data.target.epoch != data.slot / constants.slots_per_epoch
    {
        return Err(ProcessingError::InvalidState("attestation target epoch is inconsistent"));
    }
    if data.index >= 2 {
        return Err(ProcessingError::InvalidState(
            "Gloas attestation payload index must be below two",
        ));
    }
    if add(data.slot, 1, "minimum inclusion slot")? > state.slot {
        return Err(ProcessingError::InvalidState("attestation inclusion is premature"));
    }
    let indices = inputs.indices(state, operation, constants)?;
    let flags =
        flags(state, data, sub(state.slot, data.slot, "inclusion delay")?, constants, parent_slot)?;
    let indexed = IndexedAttestation {
        attesting_indices: ProgressiveList::from_vec(indices),
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
    let same_slot = same_slot_kernel(state, data, constants)?;
    let payment_index = if data.target.epoch == current {
        constants.slots_per_epoch + data.slot % constants.slots_per_epoch
    } else {
        data.slot % constants.slots_per_epoch
    };
    let payment_index = index(payment_index, state.builder_pending_payments.values.len())?;
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
        let had_no_participation = participation[index] == 0;
        let mut new_flag = false;
        for (flag, weight) in [14, 26, 14].into_iter().enumerate() {
            let mask = 1 << flag;
            if flags & mask != 0 && participation[index] & mask == 0 {
                participation[index] |= mask;
                new_flag = true;
                numerator = add(
                    numerator,
                    mul(base, weight, "proposer reward numerator")?,
                    "proposer reward numerator",
                )?;
            }
        }
        let payment = &mut state.builder_pending_payments.values[payment_index];
        if new_flag && had_no_participation && same_slot && payment.withdrawal.amount > 0 {
            payment.weight = add(
                payment.weight,
                state.validators.values[index].effective_balance,
                "builder payment participation weight",
            )?;
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
    parent_slot: u64,
    parameters: &GloasParameters,
) -> Result<(), ProcessingError> {
    operation.hash_tree_root(parameters.preset())?;
    transact(state, parameters, |candidate, constants| {
        attestation_kernel::<true>(candidate, operation, parameters, constants, parent_slot)
    })
}

const fn slashable(validator: &gloas::Validator, epoch: u64) -> bool {
    !validator.slashed &&
        validator.activation_epoch <= epoch &&
        epoch < validator.withdrawable_epoch
}
pub(crate) fn proposer_slashing_kernel(
    state: &mut BeaconState,
    operation: &ProposerSlashing,
    parameters: &GloasParameters,
    constants: &Constants,
) -> Result<(), ProcessingError> {
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
        verify_signature(&key, &root, &signed.signature)?;
    }
    let proposal_epoch = first.slot / constants.slots_per_epoch;
    let current = state.slot / constants.slots_per_epoch;
    let payment_index = if proposal_epoch == current {
        Some(constants.slots_per_epoch + first.slot % constants.slots_per_epoch)
    } else if proposal_epoch == current.saturating_sub(1) {
        Some(first.slot % constants.slots_per_epoch)
    } else {
        None
    };
    if let Some(payment_index) = payment_index {
        let payment_index = index(payment_index, state.builder_pending_payments.values.len())?;
        if state.builder_pending_payments.values[payment_index].proposer_index ==
            first.proposer_index
        {
            state.builder_pending_payments.values[payment_index] = builder::empty_payment();
        }
    }
    common::slash_validator_kernel(state, first.proposer_index, None, parameters, constants)
}
/// Strict proposer equivocation with Gloas slashing penalties and balance-based exit churn.
pub fn process_proposer_slashing(
    state: &mut BeaconState,
    operation: &ProposerSlashing,
    parameters: &GloasParameters,
) -> Result<(), ProcessingError> {
    operation.hash_tree_root(parameters.preset())?;
    transact(state, parameters, |candidate, constants| {
        proposer_slashing_kernel(candidate, operation, parameters, constants)
    })
}
pub(crate) fn attester_slashing_kernel(
    state: &mut BeaconState,
    operation: &AttesterSlashing,
    parameters: &GloasParameters,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    let first = &operation.attestation_1;
    let second = &operation.attestation_2;
    if !((first.data != second.data && first.data.target.epoch == second.data.target.epoch) ||
        (first.data.source.epoch < second.data.source.epoch &&
            second.data.target.epoch < first.data.target.epoch))
    {
        return Err(ProcessingError::InvalidState("attester slashing data do not conflict"));
    }
    indexed_kernel::<true>(state, first, parameters)?;
    indexed_kernel::<true>(state, second, parameters)?;
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
/// Strict extended indexed-attestation slashing with exact Gloas penalties.
pub fn process_attester_slashing(
    state: &mut BeaconState,
    operation: &AttesterSlashing,
    parameters: &GloasParameters,
) -> Result<(), ProcessingError> {
    operation.hash_tree_root(parameters.preset())?;
    transact(state, parameters, |candidate, constants| {
        attester_slashing_kernel(candidate, operation, parameters, constants)
    })
}
pub(crate) fn voluntary_exit_kernel(
    state: &mut BeaconState,
    operation: &SignedVoluntaryExit,
    parameters: &GloasParameters,
    constants: &Constants,
) -> Result<(), ProcessingError> {
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
                parameters
                    .fulu
                    .electra
                    .deneb
                    .capella
                    .bellatrix
                    .altair
                    .phase0
                    .shard_committee_period,
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
        parameters.fulu.electra.deneb.capella.fork_version,
        state.genesis_validators_root,
    );
    verify_signature(
        &validator.pubkey,
        &compute_signing_root(exit.hash_tree_root(parameters.preset())?, domain),
        &operation.signature,
    )?;
    common::initiate_validator_exit(state, exit.validator_index, parameters, constants)
}
/// Require the static Capella signature domain and an empty pending-withdrawal balance.
pub fn process_voluntary_exit(
    state: &mut BeaconState,
    operation: &SignedVoluntaryExit,
    parameters: &GloasParameters,
) -> Result<(), ProcessingError> {
    operation.hash_tree_root(parameters.preset())?;
    transact(state, parameters, |candidate, constants| {
        voluntary_exit_kernel(candidate, operation, parameters, constants)
    })
}
pub(crate) fn bls_change_kernel(
    state: &mut BeaconState,
    operation: &SignedBLSToExecutionChange,
    parameters: &GloasParameters,
) -> Result<(), ProcessingError> {
    let message = &operation.message;
    let i = index(message.validator_index, state.validators.values.len())?;
    let validator = &state.validators.values[i];
    let digest = reth_embedded_beacon_primitives::sha256(&message.from_bls_pubkey);
    if validator.withdrawal_credentials[0] != 0 ||
        validator.withdrawal_credentials[1..] != digest[1..]
    {
        return Err(ProcessingError::InvalidState(
            "BLS withdrawal credentials do not match withdrawal key",
        ));
    }
    let domain = compute_domain(
        [10, 0, 0, 0],
        parameters.fulu.electra.deneb.capella.bellatrix.altair.phase0.genesis_fork_version,
        state.genesis_validators_root,
    );
    verify_signature(
        &message.from_bls_pubkey,
        &compute_signing_root(message.hash_tree_root(parameters.preset())?, domain),
        &operation.signature,
    )?;
    let mut credentials = [0; 32];
    credentials[0] = 1;
    credentials[12..].copy_from_slice(&message.to_execution_address);
    state.validators.values[i].withdrawal_credentials = credentials;
    Ok(())
}
/// Require the exact withdrawal-key signature before replacing BLS credentials.
pub fn process_bls_to_execution_change(
    state: &mut BeaconState,
    operation: &SignedBLSToExecutionChange,
    parameters: &GloasParameters,
) -> Result<(), ProcessingError> {
    operation.hash_tree_root(parameters.preset())?;
    transact(state, parameters, |candidate, _| bls_change_kernel(candidate, operation, parameters))
}

pub(crate) fn operations_kernel(
    state: &mut BeaconState,
    body: &BeaconBlockBody,
    parameters: &GloasParameters,
    constants: &Constants,
    parent_slot: u64,
) -> Result<(), ProcessingError> {
    if !body.deposits.values.is_empty() ||
        body.proposer_slashings.values.len() > 16 ||
        body.attester_slashings.values.len() > 1 ||
        body.attestations.values.len() > 8 ||
        body.voluntary_exits.values.len() > 16 ||
        body.bls_to_execution_changes.values.len() > 16 ||
        body.payload_attestations.values.len() > 4
    {
        return Err(ProcessingError::InvalidState(
            "Gloas body operation count exceeds consensus limits",
        ));
    }
    for value in &body.proposer_slashings.values {
        proposer_slashing_kernel(state, value, parameters, constants)?;
    }
    for value in &body.attester_slashings.values {
        attester_slashing_kernel(state, value, parameters, constants)?;
    }
    {
        // Slashings are complete; discard preparation before exits can change the registry.
        let mut inputs = context::AttestationInputs::default();
        for value in &body.attestations.values {
            attestation_kernel_with_inputs::<true>(
                state,
                value,
                parameters,
                constants,
                parent_slot,
                &mut inputs,
            )?;
        }
    }
    for value in &body.voluntary_exits.values {
        voluntary_exit_kernel(state, value, parameters, constants)?;
    }
    for value in &body.bls_to_execution_changes.values {
        bls_change_kernel(state, value, parameters)?;
    }
    for value in &body.payload_attestations.values {
        ptc::payload_attestation_kernel(state, value, parameters)?;
    }
    Ok(())
}
/// Atomically process the exact Gloas operation order; it does not authorize an unverified EL
/// body.
pub fn process_operations(
    state: &mut BeaconState,
    body: &BeaconBlockBody,
    parent_slot: u64,
    parameters: &GloasParameters,
) -> Result<(), ProcessingError> {
    body.hash_tree_root(parameters.preset())?;
    transact(state, parameters, |candidate, constants| {
        operations_kernel(candidate, body, parameters, constants, parent_slot)
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
        parameters: &GloasParameters,
    ) -> Result<IndexedAttestation, ProcessingError> {
        validated_indexed_kernel::<false>(state, attestation, parameters)
    }
    /// Apply only a metadata-declared artificial attestation fixture with BLS disabled.
    pub fn process_attestation(
        state: &mut BeaconState,
        operation: &Attestation,
        parent_slot: u64,
        parameters: &GloasParameters,
    ) -> Result<(), ProcessingError> {
        operation.hash_tree_root(parameters.preset())?;
        transact(state, parameters, |candidate, constants| {
            attestation_kernel::<false>(candidate, operation, parameters, constants, parent_slot)
        })
    }
}

pub(crate) fn same_slot_kernel(
    state: &BeaconState,
    data: &gloas::AttestationData,
    constants: &Constants,
) -> Result<bool, ProcessingError> {
    if data.slot == 0 {
        return Ok(true);
    }
    let current = block_root(state, data.slot, constants)?;
    let prior = block_root(state, data.slot - 1, constants)?;
    Ok(data.beacon_block_root == current && current != prior)
}
/// Whether this attestation identifies a block proposed at its slot, including genesis.
pub fn is_attestation_same_slot(
    state: &BeaconState,
    data: &gloas::AttestationData,
    parameters: &GloasParameters,
) -> Result<bool, ProcessingError> {
    let constants = checked_constants(state, parameters)?;
    same_slot_kernel(state, data, &constants)
}

#[cfg(test)]
mod tests {
    use super::{super::blocks::tests::state_and_keys, *};
    use crate::ssz::{Preset, PresetBits, ProgressiveBitList, Ssz};
    use blst::min_pk::{AggregateSignature, SecretKey};
    use reth_embedded_beacon_crypto::ETHEREUM_BLS_DST;
    use reth_embedded_beacon_primitives::Checkpoint;

    pub(super) fn attestation(
        state: &BeaconState,
        keys: &[SecretKey],
        slot: u64,
        payload_index: u64,
        parameters: &GloasParameters,
    ) -> Attestation {
        let constants = checked_constants(state, parameters).unwrap();
        let indices = committee(state, slot, 0, &constants).unwrap();
        let length = indices.len();
        let mut bytes = vec![0; (length + 8) / 8];
        for bit in 0..=length {
            bytes[bit / 8] |= 1 << (bit % 8);
        }
        let data = gloas::AttestationData {
            slot,
            index: payload_index,
            beacon_block_root: state.block_roots.values[(slot % 64) as usize],
            source: state.current_justified_checkpoint,
            target: Checkpoint {
                epoch: slot / 8,
                root: state.block_roots.values[(slot / 8 * 8 % 64) as usize],
            },
        };
        let root = compute_signing_root(
            data.hash_tree_root(parameters.preset()).unwrap(),
            common::domain(state, [1, 0, 0, 0], data.target.epoch),
        );
        let signatures = indices
            .iter()
            .map(|&i| keys[i as usize].sign(&root, ETHEREUM_BLS_DST, &[]))
            .collect::<Vec<_>>();
        let references = signatures.iter().collect::<Vec<_>>();
        Attestation {
            aggregation_bits: ProgressiveBitList::from_ssz(&bytes, parameters.preset()).unwrap(),
            committee_bits: PresetBits::from_ssz(&[1], Preset::Minimal).unwrap(),
            data,
            signature: AggregateSignature::aggregate(&references, true)
                .unwrap()
                .to_signature()
                .to_bytes(),
        }
    }
    #[test]
    fn combined_indexing_preserves_strict_signature_domain_and_original_bounds() {
        let (state, keys, parameters) = state_and_keys();
        let before = state.clone();
        let vote = attestation(&state, &keys, 0, 0, &parameters);
        let expected = get_indexed_attestation(&state, &vote, &parameters).unwrap();
        is_valid_indexed_attestation(&state, &expected, &parameters).unwrap();
        assert_eq!(
            get_validated_indexed_attestation(&state, &vote, &parameters).unwrap(),
            expected,
        );
        let mut invalid = vote.clone();
        invalid.signature = [0; 96];
        assert!(get_validated_indexed_attestation(&state, &invalid, &parameters).is_err());
        let mut truncated = vote.clone();
        truncated.aggregation_bits = ProgressiveBitList::from_ssz(&[1], Preset::Minimal).unwrap();
        assert!(get_validated_indexed_attestation(&state, &truncated, &parameters).is_err());
        let mut invalid_committee = vote.clone();
        invalid_committee.committee_bits = PresetBits::from_ssz(&[8], Preset::Minimal).unwrap();
        assert!(get_validated_indexed_attestation(&state, &invalid_committee, &parameters).is_err());
        let mut wrong_domain = vote;
        wrong_domain.data.target.epoch += 1;
        assert!(get_validated_indexed_attestation(&state, &wrong_domain, &parameters).is_err());
        assert_eq!(state, before);
    }

    #[cfg(feature = "spec-tests")]
    #[test]
    fn fixture_combined_indexing_disables_only_signature_verification() {
        let (state, keys, parameters) = state_and_keys();
        let mut vote = attestation(&state, &keys, 0, 0, &parameters);
        vote.signature = [0; 96];
        assert!(spec_only::get_validated_indexed_attestation(&state, &vote, &parameters).is_ok());
        vote.aggregation_bits = ProgressiveBitList::from_ssz(&[1], Preset::Minimal).unwrap();
        assert!(spec_only::get_validated_indexed_attestation(&state, &vote, &parameters).is_err());
    }
    #[test]
    fn skipped_slot_head_availability_uses_parent_block_slot_and_never_adds_payment_weight() {
        let (mut state, keys, parameters) = state_and_keys();
        state.slot = 3;
        state.block_roots.values[0] = [7; 32];
        state.block_roots.values[1] = [9; 32];
        state.block_roots.values[2] = [9; 32];
        state.execution_payload_availability.bits.set(1, true).unwrap();
        state.execution_payload_availability.bits.set(2, false).unwrap();
        state.builder_pending_payments.values[10].withdrawal.amount = 100;
        let op = attestation(&state, &keys, 2, 1, &parameters);
        let indices = get_attesting_indices(&state, &op, &parameters).unwrap();
        let before = state.clone();
        process_attestation(&mut state, &op, 1, &parameters).unwrap();
        for &i in &indices {
            assert_eq!(state.current_epoch_participation.values[i as usize], 7);
        }
        assert_eq!(state.builder_pending_payments.values[10].weight, 0);
        let mut unavailable = before;
        unavailable.execution_payload_availability.bits.set(1, false).unwrap();
        process_attestation(&mut unavailable, &op, 1, &parameters).unwrap();
        for i in indices {
            assert_eq!(unavailable.current_epoch_participation.values[i as usize], 3);
        }
        assert_eq!(unavailable.builder_pending_payments.values[10].weight, 0);
    }
    #[test]
    fn same_slot_payment_counts_only_first_participation_and_requires_payload_index_zero() {
        let (mut state, keys, parameters) = state_and_keys();
        state.slot = 2;
        state.block_roots.values[0] = [7; 32];
        state.block_roots.values[1] = [9; 32];
        state.builder_pending_payments.values[9].withdrawal.amount = 100;
        let op = attestation(&state, &keys, 1, 0, &parameters);
        let indices = get_attesting_indices(&state, &op, &parameters).unwrap();
        let weight = indices
            .iter()
            .map(|&i| state.validators.values[i as usize].effective_balance)
            .sum::<u64>();
        let before = state.clone();
        process_attestation(&mut state, &op, 1, &parameters).unwrap();
        assert_eq!(state.builder_pending_payments.values[9].weight, weight);
        let after = state.clone();
        process_attestation(&mut state, &op, 1, &parameters).unwrap();
        assert_eq!(state, after);
        let mut previous = before.clone();
        for &i in &indices {
            previous.current_epoch_participation.values[i as usize] = 1;
        }
        process_attestation(&mut previous, &op, 1, &parameters).unwrap();
        assert_eq!(previous.builder_pending_payments.values[9].weight, 0);
        let mut invalid = before;
        let op = attestation(&invalid, &keys, 1, 1, &parameters);
        let original = invalid.clone();
        assert!(matches!(
            process_attestation(&mut invalid, &op, 1, &parameters),
            Err(ProcessingError::InvalidState("same-slot attestation payload index must be zero"))
        ));
        assert_eq!(invalid, original);
    }
    #[test]
    fn proposer_slashing_clears_only_its_own_payment_and_keeps_unrelated_proposals() {
        let (mut state, keys, parameters) = state_and_keys();
        state.slot = 1;
        let header = crate::ssz::phase0::SignedBeaconBlockHeader {
            message: reth_embedded_beacon_primitives::BeaconBlockHeader {
                slot: 0,
                proposer_index: 0,
                parent_root: [0; 32],
                state_root: [0; 32],
                body_root: [0; 32],
            },
            signature: [0; 96],
        };
        let mut first = header;
        let mut second = first.clone();
        second.message.body_root = [1; 32];
        for signed in [&mut first, &mut second] {
            signed.signature = keys[0]
                .sign(
                    &compute_signing_root(
                        signed.message.hash_tree_root(),
                        common::domain(&state, [0; 4], 0),
                    ),
                    ETHEREUM_BLS_DST,
                    &[],
                )
                .to_bytes();
        }
        let operation = ProposerSlashing { signed_header_1: first, signed_header_2: second };
        state.builder_pending_payments.values[8] = builder::empty_payment();
        state.builder_pending_payments.values[8].withdrawal.amount = 55;
        state.builder_pending_payments.values[8].proposer_index = 1;
        let mut unrelated = state.clone();
        process_proposer_slashing(&mut unrelated, &operation, &parameters).unwrap();
        assert_eq!(unrelated.builder_pending_payments.values[8].withdrawal.amount, 55);
        assert!(unrelated.validators.values[0].slashed);
        state.builder_pending_payments.values[8].proposer_index = 0;
        process_proposer_slashing(&mut state, &operation, &parameters).unwrap();
        assert_eq!(state.builder_pending_payments.values[8], builder::empty_payment());
    }

    #[test]
    fn native_loop_reuse_matches_fresh_preparation_after_slashing_and_bad_middle_vote() {
        let (mut state, keys, parameters) = state_and_keys();
        state.slot = 1;
        state.block_roots.values[0] = state.current_justified_checkpoint.root;
        let constants = checked_constants(&state, &parameters).unwrap();
        common::slash_validator_kernel(&mut state, 0, None, &parameters, &constants).unwrap();
        assert!(state.validators.values[0].slashed);
        let vote = attestation(&state, &keys, 0, 0, &parameters);
        let mut invalid = vote.clone();
        invalid.signature = [0; 96];
        let mut fresh = state.clone();
        let mut grouped = state;
        let mut inputs = context::AttestationInputs::default();
        for operation in [&vote, &invalid, &vote] {
            let before = grouped.clone();
            let expected =
                attestation_kernel::<true>(&mut fresh, operation, &parameters, &constants, 0);
            let actual = attestation_kernel_with_inputs::<true>(
                &mut grouped,
                operation,
                &parameters,
                &constants,
                0,
                &mut inputs,
            );
            assert_eq!(
                actual.as_ref().err().map(ToString::to_string),
                expected.as_ref().err().map(ToString::to_string)
            );
            assert_eq!(grouped, fresh);
            if actual.is_err() {
                assert_eq!(grouped, before);
            }
        }
    }

    #[test]
    fn native_block_vote_preparation_preserves_transaction_rollback() {
        let (mut state, keys, parameters) = state_and_keys();
        state.slot = 1;
        state.block_roots.values[0] = state.current_justified_checkpoint.root;
        let vote = attestation(&state, &keys, 0, 0, &parameters);
        let mut invalid = vote.clone();
        invalid.signature = [0; 96];
        let mut body = BeaconBlockBody::default_value(parameters.preset()).unwrap();
        body.attestations = ProgressiveList::from_vec(vec![vote.clone(), invalid, vote.clone()]);
        let before = state.clone();
        assert!(process_operations(&mut state, &body, 0, &parameters).is_err());
        assert_eq!(state, before);
        body.attestations = ProgressiveList::from_vec(vec![vote.clone(), vote.clone()]);
        let constants = checked_constants(&state, &parameters).unwrap();
        let mut expected = state.clone();
        for operation in &body.attestations.values {
            attestation_kernel::<true>(&mut expected, operation, &parameters, &constants, 0)
                .unwrap();
        }
        process_operations(&mut state, &body, 0, &parameters).unwrap();
        assert_eq!(state, expected);
    }
}

#[cfg(feature = "spec-tests")]
#[path = "operations_artificial.rs"]
pub(crate) mod artificial;

mod checked;
pub use checked::CheckedAttestationState;

mod context;
