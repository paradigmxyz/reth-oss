//! Electra multi-committee attestations, strict signatures and changed operation sequencing.

use super::{
    checked_constants, common, execution_requests, transact, with_altair, ElectraParameters,
};
use crate::{
    capella_processing, compute_committee, compute_seed, integer_squareroot,
    phase0_processing::{add, mul, position, sub, Constants, ProcessingError},
    ssz::{
        capella,
        electra::{
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
fn committee(
    state: &BeaconState,
    slot: u64,
    committee_index: u64,
    constants: &Constants,
) -> Result<Vec<u64>, ProcessingError> {
    let epoch = slot / constants.slots_per_epoch;
    let count = common::get_committee_count_per_slot(state, epoch, constants)?;
    if committee_index >= count {
        return Err(ProcessingError::InvalidState("attestation committee index is out of range"));
    }
    let active = common::active_indices(state, epoch);
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
    let seed = compute_seed([1, 0, 0, 0], epoch, mix);
    let committee_position = add(
        mul(slot % constants.slots_per_epoch, count, "slot committee position")?,
        committee_index,
        "committee position",
    )?;
    Ok(compute_committee(
        &active,
        seed,
        committee_position,
        mul(count, constants.slots_per_epoch, "committees per epoch")?,
        constants.shuffle_rounds,
    )?)
}

/// Derive sorted unique multi-committee participants without verifying their aggregate signature.
pub fn get_attesting_indices(
    state: &BeaconState,
    attestation: &Attestation,
    parameters: &ElectraParameters,
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
        let committee = committee(state, attestation.data.slot, committee_index as u64, constants)?;
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
/// Convert an Electra aggregate to exact indexed form; signature validation remains separate.
pub fn get_indexed_attestation(
    state: &BeaconState,
    attestation: &Attestation,
    parameters: &ElectraParameters,
) -> Result<IndexedAttestation, ProcessingError> {
    Ok(IndexedAttestation {
        attesting_indices: List::from_vec(get_attesting_indices(state, attestation, parameters)?),
        data: attestation.data.clone(),
        signature: attestation.signature,
    })
}
fn indexed_kernel<const VERIFY_BLS: bool>(
    state: &BeaconState,
    attestation: &IndexedAttestation,
    parameters: &ElectraParameters,
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
/// Require the strict aggregate signature for sorted unique Electra indexed participants.
pub fn is_valid_indexed_attestation(
    state: &BeaconState,
    attestation: &IndexedAttestation,
    parameters: &ElectraParameters,
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
    data: &electra::AttestationData,
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
    parameters: &ElectraParameters,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    let data = &operation.data;
    let current = state.slot / constants.slots_per_epoch;
    if (data.target.epoch != current && data.target.epoch != current.saturating_sub(1)) ||
        data.target.epoch != data.slot / constants.slots_per_epoch
    {
        return Err(ProcessingError::InvalidState("attestation target epoch is inconsistent"));
    }
    if data.index != 0 {
        return Err(ProcessingError::InvalidState("Electra attestation data index must be zero"));
    }
    if add(data.slot, 1, "minimum inclusion slot")? > state.slot {
        return Err(ProcessingError::InvalidState("attestation inclusion is premature"));
    }
    let indices = attesting_indices(state, operation, constants, true)?;
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
        integer_squareroot(common::total_active_balance(state, constants)?);
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
    parameters: &ElectraParameters,
) -> Result<(), ProcessingError> {
    operation.hash_tree_root(parameters.preset())?;
    transact(state, parameters, |candidate, constants| {
        attestation_kernel::<true>(candidate, operation, parameters, constants)
    })
}

const fn slashable(validator: &electra::Validator, epoch: u64) -> bool {
    !validator.slashed &&
        validator.activation_epoch <= epoch &&
        epoch < validator.withdrawable_epoch
}
pub(crate) fn proposer_slashing_kernel(
    state: &mut BeaconState,
    operation: &ProposerSlashing,
    parameters: &ElectraParameters,
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
    common::slash_validator_kernel(state, first.proposer_index, None, parameters, constants)
}
/// Strict proposer equivocation with Electra slashing penalties and balance-based exit churn.
pub fn process_proposer_slashing(
    state: &mut BeaconState,
    operation: &ProposerSlashing,
    parameters: &ElectraParameters,
) -> Result<(), ProcessingError> {
    operation.hash_tree_root(parameters.preset())?;
    transact(state, parameters, |candidate, constants| {
        proposer_slashing_kernel(candidate, operation, parameters, constants)
    })
}
pub(crate) fn attester_slashing_kernel(
    state: &mut BeaconState,
    operation: &AttesterSlashing,
    parameters: &ElectraParameters,
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
/// Strict extended indexed-attestation slashing with exact Electra penalties.
pub fn process_attester_slashing(
    state: &mut BeaconState,
    operation: &AttesterSlashing,
    parameters: &ElectraParameters,
) -> Result<(), ProcessingError> {
    operation.hash_tree_root(parameters.preset())?;
    transact(state, parameters, |candidate, constants| {
        attester_slashing_kernel(candidate, operation, parameters, constants)
    })
}
pub(crate) fn voluntary_exit_kernel(
    state: &mut BeaconState,
    operation: &SignedVoluntaryExit,
    parameters: &ElectraParameters,
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
                parameters.deneb.capella.bellatrix.altair.phase0.shard_committee_period,
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
        parameters.deneb.capella.fork_version,
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
    parameters: &ElectraParameters,
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
    parameters: &ElectraParameters,
) -> Result<(), ProcessingError> {
    with_altair(state, |common| {
        capella_processing::bls_to_execution_change_kernel(
            common,
            &bls_change(operation),
            &parameters.deneb.capella,
        )
    })
}
/// Require the exact withdrawal-key signature before replacing BLS credentials.
pub fn process_bls_to_execution_change(
    state: &mut BeaconState,
    operation: &SignedBLSToExecutionChange,
    parameters: &ElectraParameters,
) -> Result<(), ProcessingError> {
    operation.hash_tree_root(parameters.preset())?;
    transact(state, parameters, |candidate, _| bls_change_kernel(candidate, operation, parameters))
}

pub(crate) fn operations_kernel(
    state: &mut BeaconState,
    body: &BeaconBlockBody,
    parameters: &ElectraParameters,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    let limit = state.eth1_data.deposit_count.min(state.deposit_requests_start_index);
    let expected = if state.eth1_deposit_index < limit {
        (limit - state.eth1_deposit_index).min(16)
    } else {
        0
    };
    if body.deposits.values.len() as u64 != expected {
        return Err(ProcessingError::InvalidState(
            "legacy deposit count differs from Electra cutover limit",
        ));
    }
    for value in &body.proposer_slashings.values {
        proposer_slashing_kernel(state, value, parameters, constants)?;
    }
    for value in &body.attester_slashings.values {
        attester_slashing_kernel(state, value, parameters, constants)?;
    }
    for value in &body.attestations.values {
        attestation_kernel::<true>(state, value, parameters, constants)?;
    }
    for value in &body.deposits.values {
        execution_requests::deposit_kernel(state, value, parameters)?;
    }
    for value in &body.voluntary_exits.values {
        voluntary_exit_kernel(state, value, parameters, constants)?;
    }
    for value in &body.bls_to_execution_changes.values {
        bls_change_kernel(state, value, parameters)?;
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
/// Atomically process the exact Electra operation order; it does not authorize an unverified EL
/// body.
pub fn process_operations(
    state: &mut BeaconState,
    body: &BeaconBlockBody,
    parameters: &ElectraParameters,
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

    /// Artificial indexed aggregates under declared fixture metadata; never a runtime proof.
    /// Sorted nonempty indices and all registry/SSZ bounds remain enforced.
    #[cfg(feature = "spec-tests")]
    pub fn is_valid_indexed_attestation(
        state: &BeaconState,
        operation: &IndexedAttestation,
        parameters: &ElectraParameters,
    ) -> Result<(), ProcessingError> {
        checked_constants(state, parameters)?;
        operation.hash_tree_root(parameters.preset())?;
        indexed_kernel::<false>(state, operation, parameters)
    }

    /// Apply only a metadata-declared artificial attestation fixture with BLS disabled.
    pub fn process_attestation(
        state: &mut BeaconState,
        operation: &Attestation,
        parameters: &ElectraParameters,
    ) -> Result<(), ProcessingError> {
        operation.hash_tree_root(parameters.preset())?;
        transact(state, parameters, |candidate, constants| {
            attestation_kernel::<false>(candidate, operation, parameters, constants)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{super::blocks::tests::state_and_keys, *};
    use crate::ssz::{phase0::VoluntaryExit, Preset, PresetBitList, PresetBits, Ssz};
    use blst::min_pk::{AggregateSignature, SecretKey, Signature};
    use reth_embedded_beacon_crypto::ETHEREUM_BLS_DST;
    use reth_embedded_beacon_primitives::Checkpoint;

    fn bitlist(count: usize, active: usize) -> electra::AggregationBits {
        let mut bytes = vec![0; (count + 1).div_ceil(8)];
        for bit in 0..active {
            bytes[bit / 8] |= 1 << (bit % 8);
        }
        bytes[count / 8] |= 1 << (count % 8);
        PresetBitList::from_ssz(&bytes, Preset::Minimal).unwrap()
    }
    fn multi_attestation() -> (BeaconState, Attestation, ElectraParameters) {
        let (mut state, mut keys, parameters) = state_and_keys();
        for ordinal in 32..128u64 {
            let mut seed = [0; 32];
            seed[..8].copy_from_slice(&(ordinal + 1000).to_le_bytes());
            let key = SecretKey::key_gen(&seed, &[]).unwrap();
            let mut validator = state.validators.values[0].clone();
            validator.pubkey = key.sk_to_pk().to_bytes();
            state.validators.values.push(validator);
            state.balances.values.push(32_000_000_000);
            state.previous_epoch_participation.values.push(0);
            state.current_epoch_participation.values.push(0);
            state.inactivity_scores.values.push(0);
            keys.push(key);
        }
        state.slot = 1;
        let constants = checked_constants(&state, &parameters).unwrap();
        assert_eq!(common::get_committee_count_per_slot(&state, 0, &constants).unwrap(), 4);
        let participants = committee(&state, 0, 0, &constants).unwrap().len() +
            committee(&state, 0, 2, &constants).unwrap().len();
        let data = electra::AttestationData {
            slot: 0,
            index: 0,
            beacon_block_root: [0; 32],
            source: state.current_justified_checkpoint,
            target: Checkpoint { epoch: 0, root: [0; 32] },
        };
        let mut operation = Attestation {
            aggregation_bits: bitlist(participants, participants),
            data,
            signature: [0; 96],
            committee_bits: PresetBits::from_ssz(&[5], Preset::Minimal).unwrap(),
        };
        let indices = get_attesting_indices(&state, &operation, &parameters).unwrap();
        assert_eq!(indices.len(), participants);
        let root = compute_signing_root(
            operation.data.hash_tree_root(Preset::Minimal).unwrap(),
            common::domain(&state, [1, 0, 0, 0], 0),
        );
        let signatures = indices
            .iter()
            .map(|&i| keys[i as usize].sign(&root, ETHEREUM_BLS_DST, &[]))
            .collect::<Vec<Signature>>();
        let refs = signatures.iter().collect::<Vec<_>>();
        operation.signature =
            AggregateSignature::aggregate(&refs, true).unwrap().to_signature().to_bytes();
        (state, operation, parameters)
    }
    #[test]
    fn real_multi_committee_signature_requires_each_selected_committee_and_exact_bit_length() {
        let (mut state, operation, parameters) = multi_attestation();
        let before = state.clone();
        let mut invalid = operation.clone();
        invalid.data.index = 1;
        assert!(process_attestation(&mut state, &invalid, &parameters).is_err());
        assert_eq!(state, before);
        invalid = operation.clone();
        invalid.signature = [0; 96];
        assert!(process_attestation(&mut state, &invalid, &parameters).is_err());
        assert_eq!(state, before);
        let constants = checked_constants(&state, &parameters).unwrap();
        let first_size = committee(&state, 0, 0, &constants).unwrap().len();
        invalid = operation.clone();
        invalid.aggregation_bits = bitlist(operation.aggregation_bits.bits.len(), first_size);
        assert!(matches!(
            process_attestation(&mut state, &invalid, &parameters),
            Err(ProcessingError::InvalidState(
                "selected attestation committee has no participants"
            ))
        ));
        assert_eq!(state, before);
        invalid = operation.clone();
        invalid.aggregation_bits = bitlist(
            operation.aggregation_bits.bits.len() + 1,
            operation.aggregation_bits.bits.len(),
        );
        assert!(process_attestation(&mut state, &invalid, &parameters).is_err());
        assert_eq!(state, before);
        let indices = get_attesting_indices(&state, &operation, &parameters).unwrap();
        process_attestation(&mut state, &operation, &parameters).unwrap();
        for index in indices {
            assert_eq!(state.current_epoch_participation.values[index as usize] & 7, 7);
        }
        assert!(state
            .balances
            .values
            .iter()
            .zip(&before.balances.values)
            .any(|(after, before)| after > before));
    }
    #[test]
    fn voluntary_exit_uses_capella_domain_and_rejects_a_pending_withdrawal_balance() {
        let (mut state, keys, parameters) = state_and_keys();
        state.slot = parameters.deneb.capella.bellatrix.altair.phase0.shard_committee_period *
            parameters.slots_per_epoch();
        let epoch = state.slot / parameters.slots_per_epoch();
        let message = VoluntaryExit { epoch, validator_index: 0 };
        let root = compute_signing_root(
            message.hash_tree_root(parameters.preset()).unwrap(),
            compute_domain(
                [4, 0, 0, 0],
                parameters.deneb.capella.fork_version,
                state.genesis_validators_root,
            ),
        );
        let operation = SignedVoluntaryExit {
            message,
            signature: keys[0].sign(&root, ETHEREUM_BLS_DST, &[]).to_bytes(),
        };
        state.pending_partial_withdrawals.values.push(electra::PendingPartialWithdrawal {
            validator_index: 0,
            amount: 1,
            withdrawable_epoch: epoch + 1,
        });
        let before = state.clone();
        assert!(process_voluntary_exit(&mut state, &operation, &parameters).is_err());
        assert_eq!(state, before);
        state.pending_partial_withdrawals.values.clear();
        let before = state.clone();
        let mut invalid = operation.clone();
        let wrong = compute_signing_root(
            invalid.message.hash_tree_root(parameters.preset()).unwrap(),
            compute_domain([4, 0, 0, 0], parameters.fork_version, state.genesis_validators_root),
        );
        invalid.signature = keys[0].sign(&wrong, ETHEREUM_BLS_DST, &[]).to_bytes();
        assert!(process_voluntary_exit(&mut state, &invalid, &parameters).is_err());
        assert_eq!(state, before);
        process_voluntary_exit(&mut state, &operation, &parameters).unwrap();
        assert_ne!(state.validators.values[0].exit_epoch, u64::MAX);
    }
}

#[cfg(feature = "spec-tests")]
#[path = "operations_artificial.rs"]
pub(crate) mod artificial;
