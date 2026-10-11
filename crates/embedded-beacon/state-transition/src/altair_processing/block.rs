//! Mandatory signature and post-state validation for signed Altair block import.

use super::{
    active_indices, add, base_reward, base_reward_per_increment, block_root_at_slot,
    checked_constants, committee, epoch, exit_kernel, is_active, mul, participation_flags,
    position, sub, transact, AltairParameters, Constants, ProcessingError,
    EFFECTIVE_BALANCE_INCREMENT, MAX_EFFECTIVE_BALANCE, PROPOSER_WEIGHT, WEIGHT_DENOMINATOR,
};
use crate::{
    compute_seed, compute_shuffled_index, is_valid_merkle_branch,
    ssz::{
        altair::{
            Attestation, AttestationData, AttesterSlashing, BeaconBlock, BeaconBlockBody,
            BeaconState, Deposit, IndexedAttestation, ProposerSlashing, SignedBeaconBlock,
            SignedVoluntaryExit, SyncAggregate, Validator,
        },
        List,
    },
};
use reth_embedded_beacon_crypto::{
    compute_domain, compute_signing_root, eth_fast_aggregate_verify, fast_aggregate_verify,
    subtract_aggregate_pubkeys, verify_signature,
};
use reth_embedded_beacon_primitives::{
    bytes_root, merkleize, sha256, uint64_root, BeaconBlockHeader, Root,
};

const DOMAIN_BEACON_PROPOSER: [u8; 4] = [0, 0, 0, 0];
const DOMAIN_BEACON_ATTESTER: [u8; 4] = [1, 0, 0, 0];
const DOMAIN_RANDAO: [u8; 4] = [2, 0, 0, 0];
const DOMAIN_DEPOSIT: [u8; 4] = [3, 0, 0, 0];
const DOMAIN_VOLUNTARY_EXIT: [u8; 4] = [4, 0, 0, 0];
const WHISTLEBLOWER_REWARD_QUOTIENT: u64 = 512;

/// Import a signed Altair block with mandatory BLS and post-state root checks.
///
/// Callers must select the scheduled fork and bound the slot gap before invoking this kernel.
pub fn state_transition(
    state: &mut BeaconState,
    block: &SignedBeaconBlock,
    parameters: &AltairParameters,
) -> Result<(), ProcessingError> {
    block.hash_tree_root(parameters.phase0.preset)?;
    transact(state, parameters, |candidate, constants| {
        if candidate.slot != block.message.slot {
            super::slots_kernel(candidate, block.message.slot, parameters, constants)?;
        }
        verify_block_signature_kernel::<true>(candidate, block, parameters)?;
        block_kernel::<true>(candidate, &block.message, parameters, constants)?;
        if candidate.hash_tree_root(parameters.phase0.preset)? != block.message.state_root {
            return Err(ProcessingError::InvalidState("block post-state root mismatch"))
        }
        Ok(())
    })
}

/// Check the signed block's proposer signature without modifying state.
pub fn verify_block_signature(
    state: &BeaconState,
    block: &SignedBeaconBlock,
    parameters: &AltairParameters,
) -> Result<(), ProcessingError> {
    checked_constants(state, parameters)?;
    block.hash_tree_root(parameters.phase0.preset)?;
    verify_block_signature_kernel::<true>(state, block, parameters)
}

/// Process an unsigned block's header, RANDAO, votes and operations atomically.
///
/// Complete import must use `state_transition`, which also checks the outer signature and
/// post-root.
pub fn process_block(
    state: &mut BeaconState,
    block: &BeaconBlock,
    parameters: &AltairParameters,
) -> Result<(), ProcessingError> {
    block.hash_tree_root(parameters.phase0.preset)?;
    transact(state, parameters, |candidate, constants| {
        block_kernel::<true>(candidate, block, parameters, constants)
    })
}

/// Verify and cache a block header, including its actual complete body root.
pub fn process_block_header(
    state: &mut BeaconState,
    block: &BeaconBlock,
    parameters: &AltairParameters,
) -> Result<(), ProcessingError> {
    block.hash_tree_root(parameters.phase0.preset)?;
    transact(state, parameters, |candidate, constants| {
        header_kernel(candidate, block, parameters, constants)
    })
}

/// Verify the proposer RANDAO reveal and mix its SHA-256 digest.
pub fn process_randao(
    state: &mut BeaconState,
    body: &BeaconBlockBody,
    parameters: &AltairParameters,
) -> Result<(), ProcessingError> {
    body.hash_tree_root(parameters.phase0.preset)?;
    transact(state, parameters, |candidate, constants| {
        randao_kernel::<true>(candidate, body, parameters, constants)
    })
}

/// Append an execution-data vote and adopt a strict voting-period majority.
pub fn process_eth1_data(
    state: &mut BeaconState,
    body: &BeaconBlockBody,
    parameters: &AltairParameters,
) -> Result<(), ProcessingError> {
    body.hash_tree_root(parameters.phase0.preset)?;
    transact(state, parameters, |candidate, constants| eth1_kernel(candidate, body, constants))
}

/// Validate deposit count and process every block operation in specification order.
pub fn process_operations(
    state: &mut BeaconState,
    body: &BeaconBlockBody,
    parameters: &AltairParameters,
) -> Result<(), ProcessingError> {
    body.hash_tree_root(parameters.phase0.preset)?;
    transact(state, parameters, |candidate, constants| {
        operations_kernel::<true>(candidate, body, parameters, constants)
    })
}

/// Validate conflicting signed proposer headers and apply slashing.
pub fn process_proposer_slashing(
    state: &mut BeaconState,
    operation: &ProposerSlashing,
    parameters: &AltairParameters,
) -> Result<(), ProcessingError> {
    operation.hash_tree_root(parameters.phase0.preset)?;
    transact(state, parameters, |candidate, constants| {
        proposer_slashing_kernel::<true>(candidate, operation, parameters, constants)
    })
}

/// Validate conflicting aggregate attestations and slash their eligible intersection.
pub fn process_attester_slashing(
    state: &mut BeaconState,
    operation: &AttesterSlashing,
    parameters: &AltairParameters,
) -> Result<(), ProcessingError> {
    operation.hash_tree_root(parameters.phase0.preset)?;
    transact(state, parameters, |candidate, constants| {
        attester_slashing_kernel::<true>(candidate, operation, parameters, constants)
    })
}

/// Verify a committee attestation and record its inclusion metadata.
pub fn process_attestation(
    state: &mut BeaconState,
    operation: &Attestation,
    parameters: &AltairParameters,
) -> Result<(), ProcessingError> {
    operation.hash_tree_root(parameters.phase0.preset)?;
    transact(state, parameters, |candidate, constants| {
        attestation_kernel::<true>(candidate, operation, parameters, constants)
    })
}

/// Verify an ordered contract deposit proof and apply its protocol signature semantics.
///
/// Invalid proof-of-possession deposits are consumed without registering a validator, as specified.
pub fn process_deposit(
    state: &mut BeaconState,
    operation: &Deposit,
    parameters: &AltairParameters,
) -> Result<(), ProcessingError> {
    operation.hash_tree_root(parameters.phase0.preset)?;
    transact(state, parameters, |candidate, _| {
        deposit_kernel::<true>(candidate, operation, parameters)
    })
}

/// Validate a signed voluntary exit and place the validator in the exit queue.
pub fn process_voluntary_exit(
    state: &mut BeaconState,
    operation: &SignedVoluntaryExit,
    parameters: &AltairParameters,
) -> Result<(), ProcessingError> {
    operation.hash_tree_root(parameters.phase0.preset)?;
    transact(state, parameters, |candidate, constants| {
        voluntary_exit_kernel::<true>(candidate, operation, parameters, constants)
    })
}

/// Check sorted unique participants and the mandatory aggregate attestation signature.
pub fn is_valid_indexed_attestation(
    state: &BeaconState,
    attestation: &IndexedAttestation,
    parameters: &AltairParameters,
) -> Result<(), ProcessingError> {
    checked_constants(state, parameters)?;
    attestation.hash_tree_root(parameters.phase0.preset)?;
    indexed_attestation_kernel::<true>(state, attestation, parameters)
}

/// Compute the effective-balance-weighted proposer for the current Altair slot.
pub fn get_beacon_proposer_index(
    state: &BeaconState,
    parameters: &AltairParameters,
) -> Result<u64, ProcessingError> {
    let constants = checked_constants(state, parameters)?;
    proposer_index(state, &constants)
}

/// Apply a validator slashing and mandatory proposer/whistleblower rewards.
pub fn slash_validator(
    state: &mut BeaconState,
    index: u64,
    whistleblower: Option<u64>,
    parameters: &AltairParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, constants| {
        let index = checked_index(index, candidate.validators.values.len())?;
        slashing_kernel(candidate, index, whistleblower, parameters, constants)
    })
}

fn checked_index(index: u64, length: usize) -> Result<usize, ProcessingError> {
    let index = usize::try_from(index)
        .map_err(|_| ProcessingError::InvalidState("validator index does not fit platform"))?;
    if index >= length {
        return Err(ProcessingError::InvalidState("validator index is out of range"))
    }
    Ok(index)
}

fn domain(state: &BeaconState, domain_type: [u8; 4], signing_epoch: u64) -> Root {
    let version = if signing_epoch < state.fork.epoch {
        state.fork.previous_version
    } else {
        state.fork.current_version
    };
    compute_domain(domain_type, version, state.genesis_validators_root)
}

pub(crate) fn proposer_index(
    state: &BeaconState,
    constants: &Constants,
) -> Result<u64, ProcessingError> {
    let current = epoch(state, constants);
    let indices = active_indices(state, current);
    if indices.is_empty() {
        return Err(ProcessingError::InvalidState("no active proposer candidates"))
    }
    let mix_epoch = sub(
        add(current, constants.historical_epochs, "proposer seed epoch")?,
        2,
        "proposer seed lookback",
    )?;
    let mix = state
        .randao_mixes
        .values
        .get(position(mix_epoch, constants.historical_epochs)?)
        .copied()
        .ok_or(ProcessingError::InvalidState("RANDAO ring is truncated"))?;
    let seed = compute_seed(DOMAIN_BEACON_PROPOSER, current, mix);
    let mut input = [0u8; 40];
    input[..32].copy_from_slice(&seed);
    input[32..].copy_from_slice(&state.slot.to_le_bytes());
    let seed = sha256(&input);
    input[..32].copy_from_slice(&seed);
    let count = indices.len() as u64;
    let mut iteration = 0u64;
    loop {
        let shuffled =
            compute_shuffled_index(iteration % count, count, seed, constants.shuffle_rounds)?;
        let candidate = indices[checked_index(shuffled, indices.len())?];
        input[32..].copy_from_slice(&(iteration / 32).to_le_bytes());
        let random = sha256(&input);
        let random_byte = random[position(iteration, 32)?];
        let effective = state.validators.values
            [checked_index(candidate, state.validators.values.len())?]
        .effective_balance;
        if mul(effective, 255, "weighted proposer balance")? >=
            mul(MAX_EFFECTIVE_BALANCE, u64::from(random_byte), "weighted proposer threshold")?
        {
            return Ok(candidate)
        }
        iteration = add(iteration, 1, "proposer selection iteration")?;
    }
}

fn verify_block_signature_kernel<const VERIFY_BLS: bool>(
    state: &BeaconState,
    block: &SignedBeaconBlock,
    parameters: &AltairParameters,
) -> Result<(), ProcessingError> {
    let index = checked_index(block.message.proposer_index, state.validators.values.len())?;
    let signing_epoch = block.message.slot / parameters.slots_per_epoch();
    let root = compute_signing_root(
        block.message.hash_tree_root(parameters.phase0.preset)?,
        domain(state, DOMAIN_BEACON_PROPOSER, signing_epoch),
    );
    if VERIFY_BLS {
        verify_signature(&state.validators.values[index].pubkey, &root, &block.signature)?;
    }
    Ok(())
}

fn block_kernel<const VERIFY_BLS: bool>(
    state: &mut BeaconState,
    block: &BeaconBlock,
    parameters: &AltairParameters,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    header_kernel(state, block, parameters, constants)?;
    randao_kernel::<VERIFY_BLS>(state, &block.body, parameters, constants)?;
    eth1_kernel(state, &block.body, constants)?;
    operations_kernel::<VERIFY_BLS>(state, &block.body, parameters, constants)?;
    sync_aggregate_kernel::<VERIFY_BLS>(state, &block.body.sync_aggregate, parameters, constants)
}

fn header_kernel(
    state: &mut BeaconState,
    block: &BeaconBlock,
    parameters: &AltairParameters,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    if block.slot != state.slot || block.slot <= state.latest_block_header.slot {
        return Err(ProcessingError::InvalidState("block slot does not match a newer state slot"))
    }
    if block.proposer_index != proposer_index(state, constants)? {
        return Err(ProcessingError::InvalidState("block proposer is not the selected proposer"))
    }
    if block.parent_root != state.latest_block_header.hash_tree_root() {
        return Err(ProcessingError::InvalidState("block parent does not match latest header"))
    }
    let index = checked_index(block.proposer_index, state.validators.values.len())?;
    if state.validators.values[index].slashed {
        return Err(ProcessingError::InvalidState("block proposer is slashed"))
    }
    state.latest_block_header = BeaconBlockHeader {
        slot: block.slot,
        proposer_index: block.proposer_index,
        parent_root: block.parent_root,
        state_root: [0; 32],
        body_root: block.body.hash_tree_root(parameters.phase0.preset)?,
    };
    Ok(())
}

fn randao_kernel<const VERIFY_BLS: bool>(
    state: &mut BeaconState,
    body: &BeaconBlockBody,
    _parameters: &AltairParameters,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    randao_reveal_kernel::<VERIFY_BLS>(state, &body.randao_reveal, constants)
}

pub(crate) fn randao_reveal_kernel<const VERIFY_BLS: bool>(
    state: &mut BeaconState,
    reveal: &[u8; 96],
    constants: &Constants,
) -> Result<(), ProcessingError> {
    let current = epoch(state, constants);
    let index = checked_index(proposer_index(state, constants)?, state.validators.values.len())?;
    let root = compute_signing_root(uint64_root(current), domain(state, DOMAIN_RANDAO, current));
    if VERIFY_BLS {
        verify_signature(&state.validators.values[index].pubkey, &root, reveal)?;
    }
    let digest = sha256(reveal);
    let mix = state
        .randao_mixes
        .values
        .get_mut(position(current, constants.historical_epochs)?)
        .ok_or(ProcessingError::InvalidState("RANDAO ring is truncated"))?;
    for (byte, digest) in mix.iter_mut().zip(digest) {
        *byte ^= digest;
    }
    Ok(())
}

fn eth1_kernel(
    state: &mut BeaconState,
    body: &BeaconBlockBody,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    eth1_vote_kernel(state, &body.eth1_data, constants)
}

pub(crate) fn eth1_vote_kernel(
    state: &mut BeaconState,
    vote: &crate::ssz::phase0::Eth1Data,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    let maximum = mul(
        constants.eth1_voting_epochs,
        constants.slots_per_epoch,
        "execution-data voting period",
    )?;
    if state.eth1_data_votes.values.len() as u64 >= maximum {
        return Err(ProcessingError::InvalidState("execution-data vote list is full"))
    }
    state.eth1_data_votes.values.push(vote.clone());
    let count =
        state.eth1_data_votes.values.iter().filter(|candidate| **candidate == *vote).count() as u64;
    if mul(count, 2, "execution-data majority")? > maximum {
        state.eth1_data = vote.clone();
    }
    Ok(())
}

fn operations_kernel<const VERIFY_BLS: bool>(
    state: &mut BeaconState,
    body: &BeaconBlockBody,
    parameters: &AltairParameters,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    let outstanding =
        sub(state.eth1_data.deposit_count, state.eth1_deposit_index, "outstanding deposits")?;
    if body.deposits.values.len() as u64 != outstanding.min(16) {
        return Err(ProcessingError::InvalidState(
            "block deposit count does not match outstanding deposits",
        ))
    }
    for operation in &body.proposer_slashings.values {
        proposer_slashing_kernel::<VERIFY_BLS>(state, operation, parameters, constants)?;
    }
    for operation in &body.attester_slashings.values {
        attester_slashing_kernel::<VERIFY_BLS>(state, operation, parameters, constants)?;
    }
    for operation in &body.attestations.values {
        attestation_kernel::<VERIFY_BLS>(state, operation, parameters, constants)?;
    }
    for operation in &body.deposits.values {
        deposit_kernel::<VERIFY_BLS>(state, operation, parameters)?;
    }
    for operation in &body.voluntary_exits.values {
        voluntary_exit_kernel::<VERIFY_BLS>(state, operation, parameters, constants)?;
    }
    Ok(())
}

const fn slashable(validator: &Validator, current: u64) -> bool {
    !validator.slashed &&
        validator.activation_epoch <= current &&
        current < validator.withdrawable_epoch
}

pub(crate) fn proposer_slashing_kernel<const VERIFY_BLS: bool>(
    state: &mut BeaconState,
    operation: &ProposerSlashing,
    parameters: &AltairParameters,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    let first = operation.signed_header_1.message;
    let second = operation.signed_header_2.message;
    if first.slot != second.slot || first.proposer_index != second.proposer_index || first == second
    {
        return Err(ProcessingError::InvalidState("proposer slashing headers do not conflict"))
    }
    let index = checked_index(first.proposer_index, state.validators.values.len())?;
    if !slashable(&state.validators.values[index], epoch(state, constants)) {
        return Err(ProcessingError::InvalidState("proposer is not slashable"))
    }
    let key = state.validators.values[index].pubkey;
    for signed in [&operation.signed_header_1, &operation.signed_header_2] {
        let root = compute_signing_root(
            signed.message.hash_tree_root(),
            domain(state, DOMAIN_BEACON_PROPOSER, signed.message.slot / constants.slots_per_epoch),
        );
        if VERIFY_BLS {
            verify_signature(&key, &root, &signed.signature)?;
        }
    }
    slashing_kernel(state, index, None, parameters, constants)
}

pub(crate) fn indexed_attestation_kernel<const VERIFY_BLS: bool>(
    state: &BeaconState,
    attestation: &IndexedAttestation,
    parameters: &AltairParameters,
) -> Result<(), ProcessingError> {
    let indices = &attestation.attesting_indices.values;
    if indices.is_empty() || indices.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(ProcessingError::InvalidState(
            "indexed attestation participants are not nonempty sorted unique indexes",
        ))
    }
    let keys = indices
        .iter()
        .map(|&index| {
            let index = checked_index(index, state.validators.values.len())?;
            Ok(state.validators.values[index].pubkey)
        })
        .collect::<Result<Vec<_>, ProcessingError>>()?;
    let root = compute_signing_root(
        attestation.data.hash_tree_root(parameters.phase0.preset)?,
        domain(state, DOMAIN_BEACON_ATTESTER, attestation.data.target.epoch),
    );
    if VERIFY_BLS {
        fast_aggregate_verify(&keys, &root, &attestation.signature)?;
    }
    Ok(())
}

fn slashable_attestation_data(first: &AttestationData, second: &AttestationData) -> bool {
    (first != second && first.target.epoch == second.target.epoch) ||
        (first.source.epoch < second.source.epoch && second.target.epoch < first.target.epoch)
}

pub(crate) fn attester_slashing_kernel<const VERIFY_BLS: bool>(
    state: &mut BeaconState,
    operation: &AttesterSlashing,
    parameters: &AltairParameters,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    let first = &operation.attestation_1;
    let second = &operation.attestation_2;
    if !slashable_attestation_data(&first.data, &second.data) {
        return Err(ProcessingError::InvalidState("attester slashing data do not conflict"))
    }
    indexed_attestation_kernel::<VERIFY_BLS>(state, first, parameters)?;
    indexed_attestation_kernel::<VERIFY_BLS>(state, second, parameters)?;
    let current = epoch(state, constants);
    let mut slashed_any = false;
    for &index in &first.attesting_indices.values {
        if second.attesting_indices.values.binary_search(&index).is_ok() {
            let index = checked_index(index, state.validators.values.len())?;
            if slashable(&state.validators.values[index], current) {
                slashing_kernel(state, index, None, parameters, constants)?;
                slashed_any = true;
            }
        }
    }
    if !slashed_any {
        return Err(ProcessingError::InvalidState("attester slashing has no slashable intersection"))
    }
    Ok(())
}

/// Derive sorted indexes from the exact shuffled committee without verifying signatures.
pub fn get_attesting_indices(
    state: &BeaconState,
    attestation: &Attestation,
    parameters: &AltairParameters,
) -> Result<Vec<u64>, ProcessingError> {
    let constants = checked_constants(state, parameters)?;
    attestation.hash_tree_root(parameters.phase0.preset)?;
    attesting_indices(state, attestation, &constants)
}

/// Convert committee participants; aggregate validation remains a separate mandatory step.
pub fn get_indexed_attestation(
    state: &BeaconState,
    attestation: &Attestation,
    parameters: &AltairParameters,
) -> Result<IndexedAttestation, ProcessingError> {
    Ok(IndexedAttestation {
        attesting_indices: List::from_vec(get_attesting_indices(state, attestation, parameters)?),
        data: attestation.data.clone(),
        signature: attestation.signature,
    })
}

/// Strict signed import alias for production consensus processing.
pub fn process_signed_block(
    state: &mut BeaconState,
    block: &SignedBeaconBlock,
    parameters: &AltairParameters,
) -> Result<(), ProcessingError> {
    state_transition(state, block, parameters)
}

fn attesting_indices(
    state: &BeaconState,
    attestation: &Attestation,
    constants: &Constants,
) -> Result<Vec<u64>, ProcessingError> {
    let committee = committee(state, attestation.data.slot, attestation.data.index, constants)?;
    if committee.len() != attestation.aggregation_bits.bits.len() {
        return Err(ProcessingError::InvalidState(
            "attestation bit length differs from its committee",
        ))
    }
    let mut output = Vec::new();
    for (position, &index) in committee.iter().enumerate() {
        if attestation
            .aggregation_bits
            .bits
            .get(position)
            .ok_or(ProcessingError::InvalidState("attestation bits are truncated"))?
        {
            output.push(index);
        }
    }
    output.sort_unstable();
    output.dedup();
    Ok(output)
}

pub(crate) fn attestation_kernel<const VERIFY_BLS: bool>(
    state: &mut BeaconState,
    operation: &Attestation,
    parameters: &AltairParameters,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    let data = &operation.data;
    let current = epoch(state, constants);
    let previous = current.saturating_sub(1);
    if (data.target.epoch != current && data.target.epoch != previous) ||
        data.target.epoch != data.slot / constants.slots_per_epoch
    {
        return Err(ProcessingError::InvalidState("attestation target epoch is inconsistent"))
    }
    if add(data.slot, 1, "minimum attestation inclusion slot")? > state.slot ||
        state.slot >
            add(data.slot, constants.slots_per_epoch, "maximum attestation inclusion slot")?
    {
        return Err(ProcessingError::InvalidState(
            "attestation inclusion slot is outside its window",
        ))
    }
    let indices = attesting_indices(state, operation, constants)?;
    let expected_source = if data.target.epoch == current {
        state.current_justified_checkpoint
    } else {
        state.previous_justified_checkpoint
    };
    if data.source != expected_source {
        return Err(ProcessingError::InvalidState("attestation source checkpoint does not match"))
    }
    let indexed = IndexedAttestation {
        attesting_indices: List::from_vec(indices),
        data: data.clone(),
        signature: operation.signature,
    };
    indexed_attestation_kernel::<VERIFY_BLS>(state, &indexed, parameters)?;
    let flags = participation_flags(
        state,
        data,
        sub(state.slot, data.slot, "attestation inclusion delay")?,
        constants,
    )?;
    let proposer = checked_index(proposer_index(state, constants)?, state.validators.values.len())?;
    let mut numerator = 0;
    for index in indexed.attesting_indices.values {
        let index = checked_index(index, state.validators.values.len())?;
        let base = base_reward(state, index, constants)?;
        let participation = if data.target.epoch == current {
            &mut state.current_epoch_participation.values
        } else {
            &mut state.previous_epoch_participation.values
        };
        for (flag_index, weight) in super::FLAG_WEIGHTS.into_iter().enumerate() {
            let flag = 1 << flag_index;
            if flags & flag != 0 && participation[index] & flag == 0 {
                participation[index] |= flag;
                numerator = add(
                    numerator,
                    mul(base, weight, "attestation proposer reward numerator")?,
                    "attestation proposer reward numerator",
                )?;
            }
        }
    }
    let denominator = (WEIGHT_DENOMINATOR - PROPOSER_WEIGHT) * WEIGHT_DENOMINATOR / PROPOSER_WEIGHT;
    state.balances.values[proposer] = add(
        state.balances.values[proposer],
        numerator / denominator,
        "attestation proposer reward",
    )?;
    Ok(())
}

pub(crate) fn deposit_kernel<const VERIFY_BLS: bool>(
    state: &mut BeaconState,
    operation: &Deposit,
    parameters: &AltairParameters,
) -> Result<(), ProcessingError> {
    if !is_valid_merkle_branch(
        operation.data.hash_tree_root(parameters.phase0.preset)?,
        &operation.proof.values,
        33,
        state.eth1_deposit_index,
        state.eth1_data.deposit_root,
    ) {
        return Err(ProcessingError::InvalidState("deposit Merkle proof is invalid"))
    }
    state.eth1_deposit_index = add(state.eth1_deposit_index, 1, "ordered deposit index")?;
    if let Some(index) = state
        .validators
        .values
        .iter()
        .position(|validator| validator.pubkey == operation.data.pubkey)
    {
        state.balances.values[index] =
            add(state.balances.values[index], operation.data.amount, "deposit balance top-up")?;
    } else {
        let message_root = merkleize(
            &[
                bytes_root(&operation.data.pubkey, 48, false)?,
                operation.data.withdrawal_credentials,
                uint64_root(operation.data.amount),
            ],
            3,
        )?;
        let domain =
            compute_domain(DOMAIN_DEPOSIT, parameters.phase0.genesis_fork_version, [0; 32]);
        let root = compute_signing_root(message_root, domain);
        // Invalid new-key proofs of possession are consumed and ignored as specified.
        if !VERIFY_BLS ||
            verify_signature(&operation.data.pubkey, &root, &operation.data.signature).is_ok()
        {
            if state.validators.values.len() as u64 >= 1_099_511_627_776 {
                return Err(ProcessingError::InvalidState("validator registry limit reached"))
            }
            let amount = operation.data.amount;
            state.validators.values.push(Validator {
                pubkey: operation.data.pubkey,
                withdrawal_credentials: operation.data.withdrawal_credentials,
                effective_balance: (amount - amount % EFFECTIVE_BALANCE_INCREMENT)
                    .min(MAX_EFFECTIVE_BALANCE),
                slashed: false,
                activation_eligibility_epoch: u64::MAX,
                activation_epoch: u64::MAX,
                exit_epoch: u64::MAX,
                withdrawable_epoch: u64::MAX,
            });
            state.balances.values.push(amount);
            state.previous_epoch_participation.values.push(0);
            state.current_epoch_participation.values.push(0);
            state.inactivity_scores.values.push(0);
        }
    }
    Ok(())
}

pub(crate) fn voluntary_exit_kernel<const VERIFY_BLS: bool>(
    state: &mut BeaconState,
    operation: &SignedVoluntaryExit,
    parameters: &AltairParameters,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    let exit = &operation.message;
    let index = checked_index(exit.validator_index, state.validators.values.len())?;
    let validator = &state.validators.values[index];
    let current = epoch(state, constants);
    if !is_active(validator, current) || validator.exit_epoch != u64::MAX {
        return Err(ProcessingError::InvalidState(
            "voluntary exit validator is not active and unexited",
        ))
    }
    if current < exit.epoch ||
        current <
            add(
                validator.activation_epoch,
                parameters.phase0.shard_committee_period,
                "minimum validator active period",
            )?
    {
        return Err(ProcessingError::InvalidState("voluntary exit is premature"))
    }
    let root = compute_signing_root(
        exit.hash_tree_root(parameters.phase0.preset)?,
        domain(state, DOMAIN_VOLUNTARY_EXIT, exit.epoch),
    );
    if VERIFY_BLS {
        verify_signature(&validator.pubkey, &root, &operation.signature)?;
    }
    exit_kernel(state, index, parameters, constants)
}

pub(crate) fn slashing_kernel(
    state: &mut BeaconState,
    index: usize,
    whistleblower: Option<u64>,
    parameters: &AltairParameters,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    exit_kernel(state, index, parameters, constants)?;
    let current = epoch(state, constants);
    let effective = state.validators.values[index].effective_balance;
    let withdrawable = add(current, constants.slashing_epochs, "slashing withdrawability epoch")?;
    state.validators.values[index].slashed = true;
    state.validators.values[index].withdrawable_epoch =
        state.validators.values[index].withdrawable_epoch.max(withdrawable);
    let position = position(current, constants.slashing_epochs)?;
    let bucket = state
        .slashings
        .values
        .get_mut(position)
        .ok_or(ProcessingError::InvalidState("slashings ring is truncated"))?;
    *bucket = add(*bucket, effective, "slashing bucket balance")?;
    let minimum_penalty_quotient = constants.minimum_slashing_penalty_quotient;
    state.balances.values[index] =
        state.balances.values[index].saturating_sub(effective / minimum_penalty_quotient);
    let proposer = checked_index(proposer_index(state, constants)?, state.validators.values.len())?;
    let whistleblower =
        checked_index(whistleblower.unwrap_or(proposer as u64), state.validators.values.len())?;
    let whistleblower_reward = effective / WHISTLEBLOWER_REWARD_QUOTIENT;
    let proposer_reward = mul(whistleblower_reward, PROPOSER_WEIGHT, "slashing proposer reward")? /
        WEIGHT_DENOMINATOR;
    state.balances.values[proposer] =
        add(state.balances.values[proposer], proposer_reward, "slashing proposer reward")?;
    state.balances.values[whistleblower] = add(
        state.balances.values[whistleblower],
        sub(whistleblower_reward, proposer_reward, "slashing whistleblower remainder")?,
        "slashing whistleblower reward",
    )?;
    Ok(())
}

/// Artificial published-fixture processing with cryptographic checks disabled.
///
/// This namespace never returns a validated runtime block or exposes a policy toggle.
/// Production APIs above always instantiate mandatory verification independently.
#[cfg(feature = "spec-tests")]
pub mod spec_only {
    use super::*;

    /// Artificial fixture aggregation: preserve nonempty/sorted/bounded indices and SSZ checks.
    #[cfg(feature = "spec-tests")]
    pub fn is_valid_indexed_attestation(
        state: &BeaconState,
        operation: &IndexedAttestation,
        parameters: &AltairParameters,
    ) -> Result<(), ProcessingError> {
        checked_constants(state, parameters)?;
        operation.hash_tree_root(parameters.phase0.preset)?;
        indexed_attestation_kernel::<false>(state, operation, parameters)
    }

    /// Replay an artificial signed-block fixture while enforcing the committed post-root.
    pub fn state_transition(
        state: &mut BeaconState,
        block: &SignedBeaconBlock,
        parameters: &AltairParameters,
    ) -> Result<(), ProcessingError> {
        block.hash_tree_root(parameters.phase0.preset)?;
        transact(state, parameters, |candidate, constants| {
            if candidate.slot != block.message.slot {
                super::super::slots_kernel(candidate, block.message.slot, parameters, constants)?;
            }
            verify_block_signature_kernel::<false>(candidate, block, parameters)?;
            block_kernel::<false>(candidate, &block.message, parameters, constants)?;
            if candidate.hash_tree_root(parameters.phase0.preset)? != block.message.state_root {
                return Err(ProcessingError::InvalidState("block post-state root mismatch"))
            }
            Ok(())
        })
    }

    /// Apply `process_randao` to an artificial fixture without BLS verification.
    pub fn process_randao(
        state: &mut BeaconState,
        operation: &BeaconBlockBody,
        parameters: &AltairParameters,
    ) -> Result<(), ProcessingError> {
        operation.hash_tree_root(parameters.phase0.preset)?;
        transact(state, parameters, |candidate, constants| {
            randao_kernel::<false>(candidate, operation, parameters, constants)
        })
    }

    /// Apply `process_proposer_slashing` to an artificial fixture without BLS verification.
    pub fn process_proposer_slashing(
        state: &mut BeaconState,
        operation: &ProposerSlashing,
        parameters: &AltairParameters,
    ) -> Result<(), ProcessingError> {
        operation.hash_tree_root(parameters.phase0.preset)?;
        transact(state, parameters, |candidate, constants| {
            proposer_slashing_kernel::<false>(candidate, operation, parameters, constants)
        })
    }

    /// Apply `process_attester_slashing` to an artificial fixture without BLS verification.
    pub fn process_attester_slashing(
        state: &mut BeaconState,
        operation: &AttesterSlashing,
        parameters: &AltairParameters,
    ) -> Result<(), ProcessingError> {
        operation.hash_tree_root(parameters.phase0.preset)?;
        transact(state, parameters, |candidate, constants| {
            attester_slashing_kernel::<false>(candidate, operation, parameters, constants)
        })
    }

    /// Apply `process_attestation` to an artificial fixture without BLS verification.
    pub fn process_attestation(
        state: &mut BeaconState,
        operation: &Attestation,
        parameters: &AltairParameters,
    ) -> Result<(), ProcessingError> {
        operation.hash_tree_root(parameters.phase0.preset)?;
        transact(state, parameters, |candidate, constants| {
            attestation_kernel::<false>(candidate, operation, parameters, constants)
        })
    }

    /// Apply `process_voluntary_exit` to an artificial fixture without BLS verification.
    pub fn process_voluntary_exit(
        state: &mut BeaconState,
        operation: &SignedVoluntaryExit,
        parameters: &AltairParameters,
    ) -> Result<(), ProcessingError> {
        operation.hash_tree_root(parameters.phase0.preset)?;
        transact(state, parameters, |candidate, constants| {
            voluntary_exit_kernel::<false>(candidate, operation, parameters, constants)
        })
    }

    /// Process an artificial fixture deposit with cryptographic checks disabled.
    pub fn process_deposit(
        state: &mut BeaconState,
        operation: &Deposit,
        parameters: &AltairParameters,
    ) -> Result<(), ProcessingError> {
        operation.hash_tree_root(parameters.phase0.preset)?;
        transact(state, parameters, |candidate, _| {
            deposit_kernel::<false>(candidate, operation, parameters)
        })
    }
    /// Apply an artificial sync aggregate fixture without BLS verification.
    pub fn process_sync_aggregate(
        state: &mut BeaconState,
        aggregate: &SyncAggregate,
        parameters: &AltairParameters,
    ) -> Result<(), ProcessingError> {
        aggregate.hash_tree_root(parameters.phase0.preset)?;
        transact(state, parameters, |candidate, constants| {
            sync_aggregate_kernel::<false>(candidate, aggregate, parameters, constants)
        })
    }
}

/// Verify a sync aggregate and apply every participant and proposer balance delta atomically.
pub fn process_sync_aggregate(
    state: &mut BeaconState,
    aggregate: &SyncAggregate,
    parameters: &AltairParameters,
) -> Result<(), ProcessingError> {
    aggregate.hash_tree_root(parameters.phase0.preset)?;
    transact(state, parameters, |candidate, constants| {
        sync_aggregate_kernel::<true>(candidate, aggregate, parameters, constants)
    })
}

pub(crate) fn sync_aggregate_kernel<const VERIFY_BLS: bool>(
    state: &mut BeaconState,
    aggregate: &SyncAggregate,
    _parameters: &AltairParameters,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    let keys = &state.current_sync_committee.pubkeys.values;
    let mut bits = Vec::with_capacity(keys.len());
    for index in 0..keys.len() {
        bits.push(
            aggregate
                .sync_committee_bits
                .bits
                .get(index)
                .ok_or(ProcessingError::InvalidState("sync committee bits are truncated"))?,
        );
    }
    let count = bits.iter().filter(|&&bit| bit).count();
    if VERIFY_BLS {
        let participants = if count == keys.len() {
            vec![state.current_sync_committee.aggregate_pubkey]
        } else if count > keys.len() / 2 {
            let removed = keys
                .iter()
                .zip(&bits)
                .filter_map(|(key, &bit)| (!bit).then_some(*key))
                .collect::<Vec<_>>();
            vec![subtract_aggregate_pubkeys(
                &state.current_sync_committee.aggregate_pubkey,
                &removed,
            )?]
        } else {
            keys.iter().zip(&bits).filter_map(|(key, &bit)| bit.then_some(*key)).collect::<Vec<_>>()
        };
        let previous_slot = state.slot.saturating_sub(1);
        let root = compute_signing_root(
            block_root_at_slot(state, previous_slot, constants)?,
            domain(state, super::DOMAIN_SYNC_COMMITTEE, previous_slot / constants.slots_per_epoch),
        );
        eth_fast_aggregate_verify(&participants, &root, &aggregate.sync_committee_signature)?;
    }
    let active_increments = super::active_balance(state, constants)? / EFFECTIVE_BALANCE_INCREMENT;
    let total_base = mul(
        base_reward_per_increment(state, constants)?,
        active_increments,
        "sync total base rewards",
    )?;
    let max_participant = mul(total_base, 2, "sync maximum participant rewards")? /
        WEIGHT_DENOMINATOR /
        constants.slots_per_epoch;
    let participant_reward = max_participant / (keys.len() as u64);
    let proposer_reward = mul(participant_reward, PROPOSER_WEIGHT, "sync proposer reward")? /
        (WEIGHT_DENOMINATOR - PROPOSER_WEIGHT);
    let proposer = checked_index(proposer_index(state, constants)?, state.validators.values.len())?;
    let mut registry = std::collections::BTreeMap::new();
    for (index, validator) in state.validators.values.iter().enumerate() {
        registry.entry(validator.pubkey).or_insert(index);
    }
    for (key, &bit) in keys.iter().zip(&bits) {
        let &index = registry.get(key).ok_or(ProcessingError::InvalidState(
            "sync committee public key is absent from validator registry",
        ))?;
        if bit {
            state.balances.values[index] =
                add(state.balances.values[index], participant_reward, "sync participant reward")?;
            state.balances.values[proposer] =
                add(state.balances.values[proposer], proposer_reward, "sync proposer reward")?;
        } else {
            state.balances.values[index] =
                state.balances.values[index].saturating_sub(participant_reward);
        }
    }
    Ok(())
}
