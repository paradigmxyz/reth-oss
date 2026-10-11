//! Strict Deneb import with execution verification inside the atomic transition.

use super::{
    checked_constants, kzg_commitment_to_versioned_hash, slots_kernel, transact,
    validate_execution_payload_consensus, with_altair, Constants, DenebParameters,
    ExecutionContext, ExecutionEngine, ImportError, ProcessingError,
};
use crate::{
    altair_processing, capella_processing,
    phase0_processing::sub,
    ssz::{
        altair,
        deneb::{
            Attestation, AttesterSlashing, BeaconBlock, BeaconBlockBody, BeaconState,
            IndexedAttestation, SignedBLSToExecutionChange, SignedBeaconBlock, SyncAggregate,
        },
        phase0::{Deposit, ProposerSlashing, SignedVoluntaryExit},
        List,
    },
};
use reth_embedded_beacon_crypto::{compute_domain, compute_signing_root, verify_signature};
use reth_embedded_beacon_primitives::BeaconBlockHeader;

/// Import a signed Deneb block with mandatory BLS, execution validation and post-root.
///
/// The engine callback is invoked for the exact candidate inside this transaction; no
/// external caller-supplied verdict can bypass it. Callers enforce trusted fork scheduling,
/// execution ancestry and slot-gap limits before invoking this kernel.
pub async fn state_transition<E: ExecutionEngine>(
    state: &mut BeaconState,
    block: &SignedBeaconBlock,
    parameters: &DenebParameters,
    engine: &E,
) -> Result<(), ImportError<E::Error>> {
    state_transition_kernel::<true, E>(state, block, parameters, engine).await
}

pub(crate) async fn state_transition_kernel<const VERIFY_BLS: bool, E: ExecutionEngine>(
    state: &mut BeaconState,
    block: &SignedBeaconBlock,
    parameters: &DenebParameters,
    engine: &E,
) -> Result<(), ImportError<E::Error>> {
    crate::common::require_signature_policy::<VERIFY_BLS>()?;
    block.hash_tree_root(parameters.preset()).map_err(ProcessingError::from)?;
    let constants = checked_constants(state, parameters)?;
    let mut candidate = state.clone();
    if candidate.slot != block.message.slot {
        slots_kernel(&mut candidate, block.message.slot, parameters, &constants)?;
    }
    verify_block_signature_kernel::<VERIFY_BLS>(&candidate, block, parameters)?;
    header_kernel(&mut candidate, &block.message, parameters, &constants)?;
    withdrawals_kernel(&mut candidate, &block.message.body.execution_payload, parameters)?;
    let header = validate_execution_payload_consensus(
        &candidate,
        &block.message.body.execution_payload,
        &block.message.body.blob_kzg_commitments.values,
        parameters,
    )?;
    let context = ExecutionContext {
        slot: candidate.slot,
        parent_beacon_block_root: block.message.parent_root,
        previous_execution_payload_hash: (!crate::common::execution_header_is_empty!(
            &candidate.latest_execution_payload_header,
            blob
        ))
        .then_some(candidate.latest_execution_payload_header.block_hash),
        expected_blob_versioned_hashes: block
            .message
            .body
            .blob_kzg_commitments
            .values
            .iter()
            .map(kzg_commitment_to_versioned_hash)
            .collect(),
    };
    if context.previous_execution_payload_hash.is_none() {
        engine
            .verify_terminal_block(&block.message.body.execution_payload, context.clone())
            .await
            .map_err(ImportError::Execution)?;
    }
    engine
        .verify_payload(&block.message.body.execution_payload, context)
        .await
        .map_err(ImportError::Execution)?;
    candidate.latest_execution_payload_header = header;
    with_altair(&mut candidate, |common| {
        altair_processing::block::randao_reveal_kernel::<VERIFY_BLS>(
            common,
            &block.message.body.randao_reveal,
            &constants,
        )?;
        altair_processing::block::eth1_vote_kernel(
            common,
            &block.message.body.eth1_data,
            &constants,
        )?;
        operations_common::<VERIFY_BLS>(common, &block.message.body, parameters, &constants)?;
        altair_processing::block::sync_aggregate_kernel::<VERIFY_BLS>(
            common,
            &sync_aggregate(&block.message.body.sync_aggregate),
            &parameters.capella.bellatrix.altair,
            &constants,
        )
    })?;
    checked_constants(&candidate, parameters)?;
    if candidate.hash_tree_root(parameters.preset()).map_err(ProcessingError::from)? !=
        block.message.state_root
    {
        return Err(ProcessingError::InvalidState("block post-state root mismatch").into())
    }
    *state = candidate;
    Ok(())
}

/// Strict signed import alias; execution verification is always mandatory when enabled.
pub async fn process_signed_block<E: ExecutionEngine>(
    state: &mut BeaconState,
    block: &SignedBeaconBlock,
    parameters: &DenebParameters,
    engine: &E,
) -> Result<(), ImportError<E::Error>> {
    state_transition(state, block, parameters, engine).await
}

/// Verify only the outer signature; this does not establish block validity.
pub fn verify_block_signature(
    state: &BeaconState,
    block: &SignedBeaconBlock,
    parameters: &DenebParameters,
) -> Result<(), ProcessingError> {
    checked_constants(state, parameters)?;
    block.hash_tree_root(parameters.preset())?;
    verify_block_signature_kernel::<true>(state, block, parameters)
}

fn verify_block_signature_kernel<const VERIFY_BLS: bool>(
    state: &BeaconState,
    block: &SignedBeaconBlock,
    parameters: &DenebParameters,
) -> Result<(), ProcessingError> {
    crate::common::require_signature_policy::<VERIFY_BLS>()?;
    let index = usize::try_from(block.message.proposer_index)
        .map_err(|_| ProcessingError::InvalidState("validator index does not fit platform"))?;
    let validator = state
        .validators
        .values
        .get(index)
        .ok_or(ProcessingError::InvalidState("validator index is out of range"))?;
    let signing_epoch = block.message.slot / parameters.slots_per_epoch();
    let version = if signing_epoch < state.fork.epoch {
        state.fork.previous_version
    } else {
        state.fork.current_version
    };
    let domain = compute_domain([0; 4], version, state.genesis_validators_root);
    let root = compute_signing_root(block.message.hash_tree_root(parameters.preset())?, domain);
    if VERIFY_BLS {
        verify_signature(&validator.pubkey, &root, &block.signature)?;
    }
    Ok(())
}

fn header_kernel(
    state: &mut BeaconState,
    block: &BeaconBlock,
    parameters: &DenebParameters,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    if block.slot != state.slot || block.slot <= state.latest_block_header.slot {
        return Err(ProcessingError::InvalidState("block slot does not match a newer state slot"))
    }
    let proposer =
        with_altair(state, |common| altair_processing::block::proposer_index(common, constants))?;
    if block.proposer_index != proposer {
        return Err(ProcessingError::InvalidState("block proposer is not the selected proposer"))
    }
    if block.parent_root != state.latest_block_header.hash_tree_root() {
        return Err(ProcessingError::InvalidState("block parent does not match latest header"))
    }
    let index = usize::try_from(proposer)
        .map_err(|_| ProcessingError::InvalidState("validator index does not fit platform"))?;
    if state
        .validators
        .values
        .get(index)
        .ok_or(ProcessingError::InvalidState("validator index is out of range"))?
        .slashed
    {
        return Err(ProcessingError::InvalidState("block proposer is slashed"))
    }
    state.latest_block_header = BeaconBlockHeader {
        slot: block.slot,
        proposer_index: block.proposer_index,
        parent_root: block.parent_root,
        state_root: [0; 32],
        body_root: block.body.hash_tree_root(parameters.preset())?,
    };
    Ok(())
}

/// Process only the full-body header; complete import uses the mandatory async transition.
pub fn process_block_header(
    state: &mut BeaconState,
    block: &BeaconBlock,
    parameters: &DenebParameters,
) -> Result<(), ProcessingError> {
    block.hash_tree_root(parameters.preset())?;
    transact(state, parameters, |candidate, constants| {
        header_kernel(candidate, block, parameters, constants)
    })
}

/// Check the real proposer RANDAO signature and update the current mix atomically.
pub fn process_randao(
    state: &mut BeaconState,
    body: &BeaconBlockBody,
    parameters: &DenebParameters,
) -> Result<(), ProcessingError> {
    body.hash_tree_root(parameters.preset())?;
    transact(state, parameters, |candidate, constants| {
        with_altair(candidate, |common| {
            altair_processing::block::randao_reveal_kernel::<true>(
                common,
                &body.randao_reveal,
                constants,
            )
        })
    })
}

/// Update execution-data votes using the specified voting-period majority.
pub fn process_eth1_data(
    state: &mut BeaconState,
    body: &BeaconBlockBody,
    parameters: &DenebParameters,
) -> Result<(), ProcessingError> {
    body.hash_tree_root(parameters.preset())?;
    transact(state, parameters, |candidate, constants| {
        with_altair(candidate, |common| {
            altair_processing::block::eth1_vote_kernel(common, &body.eth1_data, constants)
        })
    })
}

/// Process all consensus operations with mandatory signatures, without importing a block.
pub fn process_operations(
    state: &mut BeaconState,
    body: &BeaconBlockBody,
    parameters: &DenebParameters,
) -> Result<(), ProcessingError> {
    body.hash_tree_root(parameters.preset())?;
    transact(state, parameters, |candidate, constants| {
        with_altair(candidate, |common| {
            operations_common::<true>(common, body, parameters, constants)
        })
    })
}

fn operations_common<const VERIFY_BLS: bool>(
    state: &mut altair::BeaconState,
    body: &BeaconBlockBody,
    parameters: &DenebParameters,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    crate::common::require_signature_policy::<VERIFY_BLS>()?;
    let outstanding =
        sub(state.eth1_data.deposit_count, state.eth1_deposit_index, "outstanding deposits")?;
    if body.deposits.values.len() as u64 != outstanding.min(16) {
        return Err(ProcessingError::InvalidState(
            "block deposit count does not match outstanding deposits",
        ))
    }
    for operation in &body.proposer_slashings.values {
        altair_processing::block::proposer_slashing_kernel::<VERIFY_BLS>(
            state,
            operation,
            &parameters.capella.bellatrix.altair,
            constants,
        )?;
    }
    for operation in &body.attester_slashings.values {
        altair_processing::block::attester_slashing_kernel::<VERIFY_BLS>(
            state,
            &attester_slashing(operation),
            &parameters.capella.bellatrix.altair,
            constants,
        )?;
    }
    for operation in &body.attestations.values {
        attestation_common::<VERIFY_BLS>(state, operation, parameters, constants)?;
    }
    for operation in &body.deposits.values {
        altair_processing::block::deposit_kernel::<VERIFY_BLS>(
            state,
            operation,
            &parameters.capella.bellatrix.altair,
        )?;
    }
    for operation in &body.voluntary_exits.values {
        voluntary_exit_common::<VERIFY_BLS>(state, operation, parameters, constants)?;
    }
    for operation in &body.bls_to_execution_changes.values {
        capella_processing::bls_to_execution_change_kernel_with_policy::<VERIFY_BLS>(
            state,
            &bls_change(operation),
            &parameters.capella,
        )?;
    }
    Ok(())
}

fn indexed_attestation(operation: &IndexedAttestation) -> altair::IndexedAttestation {
    altair::IndexedAttestation {
        attesting_indices: operation.attesting_indices.clone(),
        data: operation.data.clone(),
        signature: operation.signature,
    }
}
fn attester_slashing(operation: &AttesterSlashing) -> altair::AttesterSlashing {
    altair::AttesterSlashing {
        attestation_1: indexed_attestation(&operation.attestation_1),
        attestation_2: indexed_attestation(&operation.attestation_2),
    }
}
fn sync_aggregate(operation: &SyncAggregate) -> altair::SyncAggregate {
    altair::SyncAggregate {
        sync_committee_bits: operation.sync_committee_bits.clone(),
        sync_committee_signature: operation.sync_committee_signature,
    }
}

/// Verify a strict sorted/nonempty indexed aggregate using the state-selected fork domain.
pub fn is_valid_indexed_attestation(
    state: &BeaconState,
    operation: &IndexedAttestation,
    parameters: &DenebParameters,
) -> Result<(), ProcessingError> {
    checked_constants(state, parameters)?;
    operation.hash_tree_root(parameters.preset())?;
    with_altair(&mut state.clone(), |common| {
        altair_processing::block::indexed_attestation_kernel::<true>(
            common,
            &indexed_attestation(operation),
            &parameters.capella.bellatrix.altair,
        )
    })
}

/// Select the deterministic balance-weighted proposer from the authenticated state.
pub fn get_beacon_proposer_index(
    state: &BeaconState,
    parameters: &DenebParameters,
) -> Result<u64, ProcessingError> {
    let constants = checked_constants(state, parameters)?;
    with_altair(&mut state.clone(), |common| {
        altair_processing::block::proposer_index(common, &constants)
    })
}

/// Derive sorted participants without verifying an attestation signature.
pub fn get_attesting_indices(
    state: &BeaconState,
    operation: &Attestation,
    parameters: &DenebParameters,
) -> Result<Vec<u64>, ProcessingError> {
    let constants = checked_constants(state, parameters)?;
    operation.hash_tree_root(parameters.preset())?;
    with_altair(&mut state.clone(), |common| {
        let committee = altair_processing::committee(
            common,
            operation.data.slot,
            operation.data.index,
            &constants,
        )?;
        if operation.aggregation_bits.bits.len() != committee.len() {
            return Err(ProcessingError::InvalidState(
                "attestation bit length differs from its committee",
            ))
        }
        let mut indices = Vec::new();
        for (position, index) in committee.into_iter().enumerate() {
            if operation
                .aggregation_bits
                .bits
                .get(position)
                .ok_or(ProcessingError::InvalidState("attestation bits are truncated"))?
            {
                indices.push(index);
            }
        }
        indices.sort_unstable();
        indices.dedup();
        Ok(indices)
    })
}

/// Convert committee bits into indexed form without asserting signature validity.
pub fn get_indexed_attestation(
    state: &BeaconState,
    operation: &Attestation,
    parameters: &DenebParameters,
) -> Result<IndexedAttestation, ProcessingError> {
    Ok(IndexedAttestation {
        attesting_indices: List::from_vec(get_attesting_indices(state, operation, parameters)?),
        data: operation.data.clone(),
        signature: operation.signature,
    })
}

/// Apply `process_proposer_slashing` with mandatory signatures and exact Deneb parameters.
pub fn process_proposer_slashing(
    state: &mut BeaconState,
    operation: &ProposerSlashing,
    parameters: &DenebParameters,
) -> Result<(), ProcessingError> {
    operation.hash_tree_root(parameters.preset())?;
    transact(state, parameters, |candidate, constants| {
        with_altair(candidate, |common| {
            altair_processing::block::proposer_slashing_kernel::<true>(
                common,
                operation,
                &parameters.capella.bellatrix.altair,
                constants,
            )
        })
    })
}

/// Apply `process_attester_slashing` with mandatory signatures and exact Deneb parameters.
pub fn process_attester_slashing(
    state: &mut BeaconState,
    operation: &AttesterSlashing,
    parameters: &DenebParameters,
) -> Result<(), ProcessingError> {
    operation.hash_tree_root(parameters.preset())?;
    transact(state, parameters, |candidate, constants| {
        with_altair(candidate, |common| {
            altair_processing::block::attester_slashing_kernel::<true>(
                common,
                &attester_slashing(operation),
                &parameters.capella.bellatrix.altair,
                constants,
            )
        })
    })
}

/// Apply `process_attestation` with mandatory signatures and exact Deneb parameters.
pub fn process_attestation(
    state: &mut BeaconState,
    operation: &Attestation,
    parameters: &DenebParameters,
) -> Result<(), ProcessingError> {
    operation.hash_tree_root(parameters.preset())?;
    transact(state, parameters, |candidate, constants| {
        with_altair(candidate, |common| {
            attestation_common::<true>(common, operation, parameters, constants)
        })
    })
}

/// Apply `process_deposit` with mandatory signatures and exact Deneb parameters.
pub fn process_deposit(
    state: &mut BeaconState,
    operation: &Deposit,
    parameters: &DenebParameters,
) -> Result<(), ProcessingError> {
    operation.hash_tree_root(parameters.preset())?;
    transact(state, parameters, |candidate, _constants| {
        with_altair(candidate, |common| {
            altair_processing::block::deposit_kernel::<true>(
                common,
                operation,
                &parameters.capella.bellatrix.altair,
            )
        })
    })
}

/// Apply `process_voluntary_exit` with mandatory signatures and exact Deneb parameters.
pub fn process_voluntary_exit(
    state: &mut BeaconState,
    operation: &SignedVoluntaryExit,
    parameters: &DenebParameters,
) -> Result<(), ProcessingError> {
    operation.hash_tree_root(parameters.preset())?;
    transact(state, parameters, |candidate, constants| {
        with_altair(candidate, |common| {
            voluntary_exit_common::<true>(common, operation, parameters, constants)
        })
    })
}

/// Apply `process_sync_aggregate` with mandatory signatures and exact Deneb parameters.
pub fn process_sync_aggregate(
    state: &mut BeaconState,
    operation: &SyncAggregate,
    parameters: &DenebParameters,
) -> Result<(), ProcessingError> {
    operation.hash_tree_root(parameters.preset())?;
    transact(state, parameters, |candidate, constants| {
        with_altair(candidate, |common| {
            altair_processing::block::sync_aggregate_kernel::<true>(
                common,
                &sync_aggregate(operation),
                &parameters.capella.bellatrix.altair,
                constants,
            )
        })
    })
}

const fn bls_change(
    operation: &SignedBLSToExecutionChange,
) -> crate::ssz::capella::SignedBLSToExecutionChange {
    crate::ssz::capella::SignedBLSToExecutionChange {
        message: crate::ssz::capella::BLSToExecutionChange {
            validator_index: operation.message.validator_index,
            from_bls_pubkey: operation.message.from_bls_pubkey,
            to_execution_address: operation.message.to_execution_address,
        },
        signature: operation.signature,
    }
}

fn withdrawals_kernel(
    state: &mut BeaconState,
    payload: &crate::ssz::deneb::ExecutionPayload,
    parameters: &DenebParameters,
) -> Result<(), ProcessingError> {
    let withdrawals = payload
        .withdrawals
        .values
        .iter()
        .map(|withdrawal| crate::ssz::capella::Withdrawal {
            index: withdrawal.index,
            validator_index: withdrawal.validator_index,
            address: withdrawal.address,
            amount: withdrawal.amount,
        })
        .collect::<Vec<_>>();
    let next = state.next_withdrawal_index;
    let validator = state.next_withdrawal_validator_index;
    let (next, validator) = with_altair(state, |common| {
        capella_processing::withdrawals_kernel(
            common,
            &withdrawals,
            next,
            validator,
            &parameters.capella,
        )
    })?;
    state.next_withdrawal_index = next;
    state.next_withdrawal_validator_index = validator;
    Ok(())
}

/// Verify every expected withdrawal and update sweep counters atomically.
pub fn process_withdrawals(
    state: &mut BeaconState,
    payload: &crate::ssz::deneb::ExecutionPayload,
    parameters: &DenebParameters,
) -> Result<(), ProcessingError> {
    payload.hash_tree_root(parameters.preset())?;
    transact(state, parameters, |candidate, _| withdrawals_kernel(candidate, payload, parameters))
}

/// Verify the fork-agnostic address-change signature before updating credentials.
pub fn process_bls_to_execution_change(
    state: &mut BeaconState,
    operation: &SignedBLSToExecutionChange,
    parameters: &DenebParameters,
) -> Result<(), ProcessingError> {
    operation.hash_tree_root(parameters.preset())?;
    transact(state, parameters, |candidate, _| {
        with_altair(candidate, |common| {
            capella_processing::bls_to_execution_change_kernel(
                common,
                &bls_change(operation),
                &parameters.capella,
            )
        })
    })
}

fn voluntary_exit_common<const VERIFY_BLS: bool>(
    state: &mut crate::ssz::altair::BeaconState,
    operation: &SignedVoluntaryExit,
    parameters: &DenebParameters,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    crate::common::require_signature_policy::<VERIFY_BLS>()?;
    let index = usize::try_from(operation.message.validator_index)
        .map_err(|_| ProcessingError::InvalidState("validator index does not fit platform"))?;
    let validator = state
        .validators
        .values
        .get(index)
        .ok_or(ProcessingError::InvalidState("validator index is out of range"))?;
    let current = state.slot / constants.slots_per_epoch;
    if !crate::phase0_processing::is_active(validator, current) || validator.exit_epoch != u64::MAX
    {
        return Err(ProcessingError::InvalidState(
            "voluntary exit validator is not active and unexited",
        ))
    }
    if current < operation.message.epoch ||
        current <
            crate::phase0_processing::add(
                validator.activation_epoch,
                parameters.capella.bellatrix.altair.phase0.shard_committee_period,
                "minimum validator active period",
            )?
    {
        return Err(ProcessingError::InvalidState("voluntary exit is premature"))
    }
    let domain = compute_domain(
        [4, 0, 0, 0],
        parameters.capella.fork_version,
        state.genesis_validators_root,
    );
    let root = compute_signing_root(operation.message.hash_tree_root(parameters.preset())?, domain);
    if VERIFY_BLS {
        verify_signature(&validator.pubkey, &root, &operation.signature)?;
    }
    altair_processing::exit_kernel(state, index, &parameters.capella.bellatrix.altair, constants)
}

fn participation_flags(
    state: &crate::ssz::altair::BeaconState,
    data: &crate::ssz::phase0::AttestationData,
    delay: u64,
    constants: &Constants,
) -> Result<u8, ProcessingError> {
    let current = state.slot / constants.slots_per_epoch;
    let justified = if data.target.epoch == current {
        state.current_justified_checkpoint
    } else {
        state.previous_justified_checkpoint
    };
    if data.source != justified {
        return Err(ProcessingError::InvalidState("attestation source checkpoint does not match"))
    }
    let target = data.target.root ==
        altair_processing::block_root_at_epoch(state, data.target.epoch, constants)?;
    let head = target &&
        data.beacon_block_root ==
            altair_processing::block_root_at_slot(state, data.slot, constants)?;
    let mut flags = 0;
    if delay <= crate::integer_squareroot(constants.slots_per_epoch) {
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

fn attestation_common<const VERIFY_BLS: bool>(
    state: &mut crate::ssz::altair::BeaconState,
    operation: &Attestation,
    parameters: &DenebParameters,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    crate::common::require_signature_policy::<VERIFY_BLS>()?;
    let data = &operation.data;
    let current = state.slot / constants.slots_per_epoch;
    let previous = current.saturating_sub(1);
    if (data.target.epoch != current && data.target.epoch != previous) ||
        data.target.epoch != data.slot / constants.slots_per_epoch
    {
        return Err(ProcessingError::InvalidState("attestation target epoch is inconsistent"))
    }
    if crate::phase0_processing::add(data.slot, 1, "minimum attestation inclusion slot")? >
        state.slot
    {
        return Err(ProcessingError::InvalidState("attestation inclusion slot is premature"))
    }
    let committee = altair_processing::committee(state, data.slot, data.index, constants)?;
    if operation.aggregation_bits.bits.len() != committee.len() {
        return Err(ProcessingError::InvalidState(
            "attestation bit length differs from its committee",
        ))
    }
    let delay = sub(state.slot, data.slot, "attestation inclusion delay")?;
    let flags = participation_flags(state, data, delay, constants)?;
    let mut indices = Vec::new();
    for (position, index) in committee.into_iter().enumerate() {
        if operation
            .aggregation_bits
            .bits
            .get(position)
            .ok_or(ProcessingError::InvalidState("attestation bits are truncated"))?
        {
            indices.push(index);
        }
    }
    indices.sort_unstable();
    indices.dedup();
    let indexed = crate::ssz::altair::IndexedAttestation {
        attesting_indices: List::from_vec(indices),
        data: data.clone(),
        signature: operation.signature,
    };
    altair_processing::block::indexed_attestation_kernel::<VERIFY_BLS>(
        state,
        &indexed,
        &parameters.capella.bellatrix.altair,
    )?;
    let proposer = altair_processing::block::proposer_index(state, constants)? as usize;
    let mut numerator = 0;
    for index in indexed.attesting_indices.values {
        let index = usize::try_from(index)
            .map_err(|_| ProcessingError::InvalidState("validator index does not fit platform"))?;
        let base = altair_processing::base_reward(state, index, constants)?;
        let participation = if data.target.epoch == current {
            &mut state.current_epoch_participation.values
        } else {
            &mut state.previous_epoch_participation.values
        };
        for (flag, weight) in [14, 26, 14].into_iter().enumerate() {
            let bit = 1 << flag;
            if flags & bit != 0 && participation[index] & bit == 0 {
                participation[index] |= bit;
                numerator = crate::phase0_processing::add(
                    numerator,
                    crate::phase0_processing::mul(base, weight, "attestation proposer numerator")?,
                    "attestation proposer numerator",
                )?;
            }
        }
    }
    state.balances.values[proposer] = crate::phase0_processing::add(
        state.balances.values[proposer],
        numerator / 448,
        "attestation proposer reward",
    )?;
    Ok(())
}

/// Validate the exact execution component with mandatory V3 verification before publication.
///
/// Withdrawals are a distinct component and are processed before this in signed import.
pub async fn process_execution_payload<E: ExecutionEngine>(
    state: &mut BeaconState,
    body: &BeaconBlockBody,
    parameters: &DenebParameters,
    engine: &E,
) -> Result<(), ImportError<E::Error>> {
    body.hash_tree_root(parameters.preset()).map_err(ProcessingError::from)?;
    checked_constants(state, parameters)?;
    let mut candidate = state.clone();
    let header = validate_execution_payload_consensus(
        &candidate,
        &body.execution_payload,
        &body.blob_kzg_commitments.values,
        parameters,
    )?;
    let context = ExecutionContext {
        slot: candidate.slot,
        parent_beacon_block_root: candidate.latest_block_header.parent_root,
        previous_execution_payload_hash: (!crate::common::execution_header_is_empty!(
            &candidate.latest_execution_payload_header,
            blob
        ))
        .then_some(candidate.latest_execution_payload_header.block_hash),
        expected_blob_versioned_hashes: body
            .blob_kzg_commitments
            .values
            .iter()
            .map(kzg_commitment_to_versioned_hash)
            .collect(),
    };
    if context.previous_execution_payload_hash.is_none() {
        engine
            .verify_terminal_block(&body.execution_payload, context.clone())
            .await
            .map_err(ImportError::Execution)?;
    }
    engine
        .verify_payload(&body.execution_payload, context)
        .await
        .map_err(ImportError::Execution)?;
    candidate.latest_execution_payload_header = header;
    checked_constants(&candidate, parameters)?;
    *state = candidate;
    Ok(())
}

/// Explicit artificial attestation fixtures; production processing remains mandatory BLS.
#[cfg(feature = "spec-tests")]
pub mod spec_only {
    use super::*;

    /// Artificial fixture aggregation: preserve nonempty/sorted/bounded indices and SSZ checks.
    #[cfg(feature = "spec-tests")]
    pub fn is_valid_indexed_attestation(
        state: &BeaconState,
        operation: &IndexedAttestation,
        parameters: &DenebParameters,
    ) -> Result<(), ProcessingError> {
        checked_constants(state, parameters)?;
        operation.hash_tree_root(parameters.preset())?;
        with_altair(&mut state.clone(), |common| {
            altair_processing::block::indexed_attestation_kernel::<false>(
                common,
                &indexed_attestation(operation),
                &parameters.capella.bellatrix.altair,
            )
        })
    }

    /// Artificial published signed-block fixtures only; absent from runtime feature graphs.
    /// Every non-BLS rule, Engine callback and complete post root remains enforced.
    #[cfg(feature = "spec-tests")]
    pub async fn state_transition<E: ExecutionEngine>(
        state: &mut BeaconState,
        block: &SignedBeaconBlock,
        parameters: &DenebParameters,
        engine: &E,
    ) -> Result<(), ImportError<E::Error>> {
        state_transition_kernel::<false, E>(state, block, parameters, engine).await
    }

    /// Process a published artificial BLS-disabled attestation with every structural rule.
    pub fn process_attestation(
        state: &mut BeaconState,
        operation: &Attestation,
        parameters: &DenebParameters,
    ) -> Result<(), ProcessingError> {
        operation.hash_tree_root(parameters.preset())?;
        transact(state, parameters, |candidate, constants| {
            with_altair(candidate, |common| {
                attestation_common::<false>(common, operation, parameters, constants)
            })
        })
    }
}
