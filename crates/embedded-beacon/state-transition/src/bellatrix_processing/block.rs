//! Strict Bellatrix import with execution verification inside the atomic transition.

use super::{
    checked_constants, is_execution_enabled, is_merge_transition_complete, slots_kernel, transact,
    validate_execution_payload_consensus, with_altair, BellatrixParameters, Constants,
    ExecutionContext, ExecutionEngine, ImportError, ProcessingError,
};
use crate::{
    altair_processing,
    phase0_processing::sub,
    ssz::{
        altair,
        bellatrix::{
            Attestation, AttesterSlashing, BeaconBlock, BeaconBlockBody, BeaconState,
            IndexedAttestation, SignedBeaconBlock, SyncAggregate,
        },
        phase0::{Deposit, ProposerSlashing, SignedVoluntaryExit},
        List,
    },
};
use reth_embedded_beacon_crypto::{compute_domain, compute_signing_root, verify_signature};
use reth_embedded_beacon_primitives::BeaconBlockHeader;

/// Import a signed Bellatrix block with mandatory BLS, execution validation and post-root.
///
/// The engine callback is invoked for the exact candidate inside this transaction; no
/// external caller-supplied verdict can bypass it. Callers enforce trusted fork scheduling,
/// terminal-block fork-choice validation and slot-gap limits before invoking this kernel.
pub async fn state_transition<E: ExecutionEngine>(
    state: &mut BeaconState,
    block: &SignedBeaconBlock,
    parameters: &BellatrixParameters,
    engine: &E,
) -> Result<(), ImportError<E::Error>> {
    state_transition_kernel::<true, E>(state, block, parameters, engine).await
}

pub(crate) async fn state_transition_kernel<const VERIFY_BLS: bool, E: ExecutionEngine>(
    state: &mut BeaconState,
    block: &SignedBeaconBlock,
    parameters: &BellatrixParameters,
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
    if is_execution_enabled(&candidate, &block.message.body.execution_payload) {
        let header = validate_execution_payload_consensus(
            &candidate,
            &block.message.body.execution_payload,
            parameters,
        )?;
        let context = ExecutionContext {
            slot: candidate.slot,
            parent_beacon_block_root: block.message.parent_root,
            previous_execution_payload_hash: is_merge_transition_complete(&candidate)
                .then_some(candidate.latest_execution_payload_header.block_hash),
        };
        if context.previous_execution_payload_hash.is_none() {
            engine
                .verify_terminal_block(&block.message.body.execution_payload, context)
                .await
                .map_err(ImportError::Execution)?;
        }
        engine
            .verify_payload(&block.message.body.execution_payload, context)
            .await
            .map_err(ImportError::Execution)?;
        candidate.latest_execution_payload_header = header;
    }
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
            &parameters.altair,
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
    parameters: &BellatrixParameters,
    engine: &E,
) -> Result<(), ImportError<E::Error>> {
    state_transition(state, block, parameters, engine).await
}

/// Verify only the outer signature; this does not establish block validity.
pub fn verify_block_signature(
    state: &BeaconState,
    block: &SignedBeaconBlock,
    parameters: &BellatrixParameters,
) -> Result<(), ProcessingError> {
    checked_constants(state, parameters)?;
    block.hash_tree_root(parameters.preset())?;
    verify_block_signature_kernel::<true>(state, block, parameters)
}

fn verify_block_signature_kernel<const VERIFY_BLS: bool>(
    state: &BeaconState,
    block: &SignedBeaconBlock,
    parameters: &BellatrixParameters,
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
    parameters: &BellatrixParameters,
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
    parameters: &BellatrixParameters,
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
    parameters: &BellatrixParameters,
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
    parameters: &BellatrixParameters,
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
    parameters: &BellatrixParameters,
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
    parameters: &BellatrixParameters,
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
            &parameters.altair,
            constants,
        )?;
    }
    for operation in &body.attester_slashings.values {
        altair_processing::block::attester_slashing_kernel::<VERIFY_BLS>(
            state,
            &attester_slashing(operation),
            &parameters.altair,
            constants,
        )?;
    }
    for operation in &body.attestations.values {
        altair_processing::block::attestation_kernel::<VERIFY_BLS>(
            state,
            &attestation(operation),
            &parameters.altair,
            constants,
        )?;
    }
    for operation in &body.deposits.values {
        altair_processing::block::deposit_kernel::<VERIFY_BLS>(
            state,
            operation,
            &parameters.altair,
        )?;
    }
    for operation in &body.voluntary_exits.values {
        altair_processing::block::voluntary_exit_kernel::<VERIFY_BLS>(
            state,
            operation,
            &parameters.altair,
            constants,
        )?;
    }
    Ok(())
}

fn attestation(operation: &Attestation) -> altair::Attestation {
    altair::Attestation {
        aggregation_bits: operation.aggregation_bits.clone(),
        data: operation.data.clone(),
        signature: operation.signature,
    }
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
    parameters: &BellatrixParameters,
) -> Result<(), ProcessingError> {
    checked_constants(state, parameters)?;
    operation.hash_tree_root(parameters.preset())?;
    with_altair(&mut state.clone(), |common| {
        altair_processing::block::indexed_attestation_kernel::<true>(
            common,
            &indexed_attestation(operation),
            &parameters.altair,
        )
    })
}

/// Select the deterministic balance-weighted proposer from the authenticated state.
pub fn get_beacon_proposer_index(
    state: &BeaconState,
    parameters: &BellatrixParameters,
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
    parameters: &BellatrixParameters,
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
    parameters: &BellatrixParameters,
) -> Result<IndexedAttestation, ProcessingError> {
    Ok(IndexedAttestation {
        attesting_indices: List::from_vec(get_attesting_indices(state, operation, parameters)?),
        data: operation.data.clone(),
        signature: operation.signature,
    })
}

/// Apply `process_proposer_slashing` with mandatory signatures and exact Bellatrix parameters.
pub fn process_proposer_slashing(
    state: &mut BeaconState,
    operation: &ProposerSlashing,
    parameters: &BellatrixParameters,
) -> Result<(), ProcessingError> {
    operation.hash_tree_root(parameters.preset())?;
    transact(state, parameters, |candidate, constants| {
        with_altair(candidate, |common| {
            altair_processing::block::proposer_slashing_kernel::<true>(
                common,
                operation,
                &parameters.altair,
                constants,
            )
        })
    })
}

/// Apply `process_attester_slashing` with mandatory signatures and exact Bellatrix parameters.
pub fn process_attester_slashing(
    state: &mut BeaconState,
    operation: &AttesterSlashing,
    parameters: &BellatrixParameters,
) -> Result<(), ProcessingError> {
    operation.hash_tree_root(parameters.preset())?;
    transact(state, parameters, |candidate, constants| {
        with_altair(candidate, |common| {
            altair_processing::block::attester_slashing_kernel::<true>(
                common,
                &attester_slashing(operation),
                &parameters.altair,
                constants,
            )
        })
    })
}

/// Apply `process_attestation` with mandatory signatures and exact Bellatrix parameters.
pub fn process_attestation(
    state: &mut BeaconState,
    operation: &Attestation,
    parameters: &BellatrixParameters,
) -> Result<(), ProcessingError> {
    operation.hash_tree_root(parameters.preset())?;
    transact(state, parameters, |candidate, constants| {
        with_altair(candidate, |common| {
            altair_processing::block::attestation_kernel::<true>(
                common,
                &attestation(operation),
                &parameters.altair,
                constants,
            )
        })
    })
}

/// Apply `process_deposit` with mandatory signatures and exact Bellatrix parameters.
pub fn process_deposit(
    state: &mut BeaconState,
    operation: &Deposit,
    parameters: &BellatrixParameters,
) -> Result<(), ProcessingError> {
    operation.hash_tree_root(parameters.preset())?;
    transact(state, parameters, |candidate, _constants| {
        with_altair(candidate, |common| {
            altair_processing::block::deposit_kernel::<true>(common, operation, &parameters.altair)
        })
    })
}

/// Apply `process_voluntary_exit` with mandatory signatures and exact Bellatrix parameters.
pub fn process_voluntary_exit(
    state: &mut BeaconState,
    operation: &SignedVoluntaryExit,
    parameters: &BellatrixParameters,
) -> Result<(), ProcessingError> {
    operation.hash_tree_root(parameters.preset())?;
    transact(state, parameters, |candidate, constants| {
        with_altair(candidate, |common| {
            altair_processing::block::voluntary_exit_kernel::<true>(
                common,
                operation,
                &parameters.altair,
                constants,
            )
        })
    })
}

/// Apply `process_sync_aggregate` with mandatory signatures and exact Bellatrix parameters.
pub fn process_sync_aggregate(
    state: &mut BeaconState,
    operation: &SyncAggregate,
    parameters: &BellatrixParameters,
) -> Result<(), ProcessingError> {
    operation.hash_tree_root(parameters.preset())?;
    transact(state, parameters, |candidate, constants| {
        with_altair(candidate, |common| {
            altair_processing::block::sync_aggregate_kernel::<true>(
                common,
                &sync_aggregate(operation),
                &parameters.altair,
                constants,
            )
        })
    })
}

/// Process the exact execution component atomically with mandatory terminal/engine checks.
///
/// This standalone component does not establish validity of the enclosing signed block.
pub async fn process_execution_payload<E: ExecutionEngine>(
    state: &mut BeaconState,
    body: &BeaconBlockBody,
    parameters: &BellatrixParameters,
    engine: &E,
) -> Result<(), ImportError<E::Error>> {
    body.hash_tree_root(parameters.preset()).map_err(ProcessingError::from)?;
    checked_constants(state, parameters)?;
    let mut candidate = state.clone();
    let header =
        validate_execution_payload_consensus(&candidate, &body.execution_payload, parameters)?;
    let context = ExecutionContext {
        slot: candidate.slot,
        parent_beacon_block_root: candidate.latest_block_header.parent_root,
        previous_execution_payload_hash: is_merge_transition_complete(&candidate)
            .then_some(candidate.latest_execution_payload_header.block_hash),
    };
    if context.previous_execution_payload_hash.is_none() {
        engine
            .verify_terminal_block(&body.execution_payload, context)
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

/// Explicit artificial fixture processing; production functions verify signatures independently.
#[cfg(feature = "spec-tests")]
pub mod spec_only {
    use super::*;

    /// Artificial fixture aggregation: preserve nonempty/sorted/bounded indices and SSZ checks.
    #[cfg(feature = "spec-tests")]
    pub fn is_valid_indexed_attestation(
        state: &BeaconState,
        operation: &IndexedAttestation,
        parameters: &BellatrixParameters,
    ) -> Result<(), ProcessingError> {
        checked_constants(state, parameters)?;
        operation.hash_tree_root(parameters.preset())?;
        with_altair(&mut state.clone(), |common| {
            altair_processing::block::indexed_attestation_kernel::<false>(
                common,
                &indexed_attestation(operation),
                &parameters.altair,
            )
        })
    }

    /// Artificial published signed-block fixtures only; never enabled by runtime consumers.
    /// Engine callbacks, non-BLS consensus checks and post roots remain mandatory.
    #[cfg(feature = "spec-tests")]
    pub async fn state_transition<E: ExecutionEngine>(
        state: &mut BeaconState,
        block: &SignedBeaconBlock,
        parameters: &BellatrixParameters,
        engine: &E,
    ) -> Result<(), ImportError<E::Error>> {
        state_transition_kernel::<false, E>(state, block, parameters, engine).await
    }

    /// Apply a published artificial attestation fixture without BLS signature verification.
    /// Structural, committee, source, target, timing and incentive rules remain enforced.
    pub fn process_attestation(
        state: &mut BeaconState,
        operation: &Attestation,
        parameters: &BellatrixParameters,
    ) -> Result<(), ProcessingError> {
        operation.hash_tree_root(parameters.preset())?;
        transact(state, parameters, |candidate, constants| {
            with_altair(candidate, |common| {
                altair_processing::block::attestation_kernel::<false>(
                    common,
                    &attestation(operation),
                    &parameters.altair,
                    constants,
                )
            })
        })
    }
}
