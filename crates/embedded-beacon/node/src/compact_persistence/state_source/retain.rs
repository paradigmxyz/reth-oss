//! Durable original data emitted only by checked compact consensus admission.

use super::*;

pub(super) fn retain_block(
    repository: &AdmittedStateRepository,
    admitted: AdmittedState<'_>,
) -> Result<(), PersistenceError> {
    let binding = admitted.binding();
    let state = admitted.state();
    if binding.identity().profile_digest != repository.network.profile_digest ||
        binding.identity().consensus_config_digest != repository.network.consensus_digest ||
        binding.checkpoint_epoch().is_some() ||
        binding.state_root() != binding.origin_state_root() ||
        state.slot() != binding.slot() ||
        state.fork() != binding.fork() ||
        state.preset() != repository.network.preset() ||
        state.genesis_time() != repository.network.config.genesis_time ||
        state.genesis_validators_root() != repository.network.config.genesis_validators_root
    {
        return Err(PersistenceError::Identity)
    }
    let signed = admitted.signed_block();
    let bytes = signed.to_ssz()?;
    repository.check_bytes(&bytes)?;
    let (header, execution) = match (state, signed) {
        (StateData::Classic(state), SignedBlockData::Classic(signed)) => {
            if state.schema_fork() != signed.schema_fork() {
                return Err(PersistenceError::Linkage)
            }
            verify_original_signature(state, signed, repository.network.config.slots_per_epoch)?;
            let actual = execution(&signed.message());
            let actual_hash =
                actual.map(|association| association.block_hash).filter(|hash| *hash != [0; 32]);
            let verified_hash = admitted.verified_execution_hash();
            let execution_optimistic = admitted.execution_optimistic();
            if (execution_optimistic && (actual_hash.is_none() || verified_hash.is_some())) ||
                (!execution_optimistic && actual_hash != verified_hash)
            {
                return Err(PersistenceError::Linkage)
            }
            // The sealed optimistic admission preserves the original execution association for
            // replay. Persisted associations never supply an execution-validity receipt.
            (signed.header()?, actual)
        }
        (StateData::Gloas(state, preset), SignedBlockData::Gloas(signed, signed_preset)) => {
            if preset != signed_preset ||
                admitted.verified_execution_hash().is_some() ||
                admitted.execution_optimistic()
            {
                return Err(PersistenceError::Linkage)
            }
            let header = signed.message.header(preset)?;
            let validator = usize::try_from(header.proposer_index)
                .ok()
                .and_then(|index| state.validators.values.get(index))
                .ok_or(PersistenceError::Linkage)?;
            let version =
                if header.slot / repository.network.config.slots_per_epoch < state.fork.epoch {
                    state.fork.previous_version
                } else {
                    state.fork.current_version
                };
            verify_signature(
                &validator.pubkey,
                &compute_signing_root(
                    signed.message.hash_tree_root(preset)?,
                    compute_domain([0; 4], version, state.genesis_validators_root),
                ),
                &signed.signature,
            )?;
            (header, None)
        }
        _ => return Err(PersistenceError::Linkage),
    };
    let root = signed.message_root()?;
    if root != binding.origin_block_root() ||
        header.state_root != binding.state_root() ||
        header.slot != binding.slot()
    {
        return Err(PersistenceError::Linkage)
    }
    let mut inner = repository.lock()?;
    let anchor = inner.anchor.as_ref().ok_or(PersistenceError::MissingAnchor)?;
    if root != anchor.checkpoint.root {
        let parent = inner
            .records
            .get(&header.parent_root)
            .filter(|record| record.slot < header.slot)
            .ok_or(PersistenceError::MissingPredecessor)?;
        repository.check_saved_record(parent)?;
    }
    repository.check_capacity(&inner, root)?;
    let record = Admission {
        block_root: root,
        parent_root: header.parent_root,
        state_root: header.state_root,
        slot: header.slot,
        fork: state.fork(),
        original: ObjectId::new(ObjectKind::BeaconBlock, Encoding::Ssz, &bytes),
        format: CheckpointBlockFormat::Signed,
        execution,
        envelope: None,
    };
    if let Some(saved) = inner.records.get(&root) {
        let mut original = saved.clone();
        original.envelope = None;
        if saved.envelope.is_some() {
            original.execution = execution;
        }
        if original != record {
            return Err(PersistenceError::Linkage)
        }
        repository.check_saved_record(saved)?;
        return Ok(())
    }
    // The private AdmittedState receipt ties this exact borrowed state to its checked binding.
    // Persistence independently checks the complete root once before durable publication; do
    // not hash the same full post-state again before that identical check.
    if let Some(owner) = admitted.shared_classic_state() {
        repository.compact.persist_cached_owned_state(owner, binding.state_root())?;
    } else {
        persist_data(repository, state, binding.state_root())?;
    }
    repository.shared.put(
        ObjectKind::BeaconBlock,
        Encoding::Ssz,
        &bytes,
        Owner::Consensus(root),
    )?;
    repository.commit_record(&mut inner, record)?;
    Ok(())
}

pub(super) fn retain_envelope(
    repository: &AdmittedStateRepository,
    admitted: VerifiedEnvelope<'_>,
) -> Result<(), PersistenceError> {
    let binding = admitted.binding();
    if binding.identity().profile_digest != repository.network.profile_digest ||
        binding.identity().consensus_config_digest != repository.network.consensus_digest ||
        binding.fork() != Fork::Gloas ||
        binding.checkpoint_epoch().is_some()
    {
        return Err(PersistenceError::Identity)
    }
    let mut inner = repository.lock()?;
    let root = binding.origin_block_root();
    let mut record =
        inner.records.get(&root).cloned().ok_or(PersistenceError::MissingPredecessor)?;
    repository.check_saved_record(&record)?;
    if record.state_root != binding.state_root() ||
        record.state_root != binding.origin_state_root() ||
        record.slot != binding.slot()
    {
        return Err(PersistenceError::Linkage)
    }
    let lease = repository
        .compact
        .lease(record.state_root, record.fork, repository.network.preset())?
        .ok_or(PersistenceError::Linkage)?;
    let BeaconState::Gloas(state, preset) = lease.state() else {
        return Err(PersistenceError::Linkage)
    };
    let signed = admitted.signed();
    if signed.message.beacon_block_root != root ||
        signed.message.payload.block_hash != admitted.verified_execution_hash()
    {
        return Err(PersistenceError::Linkage)
    }
    gloas_processing::verify_execution_payload_envelope_signature(
        state,
        signed,
        &repository.runtime.transitions.gloas,
    )?;
    let bytes = signed.to_ssz(*preset)?;
    repository.check_bytes(&bytes)?;
    let saved = Envelope {
        original: ObjectId::new(ObjectKind::BeaconPayloadEnvelope, Encoding::Ssz, &bytes),
        signed_root: signed.hash_tree_root(*preset)?,
        message_root: signed.message.hash_tree_root(*preset)?,
        execution: ExecutionAssociation {
            block_hash: admitted.verified_execution_hash(),
            execution_state_root: signed.message.payload.state_root,
        },
    };
    if let Some(existing) = &record.envelope {
        if existing != &saved {
            return Err(PersistenceError::Linkage)
        }
        return Ok(())
    }
    repository.shared.put(
        ObjectKind::BeaconPayloadEnvelope,
        Encoding::Ssz,
        &bytes,
        Owner::Consensus(root),
    )?;
    record.execution = Some(saved.execution);
    record.envelope = Some(saved);
    repository.commit_record(&mut inner, record)?;
    Ok(())
}
