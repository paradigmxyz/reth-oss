//! Compact persistence sourced from concrete bootstrap or admitted strict stores.
//! Original signed bytes are retained for replay and serving. Saved snapshots are ordinary
//! recovery hints; they never grant execution or finality authority. All work is blocking.

use crate::{
    bootstrap::checked_config_digest, runtime_parameters::RuntimeParameters, BootstrapError,
    CheckpointBlockFormat, SszCheckpointVerifier, VerifiedCheckpoint,
};
use reth_embedded_beacon_config::{ChainConfig, DevnetConfig, Preset as ConfigPreset};
use reth_embedded_beacon_crypto::{
    compute_domain, compute_signing_root, verify_signature, CryptoError,
};
use reth_embedded_beacon_fork_choice::{
    gloas::{gossip_view::GloasGossipView, GloasStore},
    gossip_view::GossipView,
    ForkChoiceNode, PayloadStatus, Store, StoreError, StoreSnapshot,
};
use reth_embedded_beacon_primitives::{sha256, Checkpoint, Fork, Root, SszError};
use reth_embedded_beacon_shared_data::{
    journal::Journal, Encoding, ObjectId, ObjectKind, Owner, SharedDataStore,
    StoreError as SharedError, StoreIdentity,
};
use reth_embedded_beacon_state_transition::{
    gloas_processing,
    phase0_processing::ProcessingError,
    ssz::{
        gloas, phase0::Validator, BeaconBlock, BeaconState, Preset, SchemaError, SignedBeaconBlock,
    },
};
use reth_embedded_beacon_storage::{
    CompactStateError, CompactStateLimits, CompactStateStats, CompactStateStore, StateLease,
    WeakStateLease,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex, MutexGuard},
};

/// Serialized authenticated import bridge sharing the execution pool's existing byte backend.
#[derive(Debug)]
pub struct AdmittedStateRepository {
    shared: SharedDataStore,
    compact: CompactStateStore,
    network: PersistenceNetwork,
    runtime: RuntimeParameters,
    max_records: usize,
    #[cfg(all(
        feature = "peer-runtime",
        feature = "gossip-validation",
        feature = "execution-verification"
    ))]
    schema_cache_bytes: usize,
    #[cfg(all(
        feature = "peer-runtime",
        feature = "gossip-validation",
        feature = "execution-verification"
    ))]
    graph_resident_bytes: usize,
    boundary_proof: Mutex<Option<replayed::BoundaryProof>>,
    inner: Mutex<Inner>,
}
impl AdmittedStateRepository {
    /// Bind both complete profile identity and consensus configuration under private writer locks.
    pub fn open(
        shared: SharedDataStore,
        network: PersistenceNetwork,
        limits: CompactStateLimits,
    ) -> Result<Self, PersistenceError> {
        if shared.identity() != network.identity() {
            return Err(PersistenceError::Identity)
        }
        if limits.hot_cache_schema_bytes > limits.max_resident_schema_bytes {
            return Err(CompactStateError::Capacity.into())
        }
        let runtime = RuntimeParameters::from_config(&network.config)?;
        // Keep the complete decoded idle-state budget in one source pool. Splitting it can
        // leave each half able to hold only one public-network state, repeatedly evicting the
        // checkpoint or current replay state. Graph reads use checked owning source guards;
        // wrappers may retain idle ownership only while that pool owns the same allocation.
        let compact = CompactStateStore::open(shared.clone(), limits)?;
        let (mut journal, restored) = Journal::open::<Saved>(
            shared.namespace_root().join("beacon-admitted-states-v1/journal"),
            shared.identity(),
        )?;
        let fresh = restored.is_none();
        let saved = restored.unwrap_or_else(|| Saved::empty(network.consensus_digest));
        if saved.schema != 1 ||
            saved.consensus_digest != network.consensus_digest ||
            saved.records.len() > limits.max_retained_states
        {
            return Err(PersistenceError::Identity)
        }
        let mut records = BTreeMap::new();
        for record in saved.records {
            record.validate(&network)?;
            if records.insert(record.block_root, record).is_some() {
                return Err(PersistenceError::Linkage)
            }
        }
        if let Some(anchor) = &saved.anchor {
            let record = records.get(&anchor.checkpoint.root).ok_or(PersistenceError::Linkage)?;
            if (anchor.genesis &&
                (anchor.slot != 0 || anchor.checkpoint.epoch != 0 || anchor.boundary.is_some())) ||
                record.state_root != anchor.state_root ||
                record.slot != anchor.slot ||
                anchor.boundary.as_ref().map_or(record.slot, |boundary| boundary.slot) /
                    network.config.slots_per_epoch !=
                    anchor.checkpoint.epoch
            {
                return Err(PersistenceError::Linkage)
            }
            for record in
                records.values().filter(|record| record.block_root != anchor.checkpoint.root)
            {
                if record.format != CheckpointBlockFormat::Signed ||
                    !saved
                        .recovery
                        .iter()
                        .chain(&saved.previous_recovery)
                        .any(|generation| generation.anchor.checkpoint.root == record.block_root) &&
                        !records
                            .get(&record.parent_root)
                            .is_some_and(|parent| parent.slot < record.slot)
                {
                    return Err(PersistenceError::Linkage)
                }
            }
        } else if !records.is_empty() || saved.current.is_some() || saved.previous.is_some() {
            return Err(PersistenceError::Linkage)
        }
        for snapshot in [&saved.current, &saved.previous].into_iter().flatten() {
            snapshot.validate(&records)?;
        }
        if fresh {
            journal.commit(&Saved::empty(network.consensus_digest))?;
        }
        let repository = Self {
            shared,
            compact,
            network,
            runtime,
            max_records: limits.max_retained_states,
            // Leave room for the next cold decode while source wrappers still own old states.
            #[cfg(all(
                feature = "peer-runtime",
                feature = "gossip-validation",
                feature = "execution-verification"
            ))]
            schema_cache_bytes: limits
                .hot_cache_schema_bytes
                .min(limits.max_resident_schema_bytes / 2),
            #[cfg(all(
                feature = "peer-runtime",
                feature = "gossip-validation",
                feature = "execution-verification"
            ))]
            graph_resident_bytes: limits.max_resident_schema_bytes,
            boundary_proof: Mutex::new(None),
            inner: Mutex::new(Inner {
                journal,
                records,
                anchor: saved.anchor,
                current: saved.current,
                previous: saved.previous,
                recovery: saved.recovery,
                previous_recovery: saved.previous_recovery,
                cleanup: saved.cleanup,
                current_closure: saved.current_closure,
                previous_closure: saved.previous_closure,
                prepared: saved.prepared,
                events: saved.events,
                event_cleanup: saved.event_cleanup,
            }),
        };
        {
            let inner = repository.lock()?;
            repository.check_event_ledger(&inner)?;
            for snapshot in inner.current.iter().chain(&inner.previous) {
                repository.check_snapshot_data(&inner.records, snapshot)?;
            }
            for generation in inner.recovery.iter().chain(&inner.previous_recovery) {
                repository.check_recovery_generation(&inner.records, generation)?;
            }
            for (snapshot, closure, recovery) in [
                (inner.current.as_ref(), inner.current_closure.as_ref(), inner.recovery.as_ref()),
                (
                    inner.previous.as_ref(),
                    inner.previous_closure.as_ref(),
                    inner.previous_recovery.as_ref(),
                ),
            ] {
                if let Some(closure) = closure {
                    let snapshot = snapshot.ok_or(PersistenceError::Linkage)?;
                    repository.check_closure(&inner, snapshot, closure, recovery)?;
                }
            }
            if let Some(prepared) = &inner.prepared {
                prepared.snapshot.validate(&inner.records)?;
                repository.check_snapshot_data(&inner.records, &prepared.snapshot)?;
                repository.check_closure(
                    &inner,
                    &prepared.snapshot,
                    &prepared.closure,
                    prepared.recovery.as_ref(),
                )?;
            }
            if let Some(anchor) = &inner.anchor {
                repository.check_saved_record(
                    inner.records.get(&anchor.checkpoint.root).ok_or(PersistenceError::Linkage)?,
                )?;
                repository.check_saved_boundary(anchor)?;
            }
        }
        {
            let mut inner = repository.lock()?;
            repository.finish_event_cleanup(&mut inner)?;
            repository.finish_pending_cleanup(&mut inner)?;
        }
        Ok(repository)
    }

    /// Reauthenticate concrete bootstrap under the same checked consensus configuration.
    /// All data and pins become durable before its trusted reconstruction pointer.
    pub fn persist_checkpoint(
        &self,
        verifier: &SszCheckpointVerifier,
        verified: &VerifiedCheckpoint,
        original_state_ssz: &[u8],
        original_block_ssz: &[u8],
        now_unix_ms: u64,
        max_age_epochs: u64,
    ) -> Result<(), PersistenceError> {
        let mut inner = self.lock()?;
        if verified.config_digest != Some(self.network.consensus_digest) {
            return Err(PersistenceError::Identity)
        }
        let actual = verifier.verify_checkpoint(
            original_state_ssz,
            original_block_ssz,
            verified.checkpoint(),
            now_unix_ms,
            max_age_epochs,
        )?;
        if actual.config_digest != Some(self.network.consensus_digest) ||
            actual.state_root() != verified.state_root() ||
            actual.slot() != verified.slot()
        {
            return Err(PersistenceError::Linkage)
        }
        self.check_bytes(original_block_ssz)?;
        let fork = self
            .network
            .config
            .fork_at_slot(actual.slot())
            .map_err(|_| PersistenceError::Identity)?
            .fork;
        let state = BeaconState::from_ssz(original_state_ssz, fork, self.network.preset())?;
        self.check_state(&state)?;
        let block = match verifier.block_format() {
            CheckpointBlockFormat::Unsigned => {
                BeaconBlock::from_ssz(original_block_ssz, fork, self.network.preset())?
            }
            CheckpointBlockFormat::Signed => {
                let signed =
                    SignedBeaconBlock::from_ssz(original_block_ssz, fork, self.network.preset())?;
                verify_original_signature(&state, &signed, self.network.config.slots_per_epoch)?;
                signed.message()
            }
        };
        let header = block.header()?;
        let record = Admission {
            block_root: actual.checkpoint().root,
            parent_root: header.parent_root,
            state_root: actual.state_root(),
            slot: actual.slot(),
            fork,
            original: ObjectId::new(ObjectKind::BeaconBlock, Encoding::Ssz, original_block_ssz),
            format: verifier.block_format(),
            execution: execution(&block),
            envelope: None,
        };
        let anchor = Anchor {
            checkpoint: actual.checkpoint(),
            state_root: actual.state_root(),
            slot: actual.slot(),
            boundary: None,
            genesis: false,
        };
        if self.existing_seed(&inner, &anchor, &record)? {
            return Ok(())
        }
        self.check_capacity(&inner, record.block_root)?;
        self.compact.persist_state(&state, actual.state_root())?;
        self.shared.put(
            ObjectKind::BeaconBlock,
            Encoding::Ssz,
            original_block_ssz,
            Owner::Checkpoint(record.block_root),
        )?;
        let mut next = Saved::capture(self.network.consensus_digest, &inner);
        next.records.retain(|value| value.block_root != record.block_root);
        next.records.push(record.clone());
        next.anchor = Some(anchor.clone());
        inner.journal.commit(&next)?;
        inner.records.insert(record.block_root, record);
        inner.anchor = Some(anchor);
        Ok(())
    }

    /// Persist the strict store's actual post-state and the original complete signed container.
    pub fn persist_classic(
        &self,
        store: &Store,
        root: Root,
        original_signed_ssz: &[u8],
    ) -> Result<PersistedBlock, PersistenceError> {
        let mut inner = self.lock()?;
        self.check_classic(store)?;
        self.persist_admitted(
            &mut inner,
            root,
            store.block(root).ok_or(StoreError::UnknownRoot)?,
            store.block_state(root).ok_or(StoreError::UnknownRoot)?,
            original_signed_ssz,
        )
    }
    /// Persist an actual Gloas beacon state, independently of its admitted execution envelope.
    pub fn persist_gloas(
        &self,
        store: &GloasStore,
        root: Root,
        original_signed_ssz: &[u8],
    ) -> Result<PersistedBlock, PersistenceError> {
        let mut inner = self.lock()?;
        self.check_gloas(store)?;
        let block = store.block_arc(root).ok_or(StoreError::UnknownRoot)?;
        let state = store.block_state_arc(root).ok_or(StoreError::UnknownRoot)?;
        // The Gloas-specific Store has no borrowed generic enum view. This explicit temporary
        // copy remains until coordinated lease integration replaces complete Store retention.
        self.persist_admitted(
            &mut inner,
            root,
            &BeaconBlock::Gloas((*block).clone(), self.network.preset()),
            &BeaconState::Gloas((*state).clone(), self.network.preset()),
            original_signed_ssz,
        )
    }
    /// Persist original typed classic history after migration, without changing its schema.
    pub fn persist_gloas_history(
        &self,
        store: &GloasStore,
        root: Root,
        original_signed_ssz: &[u8],
    ) -> Result<PersistedBlock, PersistenceError> {
        let mut inner = self.lock()?;
        self.check_gloas(store)?;
        let block = store.historical_block_arc(root).ok_or(StoreError::UnknownRoot)?;
        let state = store.historical_block_state_arc(root).ok_or(StoreError::UnknownRoot)?;
        self.persist_admitted(&mut inner, root, &block, &state, original_signed_ssz)
    }
    /// Retain original signed envelope bytes after strict signature, Engine and data admission.
    /// EL payload `state_root` is recorded separately from the CL beacon state commitment.
    pub fn persist_verified_envelope(
        &self,
        store: &GloasStore,
        root: Root,
        original_signed_ssz: &[u8],
    ) -> Result<PersistedBlock, PersistenceError> {
        let mut inner = self.lock()?;
        self.check_gloas(store)?;
        self.check_bytes(original_signed_ssz)?;
        if !store.is_payload_verified(root) {
            return Err(PersistenceError::UnverifiedEnvelope)
        }
        let envelope = store.payload_arc(root).ok_or(PersistenceError::UnverifiedEnvelope)?;
        let state = store.block_state(root).ok_or(StoreError::UnknownRoot)?;
        let signed = gloas::SignedExecutionPayloadEnvelope::from_ssz(
            original_signed_ssz,
            self.network.preset(),
        )?;
        if signed.message != *envelope ||
            signed.message.beacon_block_root != root ||
            signed.to_ssz(self.network.preset())? != original_signed_ssz
        {
            return Err(PersistenceError::Linkage)
        }
        gloas_processing::verify_execution_payload_envelope_signature(
            state,
            &signed,
            store.processing_parameters(),
        )?;
        let mut record =
            inner.records.get(&root).cloned().ok_or(PersistenceError::MissingPredecessor)?;
        if record.state_root != state.hash_tree_root(self.network.preset())? ||
            record.slot != state.slot
        {
            return Err(PersistenceError::Linkage)
        }
        let saved = Envelope {
            original: ObjectId::new(
                ObjectKind::BeaconPayloadEnvelope,
                Encoding::Ssz,
                original_signed_ssz,
            ),
            signed_root: signed.hash_tree_root(self.network.preset())?,
            message_root: envelope.hash_tree_root(self.network.preset())?,
            execution: ExecutionAssociation {
                block_hash: envelope.payload.block_hash,
                execution_state_root: envelope.payload.state_root,
            },
        };
        self.check_saved_record(&record)?;
        if let Some(existing) = &record.envelope {
            if existing != &saved {
                return Err(PersistenceError::Linkage)
            }
            return Ok(PersistedBlock { admission: record })
        }
        self.shared.put(
            ObjectKind::BeaconPayloadEnvelope,
            Encoding::Ssz,
            original_signed_ssz,
            Owner::Consensus(root),
        )?;
        record.envelope = Some(saved.clone());
        record.execution = Some(saved.execution);
        self.commit_record(&mut inner, record.clone())?;
        Ok(PersistedBlock { admission: record })
    }

    /// Publish the actual classic head/checkpoints only after all referenced data is durable.
    pub fn publish_classic_snapshot(
        &self,
        store: &Store,
    ) -> Result<PersistedSnapshot, PersistenceError> {
        let mut inner = self.lock()?;
        self.check_classic(store)?;
        self.publish_with_recovery(
            &mut inner,
            store.snapshot()?,
            None,
            Some(publication::ReplayClosure {
                roots: store.retained_block_roots().collect(),
                envelopes: BTreeSet::new(),
                events: None,
                time_ms: None,
            }),
        )
    }
    /// Publish actual Gloas branch selection; a FULL head requires an admitted durable envelope.
    pub fn publish_gloas_snapshot(
        &self,
        store: &GloasStore,
    ) -> Result<PersistedSnapshot, PersistenceError> {
        let mut inner = self.lock()?;
        self.check_gloas(store)?;
        let snapshot = store.snapshot()?;
        if snapshot.head.payload_status == Some(PayloadStatus::Full) &&
            !store.is_payload_verified(snapshot.head.root)
        {
            return Err(PersistenceError::UnverifiedEnvelope)
        }
        self.publish_with_recovery(
            &mut inner,
            snapshot,
            None,
            Some(publication::ReplayClosure {
                roots: store.retained_block_roots().collect(),
                envelopes: store.verified_envelope_roots().collect(),
                events: None,
                time_ms: None,
            }),
        )
    }
    /// Saved metadata is a replay hint, never sufficient to initialize live graph authority.
    pub fn current_snapshot(&self) -> Result<Option<PersistedSnapshot>, PersistenceError> {
        Ok(self.lock()?.current.clone().map(|snapshot| PersistedSnapshot { snapshot }))
    }
    /// Exact original block encoding and its explicit signed/unsigned checkpoint format.
    pub fn original_block(
        &self,
        root: Root,
    ) -> Result<Option<OriginalBlockEncoding>, PersistenceError> {
        let inner = self.lock()?;
        let Some(record) = inner.records.get(&root) else { return Ok(None) };
        self.check_saved_record(record)?;
        let bytes = self
            .shared
            .get_bounded(record.original, self.network.config.max_payload_size)?
            .ok_or(PersistenceError::Linkage)?;
        Ok(Some((record.format, bytes)))
    }
    /// Exact original signed envelope bytes, checked against their durable receipt and owner.
    /// Recovered bytes remain replay inputs; this getter cannot confer execution authority.
    pub fn original_envelope(&self, root: Root) -> Result<Option<Vec<u8>>, PersistenceError> {
        let inner = self.lock()?;
        let Some(record) = inner.records.get(&root) else { return Ok(None) };
        self.check_saved_record(record)?;
        let Some(envelope) = &record.envelope else { return Ok(None) };
        Ok(Some(
            self.shared
                .get_bounded(envelope.original, self.network.config.max_payload_size)?
                .ok_or(PersistenceError::Linkage)?
                .to_vec(),
        ))
    }

    /// Bounded durable receipt inventory for original signed replay in topological slot order.
    /// Actual transitions, signatures, Engine responses and availability must be checked again.
    pub fn replay_inventory(&self, maximum: usize) -> Result<Vec<(u64, Root)>, PersistenceError> {
        let inner = self.lock()?;
        let selected = inner.records.values().filter(|record| {
            inner.replay_closure().map_or_else(
                || {
                    inner
                        .replay_recovery()
                        .is_none_or(|generation| generation.roots.contains(&record.block_root))
                },
                |closure| closure.roots.contains(&record.block_root),
            )
        });
        let mut inventory = Vec::new();
        for record in selected {
            if maximum == 0 || inventory.len() >= maximum {
                return Err(PersistenceError::Capacity)
            }
            self.check_saved_record(record)?;
            inventory.push((record.slot, record.block_root));
        }
        inventory.sort_unstable();
        Ok(inventory)
    }

    /// Reconstructed complete data lease; it cannot grant graph, execution or finality authority.
    pub fn block_state(&self, root: Root) -> Result<Option<StateLease>, PersistenceError> {
        let inner = self.lock()?;
        let Some(record) = inner.records.get(&root) else { return Ok(None) };
        // Keep the complete allocation returned by the original-owner and retained-graph check.
        // A second lease would repeat graph traversal and could cold-decode again after eviction.
        Ok(Some(self.check_saved_record_with_lease(record)?))
    }
    /// Reauthenticate the compact trusted seed under the current weak-subjectivity clock.
    /// Later head metadata still requires replay of the original admitted signed blocks.
    pub fn recover_checkpoint(
        &self,
        verifier: &SszCheckpointVerifier,
        now_unix_ms: u64,
        max_age_epochs: u64,
    ) -> Result<Option<RecoveredCompactCheckpoint>, PersistenceError> {
        let inner = self.lock()?;
        let Some(origin) = &inner.anchor else { return Ok(None) };
        if origin.genesis {
            return Err(PersistenceError::WrongAnchorKind)
        }
        let anchor = inner.replay_recovery().map_or(origin, |generation| &generation.anchor);
        let record = inner.records.get(&anchor.checkpoint.root).ok_or(PersistenceError::Linkage)?;
        self.check_saved_record(record)?;
        let local_verifier;
        let verifier = if inner.replay_recovery().is_some() {
            // A graph-created local cut is a real signed admission even when the immutable
            // operator seed used an unsigned container. Retain the exact saved container.
            local_verifier = SszCheckpointVerifier::new(
                self.network.config.clone(),
                verifier.decode_limits(),
                record.format,
            )?;
            &local_verifier
        } else {
            if verifier.block_format() != record.format {
                return Err(PersistenceError::Linkage)
            }
            verifier
        };
        if anchor.boundary.is_some() {
            return self
                .recover_replayed_checkpoint(verifier, anchor, record, now_unix_ms, max_age_epochs)
                .map(Some)
        }
        let state = self
            .compact
            .lease(record.state_root, record.fork, self.network.preset())?
            .ok_or(PersistenceError::Linkage)?;
        let block_ssz = self
            .shared
            .get_bounded(record.original, self.network.config.max_payload_size)?
            .ok_or(PersistenceError::Linkage)?;
        let verified = verifier.verify_checkpoint(
            &state.state().to_ssz()?,
            &block_ssz,
            anchor.checkpoint,
            now_unix_ms,
            max_age_epochs,
        )?;
        if verified.config_digest != Some(self.network.consensus_digest) ||
            verified.state_root() != anchor.state_root ||
            verified.slot() != anchor.slot
        {
            return Err(PersistenceError::Identity)
        }
        Ok(Some(RecoveredCompactCheckpoint {
            checkpoint: verified,
            state,
            block_ssz,
            block_format: record.format,
            replay: None,
        }))
    }

    /// Actual retained allocation measurements, excluding existing Store-owned states.
    pub fn stats(&self) -> Result<CompactStateStats, PersistenceError> {
        let _guard = self.lock()?;
        Ok(self.compact.stats()?)
    }

    /// Collect only unowned content after actual canonical retirement completed.
    /// Independent execution, checkpoint and active recovery owners remain pinned.
    #[cfg(all(feature = "peer-runtime", feature = "execution-verification"))]
    pub(crate) fn collect_retired_objects(&self) -> Result<usize, PersistenceError> {
        let mut guard = self.lock()?;
        self.finish_event_cleanup(&mut guard)?;
        Ok(self.shared.collect_garbage()?)
    }

    fn lock(&self) -> Result<MutexGuard<'_, Inner>, PersistenceError> {
        let inner = self.inner.lock().map_err(|_| PersistenceError::CoordinatorPoisoned)?;
        inner.journal.ensure_healthy()?;
        self.shared.ensure_healthy()?;
        Ok(inner)
    }
    fn check_state(&self, state: &BeaconState) -> Result<(), PersistenceError> {
        let activation = self
            .network
            .config
            .fork_at_slot(state.slot())
            .map_err(|_| PersistenceError::Identity)?;
        let fork = state.fork_info();
        if state.preset() != self.network.preset() ||
            state.genesis_validators_root() != self.network.config.genesis_validators_root ||
            state.genesis_time() != self.network.config.genesis_time ||
            state.schema_fork() != activation.fork ||
            fork.current_version != activation.version ||
            fork.epoch != activation.epoch
        {
            return Err(PersistenceError::Identity)
        }
        Ok(())
    }
    fn check_classic(&self, store: &Store) -> Result<(), PersistenceError> {
        if store.transition_parameters() != Some(&self.runtime.transitions) ||
            GossipView::store_parameters(store) != self.runtime.fork_choice ||
            store.genesis_time_ms() !=
                self.network
                    .config
                    .clock()
                    .map_err(|_| PersistenceError::Identity)?
                    .start_of_slot(0)
                    .map_err(|_| PersistenceError::Identity)?
        {
            return Err(PersistenceError::Identity)
        }
        Ok(())
    }
    fn check_gloas(&self, store: &GloasStore) -> Result<(), PersistenceError> {
        if store.processing_parameters() != &self.runtime.transitions.gloas ||
            GloasGossipView::store_parameters(store) != self.runtime.fork_choice ||
            store.genesis_time_ms() !=
                self.network
                    .config
                    .clock()
                    .map_err(|_| PersistenceError::Identity)?
                    .start_of_slot(0)
                    .map_err(|_| PersistenceError::Identity)?
        {
            return Err(PersistenceError::Identity)
        }
        Ok(())
    }
    const fn check_bytes(&self, bytes: &[u8]) -> Result<(), PersistenceError> {
        if bytes.len() > self.network.config.max_payload_size {
            return Err(PersistenceError::Capacity)
        }
        Ok(())
    }
    fn check_capacity(&self, inner: &Inner, root: Root) -> Result<(), PersistenceError> {
        if !inner.records.contains_key(&root) && inner.records.len() >= self.max_records {
            return Err(PersistenceError::Capacity)
        }
        Ok(())
    }
    fn persist_admitted(
        &self,
        inner: &mut Inner,
        root: Root,
        block: &BeaconBlock,
        state: &BeaconState,
        bytes: &[u8],
    ) -> Result<PersistedBlock, PersistenceError> {
        self.check_bytes(bytes)?;
        self.check_state(state)?;
        self.check_capacity(inner, root)?;
        let anchor = inner.anchor.as_ref().ok_or(PersistenceError::MissingAnchor)?;
        let header = block.header()?;
        if root != anchor.checkpoint.root {
            let parent = inner
                .records
                .get(&header.parent_root)
                .filter(|parent| parent.slot < header.slot)
                .ok_or(PersistenceError::MissingPredecessor)?;
            self.check_saved_record(parent)?;
        }
        let signed =
            SignedBeaconBlock::from_ssz(bytes, state.schema_fork(), self.network.preset())?;
        if block.hash_tree_root()? != root ||
            signed.message_root()? != root ||
            signed.message() != *block ||
            signed.to_ssz()? != bytes ||
            header.state_root != state.hash_tree_root()? ||
            header.slot != state.slot()
        {
            return Err(PersistenceError::Linkage)
        }
        verify_original_signature(state, &signed, self.network.config.slots_per_epoch)?;
        let record = Admission {
            block_root: root,
            parent_root: header.parent_root,
            state_root: header.state_root,
            slot: header.slot,
            fork: state.schema_fork(),
            original: ObjectId::new(ObjectKind::BeaconBlock, Encoding::Ssz, bytes),
            format: CheckpointBlockFormat::Signed,
            execution: execution(block),
            envelope: None,
        };
        if let Some(existing) = inner.records.get(&root) {
            if existing.block_root != record.block_root ||
                existing.parent_root != record.parent_root ||
                existing.state_root != record.state_root ||
                existing.slot != record.slot ||
                existing.fork != record.fork ||
                existing.original != record.original ||
                existing.format != record.format ||
                (existing.envelope.is_none() && existing.execution != record.execution)
            {
                return Err(PersistenceError::Linkage)
            }
            self.check_saved_record(existing)?;
            return Ok(PersistedBlock { admission: existing.clone() })
        }
        self.compact.persist_state(state, header.state_root)?;
        self.shared.put(ObjectKind::BeaconBlock, Encoding::Ssz, bytes, Owner::Consensus(root))?;
        self.commit_record(inner, record.clone())?;
        Ok(PersistedBlock { admission: record })
    }
    fn commit_record(&self, inner: &mut Inner, record: Admission) -> Result<(), PersistenceError> {
        let mut next = Saved::capture(self.network.consensus_digest, inner);
        next.records.retain(|value| value.block_root != record.block_root);
        next.records.push(record.clone());
        inner.journal.commit(&next)?;
        inner.records.insert(record.block_root, record);
        Ok(())
    }
    fn publish_with_recovery(
        &self,
        inner: &mut Inner,
        actual: StoreSnapshot,
        recovery: Option<retention::RecoveryGeneration>,
        closure: Option<publication::ReplayClosure>,
    ) -> Result<PersistedSnapshot, PersistenceError> {
        let snapshot = self.make_snapshot(inner, actual)?;
        if inner.current.as_ref() == Some(&snapshot) &&
            recovery.is_none() &&
            inner.current_closure == closure &&
            inner.prepared.is_none()
        {
            return Ok(PersistedSnapshot { snapshot })
        }
        if let Some(previous) = &inner.current {
            self.check_snapshot_data(&inner.records, previous)?;
        }
        self.commit_canonical(inner, snapshot, recovery, closure)
    }
    fn check_snapshot_data(
        &self,
        records: &BTreeMap<Root, Admission>,
        snapshot: &Snapshot,
    ) -> Result<(), PersistenceError> {
        let roots =
            BTreeSet::from([snapshot.block_root, snapshot.justified.root, snapshot.finalized.root]);
        for root in roots {
            self.check_saved_record(records.get(&root).ok_or(PersistenceError::Linkage)?)?;
        }
        Ok(())
    }
    fn check_saved_record(&self, record: &Admission) -> Result<(), PersistenceError> {
        self.check_saved_record_with_lease(record).map(|_| ())
    }

    /// Keep the independently checked immutable allocation alive for one active replay attempt.
    /// Every later read still validates the durable graph and original owners independently.
    fn check_saved_record_with_lease(
        &self,
        record: &Admission,
    ) -> Result<StateLease, PersistenceError> {
        record.validate(&self.network)?;
        self.shared.ensure_healthy()?;
        let consensus = self.shared.owner_objects(Owner::Consensus(record.block_root));
        let checkpoint = self.shared.owner_objects(Owner::Checkpoint(record.block_root));
        if !consensus.contains(&record.original) && !checkpoint.contains(&record.original) {
            return Err(PersistenceError::Linkage)
        }
        if let Some(envelope) = &record.envelope &&
            !consensus.contains(&envelope.original)
        {
            return Err(PersistenceError::Linkage)
        }
        let lease = self
            .compact
            .lease_retained(record.state_root, record.fork, self.network.preset())?
            .ok_or(PersistenceError::Linkage)?;
        self.check_state(lease.state())?;
        if lease.state().slot() != record.slot {
            return Err(PersistenceError::Linkage)
        }
        let bytes = self
            .shared
            .get_bounded(record.original, self.network.config.max_payload_size)?
            .ok_or(PersistenceError::Linkage)?;
        let block = match record.format {
            CheckpointBlockFormat::Unsigned => {
                BeaconBlock::from_ssz(&bytes, record.fork, self.network.preset())?
            }
            CheckpointBlockFormat::Signed => {
                let signed =
                    SignedBeaconBlock::from_ssz(&bytes, record.fork, self.network.preset())?;
                verify_original_signature(
                    lease.state(),
                    &signed,
                    self.network.config.slots_per_epoch,
                )?;
                signed.message()
            }
        };
        let header = block.header()?;
        if block.hash_tree_root()? != record.block_root ||
            header.parent_root != record.parent_root ||
            header.slot != record.slot ||
            header.state_root != record.state_root
        {
            return Err(PersistenceError::Linkage)
        }
        if let Some(saved) = &record.envelope {
            let bytes = self
                .shared
                .get_bounded(saved.original, self.network.config.max_payload_size)?
                .ok_or(PersistenceError::Linkage)?;
            let envelope =
                gloas::SignedExecutionPayloadEnvelope::from_ssz(&bytes, self.network.preset())?;
            let BeaconState::Gloas(state, _) = lease.state() else {
                return Err(PersistenceError::Linkage)
            };
            gloas_processing::verify_execution_payload_envelope_signature(
                state,
                &envelope,
                &self.runtime.transitions.gloas,
            )?;
            if envelope.hash_tree_root(self.network.preset())? != saved.signed_root ||
                envelope.message.hash_tree_root(self.network.preset())? != saved.message_root ||
                envelope.message.beacon_block_root != record.block_root ||
                envelope.message.payload.block_hash != saved.execution.block_hash ||
                envelope.message.payload.state_root != saved.execution.execution_state_root
            {
                return Err(PersistenceError::Linkage)
            }
        } else if execution(&block) != record.execution {
            return Err(PersistenceError::Linkage)
        }
        Ok(lease)
    }
}

/// Exact retained block bytes together with their explicit signed or unsigned format.
/// An unsigned trusted checkpoint remains unsigned when recovered or served.
pub type OriginalBlockEncoding = (CheckpointBlockFormat, Arc<[u8]>);

/// Concrete reauthenticated compact checkpoint inputs for trusted Store initialization.
#[derive(Debug)]
pub struct RecoveredCompactCheckpoint {
    /// Concrete weak-subjectivity checked checkpoint.
    pub checkpoint: VerifiedCheckpoint,
    /// Exact complete bootstrap state lease.
    pub state: StateLease,
    /// Original provider bytes, preserving their container format.
    pub block_ssz: Arc<[u8]>,
    /// Signed or unsigned checkpoint provider container.
    pub block_format: CheckpointBlockFormat,
    /// Separate original post-block lease and freshly reauthenticated scheduled replay.
    pub replay: Option<RecoveredReplayCheckpoint>,
}

#[cfg(all(
    feature = "peer-runtime",
    feature = "gossip-validation",
    feature = "execution-verification"
))]
impl RecoveredCompactCheckpoint {
    /// Consume freshly authenticated durable inputs on a blocking owner into exact import inputs.
    /// Complete serialization and sealed replay hash checks never run on an async IO worker.
    pub fn into_import_anchor(
        self,
        max_age_epochs: u64,
        decode_limits: crate::BootstrapDecodeLimits,
    ) -> Result<crate::import_coordinator::ImportAnchor, PersistenceError> {
        let state_ssz = Arc::<[u8]>::from(self.state.state().to_ssz()?);
        if state_ssz.len() > decode_limits.max_state_bytes ||
            self.block_ssz.len() > decode_limits.max_block_bytes
        {
            return Err(BootstrapError::CheckpointTooLarge.into())
        }
        let replay = self
            .replay
            .map(|replay| {
                let post_block_state_ssz =
                    Arc::<[u8]>::from(replay.post_block_state.state().to_ssz()?);
                if post_block_state_ssz.len() > decode_limits.max_state_bytes ||
                    !replay.proof.matches_inputs(
                        &state_ssz,
                        &post_block_state_ssz,
                        &self.block_ssz,
                        self.block_format,
                    ) ||
                    replay.proof.verified().checkpoint() != self.checkpoint.checkpoint() ||
                    replay.proof.verified().state_root() != self.checkpoint.state_root()
                {
                    return Err(PersistenceError::Linkage)
                }
                Ok(crate::import_coordinator::ReplayAnchor {
                    proof: replay.proof,
                    post_block_state_ssz,
                })
            })
            .transpose()?;
        Ok(crate::import_coordinator::ImportAnchor {
            verified: self.checkpoint,
            state_ssz,
            block_ssz: self.block_ssz,
            block_format: self.block_format,
            max_age_epochs,
            decode_limits,
            replay,
        })
    }
}
/// Checked profile binding. Complete private execution and terminal settings enter its digest.
#[derive(Debug, Clone)]
pub struct PersistenceNetwork {
    config: ChainConfig,
    consensus_digest: Root,
    profile_digest: Root,
}
impl PersistenceNetwork {
    /// Bind ordinary consensus settings under their canonical checked identity.
    pub fn from_config(config: ChainConfig) -> Result<Self, PersistenceError> {
        let digest = checked_config_digest(&config)?;
        Ok(Self { config, consensus_digest: digest, profile_digest: digest })
    }
    /// Bind checked private settings including the execution genesis and terminal selection.
    pub fn from_devnet(config: DevnetConfig) -> Result<Self, PersistenceError> {
        config.validate().map_err(|_| PersistenceError::Identity)?;
        let consensus_digest = checked_config_digest(&config.consensus)?;
        let profile_digest = sha256(&serde_json::to_vec(&config)?);
        Ok(Self { config: config.consensus, consensus_digest, profile_digest })
    }
    /// Full identity used by the existing byte backend.
    pub const fn identity(&self) -> StoreIdentity {
        StoreIdentity {
            genesis_root: self.config.genesis_validators_root,
            config_digest: self.profile_digest,
        }
    }
    const fn preset(&self) -> Preset {
        match self.config.preset {
            ConfigPreset::Mainnet => Preset::Mainnet,
            ConfigPreset::Minimal => Preset::Minimal,
        }
    }
}
/// Actual EL association; its trie root is always separate from the CL state commitment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionAssociation {
    /// Actual verified execution block hash.
    pub block_hash: Root,
    /// Actual EL payload trie root, never a CL state commitment.
    pub execution_state_root: Root,
}
/// Read-only durable admission receipt, constructed only after actual strict store admission.
#[derive(Debug, Clone)]
pub struct PersistedBlock {
    admission: Admission,
}
impl PersistedBlock {
    /// Original consensus message root.
    pub const fn block_root(&self) -> Root {
        self.admission.block_root
    }
    /// Beacon block's actual complete CL state commitment.
    pub const fn state_root(&self) -> Root {
        self.admission.state_root
    }
    /// Exact retained original signed container identity.
    pub const fn original(&self) -> ObjectId {
        self.admission.original
    }
}
/// Actual canonical snapshot metadata; replay is required before live recovery.
#[derive(Debug, Clone)]
pub struct PersistedSnapshot {
    snapshot: Snapshot,
}
impl PersistedSnapshot {
    /// Actual root and optional Gloas branch decision.
    pub fn head(&self) -> ForkChoiceNode {
        ForkChoiceNode {
            root: self.snapshot.block_root,
            payload_status: self.snapshot.payload.map(SavedPayloadStatus::status),
        }
    }
    /// Actual complete CL state commitment.
    pub const fn state_root(&self) -> Root {
        self.snapshot.state_root
    }
    /// Actual store checkpoint metadata, not proof of finality on its own.
    pub const fn checkpoints(&self) -> (Checkpoint, Checkpoint) {
        (self.snapshot.justified, self.snapshot.finalized)
    }
    /// EL association only when the selected branch includes its payload.
    pub const fn execution(&self) -> Option<ExecutionAssociation> {
        self.snapshot.execution
    }
    /// Exact original signed Gloas envelope root, distinct from both state roots.
    pub const fn signed_envelope_root(&self) -> Option<Root> {
        self.snapshot.envelope_root
    }
}
/// Failures stop durable publication and require the coordinator to retain its prior graph.
#[derive(Debug, thiserror::Error)]
pub enum PersistenceError {
    /// Complete compact reconstruction failure.
    #[error(transparent)]
    Compact(#[from] CompactStateError),
    /// Shared object or journal failure.
    #[error(transparent)]
    Shared(#[from] SharedError),
    /// Strict graph query failure.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// Concrete bootstrap authentication failure.
    #[error(transparent)]
    Bootstrap(#[from] BootstrapError),
    /// Concrete original genesis authentication failure.
    #[error(transparent)]
    Genesis(#[from] crate::GenesisError),
    /// Genesis and checkpoint modes require distinct concrete authentication.
    #[error("compact admission anchor is for a different startup mode")]
    WrongAnchorKind,
    /// Complete schema failure.
    #[error(transparent)]
    Schema(#[from] SchemaError),
    /// Canonical SSZ failure.
    #[error(transparent)]
    Ssz(#[from] SszError),
    /// Actual proposer signature failure.
    #[error(transparent)]
    Crypto(#[from] CryptoError),
    /// Envelope signature or parameter failure.
    #[error(transparent)]
    Processing(#[from] ProcessingError),
    /// Profile serialization failure.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    /// Fork, preset, clock, network or full profile differs.
    #[error("compact admission identity differs")]
    Identity,
    /// Root, original message or retained parent linkage differs.
    #[error("compact admission linkage differs")]
    Linkage,
    /// A concrete authenticated checkpoint must precede ordinary import.
    #[error("compact admission has no authenticated anchor")]
    MissingAnchor,
    /// An admitted ancestor must be durable first.
    #[error("compact admission predecessor is not durable")]
    MissingPredecessor,
    /// Changing the trusted anchor requires a separate isolated repository.
    #[error("compact admission trusted anchor differs")]
    ConflictingAnchor,
    /// FULL cannot be published without actual local envelope verification.
    #[error("compact admission envelope is not locally verified")]
    UnverifiedEnvelope,
    /// Bounded encoding or retained-root admission exceeded its policy.
    #[error("compact admission capacity exceeded")]
    Capacity,
    /// A panic interrupted the serialized coordinator.
    #[error("compact admission coordinator requires recovery")]
    CoordinatorPoisoned,
}
#[derive(Debug)]
struct Inner {
    journal: Journal,
    records: BTreeMap<Root, Admission>,
    anchor: Option<Anchor>,
    current: Option<Snapshot>,
    previous: Option<Snapshot>,
    recovery: Option<retention::RecoveryGeneration>,
    previous_recovery: Option<retention::RecoveryGeneration>,
    cleanup: Option<retention::RecoveryCleanup>,
    current_closure: Option<publication::ReplayClosure>,
    previous_closure: Option<publication::ReplayClosure>,
    prepared: Option<publication::PreparedPublication>,
    events: events::EventLedger,
    event_cleanup: Option<events::EventCleanup>,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Saved {
    schema: u8,
    consensus_digest: Root,
    records: Vec<Admission>,
    anchor: Option<Anchor>,
    current: Option<Snapshot>,
    previous: Option<Snapshot>,
    #[serde(default)]
    recovery: Option<retention::RecoveryGeneration>,
    #[serde(default)]
    previous_recovery: Option<retention::RecoveryGeneration>,
    #[serde(default)]
    cleanup: Option<retention::RecoveryCleanup>,
    #[serde(default)]
    current_closure: Option<publication::ReplayClosure>,
    #[serde(default)]
    previous_closure: Option<publication::ReplayClosure>,
    #[serde(default)]
    prepared: Option<publication::PreparedPublication>,
    #[serde(default)]
    events: events::EventLedger,
    #[serde(default)]
    event_cleanup: Option<events::EventCleanup>,
}
impl Saved {
    fn empty(consensus_digest: Root) -> Self {
        Self {
            schema: 1,
            consensus_digest,
            records: Vec::new(),
            anchor: None,
            current: None,
            previous: None,
            recovery: None,
            previous_recovery: None,
            cleanup: None,
            current_closure: None,
            previous_closure: None,
            prepared: None,
            events: events::EventLedger::default(),
            event_cleanup: None,
        }
    }
    fn capture(consensus_digest: Root, inner: &Inner) -> Self {
        Self {
            schema: 1,
            consensus_digest,
            records: inner.records.values().cloned().collect(),
            anchor: inner.anchor.clone(),
            current: inner.current.clone(),
            previous: inner.previous.clone(),
            recovery: inner.recovery.clone(),
            previous_recovery: inner.previous_recovery.clone(),
            cleanup: inner.cleanup.clone(),
            current_closure: inner.current_closure.clone(),
            previous_closure: inner.previous_closure.clone(),
            prepared: inner.prepared.clone(),
            events: inner.events.clone(),
            event_cleanup: inner.event_cleanup.clone(),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Anchor {
    checkpoint: Checkpoint,
    state_root: Root,
    slot: u64,
    #[serde(default)]
    boundary: Option<replayed::BoundaryAnchor>,
    #[serde(default)]
    genesis: bool,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Admission {
    block_root: Root,
    parent_root: Root,
    state_root: Root,
    slot: u64,
    fork: Fork,
    original: ObjectId,
    format: CheckpointBlockFormat,
    execution: Option<ExecutionAssociation>,
    envelope: Option<Envelope>,
}
impl Admission {
    fn validate(&self, network: &PersistenceNetwork) -> Result<(), PersistenceError> {
        if self.fork !=
            network.config.fork_at_slot(self.slot).map_err(|_| PersistenceError::Identity)?.fork ||
            self.original.kind != ObjectKind::BeaconBlock ||
            self.original.encoding != Encoding::Ssz ||
            self.envelope.as_ref().is_some_and(|envelope| {
                self.fork != Fork::Gloas ||
                    envelope.original.kind != ObjectKind::BeaconPayloadEnvelope ||
                    envelope.original.encoding != Encoding::Ssz ||
                    self.execution != Some(envelope.execution)
            })
        {
            return Err(PersistenceError::Linkage)
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    original: ObjectId,
    signed_root: Root,
    message_root: Root,
    execution: ExecutionAssociation,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Snapshot {
    block_root: Root,
    state_root: Root,
    slot: u64,
    payload: Option<SavedPayloadStatus>,
    justified: Checkpoint,
    finalized: Checkpoint,
    execution: Option<ExecutionAssociation>,
    envelope_root: Option<Root>,
}
impl Snapshot {
    fn validate(&self, records: &BTreeMap<Root, Admission>) -> Result<(), PersistenceError> {
        let record = records.get(&self.block_root).ok_or(PersistenceError::MissingPredecessor)?;
        let expected_execution =
            if self.payload.is_none() || self.payload == Some(SavedPayloadStatus::Full) {
                record.execution
            } else {
                None
            };
        let expected_envelope = if self.payload == Some(SavedPayloadStatus::Full) {
            record.envelope.as_ref().map(|value| value.signed_root)
        } else {
            None
        };
        if self.state_root != record.state_root ||
            self.slot != record.slot ||
            !records.contains_key(&self.justified.root) ||
            !records.contains_key(&self.finalized.root) ||
            self.execution != expected_execution ||
            self.envelope_root != expected_envelope ||
            (self.payload.is_some() != (record.fork == Fork::Gloas))
        {
            return Err(PersistenceError::Linkage)
        }
        if self.payload == Some(SavedPayloadStatus::Full) && record.envelope.is_none() {
            return Err(PersistenceError::UnverifiedEnvelope)
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum SavedPayloadStatus {
    Empty,
    Full,
    Pending,
}
impl SavedPayloadStatus {
    const fn from_status(status: PayloadStatus) -> Self {
        match status {
            PayloadStatus::Empty => Self::Empty,
            PayloadStatus::Full => Self::Full,
            PayloadStatus::Pending => Self::Pending,
        }
    }
    const fn status(self) -> PayloadStatus {
        match self {
            Self::Empty => PayloadStatus::Empty,
            Self::Full => PayloadStatus::Full,
            Self::Pending => PayloadStatus::Pending,
        }
    }
}
fn verify_original_signature(
    state: &BeaconState,
    signed: &SignedBeaconBlock,
    slots_per_epoch: u64,
) -> Result<(), PersistenceError> {
    let header = signed.header()?;
    let validator = usize::try_from(header.proposer_index)
        .ok()
        .and_then(|index| validators(state).get(index))
        .ok_or(PersistenceError::Linkage)?;
    let fork = state.fork_info();
    let version = if header.slot / slots_per_epoch < fork.epoch {
        fork.previous_version
    } else {
        fork.current_version
    };
    verify_signature(
        &validator.pubkey,
        &compute_signing_root(
            signed.message_root()?,
            compute_domain([0; 4], version, state.genesis_validators_root()),
        ),
        signature(signed),
    )?;
    Ok(())
}
const fn validators(state: &BeaconState) -> &[Validator] {
    match state {
        BeaconState::Phase0(value, _) => value.validators.values.as_slice(),
        BeaconState::Altair(value, _) => value.validators.values.as_slice(),
        BeaconState::Bellatrix(value, _) => value.validators.values.as_slice(),
        BeaconState::Capella(value, _) => value.validators.values.as_slice(),
        BeaconState::Deneb(value, _) => value.validators.values.as_slice(),
        BeaconState::Electra(value, _) => value.validators.values.as_slice(),
        BeaconState::Fulu(value, _) => value.validators.values.as_slice(),
        BeaconState::Gloas(value, _) => value.validators.values.as_slice(),
    }
}
const fn signature(signed: &SignedBeaconBlock) -> &[u8; 96] {
    match signed {
        SignedBeaconBlock::Phase0(value, _) => &value.signature,
        SignedBeaconBlock::Altair(value, _) => &value.signature,
        SignedBeaconBlock::Bellatrix(value, _) => &value.signature,
        SignedBeaconBlock::Capella(value, _) => &value.signature,
        SignedBeaconBlock::Deneb(value, _) => &value.signature,
        SignedBeaconBlock::Electra(value, _) => &value.signature,
        SignedBeaconBlock::Fulu(value, _) => &value.signature,
        SignedBeaconBlock::Gloas(value, _) => &value.signature,
    }
}
fn execution(block: &BeaconBlock) -> Option<ExecutionAssociation> {
    let pair = match block {
        BeaconBlock::Bellatrix(value, _) => {
            (value.body.execution_payload.block_hash, value.body.execution_payload.state_root)
        }
        BeaconBlock::Capella(value, _) => {
            (value.body.execution_payload.block_hash, value.body.execution_payload.state_root)
        }
        BeaconBlock::Deneb(value, _) => {
            (value.body.execution_payload.block_hash, value.body.execution_payload.state_root)
        }
        BeaconBlock::Electra(value, _) => {
            (value.body.execution_payload.block_hash, value.body.execution_payload.state_root)
        }
        BeaconBlock::Fulu(value, _) => {
            (value.body.execution_payload.block_hash, value.body.execution_payload.state_root)
        }
        _ => return None,
    };
    if pair.0 == [0; 32] {
        None
    } else {
        Some(ExecutionAssociation { block_hash: pair.0, execution_state_root: pair.1 })
    }
}

pub(crate) mod events;
mod publication;
mod replayed;
mod retention;

pub use replayed::RecoveredReplayCheckpoint;

#[cfg(test)]
#[path = "compact_persistence/tests.rs"]
mod tests;

mod genesis;

#[cfg(all(
    feature = "peer-runtime",
    feature = "gossip-validation",
    feature = "execution-verification"
))]
pub(crate) mod state_source;

#[cfg(all(
    feature = "peer-runtime",
    feature = "gossip-validation",
    feature = "execution-verification"
))]
pub(crate) mod retention_gc;

#[cfg(all(
    feature = "peer-runtime",
    feature = "gossip-validation",
    feature = "execution-verification"
))]
mod maintenance;
