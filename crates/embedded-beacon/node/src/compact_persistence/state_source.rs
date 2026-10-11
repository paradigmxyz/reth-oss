//! Concrete owning cold-state source; private graph receipts alone authorize retention.
//! Canonical snapshot receipts are staged until the coordinator acknowledges actual Engine FCU.

use super::*;
use retain::{retain_block, retain_envelope};
use reth_embedded_beacon_fork_choice::compact::{
    AdmittedSnapshot, AdmittedState, CompactGraphLimits, CompactStateSource, FinalizedRetention,
    LeasedState, SignedBlockData, StateBinding, StateData, StateSourceError, StateSourceIdentity,
    VerifiedEnvelope,
};
use reth_embedded_beacon_state_transition::fork_processing::replay_slots;

/// Data provider for genuine compact graph bindings, sharing one existing repository writer.
#[derive(Debug)]
pub(crate) struct RepositoryStateSource {
    repository: Arc<AdmittedStateRepository>,
    derived: Mutex<DerivedJournal>,
    leases: Mutex<LeaseCache>,
    replay_proofs: Mutex<ReplayProofCache>,
    events: Mutex<events::AppliedEvents>,
    #[cfg(feature = "peer-runtime")]
    sidecar_contexts: Mutex<sidecar_context::SidecarContexts>,
}
impl RepositoryStateSource {
    pub(crate) fn open(
        repository: Arc<AdmittedStateRepository>,
    ) -> Result<Arc<Self>, PersistenceError> {
        let (mut journal, restored) = Journal::open::<DerivedSaved>(
            repository.shared.namespace_root().join("beacon-derived-checkpoints-v1/journal"),
            repository.shared.identity(),
        )?;
        let fresh = restored.is_none();
        let saved = restored.unwrap_or(DerivedSaved {
            schema: 1,
            consensus_digest: repository.network.consensus_digest,
            records: Vec::new(),
        });
        if saved.schema != 1 ||
            saved.consensus_digest != repository.network.consensus_digest ||
            saved.records.len() > repository.max_records
        {
            return Err(PersistenceError::Identity)
        }
        let mut records = BTreeMap::new();
        for record in saved.records {
            if records.insert((record.origin_block_root, record.epoch), record).is_some() {
                return Err(PersistenceError::Linkage)
            }
        }
        if fresh {
            journal.commit(&DerivedSaved {
                schema: 1,
                consensus_digest: repository.network.consensus_digest,
                records: Vec::new(),
            })?;
        }
        #[cfg(feature = "peer-runtime")]
        let contexts = sidecar_context::SidecarContexts::open(&repository)?;
        let source = Arc::new(Self {
            repository,
            derived: Mutex::new(DerivedJournal {
                journal,
                records,
                staged: None,
                prepared_closure: None,
                staged_recovery: None,
                staged_cleanup: None,
                published_cleanup: None,
            }),
            leases: Mutex::new(LeaseCache::default()),
            replay_proofs: Mutex::new(ReplayProofCache::default()),
            events: Mutex::new(events::AppliedEvents::default()),
            #[cfg(feature = "peer-runtime")]
            sidecar_contexts: Mutex::new(contexts),
        });
        {
            let inner = source.derived.lock().map_err(|_| PersistenceError::CoordinatorPoisoned)?;
            for record in inner.records.values() {
                source.check_derived(record)?;
            }
        }
        Ok(source)
    }

    /// Persist the exact native candidate before any canonical Engine call can be attempted.
    pub(crate) fn prepare_snapshot(&self, expected: StoreSnapshot) -> Result<(), PersistenceError> {
        let mut staged = self.derived.lock().map_err(|_| PersistenceError::CoordinatorPoisoned)?;
        staged.journal.ensure_healthy()?;
        staged.prepared_closure = None;
        let admitted = staged.staged.as_ref().ok_or(PersistenceError::Linkage)?;
        if admitted.identity() != self.identity() || admitted.snapshot() != expected {
            return Err(PersistenceError::Linkage)
        }
        let recovery = staged
            .staged_recovery
            .as_ref()
            .map(|(snapshot, generation)| {
                if *snapshot != expected {
                    return Err(PersistenceError::Linkage)
                }
                Ok(generation.clone())
            })
            .transpose()?;
        let prepared = self.prepare_replay_closure(admitted, recovery.as_ref())?;
        let mut repository = self.repository.lock()?;
        self.repository.prepare_publication(
            &mut repository,
            expected,
            prepared.closure().clone(),
            recovery,
        )?;
        staged.prepared_closure = Some(prepared);
        Ok(())
    }
    pub(crate) fn commit_snapshot(
        &self,
        expected: StoreSnapshot,
    ) -> Result<PersistedSnapshot, PersistenceError> {
        let mut staged = self.derived.lock().map_err(|_| PersistenceError::CoordinatorPoisoned)?;
        staged.journal.ensure_healthy()?;
        let admitted = staged.staged.take().ok_or(PersistenceError::Linkage)?;
        if admitted.identity() != self.identity() || admitted.snapshot() != expected {
            return Err(PersistenceError::Linkage)
        }
        let recovery = staged
            .staged_recovery
            .take()
            .map(|(snapshot, generation)| {
                if snapshot != expected {
                    return Err(PersistenceError::Linkage)
                }
                Ok(generation)
            })
            .transpose()?;
        let closure = self.commit_replay_closure(
            staged.prepared_closure.take(),
            &admitted,
            recovery.as_ref(),
        )?;
        let mut repository = self.repository.lock()?;
        let committed = self.repository.publish_with_recovery(
            &mut repository,
            admitted.snapshot(),
            recovery,
            Some(closure),
        )?;
        if let Some(cleanup) = staged.staged_cleanup.take() {
            if cleanup.snapshot != expected {
                return Err(PersistenceError::Linkage)
            }
            staged.published_cleanup = Some(cleanup);
        }
        drop(repository);
        drop(staged);
        #[cfg(feature = "peer-runtime")]
        self.commit_context_snapshot(expected)?;
        Ok(committed)
    }

    pub(crate) fn discard_snapshot(&self) -> Result<(), PersistenceError> {
        let mut staged = self.derived.lock().map_err(|_| PersistenceError::CoordinatorPoisoned)?;
        staged.journal.ensure_healthy()?;
        staged.staged = None;
        staged.prepared_closure = None;
        staged.staged_recovery = None;
        staged.staged_cleanup = None;
        drop(staged);
        #[cfg(feature = "peer-runtime")]
        self.discard_context_snapshot()?;
        Ok(())
    }

    fn check_binding(&self, binding: &StateBinding) -> Result<(), PersistenceError> {
        if binding.identity() != self.identity() ||
            binding.preset() != self.repository.network.preset() ||
            binding.genesis_time() != self.repository.network.config.genesis_time ||
            binding.genesis_validators_root() !=
                self.repository.network.config.genesis_validators_root
        {
            return Err(PersistenceError::Identity)
        }
        let repository = self.repository.lock()?;
        let origin = repository
            .records
            .get(&binding.origin_block_root())
            .ok_or(PersistenceError::MissingPredecessor)?;
        self.repository.check_saved_record(origin)?;
        if origin.state_root != binding.origin_state_root() {
            return Err(PersistenceError::Linkage)
        }
        Ok(())
    }

    fn check_state_binding(
        &self,
        binding: &StateBinding,
        state: StateData<'_>,
    ) -> Result<(), PersistenceError> {
        if state.hash_tree_root()? != binding.state_root() ||
            state.fork() != binding.fork() ||
            state.preset() != binding.preset() ||
            state.slot() != binding.slot() ||
            state.genesis_time() != binding.genesis_time() ||
            state.genesis_validators_root() != binding.genesis_validators_root()
        {
            return Err(PersistenceError::Linkage)
        }
        Ok(())
    }

    fn check_derived(&self, record: &Derived) -> Result<StateLease, PersistenceError> {
        let repository = self.repository.lock()?;
        let origin = repository
            .records
            .get(&record.origin_block_root)
            .ok_or(PersistenceError::MissingPredecessor)?;
        self.repository.check_saved_record(origin)?;
        if origin.state_root != record.origin_state_root ||
            record.slot !=
                record
                    .epoch
                    .checked_mul(self.repository.network.config.slots_per_epoch)
                    .ok_or(PersistenceError::Linkage)? ||
            record.slot < origin.slot ||
            record.slot - origin.slot > 8192 ||
            self.repository
                .network
                .config
                .fork_at_slot(record.slot)
                .map_err(|_| PersistenceError::Identity)?
                .fork !=
                record.fork
        {
            return Err(PersistenceError::Linkage)
        }
        self.repository.compact.ensure_retained(record.state_root)?;
        let result = self
            .repository
            .compact
            .lease(record.state_root, record.fork, self.repository.network.preset())?
            .ok_or(PersistenceError::Linkage)?;
        let post = self
            .repository
            .compact
            .lease(origin.state_root, origin.fork, self.repository.network.preset())?
            .ok_or(PersistenceError::Linkage)?;
        // All durable receipt, original-owner and complete-state pins were checked above.
        // A memo holds the real immutable allocations, so pointer reuse after an owner expires
        // cannot transfer proof to a different state. Changed records always require native replay.
        {
            let mut cache =
                self.leases.lock().map_err(|_| PersistenceError::CoordinatorPoisoned)?;
            self.trim_lease_cache(&mut cache).map_err(|error| match error {
                StateSourceError::ResourceLimit => PersistenceError::Capacity,
                _ => PersistenceError::Linkage,
            })?;
        }
        let mut proofs =
            self.replay_proofs.lock().map_err(|_| PersistenceError::CoordinatorPoisoned)?;
        self.trim_replay_proofs(&mut proofs)?;
        proofs.clock = proofs.clock.checked_add(1).ok_or(PersistenceError::Capacity)?;
        let touched = proofs.clock;
        if let Some(proof) = proofs.entries.get_mut(&(record.origin_block_root, record.epoch)) &&
            proof.record == *record &&
            std::ptr::eq(proof.post.state(), post.state()) &&
            std::ptr::eq(proof.boundary.state(), result.state())
        {
            proof.touched = touched;
            return Ok(result)
        }
        let replayed_root = if record.slot == post.state().slot() {
            post.root()
        } else {
            replay_slots(post.state(), record.slot, &self.repository.runtime.transitions, 8192)?
                .hash_tree_root()?
        };
        // Checked persistence or cold reconstruction verified these immutable commitments. Reuse
        // them only after the fresh durable original-owner and component-pin checks above.
        if replayed_root != record.state_root ||
            result.root() != record.state_root ||
            result.state().slot() != record.slot
        {
            return Err(PersistenceError::Linkage)
        }
        let proof_cache_bytes = self.repository.schema_cache_bytes;
        let bytes = StateData::Classic(post.state())
            .allocation_bytes()?
            .checked_add(StateData::Classic(result.state()).allocation_bytes()?)
            .ok_or(PersistenceError::Capacity)?;
        if let Some(old) = proofs.entries.remove(&(record.origin_block_root, record.epoch)) {
            proofs.bytes = proofs.bytes.checked_sub(old.bytes).ok_or(PersistenceError::Capacity)?;
        }
        if bytes <= proof_cache_bytes &&
            self.repository.compact.owns_cached_state(post.root(), post.state())? &&
            self.repository.compact.owns_cached_state(result.root(), result.state())?
        {
            while proofs.bytes.checked_add(bytes).ok_or(PersistenceError::Capacity)? >
                proof_cache_bytes ||
                proofs.entries.len() >= self.repository.max_records
            {
                let key = proofs
                    .entries
                    .iter()
                    .min_by_key(|(_, entry)| entry.touched)
                    .map(|(key, _)| *key)
                    .ok_or(PersistenceError::Capacity)?;
                let old = proofs.entries.remove(&key).ok_or(PersistenceError::Capacity)?;
                proofs.bytes =
                    proofs.bytes.checked_sub(old.bytes).ok_or(PersistenceError::Capacity)?;
            }
            proofs.bytes += bytes;
            proofs.entries.insert(
                (record.origin_block_root, record.epoch),
                ReplayProof {
                    record: record.clone(),
                    post,
                    boundary: result.clone(),
                    bytes,
                    touched,
                },
            );
        }
        Ok(result)
    }

    fn check_leased_binding(
        &self,
        binding: &StateBinding,
        lease: &StateLease,
    ) -> Result<(), PersistenceError> {
        let state = lease.state();
        // StateLease owns the exact private immutable allocation checked against this root during
        // persistence or reconstruction. Caller-provided StateData requires check_state_binding.
        if lease.root() != binding.state_root() ||
            state.schema_fork() != binding.fork() ||
            state.preset() != binding.preset() ||
            state.slot() != binding.slot() ||
            state.genesis_time() != binding.genesis_time() ||
            state.genesis_validators_root() != binding.genesis_validators_root()
        {
            return Err(PersistenceError::Linkage)
        }
        Ok(())
    }

    fn trim_lease_cache(&self, cache: &mut LeaseCache) -> Result<(), StateSourceError> {
        let stale = cache
            .entries
            .iter()
            .filter_map(|(key, entry)| {
                match self.repository.compact.owns_cached_state(key.0, entry.owner.state()) {
                    Ok(true) => None,
                    Ok(false) => Some(Ok(*key)),
                    Err(_) => Some(Err(StateSourceError::Durability)),
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        for key in stale {
            let entry = cache.entries.remove(&key).ok_or(StateSourceError::ResourceLimit)?;
            cache.bytes =
                cache.bytes.checked_sub(entry.bytes).ok_or(StateSourceError::ResourceLimit)?;
        }
        Ok(())
    }

    fn trim_replay_proofs(&self, proofs: &mut ReplayProofCache) -> Result<(), PersistenceError> {
        let mut stale = Vec::new();
        for (key, proof) in &proofs.entries {
            if !self.repository.compact.owns_cached_state(proof.post.root(), proof.post.state())? ||
                !self
                    .repository
                    .compact
                    .owns_cached_state(proof.boundary.root(), proof.boundary.state())?
            {
                stale.push(*key);
            }
        }
        for key in stale {
            let proof = proofs.entries.remove(&key).ok_or(PersistenceError::Capacity)?;
            proofs.bytes =
                proofs.bytes.checked_sub(proof.bytes).ok_or(PersistenceError::Capacity)?;
        }
        Ok(())
    }

    fn trim_idle_caches(&self) -> Result<(), StateSourceError> {
        let mut cache = self.leases.lock().map_err(|_| StateSourceError::Durability)?;
        self.trim_lease_cache(&mut cache)?;
        drop(cache);
        let mut proofs = self.replay_proofs.lock().map_err(|_| StateSourceError::Durability)?;
        self.trim_replay_proofs(&mut proofs).map_err(source_error)
    }
}
impl CompactStateSource for RepositoryStateSource {
    fn identity(&self) -> StateSourceIdentity {
        StateSourceIdentity {
            profile_digest: self.repository.network.profile_digest,
            consensus_config_digest: self.repository.network.consensus_digest,
        }
    }

    fn graph_limits(&self) -> CompactGraphLimits {
        CompactGraphLimits {
            // The source owns the shared idle decoded-state pool; graph reads retain guards.
            max_hot_state_bytes: 0,
            max_resident_hot_state_bytes: self.repository.graph_resident_bytes,
            ..Default::default()
        }
    }

    fn acquire(&self, binding: &StateBinding) -> Result<Arc<dyn LeasedState>, StateSourceError> {
        self.check_binding(binding).map_err(source_error)?;
        let lease = if let Some(epoch) = binding.checkpoint_epoch() {
            let inner = self.derived.lock().map_err(|_| StateSourceError::Durability)?;
            inner.journal.ensure_healthy().map_err(|_| StateSourceError::Durability)?;
            let record = inner
                .records
                .get(&(binding.origin_block_root(), epoch))
                .ok_or(StateSourceError::Unavailable)?;
            if record.origin_state_root != binding.origin_state_root() ||
                record.state_root != binding.state_root() ||
                record.slot != binding.slot() ||
                record.fork != binding.fork()
            {
                return Err(StateSourceError::BindingMismatch)
            }
            self.check_derived(record).map_err(source_error)?
        } else {
            let inner = self.repository.lock().map_err(source_error)?;
            let record = inner
                .records
                .get(&binding.origin_block_root())
                .ok_or(StateSourceError::Unavailable)?;
            if record.state_root != binding.state_root() ||
                record.slot != binding.slot() ||
                record.fork != binding.fork()
            {
                return Err(StateSourceError::BindingMismatch)
            }
            self.repository
                .compact
                .lease(record.state_root, record.fork, binding.preset())
                .map_err(|_| StateSourceError::Durability)?
                .ok_or(StateSourceError::Unavailable)?
        };
        self.trim_idle_caches()?;
        let allocation = lease.state() as *const BeaconState as usize;
        let key = (binding.state_root(), allocation);
        let mut cache = self.leases.lock().map_err(|_| StateSourceError::Durability)?;
        cache.clock = cache.clock.checked_add(1).ok_or(StateSourceError::ResourceLimit)?;
        let touched = cache.clock;
        if let Some(existing) = cache.entries.get_mut(&key) {
            existing.touched = touched;
            return Ok(existing.owner.clone())
        }
        self.check_leased_binding(binding, &lease).map_err(source_error)?;
        let bytes = StateData::Classic(lease.state())
            .allocation_bytes()
            .map_err(|_| StateSourceError::ResourceLimit)?;
        let owned: Arc<dyn LeasedState> = Arc::new(RepositoryLease(lease));
        // Cached strong wrappers retain the actual storage allocation, whose bytes are already
        // charged by the storage resident lease budget. This separate cap bounds idle ownership.
        let cache_bytes = self.repository.schema_cache_bytes;
        if bytes <= cache_bytes &&
            self.repository
                .compact
                .owns_cached_state(binding.state_root(), owned.state())
                .map_err(|_| StateSourceError::Durability)?
        {
            while cache.bytes.checked_add(bytes).ok_or(StateSourceError::ResourceLimit)? >
                cache_bytes ||
                cache.entries.len() >= self.repository.max_records
            {
                let oldest = cache
                    .entries
                    .iter()
                    .min_by_key(|(_, entry)| entry.touched)
                    .map(|(key, _)| *key)
                    .ok_or(StateSourceError::ResourceLimit)?;
                let removed =
                    cache.entries.remove(&oldest).ok_or(StateSourceError::ResourceLimit)?;
                cache.bytes = cache
                    .bytes
                    .checked_sub(removed.bytes)
                    .ok_or(StateSourceError::ResourceLimit)?;
            }
            cache.bytes += bytes;
            cache.entries.insert(key, CachedLease { owner: owned.clone(), bytes, touched });
        }
        Ok(owned)
    }

    fn retain_checkpoint(
        &self,
        binding: &StateBinding,
        state: StateData<'_>,
    ) -> Result<(), StateSourceError> {
        self.check_binding(binding).map_err(source_error)?;
        self.check_state_binding(binding, state).map_err(source_error)?;
        let epoch = binding.checkpoint_epoch().ok_or(StateSourceError::BindingMismatch)?;
        let origin = self
            .repository
            .block_state(binding.origin_block_root())
            .map_err(source_error)?
            .ok_or(StateSourceError::Unavailable)?;
        let target = epoch
            .checked_mul(self.repository.network.config.slots_per_epoch)
            .ok_or(StateSourceError::BindingMismatch)?;
        if binding.slot() != target ||
            target.checked_sub(origin.state().slot()).is_none_or(|gap| gap > 8192)
        {
            return Err(StateSourceError::BindingMismatch)
        }
        let replayed_root = if target == origin.state().slot() {
            origin.root()
        } else {
            let replayed =
                replay_slots(origin.state(), target, &self.repository.runtime.transitions, 8192)
                    .map_err(|_| StateSourceError::BindingMismatch)?;
            replayed.hash_tree_root().map_err(|_| StateSourceError::BindingMismatch)?
        };
        if replayed_root != binding.state_root() {
            return Err(StateSourceError::BindingMismatch)
        }
        let record = Derived {
            origin_block_root: binding.origin_block_root(),
            origin_state_root: binding.origin_state_root(),
            state_root: binding.state_root(),
            slot: binding.slot(),
            fork: binding.fork(),
            epoch,
        };
        let mut inner = self.derived.lock().map_err(|_| StateSourceError::Durability)?;
        inner.journal.ensure_healthy().map_err(|_| StateSourceError::Durability)?;
        if let Some(saved) = inner.records.get(&(record.origin_block_root, epoch)) {
            if saved != &record {
                return Err(StateSourceError::BindingMismatch)
            }
            self.check_derived(saved).map_err(source_error)?;
            return self.trim_idle_caches()
        }
        if inner.records.len() >= self.repository.max_records {
            return Err(StateSourceError::ResourceLimit)
        }
        persist_data(&self.repository, state, binding.state_root()).map_err(source_error)?;
        let mut next = inner.records.clone();
        next.insert((record.origin_block_root, epoch), record);
        inner
            .journal
            .commit(&DerivedSaved {
                schema: 1,
                consensus_digest: self.repository.network.consensus_digest,
                records: next.values().cloned().collect(),
            })
            .map_err(|_| StateSourceError::Durability)?;
        inner.records = next;
        self.trim_idle_caches()
    }

    fn retention_epoch_limit(&self) -> Result<Option<u64>, StateSourceError> {
        let inner = self.repository.lock().map_err(source_error)?;
        let mut limit = None;
        for snapshot in inner.current.iter().chain(&inner.previous) {
            self.repository.check_snapshot_data(&inner.records, snapshot).map_err(source_error)?;
            limit =
                Some(limit.map_or(snapshot.finalized.epoch, |old: u64| {
                    old.min(snapshot.finalized.epoch)
                }));
        }
        Ok(limit)
    }

    fn retain_finalized(&self, admitted: FinalizedRetention<'_>) -> Result<(), StateSourceError> {
        #[cfg(feature = "peer-runtime")]
        self.finalize_context(&admitted).map_err(source_error)?;
        let origin = admitted.origin_binding();
        let boundary = admitted.boundary_binding();
        let checkpoint = admitted.boundary_checkpoint();
        if origin.origin_block_root() != checkpoint.root ||
            boundary.origin_block_root() != checkpoint.root ||
            boundary.origin_state_root() != origin.state_root() ||
            boundary.checkpoint_epoch() != Some(checkpoint.epoch) ||
            boundary.identity() != self.identity()
        {
            return Err(StateSourceError::BindingMismatch)
        }
        self.check_binding(origin).map_err(source_error)?;
        self.retain_checkpoint(boundary, admitted.boundary_state())?;
        let inner = self.repository.lock().map_err(source_error)?;
        for root in admitted.retained_roots() {
            self.repository
                .check_saved_record(inner.records.get(root).ok_or(StateSourceError::Unavailable)?)
                .map_err(source_error)?;
        }
        for binding in admitted.retained_states() {
            if binding.identity() != self.identity() ||
                !admitted.retained_roots().contains(&binding.origin_block_root())
            {
                return Err(StateSourceError::BindingMismatch)
            }
        }
        let generation = retention::RecoveryGeneration {
            anchor: Anchor {
                checkpoint,
                state_root: origin.state_root(),
                slot: origin.slot(),
                genesis: false,
                boundary: Some(replayed::BoundaryAnchor {
                    state_root: boundary.state_root(),
                    slot: boundary.slot(),
                    fork: boundary.fork(),
                    max_replay_slots: self.repository.runtime.fork_choice.max_tick_slots,
                }),
            },
            roots: admitted.retained_roots().clone(),
        };
        self.repository
            .check_recovery_generation(&inner.records, &generation)
            .map_err(source_error)?;
        drop(inner);
        let mut staged = self.derived.lock().map_err(|_| StateSourceError::Durability)?;
        staged.journal.ensure_healthy().map_err(|_| StateSourceError::Durability)?;
        staged.staged_cleanup = Some(cleanup::RetainedCleanup {
            snapshot: admitted.actual_snapshot(),
            roots: admitted.retained_roots().clone(),
            states: admitted.retained_states().iter().map(StateBinding::state_root).collect(),
        });
        staged.staged_recovery = Some((admitted.actual_snapshot(), generation));
        staged.prepared_closure = None;
        self.trim_idle_caches()
    }

    fn retain_block(&self, admitted: AdmittedState<'_>) -> Result<(), StateSourceError> {
        #[cfg(feature = "peer-runtime")]
        self.retain_context(&admitted).map_err(source_error)?;
        retain_block(&self.repository, admitted).map_err(source_error)?;
        self.trim_idle_caches()
    }

    fn retain_envelope(&self, admitted: VerifiedEnvelope<'_>) -> Result<(), StateSourceError> {
        retain_envelope(&self.repository, admitted).map_err(source_error)?;
        self.trim_idle_caches()
    }

    fn publish_snapshot(&self, admitted: AdmittedSnapshot) -> Result<(), StateSourceError> {
        if admitted.identity() != self.identity() {
            return Err(StateSourceError::IdentityMismatch)
        }
        let snapshot = admitted.snapshot();
        let repository = self.repository.lock().map_err(source_error)?;
        check_snapshot_records(&repository.records, snapshot, |record| {
            self.repository.check_saved_record(record)
        })?;
        drop(repository);
        #[cfg(feature = "peer-runtime")]
        self.restore_context_anchor(&admitted).map_err(source_error)?;
        let mut inner = self.derived.lock().map_err(|_| StateSourceError::Durability)?;
        inner.journal.ensure_healthy().map_err(|_| StateSourceError::Durability)?;
        inner.staged = Some(admitted);
        inner.prepared_closure = None;
        self.trim_idle_caches()
    }
}

#[derive(Debug, Default)]
struct ReplayProofCache {
    entries: BTreeMap<(Root, u64), ReplayProof>,
    bytes: usize,
    clock: u64,
}
#[derive(Debug)]
struct ReplayProof {
    record: Derived,
    post: StateLease,
    boundary: StateLease,
    bytes: usize,
    touched: u64,
}
#[derive(Debug, Default)]
struct LeaseCache {
    entries: BTreeMap<(Root, usize), CachedLease>,
    bytes: usize,
    clock: u64,
}
#[derive(Debug)]
struct CachedLease {
    owner: Arc<dyn LeasedState>,
    bytes: usize,
    touched: u64,
}
#[derive(Debug)]
struct RepositoryLease(StateLease);
impl LeasedState for RepositoryLease {
    fn state(&self) -> &BeaconState {
        self.0.state()
    }
}
#[derive(Debug)]
struct DerivedJournal {
    journal: Journal,
    records: BTreeMap<(Root, u64), Derived>,
    staged: Option<AdmittedSnapshot>,
    prepared_closure: Option<events::PreparedReplayClosure>,
    staged_recovery: Option<(StoreSnapshot, retention::RecoveryGeneration)>,
    staged_cleanup: Option<cleanup::RetainedCleanup>,
    published_cleanup: Option<cleanup::RetainedCleanup>,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DerivedSaved {
    schema: u8,
    consensus_digest: Root,
    records: Vec<Derived>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Derived {
    origin_block_root: Root,
    origin_state_root: Root,
    state_root: Root,
    slot: u64,
    fork: Fork,
    epoch: u64,
}
fn persist_data(
    repository: &AdmittedStateRepository,
    state: StateData<'_>,
    root: Root,
) -> Result<(), PersistenceError> {
    match state {
        StateData::Classic(state) => repository.compact.persist_cached_state(state, root)?,
        StateData::Gloas(state, preset) => {
            repository.compact.persist_cached_gloas(state, preset, root)?
        }
    }
    Ok(())
}
/// Validate each immutable retained record once within this snapshot, without caching authority.
fn check_snapshot_records(
    records: &BTreeMap<Root, Admission>,
    snapshot: StoreSnapshot,
    mut check: impl FnMut(&Admission) -> Result<(), PersistenceError>,
) -> Result<(), StateSourceError> {
    let roots = [
        snapshot.head.root,
        snapshot.justified_checkpoint.root,
        snapshot.finalized_checkpoint.root,
    ];
    // The caller holds the repository lock, so repeated roots refer to the exact same metadata.
    // A subsequent publication always rechecks its records, owners, and complete state pins.
    for (index, root) in roots.iter().enumerate() {
        if !roots[..index].contains(root) {
            check(records.get(root).ok_or(StateSourceError::Unavailable)?).map_err(source_error)?;
        }
    }
    let head = records.get(&snapshot.head.root).ok_or(StateSourceError::Unavailable)?;
    if head.slot != snapshot.head_slot ||
        (snapshot.head.payload_status == Some(PayloadStatus::Full) && head.envelope.is_none())
    {
        return Err(StateSourceError::BindingMismatch)
    }
    Ok(())
}

fn source_error(error: PersistenceError) -> StateSourceError {
    match error {
        PersistenceError::Identity => StateSourceError::IdentityMismatch,
        PersistenceError::Capacity => StateSourceError::ResourceLimit,
        PersistenceError::Linkage |
        PersistenceError::MissingAnchor |
        PersistenceError::MissingPredecessor |
        PersistenceError::UnverifiedEnvelope => StateSourceError::BindingMismatch,
        _ => StateSourceError::Durability,
    }
}

mod cleanup;
mod events;
mod retain;
#[cfg(feature = "peer-runtime")]
mod sidecar_context;
mod slashings;

#[cfg(test)]
mod tests;
