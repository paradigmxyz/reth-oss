//! Incremental content-addressed complete state persistence with independent durable pins.
//!
//! This layer verifies exact SSZ commitments and network identity, not consensus validity.
//! Only the import coordinator may supply a state root established by validated import or
//! authenticated bootstrap. Reconstructed values cannot grant execution or finality authority.
//! All work is blocking and serialized per store; async callers must use a blocking task.
//! Every published root remains reconstructible. Cache eviction never prunes durable ancestry.

use parking_lot::Mutex;
use reth_embedded_beacon_primitives::{container_fields, Fork, Root, SszError};
use reth_embedded_beacon_shared_data::{
    journal::Journal, Encoding, ObjectId, ObjectKind, Owner, SharedDataStore, StoreError,
    StoreIdentity,
};
use reth_embedded_beacon_state_transition::ssz::{gloas, BeaconState, Preset, SchemaError, Ssz};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Weak},
};

mod cache;
mod graph;

/// Durable component graphs and a bounded complete-state lease cache in a private namespace.
#[derive(Debug, Clone)]
pub struct CompactStateStore {
    shared: SharedDataStore,
    inner: Arc<Mutex<Inner>>,
}

impl CompactStateStore {
    /// Open the existing shared byte store under its checked network identity.
    /// Validate every published graph before releasing unpublished compact owners after a crash.
    pub fn open(
        shared: SharedDataStore,
        limits: CompactStateLimits,
    ) -> Result<Self, CompactStateError> {
        limits.validate()?;
        shared.ensure_healthy()?;
        let identity = shared.identity();
        let (mut journal, restored) = Journal::open::<StateJournal>(
            shared.namespace_root().join("beacon-state-components-v1/journal"),
            identity,
        )?;
        let initialize = restored.is_none();
        let state = restored.unwrap_or_default();
        if state.records.len() > limits.max_retained_states {
            return Err(CompactStateError::Capacity)
        }
        let mut records = BTreeMap::new();
        for record in state.records {
            record.validate(identity, limits)?;
            if records.insert(record.state_root, record).is_some() {
                return Err(CompactStateError::InvalidDescriptor)
            }
        }
        if initialize {
            journal.commit(&StateJournal::default())?;
        }
        let store = Self {
            shared,
            inner: Arc::new(Mutex::new(Inner {
                journal,
                records,
                limits,
                cache: cache::Cache::new(
                    limits.hot_cache_schema_bytes,
                    limits.max_resident_schema_bytes,
                ),
            })),
        };
        store.release_unpublished_owners()?;
        Ok(store)
    }

    /// Persist an already validated complete state against an independently supplied commitment.
    /// The caller owns consensus authentication. This method checks structural SSZ and the full
    /// state root, publishes all borrowed fragments and direct-child pins in one generation,
    /// then commits the reconstruction descriptor pointer last.
    pub fn persist_state(
        &self,
        state: &BeaconState,
        committed_root: Root,
    ) -> Result<(), CompactStateError> {
        if state.hash_tree_root()? != committed_root {
            return Err(CompactStateError::RootMismatch)
        }
        let bytes = state.to_ssz()?;
        let record = StateRecord::for_encoding(
            committed_root,
            state.genesis_validators_root(),
            state.slot(),
            state.schema_fork(),
            state.preset(),
            bytes.len(),
        );
        self.persist_encoding(&bytes, record)
    }

    /// Persist the exact borrowed Gloas schema without cloning a complete enum wrapper.
    /// As with `persist_state`, the caller owns consensus authentication. The selected preset,
    /// complete commitment, network identity, field offsets and durable pins are checked here.
    pub fn persist_gloas(
        &self,
        state: &gloas::BeaconState,
        preset: Preset,
        committed_root: Root,
    ) -> Result<(), CompactStateError> {
        if state.hash_tree_root(preset)? != committed_root {
            return Err(CompactStateError::RootMismatch)
        }
        let bytes = state.to_ssz(preset)?;
        let record = StateRecord::for_encoding(
            committed_root,
            state.genesis_validators_root,
            state.slot,
            Fork::Gloas,
            preset,
            bytes.len(),
        );
        self.persist_encoding(&bytes, record)
    }

    fn persist_encoding(
        &self,
        bytes: &[u8],
        mut record: StateRecord,
    ) -> Result<(), CompactStateError> {
        let mut inner = self.inner.lock();
        inner.journal.ensure_healthy()?;
        self.shared.ensure_healthy()?;
        if record.genesis_root != self.shared.identity().genesis_root {
            return Err(CompactStateError::IdentityMismatch)
        }
        if bytes.len() > inner.limits.max_state_bytes {
            return Err(CompactStateError::Capacity)
        }
        if !inner.records.contains_key(&record.state_root) &&
            inner.records.len() >= inner.limits.max_retained_states
        {
            return Err(CompactStateError::Capacity)
        }
        let layout = BeaconState::container_field_layout(record.fork, record.preset.to_preset());
        let fields = container_fields(bytes, &layout)?;
        let publication = graph::Publication::build(&fields, &record)?;
        record.manifest = publication.root;
        if let Some(existing) = inner.records.get(&record.state_root) {
            return if existing == &record {
                Ok(())
            } else {
                Err(CompactStateError::ConflictingState)
            }
        }
        self.shared.put_committed_pinned_batch(&publication.borrowed())?;
        let mut next = inner.records.clone();
        next.insert(record.state_root, record);
        inner.journal.commit(&StateJournal { records: next.values().cloned().collect() })?;
        inner.records = next;
        Ok(())
    }

    /// Reconstruct and verify one exact complete state under a caller-selected fork and preset.
    /// A lease is immutable ordinary data, not a consensus authentication token. Resident state
    /// allocations held by cache and active readers are counted once. One serialized and typed
    /// cold-reconstruction temporary is excluded from that retained budget and bounded separately
    /// by `max_state_bytes` and the selected complete schema; it never enters an async task.
    pub fn lease(
        &self,
        root: Root,
        fork: Fork,
        preset: Preset,
    ) -> Result<Option<StateLease>, CompactStateError> {
        self.lease_checked(root, fork, preset, false)
    }

    /// Verify independent durable pins and obtain the exact complete-state read lease together.
    /// Cold reconstruction checks every component and direct-child edge in its existing pass,
    /// avoiding a second complete graph read before decoding. Cached decoded values still require
    /// the complete durable graph check; cache ownership never proves retained reconstruction.
    pub fn lease_retained(
        &self,
        root: Root,
        fork: Fork,
        preset: Preset,
    ) -> Result<Option<StateLease>, CompactStateError> {
        self.lease_checked(root, fork, preset, true)
    }

    fn lease_checked(
        &self,
        root: Root,
        fork: Fork,
        preset: Preset,
        require_retained: bool,
    ) -> Result<Option<StateLease>, CompactStateError> {
        let mut inner = self.inner.lock();
        inner.journal.ensure_healthy()?;
        self.shared.ensure_healthy()?;
        let Some(record) = inner.records.get(&root).cloned() else { return Ok(None) };
        if record.fork != fork || record.preset.to_preset() != preset {
            return Err(CompactStateError::SchemaMismatch)
        }
        if let Some(lease) = inner.cache.get(root) {
            if require_retained {
                let selected = BTreeMap::from([(root, record)]);
                graph::live_nodes(&self.shared, &selected)?;
            }
            return Ok(Some(lease))
        }
        if require_retained &&
            self.shared.owner_objects(Owner::CompactState(root)) != vec![record.manifest]
        {
            return Err(CompactStateError::InvalidDescriptor)
        }
        let layout = BeaconState::container_field_layout(fork, preset);
        let bytes =
            graph::reconstruct(&self.shared, &record, &layout, inner.limits.max_state_bytes)?;
        let state = BeaconState::from_ssz(&bytes, fork, preset)?;
        if state.genesis_validators_root() != record.genesis_root ||
            state.genesis_validators_root() != self.shared.identity().genesis_root ||
            state.slot() != record.slot ||
            state.hash_tree_root()? != root
        {
            return Err(CompactStateError::RootMismatch)
        }
        Ok(Some(inner.cache.admit(root, state)?))
    }

    /// Release only compact owners left by unpublished interrupted generations.
    /// Validate all committed ancestry before mutation. Physical shared garbage collection stays
    /// separate; execution pool, consensus and authenticated checkpoint owners remain untouched.
    pub fn release_unpublished_owners(&self) -> Result<usize, CompactStateError> {
        let inner = self.inner.lock();
        inner.journal.ensure_healthy()?;
        self.shared.ensure_healthy()?;
        let live = graph::live_nodes(&self.shared, &inner.records)?;
        let abandoned = self
            .shared
            .owners()
            .into_iter()
            .filter(|owner| match owner {
                Owner::CompactState(root) => !inner.records.contains_key(root),
                Owner::CompactNode(parent) => !live.contains(parent),
                _ => false,
            })
            .collect::<Vec<_>>();
        if !abandoned.is_empty() {
            self.shared.release_owners(&abandoned)?;
        }
        Ok(abandoned.len())
    }

    /// Verify that this root still has its complete durable graph and independent pins.
    /// A cached decoded lease alone does not prove the backend retains all reconstruction bytes.
    pub fn ensure_retained(&self, root: Root) -> Result<(), CompactStateError> {
        let inner = self.inner.lock();
        inner.journal.ensure_healthy()?;
        self.shared.ensure_healthy()?;
        let record = inner.records.get(&root).ok_or(CompactStateError::InvalidDescriptor)?;
        let selected = BTreeMap::from([(root, record.clone())]);
        graph::live_nodes(&self.shared, &selected)?;
        Ok(())
    }

    /// Remove obsolete data descriptors after the coordinator publishes its complete recovery
    /// union. The keep set must include every current/previous recovery and unfinalized branch
    /// state. This data-only API does not establish finality or choose which consensus ancestry
    /// to prune. Descriptor metadata commits before owner release; active decoded leases remain
    /// valid.
    pub fn release_retained_states(
        &self,
        keep: &BTreeSet<Root>,
    ) -> Result<usize, CompactStateError> {
        let mut inner = self.inner.lock();
        inner.journal.ensure_healthy()?;
        self.shared.ensure_healthy()?;
        if keep.iter().any(|root| !inner.records.contains_key(root)) {
            return Err(CompactStateError::InvalidDescriptor)
        }
        // Damaged committed data must never be hidden by deleting its descriptor.
        graph::live_nodes(&self.shared, &inner.records)?;
        let next = inner
            .records
            .iter()
            .filter(|(root, _)| keep.contains(*root))
            .map(|(root, record)| (*root, record.clone()))
            .collect::<BTreeMap<_, _>>();
        let removed = inner.records.len() - next.len();
        if removed == 0 {
            return Ok(0)
        }
        let live = graph::live_nodes(&self.shared, &next)?;
        let abandoned = self
            .shared
            .owners()
            .into_iter()
            .filter(|owner| match owner {
                Owner::CompactState(root) => !next.contains_key(root),
                Owner::CompactNode(parent) => !live.contains(parent),
                _ => false,
            })
            .collect::<Vec<_>>();
        inner.journal.commit(&StateJournal { records: next.values().cloned().collect() })?;
        inner.records = next;
        inner.cache.retain_roots(keep);
        // An uncertain release fails closed. Reopen validates the published generation and
        // finishes only obsolete compact-owner cleanup before exposing reconstruction or GC.
        if !abandoned.is_empty() {
            self.shared.release_owners(&abandoned)?;
        }
        Ok(removed)
    }

    /// Retained complete-state schema allocations, excluding allocator bookkeeping, byte cache,
    /// transient reconstruction buffers and the caller's independent input state.
    pub fn stats(&self) -> Result<CompactStateStats, CompactStateError> {
        let inner = self.inner.lock();
        inner.journal.ensure_healthy()?;
        self.shared.ensure_healthy()?;
        Ok(inner.cache.stats(inner.records.len()))
    }

    /// Persist a fully checked state and optionally seed its bounded decoded cache allocation.
    /// This avoids immediately reconstructing, decoding and rehashing the just-published bytes.
    /// All complete-root, encoding, network, graph and durable publication checks still run.
    /// Cache admission is optional and cannot turn resident-cache pressure into a write failure.
    pub fn persist_cached_state(
        &self,
        state: &BeaconState,
        committed_root: Root,
    ) -> Result<(), CompactStateError> {
        self.persist_state(state, committed_root)?;
        let bytes = state
            .heap_bytes()?
            .checked_add(std::mem::size_of::<BeaconState>())
            .ok_or(CompactStateError::Capacity)?;
        self.cache_verified_state(committed_root, bytes, || Arc::new(state.clone()))
    }

    /// Persist the borrowed Gloas schema and optionally seed the same bounded decoded cache.
    /// The original full root is checked before cloning this immutable cache allocation.
    pub fn persist_cached_gloas(
        &self,
        state: &gloas::BeaconState,
        preset: Preset,
        committed_root: Root,
    ) -> Result<(), CompactStateError> {
        self.persist_gloas(state, preset, committed_root)?;
        let bytes = state
            .heap_bytes()?
            .checked_add(std::mem::size_of::<BeaconState>())
            .ok_or(CompactStateError::Capacity)?;
        self.cache_verified_state(committed_root, bytes, || {
            Arc::new(BeaconState::Gloas(state.clone(), preset))
        })
    }

    fn cache_verified_state(
        &self,
        root: Root,
        bytes: usize,
        state: impl FnOnce() -> Arc<BeaconState>,
    ) -> Result<(), CompactStateError> {
        let mut inner = self.inner.lock();
        inner.journal.ensure_healthy()?;
        self.shared.ensure_healthy()?;
        if !inner.records.contains_key(&root) ||
            bytes > inner.limits.hot_cache_schema_bytes ||
            inner.cache.get(root).is_some()
        {
            return Ok(())
        }
        // The cloned exact checked value enters the existing resident accounting; it never
        // changes durable ownership or consensus authority. The temporary clone is bounded by
        // the same complete-schema limit as an ordinary cold reconstruction.
        match inner.cache.admit_shared(root, state()) {
            Ok(_) | Err(CompactStateError::Capacity) => Ok(()),
            Err(error) => Err(error),
        }
    }

    /// Persist and independently validate the complete commitment of an immutable shared owner.
    /// Cache admission shares this exact allocation instead of cloning its complete schema.
    /// Arbitrary caller-owned values still undergo the same full root and durable graph checks.
    pub fn persist_cached_owned_state(
        &self,
        state: Arc<BeaconState>,
        committed_root: Root,
    ) -> Result<(), CompactStateError> {
        self.persist_state(&state, committed_root)?;
        let bytes = state
            .heap_bytes()?
            .checked_add(std::mem::size_of::<BeaconState>())
            .ok_or(CompactStateError::Capacity)?;
        self.cache_verified_state(committed_root, bytes, || state)
    }

    /// Whether the decoded cache itself owns this exact immutable state allocation.
    /// Data adapters use this only to avoid extending idle lease ownership beyond this cache's
    /// budget. Membership grants no consensus authority and never substitutes for durable checks.
    pub fn owns_cached_state(
        &self,
        root: Root,
        state: &BeaconState,
    ) -> Result<bool, CompactStateError> {
        let inner = self.inner.lock();
        inner.journal.ensure_healthy()?;
        self.shared.ensure_healthy()?;
        Ok(inner.cache.owns_cached_state(root, state))
    }
}

/// Resource limits; none is permission to discard an unfinalized state or weaken verification.
#[derive(Debug, Clone, Copy)]
pub struct CompactStateLimits {
    /// Maximum complete serialized state. Offset encoding still imposes its native 32-bit bound.
    pub max_state_bytes: usize,
    /// Maximum durable root records; admission fails when full rather than dropping a branch.
    pub max_retained_states: usize,
    /// Complete state allocations kept by hot cache ownership.
    pub hot_cache_schema_bytes: usize,
    /// Complete allocations kept by cache and active leases, counted once per allocation.
    pub max_resident_schema_bytes: usize,
}
impl Default for CompactStateLimits {
    fn default() -> Self {
        Self {
            max_state_bytes: 512 * 1024 * 1024,
            max_retained_states: 16_384,
            hot_cache_schema_bytes: 64 * 1024 * 1024,
            max_resident_schema_bytes: 2 * 1024 * 1024 * 1024,
        }
    }
}
impl CompactStateLimits {
    const fn validate(self) -> Result<(), CompactStateError> {
        if self.max_state_bytes == 0 ||
            self.max_state_bytes > 2 * 1024 * 1024 * 1024 ||
            self.max_retained_states == 0 ||
            self.max_retained_states > 65_536 ||
            self.max_resident_schema_bytes == 0 ||
            self.hot_cache_schema_bytes > self.max_resident_schema_bytes
        {
            return Err(CompactStateError::Capacity)
        }
        Ok(())
    }
}

/// Exact complete-state read lease; it remains valid after cache eviction or store closure.
#[derive(Debug, Clone)]
pub struct StateLease {
    allocation: Arc<cache::Allocation>,
}
impl StateLease {
    /// Borrow decoded complete schema data without extracting the cache's authoritative owner.
    /// This store has no execution, transition or finality authority.
    pub fn state(&self) -> &BeaconState {
        &self.allocation.state
    }
    /// Independently checked complete SSZ commitment.
    pub fn root(&self) -> Root {
        self.allocation.root
    }
    /// Weak data-allocation identity for bounded memoization; does not retain a decoded state.
    /// A match grants no durable ownership, transition, execution or finality authority.
    pub fn downgrade(&self) -> WeakStateLease {
        WeakStateLease { allocation: Arc::downgrade(&self.allocation) }
    }
}

/// Opaque weak identity of one immutable complete-state allocation.
/// Reconstructed equal bytes in a new allocation never match an expired token.
#[derive(Debug, Clone)]
pub struct WeakStateLease {
    allocation: Weak<cache::Allocation>,
}
impl WeakStateLease {
    /// Compare exact owning allocations; callers must separately recheck durable graph and pins.
    pub fn matches(&self, lease: &StateLease) -> bool {
        self.allocation.ptr_eq(&Arc::downgrade(&lease.allocation))
    }
}

/// Cache and active lease measurements. Cache bytes are a subset of resident bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompactStateStats {
    /// Durable complete state roots, including every retained branch and checkpoint.
    pub retained_states: usize,
    /// Complete states held by the hot cache.
    pub cached_states: usize,
    /// Complete cached allocation capacities and inline wrappers.
    pub cached_schema_bytes: usize,
    /// Unique complete allocations held by cache and active leases.
    pub resident_schema_bytes: usize,
}

/// Integrity, capacity and persistence failures. None admits partial reconstruction.
#[derive(Debug, thiserror::Error)]
pub enum CompactStateError {
    /// Contextual shared object or journal failure.
    #[error(transparent)]
    Shared(#[from] StoreError),
    /// Exact complete schema decoding failure.
    #[error(transparent)]
    Schema(#[from] SchemaError),
    /// Canonical SSZ or checked root failure.
    #[error(transparent)]
    Ssz(#[from] SszError),
    /// Invalid bounded descriptor, graph topology, offsets or direct-child pins.
    #[error("invalid compact state reconstruction descriptor")]
    InvalidDescriptor,
    /// Required immutable component is absent.
    #[error("missing compact state component: {0:?}")]
    MissingComponent(ObjectId),
    /// Serialized, retained-root, cache or active-lease admission exceeds its explicit budget.
    #[error("compact state resource limit exceeded")]
    Capacity,
    /// State belongs to a different authenticated network/configuration identity.
    #[error("compact state network identity differs")]
    IdentityMismatch,
    /// Reconstructed complete root or checked slot differs from its committed record.
    #[error("compact state complete commitment differs")]
    RootMismatch,
    /// Caller-selected fork/preset differs from the immutable saved interpretation.
    #[error("compact state fork or preset differs")]
    SchemaMismatch,
    /// The same state root cannot acquire different bytes, schema or slot metadata.
    #[error("conflicting compact state root")]
    ConflictingState,
}

#[derive(Debug)]
struct Inner {
    journal: Journal,
    records: BTreeMap<Root, StateRecord>,
    limits: CompactStateLimits,
    cache: cache::Cache,
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StateJournal {
    records: Vec<StateRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StateRecord {
    schema_version: u8,
    state_root: Root,
    genesis_root: Root,
    slot: u64,
    fork: Fork,
    preset: StoredPreset,
    ssz_bytes: u64,
    manifest: ObjectId,
}
impl StateRecord {
    const fn for_encoding(
        state_root: Root,
        genesis_root: Root,
        slot: u64,
        fork: Fork,
        preset: Preset,
        ssz_bytes: usize,
    ) -> Self {
        Self {
            schema_version: 1,
            state_root,
            genesis_root,
            slot,
            fork,
            preset: StoredPreset::from_preset(preset),
            ssz_bytes: ssz_bytes as u64,
            manifest: ObjectId {
                kind: ObjectKind::BeaconStateManifest,
                encoding: Encoding::Raw,
                digest: [0; 32],
            },
        }
    }
    fn validate(
        &self,
        identity: StoreIdentity,
        limits: CompactStateLimits,
    ) -> Result<(), CompactStateError> {
        if self.genesis_root != identity.genesis_root {
            return Err(CompactStateError::IdentityMismatch)
        }
        if self.schema_version != 1 ||
            self.manifest.kind != ObjectKind::BeaconStateManifest ||
            self.manifest.encoding != Encoding::Raw
        {
            return Err(CompactStateError::InvalidDescriptor)
        }
        if self.ssz_bytes == 0 || self.ssz_bytes > limits.max_state_bytes as u64 {
            return Err(CompactStateError::Capacity)
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum StoredPreset {
    Mainnet,
    Minimal,
}
impl StoredPreset {
    const fn from_preset(preset: Preset) -> Self {
        match preset {
            Preset::Mainnet => Self::Mainnet,
            Preset::Minimal => Self::Minimal,
        }
    }
    const fn to_preset(self) -> Preset {
        match self {
            Self::Mainnet => Preset::Mainnet,
            Self::Minimal => Preset::Minimal,
        }
    }
    const fn tag(self) -> u8 {
        match self {
            Self::Mainnet => 0,
            Self::Minimal => 1,
        }
    }
    const fn from_tag(tag: u8) -> Result<Self, CompactStateError> {
        match tag {
            0 => Ok(Self::Mainnet),
            1 => Ok(Self::Minimal),
            _ => Err(CompactStateError::InvalidDescriptor),
        }
    }
}

#[cfg(test)]
mod tests;
