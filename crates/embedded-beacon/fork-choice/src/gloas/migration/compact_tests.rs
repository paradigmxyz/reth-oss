//! Genuine strict imports/votes with isolated data-source failures and decoded eviction.
//! The recording source checks the adapter contract; real disk/pin durability is covered by node
//! repository tests. It supplies no signatures, Engine verdicts, finality or graph authority.
use super::*;
use crate::{
    compact::{
        AdmittedSnapshot, AdmittedState, CompactGloasStore, CompactGraphLimits, CompactStateSource,
        CompactStore, FinalizedRetention, FinalizedRetentionPolicy, LeasedState,
        RetainedExecutionAssociation, RetainedState, StateBinding, StateData, StateSourceError,
        StateSourceIdentity, VerifiedEnvelope,
    },
    gloas::body_attestations::tests::{
        corrupt_last_shape, corrupt_middle_signature, retained_vote_graph,
    },
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::atomic::AtomicBool,
};

#[derive(Clone, Debug)]
struct SavedData {
    binding: StateBinding,
    bytes: Vec<u8>,
}
#[derive(Debug)]
struct RecordingSource {
    states: Mutex<BTreeMap<Root, SavedData>>,
    pins: Mutex<BTreeSet<Root>>,
    snapshots: Mutex<Vec<StoreSnapshot>>,
    cache: Mutex<LeaseCache>,
    envelopes: Mutex<BTreeSet<Root>>,
    retained_execution: Mutex<Vec<RetainedExecutionAssociation>>,
    fail_publish: AtomicBool,
    fail_retain: AtomicBool,
}
#[derive(Debug, Default)]
struct LeaseCache {
    // Eight actual immutable allocations, independent of durable root/encoded-byte membership.
    allocations: BTreeMap<Root, (Root, Arc<dyn LeasedState>, u64)>,
    generation: u64,
}
#[derive(Debug)]
struct TestLease(AnyState);
impl LeasedState for TestLease {
    fn state(&self) -> &AnyState {
        &self.0
    }
}
impl RecordingSource {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            states: Mutex::new(BTreeMap::new()),
            pins: Mutex::new(BTreeSet::new()),
            snapshots: Mutex::new(Vec::new()),
            cache: Mutex::new(LeaseCache::default()),
            envelopes: Mutex::new(BTreeSet::new()),
            retained_execution: Mutex::new(Vec::new()),
            fail_publish: AtomicBool::new(false),
            fail_retain: AtomicBool::new(false),
        })
    }
    fn seed_classic(&self, graph: &Store) {
        for root in graph.migration_roots() {
            let Some(state) = graph.read_block_state(root).unwrap() else { continue };
            let binding = StateBinding::admitted(
                root,
                graph.block(root).unwrap().header().unwrap().state_root,
                state.data(),
                self.identity(),
            )
            .unwrap();
            self.save(&binding, state.data()).unwrap();
        }
    }
    fn seed_gloas(&self, graph: &GloasStore) {
        if let Some(history) = &graph.historical {
            self.seed_classic(history);
        }
        for (&root, block) in &graph.blocks {
            let state = graph.read_block_state(root).unwrap().unwrap();
            let binding =
                StateBinding::admitted(root, block.state_root, state.data(), self.identity())
                    .unwrap();
            self.save(&binding, state.data()).unwrap();
        }
    }
    fn save(&self, binding: &StateBinding, state: StateData<'_>) -> Result<(), StateSourceError> {
        if self.fail_retain.load(Ordering::SeqCst) {
            return Err(StateSourceError::Durability)
        }
        binding.check(state).map_err(|_| StateSourceError::BindingMismatch)?;
        let mut states = self.states.lock().unwrap();
        if states.len() >= 128 && !states.contains_key(&binding.state_root()) {
            return Err(StateSourceError::ResourceLimit)
        }
        let saved = SavedData {
            binding: binding.clone(),
            bytes: state.to_ssz().map_err(|_| StateSourceError::BindingMismatch)?,
        };
        states.insert(binding.state_root(), saved);
        self.pins.lock().unwrap().insert(binding.state_root());
        Ok(())
    }
}
impl CompactStateSource for RecordingSource {
    fn identity(&self) -> StateSourceIdentity {
        StateSourceIdentity { profile_digest: [1; 32], consensus_config_digest: [2; 32] }
    }
    fn acquire(&self, binding: &StateBinding) -> Result<Arc<dyn LeasedState>, StateSourceError> {
        if binding.identity() != self.identity() {
            return Err(StateSourceError::IdentityMismatch)
        }
        if !self.pins.lock().unwrap().contains(&binding.state_root()) {
            return Err(StateSourceError::Durability)
        }
        let states = self.states.lock().unwrap();
        let saved = states.get(&binding.state_root()).ok_or(StateSourceError::Unavailable)?;
        if saved.binding.origin_block_root() != binding.origin_block_root() ||
            saved.binding.origin_state_root() != binding.origin_state_root()
        {
            return Err(StateSourceError::BindingMismatch)
        }
        // Still inspect the actual saved bytes on every read, so corruption injection cannot
        // silently use a former valid decoded allocation. Full decode/root validation occurs
        // again for changed bytes, a new owner, or an evicted immutable allocation.
        let digest = reth_embedded_beacon_primitives::sha256(&saved.bytes);
        let mut cache = self.cache.lock().unwrap();
        cache.generation =
            cache.generation.checked_add(1).ok_or(StateSourceError::ResourceLimit)?;
        let generation = cache.generation;
        if let Some((cached_digest, allocation, touched)) =
            cache.allocations.get_mut(&binding.state_root()) &&
            *cached_digest == digest
        {
            *touched = generation;
            return Ok(allocation.clone())
        }
        let state = AnyState::from_ssz(&saved.bytes, binding.fork(), binding.preset())
            .map_err(|_| StateSourceError::BindingMismatch)?;
        let allocation = Arc::new(TestLease(state)) as Arc<dyn LeasedState>;
        cache.allocations.insert(binding.state_root(), (digest, allocation.clone(), generation));
        while cache.allocations.len() > 8 {
            let oldest = cache
                .allocations
                .iter()
                .min_by_key(|(_, (_, _, touched))| *touched)
                .map(|(root, _)| *root)
                .unwrap();
            cache.allocations.remove(&oldest);
        }
        Ok(allocation)
    }
    fn retain_block(&self, admitted: AdmittedState<'_>) -> Result<(), StateSourceError> {
        if admitted.signed_block().message_root().map_err(|_| StateSourceError::BindingMismatch)? !=
            admitted.binding().origin_block_root()
        {
            return Err(StateSourceError::BindingMismatch)
        }
        self.save(admitted.binding(), admitted.state())
    }
    fn retain_checkpoint(
        &self,
        binding: &StateBinding,
        state: StateData<'_>,
    ) -> Result<(), StateSourceError> {
        if binding.checkpoint_epoch().is_none() {
            return Err(StateSourceError::BindingMismatch)
        }
        let states = self.states.lock().unwrap();
        let origin =
            states.get(&binding.origin_state_root()).ok_or(StateSourceError::Unavailable)?;
        if origin.binding.origin_block_root() != binding.origin_block_root() {
            return Err(StateSourceError::BindingMismatch)
        }
        drop(states);
        self.save(binding, state)
    }
    fn publish_snapshot(&self, admitted: AdmittedSnapshot) -> Result<(), StateSourceError> {
        if self.fail_publish.load(Ordering::SeqCst) {
            return Err(StateSourceError::Durability)
        }
        if admitted.identity() != self.identity() {
            return Err(StateSourceError::IdentityMismatch)
        }
        let snapshot = admitted.snapshot();
        let states = self.states.lock().unwrap();
        for root in [
            snapshot.head.root,
            snapshot.justified_checkpoint.root,
            snapshot.finalized_checkpoint.root,
        ] {
            let record = states
                .values()
                .find(|s| s.binding.origin_block_root() == root)
                .ok_or(StateSourceError::Unavailable)?;
            if !self.pins.lock().unwrap().contains(&record.binding.state_root()) {
                return Err(StateSourceError::Durability)
            }
        }
        if snapshot.head.payload_status == Some(PayloadStatus::Full) &&
            !self.envelopes.lock().unwrap().contains(&snapshot.head.root)
        {
            return Err(StateSourceError::Unavailable)
        }
        self.snapshots.lock().unwrap().push(snapshot);
        Ok(())
    }
    fn retain_finalized(&self, admitted: FinalizedRetention<'_>) -> Result<(), StateSourceError> {
        self.save(admitted.boundary_binding(), admitted.boundary_state())?;
        if admitted.boundary_checkpoint().epoch >
            admitted.actual_snapshot().finalized_checkpoint.epoch ||
            !admitted.retained_roots().contains(&admitted.origin_binding().origin_block_root())
        {
            return Err(StateSourceError::BindingMismatch)
        }
        for binding in admitted.retained_states() {
            if !admitted.retained_roots().contains(&binding.origin_block_root()) {
                return Err(StateSourceError::BindingMismatch)
            }
            self.acquire(binding)?;
        }
        self.retained_execution.lock().unwrap().push(admitted.execution_association());
        Ok(())
    }
    fn retain_envelope(&self, admitted: VerifiedEnvelope<'_>) -> Result<(), StateSourceError> {
        if admitted.signed().message.beacon_block_root != admitted.binding().origin_block_root() ||
            admitted.signed().message.payload.block_hash != admitted.verified_execution_hash()
        {
            return Err(StateSourceError::BindingMismatch)
        }
        self.envelopes.lock().unwrap().insert(admitted.binding().origin_block_root());
        Ok(())
    }
}
fn cold_limits() -> CompactGraphLimits {
    CompactGraphLimits { max_hot_state_bytes: 0, ..Default::default() }
}
fn classic() -> (Store, Vec<SecretKey>, TransitionParameters, Arc<RecordingSource>) {
    let (graph, keys, transitions) = real_anchor(Fork::Phase0);
    let source = RecordingSource::new();
    source.seed_classic(&graph);
    (graph, keys, transitions, source)
}
#[test]
fn cold_parent_eviction_reloads_and_real_signed_children_match_the_hot_graph() {
    let (mut hot, keys, transitions, source) = classic();
    let anchor = hot.get_head().unwrap().root;
    let original = hot.block_state(anchor).unwrap().to_ssz().unwrap();
    let mut cold = CompactStore::from_store(hot.clone(), source.clone(), cold_limits()).unwrap();
    assert_eq!(cold.stats().unwrap().hot_states, 0);
    for slot in 1..=3 {
        let signed = ssz::SignedBeaconBlock::Phase0(
            phase0_signed_child(&hot, &keys, &transitions),
            Preset::Minimal,
        );
        hot.on_tick(slot * 6000).unwrap();
        cold.on_tick(slot * 6000).unwrap();
        hot.on_block(&signed).unwrap();
        cold.on_block(&signed).unwrap();
        assert_eq!(cold.snapshot().unwrap(), hot.snapshot().unwrap());
        assert_eq!(cold.viable_leaf_weights().unwrap(), hot.viable_leaf_weights().unwrap());
        let root = signed.message_root().unwrap();
        assert_eq!(
            cold.read_block_state(root).unwrap().unwrap().to_ssz().unwrap(),
            hot.block_state(root).unwrap().to_ssz().unwrap()
        );
        assert_eq!(cold.stats().unwrap().hot_states, 0);
        cold.evict_hot_states().unwrap();
    }
    assert_eq!(cold.read_block_state(anchor).unwrap().unwrap().to_ssz().unwrap(), original);
    let stats = cold.stats().unwrap();
    assert!(stats.cold_entries >= 4);
    assert_eq!(stats.hot_schema_bytes, 0);
    assert_eq!(
        stats.cold_binding_heap_bytes,
        stats.cold_entries * std::mem::size_of::<StateBinding>()
    );
    assert!(std::mem::size_of::<RetainedState<AnyState>>() <= 6 * std::mem::size_of::<usize>());
    assert_eq!(source.states.lock().unwrap().len(), 4);
}
#[test]
fn genuine_signed_vote_after_complete_eviction_preserves_real_head_weights_and_invalid_bls_rollback(
) {
    let (mut hot, keys, transitions, source) = classic();
    let anchor = hot.get_head().unwrap().root;
    let mut cold = CompactStore::from_store(hot.clone(), source, cold_limits()).unwrap();
    let first = ssz::SignedBeaconBlock::Phase0(
        phase0_signed_child(&hot, &keys, &transitions),
        Preset::Minimal,
    );
    hot.on_tick(6000).unwrap();
    hot.on_block(&first).unwrap();
    cold.on_tick(6000).unwrap();
    cold.on_block(&first).unwrap();
    let voted = hot.get_head().unwrap().root;
    hot.on_tick(3 * 6000).unwrap();
    cold.on_tick(3 * 6000).unwrap();
    let AnyState::Phase0(state, _) = hot.block_state(anchor).unwrap() else {
        panic!("real Phase0")
    };
    let committee = phase0_processing::get_beacon_committee(
        state,
        2,
        0,
        &transitions.gloas.fulu.electra.deneb.capella.bellatrix.altair.phase0,
    )
    .unwrap();
    let data = phase0::AttestationData {
        slot: 2,
        index: 0,
        beacon_block_root: voted,
        source: state.current_justified_checkpoint,
        target: Checkpoint { epoch: 0, root: anchor },
    };
    let vote = phase0::Attestation {
        aggregation_bits: phase0::AggregationBits::from_ssz(
            &all_bits(committee.len()),
            Preset::Minimal,
        )
        .unwrap(),
        signature: aggregate(
            &committee,
            &keys,
            signing_root(
                data.hash_tree_root(Preset::Minimal).unwrap(),
                [1, 0, 0, 0],
                state.fork.current_version,
                state.genesis_validators_root,
            ),
        ),
        data,
    };
    hot.on_attestation(&vote).unwrap();
    cold.evict_hot_states().unwrap();
    cold.on_attestation(&vote).unwrap();
    assert_eq!(cold.viable_leaf_weights().unwrap(), hot.viable_leaf_weights().unwrap());
    let before = cold.snapshot().unwrap();
    let weights = cold.viable_leaf_weights().unwrap();
    let mut invalid = vote.clone();
    invalid.signature = [0; 96];
    assert!(cold.on_attestation(&invalid).is_err());
    assert_eq!(cold.snapshot().unwrap(), before);
    assert_eq!(cold.viable_leaf_weights().unwrap(), weights);
}
#[test]
fn known_cold_missing_pin_and_changed_bytes_fail_closed_while_unknown_root_stays_absent() {
    let (hot, keys, transitions, source) = classic();
    let root = hot.get_head().unwrap().root;
    let committed = hot.block_state(root).unwrap().hash_tree_root().unwrap();
    let mut cold = CompactStore::from_store(hot.clone(), source.clone(), cold_limits()).unwrap();
    assert!(cold.read_block_state([99; 32]).unwrap().is_none());
    let before = cold.snapshot().unwrap();
    source.pins.lock().unwrap().remove(&committed);
    assert!(matches!(
        cold.read_block_state(root),
        Err(StoreError::CompactState(StateSourceError::Durability))
    ));
    assert_eq!(cold.snapshot().unwrap(), before);
    source.pins.lock().unwrap().insert(committed);
    source.states.lock().unwrap().get_mut(&committed).unwrap().bytes[0] ^= 1;
    assert!(matches!(
        cold.read_block_state(root),
        Err(StoreError::CompactState(StateSourceError::BindingMismatch))
    ));
    cold.on_tick(6000).unwrap();
    let signed = ssz::SignedBeaconBlock::Phase0(
        phase0_signed_child(&hot, &keys, &transitions),
        Preset::Minimal,
    );
    let before = cold.snapshot().unwrap();
    assert!(matches!(
        cold.on_block(&signed),
        Err(StoreError::CompactState(StateSourceError::BindingMismatch))
    ));
    assert_eq!(cold.snapshot().unwrap(), before);
    assert_eq!(cold.stats().unwrap().hot_states, 0);
}
#[test]
fn hot_read_guard_and_cloned_graph_keep_the_same_allocation_alive_across_eviction() {
    let (hot, _, _, source) = classic();
    let root = hot.get_head().unwrap().root;
    let bytes = StateData::Classic(hot.block_state(root).unwrap()).allocation_bytes().unwrap();
    let mut cold = CompactStore::from_store(hot, source, CompactGraphLimits::default()).unwrap();
    let guard = cold.read_block_state(root).unwrap().unwrap();
    let twin = cold.clone();
    let original = guard.to_ssz().unwrap();
    assert_eq!(cold.stats().unwrap().resident_hot_schema_bytes, bytes);
    cold.evict_hot_states().unwrap();
    assert_eq!(cold.stats().unwrap().hot_states, 0);
    assert_eq!(cold.stats().unwrap().resident_hot_schema_bytes, bytes);
    assert_eq!(guard.to_ssz().unwrap(), original);
    assert_eq!(twin.read_block_state(root).unwrap().unwrap().to_ssz().unwrap(), original);
    drop(twin);
    assert_eq!(cold.stats().unwrap().resident_hot_schema_bytes, bytes);
    drop(guard);
    assert_eq!(cold.stats().unwrap().resident_hot_schema_bytes, 0);
    assert_eq!(cold.read_block_state(root).unwrap().unwrap().to_ssz().unwrap(), original);
}
#[test]
fn resident_hot_guard_budget_rolls_back_import_then_succeeds_after_last_owner_drops() {
    let (mut hot, keys, transitions, source) = classic();
    let root = hot.get_head().unwrap().root;
    let bytes = StateData::Classic(hot.block_state(root).unwrap()).allocation_bytes().unwrap();
    let signed = ssz::SignedBeaconBlock::Phase0(
        phase0_signed_child(&hot, &keys, &transitions),
        Preset::Minimal,
    );
    let mut next = hot.clone();
    next.on_tick(6000).unwrap();
    next.on_block(&signed).unwrap();
    let next_bytes = StateData::Classic(next.block_state(signed.message_root().unwrap()).unwrap())
        .allocation_bytes()
        .unwrap();
    drop(next);
    let limits = CompactGraphLimits {
        max_hot_state_bytes: bytes,
        max_resident_hot_state_bytes: bytes.max(next_bytes),
        ..Default::default()
    };
    let mut cold = CompactStore::from_store(hot.clone(), source, limits).unwrap();
    let guard = cold.read_block_state(root).unwrap().unwrap();
    cold.evict_hot_states().unwrap();
    // The ordinary comparison graph is intentionally dropped before budget checks below.

    hot.on_tick(6000).unwrap();
    drop(hot);
    cold.on_tick(6000).unwrap();
    let before = cold.snapshot().unwrap();
    assert!(matches!(
        cold.on_block(&signed),
        Err(StoreError::CompactState(StateSourceError::ResourceLimit))
    ));
    assert_eq!(cold.snapshot().unwrap(), before);
    drop(guard);
    cold.on_block(&signed).unwrap();
    assert_eq!(cold.snapshot().unwrap().head.root, signed.message_root().unwrap());
}
#[test]
fn source_retention_and_uncertain_head_publication_preserve_the_previous_private_graph() {
    let (hot, keys, transitions, source) = classic();
    let mut cold = CompactStore::from_store(hot.clone(), source.clone(), cold_limits()).unwrap();
    cold.on_tick(6000).unwrap();
    let signed = ssz::SignedBeaconBlock::Phase0(
        phase0_signed_child(&hot, &keys, &transitions),
        Preset::Minimal,
    );
    let before = cold.snapshot().unwrap();
    let published = *source.snapshots.lock().unwrap().last().unwrap();
    source.fail_retain.store(true, Ordering::SeqCst);
    assert!(matches!(
        cold.on_block(&signed),
        Err(StoreError::CompactState(StateSourceError::Durability))
    ));
    assert_eq!(cold.snapshot().unwrap(), before);
    source.fail_retain.store(false, Ordering::SeqCst);
    source.fail_publish.store(true, Ordering::SeqCst);
    assert!(matches!(
        cold.on_block(&signed),
        Err(StoreError::CompactState(StateSourceError::Durability))
    ));
    assert_eq!(cold.snapshot().unwrap(), before);
    assert_eq!(*source.snapshots.lock().unwrap().last().unwrap(), published);
    source.fail_publish.store(false, Ordering::SeqCst);
    cold.on_block(&signed).unwrap();
    assert_eq!(cold.snapshot().unwrap().head.root, signed.message_root().unwrap());
}
#[test]
fn cold_gloas_migration_real_checkpoints_and_votes_finalize_the_same_historical_branch() {
    let (mut classic, keys, parameters, engine) = fulu_store_with_payload();
    let historical_root = classic.get_head().unwrap().root;
    classic.on_tick(9 * 6000).unwrap();
    let first = signed_first_at(&classic, historical_root, &keys, &parameters, 9);
    let source = RecordingSource::new();
    source.seed_classic(&classic);
    let cold_classic = CompactStore::from_store(classic.clone(), source, cold_limits()).unwrap();
    let mut hot = ready(GloasStore::migrate_from_classic(&classic, &first, &engine)).unwrap();
    let mut cold =
        ready(CompactGloasStore::migrate_from_classic(&cold_classic, &first, &engine)).unwrap();
    for slot in 10..=32 {
        let signed = finalizing_gloas_child(&hot, &keys, slot);
        hot.on_tick(slot * 6000).unwrap();
        cold.on_tick(slot * 6000).unwrap();
        hot.on_block_and_contents(&signed).unwrap();
        cold.on_block_and_contents(&signed).unwrap();
        cold.evict_hot_states().unwrap();
        assert_eq!(cold.snapshot().unwrap(), hot.snapshot().unwrap());
        assert_eq!(cold.stats().unwrap().hot_states, 0);
    }
    let stats = cold.stats().unwrap();
    assert_eq!(stats.hot_schema_bytes, 0);
    assert_eq!(
        stats.cold_binding_heap_bytes,
        stats.cold_entries * std::mem::size_of::<StateBinding>()
    );
    assert_eq!(
        cold.snapshot().unwrap().finalized_checkpoint,
        Checkpoint { epoch: 1, root: historical_root }
    );
    assert_eq!(
        cold.read_historical_block_state(historical_root).unwrap().unwrap().schema_fork(),
        Fork::Fulu
    );
    assert_eq!(
        cold.verified_execution_block_hash(ForkChoiceNode {
            root: historical_root,
            payload_status: None
        })
        .unwrap(),
        Some([41; 32])
    );
    let root = hot.get_head().unwrap().root;
    assert_eq!(
        cold.read_block_state(root).unwrap().unwrap().to_ssz(Preset::Minimal).unwrap(),
        hot.block_state(root).unwrap().to_ssz(Preset::Minimal).unwrap()
    );
    let bad = signed_gloas_child(&hot, &keys, false, 88, false);
    let before = cold.snapshot().unwrap();
    let mut bad = bad;
    bad.signature = [0; 96];
    cold.on_tick(bad.message.slot * 6000).unwrap();
    assert!(cold.on_block(&bad).is_err());
    assert_eq!(cold.snapshot().unwrap().head, before.head);
}
#[test]
fn cold_gloas_retains_every_unfinalized_sibling_and_missing_proof_never_creates_full() {
    let (mut classic, keys, parameters) = real_anchor(Fork::Fulu);
    let a = fulu_signed_child(&classic, &keys, &parameters, 41);
    let b = fulu_signed_child(&classic, &keys, &parameters, 42);
    let a_root = a.message.hash_tree_root(Preset::Minimal).unwrap();
    let b_root = b.message.hash_tree_root(Preset::Minimal).unwrap();
    let engine_a = BoundaryEngine::new(&a);
    let engine_b = BoundaryEngine::new(&b);
    let availability = EmptyBoundaryData(AtomicUsize::new(0));
    classic.on_tick(6000).unwrap();
    for (signed, engine) in [(a, &engine_a), (b, &engine_b)] {
        ready(classic.on_block_with_engine(
            &ssz::SignedBeaconBlock::Fulu(signed, Preset::Minimal),
            engine,
            &availability,
        ))
        .unwrap();
    }
    classic.on_tick(9 * 6000).unwrap();
    let first = signed_first_at(&classic, a_root, &keys, &parameters, 9);
    let source = RecordingSource::new();
    source.seed_classic(&classic);
    let cold_classic =
        CompactStore::from_store(classic.clone(), source.clone(), cold_limits()).unwrap();
    let mut cold =
        ready(CompactGloasStore::migrate_from_classic(&cold_classic, &first, &engine_a)).unwrap();
    cold.evict_hot_states().unwrap();
    for root in [a_root, b_root] {
        assert_eq!(
            cold.read_historical_block_state(root).unwrap().unwrap().schema_fork(),
            Fork::Fulu
        );
    }
    let root = first.message_root().unwrap();
    assert!(!cold.is_payload_verified(root));
    assert!(matches!(
        cold.verified_execution_block_hash(node(root, PayloadStatus::Full)),
        Err(StoreError::ExecutionImport(_))
    ));
    assert_eq!(
        cold.verified_execution_block_hash(node(root, PayloadStatus::Empty)).unwrap(),
        Some([41; 32])
    );
    let source_root = cold.read_block_state(root).unwrap().unwrap().hash_tree_root().unwrap();
    source.pins.lock().unwrap().remove(&source_root);
    assert!(matches!(
        cold.verified_execution_block_hash(node(root, PayloadStatus::Empty)),
        Err(StoreError::CompactState(StateSourceError::Durability))
    ));
    assert!(!cold.is_payload_verified(root));
}

#[test]
fn typed_gloas_guard_owns_the_exact_projection_after_cache_graph_and_source_drop() {
    let (mut classic, keys, parameters) = real_anchor(Fork::Fulu);
    classic.on_tick(8 * 6000).unwrap();
    let signed = signed_first(&classic, &keys, &parameters);
    let engine = UnavailableEngine(AtomicUsize::new(0));
    let graph = ready(GloasStore::migrate_from_classic(&classic, &signed, &engine)).unwrap();
    let root = signed.message_root().unwrap();
    let source = RecordingSource::new();
    source.seed_gloas(&graph);
    let compact = CompactGloasStore::from_store(graph, source.clone(), cold_limits()).unwrap();
    let read = compact.read_block_state(root).unwrap().unwrap();
    let twin = read.clone();
    assert!(std::ptr::eq(std::ptr::from_ref(&*read), std::ptr::from_ref(&*twin)));
    let original = read.to_ssz(Preset::Minimal).unwrap();
    let StateData::Gloas(projected, preset) = read.data() else {
        panic!("checked typed projection")
    };
    assert_eq!(preset, Preset::Minimal);
    assert!(std::ptr::eq(std::ptr::from_ref(&*read), std::ptr::from_ref(projected)));
    drop(compact);
    drop(source);
    drop(classic);
    assert_eq!(read.to_ssz(Preset::Minimal).unwrap(), original);
    assert_eq!(twin.to_ssz(Preset::Minimal).unwrap(), original);
}

#[test]
fn checked_owned_block_gossip_preserves_seen_cache_when_a_known_cold_parent_is_unavailable() {
    let (hot, keys, transitions, source) = classic();
    let mut cold = CompactStore::from_store(hot.clone(), source.clone(), cold_limits()).unwrap();
    cold.on_tick(6000).unwrap();
    let signed = ssz::SignedBeaconBlock::Phase0(
        phase0_signed_child(&hot, &keys, &transitions),
        Preset::Minimal,
    );
    let mut seen = crate::store::block_gossip::SeenBeaconBlocks::new(&cold, 8).unwrap();
    let committed =
        hot.block_state(hot.get_head().unwrap().root).unwrap().hash_tree_root().unwrap();
    source.pins.lock().unwrap().remove(&committed);
    assert!(matches!(
        crate::store::block_gossip::validate(&cold, &mut seen, &signed, 6000),
        Err(crate::store::block_gossip::GossipBlockError::Store(StoreError::CompactState(
            StateSourceError::Durability
        )))
    ));
    assert_eq!(seen.entries(), 0);
    source.pins.lock().unwrap().insert(committed);
    crate::store::block_gossip::validate(&cold, &mut seen, &signed, 6000).unwrap();
    assert_eq!(seen.entries(), 1);
    assert_eq!(cold.stats().unwrap().hot_states, 0);
    assert_eq!(cold.snapshot().unwrap().head_slot, 0);
}
#[test]
fn prepared_confirmation_owns_exact_cold_head_and_checkpoints_and_bounds_required_reads() {
    let (mut hot, _, _, source) = classic();
    let mut cold = CompactStore::from_store(hot.clone(), source, cold_limits()).unwrap();
    let (justified, finalized) = cold.checkpoints();
    let mut prepared = cold.prepare_fast_confirmation(&[justified, finalized]).unwrap();
    let mut ordinary = FastConfirmation::new(&hot, Parameters::default()).unwrap();
    let mut compact = FastConfirmation::new(&prepared, Parameters::default()).unwrap();
    ordinary.update_variables(&hot).unwrap();
    compact.update_variables(&prepared).unwrap();
    assert_eq!(ordinary.snapshot(), compact.snapshot());
    hot.on_tick(6000).unwrap();
    cold.on_tick(6000).unwrap();
    prepared = cold.prepare_fast_confirmation(&[justified, finalized]).unwrap();
    ordinary.update_variables(&hot).unwrap();
    compact.update_variables(&prepared).unwrap();
    assert_eq!(ordinary.snapshot(), compact.snapshot());
    let StateRef::Classic(checkpoint) =
        FastConfirmationView::checkpoint_state(&prepared, justified).unwrap()
    else {
        panic!("actual classic prepared state")
    };
    let original = checkpoint.to_ssz().unwrap();
    assert!(matches!(
        cold.prepare_fast_confirmation(&[justified; 32]),
        Err(StoreError::ResourceLimit)
    ));
    drop(cold);
    drop(hot);
    let StateRef::Classic(retained) =
        FastConfirmationView::checkpoint_state(&prepared, justified).unwrap()
    else {
        panic!("owned complete checkpoint remains readable")
    };
    assert_eq!(retained.to_ssz().unwrap(), original);
}

#[test]
fn no_admitted_finality_never_prunes_a_genuine_classic_parent_or_next_child() {
    let (mut hot, keys, transitions, source) = classic();
    let root = hot.get_head().unwrap().root;
    let mut cold = CompactStore::from_store(hot.clone(), source, cold_limits()).unwrap();
    let result = cold.prune_finalized_history(FinalizedRetentionPolicy::default()).unwrap();
    assert_eq!(result.removed_blocks, 0);
    assert_eq!(result.boundary, None);
    assert!(cold.read_block_state(root).unwrap().is_some());
    let signed = ssz::SignedBeaconBlock::Phase0(
        phase0_signed_child(&hot, &keys, &transitions),
        Preset::Minimal,
    );
    hot.on_tick(6000).unwrap();
    cold.on_tick(6000).unwrap();
    hot.on_block(&signed).unwrap();
    cold.on_block(&signed).unwrap();
    assert_eq!(cold.snapshot().unwrap(), hot.snapshot().unwrap());
}

#[test]
fn genuine_finalized_roll_forward_bounds_graph_and_keeps_empty_execution_and_competing_children() {
    let (mut classic, keys, parameters, engine) = fulu_store_with_payload();
    let historic = classic.get_head().unwrap().root;
    classic.on_tick(9 * 6000).unwrap();
    let first = signed_first_at(&classic, historic, &keys, &parameters, 9);
    let source = RecordingSource::new();
    source.seed_classic(&classic);
    let classic_cold = CompactStore::from_store(classic.clone(), source, cold_limits()).unwrap();
    let mut hot = ready(GloasStore::migrate_from_classic(&classic, &first, &engine)).unwrap();
    let mut cold =
        ready(CompactGloasStore::migrate_from_classic(&classic_cold, &first, &engine)).unwrap();
    // This is an explicit local metadata budget, not a consensus preset/schedule change.
    cold.graph.parameters.max_blocks = 50;
    let policy = FinalizedRetentionPolicy::new(2).unwrap();
    let mut removed = 0;
    for slot in 10..=96 {
        let signed = finalizing_gloas_child(&hot, &keys, slot);
        hot.on_tick(slot * 6000).unwrap();
        cold.on_tick(slot * 6000).unwrap();
        hot.on_block_and_contents(&signed).unwrap();
        cold.on_block_and_contents(&signed).unwrap();
        let result = cold.prune_finalized_history(policy).unwrap();
        removed += result.removed_blocks;
        assert!(result.retained_blocks <= 50);
        assert_eq!(cold.snapshot().unwrap(), hot.snapshot().unwrap());
        assert_eq!(
            cold.verified_execution_block_hash(cold.get_head().unwrap()).unwrap(),
            Some([41; 32])
        );
        assert!(!cold.is_payload_verified(cold.get_head().unwrap().root));
    }
    assert!(removed > 40);
    assert!(cold.graph.historical.is_none());
    assert!(cold.read_historical_block_state(historic).unwrap().is_none());
    let first_branch = signed_gloas_child(&hot, &keys, false, 211, false);
    let second_branch = signed_gloas_child(&hot, &keys, false, 212, false);
    let first_root = first_branch.message.hash_tree_root(Preset::Minimal).unwrap();
    let second_root = second_branch.message.hash_tree_root(Preset::Minimal).unwrap();
    hot.on_tick(97 * 6000).unwrap();
    cold.on_tick(97 * 6000).unwrap();
    hot.on_block(&first_branch).unwrap();
    hot.on_block(&second_branch).unwrap();
    cold.on_block(&first_branch).unwrap();
    cold.on_block(&second_branch).unwrap();
    cold.prune_finalized_history(policy).unwrap();
    assert!(cold.read_block_state(first_root).unwrap().is_some());
    assert!(cold.read_block_state(second_root).unwrap().is_some());
    assert_eq!(cold.snapshot().unwrap(), hot.snapshot().unwrap());
}

#[test]
fn real_finalized_retention_missing_pin_and_uncertain_publication_leave_graph_unchanged() {
    let (mut classic, keys, parameters, engine) = fulu_store_with_payload();
    let historic = classic.get_head().unwrap().root;
    classic.on_tick(9 * 6000).unwrap();
    let first = signed_first_at(&classic, historic, &keys, &parameters, 9);
    let source = RecordingSource::new();
    source.seed_classic(&classic);
    let classic_cold =
        CompactStore::from_store(classic.clone(), source.clone(), cold_limits()).unwrap();
    let mut hot = ready(GloasStore::migrate_from_classic(&classic, &first, &engine)).unwrap();
    let mut cold =
        ready(CompactGloasStore::migrate_from_classic(&classic_cold, &first, &engine)).unwrap();
    for slot in 10..=64 {
        let signed = finalizing_gloas_child(&hot, &keys, slot);
        hot.on_tick(slot * 6000).unwrap();
        cold.on_tick(slot * 6000).unwrap();
        hot.on_block_and_contents(&signed).unwrap();
        cold.on_block_and_contents(&signed).unwrap();
    }
    let policy = FinalizedRetentionPolicy::new(2).unwrap();
    let before = cold.snapshot().unwrap();
    let count = cold.graph.block_count();
    let epoch = before.finalized_checkpoint.epoch - policy.epochs();
    let root = cold.graph.get_checkpoint_block(before.finalized_checkpoint.root, epoch).unwrap();
    let state_root = cold.graph.block(root).unwrap().state_root;
    source.pins.lock().unwrap().remove(&state_root);
    assert!(matches!(
        cold.prune_finalized_history(policy),
        Err(StoreError::CompactState(StateSourceError::Durability))
    ));
    assert_eq!(cold.snapshot().unwrap(), before);
    assert_eq!(cold.graph.block_count(), count);
    source.pins.lock().unwrap().insert(state_root);
    source.fail_publish.store(true, Ordering::SeqCst);
    assert!(matches!(
        cold.prune_finalized_history(policy),
        Err(StoreError::CompactState(StateSourceError::Durability))
    ));
    assert_eq!(cold.snapshot().unwrap(), before);
    assert_eq!(cold.graph.block_count(), count);
    source.fail_publish.store(false, Ordering::SeqCst);
    assert!(cold.prune_finalized_history(policy).unwrap().removed_blocks > 0);
    assert_eq!(cold.snapshot().unwrap(), before);
}

// Consume genuine fully signed production post-state/block data at a trusted boundary.
// Dropping prior graph history cannot create a strict independently verified EL base.
fn genuine_gloas_checkpoint_without_execution_history(
) -> (GloasStore, Vec<SecretKey>, Arc<RecordingSource>, Root) {
    let (mut classic, keys, parameters, engine) = fulu_store_with_payload();
    let historic = classic.get_head().unwrap().root;
    classic.on_tick(9 * 6000).unwrap();
    let first = signed_first_at(&classic, historic, &keys, &parameters, 9);
    let mut admitted = ready(GloasStore::migrate_from_classic(&classic, &first, &engine)).unwrap();
    for slot in 10..=32 {
        let signed = finalizing_gloas_child(&admitted, &keys, slot);
        admitted.on_tick(slot * 6000).unwrap();
        admitted.on_block_and_contents(&signed).unwrap();
    }
    let root = admitted.get_head().unwrap().root;
    let state = admitted.block_state(root).unwrap().clone();
    let block = admitted.block(root).unwrap().clone();
    assert_eq!(state.slot, 32);
    assert_eq!(state.latest_block_hash, [41; 32]);
    assert_eq!(block.state_root, state.hash_tree_root(Preset::Minimal).unwrap());
    let trusted = GloasStore::get_store_at_checkpoint(
        state.clone(),
        block,
        Checkpoint { epoch: 4, root },
        &state.to_ssz(Preset::Minimal).unwrap(),
        admitted.parameters,
        admitted.processing.clone(),
    )
    .unwrap();
    assert_eq!(trusted.block_count(), 1);
    assert!(matches!(
        trusted.verified_execution_block_hash(node(root, PayloadStatus::Empty)),
        Err(StoreError::UnknownRoot)
    ));
    let source = RecordingSource::new();
    source.seed_gloas(&trusted);
    (trusted, keys, source, root)
}

#[test]
fn genuine_modern_gloas_consensus_cut_rolls_forward_without_inventing_execution_authority() {
    let (mut hot, keys, source, original) = genuine_gloas_checkpoint_without_execution_history();
    let mut cold =
        CompactGloasStore::from_store(hot.clone(), source.clone(), cold_limits()).unwrap();
    let policy = FinalizedRetentionPolicy::new(2).unwrap();
    let mut removed = 0;
    let mut observed_cut = false;
    for slot in 33..=96 {
        let signed = finalizing_gloas_child(&hot, &keys, slot);
        hot.on_tick(slot * 6000).unwrap();
        cold.on_tick(slot * 6000).unwrap();
        hot.on_block_and_contents(&signed).unwrap();
        cold.on_block_and_contents(&signed).unwrap();
        if let Some(selected) = cold.finalized_retention_boundary(policy).unwrap() {
            let before = cold.snapshot().unwrap();
            let before_count = cold.graph.block_count();
            let old_anchor = cold.graph.execution_query_anchor.unwrap();
            assert!(matches!(cold.prune_finalized_history(policy), Err(StoreError::UnknownRoot)));
            assert_eq!(cold.snapshot().unwrap(), before);
            assert_eq!(cold.graph.block_count(), before_count);
            let result = cold.prune_finalized_consensus_history(policy).unwrap();
            assert_eq!(result.boundary, Some(selected));
            removed += result.removed_blocks;
            let association = *source.retained_execution.lock().unwrap().last().unwrap();
            let RetainedExecutionAssociation::UnresolvedTrustedBoundary(unresolved) = association
            else {
                panic!("missing original EL proof must remain explicitly unresolved")
            };
            assert_eq!(unresolved.origin_block_root(), old_anchor);
            assert_eq!(unresolved.cut_block_root(), selected.root);
            assert_eq!(unresolved.cut_state_root(), hot.block(selected.root).unwrap().state_root);
            assert_eq!(unresolved.parent_hash(), [41; 32]);
            assert!(cold.execution_cut.is_none());
            assert_eq!(cold.graph.execution_query_anchor, Some(selected.root));
            assert!(matches!(
                cold.verified_execution_block_hash(node(selected.root, PayloadStatus::Empty)),
                Err(StoreError::UnknownRoot)
            ));
            assert!(matches!(
                cold.verified_execution_block_hash(cold.get_head().unwrap()),
                Err(StoreError::UnknownRoot)
            ));
            assert!(!cold.is_payload_verified(selected.root));
            observed_cut = true;
        }
        assert_eq!(cold.snapshot().unwrap(), hot.snapshot().unwrap());
    }
    assert!(observed_cut);
    assert!(removed > 20);
    assert!(cold.read_block_state(original).unwrap().is_none());
    let first = signed_gloas_child(&hot, &keys, false, 211, false);
    let second = signed_gloas_child(&hot, &keys, false, 212, false);
    let first_root = first.message.hash_tree_root(Preset::Minimal).unwrap();
    let second_root = second.message.hash_tree_root(Preset::Minimal).unwrap();
    hot.on_tick(97 * 6000).unwrap();
    cold.on_tick(97 * 6000).unwrap();
    for signed in [&first, &second] {
        hot.on_block(signed).unwrap();
        cold.on_block(signed).unwrap();
    }
    cold.prune_finalized_consensus_history(policy).unwrap();
    assert!(cold.read_block_state(first_root).unwrap().is_some());
    assert!(cold.read_block_state(second_root).unwrap().is_some());
    assert_eq!(cold.snapshot().unwrap(), hot.snapshot().unwrap());
}

#[test]
fn genuine_modern_consensus_cut_missing_boundary_pin_and_uncertain_publish_are_transactional() {
    let (mut hot, keys, source, original) = genuine_gloas_checkpoint_without_execution_history();
    let original_state_root = hot.block(original).unwrap().state_root;
    let mut cold =
        CompactGloasStore::from_store(hot.clone(), source.clone(), cold_limits()).unwrap();
    for slot in 33..=80 {
        let signed = finalizing_gloas_child(&hot, &keys, slot);
        hot.on_tick(slot * 6000).unwrap();
        cold.on_tick(slot * 6000).unwrap();
        hot.on_block_and_contents(&signed).unwrap();
        cold.on_block_and_contents(&signed).unwrap();
    }
    let policy = FinalizedRetentionPolicy::new(2).unwrap();
    let selected = cold.finalized_retention_boundary(policy).unwrap().unwrap();
    let before = cold.snapshot().unwrap();
    let before_count = cold.graph.block_count();
    source.pins.lock().unwrap().remove(&original_state_root);
    assert!(matches!(
        cold.prune_finalized_consensus_history(policy),
        Err(StoreError::CompactState(StateSourceError::Durability))
    ));
    assert_eq!(cold.snapshot().unwrap(), before);
    assert_eq!(cold.graph.block_count(), before_count);
    assert_eq!(cold.graph.execution_query_anchor, Some(original));
    source.pins.lock().unwrap().insert(original_state_root);
    source.fail_publish.store(true, Ordering::SeqCst);
    assert!(matches!(
        cold.prune_finalized_consensus_history(policy),
        Err(StoreError::CompactState(StateSourceError::Durability))
    ));
    assert_eq!(cold.snapshot().unwrap(), before);
    assert_eq!(cold.graph.block_count(), before_count);
    assert_eq!(cold.graph.execution_query_anchor, Some(original));
    source.fail_publish.store(false, Ordering::SeqCst);
    let result = cold.prune_finalized_consensus_history(policy).unwrap();
    assert_eq!(result.boundary, Some(selected));
    assert!(result.removed_blocks > 0);
    assert!(cold.execution_cut.is_none());
    assert_eq!(cold.snapshot().unwrap(), hot.snapshot().unwrap());
}

#[test]
fn compact_retained_vote_batch_matches_scalar_partial_success_before_fatal() {
    let (graph, root) = retained_vote_graph();
    let scalar_source = RecordingSource::new();
    scalar_source.seed_gloas(&graph);
    let batch_source = RecordingSource::new();
    batch_source.seed_gloas(&graph);
    let mut scalar =
        CompactGloasStore::from_store(graph.clone(), scalar_source, CompactGraphLimits::default())
            .unwrap();
    let mut batch =
        CompactGloasStore::from_store(graph, batch_source, CompactGraphLimits::default()).unwrap();
    for store in [&mut scalar, &mut batch] {
        corrupt_middle_signature(&mut store.graph, root);
        corrupt_last_shape(&mut store.graph, root);
    }
    scalar.on_retained_block_attestation(root, 0).unwrap();
    assert!(matches!(
        scalar.on_retained_block_attestation(root, 1),
        Err(StoreError::Processing(ProcessingError::Crypto(_)))
    ));
    let fatal = scalar.on_retained_block_attestation(root, 2).unwrap_err();
    assert_eq!(
        format!("{:?}", batch.on_retained_block_attestations(root).unwrap_err()),
        format!("{fatal:?}")
    );
    assert!(!batch.graph.latest_messages.is_empty());
    assert_eq!(batch.graph.latest_messages, scalar.graph.latest_messages);
    assert_eq!(batch.snapshot().unwrap(), scalar.snapshot().unwrap());
}

#[test]
fn compact_retained_vote_batch_does_not_publish_all_rejected_votes() {
    let (graph, root) = retained_vote_graph();
    let source = RecordingSource::new();
    source.seed_gloas(&graph);
    let mut batch =
        CompactGloasStore::from_store(graph, source.clone(), CompactGraphLimits::default())
            .unwrap();
    for vote in
        &mut Arc::make_mut(batch.graph.blocks.get_mut(&root).unwrap()).body.attestations.values
    {
        vote.signature = [0; 96];
    }
    let before = batch.snapshot().unwrap();
    let checkpoint_count = batch.graph.checkpoint_states.len();
    let published = source.snapshots.lock().unwrap().len();
    let results = batch.on_retained_block_attestations(root).unwrap();
    assert_eq!(results.len(), 3);
    assert!(results.iter().all(|(_, result)| matches!(
        result,
        Err(StoreError::Processing(ProcessingError::Crypto(_)))
    )));
    assert!(batch.graph.latest_messages.is_empty());
    assert_eq!(batch.graph.checkpoint_states.len(), checkpoint_count);
    assert_eq!(batch.snapshot().unwrap(), before);
    assert_eq!(source.snapshots.lock().unwrap().len(), published);
}
