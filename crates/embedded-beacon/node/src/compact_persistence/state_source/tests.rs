//! Real durable source reads retain leases; missing original owners fail despite cached data.

use super::*;
use crate::{
    bootstrap::genesis::tests::genuine,
    compact_persistence::events::{DurableEvent, EventAction},
    gossip_tests::phase0_signed_child,
    BootstrapDecodeLimits, VerifiedGenesis,
};
use reth_embedded_beacon_fork_choice::compact::{CompactGraphLimits, CompactStore};
use tempfile::TempDir;

#[test]
fn genuine_cold_reads_reuse_accounted_owner_and_missing_original_pin_fails() {
    let (config, state, _, _) = genuine(Fork::Phase0);
    let parameters = RuntimeParameters::from_config(&config).unwrap();
    let genesis = VerifiedGenesis::authenticate(
        config.clone(),
        state.to_ssz().unwrap().into(),
        BootstrapDecodeLimits::default(),
    )
    .unwrap();
    let root = genesis.block_root();
    let directory = TempDir::new().unwrap();
    let network = PersistenceNetwork::from_config(config.clone()).unwrap();
    let shared = SharedDataStore::open(directory.path(), network.identity(), 0).unwrap();
    let repository = Arc::new(
        AdmittedStateRepository::open(shared.clone(), network, CompactStateLimits::default())
            .unwrap(),
    );
    repository.persist_genesis(&genesis, BootstrapDecodeLimits::default()).unwrap();
    let (state, block) = genesis.clone().into_import_inputs(&config).unwrap();
    let mut graph = Store::get_store(
        Arc::unwrap_or_clone(state),
        Arc::unwrap_or_clone(block),
        parameters.fork_choice,
    )
    .unwrap();
    graph.set_transition_parameters(parameters.transitions).unwrap();
    let source = RepositoryStateSource::open(repository.clone()).unwrap();
    let mut graph = CompactStore::from_store(
        graph,
        source.clone(),
        CompactGraphLimits { max_hot_state_bytes: 0, ..CompactGraphLimits::default() },
    )
    .unwrap();
    assert!(repository.current_snapshot().unwrap().is_none());
    let first = graph.read_block_state(root).unwrap().unwrap();
    let first_ptr = first.as_ref() as *const BeaconState;
    drop(first);
    let second = graph.read_block_state(root).unwrap().unwrap();
    assert_eq!(first_ptr, second.as_ref() as *const BeaconState);
    assert_eq!(source.leases.lock().unwrap().entries.len(), 1);
    assert!(source.leases.lock().unwrap().bytes <= 64 * 1024 * 1024);
    source.commit_snapshot(graph.snapshot().unwrap()).unwrap();
    let original = repository.lock().unwrap().records.get(&root).unwrap().original;
    shared.release(Owner::Checkpoint(root), original).unwrap();
    assert!(matches!(
        graph.read_block_state(root),
        Err(StoreError::CompactState(StateSourceError::BindingMismatch))
    ));
    assert!(matches!(
        graph.evict_hot_states(),
        Err(StoreError::CompactState(StateSourceError::BindingMismatch))
    ));
    assert_eq!(second.hash_tree_root().unwrap(), genesis.state_root());
}

#[test]
fn snapshot_checks_duplicate_roots_once_and_revalidates_changed_records() {
    let (_directory, repository, _source, graph, roots) =
        retained_phase0_history(Some(64 * 1024 * 1024));
    let records = repository.lock().unwrap().records.clone();
    let mut snapshot = graph.snapshot().unwrap();
    snapshot.head.root = roots[0];
    snapshot.head_slot = 0;
    for (justified, finalized, expected) in [
        (roots[0], roots[0], vec![roots[0]]),
        (roots[1], roots[0], vec![roots[0], roots[1]]),
        (roots[1], roots[2], vec![roots[0], roots[1], roots[2]]),
    ] {
        snapshot.justified_checkpoint.root = justified;
        snapshot.finalized_checkpoint.root = finalized;
        let mut checked = Vec::new();
        check_snapshot_records(&records, snapshot, |record| {
            checked.push(record.block_root);
            repository.check_saved_record(record)
        })
        .unwrap();
        assert_eq!(checked, expected);
    }
    snapshot.justified_checkpoint.root = roots[0];
    snapshot.finalized_checkpoint.root = roots[0];
    for defect in 0..5 {
        let mut changed = records.clone();
        let record = changed.get_mut(&roots[0]).unwrap();
        match defect {
            0 => record.slot += 1,
            1 => record.fork = Fork::Altair,
            2 => record.parent_root[0] ^= 1,
            3 => record.block_root[0] ^= 1,
            _ => record.state_root = records.get(&roots[1]).unwrap().state_root,
        }
        let mut checks = 0;
        assert_eq!(
            check_snapshot_records(&changed, snapshot, |record| {
                checks += 1;
                repository.check_saved_record(record)
            }),
            Err(StateSourceError::BindingMismatch)
        );
        assert_eq!(checks, 1);
    }
    let mut wrong_slot = snapshot;
    wrong_slot.head_slot += 1;
    assert_eq!(
        check_snapshot_records(&records, wrong_slot, |record| {
            repository.check_saved_record(record)
        }),
        Err(StateSourceError::BindingMismatch)
    );
    let mut missing_envelope = snapshot;
    missing_envelope.head.payload_status = Some(PayloadStatus::Full);
    assert_eq!(
        check_snapshot_records(&records, missing_envelope, |record| {
            repository.check_saved_record(record)
        }),
        Err(StateSourceError::BindingMismatch)
    );
    let original = records.get(&roots[0]).unwrap().original;
    repository.shared.release(Owner::Checkpoint(roots[0]), original).unwrap();
    assert_eq!(
        check_snapshot_records(&records, snapshot, |record| repository.check_saved_record(record)),
        Err(StateSourceError::BindingMismatch)
    );
}

#[test]
fn prepared_closure_reuse_cannot_replace_fresh_original_owner_checks() {
    let (_directory, repository, source, graph, _) = retained_phase0_history(None);
    let snapshot = graph.snapshot().unwrap();
    source.prepare_snapshot(snapshot).unwrap();
    assert!(source.derived.lock().unwrap().prepared_closure.is_some());
    let original = repository.lock().unwrap().records.get(&snapshot.head.root).unwrap().original;
    repository.shared.release(Owner::Consensus(snapshot.head.root), original).unwrap();
    assert!(matches!(source.commit_snapshot(snapshot), Err(PersistenceError::Linkage)));
    assert!(repository.current_snapshot().unwrap().is_none());
}

#[test]
fn caller_selected_lease_cache_reuses_allocations_and_evicts_with_live_guards() {
    let footprints = retained_history_allocation_bytes();
    let budget = footprints.iter().sum::<usize>() - footprints.iter().min().unwrap();
    let (_directory, repository, source, graph, roots) = retained_phase0_history(Some(budget));
    assert_eq!(repository.schema_cache_bytes, budget);
    {
        let mut cache = source.leases.lock().unwrap();
        cache.entries.clear();
        cache.bytes = 0;
    }
    let anchor = repository.block_state(roots[0]).unwrap().unwrap();
    let anchor_identity = anchor.downgrade();
    let first = graph.read_block_state(roots[0]).unwrap().unwrap();
    let first_ptr = first.as_ref() as *const BeaconState;
    let first_owner = source.leases.lock().unwrap().entries.values().next().unwrap().owner.clone();
    let again = graph.read_block_state(roots[0]).unwrap().unwrap();
    assert_eq!(first_ptr, again.as_ref() as *const BeaconState);
    {
        let cache = source.leases.lock().unwrap();
        assert_eq!(cache.entries.len(), 1);
        assert!(Arc::ptr_eq(&first_owner, &cache.entries.values().next().unwrap().owner));
    }
    let second = graph.read_block_state(roots[1]).unwrap().unwrap();
    let second_bytes = second.data().allocation_bytes().unwrap();
    let first_bytes = first.data().allocation_bytes().unwrap();
    assert!(first_bytes + second_bytes <= budget);
    assert_eq!(source.leases.lock().unwrap().entries.len(), 2);
    let third = graph.read_block_state(roots[2]).unwrap().unwrap();
    let third_bytes = third.data().allocation_bytes().unwrap();
    assert!(third_bytes <= budget);
    assert!(first_bytes + second_bytes + third_bytes > budget);
    {
        let cache = source.leases.lock().unwrap();
        assert!(cache.bytes <= budget);
        assert_eq!(cache.entries.len(), 2);
        assert!(cache.entries.values().all(|entry| !Arc::ptr_eq(&first_owner, &entry.owner)));
    }
    // Cache eviction releases idle ownership, while genuine outstanding guards remain usable.
    assert_eq!(
        first.hash_tree_root().unwrap(),
        repository.lock().unwrap().records[&roots[0]].state_root
    );
    let reused = graph.read_block_state(roots[0]).unwrap().unwrap();
    assert_eq!(first_ptr, reused.as_ref() as *const BeaconState);
    let reacquired = repository.block_state(roots[0]).unwrap().unwrap();
    assert!(anchor_identity.matches(&reacquired));
    assert!(!repository.compact.owns_cached_state(anchor.root(), anchor.state()).unwrap());
    for held in [&second, &third] {
        assert!(repository
            .compact
            .owns_cached_state(held.hash_tree_root().unwrap(), held.as_ref())
            .unwrap());
    }
    assert!(source.leases.lock().unwrap().bytes <= budget);
    // A cached allocation cannot bypass exact original-record metadata checks on a later read.
    repository.lock().unwrap().records.get_mut(&roots[0]).unwrap().slot += 1;
    assert!(matches!(
        graph.read_block_state(roots[0]),
        Err(StoreError::CompactState(StateSourceError::BindingMismatch))
    ));
    assert_eq!(second.slot(), 1);
    assert_eq!(third.slot(), 2);
}

#[test]
fn two_idle_allocations_survive_alternating_cold_reads_and_original_pin_loss_fails() {
    let footprints = retained_history_allocation_bytes();
    let budget = footprints[0] + footprints[1];
    let (_directory, repository, source, graph, roots) = retained_phase0_history(Some(budget));
    assert_eq!(source.graph_limits().max_hot_state_bytes, 0);
    assert_eq!(repository.schema_cache_bytes, budget);
    let selected = [roots[0], roots[1]];
    let identities = selected.map(|root| {
        let read = graph.read_block_state(root).unwrap().unwrap();
        let lease = repository.block_state(root).unwrap().unwrap();
        assert!(std::ptr::eq(read.as_ref(), lease.state()));
        assert!(repository.compact.owns_cached_state(lease.root(), lease.state()).unwrap());
        let identity = lease.downgrade();
        // Both strong read guards drop here. Only budgeted idle cache ownership remains.
        drop(read);
        drop(lease);
        identity
    });
    for index in [0, 1, 1, 0, 1, 0, 0, 1] {
        {
            let read = graph.read_block_state(selected[index]).unwrap().unwrap();
            let lease = repository.block_state(selected[index]).unwrap().unwrap();
            assert!(identities[index].matches(&lease));
            assert!(std::ptr::eq(read.as_ref(), lease.state()));
            assert_eq!(read.hash_tree_root().unwrap(), lease.root());
        }
        source.trim_idle_caches().unwrap();
        let cache = source.leases.lock().unwrap();
        assert_eq!(cache.entries.len(), 2);
        assert!(cache.bytes <= budget);
        for (key, entry) in &cache.entries {
            assert!(repository.compact.owns_cached_state(key.0, entry.owner.state(),).unwrap());
        }
        drop(cache);
        let stats = repository.compact.stats().unwrap();
        assert_eq!(stats.cached_states, 2);
        assert!(stats.cached_schema_bytes <= budget);
    }
    // Cached bytes and a live read guard cannot authorize an origin whose durable pin was lost.
    let held = graph.read_block_state(roots[1]).unwrap().unwrap();
    let record = repository.lock().unwrap().records[&roots[1]].clone();
    repository.shared.release(Owner::Consensus(roots[1]), record.original).unwrap();
    assert!(matches!(
        graph.read_block_state(roots[1]),
        Err(StoreError::CompactState(StateSourceError::BindingMismatch))
    ));
    assert_eq!(held.hash_tree_root().unwrap(), record.state_root);
}

#[test]
fn native_checkpoint_proof_reuses_configured_cache_and_rechecks_original_ownership() {
    let (_directory, repository, source, _graph, roots) =
        retained_phase0_history(Some(1024 * 1024));
    let record = retained_checkpoint_record(&repository, roots[1]);
    let key = (record.origin_block_root, record.epoch);
    let first = source.check_derived(&record).unwrap();
    let (post, boundary, touched) = {
        let cache = source.replay_proofs.lock().unwrap();
        let proof = cache.entries.get(&key).unwrap();
        assert!(cache.bytes <= repository.schema_cache_bytes);
        (proof.post.clone(), proof.boundary.clone(), proof.touched)
    };
    let second = source.check_derived(&record).unwrap();
    {
        let cache = source.replay_proofs.lock().unwrap();
        let proof = cache.entries.get(&key).unwrap();
        assert!(std::ptr::eq(post.state(), proof.post.state()));
        assert!(std::ptr::eq(boundary.state(), proof.boundary.state()));
        assert!(proof.touched > touched);
        assert_eq!(cache.entries.len(), 1);
    }
    assert!(std::ptr::eq(first.state(), second.state()));
    assert_eq!(second.root(), record.state_root);
    let original = repository.lock().unwrap().records[&roots[1]].original;
    repository.shared.release(Owner::Consensus(roots[1]), original).unwrap();
    assert!(matches!(source.check_derived(&record), Err(PersistenceError::Linkage)));
    assert_eq!(first.state().hash_tree_root().unwrap(), record.state_root);
}

#[test]
fn native_checkpoint_proof_eviction_obeys_configured_budget_and_keeps_active_leases() {
    let budget = retained_checkpoint_proof_cache_budget();
    let (_directory, repository, source, _graph, roots) = retained_phase0_history(Some(budget));
    let first_record = retained_checkpoint_record(&repository, roots[1]);
    let second_record = retained_checkpoint_record(&repository, roots[2]);
    let first = source.check_derived(&first_record).unwrap();
    let first_bytes = source.replay_proofs.lock().unwrap().bytes;
    let second = source.check_derived(&second_record).unwrap();
    let cache = source.replay_proofs.lock().unwrap();
    let second_bytes = cache.entries[&(roots[2], second_record.epoch)].bytes;
    assert!(first_bytes <= repository.schema_cache_bytes);
    assert!(second_bytes <= repository.schema_cache_bytes);
    assert!(first_bytes + second_bytes > repository.schema_cache_bytes);
    assert_eq!(cache.entries.len(), 1);
    assert!(!cache.entries.contains_key(&(roots[1], first_record.epoch)));
    assert!(cache.bytes <= repository.schema_cache_bytes);
    assert_eq!(first.state().hash_tree_root().unwrap(), first_record.state_root);
    assert_eq!(second.state().hash_tree_root().unwrap(), second_record.state_root);
}

#[test]
fn native_checkpoint_proof_larger_than_configured_budget_is_not_retained() {
    let (_directory, repository, source, _graph, roots) = retained_phase0_history(Some(1));
    let record = retained_checkpoint_record(&repository, roots[1]);
    let result = source.check_derived(&record).unwrap();
    let cache = source.replay_proofs.lock().unwrap();
    assert!(cache.entries.is_empty());
    assert_eq!(cache.bytes, 0);
    assert_eq!(result.state().hash_tree_root().unwrap(), record.state_root);
}

fn retained_checkpoint_record(repository: &AdmittedStateRepository, root: Root) -> Derived {
    let origin_state_root = repository.lock().unwrap().records[&root].state_root;
    let post = repository.block_state(root).unwrap().unwrap();
    let epoch = 1;
    let slot = repository.network.config.slots_per_epoch;
    let boundary = replay_slots(post.state(), slot, &repository.runtime.transitions, 8192).unwrap();
    let state_root = boundary.hash_tree_root().unwrap();
    repository.compact.persist_state(&boundary, state_root).unwrap();
    // This is ordinary metadata; check_derived must authenticate it through native replay.
    Derived {
        origin_block_root: root,
        origin_state_root,
        state_root,
        slot,
        fork: boundary.schema_fork(),
        epoch,
    }
}
pub(super) fn retained_phase0_history(
    cache_bytes: Option<usize>,
) -> (TempDir, Arc<AdmittedStateRepository>, Arc<RepositoryStateSource>, CompactStore, [Root; 3]) {
    let (config, state, keys, _) = genuine(Fork::Phase0);
    let parameters = RuntimeParameters::from_config(&config).unwrap();
    let cache_bytes =
        cache_bytes.unwrap_or(StateData::Classic(&state).allocation_bytes().unwrap() * 2 + 4096);
    let genesis = VerifiedGenesis::authenticate(
        config.clone(),
        state.to_ssz().unwrap().into(),
        BootstrapDecodeLimits::default(),
    )
    .unwrap();
    let mut roots = [genesis.block_root(); 3];
    let directory = TempDir::new().unwrap();
    let network = PersistenceNetwork::from_config(config.clone()).unwrap();
    let shared = SharedDataStore::open(directory.path(), network.identity(), 0).unwrap();
    let repository = Arc::new(
        AdmittedStateRepository::open(
            shared,
            network,
            CompactStateLimits {
                hot_cache_schema_bytes: cache_bytes,
                ..CompactStateLimits::default()
            },
        )
        .unwrap(),
    );
    repository.persist_genesis(&genesis, BootstrapDecodeLimits::default()).unwrap();
    let (state, block) = genesis.into_import_inputs(&config).unwrap();
    let mut hot = Store::get_store(
        Arc::unwrap_or_clone(state),
        Arc::unwrap_or_clone(block),
        parameters.fork_choice,
    )
    .unwrap();
    hot.set_transition_parameters(parameters.transitions.clone()).unwrap();
    let source = RepositoryStateSource::open(repository.clone()).unwrap();
    let mut graph = CompactStore::from_store(
        hot.clone(),
        source.clone(),
        CompactGraphLimits { max_hot_state_bytes: 0, ..CompactGraphLimits::default() },
    )
    .unwrap();
    for (slot, root) in roots.iter_mut().enumerate().skip(1) {
        let signed = SignedBeaconBlock::Phase0(
            phase0_signed_child(&hot, &keys, &parameters.transitions),
            Preset::Minimal,
        );
        let time = config.clock().unwrap().start_of_slot(slot as u64).unwrap();
        hot.on_tick(time).unwrap();
        graph.on_tick(time).unwrap();
        hot.on_block(&signed).unwrap();
        graph.on_block(&signed).unwrap();
        *root = signed.message_root().unwrap();
    }
    (directory, repository, source, graph, roots)
}

fn retained_history_allocation_bytes() -> [usize; 3] {
    let (_directory, repository, source, graph, roots) =
        retained_phase0_history(Some(64 * 1024 * 1024));
    let native = roots.map(|root| {
        graph.read_block_state(root).unwrap().unwrap().data().allocation_bytes().unwrap()
    });
    let (repository, _source) = reopen_retained_source(repository, source, graph);
    let decoded = roots.map(|root| {
        let decoded = repository.block_state(root).unwrap().unwrap();
        StateData::Classic(decoded.state()).allocation_bytes().unwrap()
    });
    // Both real cache entry forms occur in these tests. Native pushes can reserve spare capacity
    // (including ETH1 votes), while a fresh cold decode allocates the actual serialized count.
    let maximum = std::array::from_fn(|index| native[index].max(decoded[index]));
    let minimum_three = (0..3).map(|index| native[index].min(decoded[index])).sum::<usize>();
    let maximum_two = maximum.iter().sum::<usize>() - maximum.iter().min().unwrap();
    assert!(maximum_two < minimum_three);
    maximum
}

fn retained_checkpoint_proof_cache_budget() -> usize {
    let forms = [false, true].map(|reopened| {
        let (_directory, repository, source, roots) = if reopened {
            reopened_retained_history()
        } else {
            let (directory, repository, source, graph, roots) =
                retained_phase0_history(Some(64 * 1024 * 1024));
            drop(graph);
            (directory, repository, source, roots)
        };
        let first_record = retained_checkpoint_record(&repository, roots[1]);
        let second_record = retained_checkpoint_record(&repository, roots[2]);
        source.check_derived(&first_record).unwrap();
        source.check_derived(&second_record).unwrap();
        let cache = source.replay_proofs.lock().unwrap();
        assert_eq!(cache.entries.len(), 2);
        [&first_record, &second_record]
            .map(|record| cache.entries[&(record.origin_block_root, record.epoch)].bytes)
    });
    let budget = forms.iter().flatten().copied().max().unwrap();
    assert!(forms[0][0].min(forms[1][0]) + forms[0][1].min(forms[1][1]) > budget);
    budget
}

fn reopened_retained_history(
) -> (TempDir, Arc<AdmittedStateRepository>, Arc<RepositoryStateSource>, [Root; 3]) {
    let (directory, repository, source, graph, roots) =
        retained_phase0_history(Some(64 * 1024 * 1024));
    let (repository, source) = reopen_retained_source(repository, source, graph);
    (directory, repository, source, roots)
}

fn reopen_retained_source(
    repository: Arc<AdmittedStateRepository>,
    source: Arc<RepositoryStateSource>,
    graph: CompactStore,
) -> (Arc<AdmittedStateRepository>, Arc<RepositoryStateSource>) {
    let shared = repository.shared.clone();
    let network = repository.network.clone();
    drop(graph);
    drop(source);
    drop(repository);
    // A fresh storage cache measures the real cold-decoded allocations, including their Vec
    // capacities. Original input allocation capacity and SSZ byte length are different budgets.
    let repository = Arc::new(
        AdmittedStateRepository::open(shared, network, CompactStateLimits::default()).unwrap(),
    );
    let source = RepositoryStateSource::open(repository.clone()).unwrap();
    (repository, source)
}

#[test]
fn native_same_slot_checkpoint_reuses_its_root_and_rejects_changed_record() {
    let (_directory, repository, source, _graph, roots) =
        retained_phase0_history(Some(1024 * 1024));
    let origin = repository.block_state(roots[0]).unwrap().unwrap();
    let record = Derived {
        origin_block_root: roots[0],
        origin_state_root: origin.root(),
        state_root: origin.root(),
        slot: 0,
        fork: origin.state().schema_fork(),
        epoch: 0,
    };
    let boundary = source.check_derived(&record).unwrap();
    assert!(std::ptr::eq(origin.state(), boundary.state()));
    let mut changed = record;
    changed.state_root = repository.block_state(roots[1]).unwrap().unwrap().root();
    assert!(matches!(source.check_derived(&changed), Err(PersistenceError::Linkage)));
    assert_eq!(boundary.state().hash_tree_root().unwrap(), origin.root());
}

#[test]
fn configured_decoded_budget_keeps_idle_ownership_in_the_source() {
    let (_directory, repository, source, graph, roots) =
        retained_phase0_history(Some(256 * 1024 * 1024));
    let limits = source.graph_limits();
    assert_eq!(limits.max_hot_state_bytes, 0);
    assert_eq!(repository.schema_cache_bytes, 256 * 1024 * 1024);
    assert_eq!(limits.max_hot_state_bytes + repository.schema_cache_bytes, 256 * 1024 * 1024);
    assert!(limits.max_resident_hot_state_bytes >= limits.max_hot_state_bytes);
    for root in roots {
        let held = graph.read_block_state(root).unwrap().unwrap();
        source.trim_idle_caches().unwrap();
        let cache = source.leases.lock().unwrap();
        for (key, entry) in &cache.entries {
            assert!(repository.compact.owns_cached_state(key.0, entry.owner.state()).unwrap());
        }
        assert!(cache.bytes <= repository.schema_cache_bytes);
        assert!(held.hash_tree_root().is_ok());
    }
}

#[test]
fn source_cache_cannot_hide_an_invalid_original_resident_limit() {
    let (config, _, _, _) = genuine(Fork::Phase0);
    let directory = TempDir::new().unwrap();
    let network = PersistenceNetwork::from_config(config).unwrap();
    let shared = SharedDataStore::open(directory.path(), network.identity(), 0).unwrap();
    assert!(matches!(
        AdmittedStateRepository::open(
            shared,
            network,
            CompactStateLimits {
                hot_cache_schema_bytes: 1024 * 1024,
                max_resident_schema_bytes: 512 * 1024,
                ..Default::default()
            },
        ),
        Err(PersistenceError::Compact(CompactStateError::Capacity))
    ));
}

#[test]
fn active_original_event_lease_reuses_allocation_without_hiding_original_pin_loss() {
    let (_directory, repository, _source, graph, roots) = retained_phase0_history(None);
    let event = repository
        .append_event(
            DurableEvent {
                sequence: 0,
                begin_ms: graph.time_ms(),
                completed_ms: graph.time_ms(),
                received_ms: 0,
                historical: false,
                action: EventAction::Block(roots[1]),
            },
            None,
        )
        .unwrap();
    let (original, guard) = repository.original_event_with_lease(event.sequence).unwrap();
    let guard = guard.unwrap();
    let identity = guard.downgrade();
    assert_eq!(original, event);
    let expected_root = repository.lock().unwrap().records[&roots[1]].state_root;
    assert_eq!(guard.root(), expected_root);

    // Other active reads can replace idle cache ownership. This per-attempt guard preserves the
    // same checked complete allocation, so later validation can reuse it through the live index.
    for root in [roots[0], roots[2]] {
        repository.block_state(root).unwrap().unwrap();
    }
    let (_, checked) = repository.original_event_with_lease(event.sequence).unwrap();
    assert!(identity.matches(&checked.unwrap()));
    let original = repository.lock().unwrap().records[&roots[1]].original;
    repository.shared.release(Owner::Consensus(roots[1]), original).unwrap();
    assert!(matches!(
        repository.original_event_with_lease(event.sequence),
        Err(PersistenceError::Linkage)
    ));
    assert_eq!(guard.state().hash_tree_root().unwrap(), expected_root);
}
