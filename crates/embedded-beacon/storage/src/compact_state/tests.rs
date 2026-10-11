//! Persistence tests use complete structural data; they grant no consensus authority.
//! The node's separate genuine signed genesis tests exercise authenticated state inputs.

use super::*;
use reth_embedded_beacon_primitives::sha256;
use reth_embedded_beacon_shared_data::journal::inject_post_rename_failure;
use reth_embedded_beacon_state_transition::ssz::{phase0, List, Ssz};
use reth_fs_util as fs;
use tempfile::TempDir;

fn identity() -> StoreIdentity {
    StoreIdentity { genesis_root: [1; 32], config_digest: [2; 32] }
}

fn structural_state(preset: Preset, slot: u64) -> BeaconState {
    let mut state = phase0::BeaconState::default_value(preset).unwrap();
    state.genesis_validators_root = identity().genesis_root;
    state.slot = slot;
    if preset == Preset::Mainnet {
        for (index, value) in state.randao_mixes.values.iter_mut().enumerate() {
            *value = sha256(&(index as u64).to_le_bytes());
        }
        state.balances = List::from_vec((0..8_192).collect());
    }
    BeaconState::Phase0(state, preset)
}

fn open(directory: &TempDir, limits: CompactStateLimits) -> (SharedDataStore, CompactStateStore) {
    let shared = SharedDataStore::open(directory.path(), identity(), 0).unwrap();
    let compact = CompactStateStore::open(shared.clone(), limits).unwrap();
    (shared, compact)
}

fn record(state: &BeaconState) -> StateRecord {
    StateRecord {
        schema_version: 1,
        state_root: state.hash_tree_root().unwrap(),
        genesis_root: state.genesis_validators_root(),
        slot: state.slot(),
        fork: state.schema_fork(),
        preset: StoredPreset::from_preset(state.preset()),
        ssz_bytes: state.to_ssz().unwrap().len() as u64,
        manifest: ObjectId {
            kind: ObjectKind::BeaconStateManifest,
            encoding: Encoding::Raw,
            digest: [0; 32],
        },
    }
}

fn object_path(directory: &TempDir, id: ObjectId) -> std::path::PathBuf {
    let digest = id.digest.iter().map(|byte| format!("{byte:02x}")).collect::<String>();
    directory
        .path()
        .join("shared-data-v1/objects")
        .join(format!("{:02x}-{:02x}-{digest}.obj", id.kind as u8, id.encoding as u8))
}

#[test]
fn component_sharing_preserves_variable_offsets_branches_and_restart() {
    let directory = TempDir::new().unwrap();
    let base = structural_state(Preset::Mainnet, 1);
    let mut second = base.clone();
    let mut branch = base.clone();
    if let BeaconState::Phase0(value, _) = &mut second {
        value.slot = 2;
        value.balances.values.push(17);
    }
    if let BeaconState::Phase0(value, _) = &mut branch {
        value.slot = 3;
        value.balances.values[0] = 90;
    }
    let states = [base, second, branch];
    let roots = states.each_ref().map(|state| state.hash_tree_root().unwrap());
    let full_bytes = states.iter().map(|state| state.to_ssz().unwrap().len() as u64).sum::<u64>();
    let (shared, compact) = open(&directory, CompactStateLimits::default());
    compact.persist_state(&states[0], roots[0]).unwrap();
    let first_bytes = shared.stats().unwrap().physical_bytes;
    compact.persist_state(&states[1], roots[1]).unwrap();
    let added_bytes = shared.stats().unwrap().physical_bytes - first_bytes;
    compact.persist_state(&states[2], roots[2]).unwrap();
    assert!(directory
        .path()
        .join("shared-data-v1/refs/snapshot-00000000000000000004.commit")
        .exists());
    assert!(!directory
        .path()
        .join("shared-data-v1/refs/snapshot-00000000000000000005.commit")
        .exists());
    let physical = shared.stats().unwrap().physical_bytes;
    assert!(physical < full_bytes);
    assert!(added_bytes < states[0].to_ssz().unwrap().len() as u64 / 2);
    eprintln!("compact structural test measurement: full_serializations={full_bytes} unique_object_bytes={physical} second_state_new_bytes={added_bytes}");
    for (state, root) in states.iter().zip(roots) {
        let lease = compact.lease(root, Fork::Phase0, Preset::Mainnet).unwrap().unwrap();
        assert_eq!(lease.state().to_ssz().unwrap(), state.to_ssz().unwrap());
        assert_eq!(lease.root(), root);
    }
    assert_eq!(compact.stats().unwrap().retained_states, 3);
    drop(compact);
    drop(shared);
    let (_, compact) = open(&directory, CompactStateLimits::default());
    for (state, root) in states.iter().zip(roots) {
        assert_eq!(
            compact
                .lease(root, Fork::Phase0, Preset::Mainnet)
                .unwrap()
                .unwrap()
                .state()
                .to_ssz()
                .unwrap(),
            state.to_ssz().unwrap()
        );
    }
    assert_eq!(compact.stats().unwrap().retained_states, 3);
}

#[test]
fn active_leases_share_one_budget_and_survive_cache_eviction() {
    let directory = TempDir::new().unwrap();
    let first = structural_state(Preset::Minimal, 1);
    let second = structural_state(Preset::Minimal, 2);
    let decoded =
        BeaconState::from_ssz(&first.to_ssz().unwrap(), Fork::Phase0, Preset::Minimal).unwrap();
    let footprint = decoded.heap_bytes().unwrap() + std::mem::size_of::<BeaconState>();
    let limits = CompactStateLimits {
        hot_cache_schema_bytes: footprint,
        max_resident_schema_bytes: footprint,
        ..Default::default()
    };
    let (_, compact) = open(&directory, limits);
    let first_root = first.hash_tree_root().unwrap();
    let second_root = second.hash_tree_root().unwrap();
    compact.persist_state(&first, first_root).unwrap();
    compact.persist_state(&second, second_root).unwrap();
    let lease = compact.lease(first_root, Fork::Phase0, Preset::Minimal).unwrap().unwrap();
    let another = compact.lease(first_root, Fork::Phase0, Preset::Minimal).unwrap().unwrap();
    assert_eq!(compact.stats().unwrap().resident_schema_bytes, footprint);
    assert!(matches!(
        compact.lease(second_root, Fork::Phase0, Preset::Minimal),
        Err(CompactStateError::Capacity)
    ));
    assert_eq!(compact.stats().unwrap().cached_states, 0);
    assert_eq!(lease.state().to_ssz().unwrap(), first.to_ssz().unwrap());
    drop(lease);
    assert_eq!(compact.stats().unwrap().resident_schema_bytes, footprint);
    drop(another);
    assert_eq!(compact.stats().unwrap().resident_schema_bytes, 0);
    let second_lease = compact.lease(second_root, Fork::Phase0, Preset::Minimal).unwrap().unwrap();
    drop(compact);
    assert_eq!(second_lease.state().hash_tree_root().unwrap(), second_root);
}

#[test]
fn wrong_root_identity_schema_or_capacity_never_admits_a_state() {
    let directory = TempDir::new().unwrap();
    let state = structural_state(Preset::Minimal, 1);
    let root = state.hash_tree_root().unwrap();
    let (shared, compact) = open(&directory, CompactStateLimits::default());
    assert!(matches!(compact.persist_state(&state, [7; 32]), Err(CompactStateError::RootMismatch)));
    let mut wrong_network = state.clone();
    if let BeaconState::Phase0(value, _) = &mut wrong_network {
        value.genesis_validators_root = [3; 32];
    }
    assert!(matches!(
        compact.persist_state(&wrong_network, wrong_network.hash_tree_root().unwrap()),
        Err(CompactStateError::IdentityMismatch)
    ));
    assert_eq!(shared.stats().unwrap().physical_objects, 0);
    compact.persist_state(&state, root).unwrap();
    assert!(matches!(
        compact.lease(root, Fork::Altair, Preset::Minimal),
        Err(CompactStateError::SchemaMismatch)
    ));
    assert!(matches!(
        compact.lease(root, Fork::Phase0, Preset::Mainnet),
        Err(CompactStateError::SchemaMismatch)
    ));
    drop(compact);
    let compact = CompactStateStore::open(
        shared,
        CompactStateLimits { max_retained_states: 1, ..Default::default() },
    )
    .unwrap();
    let second = structural_state(Preset::Minimal, 2);
    assert!(matches!(
        compact.persist_state(&second, second.hash_tree_root().unwrap()),
        Err(CompactStateError::Capacity)
    ));
    assert_eq!(compact.stats().unwrap().retained_states, 1);
    assert!(compact.lease(root, Fork::Phase0, Preset::Minimal).unwrap().is_some());
}

#[test]
fn one_shared_backend_has_one_compact_namespace_and_writer() {
    let directory = TempDir::new().unwrap();
    let (shared, compact) = open(&directory, CompactStateLimits::default());
    let state = structural_state(Preset::Minimal, 1);
    let root = state.hash_tree_root().unwrap();
    compact.persist_state(&state, root).unwrap();
    assert!(matches!(
        CompactStateStore::open(shared.clone(), CompactStateLimits::default()),
        Err(CompactStateError::Shared(StoreError::AlreadyOpen(_)))
    ));
    shared.collect_garbage().unwrap();
    assert_eq!(compact.lease(root, Fork::Phase0, Preset::Minimal).unwrap().unwrap().root(), root);
}

#[test]
fn changed_truncated_or_missing_components_fail_before_reconstruction() {
    for mutation in 0..3 {
        let directory = TempDir::new().unwrap();
        let (shared, compact) = open(&directory, CompactStateLimits::default());
        let state = structural_state(Preset::Minimal, 1);
        let root = state.hash_tree_root().unwrap();
        compact.persist_state(&state, root).unwrap();
        let id = shared
            .owners()
            .into_iter()
            .flat_map(|owner| shared.owner_objects(owner))
            .find(|id| {
                id.kind == ObjectKind::BeaconStateComponent &&
                    fs::metadata(object_path(&directory, *id)).unwrap().len() > 0
            })
            .unwrap();
        let path = object_path(&directory, id);
        let mut bytes = fs::read(&path).unwrap();
        match mutation {
            0 => {
                bytes[0] ^= 1;
                fs::write(&path, bytes).unwrap();
            }
            1 => {
                bytes.pop();
                fs::write(&path, bytes).unwrap();
            }
            _ => fs::remove_file(&path).unwrap(),
        }
        assert!(compact.lease(root, Fork::Phase0, Preset::Minimal).is_err());
        drop(compact);
        drop(shared);
        assert!(SharedDataStore::open(directory.path(), identity(), 0).is_err());
    }
}

#[test]
fn uncertain_root_commit_blocks_access_then_reopens_complete_new_generation() {
    let directory = TempDir::new().unwrap();
    let (shared, compact) = open(&directory, CompactStateLimits::default());
    let first = structural_state(Preset::Minimal, 1);
    let second = structural_state(Preset::Minimal, 2);
    let first_root = first.hash_tree_root().unwrap();
    let second_root = second.hash_tree_root().unwrap();
    compact.persist_state(&first, first_root).unwrap();
    let lease = compact.lease(first_root, Fork::Phase0, Preset::Minimal).unwrap().unwrap();
    inject_post_rename_failure(
        shared
            .namespace_root()
            .join("beacon-state-components-v1/journal/snapshot-00000000000000000003.commit"),
    );
    assert!(matches!(
        compact.persist_state(&second, second_root),
        Err(CompactStateError::Shared(StoreError::PublicationUncertain(_)))
    ));
    assert!(matches!(compact.stats(), Err(CompactStateError::Shared(StoreError::JournalPoisoned))));
    assert!(matches!(
        compact.lease(first_root, Fork::Phase0, Preset::Minimal),
        Err(CompactStateError::Shared(StoreError::JournalPoisoned))
    ));
    assert_eq!(lease.root(), first_root);
    drop(compact);
    drop(shared);
    let (_, compact) = open(&directory, CompactStateLimits::default());
    assert_eq!(compact.stats().unwrap().retained_states, 2);
    assert_eq!(
        compact
            .lease(second_root, Fork::Phase0, Preset::Minimal)
            .unwrap()
            .unwrap()
            .state()
            .to_ssz()
            .unwrap(),
        second.to_ssz().unwrap()
    );
}

#[test]
fn orphan_cleanup_keeps_all_committed_branches_and_independent_owners() {
    let directory = TempDir::new().unwrap();
    let (shared, compact) = open(&directory, CompactStateLimits::default());
    let state = structural_state(Preset::Minimal, 1);
    let root = state.hash_tree_root().unwrap();
    compact.persist_state(&state, root).unwrap();
    let orphan = structural_state(Preset::Minimal, 99);
    let encoded = orphan.to_ssz().unwrap();
    let layout = BeaconState::container_field_layout(Fork::Phase0, Preset::Minimal);
    let fields = container_fields(&encoded, &layout).unwrap();
    let publication = graph::Publication::build(&fields, &record(&orphan)).unwrap();
    shared.put_committed_pinned_batch(&publication.borrowed()).unwrap();
    let owners = [
        Owner::Pool([7; 32]),
        Owner::Consensus([8; 32]),
        Owner::Recovery([9; 32]),
        Owner::Checkpoint([10; 32]),
    ];
    let independent = shared
        .put(ObjectKind::Blob, Encoding::Raw, b"independent execution bytes", owners[0])
        .unwrap();
    for owner in owners.iter().skip(1) {
        shared.pin(*owner, independent).unwrap();
    }
    drop(compact);
    let compact = CompactStateStore::open(shared.clone(), CompactStateLimits::default()).unwrap();
    assert!(shared.owner_objects(Owner::CompactState(orphan.hash_tree_root().unwrap())).is_empty());
    shared.collect_garbage().unwrap();
    for owner in owners {
        assert_eq!(shared.owner_objects(owner), vec![independent]);
    }
    assert_eq!(&*shared.get(independent).unwrap().unwrap(), b"independent execution bytes");
    assert_eq!(
        compact
            .lease(root, Fork::Phase0, Preset::Minimal)
            .unwrap()
            .unwrap()
            .state()
            .to_ssz()
            .unwrap(),
        state.to_ssz().unwrap()
    );
}

#[test]
fn semantically_wrong_but_checksummed_graph_fails_complete_root_verification() {
    let directory = TempDir::new().unwrap();
    let (shared, compact) = open(&directory, CompactStateLimits::default());
    let state = structural_state(Preset::Minimal, 1);
    let root = state.hash_tree_root().unwrap();
    compact.persist_state(&state, root).unwrap();
    let mut altered = state.clone();
    if let BeaconState::Phase0(value, _) = &mut altered {
        value.randao_mixes.values[0] = [11; 32];
    }
    let encoded = altered.to_ssz().unwrap();
    let layout = BeaconState::container_field_layout(Fork::Phase0, Preset::Minimal);
    let fields = container_fields(&encoded, &layout).unwrap();
    let publication = graph::Publication::build(&fields, &record(&state)).unwrap();
    shared.put_committed_pinned_batch(&publication.borrowed()).unwrap();
    compact.inner.lock().records.get_mut(&root).unwrap().manifest = publication.root;
    assert!(matches!(
        compact.lease(root, Fork::Phase0, Preset::Minimal),
        Err(CompactStateError::RootMismatch)
    ));
    assert!(compact.lease_retained(root, Fork::Phase0, Preset::Minimal).is_err());
}

#[test]
fn oversized_descriptor_lengths_are_rejected_before_state_allocation() {
    let directory = TempDir::new().unwrap();
    let (shared, compact) = open(&directory, CompactStateLimits::default());
    let state = structural_state(Preset::Minimal, 1);
    let root = state.hash_tree_root().unwrap();
    compact.persist_state(&state, root).unwrap();
    let original = compact.inner.lock().records[&root].manifest;
    let mut bytes = shared.get(original).unwrap().unwrap().to_vec();
    bytes[93..101].copy_from_slice(&u64::MAX.to_le_bytes());
    let forged = shared
        .put(ObjectKind::BeaconStateManifest, Encoding::Raw, &bytes, Owner::CompactState(root))
        .unwrap();
    compact.inner.lock().records.get_mut(&root).unwrap().manifest = forged;
    assert!(matches!(
        compact.lease(root, Fork::Phase0, Preset::Minimal),
        Err(CompactStateError::InvalidDescriptor)
    ));
    assert_eq!(compact.stats().unwrap().resident_schema_bytes, 0);
}

#[test]
fn borrowed_gloas_persistence_matches_enum_bytes_and_reopens_both_presets() {
    for preset in [Preset::Minimal, Preset::Mainnet] {
        let directory = TempDir::new().unwrap();
        let mut state = gloas::BeaconState::default_value(preset).unwrap();
        state.genesis_validators_root = identity().genesis_root;
        state.slot = 19;
        let root = state.hash_tree_root(preset).unwrap();
        let expected = state.to_ssz(preset).unwrap();
        let (shared, compact) = open(&directory, CompactStateLimits::default());
        compact.persist_gloas(&state, preset, root).unwrap();
        let published_bytes = shared.stats().unwrap().physical_bytes;
        compact.persist_state(&BeaconState::Gloas(state, preset), root).unwrap();
        assert_eq!(shared.stats().unwrap().physical_bytes, published_bytes);
        assert_eq!(compact.stats().unwrap().retained_states, 1);
        assert_eq!(
            compact.lease(root, Fork::Gloas, preset).unwrap().unwrap().state().to_ssz().unwrap(),
            expected
        );
        drop(compact);
        drop(shared);
        let (_, reopened) = open(&directory, CompactStateLimits::default());
        reopened.ensure_retained(root).unwrap();
        assert_eq!(
            reopened.lease(root, Fork::Gloas, preset).unwrap().unwrap().state().to_ssz().unwrap(),
            expected
        );
    }
}

#[test]
fn borrowed_gloas_wrong_commitment_identity_and_preset_never_publish() {
    let directory = TempDir::new().unwrap();
    let mut state = gloas::BeaconState::default_value(Preset::Minimal).unwrap();
    state.genesis_validators_root = identity().genesis_root;
    let root = state.hash_tree_root(Preset::Minimal).unwrap();
    let (shared, compact) = open(&directory, CompactStateLimits::default());
    assert!(matches!(
        compact.persist_gloas(&state, Preset::Minimal, [9; 32]),
        Err(CompactStateError::RootMismatch)
    ));
    assert!(compact.persist_gloas(&state, Preset::Mainnet, root).is_err());
    state.genesis_validators_root = [8; 32];
    let foreign_root = state.hash_tree_root(Preset::Minimal).unwrap();
    assert!(matches!(
        compact.persist_gloas(&state, Preset::Minimal, foreign_root),
        Err(CompactStateError::IdentityMismatch)
    ));
    assert_eq!(compact.stats().unwrap().retained_states, 0);
    assert_eq!(shared.stats().unwrap().physical_bytes, 0);
}

#[test]
fn descriptor_release_preserves_selected_branches_active_leases_and_independent_owners() {
    let directory = TempDir::new().unwrap();
    let (shared, compact) = open(&directory, CompactStateLimits::default());
    let states = [
        structural_state(Preset::Minimal, 1),
        structural_state(Preset::Minimal, 2),
        structural_state(Preset::Minimal, 3),
    ];
    let roots = states.each_ref().map(|state| state.hash_tree_root().unwrap());
    for (state, root) in states.iter().zip(roots) {
        compact.persist_state(state, root).unwrap();
    }
    let active = compact.lease(roots[0], Fork::Phase0, Preset::Minimal).unwrap().unwrap();
    let el = shared
        .put(
            ObjectKind::ExecutionPayload,
            Encoding::Raw,
            b"independent EL bytes",
            Owner::Pool([9; 32]),
        )
        .unwrap();
    let consensus = shared
        .put(
            ObjectKind::BeaconBlock,
            Encoding::Ssz,
            b"independent original CL bytes",
            Owner::Consensus([8; 32]),
        )
        .unwrap();
    let keep = BTreeSet::from([roots[1], roots[2]]);
    assert_eq!(compact.release_retained_states(&keep).unwrap(), 1);
    assert_eq!(compact.release_retained_states(&keep).unwrap(), 0);
    assert_eq!(compact.stats().unwrap().retained_states, 2);
    assert!(compact.lease(roots[0], Fork::Phase0, Preset::Minimal).unwrap().is_none());
    assert_eq!(active.state().hash_tree_root().unwrap(), roots[0]);
    assert!(!shared.owners().contains(&Owner::CompactState(roots[0])));
    assert_eq!(shared.owner_objects(Owner::Pool([9; 32])), vec![el]);
    assert_eq!(shared.owner_objects(Owner::Consensus([8; 32])), vec![consensus]);
    shared.collect_garbage().unwrap();
    for root in &keep {
        compact.ensure_retained(*root).unwrap();
    }
    drop(compact);
    drop(shared);
    let (shared, reopened) = open(&directory, CompactStateLimits::default());
    assert_eq!(reopened.stats().unwrap().retained_states, 2);
    assert!(reopened.lease(roots[0], Fork::Phase0, Preset::Minimal).unwrap().is_none());
    for (state, root) in states[1..].iter().zip(&roots[1..]) {
        assert_eq!(
            reopened
                .lease(*root, Fork::Phase0, Preset::Minimal)
                .unwrap()
                .unwrap()
                .state()
                .to_ssz()
                .unwrap(),
            state.to_ssz().unwrap()
        );
    }
    assert_eq!(shared.owner_objects(Owner::Pool([9; 32])), vec![el]);
    assert_eq!(active.state().hash_tree_root().unwrap(), roots[0]);
}

#[test]
fn uncertain_descriptor_release_never_releases_live_pins_and_reopens_exact_committed_union() {
    let directory = TempDir::new().unwrap();
    let (shared, compact) = open(&directory, CompactStateLimits::default());
    let first = structural_state(Preset::Minimal, 1);
    let second = structural_state(Preset::Minimal, 2);
    let a = first.hash_tree_root().unwrap();
    let b = second.hash_tree_root().unwrap();
    compact.persist_state(&first, a).unwrap();
    compact.persist_state(&second, b).unwrap();
    inject_post_rename_failure(
        shared
            .namespace_root()
            .join("beacon-state-components-v1/journal/snapshot-00000000000000000004.commit"),
    );
    assert!(matches!(
        compact.release_retained_states(&BTreeSet::from([b])),
        Err(CompactStateError::Shared(StoreError::PublicationUncertain(_)))
    ));
    assert!(shared.owners().contains(&Owner::CompactState(a)));
    assert!(shared.owners().contains(&Owner::CompactState(b)));
    assert!(compact.lease(b, Fork::Phase0, Preset::Minimal).is_err());
    drop(compact);
    drop(shared);
    let (shared, reopened) = open(&directory, CompactStateLimits::default());
    assert!(reopened.lease(a, Fork::Phase0, Preset::Minimal).unwrap().is_none());
    reopened.ensure_retained(b).unwrap();
    assert!(!shared.owners().contains(&Owner::CompactState(a)));
    assert!(shared.owners().contains(&Owner::CompactState(b)));
}

#[test]
fn descriptor_release_rejects_unknown_keep_and_damaged_committed_ancestry_before_mutation() {
    let directory = TempDir::new().unwrap();
    let (shared, compact) = open(&directory, CompactStateLimits::default());
    let first = structural_state(Preset::Minimal, 1);
    let second = structural_state(Preset::Minimal, 2);
    let a = first.hash_tree_root().unwrap();
    let b = second.hash_tree_root().unwrap();
    compact.persist_state(&first, a).unwrap();
    compact.persist_state(&second, b).unwrap();
    assert!(matches!(
        compact.release_retained_states(&BTreeSet::from([[99; 32]])),
        Err(CompactStateError::InvalidDescriptor)
    ));
    assert_eq!(compact.stats().unwrap().retained_states, 2);
    let manifest = compact.inner.lock().records[&a].manifest;
    fs::write(object_path(&directory, manifest), b"damaged committed descriptor").unwrap();
    assert!(compact.release_retained_states(&BTreeSet::from([b])).is_err());
    assert_eq!(compact.stats().unwrap().retained_states, 2);
    assert!(shared.owners().contains(&Owner::CompactState(a)));
    assert!(shared.owners().contains(&Owner::CompactState(b)));
}

#[test]
fn checked_persistence_seeds_one_accounted_allocation_and_restart_rechecks_bytes() {
    let directory = TempDir::new().unwrap();
    let state = structural_state(Preset::Minimal, 1);
    let root = state.hash_tree_root().unwrap();
    let (shared, compact) = open(&directory, CompactStateLimits::default());
    compact.persist_cached_state(&state, root).unwrap();
    let stats = compact.stats().unwrap();
    assert_eq!(stats.cached_states, 1);
    assert_eq!(stats.cached_schema_bytes, stats.resident_schema_bytes);
    let before = shared.try_observe().unwrap().verified_disk_read_bytes_total;
    let first = compact.lease(root, Fork::Phase0, Preset::Minimal).unwrap().unwrap();
    compact.persist_cached_state(&state, root).unwrap();
    let second = compact.lease(root, Fork::Phase0, Preset::Minimal).unwrap().unwrap();
    assert!(std::ptr::eq(first.state(), second.state()));
    assert_eq!(compact.stats().unwrap().resident_schema_bytes, stats.resident_schema_bytes);
    assert_eq!(shared.try_observe().unwrap().verified_disk_read_bytes_total, before);
    assert_eq!(first.state().to_ssz().unwrap(), state.to_ssz().unwrap());
    compact.ensure_retained(root).unwrap();
    drop(first);
    drop(second);
    drop(compact);
    drop(shared);
    let (shared, reopened) = open(&directory, CompactStateLimits::default());
    assert_eq!(reopened.stats().unwrap().resident_schema_bytes, 0);
    let before = shared.try_observe().unwrap().verified_disk_read_bytes_total;
    let reconstructed = reopened.lease(root, Fork::Phase0, Preset::Minimal).unwrap().unwrap();
    assert_eq!(reconstructed.root(), root);
    assert_eq!(reconstructed.state().hash_tree_root().unwrap(), root);
    assert!(shared.try_observe().unwrap().verified_disk_read_bytes_total > before);
}

#[test]
fn owned_persistence_shares_exact_allocation_and_changed_owner_requires_new_commitment() {
    let directory = TempDir::new().unwrap();
    let mut state = Arc::new(structural_state(Preset::Minimal, 1));
    let root = state.hash_tree_root().unwrap();
    let (_shared, compact) = open(&directory, CompactStateLimits::default());
    compact.persist_cached_owned_state(state.clone(), root).unwrap();
    let lease = compact.lease(root, Fork::Phase0, Preset::Minimal).unwrap().unwrap();
    assert!(std::ptr::eq(state.as_ref(), lease.state()));
    assert!(compact.owns_cached_state(root, state.as_ref()).unwrap());
    let before = compact.stats().unwrap();
    compact.persist_cached_owned_state(state.clone(), root).unwrap();
    assert_eq!(compact.stats().unwrap(), before);

    let BeaconState::Phase0(changed, _) = Arc::make_mut(&mut state) else { unreachable!() };
    changed.slot += 1;
    assert!(!compact.owns_cached_state(root, state.as_ref()).unwrap());
    assert!(matches!(
        compact.persist_cached_owned_state(state, root),
        Err(CompactStateError::RootMismatch)
    ));
    assert_eq!(lease.state().slot(), 1);
    assert_eq!(lease.state().hash_tree_root().unwrap(), root);
    assert_eq!(compact.stats().unwrap(), before);
}

#[test]
fn owned_cache_eviction_does_not_extend_idle_ownership_or_invalidate_active_read() {
    let directory = TempDir::new().unwrap();
    let first = Arc::new(structural_state(Preset::Minimal, 1));
    let second = Arc::new(structural_state(Preset::Minimal, 2));
    let a = first.hash_tree_root().unwrap();
    let b = second.hash_tree_root().unwrap();
    let footprint = first.heap_bytes().unwrap() + std::mem::size_of::<BeaconState>();
    let (_shared, compact) = open(
        &directory,
        CompactStateLimits {
            hot_cache_schema_bytes: footprint,
            max_resident_schema_bytes: footprint * 2,
            ..CompactStateLimits::default()
        },
    );
    compact.persist_cached_owned_state(first.clone(), a).unwrap();
    let held = compact.lease(a, Fork::Phase0, Preset::Minimal).unwrap().unwrap();
    compact.persist_cached_owned_state(second.clone(), b).unwrap();
    assert!(!compact.owns_cached_state(a, first.as_ref()).unwrap());
    assert!(compact.owns_cached_state(b, second.as_ref()).unwrap());
    assert_eq!(compact.stats().unwrap().cached_states, 1);
    assert_eq!(compact.stats().unwrap().cached_schema_bytes, footprint);
    assert_eq!(compact.stats().unwrap().resident_schema_bytes, footprint * 2);
    assert_eq!(held.root(), a);
    assert!(std::ptr::eq(first.as_ref(), held.state()));
    drop(held);
    assert_eq!(compact.stats().unwrap().resident_schema_bytes, footprint);
}

#[test]
fn checked_persistence_cache_pressure_keeps_committed_data_and_active_lease() {
    let directory = TempDir::new().unwrap();
    let first = structural_state(Preset::Minimal, 1);
    let second = structural_state(Preset::Minimal, 2);
    let a = first.hash_tree_root().unwrap();
    let b = second.hash_tree_root().unwrap();
    let footprint = first.heap_bytes().unwrap() + std::mem::size_of::<BeaconState>();
    let (_shared, compact) = open(
        &directory,
        CompactStateLimits {
            hot_cache_schema_bytes: footprint,
            max_resident_schema_bytes: footprint,
            ..CompactStateLimits::default()
        },
    );
    compact.persist_cached_state(&first, a).unwrap();
    let held = compact.lease(a, Fork::Phase0, Preset::Minimal).unwrap().unwrap();
    compact.persist_cached_state(&second, b).unwrap();
    assert_eq!(compact.stats().unwrap().retained_states, 2);
    assert_eq!(compact.stats().unwrap().resident_schema_bytes, footprint);
    compact.ensure_retained(b).unwrap();
    assert!(matches!(
        compact.lease(b, Fork::Phase0, Preset::Minimal),
        Err(CompactStateError::Capacity)
    ));
    assert_eq!(held.state().hash_tree_root().unwrap(), a);
    drop(held);
    let next = compact.lease(b, Fork::Phase0, Preset::Minimal).unwrap().unwrap();
    assert_eq!(next.root(), b);
    assert_eq!(next.state().to_ssz().unwrap(), second.to_ssz().unwrap());
    assert!(compact.stats().unwrap().resident_schema_bytes <= footprint);
}

#[test]
fn checked_persistence_never_seeds_an_unverified_root_or_oversized_cache_value() {
    let directory = TempDir::new().unwrap();
    let state = structural_state(Preset::Minimal, 1);
    let root = state.hash_tree_root().unwrap();
    let (_shared, compact) = open(
        &directory,
        CompactStateLimits { hot_cache_schema_bytes: 1, ..CompactStateLimits::default() },
    );
    assert!(matches!(
        compact.persist_cached_state(&state, [7; 32]),
        Err(CompactStateError::RootMismatch)
    ));
    assert_eq!(compact.stats().unwrap().retained_states, 0);
    compact.persist_cached_state(&state, root).unwrap();
    assert_eq!(compact.stats().unwrap().retained_states, 1);
    assert_eq!(compact.stats().unwrap().cached_states, 0);
    assert_eq!(compact.stats().unwrap().resident_schema_bytes, 0);
    let checked = compact.lease(root, Fork::Phase0, Preset::Minimal).unwrap().unwrap();
    assert_eq!(checked.state().hash_tree_root().unwrap(), root);
}
#[test]
fn checked_gloas_persistence_seeds_verified_borrowed_schema() {
    let directory = TempDir::new().unwrap();
    let preset = Preset::Minimal;
    let mut state = gloas::BeaconState::default_value(preset).unwrap();
    state.genesis_validators_root = identity().genesis_root;
    state.slot = 1;
    let root = state.hash_tree_root(preset).unwrap();
    let (shared, compact) = open(&directory, CompactStateLimits::default());
    compact.persist_cached_gloas(&state, preset, root).unwrap();
    assert_eq!(compact.stats().unwrap().cached_states, 1);
    let before = shared.try_observe().unwrap().verified_disk_read_bytes_total;
    let lease = compact.lease(root, Fork::Gloas, preset).unwrap().unwrap();
    assert_eq!(lease.root(), root);
    assert_eq!(lease.state().to_ssz().unwrap(), state.to_ssz(preset).unwrap());
    assert_eq!(shared.try_observe().unwrap().verified_disk_read_bytes_total, before);
    assert!(matches!(
        compact.persist_cached_gloas(&state, preset, [7; 32]),
        Err(CompactStateError::RootMismatch)
    ));
    assert_eq!(compact.stats().unwrap().retained_states, 1);
}

#[test]
fn weak_state_identity_expires_without_residency_and_never_proves_durable_pins() {
    let directory = TempDir::new().unwrap();
    let (shared, compact) = open(
        &directory,
        CompactStateLimits { hot_cache_schema_bytes: 0, ..CompactStateLimits::default() },
    );
    let state = structural_state(Preset::Minimal, 1);
    let root = state.hash_tree_root().unwrap();
    compact.persist_state(&state, root).unwrap();
    let lease = compact.lease(root, Fork::Phase0, Preset::Minimal).unwrap().unwrap();
    let weak = lease.downgrade();
    let same = lease.clone();
    assert!(weak.matches(&same));
    assert!(compact.stats().unwrap().resident_schema_bytes > 0);
    drop(same);
    drop(lease);
    assert_eq!(compact.stats().unwrap().resident_schema_bytes, 0);
    let replacement = compact.lease(root, Fork::Phase0, Preset::Minimal).unwrap().unwrap();
    assert_eq!(replacement.root(), root);
    assert!(!weak.matches(&replacement));
    let current = replacement.downgrade();
    shared.release_owners(&[Owner::CompactState(root)]).unwrap();
    assert!(current.matches(&replacement));
    assert!(matches!(compact.ensure_retained(root), Err(CompactStateError::InvalidDescriptor)));
}

#[test]
fn retained_cold_lease_combines_graph_verification_and_reconstruction_reads() {
    let directory = TempDir::new().unwrap();
    let (shared, compact) = open(
        &directory,
        CompactStateLimits { hot_cache_schema_bytes: 0, ..CompactStateLimits::default() },
    );
    let state = structural_state(Preset::Mainnet, 1);
    let root = state.hash_tree_root().unwrap();
    compact.persist_state(&state, root).unwrap();
    let before = shared.try_observe().unwrap().verified_disk_read_bytes_total;
    compact.ensure_retained(root).unwrap();
    let separate = compact.lease(root, Fork::Phase0, Preset::Mainnet).unwrap().unwrap();
    assert_eq!(separate.state().to_ssz().unwrap(), state.to_ssz().unwrap());
    let separate_bytes = shared.try_observe().unwrap().verified_disk_read_bytes_total - before;
    drop(separate);

    let before = shared.try_observe().unwrap().verified_disk_read_bytes_total;
    let combined = compact.lease_retained(root, Fork::Phase0, Preset::Mainnet).unwrap().unwrap();
    let combined_bytes = shared.try_observe().unwrap().verified_disk_read_bytes_total - before;
    assert_eq!(combined.root(), root);
    assert_eq!(combined.state().to_ssz().unwrap(), state.to_ssz().unwrap());
    assert!(combined_bytes < separate_bytes);
}

#[test]
fn retained_lease_checks_root_and_child_pins_for_hot_and_cold_reads() {
    for cached in [false, true] {
        for root_pin in [false, true] {
            let directory = TempDir::new().unwrap();
            let (shared, compact) = open(&directory, CompactStateLimits::default());
            let state = structural_state(Preset::Minimal, 1);
            let root = state.hash_tree_root().unwrap();
            compact.persist_state(&state, root).unwrap();
            let existing = cached
                .then(|| compact.lease(root, Fork::Phase0, Preset::Minimal).unwrap().unwrap());
            let manifest = compact.inner.lock().records[&root].manifest;
            let owner =
                if root_pin { Owner::CompactState(root) } else { Owner::CompactNode(manifest) };
            shared.release_owners(&[owner]).unwrap();
            assert!(matches!(
                compact.lease_retained(root, Fork::Phase0, Preset::Minimal),
                Err(CompactStateError::InvalidDescriptor)
            ));
            if let Some(existing) = existing {
                assert_eq!(existing.state().hash_tree_root().unwrap(), root);
            }
        }
    }
}

#[test]
fn retained_lease_never_hides_corrupt_components_behind_a_decoded_cache() {
    for cached in [false, true] {
        let directory = TempDir::new().unwrap();
        let (shared, compact) = open(&directory, CompactStateLimits::default());
        let state = structural_state(Preset::Minimal, 1);
        let root = state.hash_tree_root().unwrap();
        compact.persist_state(&state, root).unwrap();
        let existing =
            cached.then(|| compact.lease(root, Fork::Phase0, Preset::Minimal).unwrap().unwrap());
        let component = shared
            .owners()
            .into_iter()
            .flat_map(|owner| shared.owner_objects(owner))
            .find(|id| {
                id.kind == ObjectKind::BeaconStateComponent &&
                    fs::metadata(object_path(&directory, *id)).unwrap().len() > 0
            })
            .unwrap();
        let path = object_path(&directory, component);
        let mut bytes = fs::read(&path).unwrap();
        bytes[0] ^= 1;
        fs::write(&path, bytes).unwrap();
        assert!(matches!(
            compact.lease_retained(root, Fork::Phase0, Preset::Minimal),
            Err(CompactStateError::Shared(StoreError::CorruptObject(id))) if id == component
        ));
        if let Some(existing) = existing {
            assert_eq!(existing.state().hash_tree_root().unwrap(), root);
        }
    }
}
