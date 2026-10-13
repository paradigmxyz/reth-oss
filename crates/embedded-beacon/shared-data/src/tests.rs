use super::*;
use proptest::prelude::*;
use tempfile::TempDir;

fn identity() -> StoreIdentity {
    StoreIdentity { genesis_root: [1; 32], config_digest: [2; 32] }
}

#[test]
fn committed_publication_preserves_exact_contents_independent_pins_and_restart() {
    let directory = TempDir::new().unwrap();
    let store = SharedDataStore::open(directory.path(), identity(), 0).unwrap();
    let bytes = b"exact original immutable fragment";
    let borrowed =
        CommittedObject::new(ObjectKind::BeaconStateComponent, Encoding::Raw, Cow::Borrowed(bytes));
    let owned = CommittedObject::new(
        ObjectKind::BeaconStateComponent,
        Encoding::Raw,
        Cow::Owned(bytes.to_vec()),
    );
    let owners = [Owner::CompactState([71; 32]), Owner::Pool([72; 32])];
    let id = ObjectId::new(ObjectKind::BeaconStateComponent, Encoding::Raw, bytes);
    assert_eq!(borrowed.id(), id);
    assert_eq!(owned.id(), id);
    assert_eq!(borrowed.bytes(), owned.bytes());
    assert_ne!(
        id,
        CommittedObject::new(ObjectKind::BeaconStateBranch, Encoding::Raw, Cow::Borrowed(bytes))
            .id()
    );
    assert_ne!(
        id,
        CommittedObject::new(ObjectKind::BeaconStateComponent, Encoding::Ssz, Cow::Borrowed(bytes))
            .id()
    );
    assert_eq!(
        store.put_committed_pinned_batch(&[borrowed.pinned(&owners[..1])]).unwrap(),
        vec![id]
    );
    let before = store.try_observe().unwrap().journal_commits_total;
    assert_eq!(store.put_committed_pinned_batch(&[owned.pinned(&owners)]).unwrap(), vec![id]);
    assert_eq!(store.try_observe().unwrap().journal_commits_total, before + 1);
    store.put_committed_pinned_batch(&[borrowed.pinned(&owners)]).unwrap();
    assert_eq!(store.try_observe().unwrap().journal_commits_total, before + 1);
    assert_eq!(store.stats().unwrap().physical_objects, 1);
    assert!(matches!(
        store.put_committed_pinned_batch(&[borrowed.pinned(&[])]),
        Err(StoreError::InvalidPublication)
    ));
    drop(store);
    let store = SharedDataStore::open(directory.path(), identity(), 0).unwrap();
    assert_eq!(store.get(id).unwrap().unwrap().as_ref(), bytes);
    for owner in owners {
        assert_eq!(store.owner_objects(owner), vec![id]);
    }
}

#[test]
fn committed_publication_checks_orphans_and_poison_before_publishing_pins() {
    let directory = TempDir::new().unwrap();
    let store = SharedDataStore::open(directory.path(), identity(), 0).unwrap();
    let object = CommittedObject::new(
        ObjectKind::BeaconStateComponent,
        Encoding::Raw,
        Cow::Borrowed(b"original complete component"),
    );
    let owners = [Owner::CompactState([73; 32])];
    let path = directory.path().join("shared-data-v1/objects").join(object.id().filename());
    fs::write(&path, b"corrupted orphan").unwrap();
    assert!(matches!(
        store.put_committed_pinned_batch(&[object.pinned(&owners)]),
        Err(StoreError::CorruptObject(id)) if id == object.id()
    ));
    assert!(store.owner_objects(owners[0]).is_empty());
    fs::remove_file(&path).unwrap();
    journal::inject_post_rename_failure(&path);
    assert!(matches!(
        store.put_committed_pinned_batch(&[object.pinned(&owners)]),
        Err(StoreError::PublicationUncertain(_))
    ));
    assert!(matches!(
        store.put_committed_pinned_batch(&[object.pinned(&owners)]),
        Err(StoreError::JournalPoisoned)
    ));
    drop(store);
    let store = SharedDataStore::open(directory.path(), identity(), 0).unwrap();
    assert_eq!(
        store.put_committed_pinned_batch(&[object.pinned(&owners)]).unwrap(),
        vec![object.id()]
    );
    assert_eq!(store.get(object.id()).unwrap().unwrap().as_ref(), object.bytes());
}

#[test]
fn committed_publication_rejects_conflicting_retained_sizes() {
    let directory = TempDir::new().unwrap();
    let store = SharedDataStore::open(directory.path(), identity(), 0).unwrap();
    let object = CommittedObject::new(
        ObjectKind::BeaconStateComponent,
        Encoding::Raw,
        Cow::Borrowed(b"immutable bytes"),
    );
    let owners = [Owner::CompactState([74; 32])];
    store.put_committed_pinned_batch(&[object.pinned(&owners)]).unwrap();
    store.inner.lock().objects.get_mut(&object.id()).unwrap().size += 1;
    assert!(matches!(
        store.put_committed_pinned_batch(&[object.pinned(&owners)]),
        Err(StoreError::CorruptObject(id)) if id == object.id()
    ));
}

#[test]
fn duplicate_owners_share_one_object_and_pool_eviction_retains_consensus() {
    let dir = TempDir::new().unwrap();
    let store = SharedDataStore::open(dir.path(), identity(), 100).unwrap();
    let pool = Owner::Pool([3; 32]);
    let consensus = Owner::Consensus([4; 32]);
    let id = store.put(ObjectKind::Cell, Encoding::Raw, b"cell", pool).unwrap();
    assert_eq!(id, store.put(ObjectKind::Cell, Encoding::Raw, b"cell", consensus).unwrap());
    store.pin(pool, id).unwrap();
    assert_eq!(store.stats().unwrap().physical_bytes, 4);
    assert_eq!(store.stats().unwrap().logical_bytes, 8);
    store.release(pool, id).unwrap();
    store.release(pool, id).unwrap();
    assert_eq!(store.collect_garbage().unwrap(), 0);
    assert_eq!(&*store.get(id).unwrap().unwrap(), b"cell");
    let reader = store.get(id).unwrap().unwrap();
    store.release(consensus, id).unwrap();
    assert_eq!(store.collect_garbage().unwrap(), 1);
    assert_eq!(&*reader, b"cell");
    assert!(store.get(id).unwrap().is_none());
}

#[test]
fn reference_recovery_preserves_owner_sets_and_rejects_network_changes() {
    let dir = TempDir::new().unwrap();
    let owner = Owner::Recovery([5; 32]);
    let id = {
        let store = SharedDataStore::open(dir.path(), identity(), 0).unwrap();
        store.put(ObjectKind::Blob, Encoding::Raw, b"blob", owner).unwrap()
    };
    let store = SharedDataStore::open(dir.path(), identity(), 0).unwrap();
    assert_eq!(store.owner_objects(owner), vec![id]);
    drop(store);
    assert!(matches!(
        SharedDataStore::open(dir.path(), StoreIdentity { genesis_root: [9; 32], ..identity() }, 0),
        Err(StoreError::IdentityMismatch)
    ));
}

#[test]
fn readers_remain_valid_across_cache_eviction_and_concurrent_collection() {
    let dir = TempDir::new().unwrap();
    let store = SharedDataStore::open(dir.path(), identity(), 4).unwrap();
    let owner = Owner::Pool([3; 32]);
    let a = store.put(ObjectKind::Cell, Encoding::Raw, b"aaaa", owner).unwrap();
    let reader = store.get(a).unwrap().unwrap();
    store.put(ObjectKind::Cell, Encoding::Raw, b"bbbb", owner).unwrap();
    assert_eq!(store.stats().unwrap().cached_bytes, 4);
    store.release(owner, a).unwrap();
    let worker = store;
    std::thread::spawn(move || worker.collect_garbage().unwrap()).join().unwrap();
    assert_eq!(&*reader, b"aaaa");
}

#[test]
fn interrupted_snapshot_is_ignored_and_orphan_object_is_collected() {
    let dir = TempDir::new().unwrap();
    let owner = Owner::Pool([3; 32]);
    let id = {
        let store = SharedDataStore::open(dir.path(), identity(), 0).unwrap();
        store.put(ObjectKind::Blob, Encoding::Raw, b"kept", owner).unwrap()
    };
    let namespace = dir.path().join("shared-data-v1");
    fs::write(namespace.join("refs/snapshot-00000000000000000003.pending"), b"interrupted")
        .unwrap();
    let orphan = ObjectId::new(ObjectKind::Cell, Encoding::Raw, b"orphan");
    fs::write(namespace.join("objects").join(orphan.filename()), b"orphan").unwrap();
    let store = SharedDataStore::open(dir.path(), identity(), 0).unwrap();
    assert_eq!(store.collect_garbage().unwrap(), 1);
    assert_eq!(&*store.get(id).unwrap().unwrap(), b"kept");
}

#[test]
fn corruption_and_multiple_writers_fail_closed() {
    let dir = TempDir::new().unwrap();
    let store = SharedDataStore::open(dir.path(), identity(), 0).unwrap();
    let id = store.put(ObjectKind::Blob, Encoding::Raw, b"blob", Owner::Pool([3; 32])).unwrap();
    assert!(matches!(
        SharedDataStore::open(dir.path(), identity(), 0),
        Err(StoreError::AlreadyOpen(_))
    ));
    fs::write(dir.path().join("shared-data-v1/objects").join(id.filename()), b"evil").unwrap();
    assert!(matches!(store.get(id), Err(StoreError::CorruptObject(_))));
    drop(store);
    assert!(matches!(
        SharedDataStore::open(dir.path(), identity(), 0),
        Err(StoreError::CorruptObject(_))
    ));
}

#[test]
fn kind_and_encoding_are_separate_namespaces_and_sparse_cells_stay_cells() {
    let dir = TempDir::new().unwrap();
    let store = SharedDataStore::open(dir.path(), identity(), 0).unwrap();
    let owner = Owner::Consensus([4; 32]);
    let cell = store.put(ObjectKind::Cell, Encoding::Raw, b"same", owner).unwrap();
    let blob = store.put(ObjectKind::Blob, Encoding::Raw, b"same", owner).unwrap();
    let encoded = store.put(ObjectKind::Blob, Encoding::Ssz, b"same", owner).unwrap();
    assert_ne!(cell, blob);
    assert_ne!(blob, encoded);
    assert_eq!(store.stats().unwrap().physical_objects, 3);
}

#[test]
fn signed_beacon_envelopes_stay_separate_from_execution_payloads_after_restart_and_gc() {
    let directory = TempDir::new().unwrap();
    let execution_owner = Owner::Pool([8; 32]);
    let consensus_owner = Owner::Consensus([9; 32]);
    let store = SharedDataStore::open(directory.path(), identity(), 0).unwrap();
    let execution = store
        .put(ObjectKind::ExecutionPayload, Encoding::Ssz, b"same bytes", execution_owner)
        .unwrap();
    let envelope = store
        .put(ObjectKind::BeaconPayloadEnvelope, Encoding::Ssz, b"same bytes", consensus_owner)
        .unwrap();
    assert_ne!(execution, envelope);
    assert_eq!(execution.kind as u8, 3);
    assert_eq!(envelope.kind as u8, 11);
    assert_eq!(ObjectId::from_filename(&envelope.filename()), Some(envelope));
    drop(store);
    let store = SharedDataStore::open(directory.path(), identity(), 0).unwrap();
    assert_eq!(store.owner_objects(execution_owner), vec![execution]);
    assert_eq!(store.owner_objects(consensus_owner), vec![envelope]);
    assert_eq!(store.get(envelope).unwrap().unwrap().as_ref(), b"same bytes");
    store.release(consensus_owner, envelope).unwrap();
    assert_eq!(store.collect_garbage().unwrap(), 1);
    assert!(store.get(envelope).unwrap().is_none());
    assert_eq!(store.get(execution).unwrap().unwrap().as_ref(), b"same bytes");
    assert_eq!(store.stats().unwrap().physical_objects, 1);
}

#[test]
fn owner_batch_is_atomic_and_missing_pin_does_not_release_previous_data() {
    let dir = TempDir::new().unwrap();
    let store = SharedDataStore::open(dir.path(), identity(), 0).unwrap();
    let owner = Owner::Consensus([4; 32]);
    let ids = store
        .put_owned_batch(
            owner,
            &[
                (ObjectKind::Blob, Encoding::Raw, b"blob".to_vec()),
                (ObjectKind::Proof, Encoding::Raw, b"proof".to_vec()),
            ],
        )
        .unwrap();
    let missing = ObjectId::new(ObjectKind::Cell, Encoding::Raw, b"missing");
    assert!(matches!(store.replace_owner(owner, &[missing]), Err(StoreError::MissingObject(_))));
    assert_eq!(store.owner_objects(owner).len(), 2);
    drop(store);
    let store = SharedDataStore::open(dir.path(), identity(), 0).unwrap();
    assert!(ids.iter().all(|id| store.owner_objects(owner).contains(id)));
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn any_release_order_preserves_remaining_ownership(operations in prop::collection::vec(0u8..6, 0..100)) {
        let dir = TempDir::new().unwrap();
        let store = SharedDataStore::open(dir.path(), identity(), 0).unwrap();
        let owners = [Owner::Pool([1; 32]), Owner::Consensus([2; 32]), Owner::Recovery([3; 32])];
        let id = store.put(ObjectKind::Cell, Encoding::Raw, b"bytes", owners[0]).unwrap();
        let mut expected = BTreeSet::from([owners[0]]);
        for operation in operations {
            let owner = owners[(operation % 3) as usize];
            if operation < 3 {
                store.pin(owner, id).unwrap();
                expected.insert(owner);
            } else {
                store.release(owner, id).unwrap();
                expected.remove(&owner);
            }
            prop_assert_eq!(store.stats().unwrap().owner_pins, expected.len() as u64);
        }
        store.collect_garbage().unwrap();
        prop_assert_eq!(store.get(id).unwrap().is_some(), !expected.is_empty());
    }
}

#[test]
fn committed_journal_corruption_and_unknown_schema_are_rejected() {
    for change_schema in [false, true] {
        let dir = TempDir::new().unwrap();
        {
            let store = SharedDataStore::open(dir.path(), identity(), 0).unwrap();
            store.put(ObjectKind::Cell, Encoding::Raw, b"cell", Owner::Pool([3; 32])).unwrap();
        }
        let path = dir.path().join("shared-data-v1/refs/snapshot-00000000000000000002.commit");
        let mut bytes = fs::read(&path).unwrap();
        if change_schema {
            bytes[8] = 2;
        } else {
            *bytes.last_mut().unwrap() ^= 1;
        }
        fs::write(&path, &bytes).unwrap();
        let result = SharedDataStore::open(dir.path(), identity(), 0);
        if change_schema {
            assert!(matches!(result, Err(StoreError::UnsupportedSchema)));
        } else {
            assert!(matches!(result, Err(StoreError::CorruptJournal)));
        }
    }
}

#[test]
fn bounded_reads_reject_hot_and_cold_objects_before_returning_bytes() {
    let dir = TempDir::new().unwrap();
    let store = SharedDataStore::open(dir.path(), identity(), 4096).unwrap();
    let id = store
        .put(ObjectKind::BeaconState, Encoding::Ssz, &[7; 1024], Owner::Checkpoint([3; 32]))
        .unwrap();
    assert!(matches!(
        store.get_bounded(id, 1023),
        Err(StoreError::ObjectTooLarge { size: 1024, limit: 1023 }),
    ));
    drop(store);
    let store = SharedDataStore::open(dir.path(), identity(), 0).unwrap();
    assert!(matches!(store.get_bounded(id, 1023), Err(StoreError::ObjectTooLarge { .. }),));
    assert_eq!(store.get_bounded(id, 1024).unwrap().unwrap().len(), 1024);
    assert_eq!(
        store
            .get_bounded(ObjectId::new(ObjectKind::BeaconState, Encoding::Ssz, b"absent"), 0)
            .unwrap(),
        None
    );
}

#[test]
fn uncertain_reference_publication_blocks_cached_reads_idempotent_writes_and_collection() {
    let directory = TempDir::new().unwrap();
    let owner = Owner::Checkpoint([3; 32]);
    let first;
    let second = ObjectId::new(ObjectKind::BeaconBlock, Encoding::Ssz, b"second");
    {
        let store = SharedDataStore::open(directory.path(), identity(), 4096).unwrap();
        first = store.put(ObjectKind::BeaconState, Encoding::Ssz, b"first", owner).unwrap();
        let reader = store.get(first).unwrap().unwrap();
        journal::inject_post_rename_failure(
            directory.path().join("shared-data-v1/refs/snapshot-00000000000000000003.commit"),
        );
        assert!(matches!(
            store.put(ObjectKind::BeaconBlock, Encoding::Ssz, b"second", owner),
            Err(StoreError::PublicationUncertain(_))
        ));
        assert!(matches!(store.ensure_healthy(), Err(StoreError::JournalPoisoned)));
        assert!(matches!(store.get(first), Err(StoreError::JournalPoisoned)));
        assert!(matches!(store.get(second), Err(StoreError::JournalPoisoned)));
        assert!(matches!(
            store.put(ObjectKind::BeaconState, Encoding::Ssz, b"first", owner),
            Err(StoreError::JournalPoisoned)
        ));
        assert!(matches!(store.pin(owner, first), Err(StoreError::JournalPoisoned)));
        assert!(matches!(
            store.release(Owner::Recovery([9; 32]), second),
            Err(StoreError::JournalPoisoned)
        ));
        assert!(matches!(store.replace_owner(owner, &[]), Err(StoreError::JournalPoisoned)));
        assert!(matches!(store.release_owners(&[]), Err(StoreError::JournalPoisoned)));
        assert!(matches!(store.put_owned_batch(owner, &[]), Err(StoreError::JournalPoisoned)));
        assert!(matches!(store.stats(), Err(StoreError::JournalPoisoned)));
        assert!(matches!(store.collect_garbage(), Err(StoreError::JournalPoisoned)));
        assert_eq!(reader.as_ref(), b"first");
    }
    let store = SharedDataStore::open(directory.path(), identity(), 0).unwrap();
    assert_eq!(store.get(first).unwrap().unwrap().as_ref(), b"first");
    assert_eq!(store.get(second).unwrap().unwrap().as_ref(), b"second");
    assert_eq!(store.owner_objects(owner), vec![first, second]);
    assert_eq!(store.collect_garbage().unwrap(), 0);
}

#[test]
fn oversized_unreferenced_existing_files_fail_without_replacing_previous_ownership() {
    let directory = TempDir::new().unwrap();
    let owner = Owner::Checkpoint([3; 32]);
    let store = SharedDataStore::open(directory.path(), identity(), 0).unwrap();
    let kept = store.put(ObjectKind::BeaconState, Encoding::Ssz, b"kept", owner).unwrap();
    let orphan = ObjectId::new(ObjectKind::BeaconBlock, Encoding::Ssz, b"bounded");
    let path = directory.path().join("shared-data-v1/objects").join(orphan.filename());
    let file = fs::create_file(&path).unwrap();
    file.set_len(16 * 1024 * 1024).unwrap();
    drop(file);
    assert!(
        matches!(store.put(ObjectKind::BeaconBlock, Encoding::Ssz, b"bounded", owner), Err(StoreError::CorruptObject(id)) if id == orphan)
    );
    assert!(matches!(store.put_owned_batch(owner, &[
        (ObjectKind::BeaconState, Encoding::Ssz, b"unpublished".to_vec()),
        (ObjectKind::BeaconBlock, Encoding::Ssz, b"bounded".to_vec()),
    ]), Err(StoreError::CorruptObject(id)) if id == orphan));
    assert_eq!(store.owner_objects(owner), vec![kept]);
    assert_eq!(store.get(kept).unwrap().unwrap().as_ref(), b"kept");
    assert_eq!(store.collect_garbage().unwrap(), 2);
    assert_eq!(store.stats().unwrap().physical_objects, 1);
}

#[test]
fn borrowed_graph_batch_adds_independent_edges_in_one_generation() {
    let directory = TempDir::new().unwrap();
    let store = SharedDataStore::open(directory.path(), identity(), 0).unwrap();
    let bytes = b"shared field bytes";
    let node = ObjectId::new(ObjectKind::BeaconStateBranch, Encoding::Raw, b"parent");
    let owners = [Owner::CompactNode(node), Owner::Pool([4; 32])];
    let ids = store
        .put_pinned_batch(&[PinnedObject {
            kind: ObjectKind::BeaconStateComponent,
            encoding: Encoding::Raw,
            bytes,
            owners: &owners,
        }])
        .unwrap();
    assert_eq!(ids.len(), 1);
    assert!(directory
        .path()
        .join("shared-data-v1/refs/snapshot-00000000000000000002.commit")
        .exists());
    assert_eq!(store.stats().unwrap().owner_pins, 2);
    store.release_owners(&[owners[0]]).unwrap();
    store.collect_garbage().unwrap();
    assert_eq!(&*store.get(ids[0]).unwrap().unwrap(), bytes);
    assert_eq!(store.owner_objects(owners[1]), ids);
    assert!(matches!(
        store.put_pinned_batch(&[PinnedObject {
            kind: ObjectKind::BeaconStateComponent,
            encoding: Encoding::Raw,
            bytes: b"unowned",
            owners: &[]
        }]),
        Err(StoreError::InvalidPublication)
    ));
    drop(store);
    let store = SharedDataStore::open(directory.path(), identity(), 0).unwrap();
    assert_eq!(store.owner_objects(owners[1]), ids);
    assert_eq!(&*store.get(ids[0]).unwrap().unwrap(), bytes);
}

#[test]
fn uncertain_object_publication_requires_reopen_and_verified_orphan_resync() {
    let directory = TempDir::new().unwrap();
    let owner = Owner::CompactState([3; 32]);
    let bytes = b"new immutable component";
    let id = ObjectId::new(ObjectKind::BeaconStateComponent, Encoding::Raw, bytes);
    let path = directory.path().join("shared-data-v1/objects").join(id.filename());
    let object = [PinnedObject { kind: id.kind, encoding: id.encoding, bytes, owners: &[owner] }];
    {
        let store = SharedDataStore::open(directory.path(), identity(), 0).unwrap();
        journal::inject_post_rename_failure(&path);
        assert!(matches!(
            store.put_pinned_batch(&object),
            Err(StoreError::PublicationUncertain(_))
        ));
        assert!(matches!(store.ensure_healthy(), Err(StoreError::JournalPoisoned)));
        assert!(matches!(store.put_pinned_batch(&object), Err(StoreError::JournalPoisoned)));
        assert!(matches!(store.collect_garbage(), Err(StoreError::JournalPoisoned)));
    }
    {
        let store = SharedDataStore::open(directory.path(), identity(), 0).unwrap();
        assert!(store.owner_objects(owner).is_empty());
        // The old orphan exists, but adding a durable reference still requires another sync.
        journal::inject_post_rename_failure(&path);
        assert!(matches!(
            store.put_pinned_batch(&object),
            Err(StoreError::PublicationUncertain(_))
        ));
        assert!(matches!(store.stats(), Err(StoreError::JournalPoisoned)));
    }
    let store = SharedDataStore::open(directory.path(), identity(), 0).unwrap();
    assert_eq!(store.put_pinned_batch(&object).unwrap(), vec![id]);
    assert_eq!(store.owner_objects(owner), vec![id]);
    assert_eq!(&*store.get(id).unwrap().unwrap(), bytes);
}

#[test]
fn original_beacon_sidecars_keep_distinct_encoding_and_proof_pins_across_restart() {
    let directory = TempDir::new().unwrap();
    let store = SharedDataStore::open(directory.path(), identity(), 0).unwrap();
    let execution_owner = Owner::Pool([221; 32]);
    let proof_owner = Owner::Recovery([222; 32]);
    let execution = store
        .put(ObjectKind::BlobSidecar, Encoding::Ssz, b"same original bytes", execution_owner)
        .unwrap();
    let beacon = store
        .put(ObjectKind::BeaconDataSidecar, Encoding::Ssz, b"same original bytes", proof_owner)
        .unwrap();
    assert_ne!(beacon, execution);
    assert_eq!(beacon.kind as u8, 12);
    assert_eq!(ObjectId::from_filename(&beacon.filename()), Some(beacon));
    drop(store);
    let store = SharedDataStore::open(directory.path(), identity(), 0).unwrap();
    assert_eq!(store.owner_objects(proof_owner), vec![beacon]);
    store.release(execution_owner, execution).unwrap();
    assert_eq!(store.collect_garbage().unwrap(), 1);
    assert_eq!(store.get(beacon).unwrap().unwrap().as_ref(), b"same original bytes");
}
#[test]
fn repeated_borrowed_publication_reuses_cached_allocation_and_refreshes_lru() {
    let directory = TempDir::new().unwrap();
    let store = SharedDataStore::open(directory.path(), identity(), 8).unwrap();
    let a = store
        .put(ObjectKind::BeaconStateComponent, Encoding::Raw, b"a111", Owner::Consensus([3; 32]))
        .unwrap();
    let b = store
        .put(ObjectKind::BeaconStateComponent, Encoding::Raw, b"b222", Owner::Consensus([4; 32]))
        .unwrap();
    let (allocation, touched) = {
        let inner = store.inner.lock();
        let entry = &inner.cache[&a];
        (entry.bytes.clone(), entry.touched)
    };
    let owner = Owner::CompactState([5; 32]);
    let before = store.try_observe().unwrap().journal_commits_total;
    let ids = store
        .put_pinned_batch(&[PinnedObject {
            kind: ObjectKind::BeaconStateComponent,
            encoding: Encoding::Raw,
            bytes: b"a111",
            owners: &[owner],
        }])
        .unwrap();
    assert_eq!(ids, vec![a]);
    assert_eq!(store.try_observe().unwrap().journal_commits_total, before + 1);
    assert_eq!(store.owner_objects(owner), vec![a]);
    {
        let inner = store.inner.lock();
        assert!(Arc::ptr_eq(&allocation, &inner.cache[&a].bytes));
        assert!(inner.cache[&a].touched > touched);
        assert_eq!(inner.cache_bytes, 8);
    }
    let c = store
        .put_pinned_batch(&[PinnedObject {
            kind: ObjectKind::BeaconStateComponent,
            encoding: Encoding::Raw,
            bytes: b"c333",
            owners: &[Owner::Consensus([6; 32])],
        }])
        .unwrap()[0];
    {
        let inner = store.inner.lock();
        assert!(inner.cache.contains_key(&a));
        assert!(inner.cache.contains_key(&c));
        assert!(!inner.cache.contains_key(&b));
        assert!(Arc::ptr_eq(&allocation, &inner.cache[&a].bytes));
        assert_eq!(inner.cache_bytes, 8);
        assert_eq!(inner.observations.cache_evictions_total, 1);
    }
    assert_eq!(allocation.as_ref(), b"a111");
    assert_eq!(store.owner_objects(Owner::Consensus([4; 32])), vec![b]);
    assert_eq!(store.get(b).unwrap().unwrap().as_ref(), b"b222");
}

#[test]
fn oversized_borrowed_publication_stays_durable_without_cache_ownership() {
    let directory = TempDir::new().unwrap();
    let store = SharedDataStore::open(directory.path(), identity(), 1).unwrap();
    let owner = Owner::CompactState([3; 32]);
    let original = b"exact component bytes";
    let ids = store
        .put_pinned_batch(&[PinnedObject {
            kind: ObjectKind::BeaconStateComponent,
            encoding: Encoding::Raw,
            bytes: original,
            owners: &[owner],
        }])
        .unwrap();
    assert_eq!(ids, vec![ObjectId::new(ObjectKind::BeaconStateComponent, Encoding::Raw, original)]);
    assert_eq!(store.try_observe().unwrap().cache_entries, 0);
    assert_eq!(store.try_observe().unwrap().cache_bytes, 0);
    assert_eq!(store.owner_objects(owner), ids);
    assert_eq!(store.get(ids[0]).unwrap().unwrap().as_ref(), original);
    assert_eq!(store.try_observe().unwrap().cache_bytes, 0);
}

#[test]
fn identical_reference_batches_preserve_pins_without_recommitting_and_poison_still_fails() {
    let directory = TempDir::new().unwrap();
    let store = SharedDataStore::open(directory.path(), identity(), 0).unwrap();
    let owner = Owner::CompactState([31; 32]);
    let bytes = b"original immutable state fragment";
    let objects = [PinnedObject {
        kind: ObjectKind::BeaconStateComponent,
        encoding: Encoding::Raw,
        bytes,
        owners: &[owner],
    }];
    let ids = store.put_pinned_batch(&objects).unwrap();
    let commits = store.try_observe().unwrap().journal_commits_total;
    store.put_pinned_batch(&objects).unwrap();
    store.replace_owner(owner, &ids).unwrap();
    store.release_owners(&[Owner::Pool([32; 32])]).unwrap();
    assert_eq!(store.try_observe().unwrap().journal_commits_total, commits);
    assert_eq!(store.owner_objects(owner), ids);
    let independent = Owner::Pool([33; 32]);
    store.pin(independent, ids[0]).unwrap();
    assert_eq!(store.try_observe().unwrap().journal_commits_total, commits + 1);
    drop(store);
    let store = SharedDataStore::open(directory.path(), identity(), 0).unwrap();
    assert_eq!(store.owner_objects(owner), ids);
    assert_eq!(store.owner_objects(independent), ids);
    assert_eq!(store.get(ids[0]).unwrap().unwrap().as_ref(), bytes);
    store.inner.lock().journal.mark_publication_uncertain();
    assert!(matches!(store.put_pinned_batch(&objects), Err(StoreError::JournalPoisoned)));
    assert!(matches!(store.replace_owner(owner, &ids), Err(StoreError::JournalPoisoned)));
    assert!(matches!(store.release_owners(&[]), Err(StoreError::JournalPoisoned)));
}

#[test]
fn single_object_put_reuses_cache_ownership_and_skips_oversized_cache_allocation() {
    let directory = TempDir::new().unwrap();
    let store = SharedDataStore::open(directory.path(), identity(), 4).unwrap();
    let first = Owner::Consensus([41; 32]);
    let second = Owner::Consensus([42; 32]);
    let id = store.put(ObjectKind::BeaconBlock, Encoding::Raw, b"same", first).unwrap();
    let (allocation, touched) = {
        let inner = store.inner.lock();
        (inner.cache[&id].bytes.clone(), inner.cache[&id].touched)
    };
    assert_eq!(store.put(ObjectKind::BeaconBlock, Encoding::Raw, b"same", second).unwrap(), id);
    {
        let inner = store.inner.lock();
        assert!(Arc::ptr_eq(&allocation, &inner.cache[&id].bytes));
        assert!(inner.cache[&id].touched > touched);
        assert_eq!(inner.cache_bytes, 4);
    }
    let large = store.put(ObjectKind::BeaconState, Encoding::Ssz, b"oversized", first).unwrap();
    assert_eq!(store.try_observe().unwrap().cache_entries, 1);
    assert_eq!(store.try_observe().unwrap().cache_bytes, 4);
    assert!(!store.inner.lock().cache.contains_key(&large));
    assert_eq!(store.get(large).unwrap().unwrap().as_ref(), b"oversized");
    assert_eq!(store.owner_objects(second), vec![id]);
}

#[test]
fn owner_replacements_publish_one_generation_and_last_duplicate_owner_wins() {
    let directory = tempfile::tempdir().unwrap();
    let first = Owner::Pool([3; 32]);
    let second = Owner::Consensus([4; 32]);
    let initial = vec![(ObjectKind::Blob, Encoding::Raw, b"initial".to_vec())];
    let replacement = vec![(ObjectKind::Blob, Encoding::Raw, b"replacement".to_vec())];
    let shared = vec![(ObjectKind::Blob, Encoding::Raw, b"shared".to_vec())];
    let ids;
    {
        let store = SharedDataStore::open(directory.path(), identity(), 0).unwrap();
        let before = store.try_observe().unwrap().journal_commits_total;
        ids = store
            .put_owned_batches(&[
                OwnedObjectBatch { owner: first, objects: &initial },
                OwnedObjectBatch { owner: second, objects: &shared },
                OwnedObjectBatch { owner: first, objects: &replacement },
            ])
            .unwrap();
        assert_eq!(store.try_observe().unwrap().journal_commits_total, before + 1);
        assert_eq!(store.owner_objects(first), ids[2]);
        assert_eq!(store.owner_objects(second), ids[1]);
    }
    let store = SharedDataStore::open(directory.path(), identity(), 0).unwrap();
    assert_eq!(store.owner_objects(first), ids[2]);
    assert_eq!(store.owner_objects(second), ids[1]);
    store.collect_garbage().unwrap();
    assert!(store.get(ids[0][0]).unwrap().is_none());
    assert_eq!(store.get(ids[2][0]).unwrap().unwrap().as_ref(), b"replacement");
}
