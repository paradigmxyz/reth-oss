//! Durable sidecar contexts reopen as data; unpublished bounded files carry no native receipt.
use super::*;
use crate::compact_persistence::{
    retention_gc::RetentionDomain, state_source::RepositoryStateSource,
};
use reth_embedded_beacon_fork_choice::compact::{CompactGraphLimits, CompactStore};
use reth_embedded_beacon_network::block_rpc::BlockRequest;
use reth_embedded_beacon_state_transition::ssz::SignedBeaconBlock;
use reth_fs_util as fs;

#[test]
fn actual_signed_context_and_partial_complete_pending_files_reopen_without_selection() {
    let directory = TempDir::new().unwrap();
    let (producer, keys, parameters) = store_and_keys(Fork::Phase0);
    let anchor = producer.get_head().unwrap().root;
    let state = producer.block_state(anchor).unwrap();
    let config = config(state, &parameters);
    let network = PersistenceNetwork::from_config(config.clone()).unwrap();
    let shared = SharedDataStore::open(directory.path(), network.identity(), 0).unwrap();
    let repository = Arc::new(
        AdmittedStateRepository::open(
            shared.clone(),
            network.clone(),
            CompactStateLimits::default(),
        )
        .unwrap(),
    );
    bootstrap(&repository, state, producer.block(anchor).unwrap(), config.clone());
    let signed = SignedBeaconBlock::Phase0(
        phase0_signed_child(&producer, &keys, &parameters),
        Preset::Minimal,
    );
    let root = signed.message_root().unwrap();
    let source = RepositoryStateSource::open(repository.clone()).unwrap();
    let mut graph =
        CompactStore::from_store(producer.clone(), source.clone(), CompactGraphLimits::default())
            .unwrap();
    graph.on_tick(config.seconds_per_slot * 1000).unwrap();
    graph.on_block(&signed).unwrap();
    // Only the actual signed block receipt authorizes by-root proof context. Finalized ancestry
    // still has the genuine unsigned slot-zero anchor, so this context selects no historical range.
    assert_eq!(
        source.serving_contexts(&BlockRequest::Roots(vec![root])).unwrap().unwrap().len(),
        1
    );
    assert!(source
        .serving_contexts(&BlockRequest::Range { start_slot: 1, count: 1 })
        .unwrap()
        .unwrap()
        .is_empty());
    source.discard_snapshot().unwrap();
    assert!(source
        .serving_contexts(&BlockRequest::Range { start_slot: 1, count: 1 })
        .unwrap()
        .unwrap()
        .is_empty());
    graph.on_tick(config.seconds_per_slot * 1000).unwrap();
    source.commit_snapshot(graph.snapshot().unwrap()).unwrap();
    let epoch = shared.namespace_root().join("beacon-sidecar-contexts-v1/00000000000000000000");
    let hex = root.iter().map(|byte| format!("{byte:02x}")).collect::<String>();
    let original = fs::read(epoch.join(format!("{hex}.json"))).unwrap();
    let orphan = [61; 32];
    let orphan_hex = orphan.iter().map(|byte| format!("{byte:02x}")).collect::<String>();
    let object = ObjectId::new(ObjectKind::BeaconBlock, Encoding::Ssz, &signed.to_ssz().unwrap());
    let independent = Owner::Pool([62; 32]);
    shared.pin(RetentionDomain::Contexts.owner(orphan), object).unwrap();
    shared.pin(independent, object).unwrap();
    fs::write(epoch.join(format!("{orphan_hex}.intent")), b"").unwrap();
    fs::write(epoch.join(format!("{hex}.intent")), b"").unwrap();
    fs::write(epoch.join(format!("{hex}.pending")), &original).unwrap();
    fs::write(epoch.join(format!("{}.pending", "f".repeat(64))), b"{\"schema\":").unwrap();
    drop(graph);
    drop(source);
    drop(repository);
    drop(shared);
    let shared = SharedDataStore::open(directory.path(), network.identity(), 0).unwrap();
    let repository = Arc::new(
        AdmittedStateRepository::open(shared.clone(), network, CompactStateLimits::default())
            .unwrap(),
    );
    let source = RepositoryStateSource::open(repository).unwrap();
    assert!(shared.owner_objects(RetentionDomain::Contexts.owner(orphan)).is_empty());
    assert_eq!(shared.owner_objects(RetentionDomain::Contexts.owner(root)), vec![object]);
    assert_eq!(shared.owner_objects(independent), vec![object]);
    assert!(!epoch.join(format!("{orphan_hex}.intent")).exists());
    assert!(!epoch.join(format!("{hex}.intent")).exists());
    assert!(source.serving_contexts(&BlockRequest::Roots(vec![root])).unwrap().unwrap().is_empty());
    assert!(source
        .serving_contexts(&BlockRequest::Range { start_slot: 1, count: 1 })
        .unwrap()
        .unwrap()
        .is_empty());
    let mut graph =
        CompactStore::from_store(producer, source.clone(), CompactGraphLimits::default()).unwrap();
    assert!(source.serving_contexts(&BlockRequest::Roots(vec![root])).unwrap().unwrap().is_empty());
    graph.on_tick(config.seconds_per_slot * 1000).unwrap();
    graph.on_block(&signed).unwrap();
    let originals = source.serving_contexts(&BlockRequest::Roots(vec![root])).unwrap().unwrap();
    assert_eq!(originals.len(), 1);
    assert_eq!(originals[0].ssz().as_ref(), signed.to_ssz().unwrap());
    assert!(source
        .serving_contexts(&BlockRequest::Range { start_slot: 1, count: 1 })
        .unwrap()
        .unwrap()
        .is_empty());
    assert_eq!(graph.snapshot().unwrap().finalized_checkpoint.root, anchor);
    // The sealed context and its already verified key cannot replace the original context owner.
    // Another independent owner still retains these bytes, so this checks the exact receipt pin.
    shared.release_owners(&[RetentionDomain::Contexts.owner(root)]).unwrap();
    assert_eq!(shared.owner_objects(independent), vec![object]);
    assert!(matches!(
        source.serving_contexts(&BlockRequest::Roots(vec![root])),
        Err(PersistenceError::Linkage)
    ));
}
