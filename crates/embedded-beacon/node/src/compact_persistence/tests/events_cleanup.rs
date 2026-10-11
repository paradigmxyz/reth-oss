//! Exact transcript-only cleanup resumes an owner release committed before acknowledgement.
use super::*;
use crate::{
    compact_persistence::{
        events::{DurableEvent, EventAction},
        state_source::RepositoryStateSource,
    },
    gossip_tests::signing_root,
};
use reth_embedded_beacon_crypto::ETHEREUM_BLS_DST;
use reth_embedded_beacon_fork_choice::compact::{CompactGraphLimits, CompactStore};
use reth_embedded_beacon_state_transition::ssz::phase0;
use reth_fs_util as fs;

#[test]
fn genuine_signed_evidence_owner_release_crash_resumes_exact_pending_descriptor() {
    let directory = TempDir::new().unwrap();
    let (graph, keys, parameters) = store_and_keys(Fork::Phase0);
    let root = graph.get_head().unwrap().root;
    let state = graph.block_state(root).unwrap();
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
    bootstrap(&repository, state, graph.block(root).unwrap(), config);
    let indexed = |marker| {
        let data = phase0::AttestationData {
            slot: 0,
            index: 0,
            beacon_block_root: [marker; 32],
            source: Checkpoint { epoch: 0, root },
            target: Checkpoint { epoch: 0, root },
        };
        let message = signing_root(
            data.hash_tree_root(Preset::Minimal).unwrap(),
            [1, 0, 0, 0],
            state.fork_info().current_version,
            state.genesis_validators_root(),
        );
        phase0::IndexedAttestation {
            attesting_indices: phase0::AttestingIndices::from_vec(vec![0]),
            data,
            signature: keys[0].sign(&message, ETHEREUM_BLS_DST, &[]).to_bytes(),
        }
    };
    let proof = phase0::AttesterSlashing { attestation_1: indexed(21), attestation_2: indexed(22) };
    let bytes = proof.to_ssz(Preset::Minimal).unwrap();
    let original = ObjectId::new(ObjectKind::BeaconObservation, Encoding::Ssz, &bytes);
    let independent = Owner::Pool([93; 32]);
    shared.put(ObjectKind::BeaconObservation, Encoding::Ssz, &bytes, independent).unwrap();
    let source = RepositoryStateSource::open(repository.clone()).unwrap();
    source.enable_event_transcript().unwrap();
    let mut store =
        CompactStore::from_store(graph, source.clone(), CompactGraphLimits::default()).unwrap();
    let event = DurableEvent {
        sequence: 0,
        begin_ms: 0,
        completed_ms: 0,
        received_ms: 0,
        historical: false,
        action: EventAction::Slashing { fork: Fork::Phase0, original, verification_slot: 0 },
    };
    store.on_original_attester_slashing(&bytes, Fork::Phase0, &parameters, 0).unwrap();
    source.record_event(store.snapshot().unwrap(), event.clone(), Some(&bytes), false).unwrap();
    source.commit_snapshot(store.snapshot().unwrap()).unwrap();
    // An actual duplicate native proof is idempotent. The canonical first original stays pinned;
    // the second unpublished copy's transcript-only owner is safe to retire independently.
    store.on_original_attester_slashing(&bytes, Fork::Phase0, &parameters, 0).unwrap();
    source.record_event(store.snapshot().unwrap(), event, Some(&bytes), false).unwrap();
    let journal = shared.namespace_root().join("refs");
    let generation = fs::read_dir(&journal)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            let name = entry.file_name();
            name.to_str()
                .and_then(|name| name.strip_prefix("snapshot-"))
                .and_then(|name| name.strip_suffix(".commit"))
                .and_then(|number| number.parse::<u64>().ok())
                .unwrap_or(0)
        })
        .max()
        .unwrap();
    // The admission deletion descriptor commits first. The shared journal then actually releases
    // only the duplicate transcript owner, but its acknowledgement fails after rename. The
    // descriptor must remain durable until reopen idempotently completes that exact release.
    inject_post_rename_failure(journal.join(format!("snapshot-{:020}.commit", generation + 1)));
    let keep = BTreeSet::from([0]);
    let mut inner = repository.lock().unwrap();
    assert!(repository.compact_events(&mut inner, &keep).is_err());
    assert!(inner.event_cleanup.is_some());
    drop(inner);
    assert_eq!(shared.owner_objects(independent), vec![original]);
    drop(store);
    drop(source);
    drop(repository);
    drop(shared);
    let shared = SharedDataStore::open(directory.path(), network.identity(), 0).unwrap();
    let repository =
        AdmittedStateRepository::open(shared.clone(), network, CompactStateLimits::default())
            .unwrap();
    assert!(repository.lock().unwrap().event_cleanup.is_none());
    assert_eq!(repository.replay_transcript().unwrap().unwrap().sequences, keep);
    assert_eq!(
        repository.original_observation(&repository.original_event(0).unwrap()).unwrap().as_ref(),
        bytes
    );
    assert!(repository.original_event(1).is_err());
    assert_eq!(shared.owner_objects(independent), vec![original]);
    assert_eq!(repository.current_snapshot().unwrap().unwrap().head().root, root);
}
