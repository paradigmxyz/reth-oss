//! Genuine native signed originals grant finalized serving context without graph authority.

use super::*;
use reth_embedded_beacon_network::{block_rpc::BlockRequest, WireError};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn genuine_native_unsigned_checkpoint_requires_original_signed_anchor_before_its_parent() {
    let directory = TempDir::new().unwrap();
    let (mut producer, keys, parameters) = store_and_keys(Fork::Phase0);
    // The ordinary setup authenticates epoch zero before this producer advances.
    let (mut initial, backend, shared) = setup_with_shared(&directory, &producer, &parameters);
    let config = initial.config.clone();

    tokio::task::spawn_blocking(move || {
        let mut children = Vec::new();
        for slot in 1..=config.slots_per_epoch {
            let signed = phase0_signed_child(&producer, &keys, &parameters);
            assert_eq!(signed.message.slot, slot);
            producer.on_tick(config.clock().unwrap().start_of_slot(slot).unwrap()).unwrap();
            producer.on_block(&SignedBeaconBlock::Phase0(signed.clone(), Preset::Minimal)).unwrap();
            children.push(signed);
        }

        let anchor_root = producer.get_head().unwrap().root;
        let state = producer.block_state(anchor_root).unwrap();
        let block = producer.block(anchor_root).unwrap();
        let anchor_slot = config.slots_per_epoch;
        assert_eq!(state.slot(), anchor_slot);
        assert_eq!(block.header().unwrap().slot, anchor_slot);
        let state_ssz = Arc::<[u8]>::from(state.to_ssz().unwrap());
        let unsigned = Arc::<[u8]>::from(block.to_ssz().unwrap());
        let now = config.clock().unwrap().start_of_slot(anchor_slot).unwrap();
        let verifier = SszCheckpointVerifier::new(
            config.clone(),
            initial.anchor.decode_limits,
            CheckpointBlockFormat::Unsigned,
        )
        .unwrap();
        let trusted = Checkpoint { epoch: 1, root: anchor_root };
        initial.anchor.verified =
            verifier.verify_checkpoint(&state_ssz, &unsigned, trusted, now, 1).unwrap();
        initial.anchor.state_ssz = state_ssz;
        initial.anchor.block_ssz = unsigned.clone();
        let OwnerClock::Controlled(clock) = &initial.clock else { unreachable!() };
        clock.store(now, Ordering::Release);

        let worker = Worker::new(initial, Ok(now)).unwrap();
        let snapshot = worker.graph.snapshot().unwrap();
        assert_eq!(snapshot.finalized_checkpoint, trusted);
        let head_state_root = match &worker.graph {
            Graph::Classic { store, .. } => store
                .read_block_state(snapshot.head.root)
                .unwrap()
                .unwrap()
                .hash_tree_root()
                .unwrap(),
            _ => unreachable!(),
        };
        let seven =
            WireBlock::from_ssz(children[6].to_ssz(Preset::Minimal).unwrap(), &config).unwrap();
        let eight =
            WireBlock::from_ssz(children[7].to_ssz(Preset::Minimal).unwrap(), &config).unwrap();
        assert_eq!(eight.root(), anchor_root);
        assert_eq!(eight.parent_root(), seven.root());
        assert_eq!(head_state_root, children[7].message.state_root);
        let pristine_pins = shared.owners();
        let pristine_stats = shared.stats().unwrap();

        let dependency = worker.custody_dependency().unwrap().unwrap();
        assert_eq!((dependency.root(), dependency.upper_slot()), (eight.root(), eight.slot() + 1));
        assert!(matches!(
            worker.retain_custody_context(seven.clone()),
            Err(ImportError::GossipReject("historical original does not match committed parent"))
        ));
        assert_eq!(shared.owners(), pristine_pins);
        assert_eq!(shared.stats().unwrap(), pristine_stats);

        let mut malformed = eight.ssz().to_vec();
        malformed[0] = 0;
        assert!(matches!(WireBlock::from_ssz(malformed, &config), Err(WireError::Ssz)));

        // Both invalid containers keep the exact committed message/root.
        let mut zero = children[7].clone();
        zero.signature = [0; 96];
        let mut wrong_key = children[7].clone();
        let proposer = wrong_key.message.proposer_index as usize;
        wrong_key.signature = keys[(proposer + 1) % keys.len()]
            .sign(
                &signing_root(
                    eight.root(),
                    [0; 4],
                    config.fork_at_slot(eight.slot()).unwrap().version,
                    config.genesis_validators_root,
                ),
                ETHEREUM_BLS_DST,
                &[],
            )
            .to_bytes();
        for invalid in [zero, wrong_key] {
            let wire =
                WireBlock::from_ssz(invalid.to_ssz(Preset::Minimal).unwrap(), &config).unwrap();
            assert_eq!(wire.root(), eight.root());
            assert!(matches!(
                worker.retain_custody_context(wire),
                Err(ImportError::GossipReject("invalid historical proposer signature"))
            ));
            assert_eq!(shared.owners(), pristine_pins);
            assert_eq!(shared.stats().unwrap(), pristine_stats);
        }
        let Some(ServingBatch::Blocks(empty)) = worker
            .serve(ServingRequest::Blocks(BlockRequest::Roots(vec![seven.root(), eight.root()])))
            .unwrap()
        else {
            panic!("bounded original-block response")
        };
        assert!(empty.is_empty());

        worker.retain_custody_context(eight.clone()).unwrap();
        let dependency = worker.custody_dependency().unwrap().unwrap();
        assert_eq!((dependency.root(), dependency.upper_slot()), (seven.root(), eight.slot()));
        let signed_anchor_pins = shared.owners();
        let signed_anchor_stats = shared.stats().unwrap();

        let mut invalid_seven = children[6].clone();
        invalid_seven.signature = [0; 96];
        let invalid_seven =
            WireBlock::from_ssz(invalid_seven.to_ssz(Preset::Minimal).unwrap(), &config).unwrap();
        assert!(matches!(
            worker.retain_custody_context(invalid_seven),
            Err(ImportError::GossipReject("invalid historical proposer signature"))
        ));
        assert_eq!(shared.owners(), signed_anchor_pins);
        assert_eq!(shared.stats().unwrap(), signed_anchor_stats);
        worker.retain_custody_context(seven.clone()).unwrap();

        let six =
            WireBlock::from_ssz(children[5].to_ssz(Preset::Minimal).unwrap(), &config).unwrap();
        let dependency = worker.custody_dependency().unwrap().unwrap();
        assert_eq!((dependency.root(), dependency.upper_slot()), (six.root(), seven.slot()));
        for request in [
            BlockRequest::Roots(vec![seven.root(), eight.root()]),
            BlockRequest::Range { start_slot: seven.slot(), count: 2 },
        ] {
            let Some(ServingBatch::Blocks(blocks)) =
                worker.serve(ServingRequest::Blocks(request)).unwrap()
            else {
                panic!("actual signed history response")
            };
            assert_eq!(blocks, vec![seven.clone(), eight.clone()]);
        }

        assert_eq!(worker.graph.snapshot().unwrap(), snapshot);
        assert!(worker.graph.serving_header(seven.root()).unwrap().is_none());
        let Graph::Classic { store, .. } = &worker.graph else { unreachable!() };
        assert!(store.read_block_state(seven.root()).unwrap().is_none());
        assert_eq!(
            store.read_block_state(snapshot.head.root).unwrap().unwrap().hash_tree_root().unwrap(),
            head_state_root
        );
        assert!(worker.repository.original_block(seven.root()).unwrap().is_none());
        assert!(worker.repository.block_state(seven.root()).unwrap().is_none());
        let (format, saved_original) =
            worker.repository.original_block(anchor_root).unwrap().unwrap();
        assert_eq!(format, CheckpointBlockFormat::Unsigned);
        assert_eq!(saved_original.as_ref(), unsigned.as_ref());
        assert_eq!(worker.repository.stats().unwrap().retained_states, 1);
        let saved = worker.repository.current_snapshot().unwrap().unwrap();
        assert_eq!(saved.head(), snapshot.head);
        assert_eq!(
            saved.checkpoints(),
            (snapshot.justified_checkpoint, snapshot.finalized_checkpoint)
        );
        assert!(worker.last_execution_state.is_none());
        assert!(worker.last_verified_forkchoice.is_none());
        assert!(backend.payload_calls.lock().unwrap().is_empty());
        assert!(backend.forkchoice_calls.lock().unwrap().is_empty());

        // Signed block context alone cannot mint a mathematical custody-history receipt.
        let mut status =
            worker.graph.authenticated_status(&config, worker.earliest_available_slot).unwrap();
        assert_eq!(status.snapshot(), snapshot);
        assert_eq!(status.config_digest(), checked_config_digest(&config).unwrap());
        status.apply_custody_history(None).unwrap();
        let network = status.into_network();
        assert_eq!(network.status.status.head_root, snapshot.head.root);
        assert_eq!(network.status.earliest_available_slot, snapshot.head_slot + 1);
    })
    .await
    .unwrap();
}

/// Keep the existing shared writer available to inspect independent pins in native tests.
pub(super) fn setup_with_shared(
    directory: &TempDir,
    store: &Store,
    parameters: &TransitionParameters,
) -> (
    WorkerSetup<BackendHandle, MissingAvailability, MissingAvailability>,
    Arc<Backend>,
    SharedDataStore,
) {
    let (mut initial, backend) = setup(directory, store, parameters);
    // Setup has not persisted its anchor yet; reopen the empty repository with a retained clone.
    drop(initial.repository);
    let network = PersistenceNetwork::from_config(initial.config.clone()).unwrap();
    let shared = SharedDataStore::open(directory.path(), network.identity(), 0).unwrap();
    initial.repository =
        AdmittedStateRepository::open(shared.clone(), network, CompactStateLimits::default())
            .unwrap();
    (initial, backend, shared)
}
