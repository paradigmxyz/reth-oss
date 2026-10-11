//! Real finalized native cuts preserve unsigned context through its independent byte owner.

use super::{
    custody_history::setup_with_shared,
    slashing_cut::{cut_without_shortening_native_policy, finalized_child},
    *,
};
use reth_embedded_beacon_network::block_rpc::BlockRequest;
use reth_embedded_beacon_primitives::sha256;
use reth_embedded_beacon_shared_data::{Encoding, ObjectId, ObjectKind, Owner};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn genuine_native_finalized_cuts_preserve_only_independently_pinned_unsigned_context() {
    let directory = TempDir::new().unwrap();
    let (producer, keys, parameters) = store_and_keys(Fork::Phase0);
    let first = phase0_signed_child(&producer, &keys, &parameters);
    let (initial, backend, shared) = setup_with_shared(&directory, &producer, &parameters);
    let config = initial.config.clone();
    let origin = initial.anchor.verified.checkpoint().root;
    let unsigned = initial.anchor.block_ssz.clone();

    tokio::task::spawn_blocking(move || {
        let mut worker = Worker::new(initial, Ok(0)).unwrap();
        let owner = Owner::Recovery(sha256(b"embedded-beacon-unsigned-sidecar-context-v1"));
        let object = ObjectId::new(ObjectKind::BeaconBlock, Encoding::Ssz, &unsigned);
        assert_eq!(shared.owner_objects(owner), vec![object]);

        // Independent retention of the same original proves releasing custody's owner is narrow.
        let pool_owner = Owner::Pool(sha256(b"native-pruned-custody-independent-pool"));
        let other_recovery = Owner::Recovery(sha256(b"native-pruned-custody-independent-recovery"));
        shared.pin(pool_owner, object).unwrap();
        shared.pin(other_recovery, object).unwrap();

        let millis = config.seconds_per_slot * 1000;
        worker.advance(millis).unwrap();
        let first = WireBlock::from_ssz(first.to_ssz(Preset::Minimal).unwrap(), &config).unwrap();
        worker.import(Input::Block(first.clone()), 0).unwrap();
        let mut first_cut = None;
        for slot in 2..=96 {
            let OwnerClock::Controlled(clock) = &worker.clock else { unreachable!() };
            clock.store(slot * millis, Ordering::Release);
            worker.advance(slot * millis).unwrap();
            let Graph::Classic { store, .. } = &worker.graph else { unreachable!() };
            let signed = finalized_child(store, &keys, &parameters, slot);
            let wire =
                WireBlock::from_ssz(signed.to_ssz(Preset::Minimal).unwrap(), &config).unwrap();
            worker.import(Input::Block(wire), (slot - 1) * millis).unwrap();
            if slot == 64 {
                first_cut = Some(cut_without_shortening_native_policy(&mut worker));
                let Graph::Classic { store, .. } = &worker.graph else { unreachable!() };
                assert!(store.block(origin).is_none());
                assert!(store.read_block_state(origin).unwrap().is_none());
            }
        }

        let second_cut = cut_without_shortening_native_policy(&mut worker);
        assert_ne!(Some(second_cut), first_cut);
        assert_ne!(second_cut, origin);
        let snapshot = worker.graph.snapshot().unwrap();
        assert!(snapshot.finalized_checkpoint.epoch > 2);
        assert_eq!(snapshot.head_slot, worker.graph.current_slot());
        assert!(worker.graph.serving_header(origin).unwrap().is_none());
        assert!(worker.graph.serving_header(first.root()).unwrap().is_none());
        let Graph::Classic { store, .. } = &worker.graph else { unreachable!() };
        assert!(store.read_block_state(origin).unwrap().is_none());
        assert!(store.read_block_state(first.root()).unwrap().is_none());

        worker.repository.collect_retired_objects().unwrap();
        assert_eq!(shared.owner_objects(owner), vec![object]);
        assert_eq!(shared.owner_objects(pool_owner), vec![object]);
        assert_eq!(shared.owner_objects(other_recovery), vec![object]);
        assert_eq!(
            shared.get_bounded(object, config.max_payload_size).unwrap().unwrap().as_ref(),
            unsigned.as_ref()
        );
        let context = worker.custody_anchor(origin).unwrap().unwrap();
        assert_eq!(context.root(), origin);
        assert_eq!(context.slot(), 0);
        assert_eq!(context.config_digest(), checked_config_digest(&config).unwrap());
        assert_eq!(context.original(), unsigned.as_ref());
        assert_eq!(context.original_len(), unsigned.len());
        assert_eq!(context.block().unwrap().hash_tree_root().unwrap(), origin);

        // Actual signed evidence remains serviceable after the native block/state was cut.
        let Some(ServingBatch::Blocks(blocks)) = worker
            .serve(ServingRequest::Blocks(BlockRequest::Roots(vec![origin, first.root()])))
            .unwrap()
        else {
            panic!("original signed history from the actual finalized chain")
        };
        assert_eq!(blocks, vec![first.clone()]);
        let Some(ServingBatch::Blocks(range)) = worker
            .serve(ServingRequest::Blocks(BlockRequest::Range {
                start_slot: first.slot(),
                count: 1,
            }))
            .unwrap()
        else {
            panic!("selected signed original remains independently serviceable")
        };
        assert_eq!(range, vec![first]);

        let verifier = SszCheckpointVerifier::new(
            config.clone(),
            BootstrapDecodeLimits::default(),
            CheckpointBlockFormat::Unsigned,
        )
        .unwrap();
        let now = config.clock().unwrap().start_of_slot(snapshot.head_slot).unwrap();
        let recovery = worker.repository.recover_checkpoint(&verifier, now, 32).unwrap().unwrap();
        assert_eq!(recovery.checkpoint.checkpoint().root, second_cut);
        assert_eq!(recovery.block_format, CheckpointBlockFormat::Signed);
        let recovery_block = SignedBeaconBlock::from_ssz(
            &recovery.block_ssz,
            config.fork_at_slot(recovery.state.state().slot()).unwrap().fork,
            Preset::Minimal,
        )
        .unwrap();
        assert_eq!(recovery_block.message_root().unwrap(), second_cut);
        drop(recovery);

        // Native block bytes do not provide a mathematical custody proof or a positive floor.
        let mut status =
            worker.graph.authenticated_status(&config, worker.earliest_available_slot).unwrap();
        assert_eq!(status.snapshot(), snapshot);
        assert_eq!(status.current_slot(), worker.graph.current_slot());
        assert_eq!(status.config_digest(), checked_config_digest(&config).unwrap());
        status.apply_custody_history(None).unwrap();
        let network = status.into_network();
        assert_eq!(network.status.status.head_root, snapshot.head.root);
        assert_eq!(network.finalized, snapshot.finalized_checkpoint);
        assert_eq!(network.status.earliest_available_slot, snapshot.head_slot + 1);

        // Immutable native origin/recovery data remain healthy when only custody's pin leaves.
        assert_eq!(
            worker.repository.original_block(origin).unwrap().unwrap().0,
            CheckpointBlockFormat::Unsigned
        );
        assert!(worker.repository.block_state(origin).unwrap().is_some());
        let other_pins = shared
            .owners()
            .into_iter()
            .filter(|candidate| *candidate != owner)
            .map(|candidate| (candidate, shared.owner_objects(candidate)))
            .collect::<Vec<_>>();
        shared.release_owners(&[owner]).unwrap();
        worker.repository.collect_retired_objects().unwrap();
        assert!(shared.owner_objects(owner).is_empty());
        for (candidate, objects) in other_pins {
            assert_eq!(shared.owner_objects(candidate), objects);
        }
        assert_eq!(
            shared.get_bounded(object, config.max_payload_size).unwrap().unwrap().as_ref(),
            unsigned.as_ref()
        );
        assert!(matches!(
            worker.custody_anchor(origin),
            Err(ImportError::Persistence(error)) if error == "compact admission linkage differs"
        ));
        assert_eq!(
            worker.repository.original_block(origin).unwrap().unwrap().1.as_ref(),
            unsigned.as_ref()
        );
        let recovery = worker.repository.recover_checkpoint(&verifier, now, 32).unwrap().unwrap();
        assert_eq!(recovery.checkpoint.checkpoint().root, second_cut);
        assert_eq!(worker.graph.snapshot().unwrap(), snapshot);
        assert!(worker.last_execution_state.is_none());
        assert!(worker.last_verified_forkchoice.is_none());
        assert!(backend.payload_calls.lock().unwrap().is_empty());
        assert!(backend.forkchoice_calls.lock().unwrap().is_empty());
    })
    .await
    .unwrap();
}
