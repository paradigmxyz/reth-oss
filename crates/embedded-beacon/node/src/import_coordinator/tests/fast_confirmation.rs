//! Opt-in timely confirmation retains native safety and real Engine acknowledgement rules.

use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missed_deadline_suspends_then_punctual_signed_progress_reinitializes_confirmation() {
    let directory = TempDir::new().unwrap();
    let (mut producer, keys, parameters) = store_and_keys(Fork::Phase0);
    let first = phase0_signed_child(&producer, &keys, &parameters);
    let (mut initial, backend) = setup(&directory, &producer, &parameters);
    initial.limits.fast_confirmation = true;
    let config = initial.config.clone();
    let first_time = config.clock().unwrap().start_of_slot(1).unwrap();
    producer.on_tick(first_time).unwrap();
    producer.on_block(&SignedBeaconBlock::Phase0(first.clone(), Preset::Minimal)).unwrap();
    let second = phase0_signed_child(&producer, &keys, &parameters);
    let first = WireBlock::from_ssz(first.to_ssz(Preset::Minimal).unwrap(), &config).unwrap();
    let second = WireBlock::from_ssz(second.to_ssz(Preset::Minimal).unwrap(), &config).unwrap();
    let OwnerClock::Controlled(clock) = &initial.clock else { unreachable!() };
    let clock = clock.clone();
    tokio::task::spawn_blocking(move || {
        let mut worker = Worker::new(initial, Ok(0)).unwrap();
        clock.store(first_time, Ordering::Release);
        worker.advance(first_time).unwrap();
        worker.import(Input::Block(first), 0).unwrap();
        let Graph::Classic { store, confirmation } = &worker.graph else { unreachable!() };
        let observed = confirmation.state.as_ref().unwrap().snapshot();
        let native = store.snapshot().unwrap();
        let parameters = ReadGossipView::store_parameters(store);
        let due = parameters.slot_duration_ms * parameters.attestation_due_bps / 10_000;
        // Additional owner commands in the same slot never manufacture another observation.
        clock.store(first_time + due, Ordering::Release);
        worker.advance(first_time + due).unwrap();
        let Graph::Classic { confirmation, .. } = &worker.graph else { unreachable!() };
        assert_eq!(confirmation.state.as_ref().unwrap().snapshot(), observed);
        let deadline = config.clock().unwrap().start_of_slot(2).unwrap() + due;
        clock.store(deadline, Ordering::Release);
        worker.advance(deadline).unwrap();
        let Graph::Classic { store, confirmation } = &worker.graph else { unreachable!() };
        assert!(confirmation.state.is_none());
        assert_eq!(store.snapshot().unwrap().head, native.head);
        assert_eq!(store.snapshot().unwrap().finalized_checkpoint, native.finalized_checkpoint);
        // A fully signed child is still admitted after the optional timer missed its deadline.
        worker.import(Input::Block(second.clone()), 0).unwrap();
        assert_eq!(worker.graph.snapshot().unwrap().head.root, second.root());
        let punctual = config.clock().unwrap().start_of_slot(3).unwrap();
        clock.store(punctual, Ordering::Release);
        worker.advance(punctual).unwrap();
        let Graph::Classic { store, confirmation } = &worker.graph else { unreachable!() };
        let actual = store.snapshot().unwrap();
        assert_eq!(
            confirmation.state.as_ref().unwrap().snapshot().confirmed_root,
            actual.finalized_checkpoint.root
        );
        assert_eq!(actual.head.root, second.root());
        assert_eq!(actual.finalized_checkpoint, native.finalized_checkpoint);
        assert!(!worker.engine.is_closed());
        assert!(backend.payload_calls.lock().unwrap().is_empty());
        assert!(backend.forkchoice_calls.lock().unwrap().is_empty());
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gloas_confirmation_prepares_historical_checkpoint_and_preserves_verified_parent_safety() {
    for enabled in [false, true] {
        let directory = TempDir::new().unwrap();
        let (store, keys, parameters, signed) = phase0_gloas_boundary();
        let (mut initial, backend) = setup(&directory, &store, &parameters);
        initial.limits.fast_confirmation = enabled;
        let config = initial.config.clone();
        let root = signed.message_root().unwrap();
        let wire = WireBlock::from_ssz(signed.to_ssz().unwrap(), &config).unwrap();
        let WorkerSetup {
            config,
            anchor,
            repository,
            engine,
            classic_availability,
            gloas_availability: _,
            limits,
            runtime,
            clock,
        } = initial;
        let OwnerClock::Controlled(controlled) = &clock else { unreachable!() };
        let controlled = controlled.clone();
        let initial = WorkerSetup {
            config: config.clone(),
            anchor,
            repository,
            engine,
            classic_availability,
            gloas_availability: ExactEmptyGloasData(root),
            limits,
            runtime,
            clock,
        };
        tokio::task::spawn_blocking(move || {
            let mut worker = Worker::new(initial, Ok(0)).unwrap();
            let upgrade = config.clock().unwrap().start_of_slot(config.slots_per_epoch).unwrap();
            controlled.store(upgrade, Ordering::Release);
            worker.advance(upgrade).unwrap();
            worker.import(Input::Block(wire.clone()), 0).unwrap();
            let Graph::Gloas { store, .. } = &worker.graph else { unreachable!() };
            let signed_envelope = original_gloas_envelope(store, root, &keys);
            let envelope = WireEnvelope::from_ssz(
                signed_envelope.to_ssz(Preset::Minimal).unwrap(),
                &wire,
                &config,
            )
            .unwrap();
            *backend.expected_payload_hash.lock().unwrap() = [9; 32];
            worker.import(Input::Envelope(envelope), 0).unwrap();
            let next = config.clock().unwrap().start_of_slot(config.slots_per_epoch + 1).unwrap();
            controlled.store(next, Ordering::Release);
            worker.advance(next).unwrap();
            let Graph::Gloas { store, confirmation } = &worker.graph else { unreachable!() };
            let actual = store.snapshot().unwrap();
            assert_eq!(actual.head.root, root);
            assert_eq!(
                actual.head.payload_status,
                Some(reth_embedded_beacon_fork_choice::PayloadStatus::Full)
            );
            assert_eq!(
                store.historical_block(actual.finalized_checkpoint.root).unwrap().schema_fork(),
                Fork::Phase0
            );
            assert_eq!(confirmation.state.is_some(), enabled);
            if let Some(confirmation) = &confirmation.state {
                assert_eq!(
                    confirmation.snapshot().confirmed_root,
                    actual.finalized_checkpoint.root
                );
            }
            let (fork, execution) =
                worker.graph.execution_state_with_boundary(None).unwrap().unwrap();
            assert_eq!(fork, Fork::Gloas);
            assert_eq!(execution.head_block_hash, B256::from([9; 32]));
            assert_eq!(execution.safe_block_hash, B256::ZERO);
            assert_eq!(execution.finalized_block_hash, B256::ZERO);
            assert!(worker
                .last_verified_forkchoice
                .as_ref()
                .unwrap()
                .matches(&worker.engine, execution));
            assert!(!worker.engine.is_closed());
        })
        .await
        .unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn optimistic_fcr_requires_real_catchup_and_retries_nonzero_safety_without_false_acknowledgement(
) {
    let directory = TempDir::new().unwrap();
    let (store, keys, parameters) = post_merge_anchor();
    let signed = bellatrix_signed_child(&store, &keys, &parameters);
    let (mut initial, backend) = setup(&directory, &store, &parameters);
    initial.limits.fast_confirmation = true;
    let wire =
        WireBlock::from_ssz(signed.to_ssz(Preset::Minimal).unwrap(), &initial.config).unwrap();
    *backend.forkchoice_status.lock().unwrap() = PayloadStatusEnum::Syncing;
    *backend.payload_status.lock().unwrap() = PayloadStatusEnum::Syncing;
    let OwnerClock::Controlled(clock) = &initial.clock else { unreachable!() };
    let clock = clock.clone();
    tokio::task::spawn_blocking(move || {
        let mut worker =
            Worker::initialize_mode(initial, Ok(0), false, StartupExecutionMode::Optimistic)
                .unwrap();
        let millis = worker.config.seconds_per_slot * 1000;
        let (_, fresh) = backend.forkchoice_calls.lock().unwrap()[0];
        assert_eq!(fresh.safe_block_hash, B256::ZERO);
        assert_eq!(fresh.finalized_block_hash, B256::ZERO);
        assert!(worker.last_verified_forkchoice.is_none());
        clock.store(millis, Ordering::Release);
        worker.advance(millis).unwrap();
        worker.import(Input::Block(wire.clone()), 0).unwrap();
        clock.store(2 * millis, Ordering::Release);
        worker.advance(2 * millis).unwrap();
        let Graph::Classic { confirmation, .. } = &worker.graph else { unreachable!() };
        assert!(confirmation.state.is_none());
        for (_, state) in backend.forkchoice_calls.lock().unwrap().iter() {
            assert_eq!(state.safe_block_hash, B256::ZERO);
            assert_eq!(state.finalized_block_hash, B256::ZERO);
        }
        *backend.payload_status.lock().unwrap() = PayloadStatusEnum::Valid;
        *backend.forkchoice_status.lock().unwrap() = PayloadStatusEnum::Valid;
        clock.store(3 * millis, Ordering::Release);
        worker.advance(3 * millis).unwrap();
        let Graph::Classic { store, .. } = &worker.graph else { unreachable!() };
        assert!(store.execution_optimism_settled());
        let (_, zero_target) = *backend.forkchoice_calls.lock().unwrap().last().unwrap();
        assert_eq!(zero_target.head_block_hash, B256::from([2; 32]));
        assert_eq!(zero_target.safe_block_hash, B256::ZERO);
        assert!(worker
            .last_verified_forkchoice
            .as_ref()
            .unwrap()
            .matches(&worker.engine, zero_target));
        *backend.forkchoice_status.lock().unwrap() = PayloadStatusEnum::Syncing;
        clock.store(4 * millis, Ordering::Release);
        worker.advance(4 * millis).unwrap();
        let (_, verified_target) = *backend.forkchoice_calls.lock().unwrap().last().unwrap();
        assert_eq!(verified_target.head_block_hash, B256::from([2; 32]));
        assert_eq!(verified_target.safe_block_hash, B256::from([1; 32]));
        assert_eq!(verified_target.finalized_block_hash, B256::from([1; 32]));
        assert!(!worker
            .last_verified_forkchoice
            .as_ref()
            .unwrap()
            .matches(&worker.engine, verified_target));
        let Observation::Status(status) = worker.observe(ObservationRequest::Status).unwrap()
        else {
            panic!("actual owner status")
        };
        assert!(!status.execution_acknowledged);
        assert!(!status.head_optimistic);
        let calls = backend.forkchoice_calls.lock().unwrap().len();
        worker.advance(4 * millis).unwrap();
        assert_eq!(backend.forkchoice_calls.lock().unwrap().len(), calls);
        clock.store(5 * millis, Ordering::Release);
        worker.advance(5 * millis).unwrap();
        assert_eq!(backend.forkchoice_calls.lock().unwrap().len(), calls + 1);
        assert_eq!(backend.forkchoice_calls.lock().unwrap().last().unwrap().1, verified_target);
        *backend.forkchoice_status.lock().unwrap() = PayloadStatusEnum::Valid;
        clock.store(6 * millis, Ordering::Release);
        worker.advance(6 * millis).unwrap();
        assert!(worker
            .last_verified_forkchoice
            .as_ref()
            .unwrap()
            .matches(&worker.engine, verified_target));
        let Observation::Status(status) = worker.observe(ObservationRequest::Status).unwrap()
        else {
            panic!("actual owner status")
        };
        assert!(status.execution_acknowledged);
        assert_eq!(status.snapshot.head.root, wire.root());
        assert!(!worker.engine.is_closed());
    })
    .await
    .unwrap();
}
