//! Genuine signed envelope and fresh same-adapter acknowledgement controls.
//! Controlled typed replies establish policy, not a live execution or network sync claim.
use super::*;

fn native_setup(
    directory: &TempDir,
    store: &Store,
    parameters: &TransitionParameters,
    root: Root,
) -> (WorkerSetup<BackendHandle, MissingAvailability, ExactEmptyGloasData>, Arc<Backend>) {
    let (setup, backend) = setup(directory, store, parameters);
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
    } = setup;
    *backend.expected_payload_hash.lock().unwrap() = [9; 32];
    let now = config.seconds_per_slot * 1000 * config.slots_per_epoch;
    let OwnerClock::Controlled(value) = &clock else { unreachable!() };
    value.store(now, Ordering::Release);
    (
        WorkerSetup {
            config,
            anchor,
            repository,
            engine,
            classic_availability,
            gloas_availability: ExactEmptyGloasData(root),
            limits,
            runtime,
            clock,
        },
        backend,
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn genuine_signed_pending_envelope_retries_without_publication_and_invalid_is_never_pending()
{
    let directory = TempDir::new().unwrap();
    let (store, keys, parameters, signed) = phase0_gloas_boundary();
    let root = signed.message_root().unwrap();
    let (setup, backend) = native_setup(&directory, &store, &parameters, root);
    let config = setup.config.clone();
    tokio::task::spawn_blocking(move || {
        let mut worker = Worker::new(setup, Ok(0)).unwrap();
        worker.advance(config.seconds_per_slot * 1000 * config.slots_per_epoch).unwrap();
        let wire = WireBlock::from_ssz(signed.to_ssz().unwrap(), &config).unwrap();
        worker.import(Input::Block(wire.clone()), 0).unwrap();
        let Graph::Gloas { store, .. } = &worker.graph else { unreachable!() };
        let envelope = WireEnvelope::from_ssz(original_gloas_envelope(store, root, &keys).to_ssz(Preset::Minimal).unwrap(), &wire, &config).unwrap();
        let pending_snapshot = worker.graph.snapshot().unwrap();
        for status in [PayloadStatusEnum::Syncing, PayloadStatusEnum::Accepted] {
            *backend.payload_status.lock().unwrap() = status;
            assert!(matches!(worker.import(Input::Envelope(envelope.clone()), 0), Err(ImportError::ExecutionPending(observed)) if observed == root));
            assert_eq!(worker.graph.snapshot().unwrap(), pending_snapshot);
            assert_eq!(worker.repository.current_snapshot().unwrap().unwrap().head(), pending_snapshot.head);
            assert!(worker.repository.original_envelope(root).unwrap().is_none());
            assert!(backend.forkchoice_calls.lock().unwrap().is_empty());
        }
        *backend.payload_status.lock().unwrap() = PayloadStatusEnum::Invalid { validation_error: "actual controlled INVALID".into() };
        assert!(matches!(worker.import(Input::Envelope(envelope.clone()), 0), Err(ImportError::Envelope(_))));
        assert_eq!(worker.graph.snapshot().unwrap(), pending_snapshot);
        assert!(!worker.repository.has_prepared_publication().unwrap());
        *backend.payload_status.lock().unwrap() = PayloadStatusEnum::Valid;
        let admitted = worker.import(Input::Envelope(envelope), 0).unwrap();
        assert_eq!(admitted.snapshot.head.payload_status, Some(reth_embedded_beacon_fork_choice::PayloadStatus::Full));
        assert!(admitted.execution_forkchoice_updated);
        assert_eq!(backend.payload_calls.lock().unwrap().len(), 4);
        assert_eq!(backend.forkchoice_calls.lock().unwrap().len(), 1);
    }).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn original_unpublished_envelope_is_excluded_and_uncertain_fcu_reconstructs_exact_full_candidate(
) {
    let directory = TempDir::new().unwrap();
    let (producer, keys, parameters, signed) = phase0_gloas_boundary();
    let root = signed.message_root().unwrap();
    let (setup, backend) = native_setup(&directory, &producer, &parameters, root);
    let config = setup.config.clone();
    tokio::task::spawn_blocking(move || {
        let mut worker = Worker::new(setup, Ok(0)).unwrap();
        let now = config.seconds_per_slot * 1000 * config.slots_per_epoch;
        worker.advance(now).unwrap();
        let wire = WireBlock::from_ssz(signed.to_ssz().unwrap(), &config).unwrap();
        worker.import(Input::Block(wire.clone()), 0).unwrap();
        let pending = worker.repository.current_snapshot().unwrap().unwrap();
        let Graph::Gloas { store, .. } = &worker.graph else { unreachable!() };
        let envelope = WireEnvelope::from_ssz(
            original_gloas_envelope(store, root, &keys).to_ssz(Preset::Minimal).unwrap(),
            &wire,
            &config,
        )
        .unwrap();
        worker.replaying = true;
        worker.import(Input::Envelope(envelope), 0).unwrap();
        assert!(worker.repository.original_envelope(root).unwrap().is_some());
        assert_eq!(worker.repository.current_snapshot().unwrap().unwrap().head(), pending.head());
        let plan = recovery::ReplayPlan::prepare(
            &worker.repository,
            &config,
            producer.get_head().unwrap().root,
            recovery::RecoveryLimits::default(),
        )
        .unwrap();
        assert!(plan
            .load(&worker.repository, &config, &plan.blocks[0])
            .unwrap()
            .envelope
            .is_none());
        let full = worker.graph.snapshot().unwrap();
        worker.source.prepare_snapshot(full).unwrap();
        let (fork, state) = worker
            .graph
            .execution_state_with_boundary(worker.execution_boundary.as_ref())
            .unwrap()
            .unwrap();
        worker
            .runtime
            .block_on(worker.engine.forkchoice_updated_receipt(
                forkchoice_version(fork).unwrap(),
                state,
                None,
            ))
            .unwrap();
        assert_eq!(backend.forkchoice_calls.lock().unwrap().len(), 1);
        assert_eq!(worker.repository.current_snapshot().unwrap().unwrap().head(), pending.head());
        assert!(worker.repository.has_prepared_publication().unwrap());
        drop(worker);

        let (setup, reopened_backend) = native_setup(&directory, &producer, &parameters, root);
        let mut restored = Worker::initialize(setup, Ok(now), true, None).unwrap();
        let plan = recovery::ReplayPlan::prepare(
            &restored.repository,
            &config,
            producer.get_head().unwrap().root,
            recovery::RecoveryLimits::default(),
        )
        .unwrap();
        assert_eq!(plan.expected.head(), full.head);
        assert!(reopened_backend.forkchoice_calls.lock().unwrap().is_empty());
        for sequence in &plan.transcript.as_ref().unwrap().sequences {
            restored.replay_original_event(*sequence).unwrap();
        }
        plan.check(&restored.graph).unwrap();
        assert!(reopened_backend.forkchoice_calls.lock().unwrap().is_empty());
        restored.replaying = false;
        let mut graph = restored.graph.clone();
        assert!(restored.publish(&mut graph).unwrap());
        restored.graph = graph;
        assert_eq!(reopened_backend.forkchoice_calls.lock().unwrap().len(), 1);
        assert!(!restored.repository.has_prepared_publication().unwrap());
        assert_eq!(restored.repository.current_snapshot().unwrap().unwrap().head(), full.head);
    })
    .await
    .unwrap();
}
