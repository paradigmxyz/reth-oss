//! Genuine signed transitions and actual typed statuses exercise optional checkpoint optimism.

use super::*;
use reth_embedded_beacon_fork_choice::ExecutionStatus;
use reth_embedded_beacon_state_transition::fulu_processing;

fn status<E, C, G>(worker: &Worker<E, C, G>) -> OwnerObservation
where
    E: ExecutionEngine<
            Payload = ExecutionData,
            PayloadReply = PayloadReply,
            Forkchoice = ForkchoiceState,
            ForkchoiceReply = ForkchoiceReply,
        > + Clone,
    E::Error: std::error::Error + Send + Sync + 'static,
    C: ClassicAvailability,
    G: GloasAvailability,
{
    let Observation::Status(status) = worker.observe(ObservationRequest::Status).unwrap() else {
        panic!("actual status")
    };
    status
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn syncing_and_accepted_advance_checked_consensus_without_execution_authority() {
    for pending in [PayloadStatusEnum::Syncing, PayloadStatusEnum::Accepted] {
        let directory = TempDir::new().unwrap();
        let (store, keys, parameters) = post_merge_anchor();
        let signed = bellatrix_signed_child(&store, &keys, &parameters);
        let (setup, backend) = setup(&directory, &store, &parameters);
        *backend.forkchoice_status.lock().unwrap() = PayloadStatusEnum::Syncing;
        *backend.payload_status.lock().unwrap() = pending;
        let wire =
            WireBlock::from_ssz(signed.to_ssz(Preset::Minimal).unwrap(), &setup.config).unwrap();
        tokio::task::spawn_blocking(move || {
            let mut worker = Worker::initialize_mode(setup, Ok(0), false, None, true).unwrap();
            assert!(!status(&worker).execution_acknowledged);
            assert_eq!(backend.forkchoice_calls.lock().unwrap().len(), 1);
            let slot_ms = worker.config.seconds_per_slot * 1000;
            worker.advance(slot_ms).unwrap();
            assert_eq!(backend.forkchoice_calls.lock().unwrap().len(), 2);
            worker.advance(slot_ms).unwrap();
            assert_eq!(backend.forkchoice_calls.lock().unwrap().len(), 2);
            let receipt = worker.import(Input::Block(wire.clone()), 0).unwrap();
            assert_eq!(receipt.snapshot.head.root, wire.root());
            assert!(!receipt.execution_forkchoice_updated);
            assert!(!status(&worker).execution_acknowledged);
            assert!(status(&worker).head_optimistic);
            // Accepted CL work does not count or manufacture execution validation.
            assert_eq!(status(&worker).imports.accepted_total, 1);
            assert_eq!(status(&worker).imports.execution_deferred_total, 0);
            assert_eq!(status(&worker).imports.measured_native_imports_total, 1);
            assert!(status(&worker).imports.transition_with_execution_micros_total > 0);
            let Graph::Classic { store, .. } = &mut worker.graph else { unreachable!() };
            assert_eq!(
                ReadGossipView::execution_status(store, wire.root()),
                ExecutionStatus::Optimistic
            );
            assert!(store.verified_execution_block_hash(wire.root()).is_err());
            assert_eq!(store.execution_target_hash(wire.root()).unwrap(), Some([2; 32]));
            let prepared = store.prepare_fast_confirmation(&[]).unwrap();
            assert_eq!(prepared.block_info(wire.root()).unwrap().safe_execution_hash, None);
            store.evict_hot_states().unwrap();
            assert_eq!(
                store.read_block_state(wire.root()).unwrap().unwrap().hash_tree_root().unwrap(),
                signed.message.state_root
            );
            assert_eq!(
                worker.repository.current_snapshot().unwrap().unwrap().head().root,
                wire.root()
            );
            assert_eq!(
                worker.repository.original_block(wire.root()).unwrap().unwrap().1.as_ref(),
                wire.ssz().as_ref()
            );
            assert_eq!(worker.repository.stats().unwrap().retained_states, 2);
            // A later real VALID promotes exact retained optimistic execution ancestors.
            let Graph::Classic { store, .. } = &worker.graph else { unreachable!() };
            let second = bellatrix_signed_child(store, &keys, &parameters);
            let second_wire =
                WireBlock::from_ssz(second.to_ssz(Preset::Minimal).unwrap(), &worker.config)
                    .unwrap();
            let OwnerClock::Controlled(clock) = &worker.clock else { unreachable!() };
            clock.store(2 * slot_ms, Ordering::Release);
            worker.advance(2 * slot_ms).unwrap();
            worker.import(Input::Block(second_wire.clone()), 0).unwrap();
            assert_eq!(worker.graph.snapshot().unwrap().head.root, second_wire.root());
            assert!(status(&worker).head_optimistic);
            *backend.expected_payload_hash.lock().unwrap() = [3; 32];
            *backend.payload_status.lock().unwrap() = PayloadStatusEnum::Valid;
            *backend.forkchoice_status.lock().unwrap() = PayloadStatusEnum::Valid;
            worker.advance(3 * slot_ms).unwrap();
            assert!(status(&worker).execution_acknowledged);
            assert!(!status(&worker).head_optimistic);
            let Graph::Classic { store, .. } = &worker.graph else { unreachable!() };
            assert_eq!(store.verified_execution_block_hash(wire.root()).unwrap(), Some([2; 32]));
            let prepared = store.prepare_fast_confirmation(&[]).unwrap();
            assert_eq!(
                prepared.block_info(wire.root()).unwrap().safe_execution_hash,
                Some([2; 32])
            );
            for (_, state) in backend.forkchoice_calls.lock().unwrap().iter() {
                assert_eq!(state.safe_block_hash, B256::ZERO);
                assert_eq!(state.finalized_block_hash, B256::ZERO);
            }
        })
        .await
        .unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn optimistic_signed_rejection_and_actual_invalid_fail_safely() {
    let directory = TempDir::new().unwrap();
    let (store, keys, parameters) = post_merge_anchor();
    let signed = bellatrix_signed_child(&store, &keys, &parameters);
    let (setup, backend) = setup(&directory, &store, &parameters);
    *backend.forkchoice_status.lock().unwrap() = PayloadStatusEnum::Syncing;
    *backend.payload_status.lock().unwrap() = PayloadStatusEnum::Syncing;
    let config = setup.config.clone();
    tokio::task::spawn_blocking(move || {
        let mut worker = Worker::initialize_mode(setup, Ok(0), false, None, true).unwrap();
        worker.advance(config.seconds_per_slot * 1000).unwrap();
        let anchor = worker.graph.snapshot().unwrap();
        // A native optimistic unknown-parent dependency must not close the graph owner.
        let mut unknown = signed.clone();
        unknown.message.parent_root = [233; 32];
        let unknown = SignedBeaconBlock::Bellatrix(unknown, Preset::Minimal);
        let Graph::Classic { store, .. } = &mut worker.graph else { unreachable!() };
        let error = worker
            .runtime
            .block_on(store.on_block_optimistic(
                &unknown,
                Arc::new(worker.engine.clone()),
                &worker.classic_availability,
            ))
            .unwrap_err();
        assert!(matches!(
            error,
            reth_embedded_beacon_fork_choice::store::optimistic::OptimisticError::InvalidParent
        ));
        let error = optimistic_error(error);
        assert!(matches!(error, ImportError::Consensus(StoreError::UnknownRoot)));
        assert!(!error.fatal());
        assert_eq!(worker.graph.snapshot().unwrap(), anchor);
        let mut corrupt = signed.clone();
        corrupt.signature = [0; 96];
        let corrupt =
            WireBlock::from_ssz(corrupt.to_ssz(Preset::Minimal).unwrap(), &config).unwrap();
        let error = worker.import(Input::Block(corrupt), 0).unwrap_err();
        assert!(matches!(error, ImportError::Consensus(_)));
        assert_eq!(worker.graph.snapshot().unwrap(), anchor);
        assert!(backend.payload_calls.lock().unwrap().is_empty());
        assert_eq!(worker.repository.stats().unwrap().retained_states, 1);
        *backend.payload_status.lock().unwrap() = PayloadStatusEnum::Invalid {
            validation_error: "actual controlled execution rejection".into(),
        };
        let wire = WireBlock::from_ssz(signed.to_ssz(Preset::Minimal).unwrap(), &config).unwrap();
        let error = worker.import(Input::Block(wire), 0).unwrap_err();
        assert!(error.fatal());
        assert_eq!(worker.graph.snapshot().unwrap(), anchor);
        assert_eq!(worker.repository.stats().unwrap().retained_states, 1);
    })
    .await
    .unwrap();
}

#[derive(Debug)]
struct EmptyFuluAvailability;
impl ClassicAvailability for EmptyFuluAvailability {
    type Error = io::Error;
    async fn verify(&self, _: Root, block: &BeaconBlock) -> Result<(), Self::Error> {
        let BeaconBlock::Fulu(block, _) = block else {
            return Err(io::Error::other("expected actual Fulu block"))
        };
        if !block.body.blob_kzg_commitments.values.is_empty() {
            return Err(io::Error::other("test does not prove blob availability"))
        }
        Ok(())
    }
}

fn fulu_child(
    store: &impl ReadGossipView,
    keys: &[SecretKey],
    parameters: &TransitionParameters,
) -> fulu::SignedBeaconBlock {
    let parent = store.get_head().unwrap().root;
    let owned = store.read_block_state(parent).unwrap().unwrap();
    let BeaconState::Fulu(mut state, preset) = owned.as_ref().clone() else {
        panic!("actual Fulu state")
    };
    let slot = state.slot + 1;
    let processing = &parameters.gloas.fulu;
    fulu_processing::process_slots(&mut state, slot, processing).unwrap();
    let proposer = fulu_processing::get_beacon_proposer_index(&state, processing).unwrap();
    let epoch = slot / parameters.slots_per_epoch();
    let mut message = fulu::BeaconBlock::default_value(preset).unwrap();
    message.slot = slot;
    message.proposer_index = proposer;
    message.parent_root = parent;
    message.body.eth1_data = state.eth1_data.clone();
    message.body.sync_aggregate.sync_committee_signature[0] = 0xc0;
    message.body.execution_payload.parent_hash = state.latest_execution_payload_header.block_hash;
    message.body.execution_payload.block_number =
        state.latest_execution_payload_header.block_number + 1;
    message.body.execution_payload.block_hash = [42; 32];
    message.body.execution_payload.gas_limit = 30_000_000;
    message.body.execution_payload.timestamp =
        state.genesis_time + slot * processing.electra.deneb.capella.bellatrix.seconds_per_slot;
    message.body.execution_payload.prev_randao = state.randao_mixes.values[(epoch % 64) as usize];
    let key = &keys[proposer as usize];
    message.body.randao_reveal = key
        .sign(
            &signing_root(
                uint64_root(epoch),
                [2, 0, 0, 0],
                state.fork.current_version,
                state.genesis_validators_root,
            ),
            ETHEREUM_BLS_DST,
            &[],
        )
        .to_bytes();
    fulu_processing::process_block_header(&mut state, &message, processing).unwrap();
    fulu_processing::process_withdrawals(&mut state, &message.body.execution_payload, processing)
        .unwrap();
    state.latest_execution_payload_header =
        fulu_processing::validate_execution_payload_consensus(&state, &message.body, processing)
            .unwrap();
    fulu_processing::process_randao(&mut state, &message.body, processing).unwrap();
    fulu_processing::process_eth1_data(&mut state, &message.body, processing).unwrap();
    fulu_processing::process_operations(&mut state, &message.body, processing).unwrap();
    fulu_processing::process_sync_aggregate(&mut state, &message.body.sync_aggregate, processing)
        .unwrap();
    message.state_root = state.hash_tree_root(preset).unwrap();
    let signature = key
        .sign(
            &signing_root(
                message.hash_tree_root(preset).unwrap(),
                [0; 4],
                state.fork.current_version,
                state.genesis_validators_root,
            ),
            ETHEREUM_BLS_DST,
            &[],
        )
        .to_bytes();
    fulu::SignedBeaconBlock { message, signature }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn genuine_fulu_checkpoint_descendant_and_exact_original_restart_remain_optimistic() {
    let directory = TempDir::new().unwrap();
    let (state, signed_anchor, keys, parameters) =
        crate::execution_bootstrap::tests::genuine_fulu().await;
    let mut producer =
        Store::get_store(state.clone(), signed_anchor.message(), parameters_for(&parameters))
            .unwrap();
    producer.set_transition_parameters(parameters.clone()).unwrap();
    let signed = fulu_child(&producer, &keys, &parameters);
    let (mut initial, backend) = setup(&directory, &producer, &parameters);
    initial.anchor.block_format = CheckpointBlockFormat::Signed;
    initial.anchor.block_ssz = Arc::from(signed_anchor.to_ssz().unwrap());
    let WorkerSetup {
        config,
        anchor,
        repository,
        engine,
        gloas_availability,
        limits,
        runtime,
        clock,
        ..
    } = initial;
    let now = (config.genesis_time + state.slot() * config.seconds_per_slot) * 1000;
    let initial = WorkerSetup {
        config: config.clone(),
        anchor,
        repository,
        engine,
        classic_availability: EmptyFuluAvailability,
        gloas_availability,
        limits,
        runtime,
        clock,
    };
    *backend.forkchoice_status.lock().unwrap() = PayloadStatusEnum::Syncing;
    *backend.payload_status.lock().unwrap() = PayloadStatusEnum::Syncing;
    *backend.expected_payload_hash.lock().unwrap() = [42; 32];
    let wire = WireBlock::from_ssz(signed.to_ssz(Preset::Minimal).unwrap(), &config).unwrap();
    tokio::task::spawn_blocking(move || {
        let mut worker = Worker::initialize_mode(initial, Ok(now), false, None, true).unwrap();
        let child_ms = now + config.seconds_per_slot * 1000;
        let OwnerClock::Controlled(clock) = &worker.clock else { unreachable!() };
        clock.store(child_ms, Ordering::Release);
        worker.advance(child_ms).unwrap();
        worker.import(Input::Block(wire.clone()), child_ms).unwrap();
        assert_eq!(worker.graph.snapshot().unwrap().head.root, wire.root());
        assert!(status(&worker).head_optimistic);
        assert!(!status(&worker).execution_acknowledged);
        assert_eq!(*backend.payload_calls.lock().unwrap(), vec![PayloadVersion::V4]);
        assert_eq!(
            worker.repository.original_block(wire.root()).unwrap().unwrap().1.as_ref(),
            wire.ssz().as_ref()
        );
        assert_eq!(worker.repository.stats().unwrap().retained_states, 2);
        drop(worker);
        let (restored, reopened) = setup(&directory, &producer, &parameters);
        let WorkerSetup {
            config,
            anchor,
            repository,
            engine,
            gloas_availability,
            limits,
            runtime,
            clock,
            ..
        } = restored;
        let mut anchor = anchor;
        anchor.block_format = CheckpointBlockFormat::Signed;
        anchor.block_ssz = Arc::from(signed_anchor.to_ssz().unwrap());
        let restored = WorkerSetup {
            config: config.clone(),
            anchor,
            repository,
            engine,
            classic_availability: EmptyFuluAvailability,
            gloas_availability,
            limits,
            runtime,
            clock,
        };
        *reopened.forkchoice_status.lock().unwrap() = PayloadStatusEnum::Syncing;
        *reopened.payload_status.lock().unwrap() = PayloadStatusEnum::Syncing;
        *reopened.expected_payload_hash.lock().unwrap() = [42; 32];
        let mut restored =
            Worker::initialize_mode(restored, Ok(child_ms), true, None, true).unwrap();
        let plan = recovery::ReplayPlan::prepare(
            &restored.repository,
            &config,
            producer.get_head().unwrap().root,
            recovery::RecoveryLimits::default(),
        )
        .unwrap();
        for sequence in &plan.transcript.as_ref().unwrap().sequences {
            restored.replay_original_event(*sequence).unwrap();
            assert!(restored.replay_state_lease.is_none());
            assert!(restored.replay_event.is_none());
        }
        assert_eq!(
            restored.import_metrics.replayed_events_total,
            plan.transcript.as_ref().unwrap().sequences.len() as u64
        );
        assert_eq!(restored.import_metrics.replay_attempts_total, 1);
        plan.check(&restored.graph).unwrap();
        assert!(reopened.forkchoice_calls.lock().unwrap().is_empty());
        assert!(status(&restored).head_optimistic);
        restored.replaying = false;
        let mut graph = restored.graph.clone();
        assert!(!restored.publish(&mut graph).unwrap());
        restored.graph = graph;
        assert!(!status(&restored).execution_acknowledged);
        *reopened.forkchoice_status.lock().unwrap() = PayloadStatusEnum::Valid;
        *reopened.payload_status.lock().unwrap() = PayloadStatusEnum::Valid;
        restored.advance(child_ms + config.seconds_per_slot * 1000).unwrap();
        assert!(status(&restored).execution_acknowledged);
        assert!(!status(&restored).head_optimistic);
    })
    .await
    .unwrap();
}

#[test]
fn bounded_optimistic_context_pressure_cannot_close_admission() {
    let error = optimistic_error(
        reth_embedded_beacon_fork_choice::store::optimistic::OptimisticError::ResourceLimit,
    );
    assert!(matches!(error, ImportError::Bounds));
    assert!(!error.fatal());
    // Resource/dependency rejection remains distinct from actual invalidated checkpoint authority.
    let invalidated = optimistic_error(
        reth_embedded_beacon_fork_choice::store::optimistic::OptimisticError::InvalidatedCheckpoint,
    );
    assert!(invalidated.fatal());
}
