//! Genuine finalized cuts retain original evidence after its competing source block is retired.
use super::{
    slashings::{modified_child, signed_pair},
    *,
};
use crate::compact_persistence::events::EventAction;
use blst::min_pk::AggregateSignature;
use reth_embedded_beacon_state_transition::{phase0_processing, ssz::phase0};

pub(super) fn finalized_child(
    store: &CompactStore,
    keys: &[SecretKey],
    parameters: &TransitionParameters,
    slot: u64,
) -> phase0::SignedBeaconBlock {
    let head = store.get_head().unwrap();
    let parent = store.read_block_state(head.root).unwrap().unwrap();
    let BeaconState::Phase0(mut state, preset) =
        fork_processing::replay_slots(&parent, slot, parameters, 1).unwrap()
    else {
        panic!("genuine Phase0 retained chain")
    };
    let processing = &parameters.gloas.fulu.electra.deneb.capella.bellatrix.altair.phase0;
    let epoch = slot / processing.slots_per_epoch();
    let proposer = phase0_processing::get_beacon_proposer_index(&state, processing).unwrap();
    let key = &keys[proposer as usize];
    let mut message = phase0::BeaconBlock::default_value(preset).unwrap();
    message.slot = slot;
    message.proposer_index = proposer;
    message.parent_root = head.root;
    message.body.eth1_data = state.eth1_data.clone();
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
    let attested = slot - 1;
    let vote_epoch = attested / processing.slots_per_epoch();
    let committee =
        phase0_processing::get_beacon_committee(&state, attested, 0, processing).unwrap();
    let data = phase0::AttestationData {
        slot: attested,
        index: 0,
        beacon_block_root: state.block_roots.values[(attested % 64) as usize],
        source: if vote_epoch == epoch {
            state.current_justified_checkpoint
        } else {
            state.previous_justified_checkpoint
        },
        target: Checkpoint {
            epoch: vote_epoch,
            root: state.block_roots.values
                [(vote_epoch * processing.slots_per_epoch() % 64) as usize],
        },
    };
    let version = if vote_epoch < state.fork.epoch {
        state.fork.previous_version
    } else {
        state.fork.current_version
    };
    let root = signing_root(
        data.hash_tree_root(preset).unwrap(),
        [1, 0, 0, 0],
        version,
        state.genesis_validators_root,
    );
    let signatures = committee
        .iter()
        .map(|index| keys[*index as usize].sign(&root, ETHEREUM_BLS_DST, &[]))
        .collect::<Vec<_>>();
    let signature = AggregateSignature::aggregate(&signatures.iter().collect::<Vec<_>>(), true)
        .unwrap()
        .to_signature()
        .to_bytes();
    let mut bits = vec![0; (committee.len() + 1).div_ceil(8)];
    for bit in 0..=committee.len() {
        bits[bit / 8] |= 1 << (bit % 8);
    }
    message.body.attestations.values.push(phase0::Attestation {
        data,
        signature,
        aggregation_bits: phase0::AggregationBits::from_ssz(&bits, preset).unwrap(),
    });
    phase0_processing::process_block(&mut state, &message, processing).unwrap();
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
    phase0::SignedBeaconBlock { message, signature }
}

pub(super) fn cut_without_shortening_native_policy<E, C, G>(worker: &mut Worker<E, C, G>) -> Root
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
    // Only this isolated control selects the already-supported two-epoch policy. Runtime
    // publication still executes its normal strict canonical/retention ordering unchanged.
    let mut candidate = worker.graph.clone();
    let Graph::Classic { store, .. } = &mut candidate else { panic!("classic graph") };
    let cut = store.prune_finalized_history(FinalizedRetentionPolicy::new(2).unwrap()).unwrap();
    assert!(cut.removed_blocks > 0);
    let root = cut.boundary.expect("genuine signed advancing finality").root;
    // The actual cut retired the optional confirmation constructor's old anchor. Restart
    // only that native optimization at this graph's real finalized checkpoint.
    candidate.reset_confirmation().unwrap();
    worker.publish(&mut candidate).unwrap();
    worker.graph = candidate;
    root
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn genuine_two_finalized_cuts_retire_competing_block_but_replay_original_signed_exclusion() {
    let directory = TempDir::new().unwrap();
    let (producer, keys, parameters) = store_and_keys(Fork::Phase0);
    let proof = signed_pair(&producer, &keys, 0);
    let slashed = modified_child(&producer, &keys, &parameters, Some(proof.clone()), 12);
    let competing = slashed.message.hash_tree_root(Preset::Minimal).unwrap();
    let clean = (0..=255)
        .map(|marker| modified_child(&producer, &keys, &parameters, None, marker))
        .find(|child| child.message.hash_tree_root(Preset::Minimal).unwrap() > competing)
        .unwrap();
    let (mut initial, backend) = setup(&directory, &producer, &parameters);
    initial.limits.fast_confirmation = true;
    let config = initial.config.clone();
    tokio::task::spawn_blocking(move || {
        let mut worker = Worker::new(initial, Ok(0)).unwrap();
        let millis = config.seconds_per_slot * 1000;
        worker.advance(millis).unwrap();
        for signed in [slashed, clean] {
            let wire =
                WireBlock::from_ssz(signed.to_ssz(Preset::Minimal).unwrap(), &config).unwrap();
            worker.import(Input::Block(wire), 0).unwrap();
        }
        let mut first = None;
        for slot in 2..=96 {
            let OwnerClock::Controlled(clock) = &worker.clock else { unreachable!() };
            clock.store(slot * millis, Ordering::Release);
            if slot == 80 {
                // Genuine finality advances native checkpoint-state retirement before the
                // epoch-start observation discards its old previous-epoch source.
                let mut next = worker.graph.clone();
                next.tick(slot * millis).unwrap();
                let Graph::Classic { store, confirmation } = &next else { unreachable!() };
                let confirmation = confirmation.state.as_ref().unwrap();
                let before = confirmation.snapshot();
                let retired = before.previous_epoch_observed_justified_checkpoint;
                assert!(store.read_checkpoint_state(retired).unwrap().is_none());
                let metadata = store.prepare_fast_confirmation(&[]).unwrap();
                let reads = confirmation.checkpoint_states_for_update(&metadata).unwrap();
                assert!(!reads.contains(&retired));
                assert_eq!(confirmation.snapshot(), before);
                for checkpoint in reads {
                    assert!(store.read_checkpoint_state(checkpoint).unwrap().is_some());
                }
            }
            worker.advance(slot * millis).unwrap_or_else(|error| {
                panic!("genuine signed-chain advance at slot {slot}: {error:?}")
            });
            let Graph::Classic { store, .. } = &worker.graph else { unreachable!() };
            let signed = finalized_child(store, &keys, &parameters, slot);
            let wire =
                WireBlock::from_ssz(signed.to_ssz(Preset::Minimal).unwrap(), &config).unwrap();
            worker.import(Input::Block(wire), (slot - 1) * millis).unwrap();
            if slot == 64 {
                first = Some(cut_without_shortening_native_policy(&mut worker));
                let Graph::Classic { store, .. } = &worker.graph else { unreachable!() };
                assert!(store.block(competing).is_none());
            }
        }
        let second = cut_without_shortening_native_policy(&mut worker);
        assert_ne!(Some(second), first);
        assert!(worker.repository.original_block(competing).unwrap().is_none());
        let expected = worker.graph.snapshot().unwrap();
        assert!(expected.finalized_checkpoint.epoch > 2);
        let Graph::Classic { store, .. } = &worker.graph else { unreachable!() };
        let weights = store.viable_leaf_weights().unwrap();
        assert!(
            !StateRef::Classic(&store.read_block_state(expected.head.root).unwrap().unwrap())
                .validators()[0]
                .slashed
        );
        let transcript = worker.repository.replay_transcript().unwrap().unwrap();
        let witness = transcript
            .sequences
            .iter()
            .map(|id| worker.repository.original_event(*id).unwrap())
            .find(|event| matches!(event.action, EventAction::Slashing { .. }))
            .unwrap();
        assert!(witness.begin_ms < config.clock().unwrap().start_of_slot(32).unwrap());
        assert_eq!(
            worker.repository.original_observation(&witness).unwrap().as_ref(),
            proof.to_ssz(Preset::Minimal).unwrap()
        );
        assert!(backend.forkchoice_calls.lock().unwrap().is_empty());
        drop(worker);

        let (mut restored, replay_backend) = setup(&directory, &producer, &parameters);
        let verifier = SszCheckpointVerifier::new(
            config.clone(),
            BootstrapDecodeLimits::default(),
            CheckpointBlockFormat::Unsigned,
        )
        .unwrap();
        let recovered =
            restored.repository.recover_checkpoint(&verifier, 96 * millis, 32).unwrap().unwrap();
        restored.anchor =
            recovered.into_import_anchor(32, BootstrapDecodeLimits::default()).unwrap();
        let mut restored = Worker::initialize(restored, Ok(96 * millis), true, None).unwrap();
        assert!(restored.graph.time_ms() > witness.completed_ms);
        let Graph::Classic { store, .. } = &mut restored.graph else { unreachable!() };
        let seed = store.snapshot().unwrap();
        let seed_weights = store.viable_leaf_weights().unwrap();
        let mut forged = proof.clone();
        forged.attestation_1.signature = [0; 96];
        assert!(matches!(
            store.on_original_attester_slashing(
                &forged.to_ssz(Preset::Minimal).unwrap(),
                Fork::Phase0,
                &parameters,
                0,
            ),
            Err(StoreError::Processing(_))
        ));
        assert_eq!(store.snapshot().unwrap(), seed);
        assert_eq!(store.viable_leaf_weights().unwrap(), seed_weights);
        let view = store.prepare_fast_confirmation(&[]).unwrap();
        assert!(!FastConfirmationView::equivocating(&view, 0));
        drop(view);
        let mut saw_dependency_gap = false;
        let mut reauthenticated_witness = false;
        for sequence in transcript.sequences {
            let event = restored.repository.original_event(sequence).unwrap();
            let before = restored.graph.snapshot().unwrap();
            restored.replay_original_event(sequence).unwrap_or_else(|error| {
                panic!(
                    "original replay sequence {sequence}, action {:?}, beginning {}, completed {}, prior head slot {}: {error:?}",
                    event.action, event.begin_ms, event.completed_ms, before.head_slot
                )
            });
            let Graph::Classic { store, .. } = &restored.graph else { unreachable!() };
            if matches!(event.action, EventAction::Slashing { .. }) {
                let view = store.prepare_fast_confirmation(&[]).unwrap();
                assert!(FastConfirmationView::equivocating(&view, 0));
                reauthenticated_witness = true;
            }
            if matches!(event.action, EventAction::Block(_)) {
                let dependency = (store.current_slot() / config.slots_per_epoch)
                    .saturating_sub(1)
                    .checked_mul(config.slots_per_epoch)
                    .unwrap()
                    .saturating_sub(1);
                if dependency < seed.head_slot {
                    // Original mandatory signed transition succeeds with the real cut registry
                    // despite an unavailable pre-cut dependency. Exact boost absence is checked
                    // by the native Store regression, whose private kernel exposes that value.
                    saw_dependency_gap = true;
                }
            }
        }
        assert!(reauthenticated_witness);
        assert!(saw_dependency_gap);
        assert_eq!(restored.graph.snapshot().unwrap(), expected);
        let Graph::Classic { store, .. } = &restored.graph else { unreachable!() };
        assert_eq!(store.viable_leaf_weights().unwrap(), weights);
        assert!(
            !StateRef::Classic(&store.read_block_state(expected.head.root).unwrap().unwrap())
                .validators()[0]
                .slashed
        );
        assert!(restored.repository.original_block(competing).unwrap().is_none());
        assert_eq!(
            restored.repository.original_observation(&witness).unwrap().as_ref(),
            proof.to_ssz(Preset::Minimal).unwrap()
        );
        assert!(replay_backend.forkchoice_calls.lock().unwrap().is_empty());
        assert!(replay_backend.payload_calls.lock().unwrap().is_empty());
    })
    .await
    .unwrap();
}
