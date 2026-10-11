//! Real competing-branch slashing proofs replay from original SSZ, with strict owner loss.
use super::*;
use crate::compact_persistence::events::EventAction;
use blst::min_pk::AggregateSignature;
use reth_embedded_beacon_network::gossip::GossipKind;
use reth_embedded_beacon_primitives::sha256;
use reth_embedded_beacon_shared_data::{Encoding, ObjectId, ObjectKind, Owner};
use reth_embedded_beacon_state_transition::{
    phase0_processing,
    ssz::{auxiliary, phase0},
};

pub(super) fn signed_pair(
    store: &Store,
    keys: &[SecretKey],
    validator: u64,
) -> phase0::AttesterSlashing {
    signed_pair_at_epoch(store, keys, validator, 0)
}
fn signed_pair_at_epoch(
    store: &Store,
    keys: &[SecretKey],
    validator: u64,
    target_epoch: u64,
) -> phase0::AttesterSlashing {
    let anchor = store.get_head().unwrap().root;
    let state = store.block_state(anchor).unwrap();
    let data = phase0::AttestationData {
        slot: 0,
        index: 0,
        beacon_block_root: anchor,
        source: store.checkpoints().0,
        target: Checkpoint { epoch: target_epoch, root: anchor },
    };
    let indexed = |data: phase0::AttestationData| {
        let root = signing_root(
            data.hash_tree_root(Preset::Minimal).unwrap(),
            [1, 0, 0, 0],
            state.fork_info().current_version,
            state.genesis_validators_root(),
        );
        phase0::IndexedAttestation {
            attesting_indices: phase0::AttestingIndices::from_vec(vec![validator]),
            data,
            signature: keys[validator as usize].sign(&root, ETHEREUM_BLS_DST, &[]).to_bytes(),
        }
    };
    let mut other = data.clone();
    other.beacon_block_root = [77; 32];
    phase0::AttesterSlashing { attestation_1: indexed(data), attestation_2: indexed(other) }
}
pub(super) fn modified_child(
    store: &Store,
    keys: &[SecretKey],
    parameters: &TransitionParameters,
    proof: Option<phase0::AttesterSlashing>,
    graffiti: u8,
) -> phase0::SignedBeaconBlock {
    let mut signed = phase0_signed_child(store, keys, parameters);
    signed.message.body.graffiti = [graffiti; 32];
    if let Some(proof) = proof {
        signed.message.body.attester_slashings.values.push(proof);
    }
    let parent = store.block_state(store.get_head().unwrap().root).unwrap();
    let BeaconState::Phase0(mut state, preset) =
        fork_processing::replay_slots(parent, 1, parameters, 1).unwrap()
    else {
        unreachable!()
    };
    phase0_processing::process_block(
        &mut state,
        &signed.message,
        &parameters.gloas.fulu.electra.deneb.capella.bellatrix.altair.phase0,
    )
    .unwrap();
    signed.message.state_root = state.hash_tree_root(preset).unwrap();
    signed.signature = keys[signed.message.proposer_index as usize]
        .sign(
            &signing_root(
                signed.message.hash_tree_root(preset).unwrap(),
                [0; 4],
                state.fork.current_version,
                state.genesis_validators_root,
            ),
            ETHEREUM_BLS_DST,
            &[],
        )
        .to_bytes();
    signed
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn genuine_competing_branch_original_slashing_replays_without_flags_and_missing_pin_rejects()
{
    let directory = TempDir::new().unwrap();
    let (producer, keys, parameters) = store_and_keys(Fork::Phase0);
    let BeaconState::Phase0(state, _) =
        producer.block_state(producer.get_head().unwrap().root).unwrap()
    else {
        unreachable!()
    };
    let participant = phase0_processing::get_beacon_committee(
        state,
        1,
        0,
        &parameters.gloas.fulu.electra.deneb.capella.bellatrix.altair.phase0,
    )
    .unwrap()[0];
    let proof = signed_pair(&producer, &keys, participant);
    let slashed = modified_child(&producer, &keys, &parameters, Some(proof.clone()), 12);
    let slashed_root = slashed.message.hash_tree_root(Preset::Minimal).unwrap();
    let clean = (0..=255)
        .map(|value| modified_child(&producer, &keys, &parameters, None, value))
        .find(|child| child.message.hash_tree_root(Preset::Minimal).unwrap() > slashed_root)
        .expect("genuine competing clean branch with native lexical selection");
    let mut voted_branch = producer.clone();
    voted_branch
        .on_tick(parameters.gloas.fulu.electra.deneb.capella.bellatrix.seconds_per_slot * 1000)
        .unwrap();
    voted_branch.on_block(&SignedBeaconBlock::Phase0(clean.clone(), Preset::Minimal)).unwrap();
    let aggregate = branch_aggregate(&voted_branch, &keys, &parameters, 1);
    let expected_weight = phase0_processing::get_beacon_committee(
        state,
        1,
        0,
        &parameters.gloas.fulu.electra.deneb.capella.bellatrix.altair.phase0,
    )
    .unwrap()
    .iter()
    .filter(|index| **index != participant)
    .map(|index| state.validators.values[*index as usize].effective_balance as u128)
    .sum::<u128>();
    assert!(expected_weight > 0);
    let (initial, backend) = setup(&directory, &producer, &parameters);
    let config = initial.config.clone();
    tokio::task::spawn_blocking(move || {
        let mut worker = Worker::new(initial, Ok(0)).unwrap();
        let now = config.seconds_per_slot * 1000;
        worker.advance(now).unwrap();
        let topic = GossipTopic {
            fork: Fork::Phase0,
            digest: config.fork_digest(0).unwrap(),
            kind: GossipKind::Aggregate,
        };

        let a = WireBlock::from_ssz(slashed.to_ssz(Preset::Minimal).unwrap(), &config).unwrap();
        let b = WireBlock::from_ssz(clean.to_ssz(Preset::Minimal).unwrap(), &config).unwrap();
        worker.import(Input::Block(a), 0).unwrap();
        worker.import(Input::Block(b.clone()), 0).unwrap();
        let OwnerClock::Controlled(clock) = &worker.clock else { unreachable!() };
        clock.store(2 * now, Ordering::Release);
        worker.advance(2 * now).unwrap();
        assert_eq!(
            worker
                .dispatch_gossip(topic, &aggregate.to_ssz(Preset::Minimal).unwrap(), 2 * now)
                .unwrap(),
            GossipVerdict::Accept
        );
        let expected = worker.graph.snapshot().unwrap();
        assert_eq!(expected.head.root, b.root());
        let Graph::Classic { store, .. } = &worker.graph else { unreachable!() };
        assert!(
            !StateRef::Classic(&store.read_block_state(b.root()).unwrap().unwrap()).validators()
                [participant as usize]
                .slashed
        );
        let weights = store.viable_leaf_weights().unwrap();
        assert_eq!(weights[&b.root()], expected_weight);
        let transcript = worker.repository.replay_transcript().unwrap().unwrap();
        let witness = transcript
            .sequences
            .iter()
            .map(|id| worker.repository.original_event(*id).unwrap())
            .find(|event| matches!(event.action, EventAction::Slashing { .. }))
            .expect("original successful native proof");
        assert_eq!(
            worker.repository.original_observation(&witness).unwrap().as_ref(),
            proof.to_ssz(Preset::Minimal).unwrap()
        );
        assert!(backend.forkchoice_calls.lock().unwrap().is_empty());
        drop(worker);
        let (restored, replay_backend) = setup(&directory, &producer, &parameters);
        let mut restored = Worker::initialize(restored, Ok(2 * now), true, None).unwrap();
        for sequence in &transcript.sequences {
            restored.replay_original_event(*sequence).unwrap();
        }
        assert_eq!(restored.graph.snapshot().unwrap(), expected);
        let Graph::Classic { store, .. } = &restored.graph else { unreachable!() };
        assert_eq!(store.viable_leaf_weights().unwrap(), weights);
        assert!(replay_backend.forkchoice_calls.lock().unwrap().is_empty());
        // Revalidate the original witness again: corruption fails before any kernel mutation.
        let mut broken = proof.clone();
        broken.attestation_1.signature = [0; 96];
        let mut graph = restored.graph.clone();
        let Graph::Classic { store, .. } = &mut graph else { unreachable!() };
        assert!(store
            .on_original_attester_slashing(
                &broken.to_ssz(Preset::Minimal).unwrap(),
                Fork::Phase0,
                &parameters,
                0,
            )
            .is_err());
        assert_eq!(store.viable_leaf_weights().unwrap(), weights);
        let EventAction::Slashing { original, .. } = witness.action else { unreachable!() };
        drop(restored);
        drop(graph);
        let network = PersistenceNetwork::from_config(config).unwrap();
        let shared = SharedDataStore::open(directory.path(), network.identity(), 0).unwrap();
        let independent = Owner::Pool([89; 32]);
        let bytes = proof.to_ssz(Preset::Minimal).unwrap();
        assert_eq!(
            shared.put(ObjectKind::BeaconObservation, Encoding::Ssz, &bytes, independent).unwrap(),
            original
        );
        let mut owner = b"beacon-original-observation-v1".to_vec();
        owner.extend_from_slice(&network.identity().config_digest);
        owner.extend_from_slice(&witness.sequence.to_le_bytes());
        shared.release(Owner::Recovery(sha256(&owner)), original).unwrap();
        assert_eq!(
            shared.owner_objects(independent),
            vec![ObjectId::new(ObjectKind::BeaconObservation, Encoding::Ssz, &bytes)]
        );
        assert!(
            AdmittedStateRepository::open(shared, network, CompactStateLimits::default()).is_err()
        );
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_future_target_slashing_replays_original_verification_fork_info() {
    let directory = TempDir::new().unwrap();
    let (producer, keys, parameters, _) = phase0_gloas_boundary();
    let proof = signed_pair_at_epoch(&producer, &keys, 0, 1);
    let signed = modified_child(&producer, &keys, &parameters, Some(proof.clone()), 29);
    let (initial, backend) = setup(&directory, &producer, &parameters);
    let config = initial.config.clone();
    tokio::task::spawn_blocking(move || {
        let mut worker = Worker::new(initial, Ok(0)).unwrap();
        let now = config.seconds_per_slot * 1000;
        worker.advance(now).unwrap();
        let wire = WireBlock::from_ssz(signed.to_ssz(Preset::Minimal).unwrap(), &config).unwrap();
        worker.import(Input::Block(wire), 0).unwrap();
        let expected = worker.graph.snapshot().unwrap();
        let transcript = worker.repository.replay_transcript().unwrap().unwrap();
        let witness = transcript
            .sequences
            .iter()
            .map(|id| worker.repository.original_event(*id).unwrap())
            .find(|event| matches!(event.action, EventAction::Slashing { .. }))
            .unwrap();
        assert!(matches!(witness.action, EventAction::Slashing { verification_slot: 0, .. }));
        // The exact admitted proof targets epoch one, whose configured active schema is Gloas;
        // the actual native verification state is Phase0. A target-epoch lookup changes its BLS
        // domain and incorrectly rejects this signed body after restart.
        assert_eq!(config.fork_at_slot(config.slots_per_epoch).unwrap().fork, Fork::Gloas);
        assert!(backend.payload_calls.lock().unwrap().is_empty());
        assert!(backend.forkchoice_calls.lock().unwrap().is_empty());
        drop(worker);
        let (restored, replay_backend) = setup(&directory, &producer, &parameters);
        let mut restored = Worker::initialize(restored, Ok(now), true, None).unwrap();
        for sequence in transcript.sequences {
            restored.replay_original_event(sequence).unwrap();
        }
        assert_eq!(restored.graph.snapshot().unwrap(), expected);
        assert!(replay_backend.forkchoice_calls.lock().unwrap().is_empty());
        let Graph::Classic { store, .. } = &mut restored.graph else { unreachable!() };
        assert!(matches!(
            store.on_original_attester_slashing(
                &proof.to_ssz(Preset::Minimal).unwrap(),
                Fork::Phase0,
                &parameters,
                config.slots_per_epoch
            ),
            Err(StoreError::InvalidParameters)
        ));
    })
    .await
    .unwrap();
}

fn branch_aggregate(
    store: &Store,
    keys: &[SecretKey],
    parameters: &TransitionParameters,
    slot: u64,
) -> auxiliary::phase0::SignedAggregateAndProof {
    let head = store.get_head().unwrap();
    let BeaconState::Phase0(state, preset) = store.block_state(head.root).unwrap() else {
        unreachable!()
    };
    let committee = phase0_processing::get_beacon_committee(
        state,
        slot,
        0,
        &parameters.gloas.fulu.electra.deneb.capella.bellatrix.altair.phase0,
    )
    .unwrap();
    let mut bits = vec![0; (committee.len() + 1).div_ceil(8)];
    for index in 0..=committee.len() {
        bits[index / 8] |= 1 << (index % 8);
    }
    let data = phase0::AttestationData {
        slot,
        index: 0,
        beacon_block_root: head.root,
        source: store.checkpoints().0,
        target: Checkpoint { epoch: 0, root: state.block_roots.values[0] },
    };
    let message_root = signing_root(
        data.hash_tree_root(*preset).unwrap(),
        [1, 0, 0, 0],
        state.fork.current_version,
        state.genesis_validators_root,
    );
    let signatures = committee
        .iter()
        .map(|index| keys[*index as usize].sign(&message_root, ETHEREUM_BLS_DST, &[]))
        .collect::<Vec<_>>();
    let signature = AggregateSignature::aggregate(&signatures.iter().collect::<Vec<_>>(), true)
        .unwrap()
        .to_signature()
        .to_bytes();
    let proof = signing_root(
        uint64_root(slot),
        [5, 0, 0, 0],
        state.fork.current_version,
        state.genesis_validators_root,
    );
    let message = auxiliary::phase0::AggregateAndProof {
        aggregator_index: committee[0],
        aggregate: phase0::Attestation {
            aggregation_bits: phase0::AggregationBits::from_ssz(&bits, *preset).unwrap(),
            data,
            signature,
        },
        selection_proof: keys[committee[0] as usize].sign(&proof, ETHEREUM_BLS_DST, &[]).to_bytes(),
    };
    let root = signing_root(
        message.hash_tree_root(*preset).unwrap(),
        [6, 0, 0, 0],
        state.fork.current_version,
        state.genesis_validators_root,
    );
    auxiliary::phase0::SignedAggregateAndProof {
        message,
        signature: keys[committee[0] as usize].sign(&root, ETHEREUM_BLS_DST, &[]).to_bytes(),
    }
}
