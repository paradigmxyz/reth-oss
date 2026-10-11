//! Native gossip cache commits follow actual strict import and graph publication.

use super::*;
use blst::min_pk::AggregateSignature;
use reth_embedded_beacon_network::{
    gossip::{GossipKind, GossipTopic},
    transport::GossipVerdict,
};
use reth_embedded_beacon_state_transition::{
    phase0_processing,
    ssz::{auxiliary, phase0},
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_invalid_signature_never_poison_valid_gossip_or_publish_a_head() {
    let directory = TempDir::new().unwrap();
    let (store, keys, parameters) = store_and_keys(Fork::Phase0);
    let signed = phase0_signed_child(&store, &keys, &parameters);
    let (setup, backend) = setup(&directory, &store, &parameters);
    let config = setup.config.clone();
    tokio::task::spawn_blocking(move || {
        let mut worker = Worker::new(setup, Ok(0)).unwrap();
        let now = config.seconds_per_slot * 1000;
        worker.advance(now).unwrap();
        let original = worker.graph.snapshot().unwrap();
        let mut invalid = signed.clone();
        invalid.signature = [0; 96];
        let invalid =
            WireBlock::from_ssz(invalid.to_ssz(Preset::Minimal).unwrap(), &config).unwrap();
        assert!(matches!(
            worker.import(Input::GossipBlock(invalid), now),
            Err(ImportError::GossipReject("invalid proposer signature"))
        ));
        assert_eq!(worker.graph.snapshot().unwrap(), original);
        assert_eq!(worker.repository.stats().unwrap().retained_states, 1);
        let wire = WireBlock::from_ssz(signed.to_ssz(Preset::Minimal).unwrap(), &config).unwrap();
        let receipt = worker.import(Input::GossipBlock(wire.clone()), now).unwrap();
        assert_eq!(receipt.admitted_root, wire.root());
        assert_eq!(receipt.snapshot.head.root, wire.root());
        assert_eq!(
            worker.repository.original_block(wire.root()).unwrap().unwrap().1.as_ref(),
            wire.ssz().as_ref()
        );
        assert!(matches!(
            worker.import(Input::GossipBlock(wire), now),
            Err(ImportError::GossipIgnore(
                "block is not the first valid block for this slot and proposer"
            ))
        ));
        assert_eq!(worker.repository.stats().unwrap().retained_states, 2);
        assert!(backend.payload_calls.lock().unwrap().is_empty());
        assert!(backend.forkchoice_calls.lock().unwrap().is_empty());
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_signature_with_wrong_post_state_cannot_poison_same_proposer_slot() {
    let directory = TempDir::new().unwrap();
    let (store, keys, parameters) = store_and_keys(Fork::Phase0);
    let signed = phase0_signed_child(&store, &keys, &parameters);
    let parent = store.block_state(store.get_head().unwrap().root).unwrap();
    let BeaconState::Phase0(parent, _) = parent else { panic!("real Phase0 parent") };
    let mut inconsistent = signed.clone();
    inconsistent.message.state_root = [201; 32];
    inconsistent.signature = keys[inconsistent.message.proposer_index as usize]
        .sign(
            &signing_root(
                inconsistent.message.hash_tree_root(Preset::Minimal).unwrap(),
                [0; 4],
                parent.fork.current_version,
                parent.genesis_validators_root,
            ),
            ETHEREUM_BLS_DST,
            &[],
        )
        .to_bytes();
    let (setup, _) = setup(&directory, &store, &parameters);
    let config = setup.config.clone();
    tokio::task::spawn_blocking(move || {
        let mut worker = Worker::new(setup, Ok(0)).unwrap();
        let now = config.seconds_per_slot * 1000;
        worker.advance(now).unwrap();
        let original = worker.graph.snapshot().unwrap();
        let invalid =
            WireBlock::from_ssz(inconsistent.to_ssz(Preset::Minimal).unwrap(), &config).unwrap();
        // The proposal is genuinely signed, so gossip alone cannot detect its incorrect state.
        worker.validate_block_gossip(&invalid, now).unwrap();
        assert!(matches!(
            worker.import(Input::GossipBlock(invalid), now),
            Err(ImportError::Consensus(_))
        ));
        assert_eq!(worker.graph.snapshot().unwrap(), original);
        assert_eq!(worker.repository.current_snapshot().unwrap().unwrap().head(), original.head);
        assert_eq!(worker.repository.stats().unwrap().retained_states, 1);
        let wire = WireBlock::from_ssz(signed.to_ssz(Preset::Minimal).unwrap(), &config).unwrap();
        let receipt = worker.import(Input::GossipBlock(wire.clone()), now).unwrap();
        assert_eq!(receipt.snapshot.head.root, wire.root());
        assert_eq!(worker.repository.stats().unwrap().retained_states, 2);
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn aggregate_dispatch_requires_all_real_signatures_before_canonical_publication() {
    let directory = TempDir::new().unwrap();
    let (store, keys, parameters) = store_and_keys(Fork::Phase0);
    let aggregate = signed_aggregate(&store, &keys, &parameters);
    let (setup, backend) = setup(&directory, &store, &parameters);
    let config = setup.config.clone();
    tokio::task::spawn_blocking(move || {
        let mut worker = Worker::new(setup, Ok(0)).unwrap();
        let now = config.seconds_per_slot * 1000;
        worker.advance(now).unwrap();
        let original = worker.graph.snapshot().unwrap();
        let Graph::Classic { store, .. } = &worker.graph else { panic!("native classic votes") };
        let before_weights = store.viable_leaf_weights().unwrap();
        let topic = GossipTopic {
            fork: Fork::Phase0,
            digest: config.fork_digest(0).unwrap(),
            kind: GossipKind::Aggregate,
        };
        let mut invalid = aggregate.clone();
        invalid.signature = [0; 96];
        assert_eq!(
            worker.dispatch_gossip(topic, &invalid.to_ssz(Preset::Minimal).unwrap(), now).unwrap(),
            GossipVerdict::Reject
        );
        assert_eq!(worker.graph.snapshot().unwrap(), original);
        assert_eq!(
            worker
                .dispatch_gossip(topic, &aggregate.to_ssz(Preset::Minimal).unwrap(), now)
                .unwrap(),
            GossipVerdict::Accept
        );
        let Graph::Classic { store, .. } = &worker.graph else { panic!("native classic votes") };
        let after_weights = store.viable_leaf_weights().unwrap();
        assert!(after_weights[&original.head.root] > before_weights[&original.head.root]);
        assert_eq!(
            worker
                .dispatch_gossip(topic, &aggregate.to_ssz(Preset::Minimal).unwrap(), now)
                .unwrap(),
            GossipVerdict::Ignore
        );
        assert_eq!(worker.graph.snapshot().unwrap().head, original.head);
        assert_eq!(worker.repository.current_snapshot().unwrap().unwrap().head(), original.head);
        assert!(backend.payload_calls.lock().unwrap().is_empty());
        assert!(backend.forkchoice_calls.lock().unwrap().is_empty());
    })
    .await
    .unwrap();
}

pub(super) fn signed_aggregate(
    store: &Store,
    keys: &[SecretKey],
    parameters: &TransitionParameters,
) -> auxiliary::phase0::SignedAggregateAndProof {
    let head = store.get_head().unwrap();
    let BeaconState::Phase0(state, preset) = store.block_state(head.root).unwrap() else {
        panic!("real Phase0 state")
    };
    let committee = phase0_processing::get_beacon_committee(
        state,
        0,
        0,
        &parameters.gloas.fulu.electra.deneb.capella.bellatrix.altair.phase0,
    )
    .unwrap();
    let mut bits = vec![0; committee.len() / 8 + 1];
    for index in 0..committee.len() {
        bits[index / 8] |= 1 << (index % 8);
    }
    bits[committee.len() / 8] |= 1 << (committee.len() % 8);
    let data = reth_embedded_beacon_state_transition::ssz::phase0::AttestationData {
        slot: 0,
        index: 0,
        beacon_block_root: head.root,
        source: store.checkpoints().0,
        target: Checkpoint { epoch: 0, root: head.root },
    };
    let root = signing_root(
        data.hash_tree_root(*preset).unwrap(),
        [1, 0, 0, 0],
        state.fork.current_version,
        state.genesis_validators_root,
    );
    let signatures = committee
        .iter()
        .map(|&index| keys[index as usize].sign(&root, ETHEREUM_BLS_DST, &[]))
        .collect::<Vec<_>>();
    let references = signatures.iter().collect::<Vec<_>>();
    let aggregate = phase0::Attestation {
        aggregation_bits: phase0::AggregationBits::from_ssz(&bits, *preset).unwrap(),
        data,
        signature: AggregateSignature::aggregate(&references, true)
            .unwrap()
            .to_signature()
            .to_bytes(),
    };
    let proof_root = signing_root(
        uint64_root(0),
        [5, 0, 0, 0],
        state.fork.current_version,
        state.genesis_validators_root,
    );
    let message = auxiliary::phase0::AggregateAndProof {
        aggregator_index: committee[0],
        aggregate,
        selection_proof: keys[committee[0] as usize]
            .sign(&proof_root, ETHEREUM_BLS_DST, &[])
            .to_bytes(),
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
