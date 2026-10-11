//! Compare strict scalar and prepared body vote handling using real aggregate BLS evidence.

use super::{
    super::tests::{child_with_attestations, signing_root, store_and_keys, DST},
    *,
};
use blst::min_pk::AggregateSignature;
use reth_embedded_beacon_primitives::BitVector;
use reth_embedded_beacon_state_transition::ssz::{phase0, PresetBits, ProgressiveBitList};

pub(crate) fn retained_vote_graph() -> (GloasStore, Root) {
    let (mut graph, keys) = store_and_keys();
    let anchor = graph.get_head().unwrap().root;
    let state = graph.block_state(anchor).unwrap();
    let committee = gloas_processing::get_beacon_committee(state, 0, 0, &graph.processing).unwrap();
    assert!(!committee.is_empty());
    let data = phase0::AttestationData {
        slot: 0,
        index: 0,
        beacon_block_root: anchor,
        source: state.current_justified_checkpoint,
        target: Checkpoint { epoch: 0, root: anchor },
    };
    let signing = signing_root(state, data.hash_tree_root(Preset::Minimal).unwrap(), [1, 0, 0, 0]);
    let signatures = committee
        .iter()
        .map(|index| keys[*index as usize].sign(&signing, DST, &[]))
        .collect::<Vec<_>>();
    let references = signatures.iter().collect::<Vec<_>>();
    let signature = AggregateSignature::aggregate(&references, true).unwrap().to_signature();
    let mut bits = vec![0u8; (committee.len() + 1).div_ceil(8)];
    for index in 0..=committee.len() {
        bits[index / 8] |= 1 << (index % 8);
    }
    let vote = gloas::Attestation {
        aggregation_bits: ProgressiveBitList::from_ssz(&bits, Preset::Minimal).unwrap(),
        data,
        signature: signature.to_bytes(),
        committee_bits: PresetBits::from_ssz(&[1], Preset::Minimal).unwrap(),
    };
    let (signed, _) =
        child_with_attestations(&graph, &keys, vec![vote.clone(), vote.clone(), vote]);
    graph.on_tick(6000).unwrap();
    graph.on_block(&signed).unwrap();
    let root = signed.message.hash_tree_root(Preset::Minimal).unwrap();
    assert!(graph.latest_messages.is_empty());
    (graph, root)
}

/// Inject bad retained evidence after admitting a complete valid signed block. This exercises
/// reauthentication rather than granting validity to the corrupted bytes or saved vote metadata.
pub(crate) fn corrupt_middle_signature(graph: &mut GloasStore, root: Root) {
    Arc::make_mut(graph.blocks.get_mut(&root).unwrap()).body.attestations.values[1].signature =
        [0; 96];
}
pub(crate) fn corrupt_last_shape(graph: &mut GloasStore, root: Root) {
    Arc::make_mut(graph.blocks.get_mut(&root).unwrap()).body.attestations.values[2]
        .committee_bits
        .bits = BitVector::from_ssz(&[0], 8).unwrap();
}

#[test]
fn prepared_body_votes_match_scalar_bls_rejections_and_vote_order() {
    let (mut original, root) = retained_vote_graph();
    corrupt_middle_signature(&mut original, root);
    let mut scalar = original.clone();
    let expected = (0..3)
        .map(|index| (index, scalar.on_retained_block_attestation(root, index)))
        .collect::<Vec<_>>();
    let mut batch = original.clone();
    let actual = batch.on_retained_block_attestations(root).unwrap();
    assert_eq!(format!("{actual:?}"), format!("{expected:?}"));
    assert!(actual[0].1.is_ok());
    assert!(matches!(&actual[1].1, Err(StoreError::Processing(ProcessingError::Crypto(_)))));
    assert!(actual[2].1.is_ok());
    assert_eq!(batch.latest_messages, scalar.latest_messages);
    assert_eq!(batch.equivocating, scalar.equivocating);
    assert_eq!(batch.checkpoints(), scalar.checkpoints());
    assert_eq!(batch.snapshot().unwrap(), scalar.snapshot().unwrap());
    assert!(original.latest_messages.is_empty());
}

#[test]
fn prepared_body_votes_preserve_prior_success_before_later_fatal_shape() {
    let (mut original, root) = retained_vote_graph();
    corrupt_middle_signature(&mut original, root);
    corrupt_last_shape(&mut original, root);
    let mut scalar = original.clone();
    scalar.on_retained_block_attestation(root, 0).unwrap();
    assert!(matches!(
        scalar.on_retained_block_attestation(root, 1),
        Err(StoreError::Processing(ProcessingError::Crypto(_)))
    ));
    let fatal = scalar.on_retained_block_attestation(root, 2).unwrap_err();
    let mut batch = original.clone();
    assert_eq!(
        format!("{:?}", batch.on_retained_block_attestations(root).unwrap_err()),
        format!("{fatal:?}")
    );
    assert!(!batch.latest_messages.is_empty());
    assert_eq!(batch.latest_messages, scalar.latest_messages);
    assert_eq!(batch.snapshot().unwrap(), scalar.snapshot().unwrap());
}
