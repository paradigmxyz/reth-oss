//! Real BLS, clock endpoints, pending observations and cache/graph isolation regressions.

use super::*;
use blst::min_pk::SecretKey;
use reth_embedded_beacon_crypto::ETHEREUM_BLS_DST;
use reth_embedded_beacon_fork_choice::gloas::GloasStore;
use reth_embedded_beacon_state_transition::ssz::gloas::PayloadAttestationData;

#[cfg(feature = "spec-tests")]
use reth_embedded_beacon_fork_choice::{gloas::spec_only::SpecStore, StoreParameters};

fn message(store: &GloasStore, keys: &[SecretKey]) -> PayloadAttestationMessage {
    let root = store.get_head().unwrap().root;
    let state = store.block_state(root).unwrap();
    let validator_index =
        gloas_processing::get_ptc(state, state.slot, store.processing_parameters()).unwrap().values
            [0];
    let data = PayloadAttestationData {
        beacon_block_root: root,
        slot: state.slot,
        payload_present: true,
        blob_data_available: true,
    };
    let signing_root = compute_signing_root(
        data.hash_tree_root(Preset::Minimal).unwrap(),
        compute_domain([12, 0, 0, 0], state.fork.current_version, state.genesis_validators_root),
    );
    let signature =
        keys[validator_index as usize].sign(&signing_root, ETHEREUM_BLS_DST, &[]).to_bytes();
    PayloadAttestationMessage { validator_index, data, signature }
}

#[test]
fn real_signature_precedes_cache_commit_and_acceptance_never_mutates_graph_votes() {
    let (store, keys) = crate::gloas_bids::tests::store_and_keys();
    let signed = message(&store, &keys);
    let before = store.snapshot().unwrap();
    let mut invalid = signed.clone();
    invalid.signature = [0; 96];
    let mut seen = SeenPayloadAttestations::new(1).unwrap();
    assert!(matches!(
        seen.validate_message(&store, &invalid, 0),
        Err(PayloadAttestationGossipError::Reject("invalid payload attestation signature"))
    ));
    assert!(seen.validators.is_empty());
    seen.validate_message(&store, &signed, 0).unwrap();
    assert!(matches!(
        seen.validate_message(&store, &invalid, u64::MAX),
        Err(PayloadAttestationGossipError::Ignore(
            "already seen payload attestation from this validator"
        ))
    ));
    assert_eq!(store.snapshot().unwrap(), before);
    assert!(store
        .payload_timeliness_votes(signed.data.beacon_block_root)
        .unwrap()
        .iter()
        .all(Option::is_none));
    assert!(!store.is_payload_verified(signed.data.beacon_block_root));
    seen.retain_from_slot(1);
    assert!(seen.validators.is_empty());
    seen.validate_message(&store, &signed, 6500).unwrap();
    assert_eq!(store.snapshot().unwrap(), before);
}

#[test]
fn clock_edges_known_failed_blocks_and_local_limits_keep_distinct_outcomes() {
    let (store, keys) = crate::gloas_bids::tests::store_and_keys();
    let signed = message(&store, &keys);
    let mut seen = SeenPayloadAttestations::new(1).unwrap();
    assert!(matches!(
        seen.validate_message(&store, &signed, 6501),
        Err(PayloadAttestationGossipError::Ignore(
            "payload attestation is not for the current slot"
        ))
    ));
    assert!(matches!(
        seen.validate_message(&store, &signed, u64::MAX),
        Err(PayloadAttestationGossipError::Overflow)
    ));
    let root = signed.data.beacon_block_root;
    let mut block = store.block_arc(root).unwrap().as_ref().clone();
    block.parent_root = [222; 32];
    let mut unknown = signed;
    unknown.data.beacon_block_root = block.hash_tree_root(Preset::Minimal).unwrap();
    assert!(matches!(
        seen.validate_message(&store, &unknown, 0),
        Err(PayloadAttestationGossipError::Ignore("payload attestation's block has not been seen"))
    ));
    seen.observe_block(&block, Preset::Minimal).unwrap();
    assert!(matches!(
        seen.validate_message(&store, &unknown, 0),
        Err(PayloadAttestationGossipError::Reject("payload attestation's block failed validation"))
    ));
    block.parent_root = [223; 32];
    assert!(matches!(
        seen.observe_block(&block, Preset::Minimal),
        Err(PayloadAttestationGossipError::ResourceLimit)
    ));
    assert_eq!(seen.observed.len(), 1);
    assert!(seen.validators.is_empty());
}

#[cfg(feature = "spec-tests")]
#[test]
fn future_fixture_activation_rejects_pre_fork_ptc_without_changing_runtime_policy() {
    let (runtime, keys) = crate::gloas_bids::tests::store_and_keys();
    let signed = message(&runtime, &keys);
    let root = signed.data.beacon_block_root;
    let mut processing = runtime.processing_parameters().clone();
    processing.fork_epoch = 1;
    let fixture = SpecStore::get_store(
        runtime.block_state(root).unwrap().clone(),
        runtime.block_arc(root).unwrap().as_ref().clone(),
        StoreParameters::for_preset(Preset::Minimal),
        processing,
    )
    .unwrap();
    let mut seen = spec_only::SpecSeenPayloadAttestations::new(1).unwrap();
    assert!(matches!(
        seen.validate_message(&fixture, &signed, 0),
        Err(PayloadAttestationGossipError::Reject("payload attestation's slot is pre-gloas"))
    ));
    SeenPayloadAttestations::new(1).unwrap().validate_message(&runtime, &signed, 0).unwrap();
}
#[cfg(feature = "spec-tests")]
#[test]
fn declared_gossip_finality_affects_only_propagation_timing_and_grants_no_graph_checkpoint() {
    let (runtime, keys) = crate::gloas_bids::tests::store_and_keys();
    let signed = crate::gloas_bids::tests::envelope(&runtime, &keys);
    let root = signed.message.beacon_block_root;
    let mut fixture = SpecStore::get_store(
        runtime.block_state(root).unwrap().clone(),
        runtime.block_arc(root).unwrap().as_ref().clone(),
        StoreParameters::for_preset(Preset::Minimal),
        runtime.processing_parameters().clone(),
    )
    .unwrap();
    let head = fixture.get_head().unwrap();
    let state_root =
        reth_embedded_beacon_fork_choice::gloas::gossip_view::GloasGossipView::block_state(
            &fixture, root,
        )
        .unwrap()
        .hash_tree_root(Preset::Minimal)
        .unwrap();
    fixture
        .fixture_set_gossip_finalized_checkpoint(reth_embedded_beacon_primitives::Checkpoint {
            epoch: 2,
            root: [250; 32],
        })
        .unwrap();
    assert_eq!(fixture.get_head().unwrap(), head);
    assert_eq!(
        reth_embedded_beacon_fork_choice::gloas::gossip_view::GloasGossipView::block_state(
            &fixture, root
        )
        .unwrap()
        .hash_tree_root(Preset::Minimal)
        .unwrap(),
        state_root
    );
    let mut seen = crate::gloas_bids::spec_only::SpecSeenBids::new(1, 16).unwrap();
    assert!(matches!(
        seen.validate_envelope(&fixture, &signed),
        Err(crate::gloas_bids::BidGossipError::Ignore(
            "envelope is from a slot before the latest finalized slot"
        ))
    ));
    assert_eq!(runtime.finalized_checkpoint().epoch, 0);
    assert!(!runtime.is_payload_verified(root));
    assert!(matches!(
        fixture.fixture_set_gossip_finalized_checkpoint(
            reth_embedded_beacon_primitives::Checkpoint { epoch: u64::MAX, root }
        ),
        Err(StoreError::Overflow)
    ));
    assert_eq!(fixture.finalized_checkpoint().epoch, 2);
}
#[test]
fn first_success_binds_ptc_context_before_duplicate_and_rejection_never_binds() {
    let (store, keys) = crate::gloas_bids::tests::store_and_keys();
    let signed = message(&store, &keys);
    let other = crate::gloas_bids::tests::store_with_genesis(&store, [198; 32]);
    let mut invalid = signed.clone();
    invalid.signature = [0; 96];
    let mut seen = SeenPayloadAttestations::new(1).unwrap();
    assert!(seen.validate_message(&store, &invalid, 0).is_err());
    assert!(seen.context.is_none());
    let other_signed = message(&other, &keys);
    seen.validate_message(&other, &other_signed, 0).unwrap();
    assert!(matches!(
        seen.validate_message(&store, &other_signed, 0),
        Err(PayloadAttestationGossipError::Reject(
            "gossip cache belongs to another chain or configuration"
        ))
    ));
    assert!(matches!(
        seen.validate_message(&other, &other_signed, 0),
        Err(PayloadAttestationGossipError::Ignore(
            "already seen payload attestation from this validator"
        ))
    ));
    assert_eq!(seen.validators.len(), 1);
    seen.retain_from_slot(1);
    assert!(matches!(
        seen.validate_message(&store, &signed, 0),
        Err(PayloadAttestationGossipError::Reject(
            "gossip cache belongs to another chain or configuration"
        ))
    ));
    assert!(seen.validators.is_empty());
}
