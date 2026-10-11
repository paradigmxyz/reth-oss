//! Real proposer signatures, cache atomicity and gossip classification regressions.

use super::*;
use blst::min_pk::SecretKey;
use reth_embedded_beacon_crypto::ETHEREUM_BLS_DST;
use reth_embedded_beacon_fork_choice::{gloas::GloasStore, StoreParameters};
use reth_embedded_beacon_primitives::{BeaconBlockHeader, Checkpoint};
use reth_embedded_beacon_state_transition::{
    altair_processing, bellatrix_processing, capella_processing, deneb_processing,
    electra_processing, fulu_processing,
    ssz::{gloas, phase0, Bits, List, PresetBits, ProgressiveList, Ssz, Vector},
};

#[cfg(feature = "spec-tests")]
use reth_embedded_beacon_fork_choice::gloas::spec_only::SpecStore;

fn requests() -> gloas::ExecutionRequests {
    gloas::ExecutionRequests {
        deposits: ProgressiveList::default(),
        withdrawals: ProgressiveList::default(),
        consolidations: ProgressiveList::default(),
        builder_deposits: ProgressiveList::default(),
        builder_exits: ProgressiveList::default(),
    }
}
fn body(state: &gloas::BeaconState) -> gloas::BeaconBlockBody {
    let mut infinity = [0; 96];
    infinity[0] = 0xc0;
    gloas::BeaconBlockBody {
        randao_reveal: [0; 96],
        eth1_data: state.eth1_data.clone(),
        graffiti: [0; 32],
        proposer_slashings: ProgressiveList::default(),
        attester_slashings: ProgressiveList::default(),
        attestations: ProgressiveList::default(),
        deposits: ProgressiveList::default(),
        voluntary_exits: ProgressiveList::default(),
        sync_aggregate: gloas::SyncAggregate {
            sync_committee_bits: PresetBits::from_ssz(&[0; 4], Preset::Minimal).unwrap(),
            sync_committee_signature: infinity,
        },
        bls_to_execution_changes: ProgressiveList::default(),
        signed_execution_payload_bid: gloas::SignedExecutionPayloadBid {
            message: state.latest_execution_payload_bid.clone(),
            signature: infinity,
        },
        payload_attestations: ProgressiveList::default(),
        parent_execution_requests: requests(),
    }
}
pub(crate) fn store_and_keys() -> (GloasStore, Vec<SecretKey>) {
    let keys =
        (1u8..=32).map(|index| SecretKey::key_gen(&[index; 32], &[]).unwrap()).collect::<Vec<_>>();
    let validators = keys
        .iter()
        .map(|key| phase0::Validator {
            pubkey: key.sk_to_pk().to_bytes(),
            withdrawal_credentials: [0; 32],
            effective_balance: 32_000_000_000,
            slashed: false,
            activation_eligibility_epoch: 0,
            activation_epoch: 0,
            exit_epoch: u64::MAX,
            withdrawable_epoch: u64::MAX,
        })
        .collect();
    let pre = phase0::BeaconState {
        genesis_time: 0,
        genesis_validators_root: [1; 32],
        slot: 0,
        fork: phase0::ForkInfo {
            previous_version: [0, 0, 0, 1],
            current_version: [0, 0, 0, 1],
            epoch: 0,
        },
        latest_block_header: BeaconBlockHeader::default(),
        block_roots: Vector::from_vec(vec![[0; 32]; 64]),
        state_roots: Vector::from_vec(vec![[0; 32]; 64]),
        historical_roots: List::default(),
        eth1_data: phase0::Eth1Data {
            deposit_root: [0; 32],
            deposit_count: 0,
            block_hash: [0; 32],
        },
        eth1_data_votes: List::default(),
        eth1_deposit_index: 0,
        validators: List::from_vec(validators),
        balances: List::from_vec(vec![32_000_000_000; 32]),
        randao_mixes: Vector::from_vec(vec![[0; 32]; 64]),
        slashings: Vector::from_vec(vec![0; 64]),
        previous_epoch_attestations: List::default(),
        current_epoch_attestations: List::default(),
        justification_bits: Bits::from_ssz(&[0], Preset::Minimal).unwrap(),
        previous_justified_checkpoint: Checkpoint::default(),
        current_justified_checkpoint: Checkpoint::default(),
        finalized_checkpoint: Checkpoint::default(),
    };
    let mut parameters = GloasParameters::for_preset(Preset::Minimal);
    parameters.fork_epoch = 0;
    parameters.fulu.fork_epoch = 0;
    parameters.fulu.electra_fork_epoch = 0;
    let a = &parameters.fulu.electra.deneb.capella.bellatrix.altair;
    let a = altair_processing::upgrade_to_altair(&pre, a).unwrap();
    let b = bellatrix_processing::upgrade_to_bellatrix(
        &a,
        &parameters.fulu.electra.deneb.capella.bellatrix,
    )
    .unwrap();
    let c =
        capella_processing::upgrade_to_capella(&b, &parameters.fulu.electra.deneb.capella).unwrap();
    let d = deneb_processing::upgrade_to_deneb(&c, &parameters.fulu.electra.deneb).unwrap();
    let e = electra_processing::upgrade_to_electra(&d, &parameters.fulu.electra).unwrap();
    let f = fulu_processing::upgrade_to_fulu(&e, &parameters.fulu).unwrap();
    let mut state = gloas_processing::upgrade_to_gloas(&f, &parameters).unwrap();
    state.latest_block_hash = [9; 32];
    state.latest_execution_payload_bid.block_hash = [10; 32];
    state.latest_execution_payload_bid.parent_block_hash = [9; 32];
    state.latest_execution_payload_bid.gas_limit = 30_000_000;
    state.latest_execution_payload_bid.execution_requests_root =
        requests().hash_tree_root(Preset::Minimal).unwrap();
    let body = body(&state);
    state.latest_block_header.body_root = body.hash_tree_root(Preset::Minimal).unwrap();
    let block = gloas::BeaconBlock {
        slot: 0,
        proposer_index: 0,
        parent_root: [0; 32],
        state_root: state.hash_tree_root(Preset::Minimal).unwrap(),
        body,
    };
    (
        GloasStore::get_store(
            state,
            block,
            StoreParameters::for_preset(Preset::Minimal),
            parameters,
        )
        .unwrap(),
        keys,
    )
}

fn preferences(store: &GloasStore, keys: &[SecretKey], slot: u64) -> SignedProposerPreferences {
    let root = store.get_head().unwrap().root;
    let state = store.block_state(root).unwrap();
    let validator_index = state.proposer_lookahead.values[slot as usize];
    let message = ProposerPreferences {
        dependent_root: root,
        proposal_slot: slot,
        validator_index,
        fee_recipient: [8; 20],
        target_gas_limit: 30_000_000,
    };
    let root = compute_signing_root(
        message.hash_tree_root(Preset::Minimal).unwrap(),
        compute_domain([13, 0, 0, 0], state.fork.current_version, state.genesis_validators_root),
    );
    let signature = keys[validator_index as usize].sign(&root, ETHEREUM_BLS_DST, &[]).to_bytes();
    SignedProposerPreferences { message, signature }
}

#[test]
fn actual_proposer_signature_precedes_cache_commit_and_invalid_message_cannot_poison_identity() {
    let (store, keys) = store_and_keys();
    let valid = preferences(&store, &keys, 1);
    let mut invalid = valid.clone();
    invalid.signature = [0; 96];
    let mut seen = SeenBids::new(1, 16).unwrap();
    assert!(matches!(
        seen.validate_preferences(&store, &invalid, 0),
        Err(BidGossipError::Reject("invalid proposer preferences signature"))
    ));
    assert!(seen.preferences.is_empty());
    seen.validate_preferences(&store, &valid, 0).unwrap();
    assert!(matches!(
        seen.validate_preferences(&store, &invalid, 0),
        Err(BidGossipError::Ignore(
            "already seen preferences for this dependent root and proposal slot"
        ))
    ));
    let next = preferences(&store, &keys, 2);
    assert!(matches!(
        seen.validate_preferences(&store, &next, 0),
        Err(BidGossipError::ResourceLimit)
    ));
    assert!(!seen.preferences.contains_key(&(2, valid.message.dependent_root)));
    seen.retain_from_slot(2);
    seen.validate_preferences(&store, &next, 0).unwrap();
}

#[test]
fn clock_disparity_and_duplicate_ordering_do_not_reclassify_invalid_input() {
    let (store, keys) = store_and_keys();
    let mut seen = SeenBids::new(8, 16).unwrap();
    let mut value = preferences(&store, &keys, 1);
    value.signature = [0; 96];
    assert!(matches!(
        seen.validate_preferences(&store, &value, 6501),
        Err(BidGossipError::Ignore("proposal slot has already started"))
    ));
    assert!(matches!(
        seen.validate_preferences(&store, &value, 6500),
        Err(BidGossipError::Reject("invalid proposer preferences signature"))
    ));
    assert!(seen.preferences.is_empty());
    assert!(current_or_next_slot(&store, 2, 5500).unwrap());
    assert!(!current_or_next_slot(&store, 2, 5499).unwrap());
    assert!(current_or_next_slot(&store, 1, 12500).unwrap());
    assert!(!current_or_next_slot(&store, 1, 12501).unwrap());
    assert!(matches!(current_or_next_slot(&store, u64::MAX, 0), Err(BidGossipError::Overflow)));
    assert!(matches!(
        seen.record_verified_payload(&store, store.get_head().unwrap().root),
        Err(BidGossipError::Ignore("execution payload envelope has not been seen"))
    ));
    assert!(seen.payloads.is_empty());
}

#[test]
fn gas_target_clamps_exactly_with_no_unsigned_overflow() {
    assert!(is_gas_limit_target_compatible(30_000_000, 29_970_705, 1).unwrap());
    assert!(is_gas_limit_target_compatible(30_000_000, 30_029_295, u64::MAX).unwrap());
    assert!(is_gas_limit_target_compatible(30_000_000, 30_001_001, 30_001_001).unwrap());
    assert!(!is_gas_limit_target_compatible(30_000_000, 29_970_704, 1).unwrap());
    assert!(is_gas_limit_target_compatible(1000, 1000, 1001).unwrap());
    assert!(matches!(
        is_gas_limit_target_compatible(u64::MAX, u64::MAX, 0),
        Err(BidGossipError::Overflow)
    ));
}

#[cfg(feature = "spec-tests")]
#[test]
fn isolated_fixture_context_preserves_real_signatures_and_cannot_poison_runtime_cache() {
    let (runtime, keys) = store_and_keys();
    let root = runtime.get_head().unwrap().root;
    let state = runtime.block_state(root).unwrap().clone();
    let block = runtime.block_arc(root).unwrap().as_ref().clone();
    let mut fixture = SpecStore::get_store(
        state,
        block,
        StoreParameters::for_preset(Preset::Minimal),
        runtime.processing_parameters().clone(),
    )
    .unwrap();
    fixture.fixture_set_finalized_checkpoint(Checkpoint { epoch: 1, root }).unwrap();
    assert_eq!(runtime.checkpoints().1.epoch, 0);
    assert!(matches!(
        fixture.fixture_set_finalized_checkpoint(Checkpoint { epoch: 2, root: [250; 32] }),
        Err(StoreError::UnknownRoot)
    ));
    let signed = preferences(&runtime, &keys, 1);
    let mut invalid = signed.clone();
    invalid.signature = [0; 96];
    let mut seen = spec_only::SpecSeenBids::new(1, 128).unwrap();
    assert!(matches!(
        seen.validate_preferences(&fixture, &invalid, 100),
        Err(BidGossipError::Reject("invalid proposer preferences signature"))
    ));
    seen.validate_preferences(&fixture, &signed, 100).unwrap();
    assert!(matches!(
        seen.validate_preferences(&fixture, &signed, 100),
        Err(BidGossipError::Ignore(
            "already seen preferences for this dependent root and proposal slot"
        ))
    ));
    let mut production_cache = SeenBids::new(1, 128).unwrap();
    production_cache.validate_preferences(&runtime, &signed, 100).unwrap();
    assert_eq!(runtime.checkpoints().1.epoch, 0);
}
pub(crate) fn envelope(store: &GloasStore, keys: &[SecretKey]) -> SignedExecutionPayloadEnvelope {
    let root = store.get_head().unwrap().root;
    let state = store.block_state(root).unwrap();
    let bid = &state.latest_execution_payload_bid;
    let message = gloas::ExecutionPayloadEnvelope {
        payload: gloas::ExecutionPayload {
            parent_hash: state.latest_block_hash,
            fee_recipient: [8; 20],
            state_root: [1; 32],
            receipts_root: [2; 32],
            logs_bloom: Vector::from_vec(vec![0; 256]),
            prev_randao: bid.prev_randao,
            block_number: 1,
            gas_limit: bid.gas_limit + 1,
            gas_used: 0,
            timestamp: 0,
            extra_data: List::default(),
            base_fee_per_gas: [0; 32],
            block_hash: bid.block_hash,
            transactions: ProgressiveList::default(),
            withdrawals: state.payload_expected_withdrawals.clone(),
            blob_gas_used: 0,
            excess_blob_gas: 0,
            block_access_list: ProgressiveList::default(),
            slot_number: state.slot,
        },
        execution_requests: requests(),
        builder_index: bid.builder_index,
        beacon_block_root: root,
        parent_beacon_block_root: state.latest_block_header.parent_root,
    };
    let signing_root = compute_signing_root(
        message.hash_tree_root(Preset::Minimal).unwrap(),
        compute_domain([11, 0, 0, 0], state.fork.current_version, state.genesis_validators_root),
    );
    let signature = keys[state.latest_block_header.proposer_index as usize]
        .sign(&signing_root, ETHEREUM_BLS_DST, &[])
        .to_bytes();
    SignedExecutionPayloadEnvelope { message, signature }
}

#[test]
fn envelope_signature_cache_retention_is_atomic_and_gossip_never_publishes_full() {
    let (store, keys) = store_and_keys();
    let signed = envelope(&store, &keys);
    let root = signed.message.beacon_block_root;
    let mut invalid = signed.clone();
    invalid.signature = [0; 96];
    let mut seen = SeenBids::new(1, 16).unwrap();
    assert!(matches!(
        seen.validate_envelope(&store, &invalid),
        Err(BidGossipError::Reject("invalid envelope signature"))
    ));
    assert!(seen.envelopes.is_empty());
    assert!(seen.payloads.is_empty());
    seen.validate_envelope(&store, &signed).unwrap();
    assert_eq!(seen.payloads.get(&signed.message.payload.block_hash), Some(&(0, 30_000_001)));
    assert!(!store.is_payload_verified(root));
    assert!(matches!(
        seen.validate_envelope(&store, &invalid),
        Err(BidGossipError::Ignore("already seen envelope for this block root from this builder"))
    ));
    seen.retain_from_slot(1);
    assert!(seen.envelopes.is_empty());
    assert!(seen.payloads.is_empty());
    seen.validate_envelope(&store, &signed).unwrap();
    assert!(!store.is_payload_verified(root));
}

#[cfg(feature = "spec-tests")]
#[test]
fn asserted_checkpoint_replay_uses_scalars_without_mutating_retained_state_or_runtime() {
    let (runtime, _) = store_and_keys();
    let root = runtime.get_head().unwrap().root;
    let state = runtime.block_state(root).unwrap().clone();
    let original_checkpoint = state.finalized_checkpoint;
    let original_state_root = state.hash_tree_root(Preset::Minimal).unwrap();
    let mut fixture = SpecStore::get_store(
        state,
        runtime.block_arc(root).unwrap().as_ref().clone(),
        StoreParameters::for_preset(Preset::Minimal),
        runtime.processing_parameters().clone(),
    )
    .unwrap();
    let asserted = Checkpoint { epoch: 0, root };
    fixture.fixture_set_finalized_checkpoint_for_states(asserted, &[root]).unwrap();
    assert_eq!(fixture.replay_finalized_checkpoint(root), Some(asserted));
    assert_eq!(runtime.replay_finalized_checkpoint(root), None);
    let stored =
        reth_embedded_beacon_fork_choice::gloas::gossip_view::GloasGossipView::block_state(
            &fixture, root,
        )
        .unwrap();
    assert_eq!(stored.finalized_checkpoint, original_checkpoint);
    assert_eq!(stored.hash_tree_root(Preset::Minimal).unwrap(), original_state_root);
    assert_eq!(runtime.block_state(root).unwrap().finalized_checkpoint, original_checkpoint);
}

#[cfg(feature = "spec-tests")]
#[test]
fn fixture_future_gossip_epoch_is_separate_from_actual_trusted_anchor_activation() {
    let (runtime, keys) = store_and_keys();
    let root = runtime.get_head().unwrap().root;
    let state = runtime.block_state(root).unwrap().clone();
    let block = runtime.block_arc(root).unwrap().as_ref().clone();
    let mut processing = runtime.processing_parameters().clone();
    processing.fork_epoch = 1;
    assert!(matches!(
        GloasStore::get_store(
            state.clone(),
            block.clone(),
            StoreParameters::for_preset(Preset::Minimal),
            processing.clone(),
        ),
        Err(StoreError::AnchorMismatch)
    ));
    let fixture = SpecStore::get_store(
        state,
        block,
        StoreParameters::for_preset(Preset::Minimal),
        processing,
    )
    .unwrap();
    let signed = preferences(&runtime, &keys, 1);
    let mut seen = spec_only::SpecSeenBids::new(1, 128).unwrap();
    assert!(matches!(
        seen.validate_preferences(&fixture, &signed, 100),
        Err(BidGossipError::Ignore("proposal epoch is pre-gloas"))
    ));
    let mut runtime_seen = SeenBids::new(1, 128).unwrap();
    runtime_seen.validate_preferences(&runtime, &signed, 100).unwrap();
}
#[test]
fn observed_failed_envelope_block_keeps_rejection_separate_from_unknown_prerequisite() {
    let (store, keys) = store_and_keys();
    let mut signed = envelope(&store, &keys);
    let mut block = store.block_arc(signed.message.beacon_block_root).unwrap().as_ref().clone();
    block.parent_root = [225; 32];
    signed.message.beacon_block_root = block.hash_tree_root(Preset::Minimal).unwrap();
    let mut seen = SeenBids::new(1, 16).unwrap();
    assert!(matches!(
        seen.validate_envelope(&store, &signed),
        Err(BidGossipError::Ignore("envelope's block has not been seen"))
    ));
    seen.observe_block(&block, Preset::Minimal).unwrap();
    assert!(matches!(
        seen.validate_envelope(&store, &signed),
        Err(BidGossipError::Reject("envelope's block failed validation"))
    ));
    assert!(seen.envelopes.is_empty());
    assert!(seen.payloads.is_empty());
    seen.retain_from_slot(1);
    assert!(matches!(
        seen.validate_envelope(&store, &signed),
        Err(BidGossipError::Ignore("envelope's block has not been seen"))
    ));
}
pub(crate) fn store_with_genesis(store: &GloasStore, genesis: Root) -> GloasStore {
    let root = store.get_head().unwrap().root;
    let mut state = store.block_state(root).unwrap().clone();
    state.genesis_validators_root = genesis;
    let mut block = store.block_arc(root).unwrap().as_ref().clone();
    block.state_root = state.hash_tree_root(Preset::Minimal).unwrap();
    GloasStore::get_store(
        state,
        block,
        store.store_parameters(),
        store.processing_parameters().clone(),
    )
    .unwrap()
}

#[test]
fn accepted_cache_context_rejects_another_genesis_processing_or_store_policy_before_duplicate() {
    let (store, keys) = store_and_keys();
    let signed = preferences(&store, &keys, 1);
    let mut invalid = signed.clone();
    invalid.signature = [0; 96];
    let mut seen = SeenBids::new(1, 16).unwrap();
    assert!(seen.validate_preferences(&store, &invalid, 0).is_err());
    assert!(seen.context.is_none());
    seen.validate_preferences(&store, &signed, 0).unwrap();
    let other = store_with_genesis(&store, [199; 32]);
    assert!(matches!(
        seen.validate_preferences(&other, &signed, 0),
        Err(BidGossipError::Reject("gossip cache belongs to another chain or configuration"))
    ));
    let root = store.get_head().unwrap().root;
    let state = store.block_state(root).unwrap().clone();
    let block = store.block_arc(root).unwrap().as_ref().clone();
    let mut policy = store.store_parameters();
    policy.max_blocks -= 1;
    let other_policy = GloasStore::get_store(
        state.clone(),
        block.clone(),
        policy,
        store.processing_parameters().clone(),
    )
    .unwrap();
    assert!(matches!(
        seen.validate_preferences(&other_policy, &signed, 0),
        Err(BidGossipError::Reject("gossip cache belongs to another chain or configuration"))
    ));
    let mut processing = store.processing_parameters().clone();
    processing.min_builder_withdrawability_delay += 1;
    let other_processing =
        GloasStore::get_store(state, block, store.store_parameters(), processing).unwrap();
    assert!(matches!(
        seen.validate_preferences(&other_processing, &signed, 0),
        Err(BidGossipError::Reject("gossip cache belongs to another chain or configuration"))
    ));
    assert!(matches!(
        seen.validate_preferences(&store, &signed, 0),
        Err(BidGossipError::Ignore(
            "already seen preferences for this dependent root and proposal slot"
        ))
    ));
    seen.retain_from_slot(2);
    assert!(matches!(
        seen.validate_preferences(&other, &signed, 0),
        Err(BidGossipError::Reject("gossip cache belongs to another chain or configuration"))
    ));
    assert!(seen.preferences.is_empty());
}
