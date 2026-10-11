//! Real BLS admission, bounded graphs and callback/cancellation regressions.

use super::*;
use blst::min_pk::SecretKey;
use reth_embedded_beacon_primitives::{hash_pair, uint64_root, BeaconBlockHeader};
use reth_embedded_beacon_state_transition::{
    altair_processing, bellatrix_processing, capella_processing, deneb_processing,
    electra_processing, fulu_processing,
    ssz::{phase0, Bits, List, PresetBits, ProgressiveList, Vector},
};
use std::{
    future::pending,
    sync::atomic::{AtomicUsize, Ordering},
    task::{Context, Poll, Waker},
};

pub(super) const DST: &[u8] = b"BLS_SIG_BLS12381G2_XMD:SHA-256_SSWU_RO_POP_";
pub(super) fn signing_root(state: &BeaconState, object: Root, domain_type: [u8; 4]) -> Root {
    let mut version = [0; 32];
    version[..4].copy_from_slice(&state.fork.current_version);
    let fork_data = hash_pair(version, state.genesis_validators_root);
    let mut domain = [0; 32];
    domain[..4].copy_from_slice(&domain_type);
    domain[4..].copy_from_slice(&fork_data[..28]);
    hash_pair(object, domain)
}
fn requests() -> gloas::ExecutionRequests {
    gloas::ExecutionRequests {
        deposits: ProgressiveList::default(),
        withdrawals: ProgressiveList::default(),
        consolidations: ProgressiveList::default(),
        builder_deposits: ProgressiveList::default(),
        builder_exits: ProgressiveList::default(),
    }
}
fn body(state: &BeaconState) -> gloas::BeaconBlockBody {
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
pub(super) fn store_and_keys() -> (GloasStore, Vec<SecretKey>) {
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
    let block = BeaconBlock {
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
pub(super) fn child(store: &GloasStore, keys: &[SecretKey]) -> (SignedBeaconBlock, BeaconState) {
    child_with_attestations(store, keys, Vec::new())
}
pub(super) fn child_with_attestations(
    store: &GloasStore,
    keys: &[SecretKey],
    attestations: Vec<gloas::Attestation>,
) -> (SignedBeaconBlock, BeaconState) {
    let head = store.get_head().unwrap();
    let mut state = store.block_state(head.root).unwrap().clone();
    let parameters = &store.processing;
    let target = state.slot + 1;
    gloas_processing::process_slots(&mut state, target, parameters).unwrap();
    let proposer = gloas_processing::get_beacon_proposer_index(&state, parameters).unwrap();
    let mut body = body(&state);
    body.attestations = ProgressiveList::from_vec(attestations);
    body.signed_execution_payload_bid.message = gloas::ExecutionPayloadBid {
        parent_block_hash: state.latest_block_hash,
        parent_block_root: head.root,
        block_hash: [11; 32],
        prev_randao: state.randao_mixes.values[0],
        fee_recipient: [8; 20],
        gas_limit: 30_000_000,
        builder_index: u64::MAX,
        slot: state.slot,
        value: 0,
        execution_payment: 0,
        blob_kzg_commitments: ProgressiveList::default(),
        execution_requests_root: requests().hash_tree_root(Preset::Minimal).unwrap(),
    };
    body.randao_reveal = keys[proposer as usize]
        .sign(&signing_root(&state, uint64_root(state.slot / 8), [2, 0, 0, 0]), DST, &[])
        .to_bytes();
    let mut message = BeaconBlock {
        slot: state.slot,
        proposer_index: proposer,
        parent_root: head.root,
        state_root: [0; 32],
        body,
    };
    let parent_slot = state.latest_block_header.slot;
    gloas_processing::process_parent_execution_payload(&mut state, &message, parameters).unwrap();
    gloas_processing::process_block_header(&mut state, &message, parameters).unwrap();
    gloas_processing::process_withdrawals(&mut state, parameters).unwrap();
    gloas_processing::process_execution_payload_bid(
        &mut state,
        &message.body.signed_execution_payload_bid,
        parameters,
    )
    .unwrap();
    gloas_processing::process_randao(&mut state, &message.body, parameters).unwrap();
    gloas_processing::process_eth1_data(&mut state, &message.body, parameters).unwrap();
    gloas_processing::process_operations(&mut state, &message.body, parent_slot, parameters)
        .unwrap();
    gloas_processing::process_sync_aggregate(&mut state, &message.body.sync_aggregate, parameters)
        .unwrap();
    message.state_root = state.hash_tree_root(Preset::Minimal).unwrap();
    let signature = keys[proposer as usize]
        .sign(
            &signing_root(&state, message.hash_tree_root(Preset::Minimal).unwrap(), [0; 4]),
            DST,
            &[],
        )
        .to_bytes();
    (SignedBeaconBlock { message, signature }, state)
}
fn envelope(store: &GloasStore, root: Root, keys: &[SecretKey]) -> SignedExecutionPayloadEnvelope {
    let state = store.block_state(root).unwrap();
    let bid = &state.latest_execution_payload_bid;
    let payload = gloas::ExecutionPayload {
        parent_hash: state.latest_block_hash,
        fee_recipient: [8; 20],
        state_root: [1; 32],
        receipts_root: [2; 32],
        logs_bloom: Vector::from_vec(vec![0; 256]),
        prev_randao: bid.prev_randao,
        block_number: 1,
        gas_limit: bid.gas_limit,
        gas_used: 0,
        timestamp: state.slot * 6,
        extra_data: List::default(),
        base_fee_per_gas: [0; 32],
        block_hash: bid.block_hash,
        transactions: ProgressiveList::default(),
        withdrawals: state.payload_expected_withdrawals.clone(),
        blob_gas_used: 0,
        excess_blob_gas: 0,
        block_access_list: ProgressiveList::default(),
        slot_number: state.slot,
    };
    let message = gloas::ExecutionPayloadEnvelope {
        payload,
        execution_requests: requests(),
        builder_index: bid.builder_index,
        beacon_block_root: root,
        parent_beacon_block_root: state.latest_block_header.parent_root,
    };
    let signature = keys[state.latest_block_header.proposer_index as usize]
        .sign(
            &signing_root(state, message.hash_tree_root(Preset::Minimal).unwrap(), [11, 0, 0, 0]),
            DST,
            &[],
        )
        .to_bytes();
    SignedExecutionPayloadEnvelope { message, signature }
}
fn ready<T>(future: impl Future<Output = T>) -> T {
    let mut future = std::pin::pin!(future);
    let mut context = Context::from_waker(Waker::noop());
    match future.as_mut().poll(&mut context) {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("unexpected pending synthetic verifier"),
    }
}
#[derive(Debug)]
struct Engine {
    allowed: bool,
    calls: AtomicUsize,
}
impl ExecutionEngine for Engine {
    type Error = std::io::Error;
    async fn verify_payload(
        &self,
        _payload: &gloas::ExecutionPayload,
        _context: gloas_processing::ExecutionContext,
    ) -> Result<(), Self::Error> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.allowed {
            Ok(())
        } else {
            Err(std::io::Error::other("synthetic Engine rejection"))
        }
    }
}
struct Availability {
    allowed: bool,
    calls: AtomicUsize,
}
impl DataAvailability for Availability {
    type Error = std::io::Error;
    async fn verify(&self, root: Root, block: &BeaconBlock) -> Result<(), Self::Error> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        assert_eq!(root, block.hash_tree_root(Preset::Minimal).unwrap());
        if self.allowed {
            Ok(())
        } else {
            Err(std::io::Error::other("missing synthetic custody data"))
        }
    }
}
struct PendingEngine;
impl ExecutionEngine for PendingEngine {
    type Error = std::io::Error;
    async fn verify_payload(
        &self,
        _payload: &gloas::ExecutionPayload,
        _context: gloas_processing::ExecutionContext,
    ) -> Result<(), Self::Error> {
        pending().await
    }
}

#[test]
fn strict_signed_import_rejects_bad_signature_and_postroot_without_exposing_blocks() {
    let (mut store, keys) = store_and_keys();
    let (block, expected) = child(&store, &keys);
    store.on_tick(6000).unwrap();
    let before = store.snapshot().unwrap();
    let root = block.message.hash_tree_root(Preset::Minimal).unwrap();
    let mut invalid = block.clone();
    invalid.signature = [0; 96];
    assert!(store.on_block(&invalid).is_err());
    assert!(store.block_arc(root).is_none());
    assert_eq!(store.snapshot().unwrap(), before);
    invalid = block.clone();
    invalid.message.state_root[0] ^= 1;
    invalid.signature = keys[invalid.message.proposer_index as usize]
        .sign(
            &signing_root(
                &expected,
                invalid.message.hash_tree_root(Preset::Minimal).unwrap(),
                [0; 4],
            ),
            DST,
            &[],
        )
        .to_bytes();
    assert!(store.on_block(&invalid).is_err());
    assert_eq!(store.snapshot().unwrap(), before);
    store.on_block(&block).unwrap();
    assert_eq!(store.block_state(root), Some(&expected));
    assert_eq!(store.block_arc(root).unwrap().as_ref(), &block.message);
    assert_eq!(store.get_head().unwrap(), node(root, PayloadStatus::Empty));
}
#[test]
fn custody_failure_engine_rejection_and_cancellation_cannot_publish_full_payload() {
    let (mut store, keys) = store_and_keys();
    let (block, _) = child(&store, &keys);
    store.on_tick(6000).unwrap();
    store.on_block(&block).unwrap();
    let root = block.message.hash_tree_root(Preset::Minimal).unwrap();
    let signed = envelope(&store, root, &keys);
    let before = store.snapshot().unwrap();
    let bytes = store.retained_schema_bytes().unwrap();
    let engine = Engine { allowed: true, calls: AtomicUsize::new(0) };
    let missing = Availability { allowed: false, calls: AtomicUsize::new(0) };
    assert!(matches!(
        ready(store.on_execution_payload_envelope(&signed, &engine, &missing)),
        Err(EnvelopeError::DataAvailability(_))
    ));
    assert_eq!(engine.calls.load(Ordering::SeqCst), 0);
    assert_eq!(missing.calls.load(Ordering::SeqCst), 1);
    let available = Availability { allowed: true, calls: AtomicUsize::new(0) };
    let rejecting = Engine { allowed: false, calls: AtomicUsize::new(0) };
    assert!(matches!(
        ready(store.on_execution_payload_envelope(&signed, &rejecting, &available)),
        Err(EnvelopeError::Verification(_))
    ));
    assert_eq!(rejecting.calls.load(Ordering::SeqCst), 1);
    {
        let mut future = std::pin::pin!(store.on_execution_payload_envelope(
            &signed,
            &PendingEngine,
            &available
        ));
        assert!(matches!(
            future.as_mut().poll(&mut Context::from_waker(Waker::noop())),
            Poll::Pending
        ));
    }
    assert!(!store.is_payload_verified(root));
    assert_eq!(store.retained_schema_bytes().unwrap(), bytes);
    assert_eq!(store.snapshot().unwrap(), before);
    let mut invalid = signed.clone();
    invalid.signature = [0; 96];
    assert!(matches!(
        ready(store.on_execution_payload_envelope(&invalid, &engine, &available)),
        Err(EnvelopeError::Verification(_))
    ));
    assert_eq!(engine.calls.load(Ordering::SeqCst), 0);
    ready(store.on_execution_payload_envelope(&signed, &engine, &available)).unwrap();
    assert_eq!(engine.calls.load(Ordering::SeqCst), 1);
    assert!(store.is_payload_verified(root));
    assert_eq!(store.get_head().unwrap(), node(root, PayloadStatus::Full));
    assert!(!store.payload_timeliness(root, false).unwrap());
}
#[test]
fn authenticated_ptc_updates_every_duplicate_position_but_cannot_establish_full_delivery() {
    let (mut store, keys) = store_and_keys();
    let (block, _) = child(&store, &keys);
    store.on_tick(6000).unwrap();
    store.on_block(&block).unwrap();
    let root = block.message.hash_tree_root(Preset::Minimal).unwrap();
    let state = store.block_state(root).unwrap();
    let ptc = gloas_processing::get_ptc(state, 1, &store.processing).unwrap();
    let validator = ptc.values[0];
    let positions = ptc
        .values
        .iter()
        .enumerate()
        .filter_map(|(position, &index)| (index == validator).then_some(position))
        .collect::<Vec<_>>();
    let data = gloas::PayloadAttestationData {
        beacon_block_root: root,
        slot: 1,
        payload_present: true,
        blob_data_available: true,
    };
    let signature = keys[validator as usize]
        .sign(
            &signing_root(state, data.hash_tree_root(Preset::Minimal).unwrap(), [12, 0, 0, 0]),
            DST,
            &[],
        )
        .to_bytes();
    let message = PayloadAttestationMessage { validator_index: validator, data, signature };
    let mut invalid = message.clone();
    invalid.signature = [0; 96];
    assert!(store.on_payload_attestation_message(&invalid).is_err());
    assert_eq!(store.payload_timeliness_votes(root).unwrap(), vec![None; 16]);
    store.on_payload_attestation_message(&message).unwrap();
    for (position, vote) in store.payload_timeliness_votes(root).unwrap().iter().enumerate() {
        assert_eq!(*vote, positions.contains(&position).then_some(true));
    }
    assert!(!store.is_payload_verified(root));
    assert!(!store.payload_timeliness(root, true).unwrap());
    assert!(store.payload_timeliness(root, false).unwrap());
    assert_eq!(store.get_head().unwrap(), node(root, PayloadStatus::Empty));
    store.on_tick(12_000).unwrap();
    assert!(store.on_payload_attestation_message(&message).is_err());
}
#[test]
fn exact_payload_ancestry_clock_bounds_and_retention_are_enforced() {
    let (mut store, keys) = store_and_keys();
    let (block, _) = child(&store, &keys);
    let anchor = store.get_head().unwrap();
    let before = store.snapshot().unwrap();
    assert!(matches!(store.on_block(&block), Err(StoreError::InvalidBlockTime)));
    assert!(matches!(store.on_tick(u64::MAX), Err(StoreError::ResourceLimit)));
    assert_eq!(store.snapshot().unwrap(), before);
    store.on_tick(6000).unwrap();
    store.on_block(&block).unwrap();
    let root = block.message.hash_tree_root(Preset::Minimal).unwrap();
    assert!(store
        .is_ancestor(node(root, PayloadStatus::Pending), node(anchor.root, PayloadStatus::Empty))
        .unwrap());
    assert!(!store
        .is_ancestor(node(root, PayloadStatus::Pending), node(anchor.root, PayloadStatus::Full))
        .unwrap());
    assert!(store
        .is_ancestor(node(root, PayloadStatus::Pending), node(anchor.root, PayloadStatus::Pending))
        .unwrap());
    let mut parameters = store.parameters;
    parameters.max_retained_schema_bytes = 1;
    let state = store.block_state(anchor.root).unwrap().clone();
    let block = store.block_arc(anchor.root).unwrap().as_ref().clone();
    assert!(matches!(
        GloasStore::get_store(state, block, parameters, store.processing.clone()),
        Err(StoreError::ResourceLimit)
    ));
}

#[cfg(feature = "spec-tests")]
#[test]
fn artificial_graph_skips_only_bls_and_preserves_postroot_and_runtime_separation() {
    let (mut runtime, keys) = store_and_keys();
    let anchor = runtime.get_head().unwrap().root;
    let mut fixture = artificial::ArtificialSpecStore::get_store(
        runtime.block_state(anchor).unwrap().clone(),
        runtime.block_arc(anchor).unwrap().as_ref().clone(),
        runtime.parameters,
        runtime.processing.clone(),
    )
    .unwrap();
    let (block, expected) = child(&runtime, &keys);
    let root = block.message.hash_tree_root(Preset::Minimal).unwrap();
    runtime.on_tick(6000).unwrap();
    fixture.on_tick(6000).unwrap();
    let runtime_before = runtime.snapshot().unwrap();
    let fixture_before = fixture.get_head().unwrap();
    let mut invalid = block.clone();
    invalid.signature = [0; 96];
    invalid.message.state_root[0] ^= 1;
    let invalid_root = invalid.message.hash_tree_root(Preset::Minimal).unwrap();
    assert!(fixture.on_block_and_contents(&invalid).is_err());
    assert!(fixture.block_state(invalid_root).is_none());
    assert_eq!(fixture.get_head().unwrap(), fixture_before);
    invalid.message.state_root = block.message.state_root;
    assert!(runtime.on_block_and_contents(&invalid).is_err());
    assert_eq!(runtime.snapshot().unwrap(), runtime_before);
    assert!(runtime.block_state(root).is_none());
    fixture.on_block_and_contents(&invalid).unwrap();
    assert_eq!(fixture.block_state(root), Some(&expected));
    assert_eq!(fixture.get_head().unwrap(), node(root, PayloadStatus::Empty));
    assert_eq!(runtime.snapshot().unwrap(), runtime_before);
}

#[cfg(feature = "spec-tests")]
#[test]
fn artificial_envelope_keeps_execution_data_and_cancellation_gates_mandatory() {
    let (mut runtime, keys) = store_and_keys();
    let anchor = runtime.get_head().unwrap().root;
    let mut fixture = artificial::ArtificialSpecStore::get_store(
        runtime.block_state(anchor).unwrap().clone(),
        runtime.block_arc(anchor).unwrap().as_ref().clone(),
        runtime.parameters,
        runtime.processing.clone(),
    )
    .unwrap();
    let (block, _) = child(&runtime, &keys);
    let root = block.message.hash_tree_root(Preset::Minimal).unwrap();
    runtime.on_tick(6000).unwrap();
    fixture.on_tick(6000).unwrap();
    runtime.on_block_and_contents(&block).unwrap();
    fixture.on_block_and_contents(&block).unwrap();
    let mut signed = envelope(&runtime, root, &keys);
    signed.signature = [0; 96];
    let before = fixture.get_head().unwrap();
    let engine = Engine { allowed: true, calls: AtomicUsize::new(0) };
    let missing = Availability { allowed: false, calls: AtomicUsize::new(0) };
    assert!(matches!(
        ready(fixture.on_execution_payload_envelope(&signed, &engine, &missing)),
        Err(EnvelopeError::DataAvailability(_))
    ));
    assert_eq!(engine.calls.load(Ordering::SeqCst), 0);
    let available = Availability { allowed: true, calls: AtomicUsize::new(0) };
    let rejecting = Engine { allowed: false, calls: AtomicUsize::new(0) };
    assert!(matches!(
        ready(fixture.on_execution_payload_envelope(&signed, &rejecting, &available)),
        Err(EnvelopeError::Verification(_))
    ));
    assert_eq!(rejecting.calls.load(Ordering::SeqCst), 1);
    {
        let mut future = std::pin::pin!(fixture.on_execution_payload_envelope(
            &signed,
            &PendingEngine,
            &available
        ));
        assert!(matches!(
            future.as_mut().poll(&mut Context::from_waker(Waker::noop())),
            Poll::Pending
        ));
    }
    assert_eq!(fixture.get_head().unwrap(), before);
    assert!(matches!(
        ready(runtime.on_execution_payload_envelope(&signed, &engine, &available)),
        Err(EnvelopeError::Verification(_))
    ));
    assert_eq!(engine.calls.load(Ordering::SeqCst), 0);
    let mut malformed = signed.clone();
    malformed.message.parent_beacon_block_root[0] ^= 1;
    assert!(matches!(
        ready(fixture.on_execution_payload_envelope(&malformed, &engine, &available)),
        Err(EnvelopeError::Verification(_))
    ));
    assert_eq!(engine.calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.get_head().unwrap(), before);
    ready(fixture.on_execution_payload_envelope(&signed, &engine, &available)).unwrap();
    assert_eq!(engine.calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.get_head().unwrap(), node(root, PayloadStatus::Full));
    assert!(!runtime.is_payload_verified(root));
    assert_eq!(runtime.get_head().unwrap(), node(root, PayloadStatus::Empty));
}

#[cfg(not(feature = "spec-tests"))]
#[test]
fn artificial_policy_is_unavailable_before_clock_or_graph_changes_without_fixture_feature() {
    let (mut runtime, _) = store_and_keys();
    let before = runtime.snapshot().unwrap();
    assert!(matches!(
        runtime.on_tick_inner::<false>(6000),
        Err(StoreError::ValidationUnavailable(_))
    ));
    assert_eq!(runtime.snapshot().unwrap(), before);
    assert_eq!(runtime.time_ms(), 0);
}
#[test]
fn envelope_propagation_does_not_relax_full_payload_commitments_or_establish_delivery() {
    let (mut store, keys) = store_and_keys();
    let (block, _) = child(&store, &keys);
    store.on_tick(6000).unwrap();
    store.on_block(&block).unwrap();
    let root = block.message.hash_tree_root(Preset::Minimal).unwrap();
    let mut signed = envelope(&store, root, &keys);
    signed.message.payload.gas_limit += 1;
    let state = store.block_state(root).unwrap();
    signed.signature = keys[state.latest_block_header.proposer_index as usize]
        .sign(
            &signing_root(
                state,
                signed.message.hash_tree_root(Preset::Minimal).unwrap(),
                [11, 0, 0, 0],
            ),
            DST,
            &[],
        )
        .to_bytes();
    envelope_gossip::validate(&store, &signed).unwrap();
    let before = store.snapshot().unwrap();
    let engine = Engine { allowed: true, calls: AtomicUsize::new(0) };
    let availability = Availability { allowed: true, calls: AtomicUsize::new(0) };
    assert!(matches!(
        ready(store.on_execution_payload_envelope(&signed, &engine, &availability)),
        Err(EnvelopeError::Verification(_))
    ));
    assert_eq!(engine.calls.load(Ordering::SeqCst), 0);
    assert!(!store.is_payload_verified(root));
    assert_eq!(store.snapshot().unwrap(), before);
    signed.signature = [0; 96];
    assert!(matches!(
        envelope_gossip::validate(&store, &signed),
        Err(envelope_gossip::GossipEnvelopeError::Reject("invalid envelope signature"))
    ));
}
