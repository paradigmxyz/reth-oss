//! Real signed CL and envelope regressions with controlled Engine/empty-custody contracts.
//! Contract callbacks are test doubles; passing these tests is not evidence of live EL validation.

use super::*;
use blst::min_pk::SecretKey;
use reth_embedded_beacon_primitives::uint64_root;
use reth_embedded_beacon_state_transition::ssz::{List, ProgressiveList, Vector};
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    task::{Context, Poll, Waker},
};

fn ready<T>(future: impl Future<Output = T>) -> T {
    let mut future = std::pin::pin!(future);
    let mut context = Context::from_waker(Waker::noop());
    match future.as_mut().poll(&mut context) {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("controlled verifier must complete synchronously"),
    }
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
fn signed_child(
    store: &GloasStore,
    keys: &[SecretKey],
    include_parent: bool,
    hash: u8,
) -> SignedBeaconBlock {
    let head = store.get_head().unwrap();
    let mut state = store.block_state(head.root).unwrap().clone();
    let target = state.slot + 1;
    gloas_processing::process_slots(&mut state, target, &store.processing).unwrap();
    let proposer = gloas_processing::get_beacon_proposer_index(&state, &store.processing).unwrap();
    let mut infinity = [0; 96];
    infinity[0] = 0xc0;
    let mut body = gloas::BeaconBlockBody::default_value(Preset::Minimal).unwrap();
    body.eth1_data = state.eth1_data.clone();
    body.sync_aggregate.sync_committee_signature = infinity;
    body.signed_execution_payload_bid.signature = infinity;
    body.signed_execution_payload_bid.message = gloas::ExecutionPayloadBid {
        parent_block_hash: if include_parent {
            state.latest_execution_payload_bid.block_hash
        } else {
            state.latest_block_hash
        },
        parent_block_root: head.root,
        block_hash: [hash; 32],
        prev_randao: state.randao_mixes.values[(state.slot / 8 % 64) as usize],
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
        .sign(
            &crate::gloas::tests::signing_root(&state, uint64_root(state.slot / 8), [2, 0, 0, 0]),
            crate::gloas::tests::DST,
            &[],
        )
        .to_bytes();
    let mut message = BeaconBlock {
        slot: state.slot,
        proposer_index: proposer,
        parent_root: head.root,
        state_root: [0; 32],
        body,
    };
    let parent_slot = state.latest_block_header.slot;
    let parameters = &store.processing;
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
            &crate::gloas::tests::signing_root(
                &state,
                message.hash_tree_root(Preset::Minimal).unwrap(),
                [0; 4],
            ),
            crate::gloas::tests::DST,
            &[],
        )
        .to_bytes();
    SignedBeaconBlock { message, signature }
}
fn import_child(
    store: &mut GloasStore,
    keys: &[SecretKey],
    include_parent: bool,
    hash: u8,
) -> Root {
    let signed = signed_child(store, keys, include_parent, hash);
    store.on_tick(signed.message.slot * store.slot_duration_ms()).unwrap();
    store.on_block(&signed).unwrap();
    signed.message.hash_tree_root(Preset::Minimal).unwrap()
}
fn signed_envelope(
    store: &GloasStore,
    root: Root,
    keys: &[SecretKey],
) -> SignedExecutionPayloadEnvelope {
    let state = store.block_state(root).unwrap();
    let bid = &state.latest_execution_payload_bid;
    let payload = gloas::ExecutionPayload {
        parent_hash: state.latest_block_hash,
        fee_recipient: bid.fee_recipient,
        state_root: [1; 32],
        receipts_root: [2; 32],
        logs_bloom: Vector::from_vec(vec![0; 256]),
        prev_randao: bid.prev_randao,
        block_number: state.slot + 1,
        gas_limit: bid.gas_limit,
        gas_used: 0,
        timestamp: state.genesis_time + state.slot * 6,
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
            &crate::gloas::tests::signing_root(
                state,
                message.hash_tree_root(Preset::Minimal).unwrap(),
                [11, 0, 0, 0],
            ),
            crate::gloas::tests::DST,
            &[],
        )
        .to_bytes();
    SignedExecutionPayloadEnvelope { message, signature }
}
struct ControlledEngine {
    expected_payload: gloas::ExecutionPayload,
    expected_context: gloas_processing::ExecutionContext,
    accept: bool,
    calls: AtomicUsize,
}
impl ExecutionEngine for ControlledEngine {
    type Error = std::io::Error;
    async fn verify_payload(
        &self,
        payload: &gloas::ExecutionPayload,
        context: gloas_processing::ExecutionContext,
    ) -> Result<(), Self::Error> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        assert_eq!(payload, &self.expected_payload);
        assert_eq!(context.slot, self.expected_context.slot);
        assert_eq!(
            context.parent_beacon_block_root,
            self.expected_context.parent_beacon_block_root
        );
        assert_eq!(
            context.previous_execution_payload_hash,
            self.expected_context.previous_execution_payload_hash
        );
        assert_eq!(
            context.expected_blob_versioned_hashes,
            self.expected_context.expected_blob_versioned_hashes
        );
        assert_eq!(context.execution_requests_list, self.expected_context.execution_requests_list);
        if self.accept {
            Ok(())
        } else {
            Err(std::io::Error::other("controlled Engine rejection"))
        }
    }
}
struct EmptyCustody {
    expected_root: Root,
    accept: bool,
    calls: AtomicUsize,
}
impl DataAvailability for EmptyCustody {
    type Error = std::io::Error;
    async fn verify(&self, root: Root, block: &BeaconBlock) -> Result<(), Self::Error> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        assert_eq!(root, self.expected_root);
        assert_eq!(root, block.hash_tree_root(Preset::Minimal).unwrap());
        assert!(block
            .body
            .signed_execution_payload_bid
            .message
            .blob_kzg_commitments
            .values
            .is_empty());
        if self.accept {
            Ok(())
        } else {
            Err(std::io::Error::other("controlled unavailable custody"))
        }
    }
}
fn verifiers(
    store: &GloasStore,
    root: Root,
    signed: &SignedExecutionPayloadEnvelope,
    engine_accept: bool,
    da_accept: bool,
) -> (ControlledEngine, EmptyCustody) {
    let state = store.block_state(root).unwrap();
    (
        ControlledEngine {
            expected_payload: signed.message.payload.clone(),
            expected_context: gloas_processing::ExecutionContext {
                slot: state.slot,
                parent_beacon_block_root: state.latest_block_header.parent_root,
                previous_execution_payload_hash: state.latest_block_hash,
                expected_blob_versioned_hashes: Vec::new(),
                execution_requests_list: gloas_processing::get_execution_requests_list(
                    &signed.message.execution_requests,
                    Preset::Minimal,
                )
                .unwrap(),
            },
            accept: engine_accept,
            calls: AtomicUsize::new(0),
        },
        EmptyCustody { expected_root: root, accept: da_accept, calls: AtomicUsize::new(0) },
    )
}
fn admit_envelope(store: &mut GloasStore, root: Root, keys: &[SecretKey]) {
    let signed = signed_envelope(store, root, keys);
    let (engine, availability) = verifiers(store, root, &signed, true, true);
    ready(store.on_execution_payload_envelope(&signed, &engine, &availability)).unwrap();
    assert_eq!(engine.calls.load(Ordering::SeqCst), 1);
    assert_eq!(availability.calls.load(Ordering::SeqCst), 1);
}
fn empty_chain() -> (GloasStore, Vec<SecretKey>, [Root; 4]) {
    let (mut store, keys) = crate::gloas::tests::store_and_keys();
    let anchor = store.execution_query_anchor.expect("checked Gloas-only anchor");
    admit_envelope(&mut store, anchor, &keys);
    let included = import_child(&mut store, &keys, true, 11);
    let empty = import_child(&mut store, &keys, false, 12);
    let newest = import_child(&mut store, &keys, false, 13);
    (store, keys, [anchor, included, empty, newest])
}
fn absence_anchor(zero_bid: bool) -> (GloasStore, Vec<SecretKey>) {
    let (original, keys) = crate::gloas::tests::store_and_keys();
    let root = original.execution_query_anchor.expect("checked Gloas-only anchor");
    let mut state = original.block_state(root).unwrap().clone();
    let mut block = original.block_arc(root).unwrap().as_ref().clone();
    state.latest_block_hash = [0; 32];
    state.latest_execution_payload_bid.parent_block_hash = [0; 32];
    if zero_bid {
        state.latest_execution_payload_bid.block_hash = [0; 32];
    }
    block.body.signed_execution_payload_bid.message = state.latest_execution_payload_bid.clone();
    state.latest_block_header.body_root = block.body.hash_tree_root(Preset::Minimal).unwrap();
    block.state_root = state.hash_tree_root(Preset::Minimal).unwrap();
    (GloasStore::get_store(state, block, original.parameters, original.processing).unwrap(), keys)
}

#[test]
fn pending_and_missing_full_cannot_invent_execution_authority() {
    let (store, _) = crate::gloas::tests::store_and_keys();
    let root = store.execution_query_anchor.expect("checked Gloas-only anchor");
    assert_eq!(
        store.verified_execution_block_hash(node(root, PayloadStatus::Pending)).unwrap(),
        None
    );
    assert!(matches!(
        store.verified_execution_block_hash(node(root, PayloadStatus::Full)),
        Err(StoreError::ExecutionImport(_))
    ));
    assert!(matches!(
        store.verified_execution_block_hash(ForkChoiceNode { root, payload_status: None }),
        Err(StoreError::InvalidAttestationTarget)
    ));
    assert!(matches!(
        store.verified_execution_block_hash(node([0xff; 32], PayloadStatus::Pending)),
        Err(StoreError::UnknownRoot)
    ));
}
#[test]
fn actual_signed_envelope_and_both_contracts_are_required_for_full_hash() {
    let (mut store, keys) = crate::gloas::tests::store_and_keys();
    let root = store.execution_query_anchor.expect("checked Gloas-only anchor");
    let signed = signed_envelope(&store, root, &keys);
    let before = store.snapshot().unwrap();
    let bytes = store.retained_schema_bytes().unwrap();
    let (accepting, missing) = verifiers(&store, root, &signed, true, false);
    assert!(matches!(
        ready(store.on_execution_payload_envelope(&signed, &accepting, &missing)),
        Err(EnvelopeError::DataAvailability(_))
    ));
    assert_eq!(accepting.calls.load(Ordering::SeqCst), 0);
    let (rejecting, available) = verifiers(&store, root, &signed, false, true);
    assert!(matches!(
        ready(store.on_execution_payload_envelope(&signed, &rejecting, &available)),
        Err(EnvelopeError::Verification(_))
    ));
    assert!(store.verified_execution_block_hash(node(root, PayloadStatus::Full)).is_err());
    let mut invalid = signed.clone();
    invalid.signature = [0; 96];
    let (accepting, available) = verifiers(&store, root, &signed, true, true);
    assert!(matches!(
        ready(store.on_execution_payload_envelope(&invalid, &accepting, &available)),
        Err(EnvelopeError::Verification(_))
    ));
    assert_eq!(accepting.calls.load(Ordering::SeqCst), 0);
    assert_eq!(store.snapshot().unwrap(), before);
    assert_eq!(store.retained_schema_bytes().unwrap(), bytes);
    ready(store.on_execution_payload_envelope(&signed, &accepting, &available)).unwrap();
    assert_eq!(
        store.verified_execution_block_hash(node(root, PayloadStatus::Full)).unwrap(),
        Some(signed.message.payload.block_hash)
    );
}
#[test]
fn genuine_multi_empty_gap_resolves_to_the_exact_verified_full_ancestor() {
    let (store, _, [anchor, included, empty, newest]) = empty_chain();
    assert!(!store.is_payload_verified(included));
    assert!(!store.is_payload_verified(empty));
    assert!(!store.is_payload_verified(newest));
    assert!(store.is_payload_verified(anchor));
    let expected = store.payloads.get(&anchor).unwrap().payload.block_hash;
    for root in [included, empty, newest] {
        assert_eq!(
            store.verified_execution_block_hash(node(root, PayloadStatus::Empty)).unwrap(),
            Some(expected)
        );
    }
    assert_eq!(store.block(newest).unwrap().parent_root, empty);
    assert_eq!(store.block(empty).unwrap().parent_root, included);
}
#[test]
fn missing_verified_ancestor_does_not_change_graph_or_publish_a_hash() {
    let (mut store, _, [_, _, _, newest]) = empty_chain();
    // Simulate missing recovered delivery metadata without fabricating any positive authority.
    store.payloads.clear();
    let snapshot = store.snapshot().unwrap();
    let bytes = store.retained_schema_bytes().unwrap();
    assert!(matches!(
        store.verified_execution_block_hash(node(newest, PayloadStatus::Empty)),
        Err(StoreError::ExecutionImport(_))
    ));
    assert_eq!(store.snapshot().unwrap(), snapshot);
    assert_eq!(store.retained_schema_bytes().unwrap(), bytes);
}
#[test]
fn unknown_nonzero_anchor_ancestry_fails_but_finalized_bid_association_is_exact() {
    let (store, _) = crate::gloas::tests::store_and_keys();
    let root = store.execution_query_anchor.expect("checked Gloas-only anchor");
    assert!(matches!(
        store.verified_execution_block_hash(node(root, PayloadStatus::Empty)),
        Err(StoreError::UnknownRoot)
    ));
    assert_eq!(
        store.finalized_execution_block_hash().unwrap(),
        Some(
            store.block(root).unwrap().body.signed_execution_payload_bid.message.parent_block_hash
        )
    );
}
#[test]
fn sealed_zero_anchor_proves_absence_without_getting_full_payload_credit() {
    let (store, _) = absence_anchor(true);
    let root = store.execution_query_anchor.expect("checked Gloas-only anchor");
    assert_eq!(
        store.verified_execution_block_hash(node(root, PayloadStatus::Empty)).unwrap(),
        None
    );
    assert_eq!(store.finalized_execution_block_hash().unwrap(), None);
    assert!(matches!(
        store.verified_execution_block_hash(node(root, PayloadStatus::Full)),
        Err(StoreError::ExecutionImport(_))
    ));
}
#[test]
fn legitimate_zero_finalized_safe_association_coexists_with_verified_nonzero_head() {
    let (mut store, keys) = absence_anchor(false);
    assert_eq!(store.finalized_execution_block_hash().unwrap(), None);
    let child = import_child(&mut store, &keys, false, 11);
    admit_envelope(&mut store, child, &keys);
    assert_eq!(store.get_head().unwrap(), node(child, PayloadStatus::Full));
    assert_eq!(
        store.verified_execution_block_hash(node(child, PayloadStatus::Full)).unwrap(),
        Some([11; 32])
    );
    assert_eq!(store.finalized_execution_block_hash().unwrap(), None);
}
#[test]
fn corrupted_internal_hash_link_and_zero_context_never_establish_authority() {
    let (mut store, _, [anchor, _, _, newest]) = empty_chain();
    // These deliberate private-map corruptions exercise defensive failures, not public ingress.
    Arc::make_mut(store.payloads.get_mut(&anchor).unwrap()).payload.block_hash = [0xee; 32];
    let snapshot = store.snapshot().unwrap();
    assert!(matches!(
        store.verified_execution_block_hash(node(newest, PayloadStatus::Empty)),
        Err(StoreError::ExecutionImport(_))
    ));
    assert_eq!(store.snapshot().unwrap(), snapshot);
    let (mut zero, _) = absence_anchor(true);
    let root = zero.execution_query_anchor.expect("checked Gloas-only anchor");
    Arc::make_mut(zero.block_states.get_mut(&root).unwrap().hot_mut().unwrap()).latest_block_hash =
        [1; 32];
    assert!(matches!(
        zero.verified_execution_block_hash(node(root, PayloadStatus::Empty)),
        Err(StoreError::ExecutionImport(_))
    ));
}
#[test]
fn corrupted_internal_cycle_and_configured_budget_fail_without_mutation() {
    let (mut store, _, [_, _, _, newest]) = empty_chain();
    store.parameters.max_blocks = 1;
    let before = store.retained_schema_bytes().unwrap();
    assert!(matches!(
        store.verified_execution_block_hash(node(newest, PayloadStatus::Empty)),
        Err(StoreError::ResourceLimit)
    ));
    assert_eq!(store.retained_schema_bytes().unwrap(), before);
    store.parameters.max_blocks = 1024;
    Arc::make_mut(store.blocks.get_mut(&newest).unwrap()).parent_root = newest;
    Arc::make_mut(store.block_states.get_mut(&newest).unwrap().hot_mut().unwrap())
        .latest_block_header
        .parent_root = newest;
    assert!(matches!(
        store.verified_execution_block_hash(node(newest, PayloadStatus::Empty)),
        Err(StoreError::InvalidBlockTime)
    ));
}
#[test]
fn empty_carry_forward_mismatch_is_rejected_even_without_an_envelope_at_that_gap() {
    let (mut store, _, [_, _, empty, newest]) = empty_chain();
    Arc::make_mut(store.blocks.get_mut(&empty).unwrap())
        .body
        .signed_execution_payload_bid
        .message
        .parent_block_hash = [0xaa; 32];
    let before = store.retained_schema_bytes().unwrap();
    assert!(matches!(
        store.verified_execution_block_hash(node(newest, PayloadStatus::Empty)),
        Err(StoreError::ExecutionImport(_))
    ));
    assert_eq!(store.retained_schema_bytes().unwrap(), before);
}

#[test]
fn corrupted_private_zero_envelope_cannot_gain_full_payload_credit() {
    let (mut store, keys) = crate::gloas::tests::store_and_keys();
    let root = store.execution_query_anchor.expect("checked Gloas-only anchor");
    admit_envelope(&mut store, root, &keys);
    // Deliberately corrupt private records after genuine admission; public queries must fail.
    Arc::make_mut(store.blocks.get_mut(&root).unwrap())
        .body
        .signed_execution_payload_bid
        .message
        .block_hash = [0; 32];
    Arc::make_mut(store.block_states.get_mut(&root).unwrap().hot_mut().unwrap())
        .latest_execution_payload_bid
        .block_hash = [0; 32];
    Arc::make_mut(store.payloads.get_mut(&root).unwrap()).payload.block_hash = [0; 32];
    assert!(matches!(store.verified_execution_block_hash(node(root,PayloadStatus::Full)),
        Err(StoreError::ExecutionImport(message)) if message=="zero execution sentinel cannot establish a full payload"));
}

#[test]
fn zero_fields_without_the_exact_sealed_anchor_are_not_absence_authority() {
    let (mut store, _) = absence_anchor(true);
    let root = store.execution_query_anchor.expect("checked Gloas-only anchor");
    // The exact immutable anchor identity is constructor-owned; this tests internal corruption.
    store.execution_query_anchor = Some([0xee; 32]);
    assert!(matches!(
        store.verified_execution_block_hash(node(root, PayloadStatus::Empty)),
        Err(StoreError::UnknownRoot)
    ));
}
