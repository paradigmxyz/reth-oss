use super::*;
use crate::ssz::{
    capella::{
        BLSToExecutionChange, BeaconBlock, BeaconBlockBody, SignedBLSToExecutionChange,
        SignedBeaconBlock, SyncAggregate, Withdrawal, Withdrawals,
    },
    PresetBits, Vector,
};
use blst::min_pk::SecretKey;
use reth_embedded_beacon_crypto::{compute_domain, compute_signing_root, ETHEREUM_BLS_DST};
use reth_embedded_beacon_primitives::{sha256, uint64_root};
use std::{
    future::Future,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    },
    task::{Context, Poll, Waker},
};

fn state_and_keys() -> (BeaconState, Vec<SecretKey>, CapellaParameters) {
    let (mut pre, keys) = crate::bellatrix_processing::tests::state_and_keys();
    pre.latest_execution_payload_header.block_hash = [9; 32];
    let parameters = CapellaParameters::for_preset(Preset::Minimal);
    (upgrade_to_capella(&pre, &parameters).unwrap(), keys, parameters)
}

fn payload(state: &BeaconState, parameters: &CapellaParameters) -> ExecutionPayload {
    ExecutionPayload {
        parent_hash: state.latest_execution_payload_header.block_hash,
        fee_recipient: [0; 20],
        state_root: [0; 32],
        receipts_root: [0; 32],
        logs_bloom: Vector::from_vec(vec![0; 256]),
        prev_randao: state.randao_mixes.values[(state.slot / 8 % 64) as usize],
        block_number: state.latest_execution_payload_header.block_number + 1,
        gas_limit: 30_000_000,
        gas_used: 0,
        timestamp: state.genesis_time + state.slot * parameters.bellatrix.seconds_per_slot,
        extra_data: List::default(),
        base_fee_per_gas: [0; 32],
        block_hash: [10; 32],
        transactions: List::default(),
        withdrawals: Withdrawals::from_vec(
            get_expected_withdrawals(state, parameters).unwrap().withdrawals,
        ),
    }
}

fn execution_credentials(address: u8) -> [u8; 32] {
    let mut credentials = [0; 32];
    credentials[0] = 1;
    credentials[12..].fill(address);
    credentials
}

#[test]
fn upgrade_preserves_complete_execution_header_and_freezes_legacy_history() {
    let (mut pre, _) = crate::bellatrix_processing::tests::state_and_keys();
    pre.latest_execution_payload_header.parent_hash = [1; 32];
    pre.latest_execution_payload_header.block_hash = [2; 32];
    pre.latest_execution_payload_header.base_fee_per_gas = [3; 32];
    pre.latest_execution_payload_header.transactions_root = [4; 32];
    pre.historical_roots.values.push([5; 32]);
    let parameters = CapellaParameters::for_preset(Preset::Minimal);
    let mut state = upgrade_to_capella(&pre, &parameters).unwrap();
    assert_eq!(state.latest_execution_payload_header.parent_hash, [1; 32]);
    assert_eq!(state.latest_execution_payload_header.block_hash, [2; 32]);
    assert_eq!(state.latest_execution_payload_header.base_fee_per_gas, [3; 32]);
    assert_eq!(state.latest_execution_payload_header.transactions_root, [4; 32]);
    assert_eq!(state.latest_execution_payload_header.withdrawals_root, [0; 32]);
    assert_eq!(state.next_withdrawal_index, 0);
    assert_eq!(state.next_withdrawal_validator_index, 0);
    assert_eq!(state.fork.previous_version, pre.fork.current_version);
    assert_eq!(state.fork.current_version, parameters.fork_version);
    state.slot = 63;
    let block_root = state.block_roots.hash_tree_root(Preset::Minimal).unwrap();
    let state_root = state.state_roots.hash_tree_root(Preset::Minimal).unwrap();
    process_historical_summaries_update(&mut state, &parameters).unwrap();
    assert_eq!(state.historical_roots.values, vec![[5; 32]]);
    assert_eq!(state.historical_summaries.values.len(), 1);
    assert_eq!(state.historical_summaries.values[0].block_summary_root, block_root);
    assert_eq!(state.historical_summaries.values[0].state_summary_root, state_root);
}

#[test]
fn exact_sweep_distinguishes_full_partial_and_ineligible_balances_and_is_atomic() {
    let (mut state, _, parameters) = state_and_keys();
    state.next_withdrawal_index = 100;
    state.validators.values[0].withdrawal_credentials = execution_credentials(11);
    state.validators.values[0].withdrawable_epoch = 0;
    state.balances.values[0] = 32_000_000_017;
    state.validators.values[1].withdrawal_credentials = execution_credentials(12);
    state.balances.values[1] = 32_000_000_003;
    state.balances.values[2] = 32_000_000_005; // Excess BLS credentials cannot be withdrawn.
    let expected = get_expected_withdrawals(&state, &parameters).unwrap();
    assert_eq!(
        expected.withdrawals,
        vec![
            Withdrawal {
                index: 100,
                validator_index: 0,
                address: [11; 20],
                amount: 32_000_000_017
            },
            Withdrawal { index: 101, validator_index: 1, address: [12; 20], amount: 3 },
        ]
    );
    assert_eq!(expected.processed_sweep_withdrawals_count, 16);
    let correct = payload(&state, &parameters);
    let mut wrong = correct.clone();
    wrong.withdrawals.values.swap(0, 1);
    let before = state.clone();
    assert!(process_withdrawals(&mut state, &wrong, &parameters).is_err());
    assert_eq!(state, before);
    process_withdrawals(&mut state, &correct, &parameters).unwrap();
    assert_eq!(state.balances.values[0], 0);
    assert_eq!(state.balances.values[1], 32_000_000_000);
    assert_eq!(state.balances.values[2], 32_000_000_005);
    assert_eq!(state.next_withdrawal_index, 102);
    assert_eq!(state.next_withdrawal_validator_index, 16);
}

#[test]
fn full_output_resumes_after_last_withdrawal_and_short_output_advances_full_sweep() {
    let (mut state, _, parameters) = state_and_keys();
    state.next_withdrawal_validator_index = 31;
    for index in [31, 0, 1, 2] {
        state.validators.values[index].withdrawal_credentials = execution_credentials(index as u8);
        state.balances.values[index] += 1;
    }
    let full = payload(&state, &parameters);
    assert_eq!(
        full.withdrawals
            .values
            .iter()
            .map(|withdrawal| withdrawal.validator_index)
            .collect::<Vec<_>>(),
        vec![31, 0, 1, 2]
    );
    process_withdrawals(&mut state, &full, &parameters).unwrap();
    assert_eq!(state.next_withdrawal_validator_index, 3);
    assert_eq!(state.next_withdrawal_index, 4);
    let empty = payload(&state, &parameters);
    assert!(empty.withdrawals.values.is_empty());
    process_withdrawals(&mut state, &empty, &parameters).unwrap();
    assert_eq!(state.next_withdrawal_validator_index, 19);
    assert_eq!(state.next_withdrawal_index, 4);
}

fn address_change(
    state: &mut BeaconState,
    key: &SecretKey,
    index: u64,
    parameters: &CapellaParameters,
) -> SignedBLSToExecutionChange {
    let pubkey = key.sk_to_pk().to_bytes();
    let mut credentials = sha256(&pubkey);
    credentials[0] = 0;
    state.validators.values[index as usize].withdrawal_credentials = credentials;
    let message = BLSToExecutionChange {
        validator_index: index,
        from_bls_pubkey: pubkey,
        to_execution_address: [15; 20],
    };
    let root = compute_signing_root(
        message.hash_tree_root(Preset::Minimal).unwrap(),
        compute_domain(
            [10, 0, 0, 0],
            parameters.bellatrix.altair.phase0.genesis_fork_version,
            state.genesis_validators_root,
        ),
    );
    SignedBLSToExecutionChange {
        message,
        signature: key.sign(&root, ETHEREUM_BLS_DST, &[]).to_bytes(),
    }
}

#[test]
fn credential_change_uses_withdrawal_key_and_genesis_domain_and_rejects_invalid_signatures() {
    let (mut state, keys, parameters) = state_and_keys();
    // The withdrawal key may differ from the validator's ordinary signing key.
    let change = address_change(&mut state, &keys[1], 0, &parameters);
    let before = state.clone();
    let mut invalid = change.clone();
    invalid.signature = [0; 96];
    assert!(process_bls_to_execution_change(&mut state, &invalid, &parameters).is_err());
    assert_eq!(state, before);
    let wrong_domain = compute_signing_root(
        change.message.hash_tree_root(Preset::Minimal).unwrap(),
        compute_domain([10, 0, 0, 0], parameters.fork_version, state.genesis_validators_root),
    );
    invalid.signature = keys[1].sign(&wrong_domain, ETHEREUM_BLS_DST, &[]).to_bytes();
    assert!(process_bls_to_execution_change(&mut state, &invalid, &parameters).is_err());
    assert_eq!(state, before);
    process_bls_to_execution_change(&mut state, &change, &parameters).unwrap();
    assert_eq!(state.validators.values[0].withdrawal_credentials, execution_credentials(15));
    assert!(process_bls_to_execution_change(&mut state, &change, &parameters).is_err());
}

fn candidate(
    pre: &BeaconState,
    keys: &[SecretKey],
    parameters: &CapellaParameters,
) -> (SignedBeaconBlock, BeaconState) {
    let mut expected = pre.clone();
    let slot = pre.slot + 1;
    process_slots(&mut expected, slot, parameters).unwrap();
    let proposer = get_beacon_proposer_index(&expected, parameters).unwrap();
    let key = &keys[proposer as usize];
    let epoch = expected.slot / parameters.slots_per_epoch();
    let randao = compute_signing_root(
        uint64_root(epoch),
        compute_domain(
            [2, 0, 0, 0],
            expected.fork.current_version,
            expected.genesis_validators_root,
        ),
    );
    let mut infinity = [0; 96];
    infinity[0] = 0xc0;
    let body = BeaconBlockBody {
        randao_reveal: key.sign(&randao, ETHEREUM_BLS_DST, &[]).to_bytes(),
        eth1_data: expected.eth1_data.clone(),
        graffiti: [0; 32],
        proposer_slashings: List::default(),
        attester_slashings: List::default(),
        attestations: List::default(),
        deposits: List::default(),
        voluntary_exits: List::default(),
        sync_aggregate: SyncAggregate {
            sync_committee_bits: PresetBits::from_ssz(&[0; 4], Preset::Minimal).unwrap(),
            sync_committee_signature: infinity,
        },
        execution_payload: payload(&expected, parameters),
        bls_to_execution_changes: List::default(),
    };
    let mut message = BeaconBlock {
        slot,
        proposer_index: proposer,
        parent_root: expected.latest_block_header.hash_tree_root(),
        state_root: [0; 32],
        body,
    };
    process_block_header(&mut expected, &message, parameters).unwrap();
    process_withdrawals(&mut expected, &message.body.execution_payload, parameters).unwrap();
    expected.latest_execution_payload_header = validate_execution_payload_consensus(
        &expected,
        &message.body.execution_payload,
        parameters,
    )
    .unwrap();
    process_randao(&mut expected, &message.body, parameters).unwrap();
    process_eth1_data(&mut expected, &message.body, parameters).unwrap();
    process_operations(&mut expected, &message.body, parameters).unwrap();
    process_sync_aggregate(&mut expected, &message.body.sync_aggregate, parameters).unwrap();
    message.state_root = expected.hash_tree_root(Preset::Minimal).unwrap();
    let root = compute_signing_root(
        message.hash_tree_root(Preset::Minimal).unwrap(),
        compute_domain([0; 4], expected.fork.current_version, expected.genesis_validators_root),
    );
    (
        SignedBeaconBlock { message, signature: key.sign(&root, ETHEREUM_BLS_DST, &[]).to_bytes() },
        expected,
    )
}

fn ready<F: Future>(future: F) -> F::Output {
    let mut context = Context::from_waker(Waker::noop());
    let mut future = std::pin::pin!(future);
    match future.as_mut().poll(&mut context) {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("test callback must be immediately ready"),
    }
}

// Private test adapter checks atomicity and exact payload submission; it proves no EL validity.
struct RecordingEngine {
    allowed: bool,
    calls: AtomicUsize,
    observed: Mutex<Option<(ExecutionPayload, ExecutionContext)>>,
}
impl RecordingEngine {
    fn new(allowed: bool) -> Self {
        Self { allowed, calls: AtomicUsize::new(0), observed: Mutex::new(None) }
    }
}
impl ExecutionEngine for RecordingEngine {
    type Error = std::io::Error;
    async fn verify_terminal_block(
        &self,
        _payload: &ExecutionPayload,
        _context: ExecutionContext,
    ) -> Result<(), Self::Error> {
        Err(std::io::Error::other("test terminal eligibility unavailable"))
    }
    async fn verify_payload(
        &self,
        payload: &ExecutionPayload,
        context: ExecutionContext,
    ) -> Result<(), Self::Error> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        *self.observed.lock().unwrap() = Some((payload.clone(), context));
        if self.allowed {
            Ok(())
        } else {
            Err(std::io::Error::other("test execution rejection"))
        }
    }
}

#[test]
fn signed_import_requires_exact_v2_engine_callback_and_rolls_back_rejection() {
    let (mut state, keys, parameters) = state_and_keys();
    state.validators.values[0].withdrawal_credentials = execution_credentials(1);
    state.balances.values[0] += 5;
    let before = state.clone();
    let (block, expected) = candidate(&state, &keys, &parameters);
    let rejecting = RecordingEngine::new(false);
    assert!(matches!(
        ready(state_transition(&mut state, &block, &parameters, &rejecting)),
        Err(ImportError::Execution(_))
    ));
    assert_eq!(state, before);
    let accepting = RecordingEngine::new(true);
    ready(state_transition(&mut state, &block, &parameters, &accepting)).unwrap();
    assert_eq!(state, expected);
    assert_eq!(accepting.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        *accepting.observed.lock().unwrap(),
        Some((
            block.message.body.execution_payload.clone(),
            ExecutionContext {
                slot: block.message.slot,
                parent_beacon_block_root: block.message.parent_root,
                previous_execution_payload_hash: Some([9; 32]),
            }
        ))
    );
}

#[test]
fn invalid_outer_signature_and_post_root_never_publish_candidate_state() {
    let (state, keys, parameters) = state_and_keys();
    let (mut block, _) = candidate(&state, &keys, &parameters);
    let original_signature = block.signature;
    block.signature = [0; 96];
    let engine = RecordingEngine::new(true);
    let mut candidate = state.clone();
    assert!(matches!(
        ready(state_transition(&mut candidate, &block, &parameters, &engine)),
        Err(ImportError::Consensus(ProcessingError::Crypto(_)))
    ));
    assert_eq!(candidate, state);
    assert_eq!(engine.calls.load(Ordering::SeqCst), 0);
    block.signature = original_signature;
    block.message.state_root[0] ^= 1;
    let root = compute_signing_root(
        block.message.hash_tree_root(Preset::Minimal).unwrap(),
        compute_domain([0; 4], state.fork.current_version, state.genesis_validators_root),
    );
    block.signature =
        keys[block.message.proposer_index as usize].sign(&root, ETHEREUM_BLS_DST, &[]).to_bytes();
    assert!(matches!(
        ready(state_transition(&mut candidate, &block, &parameters, &engine)),
        Err(ImportError::Consensus(ProcessingError::InvalidState(
            "block post-state root mismatch"
        )))
    ));
    assert_eq!(candidate, state);
    assert_eq!(engine.calls.load(Ordering::SeqCst), 1);
}

#[test]
fn first_payload_requires_terminal_eligibility_before_new_payload_acceptance() {
    let (mut state, keys, parameters) = state_and_keys();
    state.latest_execution_payload_header = empty_execution_header();
    let (block, _) = candidate(&state, &keys, &parameters);
    let before = state.clone();
    let engine = RecordingEngine::new(true);
    assert!(matches!(
        ready(state_transition(&mut state, &block, &parameters, &engine)),
        Err(ImportError::Execution(_))
    ));
    assert_eq!(state, before);
    assert_eq!(engine.calls.load(Ordering::SeqCst), 0);
}

#[test]
fn cached_capella_roots_include_all_three_new_fields_and_historical_epoch_updates() {
    let (mut state, _, parameters) = state_and_keys();
    state.slot = 7;
    state.next_withdrawal_index = 123;
    state.next_withdrawal_validator_index = 9;
    state.historical_roots.values.push([7; 32]);
    let constants = checked_constants(&state, &parameters).unwrap();
    let mut full = state.clone();
    let mut cache = slot_cache::SlotCache::new(&state, &constants).unwrap();
    for _ in 0..66 {
        assert_eq!(cache.root().unwrap(), state.hash_tree_root(Preset::Minimal).unwrap());
        cache.process_slot(&mut state).unwrap();
        slot_kernel(&mut full, &constants).unwrap();
        let next = state.slot + 1;
        if next.is_multiple_of(parameters.slots_per_epoch()) {
            epoch_kernel(&mut state, &parameters, &constants).unwrap();
            epoch_kernel(&mut full, &parameters, &constants).unwrap();
            cache.refresh_epoch(&state).unwrap();
        }
        state.slot = next;
        full.slot = next;
        cache.advance_slot(next);
        assert_eq!(state, full);
    }
    assert_eq!(state.historical_roots.values, vec![[7; 32]]);
    assert_eq!(state.historical_summaries.values.len(), 1);
    assert_eq!(state.next_withdrawal_index, 123);
    assert_eq!(state.next_withdrawal_validator_index, 9);
    assert_eq!(cache.root().unwrap(), state.hash_tree_root(Preset::Minimal).unwrap());
}

// A cancelled await must not publish any candidate consensus changes.
struct PendingEngine(AtomicUsize);
impl ExecutionEngine for PendingEngine {
    type Error = std::io::Error;
    async fn verify_terminal_block(
        &self,
        _payload: &ExecutionPayload,
        _context: ExecutionContext,
    ) -> Result<(), Self::Error> {
        Err(std::io::Error::other("test terminal eligibility unavailable"))
    }
    async fn verify_payload(
        &self,
        _payload: &ExecutionPayload,
        _context: ExecutionContext,
    ) -> Result<(), Self::Error> {
        self.0.fetch_add(1, Ordering::SeqCst);
        std::future::pending().await
    }
}

#[test]
fn cancellation_while_engine_is_pending_retains_the_original_consensus_state() {
    let (mut state, keys, parameters) = state_and_keys();
    state.validators.values[0].withdrawal_credentials = execution_credentials(1);
    state.balances.values[0] += 9;
    let (block, _) = candidate(&state, &keys, &parameters);
    let before = state.clone();
    let engine = PendingEngine(AtomicUsize::new(0));
    {
        let mut future = Box::pin(state_transition(&mut state, &block, &parameters, &engine));
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(future.as_mut().poll(&mut context), Poll::Pending));
        assert_eq!(engine.0.load(Ordering::SeqCst), 1);
    }
    assert_eq!(state, before);
}

#[test]
fn payload_operation_hands_opaque_transactions_to_engine_and_only_commits_header() {
    let (mut state, keys, parameters) = state_and_keys();
    state.validators.values[0].withdrawal_credentials = execution_credentials(1);
    state.balances.values[0] += 9;
    let (mut block, _) = candidate(&state, &keys, &parameters);
    // SSZ transactions are opaque; only the mandatory engine decides execution validity.
    block.message.body.execution_payload.transactions.values.push(List::default());
    process_slots(&mut state, block.message.slot, &parameters).unwrap();
    process_block_header(&mut state, &block.message, &parameters).unwrap();
    let before = state.clone();
    let expected_header = validate_execution_payload_consensus(
        &state,
        &block.message.body.execution_payload,
        &parameters,
    )
    .unwrap();
    let mut incorrect = block.message.body.clone();
    incorrect.execution_payload.parent_hash = [42; 32];
    let engine = RecordingEngine::new(true);
    assert!(ready(process_execution_payload(&mut state, &incorrect, &parameters, &engine)).is_err());
    assert_eq!(engine.calls.load(Ordering::SeqCst), 0);
    assert_eq!(state, before);
    let rejecting = RecordingEngine::new(false);
    assert!(matches!(
        ready(process_execution_payload(&mut state, &block.message.body, &parameters, &rejecting)),
        Err(ImportError::Execution(_)),
    ));
    assert_eq!(state, before);
    ready(process_execution_payload(&mut state, &block.message.body, &parameters, &engine))
        .unwrap();
    let mut expected = before;
    expected.latest_execution_payload_header = expected_header;
    assert_eq!(state, expected);
    assert_eq!(
        *engine.observed.lock().unwrap(),
        Some((
            block.message.body.execution_payload,
            ExecutionContext {
                slot: state.slot,
                parent_beacon_block_root: block.message.parent_root,
                previous_execution_payload_hash: Some([9; 32]),
            }
        )),
    );
}

#[test]
fn withdrawal_counter_overflow_is_rejected_before_any_balance_changes() {
    let (mut state, _, parameters) = state_and_keys();
    state.validators.values[0].withdrawal_credentials = execution_credentials(1);
    state.balances.values[0] += 1;
    let mut payload = payload(&state, &parameters);
    state.next_withdrawal_index = u64::MAX;
    payload.withdrawals.values[0].index = u64::MAX;
    let before = state.clone();
    assert!(get_expected_withdrawals(&state, &parameters).is_err());
    assert!(process_withdrawals(&mut state, &payload, &parameters).is_err());
    assert_eq!(state, before);
}

// Private synthetic proof adapter exercises dispatch ordering, not real terminal eligibility.
struct FirstPayloadEngine {
    terminal_allowed: bool,
    calls: Mutex<Vec<(&'static str, ExecutionPayload, ExecutionContext)>>,
}
impl ExecutionEngine for FirstPayloadEngine {
    type Error = std::io::Error;
    async fn verify_terminal_block(
        &self,
        payload: &ExecutionPayload,
        context: ExecutionContext,
    ) -> Result<(), Self::Error> {
        self.calls.lock().unwrap().push(("terminal", payload.clone(), context));
        if self.terminal_allowed {
            Ok(())
        } else {
            Err(std::io::Error::other("terminal proof rejected"))
        }
    }
    async fn verify_payload(
        &self,
        payload: &ExecutionPayload,
        context: ExecutionContext,
    ) -> Result<(), Self::Error> {
        self.calls.lock().unwrap().push(("payload", payload.clone(), context));
        Ok(())
    }
}
#[test]
fn first_payload_preserves_required_parent_and_requires_ordered_exact_callbacks() {
    let (mut state, keys, parameters) = state_and_keys();
    state.latest_execution_payload_header = empty_execution_header();
    let (mut block, _) = candidate(&state, &keys, &parameters);
    process_slots(&mut state, block.message.slot, &parameters).unwrap();
    process_block_header(&mut state, &block.message, &parameters).unwrap();
    // Capella requires matching the stored header hash even when the entire header is empty.
    block.message.body.execution_payload.parent_hash = [0; 32];
    let before = state.clone();
    let rejecting = FirstPayloadEngine { terminal_allowed: false, calls: Mutex::new(Vec::new()) };
    assert!(matches!(
        ready(process_execution_payload(&mut state, &block.message.body, &parameters, &rejecting)),
        Err(ImportError::Execution(_))
    ));
    assert_eq!(state, before);
    let context = ExecutionContext {
        slot: state.slot,
        parent_beacon_block_root: state.latest_block_header.parent_root,
        previous_execution_payload_hash: None,
    };
    assert_eq!(
        *rejecting.calls.lock().unwrap(),
        vec![("terminal", block.message.body.execution_payload.clone(), context)]
    );
    let accepting = FirstPayloadEngine { terminal_allowed: true, calls: Mutex::new(Vec::new()) };
    ready(process_execution_payload(&mut state, &block.message.body, &parameters, &accepting))
        .unwrap();
    assert_eq!(
        *accepting.calls.lock().unwrap(),
        vec![
            ("terminal", block.message.body.execution_payload.clone(), context),
            ("payload", block.message.body.execution_payload.clone(), context)
        ]
    );
    assert_eq!(state.latest_execution_payload_header.parent_hash, [0; 32]);
    let mut bad_parent = before.clone();
    let mut incorrect = block.message.body.clone();
    incorrect.execution_payload.parent_hash = [44; 32];
    let engine = FirstPayloadEngine { terminal_allowed: true, calls: Mutex::new(Vec::new()) };
    assert!(matches!(
        ready(process_execution_payload(&mut bad_parent, &incorrect, &parameters, &engine)),
        Err(ImportError::Consensus(ProcessingError::InvalidState(
            "execution payload parent hash mismatch"
        )))
    ));
    assert!(engine.calls.lock().unwrap().is_empty());
    assert_eq!(bad_parent, before);
    let mut expected = before;
    expected.latest_execution_payload_header = state.latest_execution_payload_header.clone();
    assert_eq!(state, expected);
}

#[test]
fn signed_import_accepts_already_processed_upgrade_slot_without_replaying_it() {
    let (mut state, keys, parameters) = state_and_keys();
    let (block, expected) = candidate(&state, &keys, &parameters);
    process_slots(&mut state, block.message.slot, &parameters).unwrap();
    let before = state.clone();
    let rejecting = RecordingEngine::new(false);
    assert!(matches!(
        ready(state_transition(&mut state, &block, &parameters, &rejecting)),
        Err(ImportError::Execution(_))
    ));
    assert_eq!(state, before);
    let accepting = RecordingEngine::new(true);
    ready(state_transition(&mut state, &block, &parameters, &accepting)).unwrap();
    assert_eq!(state, expected);
    let after = state.clone();
    assert!(ready(state_transition(&mut state, &block, &parameters, &accepting)).is_err());
    assert_eq!(state, after);
}

fn empty_execution_header() -> ExecutionPayloadHeader {
    ExecutionPayloadHeader {
        parent_hash: [0; 32],
        fee_recipient: [0; 20],
        state_root: [0; 32],
        receipts_root: [0; 32],
        logs_bloom: crate::ssz::Vector::from_vec(vec![0; 256]),
        prev_randao: [0; 32],
        block_number: 0,
        gas_limit: 0,
        gas_used: 0,
        timestamp: 0,
        extra_data: List::default(),
        base_fee_per_gas: [0; 32],
        block_hash: [0; 32],
        transactions_root: [0; 32],
        withdrawals_root: [0; 32],
    }
}
