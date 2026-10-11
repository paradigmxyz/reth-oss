//! Real-signature Deneb regressions and private execution-callback atomicity checks.

use super::*;
use crate::ssz::{
    deneb::{Attestation, BeaconBlock, BeaconBlockBody, SignedBeaconBlock, SyncAggregate},
    phase0::{AttestationData, SignedVoluntaryExit, VoluntaryExit},
    PresetBitList, PresetBits, Vector,
};
use blst::min_pk::{AggregateSignature, SecretKey, Signature};
use reth_embedded_beacon_crypto::{compute_domain, compute_signing_root, ETHEREUM_BLS_DST};
use reth_embedded_beacon_primitives::{uint64_root, Checkpoint};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    },
    task::{Context, Poll, Waker},
};

fn state_and_keys() -> (BeaconState, Vec<SecretKey>, DenebParameters) {
    let (mut pre, keys) = crate::bellatrix_processing::tests::state_and_keys();
    pre.latest_execution_payload_header.block_hash = [9; 32];
    let parameters = DenebParameters::for_preset(Preset::Minimal);
    let pre = crate::capella_processing::upgrade_to_capella(&pre, &parameters.capella).unwrap();
    (upgrade_to_deneb(&pre, &parameters).unwrap(), keys, parameters)
}

fn payload(state: &BeaconState, parameters: &DenebParameters) -> ExecutionPayload {
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
        timestamp: state.genesis_time + state.slot * parameters.capella.bellatrix.seconds_per_slot,
        extra_data: List::default(),
        base_fee_per_gas: [0; 32],
        block_hash: [10; 32],
        transactions: List::default(),
        withdrawals: List::default(),
        blob_gas_used: 262_144,
        excess_blob_gas: 393_216,
    }
}

fn body(state: &BeaconState, parameters: &DenebParameters) -> BeaconBlockBody {
    let mut infinity = [0; 96];
    infinity[0] = 0xc0;
    BeaconBlockBody {
        randao_reveal: [0; 96],
        eth1_data: state.eth1_data.clone(),
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
        execution_payload: payload(state, parameters),
        bls_to_execution_changes: List::default(),
        blob_kzg_commitments: List::from_vec(vec![[1; 48], [2; 48]]),
    }
}

fn candidate(
    pre: &BeaconState,
    keys: &[SecretKey],
    parameters: &DenebParameters,
) -> (SignedBeaconBlock, BeaconState) {
    let mut expected = pre.clone();
    process_slots(&mut expected, pre.slot + 1, parameters).unwrap();
    let proposer = get_beacon_proposer_index(&expected, parameters).unwrap();
    let key = &keys[proposer as usize];
    let current = expected.slot / parameters.slots_per_epoch();
    let randao_root = compute_signing_root(
        uint64_root(current),
        compute_domain(
            [2, 0, 0, 0],
            expected.fork.current_version,
            expected.genesis_validators_root,
        ),
    );
    let mut body = body(&expected, parameters);
    body.randao_reveal = key.sign(&randao_root, ETHEREUM_BLS_DST, &[]).to_bytes();
    let mut message = BeaconBlock {
        slot: expected.slot,
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
        &message.body.blob_kzg_commitments.values,
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
        Poll::Pending => panic!("private test adapter must be immediately ready"),
    }
}

// This private adapter verifies callback inputs and atomicity, never actual execution validity.
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
        Err(std::io::Error::other("terminal proof unavailable in post-merge test adapter"))
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
fn exact_v3_candidate_context_and_rejection_are_atomic() {
    let (mut state, keys, parameters) = state_and_keys();
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
                expected_blob_versioned_hashes: vec![
                    kzg_commitment_to_versioned_hash(&[1; 48]),
                    kzg_commitment_to_versioned_hash(&[2; 48])
                ],
            },
        ))
    );
    assert_eq!(state.latest_execution_payload_header.blob_gas_used, 262_144);
    assert_eq!(state.latest_execution_payload_header.excess_blob_gas, 393_216);
}

#[test]
fn invalid_outer_signature_and_post_root_never_publish_candidate_state() {
    let (state, keys, parameters) = state_and_keys();
    let (mut block, _) = candidate(&state, &keys, &parameters);
    block.signature = [0; 96];
    let accepting = RecordingEngine::new(true);
    let mut post = state.clone();
    assert!(matches!(
        ready(state_transition(&mut post, &block, &parameters, &accepting)),
        Err(ImportError::Consensus(ProcessingError::Crypto(_)))
    ));
    assert_eq!(post, state);
    assert_eq!(accepting.calls.load(Ordering::SeqCst), 0);
    block.message.state_root = [99; 32];
    let root = compute_signing_root(
        block.message.hash_tree_root(Preset::Minimal).unwrap(),
        compute_domain([0; 4], state.fork.current_version, state.genesis_validators_root),
    );
    block.signature =
        keys[block.message.proposer_index as usize].sign(&root, ETHEREUM_BLS_DST, &[]).to_bytes();
    assert!(matches!(
        ready(state_transition(&mut post, &block, &parameters, &accepting)),
        Err(ImportError::Consensus(ProcessingError::InvalidState(
            "block post-state root mismatch"
        )))
    ));
    assert_eq!(post, state);
    assert_eq!(accepting.calls.load(Ordering::SeqCst), 1);
}

#[test]
fn blob_limit_and_payload_invariants_precede_execution_callback() {
    let (mut state, _, parameters) = state_and_keys();
    state.latest_block_header.parent_root = [8; 32];
    let mut body = body(&state, &parameters);
    let before = state.clone();
    body.blob_kzg_commitments.values.resize(7, [1; 48]);
    let engine = RecordingEngine::new(true);
    assert!(matches!(
        ready(process_execution_payload(&mut state, &body, &parameters, &engine)),
        Err(ImportError::Consensus(ProcessingError::InvalidState(
            "Deneb block exceeds configured blob limit"
        )))
    ));
    assert_eq!(engine.calls.load(Ordering::SeqCst), 0);
    assert_eq!(state, before);
    body.blob_kzg_commitments.values.truncate(6);
    let original = body.execution_payload.clone();
    for variant in 0..3 {
        let mut payload = original.clone();
        let expected = match variant {
            0 => {
                payload.parent_hash = [0; 32];
                "execution payload parent hash mismatch"
            }
            1 => {
                payload.prev_randao = [1; 32];
                "execution payload RANDAO mismatch"
            }
            _ => {
                payload.timestamp += 1;
                "execution payload timestamp mismatch"
            }
        };
        assert!(
            matches!(validate_execution_payload_consensus(&state, &payload, &body.blob_kzg_commitments.values, &parameters), Err(ProcessingError::InvalidState(reason)) if reason == expected)
        );
    }
    body.execution_payload.transactions.values.push(List::default());
    ready(process_execution_payload(&mut state, &body, &parameters, &engine)).unwrap();
    assert_eq!(
        engine.observed.lock().unwrap().as_ref().unwrap().0.transactions,
        body.execution_payload.transactions
    );
    assert_eq!(
        engine.observed.lock().unwrap().as_ref().unwrap().1.parent_beacon_block_root,
        [8; 32]
    );
}

#[test]
fn activation_churn_cap_does_not_cap_exits() {
    let (mut state, _, mut parameters) = state_and_keys();
    parameters.capella.bellatrix.altair.phase0.min_per_epoch_churn_limit = 20;
    for validator in &mut state.validators.values[..10] {
        validator.activation_epoch = u64::MAX;
        validator.activation_eligibility_epoch = 0;
    }
    for validator in &mut state.validators.values[10..22] {
        validator.effective_balance = 16_000_000_000;
    }
    process_registry_updates(&mut state, &parameters).unwrap();
    assert_eq!(
        state.validators.values[..10].iter().filter(|v| v.activation_epoch != u64::MAX).count(),
        4
    );
    assert!(state.validators.values[..4].iter().all(|v| v.activation_epoch == 5));
    assert!(state.validators.values[10..22].iter().all(|v| v.exit_epoch == 5));
}

#[test]
fn voluntary_exit_uses_fixed_capella_domain_after_deneb_upgrade() {
    let (mut state, keys, parameters) = state_and_keys();
    state.slot = parameters.capella.bellatrix.altair.phase0.shard_committee_period *
        parameters.slots_per_epoch();
    let message = VoluntaryExit { epoch: 0, validator_index: 0 };
    let domain = compute_domain(
        [4, 0, 0, 0],
        parameters.capella.fork_version,
        state.genesis_validators_root,
    );
    let root = compute_signing_root(message.hash_tree_root(Preset::Minimal).unwrap(), domain);
    let good = SignedVoluntaryExit {
        message: message.clone(),
        signature: keys[0].sign(&root, ETHEREUM_BLS_DST, &[]).to_bytes(),
    };
    let current_domain =
        compute_domain([4, 0, 0, 0], parameters.fork_version, state.genesis_validators_root);
    let root =
        compute_signing_root(message.hash_tree_root(Preset::Minimal).unwrap(), current_domain);
    let bad = SignedVoluntaryExit {
        message,
        signature: keys[0].sign(&root, ETHEREUM_BLS_DST, &[]).to_bytes(),
    };
    let before = state.clone();
    assert!(matches!(
        process_voluntary_exit(&mut state, &bad, &parameters),
        Err(ProcessingError::Crypto(_))
    ));
    assert_eq!(state, before);
    process_voluntary_exit(&mut state, &good, &parameters).unwrap();
    assert_eq!(
        state.validators.values[0].exit_epoch,
        state.slot / parameters.slots_per_epoch() + 5
    );
}

#[test]
fn previous_epoch_attestation_remains_valid_after_slots_per_epoch_delay() {
    let (mut state, keys, parameters) = state_and_keys();
    state.slot = 15;
    state.block_roots.values[0] = [4; 32];
    let constants = checked_constants(&state, &parameters).unwrap();
    let committee =
        with_altair(&mut state, |common| altair_processing::committee(common, 0, 0, &constants))
            .unwrap();
    let data = AttestationData {
        slot: 0,
        index: 0,
        beacon_block_root: [4; 32],
        source: state.previous_justified_checkpoint,
        target: Checkpoint { epoch: 0, root: [4; 32] },
    };
    let root = compute_signing_root(
        data.hash_tree_root(Preset::Minimal).unwrap(),
        compute_domain([1, 0, 0, 0], state.fork.current_version, state.genesis_validators_root),
    );
    let signatures = committee
        .iter()
        .map(|&index| keys[index as usize].sign(&root, ETHEREUM_BLS_DST, &[]))
        .collect::<Vec<Signature>>();
    let refs = signatures.iter().collect::<Vec<_>>();
    let signature = AggregateSignature::aggregate(&refs, true).unwrap().to_signature().to_bytes();
    let mut bytes = vec![0u8; (committee.len() + 1).div_ceil(8)];
    for bit in 0..=committee.len() {
        bytes[bit / 8] |= 1 << (bit % 8);
    }
    let attestation = Attestation {
        aggregation_bits: PresetBitList::from_ssz(&bytes, Preset::Minimal).unwrap(),
        data,
        signature,
    };
    process_attestation(&mut state, &attestation, &parameters).unwrap();
    for &index in &committee {
        assert_eq!(state.previous_epoch_participation.values[index as usize], 2);
    }
    let before = state.clone();
    let mut bad = attestation;
    bad.signature = [0; 96];
    assert!(matches!(
        process_attestation(&mut state, &bad, &parameters),
        Err(ProcessingError::Crypto(_))
    ));
    assert_eq!(state, before);
    state.slot = 16;
    assert!(matches!(
        process_attestation(&mut state, &bad, &parameters),
        Err(ProcessingError::InvalidState("attestation target epoch is inconsistent"))
    ));
}

#[test]
fn complete_cached_roots_preserve_deneb_header_and_historical_summary_fields() {
    let (mut state, _, parameters) = state_and_keys();
    state.slot = 7;
    state.next_withdrawal_index = 123;
    state.next_withdrawal_validator_index = 9;
    state.historical_roots.values.push([7; 32]);
    state.latest_execution_payload_header.blob_gas_used = 262_144;
    state.latest_execution_payload_header.excess_blob_gas = 393_216;
    let mut full = state.clone();
    let constants = checked_constants(&state, &parameters).unwrap();
    let mut cache = slot_cache::SlotCache::new(&state, &constants).unwrap();
    for _ in 0..73 {
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
    assert_eq!(cache.root().unwrap(), state.hash_tree_root(Preset::Minimal).unwrap());
    assert_eq!(state.historical_roots.values, vec![[7; 32]]);
    assert_eq!(state.historical_summaries.values.len(), 1);
    assert_eq!(state.latest_execution_payload_header.blob_gas_used, 262_144);
    assert_eq!(state.next_withdrawal_index, 123);
    assert_eq!(state.next_withdrawal_validator_index, 9);
}

#[test]
fn upgrade_retains_complete_payload_header_with_new_gas_fields_zeroed() {
    let (pre, _) = crate::bellatrix_processing::tests::state_and_keys();
    let parameters = DenebParameters::for_preset(Preset::Minimal);
    let mut pre = crate::capella_processing::upgrade_to_capella(&pre, &parameters.capella).unwrap();
    pre.latest_execution_payload_header.withdrawals_root = [44; 32];
    pre.latest_execution_payload_header.base_fee_per_gas = [55; 32];
    pre.next_withdrawal_index = 88;
    pre.next_withdrawal_validator_index = 9;
    let state = upgrade_to_deneb(&pre, &parameters).unwrap();
    assert_eq!(state.latest_execution_payload_header.withdrawals_root, [44; 32]);
    assert_eq!(state.latest_execution_payload_header.base_fee_per_gas, [55; 32]);
    assert_eq!(state.latest_execution_payload_header.blob_gas_used, 0);
    assert_eq!(state.latest_execution_payload_header.excess_blob_gas, 0);
    assert_eq!(state.next_withdrawal_index, 88);
    assert_eq!(state.next_withdrawal_validator_index, 9);
    assert_eq!(state.fork.previous_version, pre.fork.current_version);
    assert_eq!(state.fork.current_version, parameters.fork_version);
}

// This private adapter records required call ordering and rejection only, never EL/PoW validity.
struct FirstPayloadEngine {
    terminal_allowed: bool,
    calls: Mutex<Vec<&'static str>>,
}
impl ExecutionEngine for FirstPayloadEngine {
    type Error = std::io::Error;
    async fn verify_terminal_block(
        &self,
        _payload: &ExecutionPayload,
        context: ExecutionContext,
    ) -> Result<(), Self::Error> {
        assert_eq!(context.previous_execution_payload_hash, None);
        self.calls.lock().unwrap().push("terminal");
        if self.terminal_allowed {
            Ok(())
        } else {
            Err(std::io::Error::other("missing terminal proof"))
        }
    }
    async fn verify_payload(
        &self,
        payload: &ExecutionPayload,
        context: ExecutionContext,
    ) -> Result<(), Self::Error> {
        assert_eq!(context.previous_execution_payload_hash, None);
        assert_eq!(payload.parent_hash, [0; 32]);
        self.calls.lock().unwrap().push("payload");
        Ok(())
    }
}
#[test]
fn first_payload_requires_terminal_callback_before_payload_and_partial_header_is_not_empty() {
    let (mut state, _, parameters) = state_and_keys();
    state.latest_execution_payload_header = ExecutionPayloadHeader {
        parent_hash: [0; 32],
        fee_recipient: [0; 20],
        state_root: [0; 32],
        receipts_root: [0; 32],
        logs_bloom: Vector::from_vec(vec![0; 256]),
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
        blob_gas_used: 0,
        excess_blob_gas: 0,
    };
    let mut candidate = body(&state, &parameters);
    candidate.execution_payload.parent_hash = [0; 32];
    let before = state.clone();
    let missing = FirstPayloadEngine { terminal_allowed: false, calls: Mutex::new(Vec::new()) };
    assert!(matches!(
        ready(process_execution_payload(&mut state, &candidate, &parameters, &missing)),
        Err(ImportError::Execution(_))
    ));
    assert_eq!(state, before);
    assert_eq!(*missing.calls.lock().unwrap(), vec!["terminal"]);
    let rejected = FirstPayloadEngine { terminal_allowed: true, calls: Mutex::new(Vec::new()) };
    candidate.execution_payload.parent_hash = [27; 32];
    assert!(matches!(
        ready(process_execution_payload(&mut state, &candidate, &parameters, &rejected)),
        Err(ImportError::Consensus(ProcessingError::InvalidState(_)))
    ));
    assert_eq!(state, before);
    assert!(rejected.calls.lock().unwrap().is_empty());
    candidate.execution_payload.parent_hash = [0; 32];
    let accepted = FirstPayloadEngine { terminal_allowed: true, calls: Mutex::new(Vec::new()) };
    ready(process_execution_payload(&mut state, &candidate, &parameters, &accepted)).unwrap();
    assert_eq!(*accepted.calls.lock().unwrap(), vec!["terminal", "payload"]);
    assert_eq!(
        state.latest_execution_payload_header.block_hash,
        candidate.execution_payload.block_hash
    );
    let mut partial = before;
    candidate.execution_payload.parent_hash = [27; 32];
    partial.latest_execution_payload_header.gas_limit = 1;
    assert!(matches!(
        validate_execution_payload_consensus(
            &partial,
            &candidate.execution_payload,
            &candidate.blob_kzg_commitments.values,
            &parameters
        ),
        Err(ProcessingError::InvalidState("execution payload parent hash mismatch"))
    ));
}
