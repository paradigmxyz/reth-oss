//! Fulu changed-rule regressions, complete-root cache and strict signed import checks.

use super::*;
use crate::ssz::{
    fulu::{
        BeaconBlock, BeaconBlockBody, DepositRequest, ExecutionPayloadHeader, ExecutionRequests,
        PendingDeposit, SignedBeaconBlock, SyncAggregate,
    },
    List, PresetBits, Ssz, Vector,
};
use blst::min_pk::SecretKey;
use reth_embedded_beacon_crypto::{compute_domain, compute_signing_root, ETHEREUM_BLS_DST};
use reth_embedded_beacon_primitives::uint64_root;
use std::{
    sync::Mutex,
    task::{Context, Poll, Waker},
};

pub(crate) fn state_and_keys() -> (BeaconState, Vec<SecretKey>, FuluParameters) {
    let (pre, keys, _) = crate::electra_processing::tests::state_and_keys();
    let parameters = FuluParameters::for_preset(Preset::Minimal);
    (upgrade_to_fulu(&pre, &parameters).unwrap(), keys, parameters)
}
fn body(state: &BeaconState, parameters: &FuluParameters) -> BeaconBlockBody {
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
        execution_payload: ExecutionPayload {
            parent_hash: state.latest_execution_payload_header.block_hash,
            fee_recipient: [0; 20],
            state_root: [0; 32],
            receipts_root: [0; 32],
            logs_bloom: Vector::from_vec(vec![0; 256]),
            prev_randao: state.randao_mixes.values[(state.slot / 8 % 64) as usize],
            block_number: state.latest_execution_payload_header.block_number + 1,
            gas_limit: 30_000_000,
            gas_used: 0,
            timestamp: state.genesis_time +
                state.slot * parameters.electra.deneb.capella.bellatrix.seconds_per_slot,
            extra_data: List::default(),
            base_fee_per_gas: [0; 32],
            block_hash: [10; 32],
            transactions: List::default(),
            withdrawals: List::default(),
            blob_gas_used: 262_144,
            excess_blob_gas: 393_216,
        },
        bls_to_execution_changes: List::default(),
        blob_kzg_commitments: List::from_vec(vec![[1; 48], [2; 48]]),
        execution_requests: ExecutionRequests {
            deposits: List::default(),
            withdrawals: List::default(),
            consolidations: List::default(),
        },
    }
}
pub(crate) fn candidate(
    pre: &BeaconState,
    keys: &[SecretKey],
    parameters: &FuluParameters,
) -> (SignedBeaconBlock, BeaconState) {
    let mut expected = pre.clone();
    process_slots(&mut expected, pre.slot + 1, parameters).unwrap();
    let proposer = get_beacon_proposer_index(&expected, parameters).unwrap();
    let key = &keys[proposer as usize];
    let root = compute_signing_root(
        uint64_root(expected.slot / parameters.slots_per_epoch()),
        compute_domain(
            [2, 0, 0, 0],
            expected.fork.current_version,
            expected.genesis_validators_root,
        ),
    );
    let mut body = body(&expected, parameters);
    body.randao_reveal = key.sign(&root, ETHEREUM_BLS_DST, &[]).to_bytes();
    let mut message = BeaconBlock {
        slot: expected.slot,
        proposer_index: proposer,
        parent_root: expected.latest_block_header.hash_tree_root(),
        state_root: [0; 32],
        body,
    };
    process_block_header(&mut expected, &message, parameters).unwrap();
    process_withdrawals(&mut expected, &message.body.execution_payload, parameters).unwrap();
    expected.latest_execution_payload_header =
        validate_execution_payload_consensus(&expected, &message.body, parameters).unwrap();
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
    let mut future = std::pin::pin!(future);
    let mut context = Context::from_waker(Waker::noop());
    match future.as_mut().poll(&mut context) {
        Poll::Ready(result) => result,
        Poll::Pending => panic!("private fixture adapter must be immediately ready"),
    }
}
// This private adapter proves exact callback inputs and rollback only, never EL validity.
struct Engine {
    allowed: bool,
    observed: Mutex<Option<(ExecutionPayload, ExecutionContext)>>,
}
impl ExecutionEngine for Engine {
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
        *self.observed.lock().unwrap() = Some((payload.clone(), context));
        if self.allowed {
            Ok(())
        } else {
            Err(std::io::Error::other("private execution rejection"))
        }
    }
}
#[test]
fn upgrade_preserves_pending_state_and_uses_committed_lookahead() {
    let (mut state, _, parameters) = state_and_keys();
    assert_eq!(state.proposer_lookahead.values.len(), 16);
    assert_eq!(
        state.proposer_lookahead.values[..8],
        get_beacon_proposer_indices(&state, 0, &parameters).unwrap().values
    );
    assert_eq!(
        state.proposer_lookahead.values[8..],
        get_beacon_proposer_indices(&state, 1, &parameters).unwrap().values
    );
    let computed = get_beacon_proposer_index(&state, &parameters).unwrap();
    let forced = (computed + 1) % 32;
    state.proposer_lookahead.values[0] = forced;
    assert_eq!(get_beacon_proposer_index(&state, &parameters).unwrap(), forced);
    state.slot = 7;
    let second = state.proposer_lookahead.values[8..].to_vec();
    let future = get_beacon_proposer_indices(&state, 2, &parameters).unwrap().values;
    process_proposer_lookahead(&mut state, &parameters).unwrap();
    assert_eq!(state.proposer_lookahead.values[..8], second);
    assert_eq!(state.proposer_lookahead.values[8..], future);
}
#[test]
fn pending_requests_ignore_legacy_bridge_and_do_not_update_start_index() {
    let (mut state, _, parameters) = state_and_keys();
    state.slot = 16;
    state.finalized_checkpoint.epoch = 2;
    let request = DepositRequest {
        pubkey: state.validators.values[0].pubkey,
        withdrawal_credentials: state.validators.values[0].withdrawal_credentials,
        amount: 5,
        signature: [0; 96],
        index: 17,
    };
    process_deposit_request(&mut state, &request, &parameters).unwrap();
    assert_eq!(state.deposit_requests_start_index, u64::MAX);
    process_pending_deposits(&mut state, &parameters).unwrap();
    assert!(state.pending_deposits.values.is_empty());
    assert_eq!(state.balances.values[0], 32_000_000_005);
}
#[test]
fn blob_only_schedule_uses_exact_boundaries_and_electra_fallback_epoch() {
    let (mut state, _, mut parameters) = state_and_keys();
    parameters.fork_epoch = 0;
    parameters.electra_fork_epoch = 0;
    parameters.blob_schedule = vec![
        BlobParameters { epoch: 1, max_blobs_per_block: 10 },
        BlobParameters { epoch: 2, max_blobs_per_block: 11 },
    ];
    assert_eq!(
        parameters.blob_parameters_at_epoch(0),
        BlobParameters { epoch: 0, max_blobs_per_block: 9 }
    );
    assert_eq!(parameters.blob_parameters_at_epoch(1).max_blobs_per_block, 10);
    assert_eq!(parameters.blob_parameters_at_epoch(2).max_blobs_per_block, 11);
    let mut body = body(&state, &parameters);
    body.blob_kzg_commitments.values.resize(10, [1; 48]);
    assert!(validate_execution_payload_consensus(&state, &body, &parameters).is_err());
    state.slot = 8;
    body.execution_payload.timestamp = 48;
    validate_execution_payload_consensus(&state, &body, &parameters).unwrap();
    parameters.blob_schedule.push(BlobParameters { epoch: 2, max_blobs_per_block: 12 });
    assert!(matches!(
        parameters.validate(),
        Err(ProcessingError::InvalidParameters("invalid Fulu blob schedule"))
    ));
    let main = FuluParameters::for_preset(Preset::Mainnet);
    assert_eq!(main.blob_parameters_at_epoch(411_392).epoch, 364_032);
    assert_eq!(main.blob_parameters_at_epoch(412_672).max_blobs_per_block, 15);
    assert_eq!(main.blob_parameters_at_epoch(419_072).max_blobs_per_block, 21);
}
#[test]
fn signed_import_uses_precomputed_proposer_and_mandatory_v4_callback_atomically() {
    let (mut state, keys, parameters) = state_and_keys();
    let before = state.clone();
    let (block, expected) = candidate(&state, &keys, &parameters);
    let reject = Engine { allowed: false, observed: Mutex::new(None) };
    assert!(matches!(
        ready(state_transition(&mut state, &block, &parameters, &reject)),
        Err(ImportError::Execution(_))
    ));
    assert_eq!(state, before);
    let accept = Engine { allowed: true, observed: Mutex::new(None) };
    ready(state_transition(&mut state, &block, &parameters, &accept)).unwrap();
    assert_eq!(state, expected);
    let mut replayed = before;
    process_slots(&mut replayed, block.message.slot, &parameters).unwrap();
    ready(state_transition(&mut replayed, &block, &parameters, &accept)).unwrap();
    assert_eq!(replayed, expected);
    let observed = accept.observed.lock().unwrap();
    let (payload, context) = observed.as_ref().unwrap();
    assert_eq!(payload, &block.message.body.execution_payload);
    assert_eq!(context.parent_beacon_block_root, block.message.parent_root);
    assert_eq!(context.previous_execution_payload_hash, Some([9; 32]));
    assert_eq!(context.execution_requests_list, Vec::<Vec<u8>>::new());
}
#[test]
fn complete_cached_roots_track_lookahead_and_pending_deposits_across_ring_wrap() {
    let (mut state, _, parameters) = state_and_keys();
    state.slot = 7;
    state.historical_roots.values.push([7; 32]);
    let deposit = PendingDeposit {
        pubkey: state.validators.values[0].pubkey,
        withdrawal_credentials: state.validators.values[0].withdrawal_credentials,
        amount: 5,
        signature: [0; 96],
        slot: 0,
    };
    state.pending_deposits = List::from_vec(vec![deposit]);
    let constants = checked_constants(&state, &parameters).unwrap();
    let mut full = state.clone();
    let mut cache = slots::SlotCache::new(&state, &constants).unwrap();
    for _ in 0..66 {
        assert_eq!(cache.root().unwrap(), state.hash_tree_root(Preset::Minimal).unwrap());
        cache.process_slot(&mut state).unwrap();
        slots::slot_kernel(&mut full, &constants).unwrap();
        let next = state.slot + 1;
        if next.is_multiple_of(parameters.slots_per_epoch()) {
            epochs::epoch_kernel(&mut state, &parameters, &constants).unwrap();
            epochs::epoch_kernel(&mut full, &parameters, &constants).unwrap();
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
    assert!(state.pending_deposits.values.is_empty());
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
        validate_execution_payload_consensus(&partial, &candidate, &parameters),
        Err(ProcessingError::InvalidState("execution payload parent hash differs"))
    ));
}

#[test]
fn checked_post_root_tracks_changed_state_and_rejects_indexed_length_mismatch() {
    let (mut state, _, parameters) = state_and_keys();
    let (_, original) = checked_constants_with_root(&state, &parameters).unwrap();
    assert_eq!(original, state.hash_tree_root(parameters.preset()).unwrap());
    state.balances.values[0] += 1;
    let (_, changed) = checked_constants_with_root(&state, &parameters).unwrap();
    assert_ne!(changed, original);
    assert_eq!(changed, state.hash_tree_root(parameters.preset()).unwrap());
    state.balances.values.pop();
    assert!(state.hash_tree_root(parameters.preset()).is_ok());
    assert!(matches!(
        checked_constants_with_root(&state, &parameters),
        Err(ProcessingError::InvalidState("Fulu validator-indexed list lengths differ"))
    ));
}

#[test]
fn valid_outer_signature_cannot_publish_an_incorrect_post_state_root() {
    let (mut state, keys, parameters) = state_and_keys();
    let before = state.clone();
    let (mut block, expected) = candidate(&state, &keys, &parameters);
    block.message.state_root[0] ^= 1;
    let signing_root = compute_signing_root(
        block.message.hash_tree_root(parameters.preset()).unwrap(),
        compute_domain([0; 4], expected.fork.current_version, expected.genesis_validators_root),
    );
    block.signature = keys[block.message.proposer_index as usize]
        .sign(&signing_root, ETHEREUM_BLS_DST, &[])
        .to_bytes();
    let engine = Engine { allowed: true, observed: Mutex::new(None) };
    assert!(matches!(
        ready(state_transition(&mut state, &block, &parameters, &engine)),
        Err(ImportError::Consensus(ProcessingError::InvalidState(
            "block post-state root mismatch"
        )))
    ));
    assert!(engine.observed.lock().unwrap().is_some());
    assert_eq!(state, before);
}

#[test]
fn disposable_import_matches_public_transaction_and_retains_late_root_rejection() {
    let (pre, keys, parameters) = state_and_keys();
    let (block, expected) = candidate(&pre, &keys, &parameters);
    let engine = Engine { allowed: true, observed: Mutex::new(None) };
    let mut disposable = pre.clone();
    ready(state_transition_in_place(&mut disposable, &block, &parameters, &engine)).unwrap();
    assert_eq!(disposable, expected);
    let mut published = pre.clone();
    ready(state_transition(&mut published, &block, &parameters, &engine)).unwrap();
    assert_eq!(published, disposable);

    let mut invalid = block;
    invalid.message.state_root[0] ^= 1;
    let signing_root = compute_signing_root(
        invalid.message.hash_tree_root(parameters.preset()).unwrap(),
        compute_domain([0; 4], expected.fork.current_version, expected.genesis_validators_root),
    );
    invalid.signature = keys[invalid.message.proposer_index as usize]
        .sign(&signing_root, ETHEREUM_BLS_DST, &[])
        .to_bytes();
    let mut disposable = pre.clone();
    assert!(matches!(
        ready(state_transition_in_place(&mut disposable, &invalid, &parameters, &engine)),
        Err(ImportError::Consensus(ProcessingError::InvalidState(
            "block post-state root mismatch"
        )))
    ));
    assert_eq!(disposable, expected);
    let mut published = pre.clone();
    assert!(matches!(
        ready(state_transition(&mut published, &invalid, &parameters, &engine)),
        Err(ImportError::Consensus(ProcessingError::InvalidState(
            "block post-state root mismatch"
        )))
    ));
    assert_eq!(published, pre);
}

#[test]
fn structural_guards_preserve_complete_root_errors_and_atomic_replay() {
    let (state, _, parameters) = state_and_keys();
    for mutation in 0..6 {
        let mut invalid = state.clone();
        match mutation {
            0 => {
                invalid.block_roots.values.pop();
            }
            1 => {
                invalid.current_sync_committee.pubkeys.values.pop();
            }
            2 => {
                invalid.latest_execution_payload_header.extra_data.values = vec![0; 33];
            }
            3 => {
                invalid.proposer_lookahead.values.pop();
            }
            4 => {
                invalid.current_epoch_participation.values.pop();
            }
            _ => {
                invalid.balances.values.pop();
            }
        }
        let guard = checked_constants(&invalid, &parameters).map(|_| ());
        let committed = checked_constants_with_root(&invalid, &parameters).map(|_| ());
        assert!(guard.is_err());
        assert_eq!(format!("{guard:?}"), format!("{committed:?}"));
        let before = invalid.clone();
        let target = invalid.slot + 1;
        assert!(process_slots(&mut invalid, target, &parameters).is_err());
        assert_eq!(invalid, before);
    }
}
