//! Exact Electra block sequencing and mandatory Engine V4 request verification.

use super::{
    checked_constants, common, execution_requests, operations, transact, with_altair, withdrawals,
    ElectraParameters, ExecutionContext, ExecutionEngine, ImportError,
};
use crate::{
    altair_processing,
    phase0_processing::{add, mul, position, Constants, ProcessingError},
    ssz::{
        electra::{
            BeaconBlock, BeaconBlockBody, BeaconState, ExecutionPayload, ExecutionPayloadHeader,
            SignedBeaconBlock, SyncAggregate,
        },
        Ssz,
    },
};
use reth_embedded_beacon_crypto::{compute_signing_root, verify_signature};
use reth_embedded_beacon_primitives::{sha256, uint64_root, BeaconBlockHeader, Root};

/// Derive the EIP-4844 versioned hash without asserting availability or KZG validity.
pub fn kzg_commitment_to_versioned_hash(commitment: &[u8; 48]) -> Root {
    let mut root = sha256(commitment);
    root[0] = 1;
    root
}
/// Select the Electra balance-weighted proposer from the authenticated state.
pub fn get_beacon_proposer_index(
    state: &BeaconState,
    parameters: &ElectraParameters,
) -> Result<u64, ProcessingError> {
    let constants = checked_constants(state, parameters)?;
    common::get_beacon_proposer_index(state, parameters, &constants)
}

pub(crate) fn verify_block_signature_kernel(
    state: &BeaconState,
    block: &SignedBeaconBlock,
    parameters: &ElectraParameters,
) -> Result<(), ProcessingError> {
    let index = common::index(block.message.proposer_index, state.validators.values.len())?;
    let root = compute_signing_root(
        block.message.hash_tree_root(parameters.preset())?,
        common::domain(state, [0; 4], block.message.slot / parameters.slots_per_epoch()),
    );
    Ok(verify_signature(&state.validators.values[index].pubkey, &root, &block.signature)?)
}
/// Verify only the outer proposer signature; complete import also verifies state, EL and root.
pub fn verify_block_signature(
    state: &BeaconState,
    block: &SignedBeaconBlock,
    parameters: &ElectraParameters,
) -> Result<(), ProcessingError> {
    checked_constants(state, parameters)?;
    block.hash_tree_root(parameters.preset())?;
    verify_block_signature_kernel(state, block, parameters)
}
pub(crate) fn header_kernel(
    state: &mut BeaconState,
    block: &BeaconBlock,
    parameters: &ElectraParameters,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    if block.slot != state.slot || block.slot <= state.latest_block_header.slot {
        return Err(ProcessingError::InvalidState("block slot does not match a newer state slot"));
    }
    let proposer = common::get_beacon_proposer_index(state, parameters, constants)?;
    if block.proposer_index != proposer {
        return Err(ProcessingError::InvalidState(
            "block proposer is not selected Electra proposer",
        ));
    }
    if block.parent_root != state.latest_block_header.hash_tree_root() {
        return Err(ProcessingError::InvalidState("block parent differs from latest beacon header"));
    }
    if state.validators.values[common::index(proposer, state.validators.values.len())?].slashed {
        return Err(ProcessingError::InvalidState("block proposer is slashed"));
    }
    state.latest_block_header = BeaconBlockHeader {
        slot: block.slot,
        proposer_index: proposer,
        parent_root: block.parent_root,
        state_root: [0; 32],
        body_root: block.body.hash_tree_root(parameters.preset())?,
    };
    Ok(())
}
/// Atomically process only the full Electra body header.
pub fn process_block_header(
    state: &mut BeaconState,
    block: &BeaconBlock,
    parameters: &ElectraParameters,
) -> Result<(), ProcessingError> {
    block.hash_tree_root(parameters.preset())?;
    transact(state, parameters, |candidate, constants| {
        header_kernel(candidate, block, parameters, constants)
    })
}

/// Pure payload consensus checks and complete header derivation; no EL verdict or import token.
pub fn validate_execution_payload_consensus(
    state: &BeaconState,
    body: &BeaconBlockBody,
    parameters: &ElectraParameters,
) -> Result<ExecutionPayloadHeader, ProcessingError> {
    checked_constants(state, parameters)?;
    body.hash_tree_root(parameters.preset())?;
    payload_header(state, &body.execution_payload, &body.blob_kzg_commitments.values, parameters)
}
fn payload_header(
    state: &BeaconState,
    payload: &ExecutionPayload,
    commitments: &[[u8; 48]],
    parameters: &ElectraParameters,
) -> Result<ExecutionPayloadHeader, ProcessingError> {
    if payload.parent_hash != state.latest_execution_payload_header.block_hash {
        return Err(ProcessingError::InvalidState("execution payload parent hash differs"));
    }
    let epoch = state.slot / parameters.slots_per_epoch();
    let mix = state
        .randao_mixes
        .values
        .get(position(
            epoch,
            match parameters.preset() {
                crate::ssz::Preset::Mainnet => 65_536,
                crate::ssz::Preset::Minimal => 64,
            },
        )?)
        .ok_or(ProcessingError::InvalidState("RANDAO ring is truncated"))?;
    if payload.prev_randao != *mix {
        return Err(ProcessingError::InvalidState("execution payload RANDAO differs"));
    }
    let timestamp = add(
        state.genesis_time,
        mul(
            state.slot,
            parameters.deneb.capella.bellatrix.seconds_per_slot,
            "execution slot duration",
        )?,
        "execution timestamp",
    )?;
    if payload.timestamp != timestamp {
        return Err(ProcessingError::InvalidState("execution payload timestamp differs"));
    }
    if commitments.len() as u64 > parameters.max_blobs_per_block {
        return Err(ProcessingError::InvalidState(
            "Electra blob commitment count exceeds network limit",
        ));
    }
    Ok(ExecutionPayloadHeader {
        parent_hash: payload.parent_hash,
        fee_recipient: payload.fee_recipient,
        state_root: payload.state_root,
        receipts_root: payload.receipts_root,
        logs_bloom: payload.logs_bloom.clone(),
        prev_randao: payload.prev_randao,
        block_number: payload.block_number,
        gas_limit: payload.gas_limit,
        gas_used: payload.gas_used,
        timestamp: payload.timestamp,
        extra_data: payload.extra_data.clone(),
        base_fee_per_gas: payload.base_fee_per_gas,
        block_hash: payload.block_hash,
        transactions_root: payload.transactions.hash_tree_root(parameters.preset())?,
        withdrawals_root: payload.withdrawals.hash_tree_root(parameters.preset())?,
        blob_gas_used: payload.blob_gas_used,
        excess_blob_gas: payload.excess_blob_gas,
    })
}
fn execution_context(
    state: &BeaconState,
    body: &BeaconBlockBody,
    parameters: &ElectraParameters,
) -> Result<ExecutionContext, ProcessingError> {
    Ok(ExecutionContext {
        slot: state.slot,
        parent_beacon_block_root: state.latest_block_header.parent_root,
        previous_execution_payload_hash: (!crate::common::execution_header_is_empty!(
            &state.latest_execution_payload_header,
            blob
        ))
        .then_some(state.latest_execution_payload_header.block_hash),
        expected_blob_versioned_hashes: body
            .blob_kzg_commitments
            .values
            .iter()
            .map(kzg_commitment_to_versioned_hash)
            .collect(),
        execution_requests_list: execution_requests::get_execution_requests_list(
            &body.execution_requests,
            parameters.preset(),
        )?,
    })
}
/// Atomically verify only the exact payload, blob hashes, parent beacon root and EIP-7685 groups.
/// A successful isolated operation does not verify the beacon signature, withdrawals or post-root.
pub async fn process_execution_payload<E: ExecutionEngine>(
    state: &mut BeaconState,
    body: &BeaconBlockBody,
    parameters: &ElectraParameters,
    engine: &E,
) -> Result<(), ImportError<E::Error>> {
    let header = validate_execution_payload_consensus(state, body, parameters)?;
    let context = execution_context(state, body, parameters)?;
    if context.previous_execution_payload_hash.is_none() {
        engine
            .verify_terminal_block(&body.execution_payload, context.clone())
            .await
            .map_err(ImportError::Execution)?;
    }
    engine
        .verify_payload(&body.execution_payload, context)
        .await
        .map_err(ImportError::Execution)?;
    state.latest_execution_payload_header = header;
    Ok(())
}

pub(crate) fn randao_kernel(
    state: &mut BeaconState,
    reveal: &[u8; 96],
    parameters: &ElectraParameters,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    let epoch = state.slot / constants.slots_per_epoch;
    let proposer = common::get_beacon_proposer_index(state, parameters, constants)?;
    let key =
        state.validators.values[common::index(proposer, state.validators.values.len())?].pubkey;
    verify_signature(
        &key,
        &compute_signing_root(uint64_root(epoch), common::domain(state, [2, 0, 0, 0], epoch)),
        reveal,
    )?;
    let mix = state
        .randao_mixes
        .values
        .get_mut(position(epoch, constants.historical_epochs)?)
        .ok_or(ProcessingError::InvalidState("RANDAO ring is truncated"))?;
    for (byte, hashed) in mix.iter_mut().zip(sha256(reveal)) {
        *byte ^= hashed;
    }
    Ok(())
}
/// Verify the Electra selected proposer's RANDAO signature before atomically changing the mix.
pub fn process_randao(
    state: &mut BeaconState,
    body: &BeaconBlockBody,
    parameters: &ElectraParameters,
) -> Result<(), ProcessingError> {
    body.hash_tree_root(parameters.preset())?;
    transact(state, parameters, |candidate, constants| {
        randao_kernel(candidate, &body.randao_reveal, parameters, constants)
    })
}
/// Apply only the unchanged Eth1 majority-vote operation using exact inherited constants.
pub fn process_eth1_data(
    state: &mut BeaconState,
    body: &BeaconBlockBody,
    parameters: &ElectraParameters,
) -> Result<(), ProcessingError> {
    body.hash_tree_root(parameters.preset())?;
    transact(state, parameters, |candidate, constants| {
        with_altair(candidate, |view| {
            altair_processing::block::eth1_vote_kernel(view, &body.eth1_data, constants)
        })
    })
}
/// Verify the sync aggregate using Electra proposer selection and exact rewards.
pub fn process_sync_aggregate(
    state: &mut BeaconState,
    aggregate: &SyncAggregate,
    parameters: &ElectraParameters,
) -> Result<(), ProcessingError> {
    aggregate.hash_tree_root(parameters.preset())?;
    transact(state, parameters, |candidate, constants| {
        common::sync_aggregate_kernel(candidate, aggregate, parameters, constants)
    })
}

/// Raw complete block sequence; callers own candidate isolation, slots, outer BLS and post-root.
pub(crate) async fn process_block<E: ExecutionEngine>(
    state: &mut BeaconState,
    block: &BeaconBlock,
    parameters: &ElectraParameters,
    constants: &Constants,
    engine: &E,
) -> Result<(), ImportError<E::Error>> {
    header_kernel(state, block, parameters, constants)?;
    withdrawals::withdrawals_kernel(state, &block.body.execution_payload, parameters)?;
    let header = validate_execution_payload_consensus(state, &block.body, parameters)?;
    let context = execution_context(state, &block.body, parameters)?;
    if context.previous_execution_payload_hash.is_none() {
        engine
            .verify_terminal_block(&block.body.execution_payload, context.clone())
            .await
            .map_err(ImportError::Execution)?;
    }
    engine
        .verify_payload(&block.body.execution_payload, context)
        .await
        .map_err(ImportError::Execution)?;
    state.latest_execution_payload_header = header;
    randao_kernel(state, &block.body.randao_reveal, parameters, constants)?;
    with_altair(state, |view| {
        altair_processing::block::eth1_vote_kernel(view, &block.body.eth1_data, constants)
    })?;
    operations::operations_kernel(state, &block.body, parameters, constants)?;
    common::sync_aggregate_kernel(state, &block.body.sync_aggregate, parameters, constants)?;
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::ssz::{electra, List, Preset, PresetBits, Vector};
    use blst::min_pk::SecretKey;
    use reth_embedded_beacon_crypto::ETHEREUM_BLS_DST;
    use std::{
        future::Future,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Mutex,
        },
        task::{Context, Poll, Waker},
    };

    pub(crate) fn state_and_keys() -> (BeaconState, Vec<SecretKey>, ElectraParameters) {
        let (mut bellatrix, keys) = crate::bellatrix_processing::tests::state_and_keys();
        bellatrix.latest_execution_payload_header.block_hash = [9; 32];
        let parameters = ElectraParameters::for_preset(Preset::Minimal);
        let capella =
            crate::capella_processing::upgrade_to_capella(&bellatrix, &parameters.deneb.capella)
                .unwrap();
        let deneb = crate::deneb_processing::upgrade_to_deneb(&capella, &parameters.deneb).unwrap();
        let state = super::super::upgrade_to_electra(&deneb, &parameters).unwrap();
        (state, keys, parameters)
    }
    pub(crate) fn payload(state: &BeaconState, parameters: &ElectraParameters) -> ExecutionPayload {
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
            timestamp: state.genesis_time +
                state.slot * parameters.deneb.capella.bellatrix.seconds_per_slot,
            extra_data: List::default(),
            base_fee_per_gas: [0; 32],
            block_hash: [10; 32],
            transactions: List::default(),
            withdrawals: List::from_vec(
                withdrawals::get_expected_withdrawals(state, parameters).unwrap().withdrawals,
            ),
            blob_gas_used: 0,
            excess_blob_gas: 0,
        }
    }
    pub(crate) fn body(state: &BeaconState, parameters: &ElectraParameters) -> BeaconBlockBody {
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
            blob_kzg_commitments: List::default(),
            execution_requests: electra::ExecutionRequests {
                deposits: List::default(),
                withdrawals: List::default(),
                consolidations: List::default(),
            },
        }
    }
    pub(crate) fn ready<F: Future>(future: F) -> F::Output {
        let mut context = Context::from_waker(Waker::noop());
        let mut future = std::pin::pin!(future);
        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => value,
            Poll::Pending => panic!("private test callback must be immediately ready"),
        }
    }
    // Private callback verifies submission/atomicity only; it proves no real execution validity.
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
            Err(std::io::Error::other("test terminal proof unavailable"))
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
                Err(std::io::Error::other("private test rejection"))
            }
        }
    }
    struct PendingEngine(AtomicUsize);
    impl ExecutionEngine for PendingEngine {
        type Error = std::io::Error;
        async fn verify_terminal_block(
            &self,
            _payload: &ExecutionPayload,
            _context: ExecutionContext,
        ) -> Result<(), Self::Error> {
            Err(std::io::Error::other("test terminal proof unavailable"))
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
    fn signed_candidate(
        pre: &BeaconState,
        keys: &[SecretKey],
        parameters: &ElectraParameters,
    ) -> SignedBeaconBlock {
        let mut candidate = pre.clone();
        super::super::process_slots(&mut candidate, pre.slot + 1, parameters).unwrap();
        let proposer = get_beacon_proposer_index(&candidate, parameters).unwrap();
        let key = &keys[proposer as usize];
        let epoch = candidate.slot / parameters.slots_per_epoch();
        let randao_root = compute_signing_root(
            uint64_root(epoch),
            common::domain(&candidate, [2, 0, 0, 0], epoch),
        );
        let mut body = body(&candidate, parameters);
        body.randao_reveal = key.sign(&randao_root, ETHEREUM_BLS_DST, &[]).to_bytes();
        body.blob_kzg_commitments.values.extend([[31; 48], [32; 48]]);
        body.execution_requests.deposits.values.push(electra::DepositRequest {
            pubkey: [42; 48],
            withdrawal_credentials: [0; 32],
            amount: 7,
            signature: [0; 96],
            index: 100,
        });
        let mut block = BeaconBlock {
            slot: candidate.slot,
            proposer_index: proposer,
            parent_root: candidate.latest_block_header.hash_tree_root(),
            state_root: [0; 32],
            body,
        };
        let constants = checked_constants(&candidate, parameters).unwrap();
        ready(process_block(
            &mut candidate,
            &block,
            parameters,
            &constants,
            &RecordingEngine::new(true),
        ))
        .unwrap();
        block.state_root = candidate.hash_tree_root(parameters.preset()).unwrap();
        let root = compute_signing_root(
            block.hash_tree_root(parameters.preset()).unwrap(),
            common::domain(&candidate, [0; 4], epoch),
        );
        SignedBeaconBlock {
            message: block,
            signature: key.sign(&root, ETHEREUM_BLS_DST, &[]).to_bytes(),
        }
    }
    #[test]
    fn exact_v4_context_is_mandatory_and_rejected_or_cancelled_import_does_not_publish_state() {
        let (mut state, keys, parameters) = state_and_keys();
        let block = signed_candidate(&state, &keys, &parameters);
        let before = state.clone();
        let rejected = RecordingEngine::new(false);
        assert!(matches!(
            ready(super::super::state_transition(&mut state, &block, &parameters, &rejected)),
            Err(ImportError::Execution(_))
        ));
        assert_eq!(state, before);
        assert_eq!(rejected.calls.load(Ordering::SeqCst), 1);
        let pending = PendingEngine(AtomicUsize::new(0));
        {
            let mut future =
                Box::pin(super::super::state_transition(&mut state, &block, &parameters, &pending));
            let mut context = Context::from_waker(Waker::noop());
            assert!(matches!(future.as_mut().poll(&mut context), Poll::Pending));
        }
        assert_eq!(state, before);
        assert_eq!(pending.0.load(Ordering::SeqCst), 1);
        let accepted = RecordingEngine::new(true);
        ready(super::super::state_transition(&mut state, &block, &parameters, &accepted)).unwrap();
        let (_, context) = accepted.observed.lock().unwrap().clone().unwrap();
        assert_eq!(context.slot, block.message.slot);
        assert_eq!(context.parent_beacon_block_root, block.message.parent_root);
        assert_eq!(context.previous_execution_payload_hash, Some([9; 32]));
        assert_eq!(
            context.expected_blob_versioned_hashes,
            vec![
                kzg_commitment_to_versioned_hash(&[31; 48]),
                kzg_commitment_to_versioned_hash(&[32; 48])
            ]
        );
        let request = &block.message.body.execution_requests.deposits.values[0];
        let mut bytes = vec![0];
        bytes.extend_from_slice(&request.pubkey);
        bytes.extend_from_slice(&request.withdrawal_credentials);
        bytes.extend_from_slice(&request.amount.to_le_bytes());
        bytes.extend_from_slice(&request.signature);
        bytes.extend_from_slice(&request.index.to_le_bytes());
        assert_eq!(context.execution_requests_list, vec![bytes]);
        assert_eq!(state.pending_deposits.values.len(), 1);
        assert_eq!(state.pending_deposits.values[0].amount, 7);
    }
    #[test]
    fn invalid_signature_blob_limit_and_payload_parent_fail_before_execution_callback() {
        let (mut state, keys, parameters) = state_and_keys();
        let mut signed = signed_candidate(&state, &keys, &parameters);
        let before = state.clone();
        let engine = RecordingEngine::new(true);
        signed.signature = [0; 96];
        assert!(ready(super::super::state_transition(&mut state, &signed, &parameters, &engine))
            .is_err());
        assert_eq!(state, before);
        assert_eq!(engine.calls.load(Ordering::SeqCst), 0);
        let mut body = body(&state, &parameters);
        body.blob_kzg_commitments.values = vec![[1; 48]; 10];
        assert!(ready(process_execution_payload(&mut state, &body, &parameters, &engine)).is_err());
        assert_eq!(state, before);
        assert_eq!(engine.calls.load(Ordering::SeqCst), 0);
        body.blob_kzg_commitments.values.clear();
        body.execution_payload.parent_hash = [20; 32];
        assert!(ready(process_execution_payload(&mut state, &body, &parameters, &engine)).is_err());
        assert_eq!(state, before);
        assert_eq!(engine.calls.load(Ordering::SeqCst), 0);
    }
    // Private proof adapter validates dispatch ordering, not real terminal eligibility.
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
    fn empty_execution_header() -> ExecutionPayloadHeader {
        ExecutionPayloadHeader {
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
        }
    }
    #[test]
    fn first_payload_requires_terminal_proof_then_exact_v4_payload_and_full_header_detection() {
        let (mut state, _, parameters) = state_and_keys();
        state.latest_execution_payload_header = empty_execution_header();
        let mut body = body(&state, &parameters);
        body.execution_payload.parent_hash = [0; 32];
        let before = state.clone();
        let rejecting =
            FirstPayloadEngine { terminal_allowed: false, calls: Mutex::new(Vec::new()) };
        assert!(matches!(
            ready(process_execution_payload(&mut state, &body, &parameters, &rejecting)),
            Err(ImportError::Execution(_))
        ));
        assert_eq!(state, before);
        let context = execution_context(&state, &body, &parameters).unwrap();
        assert_eq!(context.previous_execution_payload_hash, None);
        assert_eq!(
            *rejecting.calls.lock().unwrap(),
            vec![("terminal", body.execution_payload.clone(), context.clone())]
        );
        let accepting =
            FirstPayloadEngine { terminal_allowed: true, calls: Mutex::new(Vec::new()) };
        ready(process_execution_payload(&mut state, &body, &parameters, &accepting)).unwrap();
        assert_eq!(
            *accepting.calls.lock().unwrap(),
            vec![
                ("terminal", body.execution_payload.clone(), context.clone()),
                ("payload", body.execution_payload.clone(), context)
            ]
        );
        assert_eq!(state.latest_execution_payload_header.parent_hash, [0; 32]);
        let mut bad_parent = before.clone();
        let mut incorrect = body.clone();
        incorrect.execution_payload.parent_hash = [44; 32];
        let engine = FirstPayloadEngine { terminal_allowed: true, calls: Mutex::new(Vec::new()) };
        assert!(matches!(
            ready(process_execution_payload(&mut bad_parent, &incorrect, &parameters, &engine)),
            Err(ImportError::Consensus(ProcessingError::InvalidState(
                "execution payload parent hash differs"
            )))
        ));
        assert!(engine.calls.lock().unwrap().is_empty());
        assert_eq!(bad_parent, before);
        let mut partial = before;
        partial.latest_execution_payload_header.extra_data.values.push(1);
        assert_eq!(
            execution_context(&partial, &body, &parameters)
                .unwrap()
                .previous_execution_payload_hash,
            Some([0; 32])
        );
        body.execution_payload.parent_hash = [44; 32];
        let before = partial.clone();
        let engine = FirstPayloadEngine { terminal_allowed: true, calls: Mutex::new(Vec::new()) };
        assert!(matches!(
            ready(process_execution_payload(&mut partial, &body, &parameters, &engine)),
            Err(ImportError::Consensus(ProcessingError::InvalidState(
                "execution payload parent hash differs"
            )))
        ));
        assert!(engine.calls.lock().unwrap().is_empty());
        assert_eq!(partial, before);
    }
}

#[cfg(feature = "spec-tests")]
#[path = "blocks_artificial.rs"]
pub(crate) mod artificial;
