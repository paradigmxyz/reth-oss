//! Gloas parent payload accounting and pure signed envelope verification.
//! Envelope verification does not mutate state, import a block, or attest to blob availability.

use super::{
    builder, checked_constants, common, execution_requests, transact, ExecutionContext,
    ExecutionEngine, GloasParameters, ImportError,
};
use crate::{
    phase0_processing::{add, mul, position, Constants, ProcessingError},
    ssz::{
        gloas::{BeaconBlock, BeaconState, ExecutionRequests, SignedExecutionPayloadEnvelope},
        Ssz,
    },
};
use reth_embedded_beacon_crypto::{compute_signing_root, verify_signature};
use reth_embedded_beacon_primitives::{sha256, Root};

/// Derive an ordered execution versioned hash, without claiming KZG or availability validation.
pub fn kzg_commitment_to_versioned_hash(commitment: &[u8; 48]) -> Root {
    let mut root = sha256(commitment);
    root[0] = 1;
    root
}

pub(crate) fn apply_parent_execution_payload_kernel(
    state: &mut BeaconState,
    requests: &ExecutionRequests,
    parameters: &GloasParameters,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    if requests.withdrawals.values.len() > 16 ||
        requests.consolidations.values.len() > 2 ||
        requests.builder_deposits.values.len() > 64 ||
        requests.builder_exits.values.len() > 16
    {
        return Err(ProcessingError::InvalidState(
            "parent execution request group exceeds consensus limit",
        ));
    }
    let parent_slot = state.latest_block_header.slot;
    let parent_epoch = parent_slot / parameters.slots_per_epoch();
    let current = state.slot / parameters.slots_per_epoch();
    let parent_hash = state.latest_execution_payload_bid.block_hash;
    if parent_epoch == current {
        builder::settle_builder_payment_kernel(
            state,
            parameters.slots_per_epoch() + parent_slot % parameters.slots_per_epoch(),
            parameters,
        )?;
    } else if parent_epoch == current.saturating_sub(1) {
        builder::settle_builder_payment_kernel(
            state,
            parent_slot % parameters.slots_per_epoch(),
            parameters,
        )?;
    } else if state.latest_execution_payload_bid.value > 0 {
        let bid = &state.latest_execution_payload_bid;
        state.builder_pending_withdrawals.values.push(
            crate::ssz::gloas::BuilderPendingWithdrawal {
                fee_recipient: bid.fee_recipient,
                amount: bid.value,
                builder_index: bid.builder_index,
            },
        );
    }
    for request in &requests.deposits.values {
        execution_requests::deposit_request_kernel(state, request)?;
    }
    for request in &requests.withdrawals.values {
        execution_requests::withdrawal_request_kernel(state, request, parameters, constants)?;
    }
    for request in &requests.consolidations.values {
        execution_requests::consolidation_request_kernel(state, request, parameters, constants)?;
    }
    for request in &requests.builder_deposits.values {
        builder::builder_deposit_kernel(state, request, parameters)?;
    }
    for request in &requests.builder_exits.values {
        builder::builder_exit_kernel(state, request, parameters)?;
    }
    state
        .execution_payload_availability
        .bits
        .set(position(parent_slot, constants.historical_slots)?, true)?;
    state.latest_block_hash = parent_hash;
    Ok(())
}
/// Apply the exact parent request groups at the child's slot, settling builder payment first.
/// Isolated invocation requires the caller to authenticate the parent bid and execution payload.
pub fn apply_parent_execution_payload(
    state: &mut BeaconState,
    requests: &ExecutionRequests,
    parameters: &GloasParameters,
) -> Result<(), ProcessingError> {
    requests.hash_tree_root(parameters.preset())?;
    transact(state, parameters, |candidate, constants| {
        apply_parent_execution_payload_kernel(candidate, requests, parameters, constants)
    })
}
pub(crate) fn parent_execution_kernel(
    state: &mut BeaconState,
    block: &BeaconBlock,
    parameters: &GloasParameters,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    let requests = &block.body.parent_execution_requests;
    if block.body.signed_execution_payload_bid.message.parent_block_hash !=
        state.latest_execution_payload_bid.block_hash
    {
        if !requests.deposits.values.is_empty() ||
            !requests.withdrawals.values.is_empty() ||
            !requests.consolidations.values.is_empty() ||
            !requests.builder_deposits.values.is_empty() ||
            !requests.builder_exits.values.is_empty()
        {
            return Err(ProcessingError::InvalidState(
                "EMPTY parent cannot carry execution requests",
            ));
        }
        return Ok(());
    }
    if requests.hash_tree_root(parameters.preset())? !=
        state.latest_execution_payload_bid.execution_requests_root
    {
        return Err(ProcessingError::InvalidState(
            "parent execution requests differ from committed bid",
        ));
    }
    apply_parent_execution_payload_kernel(state, requests, parameters, constants)
}
/// Validate the parent's FULL/EMPTY selection and committed requests before changing its bid.
pub fn process_parent_execution_payload(
    state: &mut BeaconState,
    block: &BeaconBlock,
    parameters: &GloasParameters,
) -> Result<(), ProcessingError> {
    block.hash_tree_root(parameters.preset())?;
    transact(state, parameters, |candidate, constants| {
        parent_execution_kernel(candidate, block, parameters, constants)
    })
}
fn envelope_signature_kernel(
    state: &BeaconState,
    signed: &SignedExecutionPayloadEnvelope,
    parameters: &GloasParameters,
) -> Result<(), ProcessingError> {
    let builder_index = signed.message.builder_index;
    let pubkey = if builder_index == builder::BUILDER_INDEX_SELF_BUILD {
        state.validators.values[common::index(
            state.latest_block_header.proposer_index,
            state.validators.values.len(),
        )?]
        .pubkey
    } else {
        state.builders.values[common::index(builder_index, state.builders.values.len())?].pubkey
    };
    let root = compute_signing_root(
        signed.message.hash_tree_root(parameters.preset())?,
        common::domain(state, [11, 0, 0, 0], state.slot / parameters.slots_per_epoch()),
    );
    Ok(verify_signature(&pubkey, &root, &signed.signature)?)
}
/// Verify the envelope's registered builder or selected proposer signature only.
pub fn verify_execution_payload_envelope_signature(
    state: &BeaconState,
    signed: &SignedExecutionPayloadEnvelope,
    parameters: &GloasParameters,
) -> Result<(), ProcessingError> {
    checked_constants(state, parameters)?;
    signed.hash_tree_root(parameters.preset())?;
    envelope_signature_kernel(state, signed, parameters)
}
/// Verify all pinned beacon/bid/payload commitments, its strict signature, and mandatory Engine V5.
/// Success is only envelope verification; known-block custody and blob data availability remain
/// mandatory independent fork-choice requirements. No validated block token is created here.
pub async fn verify_execution_payload_envelope<E: ExecutionEngine>(
    state: &BeaconState,
    signed: &SignedExecutionPayloadEnvelope,
    parameters: &GloasParameters,
    engine: &E,
) -> Result<(), ImportError<E::Error>> {
    checked_constants(state, parameters)?;
    signed.hash_tree_root(parameters.preset()).map_err(ProcessingError::from)?;
    envelope_signature_kernel(state, signed, parameters)?;
    let envelope = &signed.message;
    let payload = &envelope.payload;
    let bid = &state.latest_execution_payload_bid;
    let mut header = state.latest_block_header;
    header.state_root = state.hash_tree_root(parameters.preset()).map_err(ProcessingError::from)?;
    if envelope.beacon_block_root != header.hash_tree_root() ||
        envelope.parent_beacon_block_root != state.latest_block_header.parent_root
    {
        return Err(ProcessingError::InvalidState(
            "envelope beacon commitments differ from authenticated state",
        )
        .into());
    }
    if envelope.builder_index != bid.builder_index ||
        payload.prev_randao != bid.prev_randao ||
        payload.gas_limit != bid.gas_limit ||
        payload.block_hash != bid.block_hash ||
        envelope
            .execution_requests
            .hash_tree_root(parameters.preset())
            .map_err(ProcessingError::from)? !=
            bid.execution_requests_root
    {
        return Err(ProcessingError::InvalidState(
            "envelope execution candidate differs from committed bid",
        )
        .into());
    }
    let timestamp = add(
        state.genesis_time,
        mul(
            state.slot,
            parameters.fulu.electra.deneb.capella.bellatrix.seconds_per_slot,
            "envelope slot duration",
        )?,
        "envelope timestamp",
    )?;
    if payload.slot_number != state.slot ||
        payload.parent_hash != state.latest_block_hash ||
        payload.timestamp != timestamp ||
        payload.withdrawals.hash_tree_root(parameters.preset()).map_err(ProcessingError::from)? !=
            state
                .payload_expected_withdrawals
                .hash_tree_root(parameters.preset())
                .map_err(ProcessingError::from)?
    {
        return Err(ProcessingError::InvalidState(
            "envelope slot, execution parent, clock or withdrawals differ",
        )
        .into());
    }
    let context = ExecutionContext {
        slot: state.slot,
        parent_beacon_block_root: envelope.parent_beacon_block_root,
        previous_execution_payload_hash: state.latest_block_hash,
        expected_blob_versioned_hashes: bid
            .blob_kzg_commitments
            .values
            .iter()
            .map(kzg_commitment_to_versioned_hash)
            .collect(),
        execution_requests_list: execution_requests::get_execution_requests_list(
            &envelope.execution_requests,
            parameters.preset(),
        )?,
    };
    engine.verify_payload(payload, context).await.map_err(ImportError::Execution)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::{
        super::{
            blocks::tests::{candidate, empty_requests, state_and_keys},
            withdrawals,
        },
        *,
    };
    use crate::ssz::{gloas, List, ProgressiveList, Vector};
    use blst::min_pk::SecretKey;
    use reth_embedded_beacon_crypto::ETHEREUM_BLS_DST;
    use std::{
        future::Future,
        sync::Mutex,
        task::{Context, Poll, Waker},
    };

    fn ready<F: Future>(future: F) -> F::Output {
        let mut future = std::pin::pin!(future);
        match future.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
            Poll::Ready(value) => value,
            Poll::Pending => panic!("test callback must complete"),
        }
    }
    fn sign(
        state: &BeaconState,
        envelope: gloas::ExecutionPayloadEnvelope,
        keys: &[SecretKey],
        parameters: &GloasParameters,
    ) -> SignedExecutionPayloadEnvelope {
        let root = compute_signing_root(
            envelope.hash_tree_root(parameters.preset()).unwrap(),
            common::domain(state, [11, 0, 0, 0], state.slot / 8),
        );
        SignedExecutionPayloadEnvelope {
            signature: keys[state.latest_block_header.proposer_index as usize]
                .sign(&root, ETHEREUM_BLS_DST, &[])
                .to_bytes(),
            message: envelope,
        }
    }
    pub(crate) fn envelope(
        state: &BeaconState,
        keys: &[SecretKey],
        parameters: &GloasParameters,
    ) -> SignedExecutionPayloadEnvelope {
        let mut header = state.latest_block_header;
        header.state_root = state.hash_tree_root(parameters.preset()).unwrap();
        sign(
            state,
            gloas::ExecutionPayloadEnvelope {
                payload: gloas::ExecutionPayload {
                    parent_hash: state.latest_block_hash,
                    fee_recipient: [77; 20],
                    state_root: [2; 32],
                    receipts_root: [3; 32],
                    logs_bloom: Vector::from_vec(vec![0; 256]),
                    prev_randao: state.latest_execution_payload_bid.prev_randao,
                    block_number: 1,
                    gas_limit: state.latest_execution_payload_bid.gas_limit,
                    gas_used: 17,
                    timestamp: state.genesis_time +
                        state.slot *
                            parameters.fulu.electra.deneb.capella.bellatrix.seconds_per_slot,
                    extra_data: List::from_vec(vec![5]),
                    base_fee_per_gas: [0xff; 32],
                    block_hash: state.latest_execution_payload_bid.block_hash,
                    transactions: ProgressiveList::from_vec(vec![
                        ProgressiveList::from_vec(vec![1, 2, 3]),
                        ProgressiveList::default(),
                    ]),
                    withdrawals: state.payload_expected_withdrawals.clone(),
                    blob_gas_used: 0,
                    excess_blob_gas: 1,
                    block_access_list: ProgressiveList::from_vec(vec![6, 7, 8]),
                    slot_number: state.slot,
                },
                execution_requests: empty_requests(),
                builder_index: state.latest_execution_payload_bid.builder_index,
                beacon_block_root: header.hash_tree_root(),
                parent_beacon_block_root: state.latest_block_header.parent_root,
            },
            keys,
            parameters,
        )
    }
    // Only checks immutable submission and failure behavior; it establishes no EL validity.
    struct Engine {
        allowed: bool,
        calls: Mutex<Vec<(gloas::ExecutionPayload, ExecutionContext)>>,
    }
    impl ExecutionEngine for Engine {
        type Error = std::io::Error;
        async fn verify_payload(
            &self,
            payload: &gloas::ExecutionPayload,
            context: ExecutionContext,
        ) -> Result<(), Self::Error> {
            self.calls.lock().unwrap().push((payload.clone(), context));
            if self.allowed {
                Ok(())
            } else {
                Err(std::io::Error::other("test execution rejection"))
            }
        }
    }
    #[test]
    fn envelope_preserves_bal_opaque_transactions_uint256_and_exact_ordered_engine_context() {
        let (pre, keys, parameters) = state_and_keys();
        let (_, mut state) = candidate(&pre, &keys, &parameters);
        state.latest_execution_payload_bid.blob_kzg_commitments =
            ProgressiveList::from_vec(vec![[1; 48], [2; 48]]);
        let mut requests = empty_requests();
        requests.builder_exits.values.push(gloas::BuilderExitRequest {
            source_address: [13; 20],
            pubkey: keys[0].sk_to_pk().to_bytes(),
        });
        state.latest_execution_payload_bid.execution_requests_root =
            requests.hash_tree_root(parameters.preset()).unwrap();
        let mut signed = envelope(&state, &keys, &parameters);
        signed.message.execution_requests = requests;
        signed = sign(&state, signed.message, &keys, &parameters);
        let before = state.clone();
        let engine = Engine { allowed: true, calls: Mutex::new(Vec::new()) };
        ready(verify_execution_payload_envelope(&state, &signed, &parameters, &engine)).unwrap();
        assert_eq!(state, before);
        assert_eq!(
            *engine.calls.lock().unwrap(),
            vec![(
                signed.message.payload.clone(),
                ExecutionContext {
                    slot: state.slot,
                    parent_beacon_block_root: state.latest_block_header.parent_root,
                    previous_execution_payload_hash: state.latest_block_hash,
                    expected_blob_versioned_hashes: vec![
                        kzg_commitment_to_versioned_hash(&[1; 48]),
                        kzg_commitment_to_versioned_hash(&[2; 48])
                    ],
                    execution_requests_list: vec![[
                        vec![4],
                        vec![13; 20],
                        keys[0].sk_to_pk().to_bytes().to_vec()
                    ]
                    .concat()]
                }
            )]
        );
        // The bid fee recipient is the payment destination; the spec does not constrain payload fee
        // recipient equality.
        assert_ne!(
            signed.message.payload.fee_recipient,
            state.latest_execution_payload_bid.fee_recipient
        );
        let rejecting = Engine { allowed: false, calls: Mutex::new(Vec::new()) };
        assert!(matches!(
            ready(verify_execution_payload_envelope(&state, &signed, &parameters, &rejecting)),
            Err(ImportError::Execution(_))
        ));
        assert_eq!(state, before);
    }
    #[test]
    fn envelope_signature_and_each_consensus_link_fail_before_the_engine() {
        let (pre, keys, parameters) = state_and_keys();
        let (_, state) = candidate(&pre, &keys, &parameters);
        let valid = envelope(&state, &keys, &parameters);
        let engine = Engine { allowed: true, calls: Mutex::new(Vec::new()) };
        let mut invalid = valid.clone();
        invalid.signature = [0; 96];
        assert!(ready(verify_execution_payload_envelope(&state, &invalid, &parameters, &engine))
            .is_err());
        for field in 0..11 {
            let mut message = valid.message.clone();
            match field {
                0 => message.beacon_block_root[0] ^= 1,
                1 => message.parent_beacon_block_root[0] ^= 1,
                2 => message.builder_index = 0,
                3 => message.payload.prev_randao[0] ^= 1,
                4 => message.payload.gas_limit += 1,
                5 => message.payload.block_hash[0] ^= 1,
                6 => message.execution_requests.builder_exits.values.push(
                    gloas::BuilderExitRequest {
                        source_address: [0; 20],
                        pubkey: keys[0].sk_to_pk().to_bytes(),
                    },
                ),
                7 => message.payload.slot_number += 1,
                8 => message.payload.parent_hash[0] ^= 1,
                9 => message.payload.timestamp += 1,
                _ => message.payload.withdrawals.values.push(gloas::Withdrawal {
                    index: 0,
                    validator_index: 0,
                    address: [0; 20],
                    amount: 1,
                }),
            };
            let invalid = sign(&state, message, &keys, &parameters);
            assert!(
                ready(verify_execution_payload_envelope(&state, &invalid, &parameters, &engine))
                    .is_err(),
                "accepted field {field}"
            );
        }
        assert!(engine.calls.lock().unwrap().is_empty());
    }
    #[test]
    fn full_parent_payment_settles_before_exit_and_requests_use_child_slot() {
        let (mut state, keys, parameters) = state_and_keys();
        state.slot = 9;
        state.latest_block_header.slot = 8;
        state.finalized_checkpoint.epoch = 1;
        state.builders.values.push(gloas::Builder {
            pubkey: keys[0].sk_to_pk().to_bytes(),
            version: 0,
            execution_address: [12; 20],
            balance: 1_000_000_100,
            deposit_epoch: 0,
            withdrawable_epoch: u64::MAX,
        });
        state.latest_execution_payload_bid.block_hash = [10; 32];
        state.latest_execution_payload_bid.builder_index = 0;
        state.latest_execution_payload_bid.value = 50;
        state.builder_pending_payments.values[8] = gloas::BuilderPendingPayment {
            weight: 0,
            withdrawal: gloas::BuilderPendingWithdrawal {
                fee_recipient: [8; 20],
                amount: 50,
                builder_index: 0,
            },
            proposer_index: 0,
        };
        let mut requests = empty_requests();
        requests.builder_exits.values.push(gloas::BuilderExitRequest {
            source_address: [12; 20],
            pubkey: keys[0].sk_to_pk().to_bytes(),
        });
        requests.deposits.values.push(gloas::DepositRequest {
            pubkey: keys[1].sk_to_pk().to_bytes(),
            withdrawal_credentials: [0; 32],
            amount: 7,
            signature: [0; 96],
            index: 42,
        });
        let start_index = state.deposit_requests_start_index;
        apply_parent_execution_payload(&mut state, &requests, &parameters).unwrap();
        assert_eq!(state.builders.values[0].withdrawable_epoch, u64::MAX);
        assert_eq!(state.builder_pending_withdrawals.values[0].amount, 50);
        assert_eq!(state.builder_pending_payments.values[8], builder::empty_payment());
        assert_eq!(state.pending_deposits.values.last().unwrap().slot, 9);
        assert_eq!(state.deposit_requests_start_index, start_index);
        assert!(state.execution_payload_availability.bits.get(8).unwrap());
        assert_eq!(state.latest_block_hash, [10; 32]);
        withdrawals::process_withdrawals(&mut state, &parameters).unwrap();
        assert_eq!(state.builders.values[0].balance, 1_000_000_050);
        assert!(state.builder_pending_withdrawals.values.is_empty());
        assert_eq!(
            state.payload_expected_withdrawals.values[0],
            gloas::Withdrawal {
                index: 0,
                validator_index: builder::BUILDER_INDEX_FLAG,
                address: [8; 20],
                amount: 50
            }
        );
        assert!(builder::can_builder_cover_bid(&state, 0, 50, &parameters).unwrap());
        assert!(!builder::can_builder_cover_bid(&state, 0, 51, &parameters).unwrap());
    }
    #[test]
    fn empty_parent_requires_all_five_groups_empty_and_committed_full_parent_root_is_mandatory() {
        let (mut state, keys, parameters) = state_and_keys();
        let (signed, _) = candidate(&state, &keys, &parameters);
        state.slot = signed.message.slot;
        let mut block = signed.message;
        block.body.signed_execution_payload_bid.message.parent_block_hash = [41; 32];
        block.body.parent_execution_requests.builder_exits.values.push(gloas::BuilderExitRequest {
            source_address: [0; 20],
            pubkey: keys[0].sk_to_pk().to_bytes(),
        });
        let before = state.clone();
        assert!(matches!(
            process_parent_execution_payload(&mut state, &block, &parameters),
            Err(ProcessingError::InvalidState("EMPTY parent cannot carry execution requests"))
        ));
        assert_eq!(state, before);
        block.body.signed_execution_payload_bid.message.parent_block_hash =
            state.latest_execution_payload_bid.block_hash;
        assert!(matches!(
            process_parent_execution_payload(&mut state, &block, &parameters),
            Err(ProcessingError::InvalidState(
                "parent execution requests differ from committed bid"
            ))
        ));
        assert_eq!(state, before);
        block.body.parent_execution_requests = empty_requests();
        block.body.signed_execution_payload_bid.message.parent_block_hash = [41; 32];
        process_parent_execution_payload(&mut state, &block, &parameters).unwrap();
        assert_eq!(state, before);
    }
    struct PendingEngine;
    impl ExecutionEngine for PendingEngine {
        type Error = std::io::Error;
        async fn verify_payload(
            &self,
            _payload: &gloas::ExecutionPayload,
            _context: ExecutionContext,
        ) -> Result<(), Self::Error> {
            std::future::pending().await
        }
    }
    #[test]
    fn cancelling_envelope_verification_cannot_mutate_the_beacon_state() {
        let (pre, keys, parameters) = state_and_keys();
        let (_, state) = candidate(&pre, &keys, &parameters);
        let signed = envelope(&state, &keys, &parameters);
        let before = state.clone();
        {
            let mut future = Box::pin(verify_execution_payload_envelope(
                &state,
                &signed,
                &parameters,
                &PendingEngine,
            ));
            assert!(matches!(
                future.as_mut().poll(&mut Context::from_waker(Waker::noop())),
                Poll::Pending
            ));
        }
        assert_eq!(state, before);
    }
}

#[cfg(feature = "spec-tests")]
#[path = "payload_artificial.rs"]
pub(crate) mod artificial;
