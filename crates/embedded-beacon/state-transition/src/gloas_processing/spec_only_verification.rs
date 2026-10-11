//! Explicit artificial Gloas verification for published BLS-disabled fixture graphs only.
//! Canonical schema, index, commitment, ancestry and mandatory execution checks remain unchanged.
//! These ordinary Results cannot become runtime proofs; DA remains mandatory in the isolated graph.

#![cfg(feature = "spec-tests")]

use super::{
    builder, checked_constants, common, execution_requests,
    payload::kzg_commitment_to_versioned_hash, ExecutionContext, ExecutionEngine, GloasParameters,
    ImportError, IndexedPayloadAttestation,
};
use crate::{
    phase0_processing::{add, mul, ProcessingError},
    spec_crypto::{
        skip_aggregate_for_artificial_fixture as fast_aggregate_verify,
        skip_signature_for_artificial_fixture as verify_signature,
    },
    ssz::{
        gloas::{BeaconState, IndexedAttestation, SignedExecutionPayloadEnvelope},
        Preset, Ssz,
    },
};
use reth_embedded_beacon_crypto::compute_signing_root;

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

/// Verify every exact envelope commitment and mandatory execution callback for an artificial
/// fixture. Only the declared BLS check is skipped; no state mutation, runtime token or DA proof is
/// produced.
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

fn indexed_kernel(
    state: &BeaconState,
    attestation: &IndexedAttestation,
    parameters: &GloasParameters,
) -> Result<(), ProcessingError> {
    let indices = &attestation.attesting_indices.values;
    if indices.is_empty() ||
        indices.len() >
            match parameters.preset() {
                crate::ssz::Preset::Mainnet => 131_072,
                crate::ssz::Preset::Minimal => 8_192,
            } ||
        indices.windows(2).any(|pair| pair[0] >= pair[1])
    {
        return Err(ProcessingError::InvalidState(
            "indexed participants are not nonempty sorted unique indexes",
        ));
    }
    let keys = indices
        .iter()
        .map(|&i| {
            Ok(state.validators.values[common::index(i, state.validators.values.len())?].pubkey)
        })
        .collect::<Result<Vec<_>, ProcessingError>>()?;
    let root = compute_signing_root(
        attestation.data.hash_tree_root(parameters.preset())?,
        common::domain(state, [1, 0, 0, 0], attestation.data.target.epoch),
    );
    fast_aggregate_verify(&keys, &root, &attestation.signature)?;
    Ok(())
}

fn verify_indexed_payload(
    state: &BeaconState,
    attestation: &IndexedPayloadAttestation,
    parameters: &GloasParameters,
) -> Result<(), ProcessingError> {
    let indices = &attestation.attesting_indices;
    if indices.is_empty() ||
        indices.len() >
            match parameters.preset() {
                Preset::Mainnet => 512,
                Preset::Minimal => 16,
            } ||
        indices.windows(2).any(|p| p[0] > p[1])
    {
        return Err(ProcessingError::InvalidState(
            "PTC participants are not nonempty sorted positions",
        ));
    }
    let keys = indices
        .iter()
        .map(|&i| {
            Ok(state.validators.values[common::index(i, state.validators.values.len())?].pubkey)
        })
        .collect::<Result<Vec<_>, ProcessingError>>()?;
    let root = compute_signing_root(
        attestation.data.hash_tree_root(parameters.preset())?,
        common::domain(state, [12, 0, 0, 0], attestation.data.slot / parameters.slots_per_epoch()),
    );
    Ok(fast_aggregate_verify(&keys, &root, &attestation.signature)?)
}

/// Check the exact nonempty sorted unique validator list and signing context for artificial data.
/// Only signature verification follows the explicit published BLS-disabled fixture policy.
pub fn is_valid_indexed_attestation(
    state: &BeaconState,
    attestation: &IndexedAttestation,
    parameters: &GloasParameters,
) -> Result<(), ProcessingError> {
    checked_constants(state, parameters)?;
    attestation.hash_tree_root(parameters.preset())?;
    indexed_kernel(state, attestation, parameters)
}

/// Check exact sorted PTC positions including allowed duplicates for artificial data.
/// This is not payload, execution, data availability or runtime vote authentication.
pub fn is_valid_indexed_payload_attestation(
    state: &BeaconState,
    attestation: &IndexedPayloadAttestation,
    parameters: &GloasParameters,
) -> Result<(), ProcessingError> {
    checked_constants(state, parameters)?;
    verify_indexed_payload(state, attestation, parameters)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        gloas_processing::{
            blocks::tests::{candidate, state_and_keys},
            payload::tests::envelope,
        },
        ssz::{gloas, ProgressiveList},
    };
    use std::{
        future::Future,
        sync::atomic::{AtomicUsize, Ordering},
        task::{Context, Poll, Waker},
    };

    struct RejectingEngine {
        calls: AtomicUsize,
    }
    impl ExecutionEngine for RejectingEngine {
        type Error = std::io::Error;
        async fn verify_payload(
            &self,
            _payload: &gloas::ExecutionPayload,
            _context: ExecutionContext,
        ) -> Result<(), Self::Error> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Err(std::io::Error::other("explicit unit execution rejection"))
        }
    }
    fn ready<F: Future>(future: F) -> F::Output {
        let mut future = std::pin::pin!(future);
        match future.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
            Poll::Ready(value) => value,
            Poll::Pending => panic!("unit execution rejection must be synchronous"),
        }
    }

    #[test]
    fn artificial_envelope_preserves_engine_linkage_and_runtime_signature_policy() {
        let (pre, keys, parameters) = state_and_keys();
        let (_, state) = candidate(&pre, &keys, &parameters);
        let before = state.to_ssz(parameters.preset()).unwrap();
        let mut signed = envelope(&state, &keys, &parameters);
        signed.signature = [0; 96];
        let engine = RejectingEngine { calls: AtomicUsize::new(0) };
        assert!(ready(super::super::verify_execution_payload_envelope(
            &state,
            &signed,
            &parameters,
            &engine
        ))
        .is_err());
        assert_eq!(engine.calls.load(Ordering::Relaxed), 0);
        assert!(matches!(
            ready(verify_execution_payload_envelope(&state, &signed, &parameters, &engine)),
            Err(ImportError::Execution(_))
        ));
        assert_eq!(engine.calls.load(Ordering::Relaxed), 1);
        signed.message.parent_beacon_block_root = [44; 32];
        assert!(ready(verify_execution_payload_envelope(&state, &signed, &parameters, &engine))
            .is_err());
        assert_eq!(engine.calls.load(Ordering::Relaxed), 1);
        assert_eq!(state.to_ssz(parameters.preset()).unwrap(), before);
    }

    #[test]
    fn artificial_indexed_validation_retains_unique_and_ptc_duplicate_rules() {
        let (state, _, parameters) = state_and_keys();
        let mut indexed = IndexedAttestation {
            attesting_indices: ProgressiveList::from_vec(vec![0, 1]),
            data: gloas::AttestationData::default_value(parameters.preset()).unwrap(),
            signature: [0; 96],
        };
        assert!(super::super::is_valid_indexed_attestation(&state, &indexed, &parameters).is_err());
        is_valid_indexed_attestation(&state, &indexed, &parameters).unwrap();
        indexed.attesting_indices.values = vec![0, 0];
        assert!(is_valid_indexed_attestation(&state, &indexed, &parameters).is_err());
        indexed.attesting_indices.values = vec![state.validators.values.len() as u64];
        assert!(is_valid_indexed_attestation(&state, &indexed, &parameters).is_err());
        let mut ptc = IndexedPayloadAttestation {
            attesting_indices: vec![0, 0],
            data: gloas::PayloadAttestationData::default_value(parameters.preset()).unwrap(),
            signature: [0; 96],
        };
        assert!(
            super::super::is_valid_indexed_payload_attestation(&state, &ptc, &parameters).is_err()
        );
        is_valid_indexed_payload_attestation(&state, &ptc, &parameters).unwrap();
        ptc.attesting_indices = vec![1, 0];
        assert!(is_valid_indexed_payload_attestation(&state, &ptc, &parameters).is_err());
        ptc.attesting_indices = vec![];
        assert!(is_valid_indexed_payload_attestation(&state, &ptc, &parameters).is_err());
    }
}
