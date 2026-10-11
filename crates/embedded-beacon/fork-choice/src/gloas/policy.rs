//! Private fixed signature policies; runtime entry points always select mandatory verification.
//! The artificial branch only exists in the fixture feature and cannot issue runtime proofs.

use super::{ExecutionEngine, GloasParameters, IndexedPayloadAttestation};
use crate::StoreError;
use reth_embedded_beacon_primitives::Fork;
use reth_embedded_beacon_state_transition::{
    gloas_processing::{self, ImportError},
    phase0_processing::ProcessingError,
    ssz::gloas::{
        Attestation, BeaconState, IndexedAttestation, SignedBeaconBlock,
        SignedExecutionPayloadEnvelope,
    },
};

pub(super) const fn require<const VERIFY_SIGNATURES: bool>() -> Result<(), StoreError> {
    if VERIFY_SIGNATURES || cfg!(feature = "spec-tests") {
        Ok(())
    } else {
        Err(StoreError::ValidationUnavailable(Fork::Gloas))
    }
}

macro_rules! select {
    ($verify:expr, $name:ident, $($argument:expr),+ $(,)?) => {{
        if $verify {
            gloas_processing::$name($($argument),+)
        } else {
            #[cfg(feature = "spec-tests")]
            {
                gloas_processing::spec_only::$name($($argument),+)
            }
            #[cfg(not(feature = "spec-tests"))]
            {
                Err(ProcessingError::InvalidState("fixture signature policy is unavailable"))
            }
        }
    }};
}

pub(super) fn process_slots<const VERIFY_SIGNATURES: bool>(
    state: &mut BeaconState,
    target: u64,
    parameters: &GloasParameters,
) -> Result<(), ProcessingError> {
    select!(VERIFY_SIGNATURES, process_slots, state, target, parameters)
}

pub(super) fn state_transition_owned<const VERIFY_SIGNATURES: bool>(
    state: BeaconState,
    block: &SignedBeaconBlock,
    parameters: &GloasParameters,
) -> Result<BeaconState, ProcessingError> {
    if VERIFY_SIGNATURES {
        gloas_processing::state_transition_owned(state, block, parameters)
    } else {
        #[cfg(feature = "spec-tests")]
        {
            let mut state = state;
            gloas_processing::spec_only::state_transition(&mut state, block, parameters)?;
            Ok(state)
        }
        #[cfg(not(feature = "spec-tests"))]
        {
            Err(ProcessingError::InvalidState("fixture signature policy is unavailable"))
        }
    }
}

pub(super) fn validated_attestation<const VERIFY_SIGNATURES: bool>(
    state: &BeaconState,
    attestation: &Attestation,
    parameters: &GloasParameters,
) -> Result<IndexedAttestation, ProcessingError> {
    select!(VERIFY_SIGNATURES, get_validated_indexed_attestation, state, attestation, parameters)
}
pub(super) fn indexed_attestation<const VERIFY_SIGNATURES: bool>(
    state: &BeaconState,
    indexed: &IndexedAttestation,
    parameters: &GloasParameters,
) -> Result<(), ProcessingError> {
    select!(VERIFY_SIGNATURES, is_valid_indexed_attestation, state, indexed, parameters)
}

pub(super) fn indexed_payload_attestation<const VERIFY_SIGNATURES: bool>(
    state: &BeaconState,
    indexed: &IndexedPayloadAttestation,
    parameters: &GloasParameters,
) -> Result<(), ProcessingError> {
    select!(VERIFY_SIGNATURES, is_valid_indexed_payload_attestation, state, indexed, parameters)
}

pub(super) async fn verify_envelope<const VERIFY_SIGNATURES: bool, E: ExecutionEngine>(
    state: &BeaconState,
    signed: &SignedExecutionPayloadEnvelope,
    parameters: &GloasParameters,
    engine: &E,
) -> Result<(), ImportError<E::Error>> {
    if VERIFY_SIGNATURES {
        gloas_processing::verify_execution_payload_envelope(state, signed, parameters, engine).await
    } else {
        #[cfg(feature = "spec-tests")]
        {
            gloas_processing::spec_only::verify_execution_payload_envelope(
                state, signed, parameters, engine,
            )
            .await
        }
        #[cfg(not(feature = "spec-tests"))]
        {
            Err(ProcessingError::InvalidState("fixture signature policy is unavailable").into())
        }
    }
}
