//! Explicit artificial published-fixture kernels; absent from runtime builds.
#![cfg(feature = "spec-tests")]
use super::*;
use crate::spec_crypto::skip_aggregate_for_artificial_fixture as fast_aggregate_verify;

fn verify_indexed(
    state: &BeaconState,
    attestation: &IndexedPayloadAttestation,
    parameters: &GloasParameters,
) -> Result<(), ProcessingError> {
    let indices = &attestation.attesting_indices;
    if indices.is_empty() ||
        indices.len() > ptc_size(parameters.preset()) ||
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

pub(crate) fn payload_attestation_kernel(
    state: &BeaconState,
    attestation: &PayloadAttestation,
    parameters: &GloasParameters,
) -> Result<(), ProcessingError> {
    if attestation.data.beacon_block_root != state.latest_block_header.parent_root ||
        add(attestation.data.slot, 1, "payload attestation inclusion")? != state.slot
    {
        return Err(ProcessingError::InvalidState(
            "payload attestation does not identify the previous slot parent",
        ));
    }
    verify_indexed(state, &indexed_kernel(state, attestation, parameters)?, parameters)
}
