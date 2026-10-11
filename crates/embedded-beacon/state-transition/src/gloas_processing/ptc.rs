//! Gloas payload timeliness committees and mandatory aggregate signatures.
//! Votes report observations; they do not establish execution validity or blob availability.

use super::{checked_constants, common, operations, transact, GloasParameters};
use crate::{
    compute_seed, compute_shuffled_index,
    phase0_processing::{add, mul, position, sub, Constants, ProcessingError},
    ssz::{
        gloas::{
            BeaconState, PayloadAttestation, PayloadAttestationData, PayloadTimelinessCommittee,
        },
        Preset, Vector,
    },
};
use reth_embedded_beacon_crypto::{compute_signing_root, fast_aggregate_verify};
use reth_embedded_beacon_primitives::{sha256, Root};

/// Indexed PTC participation preserves duplicate validator positions in a sorted list.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexedPayloadAttestation {
    /// Sorted validator indexes, including repeated committee positions.
    pub attesting_indices: Vec<u64>,
    /// Exact signed timeliness observations.
    pub data: PayloadAttestationData,
    /// Aggregate signature over every selected committee position.
    pub signature: [u8; 96],
}
const fn ptc_size(preset: Preset) -> usize {
    match preset {
        Preset::Mainnet => 512,
        Preset::Minimal => 16,
    }
}

pub(crate) fn weighted_selection_kernel(
    state: &BeaconState,
    indices: &[u64],
    seed: Root,
    size: usize,
    shuffle: bool,
    constants: &Constants,
) -> Result<Vec<u64>, ProcessingError> {
    if indices.is_empty() {
        return Err(ProcessingError::InvalidState("weighted selection candidates are empty"));
    }
    let balances = indices
        .iter()
        .map(|&index| {
            Ok(state.validators.values[common::index(index, state.validators.values.len())?]
                .effective_balance)
        })
        .collect::<Result<Vec<_>, ProcessingError>>()?;
    let mut selected = Vec::with_capacity(size);
    let mut i = 0u64;
    let mut random = [0; 32];
    while selected.len() < size {
        let offset = (i % 16 * 2) as usize;
        if offset == 0 {
            let mut input = [0; 40];
            input[..32].copy_from_slice(&seed);
            input[32..].copy_from_slice(&(i / 16).to_le_bytes());
            random = sha256(&input);
        }
        let candidate = i % indices.len() as u64;
        let candidate = if shuffle {
            compute_shuffled_index(candidate, indices.len() as u64, seed, constants.shuffle_rounds)?
        } else {
            candidate
        } as usize;
        let value = u16::from_le_bytes([random[offset], random[offset + 1]]) as u64;
        if mul(balances[candidate], 65_535, "weighted selection balance")? >=
            mul(2_048_000_000_000, value, "weighted selection threshold")?
        {
            selected.push(indices[candidate]);
        }
        i = add(i, 1, "weighted selection iteration")?;
    }
    Ok(selected)
}
pub(crate) fn compute_ptc_kernel(
    state: &BeaconState,
    slot: u64,
    parameters: &GloasParameters,
    constants: &Constants,
) -> Result<PayloadTimelinessCommittee, ProcessingError> {
    let epoch = slot / parameters.slots_per_epoch();
    let mix_epoch =
        sub(add(epoch, constants.historical_epochs, "PTC seed epoch")?, 2, "PTC seed lookback")?;
    let mix = *state
        .randao_mixes
        .values
        .get(position(mix_epoch, constants.historical_epochs)?)
        .ok_or(ProcessingError::InvalidState("PTC RANDAO ring is truncated"))?;
    let mut input = [0; 40];
    input[..32].copy_from_slice(&compute_seed([12, 0, 0, 0], epoch, mix));
    input[32..].copy_from_slice(&slot.to_le_bytes());
    let mut candidates = Vec::new();
    for index in 0..common::get_committee_count_per_slot(state, epoch, constants)? {
        candidates.extend(operations::committee(state, slot, index, constants)?);
    }
    Ok(Vector::from_vec(weighted_selection_kernel(
        state,
        &candidates,
        sha256(&input),
        ptc_size(parameters.preset()),
        false,
        constants,
    )?))
}
/// Sample the exact slot committee concatenation with 16-bit balance weighting and duplicates.
pub fn compute_ptc(
    state: &BeaconState,
    slot: u64,
    parameters: &GloasParameters,
) -> Result<PayloadTimelinessCommittee, ProcessingError> {
    let constants = checked_constants(state, parameters)?;
    compute_ptc_kernel(state, slot, parameters, &constants)
}
pub(crate) fn ptc_kernel<'a>(
    state: &'a BeaconState,
    slot: u64,
    parameters: &GloasParameters,
) -> Result<&'a PayloadTimelinessCommittee, ProcessingError> {
    let epoch = slot / parameters.slots_per_epoch();
    let current = state.slot / parameters.slots_per_epoch();
    if epoch < parameters.fork_epoch {
        return Err(ProcessingError::InvalidState("PTC slot predates Gloas"));
    }
    let offset = if epoch < current {
        if add(epoch, 1, "PTC previous epoch")? != current {
            return Err(ProcessingError::InvalidState("PTC slot is older than previous epoch"));
        }
        0
    } else {
        if epoch > add(current, 1, "PTC lookahead epoch")? {
            return Err(ProcessingError::InvalidState("PTC slot exceeds lookahead"));
        }
        mul(
            add(epoch - current, 1, "PTC window epoch")?,
            parameters.slots_per_epoch(),
            "PTC window offset",
        )?
    };
    let index = add(offset, slot % parameters.slots_per_epoch(), "PTC window position")?;
    state
        .ptc_window
        .values
        .get(common::index(index, state.ptc_window.values.len())?)
        .ok_or(ProcessingError::InvalidState("PTC window is truncated"))
}
/// Borrow the previous/current/next epoch committee from the authenticated state's window.
pub fn get_ptc<'a>(
    state: &'a BeaconState,
    slot: u64,
    parameters: &GloasParameters,
) -> Result<&'a PayloadTimelinessCommittee, ProcessingError> {
    checked_constants(state, parameters)?;
    ptc_kernel(state, slot, parameters)
}
pub(crate) fn indexed_kernel(
    state: &BeaconState,
    attestation: &PayloadAttestation,
    parameters: &GloasParameters,
) -> Result<IndexedPayloadAttestation, ProcessingError> {
    let ptc = ptc_kernel(state, attestation.data.slot, parameters)?;
    if ptc.values.len() != ptc_size(parameters.preset()) ||
        attestation.aggregation_bits.bits.len() != ptc.values.len()
    {
        return Err(ProcessingError::InvalidState("PTC aggregation length differs"));
    }
    let mut indices = Vec::new();
    for (position, &index) in ptc.values.iter().enumerate() {
        if attestation
            .aggregation_bits
            .bits
            .get(position)
            .ok_or(ProcessingError::InvalidState("PTC aggregation bits are truncated"))?
        {
            indices.push(index);
        }
    }
    indices.sort_unstable();
    Ok(IndexedPayloadAttestation {
        attesting_indices: indices,
        data: attestation.data.clone(),
        signature: attestation.signature,
    })
}
/// Derive sorted indexed participation without removing duplicate committee members.
pub fn get_indexed_payload_attestation(
    state: &BeaconState,
    attestation: &PayloadAttestation,
    parameters: &GloasParameters,
) -> Result<IndexedPayloadAttestation, ProcessingError> {
    checked_constants(state, parameters)?;
    attestation.hash_tree_root(parameters.preset())?;
    indexed_kernel(state, attestation, parameters)
}
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
/// Require every indexed PTC position's signature, including duplicate public keys.
pub fn is_valid_indexed_payload_attestation(
    state: &BeaconState,
    attestation: &IndexedPayloadAttestation,
    parameters: &GloasParameters,
) -> Result<(), ProcessingError> {
    checked_constants(state, parameters)?;
    verify_indexed(state, attestation, parameters)
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
/// Verify the previous-slot parent observation; it does not mutate state or validate the payload.
pub fn process_payload_attestation(
    state: &mut BeaconState,
    attestation: &PayloadAttestation,
    parameters: &GloasParameters,
) -> Result<(), ProcessingError> {
    attestation.hash_tree_root(parameters.preset())?;
    transact(state, parameters, |candidate, _| {
        payload_attestation_kernel(candidate, attestation, parameters)
    })
}

#[cfg(test)]
mod tests {
    use super::{super::blocks::tests::state_and_keys, *};
    use crate::ssz::{PresetBits, Ssz};
    use blst::min_pk::{AggregateSignature, Signature};
    use reth_embedded_beacon_crypto::ETHEREUM_BLS_DST;

    #[test]
    fn duplicate_ptc_positions_remain_distinct_aggregate_signature_members() {
        let (mut state, keys, parameters) = state_and_keys();
        state.slot = 1;
        state.latest_block_header.parent_root = [8; 32];
        state.ptc_window.values[8] = Vector::from_vec(vec![0; 16]);
        let data = PayloadAttestationData {
            beacon_block_root: [8; 32],
            slot: 0,
            payload_present: false,
            blob_data_available: false,
        };
        let root = compute_signing_root(
            data.hash_tree_root(Preset::Minimal).unwrap(),
            common::domain(&state, [12, 0, 0, 0], 0),
        );
        let sig = keys[0].sign(&root, ETHEREUM_BLS_DST, &[]);
        let signature =
            AggregateSignature::aggregate(&[&sig, &sig], true).unwrap().to_signature().to_bytes();
        let attestation = PayloadAttestation {
            aggregation_bits: PresetBits::from_ssz(&[3, 0], Preset::Minimal).unwrap(),
            data,
            signature,
        };
        let indexed = get_indexed_payload_attestation(&state, &attestation, &parameters).unwrap();
        assert_eq!(indexed.attesting_indices, vec![0, 0]);
        is_valid_indexed_payload_attestation(&state, &indexed, &parameters).unwrap();
        let before = state.clone();
        process_payload_attestation(&mut state, &attestation, &parameters).unwrap();
        assert_eq!(state, before);
        let mut deduplicated = indexed.clone();
        deduplicated.attesting_indices = vec![0];
        assert!(is_valid_indexed_payload_attestation(&state, &deduplicated, &parameters).is_err());
        let mut invalid = attestation.clone();
        invalid.data.blob_data_available = true;
        assert!(process_payload_attestation(&mut state, &invalid, &parameters).is_err());
        assert_eq!(state, before);
        let mut sorted = indexed;
        sorted.attesting_indices = vec![1, 0];
        assert!(is_valid_indexed_payload_attestation(&state, &sorted, &parameters).is_err());
        Signature::sig_validate(&signature, true).unwrap();
    }
    #[test]
    fn ptc_window_enforces_fork_and_previous_current_next_epoch_bounds() {
        let (mut state, _, mut parameters) = state_and_keys();
        state.slot = 16;
        parameters.fork_epoch = 1;
        assert!(get_ptc(&state, 7, &parameters).is_err());
        assert_eq!(get_ptc(&state, 8, &parameters).unwrap(), &state.ptc_window.values[0]);
        assert_eq!(get_ptc(&state, 16, &parameters).unwrap(), &state.ptc_window.values[8]);
        assert_eq!(get_ptc(&state, 24, &parameters).unwrap(), &state.ptc_window.values[16]);
        assert!(get_ptc(&state, 32, &parameters).is_err());
        let constants = checked_constants(&state, &parameters).unwrap();
        assert!(weighted_selection_kernel(&state, &[], [0; 32], 16, false, &constants).is_err());
        let index = operations::committee(&state, 16, 0, &constants).unwrap()[0];
        state.validators.values[index as usize].effective_balance = 2_048_000_000_000;
        let selection =
            weighted_selection_kernel(&state, &[index], [0; 32], 16, false, &constants).unwrap();
        assert_eq!(selection, vec![index; 16]);
    }
}

#[cfg(feature = "spec-tests")]
#[path = "ptc_artificial.rs"]
pub(crate) mod artificial;
