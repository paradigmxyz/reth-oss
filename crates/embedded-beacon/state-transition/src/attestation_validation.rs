//! Strict multi-committee wire validation against an authenticated earlier checkpoint schema.
//!
//! These immutable helpers verify committee membership and aggregate BLS only. Callers
//! authenticate the checkpoint state and enforce target ancestry and fork-choice timing.
//! No replacement state, shortened indexed container or runtime authority token is created.

use crate::{
    common::beacon_committee,
    deneb_processing::{checked_constants, DenebParameters},
    phase0_processing::{add, ProcessingError},
    ssz::{deneb, electra, fulu, phase0::AttestationData},
};
use reth_embedded_beacon_crypto::{compute_domain, compute_signing_root, fast_aggregate_verify};
use reth_embedded_beacon_primitives::{BitList, BitVector};
use std::collections::BTreeSet;

struct AttestationView<'a> {
    data: &'a AttestationData,
    aggregation: &'a BitList,
    committees: &'a BitVector,
    signature: &'a [u8; 96],
}
fn validate_kernel(
    state: &deneb::BeaconState,
    view: AttestationView<'_>,
    parameters: &DenebParameters,
) -> Result<Vec<u64>, ProcessingError> {
    let constants = checked_constants(state, parameters)?;
    let target = view.data.target.epoch;
    if target != state.slot / constants.slots_per_epoch ||
        view.data.slot / constants.slots_per_epoch != target ||
        view.data.index != 0
    {
        return Err(ProcessingError::InvalidState(
            "wire attestation does not match the target checkpoint epoch or index",
        ))
    }
    let selected = (0..view.committees.len())
        .filter(|&index| view.committees.get(index) == Some(true))
        .collect::<Vec<_>>();
    if selected.is_empty() {
        return Err(ProcessingError::InvalidState("wire attestation selects no committees"))
    }
    let committees = selected
        .into_iter()
        .map(|index| beacon_committee(state, view.data.slot, index as u64, &constants))
        .collect::<Result<Vec<_>, _>>()?;
    let length = committees.iter().try_fold(0u64, |sum, committee| {
        add(sum, committee.len() as u64, "wire aggregation length")
    })?;
    if view.aggregation.len() as u64 != length {
        return Err(ProcessingError::InvalidState(
            "wire aggregation bits differ from selected committee lengths",
        ))
    }
    let mut indices = BTreeSet::new();
    let mut offset = 0;
    for committee in committees {
        for (position, index) in committee.iter().enumerate() {
            if view
                .aggregation
                .get(offset + position)
                .ok_or(ProcessingError::InvalidState("wire aggregation bits are truncated"))?
            {
                indices.insert(*index);
            }
        }
        offset += committee.len();
    }
    if indices.is_empty() {
        return Err(ProcessingError::InvalidState("wire indexed attestation is empty"))
    }
    let keys = indices
        .iter()
        .map(|&index| {
            state
                .validators
                .values
                .get(index as usize)
                .map(|v| v.pubkey)
                .ok_or(ProcessingError::InvalidState("wire validator index is out of range"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let version = if target < state.fork.epoch {
        state.fork.previous_version
    } else {
        state.fork.current_version
    };
    let root = compute_signing_root(
        view.data.hash_tree_root(parameters.preset())?,
        compute_domain([1, 0, 0, 0], version, state.genesis_validators_root),
    );
    fast_aggregate_verify(&keys, &root, view.signature)?;
    Ok(indices.into_iter().collect())
}
/// Verify Electra wire committees and mandatory BLS against an authenticated Deneb target state.
/// This checks neither checkpoint root authentication nor inclusion/fork-choice ancestry.
pub fn validate_electra_attestation_at_deneb_checkpoint(
    state: &deneb::BeaconState,
    attestation: &electra::Attestation,
    parameters: &DenebParameters,
) -> Result<Vec<u64>, ProcessingError> {
    attestation.hash_tree_root(parameters.preset())?;
    validate_kernel(
        state,
        AttestationView {
            data: &attestation.data,
            aggregation: &attestation.aggregation_bits.bits,
            committees: &attestation.committee_bits.bits,
            signature: &attestation.signature,
        },
        parameters,
    )
}
/// Verify Fulu wire committees and mandatory BLS against an authenticated Deneb target state.
/// This checks neither checkpoint root authentication nor inclusion/fork-choice ancestry.
pub fn validate_fulu_attestation_at_deneb_checkpoint(
    state: &deneb::BeaconState,
    attestation: &fulu::Attestation,
    parameters: &DenebParameters,
) -> Result<Vec<u64>, ProcessingError> {
    attestation.hash_tree_root(parameters.preset())?;
    validate_kernel(
        state,
        AttestationView {
            data: &attestation.data,
            aggregation: &attestation.aggregation_bits.bits,
            committees: &attestation.committee_bits.bits,
            signature: &attestation.signature,
        },
        parameters,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ssz::{Preset, PresetBitList, PresetBits, Ssz};
    use blst::min_pk::AggregateSignature;
    use reth_embedded_beacon_crypto::ETHEREUM_BLS_DST;
    use reth_embedded_beacon_primitives::Checkpoint;

    #[test]
    fn new_wire_uses_real_older_committee_and_domain_without_state_replacement() {
        let (pre, keys) = crate::bellatrix_processing::tests::state_and_keys();
        let parameters = DenebParameters::for_preset(Preset::Minimal);
        let capella =
            crate::capella_processing::upgrade_to_capella(&pre, &parameters.capella).unwrap();
        let state = crate::deneb_processing::upgrade_to_deneb(&capella, &parameters).unwrap();
        let indices =
            crate::deneb_processing::get_beacon_committee(&state, 0, 0, &parameters).unwrap();
        let data = AttestationData {
            slot: 0,
            index: 0,
            beacon_block_root: [3; 32],
            source: Checkpoint::default(),
            target: Checkpoint { epoch: 0, root: [4; 32] },
        };
        let root = compute_signing_root(
            data.hash_tree_root(Preset::Minimal).unwrap(),
            compute_domain([1, 0, 0, 0], state.fork.current_version, state.genesis_validators_root),
        );
        let signatures = indices
            .iter()
            .map(|&i| keys[i as usize].sign(&root, ETHEREUM_BLS_DST, &[]))
            .collect::<Vec<_>>();
        let signature = AggregateSignature::aggregate(&signatures.iter().collect::<Vec<_>>(), true)
            .unwrap()
            .to_signature()
            .to_bytes();
        let mut bits = vec![0; (indices.len() + 1).div_ceil(8)];
        for i in 0..=indices.len() {
            bits[i / 8] |= 1 << (i % 8);
        }
        let mut attestation = electra::Attestation {
            aggregation_bits: PresetBitList::from_ssz(&bits, Preset::Minimal).unwrap(),
            data,
            signature,
            committee_bits: PresetBits::from_ssz(&[1], Preset::Minimal).unwrap(),
        };
        let mut expected = indices;
        expected.sort_unstable();
        expected.dedup();
        assert_eq!(
            validate_electra_attestation_at_deneb_checkpoint(&state, &attestation, &parameters)
                .unwrap(),
            expected
        );
        let fulu = fulu::Attestation::from_ssz(
            &attestation.to_ssz(Preset::Minimal).unwrap(),
            Preset::Minimal,
        )
        .unwrap();
        assert_eq!(
            validate_fulu_attestation_at_deneb_checkpoint(&state, &fulu, &parameters).unwrap(),
            expected
        );
        let before = state.clone();
        attestation.signature = [0; 96];
        assert!(matches!(
            validate_electra_attestation_at_deneb_checkpoint(&state, &attestation, &parameters),
            Err(ProcessingError::Crypto(_))
        ));
        assert_eq!(state, before);
        attestation.data.index = 1;
        assert!(matches!(
            validate_electra_attestation_at_deneb_checkpoint(&state, &attestation, &parameters),
            Err(ProcessingError::InvalidState(
                "wire attestation does not match the target checkpoint epoch or index"
            ))
        ));
    }
}
