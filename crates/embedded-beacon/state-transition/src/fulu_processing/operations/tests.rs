//! Native aggregate signature and immutable committee reuse regressions.

use super::*;
use crate::ssz::{Preset, Ssz};
use blst::min_pk::{AggregateSignature, SecretKey};
use reth_embedded_beacon_crypto::ETHEREUM_BLS_DST;

pub(super) fn attestation(
    state: &BeaconState,
    keys: &[SecretKey],
    parameters: &FuluParameters,
) -> Attestation {
    let indices = super::super::get_beacon_committee(state, 0, 0, parameters).unwrap();
    let mut bytes = vec![0; (indices.len() + 8) / 8];
    for bit in 0..=indices.len() {
        bytes[bit / 8] |= 1 << (bit % 8);
    }
    let data = fulu::AttestationData {
        slot: 0,
        index: 0,
        beacon_block_root: state.block_roots.values[0],
        source: state.current_justified_checkpoint,
        target: state.current_justified_checkpoint,
    };
    let root = compute_signing_root(
        data.hash_tree_root(parameters.preset()).unwrap(),
        common::domain(state, [1, 0, 0, 0], data.target.epoch),
    );
    let signatures = indices
        .iter()
        .map(|&index| keys[index as usize].sign(&root, ETHEREUM_BLS_DST, &[]))
        .collect::<Vec<_>>();
    let references = signatures.iter().collect::<Vec<_>>();
    Attestation {
        aggregation_bits: fulu::AggregationBits::from_ssz(&bytes, parameters.preset()).unwrap(),
        committee_bits: fulu::CommitteeBits::from_ssz(&[1], Preset::Minimal).unwrap(),
        data,
        signature: AggregateSignature::aggregate(&references, true)
            .unwrap()
            .to_signature()
            .to_bytes(),
    }
}

#[test]
fn combined_indexing_matches_strict_split_validation_and_rejects_invalid_originals() {
    let (state, keys, parameters) = super::super::tests::state_and_keys();
    let before = state.clone();
    let vote = attestation(&state, &keys, &parameters);
    let expected = get_indexed_attestation(&state, &vote, &parameters).unwrap();
    is_valid_indexed_attestation(&state, &expected, &parameters).unwrap();
    let indexed = get_validated_indexed_attestation(&state, &vote, &parameters).unwrap();
    assert_eq!(indexed, expected);
    assert_eq!(state, before);

    let mut invalid = vote.clone();
    invalid.signature = [0; 96];
    assert!(get_validated_indexed_attestation(&state, &invalid, &parameters).is_err());
    let mut truncated = vote.clone();
    truncated.aggregation_bits = fulu::AggregationBits::from_ssz(&[1], Preset::Minimal).unwrap();
    assert!(get_validated_indexed_attestation(&state, &truncated, &parameters).is_err());
    let mut invalid_committee = vote.clone();
    invalid_committee.committee_bits =
        fulu::CommitteeBits::from_ssz(&[8], Preset::Minimal).unwrap();
    assert!(get_validated_indexed_attestation(&state, &invalid_committee, &parameters).is_err());
    let mut wrong_domain = vote;
    wrong_domain.data.target.epoch += 1;
    assert!(get_validated_indexed_attestation(&state, &wrong_domain, &parameters).is_err());
    assert_eq!(state, before);
}

#[test]
fn reused_committee_inputs_match_native_reference_for_multiple_selected_committees() {
    let (mut state, _, parameters) = super::super::tests::state_and_keys();
    let constants = checked_constants(&state, &parameters).unwrap();
    // Private committee derivation only reads the registry and RANDAO. Grow that read-only
    // registry to exercise several committees without pretending this is an importable state.
    let validators = state.validators.values.clone();
    state.validators.values.extend(validators);
    let prepared = CommitteeInputs::new(&state, 7, &constants).unwrap();
    assert_eq!(prepared.count, 2);
    for committee_index in 0..prepared.count {
        assert_eq!(
            prepared.committee(7, committee_index, &constants).unwrap(),
            crate::common::beacon_committee(&state, 7, committee_index, &constants).unwrap(),
        );
    }
    assert!(prepared.committee(7, prepared.count, &constants).is_err());
}

#[cfg(feature = "spec-tests")]
#[test]
fn fixture_combined_indexing_keeps_committee_bounds_when_only_signature_is_disabled() {
    let (state, keys, parameters) = super::super::tests::state_and_keys();
    let mut vote = attestation(&state, &keys, &parameters);
    vote.signature = [0; 96];
    assert!(spec_only::get_validated_indexed_attestation(&state, &vote, &parameters).is_ok());
    vote.aggregation_bits = fulu::AggregationBits::from_ssz(&[1], Preset::Minimal).unwrap();
    assert!(spec_only::get_validated_indexed_attestation(&state, &vote, &parameters).is_err());
}

#[test]
fn native_loop_reuse_matches_fresh_preparation_after_slashing_and_bad_middle_vote() {
    let (mut state, keys, parameters) = super::super::tests::state_and_keys();
    state.slot = 1;
    state.block_roots.values[0] = state.current_justified_checkpoint.root;
    let constants = checked_constants(&state, &parameters).unwrap();
    common::slash_validator_kernel(&mut state, 0, None, &parameters, &constants).unwrap();
    assert!(state.validators.values[0].slashed);
    let vote = attestation(&state, &keys, &parameters);
    let mut invalid = vote.clone();
    invalid.signature = [0; 96];
    let mut fresh = state.clone();
    let mut grouped = state;
    let mut inputs = context::AttestationInputs::default();
    for operation in [&vote, &invalid, &vote] {
        let before = grouped.clone();
        let expected = attestation_kernel::<true>(&mut fresh, operation, &parameters, &constants);
        let actual = attestation_kernel_with_inputs::<true>(
            &mut grouped,
            operation,
            &parameters,
            &constants,
            &mut inputs,
        );
        assert_eq!(
            actual.as_ref().err().map(ToString::to_string),
            expected.as_ref().err().map(ToString::to_string)
        );
        assert_eq!(grouped, fresh);
        if actual.is_err() {
            // Invalid signatures fail before participation or reward mutation.
            assert_eq!(grouped, before);
        }
    }
}

#[test]
fn native_block_vote_preparation_preserves_transaction_rollback() {
    let (mut state, keys, parameters) = super::super::tests::state_and_keys();
    state.slot = 1;
    state.block_roots.values[0] = state.current_justified_checkpoint.root;
    let vote = attestation(&state, &keys, &parameters);
    let mut invalid = vote.clone();
    invalid.signature = [0; 96];
    let mut body = BeaconBlockBody::default_value(parameters.preset()).unwrap();
    body.attestations = List::from_vec(vec![vote.clone(), invalid, vote.clone()]);
    let before = state.clone();
    assert!(process_operations(&mut state, &body, &parameters).is_err());
    assert_eq!(state, before);
    body.attestations = List::from_vec(vec![vote.clone(), vote.clone()]);
    let constants = checked_constants(&state, &parameters).unwrap();
    let mut expected = state.clone();
    for operation in &body.attestations.values {
        attestation_kernel::<true>(&mut expected, operation, &parameters, &constants).unwrap();
    }
    process_operations(&mut state, &body, &parameters).unwrap();
    assert_eq!(state, expected);
}
