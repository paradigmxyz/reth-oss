//! A borrowed proof ties reusable committee preparation to one fully checked immutable state.

use super::*;
use std::{collections::BTreeMap, fmt};

/// Fully checked immutable state and parameters for a bounded batch of signed attestations.
/// Constructing the view verifies the complete fork-specific state root and collection shapes.
/// Each operation still verifies the original wire shape, indexed participants, domain and BLS.
/// The shared borrow prevents replacing or mutating the state while preparation is reused.
pub struct CheckedAttestationState<'a> {
    state: &'a BeaconState,
    parameters: &'a GloasParameters,
    constants: Constants,
    epochs: BTreeMap<u64, CommitteeInputs>,
    committees: BTreeMap<(u64, u64), Vec<u64>>,
    committee_bytes: usize,
}

impl fmt::Debug for CheckedAttestationState<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CheckedAttestationState")
            .field("slot", &self.state.slot)
            .field("preset", &self.parameters.preset())
            .field("prepared_epochs", &self.epochs.len())
            .field("committee_bytes", &self.committee_bytes)
            .finish_non_exhaustive()
    }
}

impl<'a> CheckedAttestationState<'a> {
    /// Establish all complete-state and parameter invariants from the actual borrowed schema.
    pub fn new(
        state: &'a BeaconState,
        parameters: &'a GloasParameters,
    ) -> Result<Self, ProcessingError> {
        let constants = checked_constants(state, parameters)?;
        Ok(Self {
            state,
            parameters,
            constants,
            epochs: BTreeMap::new(),
            committees: BTreeMap::new(),
            committee_bytes: 0,
        })
    }

    /// Verify an original attestation while reusing only immutable committee preparation.
    pub fn validate(
        &mut self,
        attestation: &Attestation,
    ) -> Result<IndexedAttestation, ProcessingError> {
        attestation.hash_tree_root(self.parameters.preset())?;
        let indices = attesting_indices_with(attestation, false, |index| {
            self.committee(attestation.data.slot, index)
        })?;
        let indexed = IndexedAttestation {
            attesting_indices: ProgressiveList::from_vec(indices),
            data: attestation.data.clone(),
            signature: attestation.signature,
        };
        indexed.hash_tree_root(self.parameters.preset())?;
        indexed_kernel::<true>(self.state, &indexed, self.parameters)?;
        Ok(indexed)
    }

    fn committee(&mut self, slot: u64, index: u64) -> Result<Vec<u64>, ProcessingError> {
        if let Some(committee) = self.committees.get(&(slot, index)) {
            return Ok(committee.clone())
        }
        let epoch = slot / self.constants.slots_per_epoch;
        if !self.epochs.contains_key(&epoch) && self.epochs.len() == 2 {
            self.epochs.clear();
        }
        let inputs = match self.epochs.entry(epoch) {
            std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(CommitteeInputs::new(self.state, slot, &self.constants)?)
            }
        };
        let committee = inputs.committee(slot, index, &self.constants)?;
        let bytes = committee
            .len()
            .checked_mul(std::mem::size_of::<u64>())
            .ok_or(ProcessingError::InvalidState("committee allocation overflow"))?;
        if bytes <= 8 * 1024 * 1024 {
            if self.committees.len() == 128 || self.committee_bytes + bytes > 8 * 1024 * 1024 {
                self.committees.clear();
                self.committee_bytes = 0;
            }
            self.committee_bytes += bytes;
            self.committees.insert((slot, index), committee.clone());
        }
        Ok(committee)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ssz::Ssz;

    #[test]
    fn borrowed_preparation_matches_strict_validation_and_never_caches_signature_success() {
        let (state, keys, parameters) = crate::gloas_processing::blocks::tests::state_and_keys();
        let original = super::super::tests::attestation(&state, &keys, 0, 0, &parameters);
        let expected = get_validated_indexed_attestation(&state, &original, &parameters).unwrap();
        let before = state.clone();
        let mut checked = CheckedAttestationState::new(&state, &parameters).unwrap();
        assert_eq!(checked.validate(&original).unwrap(), expected);
        assert_eq!(checked.validate(&original).unwrap(), expected);
        assert_eq!(checked.epochs.len(), 1);
        assert_eq!(checked.committees.len(), 1);
        let mut invalid = original.clone();
        invalid.signature = [0; 96];
        assert!(checked.validate(&invalid).is_err());
        assert_eq!(checked.validate(&original).unwrap(), expected);
        let mut wrong_domain = original.clone();
        wrong_domain.data.target.epoch += 1;
        assert!(checked.validate(&wrong_domain).is_err());
        let mut wrong_committee = original.clone();
        wrong_committee.committee_bits =
            crate::ssz::PresetBits::from_ssz(&[8], parameters.preset()).unwrap();
        assert!(checked.validate(&wrong_committee).is_err());
        assert_eq!(state, before);
    }

    #[test]
    fn preparation_is_bounded_and_requires_a_complete_state() {
        let (state, _, parameters) = crate::gloas_processing::blocks::tests::state_and_keys();
        let mut checked = CheckedAttestationState::new(&state, &parameters).unwrap();
        for slot in 0..288 {
            let expected = CommitteeInputs::new(&state, slot, &checked.constants)
                .unwrap()
                .committee(slot, 0, &checked.constants)
                .unwrap();
            assert_eq!(checked.committee(slot, 0).unwrap(), expected);
            assert!(checked.epochs.len() <= 2);
            assert!(checked.committees.len() <= 128);
            assert!(checked.committee_bytes <= 8 * 1024 * 1024);
        }
        drop(checked);
        let mut malformed = state.clone();
        malformed.current_epoch_participation.values.pop();
        assert!(CheckedAttestationState::new(&malformed, &parameters).is_err());
        malformed = state;
        malformed.randao_mixes.values.pop();
        assert!(CheckedAttestationState::new(&malformed, &parameters).is_err());
    }
}
