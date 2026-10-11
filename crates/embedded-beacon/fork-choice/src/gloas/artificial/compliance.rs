//! Fixed artificial compliance block events with independently admitted included votes.
//! This module is confined to the BLS-disabled fixture wrapper and grants no runtime graph.

use super::ArtificialSpecStore;
use crate::{gloas::GloasStore, StoreError};
use reth_embedded_beacon_state_transition::{
    gloas_processing,
    phase0_processing::ProcessingError,
    ssz::{auxiliary::gloas::PayloadAttestationMessage, gloas::SignedBeaconBlock},
};

/// The primary compliance runner suppresses only normative vote-admission assertions.
/// Encoding, allocation, arithmetic, configuration and graph invariant errors remain fatal.
fn normative_vote_rejection(error: &StoreError) -> bool {
    match error {
        StoreError::UnknownRoot |
        StoreError::InvalidAttestationTime |
        StoreError::InvalidAttestationTarget |
        StoreError::InvalidSlashing => true,
        StoreError::Processing(ProcessingError::InvalidState(reason)) => matches!(
            *reason,
            "attestation committee index is out of range" |
                "multi-committee aggregation bits are truncated" |
                "aggregation length differs from selected committees" |
                "indexed participants are not nonempty sorted unique indexes" |
                "validator index is out of range" |
                "payload attester is not in the assigned PTC"
        ),
        _ => false,
    }
}

impl ArtificialSpecStore {
    /// Import the exact complete block, then consider each included vote independently.
    /// Only this artificial fixture type suppresses normative vote assertions. Mandatory
    /// transition/post-root validation and all infrastructure failures remain enforced.
    pub fn on_compliance_block(&mut self, signed: &SignedBeaconBlock) -> Result<(), StoreError> {
        let mut candidate = self.clone();
        candidate.inner.on_block_inner::<false>(signed)?;
        for attestation in &signed.message.body.attestations.values {
            candidate
                .compliance_vote(|store| store.attestation_inner::<false>(attestation, true))?;
        }
        for slashing in &signed.message.body.attester_slashings.values {
            candidate
                .compliance_vote(|store| store.on_attester_slashing_inner::<false>(slashing))?;
        }
        let root = signed.message.hash_tree_root(candidate.inner.processing.preset())?;
        let state = candidate.inner.read_block_state(root)?.ok_or(StoreError::UnknownRoot)?;
        for attestation in &signed.message.body.payload_attestations.values {
            // Message reconstruction is outside the source runner's assertion-suppression scope.
            let indexed = gloas_processing::get_indexed_payload_attestation(
                &state,
                attestation,
                &candidate.inner.processing,
            )?;
            for index in indexed.attesting_indices {
                let message = PayloadAttestationMessage {
                    validator_index: index,
                    data: indexed.data.clone(),
                    signature: [0; 96],
                };
                candidate.compliance_vote(|store| {
                    store.payload_message_inner::<false>(&message, true)
                })?;
            }
        }
        candidate.inner.check_budget()?;
        *self = candidate;
        Ok(())
    }

    fn compliance_vote(
        &mut self,
        operation: impl FnOnce(&mut GloasStore) -> Result<(), StoreError>,
    ) -> Result<(), StoreError> {
        let mut candidate = self.inner.clone();
        match operation(&mut candidate) {
            Ok(()) => {
                candidate.check_budget()?;
                self.inner = candidate;
                Ok(())
            }
            Err(error) if normative_vote_rejection(&error) => Ok(()),
            Err(error) => Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gloas::{
        tests::{child, child_with_attestations, signing_root, store_and_keys, DST},
        LatestMessage,
    };
    use reth_embedded_beacon_primitives::{Checkpoint, Fork, SszError};
    use reth_embedded_beacon_state_transition::ssz::{gloas, Preset, PresetBits, Ssz};

    #[test]
    fn suppression_is_limited_to_normative_vote_assertions() {
        for error in [
            StoreError::UnknownRoot,
            StoreError::InvalidAttestationTarget,
            StoreError::InvalidAttestationTime,
            StoreError::InvalidSlashing,
            StoreError::Processing(ProcessingError::InvalidState(
                "validator index is out of range",
            )),
        ] {
            assert!(normative_vote_rejection(&error));
        }
        for error in [
            StoreError::ResourceLimit,
            StoreError::Overflow,
            StoreError::ValidationUnavailable(Fork::Gloas),
            StoreError::InvalidParameters,
            StoreError::Ssz(SszError::InvalidOffset),
            StoreError::Processing(ProcessingError::Arithmetic("vote overflow")),
            StoreError::Processing(ProcessingError::InvalidParameters("invalid preset")),
            StoreError::Kernel(crate::ForkChoiceError::UnknownRoot),
            StoreError::Processing(ProcessingError::InvalidState("PTC window is truncated")),
            StoreError::Processing(ProcessingError::InvalidState("RANDAO ring is truncated")),
            StoreError::Processing(ProcessingError::InvalidState(
                "Gloas validator-indexed list lengths differ",
            )),
            StoreError::Processing(ProcessingError::InvalidState("unclassified kernel failure")),
        ] {
            assert!(!normative_vote_rejection(&error));
        }
    }

    #[test]
    fn compliance_retains_valid_block_and_admits_later_vote_after_unknown_root() {
        let (mut runtime, keys) = store_and_keys();
        let anchor = runtime.get_head().unwrap().root;
        let mut fixture = ArtificialSpecStore::get_store(
            runtime.block_state(anchor).unwrap().clone(),
            runtime.block_arc(anchor).unwrap().as_ref().clone(),
            runtime.parameters,
            runtime.processing.clone(),
        )
        .unwrap();
        let (first, state) = child(&runtime, &keys);
        let first_root = first.message.hash_tree_root(Preset::Minimal).unwrap();
        runtime.on_tick(6000).unwrap();
        fixture.on_tick(6000).unwrap();
        runtime.on_block_and_contents(&first).unwrap();
        fixture.on_compliance_block(&first).unwrap();
        let committee =
            gloas_processing::get_beacon_committee(&state, 1, 0, &runtime.processing).unwrap();
        assert!(!committee.is_empty());
        let participant = committee[0];
        let mut encoded = vec![0; committee.len() / 8 + 1];
        encoded[0] = 1;
        encoded[committee.len() / 8] |= 1 << (committee.len() % 8);
        let vote = |root| {
            let data = gloas::AttestationData {
                slot: 1,
                index: 0,
                beacon_block_root: root,
                source: state.current_justified_checkpoint,
                target: Checkpoint { epoch: 0, root: anchor },
            };
            let signature = keys[participant as usize]
                .sign(
                    &signing_root(
                        &state,
                        data.hash_tree_root(Preset::Minimal).unwrap(),
                        [1, 0, 0, 0],
                    ),
                    DST,
                    &[],
                )
                .to_bytes();
            gloas::Attestation {
                aggregation_bits: gloas::AggregationBits::from_ssz(&encoded, Preset::Minimal)
                    .unwrap(),
                data,
                signature,
                committee_bits: PresetBits::from_ssz(&[1], Preset::Minimal).unwrap(),
            }
        };
        let (second, expected) =
            child_with_attestations(&runtime, &keys, vec![vote([240; 32]), vote(first_root)]);
        let second_root = second.message.hash_tree_root(Preset::Minimal).unwrap();
        runtime.on_tick(12_000).unwrap();
        fixture.on_tick(12_000).unwrap();
        let before = runtime.snapshot().unwrap();
        assert!(matches!(runtime.on_block_and_contents(&second), Err(StoreError::UnknownRoot)));
        assert_eq!(runtime.snapshot().unwrap(), before);
        assert!(runtime.block_state(second_root).is_none());
        fixture.on_compliance_block(&second).unwrap();
        assert_eq!(fixture.block_state(second_root), Some(&expected));
        assert_eq!(
            fixture.inner.latest_messages.get(&participant),
            Some(&LatestMessage { slot: 1, root: first_root, payload_present: false })
        );
        assert!(runtime.latest_messages.is_empty());
    }

    #[test]
    fn failed_compliance_vote_rolls_back_local_mutations_and_propagates_resource_error() {
        let (runtime, _) = store_and_keys();
        let anchor = runtime.get_head().unwrap().root;
        let mut fixture = ArtificialSpecStore::get_store(
            runtime.block_state(anchor).unwrap().clone(),
            runtime.block_arc(anchor).unwrap().as_ref().clone(),
            runtime.parameters,
            runtime.processing.clone(),
        )
        .unwrap();
        let before = fixture.get_head().unwrap();
        let result = fixture.compliance_vote(|store| {
            store
                .latest_messages
                .insert(0, LatestMessage { slot: 0, root: anchor, payload_present: false });
            Err(StoreError::ResourceLimit)
        });
        assert!(matches!(result, Err(StoreError::ResourceLimit)));
        assert!(fixture.inner.latest_messages.is_empty());
        assert_eq!(fixture.get_head().unwrap(), before);
        assert!(runtime.latest_messages.is_empty());
    }
}
