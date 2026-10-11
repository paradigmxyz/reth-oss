//! Native signed historical slashing replay against a genuine current justified registry.
use super::*;
use reth_embedded_beacon_primitives::Fork;
use reth_embedded_beacon_state_transition::fork_processing::TransitionParameters;
impl GloasStore {
    pub(super) fn original_attester_slashing(
        &mut self,
        bytes: &[u8],
        fork: Fork,
        parameters: &TransitionParameters,
        verification_slot: u64,
    ) -> Result<(), StoreError> {
        parameters.validate()?;
        if parameters.gloas != self.processing {
            return Err(StoreError::InvalidParameters)
        }
        let state = self.justified_state()?;
        if verification_slot > state.slot() {
            return Err(StoreError::InvalidParameters)
        }
        let indices = crate::slashing_replay::authenticate(
            bytes,
            fork,
            state.validators(),
            state.data().genesis_validators_root(),
            parameters,
            verification_slot,
        )?;
        drop(state);
        self.equivocating.extend(indices);
        Ok(())
    }
}

impl GloasStore {
    pub(super) fn retained_slashing_context(
        &self,
    ) -> Result<crate::slashing_replay::SlashingContext, StoreError> {
        let state = self.justified_state()?;
        Ok(crate::slashing_replay::SlashingContext { verification_slot: state.slot() })
    }
}
