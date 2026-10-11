//! Native signed historical slashing replay, preserving exact old wire domains.
use super::*;
impl Store {
    pub(super) fn original_attester_slashing(
        &mut self,
        bytes: &[u8],
        fork: Fork,
        parameters: &TransitionParameters,
        verification_slot: u64,
    ) -> Result<(), StoreError> {
        if self.transition_parameters.as_ref() != Some(parameters) {
            return Err(StoreError::InvalidParameters)
        }
        let (justified, _) = self.kernel.checkpoints();
        let state = self.read_block_state(justified.root)?.ok_or(StoreError::UnknownRoot)?;
        if verification_slot > state.slot() {
            return Err(StoreError::InvalidParameters)
        }
        let indices = crate::slashing_replay::authenticate(
            bytes,
            fork,
            crate::fast_confirmation::StateRef::Classic(&state).validators(),
            state.genesis_validators_root(),
            parameters,
            verification_slot,
        )?;
        drop(state);
        self.kernel.equivocating.extend(indices);
        Ok(())
    }
}

impl Store {
    pub(super) fn retained_slashing_context(
        &self,
    ) -> Result<crate::slashing_replay::SlashingContext, StoreError> {
        let (justified, _) = self.kernel.checkpoints();
        let state = self.read_block_state(justified.root)?.ok_or(StoreError::UnknownRoot)?;
        Ok(crate::slashing_replay::SlashingContext { verification_slot: state.slot() })
    }
}
