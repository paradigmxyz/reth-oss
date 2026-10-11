//! Proof retention receives the same private native seal as durable signed block retention.
use super::*;
impl CompactStore {
    pub(super) fn retain_block_and_availability<D: crate::data_availability::DataAvailability>(
        &self,
        signed: &SignedBeaconBlock,
        availability: &D,
    ) -> Result<(), StoreError> {
        let root = signed.message_root()?;
        let state = self.graph.read_block_state(root)?.ok_or(StoreError::UnknownRoot)?;
        let binding = self.block_binding(root, &state)?;
        let admitted = AdmittedState {
            binding: &binding,
            state: state.data(),
            signed: SignedBlockData::Classic(signed),
            verified_execution_hash: self.admitted_execution_hash(root)?,
            execution_optimistic: self.graph.execution_status(root) == ExecutionStatus::Optimistic,
            shared_state: Some(&state),
        };
        self.source.retain_block(admitted)?;
        availability
            .retain_admitted(&admitted)
            .map_err(|error| StoreError::DataAvailability(error.to_string()))
    }
}
