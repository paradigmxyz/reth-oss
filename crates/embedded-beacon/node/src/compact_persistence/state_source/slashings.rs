//! Signed equivocation evidence retires only when the finalized registry makes it irrelevant.
use super::*;
use reth_embedded_beacon_fork_choice::fast_confirmation::StateRef;
impl RepositoryStateSource {
    pub(super) fn slashing_still_relevant(
        &self,
        inner: &Inner,
        recovery: Option<&retention::RecoveryGeneration>,
        fork: Fork,
        original: ObjectId,
    ) -> Result<bool, PersistenceError> {
        let anchor = recovery
            .or_else(|| inner.replay_recovery())
            .map(|generation| &generation.anchor)
            .or(inner.anchor.as_ref())
            .ok_or(PersistenceError::MissingAnchor)?;
        let (root, schema, slot) = if let Some(boundary) = &anchor.boundary {
            (boundary.state_root, boundary.fork, boundary.slot)
        } else {
            let record =
                inner.records.get(&anchor.checkpoint.root).ok_or(PersistenceError::Linkage)?;
            (record.state_root, record.fork, record.slot)
        };
        let state = self
            .repository
            .compact
            .lease(root, schema, self.repository.network.preset())?
            .ok_or(PersistenceError::Linkage)?;
        let bytes = self
            .repository
            .shared
            .get_bounded(original, self.repository.network.config.max_payload_size)?
            .ok_or(PersistenceError::Linkage)?;
        let participants =
            reth_embedded_beacon_fork_choice::slashing_replay::original_participants(
                &bytes,
                fork,
                self.repository.network.preset(),
            )
            .map_err(|_| PersistenceError::Linkage)?;
        let epoch = slot / self.repository.network.config.slots_per_epoch;
        let validators = StateRef::Classic(state.state()).validators();
        // Slashed but still active validators can affect native head-weakness measurements.
        // Keep their real proof until every participant has irreversibly exited at this actual
        // finalized boundary. An absent registry entry always keeps the proof.
        Ok(participants.iter().any(|index| {
            usize::try_from(*index)
                .ok()
                .and_then(|index| validators.get(index))
                .is_none_or(|validator| validator.exit_epoch > epoch)
        }))
    }
}
