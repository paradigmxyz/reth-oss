//! Actual applied transcript IDs follow native receipts, never serialized vote weights.
use super::{
    super::events::{DurableEvent, EventAction, MAX_EVENTS},
    *,
};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct AppliedEvents {
    enabled: bool,
    sequences: BTreeSet<u64>,
}

/// Private construction receipt for one exact prepared candidate and transcript. This caches
/// filtering work only; prepare and commit still independently validate all durable inputs.
#[derive(Debug)]
pub(super) struct PreparedReplayClosure {
    admitted: AdmittedSnapshot,
    applied: AppliedEvents,
    recovery: Option<retention::RecoveryGeneration>,
    anchor: Option<Anchor>,
    closure: publication::ReplayClosure,
}

impl PreparedReplayClosure {
    pub(super) fn closure(&self) -> &publication::ReplayClosure {
        &self.closure
    }

    fn matches(
        &self,
        admitted: &AdmittedSnapshot,
        applied: &AppliedEvents,
        recovery: Option<&retention::RecoveryGeneration>,
        repository: &Inner,
    ) -> bool {
        self.admitted.identity() == admitted.identity() &&
            self.admitted.snapshot() == admitted.snapshot() &&
            self.admitted.time_ms() == admitted.time_ms() &&
            self.admitted.retained_roots() == admitted.retained_roots() &&
            self.admitted.verified_envelope_roots() == admitted.verified_envelope_roots() &&
            &self.applied == applied &&
            self.recovery.as_ref() == recovery.or_else(|| repository.replay_recovery()) &&
            self.anchor == repository.anchor
    }
}

impl RepositoryStateSource {
    pub(crate) fn enable_event_transcript(&self) -> Result<(), PersistenceError> {
        self.events.lock().map_err(|_| PersistenceError::CoordinatorPoisoned)?.enabled = true;
        Ok(())
    }
    /// The caller just ran the actual block/envelope/gossip/tick native kernel. The sealed staged
    /// snapshot independently binds this row to its candidate; original bytes remain data only.
    pub(crate) fn record_event(
        &self,
        expected: StoreSnapshot,
        event: DurableEvent,
        original: Option<&[u8]>,
        replay: bool,
    ) -> Result<(), PersistenceError> {
        let staged = self.derived.lock().map_err(|_| PersistenceError::CoordinatorPoisoned)?;
        let admitted = staged.staged.as_ref().ok_or(PersistenceError::Linkage)?;
        if admitted.identity() != self.identity() ||
            admitted.snapshot() != expected ||
            (admitted.time_ms() != event.completed_ms &&
                !(replay &&
                    matches!(event.action, EventAction::Slashing { .. }) &&
                    event.completed_ms <= admitted.time_ms())) ||
            event
                .referenced_root()
                .is_some_and(|root| !admitted.retained_roots().contains(&root)) ||
            matches!(event.action, EventAction::Envelope(root) if !admitted.verified_envelope_roots().contains(&root))
        {
            return Err(PersistenceError::Linkage)
        }
        let mut applied = self.events.lock().map_err(|_| PersistenceError::CoordinatorPoisoned)?;
        if !applied.enabled {
            return Err(PersistenceError::Linkage)
        }
        if applied.sequences.len() >= MAX_EVENTS {
            return Err(PersistenceError::Capacity)
        }
        let event = if replay {
            if self.repository.original_event(event.sequence)? != event {
                return Err(PersistenceError::Linkage)
            }
            event
        } else {
            self.repository.append_event(event, original)?
        };
        applied.sequences.insert(event.sequence);
        Ok(())
    }
    pub(super) fn prepare_replay_closure(
        &self,
        admitted: &AdmittedSnapshot,
        recovery: Option<&retention::RecoveryGeneration>,
    ) -> Result<PreparedReplayClosure, PersistenceError> {
        let mut closure = publication::ReplayClosure::from_admitted(admitted);
        let applied = self.events.lock().map_err(|_| PersistenceError::CoordinatorPoisoned)?;
        let repository = self.repository.lock()?;
        if applied.enabled {
            let mut events = BTreeSet::new();
            let seed = recovery
                .or_else(|| repository.replay_recovery())
                .map(|generation| generation.anchor.checkpoint.root)
                .or_else(|| repository.anchor.as_ref().map(|anchor| anchor.checkpoint.root))
                .ok_or(PersistenceError::MissingAnchor)?;
            let seed_slot = recovery.or_else(|| repository.replay_recovery()).map_or_else(
                || {
                    repository
                        .anchor
                        .as_ref()
                        .map(|anchor| {
                            anchor.checkpoint.epoch * self.repository.network.config.slots_per_epoch
                        })
                        .unwrap_or(0)
                },
                |generation| {
                    generation
                        .anchor
                        .boundary
                        .as_ref()
                        .map_or(generation.anchor.slot, |boundary| boundary.slot)
                },
            );
            self.repository.visit_events(&repository, |event| {
                if !applied.sequences.contains(&event.sequence) { return Ok(()) }
                if let EventAction::Slashing { fork, original, .. } = event.action {
                    if self.slashing_still_relevant(&repository, recovery, fork, original)? {
                        events.insert(event.sequence);
                    }
                    return Ok(())
                }
                if event.referenced_root().is_none_or(|root| closure.roots.contains(&root)) &&
                    !matches!(event.action, EventAction::Block(root) if root == seed) &&
                    !matches!(event.action, EventAction::Envelope(root) if root == seed || !closure.envelopes.contains(&root)) &&
                    self.repository
                        .network
                        .config
                        .clock()
                        .map_err(|_| PersistenceError::Identity)?
                        .slot_at(event.begin_ms)
                        .ok_or(PersistenceError::Linkage)? >=
                        seed_slot
                {
                    events.insert(event.sequence);
                }
                Ok(())
            })?;
            closure.events = Some(events);
            closure.time_ms = Some(admitted.time_ms());
        }
        Ok(PreparedReplayClosure {
            admitted: admitted.clone(),
            applied: applied.clone(),
            recovery: recovery.or_else(|| repository.replay_recovery()).cloned(),
            anchor: repository.anchor.clone(),
            closure,
        })
    }

    pub(super) fn commit_replay_closure(
        &self,
        prepared: Option<PreparedReplayClosure>,
        admitted: &AdmittedSnapshot,
        recovery: Option<&retention::RecoveryGeneration>,
    ) -> Result<publication::ReplayClosure, PersistenceError> {
        if let Some(prepared) = prepared {
            let applied = self.events.lock().map_err(|_| PersistenceError::CoordinatorPoisoned)?;
            let repository = self.repository.lock()?;
            if prepared.matches(admitted, &applied, recovery, &repository) {
                return Ok(prepared.closure)
            }
        }
        // Same-head votes, an altered root closure or a new recovery boundary invalidate the
        // private memo. Reconstruct normally; the prepared durable record must still agree.
        Ok(self.prepare_replay_closure(admitted, recovery)?.closure)
    }
    pub(super) fn retained_event_ids(
        &self,
        inner: &Inner,
    ) -> Result<BTreeSet<u64>, PersistenceError> {
        let mut ids = BTreeSet::new();
        for closure in inner
            .current_closure
            .iter()
            .chain(&inner.previous_closure)
            .chain(inner.prepared.iter().map(|prepared| &prepared.closure))
        {
            if let Some(events) = &closure.events {
                ids.extend(events.iter().copied());
            }
        }
        Ok(ids)
    }
    pub(super) fn forget_retired_events(
        &self,
        keep: &BTreeSet<u64>,
    ) -> Result<(), PersistenceError> {
        self.events
            .lock()
            .map_err(|_| PersistenceError::CoordinatorPoisoned)?
            .sequences
            .retain(|sequence| keep.contains(sequence));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{super::tests::retained_phase0_history, *};

    #[test]
    fn prepared_closure_is_bound_to_exact_candidate_events_and_recovery() {
        let (_directory, repository, source, mut graph, _) = retained_phase0_history(None);
        let admitted = source.derived.lock().unwrap().staged.clone().unwrap();
        let prepared = source.prepare_replay_closure(&admitted, None).unwrap();
        let applied = source.events.lock().unwrap().clone();
        assert!(prepared.matches(&admitted, &applied, None, &repository.lock().unwrap()));

        let mut changed = applied.clone();
        changed.enabled = !changed.enabled;
        assert!(!prepared.matches(&admitted, &changed, None, &repository.lock().unwrap()));
        let mut changed = applied.clone();
        changed.sequences.insert(19);
        assert!(!prepared.matches(&admitted, &changed, None, &repository.lock().unwrap()));

        // An off-block tick can retain the same selected head but alter the exact event clock.
        let snapshot = graph.snapshot().unwrap();
        graph.on_tick(graph.time_ms() + 1).unwrap();
        assert_eq!(graph.snapshot().unwrap(), snapshot);
        let changed = source.derived.lock().unwrap().staged.clone().unwrap();
        assert!(!prepared.matches(&changed, &applied, None, &repository.lock().unwrap()));

        let mut repository = repository.lock().unwrap();
        let original = repository.anchor.clone().unwrap();
        repository.anchor.as_mut().unwrap().slot += 1;
        assert!(!prepared.matches(&admitted, &applied, None, &repository));
        repository.anchor = Some(original.clone());
        let generation = retention::RecoveryGeneration {
            anchor: original,
            roots: admitted.retained_roots().clone(),
        };
        assert!(!prepared.matches(&admitted, &applied, Some(&generation), &repository));
    }
}
