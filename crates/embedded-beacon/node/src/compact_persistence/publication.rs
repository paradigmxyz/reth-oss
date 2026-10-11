//! Exact native replay closures and prepared canonical candidates are data-only recovery inputs.
//! An uncertain canonical call is reconstructed to its precise candidate before a fresh
//! same-adapter acknowledgement; none of these durable descriptors grants graph authority.

use super::*;

#[cfg(all(feature = "peer-runtime", feature = "execution-verification"))]
use reth_embedded_beacon_fork_choice::compact::AdmittedSnapshot;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ReplayClosure {
    pub(super) roots: BTreeSet<Root>,
    pub(super) envelopes: BTreeSet<Root>,
    #[serde(default)]
    pub(super) events: Option<BTreeSet<u64>>,
    #[serde(default)]
    pub(super) time_ms: Option<u64>,
}
#[cfg(all(feature = "peer-runtime", feature = "execution-verification"))]
impl ReplayClosure {
    pub(super) fn from_admitted(admitted: &AdmittedSnapshot) -> Self {
        Self {
            roots: admitted.retained_roots().clone(),
            envelopes: admitted.verified_envelope_roots().clone(),
            events: None,
            time_ms: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PreparedPublication {
    pub(super) snapshot: Snapshot,
    pub(super) closure: ReplayClosure,
    pub(super) recovery: Option<retention::RecoveryGeneration>,
}

impl Inner {
    pub(super) fn replay_recovery(&self) -> Option<&retention::RecoveryGeneration> {
        self.prepared
            .as_ref()
            .and_then(|prepared| prepared.recovery.as_ref())
            .or(self.recovery.as_ref())
    }

    pub(super) fn replay_closure(&self) -> Option<&ReplayClosure> {
        self.prepared.as_ref().map(|prepared| &prepared.closure).or(self.current_closure.as_ref())
    }
}

impl AdmittedStateRepository {
    pub(super) fn make_snapshot(
        &self,
        inner: &Inner,
        actual: StoreSnapshot,
    ) -> Result<Snapshot, PersistenceError> {
        let record =
            inner.records.get(&actual.head.root).ok_or(PersistenceError::MissingPredecessor)?;
        let has_payload = actual.head.payload_status.is_none() ||
            actual.head.payload_status == Some(PayloadStatus::Full);
        let snapshot = Snapshot {
            block_root: actual.head.root,
            state_root: record.state_root,
            slot: actual.head_slot,
            payload: actual.head.payload_status.map(SavedPayloadStatus::from_status),
            justified: actual.justified_checkpoint,
            finalized: actual.finalized_checkpoint,
            execution: if has_payload { record.execution } else { None },
            envelope_root: if actual.head.payload_status == Some(PayloadStatus::Full) {
                record.envelope.as_ref().map(|value| value.signed_root)
            } else {
                None
            },
        };
        snapshot.validate(&inner.records)?;
        self.check_snapshot_data(&inner.records, &snapshot)?;
        Ok(snapshot)
    }

    pub(super) fn check_closure(
        &self,
        inner: &Inner,
        snapshot: &Snapshot,
        closure: &ReplayClosure,
        recovery: Option<&retention::RecoveryGeneration>,
    ) -> Result<(), PersistenceError> {
        let origin = inner.anchor.as_ref().ok_or(PersistenceError::MissingAnchor)?;
        if closure.events.is_some() != closure.time_ms.is_some() {
            return Err(PersistenceError::Linkage)
        }
        if let Some(events) = &closure.events {
            if events.len() > events::MAX_EVENTS {
                return Err(PersistenceError::Capacity)
            }
            let mut seen_blocks = BTreeSet::new();
            let mut seen_events = BTreeSet::new();
            self.visit_events(inner, |event| {
                if !events.contains(&event.sequence) {
                    return Ok(())
                }
                seen_events.insert(event.sequence);
                if event.completed_ms > closure.time_ms.ok_or(PersistenceError::Linkage)? ||
                    event.referenced_root().is_some_and(|root| !closure.roots.contains(&root))
                {
                    return Err(PersistenceError::Linkage)
                }
                if let events::EventAction::Block(root) = event.action {
                    seen_blocks.insert(root);
                }
                Ok(())
            })?;
            if &seen_events != events {
                return Err(PersistenceError::Linkage)
            }
            let seed_root = recovery
                .map_or(origin.checkpoint.root, |generation| generation.anchor.checkpoint.root);
            if closure.roots.iter().any(|root| *root != seed_root && !seen_blocks.contains(root)) {
                return Err(PersistenceError::Linkage)
            }
        }
        let seed = recovery.map_or(origin, |generation| &generation.anchor);
        if closure.roots.is_empty() ||
            closure.roots.len() > self.max_records ||
            !closure.roots.contains(&seed.checkpoint.root) ||
            !closure.envelopes.is_subset(&closure.roots) ||
            [snapshot.block_root, snapshot.justified.root, snapshot.finalized.root]
                .iter()
                .any(|root| !closure.roots.contains(root)) ||
            (snapshot.payload == Some(SavedPayloadStatus::Full) &&
                !closure.envelopes.contains(&snapshot.block_root))
        {
            return Err(PersistenceError::Linkage)
        }
        for root in &closure.roots {
            let record = inner.records.get(root).ok_or(PersistenceError::Linkage)?;
            record.validate(&self.network)?;
            if record.slot < seed.slot ||
                (*root != seed.checkpoint.root && !closure.roots.contains(&record.parent_root)) ||
                (closure.envelopes.contains(root) && record.envelope.is_none())
            {
                return Err(PersistenceError::Linkage)
            }
        }
        if let Some(generation) = recovery {
            self.check_recovery_generation(&inner.records, generation)?;
            if generation.roots != closure.roots {
                return Err(PersistenceError::Linkage)
            }
        }
        Ok(())
    }

    pub(super) fn publication_recovery(
        &self,
        inner: &Inner,
        supplied: Option<retention::RecoveryGeneration>,
        closure: Option<&ReplayClosure>,
    ) -> Result<Option<retention::RecoveryGeneration>, PersistenceError> {
        let mut active = supplied.or_else(|| inner.replay_recovery().cloned());
        if let Some(generation) = &mut active {
            if let Some(closure) = closure {
                generation.roots = closure.roots.clone();
            } else {
                let mut ordered = inner.records.values().collect::<Vec<_>>();
                ordered.sort_unstable_by_key(|record| record.slot);
                for record in ordered {
                    if record.slot > generation.anchor.slot &&
                        generation.roots.contains(&record.parent_root)
                    {
                        generation.roots.insert(record.block_root);
                    }
                }
            }
            self.check_recovery_generation(&inner.records, generation)?;
        }
        Ok(active)
    }

    #[cfg(all(feature = "peer-runtime", feature = "execution-verification"))]
    pub(super) fn prepare_publication(
        &self,
        inner: &mut Inner,
        actual: StoreSnapshot,
        closure: ReplayClosure,
        supplied: Option<retention::RecoveryGeneration>,
    ) -> Result<(), PersistenceError> {
        let snapshot = self.make_snapshot(inner, actual)?;
        let recovery = self.publication_recovery(inner, supplied, Some(&closure))?;
        self.check_closure(inner, &snapshot, &closure, recovery.as_ref())?;
        if inner.current.as_ref() == Some(&snapshot) &&
            inner.current_closure.as_ref() == Some(&closure) &&
            inner.recovery == recovery &&
            inner.prepared.is_none()
        {
            return Ok(())
        }
        let candidate = PreparedPublication { snapshot, closure, recovery };
        if let Some(prepared) = &inner.prepared {
            return if prepared == &candidate { Ok(()) } else { Err(PersistenceError::Linkage) }
        }
        let mut next = Saved::capture(self.network.consensus_digest, inner);
        next.prepared = Some(candidate.clone());
        // Complete signed data and state owners already exist. This pointer remains explicitly
        // prepared until the actual canonical Engine acknowledgement and canonical journal commit.
        inner.journal.commit(&next)?;
        inner.prepared = Some(candidate);
        Ok(())
    }

    #[cfg(all(feature = "peer-runtime", feature = "execution-verification"))]
    pub(crate) fn replay_snapshot(&self) -> Result<Option<PersistedSnapshot>, PersistenceError> {
        let inner = self.lock()?;
        Ok(inner
            .prepared
            .as_ref()
            .map(|prepared| prepared.snapshot.clone())
            .or_else(|| inner.current.clone())
            .map(|snapshot| PersistedSnapshot { snapshot }))
    }

    #[cfg(all(feature = "peer-runtime", feature = "execution-verification"))]
    pub(crate) fn has_prepared_publication(&self) -> Result<bool, PersistenceError> {
        Ok(self.lock()?.prepared.is_some())
    }

    #[cfg(all(feature = "peer-runtime", feature = "execution-verification"))]
    pub(crate) fn replay_contains_envelope(&self, root: Root) -> Result<bool, PersistenceError> {
        let inner = self.lock()?;
        Ok(inner.replay_closure().map_or_else(
            || inner.records.get(&root).is_some_and(|record| record.envelope.is_some()),
            |closure| closure.envelopes.contains(&root),
        ))
    }
}
