//! Local finalized recovery generations preserve the immutable original trust descriptor.
//! Canonical head and the exact independently replayed cut boundary commit in one journal record.
//! Old original/current/previous data remain pinned until their published generations are obsolete.

use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RecoveryGeneration {
    pub(super) anchor: Anchor,
    pub(super) roots: BTreeSet<Root>,
}

/// Private data-only deletion plan published with its filtered admission generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RecoveryCleanup {
    pub(super) states: BTreeSet<Root>,
    pub(super) obsolete: BTreeSet<Root>,
}

/// Checked local recovery data; it cannot be constructed from public roots or validity flags.
#[derive(Debug)]
#[cfg(all(feature = "peer-runtime", feature = "execution-verification"))]
pub(crate) struct LocalRecoveryInputs {
    pub(crate) checkpoint: Checkpoint,
    pub(crate) post: StateLease,
    pub(crate) boundary: StateLease,
    pub(crate) block: BeaconBlock,
}

impl AdmittedStateRepository {
    // Reauthenticating an active local seed must not replace the immutable origin. The caller
    // has independently verified complete SSZ and roots before reaching this comparison.
    pub(super) fn existing_seed(
        &self,
        inner: &Inner,
        anchor: &Anchor,
        record: &Admission,
    ) -> Result<bool, PersistenceError> {
        let Some(origin) = &inner.anchor else { return Ok(false) };
        let origin_zero_gap = anchor.boundary.is_none() &&
            origin.boundary.as_ref().is_some_and(|boundary| {
                origin.checkpoint == anchor.checkpoint &&
                    origin.state_root == anchor.state_root &&
                    origin.slot == anchor.slot &&
                    origin.genesis == anchor.genesis &&
                    boundary.slot == anchor.slot &&
                    boundary.state_root == anchor.state_root
            });
        let descriptor = if origin == anchor || origin_zero_gap {
            origin
        } else if let Some(generation) = inner.replay_recovery() {
            let local = &generation.anchor;
            let zero_gap = anchor.boundary.is_none() &&
                local.boundary.as_ref().is_some_and(|boundary| {
                    boundary.slot == anchor.slot && boundary.state_root == anchor.state_root
                });
            if local.checkpoint != anchor.checkpoint ||
                local.state_root != anchor.state_root ||
                local.slot != anchor.slot ||
                local.genesis != anchor.genesis ||
                (!zero_gap && local.boundary != anchor.boundary)
            {
                return Err(PersistenceError::ConflictingAnchor)
            }
            self.check_recovery_generation(&inner.records, generation)?;
            local
        } else {
            return Err(PersistenceError::ConflictingAnchor)
        };
        let existing = inner.records.get(&record.block_root).ok_or(PersistenceError::Linkage)?;
        let mut original = existing.clone();
        // Independent envelope verification updates the association. Original checkpoint
        // authentication cannot erase it or compare it with the undecorated signed block.
        original.envelope = None;
        original.execution = record.execution;
        if original != *record {
            return Err(PersistenceError::Linkage)
        }
        self.check_saved_record(existing)?;
        self.check_saved_boundary(descriptor)?;
        Ok(true)
    }

    pub(super) fn check_recovery_generation(
        &self,
        records: &BTreeMap<Root, Admission>,
        generation: &RecoveryGeneration,
    ) -> Result<(), PersistenceError> {
        let anchor = &generation.anchor;
        if anchor.genesis ||
            anchor.boundary.is_none() ||
            !generation.roots.contains(&anchor.checkpoint.root) ||
            generation.roots.len() > self.max_records
        {
            return Err(PersistenceError::Linkage)
        }
        let record = records.get(&anchor.checkpoint.root).ok_or(PersistenceError::Linkage)?;
        if record.state_root != anchor.state_root ||
            record.slot != anchor.slot ||
            record.format != CheckpointBlockFormat::Signed
        {
            return Err(PersistenceError::Linkage)
        }
        self.check_saved_record(record)?;
        self.check_saved_boundary(anchor)?;
        for root in &generation.roots {
            let record = records.get(root).ok_or(PersistenceError::Linkage)?;
            if record.slot < anchor.slot ||
                (*root != anchor.checkpoint.root &&
                    !generation.roots.contains(&record.parent_root))
            {
                return Err(PersistenceError::Linkage)
            }
        }
        Ok(())
    }

    pub(super) fn commit_canonical(
        &self,
        inner: &mut Inner,
        snapshot: Snapshot,
        recovery: Option<RecoveryGeneration>,
        closure: Option<publication::ReplayClosure>,
    ) -> Result<PersistedSnapshot, PersistenceError> {
        let active = self.publication_recovery(inner, recovery, closure.as_ref())?;
        if let Some(closure) = &closure {
            self.check_closure(inner, &snapshot, closure, active.as_ref())?;
        }
        if let Some(prepared) = &inner.prepared &&
            (prepared.snapshot != snapshot ||
                Some(&prepared.closure) != closure.as_ref() ||
                prepared.recovery != active)
        {
            return Err(PersistenceError::Linkage)
        }
        let mut next = Saved::capture(self.network.consensus_digest, inner);
        next.current = Some(snapshot.clone());
        next.previous = inner.current.clone();
        next.previous_recovery = inner.recovery.clone();
        next.recovery = active.clone();
        next.previous_closure = inner.current_closure.clone();
        next.current_closure = closure.clone();
        next.prepared = None;
        // New complete boundary/original data were pinned before entering this publication.
        // No obsolete owner is released before the canonical generation is durably committed.
        inner.journal.commit(&next)?;
        inner.previous = inner.current.replace(snapshot.clone());
        inner.previous_recovery = std::mem::replace(&mut inner.recovery, active);
        inner.previous_closure = std::mem::replace(&mut inner.current_closure, closure);
        inner.prepared = None;
        Ok(PersistedSnapshot { snapshot })
    }

    #[cfg(all(feature = "peer-runtime", feature = "execution-verification"))]
    pub(crate) fn local_recovery_inputs(
        &self,
    ) -> Result<Option<LocalRecoveryInputs>, PersistenceError> {
        let inner = self.lock()?;
        let Some(generation) = inner.replay_recovery() else { return Ok(None) };
        self.check_recovery_generation(&inner.records, generation)?;
        let anchor = &generation.anchor;
        let record = inner.records.get(&anchor.checkpoint.root).ok_or(PersistenceError::Linkage)?;
        let boundary = anchor.boundary.as_ref().ok_or(PersistenceError::Linkage)?;
        let post = self
            .compact
            .lease(record.state_root, record.fork, self.network.preset())?
            .ok_or(PersistenceError::Linkage)?;
        let state = self
            .compact
            .lease(boundary.state_root, boundary.fork, self.network.preset())?
            .ok_or(PersistenceError::Linkage)?;
        let bytes = self
            .shared
            .get_bounded(record.original, self.network.config.max_payload_size)?
            .ok_or(PersistenceError::Linkage)?;
        let signed = SignedBeaconBlock::from_ssz(&bytes, record.fork, self.network.preset())?;
        Ok(Some(LocalRecoveryInputs {
            checkpoint: anchor.checkpoint,
            post,
            boundary: state,
            block: signed.message(),
        }))
    }

    #[cfg(all(feature = "peer-runtime", feature = "execution-verification"))]
    pub(crate) fn checked_recovery_seed(
        &self,
        checkpoint: Checkpoint,
        state_root: Root,
        slot: u64,
    ) -> Result<(), PersistenceError> {
        let inner = self.lock()?;
        let origin = inner.anchor.as_ref().ok_or(PersistenceError::MissingAnchor)?;
        let anchor = inner.replay_recovery().map_or(origin, |generation| &generation.anchor);
        if anchor.checkpoint != checkpoint || anchor.state_root != state_root || anchor.slot != slot
        {
            return Err(PersistenceError::Linkage)
        }
        self.check_saved_record(
            inner.records.get(&checkpoint.root).ok_or(PersistenceError::Linkage)?,
        )?;
        self.check_saved_boundary(anchor)?;
        if let Some(generation) = inner.replay_recovery() {
            self.check_recovery_generation(&inner.records, generation)?;
        }
        // The original descriptor is independently retained even after local recovery advances.
        self.check_saved_record(
            inner.records.get(&origin.checkpoint.root).ok_or(PersistenceError::Linkage)?,
        )?;
        self.check_saved_boundary(origin)?;
        Ok(())
    }

    pub(super) fn finish_pending_cleanup(&self, inner: &mut Inner) -> Result<(), PersistenceError> {
        let Some(plan) = inner.cleanup.clone() else { return Ok(()) };
        if plan.states.is_empty() ||
            plan.states.len() > self.max_records ||
            plan.obsolete.iter().any(|root| inner.records.contains_key(root)) ||
            inner.records.values().any(|record| !plan.states.contains(&record.state_root))
        {
            return Err(PersistenceError::Linkage)
        }
        for anchor in inner
            .anchor
            .iter()
            .chain(inner.recovery.iter().map(|generation| &generation.anchor))
            .chain(inner.previous_recovery.iter().map(|generation| &generation.anchor))
        {
            if !plan.states.contains(&anchor.state_root) ||
                anchor
                    .boundary
                    .as_ref()
                    .is_some_and(|boundary| !plan.states.contains(&boundary.state_root))
            {
                return Err(PersistenceError::Linkage)
            }
        }
        // Only a journal-published exact obsolete set can be retried after a crash. Other
        // Consensus owners and independent execution/recovery owners are never swept.
        self.compact.release_retained_states(&plan.states)?;
        let obsolete = plan.obsolete.iter().copied().map(Owner::Consensus).collect::<Vec<_>>();
        if !obsolete.is_empty() {
            self.shared.release_owners(&obsolete)?;
        }
        let mut saved = Saved::capture(self.network.consensus_digest, inner);
        saved.cleanup = None;
        inner.journal.commit(&saved)?;
        inner.cleanup = None;
        Ok(())
    }

    #[cfg(all(feature = "peer-runtime", feature = "execution-verification"))]
    pub(crate) fn recovery_root(&self) -> Result<Option<Root>, PersistenceError> {
        let inner = self.lock()?;
        Ok(inner.replay_recovery().map(|generation| generation.anchor.checkpoint.root))
    }
}
