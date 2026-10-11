//! Obsolete descriptors are released only after both canonical publications finish.
//! The immutable origin, two recovery generations, and actual unfinalized graph form the union.
//! Each journal becomes authoritative before dependent owners can be released.

use super::*;

#[derive(Debug)]
pub(super) struct RetainedCleanup {
    pub(super) snapshot: StoreSnapshot,
    pub(super) roots: BTreeSet<Root>,
    pub(super) states: BTreeSet<Root>,
}

impl RepositoryStateSource {
    /// Called only by the owner after exact Engine/canonical/boundary publication succeeds.
    pub(crate) fn finish_retention(&self, actual: StoreSnapshot) -> Result<(), PersistenceError> {
        let mut derived = self.derived.lock().map_err(|_| PersistenceError::CoordinatorPoisoned)?;
        derived.journal.ensure_healthy()?;
        let Some(plan) = derived.published_cleanup.take() else { return Ok(()) };
        if plan.snapshot != actual {
            return Err(PersistenceError::Linkage)
        }
        // Validate all old records too: damaged committed data cannot be hidden by cleanup.
        for record in derived.records.values() {
            self.check_derived(record)?;
        }
        let mut repository = self.repository.lock()?;
        let current = repository.current.as_ref().ok_or(PersistenceError::Linkage)?;
        if current.block_root != actual.head.root ||
            current.slot != actual.head_slot ||
            current.justified != actual.justified_checkpoint ||
            current.finalized != actual.finalized_checkpoint
        {
            return Err(PersistenceError::Linkage)
        }
        let mut roots = plan.roots;
        let mut states = plan.states;
        let origin = repository.anchor.as_ref().ok_or(PersistenceError::MissingAnchor)?;
        roots.insert(origin.checkpoint.root);
        states.insert(origin.state_root);
        if let Some(boundary) = &origin.boundary {
            states.insert(boundary.state_root);
        }
        for generation in repository.recovery.iter().chain(&repository.previous_recovery) {
            self.repository.check_recovery_generation(&repository.records, generation)?;
            roots.extend(generation.roots.iter().copied());
            states.insert(generation.anchor.state_root);
            states.insert(
                generation.anchor.boundary.as_ref().ok_or(PersistenceError::Linkage)?.state_root,
            );
        }
        for closure in repository.current_closure.iter().chain(&repository.previous_closure) {
            roots.extend(closure.roots.iter().copied());
        }
        for snapshot in repository.current.iter().chain(&repository.previous) {
            self.repository.check_snapshot_data(&repository.records, snapshot)?;
            roots.extend([snapshot.block_root, snapshot.justified.root, snapshot.finalized.root]);
        }
        for record in repository.records.values() {
            self.repository.check_saved_record(record)?;
            if roots.contains(&record.block_root) {
                states.insert(record.state_root);
            }
        }
        if roots.iter().any(|root| !repository.records.contains_key(root)) {
            return Err(PersistenceError::Linkage)
        }
        let kept_derived = derived
            .records
            .iter()
            .filter(|(_, record)| {
                roots.contains(&record.origin_block_root) && states.contains(&record.state_root)
            })
            .map(|(key, record)| (*key, record.clone()))
            .collect::<BTreeMap<_, _>>();
        // Derived references disappear first. A crash leaves extra admission/state pins,
        // never a committed derived record referencing a deleted original admission.
        if kept_derived != derived.records {
            derived.journal.commit(&DerivedSaved {
                schema: 1,
                consensus_digest: self.repository.network.consensus_digest,
                records: kept_derived.values().cloned().collect(),
            })?;
            derived.records = kept_derived;
        }
        let event_ids = self.retained_event_ids(&repository)?;
        self.repository.compact_events(&mut repository, &event_ids)?;
        self.forget_retired_events(&event_ids)?;
        let kept_records = repository
            .records
            .iter()
            .filter(|(root, _)| roots.contains(*root))
            .map(|(root, record)| (*root, record.clone()))
            .collect::<BTreeMap<_, _>>();
        let obsolete = repository
            .records
            .keys()
            .filter(|root| !roots.contains(*root))
            .copied()
            .collect::<BTreeSet<_>>();
        // Persist the exact keep/deletion plan with filtered admission metadata. Reopen can
        // finish an interrupted owner release without inferring which other owners are obsolete.
        let cleanup = retention::RecoveryCleanup { states: states.clone(), obsolete };
        let mut saved = Saved::capture(self.repository.network.consensus_digest, &repository);
        saved.records = kept_records.values().cloned().collect();
        saved.cleanup = Some(cleanup.clone());
        repository.journal.commit(&saved)?;
        repository.records = kept_records;
        repository.cleanup = Some(cleanup);
        self.repository.finish_pending_cleanup(&mut repository)?;
        let mut leases = self.leases.lock().map_err(|_| PersistenceError::CoordinatorPoisoned)?;
        leases.entries.retain(|(root, _), _| states.contains(root));
        leases.bytes = leases.entries.values().map(|entry| entry.bytes).sum();
        self.trim_lease_cache(&mut leases).map_err(|error| match error {
            StateSourceError::ResourceLimit => PersistenceError::Capacity,
            _ => PersistenceError::Linkage,
        })?;
        let mut proofs =
            self.replay_proofs.lock().map_err(|_| PersistenceError::CoordinatorPoisoned)?;
        proofs.entries.retain(|key, _| derived.records.contains_key(key));
        proofs.bytes = proofs.entries.values().map(|entry| entry.bytes).sum();
        self.trim_replay_proofs(&mut proofs)?;
        // Physical collection remains an explicit bounded maintenance action by the runtime.
        // Active immutable leases remain valid even after their obsolete durable owners leave.
        Ok(())
    }
}
