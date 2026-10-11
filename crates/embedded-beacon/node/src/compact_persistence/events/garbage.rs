//! Crash-resumable release of transcript-only owners after the exact keep descriptor commits.
use super::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::compact_persistence) struct EventCleanup {
    pub(super) pages: Vec<EventPage>,
    pub(super) tail: Vec<DurableEvent>,
}
impl AdmittedStateRepository {
    pub(in crate::compact_persistence) fn finish_event_cleanup(
        &self,
        inner: &mut Inner,
    ) -> Result<(), PersistenceError> {
        let Some(plan) = inner.event_cleanup.clone() else { return Ok(()) };
        if plan.tail.len() >= PAGE_EVENTS ||
            plan.pages.iter().map(|page| page.count).sum::<usize>() + plan.tail.len() >
                MAX_EVENTS
        {
            return Err(PersistenceError::Capacity)
        }
        let mut retained = BTreeSet::new();
        self.visit_events(inner, |event| {
            retained.insert(event.sequence);
            Ok(())
        })?;
        let kept_pages = inner.events.pages.iter().map(|page| page.object).collect::<BTreeSet<_>>();
        let release = |event: &DurableEvent| -> Result<(), PersistenceError> {
            if !retained.contains(&event.sequence) &&
                let EventAction::Gossip { original, .. } | EventAction::Slashing { original, .. } =
                    event.action
            {
                if original.kind != ObjectKind::BeaconObservation ||
                    original.encoding != Encoding::Ssz
                {
                    return Err(PersistenceError::Linkage)
                }
                // This deterministic transcript-only namespace never selects Pool,
                // CompactState, Checkpoint, or the independent sidecar
                // context's Recovery owner.
                self.shared
                    .release(event.observation_owner(self.network.consensus_digest), original)?;
            }
            Ok(())
        };
        for page in &plan.pages {
            // A previous attempt may already have released this page pin. Until this durable
            // cleanup descriptor clears, physical collection cannot remove its immutable bytes.
            for event in self.read_event_page(page, false)? {
                release(&event)?;
            }
            if !kept_pages.contains(&page.object) {
                self.shared.release(self.event_page_owner(), page.object)?;
            }
        }
        for event in &plan.tail {
            release(event)?;
        }
        let mut saved = Saved::capture(self.network.consensus_digest, inner);
        saved.event_cleanup = None;
        inner.journal.commit(&saved)?;
        inner.event_cleanup = None;
        Ok(())
    }
}
