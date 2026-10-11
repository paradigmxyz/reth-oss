//! Ordered original-data transcript. Immutable bounded pages keep metadata streaming.
//! Every row is a reconstruction hint; only replay through native kernels grants authority.
use super::*;

pub(crate) const MAX_EVENTS: usize = 262_144;
const PAGE_EVENTS: usize = 128;
const PAGE_BYTES: usize = 512 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DurableEvent {
    pub(crate) sequence: u64,
    pub(crate) begin_ms: u64,
    pub(crate) completed_ms: u64,
    pub(crate) received_ms: u64,
    pub(crate) historical: bool,
    pub(crate) action: EventAction,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum EventAction {
    Block(Root),
    Envelope(Root),
    Gossip { topic: String, root: Root, original: ObjectId },
    Slashing { fork: Fork, original: ObjectId, verification_slot: u64 },
    Tick,
}
impl DurableEvent {
    pub(crate) const fn referenced_root(&self) -> Option<Root> {
        match &self.action {
            EventAction::Block(root) |
            EventAction::Envelope(root) |
            EventAction::Gossip { root, .. } => Some(*root),
            EventAction::Slashing { .. } | EventAction::Tick => None,
        }
    }
    fn observation_owner(&self, digest: Root) -> Owner {
        let mut bytes = b"beacon-original-observation-v1".to_vec();
        bytes.extend_from_slice(&digest);
        bytes.extend_from_slice(&self.sequence.to_le_bytes());
        Owner::Recovery(sha256(&bytes))
    }
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct EventLedger {
    pub(super) next: u64,
    pages: Vec<EventPage>,
    tail: Vec<DurableEvent>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct EventPage {
    first: u64,
    last: u64,
    count: usize,
    object: ObjectId,
}
#[derive(Debug, Clone)]
#[cfg(all(feature = "peer-runtime", feature = "execution-verification"))]
pub(crate) struct ReplayTranscript {
    pub(crate) sequences: BTreeSet<u64>,
    pub(crate) time_ms: u64,
}
impl AdmittedStateRepository {
    fn event_page_owner(&self) -> Owner {
        let mut bytes = b"beacon-ordered-event-pages-v1".to_vec();
        bytes.extend_from_slice(&self.network.consensus_digest);
        Owner::Recovery(sha256(&bytes))
    }
    fn load_event_page(&self, page: &EventPage) -> Result<Vec<DurableEvent>, PersistenceError> {
        self.read_event_page(page, true)
    }
    fn read_event_page(
        &self,
        page: &EventPage,
        require_pin: bool,
    ) -> Result<Vec<DurableEvent>, PersistenceError> {
        if page.object.kind != ObjectKind::BeaconObservation ||
            page.object.encoding != Encoding::Json ||
            page.count == 0 ||
            page.count > PAGE_EVENTS ||
            page.first > page.last ||
            (require_pin &&
                !self.shared.owner_objects(self.event_page_owner()).contains(&page.object))
        {
            return Err(PersistenceError::Linkage)
        }
        let original =
            self.shared.get_bounded(page.object, PAGE_BYTES)?.ok_or(PersistenceError::Linkage)?;
        let events: Vec<DurableEvent> =
            serde_json::from_slice(&original).map_err(SharedError::from)?;
        if events.len() != page.count ||
            events.first().map(|row| row.sequence) != Some(page.first) ||
            events.last().map(|row| row.sequence) != Some(page.last) ||
            events.windows(2).any(|rows| rows[0].sequence >= rows[1].sequence)
        {
            return Err(PersistenceError::Linkage)
        }
        Ok(events)
    }
    #[cfg(all(feature = "peer-runtime", feature = "execution-verification"))]
    fn save_event_page(&self, events: &[DurableEvent]) -> Result<EventPage, PersistenceError> {
        if events.is_empty() || events.len() > PAGE_EVENTS {
            return Err(PersistenceError::Capacity)
        }
        let bytes = serde_json::to_vec(events).map_err(SharedError::from)?;
        if bytes.len() > PAGE_BYTES {
            return Err(PersistenceError::Capacity)
        }
        let object = self.shared.put(
            ObjectKind::BeaconObservation,
            Encoding::Json,
            &bytes,
            self.event_page_owner(),
        )?;
        Ok(EventPage {
            first: events[0].sequence,
            last: events[events.len() - 1].sequence,
            count: events.len(),
            object,
        })
    }
    pub(super) fn check_event_ledger(&self, inner: &Inner) -> Result<(), PersistenceError> {
        if inner.events.tail.len() >= PAGE_EVENTS ||
            inner.events.pages.iter().map(|page| page.count).sum::<usize>() +
                inner.events.tail.len() >
                MAX_EVENTS
        {
            return Err(PersistenceError::Capacity)
        }
        let mut previous = None;
        for page in &inner.events.pages {
            if previous.is_some_and(|previous| previous >= page.first) {
                return Err(PersistenceError::Linkage)
            }
            for event in self.load_event_page(page)? {
                self.check_event(inner, &event)?;
            }
            previous = Some(page.last);
        }
        for event in &inner.events.tail {
            if previous.is_some_and(|previous| previous >= event.sequence) {
                return Err(PersistenceError::Linkage)
            }
            self.check_event(inner, event)?;
            previous = Some(event.sequence);
        }
        if previous.is_some_and(|previous| previous >= inner.events.next) {
            return Err(PersistenceError::Linkage)
        }
        Ok(())
    }
    pub(super) fn check_event(
        &self,
        inner: &Inner,
        event: &DurableEvent,
    ) -> Result<(), PersistenceError> {
        self.check_event_with_lease(inner, event).map(|_| ())
    }

    fn check_event_with_lease(
        &self,
        inner: &Inner,
        event: &DurableEvent,
    ) -> Result<Option<StateLease>, PersistenceError> {
        if event.begin_ms > event.completed_ms || event.received_ms > event.begin_ms {
            return Err(PersistenceError::Linkage)
        }
        let lease = match &event.action {
            EventAction::Block(root) => {
                let record = inner.records.get(root).ok_or(PersistenceError::Linkage)?;
                if record.format != CheckpointBlockFormat::Signed {
                    return Err(PersistenceError::Linkage)
                }
                Some(self.check_saved_record_with_lease(record)?)
            }
            EventAction::Envelope(root) => {
                let record = inner.records.get(root).ok_or(PersistenceError::Linkage)?;
                if record.envelope.is_none() {
                    return Err(PersistenceError::Linkage)
                }
                Some(self.check_saved_record_with_lease(record)?)
            }
            EventAction::Gossip { topic, root, original } => {
                if topic.len() > 256 ||
                    !inner.records.contains_key(root) ||
                    original.kind != ObjectKind::BeaconObservation ||
                    original.encoding != Encoding::Ssz ||
                    !self
                        .shared
                        .owner_objects(event.observation_owner(self.network.consensus_digest))
                        .contains(original)
                {
                    return Err(PersistenceError::Linkage)
                }
                self.shared
                    .get_bounded(*original, self.network.config.max_payload_size)?
                    .ok_or(PersistenceError::Linkage)?;
                None
            }
            EventAction::Slashing { fork, original, verification_slot } => {
                if *verification_slot >
                    self.network
                        .config
                        .clock()
                        .map_err(|_| PersistenceError::Identity)?
                        .slot_at(event.completed_ms)
                        .ok_or(PersistenceError::Linkage)?
                {
                    return Err(PersistenceError::Linkage)
                }
                if original.kind != ObjectKind::BeaconObservation ||
                    original.encoding != Encoding::Ssz ||
                    !self
                        .shared
                        .owner_objects(event.observation_owner(self.network.consensus_digest))
                        .contains(original)
                {
                    return Err(PersistenceError::Linkage)
                }
                let bytes = self
                    .shared
                    .get_bounded(*original, self.network.config.max_payload_size)?
                    .ok_or(PersistenceError::Linkage)?;
                reth_embedded_beacon_fork_choice::slashing_replay::original_participants(
                    &bytes,
                    *fork,
                    self.network.preset(),
                )
                .map_err(|_| PersistenceError::Linkage)?;
                None
            }
            EventAction::Tick => None,
        };
        Ok(lease)
    }
    #[cfg(all(feature = "peer-runtime", feature = "execution-verification"))]
    pub(crate) fn replay_transcript(&self) -> Result<Option<ReplayTranscript>, PersistenceError> {
        let inner = self.lock()?;
        Ok(inner.replay_closure().and_then(|closure| {
            closure.events.as_ref().map(|sequences| ReplayTranscript {
                sequences: sequences.clone(),
                time_ms: closure.time_ms.unwrap_or(0),
            })
        }))
    }
    #[cfg(all(feature = "peer-runtime", feature = "execution-verification"))]
    pub(crate) fn original_event(&self, sequence: u64) -> Result<DurableEvent, PersistenceError> {
        let inner = self.lock()?;
        self.event_locked(&inner, sequence)
    }

    /// Authenticate the exact event and retain its already checked state for one active attempt.
    /// The lease preserves allocation lifetime only; it cannot substitute for fresh owner, graph,
    /// signature or publication checks on later reads, and disappears when the attempt ends.
    #[cfg(all(feature = "peer-runtime", feature = "execution-verification"))]
    pub(crate) fn original_event_with_lease(
        &self,
        sequence: u64,
    ) -> Result<(DurableEvent, Option<StateLease>), PersistenceError> {
        let inner = self.lock()?;
        self.event_locked_with_lease(&inner, sequence)
    }
    pub(super) fn event_locked(
        &self,
        inner: &Inner,
        sequence: u64,
    ) -> Result<DurableEvent, PersistenceError> {
        self.event_locked_with_lease(inner, sequence).map(|(event, _)| event)
    }

    fn event_locked_with_lease(
        &self,
        inner: &Inner,
        sequence: u64,
    ) -> Result<(DurableEvent, Option<StateLease>), PersistenceError> {
        let row = if let Some(page) =
            inner.events.pages.iter().find(|page| page.first <= sequence && sequence <= page.last)
        {
            self.load_event_page(page)?.into_iter().find(|row| row.sequence == sequence)
        } else {
            inner.events.tail.iter().find(|row| row.sequence == sequence).cloned()
        }
        .ok_or(PersistenceError::Linkage)?;
        let lease = self.check_event_with_lease(inner, &row)?;
        Ok((row, lease))
    }
    pub(super) fn visit_events(
        &self,
        inner: &Inner,
        mut visit: impl FnMut(&DurableEvent) -> Result<(), PersistenceError>,
    ) -> Result<(), PersistenceError> {
        for page in &inner.events.pages {
            for event in self.load_event_page(page)? {
                visit(&event)?;
            }
        }
        for event in &inner.events.tail {
            visit(event)?;
        }
        Ok(())
    }
    #[cfg(all(feature = "peer-runtime", feature = "execution-verification"))]
    pub(crate) fn original_observation(
        &self,
        event: &DurableEvent,
    ) -> Result<Arc<[u8]>, PersistenceError> {
        let inner = self.lock()?;
        if self.event_locked(&inner, event.sequence)? != *event {
            return Err(PersistenceError::Linkage)
        }
        let original = match &event.action {
            EventAction::Gossip { original, .. } | EventAction::Slashing { original, .. } => {
                original
            }
            _ => return Err(PersistenceError::Linkage),
        };
        self.shared
            .get_bounded(*original, self.network.config.max_payload_size)?
            .ok_or(PersistenceError::Linkage)
    }
    #[cfg(all(feature = "peer-runtime", feature = "execution-verification"))]
    pub(super) fn append_event(
        &self,
        mut event: DurableEvent,
        gossip_bytes: Option<&[u8]>,
    ) -> Result<DurableEvent, PersistenceError> {
        let mut inner = self.lock()?;
        if inner.events.pages.iter().map(|page| page.count).sum::<usize>() + inner.events.tail.len() >=
            MAX_EVENTS
        {
            return Err(PersistenceError::Capacity)
        }
        event.sequence = inner.events.next;
        if let EventAction::Gossip { original, .. } | EventAction::Slashing { original, .. } =
            &event.action
        {
            let bytes = gossip_bytes.ok_or(PersistenceError::Linkage)?;
            if bytes.len() > self.network.config.max_payload_size ||
                *original != ObjectId::new(ObjectKind::BeaconObservation, Encoding::Ssz, bytes)
            {
                return Err(PersistenceError::Linkage)
            }
            self.shared.put(
                ObjectKind::BeaconObservation,
                Encoding::Ssz,
                bytes,
                event.observation_owner(self.network.consensus_digest),
            )?;
        } else if gossip_bytes.is_some() {
            return Err(PersistenceError::Linkage)
        }
        self.check_event(&inner, &event)?;
        let mut ledger = inner.events.clone();
        ledger.next = ledger.next.checked_add(1).ok_or(PersistenceError::Capacity)?;
        ledger.tail.push(event.clone());
        if ledger.tail.len() == PAGE_EVENTS {
            ledger.pages.push(self.save_event_page(&ledger.tail)?);
            ledger.tail.clear();
        }
        let mut saved = Saved::capture(self.network.consensus_digest, &inner);
        saved.events = ledger.clone();
        inner.journal.commit(&saved)?;
        inner.events = ledger;
        Ok(event)
    }
    /// Filter metadata before releasing exact independent event pins. Both canonical generations
    /// and every actual unpublished admitted event are protected by the caller's union.
    #[cfg(all(feature = "peer-runtime", feature = "execution-verification"))]
    pub(super) fn compact_events(
        &self,
        inner: &mut Inner,
        keep: &BTreeSet<u64>,
    ) -> Result<(), PersistenceError> {
        self.finish_event_cleanup(inner)?;
        let old_pages = inner.events.pages.clone();
        let mut ledger = EventLedger { next: inner.events.next, ..Default::default() };
        let old_tail = inner.events.tail.clone();
        let mut retain = |event: DurableEvent| -> Result<(), PersistenceError> {
            if keep.contains(&event.sequence) {
                ledger.tail.push(event);
                if ledger.tail.len() == PAGE_EVENTS {
                    ledger.pages.push(self.save_event_page(&ledger.tail)?);
                    ledger.tail.clear();
                }
            }
            Ok(())
        };
        for page in &old_pages {
            for event in self.load_event_page(page)? {
                retain(event)?;
            }
        }
        for event in &inner.events.tail {
            retain(event.clone())?;
        }
        let mut saved = Saved::capture(self.network.consensus_digest, inner);
        let cleanup = EventCleanup { pages: old_pages, tail: old_tail };
        saved.events = ledger.clone();
        saved.event_cleanup = Some(cleanup.clone());
        inner.journal.commit(&saved)?;
        inner.events = ledger;
        inner.event_cleanup = Some(cleanup);
        self.finish_event_cleanup(inner)
    }
}

mod garbage;
pub(super) use garbage::EventCleanup;
