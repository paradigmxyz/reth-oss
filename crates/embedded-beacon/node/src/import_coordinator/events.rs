//! Ordered replay of exact original native inputs and owner clock observations.
use super::*;
use crate::compact_persistence::events::{DurableEvent, EventAction};
use reth_embedded_beacon_network::gossip::{parse_topic, GossipKind};
use reth_embedded_beacon_shared_data::{Encoding, ObjectId, ObjectKind};
use reth_embedded_beacon_state_transition::ssz::auxiliary;

impl<E, C, G> Worker<E, C, G>
where
    E: ExecutionEngine<
            Payload = ExecutionData,
            PayloadReply = PayloadReply,
            Forkchoice = ForkchoiceState,
            ForkchoiceReply = ForkchoiceReply,
        > + Clone,
    E::Error: std::error::Error + Send + Sync + 'static,
    C: ClassicAvailability,
    G: GloasAvailability,
{
    pub(super) fn record_import_event(
        &self,
        input: &Input,
        received_ms: u64,
        begin_ms: u64,
        candidate: &Graph,
    ) -> Result<(), ImportError> {
        let action = match input {
            Input::Block(wire) | Input::GossipBlock(wire) => EventAction::Block(wire.root()),
            Input::Envelope(wire) | Input::GossipEnvelope(wire) => {
                EventAction::Envelope(wire.beacon_block_root())
            }
        };
        self.record_event(action, received_ms, begin_ms, candidate, None)
    }
    pub(super) fn record_gossip_event(
        &self,
        topic: GossipTopic,
        bytes: &[u8],
        received_ms: u64,
        candidate: &Graph,
    ) -> Result<(), ImportError> {
        let root = observation_root(topic, bytes, preset(&self.config))?;
        self.record_event(
            EventAction::Gossip {
                topic: topic.name(),
                root,
                original: ObjectId::new(ObjectKind::BeaconObservation, Encoding::Ssz, bytes),
            },
            received_ms,
            self.graph.time_ms(),
            candidate,
            Some(bytes),
        )
    }
    pub(super) fn record_event(
        &self,
        action: EventAction,
        received_ms: u64,
        begin_ms: u64,
        candidate: &Graph,
        original: Option<&[u8]>,
    ) -> Result<(), ImportError> {
        let event = if let Some(event) = &self.replay_event {
            if event.action != action ||
                event.received_ms != received_ms ||
                event.begin_ms != begin_ms ||
                (event.completed_ms != candidate.time_ms() &&
                    !(matches!(event.action, EventAction::Slashing { .. }) &&
                        event.completed_ms <= candidate.time_ms()))
            {
                return Err(ImportError::RecoveryRequired)
            }
            event.clone()
        } else {
            DurableEvent {
                sequence: 0,
                begin_ms,
                completed_ms: candidate.time_ms(),
                received_ms,
                historical: self.replaying && self.origin_genesis.is_some(),
                action,
            }
        };
        self.source
            .record_event(candidate.snapshot()?, event, original, self.replay_event.is_some())
            .map_err(persistence)
    }
    pub(super) fn record_clock_event(
        &self,
        begin_ms: u64,
        candidate: &Graph,
        historical: bool,
    ) -> Result<(), ImportError> {
        let mut event = self.replay_event.clone().unwrap_or_else(|| DurableEvent {
            sequence: 0,
            begin_ms,
            completed_ms: candidate.time_ms(),
            received_ms: 0,
            historical,
            action: EventAction::Tick,
        });
        if event.action != EventAction::Tick ||
            event.begin_ms != begin_ms ||
            event.completed_ms != candidate.time_ms()
        {
            return Err(ImportError::RecoveryRequired)
        }
        event.historical = historical;
        self.source
            .record_event(candidate.snapshot()?, event, None, self.replay_event.is_some())
            .map_err(persistence)
    }
    pub(super) fn begin_original_event(
        &mut self,
        sequence: u64,
    ) -> Result<DurableEvent, ImportError> {
        if !self.replaying || self.replay_event.is_some() || self.replay_state_lease.is_some() {
            return Err(ImportError::RecoveryRequired)
        }
        let authentication_started = Instant::now();
        let (event, lease) =
            self.repository.original_event_with_lease(sequence).map_err(persistence)?;
        let authentication_ms = authentication_started.elapsed().as_millis();
        let clock_started = Instant::now();
        if self.graph.time_ms() > event.begin_ms {
            if matches!(event.action, EventAction::Slashing { .. }) {
                // A complete signed equivocation proof has no arrival-slot rule. Reauthenticate
                // its exact old signing domains against the actual cut registry without rewinding.
                self.replay_started = true;
                self.replay_event = Some(event.clone());
                self.replay_state_lease = lease;
                return Ok(event)
            }
            return Err(ImportError::RecoveryRequired)
        }
        if !self.replay_started {
            // The constructor has no observed clock history. Restore only the original owner's
            // conservative start clock before the first native event, never optional vote flags.
            self.graph.bootstrap_tick(&self.config, event.begin_ms, self.limits.max_tick_slots)?;
            self.replay_started = true;
        } else if event.historical {
            self.graph.bootstrap_tick(&self.config, event.begin_ms, self.limits.max_tick_slots)?;
        } else {
            self.graph.advance(&self.config, event.begin_ms, self.limits.max_tick_slots)?;
        }
        self.replay_event = Some(event.clone());
        self.replay_state_lease = lease;
        tracing::debug!(target: "beacon::replay", sequence, kind = replay_event_kind(&event.action),
            original_authentication_ms = authentication_ms,
            clock_reconstruction_ms = clock_started.elapsed().as_millis(),
            "[beacon] Authenticated original event and reconstructed its exact begin clock");
        Ok(event)
    }
    /// Focused owners may use this for exact replay without a retrieval broker. Production
    /// recovery retains the broker candidate guard throughout the same strict admission.
    #[cfg(test)]
    pub(super) fn replay_original_event(&mut self, sequence: u64) -> Result<(), ImportError> {
        let event = self.begin_original_event(sequence)?;
        let input = self.load_original_event_input(&event)?;
        let result = match input {
            Some(input) => self.import(input, event.received_ms).map(|_| ()),
            None => self.apply_original_observation(&event),
        };
        self.replay_event = None;
        self.replay_state_lease = None;
        if result.is_ok() {
            self.import_metrics.replayed_events_total =
                self.import_metrics.replayed_events_total.saturating_add(1);
        }
        result
    }
    pub(super) fn load_original_event_input(
        &self,
        event: &DurableEvent,
    ) -> Result<Option<Input>, ImportError> {
        match event.action {
            EventAction::Block(root) => {
                let (format, bytes) = self
                    .repository
                    .original_block(root)
                    .map_err(persistence)?
                    .ok_or(ImportError::RecoveryRequired)?;
                if format != CheckpointBlockFormat::Signed {
                    return Err(ImportError::RecoveryRequired)
                }
                let wire = WireBlock::from_ssz(bytes.to_vec(), &self.config)
                    .map_err(|_| ImportError::Wire)?;
                if wire.root() != root {
                    return Err(ImportError::RecoveryRequired)
                }
                Ok(Some(Input::Block(wire)))
            }
            EventAction::Envelope(root) => {
                let reference = match &self.graph {
                    Graph::Gloas { store, .. } => {
                        let block = store.block(root).ok_or(ImportError::RecoveryRequired)?;
                        EnvelopeBlockReference::from_block(
                            &BeaconBlock::Gloas(block.clone(), preset(&self.config)),
                            &self.config,
                        )
                        .map_err(|_| ImportError::Wire)?
                    }
                    _ => return Err(ImportError::RecoveryRequired),
                };
                let bytes = self
                    .repository
                    .original_envelope(root)
                    .map_err(persistence)?
                    .ok_or(ImportError::RecoveryRequired)?;
                Ok(Some(Input::Envelope(
                    WireEnvelope::from_reference(bytes, &reference, &self.config)
                        .map_err(|_| ImportError::Wire)?,
                )))
            }
            _ => Ok(None),
        }
    }
    pub(super) fn apply_original_observation(
        &mut self,
        event: &DurableEvent,
    ) -> Result<(), ImportError> {
        match &event.action {
            EventAction::Gossip { topic, .. } => {
                let topic =
                    parse_topic(&self.config, topic).map_err(|_| ImportError::RecoveryRequired)?;
                let bytes = self.repository.original_observation(event).map_err(persistence)?;
                if self.dispatch_gossip(topic, &bytes, event.received_ms)? != GossipVerdict::Accept
                {
                    return Err(ImportError::RecoveryRequired)
                }
            }
            EventAction::Slashing { fork, .. } => self.apply_original_slashing(event, *fork)?,
            EventAction::Tick => {
                let mut candidate = self.graph.clone();
                if event.historical {
                    candidate.bootstrap_tick(
                        &self.config,
                        event.completed_ms,
                        self.limits.max_tick_slots,
                    )?;
                } else {
                    candidate.advance(
                        &self.config,
                        event.completed_ms,
                        self.limits.max_tick_slots,
                    )?;
                }
                self.record_clock_event(event.begin_ms, &candidate, event.historical)?;
                self.graph = candidate;
            }
            _ => return Err(ImportError::RecoveryRequired),
        }
        Ok(())
    }
}
fn persistence(error: crate::compact_persistence::PersistenceError) -> ImportError {
    ImportError::Persistence(error.to_string())
}
fn observation_root(topic: GossipTopic, bytes: &[u8], preset: Preset) -> Result<Root, ImportError> {
    match topic.kind {
        GossipKind::PayloadAttestation => {
            Ok(auxiliary::gloas::PayloadAttestationMessage::from_ssz(bytes, preset)
                .map_err(|_| ImportError::Wire)?
                .data
                .beacon_block_root)
        }
        GossipKind::Aggregate => {
            macro_rules! root {
                ($module:ident) => {
                    Ok(auxiliary::$module::SignedAggregateAndProof::from_ssz(bytes, preset)
                        .map_err(|_| ImportError::Wire)?
                        .message
                        .aggregate
                        .data
                        .beacon_block_root)
                };
            }
            match topic.fork {
                Fork::Phase0 => root!(phase0),
                Fork::Altair => root!(altair),
                Fork::Bellatrix => root!(bellatrix),
                Fork::Capella => root!(capella),
                Fork::Deneb => root!(deneb),
                Fork::Electra => root!(electra),
                Fork::Fulu => root!(fulu),
                Fork::Gloas => root!(gloas),
            }
        }
        _ => Err(ImportError::RecoveryRequired),
    }
}

impl<E>
    Worker<
        E,
        crate::live_sync::availability::RetrievalAvailability,
        crate::live_sync::availability::RetrievalAvailability,
    >
where
    E: ExecutionEngine<
            Payload = ExecutionData,
            PayloadReply = PayloadReply,
            Forkchoice = ForkchoiceState,
            ForkchoiceReply = ForkchoiceReply,
        > + Clone,
    E::Error: std::error::Error + Send + Sync + 'static,
{
    pub(super) fn replay_ordered_transcript(
        &mut self,
        plan: &recovery::ReplayPlan,
        availability: &crate::live_sync::availability::RetrievalAvailability,
        ready: &oneshot::Sender<Result<(), ImportError>>,
    ) -> Result<(), ImportError> {
        let transcript = plan.transcript.as_ref().ok_or(ImportError::RecoveryRequired)?;
        let mut total_bytes = 0_u64;
        let recovery_started = Instant::now();
        let mut next_progress = recovery_started;
        let mut recovered = ReplayEventCounts::default();
        tracing::info!(target: "beacon::replay", total_events = transcript.sequences.len(),
            retained_blocks = plan.blocks.len(),
            "[beacon] Rechecking exact retained history, including clock and vote observations");
        // The actual trusted cut envelope is reverified at the constructor boundary clock.
        // Old finalized-prefix votes before that boundary cannot be backdated into this graph.
        if let Some(anchor) = &plan.anchor_envelope {
            let (envelope, block, bytes) =
                plan.load_anchor_envelope(&self.repository, &self.config, anchor)?;
            total_bytes = bytes;
            let _candidate = availability
                .bind_retained_block(&block)
                .map_err(|error| ImportError::ObservationInfrastructure(error.to_string()))?;
            self.import_replaying_until_ready(Input::Envelope(envelope), availability, ready)?;
        }
        if transcript.sequences.is_empty() {
            self.graph.bootstrap_tick(
                &self.config,
                transcript.time_ms,
                self.limits.max_tick_slots,
            )?;
        }
        for (index, sequence) in transcript.sequences.iter().enumerate() {
            if ready.is_closed() {
                return Err(ImportError::Closed)
            }
            let event_started = Instant::now();
            if event_started >= next_progress {
                tracing::info!(target: "beacon::replay", sequence,
                    completed_events = index, total_events = transcript.sequences.len(),
                    remaining_events = transcript.sequences.len().saturating_sub(index),
                    elapsed_secs = recovery_started.elapsed().as_secs(),
                    "[beacon] Verifying next retained history event and its original clock");
                next_progress = event_started + Duration::from_secs(30);
            }
            let event = self.begin_original_event(*sequence)?;
            let authentication_and_clock_ms = event_started.elapsed().as_millis();
            let load_started = Instant::now();
            let input = self.load_original_event_input(&event)?;
            let original_load_ms = load_started.elapsed().as_millis();
            let admission_started = Instant::now();
            let result = if let Some(input) = input {
                total_bytes = total_bytes
                    .checked_add(input.len() as u64)
                    .filter(|bytes| *bytes <= plan.limits.max_total_original_bytes)
                    .ok_or(ImportError::Bounds)?;
                if input.len() > plan.limits.max_original_bytes {
                    return Err(ImportError::Bounds)
                }
                let _candidate = match &input {
                    Input::Block(wire) => availability.bind_candidate(wire),
                    Input::Envelope(wire) => {
                        let Graph::Gloas { store, .. } = &self.graph else {
                            return Err(ImportError::RecoveryRequired)
                        };
                        let block = store
                            .block(wire.beacon_block_root())
                            .ok_or(ImportError::RecoveryRequired)?;
                        availability.bind_retained_block(&BeaconBlock::Gloas(
                            block.clone(),
                            preset(&self.config),
                        ))
                    }
                    _ => return Err(ImportError::RecoveryRequired),
                }
                .map_err(|error| ImportError::ObservationInfrastructure(error.to_string()))?;
                self.import_replaying_until_ready(input, availability, ready).map(|_| ())
            } else {
                if matches!(event.action, EventAction::Gossip { .. } | EventAction::Slashing { .. })
                {
                    let original =
                        self.repository.original_observation(&event).map_err(persistence)?;
                    if original.len() > plan.limits.max_original_bytes {
                        return Err(ImportError::Bounds)
                    }
                    total_bytes = total_bytes
                        .checked_add(original.len() as u64)
                        .filter(|bytes| *bytes <= plan.limits.max_total_original_bytes)
                        .ok_or(ImportError::Bounds)?;
                }
                self.apply_original_observation(&event)
            };
            self.replay_event = None;
            self.replay_state_lease = None;
            result?;
            recovered.record(&event.action);
            self.import_metrics.replayed_events_total =
                self.import_metrics.replayed_events_total.saturating_add(1);
            let admission_ms = admission_started.elapsed().as_millis();
            if Instant::now() >= next_progress ||
                matches!(event.action, EventAction::Block(_) | EventAction::Envelope(_))
            {
                tracing::info!(target: "beacon::replay", sequence,
                    kind = replay_event_kind(&event.action),
                    completed_events = index + 1, total_events = transcript.sequences.len(),
                    remaining_events = transcript.sequences.len().saturating_sub(index + 1),
                    elapsed_ms = event_started.elapsed().as_millis(),
                    authentication_and_clock_ms, original_load_ms, admission_ms,
                    recovered_blocks = recovered.blocks, recovered_envelopes = recovered.envelopes,
                    recovered_votes = recovered.votes, recovered_slashings = recovered.slashings,
                    recovered_clock_events = recovered.clocks,
                    "[beacon] Recovered exact original history event");
                next_progress = Instant::now() + Duration::from_secs(30);
            } else {
                tracing::debug!(target: "beacon::replay", sequence,
                    kind = replay_event_kind(&event.action),
                    elapsed_ms = event_started.elapsed().as_millis(),
                    authentication_and_clock_ms, original_load_ms, admission_ms,
                    "[beacon] Recovered exact original history observation");
            }
            if availability.is_halted() {
                return Err(ImportError::ObservationInfrastructure(
                    "availability halted during original observation replay".into(),
                ))
            }
        }
        if self.graph.time_ms() != transcript.time_ms {
            return Err(ImportError::RecoveryRequired)
        }
        plan.check_with_boundary(
            &self.graph,
            self.recovery_execution_boundary
                .as_ref()
                .map(|boundary| boundary.candidate())
                .or(self.execution_boundary.as_ref()),
        )?;
        tracing::info!(target: "beacon::replay", total_events = transcript.sequences.len(),
            elapsed_secs = recovery_started.elapsed().as_secs(),
            recovered_blocks = recovered.blocks, recovered_envelopes = recovered.envelopes,
            recovered_votes = recovered.votes, recovered_slashings = recovered.slashings,
            recovered_clock_events = recovered.clocks,
            "[beacon] Completed exact retained history replay and checked its reconstruction");
        Ok(())
    }
}

fn replay_event_kind(action: &EventAction) -> &'static str {
    match action {
        EventAction::Block(_) => "block",
        EventAction::Envelope(_) => "envelope",
        EventAction::Gossip { .. } => "vote",
        EventAction::Slashing { .. } => "slashing",
        EventAction::Tick => "clock",
    }
}

#[derive(Default)]
struct ReplayEventCounts {
    blocks: usize,
    envelopes: usize,
    votes: usize,
    slashings: usize,
    clocks: usize,
}

impl ReplayEventCounts {
    fn record(&mut self, action: &EventAction) {
        match action {
            EventAction::Block(_) => self.blocks += 1,
            EventAction::Envelope(_) => self.envelopes += 1,
            EventAction::Gossip { .. } => self.votes += 1,
            EventAction::Slashing { .. } => self.slashings += 1,
            EventAction::Tick => self.clocks += 1,
        }
    }
}
