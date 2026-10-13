//! Bounded checksum-verified archive replay through the same mandatory signed import owner.
//! Archive checksums are data integrity only; every candidate still performs actual BLS, native
//! transition, configured Engine and required data-availability validation. No whole archive SSZ
//! corpus is retained in memory. Original Gloas envelopes are retrieved by the concurrently
//! driven bounded peer broker and still require mandatory signed Engine/DA import.

use super::*;
use crate::live_sync::availability::RetrievalAvailability;
use reth_embedded_beacon_sync::HistoricalArchive;

const ARCHIVE_PREFIX_BLOCKS: usize = 128;

impl ImportCoordinator {
    /// Resume bounded signed archive import from the distinct original slot-zero trust input.
    /// Existing later history is reconstructed completely before any archive prefix publication.
    /// The caller must drive the real retrieval broker concurrently with startup completion.
    #[allow(clippy::too_many_arguments)]
    pub fn begin_genesis_archive<E>(
        config: ChainConfig,
        anchor: super::genesis::GenesisImportAnchor,
        repository: AdmittedStateRepository,
        engine: RuntimeEngineAdapter<E>,
        availability: RetrievalAvailability,
        limits: ImportLimits,
        archive_path: std::path::PathBuf,
    ) -> Result<ImportStartup, ImportError>
    where
        E: ExecutionEngine<
                Payload = ExecutionData,
                PayloadReply = PayloadReply,
                Forkchoice = ForkchoiceState,
                ForkchoiceReply = ForkchoiceReply,
            > + Clone
            + 'static,
        E::Error: std::error::Error + Send + Sync + 'static,
    {
        limits.validate()?;
        config.validate().map_err(|_| ImportError::Configuration)?;
        let config_digest = checked_config_digest(&config)?;
        if availability.config_digest() != config_digest {
            return Err(ImportError::Configuration)
        }
        let runtime = Handle::try_current().map_err(|_| ImportError::Runtime)?;
        let (sender, receiver) = mpsc::channel(limits.command_capacity);
        let bytes = Arc::new(Semaphore::new(limits.max_queued_bytes));
        let commands = Arc::new(Semaphore::new(limits.command_capacity));
        let halted = Arc::new(AtomicBool::new(false));
        let (status, status_result) = oneshot::channel();
        let (ready, result) = oneshot::channel();
        let shutdown =
            Shutdown { bytes: bytes.clone(), commands: commands.clone(), halted: halted.clone() };
        thread::Builder::new()
            .name("embedded-beacon-replay".into())
            .spawn(move || {
                let _shutdown = shutdown;
                let setup = super::genesis::GenesisSetup {
                    config,
                    anchor,
                    repository,
                    engine,
                    classic_availability: availability.clone(),
                    gloas_availability: availability.clone(),
                    limits,
                    runtime,
                    clock: OwnerClock::System,
                };
                let prepared =
                    HistoricalArchive::open(&archive_path, setup.config.genesis_validators_root)
                        .map_err(|error| ImportError::ObservationInfrastructure(error.to_string()));
                let initialized = prepared.and_then(|archive| {
                    let recovery = setup
                        .repository
                        .replay_snapshot()
                        .map_err(|error| ImportError::Persistence(error.to_string()))?
                        .map(|_| {
                            recovery::ReplayPlan::prepare(
                                &setup.repository,
                                &setup.config,
                                setup.anchor.genesis.block_root(),
                                recovery::RecoveryLimits::default(),
                            )
                        })
                        .transpose()?;
                    let now = setup.clock.now();
                    Worker::initialize_genesis(setup, now, true)
                        .map(|worker| (worker, archive, recovery))
                });
                let (mut worker, archive, recovery) = match initialized {
                    Ok(value) => value,
                    Err(error) => {
                        let _ = status.send(Err(error));
                        let _ = ready.send(Err(ImportError::Closed));
                        return
                    }
                };
                let actual_status = worker.authenticated_status();
                match actual_status {
                    Ok(actual) => {
                        if status.send(Ok(actual)).is_err() {
                            return
                        }
                    }
                    Err(error) => {
                        let _ = status.send(Err(error));
                        let _ = ready.send(Err(ImportError::Closed));
                        return
                    }
                }
                match worker.replay_archive(&archive, recovery.as_ref(), &availability, &ready) {
                    Ok(()) => {
                        if ready.send(Ok(())).is_ok() {
                            worker.run(receiver);
                        }
                    }
                    Err(error) => {
                        let _ = ready.send(Err(error));
                    }
                }
            })
            .map_err(|error| ImportError::Thread(error.to_string()))?;
        let coordinator = Self {
            sender,
            bytes,
            commands,
            halted,
            limits,
            config_digest,
            queue_high_water: Default::default(),
        };
        Ok(ImportStartup { coordinator, status: Some(status_result), ready: result })
    }
}

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
    /// Retain one exact original while execution or required data spans bounded retry windows.
    /// The asynchronous peer/availability owner continues independently; cancellation is checked
    /// before each attempt and after every short delay. No prefix is published by this helper.
    pub(super) fn import_replaying_until_ready(
        &mut self,
        input: Input,
        availability: &RetrievalAvailability,
        ready: &oneshot::Sender<Result<(), ImportError>>,
    ) -> Result<ImportReceipt, ImportError> {
        if !self.replaying {
            return Err(ImportError::RecoveryRequired)
        }
        let root = input.root();
        // A prior block or envelope attempt cannot classify this input's later proof errors.
        availability.take_deferred_data();
        let mut window = 0_u64;
        let mut next_missing_log = Instant::now();
        loop {
            let started = std::time::Instant::now();
            for attempt in 0..16 {
                if ready.is_closed() {
                    return Err(ImportError::Closed)
                }
                let result = self.import(
                    input.clone(),
                    self.replay.event().map_or(0, |event| event.received_ms),
                );
                let missing_data = availability.take_deferred_data();
                match result {
                    Err(ImportError::ExecutionPending(pending)) if pending == root => {
                        if self.engine.is_closed() {
                            return Err(ImportError::EngineUncertain)
                        }
                        tracing::debug!(target: "beacon::replay", root = %B256::from(root), window, attempt,
                            "[beacon] Actual execution input remains pending without native graph publication");
                    }
                    Err(
                        error @ (ImportError::Consensus(StoreError::DataAvailability(_)) |
                        ImportError::Envelope(_)),
                    ) if missing_data && !availability.is_halted() && !self.engine.is_closed() => {
                        if Instant::now() >= next_missing_log {
                            tracing::info!(target: "beacon::replay", root = %B256::from(root),
                                window, attempt, %error,
                                "[beacon] Waiting for required original data before signed-history recovery");
                            next_missing_log = Instant::now() + Duration::from_secs(30);
                        } else {
                            tracing::debug!(target: "beacon::replay", root = %B256::from(root),
                                window, attempt, %error,
                                "[beacon] Retrying required original data for signed-history recovery");
                        }
                    }
                    other => return other,
                }
                self.runtime.block_on(async { tokio::time::sleep(Duration::from_secs(1)).await });
                if ready.is_closed() {
                    return Err(ImportError::Closed)
                }
                if started.elapsed() >= Duration::from_secs(60) {
                    break
                }
            }
            // Exhaustion is a pending continuation owned by this worker, never invalid peer
            // evidence. Continue another bounded window without a manual restart or new input.
            window = window.saturating_add(1);
            tracing::info!(target: "beacon::replay", root = %B256::from(root), window,
                "[beacon] Continuing authenticated replay after bounded pending window");
        }
    }
    pub(super) fn advance_archive_clock(&mut self, slot: u64) -> Result<(), ImportError> {
        if !self.replaying || self.origin_genesis.is_none() {
            return Err(ImportError::RecoveryRequired)
        }
        let next = slot.checked_add(1).ok_or(ImportError::Clock)?;
        let late = self
            .config
            .clock()
            .map_err(|_| ImportError::Configuration)?
            .start_of_slot(next)
            .map_err(|_| ImportError::Clock)?
            .checked_sub(1)
            .ok_or(ImportError::Clock)?;
        // Historical block admission occurs after the slot's boost/confirmation deadlines.
        // No timely network arrival, FCR observation, vote, or state is invented.
        self.graph.bootstrap_tick(&self.config, late, self.limits.max_tick_slots)
    }
    /// Only the archive owner may publish genuine historical prefixes before live handoff.
    /// Ordinary retained replay continues suppressing all FCUs until its complete hint check.
    fn publish_archive_prefix(&mut self) -> Result<(), ImportError> {
        if !self.replaying || self.origin_genesis.is_none() {
            return Err(ImportError::RecoveryRequired)
        }
        let mut graph = self.graph.clone();
        self.replaying = false;
        let publication = self.publish(&mut graph);
        self.replaying = true;
        let dispatched = publication?;
        self.graph = graph;
        let removed = self
            .repository
            .collect_retired_objects()
            .map_err(|error| ImportError::Persistence(error.to_string()))?;
        let actual = self.graph.snapshot()?;
        tracing::info!(target: "beacon::archive", head_root = %B256::from(actual.head.root),
            head_payload_status = ?actual.head.payload_status,
            head_slot = actual.head_slot, finalized_epoch = actual.finalized_checkpoint.epoch,
            execution_forkchoice_updated = dispatched, removed_objects = removed,
            "[beacon] Committed genuine historical archive prefix");
        Ok(())
    }

    /// A decoded adjacent child may guide retrieval only; it grants no consensus authority.
    /// The child's actual signed transition must still prove its EMPTY parent selection later.
    fn archive_envelope_required(
        &self,
        archive: &HistoricalArchive,
        wire: &WireBlock,
        next: Option<&reth_embedded_beacon_sync::ArchiveEntry>,
    ) -> Result<bool, ImportError> {
        let Some(next) = next else { return Ok(true) };
        let following = WireBlock::from_ssz(
            archive
                .read_block(next)
                .map_err(|error| ImportError::ObservationInfrastructure(error.to_string()))?,
            &self.config,
        )
        .map_err(|_| ImportError::Wire)?;
        if following.root() != next.root || following.slot() != next.slot {
            return Err(ImportError::Wire)
        }
        if following.fork() != Fork::Gloas || following.parent_root() != wire.root() {
            return Ok(true)
        }
        let SignedBeaconBlock::Gloas(parent, _) = decode_block(wire, &self.config)? else {
            return Err(ImportError::Wire)
        };
        let SignedBeaconBlock::Gloas(child, _) = decode_block(&following, &self.config)? else {
            return Err(ImportError::Wire)
        };
        let parent_bid = &parent.message.body.signed_execution_payload_bid.message;
        let child_bid = &child.message.body.signed_execution_payload_bid.message;
        let carries_parent_execution_hash =
            child_bid.parent_block_hash == parent_bid.parent_block_hash;
        let skips_parent_payload_hash = child_bid.parent_block_hash != parent_bid.block_hash;
        Ok(!(carries_parent_execution_hash && skips_parent_payload_hash))
    }

    /// A segment starts at the actual recovered canonical head or its exact signed child.
    /// Old manifest claims are skipped only after complete native recovery authenticated the head.
    fn archive_start(&self, archive: &HistoricalArchive) -> Result<usize, ImportError> {
        let actual = self.graph.snapshot()?;
        let entries = archive.entries();
        let next = entries.partition_point(|entry| entry.slot <= actual.head_slot);
        let exact = next
            .checked_sub(1)
            .and_then(|index| entries.get(index))
            .filter(|entry| entry.slot == actual.head_slot);
        if exact.is_some_and(|entry| entry.root != actual.head.root) {
            return Err(ImportError::RecoveryRequired)
        }
        if let Some(first) = entries.get(next) {
            let wire = WireBlock::from_ssz(
                archive
                    .read_block(first)
                    .map_err(|error| ImportError::ObservationInfrastructure(error.to_string()))?,
                &self.config,
            )
            .map_err(|_| ImportError::Wire)?;
            if wire.root() != first.root ||
                wire.slot() != first.slot ||
                wire.parent_root() != actual.head.root
            {
                return Err(ImportError::RecoveryRequired)
            }
        } else if exact.is_none() &&
            !(entries.is_empty() && self.origin_genesis == Some(actual.head.root))
        {
            return Err(ImportError::RecoveryRequired)
        }
        Ok(next)
    }
}
impl<E> Worker<E, RetrievalAvailability, RetrievalAvailability>
where
    E: ExecutionEngine<
            Payload = ExecutionData,
            PayloadReply = PayloadReply,
            Forkchoice = ForkchoiceState,
            ForkchoiceReply = ForkchoiceReply,
        > + Clone,
    E::Error: std::error::Error + Send + Sync + 'static,
{
    fn replay_archive(
        &mut self,
        archive: &HistoricalArchive,
        recovery: Option<&recovery::ReplayPlan>,
        availability: &RetrievalAvailability,
        ready: &oneshot::Sender<Result<(), ImportError>>,
    ) -> Result<(), ImportError> {
        if !self.replaying || self.origin_genesis.is_none() {
            return Err(ImportError::RecoveryRequired)
        }
        if ready.is_closed() {
            return Err(ImportError::Closed)
        }
        if let Some(plan) = recovery {
            // Recover every retained branch and original envelope to the saved actual later head.
            // No FCU can acknowledge the earlier constructor seed during this reconstruction.
            self.replay_retained(plan, availability, ready)?;
        }
        let start = self.archive_start(archive)?;
        self.publish_archive_prefix()?;
        let mut since_publication = 0_usize;
        for (offset, entry) in archive.entries()[start..].iter().enumerate() {
            if ready.is_closed() {
                return Err(ImportError::Closed)
            }
            let bytes = archive
                .read_block(entry)
                .map_err(|error| ImportError::ObservationInfrastructure(error.to_string()))?;
            let wire = WireBlock::from_ssz(bytes, &self.config).map_err(|_| ImportError::Wire)?;
            if wire.root() != entry.root || wire.slot() != entry.slot {
                return Err(ImportError::Wire)
            }
            self.advance_archive_clock(wire.slot())?;
            let _candidate = availability
                .bind_candidate(&wire)
                .map_err(|error| ImportError::ObservationInfrastructure(error.to_string()))?;
            self.import_replaying_until_ready(Input::Block(wire.clone()), availability, ready)?;
            if wire.fork() == Fork::Gloas &&
                self.archive_envelope_required(
                    archive,
                    &wire,
                    archive.entries().get(start + offset + 1),
                )?
            {
                // Version-one archives contain signed blocks only. FULL-parent continuation and
                // the undecided segment tail require the exact original
                // signed envelope while the peer owner continues serving required DA callbacks.
                let batch = self
                    .runtime
                    .block_on(availability.request_envelope(&wire))
                    .map_err(|error| ImportError::ObservationInfrastructure(error.to_string()))?;
                let envelope = batch.envelopes().first().ok_or(ImportError::Wire)?;
                self.import_replaying_until_ready(
                    Input::Envelope(envelope.clone()),
                    availability,
                    ready,
                )?;
                // Keep the transport reservation alive throughout SSZ/BLS/Engine/DA work.
                drop(batch);
            }
            if availability.is_halted() {
                return Err(ImportError::ObservationInfrastructure(
                    "actual archive availability owner halted".into(),
                ))
            }
            since_publication = since_publication.checked_add(1).ok_or(ImportError::Bounds)?;
            if since_publication == ARCHIVE_PREFIX_BLOCKS {
                self.publish_archive_prefix()?;
                since_publication = 0;
            }
        }
        // Commit the last native prefix even when the segment ends before the live-clock bound.
        // An ancient segment therefore remains safely resumable with the next adjacent segment.
        if since_publication != 0 {
            self.publish_archive_prefix()?;
        }
        let begin = self.graph.time_ms();
        self.graph.bootstrap_tick(&self.config, self.clock.now()?, self.limits.max_tick_slots)?;
        self.record_clock_event(begin, &self.graph, true)?;
        self.publish_archive_prefix()?;
        self.replaying = false;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
