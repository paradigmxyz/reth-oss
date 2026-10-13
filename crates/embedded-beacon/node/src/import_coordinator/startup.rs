//! Split bounded recovery startup so actual peer IO can service mandatory availability callbacks.

use super::*;
use crate::live_sync::availability::RetrievalAvailability;
use recovery::{RecoveryLimits, ReplayPlan};

/// Recovery startup exposes only sealed anchor status until complete signed replay is acknowledged.
#[derive(Debug)]
pub struct ImportStartup {
    pub(super) coordinator: ImportCoordinator,
    pub(super) status: Option<oneshot::Receiver<Result<AuthenticatedStatus, ImportError>>>,
    pub(super) ready: oneshot::Receiver<Result<(), ImportError>>,
}
impl ImportStartup {
    /// Wait for the actual concretely authenticated anchor graph, before replay requests begin.
    /// Its status contains genuine head slot/schema and never a saved later head hint.
    pub async fn anchor_status(&mut self) -> Result<AuthenticatedStatus, ImportError> {
        self.status.take().ok_or(ImportError::Closed)?.await.map_err(|_| ImportError::Closed)?
    }

    /// Complete only after all mandatory signed replay and canonical reconciliation succeeds.
    pub async fn finish(self) -> Result<ImportCoordinator, ImportError> {
        self.ready.await.map_err(|_| ImportError::Closed)??;
        Ok(self.coordinator)
    }
}

impl ImportCoordinator {
    /// Begin bounded full retained signed replay without an early fresh-checkpoint FCU.
    /// The caller must drive the real retrieval broker concurrently with startup completion.
    #[allow(clippy::too_many_arguments)]
    pub fn begin_recovery<E>(
        config: ChainConfig,
        anchor: ImportAnchor,
        repository: AdmittedStateRepository,
        engine: RuntimeEngineAdapter<E>,
        availability: RetrievalAvailability,
        limits: ImportLimits,
        recovery_limits: RecoveryLimits,
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
        Self::begin_recovery_mode(
            config,
            anchor,
            repository,
            engine,
            availability,
            limits,
            recovery_limits,
            StartupExecutionMode::Strict,
        )
    }

    /// Reauthenticate every retained original through the optional checked optimistic importer.
    /// Saved execution associations remain reconstruction hints, never VALID status authority.
    #[allow(clippy::too_many_arguments)]
    pub fn begin_recovery_optimistic<E>(
        config: ChainConfig,
        anchor: ImportAnchor,
        repository: AdmittedStateRepository,
        engine: RuntimeEngineAdapter<E>,
        availability: RetrievalAvailability,
        limits: ImportLimits,
        recovery_limits: RecoveryLimits,
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
        Self::begin_recovery_mode(
            config,
            anchor,
            repository,
            engine,
            availability,
            limits,
            recovery_limits,
            StartupExecutionMode::Optimistic,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn begin_recovery_mode<E>(
        config: ChainConfig,
        anchor: ImportAnchor,
        repository: AdmittedStateRepository,
        engine: RuntimeEngineAdapter<E>,
        availability: RetrievalAvailability,
        limits: ImportLimits,
        recovery_limits: RecoveryLimits,
        mode: StartupExecutionMode,
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
        recovery_limits.validate()?;
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
                let setup = WorkerSetup {
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
                let prepared = ReplayPlan::prepare(
                    &setup.repository,
                    &setup.config,
                    setup.anchor.verified.checkpoint().root,
                    recovery_limits,
                );
                let initialized = prepared.and_then(|plan| {
                    let now = setup.clock.now();
                    Worker::initialize_mode(setup, now, true, mode).map(|worker| (worker, plan))
                });
                let (mut worker, plan) = match initialized {
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
                match worker.recover(&plan, &availability, &ready) {
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
    /// Begin bounded full retained signed replay from the distinct original slot-zero trust input.
    /// The caller must drive the real retrieval broker concurrently with startup completion.
    #[allow(clippy::too_many_arguments)]
    pub fn begin_genesis_recovery<E>(
        config: ChainConfig,
        anchor: super::genesis::GenesisImportAnchor,
        repository: AdmittedStateRepository,
        engine: RuntimeEngineAdapter<E>,
        availability: RetrievalAvailability,
        limits: ImportLimits,
        recovery_limits: RecoveryLimits,
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
        recovery_limits.validate()?;
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
                let prepared = ReplayPlan::prepare(
                    &setup.repository,
                    &setup.config,
                    setup.anchor.genesis.block_root(),
                    recovery_limits,
                );
                let initialized = prepared.and_then(|plan| {
                    let now = setup.clock.now();
                    Worker::initialize_genesis(setup, now, true).map(|worker| (worker, plan))
                });
                let (mut worker, plan) = match initialized {
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
                match worker.recover(&plan, &availability, &ready) {
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
    pub(super) fn replay_retained(
        &mut self,
        plan: &ReplayPlan,
        availability: &RetrievalAvailability,
        ready: &oneshot::Sender<Result<(), ImportError>>,
    ) -> Result<(), ImportError> {
        if !self.replaying {
            return Err(ImportError::RecoveryRequired)
        }
        if plan.transcript.is_some() {
            return self.replay_ordered_transcript(plan, availability, ready);
        }
        if self.origin_genesis.is_none() {
            self.graph.bootstrap_tick(
                &self.config,
                self.clock.now()?,
                self.limits.max_tick_slots,
            )?;
        }
        let mut total_original_bytes = 0_u64;
        // The actual signed envelope of the retained anchor is independent of its trusted
        // EMPTY post-state. Replay it before descendants; no synthetic FULL receipt exists.
        if let Some(anchor) = &plan.anchor_envelope {
            if ready.is_closed() {
                return Err(ImportError::Closed)
            }
            let (envelope, block, bytes) =
                plan.load_anchor_envelope(&self.repository, &self.config, anchor)?;
            total_original_bytes = bytes;
            let candidate = availability
                .bind_retained_block(&block)
                .map_err(|error| ImportError::ObservationInfrastructure(error.to_string()))?;
            self.import_replaying_until_ready(Input::Envelope(envelope), availability, ready)?;
            drop(candidate);
        }
        for record in &plan.blocks {
            if ready.is_closed() {
                return Err(ImportError::Closed)
            }
            let retained = plan.load(&self.repository, &self.config, record)?;
            total_original_bytes = total_original_bytes
                .checked_add(retained.original_bytes)
                .filter(|bytes| *bytes <= plan.limits.max_total_original_bytes)
                .ok_or(ImportError::Bounds)?;
            if self.origin_genesis.is_some() {
                self.advance_archive_clock(retained.wire.slot())?;
            }
            let _candidate = availability
                .bind_candidate(&retained.wire)
                .map_err(|error| ImportError::ObservationInfrastructure(error.to_string()))?;
            self.import_replaying_until_ready(
                Input::Block(retained.wire.clone()),
                availability,
                ready,
            )?;
            if recovery::state_root(&self.graph, retained.wire.root())? != retained.state_root {
                return Err(ImportError::RecoveryRequired)
            }
            if let Some(envelope) = &retained.envelope {
                self.import_replaying_until_ready(
                    Input::Envelope(envelope.clone()),
                    availability,
                    ready,
                )?;
            }
            if availability.is_halted() {
                return Err(ImportError::ObservationInfrastructure(
                    "availability infrastructure halted during original replay".into(),
                ))
            }
        }
        // Saved metadata remains an ordinary reconstruction hint. No override can force its
        // head/finality/state into the admitted graph if complete signed replay disagrees.
        plan.check_with_boundary(
            &self.graph,
            self.recovery_execution_boundary
                .as_ref()
                .map(|boundary| boundary.candidate())
                .or(self.execution_boundary.as_ref()),
        )?;
        Ok(())
    }

    fn recover(
        &mut self,
        plan: &ReplayPlan,
        availability: &RetrievalAvailability,
        ready: &oneshot::Sender<Result<(), ImportError>>,
    ) -> Result<(), ImportError> {
        self.replay_retained(plan, availability, ready)?;
        if self
            .repository
            .has_prepared_publication()
            .map_err(|error| ImportError::Persistence(error.to_string()))?
        {
            // Reconcile the exact reconstructed uncertain candidate before any live clock change
            // can select another branch. The durable prepared pointer grants no authority itself.
            self.replaying = false;
            let mut graph = self.graph.clone();
            self.publish(&mut graph)?;
            self.graph = graph;
            self.replaying = true;
        }
        if self.origin_genesis.is_some() {
            let begin = self.graph.time_ms();
            self.graph.bootstrap_tick(
                &self.config,
                self.clock.now()?,
                self.limits.max_tick_slots,
            )?;
            self.record_clock_event(begin, &self.graph, true)?;
        }
        self.replaying = false;
        let mut graph = self.graph.clone();
        self.publish(&mut graph)?;
        self.graph = graph;
        let actual = self.graph.snapshot()?;
        tracing::info!(target: "beacon::import", replayed_blocks = plan.blocks.len(),
            head_root = %B256::from(actual.head.root),
            head_payload_status = ?actual.head.payload_status, head_slot = actual.head_slot,
            "[beacon] Reconciled complete retained signed history");
        Ok(())
    }
}
