//! Dedicated genuine-genesis ownership, without any checkpoint or weak-subjectivity conversion.

use super::*;
use crate::VerifiedGenesis;

/// Exact original slot-zero trust input, distinct from a fresh finalized checkpoint.
#[derive(Clone, Debug)]
pub struct GenesisImportAnchor {
    /// Concrete complete native genesis authentication; no caller-created validity flags.
    pub genesis: VerifiedGenesis,
    /// Independent original input bounds rechecked by the import owner.
    pub decode_limits: BootstrapDecodeLimits,
}

pub(super) struct GenesisSetup<E, C, G> {
    pub(super) config: ChainConfig,
    pub(super) anchor: GenesisImportAnchor,
    pub(super) repository: AdmittedStateRepository,
    pub(super) engine: RuntimeEngineAdapter<E>,
    pub(super) classic_availability: C,
    pub(super) gloas_availability: G,
    pub(super) limits: ImportLimits,
    pub(super) runtime: Handle,
    pub(super) clock: OwnerClock,
}

impl ImportCoordinator {
    /// Start a genuine epoch-zero graph and validate every later signed transition normally.
    /// Ancient public genesis currently exceeds the bounded live-clock catch-up limit and is
    /// rejected explicitly; it never receives a fabricated current weak-subjectivity boundary.
    #[allow(clippy::too_many_arguments)]
    pub async fn start_genesis<E, C, G>(
        config: ChainConfig,
        anchor: GenesisImportAnchor,
        repository: AdmittedStateRepository,
        engine: RuntimeEngineAdapter<E>,
        classic_availability: C,
        gloas_availability: G,
        limits: ImportLimits,
    ) -> Result<Self, ImportError>
    where
        E: ExecutionEngine<
                Payload = ExecutionData,
                PayloadReply = PayloadReply,
                Forkchoice = ForkchoiceState,
                ForkchoiceReply = ForkchoiceReply,
            > + Clone
            + 'static,
        E::Error: std::error::Error + Send + Sync + 'static,
        C: ClassicAvailability + 'static,
        G: GloasAvailability + 'static,
    {
        limits.validate()?;
        config.validate().map_err(|_| ImportError::Configuration)?;
        let config_digest = checked_config_digest(&config)?;
        let runtime = Handle::try_current().map_err(|_| ImportError::Runtime)?;
        let (sender, receiver) = mpsc::channel(limits.command_capacity);
        let bytes = Arc::new(Semaphore::new(limits.max_queued_bytes));
        let commands = Arc::new(Semaphore::new(limits.command_capacity));
        let halted = Arc::new(AtomicBool::new(false));
        let (ready, result) = oneshot::channel();
        let shutdown =
            Shutdown { bytes: bytes.clone(), commands: commands.clone(), halted: halted.clone() };
        thread::Builder::new()
            .name("embedded-beacon-genesis-import".into())
            .spawn(move || {
                let _shutdown = shutdown;
                let setup = GenesisSetup {
                    config,
                    anchor,
                    repository,
                    engine,
                    classic_availability,
                    gloas_availability,
                    limits,
                    runtime,
                    clock: OwnerClock::System,
                };
                let now = setup.clock.now();
                match Worker::initialize_genesis(setup, now, false) {
                    Ok(mut worker) => {
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
        result.await.map_err(|_| ImportError::Closed)??;
        Ok(Self {
            sender,
            bytes,
            commands,
            halted,
            limits,
            config_digest,
            queue_high_water: Default::default(),
        })
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
    pub(super) fn initialize_genesis(
        setup: GenesisSetup<E, C, G>,
        now: Result<u64, ImportError>,
        replaying: bool,
    ) -> Result<Self, ImportError> {
        let GenesisSetup {
            config,
            anchor,
            repository,
            engine,
            classic_availability,
            gloas_availability,
            limits,
            runtime,
            clock,
        } = setup;
        let now = now?;
        limits.validate()?;
        if checked_config_digest(engine.config())? != checked_config_digest(&config)? {
            return Err(ImportError::Configuration)
        }
        let genuine = VerifiedGenesis::authenticate(
            config.clone(),
            anchor.genesis.original_state_ssz(),
            anchor.decode_limits,
        )
        .map_err(|error| ImportError::ObservationInfrastructure(error.to_string()))?;
        if genuine.config_digest() != anchor.genesis.config_digest() ||
            genuine.block_root() != anchor.genesis.block_root() ||
            genuine.state_root() != anchor.genesis.state_root()
        {
            return Err(ImportError::Configuration)
        }
        // The actual bounded clock is checked before publishing any new trust descriptor.
        let current = config
            .clock()
            .map_err(|_| ImportError::Configuration)?
            .slot_at(now)
            .ok_or(ImportError::Clock)?;
        if !replaying && current > limits.max_tick_slots {
            return Err(ImportError::Bounds)
        }
        let origin = genuine.block_root();
        // Mint the original data-only seal before a cold recovery graph selects its later cut.
        // A retained pin or lookup index never substitutes for fresh full genesis authentication.
        let unsigned_custody_context =
            UnsignedCustodyAnchor::from_genuine_genesis(&genuine, &config)?;
        repository
            .persist_genesis(&genuine, anchor.decode_limits)
            .map_err(|error| ImportError::Persistence(error.to_string()))?;
        let (state, block) = genuine
            .into_import_inputs(&config)
            .map_err(|error| ImportError::ObservationInfrastructure(error.to_string()))?;
        let repository = Arc::new(repository);
        let source = RepositoryStateSource::open(repository.clone())
            .map_err(|error| ImportError::Persistence(error.to_string()))?;
        let parameters =
            RuntimeParameters::from_config(&config).map_err(|_| ImportError::Configuration)?;
        let local = if replaying {
            repository
                .local_recovery_inputs()
                .map_err(|error| ImportError::Persistence(error.to_string()))?
        } else {
            None
        };
        let mut graph = if let Some(local) = local {
            Graph::new_at_checkpoint(
                local.post.state().clone(),
                local.block,
                local.checkpoint,
                &local.boundary.state().to_ssz().map_err(|_| ImportError::Wire)?,
                &parameters,
                source.clone(),
                limits.fast_confirmation,
            )?
        } else {
            Graph::new(
                Arc::unwrap_or_clone(state),
                Arc::unwrap_or_clone(block),
                &parameters,
                source.clone(),
                limits.fast_confirmation,
            )?
        };
        if !replaying {
            graph.bootstrap_tick(&config, now, limits.max_tick_slots)?;
        }
        if !replaying &&
            let Some(saved) = repository
                .current_snapshot()
                .map_err(|error| ImportError::Persistence(error.to_string()))?
        {
            let actual = graph.snapshot()?;
            if saved.head() != actual.head ||
                saved.state_root() != recovery::state_root(&graph, actual.head.root)? ||
                saved.checkpoints() != (actual.justified_checkpoint, actual.finalized_checkpoint)
            {
                return Err(ImportError::RecoveryRequired)
            }
        }
        let earliest_available_slot = graph
            .snapshot()?
            .finalized_checkpoint
            .epoch
            .checked_mul(config.slots_per_epoch)
            .ok_or(ImportError::Bounds)?;
        let recovery_execution_boundary = if replaying {
            match &graph {
                Graph::Gloas { store, .. }
                    if matches!(
                        store.verified_execution_block_hash(store.get_head()?),
                        Err(StoreError::UnknownRoot)
                    ) =>
                {
                    Some(
                        PreparedRecoveryExecutionBoundary::prepare(
                            &config,
                            store,
                            &repository,
                            &engine,
                        )
                        .map_err(execution_boundary_error)?,
                    )
                }
                _ => None,
            }
        } else {
            None
        };
        let mut worker = Self {
            graph,
            config,
            repository,
            source,
            origin_genesis: Some(origin),
            gossip_state: gossip::GossipState::new()?,
            engine,
            classic_availability,
            gloas_availability,
            limits,
            runtime,
            optimistic: false,
            last_execution_retry_slot: None,
            import_metrics: Default::default(),
            last_execution_state: None,
            last_verified_forkchoice: None,
            earliest_available_slot,
            execution_boundary: None,
            recovery_execution_boundary,
            replaying,
            replay: ReplayContext::default(),
            column_retention: None,
            unsigned_custody_context: Some(unsigned_custody_context),
            clock,
        };
        if let Some(context) = &worker.unsigned_custody_context {
            worker
                .source
                .retain_unsigned_context(context)
                .map_err(|error| ImportError::Persistence(error.to_string()))?;
        }
        worker
            .source
            .enable_event_transcript()
            .map_err(|error| ImportError::Persistence(error.to_string()))?;
        let mut graph = worker.graph.clone();
        worker.publish(&mut graph)?;
        Ok(worker)
    }
}

#[cfg(test)]
pub(crate) mod tests;
