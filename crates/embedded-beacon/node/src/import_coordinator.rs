//! Bounded single-owner admission of original signed consensus wire inputs.
//!
//! A dedicated thread owns the authenticated graph and performs SSZ, BLS and persistence work.
//! Actual Engine and availability futures are driven on the captured runtime from that thread.
//! Durable data precedes Engine fork choice; canonical metadata is published last. Any attempted
//! fork-choice failure or uncertain publication halts this owner and requires authenticated
//! reopen/reconciliation. Cancellation of a reply does not cancel an already queued import.
//! This component grants no live-readiness or recovery authority to persisted head hints.

use self::{
    confirmation::Confirmation,
    mode::{ReplayContext, StartupExecutionMode},
    optimistic::optimistic_error,
};
use crate::{
    bootstrap::checked_config_digest,
    compact_persistence::{state_source::RepositoryStateSource, AdmittedStateRepository},
    execution_bootstrap::ReconciledCheckpoint,
    execution_checkpoint_boundary::{
        recovery::PreparedRecoveryExecutionBoundary, ExecutionBoundaryError,
        ExecutionCheckpointBoundary,
    },
    runtime_engine::{forkchoice_receipt::VerifiedForkchoice, RuntimeEngineAdapter},
    runtime_parameters::RuntimeParameters,
    BootstrapDecodeLimits, BootstrapError, CheckpointBlockFormat, ReplayedCheckpoint,
    SszCheckpointVerifier, VerifiedCheckpoint,
};
use alloy_primitives::B256;
use alloy_rpc_types_engine::{ExecutionData, ForkchoiceState};
use reth_embedded_beacon_config::{ChainConfig, Preset as ConfigPreset};
use reth_embedded_beacon_crypto::{compute_domain, compute_signing_root, verify_signature};
use reth_embedded_beacon_execution_proxy::{
    ExecutionEngine, ForkchoiceReply, ForkchoiceVersion, PayloadReply,
};
use reth_embedded_beacon_fork_choice::{
    compact::{
        ClassicConfirmationView, CompactGloasStore, CompactStateSource, CompactStore,
        FinalizedRetentionPolicy, GloasConfirmationView, GloasStateRead, ReadGloasGossipView,
        ReadGossipView, StateRead,
    },
    data_availability::DataAvailability as ClassicAvailability,
    fast_confirmation::{FastConfirmation, FastConfirmationView, StateRef},
    gloas::{DataAvailability as GloasAvailability, EnvelopeError, GloasStore},
    ForkChoiceError, PayloadStatus, Store, StoreError, StoreSnapshot,
};
use reth_embedded_beacon_network::{
    availability_rpc::WireSidecar,
    block_rpc::WireBlock,
    envelope_rpc::{EnvelopeBlockReference, WireEnvelope},
    gossip::GossipTopic,
    transport::GossipVerdict,
};
use reth_embedded_beacon_primitives::{Fork, Root};
use reth_embedded_beacon_state_transition::{
    phase0_processing::ProcessingError,
    ssz::{gloas, BeaconBlock, BeaconState, Preset, SignedBeaconBlock, Ssz},
};
use reth_embedded_beacon_storage::StateLease;
use std::{
    collections::BTreeSet,
    future::Future,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use thiserror::Error;
use tokio::{
    runtime::Handle,
    sync::{mpsc, oneshot, OwnedSemaphorePermit, Semaphore},
};

/// Cloneable bounded command handle; graph ownership never leaves the worker thread.
#[derive(Clone, Debug)]
pub struct ImportCoordinator {
    sender: mpsc::Sender<Command>,
    bytes: Arc<Semaphore>,
    commands: Arc<Semaphore>,
    halted: Arc<AtomicBool>,
    limits: ImportLimits,
    config_digest: Root,
    queue_high_water: Arc<metrics::QueueHighWater>,
}

impl ImportCoordinator {
    /// Reauthenticate the exact checkpoint and start one dedicated graph owner.
    /// Construction, decoding and durable bootstrap execute on that owner thread.
    /// The supplied Engine adapter and repository must be bound to this checked network;
    /// concrete checkpoint persistence independently authenticates the repository binding.
    pub async fn start<E, C, G>(
        config: ChainConfig,
        anchor: ImportAnchor,
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
        Self::start_with_mode(
            config,
            anchor,
            repository,
            engine,
            classic_availability,
            gloas_availability,
            limits,
            StartupExecutionMode::Strict,
        )
        .await
    }

    /// Start only after this exact adapter acknowledged the concretely authenticated EL target.
    /// The sealed receipt also authenticates a modern Gloas anchor's carried execution base.
    #[allow(clippy::too_many_arguments)]
    pub async fn start_reconciled<E, C, G>(
        config: ChainConfig,
        anchor: ImportAnchor,
        repository: AdmittedStateRepository,
        engine: RuntimeEngineAdapter<E>,
        receipt: ReconciledCheckpoint,
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
        Self::start_with_mode(
            config,
            anchor,
            repository,
            engine,
            classic_availability,
            gloas_availability,
            limits,
            StartupExecutionMode::Reconciled(Box::new(receipt)),
        )
        .await
    }

    /// Start from an authenticated trusted checkpoint while the same owner drives EL catch-up.
    /// Descendants still require full consensus, BLS and availability validation. Only genuine
    /// typed SYNCING/ACCEPTED defer execution, and no Engine acknowledgement is fabricated.
    pub async fn start_optimistic<E, C, G>(
        config: ChainConfig,
        anchor: ImportAnchor,
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
        Self::start_with_mode(
            config,
            anchor,
            repository,
            engine,
            classic_availability,
            gloas_availability,
            limits,
            StartupExecutionMode::Optimistic,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn start_with_mode<E, C, G>(
        config: ChainConfig,
        anchor: ImportAnchor,
        repository: AdmittedStateRepository,
        engine: RuntimeEngineAdapter<E>,
        classic_availability: C,
        gloas_availability: G,
        limits: ImportLimits,
        mode: StartupExecutionMode,
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
            .name("embedded-beacon-import".into())
            .spawn(move || {
                let _shutdown = shutdown;
                let setup = WorkerSetup {
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
                let initialized = Worker::initialize_mode(setup, now, false, mode);
                match initialized {
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

    /// Immutable concrete consensus configuration authenticated before worker startup.
    pub const fn config_digest(&self) -> Root {
        self.config_digest
    }

    /// Admit complete original signed block bytes with immediate bounded backpressure.
    /// Only the worker's actual wall clock advances consensus; peer receive time is diagnostic.
    /// Reservation occurs synchronously, before the returned reply future can be polled.
    pub fn import_block(
        &self,
        block: WireBlock,
        received_unix_ms: u64,
    ) -> impl Future<Output = Result<ImportReceipt, ImportError>> + Send + 'static {
        let admitted = self.try_submit(Input::Block(block), received_unix_ms);
        async move { admitted?.await.map_err(|_| ImportError::Closed)? }
    }

    /// Admit a complete original Gloas signed envelope; PTC observations cannot verify it.
    /// Saturation returns immediately without retaining an uncounted input waiter.
    pub fn import_envelope(
        &self,
        envelope: WireEnvelope,
        received_unix_ms: u64,
    ) -> impl Future<Output = Result<ImportReceipt, ImportError>> + Send + 'static {
        let admitted = self.try_submit(Input::Envelope(envelope), received_unix_ms);
        async move { admitted?.await.map_err(|_| ImportError::Closed)? }
    }

    /// Apply native gossip admission before the same strict original block importer.
    pub fn import_gossip_block(
        &self,
        block: WireBlock,
        received_unix_ms: u64,
    ) -> impl Future<Output = Result<ImportReceipt, ImportError>> + Send + 'static {
        let admitted = self.try_submit(Input::GossipBlock(block), received_unix_ms);
        async move { admitted?.await.map_err(|_| ImportError::Closed)? }
    }

    /// Apply native gossip admission before strict signed Engine/data envelope verification.
    pub fn import_gossip_envelope(
        &self,
        envelope: WireEnvelope,
        received_unix_ms: u64,
    ) -> impl Future<Output = Result<ImportReceipt, ImportError>> + Send + 'static {
        let admitted = self.try_submit(Input::GossipEnvelope(envelope), received_unix_ms);
        async move { admitted?.await.map_err(|_| ImportError::Closed)? }
    }

    /// Validate bounded original gossip observations on the sole consensus graph owner.
    pub fn gossip(
        &self,
        topic: GossipTopic,
        input: Arc<[u8]>,
        received_unix_ms: u64,
    ) -> impl Future<Output = Result<GossipVerdict, ImportError>> + Send + 'static {
        let admitted = self.try_gossip(topic, input, received_unix_ms);
        async move { admitted?.await.map_err(|_| ImportError::Closed)? }
    }

    /// Query only the actual admitted graph, with the same immediate command-count admission.
    pub fn snapshot(
        &self,
    ) -> impl Future<Output = Result<StoreSnapshot, ImportError>> + Send + 'static {
        let admitted = self.try_snapshot();
        async move { admitted?.await.map_err(|_| ImportError::Closed)? }
    }

    /// Mint peer status exclusively from the real admitted graph and configured graph clock.
    pub fn authenticated_status(
        &self,
    ) -> impl Future<Output = Result<AuthenticatedStatus, ImportError>> + Send + 'static {
        let admitted = self.try_status();
        async move { admitted?.await.map_err(|_| ImportError::Closed)? }
    }

    /// Check only whether this exact root is retained by the actual admitted graph.
    /// Remote head hints and saved admission records cannot satisfy this query.
    pub fn contains_block(
        &self,
        root: Root,
    ) -> impl Future<Output = Result<bool, ImportError>> + Send + 'static {
        let admitted = self.try_contains_block(root);
        async move { admitted?.await.map_err(|_| ImportError::Closed)? }
    }

    /// Return linkage metadata only for an actual retained Gloas native block.
    /// No saved-root hint, unsigned signature, payload receipt or availability is fabricated.
    pub fn block_reference(
        &self,
        root: Root,
    ) -> impl Future<Output = Result<Option<EnvelopeBlockReference>, ImportError>> + Send + 'static
    {
        let admitted = self.try_block_reference(root);
        async move { admitted?.await.map_err(|_| ImportError::Closed)? }
    }

    /// A halted owner has no mutable operator override or automatic retry path.
    pub fn is_halted(&self) -> bool {
        self.halted.load(Ordering::Acquire)
    }

    fn try_submit(
        &self,
        input: Input,
        received_unix_ms: u64,
    ) -> Result<oneshot::Receiver<Result<ImportReceipt, ImportError>>, ImportError> {
        let result = self.try_submit_inner(input, received_unix_ms);
        if result.is_ok() {
            metrics::QueueHighWater::increment(&self.queue_high_water.imports_enqueued);
        } else if matches!(&result, Err(ImportError::Backpressure)) {
            metrics::QueueHighWater::increment(&self.queue_high_water.import_backpressure);
        }
        result
    }

    fn try_submit_inner(
        &self,
        input: Input,
        received_unix_ms: u64,
    ) -> Result<oneshot::Receiver<Result<ImportReceipt, ImportError>>, ImportError> {
        if self.is_halted() {
            return Err(ImportError::Halted)
        }
        let amount = self.limits.reserve_bytes(input.len())?;
        let slot = self.commands.clone().try_acquire_owned().map_err(|error| match error {
            tokio::sync::TryAcquireError::Closed => ImportError::Halted,
            tokio::sync::TryAcquireError::NoPermits => ImportError::Backpressure,
        })?;
        let queue = self.sender.try_reserve().map_err(|error| match error {
            mpsc::error::TrySendError::Closed(()) => ImportError::Closed,
            mpsc::error::TrySendError::Full(()) => ImportError::Backpressure,
        })?;
        let bytes =
            self.bytes.clone().try_acquire_many_owned(amount).map_err(|error| match error {
                tokio::sync::TryAcquireError::Closed => ImportError::Halted,
                tokio::sync::TryAcquireError::NoPermits => ImportError::Backpressure,
            })?;
        let (reply, receive) = oneshot::channel();
        self.observe_queue_admission();
        queue.send(Command::Import { input, received_unix_ms, reply, _bytes: bytes, _slot: slot });
        Ok(receive)
    }

    fn try_snapshot(
        &self,
    ) -> Result<oneshot::Receiver<Result<StoreSnapshot, ImportError>>, ImportError> {
        if self.is_halted() {
            return Err(ImportError::Halted)
        }
        let slot = self.commands.clone().try_acquire_owned().map_err(|error| match error {
            tokio::sync::TryAcquireError::Closed => ImportError::Halted,
            tokio::sync::TryAcquireError::NoPermits => ImportError::Backpressure,
        })?;
        let queue = self.sender.try_reserve().map_err(|error| match error {
            mpsc::error::TrySendError::Closed(()) => ImportError::Closed,
            mpsc::error::TrySendError::Full(()) => ImportError::Backpressure,
        })?;
        let (reply, receive) = oneshot::channel();
        self.observe_queue_admission();
        queue.send(Command::Snapshot { reply, _slot: slot });
        Ok(receive)
    }

    fn try_status(
        &self,
    ) -> Result<oneshot::Receiver<Result<AuthenticatedStatus, ImportError>>, ImportError> {
        if self.is_halted() {
            return Err(ImportError::Halted)
        }
        let slot = self.commands.clone().try_acquire_owned().map_err(|error| match error {
            tokio::sync::TryAcquireError::Closed => ImportError::Halted,
            tokio::sync::TryAcquireError::NoPermits => ImportError::Backpressure,
        })?;
        let queue = self.sender.try_reserve().map_err(|error| match error {
            mpsc::error::TrySendError::Closed(()) => ImportError::Closed,
            mpsc::error::TrySendError::Full(()) => ImportError::Backpressure,
        })?;
        let (reply, receive) = oneshot::channel();
        self.observe_queue_admission();
        queue.send(Command::Status { reply, _slot: slot });
        Ok(receive)
    }
    fn try_contains_block(
        &self,
        root: Root,
    ) -> Result<oneshot::Receiver<Result<bool, ImportError>>, ImportError> {
        if self.is_halted() {
            return Err(ImportError::Halted)
        }
        let slot = self.commands.clone().try_acquire_owned().map_err(|error| match error {
            tokio::sync::TryAcquireError::Closed => ImportError::Halted,
            tokio::sync::TryAcquireError::NoPermits => ImportError::Backpressure,
        })?;
        let queue = self.sender.try_reserve().map_err(|error| match error {
            mpsc::error::TrySendError::Closed(()) => ImportError::Closed,
            mpsc::error::TrySendError::Full(()) => ImportError::Backpressure,
        })?;
        let (reply, receive) = oneshot::channel();
        self.observe_queue_admission();
        queue.send(Command::ContainsBlock { root, reply, _slot: slot });
        Ok(receive)
    }
    fn try_gossip(
        &self,
        topic: GossipTopic,
        input: Arc<[u8]>,
        received_unix_ms: u64,
    ) -> Result<oneshot::Receiver<Result<GossipVerdict, ImportError>>, ImportError> {
        if self.is_halted() {
            return Err(ImportError::Halted)
        }
        let amount = self.limits.reserve_bytes(input.len())?;
        let slot = self.commands.clone().try_acquire_owned().map_err(|error| match error {
            tokio::sync::TryAcquireError::Closed => ImportError::Halted,
            tokio::sync::TryAcquireError::NoPermits => ImportError::Backpressure,
        })?;
        let queue = self.sender.try_reserve().map_err(|error| match error {
            mpsc::error::TrySendError::Closed(()) => ImportError::Closed,
            mpsc::error::TrySendError::Full(()) => ImportError::Backpressure,
        })?;
        let bytes =
            self.bytes.clone().try_acquire_many_owned(amount).map_err(|error| match error {
                tokio::sync::TryAcquireError::Closed => ImportError::Halted,
                tokio::sync::TryAcquireError::NoPermits => ImportError::Backpressure,
            })?;
        let (reply, receive) = oneshot::channel();
        self.observe_queue_admission();
        queue.send(Command::Gossip {
            topic,
            bytes: input,
            received_unix_ms,
            reply,
            _bytes: bytes,
            _slot: slot,
        });
        Ok(receive)
    }
    fn try_block_reference(
        &self,
        root: Root,
    ) -> Result<oneshot::Receiver<Result<Option<EnvelopeBlockReference>, ImportError>>, ImportError>
    {
        if self.is_halted() {
            return Err(ImportError::Halted)
        }
        let slot = self.commands.clone().try_acquire_owned().map_err(|error| match error {
            tokio::sync::TryAcquireError::Closed => ImportError::Halted,
            tokio::sync::TryAcquireError::NoPermits => ImportError::Backpressure,
        })?;
        let queue = self.sender.try_reserve().map_err(|error| match error {
            mpsc::error::TrySendError::Closed(()) => ImportError::Closed,
            mpsc::error::TrySendError::Full(()) => ImportError::Backpressure,
        })?;
        let (reply, receive) = oneshot::channel();
        self.observe_queue_admission();
        queue.send(Command::BlockReference { root, reply, _slot: slot });
        Ok(receive)
    }
}

/// Concrete checkpoint proof and the exact original bytes it authenticates.
#[derive(Clone, Debug)]
pub struct ImportAnchor {
    /// Must originate from the concrete SSZ checkpoint verifier.
    pub verified: VerifiedCheckpoint,
    /// Original complete fork-aware state bytes.
    pub state_ssz: Arc<[u8]>,
    /// Original complete block bytes, interpreted by the explicit container format.
    pub block_ssz: Arc<[u8]>,
    /// Provider's exact signed or unsigned container.
    pub block_format: CheckpointBlockFormat,
    /// Additional operator age cap, never a substitute for state-dependent trust verification.
    pub max_age_epochs: u64,
    /// Exact bounded bootstrap decoder limits.
    pub decode_limits: BootstrapDecodeLimits,
    /// Sealed scheduled boundary replay, preserving the actual earlier post-block schema.
    pub replay: Option<ReplayAnchor>,
}

/// Exact earlier post-block bytes bound by the concrete scheduled replay verifier.
#[derive(Clone, Debug)]
pub struct ReplayAnchor {
    /// Unforgeable evidence binding both complete states and original block/container.
    pub proof: ReplayedCheckpoint,
    /// Actual post-block state bytes at the original retained block slot and fork.
    pub post_block_state_ssz: Arc<[u8]>,
}

/// Independent bounds on queued original inputs and retained graph work.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ImportLimits {
    /// Includes imports and small snapshot commands.
    pub command_capacity: usize,
    /// Total original input bytes reserved through complete processing, including the active item.
    pub max_queued_bytes: usize,
    /// Maximum one original input; complete decoding is still mandatory.
    pub max_input_bytes: usize,
    /// Maximum checked slot advancement in one event.
    pub max_tick_slots: u64,
    /// Opt into probabilistic fast confirmation; disabled nodes use authenticated finalized
    /// safety.
    pub fast_confirmation: bool,
}
impl Default for ImportLimits {
    fn default() -> Self {
        Self {
            command_capacity: 16,
            max_queued_bytes: 32 * 1024 * 1024,
            max_input_bytes: 10 * 1024 * 1024,
            max_tick_slots: 8192,
            fast_confirmation: false,
        }
    }
}
impl ImportLimits {
    const fn validate(self) -> Result<(), ImportError> {
        if self.command_capacity == 0 ||
            self.command_capacity > 1024 ||
            self.max_input_bytes == 0 ||
            self.max_input_bytes > 10 * 1024 * 1024 ||
            self.max_queued_bytes < self.max_input_bytes ||
            self.max_queued_bytes > u32::MAX as usize ||
            self.max_queued_bytes > Semaphore::MAX_PERMITS ||
            self.max_tick_slots == 0 ||
            self.max_tick_slots > 8192
        {
            return Err(ImportError::Bounds)
        }
        Ok(())
    }
    fn reserve_bytes(self, len: usize) -> Result<u32, ImportError> {
        self.validate()?;
        if len == 0 || len > self.max_input_bytes {
            return Err(ImportError::Bounds)
        }
        u32::try_from(len).map_err(|_| ImportError::Bounds)
    }
}

/// Successful strict admission plus the resulting actual consensus snapshot.
#[derive(Clone, Debug)]
pub struct ImportReceipt {
    /// Exact admitted unsigned beacon root; envelopes refer to their retained beacon block.
    pub admitted_root: Root,
    /// Head, payload branch and finality from the actual private graph.
    pub snapshot: StoreSnapshot,
    /// Whether this event attempted and successfully acknowledged real Engine fork choice.
    pub execution_forkchoice_updated: bool,
}

/// Rejections are distinct from halts requiring authenticated reopen and EL reconciliation.
#[derive(Debug, Error)]
pub enum ImportError {
    /// Invalid checked configuration or cross-network anchor.
    #[error("import configuration or authenticated network binding mismatch")]
    Configuration,
    /// Missing runtime for actual asynchronous callbacks.
    #[error("import coordinator requires an active runtime")]
    Runtime,
    /// Bounded admission or clock advancement exceeded.
    #[error("import coordinator resource bound exceeded")]
    Bounds,
    /// Active and queued commands/bytes exhausted; the bounded network owner may retry later.
    #[error("bounded import admission saturated")]
    Backpressure,
    /// Dedicated owner could not start.
    #[error("import worker could not start: {0}")]
    Thread(String),
    /// Exact concrete checkpoint authentication failed.
    #[error(transparent)]
    Bootstrap(#[from] BootstrapError),
    /// Wire fields do not match independently decoded original schemas.
    #[error("original signed wire metadata mismatch")]
    Wire,
    /// Actual signed transition, execution or data verification rejected the candidate.
    #[error(transparent)]
    Consensus(#[from] StoreError),
    /// Actual envelope verification rejected the candidate.
    #[error("strict signed envelope rejected: {0}")]
    Envelope(String),
    /// Normative gossip dependencies or duplicate observations are not peer invalidity.
    #[error("gossip observation ignored: {0}")]
    GossipIgnore(&'static str),
    /// Native signed gossip validation rejected the original observation.
    #[error("gossip observation rejected: {0}")]
    GossipReject(&'static str),
    /// Resource, configuration or internal failure while admitting genuine contained observations.
    #[error("retained consensus observation infrastructure failed: {0}")]
    ObservationInfrastructure(String),
    /// A sealed execution checkpoint association or actual adapter binding failed.
    #[error("authenticated execution checkpoint boundary failed: {0}")]
    ExecutionBoundary(String),
    /// A graph-derived canonical association could not be proven after admission.
    #[error("canonical graph publication prerequisite failed: {0}")]
    PublicationInfrastructure(String),
    /// No unauthenticated graph representation conversion is permitted.
    #[error("authenticated graph migration is unavailable for this transition")]
    UnsupportedTransition,
    /// Later persisted head hints require complete signed replay before graph initialization.
    #[error("authenticated signed-history replay required before coordinator reopen")]
    RecoveryRequired,
    /// Persistence failure may leave durable data; no canonical success is returned.
    #[error("durable admitted-state publication failed: {0}")]
    Persistence(String),
    /// An attempted FCU can have executed remotely even when no acknowledgement arrives.
    #[error("attempted Engine fork choice requires reconciliation: {0}")]
    Forkchoice(String),
    /// Actual typed SYNCING or ACCEPTED permits bounded retry of this exact original input.
    /// It never publishes consensus, payload validity or a canonical execution receipt.
    #[error("execution payload for beacon input {0:?} remains pending")]
    ExecutionPending(Root),
    /// A strict callback timed out, was cancelled or returned ambiguous Engine evidence.
    #[error("Engine admission closed after uncertain import outcome")]
    EngineUncertain,
    /// Actual wall clock could not be represented or moved backwards.
    #[error("import wall clock invalid or regressed")]
    Clock,
    /// Owner halted; no operator validity override is available.
    #[error("import owner halted; authenticated reopen and reconciliation required")]
    Halted,
    /// Last owner or channel closed.
    #[error("import worker closed")]
    Closed,
}
impl ImportError {
    pub(crate) const fn fatal(&self) -> bool {
        matches!(
            self,
            Self::Consensus(StoreError::CompactState(_)) |
                Self::Persistence(_) |
                Self::ExecutionBoundary(_) |
                Self::PublicationInfrastructure(_) |
                Self::ObservationInfrastructure(_) |
                Self::Forkchoice(_) |
                Self::EngineUncertain |
                Self::Clock |
                Self::Halted
        )
    }
}

#[derive(Clone, Debug)]
enum Input {
    Block(WireBlock),
    Envelope(WireEnvelope),
    GossipBlock(WireBlock),
    GossipEnvelope(WireEnvelope),
}
impl Input {
    const fn root(&self) -> Root {
        match self {
            Self::Block(wire) | Self::GossipBlock(wire) => wire.root(),
            Self::Envelope(wire) | Self::GossipEnvelope(wire) => wire.beacon_block_root(),
        }
    }
    fn len(&self) -> usize {
        match self {
            Self::Block(block) | Self::GossipBlock(block) => block.ssz().len(),
            Self::Envelope(envelope) | Self::GossipEnvelope(envelope) => envelope.ssz().len(),
        }
    }
}
#[derive(Debug)]
enum Command {
    Import {
        input: Input,
        received_unix_ms: u64,
        reply: oneshot::Sender<Result<ImportReceipt, ImportError>>,
        _bytes: OwnedSemaphorePermit,
        _slot: OwnedSemaphorePermit,
    },
    BlockReference {
        root: Root,
        reply: oneshot::Sender<Result<Option<EnvelopeBlockReference>, ImportError>>,
        _slot: OwnedSemaphorePermit,
    },
    CustodyAnchor {
        root: Option<Root>,
        reply: oneshot::Sender<Result<Option<UnsignedCustodyAnchor>, ImportError>>,
        _slot: OwnedSemaphorePermit,
    },
    CustodyDependency {
        reply: oneshot::Sender<Result<Option<HistoricalDependency>, ImportError>>,
        _slot: OwnedSemaphorePermit,
    },
    CustodyContext {
        block: WireBlock,
        reply: oneshot::Sender<Result<(), ImportError>>,
        _bytes: OwnedSemaphorePermit,
        _slot: OwnedSemaphorePermit,
    },
    CustodyColumns {
        root: Root,
        originals: Vec<WireSidecar>,
        reply: oneshot::Sender<Result<(), ImportError>>,
        _bytes: OwnedSemaphorePermit,
        _slot: OwnedSemaphorePermit,
    },
    Serve {
        request: ServingRequest,
        reply: oneshot::Sender<Result<Option<ServingBatch>, ImportError>>,
        _slot: OwnedSemaphorePermit,
    },
    Observe {
        request: ObservationRequest,
        reply: oneshot::Sender<Result<Observation, ImportError>>,
        _slot: OwnedSemaphorePermit,
    },
    ColumnRetention {
        publisher: Arc<dyn ColumnRetention>,
        reply: oneshot::Sender<Result<(), ImportError>>,
        _slot: OwnedSemaphorePermit,
    },
    Gossip {
        topic: GossipTopic,
        bytes: Arc<[u8]>,
        received_unix_ms: u64,
        reply: oneshot::Sender<Result<GossipVerdict, ImportError>>,
        _bytes: OwnedSemaphorePermit,
        _slot: OwnedSemaphorePermit,
    },
    ContainsBlock {
        root: Root,
        reply: oneshot::Sender<Result<bool, ImportError>>,
        _slot: OwnedSemaphorePermit,
    },
    Snapshot {
        reply: oneshot::Sender<Result<StoreSnapshot, ImportError>>,
        _slot: OwnedSemaphorePermit,
    },
    Status {
        reply: oneshot::Sender<Result<AuthenticatedStatus, ImportError>>,
        _slot: OwnedSemaphorePermit,
    },
}
struct Shutdown {
    bytes: Arc<Semaphore>,
    commands: Arc<Semaphore>,
    halted: Arc<AtomicBool>,
}
impl Drop for Shutdown {
    fn drop(&mut self) {
        self.halted.store(true, Ordering::Release);
        self.bytes.close();
        self.commands.close();
    }
}

#[derive(Clone, Debug)]
enum Graph {
    Classic { store: CompactStore, confirmation: Confirmation<ClassicConfirmationView> },
    Gloas { store: CompactGloasStore, confirmation: Confirmation<GloasConfirmationView> },
}
impl Graph {
    fn new(
        state: BeaconState,
        block: BeaconBlock,
        parameters: &RuntimeParameters,
        source: Arc<dyn CompactStateSource>,
        fast_confirmation: bool,
    ) -> Result<Self, ImportError> {
        match (state, block) {
            (BeaconState::Gloas(state, _), BeaconBlock::Gloas(block, _)) => {
                let store = GloasStore::get_store(
                    state,
                    block,
                    parameters.fork_choice,
                    parameters.transitions.gloas.clone(),
                )?;
                Self::from_gloas(store, source, fast_confirmation)
            }
            (state, block)
                if state.schema_fork() < Fork::Gloas && block.schema_fork() < Fork::Gloas =>
            {
                let mut store = Store::get_store(state, block, parameters.fork_choice)?;
                store.set_transition_parameters(parameters.transitions.clone())?;
                Self::from_classic(store, source, fast_confirmation)
            }
            _ => Err(ImportError::Configuration),
        }
    }
    fn from_classic(
        store: Store,
        source: Arc<dyn CompactStateSource>,
        fast_confirmation: bool,
    ) -> Result<Self, ImportError> {
        let graph_limits = source.graph_limits();
        let store = CompactStore::from_store(store, source, graph_limits)?;
        Ok(Self::Classic { store, confirmation: Confirmation::new(fast_confirmation) })
    }
    fn from_gloas(
        store: GloasStore,
        source: Arc<dyn CompactStateSource>,
        fast_confirmation: bool,
    ) -> Result<Self, ImportError> {
        let graph_limits = source.graph_limits();
        let store = CompactGloasStore::from_store(store, source, graph_limits)?;
        Self::from_compact_gloas(store, fast_confirmation)
    }
    fn from_compact_gloas(
        store: CompactGloasStore,
        fast_confirmation: bool,
    ) -> Result<Self, ImportError> {
        Ok(Self::Gloas { store, confirmation: Confirmation::new(fast_confirmation) })
    }
    fn snapshot(&self) -> Result<StoreSnapshot, ImportError> {
        Ok(match self {
            Self::Classic { store, .. } => store.snapshot()?,
            Self::Gloas { store, .. } => store.snapshot()?,
        })
    }
    const fn current_slot(&self) -> u64 {
        match self {
            Self::Classic { store, .. } => store.current_slot(),
            Self::Gloas { store, .. } => store.current_slot(),
        }
    }
    const fn time_ms(&self) -> u64 {
        match self {
            Self::Classic { store, .. } => store.time_ms(),
            Self::Gloas { store, .. } => store.time_ms(),
        }
    }
    fn tick(&mut self, now: u64) -> Result<(), ImportError> {
        match self {
            Self::Classic { store, .. } => store.on_tick(now)?,
            Self::Gloas { store, .. } => store.on_tick(now)?,
        }
        Ok(())
    }
    fn checked_target(
        &self,
        config: &ChainConfig,
        now: u64,
        maximum: u64,
    ) -> Result<u64, ImportError> {
        if now < self.time_ms() {
            return Err(ImportError::Clock)
        }
        let target = config
            .clock()
            .map_err(|_| ImportError::Configuration)?
            .slot_at(now)
            .ok_or(ImportError::Clock)?;
        if target.checked_sub(self.current_slot()).ok_or(ImportError::Clock)? > maximum {
            return Err(ImportError::Bounds)
        }
        Ok(target)
    }
    fn bootstrap_tick(
        &mut self,
        config: &ChainConfig,
        now: u64,
        maximum: u64,
    ) -> Result<(), ImportError> {
        self.checked_target(config, now, maximum)?;
        self.tick(now)?;
        // Historical catch-up grants no observations at missed deadlines. The pinned initializer
        // conservatively binds every confirmation variable to this exact graph's finalized root.
        self.reset_confirmation()
    }
    fn advance(&mut self, config: &ChainConfig, now: u64, maximum: u64) -> Result<(), ImportError> {
        let target = self.checked_target(config, now, maximum)?;
        let prior = self.current_slot();
        self.tick(now)?;
        if target == prior {
            return Ok(())
        }
        self.observe_confirmation(config, prior, target, now)?;
        Ok(())
    }
    fn admit_body_observations(
        &mut self,
        root: Root,
        signed: &SignedBeaconBlock,
    ) -> Result<
        Vec<(usize, reth_embedded_beacon_fork_choice::slashing_replay::SlashingContext)>,
        ImportError,
    > {
        let (attestations, slashings) = body_observation_counts(signed);
        let mut verified = Vec::new();
        if attestations != 0 {
            let results = match self {
                Self::Classic { store, .. } => store.on_retained_block_attestations(root),
                Self::Gloas { store, .. } => store.on_retained_block_attestations(root),
            }
            .map_err(|error| ImportError::ObservationInfrastructure(error.to_string()))?;
            for (index, result) in results {
                retained_observation_result(root, index, "attestation", result)?;
            }
        }
        for index in 0..slashings {
            let result = match self {
                Self::Classic { store, .. } => {
                    store.on_retained_block_slashing_with_context(root, index)
                }
                Self::Gloas { store, .. } => {
                    store.on_retained_block_slashing_with_context(root, index)
                }
            };
            if let Ok(context) = &result {
                verified.push((index, *context));
            }
            retained_observation_result(root, index, "slashing", result.map(|_| ()))?;
        }
        Ok(verified)
    }
    fn execution_state_with_boundary(
        &self,
        boundary: Option<&ExecutionCheckpointBoundary>,
    ) -> Result<Option<(Fork, ForkchoiceState)>, ImportError> {
        if let Self::Classic { store, .. } = self &&
            store.execution_optimism_enabled() &&
            (!store.execution_optimism_settled() || !self.confirmation_enabled())
        {
            return self.optimistic_execution_target()
        }
        self.execution_state_with_snapshot(boundary, self.snapshot()?)
    }
    fn execution_state_with_snapshot(
        &self,
        boundary: Option<&ExecutionCheckpointBoundary>,
        snapshot: StoreSnapshot,
    ) -> Result<Option<(Fork, ForkchoiceState)>, ImportError> {
        if let Self::Classic { store, .. } = self &&
            store.execution_optimism_enabled() &&
            (!store.execution_optimism_settled() || !self.confirmation_enabled())
        {
            return self.optimistic_execution_target()
        }
        let (fork, head, safe, finalized) = match self {
            Self::Classic { store, confirmation } => {
                let Some(head) = store.verified_execution_block_hash(snapshot.head.root)? else {
                    return Ok(None)
                };
                let fork =
                    store.block(snapshot.head.root).ok_or(StoreError::UnknownRoot)?.schema_fork();
                // The pinned latest-confirmed query is valid at any time and preserves canonical
                // finalized fallback when a head changed after the slot's sole observation.
                let (confirmed, committed) = if let Some(confirmation) = &confirmation.state {
                    let prepared = store.prepare_fast_confirmation(&confirmation_checkpoints(
                        confirmation,
                        snapshot,
                    ))?;
                    let confirmed = confirmation.latest_confirmed(&prepared)?;
                    (confirmed, prepared.block_info(confirmed)?.safe_execution_hash)
                } else {
                    let finalized = snapshot.finalized_checkpoint.root;
                    (finalized, store.verified_execution_block_hash(finalized)?)
                };
                let verified = store.verified_execution_block_hash(confirmed)?;
                let safe = authenticated_safe_hash(committed, verified)?;
                let finalized = store
                    .verified_execution_block_hash(snapshot.finalized_checkpoint.root)?
                    .unwrap_or([0; 32]);
                (fork, head, safe, finalized)
            }
            Self::Gloas { store, confirmation } => {
                let Some(head) = (match boundary {
                    Some(boundary) => boundary
                        .verified_execution_block_hash(store, snapshot.head)
                        .map_err(execution_boundary_error)?,
                    None => store.verified_execution_block_hash(snapshot.head)?,
                }) else {
                    return Ok(None)
                };
                let (confirmed, committed) = if let Some(confirmation) = &confirmation.state {
                    let prepared = store.prepare_fast_confirmation(&confirmation_checkpoints(
                        confirmation,
                        snapshot,
                    ))?;
                    let confirmed = confirmation.latest_confirmed(&prepared)?;
                    (confirmed, prepared.block_info(confirmed)?.safe_execution_hash)
                } else {
                    let finalized = snapshot.finalized_checkpoint.root;
                    (
                        finalized,
                        match boundary {
                            Some(boundary) => boundary
                                .finalized_execution_block_hash(store)
                                .map_err(execution_boundary_error)?,
                            None => store.finalized_execution_block_hash()?,
                        },
                    )
                };
                // Historical roots retain their real schema and have no payload branch. Only an
                // actual Gloas root uses EMPTY to authenticate the confirmed parent execution hash.
                let mut confirmed_node = store.node_for_root(confirmed)?;
                if confirmed_node.payload_status.is_some() {
                    confirmed_node.payload_status = Some(PayloadStatus::Empty);
                }
                let verified = match boundary {
                    Some(boundary) => boundary
                        .verified_execution_block_hash(store, confirmed_node)
                        .map_err(execution_boundary_error)?,
                    None => store.verified_execution_block_hash(confirmed_node)?,
                };
                let fork = store
                    .historical_block(snapshot.head.root)
                    .map_or(Fork::Gloas, BeaconBlock::schema_fork);
                (
                    fork,
                    head,
                    authenticated_safe_hash(committed, verified)?,
                    match boundary {
                        Some(boundary) => boundary
                            .finalized_execution_block_hash(store)
                            .map_err(execution_boundary_error)?,
                        None => store.finalized_execution_block_hash()?,
                    }
                    .unwrap_or([0; 32]),
                )
            }
        };
        if head == [0; 32] {
            return Ok(None)
        }
        Ok(Some((
            fork,
            ForkchoiceState {
                head_block_hash: B256::from(head),
                safe_block_hash: B256::from(safe),
                finalized_block_hash: B256::from(finalized),
            },
        )))
    }
}

struct Worker<E, C, G> {
    graph: Graph,
    optimistic: bool,
    last_execution_retry_slot: Option<u64>,
    import_metrics: ImportObservation,
    config: ChainConfig,
    repository: Arc<AdmittedStateRepository>,
    source: Arc<RepositoryStateSource>,
    origin_genesis: Option<Root>,
    gossip_state: gossip::GossipState,
    engine: RuntimeEngineAdapter<E>,
    classic_availability: C,
    gloas_availability: G,
    limits: ImportLimits,
    runtime: Handle,
    last_execution_state: Option<(Fork, ForkchoiceState)>,
    last_verified_forkchoice: Option<VerifiedForkchoice>,
    earliest_available_slot: u64,
    execution_boundary: Option<ExecutionCheckpointBoundary>,
    recovery_execution_boundary: Option<PreparedRecoveryExecutionBoundary>,
    replaying: bool,
    replay: ReplayContext,
    column_retention: Option<Arc<dyn ColumnRetention>>,
    unsigned_custody_context: Option<UnsignedCustodyAnchor>,
    clock: OwnerClock,
}
#[derive(Debug)]
enum OwnerClock {
    System,
    #[cfg(test)]
    Controlled(Arc<std::sync::atomic::AtomicU64>),
}
impl OwnerClock {
    fn now(&self) -> Result<u64, ImportError> {
        match self {
            Self::System => unix_ms(),
            #[cfg(test)]
            Self::Controlled(value) => Ok(value.load(Ordering::Acquire)),
        }
    }
}

struct WorkerSetup<E, C, G> {
    config: ChainConfig,
    anchor: ImportAnchor,
    repository: AdmittedStateRepository,
    engine: RuntimeEngineAdapter<E>,
    classic_availability: C,
    gloas_availability: G,
    limits: ImportLimits,
    runtime: Handle,
    clock: OwnerClock,
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
    #[cfg(test)]
    fn new(
        setup: WorkerSetup<E, C, G>,
        now: Result<u64, ImportError>,
    ) -> Result<Self, ImportError> {
        Self::initialize(setup, now, false, None)
    }

    #[cfg(test)]
    fn initialize(
        setup: WorkerSetup<E, C, G>,
        now: Result<u64, ImportError>,
        replaying: bool,
        receipt: Option<ReconciledCheckpoint>,
    ) -> Result<Self, ImportError> {
        Self::initialize_mode(
            setup,
            now,
            replaying,
            receipt.map_or(StartupExecutionMode::Strict, |receipt| {
                StartupExecutionMode::Reconciled(Box::new(receipt))
            }),
        )
    }

    fn initialize_mode(
        setup: WorkerSetup<E, C, G>,
        now: Result<u64, ImportError>,
        replaying: bool,
        mode: StartupExecutionMode,
    ) -> Result<Self, ImportError> {
        let (receipt, optimistic) = match mode {
            StartupExecutionMode::Strict => (None, false),
            StartupExecutionMode::Reconciled(receipt) => (Some(*receipt), false),
            StartupExecutionMode::Optimistic => (None, true),
        };
        let WorkerSetup {
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
        let verifier =
            SszCheckpointVerifier::new(config.clone(), anchor.decode_limits, anchor.block_format)?;
        if anchor.verified.config_digest() != Some(checked_config_digest(&config)?) {
            return Err(ImportError::Configuration)
        }
        let verified = if let Some(replay) = &anchor.replay {
            if !replay.proof.matches_inputs(
                &anchor.state_ssz,
                &replay.post_block_state_ssz,
                &anchor.block_ssz,
                anchor.block_format,
            ) {
                return Err(ImportError::Configuration)
            }
            verifier
                .verify_checkpoint_replay(
                    &anchor.state_ssz,
                    &replay.post_block_state_ssz,
                    &anchor.block_ssz,
                    anchor.verified.checkpoint(),
                    now,
                    anchor.max_age_epochs,
                    replay.proof.max_replay_slots(),
                )?
                .verified()
                .clone()
        } else {
            verifier.verify_checkpoint(
                &anchor.state_ssz,
                &anchor.block_ssz,
                anchor.verified.checkpoint(),
                now,
                anchor.max_age_epochs,
            )?
        };
        if verified.slot() != anchor.verified.slot() ||
            verified.state_root() != anchor.verified.state_root()
        {
            return Err(ImportError::Configuration)
        }
        let fork = match &anchor.replay {
            Some(replay) => replay.proof.post_block_fork(),
            None => {
                config.fork_at_slot(verified.slot()).map_err(|_| ImportError::Configuration)?.fork
            }
        };
        let preset = preset(&config);
        let graph_state_ssz =
            anchor.replay.as_ref().map_or(&anchor.state_ssz, |replay| &replay.post_block_state_ssz);
        let state =
            BeaconState::from_ssz(graph_state_ssz, fork, preset).map_err(|_| ImportError::Wire)?;
        let block = match anchor.block_format {
            CheckpointBlockFormat::Unsigned => {
                BeaconBlock::from_ssz(&anchor.block_ssz, fork, preset)
                    .map_err(|_| ImportError::Wire)?
            }
            CheckpointBlockFormat::Signed => {
                SignedBeaconBlock::from_ssz(&anchor.block_ssz, fork, preset)
                    .map_err(|_| ImportError::Wire)?
                    .message()
            }
        };
        let parameters =
            RuntimeParameters::from_config(&config).map_err(|_| ImportError::Configuration)?;
        let persisted = if let Some(replay) = &anchor.replay {
            repository.persist_replayed_checkpoint(
                &verifier,
                &replay.proof,
                &anchor.state_ssz,
                &replay.post_block_state_ssz,
                &anchor.block_ssz,
                now,
                anchor.max_age_epochs,
            )
        } else {
            repository.persist_checkpoint(
                &verifier,
                &verified,
                &anchor.state_ssz,
                &anchor.block_ssz,
                now,
                anchor.max_age_epochs,
            )
        };
        persisted.map_err(|error| ImportError::Persistence(error.to_string()))?;
        let repository = Arc::new(repository);
        let source = RepositoryStateSource::open(repository.clone())
            .map_err(|error| ImportError::Persistence(error.to_string()))?;
        let mut graph = if anchor.replay.is_some() {
            Graph::new_at_checkpoint(
                state,
                block,
                verified.checkpoint(),
                &anchor.state_ssz,
                &parameters,
                source.clone(),
                limits.fast_confirmation,
            )?
        } else {
            Graph::new(state, block, &parameters, source.clone(), limits.fast_confirmation)?
        };
        if optimistic {
            if !(Fork::Bellatrix..=Fork::Fulu).contains(&fork) {
                return Err(ImportError::Configuration)
            }
            let Graph::Classic { store, .. } = &mut graph else {
                return Err(ImportError::Configuration)
            };
            store.enable_execution_optimism(Default::default()).map_err(optimistic_error)?;
        }
        if !replaying {
            graph.bootstrap_tick(&config, now, limits.max_tick_slots)?;
        }
        let actual_anchor = graph.snapshot()?;
        if !replaying &&
            let Some(saved) = repository
                .current_snapshot()
                .map_err(|error| ImportError::Persistence(error.to_string()))? &&
            (saved.head() != actual_anchor.head ||
                saved.state_root() !=
                    match &graph {
                        Graph::Classic { store, .. } => store
                            .read_block_state(actual_anchor.head.root)?
                            .ok_or(ImportError::Wire)?
                            .hash_tree_root()
                            .map_err(|_| ImportError::Wire)?,
                        Graph::Gloas { store, .. } => store
                            .read_block_state(actual_anchor.head.root)?
                            .ok_or(ImportError::Wire)?
                            .hash_tree_root()
                            .map_err(|_| ImportError::Wire)?,
                    } ||
                saved.checkpoints() !=
                    (
                        actual_anchor.justified_checkpoint,
                        actual_anchor.finalized_checkpoint,
                    ))
        {
            return Err(ImportError::RecoveryRequired)
        }
        let execution_boundary = if let Some(receipt) = receipt {
            receipt
                .check_binding(&config, &anchor, &engine)
                .map_err(|error| ImportError::ExecutionBoundary(error.to_string()))?;
            match &graph {
                Graph::Gloas { store, .. } => Some(
                    ExecutionCheckpointBoundary::authenticate(
                        receipt, &config, &anchor, now, store, &engine,
                    )
                    .map_err(execution_boundary_error)?,
                ),
                Graph::Classic { .. } => None,
            }
        } else {
            None
        };
        let recovery_execution_boundary = if replaying && execution_boundary.is_none() {
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
            origin_genesis: None,
            gossip_state: gossip::GossipState::new()?,
            engine,
            classic_availability,
            gloas_availability,
            limits,
            runtime,
            optimistic,
            last_execution_retry_slot: None,
            import_metrics: Default::default(),
            last_execution_state: None,
            last_verified_forkchoice: None,
            earliest_available_slot: anchor
                .replay
                .as_ref()
                .map_or_else(|| verified.slot(), |replay| replay.proof.post_block_slot()),
            execution_boundary,
            recovery_execution_boundary,
            replaying,
            replay: ReplayContext::default(),
            column_retention: None,
            unsigned_custody_context: None,
            clock,
        };
        worker.unsigned_custody_context = worker.custody_anchor(verified.checkpoint().root)?;
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
        tracing::info!(target: "beacon::import", slot = worker.graph.current_slot(),
            anchor_epoch = verified.checkpoint().epoch,
            anchor_root = %B256::from(verified.checkpoint().root),
            "[beacon] Started authenticated bounded import owner");
        Ok(worker)
    }

    fn run(&mut self, mut receiver: mpsc::Receiver<Command>) {
        loop {
            let wait = match self.until_next_slot() {
                Ok(wait) => wait,
                Err(error) => {
                    tracing::error!(target: "beacon::import", %error, "[beacon] Halting import clock owner");
                    break
                }
            };
            let command =
                self.runtime.block_on(async { tokio::time::timeout(wait, receiver.recv()).await });
            match command {
                Ok(Some(command)) => {
                    let advance = self.clock.now().and_then(|now| self.advance(now));
                    if let Err(error) = advance {
                        reply_failure(command, error);
                        break
                    }
                    match command {
                        Command::Import { input, received_unix_ms, reply, _bytes, _slot } => {
                            let result = self.import(input, received_unix_ms);
                            let fatal = result.as_ref().is_err_and(ImportError::fatal);
                            let _ = reply.send(result);
                            if fatal {
                                break
                            }
                        }
                        Command::BlockReference { root, reply, _slot } => {
                            if !reply.is_closed() {
                                let result = match &self.graph {
                                    Graph::Gloas { store, .. } => store
                                        .block(root)
                                        .map(|block| {
                                            let preset =
                                                ReadGloasGossipView::store_parameters(store)
                                                    .phase0
                                                    .preset;
                                            EnvelopeBlockReference::from_block(
                                                &BeaconBlock::Gloas(block.clone(), preset),
                                                &self.config,
                                            )
                                            .map_err(|_| ImportError::Wire)
                                        })
                                        .transpose(),
                                    Graph::Classic { .. } => Ok(None),
                                };
                                let _ = reply.send(result);
                            }
                        }
                        Command::CustodyAnchor { root, reply, _slot } => {
                            if !reply.is_closed() {
                                let result = match root {
                                    Some(root) => self.custody_anchor(root),
                                    None => self.retained_custody_anchor(),
                                };
                                let fatal = result.as_ref().is_err_and(ImportError::fatal);
                                let _ = reply.send(result);
                                if fatal {
                                    break
                                }
                            }
                        }
                        Command::CustodyDependency { reply, _slot } => {
                            if !reply.is_closed() {
                                let result = self.custody_dependency();
                                let fatal = result.as_ref().is_err_and(ImportError::fatal);
                                let _ = reply.send(result);
                                if fatal {
                                    break
                                }
                            }
                        }
                        Command::CustodyContext { block, reply, _bytes, _slot } => {
                            if !reply.is_closed() {
                                let result = self.retain_custody_context(block);
                                let fatal = result.as_ref().is_err_and(ImportError::fatal);
                                let _ = reply.send(result);
                                if fatal {
                                    break
                                }
                            }
                        }
                        Command::CustodyColumns { root, originals, reply, _bytes, _slot } => {
                            if !reply.is_closed() {
                                let result = self.retain_custody_columns(root, originals);
                                let fatal = result.as_ref().is_err_and(ImportError::fatal);
                                let _ = reply.send(result);
                                if fatal {
                                    break
                                }
                            }
                        }
                        Command::Serve { request, reply, _slot } => {
                            if !reply.is_closed() {
                                let result = self.serve(request);
                                let fatal = result.as_ref().is_err_and(ImportError::fatal);
                                let _ = reply.send(result);
                                if fatal {
                                    break
                                }
                            }
                        }
                        Command::Observe { request, reply, _slot } => {
                            if !reply.is_closed() {
                                let _ = reply.send(self.observe(request));
                            }
                        }
                        Command::ColumnRetention { publisher, reply, _slot } => {
                            if !reply.is_closed() {
                                let result = if checked_config_digest(&self.config).ok() !=
                                    Some(publisher.config_digest())
                                {
                                    Err(ImportError::Configuration)
                                } else if let Some(existing) = &self.column_retention {
                                    if Arc::ptr_eq(existing, &publisher) {
                                        Ok(())
                                    } else {
                                        Err(ImportError::Configuration)
                                    }
                                } else {
                                    self.column_retention = Some(publisher);
                                    Ok(())
                                };
                                let _ = reply.send(result);
                            }
                        }
                        Command::Gossip {
                            topic,
                            bytes,
                            received_unix_ms,
                            reply,
                            _bytes,
                            _slot,
                        } => {
                            if !reply.is_closed() {
                                let result = self.dispatch_gossip(topic, &bytes, received_unix_ms);
                                let fatal = result.as_ref().is_err_and(ImportError::fatal);
                                let _ = reply.send(result);
                                if fatal {
                                    break
                                }
                            }
                        }
                        Command::ContainsBlock { root, reply, _slot } => {
                            if !reply.is_closed() {
                                let retained = match &self.graph {
                                    Graph::Classic { store, .. } => store.block(root).is_some(),
                                    Graph::Gloas { store, .. } => {
                                        store.block(root).is_some() ||
                                            ReadGloasGossipView::historical_block(store, root)
                                                .is_some()
                                    }
                                };
                                let _ = reply.send(Ok(retained));
                            }
                        }
                        Command::Snapshot { reply, _slot } => {
                            let _ = reply.send(self.graph.snapshot());
                        }
                        Command::Status { reply, _slot } => {
                            let _ = reply.send(self.authenticated_status());
                        }
                    }
                }
                Ok(None) => break,
                Err(_) => {
                    if let Err(error) = self.clock.now().and_then(|now| self.advance(now)) {
                        tracing::error!(target: "beacon::import", %error, "[beacon] Halting import clock owner");
                        break
                    }
                }
            }
        }
        receiver.close();
        while let Ok(command) = receiver.try_recv() {
            reply_failure(command, ImportError::Halted);
        }
    }

    fn authenticated_status(&self) -> Result<AuthenticatedStatus, ImportError> {
        let mut status =
            self.graph.authenticated_status(&self.config, self.earliest_available_slot)?;
        status.origin_genesis = self.origin_genesis;
        if self.replaying {
            status.replay_retrieval_clock(&self.config, self.clock.now()?)?;
        }
        Ok(status)
    }
    fn until_next_slot(&self) -> Result<Duration, ImportError> {
        let now = self.clock.now()?;
        if now < self.graph.time_ms() {
            return Err(ImportError::Clock)
        }
        let next = self.graph.current_slot().checked_add(1).ok_or(ImportError::Clock)?;
        let start = self
            .config
            .clock()
            .map_err(|_| ImportError::Configuration)?
            .start_of_slot(next)
            .map_err(|_| ImportError::Clock)?;
        Ok(Duration::from_millis(start.saturating_sub(now)))
    }

    fn advance(&mut self, now: u64) -> Result<(), ImportError> {
        let mut candidate = self.graph.clone();
        candidate.advance(&self.config, now, self.limits.max_tick_slots)?;
        if candidate.current_slot() != self.graph.current_slot() {
            self.record_clock_event(self.graph.time_ms(), &candidate, false)?;
            self.publish(&mut candidate)?;
        }
        self.graph = candidate;
        Ok(())
    }

    fn import(
        &mut self,
        input: Input,
        received_unix_ms: u64,
    ) -> Result<ImportReceipt, ImportError> {
        let started = Instant::now();
        let input_root = input.root();
        let bytes = input.len();
        let is_block = matches!(input, Input::Block(_) | Input::GossipBlock(_));
        let replaying = self.replaying;
        let mut timings = timing::ImportTimings::new(input_root);
        let result = self.import_attempt(input, received_unix_ms, &mut timings);
        let micros = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
        self.import_metrics.record(is_block, bytes, replaying, micros, &result);
        if result.is_ok() && timings.already_admitted {
            self.import_metrics.duplicate_blocks_total =
                self.import_metrics.duplicate_blocks_total.saturating_add(1);
            tracing::debug!(target: "beacon::import", root = %B256::from(input_root),
                elapsed_ms = micros / 1000, replaying,
                "[beacon] Rechecked already admitted block signature");
        } else if result.is_ok() {
            if is_block {
                self.import_metrics.newly_admitted_blocks_total =
                    self.import_metrics.newly_admitted_blocks_total.saturating_add(1);
                self.import_metrics.new_block_duration_micros_total =
                    self.import_metrics.new_block_duration_micros_total.saturating_add(micros);
                self.import_metrics.last_new_block_duration_micros = micros;
            }
            let native = timings.native.unwrap_or_default();
            let execution = if self.optimistic {
                native.execution_wait_micros
            } else {
                self.engine.execution_micros(input_root)
            };
            self.import_metrics.record_phases(timings.native, execution);
            tracing::info!(target: "beacon::import",
                root = %alloy_primitives::B256::from(input_root),
                kind = if is_block { "block" } else { "envelope" },
                elapsed_ms = micros / 1000, input_bytes = bytes, replaying,
                preparation_ms = timings.preparation_micros / 1000,
                native_import_ms = timings.native_import_micros / 1000,
                body_observations_ms = timings.body_observations_micros / 1000,
                recording_ms = timings.recording_micros / 1000,
                publication_ms = timings.publication_micros / 1000,
                native_phases_measured = timings.native.is_some(),
                availability_ms = native.availability_micros / 1000,
                transition_with_execution_ms = native.state_transition_micros / 1000,
                execution_wait_measured = execution.is_some(),
                execution_wait_ms = execution.unwrap_or_default() / 1000,
                native_fork_choice_ms = native.fork_choice_micros / 1000,
                state_persistence_ms = native.persistence_micros / 1000,
                "[beacon] Completed checked consensus import");
        }
        result
    }

    fn import_attempt(
        &mut self,
        input: Input,
        received_unix_ms: u64,
        timings: &mut timing::ImportTimings,
    ) -> Result<ImportReceipt, ImportError> {
        let preparation = timings.start(timing::ImportPhase::Preparation);
        let input_root = input.root();
        self.engine.begin_import_attempt(input_root).map_err(|_| ImportError::EngineUncertain)?;
        let is_envelope = matches!(&input, Input::Envelope(_) | Input::GossipEnvelope(_));
        let prepared_gossip = match &input {
            Input::GossipBlock(wire) => Some(self.validate_block_gossip(wire, received_unix_ms)?),
            Input::GossipEnvelope(wire) => {
                Some(self.validate_envelope_gossip(wire, received_unix_ms)?)
            }
            Input::Envelope(_) => Some(self.gossip_state.clone()),
            _ => None,
        };
        timings.record(timing::ImportPhase::Preparation, preparation);
        let result = self.import_inner(input, received_unix_ms, timings);
        if result.is_ok() &&
            let Some(prepared) = prepared_gossip
        {
            self.gossip_state = if is_envelope {
                self.record_admitted_envelope(prepared, input_root)
            } else {
                prepared
            };
        }
        if result.is_err() && !result.as_ref().is_err_and(ImportError::fatal) {
            // Staging is not canonical authority. Keep the first consensus/Engine error unless
            // cleanup itself is the only uncertain local publication failure.
            if let Err(error) = self.source.discard_snapshot() {
                return Err(ImportError::Persistence(error.to_string()))
            }
        }
        if result.is_err() && self.engine.is_closed() {
            return Err(ImportError::EngineUncertain)
        }
        if result.is_err() &&
            !result.as_ref().is_err_and(ImportError::fatal) &&
            let Some(pending) = self
                .engine
                .take_pending_import(input_root)
                .map_err(|_| ImportError::EngineUncertain)?
        {
            tracing::debug!(target: "beacon::import", ?input_root, payload_hash = ?pending.hash,
                status = ?pending.status, "[beacon] Deferring exact original input for actual execution catch-up");
            return Err(ImportError::ExecutionPending(input_root))
        }
        result
    }

    fn import_inner(
        &mut self,
        input: Input,
        received_unix_ms: u64,
        timings: &mut timing::ImportTimings,
    ) -> Result<ImportReceipt, ImportError> {
        let preparation = timings.start(timing::ImportPhase::Preparation);
        self.limits.reserve_bytes(input.len())?;
        if received_unix_ms > self.graph.time_ms() {
            return Err(ImportError::Wire)
        }
        tracing::debug!(target: "beacon::import", received_unix_ms,
            actual_unix_ms = self.graph.time_ms(), input_bytes = input.len(),
            "[beacon] Verifying original queued consensus input");
        let begin_ms = self.graph.time_ms();
        let mut candidate = self.graph.clone();
        timings.record(timing::ImportPhase::Preparation, preparation);
        let mut slashings = Vec::new();
        let admitted_root = match &input {
            Input::Block(wire) | Input::GossipBlock(wire) => {
                let preparation = timings.start(timing::ImportPhase::Preparation);
                let signed = decode_block(wire, &self.config)?;
                if signed.heap_bytes().map_err(|_| ImportError::Wire)? > 64 * 1024 * 1024 {
                    return Err(ImportError::Bounds)
                }
                if verify_retained_duplicate(
                    &candidate,
                    wire.root(),
                    &signed,
                    self.config.slots_per_epoch,
                )? {
                    timings.record(timing::ImportPhase::Preparation, preparation);
                    timings.already_admitted = true;
                    return Ok(ImportReceipt {
                        admitted_root: wire.root(),
                        snapshot: candidate.snapshot()?,
                        execution_forkchoice_updated: false,
                    })
                }
                check_replay_bound(&candidate, wire, self.limits.max_tick_slots)?;
                timings.record(timing::ImportPhase::Preparation, preparation);
                let native = timings.start(timing::ImportPhase::NativeImport);
                match (&mut candidate, &signed) {
                    (Graph::Classic { store, .. }, _) if signed.schema_fork() < Fork::Gloas => {
                        if self.optimistic {
                            let (outcome, native_timings) = self
                                .runtime
                                .block_on(store.on_block_optimistic_timed(
                                    &signed,
                                    Arc::new(self.engine.clone()),
                                    &self.classic_availability,
                                ))
                                .map_err(optimistic_error)?;
                            timings.native = Some(native_timings);
                            if outcome == reth_embedded_beacon_fork_choice::store::optimistic::ImportOutcome::ExecutionRejected {
                                return Err(ImportError::EngineUncertain)
                            }
                        } else {
                            timings.native =
                                Some(self.runtime.block_on(store.on_block_with_engine_timed(
                                    &signed,
                                    &self.engine,
                                    &self.classic_availability,
                                ))?);
                        }
                    }
                    (Graph::Gloas { store, .. }, SignedBeaconBlock::Gloas(signed, _)) => {
                        store.on_block(signed)?;
                    }
                    (Graph::Classic { store, .. }, SignedBeaconBlock::Gloas(..)) => {
                        if self.optimistic {
                            let state = self
                                .last_execution_state
                                .ok_or(ImportError::ExecutionPending(wire.root()))?
                                .1;
                            if !self
                                .last_verified_forkchoice
                                .as_ref()
                                .is_some_and(|receipt| receipt.matches(&self.engine, state))
                            {
                                return Err(ImportError::ExecutionPending(wire.root()))
                            }
                            store.finish_execution_optimism().map_err(|error| match error {
                                reth_embedded_beacon_fork_choice::store::optimistic::OptimisticError::ExecutionDeferred => ImportError::ExecutionPending(wire.root()),
                                other => optimistic_error(other),
                            })?;
                        }
                        let migrated = self.runtime.block_on(
                            CompactGloasStore::migrate_from_classic(store, &signed, &self.engine),
                        )?;
                        // The strict migration preserves the actual historical graph/finality;
                        // initialize only optional confirmation variables conservatively.
                        candidate =
                            Graph::from_compact_gloas(migrated, self.limits.fast_confirmation)?;
                    }
                    _ => return Err(ImportError::UnsupportedTransition),
                }
                timings.record(timing::ImportPhase::NativeImport, native);
                let root = wire.root();
                // Normative signed admission is separate from every independently transactional
                // retained FC observation. One ordinary rejected vote cannot discard later items.
                let observations = timings.start(timing::ImportPhase::BodyObservations);
                let verified = candidate.admit_body_observations(root, &signed)?;
                timings.record(timing::ImportPhase::BodyObservations, observations);
                if self.replay.event().is_none() {
                    for (index, context) in verified {
                        slashings.push((
                            signed.schema_fork(),
                            slashings::original(&signed, index)?,
                            context,
                        ));
                    }
                }
                root
            }
            Input::Envelope(wire) | Input::GossipEnvelope(wire) => {
                let Graph::Gloas { store, .. } = &mut candidate else {
                    return Err(ImportError::UnsupportedTransition)
                };
                let preparation = timings.start(timing::ImportPhase::Preparation);
                let signed = decode_envelope(wire, store, &self.config)?;
                if signed.heap_bytes().map_err(|_| ImportError::Wire)? > 64 * 1024 * 1024 {
                    return Err(ImportError::Bounds)
                }
                timings.record(timing::ImportPhase::Preparation, preparation);
                let native = timings.start(timing::ImportPhase::NativeImport);
                self.runtime
                    .block_on(store.on_execution_payload_envelope(
                        &signed,
                        &self.engine,
                        &self.gloas_availability,
                    ))
                    .map_err(|error| match error {
                        EnvelopeError::Store(error @ StoreError::CompactState(_)) => error.into(),
                        other => ImportError::Envelope(other.to_string()),
                    })?;
                timings.record(timing::ImportPhase::NativeImport, native);
                wire.beacon_block_root()
            }
        };
        let recording = timings.start(timing::ImportPhase::Recording);
        // Engine/DA validation may finish in a later slot. Tick the actual admitted candidate
        // to callback completion before FCU; never backdate optional confirmation observations.
        if let Some(event) = self.replay.event() {
            if event.historical {
                candidate.bootstrap_tick(
                    &self.config,
                    event.completed_ms,
                    self.limits.max_tick_slots,
                )?;
            } else {
                candidate.advance(&self.config, event.completed_ms, self.limits.max_tick_slots)?;
            }
        } else if !self.replaying {
            candidate.advance(&self.config, self.clock.now()?, self.limits.max_tick_slots)?;
        }
        self.record_import_event(&input, received_unix_ms, begin_ms, &candidate)?;
        for (fork, bytes, context) in slashings {
            self.record_signed_slashing(fork, &bytes, context, received_unix_ms, &candidate)?;
        }
        timings.record(timing::ImportPhase::Recording, recording);
        let publication = timings.start(timing::ImportPhase::Publication);
        let (execution_forkchoice_updated, snapshot) =
            self.publish_with_snapshot(&mut candidate)?;
        self.graph = candidate;
        timings.record(timing::ImportPhase::Publication, publication);
        if snapshot.head.root != admitted_root &&
            let Graph::Classic { store, .. } = &self.graph &&
            let Ok(Some(source)) = store.block_voting_source(admitted_root)
        {
            tracing::debug!(target: "beacon::import", admitted_root = %B256::from(admitted_root),
                head_root = %B256::from(snapshot.head.root),
                current_epoch = source.current_epoch, block_epoch = source.block_epoch,
                realized_justified_epoch = source.realized_justified_epoch,
                unrealized_justified_epoch = source.unrealized_justified_epoch,
                voting_source_epoch = source.voting_source_epoch,
                required_justified_epoch = source.required_justified_epoch,
                viable = source.viable,
                "[beacon] Selected head differs from admitted block under native fork choice");
        }
        tracing::info!(target: "beacon::import", admitted_root = %B256::from(admitted_root),
            head_root = %B256::from(snapshot.head.root),
            head_payload_status = ?snapshot.head.payload_status, slot = snapshot.head_slot,
            finalized_epoch = snapshot.finalized_checkpoint.epoch,
            execution_forkchoice_updated, "[beacon] Published strict admitted consensus event");
        Ok(ImportReceipt { admitted_root, snapshot, execution_forkchoice_updated })
    }
}

fn execution_boundary_error(error: ExecutionBoundaryError) -> ImportError {
    match error {
        ExecutionBoundaryError::Store(error) => error.into(),
        other => ImportError::ExecutionBoundary(other.to_string()),
    }
}
const fn body_observation_counts(signed: &SignedBeaconBlock) -> (usize, usize) {
    macro_rules! counts {
        ($value:expr) => {
            (
                $value.message.body.attestations.values.len(),
                $value.message.body.attester_slashings.values.len(),
            )
        };
    }
    match signed {
        SignedBeaconBlock::Phase0(value, _) => counts!(value),
        SignedBeaconBlock::Altair(value, _) => counts!(value),
        SignedBeaconBlock::Bellatrix(value, _) => counts!(value),
        SignedBeaconBlock::Capella(value, _) => counts!(value),
        SignedBeaconBlock::Deneb(value, _) => counts!(value),
        SignedBeaconBlock::Electra(value, _) => counts!(value),
        SignedBeaconBlock::Fulu(value, _) => counts!(value),
        SignedBeaconBlock::Gloas(value, _) => counts!(value),
    }
}

fn retained_observation_result(
    root: Root,
    index: usize,
    kind: &'static str,
    result: Result<(), StoreError>,
) -> Result<(), ImportError> {
    match result {
        Ok(()) => Ok(()),
        Err(
            error @ (StoreError::UnknownRoot |
            StoreError::InvalidAttestationTime |
            StoreError::InvalidAttestationTarget |
            StoreError::InvalidSlashing |
            StoreError::Processing(ProcessingError::Crypto(_)) |
            StoreError::Kernel(
                ForkChoiceError::UnknownRoot |
                ForkChoiceError::InvalidAttestationTime |
                ForkChoiceError::InvalidAttestationTarget |
                ForkChoiceError::InvalidValidatorIndices,
            )),
        ) => {
            tracing::debug!(target: "beacon::import", ?root, index, kind, %error,
                "[beacon] Rejected independently checked retained fork-choice observation");
            Ok(())
        }
        Err(error) => Err(ImportError::ObservationInfrastructure(error.to_string())),
    }
}

// A sealed graph commitment alone cannot grant local execution-validation authority.
fn authenticated_safe_hash(
    committed: Option<Root>,
    verified: Option<Root>,
) -> Result<Root, ImportError> {
    match (committed, verified) {
        (Some(commitment), Some(actual)) if commitment == actual => Ok(actual),
        (commitment, None) if commitment.is_none_or(|hash| hash == [0; 32]) => Ok([0; 32]),
        _ => Err(StoreError::InvalidParameters.into()),
    }
}

enum RetainedValidationState {
    Classic(StateRead),
    Gloas(GloasStateRead),
}
fn confirmation_checkpoints<V: FastConfirmationView>(
    confirmation: &FastConfirmation<V>,
    snapshot: StoreSnapshot,
) -> Vec<reth_embedded_beacon_primitives::Checkpoint> {
    let observed = confirmation.snapshot();
    [
        snapshot.justified_checkpoint,
        snapshot.finalized_checkpoint,
        observed.previous_epoch_observed_justified_checkpoint,
        observed.current_epoch_observed_justified_checkpoint,
    ]
    .into_iter()
    .map(|checkpoint| (checkpoint.epoch, checkpoint.root))
    .collect::<BTreeSet<_>>()
    .into_iter()
    .map(|(epoch, root)| reth_embedded_beacon_primitives::Checkpoint { epoch, root })
    .collect()
}
fn verify_retained_duplicate(
    graph: &Graph,
    root: Root,
    signed: &SignedBeaconBlock,
    slots_per_epoch: u64,
) -> Result<bool, ImportError> {
    let owned = match graph {
        Graph::Classic { store, .. } => match store.read_block_state(root)? {
            Some(state) => RetainedValidationState::Classic(state),
            None => return Ok(false),
        },
        Graph::Gloas { store, .. } => {
            if let Some(state) = store.read_block_state(root)? {
                RetainedValidationState::Gloas(state)
            } else if let Some(state) = store.read_historical_block_state(root)? {
                RetainedValidationState::Classic(state)
            } else {
                return Ok(false)
            }
        }
    };
    let (validators, fork, genesis_root) = match &owned {
        RetainedValidationState::Classic(state) => (
            StateRef::Classic(state).validators(),
            state.fork_info(),
            state.genesis_validators_root(),
        ),
        RetainedValidationState::Gloas(state) => {
            (state.validators.values.as_slice(), state.fork.clone(), state.genesis_validators_root)
        }
    };
    let header = signed.header().map_err(|_| ImportError::Wire)?;
    let validator = usize::try_from(header.proposer_index)
        .ok()
        .and_then(|index| validators.get(index))
        .ok_or(ImportError::Wire)?;
    let version = if header.slot / slots_per_epoch < fork.epoch {
        fork.previous_version
    } else {
        fork.current_version
    };
    let signature = match signed {
        SignedBeaconBlock::Phase0(value, _) => &value.signature,
        SignedBeaconBlock::Altair(value, _) => &value.signature,
        SignedBeaconBlock::Bellatrix(value, _) => &value.signature,
        SignedBeaconBlock::Capella(value, _) => &value.signature,
        SignedBeaconBlock::Deneb(value, _) => &value.signature,
        SignedBeaconBlock::Electra(value, _) => &value.signature,
        SignedBeaconBlock::Fulu(value, _) => &value.signature,
        SignedBeaconBlock::Gloas(value, _) => &value.signature,
    };
    verify_signature(
        &validator.pubkey,
        &compute_signing_root(root, compute_domain([0; 4], version, genesis_root)),
        signature,
    )
    .map_err(|_| ImportError::Wire)?;
    Ok(true)
}

const fn block_slot(block: &BeaconBlock) -> u64 {
    match block {
        BeaconBlock::Phase0(value, _) => value.slot,
        BeaconBlock::Altair(value, _) => value.slot,
        BeaconBlock::Bellatrix(value, _) => value.slot,
        BeaconBlock::Capella(value, _) => value.slot,
        BeaconBlock::Deneb(value, _) => value.slot,
        BeaconBlock::Electra(value, _) => value.slot,
        BeaconBlock::Fulu(value, _) => value.slot,
        BeaconBlock::Gloas(value, _) => value.slot,
    }
}
fn check_replay_bound(graph: &Graph, wire: &WireBlock, maximum: u64) -> Result<(), ImportError> {
    let parent_slot = match graph {
        Graph::Classic { store, .. } => store.block(wire.parent_root()).map(block_slot),
        Graph::Gloas { store, .. } => store
            .block(wire.parent_root())
            .map(|block| block.slot)
            .or_else(|| store.historical_block(wire.parent_root()).map(block_slot)),
    }
    .ok_or(StoreError::UnknownRoot)?;
    if wire.slot().checked_sub(parent_slot).ok_or(ImportError::Wire)? > maximum {
        return Err(ImportError::Bounds)
    }
    Ok(())
}

fn decode_block(wire: &WireBlock, config: &ChainConfig) -> Result<SignedBeaconBlock, ImportError> {
    let fork = config.fork_at_slot(wire.slot()).map_err(|_| ImportError::Configuration)?.fork;
    let context = config
        .fork_digest(wire.slot() / config.slots_per_epoch)
        .map_err(|_| ImportError::Configuration)?;
    let signed = SignedBeaconBlock::from_ssz(wire.ssz(), fork, preset(config))
        .map_err(|_| ImportError::Wire)?;
    let header = signed.header().map_err(|_| ImportError::Wire)?;
    if wire.fork() != fork ||
        wire.context() != context ||
        header.slot != wire.slot() ||
        header.parent_root != wire.parent_root() ||
        signed.message_root().map_err(|_| ImportError::Wire)? != wire.root()
    {
        return Err(ImportError::Wire)
    }
    Ok(signed)
}
fn decode_envelope(
    wire: &WireEnvelope,
    store: &CompactGloasStore,
    config: &ChainConfig,
) -> Result<gloas::SignedExecutionPayloadEnvelope, ImportError> {
    let block = store.block(wire.beacon_block_root()).ok_or(StoreError::UnknownRoot)?;
    let context = config
        .fork_digest(block.slot / config.slots_per_epoch)
        .map_err(|_| ImportError::Configuration)?;
    let signed = gloas::SignedExecutionPayloadEnvelope::from_ssz(wire.ssz(), preset(config))
        .map_err(|_| ImportError::Wire)?;
    if config.fork_at_slot(block.slot).map_err(|_| ImportError::Configuration)?.fork != Fork::Gloas ||
        wire.context() != context ||
        signed.message.beacon_block_root != wire.beacon_block_root() ||
        signed.message.parent_beacon_block_root != wire.parent_beacon_block_root() ||
        signed.message.parent_beacon_block_root != block.parent_root ||
        signed.message.payload.slot_number != block.slot
    {
        return Err(ImportError::Wire)
    }
    Ok(signed)
}
const fn preset(config: &ChainConfig) -> Preset {
    match config.preset {
        ConfigPreset::Mainnet => Preset::Mainnet,
        ConfigPreset::Minimal => Preset::Minimal,
    }
}
// Original reply/no-attribute contracts were audited at execution-apis commit
// 34151926fbf395e66797add1f60fd759681678e6. Gloas adds optional custody via V4.
// V5 belongs to the later extended-response contract and is not selected here.
const fn forkchoice_version(fork: Fork) -> Option<ForkchoiceVersion> {
    match fork {
        Fork::Phase0 | Fork::Altair => None,
        Fork::Bellatrix => Some(ForkchoiceVersion::V1),
        Fork::Capella => Some(ForkchoiceVersion::V2),
        Fork::Deneb | Fork::Electra | Fork::Fulu => Some(ForkchoiceVersion::V3),
        Fork::Gloas => Some(ForkchoiceVersion::V4),
    }
}
fn unix_ms() -> Result<u64, ImportError> {
    u64::try_from(
        SystemTime::now().duration_since(UNIX_EPOCH).map_err(|_| ImportError::Clock)?.as_millis(),
    )
    .map_err(|_| ImportError::Clock)
}
fn reply_failure(command: Command, error: ImportError) {
    match command {
        Command::Import { reply, .. } => {
            let _ = reply.send(Err(error));
        }
        Command::BlockReference { reply, .. } => {
            let _ = reply.send(Err(error));
        }
        Command::CustodyAnchor { reply, .. } => {
            let _ = reply.send(Err(error));
        }
        Command::CustodyDependency { reply, .. } => {
            let _ = reply.send(Err(error));
        }
        Command::CustodyContext { reply, .. } |
        Command::CustodyColumns { reply, .. } |
        Command::ColumnRetention { reply, .. } => {
            let _ = reply.send(Err(error));
        }
        Command::Serve { reply, .. } => {
            let _ = reply.send(Err(error));
        }
        Command::Observe { reply, .. } => {
            let _ = reply.send(Err(error));
        }
        Command::Gossip { reply, .. } => {
            let _ = reply.send(Err(error));
        }
        Command::ContainsBlock { reply, .. } => {
            let _ = reply.send(Err(error));
        }
        Command::Snapshot { reply, _slot } => {
            let _ = reply.send(Err(error));
        }
        Command::Status { reply, _slot } => {
            let _ = reply.send(Err(error));
        }
    }
}

mod archive;
mod checkpoint;
mod confirmation;
mod custody_anchor;
mod custody_history;
mod events;
mod genesis;
mod gossip;
mod maintenance;
mod metrics;
mod mode;
mod observation;
mod optimistic;
mod publication;
mod recovery;
mod serving;
mod slashings;
mod startup;
mod status;
mod timing;

pub use custody_anchor::UnsignedCustodyAnchor;
pub(crate) use custody_history::{HistoricalContextReceipt, HistoricalDependency};
pub(crate) use gossip::{ColumnRetention, ValidatedColumn};

pub use genesis::GenesisImportAnchor;
pub use metrics::{ImportObservation, ImportQueueObservation};
pub use observation::{
    ObjectObservation, Observation, ObservationId, ObservationRequest, OwnerObservation,
    MAX_OBSERVATION_BYTES,
};
pub use recovery::RecoveryLimits;
pub use serving::{ServingBatch, ServingRequest};
pub use startup::ImportStartup;
pub use status::AuthenticatedStatus;

#[cfg(test)]
pub(crate) use genesis::tests::{
    assert_genesis_custody_after_cold_reopen, checkpoint_status_for_tests, genesis_status_for_tests,
};

#[cfg(test)]
mod tests;
