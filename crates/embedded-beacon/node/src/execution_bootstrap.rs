//! Authenticated checkpoint execution download and reconciliation, separate from beacon admission.
//!
//! Preparation is synchronous SSZ/WS work and must execute on a dedicated owner/blocking thread.
//! One mutable owner has no internal input queue. SYNCING permits only bounded download-target
//! work; only an exact original VALID reply reconciles that target. No result publishes a beacon
//! canonical pointer, invents terminal proof/data availability, or changes runtime readiness.

use crate::{
    bootstrap::checked_config_digest, engine_verification::EngineVerificationError,
    import_coordinator::ImportAnchor, runtime_engine::RuntimeEngineAdapter,
    weak_subjectivity::compute_weak_subjectivity_period, BootstrapError, CheckpointBlockFormat,
    SszCheckpointVerifier, VerifiedCheckpoint,
};
use alloy_primitives::B256;
use alloy_rpc_types_engine::{ExecutionData, ForkchoiceState, PayloadStatus};
use reth_embedded_beacon_config::{ChainConfig, Preset as ConfigPreset};
use reth_embedded_beacon_execution_proxy::{
    ExecutionEngine, ForkchoiceReply, ForkchoiceVersion, PayloadReply, ProxyError,
};
use reth_embedded_beacon_primitives::{sha256, Fork, Root};
use reth_embedded_beacon_state_transition::ssz::{
    bellatrix, capella, deneb, electra, fulu, BeaconBlock, BeaconState, Preset, SignedBeaconBlock,
    Ssz,
};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use thiserror::Error;
use tokio::{sync::watch, time::Instant};

/// An exclusively driven, bounded checkpoint execution reconciliation owner.
#[derive(Debug)]
pub struct ExecutionBootstrap {
    checkpoint: VerifiedCheckpoint,
    config_digest: Root,
    original_state_digest: Root,
    original_post_state_digest: Option<Root>,
    original_block_digest: Root,
    block_format: CheckpointBlockFormat,
    target: Option<DownloadTarget>,
    limits: BootstrapLimits,
    started: Instant,
    now_unix_ms: u64,
    expires_unix_ms: u64,
    attempts: u16,
    completed: bool,
    stopped: bool,
    uncertain: Arc<AtomicBool>,
    cancelled: watch::Receiver<bool>,
}

impl ExecutionBootstrap {
    /// Reauthenticate the exact original checkpoint on a dedicated owner/blocking thread.
    ///
    /// The prior concrete verifier binding, full roots, actual original schemas, checked
    /// configuration and state-dependent weak-subjectivity age are independently rechecked.
    /// This method creates no background task or unbounded blocking-worker queue.
    pub fn authenticate(
        config: ChainConfig,
        anchor: &ImportAnchor,
        now_unix_ms: u64,
        limits: BootstrapLimits,
    ) -> Result<(Self, BootstrapCancellation), BootstrapPrepareError> {
        limits.validate()?;
        let started = Instant::now();
        let digest = checked_config_digest(&config)?;
        if anchor.verified.config_digest() != Some(digest) {
            return Err(BootstrapPrepareError::Binding)
        }
        let verifier =
            SszCheckpointVerifier::new(config.clone(), anchor.decode_limits, anchor.block_format)?;
        let verified = if let Some(replay) = &anchor.replay {
            if !replay.proof.matches_inputs(
                &anchor.state_ssz,
                &replay.post_block_state_ssz,
                &anchor.block_ssz,
                anchor.block_format,
            ) || replay.proof.verified().checkpoint() != anchor.verified.checkpoint() ||
                replay.proof.verified().state_root() != anchor.verified.state_root() ||
                replay.proof.verified().slot() != anchor.verified.slot() ||
                replay.proof.verified().config_digest() != Some(digest)
            {
                return Err(BootstrapPrepareError::Binding)
            }
            let fresh = verifier.verify_checkpoint_replay(
                &anchor.state_ssz,
                &replay.post_block_state_ssz,
                &anchor.block_ssz,
                anchor.verified.checkpoint(),
                now_unix_ms,
                anchor.max_age_epochs,
                replay.proof.max_replay_slots(),
            )?;
            if fresh.post_block_slot() != replay.proof.post_block_slot() ||
                fresh.post_block_fork() != replay.proof.post_block_fork()
            {
                return Err(BootstrapPrepareError::Binding)
            }
            fresh.verified().clone()
        } else {
            verifier.verify_checkpoint(
                &anchor.state_ssz,
                &anchor.block_ssz,
                anchor.verified.checkpoint(),
                now_unix_ms,
                anchor.max_age_epochs,
            )?
        };
        if verified.slot() != anchor.verified.slot() ||
            verified.state_root() != anchor.verified.state_root() ||
            verified.config_digest() != Some(digest)
        {
            return Err(BootstrapPrepareError::Binding)
        }
        let preset = match config.preset {
            ConfigPreset::Mainnet => Preset::Mainnet,
            ConfigPreset::Minimal => Preset::Minimal,
        };
        let fork =
            config.fork_at_slot(verified.slot()).map_err(|_| BootstrapPrepareError::Binding)?.fork;
        let state = BeaconState::from_ssz(&anchor.state_ssz, fork, preset)
            .map_err(|error| BootstrapPrepareError::Schema(error.to_string()))?;
        let post_state = anchor
            .replay
            .as_ref()
            .map(|replay| {
                BeaconState::from_ssz(
                    &replay.post_block_state_ssz,
                    replay.proof.post_block_fork(),
                    preset,
                )
                .map_err(|error| BootstrapPrepareError::Schema(error.to_string()))
            })
            .transpose()?;
        let block_fork =
            anchor.replay.as_ref().map_or(fork, |replay| replay.proof.post_block_fork());
        let block = match anchor.block_format {
            CheckpointBlockFormat::Unsigned => {
                BeaconBlock::from_ssz(&anchor.block_ssz, block_fork, preset)
                    .map_err(|error| BootstrapPrepareError::Schema(error.to_string()))?
            }
            CheckpointBlockFormat::Signed => {
                SignedBeaconBlock::from_ssz(&anchor.block_ssz, block_fork, preset)
                    .map_err(|error| BootstrapPrepareError::Schema(error.to_string()))?
                    .message()
            }
        };
        if state
            .hash_tree_root()
            .map_err(|error| BootstrapPrepareError::Schema(error.to_string()))? !=
            verified.state_root() ||
            block
                .hash_tree_root()
                .map_err(|error| BootstrapPrepareError::Schema(error.to_string()))? !=
                verified.checkpoint().root
        {
            return Err(BootstrapPrepareError::Binding)
        }
        let hash = download_hash(post_state.as_ref().unwrap_or(&state), &block, preset)?;
        let target = hash.map(|hash| DownloadTarget { hash });
        let period = compute_weak_subjectivity_period(&state, &config)?;
        let last_age = anchor.max_age_epochs.min(period.epochs);
        let expires_epoch = verified
            .checkpoint()
            .epoch
            .checked_add(last_age)
            .and_then(|epoch| epoch.checked_add(1))
            .ok_or(BootstrapPrepareError::Binding)?;
        let expires_slot = expires_epoch
            .checked_mul(config.slots_per_epoch)
            .ok_or(BootstrapPrepareError::Binding)?;
        let expires_unix_ms = config
            .clock()
            .map_err(|_| BootstrapPrepareError::Binding)?
            .start_of_slot(expires_slot)
            .map_err(|_| BootstrapPrepareError::Binding)?;
        if started.elapsed() >= limits.total_timeout {
            return Err(BootstrapPrepareError::Deadline)
        }
        let (cancel, cancelled) = watch::channel(false);
        tracing::info!(target: "beacon::execution_bootstrap", slot = verified.slot(), fork = ?fork, has_execution_target = target.is_some(), "[beacon] Authenticated checkpoint execution download target");
        Ok((
            Self {
                checkpoint: verified,
                config_digest: digest,
                original_state_digest: sha256(&anchor.state_ssz),
                original_post_state_digest: anchor
                    .replay
                    .as_ref()
                    .map(|replay| sha256(&replay.post_block_state_ssz)),
                original_block_digest: sha256(&anchor.block_ssz),
                block_format: anchor.block_format,
                target,
                limits,
                started,
                now_unix_ms,
                expires_unix_ms,
                attempts: 0,
                completed: false,
                stopped: false,
                uncertain: Arc::new(AtomicBool::new(false)),
                cancelled,
            },
            BootstrapCancellation { cancel },
        ))
    }

    /// Download/reconcile only the privately authenticated checkpoint execution target.
    ///
    /// A future dropped during an attempted FCU closes actual proxy admission and requires
    /// reconciliation. There is no resume/reset override after an uncertain or terminal failure.
    /// The normal import coordinator must independently reauthenticate and apply its actual
    /// graph-derived head/safe/finalized fork choice after this phase.
    pub async fn reconcile<E>(
        &mut self,
        engine: &RuntimeEngineAdapter<E>,
    ) -> Result<ReconciledCheckpoint, BootstrapRunError<E::Error>>
    where
        E: ExecutionEngine<
                Payload = ExecutionData,
                PayloadReply = PayloadReply,
                Forkchoice = ForkchoiceState,
                ForkchoiceReply = ForkchoiceReply,
            > + Clone,
        E::Error: std::error::Error + Send + Sync + 'static,
    {
        if self.requires_reconciliation() || engine.is_closed() {
            return Err(BootstrapRunError::ReconciliationRequired)
        }
        if self.completed || self.stopped {
            return Err(BootstrapRunError::AlreadyFinished)
        }
        if checked_config_digest(engine.config()).map_err(|_| BootstrapRunError::Binding)? !=
            self.config_digest
        {
            self.stopped = true;
            return Err(BootstrapRunError::Binding)
        }
        let deadline = self.started + self.limits.total_timeout;
        let mut backoff = self.limits.initial_backoff;
        loop {
            if *self.cancelled.borrow() {
                self.stopped = true;
                return Err(BootstrapRunError::Cancelled)
            }
            if Instant::now() >= deadline || self.expired() {
                self.stopped = true;
                return Err(BootstrapRunError::Deadline)
            }
            let Some(target) = self.target else {
                self.completed = true;
                return Ok(self.receipt(engine.bootstrap_binding()))
            };
            if self.attempts >= self.limits.max_attempts {
                self.stopped = true;
                return Err(BootstrapRunError::Attempts)
            }
            self.attempts += 1;
            let now = self.current_unix_ms().ok_or(BootstrapRunError::Binding)?;
            let current_slot = engine
                .config()
                .clock()
                .map_err(|_| BootstrapRunError::Binding)?
                .slot_at(now)
                .ok_or(BootstrapRunError::Binding)?;
            let active = engine
                .config()
                .fork_at_slot(current_slot)
                .map_err(|_| BootstrapRunError::Binding)?
                .fork;
            let version = method(active);
            // With no payload attributes, the Engine API version follows the active clock
            // fork, independently of this authenticated target's historical payload schema.
            // Safe/finalized remain absent during download-target work. This owner supplies
            // no graph finality or proposer attributes to the execution transport.
            let state = ForkchoiceState {
                head_block_hash: target.hash.into(),
                safe_block_hash: B256::ZERO,
                finalized_block_hash: B256::ZERO,
            };
            let mut guard = AttemptGuard {
                uncertain: self.uncertain.clone(),
                close: || engine.close_bootstrap_admission(),
                armed: true,
            };
            let response = tokio::select! {
                biased;
                _ = wait_for_cancellation(&mut self.cancelled) => {
                    self.stopped = true;
                    return Err(BootstrapRunError::Cancelled)
                }
                _ = tokio::time::sleep_until(deadline) => {
                    self.stopped = true;
                    return Err(BootstrapRunError::Deadline)
                }
                response = engine.bootstrap_forkchoice_updated(version, state) => response,
            };
            // A received transport result is known; preserve the actual proxy's health.
            // Backend errors still leave this owner halted for independent reconciliation.
            guard.armed = false;
            let reply = match response {
                Ok(reply) => reply,
                Err(error) => {
                    if !matches!(
                        &error,
                        EngineVerificationError::Proxy(
                            ProxyError::QueueFull | ProxyError::CustodyVersion
                        ) | EngineVerificationError::Configuration
                    ) {
                        self.uncertain.store(true, Ordering::Release);
                    }
                    self.stopped = true;
                    return Err(BootstrapRunError::Engine(error))
                }
            };
            let ForkchoiceReply::V1(reply) = reply else {
                self.uncertain.store(true, Ordering::Release);
                engine.close_bootstrap_admission();
                self.stopped = true;
                return Err(BootstrapRunError::Malformed)
            };
            if reply.payload_id.is_some() {
                self.uncertain.store(true, Ordering::Release);
                engine.close_bootstrap_admission();
                self.stopped = true;
                return Err(BootstrapRunError::Malformed)
            }
            match reply.payload_status.status {
                alloy_rpc_types_engine::PayloadStatusEnum::Valid => {
                    if reply.payload_status.latest_valid_hash != Some(state.head_block_hash) {
                        self.uncertain.store(true, Ordering::Release);
                        engine.close_bootstrap_admission();
                        self.stopped = true;
                        return Err(BootstrapRunError::Malformed)
                    }
                    if self.expired() || Instant::now() >= deadline {
                        self.stopped = true;
                        return Err(BootstrapRunError::Deadline)
                    }
                    self.completed = true;
                    tracing::info!(target: "beacon::execution_bootstrap", slot = self.checkpoint.slot(), attempts = self.attempts, "[beacon] Execution checkpoint target reconciled by exact VALID acknowledgement");
                    return Ok(self.receipt(engine.bootstrap_binding()))
                }
                alloy_rpc_types_engine::PayloadStatusEnum::Syncing => {
                    if reply.payload_status.latest_valid_hash.is_some() {
                        self.uncertain.store(true, Ordering::Release);
                        engine.close_bootstrap_admission();
                        self.stopped = true;
                        return Err(BootstrapRunError::Malformed)
                    }
                    tracing::debug!(target: "beacon::execution_bootstrap", attempts = self.attempts, backoff_ms = backoff.as_millis(), "[beacon] Execution target still downloading without canonical authority");
                }
                _ => {
                    self.stopped = true;
                    return Err(BootstrapRunError::Rejected(reply.payload_status))
                }
            }
            tokio::select! {
                biased;
                _ = wait_for_cancellation(&mut self.cancelled) => {
                    self.stopped = true;
                    return Err(BootstrapRunError::Cancelled)
                }
                _ = tokio::time::sleep_until(deadline) => {
                    self.stopped = true;
                    return Err(BootstrapRunError::Deadline)
                }
                _ = tokio::time::sleep(backoff) => {}
            }
            backoff = backoff.saturating_mul(2).min(self.limits.max_backoff);
        }
    }

    /// An uncertain attempt can only be resolved by independent authenticated reconciliation.
    pub fn requires_reconciliation(&self) -> bool {
        self.uncertain.load(Ordering::Acquire)
    }

    fn current_unix_ms(&self) -> Option<u64> {
        u64::try_from(self.started.elapsed().as_millis())
            .ok()
            .and_then(|elapsed| self.now_unix_ms.checked_add(elapsed))
    }

    fn expired(&self) -> bool {
        self.current_unix_ms().is_none_or(|now| now >= self.expires_unix_ms)
    }

    fn receipt(&self, engine_binding: Arc<ChainConfig>) -> ReconciledCheckpoint {
        ReconciledCheckpoint {
            checkpoint: self.checkpoint.clone(),
            execution_hash: self.target.map(|target| target.hash),
            config_digest: self.config_digest,
            original_state_digest: self.original_state_digest,
            original_post_state_digest: self.original_post_state_digest,
            original_block_digest: self.original_block_digest,
            block_format: self.block_format,
            engine_binding,
            attempts: self.attempts,
        }
    }
}

/// Bounded retry work; there is no internal request queue or uncounted waiter.
#[derive(Clone, Copy, Debug)]
pub struct BootstrapLimits {
    /// Maximum actual FCU attempts, including immediately rejected proxy admission.
    pub max_attempts: u16,
    /// Total monotonic preparation/download deadline.
    pub total_timeout: Duration,
    /// Positive first SYNCING backoff.
    pub initial_backoff: Duration,
    /// Maximum exponentially increasing backoff.
    pub max_backoff: Duration,
}
impl Default for BootstrapLimits {
    fn default() -> Self {
        Self {
            max_attempts: 128,
            total_timeout: Duration::from_secs(30 * 60),
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(15),
        }
    }
}
impl BootstrapLimits {
    fn validate(self) -> Result<(), BootstrapPrepareError> {
        if self.max_attempts == 0 ||
            self.max_attempts > 256 ||
            self.total_timeout.is_zero() ||
            self.total_timeout > Duration::from_secs(60 * 60) ||
            self.initial_backoff.is_zero() ||
            self.initial_backoff > self.max_backoff ||
            self.max_backoff > Duration::from_secs(60)
        {
            return Err(BootstrapPrepareError::Bounds)
        }
        Ok(())
    }
}

/// Cancellation does not reopen execution admission after an attempted operation.
#[derive(Clone, Debug)]
pub struct BootstrapCancellation {
    cancel: watch::Sender<bool>,
}
impl BootstrapCancellation {
    /// Request cooperative cancellation; an active FCU becomes an uncertain outcome.
    pub fn cancel(&self) {
        self.cancel.send_replace(true);
    }
}

/// Target reconciliation evidence only; it is neither a canonical graph nor live-readiness proof.
#[derive(Debug)]
pub struct ReconciledCheckpoint {
    checkpoint: VerifiedCheckpoint,
    execution_hash: Option<Root>,
    config_digest: Root,
    original_state_digest: Root,
    original_post_state_digest: Option<Root>,
    original_block_digest: Root,
    block_format: CheckpointBlockFormat,
    engine_binding: Arc<ChainConfig>,
    attempts: u16,
}
impl ReconciledCheckpoint {
    /// Exact independently reauthenticated consensus checkpoint.
    pub const fn checkpoint(&self) -> &VerifiedCheckpoint {
        &self.checkpoint
    }
    /// Actual execution target acknowledged VALID, or genuine pre-execution absence.
    pub const fn execution_hash(&self) -> Option<Root> {
        self.execution_hash
    }
    /// Exact network configuration bound to authentication and actual Engine adapter.
    pub const fn config_digest(&self) -> Root {
        self.config_digest
    }
    /// Number of actual attempted FCU calls; pre-execution absence needs none.
    pub const fn attempts(&self) -> u16 {
        self.attempts
    }

    /// Check this sealed receipt against exact original inputs and the same captured adapter.
    ///
    /// Hashing the bounded original encodings must occur on the coordinator's owner thread.
    /// This is proof binding only; the coordinator independently reauthenticates current
    /// checkpoint roots/WS and derives its actual graph before canonical publication.
    pub(crate) fn check_binding<E>(
        &self,
        config: &ChainConfig,
        anchor: &ImportAnchor,
        engine: &RuntimeEngineAdapter<E>,
    ) -> Result<(), BootstrapProofError>
    where
        E: ExecutionEngine<
                Payload = ExecutionData,
                PayloadReply = PayloadReply,
                Forkchoice = ForkchoiceState,
                ForkchoiceReply = ForkchoiceReply,
            > + Clone,
        E::Error: std::error::Error + Send + Sync + 'static,
    {
        if engine.is_closed() {
            return Err(BootstrapProofError::ReconciliationRequired)
        }
        if checked_config_digest(config).map_err(|_| BootstrapProofError::Binding)? !=
            self.config_digest ||
            anchor.verified.config_digest() != Some(self.config_digest) ||
            anchor.verified.checkpoint() != self.checkpoint.checkpoint() ||
            anchor.verified.state_root() != self.checkpoint.state_root() ||
            anchor.verified.slot() != self.checkpoint.slot() ||
            sha256(&anchor.state_ssz) != self.original_state_digest ||
            anchor.replay.as_ref().map(|replay| sha256(&replay.post_block_state_ssz)) !=
                self.original_post_state_digest ||
            sha256(&anchor.block_ssz) != self.original_block_digest ||
            anchor.block_format != self.block_format ||
            !Arc::ptr_eq(&self.engine_binding, &engine.bootstrap_binding())
        {
            return Err(BootstrapProofError::Binding)
        }
        Ok(())
    }
}

/// A sealed target receipt cannot transfer authority to another adapter or input pair.
#[derive(Debug, Error)]
pub enum BootstrapProofError {
    /// Exact original checkpoint, configuration or captured adapter differs.
    #[error("checkpoint execution reconciliation proof binding mismatch")]
    Binding,
    /// Shared proxy admission actually closed after the proof was issued.
    #[error("checkpoint execution reconciliation proof requires fresh reconciliation")]
    ReconciliationRequired,
}

/// Preparation never accepts a caller-supplied execution hash or validity claim.
#[derive(Debug, Error)]
pub enum BootstrapPrepareError {
    /// Concrete roots, schemas, network binding or execution commitments disagree.
    #[error("checkpoint execution binding mismatch")]
    Binding,
    /// Invalid independently bounded retry configuration.
    #[error("checkpoint execution retry bounds invalid")]
    Bounds,
    /// Preparation exceeded the complete monotonic deadline.
    #[error("checkpoint execution preparation deadline expired")]
    Deadline,
    /// Complete checkpoint authentication failed.
    #[error(transparent)]
    Bootstrap(#[from] BootstrapError),
    /// State-dependent trust-window calculation failed.
    #[error(transparent)]
    WeakSubjectivity(#[from] crate::weak_subjectivity::WeakSubjectivityError),
    /// Exact original complete typed decoding failed.
    #[error("checkpoint execution schema failed: {0}")]
    Schema(String),
}

/// Actual transport errors and unknown outcomes cannot supply execution authority.
#[derive(Debug, Error)]
pub enum BootstrapRunError<E: std::error::Error + Send + Sync + 'static> {
    /// Actual adapter configuration differs from the reauthenticated checkpoint.
    #[error("checkpoint execution adapter binding mismatch")]
    Binding,
    /// Exact current target was deterministically rejected by the actual engine.
    #[error("checkpoint execution target was not validated: {0:?}")]
    Rejected(PayloadStatus),
    /// Original engine/proxy failure, without flattening its type or substituting success.
    #[error(transparent)]
    Engine(#[from] EngineVerificationError<E>),
    /// Wrong reply container, payload job, or hash context closed admission.
    #[error("checkpoint execution acknowledgement malformed")]
    Malformed,
    /// Total preparation/download or trust-window deadline expired.
    #[error("checkpoint execution reconciliation deadline expired")]
    Deadline,
    /// Every bounded attempt returned no execution validity.
    #[error("checkpoint execution reconciliation attempts exhausted")]
    Attempts,
    /// Cancellation before success returns no target reconciliation evidence.
    #[error("checkpoint execution reconciliation cancelled")]
    Cancelled,
    /// An uncertain prior attempt or actually closed proxy cannot be reset here.
    #[error("checkpoint execution reconciliation required before restart")]
    ReconciliationRequired,
    /// Completed/rejected owner cannot be reused to issue another request.
    #[error("checkpoint execution bootstrap owner already finished")]
    AlreadyFinished,
}

#[derive(Clone, Copy, Debug)]
struct DownloadTarget {
    hash: Root,
}

struct AttemptGuard<F: Fn()> {
    uncertain: Arc<AtomicBool>,
    close: F,
    armed: bool,
}
impl<F: Fn()> Drop for AttemptGuard<F> {
    fn drop(&mut self) {
        if self.armed {
            self.uncertain.store(true, Ordering::Release);
            (self.close)();
        }
    }
}

const fn method(fork: Fork) -> ForkchoiceVersion {
    match fork {
        Fork::Phase0 | Fork::Altair | Fork::Bellatrix => ForkchoiceVersion::V1,
        Fork::Capella => ForkchoiceVersion::V2,
        Fork::Deneb | Fork::Electra | Fork::Fulu => ForkchoiceVersion::V3,
        Fork::Gloas => ForkchoiceVersion::V4,
    }
}

fn download_hash(
    state: &BeaconState,
    block: &BeaconBlock,
    preset: Preset,
) -> Result<Option<Root>, BootstrapPrepareError> {
    macro_rules! classic {
        ($state:expr, $block:expr, $schema:ident) => {{
            let header = &$state.latest_execution_payload_header;
            let payload = &$block.body.execution_payload;
            if header.block_hash != payload.block_hash {
                return Err(BootstrapPrepareError::Binding)
            }
            if payload.block_hash == [0; 32] {
                if *header !=
                    $schema::ExecutionPayloadHeader::default_value(preset)
                        .map_err(|error| BootstrapPrepareError::Schema(error.to_string()))? ||
                    *payload !=
                        $schema::ExecutionPayload::default_value(preset).map_err(|error| {
                            BootstrapPrepareError::Schema(error.to_string())
                        })?
                {
                    return Err(BootstrapPrepareError::Binding)
                }
                Ok(None)
            } else {
                Ok(Some(payload.block_hash))
            }
        }};
    }
    match (state, block) {
        (BeaconState::Phase0(..), BeaconBlock::Phase0(..)) |
        (BeaconState::Altair(..), BeaconBlock::Altair(..)) => Ok(None),
        (BeaconState::Bellatrix(state, _), BeaconBlock::Bellatrix(block, _)) => {
            classic!(state, block, bellatrix)
        }
        (BeaconState::Capella(state, _), BeaconBlock::Capella(block, _)) => {
            classic!(state, block, capella)
        }
        (BeaconState::Deneb(state, _), BeaconBlock::Deneb(block, _)) => {
            classic!(state, block, deneb)
        }
        (BeaconState::Electra(state, _), BeaconBlock::Electra(block, _)) => {
            classic!(state, block, electra)
        }
        (BeaconState::Fulu(state, _), BeaconBlock::Fulu(block, _)) => {
            classic!(state, block, fulu)
        }
        (BeaconState::Gloas(state, _), BeaconBlock::Gloas(block, _)) => {
            let bid = &block.body.signed_execution_payload_bid.message;
            if state.latest_execution_payload_bid != *bid ||
                state.latest_block_hash != bid.parent_block_hash
            {
                return Err(BootstrapPrepareError::Binding)
            }
            Ok((bid.parent_block_hash != [0; 32]).then_some(bid.parent_block_hash))
        }
        _ => Err(BootstrapPrepareError::Binding),
    }
}

// A dropped, unused cancellation handle does not request cancellation. watch closes when its
// final Sender is dropped, so wait_for's Err is channel closure, not an observed true request.
// Leave only this select branch pending; the actual proxy/total deadline stay independently
// bounded.
async fn wait_for_cancellation(receiver: &mut watch::Receiver<bool>) {
    if receiver.wait_for(|requested| *requested).await.is_err() {
        std::future::pending::<()>().await;
    }
}

#[cfg(test)]
pub(crate) mod tests;
