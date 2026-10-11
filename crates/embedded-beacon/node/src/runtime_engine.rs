//! Composed mandatory Engine verification for every execution-bearing consensus fork.
//! The captured typed API retains actual verdicts; first-Merge ancestry uses captured local
//! evidence.

use crate::{
    engine_verification::{BellatrixEngineAdapter, EngineVerificationError},
    late_engine_verification::LateForkEngineAdapter,
    terminal_verification::{TerminalAuthority, TerminalExecutionProvider, TerminalVerifier},
};
use alloy_primitives::B256;
use alloy_rpc_types_engine::{ExecutionData, ForkchoiceState, PayloadStatusEnum};
use reth_embedded_beacon_config::{ChainConfig, Preset as ConfigPreset};
use reth_embedded_beacon_execution_proxy::{
    payload::{
        candidate::{
            from_capella_payload, from_deneb_payload, from_electra_payload, from_fulu_payload,
            EngineSidecarInputs,
        },
        from_bellatrix_payload,
    },
    ExecutionEngine as TypedExecutionEngine, ExecutionProxy, ForkchoiceReply, ForkchoiceVersion,
    PayloadReply, PayloadVersion,
};
use reth_embedded_beacon_primitives::{Fork, Root};
use reth_embedded_beacon_state_transition::{
    bellatrix_processing, capella_processing, deneb_processing, electra_processing,
    fulu_processing, gloas_processing,
    ssz::{bellatrix, capella, deneb, electra, fulu, gloas, Preset},
};
use std::sync::Arc;

/// One real typed adapter satisfying the complete composite transition Engine contract.
/// No operator flag, fixture view or caller-supplied validity declaration can authorize import.
#[derive(Debug)]
pub struct RuntimeEngineAdapter<E> {
    bellatrix: Arc<BellatrixEngineAdapter<E>>,
    late: Arc<LateForkEngineAdapter<E>>,
    proxy: ExecutionProxy<E>,
    config: Arc<ChainConfig>,
    terminal: Option<Arc<dyn TerminalAuthority>>,
    pending: pending::PendingFeedback,
}

impl<E> RuntimeEngineAdapter<E>
where
    E: TypedExecutionEngine<Payload = ExecutionData, PayloadReply = PayloadReply> + Clone,
    E::Error: std::error::Error + Send + Sync + 'static,
{
    /// Bind all mandatory callbacks to one checked network and the same ordered typed API.
    pub fn new(
        proxy: ExecutionProxy<E>,
        config: ChainConfig,
    ) -> Result<Self, EngineVerificationError<E::Error>> {
        config.validate().map_err(|_| EngineVerificationError::Configuration)?;
        config.clock().map_err(|_| EngineVerificationError::Configuration)?;
        let pending = pending::PendingFeedback::default();
        Ok(Self {
            bellatrix: Arc::new(
                BellatrixEngineAdapter::new(proxy.clone(), config.clone())?
                    .with_pending_feedback(pending.clone()),
            ),
            late: Arc::new(
                LateForkEngineAdapter::new(proxy.clone(), config.clone())?
                    .with_pending_feedback(pending.clone()),
            ),
            proxy,
            config: Arc::new(config),
            terminal: None,
            pending,
        })
    }

    /// Capture only the concrete provider-backed verifier from this exact execution node.
    /// The launcher must capture the provider and typed API from the same built node.
    pub fn with_terminal_verifier<P: TerminalExecutionProvider + 'static>(
        proxy: ExecutionProxy<E>,
        config: ChainConfig,
        terminal: TerminalVerifier<P>,
    ) -> Result<Self, EngineVerificationError<E::Error>> {
        if !terminal.configuration_matches(&config) {
            return Err(EngineVerificationError::Configuration)
        }
        let mut adapter = Self::new(proxy, config)?;
        adapter.terminal = Some(Arc::new(terminal));
        Ok(adapter)
    }

    /// Exact checked consensus configuration bound to every callback in this adapter.
    pub fn config(&self) -> &ChainConfig {
        &self.config
    }

    /// Observe actual closed admission after an ambiguous response, timeout or cancellation.
    pub fn is_closed(&self) -> bool {
        self.proxy.is_closed()
    }

    pub(crate) fn begin_import_attempt(&self, root: Root) -> Result<(), ()> {
        self.pending.begin(root)
    }
    pub(crate) fn take_pending_import(
        &self,
        root: Root,
    ) -> Result<Option<pending::PendingPayload>, ()> {
        self.pending.take(root)
    }
    #[cfg(all(feature = "peer-runtime", feature = "gossip-validation"))]
    pub(crate) fn execution_micros(&self, root: Root) -> Option<u64> {
        self.pending.execution_micros(root)
    }
    fn validate_context(
        &self,
        fork: Fork,
        slot: u64,
        timestamp: u64,
        block_number: u64,
        parent_hash: Root,
        committed_parent: Option<Root>,
    ) -> Result<Preset, EngineVerificationError<E::Error>> {
        if self.config.fork_at_slot(slot).map_err(|_| EngineVerificationError::Configuration)?.fork !=
            fork
        {
            return Err(EngineVerificationError::ForkMismatch)
        }
        let expected = self
            .config
            .clock()
            .map_err(|_| EngineVerificationError::Configuration)?
            .start_of_slot(slot)
            .map_err(|_| EngineVerificationError::Configuration)? /
            1000;
        if timestamp != expected {
            return Err(EngineVerificationError::TimestampMismatch)
        }
        if let Some(committed_parent) = committed_parent {
            if parent_hash != committed_parent {
                return Err(EngineVerificationError::ParentMismatch)
            }
        } else {
            let terminal = self
                .terminal
                .as_ref()
                .ok_or(EngineVerificationError::TerminalVerificationUnavailable)?;
            terminal.verify_context(&self.config, parent_hash, slot, block_number)?;
        }
        Ok(match self.config.preset {
            ConfigPreset::Mainnet => Preset::Mainnet,
            ConfigPreset::Minimal => Preset::Minimal,
        })
    }

    fn verify_terminal_context(
        &self,
        fork: Fork,
        slot: u64,
        timestamp: u64,
        block_number: u64,
        parent_hash: Root,
        committed_parent: Option<Root>,
    ) -> Result<(), EngineVerificationError<E::Error>> {
        if committed_parent.is_some() {
            return Err(EngineVerificationError::ParentMismatch)
        }
        self.validate_context(fork, slot, timestamp, block_number, parent_hash, None)?;
        Ok(())
    }

    async fn verify(
        &self,
        version: PayloadVersion,
        data: ExecutionData,
        expected_hash: Root,
    ) -> Result<(), EngineVerificationError<E::Error>> {
        let timer = self.pending.measure_execution();
        let response = self.proxy.new_payload(version, data).await?;
        drop(timer);
        let PayloadReply::V1(status) = response else {
            self.proxy.close();
            return Err(EngineVerificationError::UnexpectedResponse)
        };
        self.pending.observe(&status.status, expected_hash);
        if !matches!(status.status, PayloadStatusEnum::Valid) {
            return Err(EngineVerificationError::NotValidated(status.status.as_str()))
        }
        if status.latest_valid_hash != Some(B256::from(expected_hash)) {
            self.proxy.close();
            return Err(EngineVerificationError::LatestValidHashMismatch)
        }
        Ok(())
    }
}

impl<E> RuntimeEngineAdapter<E>
where
    E: TypedExecutionEngine<
            Payload = ExecutionData,
            PayloadReply = PayloadReply,
            Forkchoice = ForkchoiceState,
            ForkchoiceReply = ForkchoiceReply,
        > + Clone,
    E::Error: std::error::Error + Send + Sync + 'static,
{
    /// Apply only execution associations derived from the coordinator's admitted consensus graph.
    /// Method and custody selection remain the coordinator's fork-specific responsibility.
    /// The typed proxy sends no proposer attributes and preserves the original engine verdict.
    /// A successful response must validate this exact head and cannot start a payload job.
    pub async fn forkchoice_updated(
        &self,
        version: ForkchoiceVersion,
        state: ForkchoiceState,
        custody: Option<[u8; 16]>,
    ) -> Result<(), EngineVerificationError<E::Error>> {
        // The pinned consensus contract uses the original response through V4. Extended V5
        // requires a separately audited interpretation and must not be silently downgraded.
        if state.head_block_hash == B256::ZERO || version == ForkchoiceVersion::V5 {
            return Err(EngineVerificationError::Configuration)
        }
        let head = state.head_block_hash;
        let response = self.proxy.forkchoice_updated(version, state, custody).await?;
        let ForkchoiceReply::V1(response) = response else {
            self.proxy.close();
            return Err(EngineVerificationError::UnexpectedResponse)
        };
        if response.payload_id.is_some() {
            self.proxy.close();
            return Err(EngineVerificationError::UnexpectedResponse)
        }
        if !matches!(response.payload_status.status, PayloadStatusEnum::Valid) {
            return Err(EngineVerificationError::NotValidated(
                response.payload_status.status.as_str(),
            ))
        }
        if response.payload_status.latest_valid_hash != Some(head) {
            self.proxy.close();
            return Err(EngineVerificationError::LatestValidHashMismatch)
        }
        Ok(())
    }

    /// Send a download-target FCU without interpreting SYNCING as execution validity.
    ///
    /// Only the authenticated bootstrap owner may use this crate-private hook. It preserves
    /// the original typed response, admits no proposer job, and publishes no beacon graph.
    pub(crate) async fn bootstrap_forkchoice_updated(
        &self,
        version: ForkchoiceVersion,
        state: ForkchoiceState,
    ) -> Result<ForkchoiceReply, EngineVerificationError<E::Error>> {
        if state.head_block_hash == B256::ZERO ||
            state.safe_block_hash != B256::ZERO ||
            state.finalized_block_hash != B256::ZERO ||
            version == ForkchoiceVersion::V5
        {
            return Err(EngineVerificationError::Configuration)
        }
        Ok(self.proxy.forkchoice_updated(version, state, None).await?)
    }

    /// Close actual shared proxy admission after an ambiguous bootstrap acknowledgement.
    /// This method cannot reopen the proxy or substitute a validation verdict.
    pub(crate) fn close_bootstrap_admission(&self) {
        self.proxy.close();
    }

    /// Immutable allocation identity of this exact captured adapter, retained across moves.
    /// Equal network configurations in independently captured adapters do not share proof.
    pub(crate) fn bootstrap_binding(&self) -> Arc<ChainConfig> {
        self.config.clone()
    }
}

macro_rules! late_delegate {
    ($processing:ident, $schema:ident, $fork:ident, $convert:ident) => {
        impl<E> $processing::ExecutionEngine for RuntimeEngineAdapter<E>
        where
            E: TypedExecutionEngine<Payload = ExecutionData, PayloadReply = PayloadReply> + Clone,
            E::Error: std::error::Error + Send + Sync + 'static,
        {
            type Error = EngineVerificationError<E::Error>;
            async fn verify_terminal_block<'a>(
                &'a self,
                payload: &'a $schema::ExecutionPayload,
                context: $processing::ExecutionContext,
            ) -> Result<(), Self::Error> {
                self.verify_terminal_context(
                    Fork::$fork,
                    context.slot,
                    payload.timestamp,
                    payload.block_number,
                    payload.parent_hash,
                    context.previous_execution_payload_hash,
                )
            }
            async fn verify_payload<'a>(
                &'a self,
                payload: &'a $schema::ExecutionPayload,
                context: $processing::ExecutionContext,
            ) -> Result<(), Self::Error> {
                let preset = self.validate_context(
                    Fork::$fork,
                    context.slot,
                    payload.timestamp,
                    payload.block_number,
                    payload.parent_hash,
                    context.previous_execution_payload_hash,
                )?;
                if context.previous_execution_payload_hash.is_none() {
                    // The real provider proof above establishes first-Merge eligibility; the
                    // original context remains None and no state commitment is fabricated.
                    let data = $convert(
                        payload,
                        preset,
                        EngineSidecarInputs {
                            parent_beacon_block_root: context.parent_beacon_block_root,
                            versioned_hashes: &context.expected_blob_versioned_hashes,
                            execution_requests_list: &context.execution_requests_list,
                        },
                    )?;
                    self.verify(PayloadVersion::V4, data, payload.block_hash).await
                } else {
                    $processing::ExecutionEngine::verify_payload(
                        self.late.as_ref(),
                        payload,
                        context,
                    )
                    .await
                }
            }
        }
    };
}
late_delegate!(electra_processing, electra, Electra, from_electra_payload);
late_delegate!(fulu_processing, fulu, Fulu, from_fulu_payload);

impl<E> bellatrix_processing::ExecutionEngine for RuntimeEngineAdapter<E>
where
    E: TypedExecutionEngine<Payload = ExecutionData, PayloadReply = PayloadReply> + Clone,
    E::Error: std::error::Error + Send + Sync + 'static,
{
    type Error = EngineVerificationError<E::Error>;
    async fn verify_terminal_block<'a>(
        &'a self,
        payload: &'a bellatrix::ExecutionPayload,
        context: bellatrix_processing::ExecutionContext,
    ) -> Result<(), Self::Error> {
        self.verify_terminal_context(
            Fork::Bellatrix,
            context.slot,
            payload.timestamp,
            payload.block_number,
            payload.parent_hash,
            context.previous_execution_payload_hash,
        )
    }
    async fn verify_payload<'a>(
        &'a self,
        payload: &'a bellatrix::ExecutionPayload,
        context: bellatrix_processing::ExecutionContext,
    ) -> Result<(), Self::Error> {
        let preset = self.validate_context(
            Fork::Bellatrix,
            context.slot,
            payload.timestamp,
            payload.block_number,
            payload.parent_hash,
            context.previous_execution_payload_hash,
        )?;
        if context.previous_execution_payload_hash.is_none() {
            self.verify(
                PayloadVersion::V1,
                from_bellatrix_payload(payload, preset)?,
                payload.block_hash,
            )
            .await
        } else {
            bellatrix_processing::ExecutionEngine::verify_payload(
                self.bellatrix.as_ref(),
                payload,
                context,
            )
            .await
        }
    }
}

impl<E> capella_processing::ExecutionEngine for RuntimeEngineAdapter<E>
where
    E: TypedExecutionEngine<Payload = ExecutionData, PayloadReply = PayloadReply> + Clone,
    E::Error: std::error::Error + Send + Sync + 'static,
{
    type Error = EngineVerificationError<E::Error>;

    async fn verify_terminal_block<'a>(
        &'a self,
        payload: &'a capella::ExecutionPayload,
        context: capella_processing::ExecutionContext,
    ) -> Result<(), Self::Error> {
        self.verify_terminal_context(
            Fork::Capella,
            context.slot,
            payload.timestamp,
            payload.block_number,
            payload.parent_hash,
            context.previous_execution_payload_hash,
        )
    }

    async fn verify_payload<'a>(
        &'a self,
        payload: &'a capella::ExecutionPayload,
        context: capella_processing::ExecutionContext,
    ) -> Result<(), Self::Error> {
        let preset = self.validate_context(
            Fork::Capella,
            context.slot,
            payload.timestamp,
            payload.block_number,
            payload.parent_hash,
            context.previous_execution_payload_hash,
        )?;
        self.verify(PayloadVersion::V2, from_capella_payload(payload, preset)?, payload.block_hash)
            .await
    }
}

impl<E> deneb_processing::ExecutionEngine for RuntimeEngineAdapter<E>
where
    E: TypedExecutionEngine<Payload = ExecutionData, PayloadReply = PayloadReply> + Clone,
    E::Error: std::error::Error + Send + Sync + 'static,
{
    type Error = EngineVerificationError<E::Error>;

    async fn verify_terminal_block<'a>(
        &'a self,
        payload: &'a deneb::ExecutionPayload,
        context: deneb_processing::ExecutionContext,
    ) -> Result<(), Self::Error> {
        self.verify_terminal_context(
            Fork::Deneb,
            context.slot,
            payload.timestamp,
            payload.block_number,
            payload.parent_hash,
            context.previous_execution_payload_hash,
        )
    }

    async fn verify_payload<'a>(
        &'a self,
        payload: &'a deneb::ExecutionPayload,
        context: deneb_processing::ExecutionContext,
    ) -> Result<(), Self::Error> {
        let preset = self.validate_context(
            Fork::Deneb,
            context.slot,
            payload.timestamp,
            payload.block_number,
            payload.parent_hash,
            context.previous_execution_payload_hash,
        )?;
        let data = from_deneb_payload(
            payload,
            preset,
            context.parent_beacon_block_root,
            &context.expected_blob_versioned_hashes,
        )?;
        self.verify(PayloadVersion::V3, data, payload.block_hash).await
    }
}

impl<E> gloas_processing::ExecutionEngine for RuntimeEngineAdapter<E>
where
    E: TypedExecutionEngine<Payload = ExecutionData, PayloadReply = PayloadReply> + Clone,
    E::Error: std::error::Error + Send + Sync + 'static,
{
    type Error = EngineVerificationError<E::Error>;

    async fn verify_payload<'a>(
        &'a self,
        payload: &'a gloas::ExecutionPayload,
        context: gloas_processing::ExecutionContext,
    ) -> Result<(), Self::Error> {
        gloas_processing::ExecutionEngine::verify_payload(self.late.as_ref(), payload, context)
            .await
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod forkchoice_tests;

pub(crate) mod forkchoice_receipt;

pub(crate) mod pending;

#[cfg(feature = "gossip-validation")]
mod optimistic;

#[cfg(feature = "gossip-validation")]
pub(crate) use optimistic::OptimisticForkchoice;

impl<E: Clone> Clone for RuntimeEngineAdapter<E> {
    fn clone(&self) -> Self {
        Self {
            bellatrix: self.bellatrix.clone(),
            late: self.late.clone(),
            proxy: self.proxy.clone(),
            config: self.config.clone(),
            terminal: self.terminal.clone(),
            pending: self.pending.clone(),
        }
    }
}
