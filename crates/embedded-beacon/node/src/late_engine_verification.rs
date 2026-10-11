//! Mandatory typed execution verification for Electra, Fulu and Gloas.
//! The exact captured Engine API supplies execution validity; this adapter checks
//! the configured fork, clock, committed parent and exact latest-valid hash.

use crate::engine_verification::EngineVerificationError;
use alloy_primitives::B256;
use alloy_rpc_types_engine::{ExecutionData, PayloadStatusEnum};
use reth_embedded_beacon_config::{ChainConfig, Preset as ConfigPreset};
use reth_embedded_beacon_execution_proxy::{
    payload::candidate::{
        from_electra_payload, from_fulu_payload, from_gloas_payload, EngineSidecarInputs,
    },
    ExecutionEngine as TypedExecutionEngine, ExecutionProxy, PayloadReply, PayloadVersion,
};
use reth_embedded_beacon_primitives::{Fork, Root};
use reth_embedded_beacon_state_transition::{
    electra_processing, fulu_processing, gloas_processing,
    ssz::{electra, fulu, gloas, Preset},
};
use std::sync::Arc;

/// Invoke mandatory late-fork validation through this execution node's captured typed API.
#[derive(Debug)]
pub struct LateForkEngineAdapter<E> {
    proxy: ExecutionProxy<E>,
    config: Arc<ChainConfig>,
    pending: Option<crate::runtime_engine::pending::PendingFeedback>,
}

impl<E> LateForkEngineAdapter<E>
where
    E: TypedExecutionEngine<Payload = ExecutionData, PayloadReply = PayloadReply>,
    E::Error: std::error::Error + Send + Sync + 'static,
{
    /// Bind the typed API to the exact checked network used by the execution node.
    pub fn new(
        proxy: ExecutionProxy<E>,
        config: ChainConfig,
    ) -> Result<Self, EngineVerificationError<E::Error>> {
        config.validate().map_err(|_| EngineVerificationError::Configuration)?;
        config.clock().map_err(|_| EngineVerificationError::Configuration)?;
        Ok(Self { proxy, config: Arc::new(config), pending: None })
    }

    pub(crate) fn with_pending_feedback(
        mut self,
        pending: crate::runtime_engine::pending::PendingFeedback,
    ) -> Self {
        self.pending = Some(pending);
        self
    }
    fn validate_context(
        &self,
        fork: Fork,
        slot: u64,
        timestamp: u64,
        parent_hash: Root,
        committed_parent: Option<Root>,
    ) -> Result<Preset, EngineVerificationError<E::Error>> {
        if self.config.fork_at_slot(slot).map_err(|_| EngineVerificationError::Configuration)?.fork !=
            fork
        {
            return Err(EngineVerificationError::ForkMismatch);
        }
        let expected = self
            .config
            .clock()
            .map_err(|_| EngineVerificationError::Configuration)?
            .start_of_slot(slot)
            .map_err(|_| EngineVerificationError::Configuration)? /
            1000;
        if timestamp != expected {
            return Err(EngineVerificationError::TimestampMismatch);
        }
        let Some(committed_parent) = committed_parent else {
            return Err(EngineVerificationError::TerminalVerificationUnavailable);
        };
        if parent_hash != committed_parent {
            return Err(EngineVerificationError::ParentMismatch);
        }
        Ok(match self.config.preset {
            ConfigPreset::Mainnet => Preset::Mainnet,
            ConfigPreset::Minimal => Preset::Minimal,
        })
    }

    async fn verify(
        &self,
        data: ExecutionData,
        version: PayloadVersion,
        expected_hash: Root,
    ) -> Result<(), EngineVerificationError<E::Error>> {
        let timer = self.pending.as_ref().and_then(|pending| pending.measure_execution());
        let response = self.proxy.new_payload(version, data).await?;
        drop(timer);
        let PayloadReply::V1(status) = response else {
            self.proxy.close();
            return Err(EngineVerificationError::UnexpectedResponse);
        };
        if let Some(pending) = &self.pending {
            pending.observe(&status.status, expected_hash);
        }
        if !matches!(status.status, PayloadStatusEnum::Valid) {
            return Err(EngineVerificationError::NotValidated(status.status.as_str()));
        }
        if status.latest_valid_hash != Some(B256::from(expected_hash)) {
            self.proxy.close();
            return Err(EngineVerificationError::LatestValidHashMismatch);
        }
        Ok(())
    }
}

macro_rules! payload_v4_adapter {
    ($processing:ident, $schema:ident, $fork:ident, $convert:ident) => {
        impl<E> $processing::ExecutionEngine for LateForkEngineAdapter<E>
        where
            E: TypedExecutionEngine<Payload = ExecutionData, PayloadReply = PayloadReply>,
            E::Error: std::error::Error + Send + Sync + 'static,
        {
            type Error = EngineVerificationError<E::Error>;

            async fn verify_terminal_block<'a>(
                &'a self,
                _payload: &'a $schema::ExecutionPayload,
                _context: $processing::ExecutionContext,
            ) -> Result<(), Self::Error> {
                Err(EngineVerificationError::TerminalVerificationUnavailable)
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
                    payload.parent_hash,
                    context.previous_execution_payload_hash,
                )?;
                let data = $convert(
                    payload,
                    preset,
                    EngineSidecarInputs {
                        parent_beacon_block_root: context.parent_beacon_block_root,
                        versioned_hashes: &context.expected_blob_versioned_hashes,
                        execution_requests_list: &context.execution_requests_list,
                    },
                )?;
                self.verify(data, PayloadVersion::V4, payload.block_hash).await
            }
        }
    };
}

payload_v4_adapter!(electra_processing, electra, Electra, from_electra_payload);
payload_v4_adapter!(fulu_processing, fulu, Fulu, from_fulu_payload);

impl<E> gloas_processing::ExecutionEngine for LateForkEngineAdapter<E>
where
    E: TypedExecutionEngine<Payload = ExecutionData, PayloadReply = PayloadReply>,
    E::Error: std::error::Error + Send + Sync + 'static,
{
    type Error = EngineVerificationError<E::Error>;

    async fn verify_payload<'a>(
        &'a self,
        payload: &'a gloas::ExecutionPayload,
        context: gloas_processing::ExecutionContext,
    ) -> Result<(), Self::Error> {
        let preset = self.validate_context(
            Fork::Gloas,
            context.slot,
            payload.timestamp,
            payload.parent_hash,
            Some(context.previous_execution_payload_hash),
        )?;
        if payload.slot_number != context.slot {
            return Err(EngineVerificationError::SlotMismatch);
        }
        let data = from_gloas_payload(
            payload,
            preset,
            EngineSidecarInputs {
                parent_beacon_block_root: context.parent_beacon_block_root,
                versioned_hashes: &context.expected_blob_versioned_hashes,
                execution_requests_list: &context.execution_requests_list,
            },
        )?;
        self.verify(data, PayloadVersion::V5, payload.block_hash).await
    }
}

#[cfg(test)]
mod tests;
