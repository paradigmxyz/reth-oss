//! Exact typed Engine results for the separate optimistic follower path.
//!
//! Deferred execution is never a strict verification receipt. Every request still checks its
//! immutable fork schema, slot clock, committed parent and bounded payload conversion. The
//! shared proxy retains its ordinary timeout, cancellation and uncertain-response policy.

use super::{
    forkchoice_receipt::VerifiedForkchoice, EngineVerificationError, RuntimeEngineAdapter,
};
use alloy_primitives::B256;
use alloy_rpc_types_engine::{
    ExecutionData, ForkchoiceState, PayloadStatus as EnginePayloadStatus, PayloadStatusEnum,
};
use reth_embedded_beacon_execution_proxy::{
    payload::{
        candidate::{
            from_capella_payload, from_deneb_payload, from_electra_payload, from_fulu_payload,
            EngineSidecarInputs,
        },
        from_bellatrix_payload,
    },
    ExecutionEngine as TypedExecutionEngine, ForkchoiceReply, ForkchoiceVersion, PayloadReply,
    PayloadVersion,
};
use reth_embedded_beacon_fork_choice::store::optimistic::{
    ExecutionEngine, Payload, PayloadContext, PayloadResponse, PayloadStatus, TerminalResponse,
};
use reth_embedded_beacon_primitives::{Fork, Root};

/// Checked original FCU response; only exact VALID contains an execution acknowledgement.
#[derive(Clone, Debug)]
pub(crate) enum OptimisticForkchoice {
    /// Genuine SYNCING has no receipt and cannot publish execution authority.
    Syncing,
    /// The same captured Engine acknowledged these exact head/safe/finalized associations.
    Valid(VerifiedForkchoice),
}

struct PayloadIdentity {
    fork: Fork,
    slot: u64,
    timestamp: u64,
    block_number: u64,
    parent_hash: Root,
    committed_parent: Option<Root>,
}

impl<E> ExecutionEngine for RuntimeEngineAdapter<E>
where
    E: TypedExecutionEngine<Payload = ExecutionData, PayloadReply = PayloadReply> + Clone,
    E::Error: std::error::Error + Send + Sync + 'static,
{
    type Error = EngineVerificationError<E::Error>;

    async fn verify_terminal_block<'a>(
        &'a self,
        payload: Payload<'a>,
        context: PayloadContext,
    ) -> Result<TerminalResponse, Self::Error> {
        let identity =
            payload_identity(payload, &context).ok_or(EngineVerificationError::ForkMismatch)?;
        self.verify_terminal_context(
            identity.fork,
            identity.slot,
            identity.timestamp,
            identity.block_number,
            identity.parent_hash,
            identity.committed_parent,
        )?;
        // The captured provider currently proves complete terminal ancestry only. A missing
        // header cannot establish that BOTH required PoW blocks are unknown, so it stays an error.
        Ok(TerminalResponse::Verified)
    }

    async fn notify_new_payload<'a>(
        &'a self,
        payload: Payload<'a>,
        context: PayloadContext,
    ) -> Result<PayloadResponse, Self::Error> {
        let identity =
            payload_identity(payload, &context).ok_or(EngineVerificationError::ForkMismatch)?;
        let preset = self.validate_context(
            identity.fork,
            identity.slot,
            identity.timestamp,
            identity.block_number,
            identity.parent_hash,
            identity.committed_parent,
        )?;
        let (version, data) = match (payload, context) {
            (Payload::Bellatrix(payload), PayloadContext::Bellatrix(_)) => {
                (PayloadVersion::V1, from_bellatrix_payload(payload, preset)?)
            }
            (Payload::Capella(payload), PayloadContext::Capella(_)) => {
                (PayloadVersion::V2, from_capella_payload(payload, preset)?)
            }
            (Payload::Deneb(payload), PayloadContext::Deneb(context)) => (
                PayloadVersion::V3,
                from_deneb_payload(
                    payload,
                    preset,
                    context.parent_beacon_block_root,
                    &context.expected_blob_versioned_hashes,
                )?,
            ),
            (Payload::Electra(payload), PayloadContext::Electra(context)) => (
                PayloadVersion::V4,
                from_electra_payload(
                    payload,
                    preset,
                    EngineSidecarInputs {
                        parent_beacon_block_root: context.parent_beacon_block_root,
                        versioned_hashes: &context.expected_blob_versioned_hashes,
                        execution_requests_list: &context.execution_requests_list,
                    },
                )?,
            ),
            (Payload::Fulu(payload), PayloadContext::Fulu(context)) => (
                PayloadVersion::V4,
                from_fulu_payload(
                    payload,
                    preset,
                    EngineSidecarInputs {
                        parent_beacon_block_root: context.parent_beacon_block_root,
                        versioned_hashes: &context.expected_blob_versioned_hashes,
                        execution_requests_list: &context.execution_requests_list,
                    },
                )?,
            ),
            _ => return Err(EngineVerificationError::ForkMismatch),
        };
        let response = self.proxy.new_payload(version, data).await?;
        let PayloadReply::V1(status) = response else {
            self.proxy.close();
            return Err(EngineVerificationError::UnexpectedResponse)
        };
        self.checked_optimistic_status(status, B256::from(payload.block_hash()))
    }
}

impl<E> RuntimeEngineAdapter<E>
where
    E: TypedExecutionEngine<Payload = ExecutionData, PayloadReply = PayloadReply> + Clone,
    E::Error: std::error::Error + Send + Sync + 'static,
{
    fn checked_optimistic_status(
        &self,
        original: EnginePayloadStatus,
        expected: B256,
    ) -> Result<PayloadResponse, EngineVerificationError<E::Error>> {
        let (status, validation_error) = match original.status {
            PayloadStatusEnum::Valid => (PayloadStatus::Valid, None),
            PayloadStatusEnum::Syncing => (PayloadStatus::Syncing, None),
            PayloadStatusEnum::Accepted => (PayloadStatus::Accepted, None),
            PayloadStatusEnum::Invalid { validation_error } => {
                (PayloadStatus::Invalid, Some(validation_error))
            }
        };
        if validation_error.as_ref().is_some_and(|error| error.len() > 1024) {
            self.proxy.close();
            return Err(EngineVerificationError::UnexpectedResponse)
        }
        let malformed_hash = match status {
            PayloadStatus::Valid => original.latest_valid_hash != Some(expected),
            PayloadStatus::Syncing | PayloadStatus::Accepted | PayloadStatus::InvalidBlockHash => {
                original.latest_valid_hash.is_some()
            }
            PayloadStatus::Invalid => original.latest_valid_hash == Some(expected),
        };
        if malformed_hash {
            self.proxy.close();
            return Err(EngineVerificationError::LatestValidHashMismatch)
        }
        Ok(PayloadResponse {
            status,
            latest_valid_hash: original.latest_valid_hash.map(Into::into),
            validation_error,
        })
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
    /// Preserve the checked original outcome for the private sealed-receipt constructor.
    pub(super) async fn optimistic_forkchoice_response(
        &self,
        version: ForkchoiceVersion,
        state: ForkchoiceState,
        custody: Option<[u8; 16]>,
    ) -> Result<PayloadResponse, EngineVerificationError<E::Error>> {
        if state.head_block_hash == B256::ZERO || version == ForkchoiceVersion::V5 {
            return Err(EngineVerificationError::Configuration)
        }
        let response = self.proxy.forkchoice_updated(version, state, custody).await?;
        let ForkchoiceReply::V1(response) = response else {
            self.proxy.close();
            return Err(EngineVerificationError::UnexpectedResponse)
        };
        if response.payload_id.is_some() ||
            matches!(response.payload_status.status, PayloadStatusEnum::Accepted)
        {
            self.proxy.close();
            return Err(EngineVerificationError::UnexpectedResponse)
        }
        self.checked_optimistic_status(response.payload_status, state.head_block_hash)
    }
}

fn payload_identity(payload: Payload<'_>, context: &PayloadContext) -> Option<PayloadIdentity> {
    macro_rules! identity {
        ($payload:expr, $context:expr, $fork:ident) => {
            Some(PayloadIdentity {
                fork: Fork::$fork,
                slot: $context.slot,
                timestamp: $payload.timestamp,
                block_number: $payload.block_number,
                parent_hash: $payload.parent_hash,
                committed_parent: $context.previous_execution_payload_hash,
            })
        };
    }
    match (payload, context) {
        (Payload::Bellatrix(payload), PayloadContext::Bellatrix(context)) => {
            identity!(payload, context, Bellatrix)
        }
        (Payload::Capella(payload), PayloadContext::Capella(context)) => {
            identity!(payload, context, Capella)
        }
        (Payload::Deneb(payload), PayloadContext::Deneb(context)) => {
            identity!(payload, context, Deneb)
        }
        (Payload::Electra(payload), PayloadContext::Electra(context)) => {
            identity!(payload, context, Electra)
        }
        (Payload::Fulu(payload), PayloadContext::Fulu(context)) => {
            identity!(payload, context, Fulu)
        }
        _ => None,
    }
}
