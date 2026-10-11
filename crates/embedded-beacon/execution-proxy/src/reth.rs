//! Calls the complete Engine API server boundary directly, without wire serialization.

use super::{ExecutionEngine, ForkchoiceReply, ForkchoiceVersion, PayloadReply, PayloadVersion};
use alloy_primitives::B128;
use alloy_rpc_types_engine::{
    ExecutionData, ExecutionPayload, ExecutionPayloadInputV2, ForkchoiceState,
};
use jsonrpsee_types::{
    error::{INVALID_PARAMS_CODE, INVALID_PARAMS_MSG},
    ErrorObjectOwned,
};
use reth_chainspec::EthereumHardforks;
use reth_engine_primitives::{EngineApiValidator, EngineTypes};
use reth_payload_primitives::PayloadTypes;
use reth_rpc_engine_api::{EngineApi, EngineApiServer};
use reth_storage_api::{BalProvider, BlockReader, HeaderProvider, StateProviderFactory};
use reth_transaction_pool::TransactionPool;
use std::{fmt, sync::Arc};

/// A shared instance of the same validated API object used by execution RPC.
pub struct RethTypedEngine<P, T: PayloadTypes, Pool, V, C> {
    api: Arc<EngineApi<P, T, Pool, V, C>>,
}

impl<P, T: PayloadTypes, Pool, V, C> RethTypedEngine<P, T, Pool, V, C> {
    /// Captures a constructed API object through the public builder hook.
    pub fn new(api: EngineApi<P, T, Pool, V, C>) -> Self {
        Self { api: Arc::new(api) }
    }
}
impl<P, T: PayloadTypes, Pool, V, C> Clone for RethTypedEngine<P, T, Pool, V, C> {
    fn clone(&self) -> Self {
        Self { api: self.api.clone() }
    }
}
impl<P, T: PayloadTypes, Pool, V, C> fmt::Debug for RethTypedEngine<P, T, Pool, V, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RethTypedEngine").finish_non_exhaustive()
    }
}
impl<P, T, Pool, V, C> ExecutionEngine for RethTypedEngine<P, T, Pool, V, C>
where
    P: HeaderProvider + BlockReader + StateProviderFactory + BalProvider + 'static,
    T: PayloadTypes<ExecutionData = ExecutionData> + EngineTypes,
    Pool: TransactionPool + 'static,
    V: EngineApiValidator<T>,
    C: EthereumHardforks + Send + Sync + 'static,
{
    type Payload = ExecutionData;
    type Forkchoice = ForkchoiceState;
    type PayloadReply = PayloadReply;
    type ForkchoiceReply = ForkchoiceReply;
    type Error = ErrorObjectOwned;
    async fn new_payload(
        &self,
        version: PayloadVersion,
        data: ExecutionData,
    ) -> Result<PayloadReply, ErrorObjectOwned> {
        // Calling the public server methods retains policy checks that the lower-level
        // EngineApi methods omit (in particular acceptance of a requests hash).
        // Require an exactly representable request rather than dropping later-fork fields.
        let ExecutionData { payload, sidecar } = data;
        let cancun = sidecar.cancun().cloned();
        let inclusion = sidecar.inclusion_list_transactions().cloned();
        let prague = sidecar.into_prague();
        let api = self.api.as_ref();
        let reply = match (version, payload, cancun, prague, inclusion) {
            (PayloadVersion::V1, ExecutionPayload::V1(payload), None, None, None) => {
                EngineApiServer::<T>::new_payload_v1(api, payload).await?
            }
            (PayloadVersion::V2, ExecutionPayload::V1(payload), None, None, None) => {
                EngineApiServer::<T>::new_payload_v2(
                    api,
                    ExecutionPayloadInputV2 { execution_payload: payload, withdrawals: None },
                )
                .await?
            }
            (PayloadVersion::V2, ExecutionPayload::V2(payload), None, None, None) => {
                EngineApiServer::<T>::new_payload_v2(api, payload.into_payload_input_v2(true))
                    .await?
            }
            (PayloadVersion::V3, ExecutionPayload::V3(payload), Some(cancun), None, None) => {
                EngineApiServer::<T>::new_payload_v3(
                    api,
                    payload,
                    cancun.versioned_hashes,
                    cancun.parent_beacon_block_root,
                )
                .await?
            }
            (
                PayloadVersion::V4,
                ExecutionPayload::V3(payload),
                Some(cancun),
                Some(prague),
                None,
            ) => {
                EngineApiServer::<T>::new_payload_v4(
                    api,
                    payload,
                    cancun.versioned_hashes,
                    cancun.parent_beacon_block_root,
                    prague.requests,
                )
                .await?
            }
            (
                PayloadVersion::V5,
                ExecutionPayload::V4(payload),
                Some(cancun),
                Some(prague),
                None,
            ) => {
                EngineApiServer::<T>::new_payload_v5(
                    api,
                    payload,
                    cancun.versioned_hashes,
                    cancun.parent_beacon_block_root,
                    prague.requests,
                )
                .await?
            }
            (
                PayloadVersion::V6,
                ExecutionPayload::V4(payload),
                Some(cancun),
                Some(prague),
                Some(inclusion),
            ) => {
                return EngineApiServer::<T>::new_payload_v6(
                    api,
                    payload,
                    cancun.versioned_hashes,
                    cancun.parent_beacon_block_root,
                    prague.requests,
                    inclusion,
                )
                .await
                .map(PayloadReply::V2)
            }
            _ => {
                return Err(ErrorObjectOwned::owned(
                    INVALID_PARAMS_CODE,
                    INVALID_PARAMS_MSG,
                    Some("payload and sidecar do not match the selected Engine API method"),
                ))
            }
        };
        Ok(PayloadReply::V1(reply))
    }
    async fn forkchoice_updated(
        &self,
        version: ForkchoiceVersion,
        state: ForkchoiceState,
        custody: Option<[u8; 16]>,
    ) -> Result<ForkchoiceReply, ErrorObjectOwned> {
        let custody = custody.map(B128::from);
        let api = self.api.as_ref();
        let reply = match version {
            ForkchoiceVersion::V1 => {
                EngineApiServer::<T>::fork_choice_updated_v1(api, state, None).await?
            }
            ForkchoiceVersion::V2 => {
                EngineApiServer::<T>::fork_choice_updated_v2(api, state, None).await?
            }
            ForkchoiceVersion::V3 => {
                EngineApiServer::<T>::fork_choice_updated_v3(api, state, None).await?
            }
            ForkchoiceVersion::V4 => {
                EngineApiServer::<T>::fork_choice_updated_v4(api, state, None, custody).await?
            }
            ForkchoiceVersion::V5 => {
                return EngineApiServer::<T>::fork_choice_updated_v5(api, state, None, custody)
                    .await
                    .map(ForkchoiceReply::V2)
            }
        };
        Ok(ForkchoiceReply::V1(reply))
    }
}
