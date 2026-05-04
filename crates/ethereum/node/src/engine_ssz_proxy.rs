//! HTTP SSZ transport proxy for the authenticated Engine API server.

use alloy_eips::{
    eip4895::Withdrawal,
    eip7685::{Requests, RequestsOrHash},
};
use alloy_primitives::{Address, Bytes, B256, B64};
use alloy_rpc_types_engine::{
    CancunPayloadFields, ExecutionData, ExecutionPayload, ExecutionPayloadSidecar,
    ExecutionPayloadV1, ExecutionPayloadV2, ExecutionPayloadV3, ExecutionPayloadV4,
    ForkchoiceState, ForkchoiceUpdated, PayloadAttributes, PayloadStatus, PayloadStatusEnum,
    PraguePayloadFields,
};
use http::{header::CONTENT_TYPE, Method, Response, StatusCode};
use http_body_util::BodyExt;
use jsonrpsee::server::{HttpBody, HttpRequest, HttpResponse};
use reth_engine_primitives::ConsensusEngineHandle;
use reth_ethereum_engine_primitives::EthEngineTypes;
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::sync::RwLock;
use tower::{BoxError, Layer, Service};

const OCTET_STREAM: &str = "application/octet-stream";
const TEXT_PLAIN: &str = "text/plain";

/// Shared handle used by [`EngineSszProxyLayer`].
#[derive(Clone, Debug, Default)]
pub struct EngineSszProxyHandle {
    engine: Arc<RwLock<Option<ConsensusEngineHandle<EthEngineTypes>>>>,
}

impl EngineSszProxyHandle {
    /// Sets the consensus engine handle used by the proxy.
    pub async fn set_engine(&self, engine: ConsensusEngineHandle<EthEngineTypes>) {
        *self.engine.write().await = Some(engine);
    }

    async fn engine(&self) -> Option<ConsensusEngineHandle<EthEngineTypes>> {
        self.engine.read().await.clone()
    }
}

/// A tower layer that intercepts EIP-8178 SSZ Engine API routes under `/engine`.
#[derive(Clone, Debug, Default)]
pub struct EngineSszProxyLayer {
    handle: EngineSszProxyHandle,
}

impl EngineSszProxyLayer {
    /// Creates a new proxy layer and a handle for setting the engine after node launch.
    pub fn new() -> (Self, EngineSszProxyHandle) {
        let handle = EngineSszProxyHandle::default();
        (Self { handle: handle.clone() }, handle)
    }
}

impl<S> Layer<S> for EngineSszProxyLayer {
    type Service = EngineSszProxyService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        EngineSszProxyService { inner, handle: self.handle.clone() }
    }
}

/// The service produced by [`EngineSszProxyLayer`].
#[derive(Clone, Debug)]
pub struct EngineSszProxyService<S> {
    inner: S,
    handle: EngineSszProxyHandle,
}

impl<S> Service<HttpRequest> for EngineSszProxyService<S>
where
    S: Service<HttpRequest, Response = HttpResponse, Error = BoxError> + Send + Clone,
    S::Future: Send + 'static,
{
    type Response = HttpResponse;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: HttpRequest) -> Self::Future {
        if !request.uri().path().starts_with("/engine/") {
            let fut = self.inner.call(request);
            return Box::pin(fut)
        }

        let handle = self.handle.clone();
        Box::pin(async move { Ok(handle_engine_ssz_request(handle, request).await) })
    }
}

async fn handle_engine_ssz_request(
    handle: EngineSszProxyHandle,
    request: HttpRequest,
) -> HttpResponse {
    if request.method() != Method::POST {
        return text_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed")
    }

    let path = request.uri().path().to_owned();
    let Some((version, resource)) = parse_engine_path(&path) else {
        return text_response(StatusCode::NOT_FOUND, "unknown engine ssz endpoint")
    };

    let Ok(body) = request.into_body().collect().await.map(|body| body.to_bytes()) else {
        return text_response(StatusCode::BAD_REQUEST, "failed to read request body")
    };

    let Some(engine) = handle.engine().await else {
        return text_response(StatusCode::SERVICE_UNAVAILABLE, "engine handle unavailable")
    };

    match resource {
        "payloads" => handle_new_payload(engine, version, &body).await,
        "forkchoice" => handle_forkchoice_updated(engine, version, &body).await,
        _ => text_response(StatusCode::NOT_FOUND, "unknown engine ssz endpoint"),
    }
}

fn parse_engine_path(path: &str) -> Option<(u8, &str)> {
    let mut segments = path.trim_start_matches('/').split('/');
    match (segments.next(), segments.next(), segments.next(), segments.next()) {
        (Some("engine"), Some(version), Some(resource), None) => {
            let version = version.strip_prefix('v')?.parse().ok()?;
            Some((version, resource))
        }
        _ => None,
    }
}

async fn handle_new_payload(
    engine: ConsensusEngineHandle<EthEngineTypes>,
    version: u8,
    body: &[u8],
) -> HttpResponse {
    let payload = match decode_new_payload_request(version, body) {
        Ok(payload) => payload,
        Err(err) => return text_response(StatusCode::BAD_REQUEST, err),
    };

    match engine.new_payload(payload).await {
        Ok(status) => ssz_response(PayloadStatusSsz::from(status)),
        Err(err) => text_response(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
    }
}

async fn handle_forkchoice_updated(
    engine: ConsensusEngineHandle<EthEngineTypes>,
    version: u8,
    body: &[u8],
) -> HttpResponse {
    let (state, attrs) = match decode_forkchoice_request(version, body) {
        Ok(request) => request,
        Err(err) => return text_response(StatusCode::BAD_REQUEST, err),
    };

    match engine.fork_choice_updated(state, attrs).await {
        Ok(updated) => ssz_response(ForkchoiceUpdatedSsz::from(updated)),
        Err(err) => text_response(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
    }
}

fn decode_new_payload_request(version: u8, body: &[u8]) -> Result<ExecutionData, &'static str> {
    use ssz::Decode;

    match version {
        1 => {
            let request = NewPayloadV1Request::from_ssz_bytes(body).map_err(|_| "invalid ssz")?;
            Ok(ExecutionData::new(
                request.execution_payload.into(),
                ExecutionPayloadSidecar::none(),
            ))
        }
        2 => {
            let request = NewPayloadV2Request::from_ssz_bytes(body).map_err(|_| "invalid ssz")?;
            Ok(ExecutionData::new(
                request.execution_payload.into(),
                ExecutionPayloadSidecar::none(),
            ))
        }
        3 => {
            let request = NewPayloadV3Request::from_ssz_bytes(body).map_err(|_| "invalid ssz")?;
            let sidecar = ExecutionPayloadSidecar::v3(CancunPayloadFields {
                parent_beacon_block_root: request.parent_beacon_block_root,
                versioned_hashes: request.expected_blob_versioned_hashes,
            });
            Ok(ExecutionData::new(request.execution_payload.into(), sidecar))
        }
        4 => {
            let request = NewPayloadV4Request::from_ssz_bytes(body).map_err(|_| "invalid ssz")?;
            let sidecar = ExecutionPayloadSidecar::v4(
                CancunPayloadFields {
                    parent_beacon_block_root: request.parent_beacon_block_root,
                    versioned_hashes: request.expected_blob_versioned_hashes,
                },
                PraguePayloadFields::new(RequestsOrHash::Requests(Requests::new(
                    request.execution_requests,
                ))),
            );
            Ok(ExecutionData::new(request.execution_payload.into(), sidecar))
        }
        5 => {
            let request = NewPayloadV5Request::from_ssz_bytes(body).map_err(|_| "invalid ssz")?;
            let sidecar = ExecutionPayloadSidecar::v4(
                CancunPayloadFields {
                    parent_beacon_block_root: request.parent_beacon_block_root,
                    versioned_hashes: request.expected_blob_versioned_hashes,
                },
                PraguePayloadFields::new(RequestsOrHash::Requests(Requests::new(
                    request.execution_requests,
                ))),
            );
            Ok(ExecutionData::new(ExecutionPayload::V4(request.execution_payload), sidecar))
        }
        _ => Err("unsupported payload endpoint version"),
    }
}

fn decode_forkchoice_request(
    version: u8,
    body: &[u8],
) -> Result<(ForkchoiceState, Option<PayloadAttributes>), &'static str> {
    use ssz::Decode;

    match version {
        1 => {
            let request =
                ForkchoiceUpdatedV1Request::from_ssz_bytes(body).map_err(|_| "invalid ssz")?;
            Ok((request.forkchoice_state.into(), payload_attrs(request.payload_attributes)?))
        }
        2 => {
            let request =
                ForkchoiceUpdatedV2Request::from_ssz_bytes(body).map_err(|_| "invalid ssz")?;
            Ok((request.forkchoice_state.into(), payload_attrs(request.payload_attributes)?))
        }
        3 => {
            let request =
                ForkchoiceUpdatedV3Request::from_ssz_bytes(body).map_err(|_| "invalid ssz")?;
            Ok((request.forkchoice_state.into(), payload_attrs(request.payload_attributes)?))
        }
        4 => {
            let request =
                ForkchoiceUpdatedV4Request::from_ssz_bytes(body).map_err(|_| "invalid ssz")?;
            Ok((request.forkchoice_state.into(), payload_attrs(request.payload_attributes)?))
        }
        _ => Err("unsupported forkchoice endpoint version"),
    }
}

fn payload_attrs<T>(attrs: Vec<T>) -> Result<Option<PayloadAttributes>, &'static str>
where
    T: Into<PayloadAttributes>,
{
    if attrs.len() > 1 {
        return Err("payload_attributes must contain at most one value")
    }

    Ok(attrs.into_iter().next().map(Into::into))
}

fn ssz_response<T: ssz::Encode>(value: T) -> HttpResponse {
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, OCTET_STREAM)
        .body(HttpBody::from(value.as_ssz_bytes()))
        .expect("valid response")
}

fn text_response(status: StatusCode, body: impl Into<String>) -> HttpResponse {
    Response::builder()
        .status(status)
        .header(CONTENT_TYPE, TEXT_PLAIN)
        .body(HttpBody::from(body.into()))
        .expect("valid response")
}

#[derive(Clone, Debug, ssz_derive::Decode, ssz_derive::Encode)]
struct NewPayloadV1Request {
    execution_payload: ExecutionPayloadV1,
}

#[derive(Clone, Debug, ssz_derive::Decode, ssz_derive::Encode)]
struct NewPayloadV2Request {
    execution_payload: ExecutionPayloadV2,
}

#[derive(Clone, Debug, ssz_derive::Decode, ssz_derive::Encode)]
struct NewPayloadV3Request {
    execution_payload: ExecutionPayloadV3,
    expected_blob_versioned_hashes: Vec<B256>,
    parent_beacon_block_root: B256,
}

#[derive(Clone, Debug, ssz_derive::Decode, ssz_derive::Encode)]
struct NewPayloadV4Request {
    execution_payload: ExecutionPayloadV3,
    expected_blob_versioned_hashes: Vec<B256>,
    parent_beacon_block_root: B256,
    execution_requests: Vec<Bytes>,
}

#[derive(Clone, Debug, ssz_derive::Decode, ssz_derive::Encode)]
struct NewPayloadV5Request {
    execution_payload: ExecutionPayloadV4,
    expected_blob_versioned_hashes: Vec<B256>,
    parent_beacon_block_root: B256,
    execution_requests: Vec<Bytes>,
}

#[derive(Clone, Debug, ssz_derive::Decode, ssz_derive::Encode)]
struct ForkchoiceUpdatedV1Request {
    forkchoice_state: ForkchoiceStateSsz,
    payload_attributes: Vec<PayloadAttributesV1Ssz>,
}

#[derive(Clone, Debug, ssz_derive::Decode, ssz_derive::Encode)]
struct ForkchoiceUpdatedV2Request {
    forkchoice_state: ForkchoiceStateSsz,
    payload_attributes: Vec<PayloadAttributesV2Ssz>,
}

#[derive(Clone, Debug, ssz_derive::Decode, ssz_derive::Encode)]
struct ForkchoiceUpdatedV3Request {
    forkchoice_state: ForkchoiceStateSsz,
    payload_attributes: Vec<PayloadAttributesV3Ssz>,
}

#[derive(Clone, Debug, ssz_derive::Decode, ssz_derive::Encode)]
struct ForkchoiceUpdatedV4Request {
    forkchoice_state: ForkchoiceStateSsz,
    payload_attributes: Vec<PayloadAttributesV4Ssz>,
}

#[derive(Clone, Copy, Debug, ssz_derive::Decode, ssz_derive::Encode)]
struct ForkchoiceStateSsz {
    head_block_hash: B256,
    safe_block_hash: B256,
    finalized_block_hash: B256,
}

impl From<ForkchoiceStateSsz> for ForkchoiceState {
    fn from(value: ForkchoiceStateSsz) -> Self {
        Self {
            head_block_hash: value.head_block_hash,
            safe_block_hash: value.safe_block_hash,
            finalized_block_hash: value.finalized_block_hash,
        }
    }
}

#[derive(Clone, Debug, ssz_derive::Decode, ssz_derive::Encode)]
struct PayloadAttributesV1Ssz {
    timestamp: u64,
    prev_randao: B256,
    suggested_fee_recipient: Address,
}

impl From<PayloadAttributesV1Ssz> for PayloadAttributes {
    fn from(value: PayloadAttributesV1Ssz) -> Self {
        Self {
            timestamp: value.timestamp,
            prev_randao: value.prev_randao,
            suggested_fee_recipient: value.suggested_fee_recipient,
            withdrawals: None,
            parent_beacon_block_root: None,
            slot_number: None,
        }
    }
}

#[derive(Clone, Debug, ssz_derive::Decode, ssz_derive::Encode)]
struct PayloadAttributesV2Ssz {
    timestamp: u64,
    prev_randao: B256,
    suggested_fee_recipient: Address,
    withdrawals: Vec<Withdrawal>,
}

impl From<PayloadAttributesV2Ssz> for PayloadAttributes {
    fn from(value: PayloadAttributesV2Ssz) -> Self {
        Self {
            timestamp: value.timestamp,
            prev_randao: value.prev_randao,
            suggested_fee_recipient: value.suggested_fee_recipient,
            withdrawals: Some(value.withdrawals),
            parent_beacon_block_root: None,
            slot_number: None,
        }
    }
}

#[derive(Clone, Debug, ssz_derive::Decode, ssz_derive::Encode)]
struct PayloadAttributesV3Ssz {
    timestamp: u64,
    prev_randao: B256,
    suggested_fee_recipient: Address,
    withdrawals: Vec<Withdrawal>,
    parent_beacon_block_root: B256,
}

impl From<PayloadAttributesV3Ssz> for PayloadAttributes {
    fn from(value: PayloadAttributesV3Ssz) -> Self {
        Self {
            timestamp: value.timestamp,
            prev_randao: value.prev_randao,
            suggested_fee_recipient: value.suggested_fee_recipient,
            withdrawals: Some(value.withdrawals),
            parent_beacon_block_root: Some(value.parent_beacon_block_root),
            slot_number: None,
        }
    }
}

#[derive(Clone, Debug, ssz_derive::Decode, ssz_derive::Encode)]
struct PayloadAttributesV4Ssz {
    timestamp: u64,
    prev_randao: B256,
    suggested_fee_recipient: Address,
    withdrawals: Vec<Withdrawal>,
    parent_beacon_block_root: B256,
    slot_number: u64,
}

impl From<PayloadAttributesV4Ssz> for PayloadAttributes {
    fn from(value: PayloadAttributesV4Ssz) -> Self {
        Self {
            timestamp: value.timestamp,
            prev_randao: value.prev_randao,
            suggested_fee_recipient: value.suggested_fee_recipient,
            withdrawals: Some(value.withdrawals),
            parent_beacon_block_root: Some(value.parent_beacon_block_root),
            slot_number: Some(value.slot_number),
        }
    }
}

#[derive(Clone, Debug, ssz_derive::Encode)]
struct PayloadStatusSsz {
    status: u8,
    latest_valid_hash: B256,
    validation_error: Vec<u8>,
}

impl From<PayloadStatus> for PayloadStatusSsz {
    fn from(value: PayloadStatus) -> Self {
        let validation_error =
            value.status.validation_error().map(|err| err.as_bytes().to_vec()).unwrap_or_default();

        Self {
            status: payload_status_code(&value.status),
            latest_valid_hash: value.latest_valid_hash.unwrap_or_default(),
            validation_error,
        }
    }
}

fn payload_status_code(status: &PayloadStatusEnum) -> u8 {
    match status {
        PayloadStatusEnum::Valid => 0,
        PayloadStatusEnum::Invalid { .. } => 1,
        PayloadStatusEnum::Syncing => 2,
        PayloadStatusEnum::Accepted => 3,
    }
}

#[derive(Clone, Debug, ssz_derive::Encode)]
struct ForkchoiceUpdatedSsz {
    payload_status: PayloadStatusSsz,
    payload_id: B64,
}

impl From<ForkchoiceUpdated> for ForkchoiceUpdatedSsz {
    fn from(value: ForkchoiceUpdated) -> Self {
        Self {
            payload_status: value.payload_status.into(),
            payload_id: value.payload_id.map(|id| id.0).unwrap_or_default(),
        }
    }
}
