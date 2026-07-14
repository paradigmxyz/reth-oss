//! HTTP SSZ transport proxy for the authenticated Engine API server.
//!
//! Implements the [EIP-8178] SSZ Engine API routes under `/engine/v1`.
//!
//! [EIP-8178]: https://eips.ethereum.org/EIPS/eip-8178

use crate::engine_ssz_containers::{
    BuiltPayloadAmsterdam, BuiltPayloadCancun, BuiltPayloadOsaka, BuiltPayloadParis,
    BuiltPayloadPrague, BuiltPayloadShanghai, ExecutionPayloadEnvelopeAmsterdam,
    ExecutionPayloadEnvelopeCancun, ExecutionPayloadEnvelopeOsaka, ExecutionPayloadEnvelopeParis,
    ExecutionPayloadEnvelopePrague, ExecutionPayloadEnvelopeShanghai, ForkchoiceUpdateAmsterdam,
    ForkchoiceUpdateCancun, ForkchoiceUpdateOsaka, ForkchoiceUpdateParis, ForkchoiceUpdatePrague,
    ForkchoiceUpdateResponse, ForkchoiceUpdateShanghai, Optional,
    PayloadStatus as EngineSszPayloadStatus,
};
use alloy_consensus::{Transaction, TxEnvelope};
use alloy_eips::{eip2718::Decodable2718, eip7685::RequestsOrHash};
use alloy_primitives::{Bytes, B128, B256, B64};
use alloy_rpc_types_engine::{
    CancunPayloadFields, ExecutionData, ExecutionPayload, ExecutionPayloadFieldV2,
    ExecutionPayloadSidecar, ForkchoiceState, PayloadAttributes, PayloadId, PraguePayloadFields,
};
use http_body_util::{BodyExt, LengthLimitError, Limited};
use jsonrpsee::server::{HttpBody, HttpRequest, HttpResponse};
use reth_chainspec::EthereumHardforks;
use reth_engine_primitives::{BeaconForkChoiceUpdateError, EngineApiValidator};
use reth_ethereum_engine_primitives::EthEngineTypes;
use reth_payload_primitives::EngineObjectValidationError;
use reth_provider::{BalProvider, BlockReader, HeaderProvider, StateProviderFactory};
use reth_rpc::EngineApi;
use reth_rpc_engine_api::EngineApiError;
use reth_transaction_pool::TransactionPool;
use ssz::Decode;
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::sync::RwLock;
use tower::{BoxError, Layer, Service};

const OCTET_STREAM: &str = "application/octet-stream";
const APPLICATION_JSON: &str = "application/json";
const PROBLEM_JSON: &str = "application/problem+json";
const CONTENT_TYPE: &str = "content-type";
const CONTENT_LENGTH: &str = "content-length";
const CACHE_CONTROL: &str = "cache-control";
const ETH_EXECUTION_VERSION: &str = "eth-execution-version";

const ERROR_INVALID_REQUEST: &str = "/engine-api/errors/invalid-request";
const ERROR_SSZ_DECODE: &str = "/engine-api/errors/ssz-decode-error";
const ERROR_INVALID_BODY: &str = "/engine-api/errors/invalid-body";
const ERROR_INVALID_ATTRIBUTES: &str = "/engine-api/errors/invalid-attributes";
const ERROR_INVALID_FORKCHOICE: &str = "/engine-api/errors/invalid-forkchoice";
const ERROR_REQUEST_TOO_LARGE: &str = "/engine-api/errors/request-too-large";
const ERROR_UNSUPPORTED_FORK: &str = "/engine-api/errors/unsupported-fork";
const ERROR_UNSUPPORTED_MEDIA_TYPE: &str = "/engine-api/errors/unsupported-media-type";
const ERROR_UNKNOWN_PAYLOAD: &str = "/engine-api/errors/unknown-payload";
const ERROR_INTERNAL: &str = "/engine-api/errors/internal";
const ERROR_NOT_FOUND: &str = "/engine-api/errors/not-found";

const STATUS_OK: u16 = 200;
const STATUS_BAD_REQUEST: u16 = 400;
const STATUS_NOT_FOUND: u16 = 404;
const STATUS_METHOD_NOT_ALLOWED: u16 = 405;
const STATUS_CONFLICT: u16 = 409;
const STATUS_PAYLOAD_TOO_LARGE: u16 = 413;
const STATUS_UNSUPPORTED_MEDIA_TYPE: u16 = 415;
const STATUS_UNPROCESSABLE_ENTITY: u16 = 422;
const STATUS_INTERNAL_SERVER_ERROR: u16 = 500;
const STATUS_SERVICE_UNAVAILABLE: u16 = 503;

const MAX_BLOB_LIMIT: usize = 128;
const MAX_PAYLOAD_BYTES: u64 = 64 * 1024 * 1024;

type EthEngineApi<Provider, Pool, Validator, ChainSpec> =
    EngineApi<Provider, EthEngineTypes, Pool, Validator, ChainSpec>;
type SharedEthEngineApi<Provider, Pool, Validator, ChainSpec> =
    Arc<RwLock<Option<EthEngineApi<Provider, Pool, Validator, ChainSpec>>>>;

/// Shared handle used by [`EngineSszProxyLayer`].
pub struct EngineSszProxyHandle<ChainSpec, Provider = (), Pool = (), Validator = ()> {
    engine_api: SharedEthEngineApi<Provider, Pool, Validator, ChainSpec>,
}

impl<C, Provider, Pool, Validator> Clone for EngineSszProxyHandle<C, Provider, Pool, Validator> {
    fn clone(&self) -> Self {
        Self { engine_api: self.engine_api.clone() }
    }
}

impl<C, Provider, Pool, Validator> std::fmt::Debug
    for EngineSszProxyHandle<C, Provider, Pool, Validator>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EngineSszProxyHandle").finish_non_exhaustive()
    }
}

impl<ChainSpec, Provider, Pool, Validator>
    EngineSszProxyHandle<ChainSpec, Provider, Pool, Validator>
{
    fn new() -> Self {
        Self { engine_api: Default::default() }
    }

    fn with_engine_api(engine_api: EthEngineApi<Provider, Pool, Validator, ChainSpec>) -> Self {
        Self { engine_api: Arc::new(RwLock::new(Some(engine_api))) }
    }

    /// Sets the Engine API implementation used by the proxy.
    pub async fn set_engine_api(
        &self,
        engine_api: EthEngineApi<Provider, Pool, Validator, ChainSpec>,
    ) {
        *self.engine_api.write().await = Some(engine_api);
    }

    /// Sets the Engine API implementation during synchronous launch wiring.
    pub fn set_engine_api_sync(
        &self,
        engine_api: EthEngineApi<Provider, Pool, Validator, ChainSpec>,
    ) {
        *self
            .engine_api
            .try_write()
            .expect("engine api handle should not be locked during launch") = Some(engine_api);
    }
}

impl<ChainSpec, Provider, Pool, Validator>
    EngineSszProxyHandle<ChainSpec, Provider, Pool, Validator>
{
    /// Returns the Engine API implementation used by the proxy.
    pub async fn engine_api(&self) -> Option<EthEngineApi<Provider, Pool, Validator, ChainSpec>> {
        self.engine_api.read().await.clone()
    }
}

/// A tower layer that intercepts SSZ Engine API routes under `/engine/v1`.
#[derive(Clone, Debug)]
pub struct EngineSszProxyLayer<ChainSpec, Provider = (), Pool = (), Validator = ()> {
    handle: EngineSszProxyHandle<ChainSpec, Provider, Pool, Validator>,
}

impl<ChainSpec, Provider, Pool, Validator>
    EngineSszProxyLayer<ChainSpec, Provider, Pool, Validator>
{
    /// Creates a new proxy layer and a handle for setting the engine after node launch.
    pub fn new() -> (Self, EngineSszProxyHandle<ChainSpec, Provider, Pool, Validator>) {
        let handle = EngineSszProxyHandle::new();
        (Self { handle: handle.clone() }, handle)
    }

    /// Creates a new proxy layer with an Engine API implementation.
    pub fn with_engine_api(
        engine_api: EthEngineApi<Provider, Pool, Validator, ChainSpec>,
    ) -> (Self, EngineSszProxyHandle<ChainSpec, Provider, Pool, Validator>) {
        let handle = EngineSszProxyHandle::with_engine_api(engine_api);
        (Self { handle: handle.clone() }, handle)
    }
}

impl<S, ChainSpec, Provider, Pool, Validator> Layer<S>
    for EngineSszProxyLayer<ChainSpec, Provider, Pool, Validator>
{
    type Service = EngineSszProxyService<S, ChainSpec, Provider, Pool, Validator>;

    fn layer(&self, inner: S) -> Self::Service {
        EngineSszProxyService { inner, handle: self.handle.clone() }
    }
}

/// The service produced by [`EngineSszProxyLayer`].
#[derive(Clone, Debug)]
pub struct EngineSszProxyService<S, ChainSpec, Provider = (), Pool = (), Validator = ()> {
    inner: S,
    handle: EngineSszProxyHandle<ChainSpec, Provider, Pool, Validator>,
}

impl<S, ChainSpec, Provider, Pool, Validator> Service<HttpRequest>
    for EngineSszProxyService<S, ChainSpec, Provider, Pool, Validator>
where
    S: Service<HttpRequest, Response = HttpResponse, Error = BoxError> + Send + Clone,
    S::Future: Send + 'static,
    Provider: HeaderProvider + BlockReader + StateProviderFactory + BalProvider + 'static,
    Pool: TransactionPool + 'static,
    Validator: EngineApiValidator<EthEngineTypes>,
    ChainSpec: EthereumHardforks + Send + Sync + 'static,
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

async fn handle_engine_ssz_request<ChainSpec, Provider, Pool, Validator>(
    handle: EngineSszProxyHandle<ChainSpec, Provider, Pool, Validator>,
    request: HttpRequest,
) -> HttpResponse
where
    Provider: HeaderProvider + BlockReader + StateProviderFactory + BalProvider + 'static,
    Pool: TransactionPool + 'static,
    Validator: EngineApiValidator<EthEngineTypes>,
    ChainSpec: EthereumHardforks + Send + Sync + 'static,
{
    let method = request.method().as_str().to_owned();
    let path = request.uri().path().to_owned();
    let Some(endpoint) = parse_engine_path(&path) else {
        return problem_response(
            STATUS_NOT_FOUND,
            ERROR_NOT_FOUND,
            Some("unknown engine ssz endpoint".into()),
        )
    };

    match endpoint {
        EngineSszEndpoint::Capabilities => {
            if method != "GET" {
                return method_not_allowed_response()
            }
            handle_capabilities()
        }
        EngineSszEndpoint::Identity => {
            if method != "GET" {
                return method_not_allowed_response()
            }
            let Some(engine_api) = handle.engine_api().await else {
                return internal_response(STATUS_SERVICE_UNAVAILABLE, "engine api unavailable")
            };
            handle_identity(engine_api)
        }
        EngineSszEndpoint::NewPayload => {
            if method != "POST" {
                return method_not_allowed_response()
            }
            let Some(fork) = request_fork(&request) else {
                return problem_response(STATUS_BAD_REQUEST, ERROR_UNSUPPORTED_FORK, None)
            };
            let body = match read_ssz_body(request, MAX_PAYLOAD_BYTES).await {
                Ok(body) => body,
                Err(response) => return response,
            };
            let Some(engine_api) = handle.engine_api().await else {
                return internal_response(STATUS_SERVICE_UNAVAILABLE, "engine api unavailable")
            };
            handle_new_payload(engine_api, fork, &body).await
        }
        EngineSszEndpoint::GetPayload(payload_id) => {
            if method != "GET" {
                return method_not_allowed_response()
            }
            let Ok(payload_id) = payload_id else {
                return problem_response(
                    STATUS_BAD_REQUEST,
                    ERROR_INVALID_REQUEST,
                    Some("invalid payload id".into()),
                )
            };
            let Some(fork) = request_fork(&request) else {
                return problem_response(STATUS_BAD_REQUEST, ERROR_UNSUPPORTED_FORK, None)
            };
            let Some(engine_api) = handle.engine_api().await else {
                return internal_response(STATUS_SERVICE_UNAVAILABLE, "engine api unavailable")
            };
            handle_get_payload(engine_api, fork, payload_id).await
        }
        EngineSszEndpoint::Forkchoice => {
            if method != "POST" {
                return method_not_allowed_response()
            }
            let Some(fork) = request_fork(&request) else {
                return problem_response(STATUS_BAD_REQUEST, ERROR_UNSUPPORTED_FORK, None)
            };
            let body = match read_ssz_body(request, MAX_PAYLOAD_BYTES).await {
                Ok(body) => body,
                Err(response) => return response,
            };
            let Some(engine_api) = handle.engine_api().await else {
                return internal_response(STATUS_SERVICE_UNAVAILABLE, "engine api unavailable")
            };
            handle_forkchoice_updated(engine_api, fork, &body).await
        }
        EngineSszEndpoint::Blobs(version) => {
            if method != "POST" {
                return method_not_allowed_response()
            }
            let body = match read_ssz_body(request, MAX_PAYLOAD_BYTES).await {
                Ok(body) => body,
                Err(response) => return response,
            };
            let Some(engine_api) = handle.engine_api().await else {
                return internal_response(STATUS_SERVICE_UNAVAILABLE, "engine api unavailable")
            };
            handle_get_blobs(engine_api, version, &body).await
        }
    }
}

fn request_fork(request: &HttpRequest) -> Option<EngineSszFork> {
    request.headers().get(ETH_EXECUTION_VERSION)?.to_str().ok()?.parse().ok()
}

fn parse_engine_path(path: &str) -> Option<EngineSszEndpoint> {
    let mut segments = path.trim_start_matches('/').split('/');
    match (segments.next(), segments.next(), segments.next(), segments.next(), segments.next()) {
        (Some("engine"), Some("v1"), Some("capabilities"), None, None) => {
            Some(EngineSszEndpoint::Capabilities)
        }
        (Some("engine"), Some("v1"), Some("identity"), None, None) => {
            Some(EngineSszEndpoint::Identity)
        }
        (Some("engine"), Some("v1"), Some("payloads"), None, None) => {
            Some(EngineSszEndpoint::NewPayload)
        }
        (Some("engine"), Some("v1"), Some("payloads"), Some(payload_id), None) => {
            let payload_id = payload_id.parse::<B64>().map(PayloadId::from);
            Some(EngineSszEndpoint::GetPayload(payload_id))
        }
        (Some("engine"), Some("v1"), Some("forkchoice"), None, None) => {
            Some(EngineSszEndpoint::Forkchoice)
        }
        (Some("engine"), Some("v1"), Some("blobs"), version, None) => {
            Some(EngineSszEndpoint::Blobs(parse_method_version(version?)?))
        }
        _ => None,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EngineSszEndpoint {
    Capabilities,
    Identity,
    NewPayload,
    GetPayload(Result<PayloadId, <B64 as std::str::FromStr>::Err>),
    Forkchoice,
    Blobs(u8),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EngineSszFork {
    Paris,
    Shanghai,
    Cancun,
    Prague,
    Osaka,
    Amsterdam,
}

impl EngineSszFork {
    const fn payloads_version(self) -> u8 {
        match self {
            Self::Paris => 1,
            Self::Shanghai => 2,
            Self::Cancun => 3,
            Self::Prague | Self::Osaka => 4,
            Self::Amsterdam => 5,
        }
    }

    const fn forkchoice_version(self) -> u8 {
        match self {
            Self::Paris => 1,
            Self::Shanghai => 2,
            Self::Cancun | Self::Prague | Self::Osaka => 3,
            Self::Amsterdam => 4,
        }
    }
}

impl std::str::FromStr for EngineSszFork {
    type Err = ();

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "paris" => Ok(Self::Paris),
            "shanghai" => Ok(Self::Shanghai),
            "cancun" => Ok(Self::Cancun),
            "prague" => Ok(Self::Prague),
            "osaka" => Ok(Self::Osaka),
            "amsterdam" => Ok(Self::Amsterdam),
            _ => Err(()),
        }
    }
}

fn fork_at_timestamp<ChainSpec: EthereumHardforks>(
    chain_spec: &ChainSpec,
    timestamp: u64,
) -> EngineSszFork {
    if chain_spec.is_amsterdam_active_at_timestamp(timestamp) {
        EngineSszFork::Amsterdam
    } else if chain_spec.is_osaka_active_at_timestamp(timestamp) {
        EngineSszFork::Osaka
    } else if chain_spec.is_prague_active_at_timestamp(timestamp) {
        EngineSszFork::Prague
    } else if chain_spec.is_cancun_active_at_timestamp(timestamp) {
        EngineSszFork::Cancun
    } else if chain_spec.is_shanghai_active_at_timestamp(timestamp) {
        EngineSszFork::Shanghai
    } else {
        EngineSszFork::Paris
    }
}

fn ensure_fork_at_timestamp<ChainSpec: EthereumHardforks>(
    chain_spec: &ChainSpec,
    fork: EngineSszFork,
    timestamp: u64,
) -> Result<(), HttpResponse> {
    if fork_at_timestamp(chain_spec, timestamp) == fork {
        Ok(())
    } else {
        Err(problem_response(STATUS_BAD_REQUEST, ERROR_UNSUPPORTED_FORK, None))
    }
}

fn parse_method_version(version: &str) -> Option<u8> {
    version.strip_prefix('v')?.parse().ok().filter(|version| (1..=4).contains(version))
}

async fn read_ssz_body(request: HttpRequest, max_bytes: u64) -> Result<Vec<u8>, HttpResponse> {
    let content_type = request
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .map(str::trim);
    if content_type != Some(OCTET_STREAM) {
        return Err(problem_response(
            STATUS_UNSUPPORTED_MEDIA_TYPE,
            ERROR_UNSUPPORTED_MEDIA_TYPE,
            None,
        ))
    }

    let advertised_length = if let Some(value) = request.headers().get(CONTENT_LENGTH) {
        let Some(value) = value.to_str().ok().and_then(|value| value.parse::<u64>().ok()) else {
            return Err(problem_response(
                STATUS_BAD_REQUEST,
                ERROR_INVALID_REQUEST,
                Some("invalid content-length header".into()),
            ))
        };
        if value > max_bytes {
            return Err(problem_response(STATUS_PAYLOAD_TOO_LARGE, ERROR_REQUEST_TOO_LARGE, None))
        }
        Some(value)
    } else {
        None
    };

    let Ok(limit) = usize::try_from(max_bytes) else {
        return Err(internal_response(STATUS_INTERNAL_SERVER_ERROR, "invalid body limit"))
    };
    let body = match Limited::new(request.into_body(), limit).collect().await {
        Ok(body) => body.to_bytes().to_vec(),
        Err(err) if err.downcast_ref::<LengthLimitError>().is_some() => {
            return Err(problem_response(STATUS_PAYLOAD_TOO_LARGE, ERROR_REQUEST_TOO_LARGE, None))
        }
        Err(err) => {
            return Err(problem_response(
                STATUS_BAD_REQUEST,
                ERROR_INVALID_REQUEST,
                Some(err.to_string()),
            ))
        }
    };

    if advertised_length.is_some_and(|length| length != body.len() as u64) {
        return Err(problem_response(
            STATUS_BAD_REQUEST,
            ERROR_INVALID_REQUEST,
            Some("content-length does not match request body".into()),
        ))
    }
    Ok(body)
}

fn engine_api_error_response(err: EngineApiError) -> HttpResponse {
    let (status, problem_type) = match &err {
        EngineApiError::UnknownPayload => (STATUS_NOT_FOUND, ERROR_UNKNOWN_PAYLOAD),
        EngineApiError::PayloadRequestTooLarge { .. } |
        EngineApiError::BlobRequestTooLarge { .. } => {
            (STATUS_PAYLOAD_TOO_LARGE, ERROR_REQUEST_TOO_LARGE)
        }
        EngineApiError::InvalidBodiesRange { .. } => (STATUS_BAD_REQUEST, ERROR_INVALID_REQUEST),
        EngineApiError::EngineObjectValidationError(error) => match error {
            EngineObjectValidationError::UnsupportedFork => {
                (STATUS_BAD_REQUEST, ERROR_UNSUPPORTED_FORK)
            }
            EngineObjectValidationError::Payload(_) => {
                (STATUS_UNPROCESSABLE_ENTITY, ERROR_INVALID_BODY)
            }
            EngineObjectValidationError::PayloadAttributes(_) => {
                (STATUS_UNPROCESSABLE_ENTITY, ERROR_INVALID_ATTRIBUTES)
            }
            EngineObjectValidationError::InvalidParams(_) => {
                (STATUS_BAD_REQUEST, ERROR_INVALID_REQUEST)
            }
        },
        EngineApiError::ForkChoiceUpdate(error) => match error {
            BeaconForkChoiceUpdateError::ForkchoiceUpdateError(error) => match error {
                alloy_rpc_types_engine::ForkchoiceUpdateError::UpdatedInvalidPayloadAttributes => {
                    (STATUS_UNPROCESSABLE_ENTITY, ERROR_INVALID_ATTRIBUTES)
                }
                alloy_rpc_types_engine::ForkchoiceUpdateError::InvalidState |
                alloy_rpc_types_engine::ForkchoiceUpdateError::UnknownFinalBlock => {
                    (STATUS_CONFLICT, ERROR_INVALID_FORKCHOICE)
                }
                _ => (STATUS_INTERNAL_SERVER_ERROR, ERROR_INTERNAL),
            },
            BeaconForkChoiceUpdateError::EngineUnavailable |
            BeaconForkChoiceUpdateError::Internal(_) => {
                (STATUS_INTERNAL_SERVER_ERROR, ERROR_INTERNAL)
            }
        },
        EngineApiError::UnexpectedRequestsHash => (STATUS_UNPROCESSABLE_ENTITY, ERROR_INVALID_BODY),
        EngineApiError::TerminalBlockHash { .. } |
        EngineApiError::NewPayload(_) |
        EngineApiError::Internal(_) |
        EngineApiError::GetPayloadError(_) |
        EngineApiError::Other(_) => (STATUS_INTERNAL_SERVER_ERROR, ERROR_INTERNAL),
    };
    problem_response(status, problem_type, Some(err.to_string()))
}

fn method_not_allowed_response() -> HttpResponse {
    problem_response(
        STATUS_METHOD_NOT_ALLOWED,
        ERROR_INVALID_REQUEST,
        Some("method not allowed".into()),
    )
}

fn internal_response(status: u16, detail: impl Into<String>) -> HttpResponse {
    problem_response(status, ERROR_INTERNAL, Some(detail.into()))
}

enum RequestDecodeError {
    Ssz(&'static str),
    InvalidBody(&'static str),
}

impl From<ssz::DecodeError> for RequestDecodeError {
    fn from(_: ssz::DecodeError) -> Self {
        Self::Ssz("invalid ssz")
    }
}

fn request_decode_response(error: RequestDecodeError) -> HttpResponse {
    let (status, problem_type, detail) = match error {
        RequestDecodeError::Ssz(detail) => (STATUS_BAD_REQUEST, ERROR_SSZ_DECODE, detail),
        RequestDecodeError::InvalidBody(detail) => {
            (STATUS_UNPROCESSABLE_ENTITY, ERROR_INVALID_BODY, detail)
        }
    };
    problem_response(status, problem_type, Some(detail.into()))
}

fn handle_capabilities() -> HttpResponse {
    json_response(serde_json::json!({
        "supported_forks": ["paris", "shanghai", "cancun", "prague", "osaka", "amsterdam"],
        "fork_scoped_endpoints": ["payloads", "forkchoice", "bodies"],
        "independently_versioned": {
            "blobs": ["v1", "v2", "v3", "v4"],
        },
        "unscoped_endpoints": ["capabilities", "identity"],
        "limits": {
            "bodies.max_count": 128,
            "blobs.max_versioned_hashes": MAX_BLOB_LIMIT,
            "payload.max_bytes": MAX_PAYLOAD_BYTES,
        },
    }))
}

fn handle_identity<Provider, Pool, Validator, ChainSpec>(
    engine_api: EthEngineApi<Provider, Pool, Validator, ChainSpec>,
) -> HttpResponse
where
    Provider: HeaderProvider + BlockReader + StateProviderFactory + BalProvider + 'static,
    Pool: TransactionPool + 'static,
    Validator: EngineApiValidator<EthEngineTypes>,
    ChainSpec: EthereumHardforks + Send + Sync + 'static,
{
    json_response(vec![engine_api.client_version().clone()])
}

async fn handle_get_payload<Provider, Pool, Validator, ChainSpec>(
    engine_api: EthEngineApi<Provider, Pool, Validator, ChainSpec>,
    fork: EngineSszFork,
    payload_id: PayloadId,
) -> HttpResponse
where
    Provider: HeaderProvider + BlockReader + StateProviderFactory + BalProvider + 'static,
    Pool: TransactionPool + 'static,
    Validator: EngineApiValidator<EthEngineTypes>,
    ChainSpec: EthereumHardforks + Send + Sync + 'static,
{
    match fork {
        EngineSszFork::Paris => match engine_api.get_payload_v2_metered(payload_id).await {
            Ok(payload) => {
                let block_value = payload.block_value;
                match payload.execution_payload {
                    ExecutionPayloadFieldV2::V1(payload) => {
                        if let Err(response) = ensure_fork_at_timestamp(
                            engine_api.chain_spec(),
                            fork,
                            payload.timestamp(),
                        ) {
                            response
                        } else {
                            get_payload_response(BuiltPayloadParis { payload, block_value })
                        }
                    }
                    ExecutionPayloadFieldV2::V2(_) => {
                        internal_response(STATUS_INTERNAL_SERVER_ERROR, "unexpected payload fork")
                    }
                }
            }
            Err(err) => engine_api_error_response(err),
        },
        EngineSszFork::Shanghai => match engine_api.get_payload_v2_metered(payload_id).await {
            Ok(payload) => match BuiltPayloadShanghai::try_from(payload) {
                Ok(payload) => {
                    if let Err(response) = ensure_fork_at_timestamp(
                        engine_api.chain_spec(),
                        fork,
                        payload.payload.timestamp(),
                    ) {
                        response
                    } else {
                        get_payload_response(payload)
                    }
                }
                Err(err) => internal_response(STATUS_INTERNAL_SERVER_ERROR, err.to_string()),
            },
            Err(err) => engine_api_error_response(err),
        },
        EngineSszFork::Cancun => match engine_api.get_payload_v3_metered(payload_id).await {
            Ok(payload) => {
                let payload = BuiltPayloadCancun::from(payload);
                if let Err(response) = ensure_fork_at_timestamp(
                    engine_api.chain_spec(),
                    fork,
                    payload.execution_payload.timestamp(),
                ) {
                    response
                } else {
                    get_payload_response(payload)
                }
            }
            Err(err) => engine_api_error_response(err),
        },
        EngineSszFork::Prague => match engine_api.get_payload_v4_metered(payload_id).await {
            Ok(payload) => {
                let payload = BuiltPayloadPrague::from(payload);
                if let Err(response) = ensure_fork_at_timestamp(
                    engine_api.chain_spec(),
                    fork,
                    payload.payload.timestamp(),
                ) {
                    response
                } else {
                    get_payload_response(payload)
                }
            }
            Err(err) => engine_api_error_response(err),
        },
        EngineSszFork::Osaka => match engine_api.get_payload_v5_metered(payload_id).await {
            Ok(payload) => {
                let payload = BuiltPayloadOsaka::from(payload);
                if let Err(response) = ensure_fork_at_timestamp(
                    engine_api.chain_spec(),
                    fork,
                    payload.payload.timestamp(),
                ) {
                    response
                } else {
                    get_payload_response(payload)
                }
            }
            Err(err) => engine_api_error_response(err),
        },
        EngineSszFork::Amsterdam => match engine_api.get_payload_v6_metered(payload_id).await {
            Ok(payload) => {
                let payload = BuiltPayloadAmsterdam::from(payload);
                if let Err(response) = ensure_fork_at_timestamp(
                    engine_api.chain_spec(),
                    fork,
                    payload.payload.timestamp(),
                ) {
                    response
                } else {
                    get_payload_response(payload)
                }
            }
            Err(err) => engine_api_error_response(err),
        },
    }
}

async fn handle_new_payload<Provider, Pool, Validator, ChainSpec>(
    engine_api: EthEngineApi<Provider, Pool, Validator, ChainSpec>,
    fork: EngineSszFork,
    body: &[u8],
) -> HttpResponse
where
    Provider: HeaderProvider + BlockReader + StateProviderFactory + BalProvider + 'static,
    Pool: TransactionPool + 'static,
    Validator: EngineApiValidator<EthEngineTypes>,
    ChainSpec: EthereumHardforks + Send + Sync + 'static,
{
    let payload = match decode_new_payload_request(fork, body) {
        Ok(payload) => payload,
        Err(err) => return request_decode_response(err),
    };
    if let Err(response) =
        ensure_fork_at_timestamp(engine_api.chain_spec(), fork, payload.payload.timestamp())
    {
        return response
    }

    let response = match fork.payloads_version() {
        1 => engine_api.new_payload_v1(payload).await,
        2 => engine_api.new_payload_v2(payload).await,
        3 => engine_api.new_payload_v3(payload).await,
        4 => engine_api.new_payload_v4(payload).await,
        5 => engine_api.new_payload_v5(payload).await,
        _ => return problem_response(STATUS_BAD_REQUEST, ERROR_UNSUPPORTED_FORK, None),
    };

    match response {
        Ok(status) => match EngineSszPayloadStatus::try_from(status) {
            Ok(status) => ssz_response(status),
            Err(err) => internal_response(STATUS_INTERNAL_SERVER_ERROR, err.to_string()),
        },
        Err(err) => engine_api_error_response(err),
    }
}

async fn handle_forkchoice_updated<Provider, Pool, Validator, ChainSpec>(
    engine_api: EthEngineApi<Provider, Pool, Validator, ChainSpec>,
    fork: EngineSszFork,
    body: &[u8],
) -> HttpResponse
where
    Provider: HeaderProvider + BlockReader + StateProviderFactory + BalProvider + 'static,
    Pool: TransactionPool + 'static,
    Validator: EngineApiValidator<EthEngineTypes>,
    ChainSpec: EthereumHardforks + Send + Sync + 'static,
{
    let (state, attrs, custody_columns) = match decode_forkchoice_request(fork, body) {
        Ok(request) => request,
        Err(err) => return request_decode_response(err),
    };
    if let Some(attrs) = attrs.as_ref() {
        if let Err(response) =
            ensure_fork_at_timestamp(engine_api.chain_spec(), fork, attrs.timestamp)
        {
            return response
        }
    }

    let response = match fork.forkchoice_version() {
        1 => engine_api.fork_choice_updated_v1_metered(state, attrs).await,
        2 => engine_api.fork_choice_updated_v2_metered(state, attrs).await,
        3 => engine_api.fork_choice_updated_v3_metered(state, attrs).await,
        4 => engine_api.fork_choice_updated_v4_metered(state, attrs, custody_columns).await,
        _ => return problem_response(STATUS_BAD_REQUEST, ERROR_UNSUPPORTED_FORK, None),
    };

    match response {
        Ok(updated) => match ForkchoiceUpdateResponse::try_from(updated) {
            Ok(updated) => ssz_response(updated),
            Err(err) => internal_response(STATUS_INTERNAL_SERVER_ERROR, err.to_string()),
        },
        Err(err) => engine_api_error_response(err),
    }
}

/// Handles SSZ `engine_getBlobsV*` requests with the node's blob store.
async fn handle_get_blobs<ChainSpec, Provider, Pool, Validator>(
    engine_api: EthEngineApi<Provider, Pool, Validator, ChainSpec>,
    version: u8,
    body: &[u8],
) -> HttpResponse
where
    Provider: HeaderProvider + BlockReader + StateProviderFactory + BalProvider + 'static,
    Pool: TransactionPool + 'static,
    Validator: EngineApiValidator<EthEngineTypes>,
    ChainSpec: EthereumHardforks + Send + Sync + 'static,
{
    match version {
        1 => {
            let hashes = match decode_blob_hashes_request(body) {
                Ok(hashes) => hashes,
                Err(err) => return request_decode_response(err),
            };
            match engine_api.get_blobs_v1_metered(hashes) {
                Ok(response) => ssz_response(response),
                Err(err) => engine_api_error_response(err),
            }
        }
        2 => {
            let hashes = match decode_blob_hashes_request(body) {
                Ok(hashes) => hashes,
                Err(err) => return request_decode_response(err),
            };
            match engine_api.get_blobs_v2_metered(hashes) {
                Ok(Some(response)) => ssz_response(response),
                Ok(None) => no_content_response(),
                Err(err) => engine_api_error_response(err),
            }
        }
        3 => {
            let hashes = match decode_blob_hashes_request(body) {
                Ok(hashes) => hashes,
                Err(err) => return request_decode_response(err),
            };
            match engine_api.get_blobs_v3_metered(hashes) {
                Ok(Some(response)) => ssz_response(response),
                Ok(None) => no_content_response(),
                Err(err) => engine_api_error_response(err),
            }
        }
        4 => {
            let (hashes, indices_bitarray) = match decode_blob_cells_request(body) {
                Ok(request) => request,
                Err(err) => return request_decode_response(err),
            };
            match engine_api.get_blobs_v4_metered(hashes, indices_bitarray) {
                Ok(Some(response)) => ssz_response(response),
                Ok(None) => no_content_response(),
                Err(err) => engine_api_error_response(err),
            }
        }
        _ => problem_response(
            STATUS_NOT_FOUND,
            ERROR_NOT_FOUND,
            Some("unsupported blobs endpoint version".into()),
        ),
    }
}

/// Decodes the common getBlobs request container with only versioned hashes.
fn decode_blob_hashes_request(body: &[u8]) -> Result<Vec<B256>, RequestDecodeError> {
    Vec::<B256>::from_ssz_bytes(body).map_err(Into::into)
}

/// Decodes the Amsterdam getBlobs request container with hashes and a cell index mask.
fn decode_blob_cells_request(body: &[u8]) -> Result<(Vec<B256>, B128), RequestDecodeError> {
    <(Vec<B256>, B128) as ssz::Decode>::from_ssz_bytes(body).map_err(Into::into)
}

fn decode_new_payload_request(
    fork: EngineSszFork,
    body: &[u8],
) -> Result<ExecutionData, RequestDecodeError> {
    match fork {
        EngineSszFork::Paris => {
            let ExecutionPayloadEnvelopeParis { payload: execution_payload } =
                ExecutionPayloadEnvelopeParis::from_ssz_bytes(body)?;
            Ok(ExecutionData::new(execution_payload.into(), ExecutionPayloadSidecar::none()))
        }
        EngineSszFork::Shanghai => {
            let ExecutionPayloadEnvelopeShanghai { payload: execution_payload } =
                ExecutionPayloadEnvelopeShanghai::from_ssz_bytes(body)?;
            Ok(ExecutionData::new(execution_payload.into(), ExecutionPayloadSidecar::none()))
        }
        EngineSszFork::Cancun => {
            let ExecutionPayloadEnvelopeCancun {
                payload: execution_payload,
                parent_beacon_block_root,
            } = ExecutionPayloadEnvelopeCancun::from_ssz_bytes(body)?;
            let versioned_hashes = calculate_versioned_hashes(
                &execution_payload.payload_inner.payload_inner.transactions,
            )?;
            let sidecar = ExecutionPayloadSidecar::v3(CancunPayloadFields {
                parent_beacon_block_root,
                versioned_hashes,
            });
            Ok(ExecutionData::new(execution_payload.into(), sidecar))
        }
        EngineSszFork::Prague => {
            let ExecutionPayloadEnvelopePrague {
                payload: execution_payload,
                parent_beacon_block_root,
                execution_requests,
            } = ExecutionPayloadEnvelopePrague::from_ssz_bytes(body)?;
            let versioned_hashes = calculate_versioned_hashes(
                &execution_payload.payload_inner.payload_inner.transactions,
            )?;
            let sidecar = ExecutionPayloadSidecar::v4(
                CancunPayloadFields { parent_beacon_block_root, versioned_hashes },
                PraguePayloadFields::new(RequestsOrHash::Requests(execution_requests)),
            );
            Ok(ExecutionData::new(execution_payload.into(), sidecar))
        }
        EngineSszFork::Osaka => {
            let ExecutionPayloadEnvelopeOsaka {
                payload: execution_payload,
                parent_beacon_block_root,
                execution_requests,
            } = ExecutionPayloadEnvelopeOsaka::from_ssz_bytes(body)?;
            let versioned_hashes = calculate_versioned_hashes(
                &execution_payload.payload_inner.payload_inner.transactions,
            )?;
            let sidecar = ExecutionPayloadSidecar::v4(
                CancunPayloadFields { parent_beacon_block_root, versioned_hashes },
                PraguePayloadFields::new(RequestsOrHash::Requests(execution_requests)),
            );
            Ok(ExecutionData::new(execution_payload.into(), sidecar))
        }
        EngineSszFork::Amsterdam => {
            let ExecutionPayloadEnvelopeAmsterdam {
                payload: execution_payload,
                parent_beacon_block_root,
                execution_requests,
            } = ExecutionPayloadEnvelopeAmsterdam::from_ssz_bytes(body)?;
            let versioned_hashes = calculate_versioned_hashes(
                &execution_payload.payload_inner.payload_inner.payload_inner.transactions,
            )?;
            let sidecar = ExecutionPayloadSidecar::v4(
                CancunPayloadFields { parent_beacon_block_root, versioned_hashes },
                PraguePayloadFields::new(RequestsOrHash::Requests(execution_requests)),
            );
            Ok(ExecutionData::new(ExecutionPayload::V4(execution_payload), sidecar))
        }
    }
}

fn calculate_versioned_hashes(transactions: &[Bytes]) -> Result<Vec<B256>, RequestDecodeError> {
    let mut versioned_hashes = Vec::new();
    for transaction in transactions {
        let transaction = TxEnvelope::decode_2718_exact(transaction.as_ref())
            .map_err(|_| RequestDecodeError::InvalidBody("invalid transaction"))?;
        if let Some(hashes) = transaction.blob_versioned_hashes() {
            versioned_hashes.extend_from_slice(hashes);
        }
    }

    Ok(versioned_hashes)
}

fn decode_forkchoice_request(
    fork: EngineSszFork,
    body: &[u8],
) -> Result<(ForkchoiceState, Option<PayloadAttributes>, Option<B128>), RequestDecodeError> {
    match fork {
        EngineSszFork::Paris => {
            let ForkchoiceUpdateParis { forkchoice_state, payload_attributes } =
                ForkchoiceUpdateParis::from_ssz_bytes(body)?;
            Ok((forkchoice_state, optional_attrs(payload_attributes), None))
        }
        EngineSszFork::Shanghai => {
            let ForkchoiceUpdateShanghai { forkchoice_state, payload_attributes } =
                ForkchoiceUpdateShanghai::from_ssz_bytes(body)?;
            Ok((forkchoice_state, optional_attrs(payload_attributes), None))
        }
        EngineSszFork::Cancun => {
            let ForkchoiceUpdateCancun { forkchoice_state, payload_attributes } =
                ForkchoiceUpdateCancun::from_ssz_bytes(body)?;
            Ok((forkchoice_state, optional_attrs(payload_attributes), None))
        }
        EngineSszFork::Prague => {
            let ForkchoiceUpdatePrague { forkchoice_state, payload_attributes } =
                ForkchoiceUpdatePrague::from_ssz_bytes(body)?;
            Ok((forkchoice_state, optional_attrs(payload_attributes), None))
        }
        EngineSszFork::Osaka => {
            let ForkchoiceUpdateOsaka { forkchoice_state, payload_attributes } =
                ForkchoiceUpdateOsaka::from_ssz_bytes(body)?;
            Ok((forkchoice_state, optional_attrs(payload_attributes), None))
        }
        EngineSszFork::Amsterdam => {
            let ForkchoiceUpdateAmsterdam { forkchoice_state, payload_attributes, custody_columns } =
                ForkchoiceUpdateAmsterdam::from_ssz_bytes(body)?;
            Ok((
                forkchoice_state,
                optional_attrs(payload_attributes),
                custody_columns.into_option(),
            ))
        }
    }
}

fn optional_attrs<T>(attrs: Optional<T>) -> Option<PayloadAttributes>
where
    T: Into<PayloadAttributes>,
{
    attrs.into_option().map(Into::into)
}

fn ssz_response<T: ssz::Encode>(value: T) -> HttpResponse {
    HttpResponse::builder()
        .status(STATUS_OK)
        .header(CONTENT_TYPE, OCTET_STREAM)
        .body(HttpBody::from(value.as_ssz_bytes()))
        .expect("valid response")
}

fn get_payload_response<T: ssz::Encode>(value: T) -> HttpResponse {
    HttpResponse::builder()
        .status(STATUS_OK)
        .header(CONTENT_TYPE, OCTET_STREAM)
        .header(CACHE_CONTROL, "no-store")
        .body(HttpBody::from(value.as_ssz_bytes()))
        .expect("valid response")
}

fn json_response<T: serde::Serialize>(value: T) -> HttpResponse {
    let Ok(body) = serde_json::to_string(&value) else {
        return internal_response(STATUS_INTERNAL_SERVER_ERROR, "failed to encode json")
    };

    HttpResponse::builder()
        .status(STATUS_OK)
        .header(CONTENT_TYPE, APPLICATION_JSON)
        .body(HttpBody::from(body))
        .expect("valid response")
}

fn no_content_response() -> HttpResponse {
    HttpResponse::builder().status(204).body(HttpBody::empty()).expect("valid response")
}

fn problem_response(
    status: u16,
    problem_type: &'static str,
    detail: Option<String>,
) -> HttpResponse {
    let body = match detail {
        Some(detail) => serde_json::json!({ "type": problem_type, "detail": detail }),
        None => serde_json::json!({ "type": problem_type }),
    };
    HttpResponse::builder()
        .status(status)
        .header(CONTENT_TYPE, PROBLEM_JSON)
        .body(HttpBody::from(body.to_string()))
        .expect("valid response")
}

#[cfg(test)]
mod tests {
    use super::*;
    use ssz::Encode;

    #[test]
    fn parses_capabilities_endpoint() {
        let endpoint = parse_engine_path("/engine/v1/capabilities").unwrap();
        assert_eq!(endpoint, EngineSszEndpoint::Capabilities);
    }

    #[test]
    fn parses_identity_endpoint() {
        let endpoint = parse_engine_path("/engine/v1/identity").unwrap();
        assert_eq!(endpoint, EngineSszEndpoint::Identity);
    }

    #[test]
    fn parses_fork_scoped_payload_endpoint() {
        let endpoint = parse_engine_path("/engine/v1/payloads").unwrap();
        assert_eq!(endpoint, EngineSszEndpoint::NewPayload);
    }

    #[test]
    fn parses_fork_scoped_get_payload_endpoint() {
        let endpoint = parse_engine_path("/engine/v1/payloads/0x0000000000000001").unwrap();
        assert_eq!(
            endpoint,
            EngineSszEndpoint::GetPayload(Ok(PayloadId::new([0, 0, 0, 0, 0, 0, 0, 1])))
        );
    }

    #[test]
    fn matches_malformed_get_payload_endpoint() {
        assert!(matches!(
            parse_engine_path("/engine/v1/payloads/0x01"),
            Some(EngineSszEndpoint::GetPayload(Err(_)))
        ));
    }

    #[test]
    fn parses_fork_scoped_forkchoice_endpoint() {
        let endpoint = parse_engine_path("/engine/v1/forkchoice").unwrap();
        assert_eq!(endpoint, EngineSszEndpoint::Forkchoice);
    }

    #[test]
    fn rejects_legacy_version_scoped_endpoint() {
        assert!(parse_engine_path("/engine/v4/payloads").is_none());
    }

    #[test]
    fn decodes_top_level_blob_hashes_request() {
        let hashes = vec![B256::ZERO, B256::with_last_byte(1)];
        let decoded = decode_blob_hashes_request(&hashes.as_ssz_bytes()).unwrap();
        assert_eq!(decoded, hashes);
    }

    #[test]
    fn decodes_forkchoice_v4_with_custody_columns() {
        let forkchoice_state = ForkchoiceState {
            head_block_hash: B256::ZERO,
            safe_block_hash: B256::ZERO,
            finalized_block_hash: B256::ZERO,
        };
        let encoded = ForkchoiceUpdateAmsterdam {
            forkchoice_state,
            payload_attributes: Optional::none(),
            custody_columns: Optional::some(B128::with_last_byte(1)),
        }
        .as_ssz_bytes();

        let (decoded_state, decoded_attrs, custody_columns) =
            decode_forkchoice_request(EngineSszFork::Amsterdam, &encoded).unwrap();
        assert_eq!(decoded_state, forkchoice_state);
        assert!(decoded_attrs.is_none());
        assert_eq!(custody_columns, Some(B128::with_last_byte(1)));
    }

    #[test]
    fn decodes_forkchoice_cancun_payload_attributes() {
        let forkchoice_state = ForkchoiceState {
            head_block_hash: B256::ZERO,
            safe_block_hash: B256::ZERO,
            finalized_block_hash: B256::ZERO,
        };
        let attrs = crate::engine_ssz_containers::PayloadAttributesCancun {
            timestamp: 1,
            prev_randao: B256::with_last_byte(2),
            suggested_fee_recipient: Default::default(),
            withdrawals: Vec::new(),
            parent_beacon_block_root: B256::with_last_byte(3),
        };
        let encoded =
            ForkchoiceUpdateCancun { forkchoice_state, payload_attributes: Optional::some(attrs) }
                .as_ssz_bytes();

        let (decoded_state, decoded_attrs, custody_columns) =
            decode_forkchoice_request(EngineSszFork::Cancun, &encoded).unwrap();
        assert_eq!(decoded_state, forkchoice_state);
        let decoded_attrs = decoded_attrs.unwrap();
        assert_eq!(decoded_attrs.timestamp, 1);
        assert!(decoded_attrs.withdrawals.as_ref().unwrap().is_empty());
        assert_eq!(decoded_attrs.parent_beacon_block_root, Some(B256::with_last_byte(3)));
        assert!(custody_columns.is_none());
    }
}
