//! HTTP SSZ transport proxy for the authenticated Engine API server.
//!
//! Implements the [EIP-8178] SSZ Engine API routes under `/engine/v1`.
//!
//! [EIP-8178]: https://eips.ethereum.org/EIPS/eip-8178

use alloy_consensus::{Transaction, TxEnvelope};
use alloy_eips::{
    eip2718::Decodable2718,
    eip7685::{Requests, RequestsOrHash},
};
use alloy_primitives::{Bytes, B128, B256, B64};
use alloy_rpc_types_engine::{
    ssz_engine_types::{
        BodiesByHashRequest, BodiesResponse, BodiesResponseCancun, BodiesResponseOsaka,
        BodiesResponsePrague, BodyEntry, BuiltPayloadAmsterdam, BuiltPayloadOsaka,
        BuiltPayloadParis, BuiltPayloadPrague, BuiltPayloadShanghai, ExecutionPayloadBodyAmsterdam,
        ExecutionPayloadBodyParis, ExecutionPayloadBodyShanghai, ExecutionPayloadEnvelopeAmsterdam,
        ExecutionWitnessV1, PayloadStatus as EngineSszPayloadStatus, PayloadStatusWithWitness,
    },
    CancunPayloadFields, ExecutionData, ExecutionPayload, ExecutionPayloadSidecar,
    ExecutionPayloadV1, ExecutionPayloadV2, ExecutionPayloadV3, ForkchoiceState, PayloadAttributes,
    PayloadId, PayloadStatus, PayloadStatusEnum, PraguePayloadFields,
};
use http_body_util::BodyExt;
use jsonrpsee::server::{HttpBody, HttpRequest, HttpResponse};
use reth_chainspec::EthereumHardforks;
use reth_engine_primitives::EngineApiValidator;
use reth_ethereum_engine_primitives::EthEngineTypes;
use reth_evm::{execute::Executor, ConfigureEvm};
use reth_primitives_traits::{AlloyBlockHeader, Block, NodePrimitives};
use reth_provider::{BalProvider, BlockReader, HeaderProvider, StateProviderFactory};
use reth_revm::{database::StateProviderDatabase, witness::ExecutionWitnessRecord};
use reth_rpc::EngineApi;
use reth_storage_api::TransactionVariant;
use reth_tasks::Runtime;
use reth_transaction_pool::TransactionPool;
use reth_trie_common::ExecutionWitnessMode;
use ssz::Decode;
use std::{
    future::Future,
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::sync::RwLock;
use tower::{BoxError, Layer, Service};

const OCTET_STREAM: &str = "application/octet-stream";
const APPLICATION_JSON: &str = "application/json";
const TEXT_PLAIN: &str = "text/plain";
const CONTENT_TYPE: &str = "content-type";
const ETH_EXECUTION_VERSION: &str = "eth-execution-version";

const STATUS_OK: u16 = 200;
const STATUS_BAD_REQUEST: u16 = 400;
const STATUS_NOT_FOUND: u16 = 404;
const STATUS_METHOD_NOT_ALLOWED: u16 = 405;
const STATUS_INTERNAL_SERVER_ERROR: u16 = 500;
const STATUS_SERVICE_UNAVAILABLE: u16 = 503;

const MAX_BLOB_LIMIT: usize = 128;
const MAX_PAYLOAD_BYTES: u64 = 64 * 1024 * 1024;

type EthEngineApi<Provider, Pool, Validator, ChainSpec> =
    EngineApi<Provider, EthEngineTypes, Pool, Validator, ChainSpec>;
type SharedEngineApi<Api> = Arc<RwLock<Option<Api>>>;
type SharedWitnessHandler = Arc<RwLock<Option<Arc<dyn EngineSszWitness>>>>;

/// Engine API operations required by the SSZ transport.
pub trait EngineSszApi: Clone + Send + Sync + 'static {
    /// Returns the Engine API client identity response.
    fn identity(&self) -> HttpResponse;

    /// Handles a decoded SSZ new-payload request body.
    fn new_payload(&self, version: u8, body: Bytes) -> impl Future<Output = HttpResponse> + Send;

    /// Handles a decoded SSZ new-payload request body and returns an optional execution witness.
    fn new_payload_with_witness(
        &self,
        version: u8,
        supports_witness: bool,
        body: Bytes,
        witness_handler: Option<Arc<dyn EngineSszWitness>>,
    ) -> impl Future<Output = HttpResponse> + Send;

    /// Handles a decoded SSZ forkchoice-updated request body.
    fn forkchoice_updated(
        &self,
        version: u8,
        body: Bytes,
    ) -> impl Future<Output = HttpResponse> + Send;

    /// Handles a decoded SSZ get-blobs request body.
    fn get_blobs(&self, version: u8, body: Bytes) -> impl Future<Output = HttpResponse> + Send;

    /// Handles a fork-scoped get-payload request.
    fn get_payload(
        &self,
        version: u8,
        payload_id: PayloadId,
    ) -> impl Future<Output = HttpResponse> + Send;

    /// Handles a fork-scoped payload-bodies-by-hash request.
    fn payload_bodies_by_hash(
        &self,
        version: u8,
        hashes: Vec<B256>,
    ) -> impl Future<Output = HttpResponse> + Send;

    /// Handles a fork-scoped payload-bodies-by-range request.
    fn payload_bodies_by_range(
        &self,
        version: u8,
        start: u64,
        count: u64,
    ) -> impl Future<Output = HttpResponse> + Send;
}

/// Shared handle used by [`EngineSszProxyLayer`].
pub struct EngineSszProxyHandle<Api = ()> {
    engine_api: SharedEngineApi<Api>,
    witness_handler: SharedWitnessHandler,
}

impl<Api> Clone for EngineSszProxyHandle<Api> {
    fn clone(&self) -> Self {
        Self { engine_api: self.engine_api.clone(), witness_handler: self.witness_handler.clone() }
    }
}

impl<Api> std::fmt::Debug for EngineSszProxyHandle<Api> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EngineSszProxyHandle").finish_non_exhaustive()
    }
}

impl<Api> EngineSszProxyHandle<Api> {
    fn new() -> Self {
        Self { engine_api: Default::default(), witness_handler: Default::default() }
    }

    fn with_engine_api(engine_api: Api) -> Self {
        Self {
            engine_api: Arc::new(RwLock::new(Some(engine_api))),
            witness_handler: Default::default(),
        }
    }

    /// Sets the Engine API implementation used by the proxy.
    pub async fn set_engine_api(&self, engine_api: Api) {
        *self.engine_api.write().await = Some(engine_api);
    }

    /// Sets the Engine API implementation during synchronous launch wiring.
    pub fn set_engine_api_sync(&self, engine_api: Api) {
        *self
            .engine_api
            .try_write()
            .expect("engine api handle should not be locked during launch") = Some(engine_api);
    }

    /// Sets the witness generator used by `/payloads/witness`.
    pub async fn set_witness_handler(&self, witness_handler: Arc<dyn EngineSszWitness>) {
        *self.witness_handler.write().await = Some(witness_handler);
    }

    /// Sets the witness generator during synchronous launch wiring.
    pub fn set_witness_handler_sync(&self, witness_handler: Arc<dyn EngineSszWitness>) {
        *self
            .witness_handler
            .try_write()
            .expect("witness handler should not be locked during launch") = Some(witness_handler);
    }
}

impl<Api: Clone> EngineSszProxyHandle<Api> {
    /// Returns the Engine API implementation used by the proxy.
    pub async fn engine_api(&self) -> Option<Api> {
        self.engine_api.read().await.clone()
    }

    /// Returns the witness generator used by `/payloads/witness`.
    pub async fn witness_handler(&self) -> Option<Arc<dyn EngineSszWitness>> {
        self.witness_handler.read().await.clone()
    }
}

/// Generates an execution witness for a valid payload.
pub trait EngineSszWitness: Send + Sync + 'static {
    /// Generates a REST-SSZ execution witness for the block hash.
    fn generate_witness(
        &self,
        block_hash: B256,
    ) -> Pin<Box<dyn Future<Output = Result<ExecutionWitnessV1, String>> + Send + '_>>;
}

/// Re-executes imported blocks to produce `/payloads/witness` responses.
#[derive(Clone, Debug)]
pub struct EngineSszWitnessGenerator<Provider, Evm> {
    provider: Provider,
    evm_config: Evm,
    task_spawner: Runtime,
}

impl<Provider, Evm> EngineSszWitnessGenerator<Provider, Evm> {
    /// Creates a new witness generator.
    pub const fn new(provider: Provider, evm_config: Evm, task_spawner: Runtime) -> Self {
        Self { provider, evm_config, task_spawner }
    }
}

impl<Provider, Evm> EngineSszWitness for EngineSszWitnessGenerator<Provider, Evm>
where
    Provider: BlockReader + HeaderProvider + StateProviderFactory + Clone + Send + Sync + 'static,
    Provider::Block: Block<Header: alloy_rlp::Encodable>,
    Evm: ConfigureEvm<Primitives: NodePrimitives<Block = Provider::Block>> + 'static,
{
    fn generate_witness(
        &self,
        block_hash: B256,
    ) -> Pin<Box<dyn Future<Output = Result<ExecutionWitnessV1, String>> + Send + '_>> {
        let provider = self.provider.clone();
        let evm_config = self.evm_config.clone();
        let task_spawner = self.task_spawner.clone();

        Box::pin(async move {
            task_spawner
                .spawn_blocking(move || {
                    let block = provider
                        .recovered_block(block_hash.into(), TransactionVariant::WithHash)
                        .map_err(|err| err.to_string())?
                        .ok_or_else(|| format!("block {block_hash} not found for witness"))?;

                    let block_number = block.header().number();
                    let parent_hash = block.header().parent_hash();
                    let state_provider =
                        provider.state_by_block_hash(parent_hash).map_err(|err| err.to_string())?;
                    let state = StateProviderDatabase::new(state_provider);
                    let mut db = reth_revm::State::builder()
                        .with_database(state)
                        .with_bundle_update()
                        .build();

                    let block_executor = evm_config.executor(&mut db);
                    let mode = ExecutionWitnessMode::Legacy;
                    let mut witness_record = ExecutionWitnessRecord::default();
                    block_executor
                        .execute_with_state_closure(&block, |statedb: &reth_revm::State<_>| {
                            witness_record.record_executed_state(statedb, mode);
                        })
                        .map_err(|err| err.to_string())?;

                    let witness = witness_record
                        .into_execution_witness(&*db.database, &provider, block_number, mode)
                        .map_err(|err| err.to_string())?;

                    ExecutionWitnessV1::try_from(witness).map_err(|err| err.to_string())
                })
                .await
                .map_err(|err| {
                    io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        format!("witness generation task failed: {err}"),
                    )
                    .to_string()
                })?
        })
    }
}

/// A tower layer that intercepts SSZ Engine API routes under `/engine/v1`.
#[derive(Clone, Debug)]
pub struct EngineSszProxyLayer<Api = ()> {
    handle: EngineSszProxyHandle<Api>,
}

impl<Api> EngineSszProxyLayer<Api> {
    /// Creates a new proxy layer and a handle for setting the engine after node launch.
    pub fn new() -> (Self, EngineSszProxyHandle<Api>) {
        let handle = EngineSszProxyHandle::new();
        (Self { handle: handle.clone() }, handle)
    }

    /// Creates a new proxy layer with an Engine API implementation.
    pub fn with_engine_api(engine_api: Api) -> (Self, EngineSszProxyHandle<Api>) {
        let handle = EngineSszProxyHandle::with_engine_api(engine_api);
        (Self { handle: handle.clone() }, handle)
    }
}

impl<S, Api> Layer<S> for EngineSszProxyLayer<Api> {
    type Service = EngineSszProxyService<S, Api>;

    fn layer(&self, inner: S) -> Self::Service {
        EngineSszProxyService { inner, handle: self.handle.clone() }
    }
}

/// The service produced by [`EngineSszProxyLayer`].
#[derive(Clone, Debug)]
pub struct EngineSszProxyService<S, Api = ()> {
    inner: S,
    handle: EngineSszProxyHandle<Api>,
}

impl<S, Api> Service<HttpRequest> for EngineSszProxyService<S, Api>
where
    S: Service<HttpRequest, Response = HttpResponse, Error = BoxError> + Send + Clone,
    S::Future: Send + 'static,
    Api: EngineSszApi,
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

async fn handle_engine_ssz_request<Api>(
    handle: EngineSszProxyHandle<Api>,
    request: HttpRequest,
) -> HttpResponse
where
    Api: EngineSszApi,
{
    let method = request.method().as_str().to_owned();
    let path = request.uri().path().to_owned();
    let Some(endpoint) = parse_engine_path(&path) else {
        return text_response(STATUS_NOT_FOUND, "unknown engine ssz endpoint")
    };

    match endpoint {
        EngineSszEndpoint::Capabilities => {
            if method != "GET" {
                return text_response(STATUS_METHOD_NOT_ALLOWED, "method not allowed")
            }
            handle_capabilities()
        }
        EngineSszEndpoint::Identity => {
            if method != "GET" {
                return text_response(STATUS_METHOD_NOT_ALLOWED, "method not allowed")
            }
            let Some(engine_api) = handle.engine_api().await else {
                return text_response(STATUS_SERVICE_UNAVAILABLE, "engine api unavailable")
            };
            engine_api.identity()
        }
        EngineSszEndpoint::NewPayload => {
            if method != "POST" {
                return text_response(STATUS_METHOD_NOT_ALLOWED, "method not allowed")
            }
            let Some(fork) = request_fork(&request) else {
                return text_response(STATUS_BAD_REQUEST, "unsupported fork")
            };
            let Ok(body) = request.into_body().collect().await.map(|body| body.to_bytes()) else {
                return text_response(STATUS_BAD_REQUEST, "failed to read request body")
            };
            let Some(engine_api) = handle.engine_api().await else {
                return text_response(STATUS_SERVICE_UNAVAILABLE, "engine api unavailable")
            };
            engine_api.new_payload(fork.payloads_version(), body.into()).await
        }
        EngineSszEndpoint::PayloadsWithWitness => {
            if method != "POST" {
                return text_response(STATUS_METHOD_NOT_ALLOWED, "method not allowed")
            }
            let Some(fork) = request_fork(&request) else {
                return text_response(STATUS_BAD_REQUEST, "unsupported fork")
            };
            let Ok(body) = request.into_body().collect().await.map(|body| body.to_bytes()) else {
                return text_response(STATUS_BAD_REQUEST, "failed to read request body")
            };
            let witness_handler = handle.witness_handler().await;
            let Some(engine_api) = handle.engine_api().await else {
                return text_response(STATUS_SERVICE_UNAVAILABLE, "engine api unavailable")
            };
            engine_api
                .new_payload_with_witness(
                    fork.payloads_version(),
                    fork.supports_witness(),
                    body.into(),
                    witness_handler,
                )
                .await
        }
        EngineSszEndpoint::Payload(payload_id) => {
            if method != "GET" {
                return text_response(STATUS_METHOD_NOT_ALLOWED, "method not allowed")
            }
            let Some(fork) = request_fork(&request) else {
                return text_response(STATUS_BAD_REQUEST, "unsupported fork")
            };
            let Some(engine_api) = handle.engine_api().await else {
                return text_response(STATUS_SERVICE_UNAVAILABLE, "engine api unavailable")
            };
            engine_api.get_payload(fork.get_payload_version(), payload_id).await
        }
        EngineSszEndpoint::Forkchoice => {
            if method != "POST" {
                return text_response(STATUS_METHOD_NOT_ALLOWED, "method not allowed")
            }
            let Some(fork) = request_fork(&request) else {
                return text_response(STATUS_BAD_REQUEST, "unsupported fork")
            };
            let Ok(body) = request.into_body().collect().await.map(|body| body.to_bytes()) else {
                return text_response(STATUS_BAD_REQUEST, "failed to read request body")
            };
            let Some(engine_api) = handle.engine_api().await else {
                return text_response(STATUS_SERVICE_UNAVAILABLE, "engine api unavailable")
            };
            engine_api.forkchoice_updated(fork.forkchoice_version(), body.into()).await
        }
        EngineSszEndpoint::Blobs(version) => {
            if method != "POST" {
                return text_response(STATUS_METHOD_NOT_ALLOWED, "method not allowed")
            }
            let Ok(body) = request.into_body().collect().await.map(|body| body.to_bytes()) else {
                return text_response(STATUS_BAD_REQUEST, "failed to read request body")
            };
            let Some(engine_api) = handle.engine_api().await else {
                return text_response(STATUS_SERVICE_UNAVAILABLE, "engine api unavailable")
            };
            engine_api.get_blobs(version, body.into()).await
        }
        EngineSszEndpoint::PayloadBodiesByHash => {
            if method != "POST" {
                return text_response(STATUS_METHOD_NOT_ALLOWED, "method not allowed")
            }
            let Some(fork) = request_fork(&request) else {
                return text_response(STATUS_BAD_REQUEST, "unsupported fork")
            };
            let hashes = match request.into_body().collect().await.map(|body| body.to_bytes()) {
                Ok(body) => match BodiesByHashRequest::from_ssz_bytes(&body) {
                    Ok(request) => request.block_hashes.into_iter().collect(),
                    Err(_) => return text_response(STATUS_BAD_REQUEST, "invalid ssz"),
                },
                Err(_) => return text_response(STATUS_BAD_REQUEST, "failed to read request body"),
            };
            let Some(engine_api) = handle.engine_api().await else {
                return text_response(STATUS_SERVICE_UNAVAILABLE, "engine api unavailable")
            };
            engine_api.payload_bodies_by_hash(fork.get_payload_version(), hashes).await
        }
        EngineSszEndpoint::PayloadBodiesByRange => {
            if method != "GET" {
                return text_response(STATUS_METHOD_NOT_ALLOWED, "method not allowed")
            }
            let Some(fork) = request_fork(&request) else {
                return text_response(STATUS_BAD_REQUEST, "unsupported fork")
            };

            let Some(query) = request.uri().query() else {
                return text_response(STATUS_BAD_REQUEST, "missing payload bodies query")
            };
            let mut start = None;
            let mut count = None;
            for pair in query.split('&') {
                let Some((key, value)) = pair.split_once('=') else {
                    return text_response(STATUS_BAD_REQUEST, "invalid payload bodies query")
                };
                match key {
                    "from" => {
                        start = match value.parse::<u64>() {
                            Ok(value) => Some(value),
                            Err(_) => {
                                return text_response(
                                    STATUS_BAD_REQUEST,
                                    "invalid payload bodies from query",
                                )
                            }
                        };
                    }
                    "count" => {
                        count = match value.parse::<u64>() {
                            Ok(value) => Some(value),
                            Err(_) => {
                                return text_response(
                                    STATUS_BAD_REQUEST,
                                    "invalid payload bodies count query",
                                )
                            }
                        };
                    }
                    _ => return text_response(STATUS_BAD_REQUEST, "unknown payload bodies query"),
                }
            }
            let Some(start) = start else {
                return text_response(STATUS_BAD_REQUEST, "missing payload bodies from query")
            };
            let Some(count) = count else {
                return text_response(STATUS_BAD_REQUEST, "missing payload bodies count query")
            };

            let Some(engine_api) = handle.engine_api().await else {
                return text_response(STATUS_SERVICE_UNAVAILABLE, "engine api unavailable")
            };
            engine_api.payload_bodies_by_range(fork.get_payload_version(), start, count).await
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
        (Some("engine"), Some("v1"), Some("payloads"), Some("witness"), None) => {
            Some(EngineSszEndpoint::PayloadsWithWitness)
        }
        (Some("engine"), Some("v1"), Some("payloads"), Some(payload_id), None) => {
            Some(EngineSszEndpoint::Payload(PayloadId::from(payload_id.parse::<B64>().ok()?)))
        }
        (Some("engine"), Some("v1"), Some("forkchoice"), None, None) => {
            Some(EngineSszEndpoint::Forkchoice)
        }
        (Some("engine"), Some("v1"), Some("blobs"), version, None) => {
            Some(EngineSszEndpoint::Blobs(parse_method_version(version?)?))
        }
        (Some("engine"), Some("v1"), Some("bodies"), Some("hash"), None) => {
            Some(EngineSszEndpoint::PayloadBodiesByHash)
        }
        (Some("engine"), Some("v1"), Some("bodies"), None, None) => {
            Some(EngineSszEndpoint::PayloadBodiesByRange)
        }
        _ => None,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EngineSszEndpoint {
    Capabilities,
    Identity,
    NewPayload,
    PayloadsWithWitness,
    Payload(PayloadId),
    Forkchoice,
    Blobs(u8),
    PayloadBodiesByHash,
    PayloadBodiesByRange,
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

    const fn get_payload_version(self) -> u8 {
        match self {
            Self::Paris => 1,
            Self::Shanghai => 2,
            Self::Cancun => 3,
            Self::Prague => 4,
            Self::Osaka => 5,
            Self::Amsterdam => 6,
        }
    }

    const fn supports_witness(self) -> bool {
        matches!(self, Self::Osaka | Self::Amsterdam)
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

fn parse_method_version(version: &str) -> Option<u8> {
    version.strip_prefix('v')?.parse().ok().filter(|version| (1..=4).contains(version))
}

fn handle_capabilities() -> HttpResponse {
    json_response(serde_json::json!({
        "supported_forks": ["paris", "shanghai", "cancun", "prague", "osaka", "amsterdam"],
        "fork_scoped_endpoints": ["payloads", "payloads/witness", "forkchoice", "bodies"],
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

impl<Provider, Pool, Validator, ChainSpec> EngineSszApi
    for EthEngineApi<Provider, Pool, Validator, ChainSpec>
where
    Provider: HeaderProvider + BlockReader + StateProviderFactory + BalProvider + 'static,
    Pool: TransactionPool + 'static,
    Validator: EngineApiValidator<EthEngineTypes>,
    ChainSpec: EthereumHardforks + Send + Sync + 'static,
{
    fn identity(&self) -> HttpResponse {
        handle_identity(self.clone())
    }

    fn new_payload(&self, version: u8, body: Bytes) -> impl Future<Output = HttpResponse> + Send {
        let engine_api = self.clone();
        async move { handle_new_payload(engine_api, version, &body).await }
    }

    fn new_payload_with_witness(
        &self,
        version: u8,
        supports_witness: bool,
        body: Bytes,
        witness_handler: Option<Arc<dyn EngineSszWitness>>,
    ) -> impl Future<Output = HttpResponse> + Send {
        let engine_api = self.clone();
        async move {
            handle_new_payload_with_witness(
                engine_api,
                version,
                supports_witness,
                witness_handler,
                &body,
            )
            .await
        }
    }

    fn forkchoice_updated(
        &self,
        version: u8,
        body: Bytes,
    ) -> impl Future<Output = HttpResponse> + Send {
        let engine_api = self.clone();
        async move { handle_forkchoice_updated(engine_api, version, &body).await }
    }

    fn get_blobs(&self, version: u8, body: Bytes) -> impl Future<Output = HttpResponse> + Send {
        let engine_api = self.clone();
        async move { handle_get_blobs(engine_api, version, &body).await }
    }

    fn get_payload(
        &self,
        version: u8,
        payload_id: PayloadId,
    ) -> impl Future<Output = HttpResponse> + Send {
        let engine_api = self.clone();
        async move { handle_get_payload(engine_api, version, payload_id).await }
    }

    fn payload_bodies_by_hash(
        &self,
        version: u8,
        hashes: Vec<B256>,
    ) -> impl Future<Output = HttpResponse> + Send {
        let engine_api = self.clone();
        async move {
            handle_get_payload_bodies(engine_api, version, PayloadBodiesRequest::Hash(hashes)).await
        }
    }

    fn payload_bodies_by_range(
        &self,
        version: u8,
        start: u64,
        count: u64,
    ) -> impl Future<Output = HttpResponse> + Send {
        let engine_api = self.clone();
        async move {
            handle_get_payload_bodies(
                engine_api,
                version,
                PayloadBodiesRequest::Range { start, count },
            )
            .await
        }
    }
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
    version: u8,
    payload_id: PayloadId,
) -> HttpResponse
where
    Provider: HeaderProvider + BlockReader + StateProviderFactory + BalProvider + 'static,
    Pool: TransactionPool + 'static,
    Validator: EngineApiValidator<EthEngineTypes>,
    ChainSpec: EthereumHardforks + Send + Sync + 'static,
{
    match version {
        1 => match engine_api.get_payload_v1_with_value_metered(payload_id).await {
            Ok((payload, block_value)) => ssz_response(BuiltPayloadParis { payload, block_value }),
            Err(err) => text_response(STATUS_INTERNAL_SERVER_ERROR, err.to_string()),
        },
        2 => match engine_api.get_payload_v2_metered(payload_id).await {
            Ok(payload) => match BuiltPayloadShanghai::try_from(payload) {
                Ok(payload) => ssz_response(payload),
                Err(err) => text_response(STATUS_INTERNAL_SERVER_ERROR, err.to_string()),
            },
            Err(err) => text_response(STATUS_INTERNAL_SERVER_ERROR, err.to_string()),
        },
        3 => match engine_api.get_payload_v3_metered(payload_id).await {
            Ok(payload) => ssz_response(payload),
            Err(err) => text_response(STATUS_INTERNAL_SERVER_ERROR, err.to_string()),
        },
        4 => match engine_api.get_payload_v4_metered(payload_id).await {
            Ok(payload) => ssz_response(BuiltPayloadPrague::from(payload)),
            Err(err) => text_response(STATUS_INTERNAL_SERVER_ERROR, err.to_string()),
        },
        5 => match engine_api.get_payload_v5_metered(payload_id).await {
            Ok(payload) => ssz_response(BuiltPayloadOsaka::from(payload)),
            Err(err) => text_response(STATUS_INTERNAL_SERVER_ERROR, err.to_string()),
        },
        6 => match engine_api.get_payload_v6_metered(payload_id).await {
            Ok(payload) => ssz_response(BuiltPayloadAmsterdam::from(payload)),
            Err(err) => text_response(STATUS_INTERNAL_SERVER_ERROR, err.to_string()),
        },
        _ => text_response(STATUS_BAD_REQUEST, "unsupported getPayload endpoint version"),
    }
}

async fn handle_new_payload<Provider, Pool, Validator, ChainSpec>(
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
    let payload = match decode_new_payload_request(version, body) {
        Ok(payload) => payload,
        Err(err) => return text_response(STATUS_BAD_REQUEST, err),
    };

    let response = match version {
        1 => engine_api.new_payload_v1(payload).await,
        2 => engine_api.new_payload_v2(payload).await,
        3 => engine_api.new_payload_v3(payload).await,
        4 => engine_api.new_payload_v4(payload).await,
        5 => engine_api.new_payload_v5(payload).await,
        _ => return text_response(STATUS_BAD_REQUEST, "unsupported payload endpoint version"),
    };

    match response {
        Ok(status) => ssz_response(status),
        Err(err) => text_response(STATUS_INTERNAL_SERVER_ERROR, err.to_string()),
    }
}

async fn handle_new_payload_with_witness<Provider, Pool, Validator, ChainSpec>(
    engine_api: EthEngineApi<Provider, Pool, Validator, ChainSpec>,
    version: u8,
    supports_witness: bool,
    witness_handler: Option<Arc<dyn EngineSszWitness>>,
    body: &[u8],
) -> HttpResponse
where
    Provider: HeaderProvider + BlockReader + StateProviderFactory + BalProvider + 'static,
    Pool: TransactionPool + 'static,
    Validator: EngineApiValidator<EthEngineTypes>,
    ChainSpec: EthereumHardforks + Send + Sync + 'static,
{
    if !supports_witness {
        return text_response(STATUS_BAD_REQUEST, "unsupported fork")
    }

    let status = match new_payload_status(engine_api, version, body).await {
        Ok(status) => status,
        Err(response) => return response,
    };

    let payload_status = match EngineSszPayloadStatus::try_from(status.clone()) {
        Ok(status) => status,
        Err(err) => return text_response(STATUS_INTERNAL_SERVER_ERROR, err.to_string()),
    };
    let witness = match status.status {
        PayloadStatusEnum::Valid => {
            let Some(block_hash) = status.latest_valid_hash else {
                return text_response(STATUS_INTERNAL_SERVER_ERROR, "missing latest valid hash")
            };
            let Some(witness_handler) = witness_handler else {
                return text_response(
                    STATUS_SERVICE_UNAVAILABLE,
                    "execution witness handler unavailable",
                )
            };
            match witness_handler.generate_witness(block_hash).await {
                Ok(witness) => Some(witness),
                Err(err) => return text_response(STATUS_INTERNAL_SERVER_ERROR, err),
            }
        }
        _ => None,
    };

    ssz_response(PayloadStatusWithWitness::new(payload_status, witness))
}

async fn new_payload_status<Provider, Pool, Validator, ChainSpec>(
    engine_api: EthEngineApi<Provider, Pool, Validator, ChainSpec>,
    version: u8,
    body: &[u8],
) -> Result<PayloadStatus, HttpResponse>
where
    Provider: HeaderProvider + BlockReader + StateProviderFactory + BalProvider + 'static,
    Pool: TransactionPool + 'static,
    Validator: EngineApiValidator<EthEngineTypes>,
    ChainSpec: EthereumHardforks + Send + Sync + 'static,
{
    let payload = decode_new_payload_request(version, body)
        .map_err(|err| text_response(STATUS_BAD_REQUEST, err))?;

    match version {
        1 => engine_api.new_payload_v1(payload).await,
        2 => engine_api.new_payload_v2(payload).await,
        3 => engine_api.new_payload_v3(payload).await,
        4 => engine_api.new_payload_v4(payload).await,
        5 => engine_api.new_payload_v5(payload).await,
        _ => return Err(text_response(STATUS_BAD_REQUEST, "unsupported payload endpoint version")),
    }
    .map_err(|err| text_response(STATUS_INTERNAL_SERVER_ERROR, err.to_string()))
}

async fn handle_forkchoice_updated<Provider, Pool, Validator, ChainSpec>(
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
    let (state, attrs, custody_columns) = match decode_forkchoice_request(version, body) {
        Ok(request) => request,
        Err(err) => return text_response(STATUS_BAD_REQUEST, err),
    };

    let response = match version {
        1 => engine_api.fork_choice_updated_v1_metered(state, attrs).await,
        2 => engine_api.fork_choice_updated_v2_metered(state, attrs).await,
        3 => engine_api.fork_choice_updated_v3_metered(state, attrs).await,
        4 => engine_api.fork_choice_updated_v4_metered(state, attrs, custody_columns).await,
        _ => return text_response(STATUS_BAD_REQUEST, "unsupported forkchoice endpoint version"),
    };

    match response {
        Ok(updated) => ssz_response(updated),
        Err(err) => text_response(STATUS_INTERNAL_SERVER_ERROR, err.to_string()),
    }
}

enum PayloadBodiesRequest {
    Hash(Vec<B256>),
    Range { start: u64, count: u64 },
}

async fn handle_get_payload_bodies<Provider, Pool, Validator, ChainSpec>(
    engine_api: EthEngineApi<Provider, Pool, Validator, ChainSpec>,
    version: u8,
    request: PayloadBodiesRequest,
) -> HttpResponse
where
    Provider: HeaderProvider + BlockReader + StateProviderFactory + BalProvider + 'static,
    Pool: TransactionPool + 'static,
    Validator: EngineApiValidator<EthEngineTypes>,
    ChainSpec: EthereumHardforks + Send + Sync + 'static,
{
    match version {
        1 => {
            let response = match request {
                PayloadBodiesRequest::Hash(hashes) => {
                    engine_api.get_payload_bodies_by_hash_v1_metered(hashes).await
                }
                PayloadBodiesRequest::Range { start, count } => {
                    engine_api.get_payload_bodies_by_range_v1_metered(start, count).await
                }
            };
            match response {
                Ok(bodies) => match payload_bodies_response(bodies, |body| {
                    ExecutionPayloadBodyParis::try_from(body).ok()
                }) {
                    Ok(response) => ssz_response(response),
                    Err(err) => text_response(STATUS_INTERNAL_SERVER_ERROR, err),
                },
                Err(err) => text_response(STATUS_INTERNAL_SERVER_ERROR, err.to_string()),
            }
        }
        2 => {
            let response = match request {
                PayloadBodiesRequest::Hash(hashes) => {
                    engine_api.get_payload_bodies_by_hash_v1_metered(hashes).await
                }
                PayloadBodiesRequest::Range { start, count } => {
                    engine_api.get_payload_bodies_by_range_v1_metered(start, count).await
                }
            };
            match response {
                Ok(bodies) => match payload_bodies_response(bodies, |body| {
                    ExecutionPayloadBodyShanghai::try_from(body).ok()
                }) {
                    Ok(response) => ssz_response(response),
                    Err(err) => text_response(STATUS_INTERNAL_SERVER_ERROR, err),
                },
                Err(err) => text_response(STATUS_INTERNAL_SERVER_ERROR, err.to_string()),
            }
        }
        3 => {
            let response = match request {
                PayloadBodiesRequest::Hash(hashes) => {
                    engine_api.get_payload_bodies_by_hash_v1_metered(hashes).await
                }
                PayloadBodiesRequest::Range { start, count } => {
                    engine_api.get_payload_bodies_by_range_v1_metered(start, count).await
                }
            };
            match response {
                Ok(bodies) => match payload_bodies_response(bodies, |body| {
                    ExecutionPayloadBodyShanghai::try_from(body).ok()
                }) {
                    Ok(response) => {
                        let response: BodiesResponseCancun = response;
                        ssz_response(response)
                    }
                    Err(err) => text_response(STATUS_INTERNAL_SERVER_ERROR, err),
                },
                Err(err) => text_response(STATUS_INTERNAL_SERVER_ERROR, err.to_string()),
            }
        }
        4 => {
            let response = match request {
                PayloadBodiesRequest::Hash(hashes) => {
                    engine_api.get_payload_bodies_by_hash_v1_metered(hashes).await
                }
                PayloadBodiesRequest::Range { start, count } => {
                    engine_api.get_payload_bodies_by_range_v1_metered(start, count).await
                }
            };
            match response {
                Ok(bodies) => match payload_bodies_response(bodies, |body| {
                    ExecutionPayloadBodyShanghai::try_from(body).ok()
                }) {
                    Ok(response) => {
                        let response: BodiesResponsePrague = response;
                        ssz_response(response)
                    }
                    Err(err) => text_response(STATUS_INTERNAL_SERVER_ERROR, err),
                },
                Err(err) => text_response(STATUS_INTERNAL_SERVER_ERROR, err.to_string()),
            }
        }
        5 => {
            let response = match request {
                PayloadBodiesRequest::Hash(hashes) => {
                    engine_api.get_payload_bodies_by_hash_v1_metered(hashes).await
                }
                PayloadBodiesRequest::Range { start, count } => {
                    engine_api.get_payload_bodies_by_range_v1_metered(start, count).await
                }
            };
            match response {
                Ok(bodies) => match payload_bodies_response(bodies, |body| {
                    ExecutionPayloadBodyShanghai::try_from(body).ok()
                }) {
                    Ok(response) => {
                        let response: BodiesResponseOsaka = response;
                        ssz_response(response)
                    }
                    Err(err) => text_response(STATUS_INTERNAL_SERVER_ERROR, err),
                },
                Err(err) => text_response(STATUS_INTERNAL_SERVER_ERROR, err.to_string()),
            }
        }
        6 => {
            let response = match request {
                PayloadBodiesRequest::Hash(hashes) => {
                    engine_api.get_payload_bodies_by_hash_v2_metered(hashes).await
                }
                PayloadBodiesRequest::Range { start, count } => {
                    engine_api.get_payload_bodies_by_range_v2_metered(start, count).await
                }
            };
            match response {
                Ok(bodies) => match payload_bodies_response(bodies, |body| {
                    ExecutionPayloadBodyAmsterdam::try_from(body).ok()
                }) {
                    Ok(response) => ssz_response(response),
                    Err(err) => text_response(STATUS_INTERNAL_SERVER_ERROR, err),
                },
                Err(err) => text_response(STATUS_INTERNAL_SERVER_ERROR, err.to_string()),
            }
        }
        _ => text_response(STATUS_BAD_REQUEST, "unsupported payload bodies fork"),
    }
}

fn payload_bodies_response<LegacyBody, ForkBody>(
    bodies: Vec<Option<LegacyBody>>,
    convert: impl Fn(LegacyBody) -> Option<ForkBody>,
) -> Result<BodiesResponse<ForkBody>, String>
where
    ForkBody: Default,
{
    let entries = bodies
        .into_iter()
        .map(|body| match body.and_then(|body| convert(body)) {
            Some(body) => BodyEntry { available: true, body },
            None => BodyEntry { available: false, body: ForkBody::default() },
        })
        .collect::<Vec<_>>()
        .try_into()
        .map_err(|_| "too many payload body entries".to_string())?;

    Ok(BodiesResponse { entries })
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
                Err(err) => return text_response(STATUS_BAD_REQUEST, err),
            };
            match engine_api.get_blobs_v1_metered(hashes) {
                Ok(response) => ssz_response(response),
                Err(err) => text_response(STATUS_INTERNAL_SERVER_ERROR, err.to_string()),
            }
        }
        2 => {
            let hashes = match decode_blob_hashes_request(body) {
                Ok(hashes) => hashes,
                Err(err) => return text_response(STATUS_BAD_REQUEST, err),
            };
            match engine_api.get_blobs_v2_metered(hashes) {
                Ok(Some(response)) => ssz_response(response),
                Ok(None) => no_content_response(),
                Err(err) => text_response(STATUS_INTERNAL_SERVER_ERROR, err.to_string()),
            }
        }
        3 => {
            let hashes = match decode_blob_hashes_request(body) {
                Ok(hashes) => hashes,
                Err(err) => return text_response(STATUS_BAD_REQUEST, err),
            };
            match engine_api.get_blobs_v3_metered(hashes) {
                Ok(Some(response)) => ssz_response(response),
                Ok(None) => no_content_response(),
                Err(err) => text_response(STATUS_INTERNAL_SERVER_ERROR, err.to_string()),
            }
        }
        4 => {
            let (hashes, indices_bitarray) = match decode_blob_cells_request(body) {
                Ok(request) => request,
                Err(err) => return text_response(STATUS_BAD_REQUEST, err),
            };
            match engine_api.get_blobs_v4_metered(hashes, indices_bitarray) {
                Ok(Some(response)) => ssz_response(response),
                Ok(None) => no_content_response(),
                Err(err) => text_response(STATUS_INTERNAL_SERVER_ERROR, err.to_string()),
            }
        }
        _ => text_response(STATUS_NOT_FOUND, "unsupported blobs endpoint version"),
    }
}

/// Decodes the common getBlobs request container with only versioned hashes.
fn decode_blob_hashes_request(body: &[u8]) -> Result<Vec<B256>, &'static str> {
    Vec::<B256>::from_ssz_bytes(body).map_err(|_| "invalid ssz")
}

/// Decodes the Amsterdam getBlobs request container with hashes and a cell index mask.
fn decode_blob_cells_request(body: &[u8]) -> Result<(Vec<B256>, B128), &'static str> {
    <(Vec<B256>, B128) as ssz::Decode>::from_ssz_bytes(body).map_err(|_| "invalid ssz")
}

fn decode_new_payload_request(version: u8, body: &[u8]) -> Result<ExecutionData, &'static str> {
    match version {
        1 => {
            let execution_payload =
                decode_one::<ExecutionPayloadV1>(body).map_err(|_| "invalid ssz")?;
            Ok(ExecutionData::new(execution_payload.into(), ExecutionPayloadSidecar::none()))
        }
        2 => {
            let execution_payload =
                decode_one::<ExecutionPayloadV2>(body).map_err(|_| "invalid ssz")?;
            Ok(ExecutionData::new(execution_payload.into(), ExecutionPayloadSidecar::none()))
        }
        3 => {
            let (execution_payload, parent_beacon_block_root) =
                <(ExecutionPayloadV3, B256)>::from_ssz_bytes(body).map_err(|_| "invalid ssz")?;
            let versioned_hashes = calculate_versioned_hashes(
                &execution_payload.payload_inner.payload_inner.transactions,
            )?;
            let sidecar = ExecutionPayloadSidecar::v3(CancunPayloadFields {
                parent_beacon_block_root,
                versioned_hashes,
            });
            Ok(ExecutionData::new(execution_payload.into(), sidecar))
        }
        4 => {
            let (execution_payload, parent_beacon_block_root, execution_requests) =
                <(ExecutionPayloadV3, B256, Vec<Bytes>)>::from_ssz_bytes(body)
                    .map_err(|_| "invalid ssz")?;
            let versioned_hashes = calculate_versioned_hashes(
                &execution_payload.payload_inner.payload_inner.transactions,
            )?;
            let sidecar = ExecutionPayloadSidecar::v4(
                CancunPayloadFields { parent_beacon_block_root, versioned_hashes },
                PraguePayloadFields::new(RequestsOrHash::Requests(Requests::new(
                    execution_requests,
                ))),
            );
            Ok(ExecutionData::new(execution_payload.into(), sidecar))
        }
        5 => {
            let ExecutionPayloadEnvelopeAmsterdam {
                payload: execution_payload,
                parent_beacon_block_root,
                execution_requests,
            } = ExecutionPayloadEnvelopeAmsterdam::from_ssz_bytes(body)
                .map_err(|_| "invalid ssz")?;
            let versioned_hashes = calculate_versioned_hashes(
                &execution_payload.payload_inner.payload_inner.payload_inner.transactions,
            )?;
            let sidecar = ExecutionPayloadSidecar::v4(
                CancunPayloadFields { parent_beacon_block_root, versioned_hashes },
                PraguePayloadFields::new(RequestsOrHash::Requests(execution_requests)),
            );
            Ok(ExecutionData::new(ExecutionPayload::V4(execution_payload), sidecar))
        }
        _ => Err("unsupported payload endpoint version"),
    }
}

fn calculate_versioned_hashes(transactions: &[Bytes]) -> Result<Vec<B256>, &'static str> {
    let mut versioned_hashes = Vec::new();
    for transaction in transactions {
        let transaction =
            TxEnvelope::decode_2718_exact(transaction.as_ref()).map_err(|_| "invalid tx")?;
        if let Some(hashes) = transaction.blob_versioned_hashes() {
            versioned_hashes.extend_from_slice(hashes);
        }
    }

    Ok(versioned_hashes)
}

fn decode_forkchoice_request(
    version: u8,
    body: &[u8],
) -> Result<(ForkchoiceState, Option<PayloadAttributes>, Option<B128>), &'static str> {
    match version {
        1..=3 => {
            let (forkchoice_state, payload_attributes) =
                <(ForkchoiceState, Vec<PayloadAttributes>)>::from_ssz_bytes(body)
                    .map_err(|_| "invalid ssz")?;
            Ok((forkchoice_state, payload_attrs(version, payload_attributes)?, None))
        }
        4 => {
            let (forkchoice_state, payload_attributes, custody_columns) =
                <(ForkchoiceState, Vec<PayloadAttributes>, Vec<B128>)>::from_ssz_bytes(body)
                    .map_err(|_| "invalid ssz")?;
            Ok((
                forkchoice_state,
                payload_attrs(version, payload_attributes)?,
                custody_columns_opt(custody_columns)?,
            ))
        }
        _ => Err("unsupported forkchoice endpoint version"),
    }
}

fn decode_one<T: ssz::Decode>(body: &[u8]) -> Result<T, ssz::DecodeError> {
    let mut builder = ssz::SszDecoderBuilder::new(body);
    builder.register_type::<T>()?;
    let mut decoder = builder.build()?;
    decoder.decode_next()
}

fn payload_attrs(
    version: u8,
    attrs: Vec<PayloadAttributes>,
) -> Result<Option<PayloadAttributes>, &'static str> {
    if attrs.len() > 1 {
        return Err("payload_attributes must contain at most one value")
    }

    attrs.into_iter().next().map(|attrs| validate_payload_attrs_version(version, attrs)).transpose()
}

fn custody_columns_opt(custody_columns: Vec<B128>) -> Result<Option<B128>, &'static str> {
    if custody_columns.len() > 1 {
        return Err("invalid params")
    }

    Ok(custody_columns.into_iter().next())
}

fn validate_payload_attrs_version(
    version: u8,
    attrs: PayloadAttributes,
) -> Result<PayloadAttributes, &'static str> {
    let matches_version = match version {
        1 => {
            attrs.withdrawals.is_none() &&
                attrs.parent_beacon_block_root.is_none() &&
                attrs.slot_number.is_none()
        }
        2 => {
            attrs.withdrawals.is_some() &&
                attrs.parent_beacon_block_root.is_none() &&
                attrs.slot_number.is_none()
        }
        3 => {
            attrs.withdrawals.is_some() &&
                attrs.parent_beacon_block_root.is_some() &&
                attrs.slot_number.is_none()
        }
        4 => {
            attrs.withdrawals.is_some() &&
                attrs.parent_beacon_block_root.is_some() &&
                attrs.slot_number.is_some()
        }
        _ => false,
    };

    if matches_version {
        Ok(attrs)
    } else {
        Err("payload_attributes version does not match endpoint")
    }
}

fn ssz_response<T: ssz::Encode>(value: T) -> HttpResponse {
    HttpResponse::builder()
        .status(STATUS_OK)
        .header(CONTENT_TYPE, OCTET_STREAM)
        .body(HttpBody::from(value.as_ssz_bytes()))
        .expect("valid response")
}

fn json_response<T: serde::Serialize>(value: T) -> HttpResponse {
    let Ok(body) = serde_json::to_string(&value) else {
        return text_response(STATUS_INTERNAL_SERVER_ERROR, "failed to encode json")
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

fn text_response(status: u16, body: impl Into<String>) -> HttpResponse {
    HttpResponse::builder()
        .status(status)
        .header(CONTENT_TYPE, TEXT_PLAIN)
        .body(HttpBody::from(body.into()))
        .expect("valid response")
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_rpc_types_engine::{
        ssz_engine_types::{BodiesResponseAmsterdam, BodiesResponseParis},
        ExecutionPayloadBodyV1, ExecutionPayloadBodyV2,
    };
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
    fn parses_get_payload_endpoint() {
        let payload_id = PayloadId::new([1, 2, 3, 4, 5, 6, 7, 8]);
        let endpoint = parse_engine_path(&format!("/engine/v1/payloads/{payload_id}")).unwrap();
        assert_eq!(endpoint, EngineSszEndpoint::Payload(payload_id));
    }

    #[test]
    fn rejects_get_payload_endpoint_with_trailing_segment() {
        assert!(parse_engine_path("/engine/v1/payloads/0x0102030405060708/extra").is_none());
    }

    #[test]
    fn maps_get_payload_versions_by_fork() {
        assert_eq!(EngineSszFork::Paris.get_payload_version(), 1);
        assert_eq!(EngineSszFork::Shanghai.get_payload_version(), 2);
        assert_eq!(EngineSszFork::Cancun.get_payload_version(), 3);
        assert_eq!(EngineSszFork::Prague.get_payload_version(), 4);
        assert_eq!(EngineSszFork::Osaka.get_payload_version(), 5);
        assert_eq!(EngineSszFork::Amsterdam.get_payload_version(), 6);
    }

    #[test]
    fn parses_fork_scoped_forkchoice_endpoint() {
        let endpoint = parse_engine_path("/engine/v1/forkchoice").unwrap();
        assert_eq!(endpoint, EngineSszEndpoint::Forkchoice);
    }

    #[test]
    fn parses_payload_bodies_by_hash_endpoint() {
        let endpoint = parse_engine_path("/engine/v1/bodies/hash").unwrap();
        assert_eq!(endpoint, EngineSszEndpoint::PayloadBodiesByHash);
    }

    #[test]
    fn parses_payload_bodies_by_range_endpoint() {
        let endpoint = parse_engine_path("/engine/v1/bodies").unwrap();
        assert_eq!(endpoint, EngineSszEndpoint::PayloadBodiesByRange);
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
    fn encodes_payload_body_availability_for_selected_fork() {
        let response: BodiesResponseParis = payload_bodies_response(
            vec![
                Some(ExecutionPayloadBodyV1 { transactions: vec![], withdrawals: None }),
                Some(ExecutionPayloadBodyV1 { transactions: vec![], withdrawals: Some(vec![]) }),
                None,
            ],
            |body| ExecutionPayloadBodyParis::try_from(body).ok(),
        )
        .unwrap();

        assert_eq!(response.entries.len(), 3);
        assert!(response.entries[0].available);
        assert!(!response.entries[1].available);
        assert!(!response.entries[2].available);

        let response: BodiesResponseAmsterdam = payload_bodies_response(
            vec![
                Some(ExecutionPayloadBodyV2 {
                    transactions: vec![],
                    withdrawals: Some(vec![]),
                    block_access_list: Some(Bytes::new()),
                }),
                Some(ExecutionPayloadBodyV2 {
                    transactions: vec![],
                    withdrawals: Some(vec![]),
                    block_access_list: None,
                }),
            ],
            |body| ExecutionPayloadBodyAmsterdam::try_from(body).ok(),
        )
        .unwrap();

        assert_eq!(response.entries.len(), 2);
        assert!(response.entries[0].available);
        assert!(!response.entries[1].available);
    }

    #[test]
    fn decodes_forkchoice_v4_with_custody_columns() {
        let forkchoice_state = ForkchoiceState {
            head_block_hash: B256::ZERO,
            safe_block_hash: B256::ZERO,
            finalized_block_hash: B256::ZERO,
        };
        let encoded =
            (forkchoice_state, Vec::<PayloadAttributes>::new(), vec![B128::with_last_byte(1)])
                .as_ssz_bytes();

        let (decoded_state, decoded_attrs, custody_columns) =
            decode_forkchoice_request(4, &encoded).unwrap();
        assert_eq!(decoded_state, forkchoice_state);
        assert!(decoded_attrs.is_none());
        assert_eq!(custody_columns, Some(B128::with_last_byte(1)));
    }

    #[test]
    fn rejects_forkchoice_v4_with_multiple_custody_columns() {
        let forkchoice_state = ForkchoiceState {
            head_block_hash: B256::ZERO,
            safe_block_hash: B256::ZERO,
            finalized_block_hash: B256::ZERO,
        };
        let encoded = (
            forkchoice_state,
            Vec::<PayloadAttributes>::new(),
            vec![B128::ZERO, B128::with_last_byte(1)],
        )
            .as_ssz_bytes();

        let err = decode_forkchoice_request(4, &encoded).unwrap_err();
        assert_eq!(err, "invalid params");
    }
}
