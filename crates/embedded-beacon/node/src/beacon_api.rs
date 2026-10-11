//! Bounded read-only Beacon API backed by the same authenticated import and peer owners.
//! This server cannot import blocks, fabricate signatures, select heads or override readiness.

use crate::{
    bootstrap::checked_config_digest,
    import_coordinator::{
        ImportCoordinator, ObjectObservation, Observation, ObservationId, ObservationRequest,
        OwnerObservation, MAX_OBSERVATION_BYTES,
    },
    live_sync::PeerObservationHandle,
};
use bytes::Bytes;
use http_body_util::Full;
use hyper::{
    body::{Body, Frame, Incoming, SizeHint},
    server::conn::http1,
    service::service_fn,
    Method, Request, Response, StatusCode,
};
use hyper_util::rt::TokioIo;
use reth_embedded_beacon_config::{ChainConfig, DevnetConfig};
use reth_embedded_beacon_primitives::{root_from_hex, Fork};
use serde_json::{json, Value};
use std::{
    convert::Infallible,
    future::Future,
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use thiserror::Error;
use tokio::{
    net::TcpListener,
    sync::{OwnedSemaphorePermit, Semaphore},
    task::JoinSet,
};

const REQUEST_DEADLINE: Duration = Duration::from_secs(10);
const MAX_CONNECTIONS: usize = 4;
const MAX_ACTIVE_RESPONSES: usize = 2;
const MAX_URI_BYTES: usize = 512;

/// One socket bound after actual importer initialization; lifetime is joined by the launcher.
#[derive(Debug)]
pub struct BeaconApi {
    listener: TcpListener,
    context: Arc<ApiContext>,
}
impl BeaconApi {
    /// Bind explicit address only for the exact configuration authenticated by this importer.
    pub async fn bind(
        address: SocketAddr,
        config: ChainConfig,
        importer: ImportCoordinator,
        peers: PeerObservationHandle,
        private: Option<DevnetConfig>,
    ) -> Result<Self, BeaconApiError> {
        config.validate().map_err(|_| BeaconApiError::Configuration)?;
        if checked_config_digest(&config).map_err(|_| BeaconApiError::Configuration)? !=
            importer.config_digest()
        {
            return Err(BeaconApiError::Configuration);
        }
        if let Some(private) = &private {
            private.validate().map_err(|_| BeaconApiError::Configuration)?;
            if private.consensus != config {
                return Err(BeaconApiError::Configuration);
            }
        }
        let listener = TcpListener::bind(address).await?;
        tracing::info!(target: "beacon::api", address = %listener.local_addr()?,
            "[beacon] Bound read-only Beacon API to authenticated importer");
        Ok(Self {
            listener,
            context: Arc::new(ApiContext {
                config,
                private,
                importer,
                peers,
                responses: Arc::new(Semaphore::new(MAX_ACTIVE_RESPONSES)),
            }),
        })
    }

    /// Actual bound socket, including the operating system selected port in component controls.
    pub fn local_addr(&self) -> Result<SocketAddr, BeaconApiError> {
        Ok(self.listener.local_addr()?)
    }

    /// Bound active connections and complete response buffers; no admission waiter queue exists.
    pub async fn run(
        self,
        shutdown: impl Future<Output = ()> + Send,
    ) -> Result<(), BeaconApiError> {
        let connections = Arc::new(Semaphore::new(MAX_CONNECTIONS));
        let mut tasks = JoinSet::new();
        let mut outcome = Ok(());
        tokio::pin!(shutdown);
        loop {
            tokio::select! {
                _ = &mut shutdown => break,
                result = tasks.join_next(), if !tasks.is_empty() => {
                    if let Some(Err(error)) = result {
                        tracing::debug!(target: "beacon::api", %error, "[beacon] Beacon API connection task ended");
                    }
                }
                accepted = self.listener.accept() => {
                    let (socket, remote) = match accepted {
                        Ok(accepted) => accepted,
                        Err(error) => { outcome = Err(BeaconApiError::Io(error)); break }
                    };
                    let Ok(permit) = connections.clone().try_acquire_owned() else { drop(socket); continue };
                    let context = self.context.clone();
                    tasks.spawn(async move {
                        let _permit = permit;
                        let service = service_fn(move |request| respond(context.clone(), request));
                        let mut builder = http1::Builder::new();
                        builder.keep_alive(false).max_headers(32).max_buf_size(8192);
                        let result = tokio::time::timeout(REQUEST_DEADLINE,
                            builder.serve_connection(TokioIo::new(socket), service)).await;
                        tracing::debug!(target: "beacon::api", %remote, outcome = ?result,
                            "[beacon] Read-only Beacon API connection completed");
                    });
                }
            }
        }
        // Cancellation joins every accepted connection and releases its output reservation.
        tasks.shutdown().await;
        tracing::debug!(target: "beacon::api", "[beacon] Read-only Beacon API stopped");
        outcome
    }
}

/// The original importer remains the only graph owner; copied peer gauges are diagnostic hints.
#[derive(Debug)]
struct ApiContext {
    config: ChainConfig,
    private: Option<DevnetConfig>,
    importer: ImportCoordinator,
    peers: PeerObservationHandle,
    responses: Arc<Semaphore>,
}

/// The reservation follows the actual Bytes owner through Hyper's queue and frame clones.
#[derive(Debug)]
struct ReservedResponseBytes {
    bytes: Vec<u8>,
    _permit: Option<OwnedSemaphorePermit>,
}
impl AsRef<[u8]> for ReservedResponseBytes {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

/// Complete bounded response body; its underlying Bytes owns the admission permit.
struct ApiBody {
    inner: Full<Bytes>,
}
impl Body for ApiBody {
    type Data = Bytes;
    type Error = Infallible;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        Pin::new(&mut self.inner).poll_frame(context)
    }
    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }
    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

/// Explicit socket/identity failures; neither permits a stale-data API fallback.
#[derive(Debug, Error)]
pub enum BeaconApiError {
    /// Configuration differs from the actual authenticated importer.
    #[error("Beacon API configuration differs from the importer")]
    Configuration,
    /// Socket bind or accept failed.
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Route {
    Spec,
    Schedule,
    Genesis,
    Syncing,
    Peers,
    Header(ObservationId),
    Block(ObservationId),
    Envelope(ObservationId),
    StateRoot(ObservationId),
}
fn parse_route(path: &str) -> Option<Route> {
    if path.len() > MAX_URI_BYTES {
        return None;
    }
    match path {
        "/eth/v1/config/spec" => return Some(Route::Spec),
        "/eth/v1/config/fork_schedule" => return Some(Route::Schedule),
        "/eth/v1/beacon/genesis" => return Some(Route::Genesis),
        "/eth/v1/node/syncing" => return Some(Route::Syncing),
        "/eth/v1/node/peer_count" => return Some(Route::Peers),
        _ => {}
    }
    for (prefix, suffix, constructor) in [
        ("/eth/v1/beacon/headers/", "", Route::Header as fn(ObservationId) -> Route),
        ("/eth/v2/beacon/blocks/", "", Route::Block),
        ("/eth/v1/beacon/execution_payload_envelopes/", "", Route::Envelope),
        ("/eth/v1/beacon/states/", "/root", Route::StateRoot),
    ] {
        if let Some(value) = path.strip_prefix(prefix).and_then(|value| value.strip_suffix(suffix))
        {
            let id = match value {
                "head" => ObservationId::Head,
                "finalized" => ObservationId::Finalized,
                value if value.starts_with("0x") => ObservationId::Root(root_from_hex(value).ok()?),
                value if !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()) => {
                    ObservationId::Slot(value.parse().ok()?)
                }
                _ => return None,
            };
            return Some(constructor(id));
        }
    }
    None
}

async fn respond(
    context: Arc<ApiContext>,
    request: Request<Incoming>,
) -> Result<Response<ApiBody>, Infallible> {
    let mut permit = None;
    let (status, bytes) = if request.method() != Method::GET {
        failure(StatusCode::METHOD_NOT_ALLOWED, "Read-only GET endpoint")
    } else if request.uri().to_string().len() > MAX_URI_BYTES ||
        request.uri().query().is_some() ||
        request.headers().contains_key(hyper::header::TRANSFER_ENCODING) ||
        request.headers().get(hyper::header::CONTENT_LENGTH).is_some_and(|value| value != "0")
    {
        failure(StatusCode::BAD_REQUEST, "Invalid bounded read request")
    } else if let Some(route) = parse_route(request.uri().path()) {
        match context.responses.clone().try_acquire_owned() {
            Ok(reserved) => {
                permit = Some(reserved);
                match tokio::time::timeout(REQUEST_DEADLINE, dispatch(&context, route)).await {
                    Ok(Ok(bytes)) => (StatusCode::OK, bytes),
                    Ok(Err((status, message))) => failure(status, message),
                    Err(_) => {
                        failure(StatusCode::SERVICE_UNAVAILABLE, "Owner observation timed out")
                    }
                }
            }
            Err(_) => {
                failure(StatusCode::SERVICE_UNAVAILABLE, "Observation response budget exhausted")
            }
        }
    } else {
        failure(StatusCode::NOT_FOUND, "Endpoint or selector unavailable")
    };
    let mut response = Response::new(ApiBody {
        inner: Full::new(Bytes::from_owner(ReservedResponseBytes { bytes, _permit: permit })),
    });
    *response.status_mut() = status;
    response
        .headers_mut()
        .insert(hyper::header::CONTENT_TYPE, "application/json".parse().expect("constant header"));
    response
        .headers_mut()
        .insert(hyper::header::CONNECTION, "close".parse().expect("constant header"));
    response
        .headers_mut()
        .insert(hyper::header::CACHE_CONTROL, "no-store".parse().expect("constant header"));
    Ok(response)
}

fn failure(status: StatusCode, message: &'static str) -> (StatusCode, Vec<u8>) {
    (
        status,
        serde_json::to_vec(&json!({"code": status.as_u16(), "message": message}))
            .expect("fixed scalar JSON"),
    )
}
fn hex(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2 + 2);
    output.push_str("0x");
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 15) as usize] as char);
    }
    output
}
const fn fork_name(fork: Fork) -> &'static str {
    match fork {
        Fork::Phase0 => "phase0",
        Fork::Altair => "altair",
        Fork::Bellatrix => "bellatrix",
        Fork::Capella => "capella",
        Fork::Deneb => "deneb",
        Fork::Electra => "electra",
        Fork::Fulu => "fulu",
        Fork::Gloas => "gloas",
    }
}
fn config_response(config: &ChainConfig, private: Option<&DevnetConfig>, route: Route) -> Value {
    let activations = config.forks.activations();
    let genesis_version = activations.first().expect("checked schedule").version;
    match route {
        Route::Genesis => json!({"data": {"genesis_time": config.genesis_time.to_string(),
            "genesis_validators_root": hex(&config.genesis_validators_root), "genesis_fork_version": hex(&genesis_version)}}),
        Route::Schedule => {
            let mut previous = genesis_version;
            let schedule: Vec<_> = activations.iter().map(|activation| {
                let entry = json!({"previous_version":hex(&previous), "current_version":hex(&activation.version), "epoch":activation.epoch.to_string()});
                previous = activation.version; entry
            }).collect();
            json!({"data":schedule})
        }
        Route::Spec => {
            let mut values = serde_json::Map::new();
            values.insert("SLOTS_PER_EPOCH".into(), json!(config.slots_per_epoch.to_string()));
            values.insert("SECONDS_PER_SLOT".into(), json!(config.seconds_per_slot.to_string()));
            values.insert(
                "SLOT_DURATION_MS".into(),
                json!((config.seconds_per_slot * 1000).to_string()),
            );
            values.insert("GENESIS_FORK_VERSION".into(), json!(hex(&genesis_version)));
            for activation in activations {
                let name = fork_name(activation.fork).to_ascii_uppercase();
                values.insert(format!("{name}_FORK_EPOCH"), json!(activation.epoch.to_string()));
                values.insert(format!("{name}_FORK_VERSION"), json!(hex(&activation.version)));
            }
            if let Some(private) = private {
                values
                    .insert("TERMINAL_BLOCK_HASH".into(), json!(hex(&private.terminal_block_hash)));
                values.insert(
                    "TERMINAL_BLOCK_HASH_ACTIVATION_EPOCH".into(),
                    json!(private.terminal_block_hash_activation_epoch.to_string()),
                );
            }
            json!({"data": values})
        }
        _ => unreachable!(),
    }
}
async fn dispatch(
    context: &ApiContext,
    route: Route,
) -> Result<Vec<u8>, (StatusCode, &'static str)> {
    if matches!(route, Route::Spec | Route::Schedule | Route::Genesis) {
        return serde_json::to_vec(&config_response(
            &context.config,
            context.private.as_ref(),
            route,
        ))
        .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "Configuration serialization failed"));
    }
    if route == Route::Peers {
        return Ok(serde_json::to_vec(
            &json!({"data":{"connected":context.peers.snapshot().connected_peers.to_string()}}),
        )
        .expect("scalar count JSON"));
    }
    let request = match route {
        Route::Syncing => ObservationRequest::Status,
        Route::Header(id) => ObservationRequest::Block { id, header_only: true },
        Route::Block(id) => ObservationRequest::Block { id, header_only: false },
        Route::Envelope(id) => ObservationRequest::Envelope(id),
        Route::StateRoot(id) => ObservationRequest::StateRoot(id),
        _ => unreachable!(),
    };
    let observation = context
        .importer
        .observe(request)
        .await
        .map_err(|_| (StatusCode::SERVICE_UNAVAILABLE, "Authenticated owner unavailable"))?;
    match observation {
        Observation::Status(status) => Ok(syncing_response(context, &status)),
        Observation::Object(Some(object)) => object_response(route, object),
        Observation::Object(None) => {
            Err((StatusCode::NOT_FOUND, "Original retained object unavailable"))
        }
    }
}
fn syncing_response(context: &ApiContext, status: &OwnerObservation) -> Vec<u8> {
    let peers = context.peers.snapshot();
    let distance = peers.sync_distance(status);
    let syncing = peers.is_syncing(status);
    serde_json::to_vec(&json!({"data":{"head_slot":status.snapshot.head_slot.to_string(),
        "sync_distance":distance.to_string(), "is_syncing":syncing,
        "is_optimistic":status.head_optimistic, "el_offline":status.engine_closed}}))
    .expect("scalar owner JSON")
}
fn object_response(
    route: Route,
    object: ObjectObservation,
) -> Result<Vec<u8>, (StatusCode, &'static str)> {
    let metadata = json!({"version":fork_name(object.fork), "execution_optimistic":object.execution_optimistic,
        "finalized":object.finalized});
    let mut output = serde_json::to_vec(&metadata).expect("scalar metadata JSON");
    output.pop();
    output.extend_from_slice(b",\"data\":");
    if matches!(route, Route::StateRoot(_)) {
        output.extend_from_slice(
            &serde_json::to_vec(&json!({"root":hex(&object.header.state_root)}))
                .expect("root JSON"),
        );
    } else {
        let prefix = if matches!(route, Route::Header(_)) {
            let mut prefix = serde_json::to_vec(
                &json!({"root":hex(&object.block_root), "canonical":object.canonical}),
            )
            .expect("header metadata JSON");
            prefix.pop();
            prefix.extend_from_slice(b",\"header\":");
            prefix
        } else {
            Vec::new()
        };
        let closing = if prefix.is_empty() { 1 } else { 2 };
        let complete = output
            .len()
            .checked_add(prefix.len())
            .and_then(|size| size.checked_add(object.json.len()))
            .and_then(|size| size.checked_add(closing));
        if complete.is_none_or(|size| size > MAX_OBSERVATION_BYTES) {
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                "Complete observation response exceeds its limit",
            ));
        }
        output
            .try_reserve_exact(prefix.len() + object.json.len() + closing)
            .map_err(|_| (StatusCode::SERVICE_UNAVAILABLE, "Observation allocation unavailable"))?;
        output.extend_from_slice(&prefix);
        output.extend_from_slice(&object.json);
        if !prefix.is_empty() {
            output.push(b'}');
        }
    }
    output.push(b'}');
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use reth_embedded_beacon_primitives::BeaconBlockHeader;

    #[test]
    fn selectors_are_exact_bounded_and_reject_ambiguous_inputs() {
        assert_eq!(
            parse_route("/eth/v2/beacon/blocks/32"),
            Some(Route::Block(ObservationId::Slot(32)))
        );
        assert_eq!(
            parse_route("/eth/v1/beacon/states/finalized/root"),
            Some(Route::StateRoot(ObservationId::Finalized))
        );
        for invalid in [
            "/eth/v2/beacon/blocks/+1",
            "/eth/v2/beacon/blocks/18446744073709551616",
            "/eth/v2/beacon/blocks/0x00",
            "/eth/v1/beacon/headers/head/",
            "/eth/v1/beacon/states/1",
        ] {
            assert_eq!(parse_route(invalid), None, "{invalid}");
        }
    }
    #[test]
    fn configuration_preserves_checked_identity_and_far_future_epochs() {
        let config = ChainConfig::mainnet();
        let spec = config_response(&config, None, Route::Spec);
        assert_eq!(spec["data"]["SLOTS_PER_EPOCH"], "32");
        assert_eq!(spec["data"]["GLOAS_FORK_EPOCH"], u64::MAX.to_string());
        let genesis = config_response(&config, None, Route::Genesis);
        assert_eq!(
            genesis["data"]["genesis_validators_root"],
            hex(&config.genesis_validators_root)
        );
        let schedule = config_response(&config, None, Route::Schedule);
        assert_eq!(schedule["data"].as_array().unwrap().len(), config.forks.activations().len());
    }
    fn object(json: Vec<u8>) -> ObjectObservation {
        ObjectObservation {
            fork: Fork::Gloas,
            block_root: [1; 32],
            header: BeaconBlockHeader::default(),
            canonical: true,
            finalized: false,
            execution_optimistic: true,
            json,
        }
    }
    #[test]
    fn original_json_and_actual_metadata_remain_distinct() {
        let rendered = object_response(
            Route::Block(ObservationId::Head),
            object(b"{\"message\":{},\"signature\":\"0x12\"}".to_vec()),
        )
        .unwrap();
        let parsed: Value = serde_json::from_slice(&rendered).unwrap();
        assert_eq!(
            parsed,
            json!({"version":"gloas", "finalized":false,"execution_optimistic":true,
            "data":{"message":{},"signature":"0x12"}})
        );
    }
    #[test]
    fn complete_response_limit_includes_metadata_and_original_data() {
        let oversized = object(vec![b' '; MAX_OBSERVATION_BYTES]);
        assert_eq!(
            object_response(Route::Block(ObservationId::Head), oversized).unwrap_err(),
            (StatusCode::SERVICE_UNAVAILABLE, "Complete observation response exceeds its limit")
        );
    }

    #[test]
    fn response_reservation_survives_body_release_and_wire_frame_clones() {
        let budget = Arc::new(Semaphore::new(1));
        let permit = budget.clone().try_acquire_owned().unwrap();
        let bytes = Bytes::from_owner(ReservedResponseBytes {
            bytes: vec![1, 2, 3],
            _permit: Some(permit),
        });
        let frame_clone = bytes.clone();
        drop(bytes);
        assert_eq!(budget.available_permits(), 0);
        assert_eq!(frame_clone.as_ref(), &[1, 2, 3]);
        drop(frame_clone);
        assert_eq!(budget.available_permits(), 1);
    }
}

#[cfg(test)]
mod socket_tests;
