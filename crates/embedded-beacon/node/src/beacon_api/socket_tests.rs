//! Real read-only HTTP against an authenticated native pre-execution owner.
//! No callback may invent EL validity, and no unsigned anchor acquires a signature.

use super::*;
use crate::{
    compact_persistence::{AdmittedStateRepository, PersistenceNetwork},
    gossip_tests::{phase0_signed_child, store_and_keys},
    import_coordinator::{ImportAnchor, ImportLimits},
    live_sync::availability::RetrievalAvailability,
    runtime_engine::RuntimeEngineAdapter,
    BootstrapDecodeLimits, CheckpointBlockFormat, SszCheckpointVerifier,
};
use alloy_rpc_types_engine::{ExecutionData, ForkchoiceState};
use reth_embedded_beacon_config::{
    BlobParameters, ForkActivation, ForkSchedule, Preset as ConfigPreset,
};
use reth_embedded_beacon_execution_proxy::{
    ExecutionEngine, ExecutionProxy, ForkchoiceReply, ForkchoiceVersion, PayloadReply,
    PayloadVersion,
};
use reth_embedded_beacon_fork_choice::{Store, StoreParameters};
use reth_embedded_beacon_network::block_rpc::WireBlock;
use reth_embedded_beacon_primitives::Checkpoint;
use reth_embedded_beacon_shared_data::SharedDataStore;
use reth_embedded_beacon_state_transition::{
    fork_processing::TransitionParameters,
    ssz::{BeaconBlock, BeaconState, SignedBeaconBlock},
};
use reth_embedded_beacon_storage::CompactStateLimits;
use std::{
    io,
    time::{SystemTime, UNIX_EPOCH},
};
use tempfile::TempDir;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::oneshot,
};

#[derive(Clone, Debug)]
struct NoExecution;
impl ExecutionEngine for NoExecution {
    type Payload = ExecutionData;
    type Forkchoice = ForkchoiceState;
    type PayloadReply = PayloadReply;
    type ForkchoiceReply = ForkchoiceReply;
    type Error = io::Error;
    async fn new_payload(
        &self,
        _version: PayloadVersion,
        _payload: ExecutionData,
    ) -> Result<PayloadReply, io::Error> {
        Err(io::Error::other("pre-execution HTTP control must not call Engine"))
    }
    async fn forkchoice_updated(
        &self,
        _version: ForkchoiceVersion,
        _state: ForkchoiceState,
        _custody: Option<[u8; 16]>,
    ) -> Result<ForkchoiceReply, io::Error> {
        Err(io::Error::other("pre-execution HTTP control must not publish Engine validity"))
    }
}

fn checked_config(state: &BeaconState, parameters: &TransitionParameters) -> ChainConfig {
    ChainConfig {
        name: "actual-beacon-http-test".into(),
        preset: ConfigPreset::Minimal,
        chain_id: 12345,
        genesis_time: state.genesis_time(),
        genesis_validators_root: state.genesis_validators_root(),
        seconds_per_slot: parameters.gloas.fulu.electra.deneb.capella.bellatrix.seconds_per_slot,
        slots_per_epoch: parameters.slots_per_epoch(),
        forks: ForkSchedule::new(
            parameters
                .schedule
                .iter()
                .map(|entry| ForkActivation {
                    fork: entry.fork,
                    epoch: entry.epoch,
                    version: entry.version,
                })
                .collect(),
        )
        .unwrap(),
        blob_schedule: parameters
            .gloas
            .fulu
            .blob_schedule
            .iter()
            .map(|entry| BlobParameters {
                epoch: entry.epoch,
                max_blobs_per_block: entry.max_blobs_per_block,
            })
            .collect(),
        max_payload_size: 10 * 1024 * 1024,
    }
}

fn wallclock() -> u64 {
    u64::try_from(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis()).unwrap()
}

async fn request(
    address: SocketAddr,
    method: &str,
    path: &str,
    extra_headers: &str,
) -> (u16, Value) {
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut socket = TcpStream::connect(address).await.unwrap();
        let request = format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n{extra_headers}\r\n");
        socket.write_all(request.as_bytes()).await.unwrap();
        let mut bytes = Vec::new();
        socket.take(64 * 1024 + 1).read_to_end(&mut bytes).await.unwrap();
        assert!(bytes.len() <= 64 * 1024, "component HTTP response must remain bounded");
        let boundary = bytes.windows(4).position(|part| part == b"\r\n\r\n").unwrap();
        let headers = std::str::from_utf8(&bytes[..boundary]).unwrap();
        let status = headers.split_whitespace().nth(1).unwrap().parse().unwrap();
        let body = serde_json::from_slice(&bytes[boundary + 4..]).unwrap();
        (status, body)
    }).await.unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_socket_reports_only_original_signed_native_admissions_and_joins_shutdown() {
    let directory = TempDir::new().unwrap();
    let (original, keys, parameters) = store_and_keys(Fork::Phase0);
    let root = original.get_head().unwrap().root;
    let BeaconState::Phase0(mut state, preset) = original.block_state(root).unwrap().clone() else {
        unreachable!()
    };
    let BeaconBlock::Phase0(mut block, _) = original.block(root).unwrap().clone() else {
        unreachable!()
    };
    state.genesis_time = wallclock() / 1000 -
        2 * parameters.gloas.fulu.electra.deneb.capella.bellatrix.seconds_per_slot;
    block.state_root = state.hash_tree_root(preset).unwrap();
    let mut store = Store::get_store(
        BeaconState::Phase0(state, preset),
        BeaconBlock::Phase0(block, preset),
        StoreParameters::for_preset(preset),
    )
    .unwrap();
    store.set_transition_parameters(parameters.clone()).unwrap();
    let root = store.get_head().unwrap().root;
    let state = store.block_state(root).unwrap();
    let config = checked_config(state, &parameters);
    let trusted = Checkpoint { epoch: 0, root };
    let state_ssz = Arc::<[u8]>::from(state.to_ssz().unwrap());
    let block_ssz = Arc::<[u8]>::from(store.block(root).unwrap().to_ssz().unwrap());
    let limits = BootstrapDecodeLimits::default();
    let verifier =
        SszCheckpointVerifier::new(config.clone(), limits, CheckpointBlockFormat::Unsigned)
            .unwrap();
    let verified =
        verifier.verify_checkpoint(&state_ssz, &block_ssz, trusted, wallclock(), 1).unwrap();
    let network = PersistenceNetwork::from_config(config.clone()).unwrap();
    let shared = SharedDataStore::open(directory.path(), network.identity(), 0).unwrap();
    let repository =
        AdmittedStateRepository::open(shared, network, CompactStateLimits::default()).unwrap();
    let engine = RuntimeEngineAdapter::new(
        ExecutionProxy::new(NoExecution, Duration::from_secs(1)).unwrap(),
        config.clone(),
    )
    .unwrap();
    let (availability, _broker) = RetrievalAvailability::new(config.clone()).unwrap();
    let importer = ImportCoordinator::start(
        config.clone(),
        ImportAnchor {
            verified,
            state_ssz,
            block_ssz,
            block_format: CheckpointBlockFormat::Unsigned,
            max_age_epochs: 1,
            decode_limits: limits,
            replay: None,
        },
        repository,
        engine,
        availability.clone(),
        availability,
        ImportLimits::default(),
    )
    .await
    .unwrap();
    // No peer owner has started: these zero observations truthfully report that absence.
    let api = BeaconApi::bind(
        "127.0.0.1:0".parse().unwrap(),
        config.clone(),
        importer.clone(),
        PeerObservationHandle::default(),
        None,
    )
    .await
    .unwrap();
    let address = api.local_addr().unwrap();
    let (shutdown, stopped) = oneshot::channel();
    let task = tokio::spawn(api.run(async move {
        let _ = stopped.await;
    }));
    let (status, body) = request(address, "GET", "/eth/v1/beacon/genesis", "").await;
    assert_eq!(status, 200);
    assert_eq!(body["data"]["genesis_validators_root"], hex(&config.genesis_validators_root));
    assert_eq!(body["data"]["genesis_time"], config.genesis_time.to_string());
    assert_eq!(request(address, "GET", "/eth/v1/beacon/headers/head", "").await.0, 404);
    let signed = phase0_signed_child(&store, &keys, &parameters);
    let original_signature = signed.signature;
    let original_state_root = signed.message.state_root;
    let wire =
        WireBlock::from_ssz(SignedBeaconBlock::Phase0(signed, preset).to_ssz().unwrap(), &config)
            .unwrap();
    let signed_root = wire.root();
    let receipt = importer.import_block(wire, wallclock()).await.unwrap();
    assert_eq!(receipt.snapshot.head.root, signed_root);
    assert!(!receipt.execution_forkchoice_updated);
    let (status, body) = request(address, "GET", "/eth/v2/beacon/blocks/head", "").await;
    assert_eq!(status, 200);
    assert_eq!(body["version"], "phase0");
    assert_eq!(body["execution_optimistic"], false);
    assert_eq!(body["data"]["message"]["slot"], "1");
    assert_eq!(body["data"]["signature"], hex(&original_signature));
    let (status, body) = request(address, "GET", "/eth/v1/beacon/headers/head", "").await;
    assert_eq!(status, 200);
    assert_eq!(body["data"]["root"], hex(&signed_root));
    assert_eq!(body["data"]["canonical"], true);
    assert_eq!(body["data"]["header"]["signature"], hex(&original_signature));
    let (status, body) = request(address, "GET", "/eth/v1/beacon/states/head/root", "").await;
    assert_eq!(status, 200);
    assert_eq!(body["data"]["root"], hex(&original_state_root));
    assert_eq!(
        request(address, "GET", "/eth/v1/beacon/execution_payload_envelopes/head", "").await.0,
        404
    );
    assert_eq!(request(address, "POST", "/eth/v1/node/syncing", "").await.0, 405);
    assert_eq!(request(address, "GET", "/eth/v1/node/syncing?x=1", "").await.0, 400);
    assert_eq!(
        request(address, "GET", "/eth/v1/node/syncing", "Content-Length: 1\r\n").await.0,
        400
    );
    let (status, body) = request(address, "GET", "/eth/v1/node/peer_count", "").await;
    assert_eq!(status, 200);
    assert_eq!(body["data"]["connected"], "0");
    assert!(!importer.is_halted());
    shutdown.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(2), task).await.unwrap().unwrap().unwrap();
    assert!(TcpStream::connect(address).await.is_err());
}
