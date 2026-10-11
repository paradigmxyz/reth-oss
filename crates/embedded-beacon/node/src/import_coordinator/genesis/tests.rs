//! Genuine dedicated-owner genesis provenance and concrete cold original-state controls.

use super::*;
use crate::{bootstrap::genesis::tests::genuine, compact_persistence::PersistenceNetwork};
use reth_embedded_beacon_execution_proxy::{ExecutionProxy, PayloadVersion};
use reth_embedded_beacon_primitives::Checkpoint;
use reth_embedded_beacon_shared_data::SharedDataStore;
use reth_embedded_beacon_storage::CompactStateLimits;
use std::io;
use tempfile::TempDir;

#[derive(Clone, Debug)]
struct UnusedEngine;
impl ExecutionEngine for UnusedEngine {
    type Payload = ExecutionData;
    type PayloadReply = PayloadReply;
    type Forkchoice = ForkchoiceState;
    type ForkchoiceReply = ForkchoiceReply;
    type Error = io::Error;
    async fn new_payload(
        &self,
        _: PayloadVersion,
        _: ExecutionData,
    ) -> Result<PayloadReply, io::Error> {
        Err(io::Error::other("genesis cannot fabricate an execution payload"))
    }
    async fn forkchoice_updated(
        &self,
        _: ForkchoiceVersion,
        _: ForkchoiceState,
        _: Option<[u8; 16]>,
    ) -> Result<ForkchoiceReply, io::Error> {
        Err(io::Error::other("genesis cannot fabricate an execution fork choice"))
    }
}
#[derive(Debug)]
struct Missing;
impl ClassicAvailability for Missing {
    type Error = io::Error;
    async fn verify(&self, _: Root, _: &BeaconBlock) -> Result<(), io::Error> {
        Err(io::Error::other("no retrieved original data"))
    }
}
impl GloasAvailability for Missing {
    type Error = io::Error;
    async fn verify(&self, _: Root, _: &gloas::BeaconBlock) -> Result<(), io::Error> {
        Err(io::Error::other("no retrieved original data"))
    }
}
fn owner_parts(
    config: &ChainConfig,
    directory: &TempDir,
) -> (AdmittedStateRepository, RuntimeEngineAdapter<UnusedEngine>) {
    let network = PersistenceNetwork::from_config(config.clone()).unwrap();
    let shared = SharedDataStore::open(directory.path(), network.identity(), 0).unwrap();
    let repository =
        AdmittedStateRepository::open(shared, network, CompactStateLimits::default()).unwrap();
    let engine = RuntimeEngineAdapter::new(
        ExecutionProxy::new(UnusedEngine, Duration::from_secs(1)).unwrap(),
        config.clone(),
    )
    .unwrap();
    (repository, engine)
}
/// Mint test provenance only by constructing the actual dedicated genesis graph owner.
pub(crate) async fn genesis_status_for_tests(
    config: ChainConfig,
    genesis: VerifiedGenesis,
    now: u64,
) -> AuthenticatedStatus {
    let runtime = Handle::current();
    tokio::task::spawn_blocking(move || {
        let directory = TempDir::new().unwrap();
        let (repository, engine) = owner_parts(&config, &directory);
        let clock = OwnerClock::Controlled(Arc::new(std::sync::atomic::AtomicU64::new(now)));
        let setup = GenesisSetup {
            config,
            anchor: GenesisImportAnchor {
                genesis,
                decode_limits: BootstrapDecodeLimits::default(),
            },
            repository,
            engine,
            classic_availability: Missing,
            gloas_availability: Missing,
            limits: ImportLimits::default(),
            runtime,
            clock,
        };
        let worker = Worker::initialize_genesis(setup, Ok(now), false).unwrap();
        let context = worker.retained_custody_anchor().unwrap().unwrap();
        assert!(context.is_genuine_genesis());
        assert_eq!(context.slot(), 0);
        assert!(worker.custody_dependency().unwrap().is_none());
        worker.authenticated_status().unwrap()
    })
    .await
    .unwrap()
}
/// Same genuine originals undergo concrete checkpoint verification and its separate owner path.
pub(crate) async fn checkpoint_status_for_tests(
    config: ChainConfig,
    genesis: VerifiedGenesis,
    now: u64,
) -> AuthenticatedStatus {
    let runtime = Handle::current();
    tokio::task::spawn_blocking(move || {
        let directory = TempDir::new().unwrap();
        let (repository, engine) = owner_parts(&config, &directory);
        let state_ssz = genesis.original_state_ssz();
        let block_ssz = genesis.block_ssz();
        let verifier = SszCheckpointVerifier::new(
            config.clone(),
            BootstrapDecodeLimits::default(),
            CheckpointBlockFormat::Unsigned,
        )
        .unwrap();
        let verified = verifier
            .verify_checkpoint(
                &state_ssz,
                &block_ssz,
                Checkpoint { epoch: 0, root: genesis.block_root() },
                now,
                1,
            )
            .unwrap();
        let setup = WorkerSetup {
            config,
            anchor: ImportAnchor {
                verified,
                state_ssz,
                block_ssz,
                block_format: CheckpointBlockFormat::Unsigned,
                max_age_epochs: 1,
                decode_limits: BootstrapDecodeLimits::default(),
                replay: None,
            },
            repository,
            engine,
            classic_availability: Missing,
            gloas_availability: Missing,
            limits: ImportLimits::default(),
            runtime,
            clock: OwnerClock::Controlled(Arc::new(std::sync::atomic::AtomicU64::new(now))),
        };
        let worker = Worker::initialize(setup, Ok(now), false, None).unwrap();
        let context = worker.retained_custody_anchor().unwrap().unwrap();
        assert!(!context.is_genuine_genesis());
        let dependency = worker.custody_dependency().unwrap().unwrap();
        assert_eq!(dependency.root(), context.root());
        assert_eq!(dependency.upper_slot(), 1);
        worker.authenticated_status().unwrap()
    })
    .await
    .unwrap()
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn genuine_origin_is_preserved_only_by_distinct_genesis_owner() {
    let (config, state, _, _) = genuine(Fork::Phase0);
    let genesis = VerifiedGenesis::authenticate(
        config.clone(),
        state.to_ssz().unwrap().into(),
        BootstrapDecodeLimits::default(),
    )
    .unwrap();
    let status =
        crate::import_coordinator::genesis_status_for_tests(config.clone(), genesis.clone(), 0)
            .await;
    assert_eq!(status.genesis_origin(), Some(genesis.block_root()));
    assert_eq!(status.snapshot().head.root, genesis.block_root());
    let checkpoint =
        crate::import_coordinator::checkpoint_status_for_tests(config, genesis, 0).await;
    assert_eq!(checkpoint.genesis_origin(), None);
}

/// Run the actual dedicated genesis initializer against a physically reopened later-cut store.
pub(crate) fn assert_genesis_custody_after_cold_reopen(
    config: ChainConfig,
    genesis: VerifiedGenesis,
    directory: &std::path::Path,
    now: u64,
) {
    let network = PersistenceNetwork::from_config(config.clone()).unwrap();
    let shared = SharedDataStore::open(directory, network.identity(), 0).unwrap();
    let repository =
        AdmittedStateRepository::open(shared.clone(), network, CompactStateLimits::default())
            .unwrap();
    let engine = RuntimeEngineAdapter::new(
        ExecutionProxy::new(UnusedEngine, Duration::from_secs(1)).unwrap(),
        config.clone(),
    )
    .unwrap();
    let origin = genesis.block_root();
    let original = genesis.block_ssz();
    let state_root = genesis.state_root();
    let config_digest = genesis.config_digest();
    let setup = GenesisSetup {
        config,
        anchor: GenesisImportAnchor { genesis, decode_limits: BootstrapDecodeLimits::default() },
        repository,
        engine,
        classic_availability: Missing,
        gloas_availability: Missing,
        limits: ImportLimits::default(),
        runtime: Handle::current(),
        clock: OwnerClock::Controlled(Arc::new(std::sync::atomic::AtomicU64::new(now))),
    };
    let worker = Worker::initialize_genesis(setup, Ok(now), true).unwrap();
    assert_ne!(worker.graph.snapshot().unwrap().finalized_checkpoint.root, origin);
    assert!(worker.graph.serving_header(origin).unwrap().is_none());
    let context = worker.retained_custody_anchor().unwrap().unwrap();
    assert!(context.is_genuine_genesis());
    assert_eq!(context.original(), original.as_ref());
    assert_eq!(context.root(), origin);
    assert_eq!(context.slot(), 0);
    assert_eq!(context.config_digest(), config_digest);
    assert_eq!(context.proposer_key(), None);
    let header = context.block().unwrap().header().unwrap();
    assert_eq!(header.slot, 0);
    assert_eq!(header.parent_root, [0; 32]);
    assert_eq!(header.proposer_index, 0);
    assert_eq!(header.state_root, state_root);
    assert_eq!(worker.custody_anchor(origin).unwrap().unwrap().original(), original.as_ref());
    let owner = reth_embedded_beacon_shared_data::Owner::Recovery(
        reth_embedded_beacon_primitives::sha256(b"embedded-beacon-unsigned-sidecar-context-v1"),
    );
    let object = reth_embedded_beacon_shared_data::ObjectId::new(
        reth_embedded_beacon_shared_data::ObjectKind::BeaconBlock,
        reth_embedded_beacon_shared_data::Encoding::Ssz,
        &original,
    );
    assert_eq!(shared.owner_objects(owner), vec![object]);
    assert_eq!(shared.get(object).unwrap().unwrap().as_ref(), original.as_ref());
    assert!(worker.last_verified_forkchoice.is_none());
    assert!(worker.replaying);
}
