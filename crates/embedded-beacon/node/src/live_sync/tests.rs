//! Actual loopback signed-block IO followed by mandatory BLS/state/persistence admission.
//! This verifies the concrete boundary; it is not a public-network or real Engine sync test.

use super::*;
use crate::{
    compact_persistence::{AdmittedStateRepository, PersistenceNetwork},
    gossip_tests::{phase0_signed_child, store_and_keys},
    import_coordinator::{ImportAnchor, ImportLimits},
    runtime_engine::RuntimeEngineAdapter,
    BootstrapDecodeLimits, CheckpointBlockFormat, SszCheckpointVerifier,
};
use alloy_rpc_types_engine::{ExecutionData, ForkchoiceState};
use libp2p::{identity::Keypair, Multiaddr};
use reth_embedded_beacon_config::{
    BlobParameters, ForkActivation, ForkSchedule, Preset as ConfigPreset,
};
use reth_embedded_beacon_execution_proxy::{
    ExecutionEngine, ExecutionProxy, ForkchoiceReply, ForkchoiceVersion, PayloadReply,
    PayloadVersion,
};
use reth_embedded_beacon_fork_choice::Store;
use reth_embedded_beacon_network::{block_rpc::WireBlock, transport::StatusTransport, StatusV2};
use reth_embedded_beacon_primitives::Checkpoint;
use reth_embedded_beacon_shared_data::SharedDataStore;
use reth_embedded_beacon_state_transition::{
    fork_processing::TransitionParameters,
    ssz::{BeaconBlock, BeaconState, SignedBeaconBlock},
};
use reth_embedded_beacon_storage::CompactStateLimits;
use std::{io, sync::Arc};
use tempfile::TempDir;

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
        Err(io::Error::other("unexpected execution callback for genuine pre-execution input"))
    }
    async fn forkchoice_updated(
        &self,
        _version: ForkchoiceVersion,
        _state: ForkchoiceState,
        _custody: Option<[u8; 16]>,
    ) -> Result<ForkchoiceReply, io::Error> {
        Err(io::Error::other("unexpected execution fork choice for pre-execution input"))
    }
}
fn config(state: &BeaconState, parameters: &TransitionParameters) -> ChainConfig {
    ChainConfig {
        name: "actual-block-import-test".into(),
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
pub(super) fn transport(
    config: &ChainConfig,
    checkpoint: Checkpoint,
    head: Root,
    slot: u64,
) -> StatusTransport {
    StatusTransport::new(
        Keypair::generate_secp256k1(),
        config.clone(),
        slot,
        checkpoint,
        vec![(checkpoint.epoch, checkpoint.root)],
        StatusV2 {
            status: Status {
                fork_digest: config.fork_digest(slot / config.slots_per_epoch).unwrap(),
                finalized_root: checkpoint.root,
                finalized_epoch: checkpoint.epoch,
                head_root: head,
                head_slot: slot,
            },
            earliest_available_slot: 0,
        },
    )
    .unwrap()
}
pub(super) async fn connect(left: &mut StatusTransport, right: &mut StatusTransport) {
    left.listen_on("/ip4/127.0.0.1/tcp/0".parse::<Multiaddr>().unwrap()).unwrap();
    let TransportEvent::Listening(address) = left.next_event().await else {
        panic!("listener not ready")
    };
    right.dial(left.peer_id(), address).unwrap();
    let mut a = false;
    let mut b = false;
    while !a || !b {
        tokio::select! {
            event = left.next_event() => if let TransportEvent::PeerStatus { compatibility, .. } = event {
                assert_eq!(compatibility, StatusCompatibility::KnownFinality); a = true;
            },
            event = right.next_event() => if let TransportEvent::PeerStatus { compatibility, .. } = event {
                assert_eq!(compatibility, StatusCompatibility::KnownFinality); b = true;
            },
        }
    }
}
async fn retrieve(
    left: &mut StatusTransport,
    right: &mut StatusTransport,
    wire: WireBlock,
) -> BlockBatch {
    retrieve_with_id(left, right, wire).await.1
}
async fn retrieve_with_id(
    left: &mut StatusTransport,
    right: &mut StatusTransport,
    wire: WireBlock,
) -> (BlockRequestId, BlockBatch) {
    let id = right
        .request_blocks(left.peer_id(), BlockRequest::Range { start_slot: wire.slot(), count: 1 })
        .unwrap();
    loop {
        tokio::select! {
            event = left.next_event() => if let TransportEvent::BlockRequest { id, .. } = event {
                left.respond_blocks(id, vec![wire.clone()]).unwrap();
            },
            event = right.next_event() => if let TransportEvent::Blocks { id: observed, batch, .. } = event {
                assert_eq!(observed, id); return (id, batch);
            },
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_tcp_signed_range_rejects_forged_bls_then_commits_real_chain_advance() {
    let directory = TempDir::new().unwrap();
    let (original, keys, parameters) = store_and_keys(Fork::Phase0);
    let root = original.get_head().unwrap().root;
    let BeaconState::Phase0(mut state, preset) = original.block_state(root).unwrap().clone() else {
        unreachable!()
    };
    let BeaconBlock::Phase0(mut block, _) = original.block(root).unwrap().clone() else {
        unreachable!()
    };
    // Bind genuine genesis validator material to this isolated test's actual wall clock.
    state.genesis_time = now().unwrap() / 1000 -
        parameters.gloas.fulu.electra.deneb.capella.bellatrix.seconds_per_slot;
    block.state_root = state.hash_tree_root(preset).unwrap();
    let mut store = Store::get_store(
        BeaconState::Phase0(state, preset),
        BeaconBlock::Phase0(block, preset),
        reth_embedded_beacon_fork_choice::StoreParameters::for_preset(preset),
    )
    .unwrap();
    store.set_transition_parameters(parameters.clone()).unwrap();
    let root = store.get_head().unwrap().root;
    let state = store.block_state(root).unwrap();
    let config = config(state, &parameters);
    let trusted = Checkpoint { epoch: 0, root };
    let state_ssz = Arc::<[u8]>::from(state.to_ssz().unwrap());
    let block_ssz = Arc::<[u8]>::from(store.block(root).unwrap().to_ssz().unwrap());
    let verifier = SszCheckpointVerifier::new(
        config.clone(),
        BootstrapDecodeLimits::default(),
        CheckpointBlockFormat::Unsigned,
    )
    .unwrap();
    let verified =
        verifier.verify_checkpoint(&state_ssz, &block_ssz, trusted, now().unwrap(), 1).unwrap();
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
            decode_limits: BootstrapDecodeLimits::default(),
            replay: None,
        },
        repository,
        engine,
        availability.clone(),
        availability.clone(),
        ImportLimits::default(),
    )
    .await
    .unwrap();
    let signed = phase0_signed_child(&store, &keys, &parameters);
    let real = WireBlock::from_ssz(
        SignedBeaconBlock::Phase0(signed.clone(), preset).to_ssz().unwrap(),
        &config,
    )
    .unwrap();
    let mut forged = signed;
    forged.signature[0] ^= 1;
    let forged =
        WireBlock::from_ssz(SignedBeaconBlock::Phase0(forged, preset).to_ssz().unwrap(), &config)
            .unwrap();
    let mut server = transport(&config, trusted, real.root(), 1);
    let mut follower = transport(&config, trusted, root, 1);
    tokio::time::timeout(Duration::from_secs(10), connect(&mut server, &mut follower))
        .await
        .unwrap();
    let batch =
        tokio::time::timeout(Duration::from_secs(10), retrieve(&mut server, &mut follower, forged))
            .await
            .unwrap();
    assert!(matches!(
        importer.import_block(batch.blocks()[0].clone(), now().unwrap()).await,
        Err(ImportError::Consensus(_))
    ));
    assert_eq!(importer.snapshot().await.unwrap().head.root, root);
    drop(batch);
    let (id, batch) = tokio::time::timeout(
        Duration::from_secs(10),
        retrieve_with_id(&mut server, &mut follower, real.clone()),
    )
    .await
    .unwrap();
    let peer = server.peer_id();
    let (availability, broker) = RetrievalAvailability::new(config.clone()).unwrap();
    // A genuine retained candidate can become occupied after an RPC was already sent.
    // The matching TCP response must grant no admission and must not stop the owner.
    let busy_candidate = availability.bind_candidate(&real).unwrap();
    let mut contended =
        detached_driver(importer.clone(), availability, broker, config.clone(), root, 0);
    contended.connected.insert(peer);
    contended.block_request = Some(BlockPending {
        peer,
        id,
        request: BlockRequest::Range { start_slot: real.slot(), count: 1 },
        started: Instant::now(),
    });
    // An in-flight foreground RPC must not start another background candidate owner.
    assert!(contended.custody_receipt(root, 1, 0).await.unwrap().is_none());
    assert!(!contended.custody.has_work());
    let original_snapshot = importer.snapshot().await.unwrap();
    let original_cursor = (contended.admitted_root, contended.admitted_slot, contended.cursor);
    contended.transport(TransportEvent::Blocks { peer, id, batch }).await.unwrap();
    assert!(contended.block_request.is_none());
    assert!(contended.active.is_none());
    assert!(contended.connected.contains(&peer));
    assert_eq!(
        (contended.admitted_root, contended.admitted_slot, contended.cursor),
        original_cursor
    );
    assert_eq!(importer.snapshot().await.unwrap(), original_snapshot);
    assert!(!contended.availability.is_halted());
    assert!(matches!(contended.availability.bind_candidate(&real), Err(RetrievalError::Capacity)));

    // The compatible TCP peer still advertises a genuine child needing retrieval, even before
    // the next foreground request is scheduled.
    contended.targets.push(Target {
        peer,
        status: Status {
            fork_digest: config.fork_digest(0).unwrap(),
            finalized_root: root,
            finalized_epoch: 0,
            head_root: real.root(),
            head_slot: real.slot(),
        },
        earliest: Some(0),
        examined_root: None,
    });
    assert!(contended.custody_receipt(root, 1, 0).await.unwrap().is_none());
    assert!(!contended.custody.has_work());
    // An eligible sequential range also prevents unsolicited far-head gossip from starting
    // a competing ancestry walk. A peer retention gap restores ancestry recovery eligibility.
    assert_eq!(contended.range_target().unwrap().peer, peer);
    contended.targets[0].earliest = Some(contended.cursor + 1);
    assert!(contended.range_target().is_none());
    contended.targets[0].earliest = Some(0);
    assert_eq!(contended.range_target().unwrap().peer, peer);
    contended.targets.clear();
    assert!(contended.range_target().is_none());

    // The same contention must preserve the complete bounded ancestry queue.
    let branch_bytes = real.ssz().len();
    contended.branch = Some(BranchRecovery {
        peer,
        target_root: real.root(),
        next_root: None,
        blocks: VecDeque::from([real.clone()]),
        bytes: branch_bytes,
        range_next_slot: None,
        range_start_slot: None,
    });
    assert!(contended.custody_receipt(root, 1, 0).await.unwrap().is_none());
    assert!(!contended.custody.has_work());
    contended.schedule().await.unwrap();
    let branch = contended.branch.as_ref().unwrap();
    assert_eq!(branch.blocks.len(), 1);
    assert_eq!(branch.bytes, branch_bytes);
    assert_eq!(branch.blocks.front().unwrap().ssz().as_ref(), real.ssz().as_ref());
    assert_eq!(branch.target_root, real.root());
    assert!(contended.active.is_none());
    assert_eq!(importer.snapshot().await.unwrap(), original_snapshot);
    // Resume the RPC route after checking the independently bounded ancestry input.
    contended.branch = None;
    drop(busy_candidate);

    let (id, batch) = tokio::time::timeout(
        Duration::from_secs(10),
        retrieve_with_id(&mut server, &mut follower, real.clone()),
    )
    .await
    .unwrap();
    contended.block_request = Some(BlockPending {
        peer,
        id,
        request: BlockRequest::Range { start_slot: real.slot(), count: 1 },
        started: Instant::now(),
    });
    contended.transport(TransportEvent::Blocks { peer, id, batch }).await.unwrap();
    let receipt = tokio::time::timeout(
        Duration::from_secs(10),
        contended.active.as_mut().unwrap().reply.as_mut(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(receipt.admitted_root, real.root());
    assert_eq!(receipt.snapshot.head.root, real.root());
    assert_eq!(receipt.snapshot.head_slot, 1);
    assert!(!receipt.execution_forkchoice_updated);
    assert_noncanonical_import_advances_cursor(
        importer.clone(),
        config.clone(),
        real.clone(),
        receipt.clone(),
        id,
    )
    .await;
    contended.imported(Ok(receipt.clone())).unwrap();
    assert_eq!(
        (contended.admitted_root, contended.admitted_slot, contended.cursor),
        (real.root(), 1, 2)
    );
    assert!(!importer.is_halted());
    assert!(!contended.availability.is_halted());
    let (late_id, late_batch) = tokio::time::timeout(
        Duration::from_secs(10),
        retrieve_with_id(&mut server, &mut follower, real.clone()),
    )
    .await
    .unwrap();
    let peer = server.peer_id();
    let request = BlockRequest::Roots(vec![real.root()]);
    let current_id = follower.request_blocks(peer, request.clone()).unwrap();
    let (availability, broker) = RetrievalAvailability::new(config.clone()).unwrap();
    let mut driver = detached_driver(
        importer.clone(),
        availability,
        broker,
        config,
        receipt.snapshot.head.root,
        receipt.snapshot.head_slot,
    );
    driver.block_request =
        Some(BlockPending { peer, id: current_id, request, started: Instant::now() });
    let before = (driver.admitted_root, driver.admitted_slot, driver.cursor);
    // Deliver a genuinely decoded old TCP response after another exact request was selected.
    driver
        .transport(TransportEvent::Blocks { peer, id: late_id, batch: late_batch.clone() })
        .await
        .unwrap();
    assert_eq!(driver.block_request.as_ref().unwrap().id, current_id);
    driver
        .transport(TransportEvent::BlocksUnavailable { peer, id: late_id, code: 3 })
        .await
        .unwrap();
    assert_eq!(driver.block_request.as_ref().unwrap().id, current_id);
    driver
        .transport(TransportEvent::BlocksUnavailable {
            peer: PeerId::random(),
            id: current_id,
            code: 3,
        })
        .await
        .unwrap();
    assert_eq!(driver.block_request.as_ref().unwrap().id, current_id);
    assert_eq!((driver.admitted_root, driver.admitted_slot, driver.cursor), before);
    assert!(driver.active.is_none());
    // A transport disconnect retires the pending association. Its late completion remains data
    // without an owner and releases its reservation rather than stopping the execution node.
    driver.transport(TransportEvent::Closed(peer)).await.unwrap();
    assert!(driver.block_request.is_none());
    driver
        .transport(TransportEvent::Blocks { peer, id: current_id, batch: late_batch })
        .await
        .unwrap();
    driver
        .transport(TransportEvent::BlocksUnavailable { peer, id: current_id, code: 3 })
        .await
        .unwrap();
    assert!(driver.block_request.is_none());
    assert!(driver.active.is_none());
    assert_eq!(driver.importer.snapshot().await.unwrap(), receipt.snapshot);
    assert_eq!((driver.admitted_root, driver.admitted_slot, driver.cursor), before);
}

// Exercise live scheduling against the coordinator's real saturated admission channel.
// No peer is opened and no native head or execution receipt is fabricated.
pub(crate) async fn assert_backpressure_preserves_live_input(
    importer: ImportCoordinator,
    config: ChainConfig,
    wire: WireBlock,
) {
    let (availability, broker) = RetrievalAvailability::new(config.clone()).unwrap();
    let guard = availability.bind_candidate(&wire).unwrap();
    let metadata = EnvelopeBlockReference::from(&wire);
    let root = wire.root();
    let original = wire.ssz().clone();
    let mut driver = detached_driver(
        importer,
        availability,
        broker,
        config,
        wire.parent_root(),
        wire.slot() - 1,
    );
    driver.active = Some(ActiveImport {
        peer: PeerId::random(),
        root,
        slot: wire.slot(),
        kind: ImportKind::Block { is_gloas: false, metadata },
        reply: Box::pin(std::future::pending()),
        _batch: RetainedBatch::Branch { _wire: wire },
        _candidate: guard,
        gossip: None,
        range_next_slot: None,
    });
    let before = (driver.admitted_root, driver.admitted_slot, driver.cursor);
    assert!(admission::observation(driver.importer.snapshot().await).unwrap().is_none());
    assert!(admission::contains_block(&driver.importer, root).await.unwrap().is_none());
    driver.publish_status().await.unwrap();
    driver.imported(Err(ImportError::Backpressure)).unwrap();
    assert!(driver.active.is_none());
    let retained = driver.deferred_import.as_ref().unwrap();
    assert_eq!(retained.active.root, root);
    let RetainedBatch::Branch { _wire: retained_wire } = &retained.active._batch else {
        panic!("original retained batch changed")
    };
    assert_eq!(retained_wire.ssz().as_ref(), original.as_ref());
    assert_eq!(before, (driver.admitted_root, driver.admitted_slot, driver.cursor));
    driver.retry_deferred_import().await.unwrap();
    assert!(driver.deferred_import.is_some());
    assert!(driver.active.is_none());
}

#[test]
fn transient_admission_does_not_hide_terminal_owner_failures() {
    assert!(matches!(
        admission::observation::<()>(Err(ImportError::Closed)),
        Err(LiveSyncError::Import(ImportError::Closed))
    ));
    assert!(matches!(
        admission::observation::<()>(Err(ImportError::Halted)),
        Err(LiveSyncError::Import(ImportError::Halted))
    ));
    assert!(matches!(
        admission::observation::<()>(Err(ImportError::EngineUncertain)),
        Err(LiveSyncError::Import(ImportError::EngineUncertain))
    ));
}

// Exercise the scheduling boundary when admission succeeds before canonical selection moves.
// Range provenance grants retrieval progress only; unrelated ancestry grants no range progress.
async fn assert_noncanonical_import_advances_cursor(
    importer: ImportCoordinator,
    config: ChainConfig,
    wire: WireBlock,
    mut receipt: ImportReceipt,
    id: BlockRequestId,
) {
    let canonical_root = wire.parent_root();
    let canonical_slot = wire.slot() - 1;
    receipt.snapshot.head.root = canonical_root;
    receipt.snapshot.head_slot = canonical_slot;
    let range_next = wire.slot() + 1;
    for (cursor, range_next_slot) in [
        (wire.slot(), Some(range_next)),
        (wire.slot() + 8, Some(range_next)),
        (wire.slot(), None),
        (wire.slot() + 8, None),
    ] {
        let (availability, broker) = RetrievalAvailability::new(config.clone()).unwrap();
        let candidate = availability.bind_candidate(&wire).unwrap();
        let mut driver = detached_driver(
            importer.clone(),
            availability,
            broker,
            config.clone(),
            canonical_root,
            canonical_slot,
        );
        let peer = PeerId::random();
        driver.cursor = cursor;
        driver.active = Some(ActiveImport {
            peer,
            root: wire.root(),
            slot: wire.slot(),
            kind: ImportKind::Block {
                is_gloas: false,
                metadata: EnvelopeBlockReference::from(&wire),
            },
            reply: Box::pin(std::future::pending()),
            _batch: RetainedBatch::Branch { _wire: wire.clone() },
            _candidate: candidate,
            gossip: None,
            range_next_slot,
        });
        driver.imported(Ok(receipt.clone())).unwrap();
        let expected_frontier = range_next_slot.unwrap_or(canonical_slot + 1);
        assert_eq!(driver.validated_range_cursor, expected_frontier);
        assert_eq!(driver.cursor, cursor.max(expected_frontier));
        assert_eq!((driver.admitted_root, driver.admitted_slot), (canonical_root, canonical_slot));
        assert!(!driver.rebase);

        // A disconnected range owner and an unavailable response both rebase without losing
        // strictly admitted range work, while unverified empty-slot hints are discarded.
        for disconnected in [true, false] {
            driver.cursor = expected_frontier + 32;
            driver.block_request = Some(BlockPending {
                peer,
                id,
                request: BlockRequest::Range { start_slot: driver.cursor, count: 1 },
                started: Instant::now(),
            });
            let event = if disconnected {
                TransportEvent::Closed(peer)
            } else {
                TransportEvent::BlocksUnavailable { peer, id, code: 3 }
            };
            driver.transport(event).await.unwrap();
            assert!(driver.block_request.is_none());
            assert!(driver.rebase);
            driver.rebase_cursor(canonical_slot).unwrap();
            assert_eq!(driver.cursor, expected_frontier);
            driver.rebase = false;
        }
    }
}

fn detached_driver(
    importer: ImportCoordinator,
    availability: RetrievalAvailability,
    broker: RetrievalBroker,
    config: ChainConfig,
    admitted_root: Root,
    admitted_slot: u64,
) -> LiveSync {
    let (_, stopped) = oneshot::channel();
    LiveSync {
        peer: None,
        importer,
        availability,
        broker,
        config,
        targets: Vec::new(),
        rebase: false,
        discovery_pending: false,
        discovery_started: Instant::now(),
        admitted_root,
        admitted_slot,
        cursor: admitted_slot + 1,
        validated_range_cursor: admitted_slot + 1,
        gossip_turn: true,
        block_request: None,
        envelope_request: None,
        sidecar_request: None,
        active: None,
        stopped,
        branch: None,
        observations: PeerObservationHandle::default(),
        connected: HashSet::new(),
        gossip_queue: VecDeque::new(),
        gossip_active: None,
        sidecar_retry: None,
        sidecar_cycle: DataPeerCycle::default(),
        serving: None,
        deferred_import: None,
        envelope_retry: None,
        broker_envelope: None,
        broker_envelope_retry: None,
        custody: CustodyTask::default(),
        history: HistoryBroker::default(),
    }
}

// Called with the genuine native Gloas graph from the custody/KZG regression. Coverage retains
// its original signed candidate guard while the event owner services the actual broker job.
pub(crate) async fn assert_background_custody_job_without_active_import(
    importer: ImportCoordinator,
    availability: RetrievalAvailability,
    broker: RetrievalBroker,
    config: ChainConfig,
    head: Root,
    slot: u64,
) -> RetrievalBroker {
    let before = importer.snapshot().await.unwrap();
    assert_eq!((before.head.root, before.head_slot), (head, slot));
    let mut driver = detached_driver(importer, availability, broker, config, head, slot);
    for mutation in 0..4 {
        let availability = driver.availability.clone();
        let importer = driver.importer.clone();
        let background =
            tokio::spawn(async move { availability.custody_coverage(importer, head, slot).await });
        let mut job = tokio::time::timeout(Duration::from_secs(10), driver.broker.receiver.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(driver.active.is_none());
        assert!(driver.availability.custody_retrieving());
        assert_eq!((job.root, job.slot), (head, slot));
        assert!(driver.availability.owns_retrieval_job(&job).unwrap());
        assert!(driver.peer.is_none());
        assert!(driver.targets.is_empty());
        let original_created = job.created;
        match mutation {
            0 => job.root = [99; 32],
            1 => job.slot += 1,
            2 => {
                job.request =
                    reth_embedded_beacon_network::availability_rpc::SidecarRequest::BlobRoots(
                        vec![reth_embedded_beacon_network::availability_rpc::BlobIdentifier {
                            block_root: head,
                            index: 0,
                        }],
                    );
            }
            _ => {}
        }
        driver.retrieve(job).unwrap();
        if mutation == 3 {
            let pending = driver.sidecar_retry.take().unwrap();
            assert_eq!((pending.job.root, pending.job.slot), (head, slot));
            assert_eq!(pending.started, original_created);
            assert_eq!(pending.job.created, original_created);
            assert!(pending.tried.is_empty());
            // Missing remote data is a known result, never a proof seal or new graph authority.
            pending.job.complete(Err(RetrievalError::Missing));
        } else {
            assert!(driver.sidecar_retry.is_none());
        }
        assert!(driver.sidecar_request.is_none());
        assert!(tokio::time::timeout(Duration::from_secs(10), background)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .is_none());
        assert!(!driver.availability.custody_retrieving());
        assert_eq!(driver.importer.snapshot().await.unwrap(), before);
    }
    driver.broker
}
