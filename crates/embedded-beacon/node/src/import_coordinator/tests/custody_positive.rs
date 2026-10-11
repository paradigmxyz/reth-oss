//! Real native KZG custody, persisted discovery identity, honest Status and cold proof recheck.

use super::*;
use crate::{
    gossip_tests::classic_to_gloas_child_with_commitments,
    live_sync::availability::RetrievalAvailability,
    peer_runtime::{CheckpointPeerRuntime, PeerListenConfig},
};
use c_kzg::{ethereum_kzg_settings, Blob, BYTES_PER_BLOB};
use reth_embedded_beacon_network::{
    availability_rpc::{ColumnIdentifier, SidecarRequest, WireSidecar},
    custody::CustodyParameters,
    EndpointPolicy, Protocol,
};
use reth_embedded_beacon_state_transition::ssz::auxiliary;
use std::net::{IpAddr, Ipv4Addr};
use tokio::task::JoinHandle;

type NativeWorker = Worker<BackendHandle, MissingAvailability, MissingAvailability>;

async fn start_native_owner(
    limits: ImportLimits,
    digest: Root,
    initialize: impl FnOnce() -> NativeWorker + Send + 'static,
) -> (ImportCoordinator, JoinHandle<()>) {
    let (sender, receiver) = mpsc::channel(limits.command_capacity);
    let bytes = Arc::new(Semaphore::new(limits.max_queued_bytes));
    let commands = Arc::new(Semaphore::new(limits.command_capacity));
    let halted = Arc::new(AtomicBool::new(false));
    let shutdown =
        Shutdown { bytes: bytes.clone(), commands: commands.clone(), halted: halted.clone() };
    let (ready, result) = oneshot::channel();
    let owner = tokio::task::spawn_blocking(move || {
        let _shutdown = shutdown;
        let mut worker = initialize();
        ready.send(()).unwrap();
        worker.run(receiver);
    });
    result.await.unwrap();
    (
        ImportCoordinator {
            sender,
            bytes,
            commands,
            halted,
            limits,
            config_digest: digest,
            queue_high_water: Default::default(),
        },
        owner,
    )
}

fn listeners() -> PeerListenConfig {
    PeerListenConfig {
        ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
        bind_ip: None,
        discovery_port: 19101,
        quic_port: 19102,
        tcp_port: 19103,
        policy: EndpointPolicy::LoopbackOnly,
    }
}

fn cgc(peer: &CheckpointPeerRuntime) -> u64 {
    peer.local_enr().unwrap().get_decodable::<u64>("cgc").unwrap().unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_native_four_columns_status_reopen_and_epoch_pruning_use_actual_owner_evidence() {
    let directory = TempDir::new().unwrap();
    let (store, keys, parameters, _) = fulu_gloas_boundary();
    let (mut initial, backend) = setup(&directory, &store, &parameters);
    let config = initial.config.clone();
    let digest = checked_config_digest(&config).unwrap();
    let anchor = initial.anchor.clone();
    let origin = anchor.verified.checkpoint().root;
    // Cold reopen uses a fresh bound adapter. Execution receipts remain local to each owner;
    // neither adapter cloning nor a controlled backend can substitute an acknowledgement.
    let engine = RuntimeEngineAdapter::new(
        ExecutionProxy::new(BackendHandle(backend.clone()), Duration::from_millis(50)).unwrap(),
        config.clone(),
    )
    .unwrap();
    let limits = initial.limits;
    let runtime = initial.runtime.clone();
    let OwnerClock::Controlled(clock) = &initial.clock else { unreachable!() };
    let clock = clock.clone();
    drop(initial.repository);
    let network = PersistenceNetwork::from_config(config.clone()).unwrap();
    let identity = network.identity();
    let shared = SharedDataStore::open(directory.path(), identity, 0).unwrap();
    initial.repository =
        AdmittedStateRepository::open(shared.clone(), network, CompactStateLimits::default())
            .unwrap();
    let mut peer = CheckpointPeerRuntime::open(
        directory.path(),
        identity,
        config.clone(),
        anchor.verified.clone(),
        0,
        listeners(),
    )
    .unwrap();
    let peer_id = peer.peer_id().unwrap();
    let node_id = peer.local_enr().unwrap().node_id().raw();
    let mut node_id_le = node_id;
    node_id_le.reverse();
    let columns = CustodyParameters::default().get_custody_groups(node_id_le, 4).unwrap();
    assert_eq!(columns.len(), 4);
    let (availability, broker) =
        RetrievalAvailability::with_shared_store(config.clone(), shared.clone()).unwrap();
    availability.bind_custody_peer_for_tests(&peer).unwrap();
    let settings = ethereum_kzg_settings(0);
    let blob = Blob::new([0; BYTES_PER_BLOB]);
    let (cells, proofs) = settings.compute_cells_and_kzg_proofs(&blob).unwrap();
    let publisher = availability.clone();
    let commitment = settings.blob_to_kzg_commitment(&blob).unwrap().to_bytes().into_inner();
    let block_slot = config.slots_per_epoch;
    let now = config.clock().unwrap().start_of_slot(block_slot).unwrap();
    let owner_clock = clock.clone();
    let (importer, owner) = start_native_owner(limits, digest, move || {
        let mut worker = Worker::new(initial, Ok(0)).unwrap();
        worker.column_retention = Some(Arc::new(publisher));
        assert_eq!(worker.graph.snapshot().unwrap().head.root, origin);
        // Admit the actual signed upgrade block. Its Fulu parent requires no invented Gloas
        // envelope, and the commitment enters complete native processing before BLS signing.
        let child =
            classic_to_gloas_child_with_commitments(&store, &keys, &parameters, vec![commitment]);
        let wire = WireBlock::from_ssz(child.to_ssz().unwrap(), &worker.config).unwrap();
        owner_clock.store(now, Ordering::Release);
        worker.advance(now).unwrap();
        worker.import(Input::Block(wire), 0).unwrap();
        worker
    })
    .await;
    let snapshot = importer.snapshot().await.unwrap();
    assert_eq!(snapshot.head_slot, block_slot);
    assert_ne!(snapshot.head.root, origin);
    let head = snapshot.head.root;
    let originals = columns
        .iter()
        .map(|index| {
            let index_usize = *index as usize;
            let sidecar = auxiliary::gloas::DataColumnSidecar {
                index: *index,
                column: ProgressiveList::from_vec(vec![Vector::from_vec(
                    cells[index_usize].to_bytes().to_vec(),
                )]),
                kzg_proofs: ProgressiveList::from_vec(vec![proofs[index_usize]
                    .to_bytes()
                    .into_inner()]),
                slot: block_slot,
                beacon_block_root: head,
            };
            WireSidecar::from_ssz(
                sidecar.to_ssz(Preset::Minimal).unwrap(),
                config.fork_digest(block_slot / config.slots_per_epoch).unwrap(),
                Protocol::DataColumnsByRootV1,
                &config,
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let request = SidecarRequest::ColumnRoots(vec![ColumnIdentifier {
        block_root: head,
        columns: columns.clone(),
    }]);
    let Some(ServingBatch::Sidecars(empty)) =
        availability.serve_sidecars(importer.clone(), request.clone()).await.unwrap()
    else {
        panic!("actual signed native context is available before proof inputs")
    };
    assert!(empty.is_empty());
    let startup =
        availability.custody_startup(head, block_slot, block_slot).await.unwrap().unwrap();
    assert_eq!(startup.earliest(), block_slot + 1);
    let mut status = importer.authenticated_status().await.unwrap();
    status.apply_custody_history(Some(startup)).unwrap();
    peer.apply_authenticated_status(status).unwrap();
    peer.apply_custody(Some(startup)).unwrap();
    assert_eq!(cgc(&peer), 4);
    let four_seq = peer.local_enr().unwrap().seq();
    let broker = crate::live_sync::tests::assert_background_custody_job_without_active_import(
        importer.clone(),
        availability.clone(),
        broker,
        config.clone(),
        head,
        block_slot,
    )
    .await;
    let pins = shared.owners();
    let mut invalid =
        auxiliary::gloas::DataColumnSidecar::from_ssz(originals[0].ssz(), Preset::Minimal).unwrap();
    invalid.kzg_proofs.values[0][0] ^= 1;
    let invalid = WireSidecar::from_ssz(
        invalid.to_ssz(Preset::Minimal).unwrap(),
        config.fork_digest(block_slot / config.slots_per_epoch).unwrap(),
        Protocol::DataColumnsByRootV1,
        &config,
    )
    .unwrap();
    assert!(matches!(
        importer.retain_anchor_columns(head, vec![invalid]).await,
        Err(ImportError::GossipReject("invalid native anchor column proof"))
    ));
    assert_eq!(shared.owners(), pins);
    importer.retain_anchor_columns(head, originals.clone()).await.unwrap();
    let covered =
        availability.custody_coverage(importer.clone(), head, block_slot).await.unwrap().unwrap();
    assert_eq!(covered.earliest(), 1);
    let covered =
        availability.custody_startup(head, block_slot, block_slot).await.unwrap().unwrap();
    assert_eq!(covered.earliest(), 1);
    let mut status = importer.authenticated_status().await.unwrap();
    status.apply_custody_history(Some(covered)).unwrap();
    peer.apply_authenticated_status(status).unwrap();
    peer.apply_custody(Some(covered)).unwrap();
    assert_eq!(peer.status().unwrap().earliest_available_slot, 1);
    assert_eq!(cgc(&peer), 4);
    assert_eq!(peer.local_enr().unwrap().seq(), four_seq);
    let Some(ServingBatch::Sidecars(served)) =
        availability.serve_sidecars(importer.clone(), request.clone()).await.unwrap()
    else {
        panic!("fresh native original and real KZG proofs serve the four groups")
    };
    assert_eq!(served, originals);
    assert_eq!(importer.snapshot().await.unwrap(), snapshot);
    drop(importer);
    owner.await.unwrap();
    drop(availability);
    drop(broker);
    drop(peer);
    drop(shared);

    // Reopen the physical writer and discovery identity. Inventory bytes confer no receipt;
    // selected native contexts and every retained mathematical proof are checked afresh.
    let network = PersistenceNetwork::from_config(config.clone()).unwrap();
    let shared = SharedDataStore::open(directory.path(), identity, 0).unwrap();
    let repository =
        AdmittedStateRepository::open(shared.clone(), network, CompactStateLimits::default())
            .unwrap();
    let restored = WorkerSetup {
        config: config.clone(),
        anchor: anchor.clone(),
        repository,
        engine,
        classic_availability: MissingAvailability,
        gloas_availability: MissingAvailability,
        limits,
        runtime,
        clock: OwnerClock::Controlled(clock.clone()),
    };
    let (availability, broker) =
        RetrievalAvailability::with_shared_store(config.clone(), shared.clone()).unwrap();
    let mut peer = CheckpointPeerRuntime::open(
        directory.path(),
        identity,
        config.clone(),
        anchor.verified.clone(),
        now,
        listeners(),
    )
    .unwrap();
    assert_eq!(peer.peer_id().unwrap(), peer_id);
    assert_eq!(peer.local_enr().unwrap().node_id().raw(), node_id);
    availability.bind_custody_peer_for_tests(&peer).unwrap();
    let publisher = availability.clone();
    let (importer, owner) = start_native_owner(limits, digest, move || {
        let plan = recovery::ReplayPlan::prepare(
            &restored.repository,
            &restored.config,
            origin,
            RecoveryLimits::default(),
        )
        .unwrap();
        let mut worker = Worker::initialize(restored, Ok(now), true, None).unwrap();
        let transcript = plan.transcript.as_ref().unwrap();
        for sequence in &transcript.sequences {
            worker.replay_original_event(*sequence).unwrap();
        }
        plan.check(&worker.graph).unwrap();
        worker.replaying = false;
        let mut candidate = worker.graph.clone();
        worker.publish(&mut candidate).unwrap();
        worker.column_retention = Some(Arc::new(publisher));
        worker
    })
    .await;
    assert_eq!(importer.snapshot().await.unwrap(), snapshot);
    let cold = availability.custody_startup(head, block_slot, block_slot).await.unwrap().unwrap();
    assert_eq!(cold.earliest(), block_slot + 1);
    let covered =
        availability.custody_coverage(importer.clone(), head, block_slot).await.unwrap().unwrap();
    assert_eq!(covered.earliest(), 1);
    let mut status = importer.authenticated_status().await.unwrap();
    status.apply_custody_history(Some(covered)).unwrap();
    peer.apply_authenticated_status(status).unwrap();
    peer.apply_custody(Some(covered)).unwrap();
    assert_eq!(peer.status().unwrap().earliest_available_slot, 1);
    assert_eq!(cgc(&peer), 4);
    let Some(ServingBatch::Sidecars(served)) =
        availability.serve_sidecars(importer.clone(), request.clone()).await.unwrap()
    else {
        panic!("reopened native context freshly verifies retained cells")
    };
    assert_eq!(served, originals);

    // Advance the real owner clock within its native 8192-slot event bound. Prune only the
    // sidecar domain at that sealed clock, then prove cached floor one cannot survive rollover.
    let target = (4096 + 2) * config.slots_per_epoch;
    let mut slot = block_slot;
    while slot < target {
        slot = (slot + limits.max_tick_slots).min(target);
        clock.store(config.clock().unwrap().start_of_slot(slot).unwrap(), Ordering::Release);
        assert_eq!(importer.authenticated_status().await.unwrap().current_slot(), slot);
    }
    let mut status = importer.authenticated_status().await.unwrap();
    let pruning_clock = status.clone();
    let archive = availability.clone();
    tokio::task::spawn_blocking(move || archive.prune_custody_clock_for_tests(&pruning_clock))
        .await
        .unwrap()
        .unwrap();
    let floor = 2 * config.slots_per_epoch;
    let receipt = availability.custody_startup(head, block_slot, target).await.unwrap().unwrap();
    assert_eq!(receipt.earliest(), floor);
    status.apply_custody_history(Some(receipt)).unwrap();
    peer.apply_authenticated_status(status).unwrap();
    peer.apply_custody(Some(receipt)).unwrap();
    assert_eq!(peer.status().unwrap().earliest_available_slot, floor);
    assert_eq!(cgc(&peer), 4);
    let Some(ServingBatch::Sidecars(pruned)) =
        availability.serve_sidecars(importer.clone(), request).await.unwrap()
    else {
        panic!("native signed context remains but its expired columns were pruned")
    };
    assert!(pruned.is_empty());
    assert!(backend.payload_calls.lock().unwrap().is_empty());
    assert!(backend.forkchoice_calls.lock().unwrap().is_empty());
    drop(importer);
    owner.await.unwrap();
    drop(availability);
    drop(broker);
}
