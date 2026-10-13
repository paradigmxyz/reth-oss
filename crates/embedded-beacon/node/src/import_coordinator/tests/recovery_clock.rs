//! Replay discovery clocks preserve genuine checkpoint authority and durable peer identity.

use super::*;
use crate::peer_runtime::{CheckpointPeerRuntime, PeerListenConfig};
use reth_embedded_beacon_network::EndpointPolicy;
use std::net::{IpAddr, Ipv4Addr};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkpoint_recovery_opens_newer_peer_identity_without_advancing_anchor_authority() {
    let directory = TempDir::new().unwrap();
    let (store, _, parameters) = store_and_keys(Fork::Phase0);
    let (initial, _) = setup(&directory, &store, &parameters);
    let config = initial.config.clone();
    let checkpoint = initial.anchor.verified.clone();
    let clock = config.clock().unwrap();
    let anchor_time = clock.start_of_slot(0).unwrap();
    let signed_time = clock.start_of_slot(2).unwrap();
    let recovery_time = clock.start_of_slot(3).unwrap();
    let identity = PersistenceNetwork::from_config(config.clone()).unwrap().identity();
    tokio::task::spawn_blocking(move || {
        let mut worker = Worker::new(initial, Ok(anchor_time)).unwrap();
        worker.advance(signed_time).unwrap();
        let prior = worker.authenticated_status().unwrap();
        let peer = CheckpointPeerRuntime::open_authenticated(
            directory.path(),
            identity,
            config.clone(),
            checkpoint.clone(),
            prior,
            signed_time,
            listeners(),
        )
        .unwrap();
        let peer_id = peer.peer_id().unwrap();
        drop(peer);
        drop(worker);

        let (restored, _) = setup(&directory, &store, &parameters);
        let OwnerClock::Controlled(clock) = &restored.clock else { unreachable!() };
        clock.store(recovery_time, Ordering::Release);
        let restored = Worker::initialize_mode(
            restored,
            Ok(recovery_time),
            true,
            StartupExecutionMode::Strict,
        )
        .unwrap();
        let plan = recovery::ReplayPlan::prepare(
            &restored.repository,
            &config,
            checkpoint.checkpoint().root,
            recovery::RecoveryLimits::default(),
        )
        .unwrap();
        assert!(plan.transcript.is_some());
        let anchor = restored.graph.snapshot().unwrap();
        let native =
            restored.graph.authenticated_status(&config, restored.earliest_available_slot).unwrap();
        let retrieval = restored.authenticated_status().unwrap();
        assert_eq!(restored.graph.current_slot(), 0);
        assert_eq!(native.current_slot(), 0);
        assert_eq!(retrieval.current_slot(), 3);
        assert_eq!(retrieval.config_digest(), native.config_digest());
        assert_eq!(retrieval.genesis_origin(), None);
        assert_eq!(retrieval.snapshot(), anchor);
        let network = retrieval.clone().into_network();
        let native_network = native.into_network();
        assert_eq!(network.finalized, native_network.finalized);
        assert_eq!(network.canonical_roots, native_network.canonical_roots);
        assert_eq!(network.status.status.head_root, native_network.status.status.head_root);
        assert_eq!(network.status.status.head_slot, native_network.status.status.head_slot);
        assert_eq!(
            network.status.earliest_available_slot,
            native_network.status.earliest_available_slot
        );

        let peer = CheckpointPeerRuntime::open_authenticated(
            directory.path(),
            identity,
            config,
            checkpoint,
            retrieval,
            recovery_time,
            listeners(),
        )
        .unwrap();
        assert_eq!(peer.peer_id().unwrap(), peer_id);
        assert_eq!(peer.status().unwrap(), network.status);
        assert_eq!(restored.graph.snapshot().unwrap(), anchor);
        assert_eq!(restored.import_metrics.replayed_events_total, 0);
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replay_retrieval_clock_rejects_regression_without_changing_graph_authority() {
    let directory = TempDir::new().unwrap();
    let (store, _, parameters) = store_and_keys(Fork::Phase0);
    let (setup, _) = setup(&directory, &store, &parameters);
    let now = setup.config.clock().unwrap().start_of_slot(2).unwrap();
    let earlier = setup.config.clock().unwrap().start_of_slot(1).unwrap();
    let OwnerClock::Controlled(clock) = &setup.clock else { unreachable!() };
    clock.store(earlier, Ordering::Release);
    tokio::task::spawn_blocking(move || {
        let mut worker = Worker::new(setup, Ok(now)).unwrap();
        let actual = worker.graph.snapshot().unwrap();
        assert_eq!(worker.graph.current_slot(), 2);
        worker.replaying = true;
        assert!(matches!(worker.authenticated_status(), Err(ImportError::Clock)));
        assert_eq!(worker.graph.current_slot(), 2);
        assert_eq!(worker.graph.snapshot().unwrap(), actual);
    })
    .await
    .unwrap();
}

fn listeners() -> PeerListenConfig {
    PeerListenConfig {
        ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
        bind_ip: None,
        discovery_port: 12000,
        tcp_port: 12001,
        quic_port: 12002,
        policy: EndpointPolicy::LoopbackOnly,
    }
}
