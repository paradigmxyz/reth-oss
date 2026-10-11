//! Actual genesis-owner tokens gate dormant peer identities and mode-preserving updates.

use super::*;
use crate::{
    bootstrap::genesis::tests::genuine,
    compact_persistence::PersistenceNetwork,
    import_coordinator::{checkpoint_status_for_tests, genesis_status_for_tests},
    BootstrapDecodeLimits, CheckpointBlockFormat, SszCheckpointVerifier,
};
use reth_embedded_beacon_primitives::Fork;
use std::net::Ipv4Addr;
use tempfile::TempDir;

fn inputs() -> (ChainConfig, VerifiedGenesis) {
    let (config, state, _, _) = genuine(Fork::Phase0);
    let genesis = VerifiedGenesis::authenticate(
        config.clone(),
        state.to_ssz().unwrap().into(),
        BootstrapDecodeLimits::default(),
    )
    .unwrap();
    (config, genesis)
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_genesis_owner_opens_dormant_identity_and_restart_preserves_signed_record() {
    let (config, genesis) = tokio::task::spawn_blocking(inputs).await.unwrap();
    let now = config.clock().unwrap().start_of_slot(0).unwrap();
    let status = genesis_status_for_tests(config.clone(), genesis.clone(), now).await;
    assert_eq!(status.genesis_origin(), Some(genesis.block_root()));
    let directory = TempDir::new().unwrap();
    let identity = PersistenceNetwork::from_config(config.clone()).unwrap().identity();
    let root = genesis.block_root();
    let status_again = status.clone();
    tokio::task::spawn_blocking(move || {
        let mut peer = CheckpointPeerRuntime::open_genesis(
            directory.path(),
            identity,
            config.clone(),
            genesis.clone(),
            status,
            now,
            listeners(),
        )
        .unwrap();
        assert!(!peer.started);
        assert_eq!(peer.genesis_origin, Some(root));
        assert_eq!(peer.status().unwrap().status.head_root, root);
        assert_eq!(peer.status().unwrap().status.head_slot, 0);
        assert_eq!(peer.status().unwrap().status.finalized_root, root);
        assert_eq!(peer.status().unwrap().earliest_available_slot, 0);
        let enr = peer.local_enr().unwrap();
        let id = peer.peer_id().unwrap();
        peer.apply_authenticated_status(status_again.clone()).unwrap();
        assert!(!peer.started);
        assert!(same_enr_content(&enr, &peer.local_enr().unwrap()));
        drop(peer);
        let reopened = CheckpointPeerRuntime::open_genesis(
            directory.path(),
            identity,
            config,
            genesis,
            status_again,
            now,
            listeners(),
        )
        .unwrap();
        assert_eq!(reopened.peer_id().unwrap(), id);
        assert!(same_enr_content(&enr, &reopened.local_enr().unwrap()));
        assert!(!reopened.started);
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn genuine_checkpoint_token_cannot_be_relabelled_as_genesis_or_replace_genesis_status() {
    let (config, genesis) = tokio::task::spawn_blocking(inputs).await.unwrap();
    let now = config.clock().unwrap().start_of_slot(0).unwrap();
    let actual = genesis_status_for_tests(config.clone(), genesis.clone(), now).await;
    let checkpoint = checkpoint_status_for_tests(config.clone(), genesis.clone(), now).await;
    assert_eq!(checkpoint.genesis_origin(), None);
    let directory = TempDir::new().unwrap();
    let rejected = TempDir::new().unwrap();
    let identity = PersistenceNetwork::from_config(config.clone()).unwrap().identity();
    tokio::task::spawn_blocking(move || {
        assert!(matches!(
            CheckpointPeerRuntime::open_genesis(
                rejected.path(),
                identity,
                config.clone(),
                genesis.clone(),
                checkpoint.clone(),
                now,
                listeners(),
            ),
            Err(PeerStartupError::UnverifiedGenesis)
        ));
        assert!(!rejected.path().join("peer-identity").exists());
        let mut peer = CheckpointPeerRuntime::open_genesis(
            directory.path(),
            identity,
            config,
            genesis,
            actual,
            now,
            listeners(),
        )
        .unwrap();
        let before = peer.local_enr().unwrap();
        let before_status = peer.status().unwrap();
        assert!(matches!(
            peer.apply_authenticated_status(checkpoint),
            Err(PeerStartupError::UnverifiedCheckpoint)
        ));
        assert_eq!(peer.status().unwrap(), before_status);
        assert!(same_enr_content(&before, &peer.local_enr().unwrap()));
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exact_genesis_configuration_rejects_before_durable_identity_creation() {
    let (config, genesis) = tokio::task::spawn_blocking(inputs).await.unwrap();
    let now = config.clock().unwrap().start_of_slot(0).unwrap();
    let status = genesis_status_for_tests(config.clone(), genesis.clone(), now).await;
    tokio::task::spawn_blocking(move || {
        let directory = TempDir::new().unwrap();
        let mut different = config;
        different.name.push_str("-other");
        let identity = PersistenceNetwork::from_config(different.clone()).unwrap().identity();
        assert!(matches!(
            CheckpointPeerRuntime::open_genesis(
                directory.path(),
                identity,
                different,
                genesis,
                status,
                now,
                listeners(),
            ),
            Err(PeerStartupError::UnverifiedGenesis)
        ));
        assert!(!directory.path().join("peer-identity").exists());
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn original_genesis_peer_retains_live_readiness_gate() {
    let (config, genesis) = tokio::task::spawn_blocking(inputs).await.unwrap();
    let now = config.clock().unwrap().start_of_slot(0).unwrap();
    let status = genesis_status_for_tests(config.clone(), genesis.clone(), now).await;
    let directory = TempDir::new().unwrap();
    let identity = PersistenceNetwork::from_config(config.clone()).unwrap().identity();
    let (mut peer, _directory) = tokio::task::spawn_blocking(move || {
        let peer = CheckpointPeerRuntime::open_genesis(
            directory.path(),
            identity,
            config,
            genesis,
            status,
            now,
            listeners(),
        )
        .unwrap();
        (peer, directory)
    })
    .await
    .unwrap();
    assert!(matches!(peer.start().await, Err(PeerStartupError::Bootstrap(_))));
    assert!(!peer.started);
    assert!(matches!(peer.next_event().await, Err(PeerStartupError::Dormant)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn experimental_checkpoint_constructor_keeps_sealed_config_and_genesis_binding() {
    let (config, genesis) = tokio::task::spawn_blocking(inputs).await.unwrap();
    let now = config.clock().unwrap().start_of_slot(0).unwrap();
    let checkpoint_status = checkpoint_status_for_tests(config.clone(), genesis.clone(), now).await;
    let genesis_status = genesis_status_for_tests(config.clone(), genesis.clone(), now).await;
    tokio::task::spawn_blocking(move || {
        let verifier = SszCheckpointVerifier::new(
            config.clone(),
            BootstrapDecodeLimits::default(),
            CheckpointBlockFormat::Unsigned,
        )
        .unwrap();
        let checkpoint = verifier
            .verify_checkpoint(
                &genesis.original_state_ssz(),
                &genesis.block_ssz(),
                reth_embedded_beacon_primitives::Checkpoint {
                    epoch: 0,
                    root: genesis.block_root(),
                },
                now,
                1,
            )
            .unwrap();
        let public = PeerListenConfig {
            ip: "8.8.8.8".parse().unwrap(),
            bind_ip: Some("0.0.0.0".parse().unwrap()),
            policy: EndpointPolicy::Public,
            ..listeners()
        };
        let directory = TempDir::new().unwrap();
        let canonical = ChainConfig::hoodi();
        let canonical_identity =
            PersistenceNetwork::from_config(canonical.clone()).unwrap().identity();
        assert!(matches!(
            CheckpointPeerRuntime::open_experimental_hoodi_checkpoint(
                directory.path(),
                canonical_identity,
                canonical.clone(),
                checkpoint.clone(),
                checkpoint_status.clone(),
                now,
                public,
            ),
            Err(PeerStartupError::UnverifiedCheckpoint)
        ));
        assert!(!directory.path().join("peer-identity").exists());
        assert!(matches!(
            CheckpointPeerRuntime::open_experimental_hoodi_checkpoint(
                directory.path(),
                canonical_identity,
                canonical,
                checkpoint.clone(),
                genesis_status,
                now,
                public,
            ),
            Err(PeerStartupError::ExperimentalAdmission)
        ));
        assert!(!directory.path().join("peer-identity").exists());
        let identity = PersistenceNetwork::from_config(config.clone()).unwrap().identity();
        assert!(matches!(
            CheckpointPeerRuntime::open_experimental_hoodi_checkpoint(
                directory.path(),
                identity,
                config.clone(),
                checkpoint.clone(),
                checkpoint_status.clone(),
                now,
                listeners(),
            ),
            Err(PeerStartupError::ExperimentalAdmission)
        ));
        assert!(!directory.path().join("peer-identity").exists());
        let default = CheckpointPeerRuntime::open_authenticated(
            directory.path(),
            identity,
            config,
            checkpoint,
            checkpoint_status,
            now,
            listeners(),
        )
        .unwrap();
        assert!(!default.experimental_sync);
        assert!(!default.started);
    })
    .await
    .unwrap();
}

fn verified_checkpoint(
    config: &ChainConfig,
    genesis: &VerifiedGenesis,
    now: u64,
) -> VerifiedCheckpoint {
    SszCheckpointVerifier::new(
        config.clone(),
        BootstrapDecodeLimits::default(),
        CheckpointBlockFormat::Unsigned,
    )
    .unwrap()
    .verify_checkpoint(
        &genesis.original_state_ssz(),
        &genesis.block_ssz(),
        reth_embedded_beacon_primitives::Checkpoint { epoch: 0, root: genesis.block_root() },
        now,
        1,
    )
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delayed_checkpoint_owner_preserves_sealed_clock_until_authenticated_update() {
    let (config, genesis) = tokio::task::spawn_blocking(inputs).await.unwrap();
    let clock = config.clock().unwrap();
    let sealed_at = clock.start_of_slot(0).unwrap();
    let opened_at = clock.start_of_slot(2).unwrap() + 500;
    let sealed = checkpoint_status_for_tests(config.clone(), genesis.clone(), sealed_at).await;
    let updated = checkpoint_status_for_tests(config.clone(), genesis.clone(), opened_at).await;
    assert_eq!(sealed.current_slot(), 0);
    assert_eq!(updated.current_slot(), 2);
    assert_eq!(sealed.snapshot().head, updated.snapshot().head);
    assert_eq!(sealed.snapshot().finalized_checkpoint, updated.snapshot().finalized_checkpoint);
    tokio::task::spawn_blocking(move || {
        let directory = TempDir::new().unwrap();
        let identity = PersistenceNetwork::from_config(config.clone()).unwrap().identity();
        let checkpoint = verified_checkpoint(&config, &genesis, sealed_at);
        let original = sealed.clone().into_network();
        let mut peer = CheckpointPeerRuntime::open_authenticated(
            directory.path(),
            identity,
            config,
            checkpoint,
            sealed,
            opened_at,
            listeners(),
        )
        .unwrap();
        assert!(!peer.started);
        assert_eq!(peer.current_slot, 0);
        assert_eq!(peer.trusted_status.current_slot, original.current_slot);
        assert_eq!(peer.trusted_status.finalized, original.finalized);
        assert_eq!(peer.trusted_status.canonical_roots, original.canonical_roots);
        assert_eq!(peer.status().unwrap(), original.status);
        peer.apply_authenticated_status(updated.clone()).unwrap();
        assert!(!peer.started);
        assert_eq!(peer.current_slot, 2);
        let updated = updated.into_network();
        assert_eq!(peer.trusted_status.current_slot, updated.current_slot);
        assert_eq!(peer.trusted_status.finalized, updated.finalized);
        assert_eq!(peer.trusted_status.canonical_roots, updated.canonical_roots);
        assert_eq!(peer.status().unwrap(), original.status);
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn future_checkpoint_owner_clock_rejects_before_durable_identity_creation() {
    let (config, genesis) = tokio::task::spawn_blocking(inputs).await.unwrap();
    let clock = config.clock().unwrap();
    let opened_at = clock.start_of_slot(0).unwrap();
    let sealed_at = clock.start_of_slot(2).unwrap();
    let sealed = checkpoint_status_for_tests(config.clone(), genesis.clone(), sealed_at).await;
    assert_eq!(sealed.current_slot(), 2);
    tokio::task::spawn_blocking(move || {
        let directory = TempDir::new().unwrap();
        let identity = PersistenceNetwork::from_config(config.clone()).unwrap().identity();
        let checkpoint = verified_checkpoint(&config, &genesis, opened_at);
        assert!(matches!(
            CheckpointPeerRuntime::open_authenticated(
                directory.path(),
                identity,
                config,
                checkpoint,
                sealed,
                opened_at,
                listeners(),
            ),
            Err(PeerStartupError::ClockRegression)
        ));
        assert!(!directory.path().join("peer-identity").exists());
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delayed_checkpoint_owner_cannot_relax_exact_configuration_binding() {
    let (config, genesis) = tokio::task::spawn_blocking(inputs).await.unwrap();
    let clock = config.clock().unwrap();
    let sealed_at = clock.start_of_slot(0).unwrap();
    let opened_at = clock.start_of_slot(2).unwrap();
    let sealed = checkpoint_status_for_tests(config.clone(), genesis.clone(), sealed_at).await;
    tokio::task::spawn_blocking(move || {
        let directory = TempDir::new().unwrap();
        let checkpoint = verified_checkpoint(&config, &genesis, sealed_at);
        let mut different = config;
        different.name.push_str("-other");
        let identity = PersistenceNetwork::from_config(different.clone()).unwrap().identity();
        assert!(matches!(
            CheckpointPeerRuntime::open_authenticated(
                directory.path(),
                identity,
                different,
                checkpoint,
                sealed,
                opened_at,
                listeners(),
            ),
            Err(PeerStartupError::UnverifiedCheckpoint)
        ));
        assert!(!directory.path().join("peer-identity").exists());
    })
    .await
    .unwrap();
}
