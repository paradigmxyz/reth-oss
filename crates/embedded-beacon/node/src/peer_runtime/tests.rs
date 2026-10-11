use super::*;
use crate::{BootstrapDecodeLimits, CheckpointBlockFormat, SszCheckpointVerifier};
use reth_embedded_beacon_config::{ForkActivation, ForkSchedule};
use reth_embedded_beacon_primitives::Fork;
use reth_embedded_beacon_shared_data::journal::inject_post_rename_failure;
use reth_embedded_beacon_state_transition::ssz::Preset;
use std::net::{Ipv4Addr, TcpListener, UdpSocket};
use tempfile::TempDir;

fn checkpoint() -> (ChainConfig, VerifiedCheckpoint, StoreIdentity, u64) {
    let (state, block, mut config, trusted) = crate::bootstrap::tests::linked_fixture();
    config.forks = ForkSchedule::new(vec![
        ForkActivation { fork: Fork::Phase0, epoch: 0, version: [0; 4] },
        ForkActivation { fork: Fork::Altair, epoch: 10, version: [1, 0, 0, 0] },
    ])
    .unwrap();
    let verifier = SszCheckpointVerifier::new(
        config.clone(),
        BootstrapDecodeLimits::default(),
        CheckpointBlockFormat::Unsigned,
    )
    .unwrap();
    let now = config.clock().unwrap().start_of_slot(72).unwrap();
    let checkpoint = verifier
        .verify_checkpoint(
            &state.to_ssz(Preset::Minimal).unwrap(),
            &block.to_ssz(Preset::Minimal).unwrap(),
            trusted,
            now,
            4,
        )
        .unwrap();
    let identity =
        StoreIdentity { genesis_root: config.genesis_validators_root, config_digest: [7; 32] };
    (config, checkpoint, identity, now)
}

fn sockets() -> (PeerListenConfig, [UdpSocket; 2], TcpListener) {
    let discovery = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let quic = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let tcp = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let settings = PeerListenConfig {
        ip: Ipv4Addr::LOCALHOST.into(),
        bind_ip: None,
        discovery_port: discovery.local_addr().unwrap().port(),
        quic_port: quic.local_addr().unwrap().port(),
        tcp_port: tcp.local_addr().unwrap().port(),
        policy: EndpointPolicy::LoopbackOnly,
    };
    (settings, [discovery, quic], tcp)
}

fn open(
    path: &Path,
    config: ChainConfig,
    checkpoint: VerifiedCheckpoint,
    identity: StoreIdentity,
    now: u64,
    settings: PeerListenConfig,
) -> CheckpointPeerRuntime {
    CheckpointPeerRuntime::open(path, identity, config, checkpoint, now, settings).unwrap()
}

fn snapshot_file(directory: &TempDir, generation: u64) -> PathBuf {
    directory.path().join("peer-identity").join(format!("identity-{generation:020}.commit"))
}

#[tokio::test]
async fn actual_identity_restarts_keep_peer_id_and_sequence_without_opening_sockets() {
    let directory = TempDir::new().unwrap();
    let (config, checkpoint, identity, now) = checkpoint();
    let (settings, held_udp, held_tcp) = sockets();
    let mut runtime =
        open(directory.path(), config.clone(), checkpoint.clone(), identity, now, settings);
    let id = runtime.peer_id().unwrap();
    let enr = runtime.local_enr().unwrap();
    assert_eq!(enr.seq(), 1);
    assert_eq!(runtime.status().unwrap().status.head_root, checkpoint.checkpoint().root);
    assert_eq!(runtime.status().unwrap().status.finalized_epoch, checkpoint.checkpoint().epoch);
    assert_eq!(runtime.status().unwrap().earliest_available_slot, checkpoint.slot());
    assert!(matches!(runtime.next_event().await, Err(PeerStartupError::Dormant)));
    assert!(matches!(runtime.request_discovery(), Err(PeerStartupError::Dormant)));
    assert!(matches!(
        runtime.request_blocks(id, BlockRequest::Range { start_slot: 72, count: 1 }),
        Err(PeerStartupError::Dormant)
    ));
    assert!(matches!(
        runtime.request_envelopes(
            id,
            EnvelopeRequest::Roots(vec![checkpoint.checkpoint().root]),
            Vec::new()
        ),
        Err(PeerStartupError::Dormant)
    ));
    assert!(matches!(runtime.ensure_started(), Err(PeerStartupError::Dormant)));
    assert!(matches!(
        runtime.start().await,
        Err(PeerStartupError::Bootstrap(BootstrapError::IncompleteImplementation))
    ));
    drop(held_udp);
    drop(held_tcp);
    // Constructor and blocked start cannot have consumed any of the actual configured ports.
    let discovery = UdpSocket::bind((settings.ip, settings.discovery_port)).unwrap();
    let quic = UdpSocket::bind((settings.ip, settings.quic_port)).unwrap();
    let tcp = TcpListener::bind((settings.ip, settings.tcp_port)).unwrap();
    drop(runtime);
    for _ in 0..3 {
        let reopened =
            open(directory.path(), config.clone(), checkpoint.clone(), identity, now, settings);
        assert_eq!(reopened.peer_id().unwrap(), id);
        assert_eq!(reopened.local_enr().unwrap(), enr);
        drop(reopened);
    }
    drop((discovery, quic, tcp));
}

#[tokio::test]
async fn changed_fork_is_persisted_before_published_enr_and_restarts_are_monotonic() {
    let directory = TempDir::new().unwrap();
    let (config, checkpoint, identity, now) = checkpoint();
    let (settings, _udp, _tcp) = sockets();
    let mut runtime =
        open(directory.path(), config.clone(), checkpoint.clone(), identity, now, settings);
    let id = runtime.peer_id().unwrap();
    let old = runtime.local_enr().unwrap();
    let later = config.clock().unwrap().start_of_slot(80).unwrap();
    runtime.advance_clock(later).unwrap();
    let current = runtime.local_enr().unwrap();
    assert_eq!(current.seq(), old.seq() + 1);
    assert_ne!(current, old);
    assert_eq!(runtime.status().unwrap().status.head_slot, checkpoint.slot());
    assert_eq!(runtime.status().unwrap().status.fork_digest, config.fork_digest(10).unwrap());
    let digest = checked_config_digest(&config).unwrap();
    drop(runtime);
    let store = IdentityStore::open(directory.path(), identity, config.clone(), digest).unwrap();
    assert_eq!(store.enr.unwrap(), current);
    drop(store._lock);
    let reopened =
        open(directory.path(), config.clone(), checkpoint.clone(), identity, later, settings);
    assert_eq!(reopened.peer_id().unwrap(), id);
    assert_eq!(reopened.local_enr().unwrap(), current);
    drop(reopened);
    assert!(matches!(
        CheckpointPeerRuntime::open(directory.path(), identity, config, checkpoint, now, settings),
        Err(PeerStartupError::ClockRegression)
    ));
}

#[tokio::test]
async fn recorded_clock_progress_without_enr_change_still_cannot_regress_on_restart() {
    let directory = TempDir::new().unwrap();
    let (config, checkpoint, identity, now) = checkpoint();
    let (settings, _udp, _tcp) = sockets();
    let mut runtime =
        open(directory.path(), config.clone(), checkpoint.clone(), identity, now, settings);
    let enr = runtime.local_enr().unwrap();
    let next = config.clock().unwrap().start_of_slot(73).unwrap();
    runtime.advance_clock(next).unwrap();
    assert_eq!(runtime.local_enr().unwrap(), enr);
    assert!(matches!(runtime.advance_clock(now), Err(PeerStartupError::ClockRegression)));
    drop(runtime);
    assert!(matches!(
        CheckpointPeerRuntime::open(directory.path(), identity, config, checkpoint, now, settings),
        Err(PeerStartupError::ClockRegression)
    ));
}

#[tokio::test]
async fn post_rename_callback_failure_poison_closes_reads_and_reopen_resolves_new_sequence() {
    let directory = TempDir::new().unwrap();
    let (config, checkpoint, identity, now) = checkpoint();
    let (settings, _udp, _tcp) = sockets();
    let mut runtime =
        open(directory.path(), config.clone(), checkpoint.clone(), identity, now, settings);
    let peer = runtime.peer_id().unwrap();
    let old = runtime.local_enr().unwrap();
    let later = config.clock().unwrap().start_of_slot(80).unwrap();
    inject_post_rename_failure(snapshot_file(&directory, 2));
    assert!(matches!(
        runtime.advance_clock(later),
        Err(PeerStartupError::Store(StoreError::PublicationUncertain(_)))
    ));
    assert!(matches!(runtime.peer_id(), Err(PeerStartupError::Store(StoreError::JournalPoisoned))));
    assert!(matches!(
        runtime.request_blocks(peer, BlockRequest::Range { start_slot: 80, count: 1 }),
        Err(PeerStartupError::Store(StoreError::JournalPoisoned))
    ));
    assert!(matches!(
        runtime.request_envelopes(
            peer,
            EnvelopeRequest::Roots(vec![checkpoint.checkpoint().root]),
            Vec::new()
        ),
        Err(PeerStartupError::Store(StoreError::JournalPoisoned))
    ));
    assert!(matches!(
        runtime.ensure_started(),
        Err(PeerStartupError::Store(StoreError::JournalPoisoned))
    ));
    assert!(matches!(
        runtime.local_enr(),
        Err(PeerStartupError::Store(StoreError::JournalPoisoned))
    ));
    assert!(matches!(runtime.status(), Err(PeerStartupError::Store(StoreError::JournalPoisoned))));
    assert!(matches!(
        runtime.start().await,
        Err(PeerStartupError::Store(StoreError::JournalPoisoned))
    ));
    assert!(matches!(
        runtime.advance_clock(now),
        Err(PeerStartupError::Store(StoreError::JournalPoisoned))
    ));
    drop(runtime);
    let reopened = open(directory.path(), config, checkpoint, identity, later, settings);
    assert_eq!(reopened.local_enr().unwrap().seq(), old.seq() + 1);
}

#[tokio::test]
async fn uncertain_first_identity_write_recovers_the_same_published_identity() {
    let directory = TempDir::new().unwrap();
    let (config, checkpoint, identity, now) = checkpoint();
    let (settings, _udp, _tcp) = sockets();
    inject_post_rename_failure(snapshot_file(&directory, 1));
    assert!(matches!(
        CheckpointPeerRuntime::open(
            directory.path(),
            identity,
            config.clone(),
            checkpoint.clone(),
            now,
            settings
        ),
        Err(PeerStartupError::Store(StoreError::PublicationUncertain(_)))
    ));
    let digest = checked_config_digest(&config).unwrap();
    let store = IdentityStore::open(directory.path(), identity, config.clone(), digest).unwrap();
    let published = store.enr.clone().unwrap();
    drop(store);
    let reopened = open(directory.path(), config, checkpoint, identity, now, settings);
    assert_eq!(reopened.local_enr().unwrap(), published);
}

#[tokio::test]
async fn newest_corrupt_snapshot_is_rejected_without_falling_back_to_an_older_identity() {
    let directory = TempDir::new().unwrap();
    let (config, checkpoint, identity, now) = checkpoint();
    let (settings, _udp, _tcp) = sockets();
    let mut runtime =
        open(directory.path(), config.clone(), checkpoint.clone(), identity, now, settings);
    runtime.advance_clock(config.clock().unwrap().start_of_slot(73).unwrap()).unwrap();
    drop(runtime);
    let path = snapshot_file(&directory, 2);
    let mut bytes = fs::read(&path).unwrap();
    bytes[125] ^= 1;
    std::fs::write(&path, &bytes).unwrap();
    assert!(matches!(
        CheckpointPeerRuntime::open(directory.path(), identity, config, checkpoint, now, settings),
        Err(PeerStartupError::CorruptIdentity)
    ));
    assert!(snapshot_file(&directory, 1).exists());
}

#[tokio::test]
async fn self_consistent_file_checksum_cannot_hide_key_and_enr_identity_mismatch() {
    let directory = TempDir::new().unwrap();
    let (config, checkpoint, identity, now) = checkpoint();
    let (settings, _udp, _tcp) = sockets();
    let runtime =
        open(directory.path(), config.clone(), checkpoint.clone(), identity, now, settings);
    drop(runtime);
    let path = snapshot_file(&directory, 1);
    let mut bytes = Zeroizing::new(fs::read(&path).unwrap());
    bytes[125..157].fill(0);
    bytes[156] = 1;
    let end = bytes.len() - 32;
    let checksum = sha256(&bytes[..end]);
    bytes[end..].copy_from_slice(&checksum);
    std::fs::write(&path, &*bytes).unwrap();
    assert!(matches!(
        CheckpointPeerRuntime::open(directory.path(), identity, config, checkpoint, now, settings),
        Err(PeerStartupError::CorruptIdentity)
    ));
}

#[tokio::test]
async fn full_profile_and_consensus_configuration_are_both_bound_before_recovery() {
    let directory = TempDir::new().unwrap();
    let (config, checkpoint, identity, now) = checkpoint();
    let (settings, _udp, _tcp) = sockets();
    drop(open(directory.path(), config.clone(), checkpoint.clone(), identity, now, settings));
    let mut different = identity;
    different.config_digest[0] ^= 1;
    assert!(matches!(
        CheckpointPeerRuntime::open(
            directory.path(),
            different,
            config.clone(),
            checkpoint.clone(),
            now,
            settings
        ),
        Err(PeerStartupError::Store(StoreError::IdentityMismatch))
    ));
    let mut changed = config.clone();
    changed.seconds_per_slot *= 2;
    assert!(matches!(
        CheckpointPeerRuntime::open(
            directory.path(),
            identity,
            changed,
            checkpoint.clone(),
            now,
            settings
        ),
        Err(PeerStartupError::UnverifiedCheckpoint)
    ));
    let mut changed = config;
    changed.preset = reth_embedded_beacon_config::Preset::Mainnet;
    assert!(matches!(
        CheckpointPeerRuntime::open(directory.path(), identity, changed, checkpoint, now, settings),
        Err(PeerStartupError::UnverifiedCheckpoint)
    ));
}

#[tokio::test]
async fn generic_verifier_evidence_does_not_authorize_peer_startup() {
    let directory = TempDir::new().unwrap();
    let (state, block, config, trusted) = crate::bootstrap::tests::linked_fixture();
    let verifier = SszCheckpointVerifier::new(
        config.clone(),
        BootstrapDecodeLimits::default(),
        CheckpointBlockFormat::Unsigned,
    )
    .unwrap();
    let checkpoint = VerifiedCheckpoint::verify(
        &verifier,
        &state.to_ssz(Preset::Minimal).unwrap(),
        &block.to_ssz(Preset::Minimal).unwrap(),
        trusted,
        crate::BootstrapSettings {
            network: config.genesis_validators_root,
            slots_per_epoch: config.slots_per_epoch,
            current_epoch: 9,
            max_age_epochs: 4,
        },
    )
    .unwrap();
    let now = config.clock().unwrap().start_of_slot(72).unwrap();
    let identity =
        StoreIdentity { genesis_root: config.genesis_validators_root, config_digest: [7; 32] };
    let (settings, _udp, _tcp) = sockets();
    assert!(matches!(
        CheckpointPeerRuntime::open(directory.path(), identity, config, checkpoint, now, settings),
        Err(PeerStartupError::UnverifiedCheckpoint)
    ));
    assert!(!directory.path().join("peer-identity").exists());
}

#[tokio::test]
async fn exclusive_writer_and_changed_listener_sequence_are_enforced() {
    let directory = TempDir::new().unwrap();
    let (config, checkpoint, identity, now) = checkpoint();
    let (settings, _udp, _tcp) = sockets();
    let runtime =
        open(directory.path(), config.clone(), checkpoint.clone(), identity, now, settings);
    let id = runtime.peer_id().unwrap();
    let prior = runtime.local_enr().unwrap();
    assert!(matches!(
        CheckpointPeerRuntime::open(
            directory.path(),
            identity,
            config.clone(),
            checkpoint.clone(),
            now,
            settings
        ),
        Err(PeerStartupError::Store(StoreError::AlreadyOpen(_)))
    ));
    drop(runtime);
    let tcp = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let changed = PeerListenConfig { tcp_port: tcp.local_addr().unwrap().port(), ..settings };
    let reopened = open(directory.path(), config, checkpoint, identity, now, changed);
    assert_eq!(reopened.peer_id().unwrap(), id);
    assert_eq!(reopened.local_enr().unwrap().seq(), prior.seq() + 1);
    assert_eq!(reopened.local_enr().unwrap().tcp4(), Some(changed.tcp_port));
}

#[tokio::test]
async fn identity_file_size_and_explicit_listener_parameters_fail_closed() {
    let directory = TempDir::new().unwrap();
    let (config, checkpoint, identity, now) = checkpoint();
    let (settings, _udp, _tcp) = sockets();
    for bad in [
        PeerListenConfig { quic_port: settings.discovery_port, ..settings },
        PeerListenConfig { tcp_port: 0, ..settings },
        PeerListenConfig { policy: EndpointPolicy::Public, ..settings },
    ] {
        assert!(matches!(
            CheckpointPeerRuntime::open(
                directory.path(),
                identity,
                config.clone(),
                checkpoint.clone(),
                now,
                bad
            ),
            Err(PeerStartupError::Configuration)
        ));
        assert!(!directory.path().join("peer-identity").exists());
    }
    drop(open(directory.path(), config.clone(), checkpoint.clone(), identity, now, settings));
    let path = snapshot_file(&directory, 1);
    let file = OpenOptions::new().write(true).open(path).unwrap();
    file.set_len(MAX_IDENTITY_BYTES as u64 + 1).unwrap();
    drop(file);
    assert!(matches!(
        CheckpointPeerRuntime::open(directory.path(), identity, config, checkpoint, now, settings),
        Err(PeerStartupError::IdentityTooLarge)
    ));
}

#[tokio::test]
async fn identity_recovery_bounds_directory_entries_and_discards_only_unpublished_pending_data() {
    let directory = TempDir::new().unwrap();
    let (config, checkpoint, identity, now) = checkpoint();
    let (settings, _udp, _tcp) = sockets();
    let root = directory.path().join("peer-identity");
    fs::create_dir_all(&root).unwrap();
    std::fs::write(
        root.join("identity-00000000000000000001.pending"),
        b"unpublished incomplete identity",
    )
    .unwrap();
    let runtime =
        open(directory.path(), config.clone(), checkpoint.clone(), identity, now, settings);
    assert_eq!(runtime.local_enr().unwrap().seq(), 1);
    drop(runtime);
    for number in 2..=MAX_DIRECTORY_ENTRIES {
        std::fs::write(root.join(format!("identity-{number:020}.pending")), b"partial").unwrap();
    }
    assert!(matches!(
        CheckpointPeerRuntime::open(directory.path(), identity, config, checkpoint, now, settings),
        Err(PeerStartupError::IdentityTooLarge)
    ));
}

#[test]
fn record_content_changes_require_higher_sequence_and_existing_record_cannot_regress() {
    let directory = TempDir::new().unwrap();
    let (config, _, identity, _) = checkpoint();
    let (settings, _udp, _tcp) = sockets();
    let digest = checked_config_digest(&config).unwrap();
    let mut store =
        IdentityStore::open(directory.path(), identity, config.clone(), digest).unwrap();
    let original =
        sign_local_enr(&store.key, 8, &config, 72, &settings.advertisement(), settings.policy)
            .unwrap();
    store.persist(&original, 72, settings.policy).unwrap();
    let lower =
        sign_local_enr(&store.key, 7, &config, 72, &settings.advertisement(), settings.policy)
            .unwrap();
    assert!(matches!(
        store.persist(&lower, 72, settings.policy),
        Err(PeerStartupError::SequenceRegression)
    ));
    let changed =
        PeerListenConfig { tcp_port: settings.tcp_port.checked_add(1).unwrap_or(1), ..settings };
    let reused =
        sign_local_enr(&store.key, 8, &config, 72, &changed.advertisement(), changed.policy)
            .unwrap();
    assert!(matches!(
        store.persist(&reused, 72, changed.policy),
        Err(PeerStartupError::SequenceRegression)
    ));
    assert_eq!(store.enr.unwrap(), original);
    assert!(!snapshot_file(&directory, 2).exists());
}

#[test]
fn durable_private_container_identity_keeps_literal_address_policy_and_original_key() {
    let directory = TempDir::new().unwrap();
    let (config, proof, identity, now) = checkpoint();
    let (mut settings, _udp, _tcp) = sockets();
    settings.ip = "172.18.0.3".parse().unwrap();
    settings.policy = EndpointPolicy::PrivateNetwork;
    let first = open(directory.path(), config.clone(), proof.clone(), identity, now, settings);
    let original_key = first.peer_id().unwrap();
    let original_record = first.local_enr().unwrap();
    assert_eq!(original_record.ip4().unwrap().to_string(), "172.18.0.3");
    drop(first);
    let reopened = open(directory.path(), config, proof, identity, now, settings);
    assert_eq!(reopened.peer_id().unwrap(), original_key);
    assert_eq!(reopened.local_enr().unwrap(), original_record);
    assert_eq!(reopened.identity.policy, EndpointPolicy::PrivateNetwork);
    assert!(!reopened.started);
}

#[test]
fn local_binding_and_signed_advertisement_are_distinct_with_matching_address_families() {
    let settings = PeerListenConfig {
        ip: "8.8.8.8".parse().unwrap(),
        bind_ip: Some("0.0.0.0".parse().unwrap()),
        discovery_port: 12000,
        tcp_port: 12001,
        quic_port: 12002,
        policy: EndpointPolicy::Public,
    };
    settings.validate().unwrap();
    assert_eq!(settings.advertisement().discovery_socket.ip(), settings.ip);
    assert_eq!(settings.transport_listeners().unwrap()[0].to_string(), "/ip4/0.0.0.0/tcp/12001");
    assert!(
        matches!(settings.discovery_listener(), ListenConfig::Ipv4 { ip, port: 12000 } if ip.is_unspecified())
    );
    assert!(PeerListenConfig { bind_ip: Some("::".parse().unwrap()), ..settings }
        .validate()
        .is_err());
    assert!(PeerListenConfig { ip: "192.168.1.2".parse().unwrap(), ..settings }
        .validate()
        .is_err());
    assert!(PeerListenConfig { bind_ip: Some("224.0.0.1".parse().unwrap()), ..settings }
        .validate()
        .is_err());
    let isolated = PeerListenConfig {
        ip: "127.0.0.1".parse().unwrap(),
        policy: EndpointPolicy::LoopbackOnly,
        ..settings
    };
    assert!(isolated.validate().is_err());
}

#[test]
fn experimental_peer_admission_is_exactly_canonical_hoodi_public_checkpoint() {
    let config = ChainConfig::hoodi();
    let listeners = PeerListenConfig {
        ip: "8.8.8.8".parse().unwrap(),
        bind_ip: Some("0.0.0.0".parse().unwrap()),
        discovery_port: 12000,
        tcp_port: 12001,
        quic_port: 12002,
        policy: EndpointPolicy::Public,
    };
    validate_experimental_admission(&config, listeners, None).unwrap();
    assert!(Readiness::current().require_live_sync().is_err());
    for changed in [ChainConfig::mainnet(), {
        let mut changed = config.clone();
        changed.genesis_validators_root = [3; 32];
        changed
    }] {
        assert!(matches!(
            validate_experimental_admission(&changed, listeners, None),
            Err(PeerStartupError::ExperimentalAdmission)
        ));
    }
    for policy in [EndpointPolicy::LoopbackOnly, EndpointPolicy::PrivateNetwork] {
        assert!(matches!(
            validate_experimental_admission(
                &config,
                PeerListenConfig { policy, ..listeners },
                None
            ),
            Err(PeerStartupError::ExperimentalAdmission)
        ));
    }
    assert!(matches!(
        validate_experimental_admission(&config, listeners, Some([3; 32])),
        Err(PeerStartupError::ExperimentalAdmission)
    ));
    assert!(matches!(
        validate_experimental_admission(
            &config,
            PeerListenConfig { ip: "127.0.0.1".parse().unwrap(), ..listeners },
            None
        ),
        Err(PeerStartupError::Configuration)
    ));
}
