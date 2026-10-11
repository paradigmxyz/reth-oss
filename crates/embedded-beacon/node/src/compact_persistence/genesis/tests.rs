//! Genuine complete genesis persistence, durable-owner recovery and mode separation controls.

use super::*;
use crate::{bootstrap::genesis::tests::genuine, GenesisError};
use reth_embedded_beacon_shared_data::journal::inject_post_rename_failure;
use tempfile::TempDir;

const FORKS: [Fork; 8] = [
    Fork::Phase0,
    Fork::Altair,
    Fork::Bellatrix,
    Fork::Capella,
    Fork::Deneb,
    Fork::Electra,
    Fork::Fulu,
    Fork::Gloas,
];
fn authenticated(fork: Fork) -> (ChainConfig, VerifiedGenesis) {
    let (config, state, _, _) = genuine(fork);
    let proof = VerifiedGenesis::authenticate(
        config.clone(),
        state.to_ssz().unwrap().into(),
        BootstrapDecodeLimits::default(),
    )
    .unwrap();
    (config, proof)
}
fn repository(directory: &TempDir, config: ChainConfig) -> AdmittedStateRepository {
    let network = PersistenceNetwork::from_config(config).unwrap();
    let shared = SharedDataStore::open(directory.path(), network.identity(), 0).unwrap();
    AdmittedStateRepository::open(shared, network, CompactStateLimits::default()).unwrap()
}

#[test]
fn authentic_original_genesis_restarts_in_all_eight_schemas_without_checkpoint_conversion() {
    for fork in FORKS {
        let directory = TempDir::new().unwrap();
        let (config, genesis) = authenticated(fork);
        let repo = repository(&directory, config.clone());
        repo.persist_genesis(&genesis, BootstrapDecodeLimits::default()).unwrap();
        repo.persist_genesis(&genesis, BootstrapDecodeLimits::default()).unwrap();
        assert!(repo.current_snapshot().unwrap().is_none());
        assert_eq!(repo.stats().unwrap().retained_states, 1);
        let original = repo.original_block(genesis.block_root()).unwrap().unwrap();
        assert_eq!(original.0, CheckpointBlockFormat::Unsigned);
        assert_eq!(original.1, genesis.block_ssz());
        drop(repo);
        let repo = repository(&directory, config.clone());
        let recovered = repo.recover_genesis(BootstrapDecodeLimits::default()).unwrap().unwrap();
        assert_eq!(recovered.config_digest(), genesis.config_digest());
        assert_eq!(recovered.state_root(), genesis.state_root());
        assert_eq!(recovered.block_root(), genesis.block_root());
        assert_eq!(recovered.original_state_ssz(), genesis.original_state_ssz());
        assert_eq!(recovered.block_ssz(), genesis.block_ssz());
        let verifier = SszCheckpointVerifier::new(
            config.clone(),
            BootstrapDecodeLimits::default(),
            CheckpointBlockFormat::Unsigned,
        )
        .unwrap();
        let ancient = config.clock().unwrap().start_of_slot(8_000_000).unwrap();
        assert!(matches!(
            repo.recover_checkpoint(&verifier, ancient, 2),
            Err(PersistenceError::WrongAnchorKind)
        ));
        assert!(repo.inner.lock().unwrap().anchor.as_ref().unwrap().genesis);
    }
}
#[test]
fn sealed_configuration_and_capacity_limits_reject_before_a_genesis_pointer_exists() {
    let directory = TempDir::new().unwrap();
    let (config, genesis) = authenticated(Fork::Phase0);
    let mut different = config.clone();
    different.name.push_str("-different");
    let repo = repository(&directory, different);
    assert!(matches!(
        repo.persist_genesis(&genesis, BootstrapDecodeLimits::default()),
        Err(PersistenceError::Identity)
    ));
    assert!(repo.recover_genesis(BootstrapDecodeLimits::default()).unwrap().is_none());
    assert_eq!(repo.stats().unwrap().retained_states, 0);
    let directory = TempDir::new().unwrap();
    let repo = repository(&directory, config);
    assert!(matches!(
        repo.persist_genesis(
            &genesis,
            BootstrapDecodeLimits { max_state_bytes: 1, max_block_bytes: 10_000 }
        ),
        Err(PersistenceError::Genesis(GenesisError::Bounds))
    ));
    assert!(repo.inner.lock().unwrap().anchor.is_none());
    assert_eq!(repo.stats().unwrap().retained_states, 0);
}
#[test]
fn ordinary_checkpoint_metadata_cannot_be_consumed_as_genesis_or_overwritten_by_it() {
    let directory = TempDir::new().unwrap();
    let (config, genesis) = authenticated(Fork::Phase0);
    let repo = repository(&directory, config.clone());
    let verifier = SszCheckpointVerifier::new(
        config,
        BootstrapDecodeLimits::default(),
        CheckpointBlockFormat::Unsigned,
    )
    .unwrap();
    let actual = verifier
        .verify_checkpoint(
            &genesis.original_state_ssz(),
            &genesis.block_ssz(),
            Checkpoint { epoch: 0, root: genesis.block_root() },
            0,
            1,
        )
        .unwrap();
    repo.persist_checkpoint(
        &verifier,
        &actual,
        &genesis.original_state_ssz(),
        &genesis.block_ssz(),
        0,
        1,
    )
    .unwrap();
    assert!(matches!(
        repo.recover_genesis(BootstrapDecodeLimits::default()),
        Err(PersistenceError::WrongAnchorKind)
    ));
    assert!(matches!(
        repo.persist_genesis(&genesis, BootstrapDecodeLimits::default()),
        Err(PersistenceError::ConflictingAnchor)
    ));
    assert!(repo.recover_checkpoint(&verifier, 0, 1).unwrap().is_some());
}
#[test]
fn missing_original_or_compact_owner_is_not_hidden_by_a_hot_state_lease() {
    let directory = TempDir::new().unwrap();
    let (config, genesis) = authenticated(Fork::Gloas);
    let repo = repository(&directory, config);
    repo.persist_genesis(&genesis, BootstrapDecodeLimits::default()).unwrap();
    let held = repo.block_state(genesis.block_root()).unwrap().unwrap();
    let original = repo.inner.lock().unwrap().records[&genesis.block_root()].original;
    repo.shared.release(Owner::Checkpoint(genesis.block_root()), original).unwrap();
    assert!(matches!(
        repo.recover_genesis(BootstrapDecodeLimits::default()),
        Err(PersistenceError::Linkage)
    ));
    repo.shared.pin(Owner::Checkpoint(genesis.block_root()), original).unwrap();
    let owners = repo.shared.owner_objects(Owner::CompactState(genesis.state_root()));
    repo.shared.release_owners(&[Owner::CompactState(genesis.state_root())]).unwrap();
    assert!(matches!(
        repo.recover_genesis(BootstrapDecodeLimits::default()),
        Err(PersistenceError::Compact(CompactStateError::InvalidDescriptor))
    ));
    assert_eq!(held.state().hash_tree_root().unwrap(), genesis.state_root());
    for object in owners {
        repo.shared.pin(Owner::CompactState(genesis.state_root()), object).unwrap()
    }
    assert!(repo.recover_genesis(BootstrapDecodeLimits::default()).unwrap().is_some());
}
#[test]
fn uncertain_genesis_pointer_publication_requires_real_reopen_and_reauthentication() {
    let directory = TempDir::new().unwrap();
    let (config, genesis) = authenticated(Fork::Phase0);
    let repo = repository(&directory, config.clone());
    inject_post_rename_failure(directory.path().join(
        "shared-data-v1/beacon-admitted-states-v1/journal/snapshot-00000000000000000002.commit",
    ));
    assert!(matches!(
        repo.persist_genesis(&genesis, BootstrapDecodeLimits::default()),
        Err(PersistenceError::Shared(SharedError::PublicationUncertain(_)))
    ));
    assert!(matches!(
        repo.recover_genesis(BootstrapDecodeLimits::default()),
        Err(PersistenceError::Shared(SharedError::JournalPoisoned))
    ));
    drop(repo);
    let repo = repository(&directory, config);
    let actual = repo.recover_genesis(BootstrapDecodeLimits::default()).unwrap().unwrap();
    assert_eq!(actual.block_root(), genesis.block_root());
    assert_eq!(actual.original_state_ssz(), genesis.original_state_ssz());
    assert!(repo.current_snapshot().unwrap().is_none());
}
#[test]
fn saved_metadata_and_fresh_decode_bounds_do_not_replace_concrete_genesis_validation() {
    let directory = TempDir::new().unwrap();
    let (config, genesis) = authenticated(Fork::Phase0);
    let repo = repository(&directory, config);
    repo.persist_genesis(&genesis, BootstrapDecodeLimits::default()).unwrap();
    assert!(matches!(
        repo.recover_genesis(BootstrapDecodeLimits { max_state_bytes: 1, max_block_bytes: 10_000 }),
        Err(PersistenceError::Genesis(GenesisError::Bounds))
    ));
    repo.inner.lock().unwrap().anchor.as_mut().unwrap().slot = 1;
    assert!(matches!(
        repo.recover_genesis(BootstrapDecodeLimits::default()),
        Err(PersistenceError::Linkage)
    ));
    repo.inner.lock().unwrap().anchor.as_mut().unwrap().slot = 0;
    assert!(repo.recover_genesis(BootstrapDecodeLimits::default()).unwrap().is_some());
}
