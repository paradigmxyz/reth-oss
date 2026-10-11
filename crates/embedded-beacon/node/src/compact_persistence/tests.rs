//! Genuine signed-deposit bootstrap and admitted-block persistence regressions.

use super::*;
use crate::{
    gossip_tests::{gloas_store_and_keys, phase0_signed_child, store_and_keys},
    BootstrapDecodeLimits, BootstrapSettings, CheckpointEvidence, CheckpointVerifier,
};
use reth_embedded_beacon_config::{ForkActivation as ConfigActivation, ForkSchedule};
use reth_embedded_beacon_shared_data::journal::inject_post_rename_failure;
use reth_embedded_beacon_state_transition::fork_processing::TransitionParameters;
use tempfile::TempDir;

fn config(state: &BeaconState, parameters: &TransitionParameters) -> ChainConfig {
    ChainConfig {
        name: "genuine-compact-test".to_owned(),
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
                .map(|entry| ConfigActivation {
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
            .map(|entry| reth_embedded_beacon_config::BlobParameters {
                epoch: entry.epoch,
                max_blobs_per_block: entry.max_blobs_per_block,
            })
            .collect(),
        max_payload_size: 10 * 1024 * 1024,
    }
}
fn open_repository(directory: &TempDir, config: ChainConfig) -> AdmittedStateRepository {
    let network = PersistenceNetwork::from_config(config).unwrap();
    let shared = SharedDataStore::open(directory.path(), network.identity(), 0).unwrap();
    AdmittedStateRepository::open(shared, network, CompactStateLimits::default()).unwrap()
}
fn bootstrap(
    repository: &AdmittedStateRepository,
    state: &BeaconState,
    block: &BeaconBlock,
    config: ChainConfig,
) {
    let verifier = SszCheckpointVerifier::new(
        config,
        BootstrapDecodeLimits::default(),
        CheckpointBlockFormat::Unsigned,
    )
    .unwrap();
    let state_bytes = state.to_ssz().unwrap();
    let block_bytes = block.to_ssz().unwrap();
    let checkpoint = Checkpoint { epoch: 0, root: block.hash_tree_root().unwrap() };
    let verified =
        verifier.verify_checkpoint(&state_bytes, &block_bytes, checkpoint, 0, 1).unwrap();
    repository.persist_checkpoint(&verifier, &verified, &state_bytes, &block_bytes, 0, 1).unwrap();
}

#[test]
fn concrete_genuine_bootstrap_and_actual_store_snapshot_restart_in_every_fork() {
    for fork in [
        Fork::Phase0,
        Fork::Altair,
        Fork::Bellatrix,
        Fork::Capella,
        Fork::Deneb,
        Fork::Electra,
        Fork::Fulu,
        Fork::Gloas,
    ] {
        let directory = TempDir::new().unwrap();
        let (state, block, parameters, classic, gloas) = if fork == Fork::Gloas {
            let (store, keys, parameters) = gloas_store_and_keys();
            assert_eq!(keys.len(), 32);
            let root = store.get_head().unwrap().root;
            (
                BeaconState::Gloas(store.block_state(root).unwrap().clone(), Preset::Minimal),
                BeaconBlock::Gloas((*store.block_arc(root).unwrap()).clone(), Preset::Minimal),
                parameters,
                None,
                Some(store),
            )
        } else {
            let (store, keys, parameters) = store_and_keys(fork);
            assert_eq!(keys.len(), 32);
            let root = store.get_head().unwrap().root;
            (
                store.block_state(root).unwrap().clone(),
                store.block(root).unwrap().clone(),
                parameters,
                Some(store),
                None,
            )
        };
        let config = config(&state, &parameters);
        let repository = open_repository(&directory, config.clone());
        bootstrap(&repository, &state, &block, config.clone());
        let snapshot = if let Some(store) = &classic {
            repository.publish_classic_snapshot(store).unwrap()
        } else {
            repository.publish_gloas_snapshot(gloas.as_ref().unwrap()).unwrap()
        };
        assert_eq!(snapshot.state_root(), state.hash_tree_root().unwrap());
        assert_eq!(snapshot.head().root, block.hash_tree_root().unwrap());
        assert_eq!(snapshot.signed_envelope_root(), None);
        assert_eq!(
            repository.original_block(snapshot.head().root).unwrap().unwrap().1.as_ref(),
            block.to_ssz().unwrap()
        );
        drop(repository);
        let repository = open_repository(&directory, config);
        assert_eq!(repository.current_snapshot().unwrap().unwrap().head(), snapshot.head());
        assert_eq!(
            repository
                .block_state(snapshot.head().root)
                .unwrap()
                .unwrap()
                .state()
                .to_ssz()
                .unwrap(),
            state.to_ssz().unwrap()
        );
    }
}

#[test]
fn original_signed_child_and_real_post_state_are_required_before_head_publication() {
    let directory = TempDir::new().unwrap();
    let (mut store, keys, parameters) = store_and_keys(Fork::Phase0);
    let anchor = store.get_head().unwrap().root;
    let state = store.block_state(anchor).unwrap().clone();
    let block = store.block(anchor).unwrap().clone();
    let config = config(&state, &parameters);
    let repository = open_repository(&directory, config.clone());
    let signed = phase0_signed_child(&store, &keys, &parameters);
    let root = signed.message.hash_tree_root(Preset::Minimal).unwrap();
    store.on_tick(store.slot_duration_ms()).unwrap();
    store.on_block(&SignedBeaconBlock::Phase0(signed.clone(), Preset::Minimal)).unwrap();
    let bytes = signed.to_ssz(Preset::Minimal).unwrap();
    assert!(matches!(
        repository.persist_classic(&store, root, &bytes),
        Err(PersistenceError::MissingAnchor)
    ));
    bootstrap(&repository, &state, &block, config.clone());
    assert!(matches!(
        repository.publish_classic_snapshot(&store),
        Err(PersistenceError::MissingPredecessor)
    ));
    let mut altered_signature = signed.clone();
    altered_signature.signature = [0; 96];
    assert!(matches!(
        repository.persist_classic(
            &store,
            root,
            &altered_signature.to_ssz(Preset::Minimal).unwrap()
        ),
        Err(PersistenceError::Crypto(_))
    ));
    assert_eq!(repository.stats().unwrap().retained_states, 1);
    assert!(repository.current_snapshot().unwrap().is_none());
    let receipt = repository.persist_classic(&store, root, &bytes).unwrap();
    assert_eq!(receipt.block_root(), root);
    assert_eq!(receipt.state_root(), signed.message.state_root);
    let snapshot = repository.publish_classic_snapshot(&store).unwrap();
    let duplicate = repository.persist_classic(&store, root, &bytes).unwrap();
    assert_eq!(duplicate.original(), receipt.original());
    assert!(!directory
        .path()
        .join(
            "shared-data-v1/beacon-admitted-states-v1/journal/snapshot-00000000000000000005.commit"
        )
        .exists());
    assert_eq!(snapshot.head(), store.get_head().unwrap());
    let original = repository.original_block(root).unwrap().unwrap();
    assert_eq!(original.0, CheckpointBlockFormat::Signed);
    assert_eq!(original.1.as_ref(), bytes);
    assert_eq!(
        repository.block_state(root).unwrap().unwrap().state().to_ssz().unwrap(),
        store.block_state(root).unwrap().to_ssz().unwrap()
    );
    drop(repository);
    let repository = open_repository(&directory, config);
    assert_eq!(repository.current_snapshot().unwrap().unwrap().head(), snapshot.head());
    assert_eq!(repository.original_block(root).unwrap().unwrap().1.as_ref(), bytes);
}

#[test]
fn uncertain_head_pointer_stops_access_and_reopen_restores_complete_generation() {
    let directory = TempDir::new().unwrap();
    let (mut store, keys, parameters) = store_and_keys(Fork::Phase0);
    let anchor = store.get_head().unwrap().root;
    let state = store.block_state(anchor).unwrap().clone();
    let block = store.block(anchor).unwrap().clone();
    let config = config(&state, &parameters);
    let repository = open_repository(&directory, config.clone());
    bootstrap(&repository, &state, &block, config.clone()); // admission generation 2
    repository.publish_classic_snapshot(&store).unwrap(); // generation 3
    let lease = repository.block_state(anchor).unwrap().unwrap();
    let signed = phase0_signed_child(&store, &keys, &parameters);
    let root = signed.message.hash_tree_root(Preset::Minimal).unwrap();
    let bytes = signed.to_ssz(Preset::Minimal).unwrap();
    store.on_tick(store.slot_duration_ms()).unwrap();
    store.on_block(&SignedBeaconBlock::Phase0(signed, Preset::Minimal)).unwrap();
    repository.persist_classic(&store, root, &bytes).unwrap(); // generation 4
    inject_post_rename_failure(directory.path().join(
        "shared-data-v1/beacon-admitted-states-v1/journal/snapshot-00000000000000000005.commit",
    ));
    assert!(matches!(
        repository.publish_classic_snapshot(&store),
        Err(PersistenceError::Shared(SharedError::PublicationUncertain(_)))
    ));
    assert!(matches!(
        repository.current_snapshot(),
        Err(PersistenceError::Shared(SharedError::JournalPoisoned))
    ));
    assert!(matches!(
        repository.original_block(anchor),
        Err(PersistenceError::Shared(SharedError::JournalPoisoned))
    ));
    assert_eq!(lease.state().hash_tree_root().unwrap(), state.hash_tree_root().unwrap());
    drop(repository);
    let repository = open_repository(&directory, config);
    assert_eq!(repository.current_snapshot().unwrap().unwrap().head(), store.get_head().unwrap());
    assert_eq!(repository.original_block(root).unwrap().unwrap().1.as_ref(), bytes);
}

#[derive(Clone)]
struct ClaimedEvidence(CheckpointEvidence);
impl CheckpointVerifier for ClaimedEvidence {
    fn verify_state_and_block(
        &self,
        _: &[u8],
        _: &[u8],
    ) -> Result<CheckpointEvidence, BootstrapError> {
        Ok(self.0.clone())
    }
}
#[test]
fn caller_evidence_and_foreign_profile_cannot_initialize_a_trusted_repository() {
    let directory = TempDir::new().unwrap();
    let (store, _, parameters) = store_and_keys(Fork::Phase0);
    let root = store.get_head().unwrap().root;
    let state = store.block_state(root).unwrap();
    let block = store.block(root).unwrap();
    let config = config(state, &parameters);
    let network = PersistenceNetwork::from_config(config.clone()).unwrap();
    let identity = StoreIdentity { config_digest: [9; 32], ..network.identity() };
    let shared = SharedDataStore::open(directory.path(), identity, 0).unwrap();
    assert!(matches!(
        AdmittedStateRepository::open(shared, network, CompactStateLimits::default()),
        Err(PersistenceError::Identity)
    ));
    let directory = TempDir::new().unwrap();
    let repository = open_repository(&directory, config.clone());
    let header = block.header().unwrap();
    let checkpoint = Checkpoint { epoch: 0, root };
    let evidence = CheckpointEvidence {
        genesis_validators_root: state.genesis_validators_root(),
        block_root: root,
        state_root: state.hash_tree_root().unwrap(),
        state_slot: 0,
        block_slot: 0,
        latest_block_root: root,
        block_state_root: header.state_root,
    };
    let state_bytes = state.to_ssz().unwrap();
    let block_bytes = block.to_ssz().unwrap();
    let claimed = VerifiedCheckpoint::verify(
        &ClaimedEvidence(evidence),
        &state_bytes,
        &block_bytes,
        checkpoint,
        BootstrapSettings {
            network: state.genesis_validators_root(),
            slots_per_epoch: 8,
            current_epoch: 0,
            max_age_epochs: 1,
        },
    )
    .unwrap();
    let verifier = SszCheckpointVerifier::new(
        config,
        BootstrapDecodeLimits::default(),
        CheckpointBlockFormat::Unsigned,
    )
    .unwrap();
    assert!(matches!(
        repository.persist_checkpoint(&verifier, &claimed, &state_bytes, &block_bytes, 0, 1),
        Err(PersistenceError::Identity)
    ));
    assert_eq!(repository.stats().unwrap().retained_states, 0);
}

#[test]
fn absent_gloas_envelope_cannot_create_a_payload_receipt() {
    let directory = TempDir::new().unwrap();
    let (store, _, parameters) = gloas_store_and_keys();
    let root = store.get_head().unwrap().root;
    let state = BeaconState::Gloas(store.block_state(root).unwrap().clone(), Preset::Minimal);
    let block = BeaconBlock::Gloas((*store.block_arc(root).unwrap()).clone(), Preset::Minimal);
    let config = config(&state, &parameters);
    let repository = open_repository(&directory, config.clone());
    bootstrap(&repository, &state, &block, config);
    assert!(matches!(
        repository.persist_verified_envelope(&store, root, &[]),
        Err(PersistenceError::UnverifiedEnvelope)
    ));
    let snapshot = repository.publish_gloas_snapshot(&store).unwrap();
    assert_eq!(snapshot.execution(), None);
    assert_eq!(snapshot.signed_envelope_root(), None);
}

#[test]
fn recovered_trusted_seed_is_reauthenticated_and_stale_seed_is_rejected() {
    let directory = TempDir::new().unwrap();
    let (store, _, parameters) = store_and_keys(Fork::Phase0);
    let root = store.get_head().unwrap().root;
    let state = store.block_state(root).unwrap();
    let block = store.block(root).unwrap();
    let config = config(state, &parameters);
    let repository = open_repository(&directory, config.clone());
    bootstrap(&repository, state, block, config.clone());
    let verifier = SszCheckpointVerifier::new(
        config.clone(),
        BootstrapDecodeLimits::default(),
        CheckpointBlockFormat::Unsigned,
    )
    .unwrap();
    let recovered = repository.recover_checkpoint(&verifier, 0, 1).unwrap().unwrap();
    assert_eq!(recovered.checkpoint.state_root(), state.hash_tree_root().unwrap());
    assert_eq!(recovered.state.state().to_ssz().unwrap(), state.to_ssz().unwrap());
    assert_eq!(recovered.block_ssz.as_ref(), block.to_ssz().unwrap());
    assert_eq!(recovered.block_format, CheckpointBlockFormat::Unsigned);
    let stale = config.clock().unwrap().start_of_slot(16).unwrap();
    assert!(matches!(
        repository.recover_checkpoint(&verifier, stale, 1),
        Err(PersistenceError::Bootstrap(BootstrapError::StaleCheckpoint))
    ));
}

#[test]
fn cached_state_or_original_bytes_without_durable_pins_cannot_publish_a_head() {
    let directory = TempDir::new().unwrap();
    let (store, _, parameters) = store_and_keys(Fork::Phase0);
    let root = store.get_head().unwrap().root;
    let state = store.block_state(root).unwrap();
    let block = store.block(root).unwrap();
    let config = config(state, &parameters);
    let repository = open_repository(&directory, config.clone());
    bootstrap(&repository, state, block, config);
    repository.publish_classic_snapshot(&store).unwrap();
    let held = repository.block_state(root).unwrap().unwrap();
    let original = repository.inner.lock().unwrap().records[&root].original;
    repository.shared.release(Owner::Checkpoint(root), original).unwrap();
    assert!(matches!(repository.publish_classic_snapshot(&store), Err(PersistenceError::Linkage)));
    repository.shared.pin(Owner::Checkpoint(root), original).unwrap();
    let state_root = held.root();
    let manifests = repository.shared.owner_objects(Owner::CompactState(state_root));
    assert_eq!(manifests.len(), 1);
    repository.shared.release_owners(&[Owner::CompactState(state_root)]).unwrap();
    assert!(matches!(
        repository.publish_classic_snapshot(&store),
        Err(PersistenceError::Compact(CompactStateError::InvalidDescriptor))
    ));
    assert_eq!(held.state().hash_tree_root().unwrap(), state_root);
    repository.shared.pin(Owner::CompactState(state_root), manifests[0]).unwrap();
    repository.publish_classic_snapshot(&store).unwrap();
}

#[test]
fn a_distinct_head_cannot_hide_missing_checkpoint_pins_on_publication_or_restart() {
    let directory = TempDir::new().unwrap();
    let (mut store, keys, parameters) = store_and_keys(Fork::Phase0);
    let anchor = store.get_head().unwrap().root;
    let state = store.block_state(anchor).unwrap().clone();
    let block = store.block(anchor).unwrap().clone();
    let config = config(&state, &parameters);
    let repository = open_repository(&directory, config.clone());
    bootstrap(&repository, &state, &block, config.clone());
    let signed = phase0_signed_child(&store, &keys, &parameters);
    let head = signed.message.hash_tree_root(Preset::Minimal).unwrap();
    store.on_tick(store.slot_duration_ms()).unwrap();
    store.on_block(&SignedBeaconBlock::Phase0(signed.clone(), Preset::Minimal)).unwrap();
    let original = signed.to_ssz(Preset::Minimal).unwrap();
    repository.persist_classic(&store, head, &original).unwrap();
    let snapshot = repository.publish_classic_snapshot(&store).unwrap();
    assert_ne!(snapshot.head().root, anchor);
    assert_eq!(snapshot.checkpoints().0.root, anchor);
    assert_eq!(snapshot.checkpoints().1.root, anchor);
    let held = repository.block_state(head).unwrap().unwrap();
    let anchor_original = repository.inner.lock().unwrap().records[&anchor].original;
    repository.shared.release(Owner::Checkpoint(anchor), anchor_original).unwrap();
    assert_eq!(repository.original_block(head).unwrap().unwrap().1.as_ref(), original);
    assert!(matches!(repository.publish_classic_snapshot(&store), Err(PersistenceError::Linkage)));
    assert_eq!(held.state().hash_tree_root().unwrap(), snapshot.state_root());
    let identity = repository.shared.identity();
    drop(repository);
    let shared = SharedDataStore::open(directory.path(), identity, 0).unwrap();
    assert!(matches!(
        AdmittedStateRepository::open(
            shared.clone(),
            PersistenceNetwork::from_config(config.clone()).unwrap(),
            CompactStateLimits::default(),
        ),
        Err(PersistenceError::Linkage)
    ));
    shared.pin(Owner::Checkpoint(anchor), anchor_original).unwrap();
    drop(shared);
    let repository = open_repository(&directory, config);
    assert_eq!(repository.current_snapshot().unwrap().unwrap().head().root, head);
    assert_eq!(repository.original_block(head).unwrap().unwrap().1.as_ref(), original);
}

#[test]
fn private_profile_terminal_identity_stays_separate_from_consensus_subset_digest() {
    let mut consensus = ChainConfig::mainnet();
    consensus.name = "compact-private".to_owned();
    consensus.chain_id = 12345;
    consensus.seconds_per_slot = 6;
    consensus.blob_schedule.clear();
    consensus.forks = ForkSchedule::new(
        [
            Fork::Phase0,
            Fork::Altair,
            Fork::Bellatrix,
            Fork::Capella,
            Fork::Deneb,
            Fork::Electra,
            Fork::Fulu,
            Fork::Gloas,
        ]
        .into_iter()
        .enumerate()
        .map(|(index, fork)| ConfigActivation {
            fork,
            epoch: index as u64,
            version: [index as u8, 0, 0, 42],
        })
        .collect(),
    )
    .unwrap();
    let profile = DevnetConfig {
        schema_version: 1,
        execution_genesis_hash: [7; 32],
        terminal_block_hash: [7; 32],
        terminal_block_hash_activation_epoch: 2,
        consensus: consensus.clone(),
    };
    let network = PersistenceNetwork::from_devnet(profile.clone()).unwrap();
    assert_ne!(network.identity().config_digest, checked_config_digest(&consensus).unwrap());
    let directory = TempDir::new().unwrap();
    let shared = SharedDataStore::open(directory.path(), network.identity(), 0).unwrap();
    assert!(matches!(
        AdmittedStateRepository::open(
            shared.clone(),
            PersistenceNetwork::from_config(consensus).unwrap(),
            CompactStateLimits::default()
        ),
        Err(PersistenceError::Identity)
    ));
    let repository =
        AdmittedStateRepository::open(shared, network, CompactStateLimits::default()).unwrap();
    assert!(repository.current_snapshot().unwrap().is_none());
    let mut changed = profile;
    changed.execution_genesis_hash = [8; 32];
    changed.terminal_block_hash = [8; 32];
    assert_ne!(
        PersistenceNetwork::from_devnet(changed).unwrap().identity(),
        repository.shared.identity()
    );
}

#[cfg(all(feature = "peer-runtime", feature = "execution-verification"))]
#[path = "tests/events.rs"]
mod events;

#[cfg(all(feature = "peer-runtime", feature = "execution-verification"))]
#[path = "tests/context.rs"]
mod context;

#[cfg(all(feature = "peer-runtime", feature = "execution-verification"))]
#[path = "tests/events_cleanup.rs"]
mod events_cleanup;
