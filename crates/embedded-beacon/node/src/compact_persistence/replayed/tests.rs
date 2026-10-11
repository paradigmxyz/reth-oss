//! Genuine original signed child plus exact boundary persistence and recovery controls.

use super::*;
use crate::{
    bootstrap::{boundary_replay::tests::fixture, genesis::tests::genuine},
    BootstrapDecodeLimits, VerifiedGenesis,
};
use tempfile::TempDir;

fn open_repository(directory: &TempDir, config: ChainConfig) -> AdmittedStateRepository {
    let network = PersistenceNetwork::from_config(config).unwrap();
    let shared = SharedDataStore::open(directory.path(), network.identity(), 0).unwrap();
    AdmittedStateRepository::open(shared, network, CompactStateLimits::default()).unwrap()
}

#[test]
fn genuine_original_post_state_and_separate_boundary_survive_complete_reauthentication() {
    for upgrade in [false, true] {
        let fixture = fixture(upgrade);
        let directory = TempDir::new().unwrap();
        let verifier = SszCheckpointVerifier::new(
            fixture.config.clone(),
            BootstrapDecodeLimits::default(),
            CheckpointBlockFormat::Signed,
        )
        .unwrap();
        let boundary = fixture.boundary.to_ssz().unwrap();
        let post = fixture.post.to_ssz().unwrap();
        let proof = verifier
            .verify_checkpoint_replay(
                &boundary,
                &post,
                &fixture.block,
                fixture.trusted,
                fixture.now,
                4,
                7,
            )
            .unwrap();
        let repository = open_repository(&directory, fixture.config.clone());
        repository
            .persist_replayed_checkpoint(
                &verifier,
                &proof,
                &boundary,
                &post,
                &fixture.block,
                fixture.now,
                4,
            )
            .unwrap();
        assert_eq!(
            repository.original_block(fixture.trusted.root).unwrap().unwrap().0,
            CheckpointBlockFormat::Signed
        );
        let original = repository.block_state(fixture.trusted.root).unwrap().unwrap();
        assert_eq!(original.state().to_ssz().unwrap(), post);
        assert_eq!(original.state().schema_fork(), Fork::Phase0);
        assert_eq!(original.state().slot(), 1);
        let recovered = repository.recover_checkpoint(&verifier, fixture.now, 4).unwrap().unwrap();
        assert_eq!(recovered.state.state().to_ssz().unwrap(), boundary);
        assert_eq!(recovered.state.state().slot(), 8);
        let replay = recovered.replay.unwrap();
        assert_eq!(replay.post_block_state.state().to_ssz().unwrap(), post);
        assert!(replay.proof.matches_inputs(
            &boundary,
            &post,
            &fixture.block,
            CheckpointBlockFormat::Signed
        ));
        drop(original);
        drop(repository);
        let repository = open_repository(&directory, fixture.config.clone());
        assert!(repository.recover_checkpoint(&verifier, fixture.now, 4).unwrap().is_some());
        let stale = fixture.config.clock().unwrap().start_of_slot(48).unwrap();
        assert!(matches!(
            repository.recover_checkpoint(&verifier, stale, 1),
            Err(PersistenceError::Bootstrap(BootstrapError::StaleCheckpoint))
        ));
    }
}

#[test]
fn changed_original_input_cannot_use_existing_sealed_boundary_proof_or_publish_data() {
    let fixture = fixture(true);
    let verifier = SszCheckpointVerifier::new(
        fixture.config.clone(),
        BootstrapDecodeLimits::default(),
        CheckpointBlockFormat::Signed,
    )
    .unwrap();
    let boundary = fixture.boundary.to_ssz().unwrap();
    let post = fixture.post.to_ssz().unwrap();
    let proof = verifier
        .verify_checkpoint_replay(
            &boundary,
            &post,
            &fixture.block,
            fixture.trusted,
            fixture.now,
            4,
            7,
        )
        .unwrap();
    let directory = TempDir::new().unwrap();
    let repository = open_repository(&directory, fixture.config);
    let mut changed = boundary;
    let last = changed.last_mut().unwrap();
    *last ^= 1;
    assert!(matches!(
        repository.persist_replayed_checkpoint(
            &verifier,
            &proof,
            &changed,
            &post,
            &fixture.block,
            fixture.now,
            4
        ),
        Err(PersistenceError::Identity)
    ));
    assert!(repository.current_snapshot().unwrap().is_none());
    assert!(repository.replay_inventory(256).unwrap().is_empty());
}

#[test]
fn cached_boundary_without_its_independent_durable_pin_cannot_recover_or_reopen() {
    let fixture = fixture(true);
    let directory = TempDir::new().unwrap();
    let verifier = SszCheckpointVerifier::new(
        fixture.config.clone(),
        BootstrapDecodeLimits::default(),
        CheckpointBlockFormat::Signed,
    )
    .unwrap();
    let boundary = fixture.boundary.to_ssz().unwrap();
    let post = fixture.post.to_ssz().unwrap();
    let proof = verifier
        .verify_checkpoint_replay(
            &boundary,
            &post,
            &fixture.block,
            fixture.trusted,
            fixture.now,
            4,
            7,
        )
        .unwrap();
    let repository = open_repository(&directory, fixture.config.clone());
    repository
        .persist_replayed_checkpoint(
            &verifier,
            &proof,
            &boundary,
            &post,
            &fixture.block,
            fixture.now,
            4,
        )
        .unwrap();
    let root = proof.verified().state_root();
    let objects = repository.shared.owner_objects(Owner::CompactState(root));
    assert_eq!(objects.len(), 1);
    let shared = repository.shared.clone();
    let held = repository.recover_checkpoint(&verifier, fixture.now, 4).unwrap().unwrap();
    shared.release_owners(&[Owner::CompactState(root)]).unwrap();
    assert_eq!(held.state.state().hash_tree_root().unwrap(), root);
    assert_eq!(
        repository.block_state(fixture.trusted.root).unwrap().unwrap().state().to_ssz().unwrap(),
        post
    );
    assert!(matches!(
        repository.recover_checkpoint(&verifier, fixture.now, 4),
        Err(PersistenceError::Compact(CompactStateError::InvalidDescriptor))
    ));
    drop(repository);
    assert!(AdmittedStateRepository::open(
        shared.clone(),
        PersistenceNetwork::from_config(fixture.config.clone()).unwrap(),
        CompactStateLimits::default()
    )
    .is_err());
    shared.pin(Owner::CompactState(root), objects[0]).unwrap();
    let repository = AdmittedStateRepository::open(
        shared,
        PersistenceNetwork::from_config(fixture.config).unwrap(),
        CompactStateLimits::default(),
    )
    .unwrap();
    assert!(repository.recover_checkpoint(&verifier, fixture.now, 4).unwrap().is_some());
}

#[test]
fn zero_gap_boundary_uses_exact_verified_leases_and_requires_fresh_pins() {
    let (config, state, _, _) = genuine(Fork::Phase0);
    let genesis = VerifiedGenesis::authenticate(
        config.clone(),
        state.to_ssz().unwrap().into(),
        BootstrapDecodeLimits::default(),
    )
    .unwrap();
    let directory = TempDir::new().unwrap();
    let repository = open_repository(&directory, config);
    repository.persist_genesis(&genesis, BootstrapDecodeLimits::default()).unwrap();
    let mut anchor = repository.lock().unwrap().anchor.clone().unwrap();
    anchor.boundary = Some(BoundaryAnchor {
        state_root: anchor.state_root,
        slot: anchor.slot,
        fork: Fork::Phase0,
        max_replay_slots: 1,
    });
    repository.check_saved_boundary(&anchor).unwrap();
    repository.check_saved_boundary(&anchor).unwrap();
    let mut changed = anchor.clone();
    changed.boundary.as_mut().unwrap().slot += 1;
    assert!(matches!(repository.check_saved_boundary(&changed), Err(PersistenceError::Linkage)));
    let held = repository
        .compact
        .lease(anchor.state_root, Fork::Phase0, Preset::Minimal)
        .unwrap()
        .unwrap();
    repository.shared.release_owners(&[Owner::CompactState(anchor.state_root)]).unwrap();
    assert_eq!(held.root(), anchor.state_root);
    assert!(matches!(
        repository.check_saved_boundary(&anchor),
        Err(PersistenceError::Compact(CompactStateError::InvalidDescriptor))
    ));
}

#[test]
fn native_boundary_memo_is_weak_exact_and_checks_changed_state_and_durable_pins() {
    let fixture = fixture(false);
    let directory = TempDir::new().unwrap();
    let verifier = SszCheckpointVerifier::new(
        fixture.config.clone(),
        BootstrapDecodeLimits::default(),
        CheckpointBlockFormat::Signed,
    )
    .unwrap();
    let boundary = fixture.boundary.to_ssz().unwrap();
    let post = fixture.post.to_ssz().unwrap();
    let proof = verifier
        .verify_checkpoint_replay(
            &boundary,
            &post,
            &fixture.block,
            fixture.trusted,
            fixture.now,
            4,
            7,
        )
        .unwrap();
    let repository = open_repository(&directory, fixture.config.clone());
    repository
        .persist_replayed_checkpoint(
            &verifier,
            &proof,
            &boundary,
            &post,
            &fixture.block,
            fixture.now,
            4,
        )
        .unwrap();
    let anchor = repository.lock().unwrap().anchor.clone().unwrap();
    repository.check_saved_boundary(&anchor).unwrap();
    let held_post = repository
        .compact
        .lease(anchor.state_root, Fork::Phase0, Preset::Minimal)
        .unwrap()
        .unwrap();
    let root = anchor.boundary.as_ref().unwrap().state_root;
    let held_boundary =
        repository.compact.lease(root, Fork::Phase0, Preset::Minimal).unwrap().unwrap();
    {
        let memo = repository.boundary_proof.lock().unwrap();
        let memo = memo.as_ref().unwrap();
        assert_eq!(memo.anchor, anchor);
        assert!(memo.post.matches(&held_post));
        assert!(memo.boundary.matches(&held_boundary));
    }
    let resident = repository.stats().unwrap().resident_schema_bytes;
    repository.check_saved_boundary(&anchor).unwrap();
    assert_eq!(repository.stats().unwrap().resident_schema_bytes, resident);
    let mut changed_state = fixture.boundary.clone();
    let BeaconState::Phase0(state, _) = &mut changed_state else { unreachable!() };
    state.balances.values[0] += 1;
    let changed_root = changed_state.hash_tree_root().unwrap();
    repository.compact.persist_state(&changed_state, changed_root).unwrap();
    let mut changed = anchor.clone();
    changed.boundary.as_mut().unwrap().state_root = changed_root;
    assert!(matches!(repository.check_saved_boundary(&changed), Err(PersistenceError::Linkage)));
    assert_eq!(repository.boundary_proof.lock().unwrap().as_ref().unwrap().anchor, anchor);
    repository.shared.release_owners(&[Owner::CompactState(root)]).unwrap();
    assert_eq!(held_boundary.root(), root);
    assert!(matches!(
        repository.check_saved_boundary(&anchor),
        Err(PersistenceError::Compact(CompactStateError::InvalidDescriptor))
    ));
}
