//! Genuine signed post-block commitments and exact scheduled empty-slot boundary regressions.

use super::*;
use crate::gossip_tests::{phase0_signed_child, store_and_keys};
use reth_embedded_beacon_config::{ForkActivation, ForkSchedule};
use reth_embedded_beacon_primitives::Fork;
use reth_embedded_beacon_state_transition::{
    phase0_processing,
    ssz::{BeaconState, Preset, SignedBeaconBlock},
};

pub(crate) struct Fixture {
    pub(crate) config: ChainConfig,
    pub(crate) post: BeaconState,
    pub(crate) boundary: BeaconState,
    pub(crate) block: Vec<u8>,
    pub(crate) trusted: Checkpoint,
    pub(crate) now: u64,
}

pub(crate) fn fixture(upgrade: bool) -> Fixture {
    let (store, keys, parameters) = store_and_keys(Fork::Phase0);
    let signed = phase0_signed_child(&store, &keys, &parameters);
    let parent = store.block_state(store.get_head().unwrap().root).unwrap();
    let BeaconState::Phase0(mut state, preset) = replay_slots(parent, 1, &parameters, 1).unwrap()
    else {
        panic!("genuine Phase0 parent");
    };
    phase0_processing::process_block(
        &mut state,
        &signed.message,
        &parameters.gloas.fulu.electra.deneb.capella.bellatrix.altair.phase0,
    )
    .unwrap();
    assert_eq!(state.hash_tree_root(preset).unwrap(), signed.message.state_root);
    let post = BeaconState::Phase0(state, preset);
    let config = ChainConfig {
        name: "authenticated-empty-boundary".into(),
        preset: ConfigPreset::Minimal,
        chain_id: 12345,
        genesis_time: post.genesis_time(),
        genesis_validators_root: post.genesis_validators_root(),
        seconds_per_slot: 6,
        slots_per_epoch: 8,
        forks: ForkSchedule::new(
            parameters
                .schedule
                .iter()
                .filter_map(|activation| {
                    (activation.fork == Fork::Phase0 || upgrade && activation.fork == Fork::Altair)
                        .then_some(ForkActivation {
                            fork: activation.fork,
                            epoch: if activation.fork == Fork::Phase0 { 0 } else { 1 },
                            version: activation.version,
                        })
                })
                .collect(),
        )
        .unwrap(),
        blob_schedule: vec![],
        max_payload_size: 10 * 1024 * 1024,
    };
    let runtime = RuntimeParameters::from_config(&config).unwrap();
    let boundary = replay_slots(&post, 8, &runtime.transitions, 7).unwrap();
    let trusted =
        Checkpoint { epoch: 1, root: signed.message.hash_tree_root(Preset::Minimal).unwrap() };
    let block = SignedBeaconBlock::Phase0(signed, Preset::Minimal).to_ssz().unwrap();
    let now = config.clock().unwrap().start_of_slot(16).unwrap();
    Fixture { config, post, boundary, block, trusted, now }
}

fn verifier(fixture: &Fixture) -> SszCheckpointVerifier {
    SszCheckpointVerifier::new(
        fixture.config.clone(),
        BootstrapDecodeLimits::default(),
        CheckpointBlockFormat::Signed,
    )
    .unwrap()
}

#[test]
fn real_signed_post_block_authenticates_exact_empty_boundary_and_scheduled_upgrade() {
    for upgrade in [false, true] {
        let fixture = fixture(upgrade);
        let boundary = fixture.boundary.to_ssz().unwrap();
        let post = fixture.post.to_ssz().unwrap();
        let verifier = verifier(&fixture);
        assert_eq!(
            verifier
                .verify_checkpoint(&boundary, &fixture.block, fixture.trusted, fixture.now, 4)
                .unwrap_err(),
            BootstrapError::UnverifiedBoundaryState
        );
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
        assert_ne!(boundary, post);
        assert_eq!(proof.post_block_slot(), 1);
        assert!(proof.matches_inputs(
            &boundary,
            &post,
            &fixture.block,
            CheckpointBlockFormat::Signed
        ));
        assert!(!proof.matches_inputs(
            &post,
            &boundary,
            &fixture.block,
            CheckpointBlockFormat::Signed
        ));
        let verified = proof.verified();
        assert_eq!(verified.slot(), 8);
        assert_eq!(verified.state_root(), fixture.boundary.hash_tree_root().unwrap());
        assert_eq!(verified.checkpoint(), fixture.trusted);
        assert_eq!(verified.config_digest(), Some(checked_config_digest(&fixture.config).unwrap()));
        assert_eq!(
            fixture.boundary.schema_fork(),
            if upgrade { Fork::Altair } else { Fork::Phase0 }
        );
    }
}

#[test]
fn copied_header_cannot_authenticate_changed_boundary_or_changed_committed_state() {
    let fixture = fixture(false);
    let verifier = verifier(&fixture);
    let boundary = fixture.boundary.to_ssz().unwrap();
    let post = fixture.post.to_ssz().unwrap();
    let BeaconState::Phase0(mut tampered, preset) = fixture.boundary.clone() else {
        unreachable!()
    };
    tampered.balances.values[0] -= 1;
    assert_eq!(
        verifier
            .verify_checkpoint_with_replay(
                &tampered.to_ssz(preset).unwrap(),
                &post,
                &fixture.block,
                fixture.trusted,
                fixture.now,
                4,
                7
            )
            .unwrap_err(),
        BootstrapError::UnverifiedBoundaryState
    );
    let BeaconState::Phase0(mut tampered, preset) = fixture.post.clone() else { unreachable!() };
    tampered.balances.values[0] -= 1;
    assert_eq!(
        verifier
            .verify_checkpoint_with_replay(
                &boundary,
                &tampered.to_ssz(preset).unwrap(),
                &fixture.block,
                fixture.trusted,
                fixture.now,
                4,
                7
            )
            .unwrap_err(),
        BootstrapError::StateBlockMismatch
    );
}

#[test]
fn slot_work_input_size_trust_age_and_exact_root_are_bounded_before_acceptance() {
    let fixture = fixture(false);
    let verifier = verifier(&fixture);
    let boundary = fixture.boundary.to_ssz().unwrap();
    let post = fixture.post.to_ssz().unwrap();
    assert_eq!(
        verifier
            .verify_checkpoint_with_replay(
                &boundary,
                &post,
                &fixture.block,
                fixture.trusted,
                fixture.now,
                4,
                6
            )
            .unwrap_err(),
        BootstrapError::CheckpointReplayLimit
    );
    for slots in [0, 8193] {
        assert_eq!(
            verifier
                .verify_checkpoint_with_replay(
                    &boundary,
                    &post,
                    &fixture.block,
                    fixture.trusted,
                    fixture.now,
                    4,
                    slots
                )
                .unwrap_err(),
            BootstrapError::InvalidConfiguration
        );
    }
    let wrong = Checkpoint { root: [3; 32], ..fixture.trusted };
    assert_eq!(
        verifier
            .verify_checkpoint_with_replay(
                &boundary,
                &post,
                &fixture.block,
                wrong,
                fixture.now,
                4,
                7
            )
            .unwrap_err(),
        BootstrapError::WrongTrustedRoot
    );
    assert_eq!(
        verifier
            .verify_checkpoint_with_replay(
                &boundary,
                &post,
                &fixture.block,
                fixture.trusted,
                fixture.config.clock().unwrap().start_of_slot(48).unwrap(),
                4,
                7
            )
            .unwrap_err(),
        BootstrapError::StaleCheckpoint
    );
    let bounded = SszCheckpointVerifier::new(
        fixture.config.clone(),
        BootstrapDecodeLimits {
            max_state_bytes: post.len() - 1,
            max_block_bytes: fixture.block.len(),
        },
        CheckpointBlockFormat::Signed,
    )
    .unwrap();
    assert_eq!(
        bounded
            .verify_checkpoint_with_replay(
                &boundary,
                &post,
                &fixture.block,
                fixture.trusted,
                fixture.now,
                4,
                7
            )
            .unwrap_err(),
        BootstrapError::CheckpointTooLarge
    );
}

// Admit eight genuine signed descendants to obtain a directly committed epoch-boundary state.
fn identical_fixture() -> Fixture {
    let mut fixture = fixture(false);
    let (mut store, keys, parameters) = store_and_keys(Fork::Phase0);
    for _ in 0..8 {
        let signed = phase0_signed_child(&store, &keys, &parameters);
        let root = signed.message.hash_tree_root(Preset::Minimal).unwrap();
        let late =
            fixture.config.clock().unwrap().start_of_slot(signed.message.slot + 1).unwrap() - 1;
        let block = SignedBeaconBlock::Phase0(signed, Preset::Minimal);
        store.on_tick(late).unwrap();
        store.on_block(&block).unwrap();
        if block.header().unwrap().slot == 8 {
            fixture.post = store.block_state(root).unwrap().clone();
            fixture.boundary = fixture.post.clone();
            fixture.block = block.to_ssz().unwrap();
            fixture.trusted = Checkpoint { epoch: 1, root };
        }
    }
    assert_eq!(fixture.post.slot(), 8);
    fixture
}

#[test]
fn identical_original_states_authenticate_once_and_keep_every_input_binding() {
    let fixture = identical_fixture();
    let state = fixture.post.to_ssz().unwrap();
    let proof = verifier(&fixture)
        .verify_checkpoint_replay(
            &state,
            &state.clone(),
            &fixture.block,
            fixture.trusted,
            fixture.now,
            4,
            1,
        )
        .unwrap();
    assert_eq!(proof.verified().state_root(), fixture.post.hash_tree_root().unwrap());
    assert_eq!(proof.verified().checkpoint(), fixture.trusted);
    assert_eq!(proof.verified().slot(), 8);
    assert_eq!(proof.post_block_slot(), 8);
    assert!(proof.matches_inputs(&state, &state, &fixture.block, CheckpointBlockFormat::Signed));
    let mut changed = state.clone();
    let last = changed.len() - 1;
    changed[last] ^= 1;
    assert!(!proof.matches_inputs(&changed, &state, &fixture.block, CheckpointBlockFormat::Signed));
    assert!(!proof.matches_inputs(&state, &changed, &fixture.block, CheckpointBlockFormat::Signed));
    let mut changed_block = fixture.block.clone();
    changed_block[4] ^= 1;
    assert!(!proof.matches_inputs(&state, &state, &changed_block, CheckpointBlockFormat::Signed));
    assert!(!proof.matches_inputs(&state, &state, &fixture.block, CheckpointBlockFormat::Unsigned));
}

#[test]
fn identical_state_bytes_do_not_bypass_commitments_or_require_only_claimed_slots() {
    let fixture = identical_fixture();
    let checked = verifier(&fixture);
    let state = fixture.post.to_ssz().unwrap();
    let BeaconState::Phase0(mut tampered, preset) = fixture.post.clone() else { unreachable!() };
    tampered.balances.values[0] -= 1;
    let changed = tampered.to_ssz(preset).unwrap();
    assert_eq!(
        checked
            .verify_checkpoint_with_replay(
                &changed,
                &changed,
                &fixture.block,
                fixture.trusted,
                fixture.now,
                4,
                1,
            )
            .unwrap_err(),
        BootstrapError::StateBlockMismatch
    );
    let wrong = Checkpoint { root: [3; 32], ..fixture.trusted };
    assert_eq!(
        checked.verify_checkpoint_with_replay(
            &state, &state, &fixture.block, wrong, fixture.now, 4, 1,
        ).unwrap_err(),
        BootstrapError::WrongTrustedRoot
    );
    let SignedBeaconBlock::Phase0(mut block, preset) =
        SignedBeaconBlock::from_ssz(&fixture.block, Fork::Phase0, Preset::Minimal).unwrap()
    else {
        unreachable!()
    };
    block.message.state_root = [7; 32];
    let wrong =
        Checkpoint { root: block.message.hash_tree_root(preset).unwrap(), ..fixture.trusted };
    let changed_block = SignedBeaconBlock::Phase0(block, preset).to_ssz().unwrap();
    assert_eq!(
        checked.verify_checkpoint_with_replay(
            &state, &state, &changed_block, wrong, fixture.now, 4, 1,
        ).unwrap_err(),
        BootstrapError::StateBlockMismatch
    );
    let earlier = self::fixture(false);
    let boundary = earlier.boundary.to_ssz().unwrap();
    assert_eq!(
        verifier(&earlier)
            .verify_checkpoint_with_replay(
                &boundary,
                &boundary,
                &earlier.block,
                earlier.trusted,
                earlier.now,
                4,
                7,
            )
            .unwrap_err(),
        BootstrapError::StateBlockMismatch
    );
}

#[test]
fn identical_input_path_preserves_network_schema_age_and_size_guards() {
    let fixture = identical_fixture();
    let checked = verifier(&fixture);
    let state = fixture.post.to_ssz().unwrap();
    let mut wrong_network = fixture.config.clone();
    wrong_network.genesis_validators_root = [3; 32];
    let wrong = SszCheckpointVerifier::new(
        wrong_network,
        BootstrapDecodeLimits::default(),
        CheckpointBlockFormat::Signed,
    )
    .unwrap();
    assert_eq!(
        wrong
            .verify_checkpoint_with_replay(
                &state,
                &state,
                &fixture.block,
                fixture.trusted,
                fixture.now,
                4,
                1,
            )
            .unwrap_err(),
        BootstrapError::WrongNetwork
    );
    let BeaconState::Phase0(mut changed, preset) = fixture.post.clone() else { unreachable!() };
    changed.fork.current_version[0] ^= 1;
    let changed = changed.to_ssz(preset).unwrap();
    assert_eq!(
        checked
            .verify_checkpoint_with_replay(
                &changed,
                &changed,
                &fixture.block,
                fixture.trusted,
                fixture.now,
                4,
                1,
            )
            .unwrap_err(),
        BootstrapError::StateForkMismatch
    );
    let expired = fixture.config.clock().unwrap().start_of_slot(48).unwrap();
    assert_eq!(
        checked
            .verify_checkpoint_with_replay(
                &state,
                &state,
                &fixture.block,
                fixture.trusted,
                expired,
                4,
                1,
            )
            .unwrap_err(),
        BootstrapError::StaleCheckpoint
    );
    let bounded = SszCheckpointVerifier::new(
        fixture.config.clone(),
        BootstrapDecodeLimits {
            max_state_bytes: state.len() - 1,
            max_block_bytes: fixture.block.len(),
        },
        CheckpointBlockFormat::Signed,
    )
    .unwrap();
    assert_eq!(
        bounded
            .verify_checkpoint_with_replay(
                &state,
                &state,
                &fixture.block,
                fixture.trusted,
                fixture.now,
                4,
                1,
            )
            .unwrap_err(),
        BootstrapError::CheckpointTooLarge
    );
}
