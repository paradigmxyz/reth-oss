//! Genuine signed-deposit genesis, complete native upgrades and signed forward-import controls.

use super::*;
use crate::{
    gossip_tests::{gloas_store_and_keys, phase0_signed_child, store_and_keys},
    BootstrapDecodeLimits, CheckpointBlockFormat, SszCheckpointVerifier,
};
use blst::min_pk::SecretKey;
use reth_embedded_beacon_config::{BlobParameters, ForkActivation, ForkSchedule};
use reth_embedded_beacon_fork_choice::{Store, StoreParameters};
use reth_embedded_beacon_primitives::Checkpoint;
use reth_embedded_beacon_state_transition::fork_processing::TransitionParameters;

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
pub(crate) fn genuine(
    fork: Fork,
) -> (ChainConfig, BeaconState, Vec<SecretKey>, TransitionParameters) {
    let (mut state, keys, parameters) = if fork == Fork::Gloas {
        let (store, keys, parameters) = gloas_store_and_keys();
        let root = store.get_head().unwrap().root;
        (
            BeaconState::Gloas(store.block_state(root).unwrap().clone(), Preset::Minimal),
            keys,
            parameters,
        )
    } else {
        let (store, keys, parameters) = store_and_keys(fork);
        let root = store.get_head().unwrap().root;
        (store.block_state(root).unwrap().clone(), keys, parameters)
    };
    // The shared gossip anchor includes an Eth1 vote in its empty body. Genuine genesis has
    // the unsigned initial body, before any block vote, and the original slot-zero header binds it.
    let body_root = unsigned_genesis_block(&state, Preset::Minimal, [0; 32])
        .unwrap()
        .header()
        .unwrap()
        .body_root;
    let previous = state.fork_info().previous_version;
    normalize_expected_header(&mut state, body_root, previous);
    let config = ChainConfig {
        name: "genuine-genesis-control".into(),
        preset: ConfigPreset::Minimal,
        chain_id: 321,
        genesis_time: state.genesis_time(),
        genesis_validators_root: state.genesis_validators_root(),
        seconds_per_slot: 6,
        slots_per_epoch: 8,
        forks: ForkSchedule::new(
            parameters
                .schedule
                .iter()
                .map(|active| ForkActivation {
                    fork: active.fork,
                    epoch: active.epoch,
                    version: active.version,
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
    };
    (config, state, keys, parameters)
}
/// Genuine Phase0 genesis whose complete native upgrades activate at epoch one.
pub(crate) fn genuine_phase0_gloas(
) -> (ChainConfig, BeaconState, Vec<SecretKey>, TransitionParameters) {
    let (mut config, state, keys, original) = genuine(Fork::Phase0);
    let mut schedule = original.schedule;
    for activation in &mut schedule {
        if activation.fork > Fork::Phase0 {
            activation.epoch = 1;
        }
    }
    let parameters = TransitionParameters::new(original.gloas, schedule).unwrap();
    config.forks = ForkSchedule::new(
        parameters
            .schedule
            .iter()
            .map(|active| ForkActivation {
                fork: active.fork,
                epoch: active.epoch,
                version: active.version,
            })
            .collect(),
    )
    .unwrap();
    (config, state, keys, parameters)
}
fn verify(config: ChainConfig, state: &BeaconState) -> Result<VerifiedGenesis, GenesisError> {
    VerifiedGenesis::authenticate(
        config,
        state.to_ssz().unwrap().into(),
        BootstrapDecodeLimits::default(),
    )
}

#[test]
fn signed_deposit_genesis_and_real_upgrades_authenticate_all_eight_original_schemas() {
    for fork in FORKS {
        let (config, state, _, _) = genuine(fork);
        let original = state.to_ssz().unwrap();
        let proof = VerifiedGenesis::authenticate(
            config.clone(),
            Arc::from(original.clone()),
            BootstrapDecodeLimits::default(),
        )
        .unwrap();
        assert_eq!(proof.fork(), fork);
        assert_eq!(proof.state_root(), state.hash_tree_root().unwrap());
        assert_eq!(&*proof.original_state_ssz(), original);
        let block = BeaconBlock::from_ssz(&proof.block_ssz(), fork, Preset::Minimal).unwrap();
        assert_eq!(block.hash_tree_root().unwrap(), proof.block_root());
        let header = block.header().unwrap();
        assert_eq!(header.slot, 0);
        assert_eq!(header.parent_root, [0; 32]);
        assert_eq!(header.state_root, proof.state_root());
        let (typed_state, typed_block) = proof.into_import_inputs(&config).unwrap();
        assert_eq!(typed_state.as_ref(), &state);
        assert_eq!(typed_block.as_ref(), &block);
    }
}
#[test]
fn exact_epoch_zero_store_imports_a_genuine_signed_child_without_checkpoint_conversion() {
    let (config, state, keys, parameters) = genuine(Fork::Phase0);
    let proof = verify(config.clone(), &state).unwrap();
    let (state, block) = proof.into_import_inputs(&config).unwrap();
    let mut store = Store::get_store(
        state.as_ref().clone(),
        block.as_ref().clone(),
        StoreParameters::for_preset(Preset::Minimal),
    )
    .unwrap();
    store.set_transition_parameters(parameters.clone()).unwrap();
    assert_eq!(
        store.checkpoints().1,
        Checkpoint { epoch: 0, root: block.hash_tree_root().unwrap() }
    );
    let signed = phase0_signed_child(&store, &keys, &parameters);
    store.on_tick(config.clock().unwrap().start_of_slot(1).unwrap()).unwrap();
    store
        .on_block(&reth_embedded_beacon_state_transition::ssz::SignedBeaconBlock::Phase0(
            signed.clone(),
            Preset::Minimal,
        ))
        .unwrap();
    assert_eq!(
        store.get_head().unwrap().root,
        signed.message.hash_tree_root(Preset::Minimal).unwrap()
    );
    assert_eq!(store.block_state(store.get_head().unwrap().root).unwrap().slot(), 1);
}
#[test]
fn ancient_genesis_remains_genesis_and_the_same_inputs_fail_fresh_checkpoint_age() {
    let (config, state, _, _) = genuine(Fork::Phase0);
    let proof = verify(config.clone(), &state).unwrap();
    let block_ssz = proof.block_ssz();
    let root = proof.block_root();
    let now = config.clock().unwrap().start_of_slot(8 * 1_000_000).unwrap();
    assert!(matches!(
        SszCheckpointVerifier::new(
            config.clone(),
            BootstrapDecodeLimits::default(),
            CheckpointBlockFormat::Unsigned
        )
        .unwrap()
        .verify_checkpoint(
            &proof.original_state_ssz(),
            &block_ssz,
            Checkpoint { epoch: 0, root },
            now,
            2
        ),
        Err(crate::BootstrapError::StaleCheckpoint)
    ));
    let (state, block) = proof.into_import_inputs(&config).unwrap();
    assert_eq!(state.slot(), 0);
    assert_eq!(block.header().unwrap().slot, 0);
}
#[test]
fn wrong_genesis_time_validator_identity_and_nonzero_slot_are_rejected_all_forks() {
    for fork in FORKS {
        let (config, state, _, _) = genuine(fork);
        let mut wrong = config.clone();
        wrong.genesis_time += 1;
        assert!(matches!(verify(wrong, &state), Err(GenesisError::Network)));
        let mut wrong = config;
        wrong.genesis_validators_root = [99; 32];
        assert!(matches!(verify(wrong, &state), Err(GenesisError::Network)));
        let (config, mut state, _, _) = genuine(fork);
        macro_rules! slot {
            ($state:expr) => {
                $state.slot = 1
            };
        }
        match &mut state {
            BeaconState::Phase0(s, _) => slot!(s),
            BeaconState::Altair(s, _) => slot!(s),
            BeaconState::Bellatrix(s, _) => slot!(s),
            BeaconState::Capella(s, _) => slot!(s),
            BeaconState::Deneb(s, _) => slot!(s),
            BeaconState::Electra(s, _) => slot!(s),
            BeaconState::Fulu(s, _) => slot!(s),
            BeaconState::Gloas(s, _) => slot!(s),
        }
        assert!(matches!(verify(config, &state), Err(GenesisError::Network)));
    }
}
#[test]
fn complete_initial_state_fields_and_header_body_cannot_be_swapped_for_all_forks() {
    for fork in FORKS {
        for defect in 0..4 {
            let (config, mut state, _, _) = genuine(fork);
            macro_rules! corrupt {
                ($state:expr) => {{
                    match defect {
                        0 => $state.block_roots.values[0] = [71; 32],
                        1 => $state.slashings.values[0] = 1,
                        2 => $state.eth1_deposit_index = 0,
                        _ => $state.latest_block_header.body_root = [81; 32],
                    }
                }};
            }
            match &mut state {
                BeaconState::Phase0(s, _) => corrupt!(s),
                BeaconState::Altair(s, _) => corrupt!(s),
                BeaconState::Bellatrix(s, _) => corrupt!(s),
                BeaconState::Capella(s, _) => corrupt!(s),
                BeaconState::Deneb(s, _) => corrupt!(s),
                BeaconState::Electra(s, _) => corrupt!(s),
                BeaconState::Fulu(s, _) => corrupt!(s),
                BeaconState::Gloas(s, _) => corrupt!(s),
            }
            assert!(matches!(verify(config, &state), Err(GenesisError::CanonicalState)));
        }
    }
}
#[test]
fn authenticated_registry_root_and_real_bls_keys_cannot_be_replaced_by_declared_metadata() {
    for duplicate in [false, true] {
        let (mut config, mut state, _, _) = genuine(Fork::Phase0);
        let BeaconState::Phase0(s, _) = &mut state else { unreachable!() };
        s.validators.values[0].pubkey =
            if duplicate { s.validators.values[1].pubkey } else { [0; 48] };
        s.genesis_validators_root = s.validators.hash_tree_root(Preset::Minimal).unwrap();
        config.genesis_validators_root = s.genesis_validators_root;
        assert!(matches!(verify(config, &state), Err(GenesisError::Registry)));
    }
    let (config, mut state, _, _) = genuine(Fork::Gloas);
    let BeaconState::Gloas(s, _) = &mut state else { unreachable!() };
    s.validators.values[0].withdrawal_credentials = [77; 32];
    assert!(matches!(verify(config, &state), Err(GenesisError::Registry)));
}
#[test]
fn whole_native_sync_committee_lookahead_and_gloas_bid_defaults_are_verified() {
    let (config, mut state, _, _) = genuine(Fork::Altair);
    let BeaconState::Altair(s, _) = &mut state else { unreachable!() };
    s.current_sync_committee.aggregate_pubkey = [0; 48];
    assert!(matches!(verify(config, &state), Err(GenesisError::CanonicalState)));
    let (config, mut state, _, _) = genuine(Fork::Fulu);
    let BeaconState::Fulu(s, _) = &mut state else { unreachable!() };
    s.proposer_lookahead.values[0] ^= 1;
    assert!(matches!(verify(config, &state), Err(GenesisError::CanonicalState)));
    let (config, mut state, _, _) = genuine(Fork::Gloas);
    let BeaconState::Gloas(s, _) = &mut state else { unreachable!() };
    s.latest_execution_payload_bid.block_hash = [41; 32];
    assert!(matches!(verify(config, &state), Err(GenesisError::CanonicalState)));
}
#[test]
fn wrong_fork_records_and_preset_never_select_a_different_genesis_schema() {
    for fork in FORKS {
        let (config, mut state, _, _) = genuine(fork);
        let previous = [98, 0, 0, 0];
        let body_root = state.latest_block_header().body_root;
        normalize_expected_header(&mut state, body_root, previous);
        assert!(matches!(verify(config, &state), Err(GenesisError::Fork)));
    }
    let (mut config, state, _, _) = genuine(Fork::Phase0);
    config.preset = ConfigPreset::Mainnet;
    config.slots_per_epoch = 32;
    assert!(matches!(verify(config, &state), Err(GenesisError::Schema(_))));
}
#[test]
fn original_input_and_unsigned_body_bounds_apply_before_a_sealed_anchor_is_issued() {
    let (config, state, _, _) = genuine(Fork::Phase0);
    let bytes: Arc<[u8]> = state.to_ssz().unwrap().into();
    for limits in [
        BootstrapDecodeLimits { max_state_bytes: 1, max_block_bytes: 10_000 },
        BootstrapDecodeLimits { max_state_bytes: 10_000_000, max_block_bytes: 1 },
        BootstrapDecodeLimits { max_state_bytes: 0, max_block_bytes: 10_000 },
    ] {
        assert!(matches!(
            VerifiedGenesis::authenticate(config.clone(), bytes.clone(), limits),
            Err(GenesisError::Bounds)
        ));
    }
    let mut malformed = bytes.to_vec();
    malformed.pop();
    assert!(matches!(
        VerifiedGenesis::authenticate(config, malformed.into(), BootstrapDecodeLimits::default()),
        Err(GenesisError::Schema(_))
    ));
}
#[test]
fn consuming_sealed_originals_rechecks_exact_config_and_preserves_original_bytes() {
    let (config, state, _, _) = genuine(Fork::Phase0);
    let proof = verify(config.clone(), &state).unwrap();
    let original = proof.original_state_ssz();
    let mut wrong = config.clone();
    wrong.chain_id += 1;
    assert!(matches!(proof.clone().into_import_inputs(&wrong), Err(GenesisError::Network)));
    let (actual, block) = proof.into_import_inputs(&config).unwrap();
    assert_eq!(actual.to_ssz().unwrap(), &*original);
    assert_eq!(block.header().unwrap().state_root, actual.hash_tree_root().unwrap());
}
