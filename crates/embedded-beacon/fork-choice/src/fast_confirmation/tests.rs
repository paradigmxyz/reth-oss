//! Confirmation arithmetic, schedule binding and atomic observation regressions.

use super::*;
use crate::Store;
use reth_embedded_beacon_primitives::Fork;
use reth_embedded_beacon_state_transition::ssz::BeaconBlock;

fn anchor(parameters: StoreParameters) -> Store {
    let base =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("testdata/official-anchors/altair");
    let state = BeaconState::from_ssz(
        &std::fs::read(base.join("anchor_state.ssz")).unwrap(),
        Fork::Altair,
        Preset::Minimal,
    )
    .unwrap();
    let block = BeaconBlock::from_ssz(
        &std::fs::read(base.join("anchor_block.ssz")).unwrap(),
        Fork::Altair,
        Preset::Minimal,
    )
    .unwrap();
    Store::get_store(state, block, parameters).unwrap()
}

#[test]
fn committee_estimate_respects_full_epochs_and_cross_epoch_overlap() {
    for (start, end, covered, estimate) in [
        (0, 7, true, 8000),
        (8, 15, true, 8000),
        (3, 5, false, 3000),
        (3, 10, false, 7035),
        (7, 8, false, 2010),
        (4, 11, false, 6030),
        (7, 14, false, 8040),
        (3, 16, true, 8000),
    ] {
        assert_eq!(is_full_validator_set_covered(start, end, 8).unwrap(), covered);
        assert_eq!(estimate_committee_weight_between_slots(8000, start, end, 8).unwrap(), estimate);
    }
    assert_eq!(estimate_committee_weight_between_slots(8000, 7, 6, 8).unwrap(), 0);
}

#[test]
fn committee_arithmetic_rejects_invalid_presets_and_overflow() {
    assert!(matches!(is_full_validator_set_covered(0, 1, 0), Err(StoreError::InvalidParameters)));
    assert!(matches!(
        estimate_committee_weight_between_slots(10, 0, 1, 0),
        Err(StoreError::InvalidParameters)
    ));
    assert!(matches!(
        is_full_validator_set_covered(u64::MAX, u64::MAX, 8),
        Err(StoreError::Overflow)
    ));
    assert!(matches!(
        estimate_committee_weight_between_slots(u64::MAX, 1, 16_382, 8192),
        Err(StoreError::Overflow)
    ));
    assert_eq!(estimate_committee_weight_between_slots(u64::MAX, 0, 7, 8).unwrap(), u64::MAX);
}

#[test]
fn actual_anchor_observations_are_once_per_slot_and_atomic_at_deadline() {
    let mut graph = anchor(StoreParameters::for_preset(Preset::Minimal));
    let mut confirmation = FastConfirmation::new(&graph, Parameters::default()).unwrap();
    let trusted = graph.checkpoints().1;
    assert_eq!(confirmation.snapshot().confirmed_root, trusted.root);
    confirmation.on_fast_confirmation(&graph).unwrap();
    let before = confirmation.snapshot();
    assert!(matches!(
        confirmation.on_fast_confirmation(&graph),
        Err(StoreError::InvalidParameters)
    ));
    assert_eq!(confirmation.snapshot(), before);
    graph.on_tick(graph.genesis_time_ms() + 6000).unwrap();
    confirmation.on_fast_confirmation(&graph).unwrap();
    let before = confirmation.snapshot();
    graph.on_tick(graph.genesis_time_ms() + 14_000).unwrap();
    assert!(matches!(
        confirmation.on_fast_confirmation(&graph),
        Err(StoreError::InvalidParameters)
    ));
    assert_eq!(confirmation.snapshot(), before);
    assert!(matches!(
        confirmation.safe_execution_block_hash(&graph),
        Err(StoreError::InvalidParameters)
    ));
}

#[test]
fn observations_cannot_move_to_another_graph_policy() {
    let parameters = StoreParameters::for_preset(Preset::Minimal);
    let graph = anchor(parameters);
    let mut confirmation = FastConfirmation::new(&graph, Parameters::default()).unwrap();
    let mut changed = parameters;
    changed.proposer_score_boost += 1;
    let different = anchor(changed);
    let before = confirmation.snapshot();
    assert!(matches!(
        confirmation.on_fast_confirmation(&different),
        Err(StoreError::AnchorMismatch)
    ));
    assert_eq!(confirmation.snapshot(), before);
    assert!(matches!(
        FastConfirmation::new(
            &graph,
            Parameters { byzantine_threshold: 26, ..Parameters::default() }
        ),
        Err(StoreError::InvalidParameters)
    ));
    assert!(matches!(
        FastConfirmation::new(
            &graph,
            Parameters { max_committee_slots: 0, ..Parameters::default() }
        ),
        Err(StoreError::InvalidParameters)
    ));
}

#[test]
fn same_genesis_and_clock_cannot_replace_the_bound_fork_kernels() {
    let parameters = StoreParameters::for_preset(Preset::Minimal);
    let mut graph = anchor(parameters);
    let defaults = TransitionParameters::for_preset(Preset::Minimal);
    let mut schedule = defaults.schedule;
    schedule[1].epoch = 0;
    let transition = TransitionParameters::new(defaults.gloas, schedule).unwrap();
    graph.set_transition_parameters(transition.clone()).unwrap();
    let mut confirmation = FastConfirmation::new(&graph, Parameters::default()).unwrap();
    let mut modified = transition;
    modified.gloas.min_builder_withdrawability_delay += 1;
    let mut different = anchor(parameters);
    different.set_transition_parameters(modified).unwrap();
    let before = confirmation.snapshot();
    assert!(matches!(
        confirmation.on_fast_confirmation(&different),
        Err(StoreError::AnchorMismatch)
    ));
    assert_eq!(confirmation.snapshot(), before);
}

#[test]
fn actual_observation_read_plan_is_pure_and_preserves_deadline_and_once_per_slot_rules() {
    let mut graph = anchor(StoreParameters::for_preset(Preset::Minimal));
    let mut confirmation = FastConfirmation::new(&graph, Parameters::default()).unwrap();
    let before = confirmation.snapshot();
    let reads = confirmation.checkpoint_states_for_update(&graph).unwrap();
    assert_eq!(confirmation.snapshot(), before);
    assert!(reads.contains(&graph.checkpoints().1));
    confirmation.on_fast_confirmation(&graph).unwrap();
    let after = confirmation.snapshot();
    assert!(matches!(
        confirmation.checkpoint_states_for_update(&graph),
        Err(StoreError::InvalidParameters)
    ));
    assert_eq!(confirmation.snapshot(), after);
    graph.on_tick(graph.genesis_time_ms() + 14_000).unwrap();
    assert!(matches!(
        confirmation.checkpoint_states_for_update(&graph),
        Err(StoreError::InvalidParameters)
    ));
    assert_eq!(confirmation.snapshot(), after);
}
