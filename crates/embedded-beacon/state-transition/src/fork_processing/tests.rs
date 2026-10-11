//! Scheduled fork boundaries, strict metadata binding and atomic failure regressions.

use super::*;
use crate::ssz::electra;

fn parameters(fulu_epoch: u64, gloas_epoch: u64) -> TransitionParameters {
    let mut raw = gloas_processing::GloasParameters::for_preset(Preset::Minimal);
    raw.fulu.blob_schedule.clear();
    let schedule = FORKS
        .iter()
        .copied()
        .enumerate()
        .map(|(i, fork)| ForkActivation {
            fork,
            epoch: if i < 6 {
                0
            } else if i == 6 {
                fulu_epoch
            } else {
                gloas_epoch
            },
            version: [i as u8, 0, 0, 1],
        })
        .collect();
    TransitionParameters::new(raw, schedule).unwrap()
}
fn initial() -> electra::BeaconState {
    crate::electra_processing::tests::state_and_keys().0
}

#[test]
fn equal_epoch_upgrades_follow_old_epoch_and_match_explicit_full_states() {
    let parameters = parameters(1, 1);
    let pre = initial();
    let mut expected = pre.clone();
    electra_processing::process_slots(&mut expected, 8, &parameters.gloas.fulu.electra).unwrap();
    let expected = fulu_processing::upgrade_to_fulu(&expected, &parameters.gloas.fulu).unwrap();
    let mut expected = gloas_processing::upgrade_to_gloas(&expected, &parameters.gloas).unwrap();
    gloas_processing::process_slots(&mut expected, 16, &parameters.gloas).unwrap();
    let mut actual = BeaconState::Electra(pre, Preset::Minimal);
    process_slots(&mut actual, 16, &parameters).unwrap();
    assert_eq!(actual, BeaconState::Gloas(expected, Preset::Minimal));
    assert_eq!(actual.fork_info().previous_version, [6, 0, 0, 1]);
    assert_eq!(actual.fork_info().epoch, 1);
    assert_eq!(parameters.fork_at_slot(7).unwrap(), Fork::Electra);
    assert_eq!(parameters.fork_at_slot(8).unwrap(), Fork::Gloas);
}

#[test]
fn distinct_boundaries_cache_roots_in_the_actual_active_schema() {
    let parameters = parameters(1, 2);
    let mut expected = initial();
    electra_processing::process_slots(&mut expected, 8, &parameters.gloas.fulu.electra).unwrap();
    let mut expected = fulu_processing::upgrade_to_fulu(&expected, &parameters.gloas.fulu).unwrap();
    fulu_processing::process_slots(&mut expected, 16, &parameters.gloas.fulu).unwrap();
    let mut expected = gloas_processing::upgrade_to_gloas(&expected, &parameters.gloas).unwrap();
    gloas_processing::process_slots(&mut expected, 17, &parameters.gloas).unwrap();
    let mut actual = BeaconState::Electra(initial(), Preset::Minimal);
    process_slots(&mut actual, 17, &parameters).unwrap();
    assert_eq!(actual, BeaconState::Gloas(expected, Preset::Minimal));
    assert_eq!(parameters.fork_at_slot(15).unwrap(), Fork::Fulu);
    assert_eq!(parameters.fork_at_slot(16).unwrap(), Fork::Gloas);
}

#[test]
fn an_explicit_pending_boundary_upgrades_without_replaying_the_last_old_slot() {
    let parameters = parameters(1, 1);
    let mut pre = initial();
    electra_processing::process_slots(&mut pre, 8, &parameters.gloas.fulu.electra).unwrap();
    let roots = pre.state_roots.clone();
    let mut actual = BeaconState::Electra(pre, Preset::Minimal);
    upgrade_state_at_slot(&mut actual, &parameters).unwrap();
    let BeaconState::Gloas(value, _) = actual else { panic!("scheduled Gloas not selected") };
    assert_eq!(value.state_roots, roots);
    assert_eq!(value.slot, 8);
}

#[test]
fn stale_versions_missed_boundaries_and_inconsistent_presets_fail_atomically() {
    let parameters = parameters(1, 2);
    for variant in 0..4 {
        let mut pre = initial();
        match variant {
            0 => pre.fork.current_version = [99; 4],
            1 => pre.fork.epoch = 1,
            2 => pre.slot = 9,
            _ => {}
        }
        let preset = if variant == 3 { Preset::Mainnet } else { Preset::Minimal };
        let mut state = BeaconState::Electra(pre, preset);
        let before = state.clone();
        assert!(process_slots(&mut state, 17, &parameters).is_err());
        assert_eq!(state, before);
    }
}

#[test]
fn failed_late_epoch_after_upgrade_does_not_publish_any_intermediate_fork() {
    let parameters = parameters(1, 1);
    let mut pre = initial();
    for validator in &mut pre.validators.values {
        validator.slashed = true;
    }
    let mut state = BeaconState::Electra(pre, Preset::Minimal);
    let before = state.clone();
    assert!(process_slots(&mut state, 16, &parameters).is_err());
    assert_eq!(state, before);
}

#[test]
fn ordered_equal_epochs_and_disabled_suffixes_are_checked() {
    let good = parameters(1, 2);
    for variant in 0..4 {
        let mut bad = good.clone();
        match variant {
            0 => bad.schedule.swap(3, 4),
            1 => bad.schedule[7].epoch = 0,
            2 => bad.schedule[7].version = bad.schedule[6].version,
            _ => bad.gloas.fulu.electra_fork_epoch = 10,
        }
        assert!(bad.validate().is_err());
    }
    let mut raw = gloas_processing::GloasParameters::for_preset(Preset::Minimal);
    raw.fulu.blob_schedule.clear();
    let mut schedule = good.schedule;
    schedule[6].epoch = u64::MAX;
    schedule[7].epoch = 2;
    assert!(TransitionParameters::new(raw, schedule).is_err());
    for preset in [Preset::Mainnet, Preset::Minimal] {
        let default = TransitionParameters::for_preset(preset);
        default.validate().unwrap();
        assert_ne!(default.fork_at_slot(u64::MAX).unwrap(), Fork::Gloas);
    }
}

#[test]
fn unchanged_upgrade_still_validates_schedule_metadata_and_preserves_state() {
    let parameters = parameters(1, 2);
    let mut state = BeaconState::Electra(initial(), Preset::Minimal);
    let before = state.clone();
    upgrade_state_at_slot(&mut state, &parameters).unwrap();
    assert_eq!(state, before);
    let BeaconState::Electra(value, _) = &mut state else { panic!("unchanged Electra expected") };
    value.fork.current_version = [99; 4];
    let invalid = state.clone();
    assert!(matches!(
        upgrade_state_at_slot(&mut state, &parameters),
        Err(ProcessingError::InvalidState(
            "state fork versions or epoch differ from trusted schedule"
        ))
    ));
    assert_eq!(state, invalid);
}

mod owned;
