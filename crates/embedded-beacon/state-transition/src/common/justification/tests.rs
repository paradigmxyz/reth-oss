//! Compare immutable checkpoint derivation with the independently implemented mutable kernel.

use super::*;
use crate::{
    fulu_processing, gloas_processing,
    ssz::{Preset, Ssz},
};

#[test]
fn immutable_fulu_and_gloas_checkpoints_match_full_mutable_kernels() {
    let (base, _, parameters) = fulu_processing::tests::state_and_keys();
    let mut gloas = gloas_processing::GloasParameters::for_preset(Preset::Minimal);
    gloas.fulu = parameters.clone();
    gloas.fork_epoch = 0;
    let gloas_base = gloas_processing::upgrade_to_gloas(&base, &gloas).unwrap();
    for epoch in [0u64, 1, 2, 3, 4, 8] {
        for bits in 0..16u8 {
            for participation in [0u8, 2, 7] {
                let mut state = base.clone();
                state.slot = epoch * 8 + 1;
                state.previous_justified_checkpoint =
                    Checkpoint { epoch: epoch.saturating_sub(2), root: [1; 32] };
                state.current_justified_checkpoint =
                    Checkpoint { epoch: epoch.saturating_sub(1), root: [2; 32] };
                state
                    .block_roots
                    .values
                    .iter_mut()
                    .enumerate()
                    .for_each(|(index, root)| *root = [index as u8; 32]);
                state.justification_bits =
                    crate::ssz::PresetBits::from_ssz(&[bits], Preset::Minimal).unwrap();
                state.previous_epoch_participation.values.fill(participation);
                state.current_epoch_participation.values.fill(participation);
                state.validators.values[0].slashed = bits & 1 != 0;
                state.validators.values[1].exit_epoch = epoch;
                let before = state.clone();
                let expected =
                    fulu_processing::get_unrealized_checkpoints(&state, &parameters).unwrap();
                let mut mutable = state.clone();
                fulu_processing::process_justification_and_finalization(&mut mutable, &parameters)
                    .unwrap();
                assert_eq!(
                    expected,
                    (mutable.current_justified_checkpoint, mutable.finalized_checkpoint)
                );
                assert_eq!(state, before);

                let mut state_gloas = gloas_base.clone();
                state_gloas.slot = state.slot;
                state_gloas.previous_justified_checkpoint = state.previous_justified_checkpoint;
                state_gloas.current_justified_checkpoint = state.current_justified_checkpoint;
                state_gloas.block_roots.values.clone_from(&state.block_roots.values);
                state_gloas.justification_bits = state.justification_bits.clone();
                state_gloas
                    .previous_epoch_participation
                    .values
                    .clone_from(&state.previous_epoch_participation.values);
                state_gloas
                    .current_epoch_participation
                    .values
                    .clone_from(&state.current_epoch_participation.values);
                state_gloas.validators.values.clone_from(&state.validators.values);
                let state = state_gloas;
                let before = state.clone();
                let expected =
                    gloas_processing::get_unrealized_checkpoints(&state, &gloas).unwrap();
                let mut mutable = state.clone();
                gloas_processing::process_justification_and_finalization(&mut mutable, &gloas)
                    .unwrap();
                assert_eq!(
                    expected,
                    (mutable.current_justified_checkpoint, mutable.finalized_checkpoint)
                );
                assert_eq!(state, before);
            }
        }
    }
}

#[test]
fn immutable_derivation_keeps_complete_shapes_and_balance_overflow_checks() {
    let (mut state, _, parameters) = fulu_processing::tests::state_and_keys();
    state.slot = 17;
    state.previous_epoch_participation.values.fill(2);
    state.current_epoch_participation.values.fill(2);
    state.validators.values[0].effective_balance = u64::MAX;
    let before = state.clone();
    let error = fulu_processing::get_unrealized_checkpoints(&state, &parameters).unwrap_err();
    let mut mutable = state.clone();
    let expected =
        fulu_processing::process_justification_and_finalization(&mut mutable, &parameters)
            .unwrap_err();
    assert_eq!(format!("{error:?}"), format!("{expected:?}"));
    assert_eq!(state, before);
    assert_eq!(mutable, before);

    state.current_epoch_participation.values.pop();
    assert!(matches!(
        fulu_processing::get_unrealized_checkpoints(&state, &parameters),
        Err(ProcessingError::InvalidState(_))
    ));
    state = before;
    state.block_roots.values.pop();
    assert!(fulu_processing::get_unrealized_checkpoints(&state, &parameters).is_err());
}
