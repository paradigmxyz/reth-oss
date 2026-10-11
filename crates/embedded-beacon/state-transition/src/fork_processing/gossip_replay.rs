//! Bounded immutable-input slot replay without nested whole-state transaction copies.
//! Fork upgrades still allocate the new schema required by the scheduled transition.

use super::{slots_kernel_with_replay, TransitionParameters};
use crate::{
    altair_processing, bellatrix_processing, capella_processing, deneb_processing,
    electra_processing, fulu_processing, gloas_processing,
    phase0_processing::{self, ProcessingError},
    ssz::BeaconState,
};

/// Replay actual empty slots and scheduled upgrades without changing the borrowed original.
/// The returned temporary state is no import, execution or ancestry authority token.
pub fn replay_slots(
    state: &BeaconState,
    target: u64,
    parameters: &TransitionParameters,
    maximum_slots: u64,
) -> Result<BeaconState, ProcessingError> {
    let gap = target
        .checked_sub(state.slot())
        .filter(|gap| *gap != 0)
        .ok_or_else(|| ProcessingError::SlotNotIncreasing { current: state.slot(), target })?;
    if maximum_slots == 0 || gap > maximum_slots {
        return Err(ProcessingError::InvalidParameters("temporary slot replay budget exceeded"))
    }
    let mut replay = state.clone();
    slots_kernel_with_replay::<true>(&mut replay, target, parameters)?;
    Ok(replay)
}

pub(super) fn advance_one_fork_in_place(
    state: &mut BeaconState,
    target: u64,
    parameters: &TransitionParameters,
) -> Result<(), ProcessingError> {
    match state {
        BeaconState::Phase0(value, _) => phase0_processing::process_slots_in_place(
            value,
            target,
            &parameters.gloas.fulu.electra.deneb.capella.bellatrix.altair.phase0,
        ),
        BeaconState::Altair(value, _) => altair_processing::process_slots_in_place(
            value,
            target,
            &parameters.gloas.fulu.electra.deneb.capella.bellatrix.altair,
        ),
        BeaconState::Bellatrix(value, _) => bellatrix_processing::process_slots_in_place(
            value,
            target,
            &parameters.gloas.fulu.electra.deneb.capella.bellatrix,
        ),
        BeaconState::Capella(value, _) => capella_processing::process_slots_in_place(
            value,
            target,
            &parameters.gloas.fulu.electra.deneb.capella,
        ),
        BeaconState::Deneb(value, _) => deneb_processing::process_slots_in_place(
            value,
            target,
            &parameters.gloas.fulu.electra.deneb,
        ),
        BeaconState::Electra(value, _) => electra_processing::process_slots_in_place(
            value,
            target,
            &parameters.gloas.fulu.electra,
        ),
        BeaconState::Fulu(value, _) => {
            fulu_processing::process_slots_in_place(value, target, &parameters.gloas.fulu)
        }
        BeaconState::Gloas(value, _) => {
            gloas_processing::process_slots_in_place(value, target, &parameters.gloas)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ssz::Preset;
    use reth_embedded_beacon_primitives::Fork;

    #[test]
    fn bounded_borrowed_replay_matches_real_fork_boundary_and_preserves_original_on_error() {
        let (state, _) = crate::bellatrix_processing::tests::state_and_keys();
        let state = BeaconState::Bellatrix(state, Preset::Minimal);
        let mut parameters = TransitionParameters::for_preset(Preset::Minimal);
        for activation in &mut parameters.schedule {
            activation.epoch = if activation.fork <= Fork::Bellatrix {
                0
            } else if activation.fork == Fork::Capella {
                1
            } else {
                u64::MAX
            };
        }
        let parameters = TransitionParameters::new(parameters.gloas, parameters.schedule).unwrap();
        let original = state.to_ssz().unwrap();
        let original_root = state.hash_tree_root().unwrap();
        assert!(replay_slots(&state, 8, &parameters, 7).is_err());
        assert_eq!(state.to_ssz().unwrap(), original);
        assert_eq!(state.hash_tree_root().unwrap(), original_root);
        let mut invalid = parameters.clone();
        invalid.schedule[3].version = invalid.schedule[2].version;
        assert!(replay_slots(&state, 8, &invalid, 8).is_err());
        assert_eq!(state.to_ssz().unwrap(), original);
        assert_eq!(state.hash_tree_root().unwrap(), original_root);
        let replay = replay_slots(&state, 8, &parameters, 8).unwrap();
        let mut expected = state.clone();
        super::super::process_slots(&mut expected, 8, &parameters).unwrap();
        assert_eq!(replay.schema_fork(), Fork::Capella);
        assert_eq!(replay.slot(), 8);
        assert_eq!(replay.to_ssz().unwrap(), expected.to_ssz().unwrap());
        assert_eq!(replay.hash_tree_root().unwrap(), expected.hash_tree_root().unwrap());
        assert_eq!(state.to_ssz().unwrap(), original);
    }
    #[test]
    fn late_epoch_failure_discards_temporary_mutations_without_touching_borrowed_input() {
        let (mut state, _) = crate::bellatrix_processing::tests::state_and_keys();
        state.finalized_checkpoint.epoch = u64::MAX;
        let state = BeaconState::Bellatrix(state, Preset::Minimal);
        let mut defaults = TransitionParameters::for_preset(Preset::Minimal);
        for activation in &mut defaults.schedule {
            activation.epoch = if activation.fork <= Fork::Bellatrix { 0 } else { u64::MAX };
        }
        let parameters = TransitionParameters::new(defaults.gloas, defaults.schedule).unwrap();
        let original = state.to_ssz().unwrap();
        let mut temporary = state.clone();
        assert!(matches!(
            advance_one_fork_in_place(&mut temporary, 16, &parameters),
            Err(ProcessingError::Arithmetic("finality delay"))
        ));
        assert!(temporary.slot() > state.slot());
        assert_ne!(temporary.to_ssz().unwrap(), original);
        assert!(matches!(
            replay_slots(&state, 16, &parameters, 16),
            Err(ProcessingError::Arithmetic("finality delay"))
        ));
        assert_eq!(state.to_ssz().unwrap(), original);
    }
}
