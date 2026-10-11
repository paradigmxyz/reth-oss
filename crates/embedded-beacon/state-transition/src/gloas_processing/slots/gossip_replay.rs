//! Bounded Gloas replay from an immutable retained state with one temporary copy.

use super::{
    super::checked_constants, slots_kernel, BeaconState, GloasParameters, ProcessingError,
};

/// Replay complete Gloas slots and epochs without replacing the borrowed retained state.
/// This temporary result grants no graph, payload, execution or availability authority.
pub fn replay_slots(
    state: &BeaconState,
    target: u64,
    parameters: &GloasParameters,
    maximum_slots: u64,
) -> Result<BeaconState, ProcessingError> {
    let gap = target
        .checked_sub(state.slot)
        .filter(|gap| *gap != 0)
        .ok_or(ProcessingError::SlotNotIncreasing { current: state.slot, target })?;
    if maximum_slots == 0 || gap > maximum_slots {
        return Err(ProcessingError::InvalidParameters(
            "temporary Gloas slot replay budget exceeded",
        ))
    }
    let constants = checked_constants(state, parameters)?;
    let mut replay = state.clone();
    slots_kernel(&mut replay, target, parameters, &constants)?;
    checked_constants(&replay, parameters)?;
    Ok(replay)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{gloas_processing, ssz::Preset};

    #[test]
    fn bounded_gloas_epoch_replay_matches_complete_roots_without_changing_original() {
        let (pre, _, fulu) = crate::fulu_processing::tests::state_and_keys();
        let mut parameters = GloasParameters::for_preset(Preset::Minimal);
        parameters.fulu = fulu;
        parameters.fork_epoch = 0;
        let state = gloas_processing::upgrade_to_gloas(&pre, &parameters).unwrap();
        let original = state.to_ssz(Preset::Minimal).unwrap();
        let original_root = state.hash_tree_root(Preset::Minimal).unwrap();
        assert!(replay_slots(&state, 8, &parameters, 7).is_err());
        assert_eq!(state.to_ssz(Preset::Minimal).unwrap(), original);
        assert_eq!(state.hash_tree_root(Preset::Minimal).unwrap(), original_root);
        let replay = replay_slots(&state, 8, &parameters, 8).unwrap();
        let mut expected = state.clone();
        gloas_processing::process_slots(&mut expected, 8, &parameters).unwrap();
        assert_eq!(
            replay.to_ssz(Preset::Minimal).unwrap(),
            expected.to_ssz(Preset::Minimal).unwrap()
        );
        assert_eq!(
            replay.hash_tree_root(Preset::Minimal).unwrap(),
            expected.hash_tree_root(Preset::Minimal).unwrap()
        );
        assert_eq!(state.to_ssz(Preset::Minimal).unwrap(), original);
    }
}
