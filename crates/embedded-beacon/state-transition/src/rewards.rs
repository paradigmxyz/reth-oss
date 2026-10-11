//! Shared immutable participation reward components for every fork from Altair through Gloas.
//! These are the same kernels used during epoch processing. Complete typed encodings are
//! checked by public queries; no state projection, registry clone or state mutation is needed.

use crate::{
    altair_processing::AltairParameters,
    integer_squareroot,
    phase0_processing::{add, is_active, mul, sub, Constants, ProcessingError, RewardDeltas},
    ssz::{self, phase0::Validator, Preset},
};

const INCREMENT: u64 = 1_000_000_000;
const FLAG_WEIGHTS: [u64; 3] = [14, 26, 14];

mod sealed {
    pub trait Sealed {}
}

/// Immutable data shared by the participation reward rules, sealed to complete fork schemas.
pub trait ParticipationRewardState: sealed::Sealed {
    /// Exact trusted processing parameters associated with this state's fork.
    type Parameters;
    /// Current beacon slot.
    fn reward_slot(&self) -> u64;
    /// Borrowed validator registry, preserving each validator's actual effective balance.
    fn reward_validators(&self) -> &[Validator];
    /// Current participation flags, also used for the previous epoch at genesis.
    fn reward_current_participation(&self) -> &[u8];
    /// Previous participation flags.
    fn reward_previous_participation(&self) -> &[u8];
    /// Per-validator inactivity scores.
    fn reward_inactivity_scores(&self) -> &[u64];
    /// Actual balance-list length for registry-indexed shape checks.
    fn reward_balances_len(&self) -> usize;
    /// Finalized checkpoint epoch used by the inactivity leak threshold.
    fn reward_finalized_epoch(&self) -> u64;
    /// Validate the complete actual fork SSZ encoding, including all extra fields.
    fn validate_reward_encoding(&self, preset: Preset) -> Result<(), ProcessingError>;
    /// Derive the fork's reward constants after validating the trusted configuration.
    fn reward_configuration(
        parameters: &Self::Parameters,
    ) -> Result<RewardConfiguration, ProcessingError>;
}
/// Internal checked constants returned by the sealed reward state implementations.
#[doc(hidden)]
#[derive(Clone, Copy, Debug)]
pub struct RewardConfiguration {
    preset: Preset,
    slots_per_epoch: u64,
    inactivity_score_bias: u64,
    inactivity_penalty_quotient: u64,
}

macro_rules! reward_state {
    ($state:path,$parameters:path,$inherited:expr,$quotient:expr) => {
        impl sealed::Sealed for $state {}
        impl ParticipationRewardState for $state {
            type Parameters = $parameters;
            fn reward_slot(&self) -> u64 {
                self.slot
            }
            fn reward_validators(&self) -> &[Validator] {
                &self.validators.values
            }
            fn reward_current_participation(&self) -> &[u8] {
                &self.current_epoch_participation.values
            }
            fn reward_previous_participation(&self) -> &[u8] {
                &self.previous_epoch_participation.values
            }
            fn reward_inactivity_scores(&self) -> &[u64] {
                &self.inactivity_scores.values
            }
            fn reward_balances_len(&self) -> usize {
                self.balances.values.len()
            }
            fn reward_finalized_epoch(&self) -> u64 {
                self.finalized_checkpoint.epoch
            }
            fn validate_reward_encoding(&self, preset: Preset) -> Result<(), ProcessingError> {
                self.hash_tree_root(preset).map(|_| ()).map_err(ProcessingError::from)
            }
            fn reward_configuration(
                parameters: &Self::Parameters,
            ) -> Result<RewardConfiguration, ProcessingError> {
                parameters.validate()?;
                let inherited = ($inherited)(parameters);
                Ok(RewardConfiguration {
                    preset: inherited.phase0.preset,
                    slots_per_epoch: inherited.slots_per_epoch(),
                    inactivity_score_bias: inherited.inactivity_score_bias,
                    inactivity_penalty_quotient: $quotient,
                })
            }
        }
    };
}
reward_state!(ssz::altair::BeaconState, AltairParameters, |p: &AltairParameters| *p, 50_331_648);
reward_state!(
    ssz::bellatrix::BeaconState,
    crate::bellatrix_processing::BellatrixParameters,
    |p: &crate::bellatrix_processing::BellatrixParameters| p.altair,
    16_777_216
);
reward_state!(
    ssz::capella::BeaconState,
    crate::capella_processing::CapellaParameters,
    |p: &crate::capella_processing::CapellaParameters| p.bellatrix.altair,
    16_777_216
);
reward_state!(
    ssz::deneb::BeaconState,
    crate::deneb_processing::DenebParameters,
    |p: &crate::deneb_processing::DenebParameters| p.capella.bellatrix.altair,
    16_777_216
);
reward_state!(
    ssz::electra::BeaconState,
    crate::electra_processing::ElectraParameters,
    |p: &crate::electra_processing::ElectraParameters| p.deneb.capella.bellatrix.altair,
    16_777_216
);
reward_state!(
    ssz::fulu::BeaconState,
    crate::fulu_processing::FuluParameters,
    |p: &crate::fulu_processing::FuluParameters| p.electra.deneb.capella.bellatrix.altair,
    16_777_216
);
reward_state!(
    ssz::gloas::BeaconState,
    crate::gloas_processing::GloasParameters,
    |p: &crate::gloas_processing::GloasParameters| p.fulu.electra.deneb.capella.bellatrix.altair,
    16_777_216
);

fn check_shape<S: ParticipationRewardState>(state: &S) -> Result<(), ProcessingError> {
    let count = state.reward_validators().len();
    if [
        state.reward_balances_len(),
        state.reward_current_participation().len(),
        state.reward_previous_participation().len(),
        state.reward_inactivity_scores().len(),
    ]
    .into_iter()
    .any(|length| length != count)
    {
        return Err(ProcessingError::InvalidState("reward validator-indexed list lengths differ"));
    }
    Ok(())
}
fn participation<S: ParticipationRewardState>(state: &S, slots_per_epoch: u64) -> &[u8] {
    if state.reward_slot() / slots_per_epoch == 0 {
        state.reward_current_participation()
    } else {
        state.reward_previous_participation()
    }
}
const fn eligible(validator: &Validator, previous: u64, next_previous: u64) -> bool {
    is_active(validator, previous) ||
        (validator.slashed && next_previous < validator.withdrawable_epoch)
}
fn flag_kernel<S: ParticipationRewardState>(
    state: &S,
    flag: usize,
    slots_per_epoch: u64,
) -> Result<RewardDeltas, ProcessingError> {
    check_shape(state)?;
    let Some(&weight) = FLAG_WEIGHTS.get(flag) else {
        return Err(ProcessingError::InvalidParameters("participation flag index is out of range"));
    };
    let current = state.reward_slot() / slots_per_epoch;
    let previous = current.saturating_sub(1);
    let next_previous = add(previous, 1, "eligible previous epoch")?;
    let flags = participation(state, slots_per_epoch);
    let mut active_balance = 0;
    let mut participating_balance = 0;
    for (validator, &flags) in state.reward_validators().iter().zip(flags) {
        if is_active(validator, current) {
            active_balance =
                add(active_balance, validator.effective_balance, "total active balance")?;
        }
        if is_active(validator, previous) && !validator.slashed && flags & (1 << flag) != 0 {
            participating_balance = add(
                participating_balance,
                validator.effective_balance,
                "total participating balance",
            )?;
        }
    }
    let active = active_balance.max(INCREMENT) / INCREMENT;
    let participating = participating_balance.max(INCREMENT) / INCREMENT;
    let per_increment = mul(INCREMENT, 64, "base reward numerator")? /
        integer_squareroot(active_balance.max(INCREMENT));
    let leak = sub(previous, state.reward_finalized_epoch(), "finality delay")? > 4;
    let mut deltas = RewardDeltas {
        rewards: vec![0; state.reward_validators().len()],
        penalties: vec![0; state.reward_validators().len()],
    };
    for (index, (validator, &flags)) in state.reward_validators().iter().zip(flags).enumerate() {
        if !eligible(validator, previous, next_previous) {
            continue;
        }
        let base = mul(
            validator.effective_balance / INCREMENT,
            per_increment,
            "participation base reward",
        )?;
        if is_active(validator, previous) && !validator.slashed && flags & (1 << flag) != 0 {
            if !leak {
                deltas.rewards[index] = mul(
                    mul(base, weight, "flag reward numerator")?,
                    participating,
                    "participating reward numerator",
                )? / mul(active, 64, "flag reward denominator")?;
            }
        } else if flag != 2 {
            deltas.penalties[index] = mul(base, weight, "flag penalty numerator")? / 64;
        }
    }
    Ok(deltas)
}
fn inactivity_kernel<S: ParticipationRewardState>(
    state: &S,
    slots_per_epoch: u64,
    bias: u64,
    quotient: u64,
) -> Result<RewardDeltas, ProcessingError> {
    check_shape(state)?;
    let previous = (state.reward_slot() / slots_per_epoch).saturating_sub(1);
    let next_previous = add(previous, 1, "eligible previous epoch")?;
    let denominator = mul(bias, quotient, "inactivity penalty denominator")?;
    if denominator == 0 {
        return Err(ProcessingError::InvalidParameters(
            "inactivity penalty denominator must be nonzero",
        ));
    }
    let flags = participation(state, slots_per_epoch);
    let mut deltas = RewardDeltas {
        rewards: vec![0; state.reward_validators().len()],
        penalties: vec![0; state.reward_validators().len()],
    };
    for (index, validator) in state.reward_validators().iter().enumerate() {
        let participated =
            is_active(validator, previous) && !validator.slashed && flags[index] & 2 != 0;
        if eligible(validator, previous, next_previous) && !participated {
            deltas.penalties[index] = mul(
                validator.effective_balance,
                state.reward_inactivity_scores()[index],
                "inactivity penalty numerator",
            )? / denominator;
        }
    }
    Ok(deltas)
}
/// Compute one immutable timely participation component using the exact typed fork configuration.
pub fn get_flag_index_deltas<S: ParticipationRewardState>(
    state: &S,
    flag: usize,
    parameters: &S::Parameters,
) -> Result<RewardDeltas, ProcessingError> {
    let config = S::reward_configuration(parameters)?;
    state.validate_reward_encoding(config.preset)?;
    flag_kernel(state, flag, config.slots_per_epoch)
}
/// Compute immutable inactivity penalties without projecting, cloning or changing the state.
pub fn get_inactivity_penalty_deltas<S: ParticipationRewardState>(
    state: &S,
    parameters: &S::Parameters,
) -> Result<RewardDeltas, ProcessingError> {
    let config = S::reward_configuration(parameters)?;
    state.validate_reward_encoding(config.preset)?;
    inactivity_kernel(
        state,
        config.slots_per_epoch,
        config.inactivity_score_bias,
        config.inactivity_penalty_quotient,
    )
}
pub(crate) fn flag_deltas<S: ParticipationRewardState>(
    state: &S,
    flag: usize,
    constants: &Constants,
) -> Result<RewardDeltas, ProcessingError> {
    flag_kernel(state, flag, constants.slots_per_epoch)
}
pub(crate) fn inactivity_deltas<S: ParticipationRewardState>(
    state: &S,
    parameters: &AltairParameters,
    constants: &Constants,
) -> Result<RewardDeltas, ProcessingError> {
    inactivity_kernel(
        state,
        constants.slots_per_epoch,
        parameters.inactivity_score_bias,
        constants.inactivity_penalty_quotient,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bellatrix_processing::{tests::state_and_keys, BellatrixParameters};

    fn reward_state() -> ssz::bellatrix::BeaconState {
        let (mut state, _) = state_and_keys();
        state.slot = 8;
        state.validators.values.truncate(2);
        state.balances.values.truncate(2);
        state.current_epoch_participation.values = vec![0; 2];
        state.previous_epoch_participation.values = vec![7; 2];
        state.inactivity_scores.values = vec![0; 2];
        state
    }

    #[test]
    fn exact_components_and_missing_head_have_no_penalty() {
        let mut state = reward_state();
        let parameters = BellatrixParameters::for_preset(Preset::Minimal);
        let before = state.clone();
        for (flag, expected) in [1_770_874, 3_288_766, 1_770_874].into_iter().enumerate() {
            let deltas = get_flag_index_deltas(&state, flag, &parameters).unwrap();
            assert_eq!(deltas.rewards, [expected; 2]);
            assert_eq!(deltas.penalties, [0; 2]);
        }
        assert_eq!(state, before);
        state.previous_epoch_participation.values = vec![3; 2];
        let head = get_flag_index_deltas(&state, 2, &parameters).unwrap();
        assert_eq!(head.rewards, [0; 2]);
        assert_eq!(head.penalties, [0; 2]);
        state.previous_epoch_participation.values = vec![0; 2];
        let source = get_flag_index_deltas(&state, 0, &parameters).unwrap();
        assert_eq!(source.penalties, [1_770_874; 2]);
    }

    #[test]
    fn compounding_effective_balances_are_not_capped_to_legacy_maximum() {
        let mut state = reward_state();
        let parameters = BellatrixParameters::for_preset(Preset::Minimal);
        state.validators.values[1].effective_balance = 2_048_000_000_000;
        state.balances.values[1] = 2_048_000_000_000;
        let source = get_flag_index_deltas(&state, 0, &parameters).unwrap();
        assert_eq!(source.rewards, [310_632, 19_880_448]);
        state.previous_epoch_participation.values = vec![0; 2];
        state.inactivity_scores.values = vec![65_536; 2];
        let inactivity = get_inactivity_penalty_deltas(&state, &parameters).unwrap();
        assert_eq!(inactivity.rewards, [0; 2]);
        assert_eq!(inactivity.penalties, [31_250_000, 2_000_000_000]);
    }

    #[test]
    fn leak_begins_after_four_epochs_and_genesis_uses_current_flags() {
        let mut state = reward_state();
        let parameters = BellatrixParameters::for_preset(Preset::Minimal);
        state.slot = 5 * 8;
        assert_eq!(get_flag_index_deltas(&state, 1, &parameters).unwrap().rewards, [3_288_766; 2]);
        state.slot = 6 * 8;
        assert_eq!(get_flag_index_deltas(&state, 1, &parameters).unwrap().rewards, [0; 2]);
        state.slot = 0;
        state.current_epoch_participation.values = vec![7; 2];
        state.previous_epoch_participation.values = vec![0; 2];
        assert_eq!(get_flag_index_deltas(&state, 1, &parameters).unwrap().rewards, [3_288_766; 2]);
    }

    #[test]
    fn malformed_shapes_invalid_flags_and_overflow_fail_immutably() {
        let mut state = reward_state();
        let parameters = BellatrixParameters::for_preset(Preset::Minimal);
        assert!(matches!(
            get_flag_index_deltas(&state, 3, &parameters),
            Err(ProcessingError::InvalidParameters("participation flag index is out of range"))
        ));
        state.inactivity_scores.values.pop();
        let before = state.clone();
        assert!(matches!(
            get_flag_index_deltas(&state, 0, &parameters),
            Err(ProcessingError::InvalidState("reward validator-indexed list lengths differ"))
        ));
        assert_eq!(state, before);
        state.inactivity_scores.values.push(0);
        state.validators.values[0].effective_balance = u64::MAX;
        let before = state.clone();
        assert!(matches!(
            get_flag_index_deltas(&state, 0, &parameters),
            Err(ProcessingError::Arithmetic("total active balance"))
        ));
        assert_eq!(state, before);
    }
}
