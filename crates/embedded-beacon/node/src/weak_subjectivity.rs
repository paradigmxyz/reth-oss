//! State-dependent checkpoint trust periods from the pinned consensus specification.
//!
//! Phase0 through Deneb retain count and deposit limits. Electra and Fulu use balance
//! churn; Gloas combines separately rounded exit, activation and consolidation budgets.
//! Exact source and published-state hashes are in `testdata/weak-subjectivity`.

use reth_embedded_beacon_config::{ChainConfig, Preset as ConfigPreset};
use reth_embedded_beacon_primitives::{Checkpoint, Fork};
use reth_embedded_beacon_state_transition::{
    electra_processing::ElectraParameters,
    gloas_processing::GloasParameters,
    phase0_processing::Phase0Parameters,
    ssz::{phase0::Validator, BeaconState, Preset},
};
use thiserror::Error;

const EFFECTIVE_BALANCE_INCREMENT: u64 = 1_000_000_000;
const LEGACY_MAX_EFFECTIVE_BALANCE: u64 = 32_000_000_000;
const COMPOUNDING_MAX_EFFECTIVE_BALANCE: u64 = 2_048_000_000_000;
const SAFETY_DECAY: u128 = 10;
const MAX_DEPOSITS: u128 = 16;

/// Period computed from the authenticated checkpoint state's actual active registry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WeakSubjectivityPeriod {
    /// Inclusive maximum age in epochs, also constrained by the operator's cap.
    pub epochs: u64,
    /// Validators active at the checkpoint state's epoch, including slashed validators.
    pub active_validator_count: u64,
    /// Checked sum of active effective balances, denominated in Gwei.
    pub total_active_balance: u64,
}

/// State-dependent trust-window authentication failures.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum WeakSubjectivityError {
    /// Clock, preset, network or fork selection disagrees with the decoded state.
    #[error("weak-subjectivity state and network configuration disagree")]
    InvalidConfiguration,
    /// The authenticated state cannot supply a meaningful, valid active registry.
    #[error("invalid weak-subjectivity state: {0}")]
    InvalidState(&'static str),
    /// A consensus quantity cannot be represented without wrapping.
    #[error("weak-subjectivity arithmetic overflow: {0}")]
    ArithmeticOverflow(&'static str),
    /// Checkpoint state is future or older than its state-dependent period.
    #[error("checkpoint epoch {checkpoint_epoch} is outside weak-subjectivity period {period_epochs} at epoch {current_epoch}")]
    OutsidePeriod {
        /// Epoch of the authenticated checkpoint state.
        checkpoint_epoch: u64,
        /// Epoch from the selected network's clock.
        current_epoch: u64,
        /// Maximum inclusive state-dependent age.
        period_epochs: u64,
    },
}

/// Compute the pinned fork-specific period using actual active effective balances.
///
/// This calculation does not authenticate a state commitment or replay a transition.
/// The caller must separately authenticate the complete state and trusted block linkage.
pub fn compute_weak_subjectivity_period(
    state: &BeaconState,
    config: &ChainConfig,
) -> Result<WeakSubjectivityPeriod, WeakSubjectivityError> {
    config.validate().map_err(|_| WeakSubjectivityError::InvalidConfiguration)?;
    let parameters = Phase0Parameters::for_preset(state.preset());
    let config_preset = match config.preset {
        ConfigPreset::Mainnet => Preset::Mainnet,
        ConfigPreset::Minimal => Preset::Minimal,
    };
    if config_preset != state.preset() ||
        config.slots_per_epoch != parameters.slots_per_epoch() ||
        state.genesis_time() != config.genesis_time ||
        state.genesis_validators_root() != config.genesis_validators_root ||
        config
            .fork_at_slot(state.slot())
            .map_err(|_| WeakSubjectivityError::InvalidConfiguration)?
            .fork !=
            state.schema_fork()
    {
        return Err(WeakSubjectivityError::InvalidConfiguration)
    }
    let epoch = state.slot() / parameters.slots_per_epoch();
    let (validators, balances_len, finalized) = state_fields(state);
    if validators.len() != balances_len {
        return Err(WeakSubjectivityError::InvalidState("registry and balance lengths differ"))
    }
    if finalized.epoch > epoch {
        return Err(WeakSubjectivityError::InvalidState("finalized checkpoint is in the future"))
    }
    let (active_validator_count, total_active_balance) =
        active_balances(validators, epoch, state.schema_fork())?;
    let epochs = period_from_balances(
        state.schema_fork(),
        state.preset(),
        active_validator_count,
        total_active_balance,
    )?;
    Ok(WeakSubjectivityPeriod { epochs, active_validator_count, total_active_balance })
}

pub(crate) fn verify_checkpoint_age(
    state: &BeaconState,
    config: &ChainConfig,
    current_epoch: u64,
) -> Result<WeakSubjectivityPeriod, WeakSubjectivityError> {
    let period = compute_weak_subjectivity_period(state, config)?;
    let checkpoint_epoch = state.slot() / config.slots_per_epoch;
    // Subtract ages instead of adding a deadline: an epoch near u64::MAX cannot wrap.
    if !state.slot().is_multiple_of(config.slots_per_epoch) {
        return Err(WeakSubjectivityError::InvalidState("checkpoint state is not an epoch boundary"))
    }
    if current_epoch.checked_sub(checkpoint_epoch).is_none_or(|age| age > period.epochs) {
        return Err(WeakSubjectivityError::OutsidePeriod {
            checkpoint_epoch,
            current_epoch,
            period_epochs: period.epochs,
        })
    }
    Ok(period)
}

fn state_fields(state: &BeaconState) -> (&[Validator], usize, Checkpoint) {
    match state {
        BeaconState::Phase0(value, _) => {
            (&value.validators.values, value.balances.values.len(), value.finalized_checkpoint)
        }
        BeaconState::Altair(value, _) => {
            (&value.validators.values, value.balances.values.len(), value.finalized_checkpoint)
        }
        BeaconState::Bellatrix(value, _) => {
            (&value.validators.values, value.balances.values.len(), value.finalized_checkpoint)
        }
        BeaconState::Capella(value, _) => {
            (&value.validators.values, value.balances.values.len(), value.finalized_checkpoint)
        }
        BeaconState::Deneb(value, _) => {
            (&value.validators.values, value.balances.values.len(), value.finalized_checkpoint)
        }
        BeaconState::Electra(value, _) => {
            (&value.validators.values, value.balances.values.len(), value.finalized_checkpoint)
        }
        BeaconState::Fulu(value, _) => {
            (&value.validators.values, value.balances.values.len(), value.finalized_checkpoint)
        }
        BeaconState::Gloas(value, _) => {
            (&value.validators.values, value.balances.values.len(), value.finalized_checkpoint)
        }
    }
}

fn active_balances(
    validators: &[Validator],
    epoch: u64,
    fork: Fork,
) -> Result<(u64, u64), WeakSubjectivityError> {
    let mut count = 0;
    let mut total = 0;
    for validator in validators {
        // Slashing does not remove a validator from the active set before its exit.
        if validator.activation_epoch <= epoch && epoch < validator.exit_epoch {
            let maximum = if fork >= Fork::Electra && validator.withdrawal_credentials[0] == 2 {
                COMPOUNDING_MAX_EFFECTIVE_BALANCE
            } else {
                LEGACY_MAX_EFFECTIVE_BALANCE
            };
            if validator.effective_balance > maximum ||
                !validator.effective_balance.is_multiple_of(EFFECTIVE_BALANCE_INCREMENT)
            {
                return Err(WeakSubjectivityError::InvalidState(
                    "active effective balance is invalid",
                ))
            }
            count = checked_add(count, 1, "active validator count")?;
            total = checked_add(total, validator.effective_balance, "total active balance")?;
        }
    }
    if count == 0 || total == 0 {
        return Err(WeakSubjectivityError::InvalidState("active registry has no positive stake"))
    }
    Ok((count, total))
}

fn period_from_balances(
    fork: Fork,
    preset: Preset,
    active_count: u64,
    total_balance: u64,
) -> Result<u64, WeakSubjectivityError> {
    if active_count == 0 || total_balance == 0 {
        return Err(WeakSubjectivityError::InvalidState("active registry has no positive stake"))
    }
    let phase0 = Phase0Parameters::for_preset(preset);
    let extra = if fork < Fork::Electra {
        legacy_period_extra(active_count, total_balance, &phase0)
    } else {
        let electra = ElectraParameters::for_preset(preset);
        let churn = if fork < Fork::Gloas {
            u128::from(rounded_churn(
                total_balance,
                phase0.churn_limit_quotient,
                electra.min_per_epoch_churn_limit,
            ))
        } else {
            let gloas = GloasParameters::for_preset(preset);
            let exit = rounded_churn(
                total_balance,
                gloas.churn_limit_quotient,
                electra.min_per_epoch_churn_limit,
            );
            let activation = exit.min(gloas.max_per_epoch_activation_churn_limit);
            let consolidation =
                rounded_churn(total_balance, gloas.consolidation_churn_limit_quotient, 0);
            // The two floor divisions precede addition; combining them changes the result.
            weighted_gloas_churn(exit, activation, consolidation)
        };
        SAFETY_DECAY * u128::from(total_balance) / (2 * churn * 100)
    };
    // Wide intermediates retain the specified integer divisions without balance-product overflow.
    u64::try_from(u128::from(phase0.min_validator_withdrawability_delay) + extra)
        .map_err(|_| WeakSubjectivityError::ArithmeticOverflow("period epochs"))
}

const fn rounded_churn(total: u64, quotient: u64, minimum: u64) -> u64 {
    let quotient_churn = total / quotient;
    let churn = if quotient_churn > minimum { quotient_churn } else { minimum };
    churn - churn % EFFECTIVE_BALANCE_INCREMENT
}

fn weighted_gloas_churn(exit: u64, activation: u64, consolidation: u64) -> u128 {
    2 * u128::from(exit) / 3 + u128::from(activation) / 3 + u128::from(consolidation)
}

fn legacy_period_extra(count: u64, total: u64, parameters: &Phase0Parameters) -> u128 {
    let average = u128::from(total / count / EFFECTIVE_BALANCE_INCREMENT);
    let count = u128::from(count);
    let maximum = u128::from(LEGACY_MAX_EFFECTIVE_BALANCE / EFFECTIVE_BALANCE_INCREMENT);
    let churn = u128::from(parameters.min_per_epoch_churn_limit)
        .max(count / u128::from(parameters.churn_limit_quotient));
    let deposits = MAX_DEPOSITS * u128::from(parameters.slots_per_epoch());
    let average_threshold = average * (200 + 12 * SAFETY_DECAY);
    let maximum_threshold = maximum * (200 + 3 * SAFETY_DECAY);
    if average_threshold > maximum_threshold {
        let validator_churn = count * (average_threshold - maximum_threshold) /
            (600 * churn * (2 * average + maximum));
        let balance_top_ups = count * (200 + 3 * SAFETY_DECAY) / (600 * deposits);
        validator_churn.max(balance_top_ups)
    } else {
        3 * count * SAFETY_DECAY * average / (200 * deposits * (maximum - average))
    }
}

fn checked_add(left: u64, right: u64, context: &'static str) -> Result<u64, WeakSubjectivityError> {
    left.checked_add(right).ok_or(WeakSubjectivityError::ArithmeticOverflow(context))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bootstrap::tests::linked_fixture;
    use reth_embedded_beacon_config::{ForkActivation, ForkSchedule};
    use snap::raw::Decoder;

    fn state_and_config() -> (BeaconState, ChainConfig) {
        let (mut state, _, config, _) = linked_fixture();
        let validator = state.validators.values[0].clone();
        state.validators.values = vec![validator; 64];
        state.balances.values = vec![LEGACY_MAX_EFFECTIVE_BALANCE; 64];
        (BeaconState::Phase0(state, Preset::Minimal), config)
    }

    fn config_for(state: &BeaconState) -> ChainConfig {
        let (_, _, mut config, _) = linked_fixture();
        config.preset = match state.preset() {
            Preset::Mainnet => ConfigPreset::Mainnet,
            Preset::Minimal => ConfigPreset::Minimal,
        };
        config.slots_per_epoch = match state.preset() {
            Preset::Mainnet => 32,
            Preset::Minimal => 8,
        };
        config.genesis_time = state.genesis_time();
        config.genesis_validators_root = state.genesis_validators_root();
        let fork = state.fork_info();
        let mut activations =
            vec![ForkActivation { fork: Fork::Phase0, epoch: 0, version: [0; 4] }];
        if state.schema_fork() != Fork::Phase0 {
            activations.push(ForkActivation {
                fork: state.schema_fork(),
                epoch: fork.epoch,
                version: fork.current_version,
            });
        }
        config.forks = ForkSchedule::new(activations).unwrap();
        config
    }

    #[test]
    fn phase0_to_deneb_match_pinned_reference_table_and_both_branches() {
        for (balance, count, expected) in [
            (28, 32768, 504),
            (28, 65536, 752),
            (28, 131072, 1248),
            (28, 262144, 2241),
            (28, 524288, 2241),
            (28, 1048576, 2241),
            (32, 32768, 665),
            (32, 65536, 1075),
            (32, 131072, 1894),
            (32, 262144, 3532),
            (32, 524288, 3532),
            (32, 1048576, 3532),
            (20, 32768, 272),
            (23, 32768, 280),
        ] {
            for fork in [Fork::Phase0, Fork::Altair, Fork::Bellatrix, Fork::Capella, Fork::Deneb] {
                assert_eq!(
                    period_from_balances(
                        fork,
                        Preset::Mainnet,
                        count,
                        count * balance * EFFECTIVE_BALANCE_INCREMENT
                    )
                    .unwrap(),
                    expected
                );
            }
        }
    }

    #[test]
    fn electra_and_fulu_match_uncapped_balance_churn_reference_table() {
        for (balance_eth, expected) in [
            (1048576, 665),
            (2097152, 1075),
            (4194304, 1894),
            (8388608, 3532),
            (16777216, 3532),
            (33554432, 3532),
        ] {
            for fork in [Fork::Electra, Fork::Fulu] {
                assert_eq!(
                    period_from_balances(
                        fork,
                        Preset::Mainnet,
                        1,
                        balance_eth * EFFECTIVE_BALANCE_INCREMENT
                    )
                    .unwrap(),
                    expected
                );
            }
        }
    }

    #[test]
    fn gloas_matches_independent_churn_reference_table() {
        for (balance_eth, expected) in [
            (1048576, 620),
            (2097152, 911),
            (4194304, 1348),
            (8388608, 1348),
            (16777216, 1484),
            (33554432, 1566),
            (67108864, 1611),
            (134217728, 1635),
        ] {
            assert_eq!(
                period_from_balances(
                    Fork::Gloas,
                    Preset::Mainnet,
                    1,
                    balance_eth * EFFECTIVE_BALANCE_INCREMENT
                )
                .unwrap(),
                expected
            );
        }
    }

    #[test]
    fn actual_active_epoch_and_slashed_stake_determine_the_period() {
        let (mut state, config) = state_and_config();
        let BeaconState::Phase0(value, _) = &mut state else { unreachable!() };
        value.validators.values[0].exit_epoch = 8;
        value.validators.values[1].activation_epoch = 9;
        value.validators.values[2].slashed = true;
        let period = compute_weak_subjectivity_period(&state, &config).unwrap();
        assert_eq!(period.active_validator_count, 62);
        assert_eq!(period.total_active_balance, 62 * LEGACY_MAX_EFFECTIVE_BALANCE);
        assert_eq!(period.epochs, 257);
    }

    #[test]
    fn period_boundary_is_inclusive_and_finalized_epoch_does_not_rebase_age() {
        let (mut state, config) = state_and_config();
        let BeaconState::Phase0(value, _) = &mut state else { unreachable!() };
        value.finalized_checkpoint.epoch = 6;
        let period = verify_checkpoint_age(&state, &config, 265).unwrap();
        assert_eq!(period.epochs, 257);
        assert_eq!(
            verify_checkpoint_age(&state, &config, 266).unwrap_err(),
            WeakSubjectivityError::OutsidePeriod {
                checkpoint_epoch: 8,
                current_epoch: 266,
                period_epochs: 257
            }
        );
        assert_eq!(
            verify_checkpoint_age(&state, &config, 7).unwrap_err(),
            WeakSubjectivityError::OutsidePeriod {
                checkpoint_epoch: 8,
                current_epoch: 7,
                period_epochs: 257
            }
        );
    }

    #[test]
    fn future_finality_and_unreplayed_empty_boundary_are_rejected() {
        let (mut state, config) = state_and_config();
        let BeaconState::Phase0(value, _) = &mut state else { unreachable!() };
        value.finalized_checkpoint.epoch = 9;
        assert_eq!(
            compute_weak_subjectivity_period(&state, &config).unwrap_err(),
            WeakSubjectivityError::InvalidState("finalized checkpoint is in the future")
        );
        let BeaconState::Phase0(value, _) = &mut state else { unreachable!() };
        value.finalized_checkpoint.epoch = 8;
        value.slot = 65;
        assert_eq!(
            verify_checkpoint_age(&state, &config, 9).unwrap_err(),
            WeakSubjectivityError::InvalidState("checkpoint state is not an epoch boundary")
        );
    }

    #[test]
    fn empty_zero_and_invalid_active_balances_fail_closed() {
        let (state, config) = state_and_config();
        for invalid_balance in
            [0, 1, LEGACY_MAX_EFFECTIVE_BALANCE + EFFECTIVE_BALANCE_INCREMENT, u64::MAX]
        {
            let mut candidate = state.clone();
            let BeaconState::Phase0(value, _) = &mut candidate else { unreachable!() };
            for validator in &mut value.validators.values {
                validator.effective_balance = invalid_balance;
            }
            let expected = if invalid_balance == 0 {
                "active registry has no positive stake"
            } else {
                "active effective balance is invalid"
            };
            assert_eq!(
                compute_weak_subjectivity_period(&candidate, &config).unwrap_err(),
                WeakSubjectivityError::InvalidState(expected)
            );
        }
        let mut empty = state.clone();
        let BeaconState::Phase0(value, _) = &mut empty else { unreachable!() };
        value.validators.values.clear();
        value.balances.values.clear();
        assert_eq!(
            compute_weak_subjectivity_period(&empty, &config).unwrap_err(),
            WeakSubjectivityError::InvalidState("active registry has no positive stake")
        );
        let mut mismatch = state;
        let BeaconState::Phase0(value, _) = &mut mismatch else { unreachable!() };
        value.balances.values.pop();
        assert_eq!(
            compute_weak_subjectivity_period(&mismatch, &config).unwrap_err(),
            WeakSubjectivityError::InvalidState("registry and balance lengths differ")
        );
    }

    #[test]
    fn checked_sum_and_large_products_never_wrap() {
        assert_eq!(
            checked_add(u64::MAX, 1, "total active balance").unwrap_err(),
            WeakSubjectivityError::ArithmeticOverflow("total active balance")
        );
        assert_eq!(
            period_from_balances(Fork::Electra, Preset::Mainnet, 1, u64::MAX).unwrap(),
            3532
        );
        assert_eq!(period_from_balances(Fork::Gloas, Preset::Mainnet, 1, u64::MAX).unwrap(), 1660);
    }

    #[test]
    fn published_fulu_and_gloas_states_match_independent_pinned_calculations() {
        for (fork, bytes, active_count, balance) in [
            (
                Fork::Fulu,
                include_bytes!("../testdata/fulu-upgrade/post.ssz_snappy").as_slice(),
                32,
                1024000000000,
            ),
            (
                Fork::Gloas,
                include_bytes!("../testdata/gloas-invalid-payload-attestation/state.ssz_snappy")
                    .as_slice(),
                64,
                2048000000000,
            ),
        ] {
            let raw = Decoder::new().decompress_vec(bytes).unwrap();
            let state = BeaconState::from_ssz(&raw, fork, Preset::Minimal).unwrap();
            let period = compute_weak_subjectivity_period(&state, &config_for(&state)).unwrap();
            assert_eq!(
                period,
                WeakSubjectivityPeriod {
                    epochs: 256,
                    active_validator_count: active_count,
                    total_active_balance: balance
                }
            );
        }
    }

    #[test]
    fn compounding_caps_and_presets_are_enforced_from_the_actual_state() {
        let raw = Decoder::new()
            .decompress_vec(include_bytes!(
                "../testdata/gloas-invalid-payload-attestation/state.ssz_snappy"
            ))
            .unwrap();
        let mut state = BeaconState::from_ssz(&raw, Fork::Gloas, Preset::Minimal).unwrap();
        let config = config_for(&state);
        let BeaconState::Gloas(value, _) = &mut state else { unreachable!() };
        value.validators.values[0].effective_balance = COMPOUNDING_MAX_EFFECTIVE_BALANCE;
        value.validators.values[0].withdrawal_credentials[0] = 2;
        assert!(compute_weak_subjectivity_period(&state, &config).is_ok());
        let BeaconState::Gloas(value, _) = &mut state else { unreachable!() };
        value.validators.values[0].withdrawal_credentials[0] = 1;
        assert_eq!(
            compute_weak_subjectivity_period(&state, &config).unwrap_err(),
            WeakSubjectivityError::InvalidState("active effective balance is invalid")
        );
        let mut wrong_preset = config;
        wrong_preset.preset = ConfigPreset::Mainnet;
        assert_eq!(
            compute_weak_subjectivity_period(&state, &wrong_preset).unwrap_err(),
            WeakSubjectivityError::InvalidConfiguration
        );
    }

    #[test]
    fn gloas_weights_floor_each_budget_before_adding() {
        assert_eq!(
            weighted_gloas_churn(128_000_000_000, 128_000_000_000, 16_000_000_000),
            143_999_999_999
        );
        assert_eq!(weighted_gloas_churn(256_000_000_000, 128_000_000_000, 0), 213_333_333_332);
    }

    #[test]
    fn large_checkpoint_epochs_use_checked_age_subtraction() {
        let (mut state, config) = state_and_config();
        let BeaconState::Phase0(value, _) = &mut state else { unreachable!() };
        value.slot = u64::MAX - 7;
        let checkpoint_epoch = value.slot / 8;
        value.finalized_checkpoint.epoch = checkpoint_epoch - 2;
        assert!(verify_checkpoint_age(&state, &config, checkpoint_epoch + 257).is_ok());
        assert_eq!(
            verify_checkpoint_age(&state, &config, u64::MAX).unwrap_err(),
            WeakSubjectivityError::OutsidePeriod {
                checkpoint_epoch,
                current_epoch: u64::MAX,
                period_epochs: 257
            }
        );
    }
}
