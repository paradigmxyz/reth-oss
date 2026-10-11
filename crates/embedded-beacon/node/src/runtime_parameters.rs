//! Runtime kernel parameters derived from the same checked network used by peers and execution.
//! The resource profile never changes a chain's consensus preset or scheduled signing domains.

use reth_embedded_beacon_config::{ChainConfig, Preset as ConfigPreset};
use reth_embedded_beacon_fork_choice::StoreParameters;
use reth_embedded_beacon_state_transition::{
    fork_processing::{ForkActivation, TransitionParameters},
    fulu_processing::BlobParameters,
    phase0_processing::ProcessingError,
    ssz::Preset,
};

/// One checked source of consensus clocks, domains and blob upgrades for the import owner.
#[derive(Clone, Debug)]
pub struct RuntimeParameters {
    /// Exact state-transition schedule and inherited fork kernels.
    pub transitions: TransitionParameters,
    /// Exact fork-choice preset, deadlines and wall-clock duration.
    pub fork_choice: StoreParameters,
}

impl RuntimeParameters {
    /// Bind production preset constants to explicit network versions, epochs, clock and BPOs.
    /// Omitted trailing forks stay disabled; their preset versions never select active domains.
    pub fn from_config(config: &ChainConfig) -> Result<Self, ProcessingError> {
        config
            .validate()
            .map_err(|_| ProcessingError::InvalidParameters("invalid runtime network"))?;
        let preset = match config.preset {
            ConfigPreset::Mainnet => Preset::Mainnet,
            ConfigPreset::Minimal => Preset::Minimal,
        };
        let mut defaults = TransitionParameters::for_preset(preset);
        if config.slots_per_epoch != defaults.slots_per_epoch() {
            return Err(ProcessingError::InvalidParameters(
                "network epoch length differs from consensus preset",
            ));
        }
        let schedule = defaults
            .schedule
            .iter()
            .map(|default| {
                config
                    .forks
                    .activations()
                    .iter()
                    .find(|activation| activation.fork == default.fork)
                    .map_or(ForkActivation { epoch: u64::MAX, ..*default }, |activation| {
                        ForkActivation {
                            fork: activation.fork,
                            epoch: activation.epoch,
                            version: activation.version,
                        }
                    })
            })
            .collect();
        defaults.gloas.fulu.electra.deneb.capella.bellatrix.seconds_per_slot =
            config.seconds_per_slot;
        defaults.gloas.fulu.blob_schedule = config
            .blob_schedule
            .iter()
            .map(|entry| BlobParameters {
                epoch: entry.epoch,
                max_blobs_per_block: entry.max_blobs_per_block,
            })
            .collect();
        let transitions = TransitionParameters::new(defaults.gloas, schedule)?;
        let mut fork_choice = StoreParameters::for_preset(preset);
        fork_choice.phase0 = transitions.gloas.fulu.electra.deneb.capella.bellatrix.altair.phase0;
        fork_choice.altair_fork_version =
            transitions.gloas.fulu.electra.deneb.capella.bellatrix.altair.fork_version;
        fork_choice.inactivity_score_bias =
            transitions.gloas.fulu.electra.deneb.capella.bellatrix.altair.inactivity_score_bias;
        fork_choice.inactivity_score_recovery_rate = transitions
            .gloas
            .fulu
            .electra
            .deneb
            .capella
            .bellatrix
            .altair
            .inactivity_score_recovery_rate;
        fork_choice.slot_duration_ms = config
            .seconds_per_slot
            .checked_mul(1000)
            .ok_or(ProcessingError::InvalidParameters("runtime slot duration overflow"))?;
        Ok(Self { transitions, fork_choice })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reth_embedded_beacon_config::{ForkActivation as ConfigActivation, ForkSchedule};
    use reth_embedded_beacon_primitives::Fork;

    #[test]
    fn public_network_signing_versions_and_blob_changes_match_the_selected_network() {
        for config in [ChainConfig::mainnet(), ChainConfig::hoodi()] {
            let parameters = RuntimeParameters::from_config(&config).unwrap();
            for activation in
                config.forks.activations().iter().filter(|activation| activation.epoch != u64::MAX)
            {
                let slot = activation.epoch * config.slots_per_epoch;
                assert_eq!(
                    parameters.transitions.fork_at_slot(slot).unwrap(),
                    config.fork_at_slot(slot).unwrap().fork
                );
                assert_eq!(
                    parameters
                        .transitions
                        .schedule
                        .iter()
                        .find(|item| item.fork == activation.fork)
                        .unwrap()
                        .version,
                    activation.version
                );
            }
            for transition in &parameters.transitions.schedule {
                let expected_epoch = config
                    .forks
                    .activations()
                    .iter()
                    .find(|activation| activation.fork == transition.fork)
                    .map_or(u64::MAX, |activation| activation.epoch);
                assert_eq!(transition.epoch, expected_epoch);
            }
            for upgrade in &config.blob_schedule {
                assert_eq!(
                    parameters
                        .transitions
                        .gloas
                        .fulu
                        .blob_parameters_at_epoch(upgrade.epoch)
                        .max_blobs_per_block,
                    upgrade.max_blobs_per_block
                );
            }
            assert_eq!(parameters.fork_choice.slot_duration_ms, 12000);
            assert_eq!(parameters.transitions.slots_per_epoch(), 32);
        }
    }

    #[test]
    fn private_epoch_forks_keep_mainnet_consensus_rules_with_a_six_second_clock() {
        let mut config = ChainConfig::mainnet();
        config.seconds_per_slot = 6;
        config.blob_schedule.clear();
        config.forks = ForkSchedule::new(
            [
                Fork::Phase0,
                Fork::Altair,
                Fork::Bellatrix,
                Fork::Capella,
                Fork::Deneb,
                Fork::Electra,
                Fork::Fulu,
                Fork::Gloas,
            ]
            .into_iter()
            .enumerate()
            .map(|(index, fork)| ConfigActivation {
                fork,
                epoch: index as u64,
                version: [index as u8, 0, 0, 42],
            })
            .collect(),
        )
        .unwrap();
        let parameters = RuntimeParameters::from_config(&config).unwrap();
        assert_eq!(parameters.fork_choice.slot_duration_ms, 6000);
        assert_eq!(
            parameters.transitions.gloas.fulu.electra.deneb.capella.bellatrix.seconds_per_slot,
            6
        );
        assert_eq!(parameters.transitions.slots_per_epoch(), 32);
        for (index, activation) in config.forks.activations().iter().enumerate() {
            let slot = index as u64 * 32;
            assert_eq!(parameters.transitions.fork_at_slot(slot).unwrap(), activation.fork);
            assert_eq!(
                config.clock().unwrap().start_of_slot(slot).unwrap(),
                (config.genesis_time + slot * 6) * 1000
            );
        }
        config.slots_per_epoch = 1;
        assert!(RuntimeParameters::from_config(&config).is_err());
    }
}
