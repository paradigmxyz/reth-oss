//! Explicit bounded configuration for isolated private consensus networks.
//!
//! This verifies operator input and execution-network binding. It does not decode
//! `BeaconState` or authenticate genesis, and it never enables incomplete consensus.

use crate::{ChainConfig, ConfigError, Preset};
use reth_embedded_beacon_primitives::{Fork, Root};
use serde::{Deserialize, Serialize};
use std::io::Read;
use thiserror::Error;

/// Maximum encoded private-network configuration size.
pub const MAX_DEVNET_CONFIG_BYTES: usize = 1024 * 1024;

/// Operator-supplied isolated network identity and schedule.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DevnetConfig {
    /// Version of this input schema; currently one.
    pub schema_version: u32,
    /// Expected execution genesis block hash.
    pub execution_genesis_hash: Root,
    /// Explicit terminal execution block, restricted to this network's genesis.
    pub terminal_block_hash: Root,
    /// Epoch when the explicitly selected terminal block becomes eligible.
    pub terminal_block_hash_activation_epoch: u64,
    /// Consensus network subset; complete preset and state validation remain required.
    pub consensus: ChainConfig,
}

impl DevnetConfig {
    /// Parse bounded strict JSON and validate every represented parameter.
    pub fn from_json(bytes: &[u8]) -> Result<Self, DevnetConfigError> {
        if bytes.len() > MAX_DEVNET_CONFIG_BYTES {
            return Err(DevnetConfigError::TooLarge);
        }
        let config: Self = serde_json::from_slice(bytes)?;
        config.validate()?;
        Ok(config)
    }

    /// Bound streaming reads before allocating or decoding the complete document.
    pub fn from_reader(reader: impl Read) -> Result<Self, DevnetConfigError> {
        let mut bytes = Vec::new();
        reader.take(MAX_DEVNET_CONFIG_BYTES as u64 + 1).read_to_end(&mut bytes)?;
        Self::from_json(&bytes)
    }

    /// Validate private identity, full fork sequence, bounds and explicit Merge selection.
    pub fn validate(&self) -> Result<(), DevnetConfigError> {
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
        let config = &self.consensus;
        if self.schema_version != 1 {
            return Err(DevnetConfigError::SchemaVersion);
        }
        if config.chain_id == 0 ||
            matches!(config.chain_id, 1 | 560048) ||
            config.name.is_empty() ||
            config.name.len() > 64 ||
            matches!(config.name.as_str(), "mainnet" | "hoodi") ||
            !config
                .name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_')) ||
            self.execution_genesis_hash == [0; 32] ||
            config.genesis_validators_root == [0; 32]
        {
            return Err(DevnetConfigError::PrivateIdentity);
        }
        if config.preset != Preset::Mainnet ||
            config.slots_per_epoch != 32 ||
            !(1..=3600).contains(&config.seconds_per_slot) ||
            !(1..=64 * 1024 * 1024).contains(&config.max_payload_size) ||
            config.blob_schedule.len() > 64
        {
            return Err(DevnetConfigError::Bounds);
        }
        let activations = config.forks.activations();
        if activations.len() != FORKS.len() ||
            activations.iter().zip(FORKS).any(|(activation, fork)| {
                activation.fork != fork || activation.epoch == u64::MAX
            })
        {
            return Err(DevnetConfigError::ForkSequence);
        }
        config.validate()?;
        let clock = config.clock()?;
        for activation in activations {
            clock.start_of_slot(clock.start_slot_at_epoch(activation.epoch)?)?;
        }
        let bellatrix_epoch = activations[2].epoch;
        if self.terminal_block_hash != self.execution_genesis_hash ||
            self.terminal_block_hash_activation_epoch != bellatrix_epoch
        {
            return Err(DevnetConfigError::TerminalBlock);
        }
        Ok(())
    }

    /// Bind the checked input to the actual parsed execution specification.
    pub fn validate_execution_identity(
        &self,
        chain_id: u64,
        genesis_hash: Root,
    ) -> Result<(), DevnetConfigError> {
        self.validate()?;
        if chain_id != self.consensus.chain_id || genesis_hash != self.execution_genesis_hash {
            return Err(DevnetConfigError::ExecutionIdentity);
        }
        Ok(())
    }
}

/// Rejected private-network configuration input.
#[derive(Debug, Error)]
pub enum DevnetConfigError {
    /// Encoded input exceeds its fixed allocation bound.
    #[error("private beacon configuration exceeds 1 MiB")]
    TooLarge,
    /// Unknown schema versions are never interpreted as current input.
    #[error("unsupported private beacon configuration schema version")]
    SchemaVersion,
    /// Public identities and absent roots cannot select an isolated private profile.
    #[error("invalid isolated private beacon network identity")]
    PrivateIdentity,
    /// Preset, clock or resource limits are unsupported.
    #[error("unsupported private beacon preset, timing or resource bounds")]
    Bounds,
    /// Historical forks must be represented in order through a finite Gloas activation.
    #[error("private beacon configuration requires every fork from Phase0 through Gloas")]
    ForkSequence,
    /// This interface supports only an explicit genesis terminal block at Bellatrix activation.
    #[error(
        "private beacon terminal block must equal execution genesis and activate at Bellatrix"
    )]
    TerminalBlock,
    /// Parsed execution identity differs from the declared private network.
    #[error("private beacon configuration does not match execution chain ID and genesis hash")]
    ExecutionIdentity,
    /// Invalid represented consensus parameters.
    #[error(transparent)]
    Consensus(#[from] ConfigError),
    /// Invalid strict JSON encoding.
    #[error("invalid private beacon JSON: {0}")]
    Json(#[from] serde_json::Error),
    /// Failed bounded input read.
    #[error("private beacon configuration read failed: {0}")]
    Io(#[from] std::io::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ForkActivation, ForkSchedule};

    fn private_config() -> DevnetConfig {
        let mut consensus = ChainConfig::hoodi();
        consensus.name = "embedded-devnet".to_owned();
        consensus.chain_id = 3151908;
        consensus.genesis_time = 100;
        consensus.genesis_validators_root = [2; 32];
        consensus.seconds_per_slot = 6;
        consensus.blob_schedule.clear();
        consensus.forks = ForkSchedule::new(
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
            .map(|(index, fork)| ForkActivation {
                fork,
                epoch: index as u64,
                version: [index as u8, 0, 0, 42],
            })
            .collect(),
        )
        .unwrap();
        DevnetConfig {
            schema_version: 1,
            execution_genesis_hash: [3; 32],
            terminal_block_hash: [3; 32],
            terminal_block_hash_activation_epoch: 2,
            consensus,
        }
    }

    #[test]
    fn private_six_second_schedule_roundtrips_and_binds_execution() {
        let config = private_config();
        let bytes = serde_json::to_vec(&config).unwrap();
        assert_eq!(DevnetConfig::from_reader(bytes.as_slice()).unwrap(), config);
        config.validate_execution_identity(3151908, [3; 32]).unwrap();
        let clock = config.consensus.clock().unwrap();
        assert_eq!(clock.start_slot_at_epoch(7), Ok(224));
        assert_eq!(clock.start_of_slot(224), Ok(1444000));
        assert!(matches!(
            config.validate_execution_identity(42, [3; 32]),
            Err(DevnetConfigError::ExecutionIdentity)
        ));
        assert!(matches!(
            config.validate_execution_identity(3151908, [4; 32]),
            Err(DevnetConfigError::ExecutionIdentity)
        ));
    }

    #[test]
    fn encoded_size_bound_applies_before_deserialization() {
        let input = vec![b' '; MAX_DEVNET_CONFIG_BYTES + 1];
        assert!(matches!(DevnetConfig::from_json(&input), Err(DevnetConfigError::TooLarge)));
        assert!(matches!(
            DevnetConfig::from_reader(input.as_slice()),
            Err(DevnetConfigError::TooLarge)
        ));
    }

    #[test]
    fn unknown_fields_at_each_nested_level_are_rejected() {
        let config = serde_json::to_value(private_config()).unwrap();
        for path in [vec![], vec!["consensus"], vec!["consensus", "forks"]] {
            let mut value = config.clone();
            let mut target = &mut value;
            for component in path {
                target = &mut target[component];
            }
            target
                .as_object_mut()
                .unwrap()
                .insert("ignored_typo".to_owned(), serde_json::json!(true));
            assert!(matches!(
                DevnetConfig::from_json(&serde_json::to_vec(&value).unwrap()),
                Err(DevnetConfigError::Json(_))
            ));
        }
        let mut value = config.clone();
        value["consensus"]["forks"]["activations"][0]["ignored_typo"] = serde_json::json!(true);
        assert!(matches!(
            DevnetConfig::from_json(&serde_json::to_vec(&value).unwrap()),
            Err(DevnetConfigError::Json(_))
        ));
        let mut value = config;
        value["consensus"]["blob_schedule"] =
            serde_json::json!([{ "epoch": 6, "max_blobs_per_block": 9, "ignored_typo": true }]);
        assert!(matches!(
            DevnetConfig::from_json(&serde_json::to_vec(&value).unwrap()),
            Err(DevnetConfigError::Json(_))
        ));
    }

    #[test]
    fn public_networks_missing_forks_and_invented_terminal_blocks_are_rejected() {
        let good = private_config();
        for chain_id in [0, 1, 560048] {
            let mut config = good.clone();
            config.consensus.chain_id = chain_id;
            assert!(matches!(config.validate(), Err(DevnetConfigError::PrivateIdentity)));
        }
        let mut missing = good.clone();
        missing.consensus.forks =
            ForkSchedule::new(ChainConfig::hoodi().forks.activations()[..7].to_vec()).unwrap();
        assert!(matches!(missing.validate(), Err(DevnetConfigError::ForkSequence)));
        let mut terminal = good.clone();
        terminal.terminal_block_hash = [4; 32];
        assert!(matches!(terminal.validate(), Err(DevnetConfigError::TerminalBlock)));
        terminal = good;
        terminal.terminal_block_hash_activation_epoch = 3;
        assert!(matches!(terminal.validate(), Err(DevnetConfigError::TerminalBlock)));
    }

    #[test]
    fn invalid_limits_versions_and_overflow_fail_closed() {
        let good = private_config();
        let mut config = good.clone();
        config.schema_version = 2;
        assert!(matches!(config.validate(), Err(DevnetConfigError::SchemaVersion)));
        for seconds in [0, 3601] {
            let mut config = good.clone();
            config.consensus.seconds_per_slot = seconds;
            assert!(matches!(config.validate(), Err(DevnetConfigError::Bounds)));
        }
        let mut config = good;
        config.consensus.genesis_time = u64::MAX;
        assert!(matches!(
            config.validate(),
            Err(DevnetConfigError::Consensus(ConfigError::Overflow))
        ));
    }
}
