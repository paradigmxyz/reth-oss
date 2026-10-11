//! Checked fork schedules, network identity, and deterministic beacon clocks.

use reth_embedded_beacon_primitives::{hash_pair, sha256, Epoch, Fork, Root, Slot};
use serde::{Deserialize, Serialize};
use thiserror::Error;

mod devnet;
mod profile;

pub use devnet::{DevnetConfig, DevnetConfigError, MAX_DEVNET_CONFIG_BYTES};
pub use profile::{EmbeddedProfile, ProfileError};

/// Mainnet or minimal consensus preset.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Preset {
    /// Public-network consensus preset.
    Mainnet,
    /// Reduced consensus preset for specification testing.
    Minimal,
}

/// An explicit fork activation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForkActivation {
    /// Consensus schema activated at this epoch.
    pub fork: Fork,
    /// Epoch number.
    pub epoch: Epoch,
    /// Four-byte network fork version.
    pub version: [u8; 4],
}

/// A checked, monotonic fork schedule.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForkSchedule {
    activations: Vec<ForkActivation>,
}

impl ForkSchedule {
    /// Validate chronological and fork ordering; equal epochs are valid at genesis.
    pub fn new(activations: Vec<ForkActivation>) -> Result<Self, ConfigError> {
        if activations.first().is_none_or(|a| a.fork != Fork::Phase0 || a.epoch != 0) {
            return Err(ConfigError::InvalidForkSchedule);
        }
        if activations.windows(2).any(|w| w[0].epoch > w[1].epoch || w[0].fork >= w[1].fork) {
            return Err(ConfigError::InvalidForkSchedule);
        }
        for (index, activation) in activations.iter().enumerate() {
            if activations[..index].iter().any(|other| other.version == activation.version) {
                return Err(ConfigError::DuplicateForkVersion);
            }
        }
        Ok(Self { activations })
    }

    /// Fork and version at the supplied epoch; far-future activations stay disabled.
    pub fn at_epoch(&self, epoch: Epoch) -> Result<ForkActivation, ConfigError> {
        self.validate()?;
        self.activations
            .iter()
            .rev()
            .find(|a| a.epoch != u64::MAX && a.epoch <= epoch)
            .copied()
            .ok_or(ConfigError::InvalidForkSchedule)
    }

    /// All explicitly configured activations.
    pub fn activations(&self) -> &[ForkActivation] {
        &self.activations
    }

    /// Append Gloas using an explicitly supplied activation and version.
    pub fn with_gloas(&self, epoch: Epoch, version: [u8; 4]) -> Result<Self, ConfigError> {
        let mut activations = self.activations.clone();
        activations.push(ForkActivation { fork: Fork::Gloas, epoch, version });
        Self::new(activations)
    }

    /// Revalidate deserialized data before using it as a network configuration.
    pub fn validate(&self) -> Result<(), ConfigError> {
        Self::new(self.activations.clone()).map(|_| ())
    }
}

/// A blob-parameter-only network upgrade.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BlobParameters {
    /// Epoch number.
    pub epoch: Epoch,
    /// Maximum blobs permitted per block.
    pub max_blobs_per_block: u64,
}

/// Network configuration; Gloas is never assigned an invented public activation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChainConfig {
    /// Network configuration name.
    pub name: String,
    /// Consensus preset for collection sizes.
    pub preset: Preset,
    /// Execution chain identifier.
    pub chain_id: u64,
    /// Unix genesis time in seconds.
    pub genesis_time: u64,
    /// Network genesis validator registry root.
    pub genesis_validators_root: Root,
    /// Duration of one slot in seconds.
    pub seconds_per_slot: u64,
    /// Number of slots in an epoch.
    pub slots_per_epoch: u64,
    /// Explicit chronological fork activations.
    pub forks: ForkSchedule,
    /// Chronological blob-parameter-only activations.
    pub blob_schedule: Vec<BlobParameters>,
    /// Maximum decompressed network message size in bytes.
    pub max_payload_size: usize,
}

impl ChainConfig {
    /// Verified Hoodi network identity and announced schedule.
    pub fn hoodi() -> Self {
        Self {
            name: "hoodi".to_owned(),
            preset: Preset::Mainnet,
            chain_id: 560048,
            genesis_time: 1742213400,
            genesis_validators_root: [
                0x21, 0x2f, 0x13, 0xfc, 0x4d, 0xf0, 0x78, 0xb6, 0xcb, 0x7d, 0xb2, 0x28, 0xf1, 0xc8,
                0x30, 0x75, 0x66, 0xdc, 0xec, 0xf9, 0x00, 0x86, 0x74, 0x01, 0xa9, 0x20, 0x23, 0xd7,
                0xba, 0x99, 0xcb, 0x5f,
            ],
            seconds_per_slot: 12,
            slots_per_epoch: 32,
            forks: ForkSchedule::new(vec![
                ForkActivation { fork: Fork::Phase0, epoch: 0, version: [0x10, 0, 9, 0x10] },
                ForkActivation { fork: Fork::Altair, epoch: 0, version: [0x20, 0, 9, 0x10] },
                ForkActivation { fork: Fork::Bellatrix, epoch: 0, version: [0x30, 0, 9, 0x10] },
                ForkActivation { fork: Fork::Capella, epoch: 0, version: [0x40, 0, 9, 0x10] },
                ForkActivation { fork: Fork::Deneb, epoch: 0, version: [0x50, 0, 9, 0x10] },
                ForkActivation { fork: Fork::Electra, epoch: 2048, version: [0x60, 0, 9, 0x10] },
                ForkActivation { fork: Fork::Fulu, epoch: 50688, version: [0x70, 0, 9, 0x10] },
                ForkActivation { fork: Fork::Gloas, epoch: 132352, version: [0x80, 0, 9, 0x10] },
            ])
            .expect("static monotonic schedule"),
            blob_schedule: vec![
                BlobParameters { epoch: 52480, max_blobs_per_block: 15 },
                BlobParameters { epoch: 54016, max_blobs_per_block: 21 },
            ],
            max_payload_size: 10 * 1024 * 1024,
        }
    }

    /// Canonical mainnet genesis identity and schedule through the Gloas target.
    ///
    /// Gloas has a reserved version and remains disabled at the far-future sentinel.
    pub fn mainnet() -> Self {
        Self {
            name: "mainnet".to_owned(),
            preset: Preset::Mainnet,
            chain_id: 1,
            genesis_time: 1606824023,
            genesis_validators_root: [
                0x4b, 0x36, 0x3d, 0xb9, 0x4e, 0x28, 0x61, 0x20, 0xd7, 0x6e, 0xb9, 0x05, 0x34, 0x0f,
                0xdd, 0x4e, 0x54, 0xbf, 0xe9, 0xf0, 0x6b, 0xf3, 0x3f, 0xf6, 0xcf, 0x5a, 0xd2, 0x7f,
                0x51, 0x1b, 0xfe, 0x95,
            ],
            seconds_per_slot: 12,
            slots_per_epoch: 32,
            forks: ForkSchedule::new(vec![
                ForkActivation { fork: Fork::Phase0, epoch: 0, version: [0, 0, 0, 0] },
                ForkActivation { fork: Fork::Altair, epoch: 74240, version: [1, 0, 0, 0] },
                ForkActivation { fork: Fork::Bellatrix, epoch: 144896, version: [2, 0, 0, 0] },
                ForkActivation { fork: Fork::Capella, epoch: 194048, version: [3, 0, 0, 0] },
                ForkActivation { fork: Fork::Deneb, epoch: 269568, version: [4, 0, 0, 0] },
                ForkActivation { fork: Fork::Electra, epoch: 364032, version: [5, 0, 0, 0] },
                ForkActivation { fork: Fork::Fulu, epoch: 411392, version: [6, 0, 0, 0] },
                ForkActivation { fork: Fork::Gloas, epoch: u64::MAX, version: [7, 0, 0, 0] },
            ])
            .expect("static monotonic mainnet schedule"),
            blob_schedule: vec![
                BlobParameters { epoch: 412672, max_blobs_per_block: 15 },
                BlobParameters { epoch: 419072, max_blobs_per_block: 21 },
            ],
            max_payload_size: 10 * 1024 * 1024,
        }
    }
    /// Reject unsafe clocks, malformed schedules, and conflicting blob activations.
    pub fn validate(&self) -> Result<(), ConfigError> {
        self.forks.validate()?;
        if self.seconds_per_slot == 0 || self.slots_per_epoch == 0 || self.max_payload_size == 0 {
            return Err(ConfigError::InvalidParameters);
        }
        let fulu = self.forks.activations().iter().find(|a| a.fork == Fork::Fulu);
        if self.blob_schedule.windows(2).any(|w| w[0].epoch >= w[1].epoch) ||
            self.blob_schedule
                .iter()
                .any(|b| b.max_blobs_per_block > 4096 || fulu.is_none_or(|f| b.epoch < f.epoch))
        {
            return Err(ConfigError::InvalidBlobSchedule);
        }
        self.seconds_per_slot.checked_mul(1000).ok_or(ConfigError::Overflow)?;
        self.genesis_time.checked_mul(1000).ok_or(ConfigError::Overflow)?;
        Ok(())
    }

    /// Scheduled fork at an epoch.
    pub fn fork_at_epoch(&self, epoch: Epoch) -> Result<ForkActivation, ConfigError> {
        self.validate()?;
        self.forks.at_epoch(epoch)
    }

    /// Scheduled fork at a slot.
    pub fn fork_at_slot(&self, slot: Slot) -> Result<ForkActivation, ConfigError> {
        self.validate()?;
        self.fork_at_epoch(slot / self.slots_per_epoch)
    }

    /// Network clock with explicit before-genesis handling.
    pub fn clock(&self) -> Result<SlotClock, ConfigError> {
        self.validate()?;
        SlotClock::new(self.genesis_time, self.seconds_per_slot, self.slots_per_epoch)
    }

    /// Active blob parameters, including the pre-schedule Electra fallback.
    pub fn blob_parameters(&self, epoch: Epoch) -> Result<BlobParameters, ConfigError> {
        self.validate()?;
        if let Some(parameters) = self.blob_schedule.iter().rev().find(|b| b.epoch <= epoch) {
            return Ok(*parameters);
        }
        let electra = self.forks.activations().iter().find(|a| a.fork == Fork::Electra);
        let activation = self.fork_at_epoch(epoch)?;
        Ok(BlobParameters {
            epoch: if activation.fork >= Fork::Electra {
                electra.map_or(0, |a| a.epoch)
            } else {
                self.forks
                    .activations()
                    .iter()
                    .find(|a| a.fork == Fork::Deneb)
                    .map_or(0, |a| a.epoch)
            },
            max_blobs_per_block: if activation.fork >= Fork::Electra {
                9
            } else if activation.fork >= Fork::Deneb {
                6
            } else {
                0
            },
        })
    }

    /// P2P digest with the blob schedule mixed in starting at Fulu.
    pub fn fork_digest(&self, epoch: Epoch) -> Result<[u8; 4], ConfigError> {
        let activation = self.fork_at_epoch(epoch)?;
        let mut version = [0; 32];
        version[..4].copy_from_slice(&activation.version);
        let mut root = hash_pair(version, self.genesis_validators_root);
        if activation.fork >= Fork::Fulu {
            let parameters = self.blob_parameters(epoch)?;
            let mut encoded = [0; 16];
            encoded[..8].copy_from_slice(&parameters.epoch.to_le_bytes());
            encoded[8..].copy_from_slice(&parameters.max_blobs_per_block.to_le_bytes());
            for (byte, mask) in root.iter_mut().zip(sha256(&encoded)) {
                *byte ^= mask;
            }
        }
        Ok([root[0], root[1], root[2], root[3]])
    }
}

/// A clock whose inputs can be supplied deterministically in tests.
#[derive(Clone, Copy, Debug)]
pub struct SlotClock {
    genesis_time_ms: u64,
    slot_duration_ms: u64,
    slots_per_epoch: u64,
}

impl SlotClock {
    /// Construct without wall-clock reads.
    pub fn new(
        genesis_time: u64,
        seconds_per_slot: u64,
        slots_per_epoch: u64,
    ) -> Result<Self, ConfigError> {
        if seconds_per_slot == 0 || slots_per_epoch == 0 {
            return Err(ConfigError::InvalidParameters);
        }
        Ok(Self {
            genesis_time_ms: genesis_time.checked_mul(1000).ok_or(ConfigError::Overflow)?,
            slot_duration_ms: seconds_per_slot.checked_mul(1000).ok_or(ConfigError::Overflow)?,
            slots_per_epoch,
        })
    }

    /// Slot at Unix milliseconds; time before genesis has no current slot.
    pub fn slot_at(&self, unix_ms: u64) -> Option<Slot> {
        unix_ms.checked_sub(self.genesis_time_ms).map(|delta| delta / self.slot_duration_ms)
    }

    /// Start time of a slot, rejecting arithmetic overflow.
    pub fn start_of_slot(&self, slot: Slot) -> Result<u64, ConfigError> {
        slot.checked_mul(self.slot_duration_ms)
            .and_then(|delta| self.genesis_time_ms.checked_add(delta))
            .ok_or(ConfigError::Overflow)
    }

    /// Epoch of a slot.
    pub const fn epoch_at_slot(&self, slot: Slot) -> Epoch {
        slot / self.slots_per_epoch
    }

    /// First slot of an epoch, rejecting arithmetic overflow.
    pub fn start_slot_at_epoch(&self, epoch: Epoch) -> Result<Slot, ConfigError> {
        epoch.checked_mul(self.slots_per_epoch).ok_or(ConfigError::Overflow)
    }
}

/// Configuration errors.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum ConfigError {
    /// Invalid Fork Schedule.
    #[error("fork schedule must start at genesis and increase monotonically")]
    InvalidForkSchedule,
    /// Duplicate Fork Version.
    #[error("fork versions must be unique")]
    DuplicateForkVersion,
    /// Invalid Parameters.
    #[error("slot duration, epoch length, and payload size must be nonzero")]
    InvalidParameters,
    /// Invalid Blob Schedule.
    #[error("invalid blob schedule")]
    InvalidBlobSchedule,
    /// Overflow.
    #[error("configuration arithmetic overflow")]
    Overflow,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hoodi_announced_fork_boundaries() {
        let config = ChainConfig::hoodi();
        config.validate().unwrap();
        assert_eq!(config.fork_at_epoch(0).unwrap().fork, Fork::Deneb);
        assert_eq!(config.fork_at_epoch(2047).unwrap().fork, Fork::Deneb);
        assert_eq!(config.fork_at_epoch(2048).unwrap().fork, Fork::Electra);
        assert_eq!(config.fork_at_epoch(50688).unwrap().fork, Fork::Fulu);
        assert_eq!(config.fork_at_epoch(132351).unwrap().fork, Fork::Fulu);
        assert_eq!(
            config.fork_at_epoch(132352).unwrap(),
            ForkActivation { fork: Fork::Gloas, epoch: 132352, version: [0x80, 0, 9, 0x10] }
        );
        assert_eq!(config.fork_at_slot(132352 * 32 - 1).unwrap().fork, Fork::Fulu);
        assert_eq!(config.fork_at_slot(132352 * 32).unwrap().fork, Fork::Gloas);
        assert_eq!(config.fork_at_epoch(u64::MAX).unwrap().fork, Fork::Gloas);
        let clock = config.clock().unwrap();
        assert_eq!(
            clock.start_of_slot(clock.start_slot_at_epoch(132352).unwrap()),
            Ok(1_793_036_568_000)
        );
        assert_eq!(config.blob_parameters(0).unwrap().max_blobs_per_block, 6);
        assert_eq!(config.blob_parameters(2048).unwrap().max_blobs_per_block, 9);
        assert_eq!(config.blob_parameters(54016).unwrap().max_blobs_per_block, 21);
        assert_ne!(config.fork_digest(52479), config.fork_digest(52480));
        assert_ne!(config.fork_digest(54015), config.fork_digest(54016));
    }

    #[test]
    fn fork_digests_match_independent_reference_values() {
        // Values calculated from ForkData SSZ and the pinned blob-parameter XOR rule.
        let config = ChainConfig::hoodi();
        for (epoch, digest) in [
            (0, [0xd2, 0xf1, 0x99, 0x7f]),
            (2048, [0x82, 0x55, 0x6a, 0x32]),
            (50688, [0xe2, 0xab, 0xcc, 0xa4]),
            (52480, [0xae, 0x9f, 0x70, 0xa0]),
            (54016, [0xc6, 0xec, 0xb7, 0x6c]),
            (132352, [0x5a, 0xd3, 0x01, 0x29]),
        ] {
            assert_eq!(config.fork_digest(epoch), Ok(digest));
        }
    }

    #[test]
    fn mainnet_matches_canonical_genesis_and_fork_boundaries() {
        let config = ChainConfig::mainnet();
        config.validate().unwrap();
        assert_eq!(config.chain_id, 1);
        assert_eq!(config.preset, Preset::Mainnet);
        assert_eq!(config.genesis_time, 1606824023);
        assert_eq!(
            config.genesis_validators_root,
            [
                0x4b, 0x36, 0x3d, 0xb9, 0x4e, 0x28, 0x61, 0x20, 0xd7, 0x6e, 0xb9, 0x05, 0x34, 0x0f,
                0xdd, 0x4e, 0x54, 0xbf, 0xe9, 0xf0, 0x6b, 0xf3, 0x3f, 0xf6, 0xcf, 0x5a, 0xd2, 0x7f,
                0x51, 0x1b, 0xfe, 0x95,
            ]
        );
        assert_eq!(config.clock().unwrap().slot_at(1606824022999), None);
        assert_eq!(config.clock().unwrap().slot_at(1606824023000), Some(0));
        let mut previous = Fork::Phase0;
        for (fork, epoch, version) in [
            (Fork::Altair, 74240, [1, 0, 0, 0]),
            (Fork::Bellatrix, 144896, [2, 0, 0, 0]),
            (Fork::Capella, 194048, [3, 0, 0, 0]),
            (Fork::Deneb, 269568, [4, 0, 0, 0]),
            (Fork::Electra, 364032, [5, 0, 0, 0]),
            (Fork::Fulu, 411392, [6, 0, 0, 0]),
        ] {
            assert_eq!(config.fork_at_epoch(epoch - 1).unwrap().fork, previous);
            assert_eq!(
                config.fork_at_epoch(epoch).unwrap(),
                ForkActivation { fork, epoch, version }
            );
            assert_eq!(config.fork_at_slot(epoch * 32 - 1).unwrap().fork, previous);
            assert_eq!(config.fork_at_slot(epoch * 32).unwrap().fork, fork);
            previous = fork;
        }
    }

    #[test]
    fn mainnet_blob_upgrades_change_digest_without_activating_gloas() {
        let config = ChainConfig::mainnet();
        assert_eq!(config.blob_parameters(269567).unwrap().max_blobs_per_block, 0);
        assert_eq!(config.blob_parameters(269568).unwrap().max_blobs_per_block, 6);
        assert_eq!(config.blob_parameters(364032).unwrap().max_blobs_per_block, 9);
        assert_eq!(config.blob_parameters(412671).unwrap().max_blobs_per_block, 9);
        assert_eq!(config.blob_parameters(412672).unwrap().max_blobs_per_block, 15);
        assert_eq!(config.blob_parameters(419071).unwrap().max_blobs_per_block, 15);
        assert_eq!(config.blob_parameters(419072).unwrap().max_blobs_per_block, 21);
        assert_ne!(config.fork_digest(412671), config.fork_digest(412672));
        assert_ne!(config.fork_digest(419071), config.fork_digest(419072));
        assert_eq!(config.fork_at_epoch(u64::MAX).unwrap().fork, Fork::Fulu);
        assert_eq!(config.fork_at_slot(u64::MAX).unwrap().fork, Fork::Fulu);
        assert_eq!(
            config.forks.activations().last().unwrap(),
            &ForkActivation { fork: Fork::Gloas, epoch: u64::MAX, version: [7, 0, 0, 0] }
        );
        assert_ne!(config.genesis_validators_root, ChainConfig::hoodi().genesis_validators_root);
        assert_ne!(config.fork_digest(0), ChainConfig::hoodi().fork_digest(0));
    }

    #[test]
    fn far_future_schedule_entry_stays_disabled_at_maximum_epoch() {
        let schedule = ForkSchedule::new(vec![
            ForkActivation { fork: Fork::Phase0, epoch: 0, version: [0, 0, 0, 0] },
            ForkActivation { fork: Fork::Gloas, epoch: u64::MAX, version: [7, 0, 0, 0] },
        ])
        .unwrap();
        assert_eq!(schedule.at_epoch(u64::MAX).unwrap().fork, Fork::Phase0);
        assert_eq!(schedule.activations()[1].epoch, u64::MAX);
    }
    #[test]
    fn clocks_handle_genesis_and_overflow() {
        let clock = SlotClock::new(100, 12, 32).unwrap();
        assert_eq!(clock.slot_at(99999), None);
        assert_eq!(clock.slot_at(100000), Some(0));
        assert_eq!(clock.slot_at(112000), Some(1));
        assert_eq!(clock.start_slot_at_epoch(2), Ok(64));
        assert_eq!(clock.start_of_slot(u64::MAX), Err(ConfigError::Overflow));
    }

    #[test]
    fn deserialized_schedule_must_be_revalidated() {
        let malformed = ForkSchedule { activations: vec![] };
        assert_eq!(malformed.validate(), Err(ConfigError::InvalidForkSchedule));
        assert_eq!(malformed.at_epoch(0), Err(ConfigError::InvalidForkSchedule));
        let mut zero_slots = ChainConfig::hoodi();
        zero_slots.slots_per_epoch = 0;
        assert_eq!(zero_slots.fork_at_slot(1), Err(ConfigError::InvalidParameters));
        assert!(ChainConfig::hoodi().forks.with_gloas(50687, [0x80, 0, 9, 0x10]).is_err());
    }
}
