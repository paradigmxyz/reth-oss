//! Bounded checkpoint authentication using complete fork-specific SSZ schemas.

use crate::{BootstrapError, CheckpointEvidence, CheckpointVerifier};
use reth_embedded_beacon_config::{ChainConfig, Preset as ConfigPreset};
use reth_embedded_beacon_primitives::SszError;
use reth_embedded_beacon_state_transition::ssz::{
    BeaconBlock, BeaconState, Preset, SchemaError, SignedBeaconBlock,
};

/// Explicit wire container supplied by a checkpoint provider.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum CheckpointBlockFormat {
    /// Unsigned `BeaconBlock`; its tree root identifies the trusted checkpoint.
    Unsigned,
    /// `SignedBeaconBlock`; the message root identifies the trusted checkpoint.
    Signed,
}

/// Input limits checked before allocating a decoded checkpoint.
#[derive(Clone, Copy, Debug)]
pub struct BootstrapDecodeLimits {
    /// Maximum complete state encoding size.
    pub max_state_bytes: usize,
    /// Maximum complete block encoding size.
    pub max_block_bytes: usize,
}

impl Default for BootstrapDecodeLimits {
    fn default() -> Self {
        Self { max_state_bytes: 512 * 1024 * 1024, max_block_bytes: 10 * 1024 * 1024 }
    }
}

/// Canonical SSZ authentication under an explicit, checked network configuration.
///
/// This verifies state commitment and trusted unsigned block identity. It does
/// not replace transition validation. The concrete checkpoint entry point also
/// enforces the fork-specific state-dependent trust period.
/// State and block codecs do not establish transition validity.
#[derive(Debug)]
pub struct SszCheckpointVerifier {
    config: ChainConfig,
    limits: BootstrapDecodeLimits,
    block_format: CheckpointBlockFormat,
}

impl SszCheckpointVerifier {
    /// Check the network preset, clock and bounded decoding policy.
    pub fn new(
        config: ChainConfig,
        limits: BootstrapDecodeLimits,
        block_format: CheckpointBlockFormat,
    ) -> Result<Self, BootstrapError> {
        config.validate().map_err(|_| BootstrapError::InvalidConfiguration)?;
        let preset_epoch = match config.preset {
            ConfigPreset::Mainnet => 32,
            ConfigPreset::Minimal => 8,
        };
        if config.slots_per_epoch != preset_epoch ||
            limits.max_state_bytes == 0 ||
            limits.max_block_bytes == 0
        {
            return Err(BootstrapError::InvalidConfiguration)
        }
        Ok(Self { config, limits, block_format })
    }

    /// Authenticate a checkpoint using this network's preset and current slot clock.
    ///
    /// The age limit is an operator cap in addition to the authenticated state
    /// dependent weak-subjectivity period.
    pub fn verify_checkpoint(
        &self,
        state_ssz: &[u8],
        block_ssz: &[u8],
        trusted: reth_embedded_beacon_primitives::Checkpoint,
        now_unix_ms: u64,
        max_age_epochs: u64,
    ) -> Result<crate::VerifiedCheckpoint, BootstrapError> {
        tracing::debug!(target: "beacon::bootstrap", trusted_epoch = trusted.epoch, state_bytes = state_ssz.len(), block_bytes = block_ssz.len(), max_age_epochs, "[beacon] Authenticating checkpoint inputs");
        let clock = self.config.clock().map_err(|_| BootstrapError::InvalidConfiguration)?;
        let current_slot =
            clock.slot_at(now_unix_ms).ok_or(BootstrapError::InvalidConfiguration)?;
        let (state, evidence) = self.decode_state_and_block(state_ssz, block_ssz)?;
        let current_epoch = clock.epoch_at_slot(current_slot);
        let mut verified = crate::VerifiedCheckpoint::verify(
            &DecodedCheckpointEvidence(evidence),
            state_ssz,
            block_ssz,
            trusted,
            crate::BootstrapSettings {
                network: self.config.genesis_validators_root,
                slots_per_epoch: self.config.slots_per_epoch,
                current_epoch,
                max_age_epochs,
            },
        )?;
        let period =
            crate::weak_subjectivity::verify_checkpoint_age(&state, &self.config, current_epoch)?;
        verified.config_digest = Some(checked_config_digest(&self.config)?);
        tracing::info!(target: "beacon::bootstrap", network = %self.config.name, slot = verified.slot(), epoch = trusted.epoch, weak_subjectivity_period = period.epochs, active_validators = period.active_validator_count, total_active_balance = period.total_active_balance, "[beacon] Authenticated checkpoint state, block linkage and trust period");
        Ok(verified)
    }

    pub(crate) const fn network_root(&self) -> [u8; 32] {
        self.config.genesis_validators_root
    }

    pub(crate) const fn block_format(&self) -> CheckpointBlockFormat {
        self.block_format
    }
    pub(crate) const fn decode_limits(&self) -> BootstrapDecodeLimits {
        self.limits
    }
    const fn preset(&self) -> Preset {
        match self.config.preset {
            ConfigPreset::Mainnet => Preset::Mainnet,
            ConfigPreset::Minimal => Preset::Minimal,
        }
    }
}

impl SszCheckpointVerifier {
    fn decode_state_and_block(
        &self,
        state_ssz: &[u8],
        block_ssz: &[u8],
    ) -> Result<(BeaconState, CheckpointEvidence), BootstrapError> {
        if state_ssz.len() > self.limits.max_state_bytes ||
            block_ssz.len() > self.limits.max_block_bytes
        {
            return Err(BootstrapError::CheckpointTooLarge)
        }
        // Every supported BeaconState starts with genesis_time, genesis root, slot.
        // These bytes choose a schema only; full decoding and root computation
        // below authenticate them.
        let state_slot = slot_at(state_ssz, 40)?;
        let block_slot = match self.block_format {
            CheckpointBlockFormat::Unsigned => slot_at(block_ssz, 0)?,
            CheckpointBlockFormat::Signed => {
                // The fixed section is the message offset followed by a 96-byte
                // signature. Reject a noncanonical offset before schema selection.
                if block_ssz.get(..4) != Some(100u32.to_le_bytes().as_slice()) {
                    return Err(BootstrapError::InvalidCheckpointSsz(
                        "signed block message offset must be 100".to_owned(),
                    ))
                }
                slot_at(block_ssz, 100)?
            }
        };
        let state_activation = self
            .config
            .fork_at_slot(state_slot)
            .map_err(|_| BootstrapError::InvalidConfiguration)?;
        let block_activation = self
            .config
            .fork_at_slot(block_slot)
            .map_err(|_| BootstrapError::InvalidConfiguration)?;
        let preset = self.preset();
        let state = BeaconState::from_ssz(state_ssz, state_activation.fork, preset)
            .map_err(schema_error)?;
        if state.genesis_validators_root() != self.config.genesis_validators_root ||
            state.genesis_time() != self.config.genesis_time
        {
            return Err(BootstrapError::WrongNetwork)
        }
        let fork = state.fork_info();
        if (fork.current_version, fork.epoch) != (state_activation.version, state_activation.epoch)
        {
            return Err(BootstrapError::StateForkMismatch)
        }
        // Before a non-genesis upgrade, signing domains use the previous version.
        // Consecutive upgrades at the same epoch use the immediately preceding
        // scheduled version. At genesis no unsigned epoch precedes fork.epoch=0.
        if state_activation.epoch != 0 {
            let previous = self
                .config
                .forks
                .activations()
                .iter()
                .take_while(|activation| activation.fork < state_activation.fork)
                .last()
                .ok_or(BootstrapError::InvalidConfiguration)?;
            if fork.previous_version != previous.version {
                return Err(BootstrapError::StateForkMismatch)
            }
        }
        let (block_root, block_header) = match self.block_format {
            CheckpointBlockFormat::Unsigned => {
                let block = BeaconBlock::from_ssz(block_ssz, block_activation.fork, preset)
                    .map_err(schema_error)?;
                (block.hash_tree_root().map_err(ssz_error)?, block.header().map_err(ssz_error)?)
            }
            CheckpointBlockFormat::Signed => {
                let block = SignedBeaconBlock::from_ssz(block_ssz, block_activation.fork, preset)
                    .map_err(schema_error)?;
                (block.message_root().map_err(ssz_error)?, block.header().map_err(ssz_error)?)
            }
        };
        let state_root = state.hash_tree_root().map_err(ssz_error)?;
        let mut latest_header = state.latest_block_header();
        if latest_header.state_root == [0; 32] {
            latest_header.state_root = state_root;
        }
        let evidence = CheckpointEvidence {
            genesis_validators_root: state.genesis_validators_root(),
            state_root,
            state_slot: state.slot(),
            latest_block_root: latest_header.hash_tree_root(),
            block_root,
            block_state_root: block_header.state_root,
            block_slot: block_header.slot,
        };
        Ok((state, evidence))
    }
}

impl CheckpointVerifier for SszCheckpointVerifier {
    fn verify_state_and_block(
        &self,
        state_ssz: &[u8],
        block_ssz: &[u8],
    ) -> Result<CheckpointEvidence, BootstrapError> {
        self.decode_state_and_block(state_ssz, block_ssz).map(|(_, evidence)| evidence)
    }
}

// The same immutable input pair was decoded and hashed once by the concrete verifier.
// Reuse its fixed-size evidence for the existing linkage gate while retaining the state.
struct DecodedCheckpointEvidence(CheckpointEvidence);

impl CheckpointVerifier for DecodedCheckpointEvidence {
    fn verify_state_and_block(
        &self,
        _state_ssz: &[u8],
        _block_ssz: &[u8],
    ) -> Result<CheckpointEvidence, BootstrapError> {
        Ok(self.0.clone())
    }
}

pub(crate) fn checked_config_digest(config: &ChainConfig) -> Result<[u8; 32], BootstrapError> {
    config.validate().map_err(|_| BootstrapError::InvalidConfiguration)?;
    let bytes = serde_json::to_vec(config).map_err(|_| BootstrapError::InvalidConfiguration)?;
    Ok(reth_embedded_beacon_primitives::sha256(&bytes))
}

fn slot_at(bytes: &[u8], offset: usize) -> Result<u64, BootstrapError> {
    let value = bytes
        .get(offset..offset + 8)
        .ok_or_else(|| BootstrapError::InvalidCheckpointSsz("missing slot".to_owned()))?;
    Ok(u64::from_le_bytes(value.try_into().expect("checked eight-byte slice")))
}

fn schema_error(error: SchemaError) -> BootstrapError {
    match error {
        SchemaError::Ssz(error) => ssz_error(error),
    }
}

fn ssz_error(error: SszError) -> BootstrapError {
    BootstrapError::InvalidCheckpointSsz(error.to_string())
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::{weak_subjectivity::WeakSubjectivityError, BootstrapSettings, VerifiedCheckpoint};
    use reth_embedded_beacon_config::{ForkActivation, ForkSchedule};
    use reth_embedded_beacon_primitives::{Checkpoint, Fork};
    use reth_embedded_beacon_state_transition::ssz::phase0;
    use snap::raw::Decoder;

    pub(crate) fn linked_fixture(
    ) -> (phase0::BeaconState, phase0::BeaconBlock, ChainConfig, Checkpoint) {
        // All complete nested data originates in published static SSZ fixtures.
        // The linkage below is a synthetic checkpoint commitment, not a published
        // transition vector or proof that these randomized operations are valid.
        let state_bytes = Decoder::new().decompress_vec(include_bytes!(
            "../../../../testing/embedded-beacon/spec-tests/testdata/official-phase0/minimal/phase0/ssz_static/BeaconState/ssz_lengthy/case_0/serialized.ssz_snappy"
        )).unwrap();
        let block_bytes = Decoder::new().decompress_vec(include_bytes!(
            "../../../../testing/embedded-beacon/spec-tests/testdata/official-phase0/minimal/phase0/ssz_static/BeaconBlock/ssz_lengthy/case_0/serialized.ssz_snappy"
        )).unwrap();
        let mut state = phase0::BeaconState::from_ssz(&state_bytes, Preset::Minimal).unwrap();
        let mut block = phase0::BeaconBlock::from_ssz(&block_bytes, Preset::Minimal).unwrap();
        let config = ChainConfig {
            name: "checkpoint-fixture".to_owned(),
            preset: ConfigPreset::Minimal,
            chain_id: 12345,
            genesis_time: 1606824023,
            genesis_validators_root: [1; 32],
            seconds_per_slot: 6,
            slots_per_epoch: 8,
            forks: ForkSchedule::new(vec![ForkActivation {
                fork: Fork::Phase0,
                epoch: 0,
                version: [0; 4],
            }])
            .unwrap(),
            blob_schedule: vec![],
            max_payload_size: 10 * 1024 * 1024,
        };
        state.genesis_time = config.genesis_time;
        state.genesis_validators_root = config.genesis_validators_root;
        state.slot = 64;
        state.fork =
            phase0::ForkInfo { previous_version: [0; 4], current_version: [0; 4], epoch: 0 };
        // Randomized static data is linked synthetically; give this checkpoint
        // helper a coherent active registry for state-dependent trust-period checks.
        for validator in &mut state.validators.values {
            validator.effective_balance = 32_000_000_000;
            validator.withdrawal_credentials[0] = 0;
            validator.activation_eligibility_epoch = 0;
            validator.activation_epoch = 0;
            validator.exit_epoch = u64::MAX;
            validator.withdrawable_epoch = u64::MAX;
        }
        state.balances.values = vec![32_000_000_000; state.validators.values.len()];
        state.finalized_checkpoint = Checkpoint::default();
        block.slot = state.slot;
        block.state_root = [0; 32];
        state.latest_block_header = block.header(Preset::Minimal).unwrap();
        block.state_root = state.hash_tree_root(Preset::Minimal).unwrap();
        let trusted = Checkpoint { epoch: 8, root: block.hash_tree_root(Preset::Minimal).unwrap() };
        (state, block, config, trusted)
    }

    fn settings(config: &ChainConfig) -> BootstrapSettings {
        BootstrapSettings {
            network: config.genesis_validators_root,
            slots_per_epoch: config.slots_per_epoch,
            current_epoch: 9,
            max_age_epochs: 4,
        }
    }

    #[test]
    fn complete_checkpoint_links_unsigned_message_and_real_state_roots() {
        let (state, block, config, trusted) = linked_fixture();
        let state_bytes = state.to_ssz(Preset::Minimal).unwrap();
        let unsigned_bytes = block.to_ssz(Preset::Minimal).unwrap();
        let signed = phase0::SignedBeaconBlock { message: block.clone(), signature: [7; 96] };
        let signed_bytes = signed.to_ssz(Preset::Minimal).unwrap();
        assert_ne!(signed.hash_tree_root(Preset::Minimal).unwrap(), trusted.root);
        for (format, bytes) in [
            (CheckpointBlockFormat::Unsigned, unsigned_bytes.as_slice()),
            (CheckpointBlockFormat::Signed, signed_bytes.as_slice()),
        ] {
            let verifier = SszCheckpointVerifier::new(
                config.clone(),
                BootstrapDecodeLimits::default(),
                format,
            )
            .unwrap();
            let now = config.clock().unwrap().start_of_slot(72).unwrap();
            let checkpoint =
                verifier.verify_checkpoint(&state_bytes, bytes, trusted, now, 4).unwrap();
            assert_eq!(checkpoint.config_digest(), Some(checked_config_digest(&config).unwrap()));
            assert_eq!(checkpoint.state_root(), block.state_root);
            assert_eq!(checkpoint.checkpoint().root, trusted.root);
        }
    }

    #[test]
    fn complete_decoder_rejects_state_tampering_despite_copied_trusted_header() {
        let (mut state, block, config, trusted) = linked_fixture();
        let verifier = SszCheckpointVerifier::new(
            config.clone(),
            BootstrapDecodeLimits::default(),
            CheckpointBlockFormat::Unsigned,
        )
        .unwrap();
        let block_bytes = block.to_ssz(Preset::Minimal).unwrap();
        state.latest_block_header = block.header(Preset::Minimal).unwrap();
        state.balances.values[0] ^= 1;
        let tampered_bytes = state.to_ssz(Preset::Minimal).unwrap();
        let evidence = verifier.verify_state_and_block(&tampered_bytes, &block_bytes).unwrap();
        assert_eq!(evidence.latest_block_root, trusted.root);
        assert_ne!(evidence.state_root, evidence.block_state_root);
        assert_eq!(
            VerifiedCheckpoint::verify(
                &verifier,
                &tampered_bytes,
                &block_bytes,
                trusted,
                settings(&config),
            )
            .unwrap_err(),
            BootstrapError::UnverifiedBoundaryState,
        );
    }

    #[test]
    fn checkpoint_decoder_enforces_network_fork_format_and_preallocation_limits() {
        let (mut state, block, config, _) = linked_fixture();
        let block_bytes = block.to_ssz(Preset::Minimal).unwrap();
        let verifier = SszCheckpointVerifier::new(
            config.clone(),
            BootstrapDecodeLimits::default(),
            CheckpointBlockFormat::Unsigned,
        )
        .unwrap();
        state.fork.current_version = [9; 4];
        assert_eq!(
            verifier
                .verify_state_and_block(&state.to_ssz(Preset::Minimal).unwrap(), &block_bytes)
                .unwrap_err(),
            BootstrapError::StateForkMismatch,
        );
        state.fork.current_version = [0; 4];
        state.genesis_time += 1;
        assert_eq!(
            verifier
                .verify_state_and_block(&state.to_ssz(Preset::Minimal).unwrap(), &block_bytes)
                .unwrap_err(),
            BootstrapError::WrongNetwork,
        );
        let bounded = SszCheckpointVerifier::new(
            config.clone(),
            BootstrapDecodeLimits { max_state_bytes: 1, max_block_bytes: 1 },
            CheckpointBlockFormat::Unsigned,
        )
        .unwrap();
        assert_eq!(
            bounded.verify_state_and_block(&[0; 2], &[]).unwrap_err(),
            BootstrapError::CheckpointTooLarge,
        );
        let signed = SszCheckpointVerifier::new(
            config,
            BootstrapDecodeLimits::default(),
            CheckpointBlockFormat::Signed,
        )
        .unwrap();
        assert!(matches!(
            signed.verify_state_and_block(&state.to_ssz(Preset::Minimal).unwrap(), &block_bytes),
            Err(BootstrapError::InvalidCheckpointSsz(_)),
        ));
    }

    #[test]
    fn authentic_fulu_upgrade_rejects_a_changed_previous_fork_configuration() {
        use reth_embedded_beacon_state_transition::ssz::fulu;
        let state_bytes = Decoder::new()
            .decompress_vec(include_bytes!("../testdata/fulu-upgrade/post.ssz_snappy"))
            .unwrap();
        let state = fulu::BeaconState::from_ssz(&state_bytes, Preset::Minimal).unwrap();
        assert!(state.fork.epoch > 0, "published upgrade must exercise a non-genesis fork");
        assert_ne!(state.fork.previous_version, state.fork.current_version);
        let block_bytes = Decoder::new().decompress_vec(include_bytes!(
            "../../../../testing/embedded-beacon/spec-tests/testdata/official-minimal-upgrades/minimal/fulu/ssz_static/BeaconBlock/ssz_lengthy/case_0/serialized.ssz_snappy"
        )).unwrap();
        let mut block = fulu::BeaconBlock::from_ssz(&block_bytes, Preset::Minimal).unwrap();
        block.slot = state.slot;
        let config = ChainConfig {
            name: "published-fulu-upgrade".to_owned(),
            preset: ConfigPreset::Minimal,
            chain_id: 12345,
            genesis_time: state.genesis_time,
            genesis_validators_root: state.genesis_validators_root,
            seconds_per_slot: 6,
            slots_per_epoch: 8,
            forks: ForkSchedule::new(vec![
                ForkActivation { fork: Fork::Phase0, epoch: 0, version: [99; 4] },
                ForkActivation {
                    fork: Fork::Electra,
                    epoch: 0,
                    version: state.fork.previous_version,
                },
                ForkActivation {
                    fork: Fork::Fulu,
                    epoch: state.fork.epoch,
                    version: state.fork.current_version,
                },
            ])
            .unwrap(),
            blob_schedule: vec![],
            max_payload_size: 10 * 1024 * 1024,
        };
        let verifier = SszCheckpointVerifier::new(
            config.clone(),
            BootstrapDecodeLimits::default(),
            CheckpointBlockFormat::Unsigned,
        )
        .unwrap();
        // This checks configuration binding of the unchanged authentic post-state.
        // The separate randomized block is not an asserted valid checkpoint pair.
        let block_bytes = block.to_ssz(Preset::Minimal).unwrap();
        assert!(verifier.verify_state_and_block(&state_bytes, &block_bytes).is_ok());
        let mut activations = config.forks.activations().to_vec();
        activations[1].version[0] ^= 1;
        let inconsistent = ChainConfig { forks: ForkSchedule::new(activations).unwrap(), ..config };
        let verifier = SszCheckpointVerifier::new(
            inconsistent,
            BootstrapDecodeLimits::default(),
            CheckpointBlockFormat::Unsigned,
        )
        .unwrap();
        assert_eq!(
            verifier.verify_state_and_block(&state_bytes, &block_bytes).unwrap_err(),
            BootstrapError::StateForkMismatch,
        );
    }

    #[test]
    fn concrete_checkpoint_requires_operator_cap_and_state_dependent_age() {
        let (state, block, config, trusted) = linked_fixture();
        let verifier = SszCheckpointVerifier::new(
            config.clone(),
            BootstrapDecodeLimits::default(),
            CheckpointBlockFormat::Unsigned,
        )
        .unwrap();
        let state_bytes = state.to_ssz(Preset::Minimal).unwrap();
        let block_bytes = block.to_ssz(Preset::Minimal).unwrap();
        let at_epoch = |epoch| config.clock().unwrap().start_of_slot(epoch * 8_u64).unwrap();
        assert!(verifier
            .verify_checkpoint(&state_bytes, &block_bytes, trusted, at_epoch(264), 1000)
            .is_ok());
        assert_eq!(
            verifier
                .verify_checkpoint(&state_bytes, &block_bytes, trusted, at_epoch(265), 1000)
                .unwrap_err(),
            BootstrapError::WeakSubjectivity(WeakSubjectivityError::OutsidePeriod {
                checkpoint_epoch: 8,
                current_epoch: 265,
                period_epochs: 256
            })
        );
        assert_eq!(
            verifier
                .verify_checkpoint(&state_bytes, &block_bytes, trusted, at_epoch(10), 1)
                .unwrap_err(),
            BootstrapError::StaleCheckpoint
        );
        let wrong_root = Checkpoint { root: [9; 32], ..trusted };
        assert_eq!(
            verifier
                .verify_checkpoint(&state_bytes, &block_bytes, wrong_root, at_epoch(265), 1000)
                .unwrap_err(),
            BootstrapError::WrongTrustedRoot
        );
    }
}

#[cfg(feature = "gossip-validation")]
pub(crate) mod boundary_replay;

#[cfg(feature = "gossip-validation")]
pub use boundary_replay::ReplayedCheckpoint;

#[cfg(feature = "gossip-validation")]
pub(crate) mod genesis;

#[cfg(feature = "gossip-validation")]
pub use genesis::{GenesisError, VerifiedGenesis};
