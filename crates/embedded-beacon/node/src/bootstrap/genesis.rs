//! Distinct concrete genesis authentication and original unsigned anchor construction.
//!
//! The original operator-selected genesis file is the initial trust input. Its complete validator
//! registry is authenticated against configured `genesis_validators_root`; that root alone cannot
//! externally commit arbitrary eth1 data or excess deposit balances. Full canonical genesis fields
//! and real scheduled upgrades are reconstructed and compared. Ancient genesis is never converted
//! into a fresh weak-subjectivity checkpoint or a skipped signed-chain validation claim.

use super::{checked_config_digest, BootstrapDecodeLimits};
use crate::runtime_parameters::RuntimeParameters;
use reth_embedded_beacon_config::{ChainConfig, Preset as ConfigPreset};
use reth_embedded_beacon_crypto::validate_public_key;
use reth_embedded_beacon_primitives::{sha256, Fork, Root};
use reth_embedded_beacon_state_transition::{
    fork_processing,
    phase0_processing::ProcessingError,
    ssz::{
        altair, bellatrix, capella, deneb, electra, fulu, gloas, phase0, BeaconBlock, BeaconState,
        List, Preset, Ssz,
    },
};
use std::{collections::BTreeMap, sync::Arc};
use thiserror::Error;

/// Sealed original slot-zero inputs for a separate forward-validation startup path.
#[derive(Clone, Debug)]
pub struct VerifiedGenesis {
    state: Arc<BeaconState>,
    block: Arc<BeaconBlock>,
    state_ssz: Arc<[u8]>,
    block_ssz: Arc<[u8]>,
    config_digest: Root,
    state_root: Root,
    block_root: Root,
    input_digest: Root,
    fork: Fork,
}
impl VerifiedGenesis {
    /// Decode and authenticate the original genesis on a dedicated owner/blocking thread.
    /// No caller root, status, fork hint, checkpoint age or boolean can substitute for these
    /// checks.
    pub fn authenticate(
        config: ChainConfig,
        state_ssz: Arc<[u8]>,
        limits: BootstrapDecodeLimits,
    ) -> Result<Self, GenesisError> {
        let config_digest =
            checked_config_digest(&config).map_err(|_| GenesisError::Configuration)?;
        let preset = match config.preset {
            ConfigPreset::Mainnet => Preset::Mainnet,
            ConfigPreset::Minimal => Preset::Minimal,
        };
        let parameters = RuntimeParameters::from_config(&config)?.transitions;
        if limits.max_state_bytes == 0 ||
            limits.max_block_bytes == 0 ||
            state_ssz.len() > limits.max_state_bytes
        {
            return Err(GenesisError::Bounds)
        }
        let active = config.fork_at_slot(0).map_err(|_| GenesisError::Configuration)?;
        let state = BeaconState::from_ssz(&state_ssz, active.fork, preset)
            .map_err(|error| GenesisError::Schema(error.to_string()))?;
        if state.slot() != 0 ||
            state.genesis_time() != config.genesis_time ||
            state.genesis_validators_root() != config.genesis_validators_root
        {
            return Err(GenesisError::Network)
        }
        let info = state.fork_info();
        if info.epoch != 0 || active.epoch != 0 || info.current_version != active.version {
            return Err(GenesisError::Fork)
        }
        let mut seed = canonical_seed(&state, preset)?;
        let initial = parameters.schedule.first().ok_or(GenesisError::Configuration)?.version;
        seed.fork =
            phase0::ForkInfo { previous_version: initial, current_version: initial, epoch: 0 };
        if seed
            .validators
            .hash_tree_root(preset)
            .map_err(|error| GenesisError::Schema(error.to_string()))? !=
            config.genesis_validators_root
        {
            return Err(GenesisError::Registry)
        }
        let mut expected = BeaconState::Phase0(seed, preset);
        fork_processing::upgrade_state_at_slot(&mut expected, &parameters)?;
        if expected.schema_fork() != active.fork {
            return Err(GenesisError::Fork)
        }
        // There is no epoch before genesis. Preserve the actual supported initial fork record,
        // allowing current-version initialization or the real immediately prior scheduled version.
        let expected_previous = expected.fork_info().previous_version;
        if info.previous_version != info.current_version &&
            info.previous_version != expected_previous
        {
            return Err(GenesisError::Fork)
        }
        let state_root =
            state.hash_tree_root().map_err(|error| GenesisError::Schema(error.to_string()))?;
        let block = unsigned_genesis_block(&expected, preset, state_root)?;
        let body_root =
            block.header().map_err(|error| GenesisError::Schema(error.to_string()))?.body_root;
        normalize_expected_header(&mut expected, body_root, info.previous_version);
        if expected != state {
            return Err(GenesisError::CanonicalState)
        }
        let block_root =
            block.hash_tree_root().map_err(|error| GenesisError::Schema(error.to_string()))?;
        let mut header = state.latest_block_header();
        if header.slot != 0 ||
            header.proposer_index != 0 ||
            header.parent_root != [0; 32] ||
            header.state_root != [0; 32] ||
            header.body_root != body_root
        {
            return Err(GenesisError::Header)
        }
        header.state_root = state_root;
        if header.hash_tree_root() != block_root {
            return Err(GenesisError::Header)
        }
        let block_ssz = block.to_ssz().map_err(|error| GenesisError::Schema(error.to_string()))?;
        if block_ssz.len() > limits.max_block_bytes {
            return Err(GenesisError::Bounds)
        }
        tracing::info!(target:"beacon::genesis",fork=?active.fork,state_bytes=state_ssz.len(),
            "[beacon] Authenticated original genesis for forward signed-chain validation");
        Ok(Self {
            state: Arc::new(state),
            block: Arc::new(block),
            input_digest: sha256(&state_ssz),
            state_ssz,
            block_ssz: block_ssz.into(),
            config_digest,
            state_root,
            block_root,
            fork: active.fork,
        })
    }
    /// Actual complete original genesis state commitment.
    pub const fn state_root(&self) -> Root {
        self.state_root
    }
    /// Actual original unsigned genesis block commitment, derived from its exact typed body.
    pub const fn block_root(&self) -> Root {
        self.block_root
    }
    /// Actual schema scheduled at slot zero; resource profiles never select it.
    pub const fn fork(&self) -> Fork {
        self.fork
    }
    /// Exact checked consensus network captured during authentication.
    pub const fn config_digest(&self) -> Root {
        self.config_digest
    }
    /// Original immutable complete state bytes, without relabeling or mutation.
    pub fn original_state_ssz(&self) -> Arc<[u8]> {
        self.state_ssz.clone()
    }
    /// Actual constructed unsigned genesis encoding; no signature is invented.
    pub fn block_ssz(&self) -> Arc<[u8]> {
        self.block_ssz.clone()
    }
    /// Consume sealed originals for the dedicated genesis import owner; never a checkpoint proof.
    pub(crate) fn into_import_inputs(
        self,
        config: &ChainConfig,
    ) -> Result<(Arc<BeaconState>, Arc<BeaconBlock>), GenesisError> {
        if checked_config_digest(config).map_err(|_| GenesisError::Configuration)? !=
            self.config_digest ||
            sha256(&self.state_ssz) != self.input_digest
        {
            return Err(GenesisError::Network)
        }
        Ok((self.state, self.block))
    }
}

/// Explicit full-genesis authentication failures.
#[derive(Debug, Error)]
pub enum GenesisError {
    /// Invalid exact preset, schedule or slot clock.
    #[error("invalid beacon genesis configuration")]
    Configuration,
    /// Original input or actual unsigned body exceeds independently checked decode bounds.
    #[error("beacon genesis input exceeds decode bounds")]
    Bounds,
    /// Slot zero, network time or configured validator identity differs.
    #[error("beacon genesis network or slot mismatch")]
    Network,
    /// Actual slot-zero fork record differs from the checked schedule.
    #[error("beacon genesis fork mismatch")]
    Fork,
    /// Complete canonical pre-upgrade validator registry does not match configured genesis root.
    #[error("beacon genesis validator registry mismatch")]
    Registry,
    /// Native canonical initial fields/scheduled upgrades do not reproduce the complete input.
    #[error("beacon genesis state differs from canonical initialization")]
    CanonicalState,
    /// Actual initial header cannot bind the exact original unsigned genesis body/state.
    #[error("beacon genesis header/body commitment mismatch")]
    Header,
    /// Complete canonical SSZ cannot be decoded or hashed.
    #[error("beacon genesis schema failed: {0}")]
    Schema(String),
    /// Actual scheduled processing/committee/upgrade validation failed.
    #[error(transparent)]
    Processing(#[from] ProcessingError),
}

fn canonical_seed(
    state: &BeaconState,
    preset: Preset,
) -> Result<phase0::BeaconState, GenesisError> {
    let mut seed = phase0::BeaconState::default_value(preset)
        .map_err(|error| GenesisError::Schema(error.to_string()))?;
    macro_rules! common {
        ($state:expr) => {{
            seed.genesis_time = $state.genesis_time;
            seed.genesis_validators_root = $state.genesis_validators_root;
            seed.eth1_data = $state.eth1_data.clone();
            seed.eth1_deposit_index = $state.eth1_data.deposit_count;
            seed.validators = List::from_vec($state.validators.values.clone());
            seed.balances = List::from_vec($state.balances.values.clone());
        }};
    }
    match state {
        BeaconState::Phase0(s, _) => common!(s),
        BeaconState::Altair(s, _) => common!(s),
        BeaconState::Bellatrix(s, _) => common!(s),
        BeaconState::Capella(s, _) => common!(s),
        BeaconState::Deneb(s, _) => common!(s),
        BeaconState::Electra(s, _) => common!(s),
        BeaconState::Fulu(s, _) => common!(s),
        BeaconState::Gloas(s, _) => common!(s),
    }
    if seed.validators.values.is_empty() ||
        seed.validators.values.len() != seed.balances.values.len() ||
        seed.eth1_data.deposit_count < seed.validators.values.len() as u64
    {
        return Err(GenesisError::CanonicalState)
    }
    let mut indices = BTreeMap::new();
    for (index, validator) in seed.validators.values.iter().enumerate() {
        validate_public_key(&validator.pubkey).map_err(|_| GenesisError::Registry)?;
        if indices.insert(validator.pubkey, index).is_some() {
            return Err(GenesisError::Registry)
        }
    }
    // Electra's real genesis upgrade queues inactive and compounding excess balances. Recover
    // only those original balances, then rerun the exact public upgrade and compare every field.
    macro_rules! pending {
        ($state:expr) => {{
            for deposit in &$state.pending_deposits.values {
                let index = *indices.get(&deposit.pubkey).ok_or(GenesisError::CanonicalState)?;
                if deposit.slot != 0 ||
                    deposit.withdrawal_credentials !=
                        seed.validators.values[index].withdrawal_credentials
                {
                    return Err(GenesisError::CanonicalState)
                }
                seed.balances.values[index] = seed.balances.values[index]
                    .checked_add(deposit.amount)
                    .ok_or(GenesisError::CanonicalState)?;
            }
        }};
    }
    match state {
        BeaconState::Electra(s, _) => pending!(s),
        BeaconState::Fulu(s, _) => pending!(s),
        BeaconState::Gloas(s, _) => {
            pending!(s);
            for builder in &s.builders.values {
                let index = *indices.get(&builder.pubkey).ok_or(GenesisError::CanonicalState)?;
                if seed.validators.values[index].withdrawal_credentials[0] != 3 {
                    return Err(GenesisError::CanonicalState)
                }
                seed.balances.values[index] = seed.balances.values[index]
                    .checked_add(builder.balance)
                    .ok_or(GenesisError::CanonicalState)?;
            }
        }
        _ => {}
    }
    let mut active = 0usize;
    for (validator, balance) in seed.validators.values.iter_mut().zip(&seed.balances.values) {
        validator.effective_balance = (balance - balance % 1_000_000_000).min(32_000_000_000);
        let activated = validator.effective_balance == 32_000_000_000;
        active += usize::from(activated);
        validator.slashed = false;
        validator.activation_eligibility_epoch = if activated { 0 } else { u64::MAX };
        validator.activation_epoch = if activated { 0 } else { u64::MAX };
        validator.exit_epoch = u64::MAX;
        validator.withdrawable_epoch = u64::MAX;
    }
    if active == 0 {
        return Err(GenesisError::CanonicalState)
    }
    seed.randao_mixes.values.fill(seed.eth1_data.block_hash);
    seed.latest_block_header.body_root = phase0::BeaconBlockBody::default_value(preset)
        .and_then(|body| body.hash_tree_root(preset))
        .map_err(|error| GenesisError::Schema(error.to_string()))?;
    Ok(seed)
}
fn unsigned_genesis_block(
    state: &BeaconState,
    preset: Preset,
    state_root: Root,
) -> Result<BeaconBlock, GenesisError> {
    macro_rules! empty {
        ($schema:ident,$variant:ident) => {{
            let mut block = $schema::BeaconBlock::default_value(preset)
                .map_err(|error| GenesisError::Schema(error.to_string()))?;
            block.state_root = state_root;
            BeaconBlock::$variant(block, preset)
        }};
    }
    Ok(match state {
        BeaconState::Phase0(..) => empty!(phase0, Phase0),
        BeaconState::Altair(..) => empty!(altair, Altair),
        BeaconState::Bellatrix(..) => empty!(bellatrix, Bellatrix),
        BeaconState::Capella(..) => empty!(capella, Capella),
        BeaconState::Deneb(..) => empty!(deneb, Deneb),
        BeaconState::Electra(..) => empty!(electra, Electra),
        BeaconState::Fulu(..) => empty!(fulu, Fulu),
        BeaconState::Gloas(s, _) => {
            let mut block = gloas::BeaconBlock::default_value(preset)
                .map_err(|error| GenesisError::Schema(error.to_string()))?;
            block.body.signed_execution_payload_bid.message =
                s.latest_execution_payload_bid.clone();
            block.state_root = state_root;
            BeaconBlock::Gloas(block, preset)
        }
    })
}
const fn normalize_expected_header(
    state: &mut BeaconState,
    body_root: Root,
    previous_version: [u8; 4],
) {
    macro_rules! normalize {
        ($state:expr) => {{
            $state.latest_block_header.body_root = body_root;
            $state.fork.previous_version = previous_version;
        }};
    }
    match state {
        BeaconState::Phase0(s, _) => normalize!(s),
        BeaconState::Altair(s, _) => normalize!(s),
        BeaconState::Bellatrix(s, _) => normalize!(s),
        BeaconState::Capella(s, _) => normalize!(s),
        BeaconState::Deneb(s, _) => normalize!(s),
        BeaconState::Electra(s, _) => normalize!(s),
        BeaconState::Fulu(s, _) => normalize!(s),
        BeaconState::Gloas(s, _) => normalize!(s),
    }
}

#[cfg(test)]
pub(crate) mod tests;
