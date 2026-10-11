//! Phase0 genesis initialization with ordered deposit proofs and mandatory signatures.
//!
//! A fixed-size Merkle frontier computes each deposit prefix root in linear total work.
//! Registry indexing avoids repeated validator scans, and the output is published only on success.

use super::{
    add, block::deposit_kernel_with_index, checked_constants, is_active, Phase0Parameters,
    ProcessingError, EFFECTIVE_BALANCE_INCREMENT, MAX_EFFECTIVE_BALANCE,
};
use crate::ssz::{
    phase0::{BeaconBlockBody, BeaconState, Deposit, ForkInfo},
    Preset, Ssz,
};
use reth_embedded_beacon_primitives::{hash_pair, mix_in_length, Root};
use std::collections::{btree_map::Entry, BTreeMap};

/// Chain configuration for the initial state and minimum genesis conditions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GenesisParameters {
    /// Exact Phase0 preset and deposit-signature fork version.
    pub phase0: Phase0Parameters,
    /// Delay added to the execution deposit block timestamp.
    pub genesis_delay: u64,
    /// Earliest permitted genesis time.
    pub min_genesis_time: u64,
    /// Required validators active at epoch zero.
    pub min_genesis_active_validator_count: u64,
}

impl GenesisParameters {
    /// Pinned preset defaults; a network may provide its own configuration.
    pub const fn for_preset(preset: Preset) -> Self {
        Self {
            phase0: Phase0Parameters::for_preset(preset),
            genesis_delay: match preset {
                Preset::Mainnet => 604_800,
                Preset::Minimal => 300,
            },
            min_genesis_time: match preset {
                Preset::Mainnet => 1_606_824_000,
                Preset::Minimal => 1_578_009_600,
            },
            min_genesis_active_validator_count: match preset {
                Preset::Mainnet => 16_384,
                Preset::Minimal => 64,
            },
        }
    }

    /// Validate configuration without initializing any state.
    pub const fn validate(&self) -> Result<(), ProcessingError> {
        if self.min_genesis_active_validator_count > 1_099_511_627_776 {
            return Err(ProcessingError::InvalidParameters(
                "minimum genesis validator count exceeds registry limit",
            ))
        }
        self.phase0.validate()
    }
}

/// Initialize a candidate genesis state with real proof-of-possession verification.
///
/// Every deposit is checked against its ordered prefix root. Invalid new-key signatures
/// consume their deposits without registering a validator, as required by the protocol.
/// Callers must bound the received deposit input before allocating its decoded representation.
pub fn initialize_beacon_state_from_eth1(
    eth1_block_hash: Root,
    eth1_timestamp: u64,
    deposits: &[Deposit],
    parameters: &GenesisParameters,
) -> Result<BeaconState, ProcessingError> {
    parameters.validate()?;
    let preset = parameters.phase0.preset;
    let deposit_count = u64::try_from(deposits.len())
        .map_err(|_| ProcessingError::Arithmetic("genesis deposit count"))?;
    if deposit_count > DEPOSIT_TREE_CAPACITY {
        return Err(ProcessingError::InvalidState("genesis deposit tree limit reached"))
    }
    let mut state = BeaconState::default_value(preset)?;
    state.genesis_time = add(eth1_timestamp, parameters.genesis_delay, "genesis time")?;
    state.fork = ForkInfo {
        previous_version: parameters.phase0.genesis_fork_version,
        current_version: parameters.phase0.genesis_fork_version,
        epoch: 0,
    };
    state.eth1_data.deposit_count = deposit_count;
    state.eth1_data.block_hash = eth1_block_hash;
    state.latest_block_header.body_root =
        BeaconBlockBody::default_value(preset)?.hash_tree_root(preset)?;
    state.randao_mixes.values.fill(eth1_block_hash);
    let mut frontier = DepositFrontier::new();
    let mut registry = BTreeMap::new();
    for deposit in deposits {
        // Validate complete typed proof length before the fixed-depth branch verifier.
        deposit.hash_tree_root(preset)?;
        state.eth1_data.deposit_root = frontier.append(deposit.data.hash_tree_root(preset)?)?;
        let entry = registry.entry(deposit.data.pubkey);
        let known_index = match &entry {
            Entry::Occupied(value) => Some(*value.get()),
            Entry::Vacant(_) => None,
        };
        let previous_count = state.validators.values.len();
        deposit_kernel_with_index::<true>(&mut state, deposit, &parameters.phase0, known_index)?;
        if state.validators.values.len() > previous_count &&
            let Entry::Vacant(value) = entry
        {
            value.insert(previous_count);
        }
    }
    for (validator, balance) in state.validators.values.iter_mut().zip(&state.balances.values) {
        validator.effective_balance =
            (balance - balance % EFFECTIVE_BALANCE_INCREMENT).min(MAX_EFFECTIVE_BALANCE);
        if validator.effective_balance == MAX_EFFECTIVE_BALANCE {
            validator.activation_eligibility_epoch = 0;
            validator.activation_epoch = 0;
        }
    }
    state.genesis_validators_root = state.validators.hash_tree_root(preset)?;
    checked_constants(&state, &parameters.phase0)?;
    Ok(state)
}

/// Check the exact minimum timestamp and active-validator genesis conditions.
pub fn is_valid_genesis_state(
    state: &BeaconState,
    parameters: &GenesisParameters,
) -> Result<bool, ProcessingError> {
    parameters.validate()?;
    checked_constants(state, &parameters.phase0)?;
    if state.genesis_time < parameters.min_genesis_time {
        return Ok(false)
    }
    let active_count =
        state.validators.values.iter().filter(|validator| is_active(validator, 0)).count();
    let active_count = u64::try_from(active_count)
        .map_err(|_| ProcessingError::Arithmetic("genesis active validator count"))?;
    Ok(active_count >= parameters.min_genesis_active_validator_count)
}

const DEPOSIT_TREE_DEPTH: usize = 32;
const DEPOSIT_TREE_CAPACITY: u64 = 1 << DEPOSIT_TREE_DEPTH;

#[derive(Clone, Debug)]
struct DepositFrontier {
    branches: [Root; DEPOSIT_TREE_DEPTH + 1],
    zeros: [Root; DEPOSIT_TREE_DEPTH],
    count: u64,
}

impl DepositFrontier {
    fn new() -> Self {
        let mut zeros = [[0; 32]; DEPOSIT_TREE_DEPTH];
        for height in 1..DEPOSIT_TREE_DEPTH {
            zeros[height] = hash_pair(zeros[height - 1], zeros[height - 1]);
        }
        Self { branches: [[0; 32]; DEPOSIT_TREE_DEPTH + 1], zeros, count: 0 }
    }

    fn append(&mut self, mut leaf: Root) -> Result<Root, ProcessingError> {
        if self.count >= DEPOSIT_TREE_CAPACITY {
            return Err(ProcessingError::InvalidState("genesis deposit tree limit reached"))
        }
        let mut size = self.count + 1;
        for height in 0..=DEPOSIT_TREE_DEPTH {
            if size & 1 != 0 {
                self.branches[height] = leaf;
                self.count += 1;
                return Ok(self.root())
            }
            leaf = hash_pair(self.branches[height], leaf);
            size >>= 1;
        }
        Err(ProcessingError::InvalidState("genesis deposit frontier is inconsistent"))
    }

    fn root(&self) -> Root {
        let node = if self.count == DEPOSIT_TREE_CAPACITY {
            self.branches[DEPOSIT_TREE_DEPTH]
        } else {
            let mut node = [0; 32];
            for height in 0..DEPOSIT_TREE_DEPTH {
                node = if (self.count >> height) & 1 != 0 {
                    hash_pair(self.branches[height], node)
                } else {
                    hash_pair(node, self.zeros[height])
                };
            }
            node
        };
        mix_in_length(node, self.count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ssz::phase0::{DepositData, DepositProof};
    use blst::min_pk::SecretKey;
    use reth_embedded_beacon_crypto::{compute_domain, compute_signing_root, ETHEREUM_BLS_DST};
    use reth_embedded_beacon_primitives::{bytes_root, merkleize, uint64_root};

    #[test]
    fn incremental_prefix_roots_match_complete_merkleization() {
        let mut frontier = DepositFrontier::new();
        let mut leaves = Vec::new();
        assert_eq!(frontier.root(), mix_in_length(merkleize(&[], 1 << 32).unwrap(), 0));
        for value in 0..130u64 {
            leaves.push(uint64_root(value));
            let root = frontier.append(*leaves.last().unwrap()).unwrap();
            assert_eq!(root, mix_in_length(merkleize(&leaves, 1 << 32).unwrap(), value + 1));
        }
    }

    #[test]
    fn full_frontier_rejects_append_without_mutation() {
        let mut frontier = DepositFrontier::new();
        frontier.count = DEPOSIT_TREE_CAPACITY - 1;
        frontier.append([5; 32]).unwrap();
        let original = frontier.clone();
        assert!(frontier.append([6; 32]).is_err());
        assert_eq!(frontier.count, original.count);
        assert_eq!(frontier.branches, original.branches);
    }

    #[test]
    fn empty_candidate_has_exact_genesis_fields_and_checked_time() {
        let parameters = GenesisParameters::for_preset(Preset::Minimal);
        let state = initialize_beacon_state_from_eth1([7; 32], 100, &[], &parameters).unwrap();
        assert_eq!(state.genesis_time, 400);
        assert_eq!(state.fork.previous_version, [0, 0, 0, 1]);
        assert_eq!(state.fork.current_version, [0, 0, 0, 1]);
        assert_eq!(state.eth1_data.block_hash, [7; 32]);
        assert_eq!(state.eth1_data.deposit_root, [0; 32]);
        assert!(state.randao_mixes.values.iter().all(|value| *value == [7; 32]));
        assert_eq!(
            state.genesis_validators_root,
            state.validators.hash_tree_root(Preset::Minimal).unwrap()
        );
        assert!(!is_valid_genesis_state(&state, &parameters).unwrap());
        assert!(initialize_beacon_state_from_eth1([0; 32], u64::MAX, &[], &parameters).is_err());
    }

    #[test]
    fn verified_deposits_top_up_activate_and_ignore_bad_new_keys() {
        let mut parameters = GenesisParameters::for_preset(Preset::Minimal);
        parameters.min_genesis_active_validator_count = 1;
        let first = signed_data(11, 16_000_000_000, parameters.phase0.genesis_fork_version);
        let mut second = first.clone();
        second.signature = [0; 96];
        let mut ignored = signed_data(12, 32_000_000_000, parameters.phase0.genesis_fork_version);
        ignored.signature = [0; 96];
        let deposits = deposits_with_prefix_proofs(vec![first, second, ignored], Preset::Minimal);
        let state = initialize_beacon_state_from_eth1(
            [8; 32],
            parameters.min_genesis_time,
            &deposits,
            &parameters,
        )
        .unwrap();
        assert_eq!(state.eth1_deposit_index, 3);
        assert_eq!(state.eth1_data.deposit_count, 3);
        assert_eq!(state.validators.values.len(), 1);
        assert_eq!(state.balances.values, vec![32_000_000_000]);
        assert_eq!(state.validators.values[0].activation_epoch, 0);
        assert!(is_valid_genesis_state(&state, &parameters).unwrap());
        let mut early = state.clone();
        early.genesis_time = parameters.min_genesis_time - 1;
        assert!(!is_valid_genesis_state(&early, &parameters).unwrap());
        parameters.min_genesis_active_validator_count = 2;
        assert!(!is_valid_genesis_state(&state, &parameters).unwrap());
        let mut malformed = deposits;
        malformed[0].proof.values[0][0] ^= 1;
        assert!(initialize_beacon_state_from_eth1([8; 32], 100, &malformed, &parameters).is_err());
    }

    fn signed_data(seed: u8, amount: u64, version: [u8; 4]) -> DepositData {
        let key = SecretKey::key_gen(&[seed; 32], &[]).unwrap();
        let mut data = DepositData {
            pubkey: key.sk_to_pk().to_bytes(),
            withdrawal_credentials: [9; 32],
            amount,
            signature: [0; 96],
        };
        let message = merkleize(
            &[
                bytes_root(&data.pubkey, 48, false).unwrap(),
                data.withdrawal_credentials,
                uint64_root(data.amount),
            ],
            3,
        )
        .unwrap();
        let root = compute_signing_root(message, compute_domain([3, 0, 0, 0], version, [0; 32]));
        data.signature = key.sign(&root, ETHEREUM_BLS_DST, &[]).to_bytes();
        data
    }

    fn deposits_with_prefix_proofs(data: Vec<DepositData>, preset: Preset) -> Vec<Deposit> {
        let mut leaves = Vec::new();
        let mut deposits = Vec::new();
        for value in data {
            let index = leaves.len();
            leaves.push(value.hash_tree_root(preset).unwrap());
            let mut proof = Vec::new();
            for height in 0..DEPOSIT_TREE_DEPTH {
                let width = 1usize << height;
                let sibling_start = ((index >> height) ^ 1) << height;
                let sibling = if sibling_start < leaves.len() {
                    &leaves[sibling_start..(sibling_start + width).min(leaves.len())]
                } else {
                    &[]
                };
                proof.push(merkleize(sibling, width).unwrap());
            }
            proof.push(uint64_root(leaves.len() as u64));
            deposits.push(Deposit { proof: DepositProof::from_vec(proof), data: value });
        }
        deposits
    }
}
