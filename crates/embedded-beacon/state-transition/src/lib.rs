//! Complete fork-aware SSZ schemas, deterministic helpers, and a full-transition gate.
//!
//! Typed states, blocks, and payload envelopes support canonical decoding and roots.
//! Phase0 and Altair kernels process slots, epochs, upgrades and strict signed blocks.
//! Bellatrix import additionally invokes mandatory terminal and execution verification.
//! Capella and Deneb imports require versioned execution checks and exact withdrawals.
//! Electra and Fulu add balance churn, execution requests and cached proposer selection.
//! Gloas imports beacon blocks separately from immutable execution envelope validation.
//! Complete-transition readiness stays disabled until the required protocol matrix passes.

use reth_embedded_beacon_primitives::{hash_pair, sha256, Fork, Root};
use thiserror::Error;

pub mod altair_processing;

pub mod attestation_validation;

pub mod bellatrix_processing;

pub mod capella_processing;

mod common;

#[cfg(feature = "spec-tests")]
mod spec_crypto;

pub mod deneb_processing;

pub mod electra_processing;

pub mod fork_processing;

pub mod fulu_processing;

pub mod gloas_processing;

pub mod gossip_operations;

pub mod light_client;

pub mod phase0_processing;

pub mod rewards;

pub mod ssz;

pub mod sync_committee_gossip;

mod shuffle;

pub use shuffle::compute_shuffled_indices;

/// Whether the full fork transition is implemented and verified.
pub const fn supports_full_transition(_fork: Fork) -> bool {
    false
}

/// Require a complete transition before starting production import.
pub const fn require_full_transition(fork: Fork) -> Result<(), TransitionError> {
    Err(TransitionError::UnsupportedFullTransition(fork))
}

/// Deterministic helper validation errors.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub enum TransitionError {
    /// Unsupported Full Transition.
    #[error("full state transition is not implemented for {0:?}")]
    UnsupportedFullTransition(Fork),
    /// Invalid Shuffle Index.
    #[error("shuffle index {index} is outside count {count}")]
    InvalidShuffleIndex {
        /// Shuffle index being validated.
        index: u64,
        /// Total index count.
        count: u64,
    },
    /// Shuffle Count Limit.
    #[error("shuffle count exceeds the 2^40 specification limit")]
    ShuffleCountLimit,
    /// Invalid Committee.
    #[error("invalid committee index or count")]
    InvalidCommittee,
    /// Overflow.
    #[error("consensus arithmetic overflow")]
    Overflow,
    /// The complete shuffled mapping cannot be allocated on this host.
    #[error("unable to allocate consensus shuffled mapping")]
    AllocationFailed,
}

/// Consensus swap-or-not shuffle, including the specification's 40-bit count bound.
pub fn compute_shuffled_index(
    mut index: u64,
    index_count: u64,
    seed: Root,
    rounds: u8,
) -> Result<u64, TransitionError> {
    if index >= index_count {
        return Err(TransitionError::InvalidShuffleIndex { index, count: index_count });
    }
    if index_count > (1u64 << 40) {
        return Err(TransitionError::ShuffleCountLimit);
    }
    let mut pivot_input = [0; 33];
    pivot_input[..32].copy_from_slice(&seed);
    let mut source_input = [0; 37];
    source_input[..32].copy_from_slice(&seed);
    for round in 0..rounds {
        pivot_input[32] = round;
        let pivot_hash = sha256(&pivot_input);
        let pivot =
            u64::from_le_bytes(pivot_hash[..8].try_into().expect("eight hash bytes")) % index_count;
        let flip = (pivot + index_count - index) % index_count;
        let position = index.max(flip);
        source_input[32] = round;
        source_input[33..].copy_from_slice(&((position / 256) as u32).to_le_bytes());
        let source = sha256(&source_input);
        let byte = source[((position % 256) / 8) as usize];
        if (byte >> (position % 8)) & 1 != 0 {
            index = flip;
        }
    }
    Ok(index)
}

/// Partition shuffled validator indices into a committee.
pub fn compute_committee(
    indices: &[u64],
    seed: Root,
    index: u64,
    count: u64,
    rounds: u8,
) -> Result<Vec<u64>, TransitionError> {
    if count == 0 || index >= count {
        return Err(TransitionError::InvalidCommittee);
    }
    let length = indices.len() as u64;
    let start = ((u128::from(length) * u128::from(index)) / u128::from(count)) as u64;
    let end = ((u128::from(length) * u128::from(index + 1)) / u128::from(count)) as u64;
    let mut committee = shuffle::compute_shuffled_range(start, end, length, seed, rounds)?;
    for position in &mut committee {
        *position = indices[*position as usize];
    }
    Ok(committee)
}

/// Compute a seed using a domain, epoch, and already selected RANDAO mix.
pub fn compute_seed(domain_type: [u8; 4], epoch: u64, randao_mix: Root) -> Root {
    let mut bytes = [0; 44];
    bytes[..4].copy_from_slice(&domain_type);
    bytes[4..12].copy_from_slice(&epoch.to_le_bytes());
    bytes[12..].copy_from_slice(&randao_mix);
    sha256(&bytes)
}

/// Integer square root without overflow over the full uint64 range.
pub fn integer_squareroot(value: u64) -> u64 {
    if value < 2 {
        return value;
    }
    let mut x = value;
    let mut y = x / 2 + x % 2;
    while y < x {
        x = y;
        y = ((u128::from(x) + u128::from(value / x)) / 2) as u64;
    }
    x
}

/// Verify a binary Merkle branch with exact depth and leaf-index checks.
pub fn is_valid_merkle_branch(
    leaf: Root,
    branch: &[Root],
    depth: usize,
    index: u64,
    root: Root,
) -> bool {
    if branch.len() != depth || depth > 64 || (depth < 64 && index >= (1u64 << depth)) {
        return false;
    }
    let mut value = leaf;
    for (level, sibling) in branch.iter().copied().enumerate() {
        value = if (index >> level) & 1 == 1 {
            hash_pair(sibling, value)
        } else {
            hash_pair(value, sibling)
        };
    }
    value == root
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn shuffling_is_a_permutation_across_chunk_boundaries() {
        for length in [1, 2, 255, 256, 257, 1025] {
            let shuffled = (0..length)
                .map(|index| compute_shuffled_index(index, length, [42; 32], 90).unwrap())
                .collect::<BTreeSet<_>>();
            assert_eq!(shuffled, (0..length).collect());
        }
        assert!(compute_shuffled_index(0, 0, [0; 32], 90).is_err());
        assert!(compute_shuffled_index(0, (1 << 40) + 1, [0; 32], 90).is_err());
    }

    #[test]
    fn committees_partition_all_indices_without_overlap() {
        let indices = (0..1000).collect::<Vec<_>>();
        let mut members = BTreeSet::new();
        for index in 0..32 {
            for member in compute_committee(&indices, [3; 32], index, 32, 10).unwrap() {
                assert!(members.insert(member));
            }
        }
        assert_eq!(members, indices.into_iter().collect());
    }

    #[test]
    fn square_root_covers_full_range() {
        for value in [0, 1, 2, 3, 4, 15, 16, 17, u64::MAX] {
            let root = u128::from(integer_squareroot(value));
            assert!(root * root <= u128::from(value));
            assert!((root + 1) * (root + 1) > u128::from(value));
        }
    }

    #[test]
    fn unsupported_transitions_cannot_succeed() {
        for fork in [
            Fork::Phase0,
            Fork::Altair,
            Fork::Bellatrix,
            Fork::Capella,
            Fork::Deneb,
            Fork::Electra,
            Fork::Fulu,
            Fork::Gloas,
        ] {
            assert!(!supports_full_transition(fork));
            assert_eq!(
                require_full_transition(fork),
                Err(TransitionError::UnsupportedFullTransition(fork))
            );
        }
    }
}
