//! Complete consensus shuffling with shared round pivots and source hashes.

use crate::TransitionError;
use reth_embedded_beacon_primitives::{sha256, Root};

/// Compute every swap-or-not shuffled index using shared round hash inputs.
///
/// This is exactly equivalent to `compute_shuffled_index` for all returned entries.
/// Callers must impose their input/resource budget before requesting a complete mapping.
pub fn compute_shuffled_indices(
    count: u64,
    seed: Root,
    rounds: u8,
) -> Result<Vec<u64>, TransitionError> {
    if count == 0 {
        return Err(TransitionError::InvalidShuffleIndex { index: 0, count })
    }
    if count > (1u64 << 40) {
        return Err(TransitionError::ShuffleCountLimit)
    }
    let length = usize::try_from(count).map_err(|_| TransitionError::AllocationFailed)?;
    let mut mapping = Vec::new();
    mapping.try_reserve_exact(length).map_err(|_| TransitionError::AllocationFailed)?;
    mapping.extend(0..count);
    let groups =
        usize::try_from(count.div_ceil(256)).map_err(|_| TransitionError::AllocationFailed)?;
    let mut sources = Vec::new();
    sources.try_reserve_exact(groups).map_err(|_| TransitionError::AllocationFailed)?;
    sources.resize(groups, [0; 32]);
    let mut pivot_input = [0; 33];
    pivot_input[..32].copy_from_slice(&seed);
    let mut source_input = [0; 37];
    source_input[..32].copy_from_slice(&seed);
    for round in 0..rounds {
        pivot_input[32] = round;
        let hash = sha256(&pivot_input);
        let pivot = u64::from_le_bytes(hash[..8].try_into().expect("eight hash bytes")) % count;
        source_input[32] = round;
        for (group, source) in sources.iter_mut().enumerate() {
            let group = u32::try_from(group).map_err(|_| TransitionError::Overflow)?;
            source_input[33..].copy_from_slice(&group.to_le_bytes());
            *source = sha256(&source_input);
        }
        for index in &mut mapping {
            let flip = (pivot + count - *index) % count;
            let position = (*index).max(flip);
            let source = &sources[(position / 256) as usize];
            let byte = source[((position % 256) / 8) as usize];
            if (byte >> (position % 8)) & 1 == 1 {
                *index = flip;
            }
        }
    }
    Ok(mapping)
}

/// Shuffle only a contiguous committee partition, sharing round hashes with bounded scratch.
/// Empty partitions do not invoke an index shuffle, matching the scalar committee behavior.
/// Cache entries are keyed by both group and round; collisions only recompute an exact hash.
pub(crate) fn compute_shuffled_range(
    start: u64,
    end: u64,
    count: u64,
    seed: Root,
    rounds: u8,
) -> Result<Vec<u64>, TransitionError> {
    if start > end || end > count {
        return Err(TransitionError::InvalidShuffleIndex { index: end.max(start), count })
    }
    let length = usize::try_from(end - start).map_err(|_| TransitionError::AllocationFailed)?;
    if length == 0 {
        return Ok(Vec::new())
    }
    if count > (1u64 << 40) {
        return Err(TransitionError::ShuffleCountLimit)
    }
    let mut mapping = Vec::new();
    mapping.try_reserve_exact(length).map_err(|_| TransitionError::AllocationFailed)?;
    mapping.extend(start..end);
    if rounds == 0 {
        return Ok(mapping)
    }
    let mut cache = SourceCache::new(count)?;
    let mut pivot_input = [0; 33];
    pivot_input[..32].copy_from_slice(&seed);
    let mut source_input = [0; 37];
    source_input[..32].copy_from_slice(&seed);
    for round in 0..rounds {
        pivot_input[32] = round;
        let pivot_hash = sha256(&pivot_input);
        let pivot =
            u64::from_le_bytes(pivot_hash[..8].try_into().expect("eight hash bytes")) % count;
        source_input[32] = round;
        for index in &mut mapping {
            let flip = (pivot + count - *index) % count;
            let position = (*index).max(flip);
            // The checked 40-bit count makes the source group exactly representable as u32.
            let source = cache.get((position / 256) as u32, round, &mut source_input);
            let byte = source[((position % 256) / 8) as usize];
            if (byte >> (position % 8)) & 1 != 0 {
                *index = flip;
            }
        }
    }
    Ok(mapping)
}

const MAX_SOURCE_CACHE_ENTRIES: usize = 1_024;

struct SourceCache {
    entries: Vec<SourceEntry>,
}

#[derive(Clone, Copy)]
struct SourceEntry {
    group: u32,
    round: u8,
    root: Root,
}

impl SourceCache {
    fn new(count: u64) -> Result<Self, TransitionError> {
        let length = count.div_ceil(256).min(MAX_SOURCE_CACHE_ENTRIES as u64) as usize;
        let mut entries = Vec::new();
        entries.try_reserve_exact(length).map_err(|_| TransitionError::AllocationFailed)?;
        // A shuffle executes rounds 0..rounds, so round 255 is never an active cache key.
        entries.resize(length, SourceEntry { group: 0, round: u8::MAX, root: [0; 32] });
        Ok(Self { entries })
    }

    fn get(&mut self, group: u32, round: u8, input: &mut [u8; 37]) -> Root {
        let length = self.entries.len();
        let entry = &mut self.entries[group as usize % length];
        if entry.group != group || entry.round != round {
            input[33..].copy_from_slice(&group.to_le_bytes());
            *entry = SourceEntry { group, round, root: sha256(input) };
        }
        entry.root
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compute_shuffled_index;
    use std::{hint::black_box, time::Instant};

    #[test]
    fn batched_mapping_matches_scalar_at_source_boundaries() {
        for count in [1, 2, 3, 31, 255, 256, 257, 513] {
            for rounds in [0, 10, 90] {
                let seed = [42; 32];
                let mapping = compute_shuffled_indices(count, seed, rounds).unwrap();
                for (index, &value) in mapping.iter().enumerate() {
                    assert_eq!(
                        value,
                        compute_shuffled_index(index as u64, count, seed, rounds).unwrap()
                    );
                }
                let mut sorted = mapping;
                sorted.sort_unstable();
                assert_eq!(sorted, (0..count).collect::<Vec<_>>());
            }
        }
    }

    #[test]
    fn empty_and_spec_limit_are_checked_before_allocation() {
        assert_eq!(
            compute_shuffled_indices(0, [0; 32], 90),
            Err(TransitionError::InvalidShuffleIndex { index: 0, count: 0 })
        );
        assert_eq!(
            compute_shuffled_indices((1 << 40) + 1, [0; 32], 90),
            Err(TransitionError::ShuffleCountLimit)
        );
    }

    #[test]
    fn range_mapping_matches_scalar_at_partitions_rounds_and_cache_collisions() {
        for count in [1, 2, 3, 255, 256, 257, 513, 65_537, 262_145, 1_144_679, 1u64 << 40] {
            for rounds in [0, 1, 10, 90, 255] {
                for seed in [[0; 32], [42; 32]] {
                    let start = count.saturating_sub(513) / 2;
                    let end = (start + 17).min(count);
                    let expected = (start..end)
                        .map(|index| compute_shuffled_index(index, count, seed, rounds).unwrap())
                        .collect::<Vec<_>>();
                    assert_eq!(
                        compute_shuffled_range(start, end, count, seed, rounds).unwrap(),
                        expected
                    );
                }
            }
        }
        let count = 1_144_679;
        let start = count * 731 / 2_048;
        let end = count * 732 / 2_048;
        let expected = (start..end)
            .map(|index| compute_shuffled_index(index, count, [99; 32], 90).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(compute_shuffled_range(start, end, count, [99; 32], 90).unwrap(), expected);
    }

    #[test]
    fn source_cache_is_bounded_and_collisions_do_not_reuse_wrong_round_or_group() {
        let mut cache = SourceCache::new(1u64 << 40).unwrap();
        assert_eq!(cache.entries.len(), MAX_SOURCE_CACHE_ENTRIES);
        let mut input = [19; 37];
        for (round, group) in [(0, 0), (0, 1_024), (1, 0), (254, u32::MAX), (0, 0)] {
            input[32] = round;
            input[33..].copy_from_slice(&group.to_le_bytes());
            assert_eq!(cache.get(group, round, &mut input), sha256(&input));
            assert_eq!(cache.get(group, round, &mut input), sha256(&input));
        }
        assert_eq!(compute_shuffled_range(0, 0, 0, [0; 32], 90), Ok(Vec::new()));
        assert_eq!(compute_shuffled_range(7, 7, 7, [0; 32], 90), Ok(Vec::new()));
        assert_eq!(
            compute_shuffled_range(0, 1, (1u64 << 40) + 1, [0; 32], 90),
            Err(TransitionError::ShuffleCountLimit)
        );
        assert_eq!(
            compute_shuffled_range(0, 2, 1, [0; 32], 90),
            Err(TransitionError::InvalidShuffleIndex { index: 2, count: 1 })
        );
    }

    /// Compare exact scalar and bounded-range work for a real-size Hoodi committee.
    #[test]
    #[ignore = "explicit performance measurement"]
    fn hoodi_committee_range_latency() {
        let count = 1_144_679;
        let start = count * 731 / 2_048;
        let end = count * 732 / 2_048;
        let expected = (start..end)
            .map(|index| compute_shuffled_index(index, count, [99; 32], 90).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(compute_shuffled_range(start, end, count, [99; 32], 90).unwrap(), expected);
        let iterations = 32;
        let started = Instant::now();
        for _ in 0..iterations {
            black_box(
                (start..end)
                    .map(|index| {
                        compute_shuffled_index(index, count, black_box([99; 32]), 90).unwrap()
                    })
                    .collect::<Vec<_>>(),
            );
        }
        let scalar = started.elapsed();
        let started = Instant::now();
        for _ in 0..iterations {
            black_box(compute_shuffled_range(start, end, count, black_box([99; 32]), 90).unwrap());
        }
        let ranged = started.elapsed();
        eprintln!("Hoodi committee: {} members; scalar={:.3}ms range={:.3}ms per committee; exact_equal=true",
            end - start, scalar.as_secs_f64() * 1_000.0 / f64::from(iterations),
            ranged.as_secs_f64() * 1_000.0 / f64::from(iterations));
    }
}
