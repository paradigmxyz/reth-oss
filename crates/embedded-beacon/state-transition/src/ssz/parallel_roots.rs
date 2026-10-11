//! Bounded parallel Validator leaf hashing with exact ordered serial fallback.
//!
//! Only Validator opts in. Collection bounds, every element root and the final Merkle
//! reduction remain in the ordinary SSZ codecs. A nonblocking process-wide permit prevents
//! simultaneous full-state roots and fixture runners from multiplying worker pools.

use super::{phase0::Validator, Preset, Ssz};
use reth_embedded_beacon_primitives::{Root, SszError};
use std::{
    panic::resume_unwind,
    sync::atomic::{AtomicBool, Ordering},
    thread,
};

const MIN_VALIDATORS_PER_WORKER: usize = 32_768;
const MAX_WORKERS: usize = 32;
static PARALLEL_HASH_ACTIVE: AtomicBool = AtomicBool::new(false);

pub(crate) fn validator_roots(values: &[Validator], preset: Preset) -> Result<Vec<Root>, SszError> {
    let available = thread::available_parallelism().map_or(1, usize::from);
    validator_roots_with_budget(values, preset, available, &PARALLEL_HASH_ACTIVE)
}

struct HashPermit<'a>(&'a AtomicBool);

impl<'a> HashPermit<'a> {
    fn acquire(active: &'a AtomicBool) -> Option<Self> {
        active
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .ok()
            .map(|_| Self(active))
    }
}

impl Drop for HashPermit<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

fn validator_roots_with_budget(
    values: &[Validator],
    preset: Preset,
    available: usize,
    active: &AtomicBool,
) -> Result<Vec<Root>, SszError> {
    let workers = available.min(MAX_WORKERS).min(values.len() / MIN_VALIDATORS_PER_WORKER);
    if workers < 2 {
        return serial_roots(values, preset)
    }
    let Some(_permit) = HashPermit::acquire(active) else { return serial_roots(values, preset) };
    parallel_roots(values, preset, workers).unwrap_or_else(|| serial_roots(values, preset))
}

fn serial_roots<T: Ssz>(values: &[T], preset: Preset) -> Result<Vec<Root>, SszError> {
    values.iter().map(|value| value.hash_tree_root(preset)).collect()
}

/// Join every started worker before returning; a spawn failure requests exact serial fallback.
/// Workers never mutate values or cache validity, and errors retain original input order.
fn parallel_roots<T: Ssz + Sync>(
    values: &[T],
    preset: Preset,
    workers: usize,
) -> Option<Result<Vec<Root>, SszError>> {
    let workers = workers.clamp(1, MAX_WORKERS).min(values.len().max(1));
    let chunk_size = values.len().div_ceil(workers).max(1);
    thread::scope(|scope| {
        let mut handles = Vec::with_capacity(workers);
        let mut spawn_failed = false;
        for chunk in values.chunks(chunk_size) {
            match thread::Builder::new()
                .name("beacon-ssz".to_owned())
                .stack_size(256 * 1024)
                .spawn_scoped(scope, move || serial_roots(chunk, preset))
            {
                Ok(handle) => handles.push(handle),
                Err(_) => {
                    spawn_failed = true;
                    break
                }
            }
        }
        let mut roots = Vec::with_capacity(values.len());
        let mut first_error = None;
        let mut first_panic = None;
        for handle in handles {
            match handle.join() {
                Ok(Ok(mut chunk)) => roots.append(&mut chunk),
                Ok(Err(error)) => {
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
                Err(panic) => {
                    if first_panic.is_none() {
                        first_panic = Some(panic);
                    }
                }
            }
        }
        if let Some(panic) = first_panic {
            resume_unwind(panic);
        }
        if spawn_failed {
            None
        } else if let Some(error) = first_error {
            Some(Err(error))
        } else {
            Some(Ok(roots))
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ssz::{Fixed, List, ProgressiveList};
    use reth_embedded_beacon_primitives::{merkleize, merkleize_progressive, mix_in_length};
    use std::time::Instant;

    fn validator(index: usize) -> Validator {
        Validator {
            pubkey: [index as u8; 48],
            withdrawal_credentials: [index.wrapping_mul(19) as u8; 32],
            effective_balance: 32_000_000_000 + index as u64,
            slashed: index % 3 == 0,
            activation_eligibility_epoch: index as u64,
            activation_epoch: index as u64 + 1,
            exit_epoch: u64::MAX,
            withdrawable_epoch: u64::MAX,
        }
    }

    #[test]
    fn ordered_parallel_leaves_match_serial_at_empty_and_chunk_boundaries() {
        for length in [0, 1, 3, 15, 16, 17, 33] {
            let mut values = (0..length).map(validator).collect::<Vec<_>>();
            let expected = serial_roots(&values, Preset::Minimal).unwrap();
            assert_eq!(parallel_roots(&values, Preset::Minimal, 4).unwrap().unwrap(), expected);
            if let Some(value) = values.last_mut() {
                value.effective_balance += 1;
                let changed = serial_roots(&values, Preset::Minimal).unwrap();
                assert_ne!(changed, expected);
                assert_eq!(parallel_roots(&values, Preset::Minimal, 4).unwrap().unwrap(), changed);
            }
        }
    }

    #[test]
    fn budget_threshold_and_occupied_permit_preserve_exact_roots() {
        let active = AtomicBool::new(false);
        for length in [
            MIN_VALIDATORS_PER_WORKER - 1,
            MIN_VALIDATORS_PER_WORKER,
            2 * MIN_VALIDATORS_PER_WORKER - 1,
            2 * MIN_VALIDATORS_PER_WORKER + 1,
        ] {
            let values = (0..length).map(validator).collect::<Vec<_>>();
            let expected = serial_roots(&values, Preset::Mainnet).unwrap();
            assert_eq!(
                validator_roots_with_budget(&values, Preset::Mainnet, 4, &active).unwrap(),
                expected
            );
            assert!(!active.load(Ordering::Relaxed));
            let permit = HashPermit::acquire(&active).unwrap();
            assert!(HashPermit::acquire(&active).is_none());
            assert_eq!(
                validator_roots_with_budget(&values, Preset::Mainnet, 4, &active).unwrap(),
                expected
            );
            assert!(active.load(Ordering::Relaxed));
            drop(permit);
            assert!(!active.load(Ordering::Relaxed));
        }
    }

    #[test]
    fn list_and_progressive_reductions_keep_padding_length_and_invalid_count_checks() {
        let values = (0..17).map(validator).collect::<Vec<_>>();
        let roots = parallel_roots(&values, Preset::Minimal, 4).unwrap().unwrap();
        let list = List::<Validator, Fixed<32>>::from_vec(values.clone());
        assert_eq!(
            list.hash_tree_root(Preset::Minimal).unwrap(),
            mix_in_length(merkleize(&roots, 32).unwrap(), 17)
        );
        let progressive = ProgressiveList::from_vec(values.clone());
        assert_eq!(
            progressive.hash_tree_root(Preset::Minimal).unwrap(),
            mix_in_length(merkleize_progressive(&roots).unwrap(), 17)
        );
        let invalid = List::<Validator, Fixed<16>>::from_vec(values);
        assert_eq!(
            invalid.hash_tree_root(Preset::Minimal),
            Err(SszError::LimitExceeded { actual: 17, limit: 16 })
        );
        let invalid = [
            List::<u64, Fixed<1>>::from_vec(vec![1, 2]),
            List::<u64, Fixed<1>>::from_vec(vec![3, 4, 5]),
        ];
        assert_eq!(
            parallel_roots(&invalid, Preset::Minimal, 2).unwrap(),
            Err(SszError::LimitExceeded { actual: 2, limit: 1 })
        );
    }
    #[test]
    #[ignore = "manual full-registry Validator leaf hashing benchmark"]
    fn full_registry_leaf_throughput_matches_serial() {
        let values = (0..1_144_679).map(validator).collect::<Vec<_>>();
        let started = Instant::now();
        let expected = serial_roots(&values, Preset::Mainnet).unwrap();
        let serial = started.elapsed();
        let started = Instant::now();
        let actual = validator_roots(&values, Preset::Mainnet).unwrap();
        let bounded = started.elapsed();
        assert_eq!(actual, expected);
        println!(
            "Validator leaves={} available_threads={} serial_ms={} bounded_ms={}",
            values.len(),
            thread::available_parallelism().map_or(1, usize::from),
            serial.as_millis(),
            bounded.as_millis(),
        );
    }
}
