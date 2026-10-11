//! Bounded parallel mandatory public-key decoding and subgroup validation.
//!
//! Every original participant uses an exact fully checked key; duplicates and input order
//! are preserved. Validated immutable keys use the bounded shared cache, while signatures
//! are always checked. A nonblocking permit prevents multiplying simultaneous worker pools.

use crate::CryptoError;
use blst::min_pk::PublicKey;
use std::{
    panic::resume_unwind,
    sync::atomic::{AtomicBool, Ordering},
    thread,
};

const MIN_KEYS_PER_WORKER: usize = 1_024;
const MAX_WORKERS: usize = 32;
static PARALLEL_VALIDATION_ACTIVE: AtomicBool = AtomicBool::new(false);

pub(super) fn validate_keys(keys: &[[u8; 48]]) -> Result<Vec<PublicKey>, CryptoError> {
    if keys.len() < 2 * MIN_KEYS_PER_WORKER {
        return cached_keys(keys)
    }
    let available = thread::available_parallelism().map_or(1, usize::from);
    validate_with_budget(keys, available, &PARALLEL_VALIDATION_ACTIVE)
}

struct ValidationPermit<'a>(&'a AtomicBool);

impl<'a> ValidationPermit<'a> {
    fn acquire(active: &'a AtomicBool) -> Option<Self> {
        active
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .ok()
            .map(|_| Self(active))
    }
}

impl Drop for ValidationPermit<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

fn worker_count(length: usize, available: usize) -> usize {
    available.min(MAX_WORKERS).min(length / MIN_KEYS_PER_WORKER)
}

fn validate_with_budget(
    keys: &[[u8; 48]],
    available: usize,
    active: &AtomicBool,
) -> Result<Vec<PublicKey>, CryptoError> {
    let workers = worker_count(keys.len(), available);
    if workers < 2 {
        return cached_keys(keys)
    }
    let Some(_permit) = ValidationPermit::acquire(active) else { return cached_keys(keys) };
    parallel_keys(keys, workers).unwrap_or_else(|| cached_keys(keys))
}

#[cfg(test)]
pub(super) fn serial_keys(keys: &[[u8; 48]]) -> Result<Vec<PublicKey>, CryptoError> {
    keys.iter()
        .map(|key| PublicKey::key_validate(key).map_err(|_| CryptoError::InvalidPublicKey))
        .collect()
}

fn cached_keys(keys: &[[u8; 48]]) -> Result<Vec<PublicKey>, CryptoError> {
    keys.iter().map(crate::validated_keys::validate).collect()
}

/// Join every started worker before success, failure or unwinding. A spawn failure requests
/// the identical scalar path after all already-started workers have completed.
fn parallel_keys(keys: &[[u8; 48]], workers: usize) -> Option<Result<Vec<PublicKey>, CryptoError>> {
    let workers = workers.clamp(1, MAX_WORKERS).min(keys.len().max(1));
    let chunk_size = keys.len().div_ceil(workers).max(1);
    thread::scope(|scope| {
        let mut handles = Vec::with_capacity(workers);
        let mut spawn_failed = false;
        for chunk in keys.chunks(chunk_size) {
            match thread::Builder::new()
                .name("beacon-bls".to_owned())
                .stack_size(256 * 1024)
                .spawn_scoped(scope, move || cached_keys(chunk))
            {
                Ok(handle) => handles.push(handle),
                Err(_) => {
                    spawn_failed = true;
                    break
                }
            }
        }
        let mut decoded = Vec::with_capacity(keys.len());
        let mut first_error = None;
        let mut first_panic = None;
        for handle in handles {
            match handle.join() {
                Ok(Ok(mut chunk)) => decoded.append(&mut chunk),
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
            Some(Ok(decoded))
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use blst::min_pk::SecretKey;

    fn keys(length: usize) -> Vec<[u8; 48]> {
        let first = SecretKey::key_gen(&[7; 32], &[]).unwrap().sk_to_pk().to_bytes();
        let second = SecretKey::key_gen(&[19; 32], &[]).unwrap().sk_to_pk().to_bytes();
        (0..length).map(|index| if index % 3 == 0 { second } else { first }).collect()
    }

    fn encoded(result: Result<Vec<PublicKey>, CryptoError>) -> Result<Vec<[u8; 48]>, CryptoError> {
        result.map(|keys| keys.iter().map(PublicKey::to_bytes).collect())
    }

    #[test]
    fn ordered_parallel_validation_preserves_every_participant_and_duplicate() {
        for length in [0, 1, 3, 31, 129, 2 * MIN_KEYS_PER_WORKER + 1] {
            let original = keys(length);
            let expected = encoded(serial_keys(&original));
            assert_eq!(encoded(parallel_keys(&original, 4).unwrap()), expected);
            assert_eq!(expected.unwrap(), original);
        }
    }

    #[test]
    fn malformed_and_infinite_keys_fail_identically_at_every_chunk_position() {
        let valid = keys(2 * MIN_KEYS_PER_WORKER + 1);
        let mut infinity = [0; 48];
        infinity[0] = 0xc0;
        for invalid in [[0; 48], infinity] {
            for position in [0, 1, MIN_KEYS_PER_WORKER, valid.len() - 1] {
                let mut original = valid.clone();
                original[position] = invalid;
                let expected = encoded(serial_keys(&original));
                assert_eq!(expected, Err(CryptoError::InvalidPublicKey));
                assert_eq!(encoded(parallel_keys(&original, 4).unwrap()), expected);
            }
        }
    }

    #[test]
    fn worker_threshold_and_busy_process_permit_preserve_scalar_results() {
        assert_eq!(worker_count(usize::MAX, usize::MAX), MAX_WORKERS);
        assert_eq!(worker_count(usize::MAX, 2), 2);
        assert_eq!(worker_count(2 * MIN_KEYS_PER_WORKER - 1, MAX_WORKERS), 1);
        assert_eq!(worker_count(2 * MIN_KEYS_PER_WORKER, MAX_WORKERS), 2);
        assert_eq!(worker_count(2 * MIN_KEYS_PER_WORKER, 1), 1);
        let active = AtomicBool::new(false);
        for length in [0, MIN_KEYS_PER_WORKER - 1, MIN_KEYS_PER_WORKER, 2 * MIN_KEYS_PER_WORKER + 1]
        {
            let original = keys(length);
            let expected = encoded(serial_keys(&original));
            assert_eq!(encoded(validate_with_budget(&original, 4, &active)), expected);
            assert!(!active.load(Ordering::Relaxed));
            let permit = ValidationPermit::acquire(&active).unwrap();
            assert!(ValidationPermit::acquire(&active).is_none());
            assert_eq!(encoded(validate_with_budget(&original, 4, &active)), expected);
            assert!(active.load(Ordering::Relaxed));
            drop(permit);
            assert!(!active.load(Ordering::Relaxed));
        }
        let mut invalid = keys(2 * MIN_KEYS_PER_WORKER + 1);
        invalid[MIN_KEYS_PER_WORKER] = [0; 48];
        assert_eq!(
            encoded(validate_with_budget(&invalid, 4, &active)),
            Err(CryptoError::InvalidPublicKey)
        );
        assert!(!active.load(Ordering::Relaxed));
    }
}
