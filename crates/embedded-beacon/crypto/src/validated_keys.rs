//! Bounded reuse of fully decoded and subgroup-checked immutable public keys.
//!
//! Keys are identified by all original compressed bytes. Only successful `key_validate`
//! results enter the cache; signature results, proof of possession and participant membership
//! never do. Fixed slots bound resident allocations, including keys repeated by hostile traffic.

use crate::CryptoError;
use blst::min_pk::PublicKey;
use std::{
    collections::hash_map::RandomState,
    hash::BuildHasher,
    mem::size_of,
    sync::{Mutex, OnceLock},
};

const CACHE_BYTES: usize = 64 * 1024 * 1024;
const SHARDS: usize = 32;
const SLOTS_PER_SHARD: usize = CACHE_BYTES / SHARDS / size_of::<Option<CachedKey>>();
static CHECKED_KEYS: OnceLock<ValidatedKeys> = OnceLock::new();

struct ValidatedKeys {
    shards: [Mutex<Option<Box<[Option<CachedKey>]>>>; SHARDS],
    hashing: RandomState,
    slots_per_shard: usize,
}

#[derive(Clone)]
struct CachedKey {
    encoded: [u8; 48],
    decoded: PublicKey,
}

impl ValidatedKeys {
    fn new(slots_per_shard: usize) -> Self {
        assert!(slots_per_shard > 0);
        Self {
            shards: std::array::from_fn(|_| Mutex::new(None)),
            hashing: RandomState::new(),
            slots_per_shard,
        }
    }

    fn position(&self, key: &[u8; 48]) -> (usize, usize) {
        let hash = self.hashing.hash_one(key) as usize;
        (hash % SHARDS, (hash / SHARDS) % self.slots_per_shard)
    }

    fn get(&self, key: &[u8; 48]) -> Option<PublicKey> {
        let (shard, position) = self.position(key);
        // Poisoning cannot turn an already checked immutable entry into authority for other
        // bytes. Recovering the acceleration cache also keeps unrelated validation available.
        let shard = self.shards[shard].lock().unwrap_or_else(|error| error.into_inner());
        let entry = shard.as_ref()?.get(position)?.as_ref()?;
        (entry.encoded == *key).then_some(entry.decoded)
    }

    fn validate(&self, key: &[u8; 48]) -> Result<PublicKey, CryptoError> {
        if let Some(decoded) = self.get(key) {
            return Ok(decoded)
        }
        // Expensive subgroup validation runs outside the shard lock. Concurrent misses may
        // repeat this check, but neither exposes unchecked keys or serializes unrelated work.
        let decoded = PublicKey::key_validate(key).map_err(|_| CryptoError::InvalidPublicKey)?;
        let (shard, position) = self.position(key);
        let mut shard = self.shards[shard].lock().unwrap_or_else(|error| error.into_inner());
        let entries =
            shard.get_or_insert_with(|| vec![None; self.slots_per_shard].into_boxed_slice());
        entries[position] = Some(CachedKey { encoded: *key, decoded });
        Ok(decoded)
    }
}

pub(super) fn validate(key: &[u8; 48]) -> Result<PublicKey, CryptoError> {
    CHECKED_KEYS.get_or_init(|| ValidatedKeys::new(SLOTS_PER_SHARD)).validate(key)
}

#[cfg(test)]
mod tests {
    use super::*;
    use blst::min_pk::SecretKey;
    use std::{hint::black_box, time::Instant};

    fn key(index: u64) -> [u8; 48] {
        let mut seed = [0; 32];
        seed[..8].copy_from_slice(&index.to_le_bytes());
        SecretKey::key_gen(&seed, &[]).unwrap().sk_to_pk().to_bytes()
    }

    #[test]
    fn only_exact_successfully_checked_keys_are_reused() {
        let cache = ValidatedKeys::new(2);
        let original = key(73);
        assert!(cache.get(&original).is_none());
        assert_eq!(cache.validate(&original).unwrap().to_bytes(), original);
        assert_eq!(cache.get(&original).unwrap().to_bytes(), original);
        assert_eq!(cache.validate(&original).unwrap().to_bytes(), original);
        let mut infinity = [0; 48];
        infinity[0] = 0xc0;
        for invalid in [[0; 48], infinity] {
            assert_eq!(cache.validate(&invalid).err(), Some(CryptoError::InvalidPublicKey));
            assert!(cache.get(&invalid).is_none());
        }
        assert_eq!(cache.get(&original).unwrap().to_bytes(), original);
    }

    #[test]
    fn collisions_replace_checked_entries_without_aliasing_encoded_keys() {
        let cache = ValidatedKeys::new(1);
        // With one slot per shard, SHARDS + 1 distinct checked keys guarantee a collision.
        let mut positions = [None; SHARDS];
        let (first, replacement) = (1..=SHARDS as u64 + 1)
            .map(key)
            .find_map(|candidate| {
                let (shard, _) = cache.position(&candidate);
                positions[shard].replace(candidate).map(|prior| (prior, candidate))
            })
            .unwrap();
        cache.validate(&first).unwrap();
        cache.validate(&replacement).unwrap();
        assert!(cache.get(&first).is_none());
        assert_eq!(cache.get(&replacement).unwrap().to_bytes(), replacement);
        assert_eq!(cache.validate(&first).unwrap().to_bytes(), first);
        assert!(cache.get(&replacement).is_none());
        let allocated = cache
            .shards
            .iter()
            .map(|shard| {
                shard
                    .lock()
                    .unwrap()
                    .as_ref()
                    .map_or(0, |slots| slots.len() * size_of::<Option<CachedKey>>())
            })
            .sum::<usize>();
        assert!(allocated <= SHARDS * size_of::<Option<CachedKey>>());
        assert!(SHARDS * SLOTS_PER_SHARD * size_of::<Option<CachedKey>>() <= CACHE_BYTES);
    }

    #[test]
    #[ignore = "explicit distinct-key cold/warm validation preparation measurement"]
    fn distinct_slot_keys_measure_cache_reuse_with_real_cold_validation() {
        let original = (1..=35_772).map(key).collect::<Vec<_>>();
        let cache = ValidatedKeys::new(SLOTS_PER_SHARD);
        let started = Instant::now();
        for encoded in &original {
            assert_eq!(cache.validate(black_box(encoded)).unwrap().to_bytes(), *encoded);
        }
        let cold = started.elapsed();
        let retained = original.iter().filter(|encoded| cache.get(encoded).is_some()).count();
        let started = Instant::now();
        for encoded in &original {
            assert_eq!(cache.validate(black_box(encoded)).unwrap().to_bytes(), *encoded);
        }
        let warm = started.elapsed();
        eprintln!(
            "Distinct slot BLS key preparation: participants={} retained_before_second_pass={} cold_ms={} second_pass_ms={} backing_limit_bytes={}",
            original.len(), retained, cold.as_millis(), warm.as_millis(), CACHE_BYTES
        );
    }
}
