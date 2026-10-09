//! Bounded, snapshot-local sharing of canonical reconstructed trie records.

use alloy_primitives::B256;
use alloy_trie::{BranchNodeCompact, Nibbles};
use std::{collections::BTreeMap, sync::Arc};

pub(crate) type TrieNodes = BTreeMap<Nibbles, BranchNodeCompact>;
type Key = (B256, Option<Nibbles>);

pub(crate) const CACHE_BYTES: usize = 8 * 1024 * 1024;
const CACHE_ENTRIES: usize = 64;

/// The budget includes an estimate of map allocations and child hashes. Oversized results are
/// usable by their caller but are not admitted to the shared cache.
#[derive(Debug, Default)]
pub(crate) struct TrieCache {
    entries: BTreeMap<Key, Cached>,
    bytes: usize,
    clock: u64,
}

#[derive(Debug)]
struct Cached {
    version: (u64, u64),
    nodes: Arc<TrieNodes>,
    bytes: usize,
    used: u64,
}

impl TrieCache {
    pub(crate) fn get(&mut self, key: Key, version: (u64, u64)) -> Option<Arc<TrieNodes>> {
        self.clock += 1;
        let cached = self.entries.get_mut(&key)?;
        if cached.version != version {
            return None;
        }
        cached.used = self.clock;
        Some(Arc::clone(&cached.nodes))
    }

    pub(crate) fn insert(&mut self, key: Key, version: (u64, u64), nodes: Arc<TrieNodes>) {
        if let Some(old) = self.entries.remove(&key) {
            self.bytes -= old.bytes;
        }
        let bytes = node_bytes(&nodes);
        if bytes > CACHE_BYTES {
            return;
        }
        while self.entries.len() >= CACHE_ENTRIES || self.bytes + bytes > CACHE_BYTES {
            let Some(oldest) = self.entries.iter().min_by_key(|(_, v)| v.used).map(|(k, _)| *k)
            else {
                break;
            };
            if let Some(old) = self.entries.remove(&oldest) {
                self.bytes -= old.bytes;
            }
        }
        self.clock += 1;
        self.bytes += bytes;
        self.entries.insert(key, Cached { version, nodes, bytes, used: self.clock });
    }
}

pub(crate) fn node_bytes(nodes: &TrieNodes) -> usize {
    128 + nodes
        .values()
        .map(|node| {
            std::mem::size_of::<(Nibbles, BranchNodeCompact)>() + 96 + node.hashes.len() * 32
        })
        .sum::<usize>()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_evicts_lru_and_rejects_stale_versions() {
        let mut cache = TrieCache::default();
        for i in 0..CACHE_ENTRIES {
            cache.insert((B256::repeat_byte(i as u8), None), (1, 0), Arc::default());
        }
        assert!(cache.get((B256::ZERO, None), (1, 0)).is_some());
        cache.insert((B256::repeat_byte(64), None), (1, 0), Arc::default());
        assert!(cache.get((B256::repeat_byte(1), None), (1, 0)).is_none());
        assert!(cache.get((B256::ZERO, None), (2, 0)).is_none());
        assert!(cache.bytes <= CACHE_BYTES);
        assert_eq!(cache.entries.len(), CACHE_ENTRIES);
    }

    #[test]
    fn oversized_results_do_not_enter_cache() {
        let mut cache = TrieCache::default();
        let node = BranchNodeCompact::new(u16::MAX, u16::MAX, u16::MAX, vec![B256::ZERO; 16], None);
        let nodes: TrieNodes = (0..20000u64)
            .map(|i| {
                (
                    Nibbles::unpack(B256::from(
                        alloy_primitives::U256::from(i).to_be_bytes::<32>(),
                    )),
                    node.clone(),
                )
            })
            .collect();
        assert!(node_bytes(&nodes) > CACHE_BYTES);
        cache.insert((B256::ZERO, None), (1, 0), Arc::new(nodes));
        assert!(cache.get((B256::ZERO, None), (1, 0)).is_none());
        assert_eq!(cache.bytes, 0);
    }
}
