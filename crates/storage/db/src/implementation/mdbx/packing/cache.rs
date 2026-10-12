//! Bounded sharing of canonical reconstructed trie records and transaction-local blobs.

use super::codec::OwnedBlob;
use alloy_primitives::B256;
use alloy_trie::{BranchNodeCompact, Nibbles};
use std::{collections::BTreeMap, sync::Arc};

pub(crate) type TrieNodes = BTreeMap<Nibbles, BranchNodeCompact>;
type Key = (B256, Option<Nibbles>);

pub(crate) const CACHE_BYTES: usize = 8 * 1024 * 1024;
const CACHE_ENTRIES: usize = 64;
const SNAPSHOT_CACHE_ENTRIES: usize = 1024;

/// The budget includes an estimate of map allocations and child hashes. Oversized results are
/// usable by their caller but are not admitted to the shared cache.
#[derive(Debug, Default)]
pub(crate) struct TrieCache {
    entries: BTreeMap<Key, Cached>,
    recency: Recency<Key>,
    bytes: usize,
}

#[derive(Debug)]
struct Cached {
    version: (u64, u64),
    nodes: Arc<TrieNodes>,
    bytes: usize,
    slot: usize,
}

impl TrieCache {
    pub(crate) fn get(&mut self, key: Key, version: (u64, u64)) -> Option<Arc<TrieNodes>> {
        let cached = self.entries.get_mut(&key)?;
        if cached.version != version {
            return None;
        }
        self.recency.touch(cached.slot);
        Some(Arc::clone(&cached.nodes))
    }

    pub(crate) fn insert(&mut self, key: Key, version: (u64, u64), nodes: Arc<TrieNodes>) {
        if let Some(old) = self.entries.remove(&key) {
            self.bytes -= old.bytes;
            self.recency.remove(old.slot);
        }
        let bytes = node_bytes(&nodes);
        if bytes > CACHE_BYTES {
            return;
        }
        while self.entries.len() >= CACHE_ENTRIES || self.bytes + bytes > CACHE_BYTES {
            let Some(oldest) = self.recency.pop_oldest() else {
                break;
            };
            if let Some(old) = self.entries.remove(&oldest) {
                self.bytes -= old.bytes;
            }
        }
        self.bytes += bytes;
        let slot = self.recency.insert(key);
        self.entries.insert(key, Cached { version, nodes, bytes, slot });
    }
}

pub(crate) fn node_bytes(nodes: &TrieNodes) -> usize {
    256 + nodes
        .values()
        .map(|node| {
            std::mem::size_of::<(Nibbles, BranchNodeCompact)>() + 96 + node.hashes.len() * 32
        })
        .sum::<usize>()
}

/// Immutable read-only results owned by one opened MDBX environment. Transaction IDs are
/// snapshot IDs, not block heights: old readers retain their entries after a writer commits.
/// This cache must never receive write-transaction overlays or their local generations.
#[derive(Debug, Default)]
pub(crate) struct SnapshotTrieCache {
    entries: BTreeMap<SnapshotKey, SnapshotCached>,
    recency: Recency<SnapshotKey>,
    bytes: usize,
}

type SnapshotKey = (SnapshotIdentity, (u64, usize), Key);

/// Regions can survive an unrelated commit only when a retained ancestor in the current
/// immutable snapshot commits to their contents. Transaction identities remain the fallback.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum SnapshotIdentity {
    Transaction(u64),
    Subtree { path: Nibbles, hash: B256 },
}

#[derive(Debug)]
struct SnapshotCached {
    nodes: Arc<TrieNodes>,
    bytes: usize,
    slot: usize,
}

impl SnapshotTrieCache {
    pub(crate) fn get(
        &mut self,
        snapshot: u64,
        layout: (u64, usize),
        key: Key,
    ) -> Option<Arc<TrieNodes>> {
        self.get_identity(SnapshotIdentity::Transaction(snapshot), layout, key)
    }

    pub(crate) fn get_subtree(
        &mut self,
        commitment: (Nibbles, B256),
        layout: (u64, usize),
        key: Key,
    ) -> Option<Arc<TrieNodes>> {
        self.get_identity(
            SnapshotIdentity::Subtree { path: commitment.0, hash: commitment.1 },
            layout,
            key,
        )
    }

    fn get_identity(
        &mut self,
        identity: SnapshotIdentity,
        layout: (u64, usize),
        key: Key,
    ) -> Option<Arc<TrieNodes>> {
        let cached = self.entries.get_mut(&(identity, layout, key))?;
        self.recency.touch(cached.slot);
        Some(Arc::clone(&cached.nodes))
    }

    pub(crate) fn insert(
        &mut self,
        snapshot: u64,
        layout: (u64, usize),
        key: Key,
        nodes: Arc<TrieNodes>,
    ) {
        self.insert_identity(SnapshotIdentity::Transaction(snapshot), layout, key, nodes);
    }

    pub(crate) fn insert_subtree(
        &mut self,
        commitment: (Nibbles, B256),
        layout: (u64, usize),
        key: Key,
        nodes: Arc<TrieNodes>,
    ) {
        self.insert_identity(
            SnapshotIdentity::Subtree { path: commitment.0, hash: commitment.1 },
            layout,
            key,
            nodes,
        );
    }

    fn insert_identity(
        &mut self,
        identity: SnapshotIdentity,
        layout: (u64, usize),
        key: Key,
        nodes: Arc<TrieNodes>,
    ) {
        let key = (identity, layout, key);
        if let Some(old) = self.entries.remove(&key) {
            self.bytes -= old.bytes;
            self.recency.remove(old.slot);
        }
        let bytes = node_bytes(&nodes) + std::mem::size_of::<SnapshotKey>() + 96;
        if bytes > CACHE_BYTES {
            return;
        }
        // One aggregate budget covers every retained snapshot and layout. A long-lived old
        // reader cannot multiply memory usage by keeping another per-snapshot cache alive.
        while self.entries.len() >= SNAPSHOT_CACHE_ENTRIES || self.bytes + bytes > CACHE_BYTES {
            let Some(oldest) = self.recency.pop_oldest() else {
                break;
            };
            if let Some(old) = self.entries.remove(&oldest) {
                self.bytes -= old.bytes;
            }
        }
        self.bytes += bytes;
        let slot = self.recency.insert(key);
        self.entries.insert(key, SnapshotCached { nodes, bytes, slot });
    }
}

/// Validated immutable bytes shared only within one MDBX transaction. Cursor-local integer
/// group decoding remains independent. Admission bounds include estimated map overhead.
#[derive(Debug, Default)]
pub(crate) struct BlobCache {
    entries: BTreeMap<(B256, B256), CachedBlob>,
    recency: Recency<(B256, B256)>,
    bytes: usize,
    #[cfg(test)]
    pub(crate) hits: usize,
}

#[derive(Debug)]
struct CachedBlob {
    blob: OwnedBlob,
    bytes: usize,
    slot: usize,
}

impl BlobCache {
    pub(crate) fn get(&mut self, scope: B256, slot: B256) -> Option<(Vec<u8>, OwnedBlob)> {
        let (&key, entry) = self.entries.range_mut(..=(scope, slot)).next_back()?;
        if key.0 != scope || slot > entry.blob.key(entry.blob.len() - 1) {
            return None;
        }
        self.recency.touch(entry.slot);
        #[cfg(test)]
        {
            self.hits += 1;
        }
        let mut anchor = key.0.to_vec();
        anchor.extend_from_slice(key.1.as_slice());
        Some((anchor, entry.blob.clone()))
    }

    pub(crate) fn insert(&mut self, scope: B256, blob: OwnedBlob) {
        let key = (scope, blob.key(0));
        if let Some(old) = self.entries.remove(&key) {
            self.bytes -= old.bytes;
            self.recency.remove(old.slot);
        }
        let bytes = blob.byte_len() + std::mem::size_of::<CachedBlob>() + 320;
        if bytes > CACHE_BYTES {
            return;
        }
        while self.entries.len() >= 256 || self.bytes + bytes > CACHE_BYTES {
            let Some(key) = self.recency.pop_oldest() else {
                break;
            };
            if let Some(old) = self.entries.remove(&key) {
                self.bytes -= old.bytes;
            }
        }
        self.bytes += bytes;
        let slot = self.recency.insert(key);
        self.entries.insert(key, CachedBlob { blob, bytes, slot });
    }
}

/// Indexed intrusive recency links keep cache touches allocation-free. Slots are reused
/// after removal, so the arena is bounded by the cache's maximum admitted entry count.
#[derive(Debug)]
pub(crate) struct Recency<K> {
    slots: Vec<Option<RecencyEntry<K>>>,
    free: Vec<usize>,
    oldest: Option<usize>,
    newest: Option<usize>,
}

#[derive(Debug, Clone, Copy)]
struct RecencyEntry<K> {
    key: K,
    previous: Option<usize>,
    next: Option<usize>,
}

impl<K> Default for Recency<K> {
    fn default() -> Self {
        Self { slots: Vec::new(), free: Vec::new(), oldest: None, newest: None }
    }
}

impl<K: Copy> Recency<K> {
    pub(crate) fn insert(&mut self, key: K) -> usize {
        let entry = RecencyEntry { key, previous: self.newest, next: None };
        let slot = if let Some(slot) = self.free.pop() {
            self.slots[slot] = Some(entry);
            slot
        } else {
            self.slots.push(Some(entry));
            self.slots.len() - 1
        };
        if let Some(newest) = self.newest {
            self.slots[newest].as_mut().expect("live recency slot").next = Some(slot);
        } else {
            self.oldest = Some(slot);
        }
        self.newest = Some(slot);
        slot
    }

    pub(crate) fn touch(&mut self, slot: usize) {
        if self.newest == Some(slot) {
            return
        }
        self.detach(slot);
        let entry = self.slots[slot].as_mut().expect("live recency slot");
        entry.previous = self.newest;
        entry.next = None;
        if let Some(newest) = self.newest {
            self.slots[newest].as_mut().expect("live recency slot").next = Some(slot);
        } else {
            self.oldest = Some(slot);
        }
        self.newest = Some(slot);
    }

    pub(crate) fn remove(&mut self, slot: usize) -> K {
        self.detach(slot);
        let key = self.slots[slot].take().expect("live recency slot").key;
        self.free.push(slot);
        key
    }

    pub(crate) fn pop_oldest(&mut self) -> Option<K> {
        self.oldest.map(|slot| self.remove(slot))
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.slots.len() - self.free.len()
    }

    fn detach(&mut self, slot: usize) {
        let entry = self.slots[slot].expect("live recency slot");
        if let Some(previous) = entry.previous {
            self.slots[previous].as_mut().expect("live recency slot").next = entry.next;
        } else {
            self.oldest = entry.next;
        }
        if let Some(next) = entry.next {
            self.slots[next].as_mut().expect("live recency slot").previous = entry.previous;
        } else {
            self.newest = entry.previous;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::implementation::mdbx::packing::{codec::encode_record, PackingMode};
    use alloy_primitives::U256;
    use reth_primitives_traits::StorageEntry;

    #[test]
    fn shared_blob_cache_evicts_and_respects_contract_boundaries() {
        let mut cache = BlobCache::default();
        let rows: Vec<_> = (0..512u64)
            .map(|i| StorageEntry::new(B256::from(U256::from(i).to_be_bytes::<32>()), U256::MAX))
            .collect();
        for i in 0..300u64 {
            let scope = B256::from(U256::from(i).to_be_bytes::<32>());
            let bytes = encode_record(&rows, scope, PackingMode::Dense).unwrap();
            let mut anchor = scope.to_vec();
            anchor.extend_from_slice(rows[0].key.as_slice());
            cache.insert(scope, OwnedBlob::parse(bytes, &anchor, PackingMode::Dense).unwrap());
            assert!(cache.bytes <= CACHE_BYTES);
            assert!(cache.entries.len() <= 256);
        }
        assert!(cache.get(B256::ZERO, rows[50].key).is_none());
        assert!(cache.get(B256::from(U256::from(299).to_be_bytes::<32>()), rows[50].key).is_some());
        assert!(cache.get(B256::from(U256::from(300).to_be_bytes::<32>()), rows[50].key).is_none());
        assert!(cache
            .get(B256::from(U256::from(299).to_be_bytes::<32>()), B256::repeat_byte(255))
            .is_none());
    }

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
        cache.insert((B256::ZERO, None), (0, 0), Arc::default());
        cache.insert((B256::ZERO, None), (1, 0), Arc::new(nodes));
        assert!(cache.get((B256::ZERO, None), (1, 0)).is_none());
        assert_eq!(cache.bytes, 0);
        assert_eq!(cache.recency.len(), 0);
        assert!(cache.entries.is_empty());
    }

    #[test]
    fn replacing_cached_records_keeps_recency_and_budgets_consistent() {
        let mut trie = TrieCache::default();
        trie.insert((B256::ZERO, None), (1, 0), Arc::default());
        trie.insert((B256::ZERO, None), (2, 0), Arc::default());
        assert_eq!(trie.entries.len(), 1);
        assert_eq!(trie.recency.len(), 1);
        assert_eq!(trie.bytes, node_bytes(&TrieNodes::default()));
        assert!(trie.get((B256::ZERO, None), (1, 0)).is_none());
        assert!(trie.get((B256::ZERO, None), (2, 0)).is_some());

        let mut blobs = BlobCache::default();
        let rows = [StorageEntry::new(B256::ZERO, U256::from(1))];
        let anchor = [0; 64];
        for _ in 0..3 {
            let bytes = encode_record(&rows, B256::ZERO, PackingMode::Dense).unwrap();
            blobs.insert(B256::ZERO, OwnedBlob::parse(bytes, &anchor, PackingMode::Dense).unwrap());
            assert!(blobs.get(B256::ZERO, B256::ZERO).is_some());
        }
        assert_eq!(blobs.entries.len(), 1);
        assert_eq!(blobs.recency.len(), 1);
        assert_eq!(blobs.bytes, blobs.entries.values().map(|entry| entry.bytes).sum::<usize>());
    }

    #[test]
    fn recency_reuses_slots_and_handles_all_link_positions() {
        let mut recency = Recency::default();
        assert_eq!(recency.pop_oldest(), None::<u8>);
        let a = recency.insert(1);
        let b = recency.insert(2);
        let c = recency.insert(3);
        recency.touch(b);
        recency.touch(a);
        recency.touch(a);
        assert_eq!(recency.pop_oldest(), Some(3));
        assert_eq!(recency.remove(b), 2);
        assert_eq!(recency.len(), 1);
        let d = recency.insert(4);
        assert_eq!(d, b);
        let e = recency.insert(5);
        assert_eq!(e, c);
        assert_eq!(recency.slots.len(), 3);
        recency.touch(e);
        recency.touch(d);
        assert_eq!(recency.remove(d), 4);
        assert_eq!(recency.pop_oldest(), Some(1));
        assert_eq!(recency.pop_oldest(), Some(5));
        assert_eq!(recency.len(), 0);
        assert!(recency.oldest.is_none());
        assert!(recency.newest.is_none());
        assert_eq!(recency.pop_oldest(), None);
        let single = recency.insert(6);
        recency.touch(single);
        assert_eq!(recency.pop_oldest(), Some(6));
        assert_eq!(recency.slots.len(), 3);
    }

    #[test]
    fn eviction_releases_cache_ownership_without_invalidating_caller_snapshot() {
        let mut cache = TrieCache::default();
        let retained = Arc::new(TrieNodes::default());
        cache.insert((B256::ZERO, None), (1, 0), Arc::clone(&retained));
        let caller = cache.get((B256::ZERO, None), (1, 0)).unwrap();
        assert_eq!(Arc::strong_count(&retained), 3);
        for index in 1..=CACHE_ENTRIES {
            cache.insert((B256::repeat_byte(index as u8), None), (1, 0), Arc::default());
        }
        assert!(cache.get((B256::ZERO, None), (1, 0)).is_none());
        assert_eq!(Arc::strong_count(&retained), 2);
        assert!(Arc::ptr_eq(&caller, &retained));
        drop(caller);
        assert_eq!(Arc::strong_count(&retained), 1);
        assert_eq!(cache.recency.slots.len(), CACHE_ENTRIES);
    }

    #[test]
    fn snapshot_cache_keys_cover_snapshot_layout_contract_and_region() {
        let mut cache = SnapshotTrieCache::default();
        let layout = (10003, 3);
        let scope = B256::repeat_byte(1);
        let region = Nibbles::from_nibbles_unchecked(vec![1, 2, 3]);
        let nodes = Arc::new(TrieNodes::default());
        cache.insert(42, layout, (scope, Some(region)), Arc::clone(&nodes));
        assert!(Arc::ptr_eq(&nodes, &cache.get(42, layout, (scope, Some(region))).unwrap()));
        assert!(cache.get(43, layout, (scope, Some(region))).is_none());
        assert!(cache.get(42, (10004, 3), (scope, Some(region))).is_none());
        assert!(cache.get(42, (10003, 4), (scope, Some(region))).is_none());
        assert!(cache.get(42, layout, (B256::ZERO, Some(region))).is_none());
        assert!(cache.get(42, layout, (scope, None)).is_none());
        assert!(cache
            .get(42, layout, (scope, Some(Nibbles::from_nibbles_unchecked(vec![1, 2, 4]))))
            .is_none());
    }

    #[test]
    fn snapshot_cache_has_one_lru_entry_budget_across_all_snapshots() {
        let mut cache = SnapshotTrieCache::default();
        let layout = (10003, 3);
        let retained = Arc::new(TrieNodes::default());
        for index in 0..SNAPSHOT_CACHE_ENTRIES {
            let scope = B256::from(U256::from(index).to_be_bytes::<32>());
            cache.insert(1, layout, (scope, None), Arc::clone(&retained));
        }
        let caller = cache.get(1, layout, (B256::ZERO, None)).unwrap();
        cache.insert(2, layout, (B256::ZERO, None), Arc::default());
        assert!(cache.get(1, layout, (B256::ZERO, None)).is_some());
        assert!(cache.get(1, layout, (B256::with_last_byte(1), None)).is_none());
        assert!(cache.get(2, layout, (B256::ZERO, None)).is_some());
        assert_eq!(cache.entries.len(), SNAPSHOT_CACHE_ENTRIES);
        assert_eq!(cache.recency.len(), SNAPSHOT_CACHE_ENTRIES);
        assert_eq!(cache.recency.slots.len(), SNAPSHOT_CACHE_ENTRIES);
        assert!(cache.bytes <= CACHE_BYTES);
        assert!(Arc::ptr_eq(&caller, &retained));
    }

    #[test]
    fn snapshot_cache_bounds_bytes_and_keeps_evicted_caller_nodes_alive() {
        let mut cache = SnapshotTrieCache::default();
        let layout = (10003, 3);
        let node = BranchNodeCompact::new(u16::MAX, u16::MAX, u16::MAX, vec![B256::ZERO; 16], None);
        let nodes = Arc::new(
            (0..2000u64)
                .map(|index| {
                    (
                        Nibbles::unpack(B256::from(U256::from(index).to_be_bytes::<32>())),
                        node.clone(),
                    )
                })
                .collect::<TrieNodes>(),
        );
        cache.insert(1, layout, (B256::ZERO, None), Arc::clone(&nodes));
        let caller = cache.get(1, layout, (B256::ZERO, None)).unwrap();
        for snapshot in 2..20 {
            cache.insert(snapshot, layout, (B256::ZERO, None), Arc::clone(&nodes));
            assert!(cache.bytes <= CACHE_BYTES);
            assert_eq!(cache.bytes, cache.entries.values().map(|entry| entry.bytes).sum::<usize>());
        }
        assert!(cache.get(1, layout, (B256::ZERO, None)).is_none());
        assert!(Arc::ptr_eq(&caller, &nodes));
        assert_eq!(caller.len(), 2000);
    }

    #[test]
    fn snapshot_and_content_entries_share_one_budget_and_distinct_keys() {
        let mut cache = SnapshotTrieCache::default();
        let layout = (10003, 3);
        let region = Nibbles::from_nibbles_unchecked([1, 2, 3]);
        let anchor = Nibbles::from_nibbles_unchecked([1]);
        let commitment = (anchor, B256::repeat_byte(9));
        let nodes = Arc::new(TrieNodes::default());
        cache.insert_subtree(commitment, layout, (B256::ZERO, Some(region)), Arc::clone(&nodes));
        assert!(Arc::ptr_eq(
            &nodes,
            &cache.get_subtree(commitment, layout, (B256::ZERO, Some(region))).unwrap()
        ));
        assert!(cache.get(9, layout, (B256::ZERO, Some(region))).is_none());
        assert!(cache
            .get_subtree((anchor, B256::repeat_byte(10)), layout, (B256::ZERO, Some(region)))
            .is_none());
        assert!(cache
            .get_subtree((region, commitment.1), layout, (B256::ZERO, Some(region)))
            .is_none());
        assert!(cache.get_subtree(commitment, (10004, 3), (B256::ZERO, Some(region))).is_none());
        assert!(cache.get_subtree(commitment, (10003, 4), (B256::ZERO, Some(region))).is_none());
        assert!(cache
            .get_subtree(commitment, layout, (B256::with_last_byte(1), Some(region)))
            .is_none());
        assert!(cache.get_subtree(commitment, layout, (B256::ZERO, None)).is_none());
        assert!(cache
            .get_subtree(commitment, layout, (B256::ZERO, Some(region.slice(..2))))
            .is_none());
        for index in 1..SNAPSHOT_CACHE_ENTRIES {
            let scope = B256::from(U256::from(index).to_be_bytes::<32>());
            if index.is_multiple_of(2) {
                cache.insert_subtree(commitment, layout, (scope, Some(region)), Arc::default());
            } else {
                cache.insert(index as u64, layout, (scope, Some(region)), Arc::default());
            }
        }
        let caller = cache.get_subtree(commitment, layout, (B256::ZERO, Some(region))).unwrap();
        cache.insert(99999, layout, (B256::ZERO, Some(region)), Arc::default());
        assert!(cache.get_subtree(commitment, layout, (B256::ZERO, Some(region))).is_some());
        assert!(cache.get(1, layout, (B256::with_last_byte(1), Some(region))).is_none());
        assert_eq!(cache.entries.len(), SNAPSHOT_CACHE_ENTRIES);
        assert_eq!(cache.recency.len(), SNAPSHOT_CACHE_ENTRIES);
        assert!(cache.bytes <= CACHE_BYTES);
        assert_eq!(cache.bytes, cache.entries.values().map(|entry| entry.bytes).sum::<usize>());
        assert!(Arc::ptr_eq(&nodes, &caller));
    }
}
