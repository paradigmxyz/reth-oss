//! Snapshot-local storage deltas and ordinary MDBX blob persistence.

use super::{
    cache::{node_bytes, TrieCache, TrieNodes, CACHE_BYTES},
    codec::{self, Blob, OwnedBlob},
    PackingMode,
};
use crate::DatabaseError;
use alloy_primitives::{B256, U256};
use alloy_trie::{BranchNodeCompact, HashBuilder, Nibbles};
use reth_codecs::Compact;
use reth_db_api::{
    table::{Compress, DupSort, Encode, Table},
    tables::PackedStoragesTrie,
};
use reth_libmdbx::{ffi::MDBX_dbi, Transaction, TransactionKind, WriteFlags, RW};
use reth_primitives_traits::StorageEntry;
use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Bound,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
};

pub(crate) const TABLE: &str = "ExperimentalPackedStoragesV1";
const ROW_TARGET: usize = 512;
type TrieEntry = <PackedStoragesTrie as Table>::Value;

/// Transaction-owned deltas shared by all its logical cursors. No global state cache is used.
#[derive(Debug)]
pub(crate) struct Shared {
    pub(crate) mode: PackingMode,
    pub(crate) depth: usize,
    pub(crate) dbi: MDBX_dbi,
    pub(crate) generation: AtomicU64,
    /// Changes to retained trie rows, independent of canonical storage mutations.
    pub(crate) trie_epoch: AtomicU64,
    pub(crate) closed: AtomicBool,
    writer: Option<Transaction<RW>>,
    pending: Mutex<Pending>,
    cache: Mutex<TrieCache>,
    #[cfg(test)]
    pub(crate) row_target: std::sync::atomic::AtomicUsize,
}

#[derive(Debug, Default)]
struct Pending {
    clear: bool,
    cleared: BTreeSet<B256>,
    values: BTreeMap<(B256, B256), Option<U256>>,
    trie_generation: BTreeMap<B256, u64>,
    contracts: BTreeMap<B256, u64>,
    contract_clears: BTreeMap<B256, u64>,
    regions: BTreeMap<(B256, Nibbles), u64>,
    clear_generation: u64,
    trie_contracts: BTreeMap<B256, u64>,
    trie_clear_generation: u64,
    baselines: BTreeMap<B256, Arc<TrieNodes>>,
    baseline_bytes: usize,
}

impl Shared {
    pub(crate) fn new(
        mode: PackingMode,
        depth: usize,
        dbi: MDBX_dbi,
        writer: Option<Transaction<RW>>,
    ) -> Self {
        Self {
            mode,
            depth,
            dbi,
            generation: AtomicU64::new(0),
            trie_epoch: AtomicU64::new(0),
            closed: AtomicBool::new(false),
            writer,
            pending: Mutex::new(Pending::default()),
            cache: Mutex::new(TrieCache::default()),
            #[cfg(test)]
            row_target: std::sync::atomic::AtomicUsize::new(ROW_TARGET),
        }
    }

    pub(crate) fn check(&self) -> Result<(), DatabaseError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(DatabaseError::Other("experimental transaction is closed".into()));
        }
        Ok(())
    }

    pub(crate) fn put(&self, contract: B256, entry: StorageEntry) -> Result<(), DatabaseError> {
        self.check()?;
        let mut p = self
            .pending
            .lock()
            .map_err(|_| DatabaseError::Other("packing delta lock poisoned".into()))?;
        self.capture_baseline(&mut p, contract)?;
        p.values.insert((contract, entry.key), Some(entry.value));
        self.changed(&mut p, contract, Some(entry.key));
        Ok(())
    }

    pub(crate) fn delete(&self, contract: B256, slot: B256) -> Result<(), DatabaseError> {
        self.check()?;
        let mut p = self
            .pending
            .lock()
            .map_err(|_| DatabaseError::Other("packing delta lock poisoned".into()))?;
        self.capture_baseline(&mut p, contract)?;
        p.values.insert((contract, slot), None);
        self.changed(&mut p, contract, Some(slot));
        Ok(())
    }

    pub(crate) fn clear_contract(&self, contract: B256) -> Result<(), DatabaseError> {
        self.check()?;
        let mut p = self
            .pending
            .lock()
            .map_err(|_| DatabaseError::Other("packing delta lock poisoned".into()))?;
        p.cleared.insert(contract);
        p.values.retain(|(scope, _), _| *scope != contract);
        if let Some(nodes) = p.baselines.remove(&contract) {
            p.baseline_bytes -= node_bytes(&nodes);
        }
        self.changed(&mut p, contract, None);
        Ok(())
    }

    pub(crate) fn clear(&self) -> Result<(), DatabaseError> {
        self.check()?;
        let generation = self.generation.fetch_add(1, Ordering::Release) + 1;
        let mut p = self
            .pending
            .lock()
            .map_err(|_| DatabaseError::Other("packing delta lock poisoned".into()))?;
        // Retained-trie changes still matter after clearing canonical storage.
        let trie_contracts = std::mem::take(&mut p.trie_contracts);
        let trie_clear_generation = p.trie_clear_generation;
        *p = Pending {
            clear: true,
            clear_generation: generation,
            trie_contracts,
            trie_clear_generation,
            ..Default::default()
        };
        Ok(())
    }

    pub(crate) fn find<K: TransactionKind>(
        &self,
        store: &mut Store<K>,
        bound: Option<(B256, B256)>,
        inclusive: bool,
        reverse: bool,
    ) -> Result<Option<(B256, StorageEntry)>, DatabaseError> {
        self.check()?;
        // Read-only MDBX snapshots never have an overlay or mutable generations.
        if self.writer.is_none() {
            return store.find(bound, inclusive, reverse);
        }
        let p = self
            .pending
            .lock()
            .map_err(|_| DatabaseError::Other("packing delta lock poisoned".into()))?;
        let edge = bound.map_or(Bound::Unbounded, |b| {
            if inclusive {
                Bound::Included(b)
            } else {
                Bound::Excluded(b)
            }
        });
        let range = p.values.range(if reverse {
            (Bound::Unbounded, edge)
        } else {
            (edge, Bound::Unbounded)
        });
        let delta = if reverse {
            range.rev().find_map(|(key, val)| val.map(|v| (key.0, StorageEntry::new(key.1, v))))
        } else {
            range
                .into_iter()
                .find_map(|(key, val)| val.map(|v| (key.0, StorageEntry::new(key.1, v))))
        };
        let mut physical = if p.clear { None } else { store.find(bound, inclusive, reverse)? };
        while let Some((scope, entry)) = physical {
            if !p.cleared.contains(&scope) && !p.values.contains_key(&(scope, entry.key)) {
                break;
            }
            let skip = if p.cleared.contains(&scope) {
                (scope, if reverse { B256::ZERO } else { B256::repeat_byte(0xff) })
            } else {
                (scope, entry.key)
            };
            physical = store.find(Some(skip), false, reverse)?;
        }
        Ok(match (physical, delta) {
            (Some(a), Some(b)) => {
                Some(if ((a.0, a.1.key) <= (b.0, b.1.key)) == reverse { b } else { a })
            }
            (a, b) => a.or(b),
        })
    }

    pub(crate) fn mark_trie_root_written(&self, scope: B256) -> Result<(), DatabaseError> {
        let mut p = self
            .pending
            .lock()
            .map_err(|_| DatabaseError::Other("packing delta lock poisoned".into()))?;
        let generation = p.contracts.get(&scope).copied().unwrap_or(p.clear_generation);
        p.trie_generation.insert(scope, generation);
        Ok(())
    }

    pub(crate) fn invalidate_trie_root(&self, scope: Option<B256>) -> Result<(), DatabaseError> {
        let mut pending = self
            .pending
            .lock()
            .map_err(|_| DatabaseError::Other("packing delta lock poisoned".into()))?;
        if let Some(scope) = scope {
            pending.trie_generation.remove(&scope);
        } else {
            pending.trie_generation.clear();
        }
        Ok(())
    }

    pub(crate) fn trie_changed(&self, scope: Option<B256>) -> Result<(), DatabaseError> {
        let generation = self.trie_epoch.fetch_add(1, Ordering::Release) + 1;
        let mut p = self
            .pending
            .lock()
            .map_err(|_| DatabaseError::Other("packing delta lock poisoned".into()))?;
        if let Some(scope) = scope {
            p.trie_contracts.insert(scope, generation);
        } else {
            p.trie_clear_generation = generation;
            p.trie_contracts.clear();
        }
        Ok(())
    }

    pub(crate) fn flush(&self) -> Result<(), DatabaseError> {
        let Some(tx) = &self.writer else { return Ok(()) };
        let mut p = self
            .pending
            .lock()
            .map_err(|_| DatabaseError::Other("packing delta lock poisoned".into()))?;
        let trie = tx
            .open_db(Some(PackedStoragesTrie::NAME))
            .map_err(|e| DatabaseError::InitCursor(e.into()))?
            .dbi();
        let dirty: BTreeSet<_> =
            p.cleared.iter().copied().chain(p.values.keys().map(|(s, _)| *s)).collect();
        let unchanged: BTreeMap<_, _> = dirty
            .iter()
            .copied()
            .filter(|scope| {
                p.clear ||
                    p.cleared.contains(scope) ||
                    p.trie_generation.get(scope) != p.contracts.get(scope)
            })
            .map(|scope| (scope, unchanged(&p, scope)))
            .collect();
        let row_target = self.row_target();
        if p.clear {
            tx.clear_db(trie).map_err(|e| DatabaseError::Delete(e.into()))?;
            tx.clear_db(self.dbi).map_err(|e| DatabaseError::Delete(e.into()))?;
        }
        for scope in &p.cleared {
            let mut c =
                tx.cursor_with_dbi(self.dbi).map_err(|e| DatabaseError::InitCursor(e.into()))?;
            loop {
                let row = c
                    .set_range::<Vec<u8>, Vec<u8>>(&key(*scope, B256::ZERO))
                    .map_err(|e| DatabaseError::Read(e.into()))?;
                if row.as_ref().is_none_or(|(k, _)| k[..32] != scope[..]) {
                    break;
                }
                c.del(WriteFlags::CURRENT).map_err(|e| DatabaseError::Delete(e.into()))?;
            }
        }
        let mut changes = std::mem::take(&mut p.values).into_iter().peekable();
        while let Some(((scope, slot), value)) = changes.next() {
            let mut deleted = value.is_none();
            let mut store = Store::new(tx.clone(), self.dbi, self.mode)?;
            let mut rows = BTreeMap::new();
            let mut old_key = None;
            let mut next_anchor = None;
            let probe = key(scope, slot);
            let mut candidate = store
                .cursor
                .set_range::<Vec<u8>, Vec<u8>>(&probe)
                .map_err(|e| DatabaseError::Read(e.into()))?;
            if candidate.as_ref().is_none_or(|(k, _)| *k != probe) {
                candidate = if candidate.is_some() {
                    store.cursor.prev().map_err(|e| DatabaseError::Read(e.into()))?
                } else {
                    store.cursor.last().map_err(|e| DatabaseError::Read(e.into()))?
                };
            }
            if let Some((k, blob)) = candidate &&
                k[..32] == scope[..]
            {
                let parsed = Blob::parse_record(&blob, &k, self.mode)?;
                if parsed.mode() != self.mode {
                    return Err(DatabaseError::Decode);
                }
                for row in parsed.rows()? {
                    rows.insert(row.key, row.value);
                }
                old_key = Some(k);
                if let Some((k, _)) = store
                    .cursor
                    .next::<Vec<u8>, Vec<u8>>()
                    .map_err(|e| DatabaseError::Read(e.into()))? &&
                    k[..32] == scope[..]
                {
                    next_anchor = Some(B256::from_slice(&k[32..]));
                }
            } else if let Some((k, _)) = store
                .cursor
                .set_range::<Vec<u8>, Vec<u8>>(&probe)
                .map_err(|e| DatabaseError::Read(e.into()))? &&
                k[..32] == scope[..]
            {
                next_anchor = Some(B256::from_slice(&k[32..]));
            }
            // Prepending to a contract's first range should extend its first blob rather than
            // creating a one-row blob before it. All changes in that extended range are batched.
            if old_key.is_none() &&
                let Some(anchor) = next_anchor
            {
                let first = key(scope, anchor);
                let bytes = store
                    .cursor
                    .set::<Vec<u8>>(&first)
                    .map_err(|e| DatabaseError::Read(e.into()))?
                    .ok_or(DatabaseError::Decode)?;
                let parsed = Blob::parse_record(&bytes, &first, self.mode)?;
                if parsed.mode() != self.mode {
                    return Err(DatabaseError::Decode);
                }
                for row in parsed.rows()? {
                    rows.insert(row.key, row.value);
                }
                old_key = Some(first);
                next_anchor = store
                    .cursor
                    .next::<Vec<u8>, Vec<u8>>()
                    .map_err(|e| DatabaseError::Read(e.into()))?
                    .filter(|(k, _)| k[..32] == scope[..])
                    .map(|(k, _)| B256::from_slice(&k[32..]));
            }
            match value {
                Some(v) => {
                    rows.insert(slot, v);
                }
                None => {
                    rows.remove(&slot);
                }
            }
            while changes
                .peek()
                .is_some_and(|((s, k), _)| *s == scope && next_anchor.is_none_or(|a| *k < a))
            {
                let ((_, k), v) = changes.next().ok_or(DatabaseError::Decode)?;
                deleted |= v.is_none();
                match v {
                    Some(v) => {
                        rows.insert(k, v);
                    }
                    None => {
                        rows.remove(&k);
                    }
                }
            }
            // Merge only after deletions below a low-water mark. The merged maximum stays
            // well below the split limit, so small insert/delete cycles do not thrash.
            if deleted && !rows.is_empty() && rows.len() <= row_target / 4 {
                let anchor = old_key.as_ref().unwrap_or(&probe);
                let found = store
                    .cursor
                    .set_range::<Vec<u8>, Vec<u8>>(anchor)
                    .map_err(|e| DatabaseError::Read(e.into()))?;
                let neighbor = if found.is_some() {
                    store.cursor.prev::<Vec<u8>, Vec<u8>>()
                } else {
                    store.cursor.last::<Vec<u8>, Vec<u8>>()
                }
                .map_err(|e| DatabaseError::Read(e.into()))?;
                let merged =
                    if let Some((anchor, bytes)) = neighbor.filter(|(k, _)| k[..32] == scope[..]) {
                        merge_blob(
                            tx,
                            self.dbi,
                            self.mode,
                            &mut rows,
                            anchor,
                            &bytes,
                            row_target * 3 / 4,
                        )?
                    } else {
                        false
                    };
                if !merged && changes.peek().is_none_or(|((s, _), _)| *s != scope) {
                    // A successor is safe only when it has no remaining pending mutations.
                    if let Some(next) = next_anchor &&
                        let Some(bytes) = store
                            .cursor
                            .set::<Vec<u8>>(&key(scope, next))
                            .map_err(|e| DatabaseError::Read(e.into()))?
                    {
                        merge_blob(
                            tx,
                            self.dbi,
                            self.mode,
                            &mut rows,
                            key(scope, next),
                            &bytes,
                            row_target * 3 / 4,
                        )?;
                    }
                }
            }
            if let Some(k) = old_key {
                tx.del(self.dbi, k, None).map_err(|e| DatabaseError::Delete(e.into()))?;
            }
            let rows: Vec<_> = rows.into_iter().map(|(k, v)| StorageEntry::new(k, v)).collect();
            // Balance splits so a 513-row block does not leave a one-row tail.
            let chunk_size = rows.len().div_ceil(rows.len().div_ceil(row_target).max(1)).max(1);
            for block in rows.chunks(chunk_size) {
                let blob = codec::encode_record(block, scope, self.mode)?;
                tx.put(self.dbi, key(scope, block[0].key), blob, WriteFlags::UPSERT)
                    .map_err(|e| DatabaseError::Read(e.into()))?;
            }
        }
        // State and retained trie nodes must describe the same committed snapshot, even when
        // a hashing stage commits without going through a trie writer. Rebuild only the
        // affected contracts and discard deeper updates as they are produced.
        let mut store = Store::new(tx.clone(), self.dbi, self.mode)?;
        for scope in dirty {
            // A root written after the last state mutation means the normal trie writer
            // already supplied this snapshot's upper updates. Otherwise repair them here.
            if !p.clear &&
                !p.cleared.contains(&scope) &&
                p.trie_generation.get(&scope) == p.contracts.get(&scope)
            {
                continue;
            }
            tx.del(trie, scope, None).map_err(|e| DatabaseError::Delete(e.into()))?;
            let (base, skips) = unchanged.get(&scope).ok_or(DatabaseError::Decode)?;
            let (nodes, _) = rebuild_upper(scope, self.depth, base, skips, |bound, inclusive| {
                store.find(Some(bound), inclusive, false)
            })?;
            for (path, node) in nodes {
                let entry = TrieEntry { nibbles: path.into(), node };
                tx.put(trie, scope, entry.compress(), WriteFlags::UPSERT)
                    .map_err(|e| DatabaseError::Read(e.into()))?;
            }
        }
        Ok(())
    }

    fn changed(&self, p: &mut Pending, scope: B256, slot: Option<B256>) {
        let generation = self.generation.fetch_add(1, Ordering::Release) + 1;
        p.contracts.insert(scope, generation);
        if let Some(slot) = slot {
            p.regions.insert((scope, Nibbles::unpack(slot).slice(..self.depth)), generation);
        } else {
            p.contract_clears.insert(scope, generation);
        }
    }

    /// Capture retained records before the first storage mutation, never after trie-only edits.
    /// An absent/oversized baseline safely selects the original full reconstruction path.
    fn capture_baseline(&self, p: &mut Pending, scope: B256) -> Result<(), DatabaseError> {
        if p.clear ||
            p.contracts.contains_key(&scope) ||
            p.trie_clear_generation != 0 ||
            p.trie_contracts.contains_key(&scope) ||
            p.baseline_bytes >= CACHE_BYTES
        {
            return Ok(());
        }
        if let Some(tx) = &self.writer {
            let dbi = tx
                .open_db(Some(PackedStoragesTrie::NAME))
                .map_err(|e| DatabaseError::Open(e.into()))?
                .dbi();
            let nodes = read_upper(tx, dbi, scope, self.depth, CACHE_BYTES - p.baseline_bytes)?;
            let bytes = node_bytes(&nodes);
            if !nodes.is_empty() && bytes + p.baseline_bytes <= CACHE_BYTES {
                p.baseline_bytes += bytes;
                p.baselines.insert(scope, Arc::new(nodes));
            }
        }
        Ok(())
    }

    pub(crate) fn revision(&self, scope: B256) -> Result<(u64, u64, bool), DatabaseError> {
        if self.writer.is_none() {
            return Ok((0, 0, false));
        }
        let p = self
            .pending
            .lock()
            .map_err(|_| DatabaseError::Other("packing delta lock poisoned".into()))?;
        Ok((
            p.contracts.get(&scope).copied().unwrap_or(p.clear_generation),
            p.trie_contracts.get(&scope).copied().unwrap_or(p.trie_clear_generation),
            p.clear || p.contracts.contains_key(&scope),
        ))
    }

    pub(crate) fn region_generation(
        &self,
        scope: B256,
        prefix: Nibbles,
    ) -> Result<u64, DatabaseError> {
        if self.writer.is_none() {
            return Ok(0);
        }
        let p = self
            .pending
            .lock()
            .map_err(|_| DatabaseError::Other("packing delta lock poisoned".into()))?;
        Ok(p.regions
            .get(&(scope, prefix))
            .copied()
            .unwrap_or(0)
            .max(p.contract_clears.get(&scope).copied().unwrap_or(p.clear_generation)))
    }

    pub(crate) fn cached(
        &self,
        scope: B256,
        prefix: Option<Nibbles>,
        version: (u64, u64),
    ) -> Result<Option<Arc<TrieNodes>>, DatabaseError> {
        Ok(self
            .cache
            .lock()
            .map_err(|_| DatabaseError::Other("packing trie cache lock poisoned".into()))?
            .get((scope, prefix), version))
    }

    pub(crate) fn cache(
        &self,
        scope: B256,
        prefix: Option<Nibbles>,
        version: (u64, u64),
        nodes: Arc<TrieNodes>,
    ) -> Result<(), DatabaseError> {
        self.cache
            .lock()
            .map_err(|_| DatabaseError::Other("packing trie cache lock poisoned".into()))?
            .insert((scope, prefix), version, nodes);
        Ok(())
    }

    pub(crate) fn unchanged(
        &self,
        scope: B256,
    ) -> Result<(Arc<TrieNodes>, Vec<Subtree>), DatabaseError> {
        let p = self
            .pending
            .lock()
            .map_err(|_| DatabaseError::Other("packing delta lock poisoned".into()))?;
        Ok(unchanged(&p, scope))
    }

    #[cfg(not(test))]
    const fn row_target(&self) -> usize {
        ROW_TARGET
    }

    #[cfg(test)]
    fn row_target(&self) -> usize {
        self.row_target.load(Ordering::Relaxed)
    }
}

/// A physical blob cursor with one bounded validated-blob cache.
#[derive(Debug)]
pub(crate) struct Store<K: TransactionKind> {
    pub(crate) cursor: reth_libmdbx::Cursor<K>,
    cached: Option<(Vec<u8>, OwnedBlob)>,
    mode: PackingMode,
}

impl<K: TransactionKind> Store<K> {
    pub(crate) fn new(
        tx: Transaction<K>,
        dbi: MDBX_dbi,
        mode: PackingMode,
    ) -> Result<Self, DatabaseError> {
        Ok(Self {
            cursor: tx.cursor_with_dbi(dbi).map_err(|e| DatabaseError::InitCursor(e.into()))?,
            cached: None,
            mode,
        })
    }

    fn load(&mut self, row: (Vec<u8>, Vec<u8>)) -> Result<(), DatabaseError> {
        if row.0.len() != 64 {
            return Err(DatabaseError::Decode);
        }
        let rows = OwnedBlob::parse(row.1, &row.0, self.mode)?;
        if rows.len() == 0 || rows.key(0)[..] != row.0[32..] {
            return Err(DatabaseError::Decode);
        }
        self.cached = Some((row.0, rows));
        Ok(())
    }

    pub(crate) fn find(
        &mut self,
        bound: Option<(B256, B256)>,
        inclusive: bool,
        reverse: bool,
    ) -> Result<Option<(B256, StorageEntry)>, DatabaseError> {
        if let Some((scope, slot)) = bound &&
            let Some((anchor, rows)) = &mut self.cached &&
            anchor[..32] == scope[..] &&
            slot >= rows.key(0) &&
            slot <= rows.key(rows.len() - 1) &&
            let Some(index) = rows.find(scope, bound, inclusive, reverse)
        {
            return Ok(Some((scope, rows.row(index)?)));
        }
        let mut candidate = if let Some((scope, slot)) = bound {
            let k = key(scope, slot);
            let found = self
                .cursor
                .set_range::<Vec<u8>, Vec<u8>>(&k)
                .map_err(|e| DatabaseError::Read(e.into()))?;
            if found.as_ref().is_some_and(|(a, _)| *a == k) {
                found
            } else if found.is_some() {
                self.cursor.prev().map_err(|e| DatabaseError::Read(e.into()))?
            } else {
                self.cursor.last().map_err(|e| DatabaseError::Read(e.into()))?
            }
        } else if reverse {
            self.cursor.last().map_err(|e| DatabaseError::Read(e.into()))?
        } else {
            self.cursor.first().map_err(|e| DatabaseError::Read(e.into()))?
        };
        // No predecessor means the first blob may contain the first qualifying row.
        if candidate.is_none() && !reverse {
            candidate = self.cursor.first().map_err(|e| DatabaseError::Read(e.into()))?;
        }
        while let Some(row) = candidate {
            self.load(row)?;
            let (anchor, rows) = self.cached.as_mut().ok_or(DatabaseError::Decode)?;
            let scope = B256::from_slice(&anchor[..32]);
            if let Some(index) = rows.find(scope, bound, inclusive, reverse) {
                return Ok(Some((scope, rows.row(index)?)));
            }
            candidate = if reverse {
                self.cursor.prev().map_err(|e| DatabaseError::Read(e.into()))?
            } else {
                self.cursor.next().map_err(|e| DatabaseError::Read(e.into()))?
            };
        }
        Ok(None)
    }
}

pub(crate) fn key(scope: B256, slot: B256) -> Vec<u8> {
    let mut out = scope.to_vec();
    out.extend_from_slice(slot.as_slice());
    out
}

pub(crate) fn read_upper<K: TransactionKind>(
    tx: &Transaction<K>,
    dbi: MDBX_dbi,
    scope: B256,
    depth: usize,
    budget: usize,
) -> Result<TrieNodes, DatabaseError> {
    let mut cursor = tx.cursor_with_dbi(dbi).map_err(|e| DatabaseError::InitCursor(e.into()))?;
    let start = <PackedStoragesTrie as DupSort>::SubKey::from(Nibbles::default()).encode();
    let mut row = cursor
        .get_both_range::<Vec<u8>>(scope.as_slice(), start.as_ref())
        .map_err(|e| DatabaseError::Read(e.into()))?;
    let mut nodes = TrieNodes::new();
    let mut estimated_bytes = 128;
    while let Some(bytes) = row {
        if bytes.len() < 33 {
            return Err(DatabaseError::Decode);
        }
        let entry = TrieEntry::from_compact(&bytes, bytes.len()).0;
        if entry.nibbles.0.len() < depth {
            estimated_bytes += std::mem::size_of::<(Nibbles, BranchNodeCompact)>() +
                96 +
                entry.node.hashes.len() * 32;
            if estimated_bytes > budget {
                // A partial baseline cannot be used as a complete source of retained records.
                return Ok(TrieNodes::new());
            }
            nodes.insert(entry.nibbles.0, entry.node);
        }
        row = cursor
            .next_dup::<Vec<u8>, Vec<u8>>()
            .map_err(|e| DatabaseError::Read(e.into()))?
            .map(|(_, v)| v);
    }
    Ok(nodes)
}

/// A canonical hashed child that has no pending storage mutations beneath its path.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Subtree {
    path: Nibbles,
    hash: B256,
    stored: bool,
}

fn unchanged(p: &Pending, scope: B256) -> (Arc<TrieNodes>, Vec<Subtree>) {
    let Some(base) = p.baselines.get(&scope).filter(|_| !p.clear && !p.cleared.contains(&scope))
    else {
        return (Arc::default(), Vec::new());
    };
    let mut candidates = BTreeMap::new();
    for (path, node) in base.iter() {
        for nibble in 0..16 {
            if node.hash_mask.is_bit_set(nibble) {
                let mut child = *path;
                child.push(nibble);
                let (lower, upper) = prefix_bounds(child);
                if p.values.range((scope, lower)..=(scope, upper)).next().is_none() {
                    candidates.insert(
                        child,
                        Subtree {
                            path: child,
                            hash: node.hash_for_nibble(nibble),
                            stored: node.tree_mask.is_bit_set(nibble),
                        },
                    );
                }
            }
        }
    }
    let mut skips: Vec<Subtree> = Vec::new();
    for subtree in candidates.into_values() {
        if skips.last().is_none_or(|last| !subtree.path.starts_with(&last.path)) {
            skips.push(subtree);
        }
    }
    (Arc::clone(base), skips)
}

fn prefix_bounds(prefix: Nibbles) -> (B256, B256) {
    let mut lower = [0; 32];
    let mut upper = [255; 32];
    let packed = prefix.pack();
    lower[..packed.len()].copy_from_slice(&packed);
    upper[..packed.len()].copy_from_slice(&packed);
    if !prefix.len().is_multiple_of(2) {
        upper[packed.len() - 1] |= 15;
    }
    (B256::from(lower), B256::from(upper))
}

/// Rebuild changed branches while feeding immutable hashed children to the canonical `HashBuilder`.
/// Preserve upper records inside skipped subtrees; `HashBuilder` emits all other current records.
pub(crate) fn rebuild_upper(
    scope: B256,
    depth: usize,
    base: &TrieNodes,
    skips: &[Subtree],
    mut find: impl FnMut((B256, B256), bool) -> Result<Option<(B256, StorageEntry)>, DatabaseError>,
) -> Result<(TrieNodes, usize), DatabaseError> {
    let mut nodes: TrieNodes = base
        .iter()
        .filter(|(path, _)| skips.iter().any(|s| path.starts_with(&s.path)))
        .map(|(path, node)| (*path, node.clone()))
        .collect();
    let mut builder = HashBuilder::default().with_updates(true);
    let mut bound = (scope, B256::ZERO);
    let mut inclusive = true;
    let mut skipped = skips.iter().peekable();
    let mut leaves = 0;
    loop {
        let row = find(bound, inclusive)?.filter(|(s, _)| *s == scope);
        if let Some(subtree) = skipped.peek() &&
            row.as_ref().is_none_or(|(_, row)| subtree.path <= Nibbles::unpack(row.key))
        {
            builder.add_branch(subtree.path, subtree.hash, subtree.stored);
            bound = (scope, prefix_bounds(subtree.path).1);
            inclusive = false;
            skipped.next();
        } else if let Some((_, row)) = row {
            builder.add_leaf(
                Nibbles::unpack(row.key),
                alloy_rlp::encode_fixed_size(&row.value).as_ref(),
            );
            bound = (scope, row.key);
            inclusive = false;
            leaves += 1;
        } else {
            break;
        }
        if let Some(updates) = builder.updated_branch_nodes.as_mut() {
            nodes.extend(updates.drain().filter(|(path, _)| path.len() < depth));
        }
    }
    builder.root();
    if let Some(updates) = builder.updated_branch_nodes.as_mut() {
        nodes.extend(updates.drain().filter(|(path, _)| path.len() < depth));
    }
    Ok((nodes, leaves))
}

fn merge_blob(
    tx: &Transaction<RW>,
    dbi: MDBX_dbi,
    mode: PackingMode,
    rows: &mut BTreeMap<B256, U256>,
    anchor: Vec<u8>,
    bytes: &[u8],
    max: usize,
) -> Result<bool, DatabaseError> {
    let neighbor = Blob::parse_record(bytes, &anchor, mode)?;
    if neighbor.mode() != mode {
        return Err(DatabaseError::Decode);
    }
    if rows.len() + neighbor.len() > max {
        return Ok(false);
    }
    for row in neighbor.rows()? {
        rows.insert(row.key, row.value);
    }
    tx.del(dbi, anchor, None).map_err(|e| DatabaseError::Delete(e.into()))?;
    Ok(true)
}
