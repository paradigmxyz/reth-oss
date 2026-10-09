//! Snapshot-local storage deltas and ordinary MDBX blob persistence.

use super::{
    codec::{self, Blob, OwnedBlob},
    PackingMode,
};
use crate::DatabaseError;
use alloy_primitives::{B256, U256};
use alloy_trie::{HashBuilder, Nibbles};
use reth_db_api::{
    table::{Compress, Table},
    tables::PackedStoragesTrie,
};
use reth_libmdbx::{ffi::MDBX_dbi, Transaction, TransactionKind, WriteFlags, RW};
use reth_primitives_traits::StorageEntry;
use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Bound,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Mutex,
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
    pub(crate) closed: AtomicBool,
    writer: Option<Transaction<RW>>,
    pending: Mutex<Pending>,
}

#[derive(Debug, Default)]
struct Pending {
    clear: bool,
    cleared: BTreeSet<B256>,
    values: BTreeMap<(B256, B256), Option<U256>>,
    trie_generation: BTreeMap<B256, u64>,
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
            closed: AtomicBool::new(false),
            writer,
            pending: Mutex::new(Pending::default()),
        }
    }

    pub(crate) fn check(&self) -> Result<(), DatabaseError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(DatabaseError::Other("experimental transaction is closed".into()));
        }
        Ok(())
    }

    pub(crate) fn dirty(&self, contract: B256) -> Result<bool, DatabaseError> {
        let p = self
            .pending
            .lock()
            .map_err(|_| DatabaseError::Other("packing delta lock poisoned".into()))?;
        Ok(p.clear ||
            p.cleared.contains(&contract) ||
            p.values
                .range((contract, B256::ZERO)..=(contract, B256::repeat_byte(0xff)))
                .next()
                .is_some())
    }

    pub(crate) fn put(&self, contract: B256, entry: StorageEntry) -> Result<(), DatabaseError> {
        self.check()?;
        self.pending
            .lock()
            .map_err(|_| DatabaseError::Other("packing delta lock poisoned".into()))?
            .values
            .insert((contract, entry.key), Some(entry.value));
        self.generation.fetch_add(1, Ordering::Release);
        Ok(())
    }

    pub(crate) fn delete(&self, contract: B256, slot: B256) -> Result<(), DatabaseError> {
        self.check()?;
        self.pending
            .lock()
            .map_err(|_| DatabaseError::Other("packing delta lock poisoned".into()))?
            .values
            .insert((contract, slot), None);
        self.generation.fetch_add(1, Ordering::Release);
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
        self.generation.fetch_add(1, Ordering::Release);
        Ok(())
    }

    pub(crate) fn clear(&self) -> Result<(), DatabaseError> {
        self.check()?;
        *self
            .pending
            .lock()
            .map_err(|_| DatabaseError::Other("packing delta lock poisoned".into()))? =
            Pending { clear: true, ..Default::default() };
        self.generation.fetch_add(1, Ordering::Release);
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
        self.pending
            .lock()
            .map_err(|_| DatabaseError::Other("packing delta lock poisoned".into()))?
            .trie_generation
            .insert(scope, self.generation.load(Ordering::Acquire));
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
                let parsed = Blob::parse(&blob)?;
                if parsed.mode() != self.mode {
                    return Err(DatabaseError::Decode)
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
                match v {
                    Some(v) => {
                        rows.insert(k, v);
                    }
                    None => {
                        rows.remove(&k);
                    }
                }
            }
            if let Some(k) = old_key {
                tx.del(self.dbi, k, None).map_err(|e| DatabaseError::Delete(e.into()))?;
            }
            let rows: Vec<_> = rows.into_iter().map(|(k, v)| StorageEntry::new(k, v)).collect();
            // Balance splits so a 513-row block does not leave a one-row tail.
            let chunk_size = rows.len().div_ceil(rows.len().div_ceil(ROW_TARGET).max(1)).max(1);
            for block in rows.chunks(chunk_size) {
                let blob = codec::encode(block, self.mode)?;
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
                p.trie_generation.get(&scope) == Some(&self.generation.load(Ordering::Acquire))
            {
                continue
            }
            tx.del(trie, scope, None).map_err(|e| DatabaseError::Delete(e.into()))?;
            let mut builder = HashBuilder::default().with_updates(true);
            let mut bound = Some((scope, B256::ZERO));
            let mut inclusive = true;
            let mut count = 0;
            loop {
                let Some((s, row)) = store.find(bound, inclusive, false)? else { break };
                if s != scope {
                    break
                }
                builder.add_leaf(
                    Nibbles::unpack(row.key),
                    alloy_rlp::encode_fixed_size(&row.value).as_ref(),
                );
                count += 1;
                if count % 256 == 0 {
                    persist_upper(tx, trie, scope, self.depth, &mut builder)?;
                }
                bound = Some((scope, row.key));
                inclusive = false;
            }
            builder.root();
            persist_upper(tx, trie, scope, self.depth, &mut builder)?;
        }
        Ok(())
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
        let rows = OwnedBlob::parse(row.1, self.mode)?;
        if rows.len() == 0 || rows.key(0)[..] != row.0[32..] {
            return Err(DatabaseError::Decode)
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
            return Ok(Some((scope, rows.row(index)?)))
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
                return Ok(Some((scope, rows.row(index)?)))
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

fn persist_upper(
    tx: &Transaction<RW>,
    trie: MDBX_dbi,
    scope: B256,
    depth: usize,
    builder: &mut HashBuilder,
) -> Result<(), DatabaseError> {
    if let Some(updates) = builder.updated_branch_nodes.as_mut() {
        for (path, node) in updates.drain().filter(|(p, _)| p.len() < depth) {
            let entry = TrieEntry { nibbles: path.into(), node };
            tx.put(trie, scope, entry.compress(), WriteFlags::UPSERT)
                .map_err(|e| DatabaseError::Read(e.into()))?;
        }
    }
    Ok(())
}
