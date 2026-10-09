//! Logical storage and trie rows backed by packed blobs in the caller's MDBX snapshot.

use super::{cache::TrieNodes, store::rebuild_upper, Shared, Store};
use crate::DatabaseError;
use alloy_primitives::B256;
use alloy_trie::{BranchNodeCompact, HashBuilder, Nibbles};
use reth_codecs::Compact;
use reth_db_api::{
    table::{Compress, Decode, DupSort, Encode, Table},
    tables::PackedStoragesTrie,
};
use reth_libmdbx::{Transaction, TransactionKind, WriteFlags, RW};
use reth_primitives_traits::StorageEntry;
use std::{collections::BTreeMap, sync::Arc};

type TrieEntry = <PackedStoragesTrie as Table>::Value;
type TrieKey = <PackedStoragesTrie as DupSort>::SubKey;
pub(crate) type RawRow = (Vec<u8>, Vec<u8>);
const MAX_REBUILD_LEAVES: usize = 262_144;

/// Cursor operation independent of a table's native value type.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Move {
    First,
    Last,
    Current,
    Next,
    Prev,
    NextDup,
    PrevDup,
    NextScope,
    LastDup,
}

/// An experimental cursor with transaction-shared reconstruction and a validated state blob.
#[derive(Debug)]
pub(crate) struct Logical<K: TransactionKind> {
    tx: Transaction<K>,
    shared: Arc<Shared>,
    store: Store<K>,
    physical: reth_libmdbx::Cursor<K>,
    trie: bool,
    position: Position,
    upper: Option<(B256, u64, u64, Arc<TrieNodes>)>,
    region: Option<(B256, Nibbles, u64, Arc<TrieNodes>)>,
    #[cfg(test)]
    pub(crate) upper_rebuilds: usize,
    #[cfg(test)]
    pub(crate) upper_leaves: usize,
    #[cfg(test)]
    pub(crate) region_rebuilds: usize,
}

/// A failed seek must not be confused with a cursor that has never been positioned.
#[derive(Debug)]
enum Position {
    Uninitialized,
    Row(RawRow),
    Boundary(B256, Vec<u8>),
    Missing,
}

impl<K: TransactionKind> Logical<K> {
    pub(crate) fn new(
        tx: Transaction<K>,
        shared: Arc<Shared>,
        physical: reth_libmdbx::Cursor<K>,
        trie: bool,
    ) -> Result<Self, DatabaseError> {
        Ok(Self {
            store: Store::new(tx.clone(), shared.dbi, shared.mode)?,
            tx,
            shared,
            physical,
            trie,
            position: Position::Uninitialized,
            upper: None,
            region: None,
            #[cfg(test)]
            upper_rebuilds: 0,
            #[cfg(test)]
            upper_leaves: 0,
            #[cfg(test)]
            region_rebuilds: 0,
        })
    }

    pub(crate) fn seek(
        &mut self,
        scope: &[u8],
        sub: Option<&[u8]>,
        exact_scope: bool,
    ) -> Result<Option<RawRow>, DatabaseError> {
        self.tx.txn_execute(|_| ()).map_err(|e| DatabaseError::Read(e.into()))?;
        self.shared.check()?;
        if scope.len() != 32 {
            return Err(DatabaseError::Decode);
        }
        let scope = B256::from_slice(scope);
        let sub = sub.map(Vec::from).unwrap_or_else(|| vec![0; if self.trie { 33 } else { 32 }]);
        let result = self.find(Some((scope, sub.clone())), true, false)?;
        let result = if exact_scope { result.filter(|(k, _)| k[..] == scope[..]) } else { result };
        self.position = if let Some(row) = &result {
            Position::Row(row.clone())
        } else if exact_scope {
            Position::Missing
        } else {
            Position::Boundary(scope, sub)
        };
        Ok(result)
    }

    pub(crate) fn movement(&mut self, movement: Move) -> Result<Option<RawRow>, DatabaseError> {
        self.tx.txn_execute(|_| ()).map_err(|e| DatabaseError::Read(e.into()))?;
        self.shared.check()?;
        if matches!(movement, Move::Current) {
            let Position::Row((key, value)) = &self.position else { return Ok(None) };
            let (key, value) = (key.clone(), value.clone());
            let bound = (B256::from_slice(&key), value[..if self.trie { 33 } else { 32 }].to_vec());
            let current = self.find(Some(bound), true, false)?.filter(|(k, v)| {
                *k == key &&
                    v[..if self.trie { 33 } else { 32 }] ==
                        value[..if self.trie { 33 } else { 32 }]
            });
            if let Some(row) = &current {
                self.position = Position::Row(row.clone());
            }
            return Ok(current);
        }
        if matches!(self.position, Position::Missing) &&
            !matches!(movement, Move::First | Move::Last)
        {
            return Ok(None);
        }
        let bound = match &self.position {
            Position::Row((k, v)) => {
                Some((B256::from_slice(k), v[..if self.trie { 33 } else { 32 }].to_vec()))
            }
            Position::Boundary(scope, sub) => Some((*scope, sub.clone())),
            Position::Uninitialized | Position::Missing => None,
        };
        let old_scope = bound.as_ref().map(|(s, _)| *s);
        let (bound, inclusive, reverse) = match movement {
            Move::First => (None, true, false),
            Move::Last => (None, true, true),
            Move::Next | Move::NextDup => (bound, false, false),
            Move::Prev | Move::PrevDup => (bound, false, true),
            Move::NextScope => {
                let Some(scope) = old_scope else { return self.movement(Move::First) };
                let next = self.shared.find(
                    &mut self.store,
                    Some((scope, B256::repeat_byte(0xff))),
                    false,
                    false,
                )?;
                let Some((scope, _)) = next else {
                    self.position = Position::Missing;
                    return Ok(None);
                };
                (Some((scope, vec![0; if self.trie { 33 } else { 32 }])), true, false)
            }
            Move::LastDup => {
                let Some(scope) = old_scope else { return Ok(None) };
                let sub = if self.trie {
                    TrieKey::from(Nibbles::unpack(B256::repeat_byte(0xff)))
                        .encode()
                        .as_ref()
                        .to_vec()
                } else {
                    B256::repeat_byte(0xff).to_vec()
                };
                (Some((scope, sub)), true, true)
            }
            Move::Current => unreachable!(),
        };
        let next = self.find(bound, inclusive, reverse)?;
        if matches!(movement, Move::NextDup | Move::PrevDup | Move::LastDup) &&
            next.as_ref().is_none_or(|(k, _)| Some(B256::from_slice(k)) != old_scope)
        {
            // MDBX leaves a duplicate cursor on the last valid item at end-of-scope.
            return Ok(None);
        }
        if let Some(row) = &next {
            self.position = Position::Row(row.clone());
        } else if matches!(movement, Move::First | Move::Last) ||
            matches!(self.position, Position::Row(_))
        {
            self.position = Position::Missing;
        }
        Ok(next)
    }

    fn find(
        &mut self,
        bound: Option<(B256, Vec<u8>)>,
        inclusive: bool,
        reverse: bool,
    ) -> Result<Option<RawRow>, DatabaseError> {
        if !self.trie {
            let bound = bound
                .map(|(s, k)| {
                    if k.len() == 32 {
                        Ok((s, B256::from_slice(&k)))
                    } else {
                        Err(DatabaseError::Decode)
                    }
                })
                .transpose()?;
            return self
                .shared
                .find(&mut self.store, bound, inclusive, reverse)?
                .map(|(s, e)| {
                    let mut bytes = Vec::new();
                    e.to_compact(&mut bytes);
                    Ok((s.to_vec(), bytes))
                })
                .transpose();
        }
        let initial = if let Some((scope, sub)) = bound {
            if sub.len() != 33 {
                return Err(DatabaseError::Decode);
            }
            Some((scope, TrieKey::decode(&sub)?.0))
        } else {
            self.shared.find(&mut self.store, None, true, reverse)?.map(|(s, _)| {
                (
                    s,
                    if reverse {
                        Nibbles::unpack(B256::repeat_byte(0xff))
                    } else {
                        Nibbles::default()
                    },
                )
            })
        };
        let Some((mut scope, mut query)) = initial else { return Ok(None) };
        let mut included = inclusive;
        loop {
            if let Some((path, node)) = self.find_trie(scope, &query, included, reverse)? {
                let value = TrieEntry { nibbles: TrieKey::from(path), node }.compress();
                return Ok(Some((scope.to_vec(), value)));
            }
            let next = self.shared.find(
                &mut self.store,
                Some((scope, if reverse { B256::ZERO } else { B256::repeat_byte(0xff) })),
                false,
                reverse,
            )?;
            let Some((next_scope, _)) = next else { return Ok(None) };
            scope = next_scope;
            query =
                if reverse { Nibbles::unpack(B256::repeat_byte(0xff)) } else { Nibbles::default() };
            included = true;
        }
    }

    fn rebuild(
        &mut self,
        scope: B256,
        prefix: &Nibbles,
        upper: bool,
    ) -> Result<BTreeMap<Nibbles, BranchNodeCompact>, DatabaseError> {
        if upper {
            let (base, skips) = self.shared.unchanged(scope)?;
            let (nodes, leaves) =
                rebuild_upper(scope, self.shared.depth, &base, &skips, |bound, inclusive| {
                    self.shared.find(&mut self.store, Some(bound), inclusive, false)
                })?;
            #[cfg(test)]
            {
                self.upper_rebuilds += 1;
                self.upper_leaves += leaves;
            }
            #[cfg(not(test))]
            let _ = leaves;
            return Ok(nodes);
        }
        #[cfg(test)]
        {
            self.region_rebuilds += 1;
        }
        let mut builder = HashBuilder::default().with_updates(true);
        let mut nodes = BTreeMap::new();
        let mut lower = [0u8; 32];
        let packed = prefix.pack();
        lower[..packed.len()].copy_from_slice(&packed);
        let mut bound = Some((scope, B256::from(lower)));
        let mut include = true;
        let mut count = 0;
        loop {
            let Some((s, row)) = self.shared.find(&mut self.store, bound, include, false)? else {
                break;
            };
            let path = Nibbles::unpack(row.key);
            if s != scope || !path.starts_with(prefix) {
                break;
            }
            count += 1;
            if count > MAX_REBUILD_LEAVES {
                return Err(DatabaseError::Other("experimental trie region exceeds 262144 leaves; use a larger --db.experimental-trie-depth".into()));
            }
            builder.add_leaf(path, alloy_rlp::encode_fixed_size(&row.value).as_ref());
            if count % 256 == 0 {
                Self::drain(&mut builder, &mut nodes, self.shared.depth, upper);
            }
            bound = Some((scope, row.key));
            include = false;
        }
        builder.root();
        Self::drain(&mut builder, &mut nodes, self.shared.depth, upper);
        Ok(nodes)
    }

    fn drain(
        builder: &mut HashBuilder,
        nodes: &mut BTreeMap<Nibbles, BranchNodeCompact>,
        depth: usize,
        upper: bool,
    ) {
        if let Some(updates) = builder.updated_branch_nodes.as_mut() {
            nodes.extend(updates.drain().filter(|(p, _)| (p.len() < depth) == upper));
        }
    }

    fn find_trie(
        &mut self,
        scope: B256,
        query: &Nibbles,
        inclusive: bool,
        reverse: bool,
    ) -> Result<Option<(Nibbles, BranchNodeCompact)>, DatabaseError> {
        let (generation, physical_epoch, dirty) = self.shared.revision(scope)?;
        let trie_epoch = if dirty { 0 } else { physical_epoch };
        // Dirty-contract nodes are derived from storage, so trie persistence cannot change them.
        // Clean-contract caches may reflect retained rows written through a different cursor.
        if self.upper.as_ref().is_none_or(|(s, g, t, _)| {
            *s != scope || *g != generation || (!dirty && *t != trie_epoch)
        }) {
            let nodes =
                if let Some(nodes) = self.shared.cached(scope, None, (generation, trie_epoch))? {
                    nodes
                } else if dirty {
                    Arc::new(self.rebuild(scope, &Nibbles::default(), true)?)
                } else {
                    let mut nodes = BTreeMap::new();
                    let start = TrieKey::from(Nibbles::default()).encode();
                    let mut row = self
                        .physical
                        .get_both_range::<Vec<u8>>(scope.as_slice(), start.as_ref())
                        .map_err(|e| DatabaseError::Read(e.into()))?;
                    while let Some(bytes) = row {
                        if bytes.len() < 33 {
                            return Err(DatabaseError::Decode);
                        }
                        let entry = TrieEntry::from_compact(&bytes, bytes.len()).0;
                        if entry.nibbles.0.len() < self.shared.depth {
                            nodes.insert(entry.nibbles.0, entry.node);
                        }
                        row = self
                            .physical
                            .next_dup::<Vec<u8>, Vec<u8>>()
                            .map_err(|e| DatabaseError::Read(e.into()))?
                            .map(|(_, v)| v);
                    }
                    if nodes.is_empty() {
                        nodes = self.rebuild(scope, &Nibbles::default(), true)?;
                    }
                    Arc::new(nodes)
                };
            self.shared.cache(scope, None, (generation, trie_epoch), Arc::clone(&nodes))?;
            self.upper = Some((scope, generation, trie_epoch, nodes));
        }
        let accepts = |p: &Nibbles| {
            if reverse {
                p < query || (inclusive && p == query)
            } else {
                p > query || (inclusive && p == query)
            }
        };
        let nodes = &self.upper.as_ref().ok_or(DatabaseError::Decode)?.3;
        let candidate = if reverse {
            nodes.iter().rev().find(|(p, _)| accepts(p))
        } else {
            nodes.iter().find(|(p, _)| accepts(p))
        }
        .map(|(p, n)| (*p, n.clone()));
        if candidate.as_ref().is_some_and(|(p, _)| p == query && inclusive) {
            return Ok(candidate);
        }
        if reverse && query.is_empty() {
            return Ok(candidate);
        }
        let mut digits = query.iter().take(self.shared.depth).collect::<Vec<_>>();
        digits.resize(self.shared.depth, if reverse { 15 } else { 0 });
        let mut prefix = Nibbles::from_nibbles_unchecked(digits);
        loop {
            let mut edge = [if reverse { 255 } else { 0 }; 32];
            let packed = prefix.pack();
            edge[..packed.len()].copy_from_slice(&packed);
            if reverse && !prefix.len().is_multiple_of(2) {
                edge[packed.len() - 1] |= 15;
            }
            let first = self.shared.find(
                &mut self.store,
                Some((scope, B256::from(edge))),
                true,
                reverse,
            )?;
            let Some((s, row)) = first else { return Ok(candidate) };
            if s != scope {
                return Ok(candidate);
            }
            prefix = Nibbles::unpack(row.key).slice(..self.shared.depth);
            if candidate
                .as_ref()
                .is_some_and(|(p, _)| if reverse { prefix < *p } else { prefix >= *p })
            {
                return Ok(candidate);
            }
            let region_generation = self.shared.region_generation(scope, prefix)?;
            if self
                .region
                .as_ref()
                .is_none_or(|(s, p, g, _)| *s != scope || *p != prefix || *g != region_generation)
            {
                let rebuilt = if let Some(nodes) =
                    self.shared.cached(scope, Some(prefix), (region_generation, 0))?
                {
                    nodes
                } else {
                    let nodes = Arc::new(self.rebuild(scope, &prefix, false)?);
                    self.shared.cache(
                        scope,
                        Some(prefix),
                        (region_generation, 0),
                        Arc::clone(&nodes),
                    )?;
                    nodes
                };
                self.region = Some((scope, prefix, region_generation, rebuilt));
            }
            let nodes = &self.region.as_ref().ok_or(DatabaseError::Decode)?.3;
            let hit = if reverse {
                nodes.iter().rev().find(|(p, _)| accepts(p))
            } else {
                nodes.iter().find(|(p, _)| accepts(p))
            };
            if let Some((p, n)) = hit {
                return Ok(Some((*p, n.clone())));
            }
            let mut digits = prefix.iter().collect::<Vec<_>>();
            let mut i = digits.len();
            loop {
                if i == 0 {
                    return Ok(candidate);
                }
                i -= 1;
                if reverse {
                    if digits[i] > 0 {
                        digits[i] -= 1;
                        break;
                    }
                    digits[i] = 15;
                } else {
                    if digits[i] < 15 {
                        digits[i] += 1;
                        break;
                    }
                    digits[i] = 0;
                }
            }
            prefix = Nibbles::from_nibbles_unchecked(digits);
        }
    }
}

impl Logical<RW> {
    pub(crate) fn write(
        &mut self,
        scope: &[u8],
        value: &[u8],
        insert: bool,
        append: bool,
    ) -> Result<(), DatabaseError> {
        self.tx.txn_execute(|_| ()).map_err(|e| DatabaseError::Read(e.into()))?;
        self.shared.check()?;
        if scope.len() != 32 {
            return Err(DatabaseError::Decode);
        }
        let s = B256::from_slice(scope);
        if insert &&
            self.find(Some((s, vec![0; if self.trie { 33 } else { 32 }])), true, false)?
                .is_some_and(|(k, _)| k == scope)
        {
            return Err(DatabaseError::Other(
                "experimental cursor insert: key already exists".into(),
            ));
        }
        let sub_len = if self.trie { 33 } else { 32 };
        if value.len() < sub_len {
            return Err(DatabaseError::Decode);
        }
        if append &&
            !self.trie &&
            self.find(None, true, true)?.is_some_and(|(k, v)| {
                (k.as_slice(), &v[..sub_len]) >= (scope, &value[..sub_len])
            })
        {
            return Err(DatabaseError::Other(
                "experimental cursor append: keys are not increasing".into(),
            ));
        }
        if self.trie {
            let entry = TrieEntry::from_compact(value, value.len()).0;
            if entry.nibbles.0.len() < self.shared.depth {
                let encoded = entry.nibbles.clone().encode();
                if self
                    .physical
                    .get_both_range::<Vec<u8>>(scope, encoded.as_ref())
                    .map_err(|e| DatabaseError::Read(e.into()))?
                    .is_some_and(|v| v[..33] == *encoded.as_ref())
                {
                    self.physical
                        .del(WriteFlags::CURRENT)
                        .map_err(|e| DatabaseError::Delete(e.into()))?;
                    self.shared.trie_changed(Some(s))?;
                }
                self.physical
                    .put(scope, value, WriteFlags::UPSERT)
                    .map_err(|e| DatabaseError::Read(e.into()))?;
                self.shared.trie_changed(Some(s))?;
            }
            if entry.nibbles.0.is_empty() {
                self.shared.mark_trie_root_written(s)?;
            }
        } else {
            if value.len() > 64 {
                return Err(DatabaseError::Decode);
            }
            self.shared.put(s, StorageEntry::from_compact(value, value.len()).0)?;
        }
        self.position = Position::Row((scope.to_vec(), value.to_vec()));
        Ok(())
    }

    pub(crate) fn delete(&mut self, duplicates: bool) -> Result<(), DatabaseError> {
        self.tx.txn_execute(|_| ()).map_err(|e| DatabaseError::Read(e.into()))?;
        self.shared.check()?;
        let Position::Row((scope, value)) = &self.position else { return Ok(()) };
        if self.trie {
            if duplicates || value[..33].iter().all(|b| *b == 0) {
                self.shared.invalidate_trie_root(Some(B256::from_slice(scope)))?;
            }

            if duplicates {
                if self
                    .physical
                    .set::<Vec<u8>>(scope)
                    .map_err(|e| DatabaseError::Read(e.into()))?
                    .is_some()
                {
                    self.physical
                        .del(WriteFlags::NO_DUP_DATA)
                        .map_err(|e| DatabaseError::Delete(e.into()))?;
                    self.shared.trie_changed(Some(B256::from_slice(scope)))?;
                }
            } else if self
                .physical
                .get_both_range::<Vec<u8>>(scope, &value[..33])
                .map_err(|e| DatabaseError::Read(e.into()))?
                .is_some_and(|v| v[..33] == value[..33])
            {
                self.physical
                    .del(WriteFlags::CURRENT)
                    .map_err(|e| DatabaseError::Delete(e.into()))?;
                self.shared.trie_changed(Some(B256::from_slice(scope)))?;
            }
        } else if duplicates {
            self.shared.clear_contract(B256::from_slice(scope))?;
        } else {
            self.shared.delete(B256::from_slice(scope), B256::from_slice(&value[..32]))?;
        }
        Ok(())
    }
}
