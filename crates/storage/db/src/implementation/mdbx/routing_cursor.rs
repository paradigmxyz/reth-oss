//! Cursor trait forwarding. Native table encodings and operations remain unchanged.

use super::{
    native_cursor::NativeCursor,
    packing::{Logical, Move},
    utils::decoder,
};
use crate::{metrics::TableOperationMetrics, DatabaseError};
use reth_db_api::{
    common::{PairResult, ValueOnlyResult},
    cursor::{
        DbCursorRO, DbCursorRW, DbDupCursorRO, DbDupCursorRW, DupWalker, RangeWalker,
        ReverseWalker, Walker,
    },
    table::{Compress, DupSort, Encode, Table},
};
use reth_libmdbx::{TransactionKind, RO, RW};
use std::{
    borrow::Cow,
    ops::{Bound, RangeBounds},
};

/// Read-only cursor over the selected database layout.
pub type CursorRO<T> = Cursor<RO, T>;
/// Read-write cursor over the selected database layout.
pub type CursorRW<T> = Cursor<RW, T>;

/// Native cursor by default; packed-state cursor only for an explicitly selected custom DB.
#[derive(Debug)]
pub enum Cursor<K: TransactionKind, T: Table> {
    /// Original MDBX table access.
    Native(NativeCursor<K, T>),
    /// Experimental logical state/trie access.
    #[allow(private_interfaces)]
    Packed(Box<Logical<K>>),
}

impl<K: TransactionKind, T: Table> Cursor<K, T> {
    pub(crate) const fn new_with_metrics(
        inner: reth_libmdbx::Cursor<K>,
        metrics: Option<TableOperationMetrics>,
    ) -> Self {
        Self::Native(NativeCursor::new_with_metrics(inner, metrics))
    }

    fn decode(row: Option<(Vec<u8>, Vec<u8>)>) -> PairResult<T> {
        row.map(|(k, v)| decoder::<T>((Cow::Owned(k), Cow::Owned(v)))).transpose()
    }
}

impl<K: TransactionKind, T: Table> DbCursorRO<T> for Cursor<K, T> {
    fn first(&mut self) -> PairResult<T> {
        match self {
            Self::Native(c) => c.first(),
            Self::Packed(c) => Self::decode(c.movement(Move::First)?),
        }
    }
    fn last(&mut self) -> PairResult<T> {
        match self {
            Self::Native(c) => c.last(),
            Self::Packed(c) => Self::decode(c.movement(Move::Last)?),
        }
    }
    fn current(&mut self) -> PairResult<T> {
        match self {
            Self::Native(c) => c.current(),
            Self::Packed(c) => Self::decode(c.movement(Move::Current)?),
        }
    }
    fn next(&mut self) -> PairResult<T> {
        match self {
            Self::Native(c) => c.next(),
            Self::Packed(c) => Self::decode(c.movement(Move::Next)?),
        }
    }
    fn prev(&mut self) -> PairResult<T> {
        match self {
            Self::Native(c) => c.prev(),
            Self::Packed(c) => Self::decode(c.movement(Move::Prev)?),
        }
    }
    fn seek(&mut self, key: T::Key) -> PairResult<T> {
        match self {
            Self::Native(c) => c.seek(key),
            Self::Packed(c) => Self::decode(c.seek(key.encode().as_ref(), None, false)?),
        }
    }
    fn seek_exact(&mut self, key: T::Key) -> PairResult<T> {
        match self {
            Self::Native(c) => c.seek_exact(key),
            Self::Packed(c) => Self::decode(c.seek(key.encode().as_ref(), None, true)?),
        }
    }
    fn walk(&mut self, start: Option<T::Key>) -> Result<Walker<'_, T, Self>, DatabaseError> {
        let first = if let Some(key) = start { self.seek(key) } else { self.first() }.transpose();
        Ok(Walker::new(self, first))
    }
    fn walk_back(
        &mut self,
        start: Option<T::Key>,
    ) -> Result<ReverseWalker<'_, T, Self>, DatabaseError> {
        let first = if let Some(key) = start { self.seek(key) } else { self.last() }.transpose();
        Ok(ReverseWalker::new(self, first))
    }
    fn walk_range(
        &mut self,
        range: impl RangeBounds<T::Key>,
    ) -> Result<RangeWalker<'_, T, Self>, DatabaseError> {
        let first = match range.start_bound().cloned() {
            Bound::Included(k) => self.seek(k),
            Bound::Unbounded => self.first(),
            Bound::Excluded(_) => {
                unreachable!("Rust doesn't allow for Bound::Excluded in starting bounds")
            }
        }
        .transpose();
        Ok(RangeWalker::new(self, first, range.end_bound().cloned()))
    }
}

impl<K: TransactionKind, T: DupSort> DbDupCursorRO<T> for Cursor<K, T> {
    fn next_dup(&mut self) -> PairResult<T> {
        match self {
            Self::Native(c) => c.next_dup(),
            Self::Packed(c) => Self::decode(c.movement(Move::NextDup)?),
        }
    }
    fn prev_dup(&mut self) -> PairResult<T> {
        match self {
            Self::Native(c) => c.prev_dup(),
            Self::Packed(c) => Self::decode(c.movement(Move::PrevDup)?),
        }
    }
    fn next_no_dup(&mut self) -> PairResult<T> {
        match self {
            Self::Native(c) => c.next_no_dup(),
            Self::Packed(c) => Self::decode(c.movement(Move::NextScope)?),
        }
    }
    fn last_dup(&mut self) -> ValueOnlyResult<T> {
        match self {
            Self::Native(c) => c.last_dup(),
            Self::Packed(c) => Ok(Self::decode(c.movement(Move::LastDup)?)?.map(|(_, v)| v)),
        }
    }
    fn next_dup_val(&mut self) -> ValueOnlyResult<T> {
        match self {
            Self::Native(c) => c.next_dup_val(),
            Self::Packed(c) => Ok(Self::decode(c.movement(Move::NextDup)?)?.map(|(_, v)| v)),
        }
    }
    fn seek_by_key_subkey(&mut self, key: T::Key, subkey: T::SubKey) -> ValueOnlyResult<T> {
        match self {
            Self::Native(c) => c.seek_by_key_subkey(key, subkey),
            Self::Packed(c) => Ok(Self::decode(c.seek(
                key.encode().as_ref(),
                Some(subkey.encode().as_ref()),
                true,
            )?)?
            .map(|(_, v)| v)),
        }
    }
    fn walk_dup(
        &mut self,
        key: Option<T::Key>,
        sub: Option<T::SubKey>,
    ) -> Result<DupWalker<'_, T, Self>, DatabaseError> {
        let first = match (key, sub) {
            (Some(k), Some(s)) => self.seek_by_key_subkey(k.clone(), s).map(|v| v.map(|v| (k, v))),
            (Some(k), None) => self.seek_exact(k),
            (None, Some(s)) => {
                if let Some((k, _)) = self.first()? {
                    self.seek_by_key_subkey(k.clone(), s).map(|v| v.map(|v| (k, v)))
                } else {
                    Err(DatabaseError::Read(reth_libmdbx::Error::NotFound.into()))
                }
            }
            (None, None) => self.first(),
        }
        .transpose();
        Ok(DupWalker { cursor: self, start: first })
    }
}

impl<T: Table> DbCursorRW<T> for Cursor<RW, T> {
    fn upsert(&mut self, key: T::Key, value: &T::Value) -> Result<(), DatabaseError> {
        match self {
            Self::Native(c) => c.upsert(key, value),
            Self::Packed(c) => {
                c.write(key.encode().as_ref(), encoded_value::<T>(value).as_slice(), false, false)
            }
        }
    }
    fn insert(&mut self, key: T::Key, value: &T::Value) -> Result<(), DatabaseError> {
        match self {
            Self::Native(c) => c.insert(key, value),
            Self::Packed(c) => {
                c.write(key.encode().as_ref(), encoded_value::<T>(value).as_slice(), true, false)
            }
        }
    }
    fn append(&mut self, key: T::Key, value: &T::Value) -> Result<(), DatabaseError> {
        match self {
            Self::Native(c) => c.append(key, value),
            Self::Packed(c) => {
                c.write(key.encode().as_ref(), encoded_value::<T>(value).as_slice(), false, true)
            }
        }
    }
    fn delete_current(&mut self) -> Result<(), DatabaseError> {
        match self {
            Self::Native(c) => c.delete_current(),
            Self::Packed(c) => c.delete(false),
        }
    }
}

impl<T: DupSort> DbDupCursorRW<T> for Cursor<RW, T> {
    fn delete_current_duplicates(&mut self) -> Result<(), DatabaseError> {
        match self {
            Self::Native(c) => c.delete_current_duplicates(),
            Self::Packed(c) => c.delete(true),
        }
    }
    fn append_dup(&mut self, key: T::Key, value: T::Value) -> Result<(), DatabaseError> {
        match self {
            Self::Native(c) => c.append_dup(key, value),
            Self::Packed(c) => {
                c.write(key.encode().as_ref(), encoded_value::<T>(&value).as_slice(), false, true)
            }
        }
    }
}

fn encoded_value<T: Table>(value: &T::Value) -> Vec<u8> {
    let mut bytes = Vec::new();
    value.compress_to_buf(&mut bytes);
    bytes
}
