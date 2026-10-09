//! Snapshot and ordering regressions for opt-in state packing.

use super::*;
use crate::{
    cursor::{DbCursorRO, DbCursorRW, DbDupCursorRO, DbDupCursorRW},
    init_db,
    mdbx::{cursor::Cursor, DatabaseArguments},
    tables::{HashedStorages, PackedStoragesTrie},
    Database,
};
use alloy_primitives::{keccak256, B256, U256};
use alloy_trie::{HashBuilder, Nibbles};
use proptest::prelude::*;
use reth_db_api::{
    table::{DupSort, Table},
    transaction::{DbTx, DbTxMut},
};
use reth_primitives_traits::StorageEntry;
use std::{collections::BTreeMap, time::Instant};
use tempfile::tempdir;

type TrieEntry = <PackedStoragesTrie as Table>::Value;

fn fixture(n: usize) -> Vec<StorageEntry> {
    let mut rows: Vec<_> = (0..n)
        .map(|i| {
            StorageEntry::new(
                keccak256(i.to_be_bytes()),
                match i % 5 {
                    0 => U256::MAX,
                    1 => U256::from(1),
                    _ => U256::from(i + 1),
                },
            )
        })
        .collect();
    rows.sort_by_key(|r| r.key);
    rows
}

#[test]
fn cursor_seek_exhaustion_matches_native() {
    let mut reference = None;
    for mode in [None, Some(PackingMode::Dense), Some(PackingMode::Integer32)] {
        let dir = tempdir().unwrap();
        let db = init_db(dir.path(), DatabaseArguments::test().with_experimental_packing(mode, 3))
            .unwrap();
        let scope = B256::repeat_byte(1);
        let beyond = B256::repeat_byte(2);
        let rows = fixture(64);
        let tx = db.tx_mut().unwrap();
        let mut builder = HashBuilder::default().with_updates(true);
        for row in &rows {
            tx.put::<HashedStorages>(scope, *row).unwrap();
            builder.add_leaf(
                Nibbles::unpack(row.key),
                alloy_rlp::encode_fixed_size(&row.value).as_ref(),
            );
        }
        builder.root();
        for (path, node) in builder.split().1 {
            tx.put::<PackedStoragesTrie>(scope, TrieEntry { nibbles: path.into(), node }).unwrap();
        }
        tx.commit().unwrap();
        let tx = db.tx().unwrap();
        let mut storage = tx.cursor_dup_read::<HashedStorages>().unwrap();
        let mut trie = tx.cursor_dup_read::<PackedStoragesTrie>().unwrap();
        let storage_walk =
            storage.walk(Some(beyond)).unwrap().collect::<Result<Vec<_>, _>>().unwrap();
        let trie_walk = trie.walk(Some(beyond)).unwrap().collect::<Result<Vec<_>, _>>().unwrap();
        let result = (
            storage_walk,
            trie_walk,
            seek_trace(&mut storage, beyond),
            seek_trace(&mut trie, beyond),
        );
        if let Some(expected) = &reference {
            assert_eq!(&result, expected, "cursor positioning differs for {mode:?}");
        } else {
            reference = Some(result);
        }
    }
}

fn seek_trace<T: DupSort<Key = B256>>(
    cursor: &mut (impl DbDupCursorRO<T> + DbCursorRO<T>),
    key: B256,
) -> Vec<Option<(B256, T::Value)>> {
    vec![
        cursor.seek(key).unwrap(),
        cursor.next().unwrap(),
        cursor.next().unwrap(),
        cursor.prev().unwrap(),
        cursor.current().unwrap(),
        cursor.next().unwrap(),
        cursor.next().unwrap(),
        cursor.first().unwrap(),
        cursor.seek_exact(key).unwrap(),
        cursor.next().unwrap(),
        cursor.prev().unwrap(),
        cursor.last().unwrap(),
        cursor.next_no_dup().unwrap(),
        cursor.next_no_dup().unwrap(),
        cursor.next().unwrap(),
        cursor.prev().unwrap(),
        cursor.last().unwrap(),
        cursor.next().unwrap(),
        cursor.prev().unwrap(),
    ]
}

#[test]
fn dense_cursor_persistence_snapshots_abort_and_reopen() {
    let dir = tempdir().unwrap();
    let args = DatabaseArguments::test().with_experimental_packing(Some(PackingMode::Dense), 3);
    let db = init_db(dir.path(), args.clone()).unwrap();
    let scope = B256::repeat_byte(1);
    let other = B256::repeat_byte(2);
    let rows = fixture(1300);
    let tx = db.tx_mut().unwrap();
    for row in &rows {
        tx.put::<HashedStorages>(scope, *row).unwrap();
    }
    tx.put::<HashedStorages>(other, StorageEntry::new(B256::ZERO, U256::from(7))).unwrap();
    assert_eq!(tx.entries::<HashedStorages>().unwrap(), 1301);
    let mut c = tx.cursor_dup_read::<HashedStorages>().unwrap();
    assert_eq!(c.seek_by_key_subkey(scope, rows[517].key).unwrap(), Some(rows[517]));
    drop(c);
    tx.commit().unwrap();
    let snapshot = db.tx().unwrap();
    let mut old = snapshot.cursor_dup_read::<HashedStorages>().unwrap();
    assert_eq!(
        old.walk_dup(Some(scope), None).unwrap().collect::<Result<Vec<_>, _>>().unwrap(),
        rows.iter().map(|r| (scope, *r)).collect::<Vec<_>>()
    );
    assert!(
        snapshot
            .inner()
            .db_stat_with_dbi(snapshot.get_dbi::<HashedStorages>().unwrap())
            .unwrap()
            .entries() ==
            0
    );
    let tx = db.tx_mut().unwrap();
    let mut c = tx.cursor_dup_write::<HashedStorages>().unwrap();
    c.seek_by_key_subkey(scope, rows[517].key).unwrap();
    c.delete_current().unwrap();
    c.upsert(scope, &StorageEntry::new(rows[600].key, U256::from(999))).unwrap();
    drop(c);
    tx.commit().unwrap();
    assert_eq!(old.seek_by_key_subkey(scope, rows[517].key).unwrap(), Some(rows[517]));
    let latest = db.tx().unwrap();
    let mut c = latest.cursor_dup_read::<HashedStorages>().unwrap();
    assert_eq!(c.seek_by_key_subkey(scope, rows[517].key).unwrap(), Some(rows[518]));
    assert_eq!(c.seek_by_key_subkey(scope, rows[600].key).unwrap().unwrap().value, U256::from(999));
    assert_eq!(c.next_no_dup().unwrap().unwrap().0, other);
    drop(c);
    latest.abort();
    drop(old);
    snapshot.abort();
    let aborted = db.tx_mut().unwrap();
    aborted.delete::<HashedStorages>(scope, None).unwrap();
    aborted.abort();
    assert_eq!(db.tx().unwrap().entries::<HashedStorages>().unwrap(), 1300);
    drop(db);
    assert!(init_db(dir.path(), DatabaseArguments::test()).is_err());
    assert!(init_db(
        dir.path(),
        args.clone().with_experimental_packing(Some(PackingMode::Dense), 4)
    )
    .is_err());
    let db = init_db(dir.path(), args).unwrap();
    assert_eq!(db.tx().unwrap().entries::<HashedStorages>().unwrap(), 1300);
    let tx = db.tx_mut().unwrap();
    tx.delete::<HashedStorages>(scope, None).unwrap();
    tx.commit().unwrap();
    let tx = db.tx().unwrap();
    assert_eq!(tx.entries::<HashedStorages>().unwrap(), 1);
}

#[test]
fn reconstructed_trie_cursor_is_complete_and_ordered() {
    let dir = tempdir().unwrap();
    let db = init_db(
        dir.path(),
        DatabaseArguments::test().with_experimental_packing(Some(PackingMode::Dense), 2),
    )
    .unwrap();
    let scope = B256::repeat_byte(3);
    let rows = fixture(8192);
    let mut hb = HashBuilder::default().with_updates(true);
    for row in &rows {
        hb.add_leaf(Nibbles::unpack(row.key), alloy_rlp::encode_fixed_size(&row.value).as_ref());
    }
    let expected_root = hb.root();
    let expected: BTreeMap<_, _> = hb.split().1.into_iter().collect();
    let tx = db.tx_mut().unwrap();
    for row in &rows {
        tx.put::<HashedStorages>(scope, *row).unwrap();
    }
    let mut c = tx.cursor_dup_write::<PackedStoragesTrie>().unwrap();
    for (path, node) in &expected {
        c.upsert(scope, &TrieEntry { nibbles: (*path).into(), node: node.clone() }).unwrap();
    }
    drop(c);
    tx.commit().unwrap();
    let tx = db.tx().unwrap();
    let physical =
        tx.inner().db_stat_with_dbi(tx.get_dbi::<PackedStoragesTrie>().unwrap()).unwrap().entries();
    assert_eq!(physical, expected.keys().filter(|p| p.len() < 2).count());
    assert!(physical < expected.len());
    let mut c = tx.cursor_dup_read::<PackedStoragesTrie>().unwrap();
    let actual: BTreeMap<_, _> = c
        .walk_dup(Some(scope), None)
        .unwrap()
        .map(|r| r.map(|(_, e)| (e.nibbles.0, e.node)))
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(actual, expected);
    assert_eq!(actual[&Nibbles::default()].root_hash, Some(expected_root));
    let reverse = c
        .walk_back(None)
        .unwrap()
        .map(|r| r.map(|(_, e)| (e.nibbles.0, e.node)))
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(reverse, expected.iter().rev().map(|(p, n)| (*p, n.clone())).collect::<Vec<_>>());
    for path in expected.keys().step_by(19) {
        let got = c.seek_by_key_subkey(scope, (*path).into()).unwrap().unwrap();
        assert_eq!(got.nibbles.0, *path);
        assert_eq!(got.node, expected[path]);
    }
}

#[test]
fn native_directory_is_never_converted_in_place() {
    let dir = tempdir().unwrap();
    let db = init_db(dir.path(), DatabaseArguments::test()).unwrap();
    let tx = db.tx_mut().unwrap();
    tx.put::<HashedStorages>(B256::ZERO, StorageEntry::new(B256::ZERO, U256::from(8))).unwrap();
    tx.commit().unwrap();
    drop(db);
    assert!(init_db(
        dir.path(),
        DatabaseArguments::test().with_experimental_packing(Some(PackingMode::Dense), 3)
    )
    .is_err());
    assert_eq!(crate::version::get_db_version(dir.path()).unwrap(), crate::version::DB_VERSION);
    let db = init_db(dir.path(), DatabaseArguments::test()).unwrap();
    assert_eq!(
        db.tx().unwrap().get::<HashedStorages>(B256::ZERO).unwrap().unwrap().value,
        U256::from(8)
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]
    #[test]
    fn state_deltas_match_ordered_model(integer in any::<bool>(), ops in prop::collection::vec((0u8..3, any::<[u8;32]>(), any::<u64>(), 0u8..6),1..160)) {
        let dir=tempdir().unwrap();
        let db=init_db(dir.path(),DatabaseArguments::test().with_experimental_packing(Some(if integer {PackingMode::Integer32} else {PackingMode::Dense}),3)).unwrap();
        let mut model=BTreeMap::new();
        for batch in ops.chunks(20) {
            let tx=db.tx_mut().unwrap();
            for (contract,key,value,op) in batch {
                let scope=B256::repeat_byte(*contract);
                let key=B256::from(*key);
                match op {
                    0=>{tx.delete::<HashedStorages>(scope,None).unwrap();model.retain(|(s,_),_|*s!=scope);},
                    1=>{let mut c=tx.cursor_dup_write::<HashedStorages>().unwrap();
                        if c.seek_by_key_subkey(scope,key).unwrap().is_some_and(|r|r.key==key) {c.delete_current().unwrap();}
                        model.remove(&(scope,key));},
                    _=>{let value=U256::from(*value);tx.put::<HashedStorages>(scope,StorageEntry::new(key,value)).unwrap();model.insert((scope,key),value);},
                }
            }
            let expected:Vec<_>=model.iter().map(|((s,k),v)|(*s,StorageEntry::new(*k,*v))).collect();
            let mut c=tx.cursor_dup_read::<HashedStorages>().unwrap();
            prop_assert_eq!(c.walk(None).unwrap().collect::<Result<Vec<_>,_>>().unwrap(),expected.clone());
            drop(c);tx.commit().unwrap();
            let tx=db.tx().unwrap();let mut c=tx.cursor_dup_read::<HashedStorages>().unwrap();
            prop_assert_eq!(c.walk(None).unwrap().collect::<Result<Vec<_>,_>>().unwrap(),expected.clone());
            prop_assert_eq!(c.walk_back(None).unwrap().collect::<Result<Vec<_>,_>>().unwrap(),expected.into_iter().rev().collect::<Vec<_>>());
        }
    }
}

#[test]
fn reconstruction_handles_long_common_prefix_and_cross_cursor_updates() {
    let dir = tempdir().unwrap();
    let db = init_db(
        dir.path(),
        DatabaseArguments::test().with_experimental_packing(Some(PackingMode::Integer32), 3),
    )
    .unwrap();
    let scope = B256::repeat_byte(4);
    let rows: Vec<_> = (1..258)
        .map(|i| StorageEntry::new(B256::from(U256::from(i).to_be_bytes::<32>()), U256::from(i)))
        .collect();
    let tx = db.tx_mut().unwrap();
    for row in &rows {
        tx.put::<HashedStorages>(scope, *row).unwrap();
    }
    let mut reader = tx.cursor_dup_read::<HashedStorages>().unwrap();
    reader.seek_by_key_subkey(scope, rows[128].key).unwrap();
    tx.put::<HashedStorages>(scope, StorageEntry::new(rows[128].key, U256::from(999))).unwrap();
    assert_eq!(reader.current().unwrap().unwrap().1.value, U256::from(999));
    drop(reader);
    tx.commit().unwrap();
    let mut hb = HashBuilder::default().with_updates(true);
    for (i, row) in rows.iter().enumerate() {
        let value = if i == 128 { U256::from(999) } else { row.value };
        hb.add_leaf(Nibbles::unpack(row.key), alloy_rlp::encode_fixed_size(&value).as_ref());
    }
    hb.root();
    let expected: BTreeMap<_, _> = hb.split().1.into_iter().collect();
    assert!(expected.keys().all(|p| p.len() >= 3));
    let tx = db.tx().unwrap();
    let mut c = tx.cursor_dup_read::<PackedStoragesTrie>().unwrap();
    let actual: BTreeMap<_, _> = c
        .walk_dup(Some(scope), None)
        .unwrap()
        .map(|r| r.map(|(_, e)| (e.nibbles.0, e.node)))
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(actual, expected);
    drop(c);
    tx.abort();
}

#[test]
fn dirty_trie_writer_hashes_upper_once_per_storage_generation() {
    for mode in [PackingMode::Dense, PackingMode::Integer32] {
        let dir = tempdir().unwrap();
        let db =
            init_db(dir.path(), DatabaseArguments::test().with_experimental_packing(Some(mode), 3))
                .unwrap();
        let scope = B256::repeat_byte(6);
        let mut rows = fixture(8192);
        let tx = db.tx_mut().unwrap();
        for row in &rows {
            tx.put::<HashedStorages>(scope, *row).unwrap();
        }
        tx.commit().unwrap();
        let tx = db.tx_mut().unwrap();
        rows[0].value = U256::from(100_000);
        tx.put::<HashedStorages>(scope, rows[0]).unwrap();
        let expected = trie_nodes(&rows);
        let mut writer = tx.cursor_dup_write::<PackedStoragesTrie>().unwrap();
        let start = Instant::now();
        for (path, node) in expected.iter().take(100) {
            let found = writer.seek_by_key_subkey(scope, (*path).into()).unwrap().unwrap();
            assert_eq!(found.nibbles.0, *path);
            writer.delete_current().unwrap();
            writer
                .upsert(scope, &TrieEntry { nibbles: (*path).into(), node: node.clone() })
                .unwrap();
            assert_eq!(upper_stats(&writer), (1, rows.len()));
        }
        eprintln!(
            "{mode:?}: 100 production-style trie replacements: {:?}; upper rebuilds={}, leaves={}",
            start.elapsed(),
            upper_stats(&writer).0,
            upper_stats(&writer).1
        );
        writer.delete_current_duplicates().unwrap();
        let root = writer.seek_by_key_subkey(scope, Nibbles::default().into()).unwrap().unwrap();
        assert_eq!(root.node, expected[&Nibbles::default()]);
        assert_eq!(upper_stats(&writer), (1, rows.len()));
        rows[0].value = U256::from(100_001);
        tx.put::<HashedStorages>(scope, rows[0]).unwrap();
        let root = writer.seek_by_key_subkey(scope, Nibbles::default().into()).unwrap().unwrap();
        assert_eq!(root.node, trie_nodes(&rows)[&Nibbles::default()]);
        assert_eq!(upper_stats(&writer), (2, rows.len() * 2));
    }
}

#[test]
fn retained_trie_changes_invalidate_other_clean_cursors() {
    for mode in [PackingMode::Dense, PackingMode::Integer32] {
        let dir = tempdir().unwrap();
        let db =
            init_db(dir.path(), DatabaseArguments::test().with_experimental_packing(Some(mode), 3))
                .unwrap();
        let scope = B256::repeat_byte(7);
        let rows = fixture(1024);
        let tx = db.tx_mut().unwrap();
        for row in &rows {
            tx.put::<HashedStorages>(scope, *row).unwrap();
        }
        tx.commit().unwrap();
        let tx = db.tx_mut().unwrap();
        let mut reader = tx.cursor_dup_read::<PackedStoragesTrie>().unwrap();
        let mut writer = tx.cursor_dup_write::<PackedStoragesTrie>().unwrap();
        let mut root = reader.seek_exact(scope).unwrap().unwrap().1;
        let canonical = root.clone();
        root.node.root_hash = Some(B256::repeat_byte(9));
        writer.upsert(scope, &root).unwrap();
        assert_eq!(reader.current().unwrap().unwrap().1, root);
        writer.seek_by_key_subkey(scope, Nibbles::default().into()).unwrap();
        writer.delete_current().unwrap();
        assert!(!reader.seek_exact(scope).unwrap().unwrap().1.nibbles.0.is_empty());
        writer.upsert(scope, &canonical).unwrap();
        assert_eq!(reader.seek_exact(scope).unwrap().unwrap().1, canonical);
        tx.clear::<PackedStoragesTrie>().unwrap();
        assert_eq!(reader.seek_exact(scope).unwrap().unwrap().1, canonical);
    }
}

fn upper_stats<K: reth_libmdbx::TransactionKind>(
    cursor: &Cursor<K, PackedStoragesTrie>,
) -> (usize, usize) {
    let Cursor::Packed(logical) = cursor else { panic!("expected packed cursor") };
    (logical.upper_rebuilds, logical.upper_leaves)
}

fn trie_nodes(rows: &[StorageEntry]) -> BTreeMap<Nibbles, alloy_trie::BranchNodeCompact> {
    let mut builder = HashBuilder::default().with_updates(true);
    for row in rows {
        builder
            .add_leaf(Nibbles::unpack(row.key), alloy_rlp::encode_fixed_size(&row.value).as_ref());
    }
    builder.root();
    builder.split().1.into_iter().collect()
}

#[test]
fn scoped_seek_and_duplicate_iteration_match_native() {
    let mut reference = None;
    for mode in [None, Some(PackingMode::Dense), Some(PackingMode::Integer32)] {
        let dir = tempdir().unwrap();
        let db = init_db(dir.path(), DatabaseArguments::test().with_experimental_packing(mode, 3))
            .unwrap();
        let rows = fixture(64);
        let nodes = trie_nodes(&rows);
        let tx = db.tx_mut().unwrap();
        for scope in [B256::repeat_byte(1), B256::repeat_byte(3), B256::repeat_byte(5)] {
            for row in &rows {
                tx.put::<HashedStorages>(scope, *row).unwrap();
            }
            for (path, node) in &nodes {
                tx.put::<PackedStoragesTrie>(
                    scope,
                    TrieEntry { nibbles: (*path).into(), node: node.clone() },
                )
                .unwrap();
            }
        }
        tx.commit().unwrap();
        let tx = db.tx().unwrap();
        let mut storage = tx.cursor_dup_read::<HashedStorages>().unwrap();
        let mut trie = tx.cursor_dup_read::<PackedStoragesTrie>().unwrap();
        let missing = B256::repeat_byte(2);
        assert!(storage.walk_dup(Some(missing), None).unwrap().next().is_none());
        assert!(trie.walk_dup(Some(missing), None).unwrap().next().is_none());
        let scope = B256::repeat_byte(3);
        let result = (
            duplicate_seek_trace(&mut storage, scope, B256::repeat_byte(255)),
            duplicate_seek_trace(&mut trie, scope, Nibbles::unpack(B256::repeat_byte(255)).into()),
        );
        if let Some(expected) = &reference {
            assert_eq!(&result, expected, "duplicate positioning differs for {mode:?}");
        } else {
            reference = Some(result);
        }
    }
}

fn duplicate_seek_trace<T: DupSort<Key = B256>>(
    cursor: &mut (impl DbDupCursorRO<T> + DbCursorRO<T>),
    key: B256,
    subkey: T::SubKey,
) -> Vec<Option<(B256, T::Value)>> {
    vec![
        cursor.first().unwrap(),
        cursor.seek_by_key_subkey(key, subkey).unwrap().map(|v| (key, v)),
        cursor.next_dup().unwrap(),
        cursor.prev_dup().unwrap(),
        cursor.next_no_dup().unwrap(),
        cursor.next().unwrap(),
        cursor.prev().unwrap(),
    ]
}
