//! Snapshot and ordering regressions for opt-in state packing.

use super::*;
use crate::{
    cursor::{DbCursorRO, DbCursorRW, DbDupCursorRO},
    init_db,
    mdbx::DatabaseArguments,
    tables::{HashedStorages, PackedStoragesTrie},
    Database,
};
use alloy_primitives::{keccak256, B256, U256};
use alloy_trie::{HashBuilder, Nibbles};
use proptest::prelude::*;
use reth_db_api::{
    table::Table,
    transaction::{DbTx, DbTxMut},
};
use reth_primitives_traits::StorageEntry;
use std::collections::BTreeMap;
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
