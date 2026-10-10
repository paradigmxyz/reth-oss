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
fn cursor_multicontract_reversal_matches_native() {
    for count in [1, 3, 64] {
        let mut reference = None;
        for mode in [None, Some(PackingMode::Dense), Some(PackingMode::Integer32)] {
            let dir = tempdir().unwrap();
            let db =
                init_db(dir.path(), DatabaseArguments::test().with_experimental_packing(mode, 3))
                    .unwrap();
            let tx = db.tx_mut().unwrap();
            for contract in 1..=3 {
                let scope = B256::repeat_byte(contract);
                let mut builder = HashBuilder::default().with_updates(true);
                for row in fixture(count) {
                    tx.put::<HashedStorages>(scope, row).unwrap();
                    builder.add_leaf(
                        Nibbles::unpack(row.key),
                        alloy_rlp::encode_fixed_size(&row.value).as_ref(),
                    );
                }
                builder.root();
                for (path, node) in builder.split().1 {
                    tx.put::<PackedStoragesTrie>(scope, TrieEntry { nibbles: path.into(), node })
                        .unwrap();
                }
            }
            tx.commit().unwrap();
            let tx = db.tx().unwrap();
            let result = (
                reversal_trace(&mut tx.cursor_dup_read::<HashedStorages>().unwrap()),
                reversal_trace(&mut tx.cursor_dup_read::<PackedStoragesTrie>().unwrap()),
                walker_reversal_trace(&mut tx.cursor_dup_read::<HashedStorages>().unwrap()),
                walker_reversal_trace(&mut tx.cursor_dup_read::<PackedStoragesTrie>().unwrap()),
            );
            if let Some(expected) = &reference {
                assert_eq!(&result, expected, "reversal differs for {mode:?}, {count} slots");
            } else {
                reference = Some(result);
            }
        }
    }
}

fn walker_reversal_trace<T: DupSort<Key = B256>>(
    cursor: &mut (impl DbDupCursorRO<T> + DbCursorRO<T>),
) -> Vec<(B256, T::Value)> {
    let mut walk = cursor.walk(None).unwrap();
    for row in walk.by_ref() {
        row.unwrap();
    }
    let mut rows = walk.rev().collect::<Result<Vec<_>, _>>().unwrap();
    let mut walk = cursor.walk_back(None).unwrap();
    for row in walk.by_ref() {
        row.unwrap();
    }
    rows.extend(walk.forward().collect::<Result<Vec<_>, _>>().unwrap());
    rows
}

fn reversal_trace<T: DupSort<Key = B256>>(
    cursor: &mut (impl DbDupCursorRO<T> + DbCursorRO<T>),
) -> Vec<Option<(B256, T::Value)>> {
    vec![
        cursor.last().unwrap(),
        cursor.next().unwrap(),
        cursor.current().unwrap(),
        cursor.next().unwrap(),
        cursor.prev().unwrap(),
        cursor.current().unwrap(),
        cursor.next().unwrap(),
        cursor.first().unwrap(),
        cursor.prev().unwrap(),
        cursor.current().unwrap(),
        cursor.prev().unwrap(),
        cursor.next().unwrap(),
        cursor.current().unwrap(),
        cursor.prev().unwrap(),
        cursor.last().unwrap(),
        cursor.next_no_dup().unwrap(),
        cursor.current().unwrap(),
        cursor.prev().unwrap(),
        cursor.last().unwrap(),
        cursor.next_dup().unwrap(),
        cursor.prev().unwrap(),
        cursor.first().unwrap(),
        cursor.prev_dup().unwrap(),
        cursor.next().unwrap(),
    ]
}

#[test]
fn inline_tier_transitions_are_atomic() {
    for mode in [PackingMode::Dense, PackingMode::Integer32] {
        let dir = tempdir().unwrap();
        let args = DatabaseArguments::test().with_experimental_packing(Some(mode), 3);
        let db = init_db(dir.path(), args.clone()).unwrap();
        let scope = B256::repeat_byte(3);
        let rows = fixture(80);
        let mut expected = Vec::new();
        for count in [1, 4, 80, 4, 1, 0, 1] {
            let old = db.tx().unwrap();
            let tx = db.tx_mut().unwrap();
            tx.delete::<HashedStorages>(scope, None).unwrap();
            for row in &rows[..count] {
                tx.put::<HashedStorages>(scope, *row).unwrap();
            }
            let pending = tx
                .cursor_dup_read::<HashedStorages>()
                .unwrap()
                .walk(None)
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert_eq!(pending, rows[..count].iter().map(|row| (scope, *row)).collect::<Vec<_>>());
            tx.abort();
            assert_eq!(
                db.tx()
                    .unwrap()
                    .cursor_dup_read::<HashedStorages>()
                    .unwrap()
                    .walk(None)
                    .unwrap()
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap(),
                expected
            );
            let tx = db.tx_mut().unwrap();
            tx.delete::<HashedStorages>(scope, None).unwrap();
            for row in &rows[..count] {
                tx.put::<HashedStorages>(scope, *row).unwrap();
            }
            tx.commit().unwrap();
            assert_eq!(
                old.cursor_dup_read::<HashedStorages>()
                    .unwrap()
                    .walk(None)
                    .unwrap()
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap(),
                expected
            );
            old.abort();
            expected = rows[..count].iter().map(|row| (scope, *row)).collect();
            let tx = db.tx().unwrap();
            let dbi = tx.inner().open_db(Some(TABLE)).unwrap().dbi();
            let record =
                tx.inner().cursor_with_dbi(dbi).unwrap().first::<Vec<u8>, Vec<u8>>().unwrap();
            if count > 0 {
                let (anchor, bytes) = record.unwrap();
                assert_eq!(bytes[0] & 0x80 != 0, count < 80);
                assert_eq!(
                    codec::Blob::parse_record(&bytes, &anchor, mode).unwrap().rows().unwrap(),
                    rows[..count]
                );
            } else {
                assert!(record.is_none());
            }
            tx.abort();
        }
        drop(db);
        let db = init_db(dir.path(), args).unwrap();
        let tx = db.tx().unwrap();
        assert_eq!(
            tx.cursor_dup_read::<HashedStorages>()
                .unwrap()
                .walk(None)
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap(),
            expected
        );
    }
}

#[test]
fn old_experimental_versions_are_rejected() {
    for (mode, version) in [(PackingMode::Dense, 10001), (PackingMode::Integer32, 10002)] {
        let dir = tempdir().unwrap();
        reth_fs_util::write(crate::version::db_version_file_path(dir.path()), version.to_string())
            .unwrap();
        reth_fs_util::write(dir.path().join(CONFIG_FILE), format!("{version}:3")).unwrap();
        assert!(check_directory(dir.path(), Some(mode), 3).is_err());
        assert!(check_directory(dir.path(), None, 3).is_err());
        assert!(prepare_directory(dir.path(), mode, 3).is_err());
        assert_eq!(crate::version::get_db_version(dir.path()).unwrap(), version);
    }
}

/// Allocation census for the audit's many-small-contracts fixture, including a full-blob control.
#[test]
#[ignore = "explicit singleton allocation experiment"]
fn benchmark_singleton_allocation() {
    println!("mode,representation,live_storage_bytes");
    let mut native = 0;
    for mode in [None, Some(PackingMode::Dense), Some(PackingMode::Integer32)] {
        let mut sizes = Vec::new();
        for full in [false, true] {
            if mode.is_none() && full {
                continue;
            }
            let dir = tempdir().unwrap();
            let db =
                init_db(dir.path(), DatabaseArguments::test().with_experimental_packing(mode, 3))
                    .unwrap();
            let tx = db.tx_mut().unwrap();
            let mut records: Vec<_> = (0..4096u64)
                .map(|i| {
                    (
                        keccak256(i.to_be_bytes()),
                        StorageEntry::new(keccak256((i + 4096).to_be_bytes()), U256::ONE),
                    )
                })
                .collect();
            records.sort_by_key(|(scope, _)| *scope);
            for (scope, row) in records {
                if full {
                    let dbi = tx.inner().open_db(Some(TABLE)).unwrap().dbi();
                    tx.inner()
                        .put(
                            dbi,
                            store::key(scope, row.key),
                            codec::encode(&[row], mode.unwrap()).unwrap(),
                            reth_libmdbx::WriteFlags::UPSERT,
                        )
                        .unwrap();
                } else {
                    tx.put::<HashedStorages>(scope, row).unwrap();
                }
            }
            tx.commit().unwrap();
            let tx = db.tx().unwrap();
            assert_eq!(tx.entries::<HashedStorages>().unwrap(), 4096);
            let dbi = tx
                .inner()
                .open_db(Some(if mode.is_some() { TABLE } else { HashedStorages::NAME }))
                .unwrap()
                .dbi();
            let stat = tx.inner().db_stat_with_dbi(dbi).unwrap();
            let live = (stat.branch_pages() + stat.leaf_pages() + stat.overflow_pages()) *
                stat.page_size() as usize;
            println!("{mode:?},{},{live}", if full { "full" } else { "inline/native" });
            sizes.push(live);
            if mode.is_none() {
                native = live;
            }
        }
        if mode.is_some() {
            assert!(sizes[0] < sizes[1]);
            println!("{mode:?} inline/native={:.4}", sizes[0] as f64 / native as f64);
        }
    }
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
            assert_eq!(upper_stats(&writer).0, 1);
            assert!(upper_stats(&writer).1 < 256, "unchanged subtree hashes were not reused");
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
        let first = upper_stats(&writer);
        assert_eq!(first.0, 1);
        rows[0].value = U256::from(100_001);
        tx.put::<HashedStorages>(scope, rows[0]).unwrap();
        let root = writer.seek_by_key_subkey(scope, Nibbles::default().into()).unwrap().unwrap();
        assert_eq!(root.node, trie_nodes(&rows)[&Nibbles::default()]);
        assert_eq!(upper_stats(&writer).0, 2);
        assert!(upper_stats(&writer).1 < 512);
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

#[test]
fn shared_reconstruction_survives_unrelated_writes() {
    for mode in [PackingMode::Dense, PackingMode::Integer32] {
        let dir = tempdir().unwrap();
        let db =
            init_db(dir.path(), DatabaseArguments::test().with_experimental_packing(Some(mode), 2))
                .unwrap();
        let scope = B256::repeat_byte(10);
        let other = B256::repeat_byte(11);
        let rows = fixture(8192);
        let tx = db.tx_mut().unwrap();
        for row in &rows {
            tx.put::<HashedStorages>(scope, *row).unwrap();
        }
        tx.commit().unwrap();
        let tx = db.tx_mut().unwrap();
        let mut reader = tx.cursor_dup_read::<PackedStoragesTrie>().unwrap();
        let expected = trie_nodes(&rows);
        let paths: Vec<_> = expected.keys().filter(|p| p.len() >= 2).copied().collect();
        let a = paths[0];
        let b = *paths.iter().find(|p| p.slice(..2) != a.slice(..2)).unwrap();
        for path in [a, b, a, b] {
            let got = reader.seek_by_key_subkey(scope, path.into()).unwrap().unwrap();
            assert_eq!(got.node, expected[&path]);
        }
        assert_eq!(region_stats(&reader), 2);
        tx.put::<HashedStorages>(other, StorageEntry::new(rows[0].key, U256::ONE)).unwrap();
        reader.seek_by_key_subkey(scope, a.into()).unwrap();
        assert_eq!(region_stats(&reader), 2);
        let mut second = tx.cursor_dup_read::<PackedStoragesTrie>().unwrap();
        second.seek_by_key_subkey(scope, b.into()).unwrap();
        assert_eq!(region_stats(&second), 0, "another cursor must share the cached region");
        let changed =
            *rows.iter().find(|r| Nibbles::unpack(r.key).starts_with(&a.slice(..2))).unwrap();
        tx.put::<HashedStorages>(scope, StorageEntry::new(changed.key, U256::from(90000))).unwrap();
        second.seek_by_key_subkey(scope, b.into()).unwrap();
        assert_eq!(
            region_stats(&second),
            0,
            "another region's mutation must not invalidate this one"
        );
        second.seek_by_key_subkey(scope, a.into()).unwrap();
        assert_eq!(region_stats(&second), 1);
        tx.delete::<HashedStorages>(scope, None).unwrap();
        assert!(reader.seek_exact(scope).unwrap().is_none());
        tx.clear::<HashedStorages>().unwrap();
        assert!(second.first().unwrap().is_none());
        for row in &rows[..64] {
            tx.put::<HashedStorages>(scope, *row).unwrap();
        }
        let expected = trie_nodes(&rows[..64]);
        assert_eq!(logical_nodes(&mut second, scope), expected);
        drop(reader);
        drop(second);
        tx.commit().unwrap();
        assert_eq!(
            logical_nodes(
                &mut db.tx().unwrap().cursor_dup_read::<PackedStoragesTrie>().unwrap(),
                scope
            ),
            expected
        );
    }
}

fn region_stats<K: reth_libmdbx::TransactionKind>(cursor: &Cursor<K, PackedStoragesTrie>) -> usize {
    let Cursor::Packed(logical) = cursor else { panic!("expected packed cursor") };
    logical.region_rebuilds
}

#[test]
fn incremental_upper_matches_complete_trie_after_mutations() {
    for mode in [PackingMode::Dense, PackingMode::Integer32] {
        for long_prefix in [false, true] {
            let dir = tempdir().unwrap();
            let args = DatabaseArguments::test().with_experimental_packing(Some(mode), 3);
            let db = init_db(dir.path(), args.clone()).unwrap();
            let scope = B256::repeat_byte(12);
            let mut rows: BTreeMap<_, _> = fixture(2048)
                .into_iter()
                .map(|r| {
                    let mut key = r.key;
                    if long_prefix {
                        key[..16].fill(0);
                    }
                    (key, r.value)
                })
                .collect();
            let tx = db.tx_mut().unwrap();
            for (key, value) in &rows {
                tx.put::<HashedStorages>(scope, StorageEntry::new(*key, *value)).unwrap();
            }
            tx.commit().unwrap();
            let old = db.tx().unwrap();
            let old_expected = trie_nodes(
                &rows.iter().map(|(k, v)| StorageEntry::new(*k, *v)).collect::<Vec<_>>(),
            );
            for round in 0..5 {
                let tx = db.tx_mut().unwrap();
                let keys: Vec<_> = rows.keys().step_by(31).copied().collect();
                for key in keys {
                    if round % 2 == 0 {
                        tx.delete::<HashedStorages>(
                            scope,
                            Some(StorageEntry::new(key, rows[&key])),
                        )
                        .unwrap();
                        rows.remove(&key);
                    } else {
                        rows.insert(key, U256::from(round + 900));
                        tx.put::<HashedStorages>(scope, StorageEntry::new(key, rows[&key]))
                            .unwrap();
                    }
                }
                let new_key = B256::repeat_byte(round as u8 + 100);
                rows.insert(new_key, U256::MAX);
                tx.put::<HashedStorages>(scope, StorageEntry::new(new_key, U256::MAX)).unwrap();
                let expected = trie_nodes(
                    &rows.iter().map(|(k, v)| StorageEntry::new(*k, *v)).collect::<Vec<_>>(),
                );
                let mut cursor = tx.cursor_dup_read::<PackedStoragesTrie>().unwrap();
                assert_eq!(logical_nodes(&mut cursor, scope), expected);
                drop(cursor);
                tx.commit().unwrap();
                assert_eq!(
                    logical_nodes(
                        &mut db.tx().unwrap().cursor_dup_read::<PackedStoragesTrie>().unwrap(),
                        scope
                    ),
                    expected
                );
            }
            assert_eq!(
                logical_nodes(&mut old.cursor_dup_read::<PackedStoragesTrie>().unwrap(), scope),
                old_expected
            );
            old.abort();
            drop(db);
            let db = init_db(dir.path(), args).unwrap();
            let expected = trie_nodes(
                &rows.iter().map(|(k, v)| StorageEntry::new(*k, *v)).collect::<Vec<_>>(),
            );
            assert_eq!(
                logical_nodes(
                    &mut db.tx().unwrap().cursor_dup_read::<PackedStoragesTrie>().unwrap(),
                    scope
                ),
                expected
            );
        }
    }
}

fn logical_nodes<K: reth_libmdbx::TransactionKind>(
    cursor: &mut Cursor<K, PackedStoragesTrie>,
    scope: B256,
) -> BTreeMap<Nibbles, alloy_trie::BranchNodeCompact> {
    cursor
        .walk_dup(Some(scope), None)
        .unwrap()
        .map(|r| r.map(|(_, e)| (e.nibbles.0, e.node)))
        .collect::<Result<_, _>>()
        .unwrap()
}

#[test]
fn deletion_merges_blobs_atomically_and_preserves_snapshots() {
    for mode in [PackingMode::Dense, PackingMode::Integer32] {
        let dir = tempdir().unwrap();
        let args = DatabaseArguments::test().with_experimental_packing(Some(mode), 3);
        let db = init_db(dir.path(), args.clone()).unwrap();
        let scope = B256::repeat_byte(13);
        let other = B256::repeat_byte(14);
        let rows = fixture(1024);
        let tx = db.tx_mut().unwrap();
        for row in &rows {
            tx.put::<HashedStorages>(scope, *row).unwrap();
        }
        tx.put::<HashedStorages>(other, rows[0]).unwrap();
        tx.commit().unwrap();
        let old = db.tx().unwrap();
        assert_eq!(blob_count(&old), 3);
        let retained: Vec<_> =
            rows.chunks(512).flat_map(|chunk| chunk.iter().skip(384)).copied().collect();
        for abort in [true, false] {
            let tx = db.tx_mut().unwrap();
            for chunk in rows.chunks(512) {
                for row in chunk.iter().take(384) {
                    tx.delete::<HashedStorages>(scope, Some(*row)).unwrap();
                }
            }
            if abort {
                tx.abort();
            } else {
                tx.commit().unwrap();
            }
            assert_eq!(blob_count(&old), 3);
            assert_eq!(old.entries::<HashedStorages>().unwrap(), 1025);
            let latest = db.tx().unwrap();
            assert_eq!(blob_count(&latest), if abort { 3 } else { 2 });
            let actual = latest
                .cursor_dup_read::<HashedStorages>()
                .unwrap()
                .walk_dup(Some(scope), None)
                .unwrap()
                .map(|r| r.unwrap().1)
                .collect::<Vec<_>>();
            assert_eq!(actual, if abort { rows.clone() } else { retained.clone() });
        }
        old.abort();
        // Inserting into the merged range stays one blob, rather than immediately splitting.
        let tx = db.tx_mut().unwrap();
        tx.put::<HashedStorages>(scope, rows[0]).unwrap();
        tx.commit().unwrap();
        assert_eq!(blob_count(&db.tx().unwrap()), 2);
        drop(db);
        let db = init_db(dir.path(), args).unwrap();
        assert_eq!(blob_count(&db.tx().unwrap()), 2);
        assert_eq!(db.tx().unwrap().entries::<HashedStorages>().unwrap(), 258);
    }
}

fn blob_count<K: reth_libmdbx::TransactionKind>(tx: &crate::mdbx::tx::Tx<K>) -> usize {
    let dbi = tx.inner().open_db(Some(TABLE)).unwrap().dbi();
    tx.inner().db_stat_with_dbi(dbi).unwrap().entries()
}

#[test]
fn small_blob_merges_successor_when_predecessor_is_full() {
    for mode in [PackingMode::Dense, PackingMode::Integer32] {
        let dir = tempdir().unwrap();
        let db =
            init_db(dir.path(), DatabaseArguments::test().with_experimental_packing(Some(mode), 3))
                .unwrap();
        let scope = B256::repeat_byte(16);
        let rows = fixture(1536);
        let tx = db.tx_mut().unwrap();
        for row in &rows {
            tx.put::<HashedStorages>(scope, *row).unwrap();
        }
        tx.commit().unwrap();
        for (round, start) in [1024, 512].into_iter().enumerate() {
            let tx = db.tx_mut().unwrap();
            for row in &rows[start..start + 384] {
                tx.delete::<HashedStorages>(scope, Some(*row)).unwrap();
            }
            tx.commit().unwrap();
            let tx = db.tx().unwrap();
            assert_eq!(blob_count(&tx), if round == 0 { 3 } else { 2 });
            assert_eq!(tx.entries::<HashedStorages>().unwrap(), 1536 - 384 * (round + 1));
        }
    }
}

/// DB-backed tuning experiment. Test-only targets never become runtime flags or format settings.
#[test]
#[ignore = "explicit blob-size/performance experiment"]
fn benchmark_blob_targets() {
    let scope = B256::repeat_byte(15);
    let rows = fixture(65536);
    println!("mode,target,round,live_bytes,random_read_ns,update_commit_ms,delete_commit_ms,post_delete_blobs");
    for mode in [PackingMode::Dense, PackingMode::Integer32] {
        for target in [128, 256, 512] {
            for round in 0..3 {
                let dir = tempdir().unwrap();
                let db = init_db(
                    dir.path(),
                    DatabaseArguments::test().with_experimental_packing(Some(mode), 3),
                )
                .unwrap();
                let tx = db.tx_mut().unwrap();
                tx.packing
                    .as_ref()
                    .unwrap()
                    .row_target
                    .store(target, std::sync::atomic::Ordering::Relaxed);
                for row in &rows {
                    tx.put::<HashedStorages>(scope, *row).unwrap();
                }
                tx.commit().unwrap();
                let tx = db.tx().unwrap();
                let mut live = 0;
                for name in [TABLE, PackedStoragesTrie::NAME] {
                    let dbi = tx.inner().open_db(Some(name)).unwrap().dbi();
                    let stat = tx.inner().db_stat_with_dbi(dbi).unwrap();
                    live += (stat.branch_pages() + stat.leaf_pages() + stat.overflow_pages()) *
                        stat.page_size() as usize;
                }
                let mut cursor = tx.cursor_dup_read::<HashedStorages>().unwrap();
                let start = Instant::now();
                for i in 0..4096 {
                    let at = (i * 7919) % rows.len();
                    assert_eq!(
                        cursor.seek_by_key_subkey(scope, rows[at].key).unwrap(),
                        Some(rows[at])
                    );
                }
                let random = start.elapsed().as_nanos() as f64 / 4096.;
                drop(cursor);
                tx.abort();
                let start = Instant::now();
                let tx = db.tx_mut().unwrap();
                tx.packing
                    .as_ref()
                    .unwrap()
                    .row_target
                    .store(target, std::sync::atomic::Ordering::Relaxed);
                for i in (0..rows.len()).step_by(1024) {
                    tx.put::<HashedStorages>(
                        scope,
                        StorageEntry::new(rows[i].key, U256::from(777)),
                    )
                    .unwrap();
                }
                tx.commit().unwrap();
                let update = start.elapsed().as_secs_f64() * 1000.;
                let start = Instant::now();
                let tx = db.tx_mut().unwrap();
                tx.packing
                    .as_ref()
                    .unwrap()
                    .row_target
                    .store(target, std::sync::atomic::Ordering::Relaxed);
                let mut cursor = tx.cursor_dup_write::<HashedStorages>().unwrap();
                for chunk in rows.chunks(target) {
                    for row in chunk.iter().take(target * 7 / 8) {
                        // Updated values may differ, so position/delete by key.
                        cursor.seek_by_key_subkey(scope, row.key).unwrap();
                        cursor.delete_current().unwrap();
                    }
                }
                drop(cursor);
                tx.commit().unwrap();
                let deletion = start.elapsed().as_secs_f64() * 1000.;
                let tx = db.tx().unwrap();
                assert_eq!(tx.entries::<HashedStorages>().unwrap(), 8192);
                println!(
                    "{mode:?},{target},{round},{live},{random:.1},{update:.3},{deletion:.3},{}",
                    blob_count(&tx)
                );
            }
        }
    }
}
