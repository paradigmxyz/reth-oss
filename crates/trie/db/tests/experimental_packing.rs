//! Packed database interoperability through the production trie root and proof factories.

use alloy_primitives::{keccak256, B256, U256};
use reth_db::{
    cursor::DbDupCursorRO,
    init_db,
    mdbx::{packing::PackingMode, DatabaseArguments},
    tables::{HashedStorages, PackedStoragesTrie},
    Database,
};
use reth_db_api::{
    table::Table,
    transaction::{DbTx, DbTxMut},
};
use reth_primitives_traits::StorageEntry;
use reth_trie::{
    hashed_cursor::HashedPostStateCursorFactory, prefix_set::PrefixSetMut, proof::StorageProof,
    trie_cursor::InMemoryTrieCursorFactory, updates::TrieUpdates, HashBuilder, HashedPostState,
    HashedStorage, Nibbles, StorageRoot,
};
use reth_trie_db::{
    DatabaseHashedCursorFactory, DatabaseStorageRoot, DatabaseTrieCursorFactory, PackedKeyAdapter,
};
use std::{hint::black_box, time::Instant};
use tempfile::tempdir;

type TrieEntry = <PackedStoragesTrie as Table>::Value;
const TABLE: &str = "ExperimentalPackedStoragesV1";

fn fixture(n: usize) -> Vec<StorageEntry> {
    let mut rows: Vec<_> = (0..n)
        .map(|i| {
            StorageEntry::new(
                keccak256(i.to_be_bytes()),
                match i % 5 {
                    0 => U256::MAX,
                    1 => U256::ONE,
                    _ => U256::from(i + 1),
                },
            )
        })
        .collect();
    rows.sort_by_key(|r| r.key);
    rows
}

#[test]
fn roots_and_proofs_match_native_before_after_update_and_reopen() {
    let native_dir = tempdir().unwrap();
    let native = init_db(native_dir.path(), DatabaseArguments::test()).unwrap();
    let scope = B256::repeat_byte(7);
    for count in [1, 4, 4096] {
        for mode in [PackingMode::Dense, PackingMode::Integer32] {
            let mut rows = fixture(count);
            let dir = tempdir().unwrap();
            let args = DatabaseArguments::test().with_experimental_packing(Some(mode), 3);
            let packed = init_db(dir.path(), args.clone()).unwrap();
            for round in 0..3 {
                if round == 1 {
                    let at = rows.len() / 2;
                    rows[at].value = U256::from(98765);
                    if rows.len() > 1 {
                        rows.remove((rows.len() / 3).min(111));
                    }
                }
                for (db, write_trie) in [(&native, true), (&packed, round == 0)] {
                    let tx = db.tx_mut().unwrap();
                    tx.delete::<HashedStorages>(scope, None).unwrap();
                    for row in &rows {
                        tx.put::<HashedStorages>(scope, *row).unwrap();
                    }
                    if write_trie {
                        tx.delete::<PackedStoragesTrie>(scope, None).unwrap();
                        let mut hb = HashBuilder::default().with_updates(true);
                        for row in &rows {
                            hb.add_leaf(
                                Nibbles::unpack(row.key),
                                alloy_rlp::encode_fixed_size(&row.value).as_ref(),
                            );
                        }
                        hb.root();
                        for (path, node) in hb.split().1 {
                            tx.put::<PackedStoragesTrie>(
                                scope,
                                TrieEntry { nibbles: path.into(), node },
                            )
                            .unwrap();
                        }
                    }
                    tx.commit().unwrap();
                }
                let nt = native.tx().unwrap();
                let pt = packed.tx().unwrap();
                let nr = storage_root(&nt, scope);
                let pr = storage_root(&pt, scope);
                assert_eq!(pr, nr);
                let targets = rows
                    .iter()
                    .step_by((rows.len() / 32).max(1))
                    .map(|r| r.key)
                    .chain([B256::ZERO, B256::repeat_byte(255)])
                    .collect::<alloy_primitives::map::B256Set>();
                let np = StorageProof::new_hashed(
                    DatabaseTrieCursorFactory::<_, PackedKeyAdapter>::new(&nt),
                    DatabaseHashedCursorFactory::new(&nt),
                    scope,
                )
                .storage_multiproof(targets.clone())
                .unwrap();
                let pp = StorageProof::new_hashed(
                    DatabaseTrieCursorFactory::<_, PackedKeyAdapter>::new(&pt),
                    DatabaseHashedCursorFactory::new(&pt),
                    scope,
                )
                .storage_multiproof(targets)
                .unwrap();
                assert_eq!(pp, np);
            }
            drop(packed);
            let reopened = init_db(dir.path(), args).unwrap();
            assert_eq!(reopened.tx().unwrap().entries::<HashedStorages>().unwrap(), rows.len());
        }
    }
}

/// Reproducible DB-backed smoke benchmark. Use --release for throughput comparisons.
#[test]
#[ignore = "explicit storage/performance experiment"]
fn benchmark_packed_storage() {
    let scope = B256::repeat_byte(9);
    let rows = fixture(65536);
    let mut hb = HashBuilder::default().with_updates(true);
    for row in &rows {
        hb.add_leaf(Nibbles::unpack(row.key), alloy_rlp::encode_fixed_size(&row.value).as_ref());
    }
    let root = hb.root();
    let nodes = hb.split().1;
    println!("mode,live_table_bytes,initial_commit_ms,random_read_ns,sequential_row_ns,multiproof_ms,update_commit_ms");
    for mode in [None, Some(PackingMode::Dense), Some(PackingMode::Integer32)] {
        let dir = tempdir().unwrap();
        let db = init_db(dir.path(), DatabaseArguments::test().with_experimental_packing(mode, 3))
            .unwrap();
        let start = Instant::now();
        let tx = db.tx_mut().unwrap();
        for row in &rows {
            tx.put::<HashedStorages>(scope, *row).unwrap();
        }
        for (path, node) in &nodes {
            tx.put::<PackedStoragesTrie>(
                scope,
                TrieEntry { nibbles: path.clone().into(), node: node.clone() },
            )
            .unwrap();
        }
        tx.commit().unwrap();
        let initial = start.elapsed().as_secs_f64() * 1000.;
        let tx = db.tx().unwrap();
        let mut live = 0;
        for name in [HashedStorages::NAME, PackedStoragesTrie::NAME, TABLE] {
            if let Ok(table) = tx.inner().open_db(Some(name)) {
                let stat = tx.inner().db_stat_with_dbi(table.dbi()).unwrap();
                live += (stat.branch_pages() + stat.leaf_pages() + stat.overflow_pages()) *
                    stat.page_size() as usize;
            }
        }
        let mut c = tx.cursor_dup_read::<HashedStorages>().unwrap();
        let start = Instant::now();
        for i in 0..4096 {
            let at = (i * 7919) % rows.len();
            assert_eq!(
                black_box(c.seek_by_key_subkey(scope, rows[at].key).unwrap()),
                Some(rows[at])
            );
        }
        let random = start.elapsed().as_nanos() as f64 / 4096.;
        let start = Instant::now();
        let mut count = 0;
        for row in c.walk_dup(Some(scope), None).unwrap() {
            black_box(row.unwrap());
            count += 1;
        }
        assert_eq!(count, rows.len());
        let sequential = start.elapsed().as_nanos() as f64 / count as f64;
        let targets =
            rows.iter().step_by(8191).map(|r| r.key).collect::<alloy_primitives::map::B256Set>();
        let start = Instant::now();
        let proof = StorageProof::new_hashed(
            DatabaseTrieCursorFactory::<_, PackedKeyAdapter>::new(&tx),
            DatabaseHashedCursorFactory::new(&tx),
            scope,
        )
        .storage_multiproof(targets)
        .unwrap();
        assert_eq!(proof.root, root);
        black_box(proof);
        let proof_ms = start.elapsed().as_secs_f64() * 1000.;
        drop(c);
        tx.abort();
        let start = Instant::now();
        let tx = db.tx_mut().unwrap();
        // State-only update exercises the commit-time consistency repair path.
        for i in (0..rows.len()).step_by(1024) {
            tx.delete::<HashedStorages>(scope, Some(rows[i])).unwrap();
            tx.put::<HashedStorages>(scope, StorageEntry::new(rows[i].key, U256::from(777)))
                .unwrap();
        }
        if mode.is_none() {
            tx.delete::<PackedStoragesTrie>(scope, None).unwrap();
            let mut hb = HashBuilder::default().with_updates(true);
            for (i, row) in rows.iter().enumerate() {
                let value = if i % 1024 == 0 { U256::from(777) } else { row.value };
                hb.add_leaf(
                    Nibbles::unpack(row.key),
                    alloy_rlp::encode_fixed_size(&value).as_ref(),
                );
            }
            hb.root();
            for (path, node) in hb.split().1 {
                tx.put::<PackedStoragesTrie>(scope, TrieEntry { nibbles: path.into(), node })
                    .unwrap();
            }
        }
        tx.commit().unwrap();
        let update = start.elapsed().as_secs_f64() * 1000.;
        println!(
            "{mode:?},{live},{initial:.3},{random:.1},{sequential:.1},{proof_ms:.3},{update:.3}"
        );
    }
}

fn storage_root<TX: DbTx>(tx: &TX, scope: B256) -> B256 {
    StorageRoot::<DatabaseTrieCursorFactory<&TX,PackedKeyAdapter>,DatabaseHashedCursorFactory<&TX>>::from_tx_hashed(tx,scope).root().unwrap()
}

#[test]
fn sparse_incremental_updates_preserve_roots_and_multiproofs() {
    for mode in [PackingMode::Dense, PackingMode::Integer32] {
        let native_dir = tempdir().unwrap();
        let packed_dir = tempdir().unwrap();
        let native = init_db(native_dir.path(), DatabaseArguments::test()).unwrap();
        let packed = init_db(
            packed_dir.path(),
            DatabaseArguments::test().with_experimental_packing(Some(mode), 3),
        )
        .unwrap();
        let scope = B256::repeat_byte(19);
        let mut rows = fixture(8192);
        let tx = packed.tx_mut().unwrap();
        for row in &rows {
            tx.put::<HashedStorages>(scope, *row).unwrap();
        }
        tx.commit().unwrap();
        for round in 0..3 {
            let tx = packed.tx_mut().unwrap();
            let removed = rows.remove(round * 13);
            tx.delete::<HashedStorages>(scope, Some(removed)).unwrap();
            rows[100].value = U256::from(70000 + round);
            tx.put::<HashedStorages>(scope, rows[100]).unwrap();
            let inserted = StorageEntry::new(keccak256([round as u8, 252]), U256::MAX);
            tx.put::<HashedStorages>(scope, inserted).unwrap();
            rows.push(inserted);
            rows.sort_by_key(|r| r.key);
            tx.commit().unwrap();
            let tx = native.tx_mut().unwrap();
            tx.delete::<HashedStorages>(scope, None).unwrap();
            tx.delete::<PackedStoragesTrie>(scope, None).unwrap();
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
                tx.put::<PackedStoragesTrie>(scope, TrieEntry { nibbles: path.into(), node })
                    .unwrap();
            }
            tx.commit().unwrap();
            let nt = native.tx().unwrap();
            let pt = packed.tx().unwrap();
            assert_eq!(storage_root(&pt, scope), storage_root(&nt, scope));
            let targets = rows
                .iter()
                .step_by(511)
                .map(|r| r.key)
                .chain([removed.key, inserted.key, B256::ZERO])
                .collect::<alloy_primitives::map::B256Set>();
            let np = StorageProof::new_hashed(
                DatabaseTrieCursorFactory::<_, PackedKeyAdapter>::new(&nt),
                DatabaseHashedCursorFactory::new(&nt),
                scope,
            )
            .storage_multiproof(targets.clone())
            .unwrap();
            let pp = StorageProof::new_hashed(
                DatabaseTrieCursorFactory::<_, PackedKeyAdapter>::new(&pt),
                DatabaseHashedCursorFactory::new(&pt),
                scope,
            )
            .storage_multiproof(targets)
            .unwrap();
            assert_eq!(pp, np);
        }
    }
}

#[test]
fn system_contract_overlays_match_full_storage_roots() {
    for mode in [None, Some(PackingMode::Dense), Some(PackingMode::Integer32)] {
        for initial_leaf in [false, true] {
            let dir = tempdir().unwrap();
            let db =
                init_db(dir.path(), DatabaseArguments::test().with_experimental_packing(mode, 3))
                    .unwrap();
            let scope = B256::repeat_byte(31);
            let mut storage = std::collections::BTreeMap::new();
            if initial_leaf {
                let key = keccak256(U256::ZERO.to_be_bytes::<32>());
                storage.insert(key, U256::MAX);
                let tx = db.tx_mut().unwrap();
                tx.put::<HashedStorages>(scope, StorageEntry::new(key, U256::MAX)).unwrap();
                tx.commit().unwrap();
            }
            let mut nodes = TrieUpdates::default();
            for block in 1..=128u64 {
                let timestamp = 1_791_629_607 + block * 6;
                let slots = [timestamp % 8191, timestamp % 8191 + 8191];
                let mut prefix_set = PrefixSetMut::default();
                for slot in slots {
                    let key = keccak256(U256::from(slot).to_be_bytes::<32>());
                    let value = U256::from(block * 8191 + slot);
                    storage.insert(key, value);
                    prefix_set.insert(Nibbles::unpack(key));
                }
                let state = HashedPostState::from_hashed_storage(
                    scope,
                    HashedStorage::from_iter(storage.iter().map(|(key, value)| (*key, *value))),
                )
                .into_sorted();
                let sorted_nodes = nodes.clone().into_sorted();
                let tx = db.tx().unwrap();
                let (root, _, updates) = StorageRoot::new_hashed(
                    InMemoryTrieCursorFactory::new(
                        DatabaseTrieCursorFactory::<_, PackedKeyAdapter>::new(&tx),
                        &sorted_nodes,
                    ),
                    HashedPostStateCursorFactory::new(
                        DatabaseHashedCursorFactory::new(&tx),
                        &state,
                    ),
                    scope,
                    prefix_set.freeze(),
                )
                .root_with_updates()
                .unwrap();
                let mut builder = HashBuilder::default();
                for (key, value) in &storage {
                    builder.add_leaf(
                        Nibbles::unpack(key),
                        alloy_rlp::encode_fixed_size(value).as_ref(),
                    );
                }
                assert_eq!(root, builder.root(), "{mode:?}, initial={initial_leaf}, block={block}");
                nodes.storage_tries.entry(scope).or_default().extend(updates);
            }
        }
    }
}

#[test]
fn persisted_system_contract_incremental_roots_match_full_storage() {
    for (mode, full) in [
        (None, false),
        (Some(PackingMode::Dense), false),
        (Some(PackingMode::Integer32), false),
        (Some(PackingMode::Dense), true),
        (Some(PackingMode::Integer32), true),
    ] {
        let dir = tempdir().unwrap();
        let db = init_db(
            dir.path(),
            DatabaseArguments::test()
                .with_experimental_packing(mode, 3)
                .with_experimental_full_trie(full),
        )
        .unwrap();
        let scope = B256::repeat_byte(32);
        let mut storage = std::collections::BTreeMap::new();
        for block in 1..=128u64 {
            let timestamp = 1_791_629_607 + block * 6;
            let mut changed = Vec::new();
            let mut prefixes = PrefixSetMut::default();
            for slot in [timestamp % 8191, timestamp % 8191 + 8191] {
                let key = keccak256(U256::from(slot).to_be_bytes::<32>());
                let value = U256::from(block * 8191 + slot);
                storage.insert(key, value);
                changed.push((key, value));
                prefixes.insert(Nibbles::unpack(key));
            }
            let tx = db.tx().unwrap();
            let state = HashedPostState::from_hashed_storage(
                scope,
                HashedStorage::from_iter(changed.clone()),
            )
            .into_sorted();
            let (root, _, _) = StorageRoot::new_hashed(
                DatabaseTrieCursorFactory::<_, PackedKeyAdapter>::new(&tx),
                HashedPostStateCursorFactory::new(DatabaseHashedCursorFactory::new(&tx), &state),
                scope,
                prefixes.freeze(),
            )
            .root_with_updates()
            .unwrap();
            let mut builder = HashBuilder::default().with_updates(true);
            for (key, value) in &storage {
                builder
                    .add_leaf(Nibbles::unpack(key), alloy_rlp::encode_fixed_size(value).as_ref());
            }
            assert_eq!(root, builder.root(), "{mode:?}, persisted block={block}");
            let nodes = builder.split().1;
            drop(tx);
            let tx = db.tx_mut().unwrap();
            for (key, value) in changed {
                tx.put::<HashedStorages>(scope, StorageEntry::new(key, value)).unwrap();
            }
            tx.delete::<PackedStoragesTrie>(scope, None).unwrap();
            for (path, node) in nodes {
                if !path.is_empty() {
                    tx.put::<PackedStoragesTrie>(scope, TrieEntry { nibbles: path.into(), node })
                        .unwrap();
                }
            }
            tx.finish_storage_trie_updates(scope).unwrap();
            tx.commit().unwrap();
        }
    }
}

#[test]
fn pending_storage_trie_overlays_match_native_roots() {
    for mode in [None, Some(PackingMode::Dense), Some(PackingMode::Integer32)] {
        let dir = tempdir().unwrap();
        let db = init_db(dir.path(), DatabaseArguments::test().with_experimental_packing(mode, 3))
            .unwrap();
        let scope = B256::repeat_byte(33);
        let mut rows = fixture(31);
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
            if !path.is_empty() {
                tx.put::<PackedStoragesTrie>(scope, TrieEntry { nibbles: path.into(), node })
                    .unwrap();
            }
        }
        tx.commit().unwrap();

        rows[0].value = U256::from(98765);
        let state = HashedPostState::from_hashed_storage(
            scope,
            HashedStorage::from_iter(rows.iter().map(|row| (row.key, row.value))),
        )
        .into_sorted();
        let mut prefixes = PrefixSetMut::default();
        prefixes.insert(Nibbles::unpack(rows[0].key));
        let tx = db.tx().unwrap();
        let (expected, _, updates) = StorageRoot::new_hashed(
            DatabaseTrieCursorFactory::<_, PackedKeyAdapter>::new(&tx),
            HashedPostStateCursorFactory::new(DatabaseHashedCursorFactory::new(&tx), &state),
            scope,
            prefixes.freeze(),
        )
        .root_with_updates()
        .unwrap();
        let mut nodes = TrieUpdates::default();
        nodes.storage_tries.insert(scope, updates);
        let nodes = nodes.into_sorted();
        let mut builder = HashBuilder::default();
        for row in &rows {
            builder.add_leaf(
                Nibbles::unpack(row.key),
                alloy_rlp::encode_fixed_size(&row.value).as_ref(),
            );
        }
        assert_eq!(expected, builder.root());
        // A subsequent block can leave this storage unchanged, or change another
        // subtree. Its prefix set does not include the preceding block's mutation.
        for change_another_subtree in [false, true] {
            let mut prefixes = PrefixSetMut::default();
            if change_another_subtree {
                let last = rows.last_mut().unwrap();
                last.value = U256::from(54321);
                prefixes.insert(Nibbles::unpack(last.key));
            }
            let state = HashedPostState::from_hashed_storage(
                scope,
                HashedStorage::from_iter(rows.iter().map(|row| (row.key, row.value))),
            )
            .into_sorted();
            let actual = StorageRoot::new_hashed(
                InMemoryTrieCursorFactory::new(
                    DatabaseTrieCursorFactory::<_, PackedKeyAdapter>::new(&tx),
                    &nodes,
                ),
                HashedPostStateCursorFactory::new(DatabaseHashedCursorFactory::new(&tx), &state),
                scope,
                prefixes.freeze(),
            )
            .root()
            .unwrap();
            let mut builder = HashBuilder::default();
            for row in &rows {
                builder.add_leaf(
                    Nibbles::unpack(row.key),
                    alloy_rlp::encode_fixed_size(&row.value).as_ref(),
                );
            }
            assert_eq!(
                actual,
                builder.root(),
                "{mode:?}: pending overlay, later mutation={change_another_subtree}"
            );
        }
    }
}
