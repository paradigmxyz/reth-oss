//! Durable PBT checkpoint and branch recovery tests.

#![cfg(feature = "persistence")]

use alloy_primitives::{Address, B256, U256};
use reth_trie_pbt::{
    migration::DecodeLimits, persistence::SnapshotStore, AccountData, Blake3Hasher, StateTree,
};

#[test]
fn checkpoints_survive_reopening_and_keep_competing_branches() {
    let directory = tempfile::tempdir().unwrap();
    let store =
        SnapshotStore::<Blake3Hasher>::open(directory.path(), DecodeLimits::default()).unwrap();
    let a = B256::repeat_byte(1);
    let b = B256::repeat_byte(2);
    let mut state = StateTree::<Blake3Hasher>::new();
    state
        .set_account(
            Address::ZERO,
            AccountData { nonce: 1, balance: U256::from(2), code: Default::default() },
        )
        .unwrap();
    let a_root = store.save_block(a, &state).unwrap();
    state.set_storage(Address::ZERO, U256::from(64), B256::repeat_byte(3)).unwrap();
    let b_root = store.save_block(b, &state).unwrap();
    drop(store);
    let reopened =
        SnapshotStore::<Blake3Hasher>::open(directory.path(), DecodeLimits::default()).unwrap();
    assert_eq!(reopened.load_block(a).unwrap().root(), a_root);
    assert_eq!(reopened.load_block(b).unwrap().root(), b_root);
    assert!(reopened.load(B256::repeat_byte(9)).is_err());
    reth_fs_util::write(directory.path().join("states").join(b_root.to_string()), [0; 7]).unwrap();
    drop(reopened);
    let reopened =
        SnapshotStore::<Blake3Hasher>::open(directory.path(), DecodeLimits::default()).unwrap();
    assert!(reopened.load_block(b).is_err());
    assert_eq!(reopened.load_block(a).unwrap().root(), a_root);
}

#[test]
fn store_rejects_state_it_could_not_restore_under_its_limits() {
    let directory = tempfile::tempdir().unwrap();
    let store = SnapshotStore::<Blake3Hasher>::open(
        directory.path(),
        DecodeLimits { max_leaves: 1, ..Default::default() },
    )
    .unwrap();
    let mut state = StateTree::<Blake3Hasher>::new();
    state.set_account(Address::ZERO, AccountData { nonce: 1, ..Default::default() }).unwrap();
    assert!(store.save_block(B256::ZERO, &state).is_err());
    assert!(store.load_block(B256::ZERO).is_err());
}
