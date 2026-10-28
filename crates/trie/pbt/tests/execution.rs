//! Translation of authoritative Reth execution state into PBT snapshots.

#![cfg(feature = "execution")]

use alloy_primitives::{Address, B256, U256};
use reth_trie_common::bundle_state::{AccountStatus, BundleAccount, BundleState, StorageSlot};
use reth_trie_pbt::{execution::apply_bundle, AccountData, Blake3Hasher, StateTree};

#[test]
fn transfer_and_storage_output_matches_direct_encoding() {
    let a = Address::repeat_byte(1);
    let mut bundle = BundleState::default();
    let mut account = BundleAccount::new(
        None,
        Some(Default::default()),
        Default::default(),
        AccountStatus::InMemoryChange,
    );
    account.info.as_mut().unwrap().balance = U256::from(100);
    account.info.as_mut().unwrap().nonce = 1;
    account.storage.insert(U256::from(64), StorageSlot::new_changed(U256::ZERO, U256::from(7)));
    bundle.state.insert(a, account);
    let parent = StateTree::<Blake3Hasher>::new();
    let state = apply_bundle(&parent, &bundle).unwrap();
    let mut direct = parent.clone();
    direct
        .set_account(
            a,
            AccountData { nonce: 1, balance: U256::from(100), code: Default::default() },
        )
        .unwrap();
    direct.set_storage(a, U256::from(64), B256::from(U256::from(7))).unwrap();
    assert_eq!(state.root(), direct.root());
    assert_eq!(parent.root(), B256::ZERO);
}

#[test]
fn destruction_and_recreation_wipe_parent_storage() {
    let a = Address::repeat_byte(1);
    let mut parent = StateTree::<Blake3Hasher>::new();
    parent
        .set_account(a, AccountData { nonce: 1, balance: U256::from(5), code: Default::default() })
        .unwrap();
    parent.set_storage(a, U256::from(63), B256::repeat_byte(1)).unwrap();
    parent.set_storage(a, U256::from(256), B256::repeat_byte(1)).unwrap();
    let mut bundle = BundleState::default();
    let mut account = BundleAccount::new(
        None,
        Some(Default::default()),
        Default::default(),
        AccountStatus::DestroyedChanged,
    );
    account.info.as_mut().unwrap().balance = U256::from(6);
    bundle.state.insert(a, account);
    let state = apply_bundle(&parent, &bundle).unwrap();
    assert_eq!(state.storage(a, U256::from(63)), B256::ZERO);
    assert_eq!(state.storage(a, U256::from(256)), B256::ZERO);
    bundle.state.get_mut(&a).unwrap().info = None;
    assert_eq!(apply_bundle(&state, &bundle).unwrap().root(), B256::ZERO);
}

#[test]
fn missing_new_bytecode_fails_without_modifying_parent() {
    let a = Address::repeat_byte(1);
    let mut bundle = BundleState::default();
    let mut account = BundleAccount::new(
        None,
        Some(Default::default()),
        Default::default(),
        AccountStatus::InMemoryChange,
    );
    account.info.as_mut().unwrap().code = None;
    account.info.as_mut().unwrap().code_hash = B256::repeat_byte(1);
    bundle.state.insert(a, account);
    let parent = StateTree::<Blake3Hasher>::new();
    assert!(apply_bundle(&parent, &bundle).is_err());
    assert_eq!(parent.root(), B256::ZERO);
}
