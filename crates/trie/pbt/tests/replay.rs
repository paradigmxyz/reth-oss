//! Authenticated BAL replay, branch isolation, and finality.

#![cfg(feature = "bal")]

use alloy_eip7928::{
    compute_block_access_list_hash, AccountChanges, BalanceChange, BlockAccessIndex, CodeChange,
    SlotChanges, StorageChange,
};
use alloy_primitives::{Address, B256, U256};
use reth_trie_pbt::{
    replay::{replay, ReplayChain},
    AccountData, Blake3Hasher, StateTree,
};

fn balance(address: Address, value: u64) -> AccountChanges {
    let mut account = AccountChanges::new(address);
    account.balance_changes.push(BalanceChange::new(BlockAccessIndex::new(1), U256::from(value)));
    account
}

#[test]
fn fresh_transfer_writes_both_header_leaves() {
    let address = Address::repeat_byte(1);
    let bal = vec![balance(address, 10)];
    let parent = StateTree::<Blake3Hasher>::new();
    let state = replay(&parent, &bal, compute_block_access_list_hash(&bal)).unwrap();
    let mut expected = parent.clone();
    expected
        .set_account(address, AccountData { balance: U256::from(10), ..Default::default() })
        .unwrap();
    assert_eq!(state.root(), expected.root());
    assert_eq!(state.tree().len(), 2);
    assert_eq!(parent.root(), B256::ZERO);
}

#[test]
fn storage_zero_and_delegation_clear_match_direct_state() {
    let address = Address::repeat_byte(1);
    let mut parent = StateTree::<Blake3Hasher>::new();
    let mut code = vec![0xef, 1, 0];
    code.extend_from_slice(address.as_slice());
    parent
        .set_account(address, AccountData { nonce: 1, balance: U256::from(10), code: code.into() })
        .unwrap();
    parent.set_storage(address, U256::from(63), B256::repeat_byte(1)).unwrap();
    let mut change = AccountChanges::new(address);
    change.code_changes.push(CodeChange::new(BlockAccessIndex::new(1), Default::default()));
    change.storage_changes.push(SlotChanges::new(
        U256::from(63),
        vec![StorageChange::new(BlockAccessIndex::new(1), U256::ZERO)],
    ));
    let bal = vec![change];
    let state = replay(&parent, &bal, compute_block_access_list_hash(&bal)).unwrap();
    let mut expected = StateTree::<Blake3Hasher>::new();
    expected
        .set_account(
            address,
            AccountData { nonce: 1, balance: U256::from(10), code: Default::default() },
        )
        .unwrap();
    assert_eq!(state.root(), expected.root());
}

#[test]
fn fork_layers_are_keyed_by_hash_and_finality_discards_other_branch() {
    let address = Address::repeat_byte(1);
    let anchor = B256::repeat_byte(1);
    let a = B256::repeat_byte(2);
    let b = B256::repeat_byte(3);
    let mut chain = ReplayChain::new(anchor, StateTree::<Blake3Hasher>::new());
    let a_bal = vec![balance(address, 10)];
    let b_bal = vec![balance(address, 20)];
    let a_root = chain.insert(a, anchor, &a_bal, compute_block_access_list_hash(&a_bal)).unwrap();
    let b_root = chain.insert(b, anchor, &b_bal, compute_block_access_list_hash(&b_bal)).unwrap();
    assert_ne!(a_root, b_root);
    assert_eq!(chain.state(a).unwrap().root(), a_root);
    assert_eq!(chain.finalize(b).unwrap().root(), b_root);
    assert_eq!(chain.finalized(), b);
    assert!(chain.state(a).is_none());
    assert!(chain.state(anchor).is_none());
    assert!(chain.insert(a, anchor, &a_bal, compute_block_access_list_hash(&a_bal)).is_err());
}

#[test]
fn read_only_bal_does_not_create_accounts_and_bad_hash_is_rejected() {
    let bal = vec![AccountChanges::new(Address::ZERO)];
    let parent = StateTree::<Blake3Hasher>::new();
    assert_eq!(
        replay(&parent, &bal, compute_block_access_list_hash(&bal)).unwrap().root(),
        B256::ZERO
    );
    assert!(replay(&parent, &bal, B256::ZERO).is_err());
    let bal = vec![balance(Address::ZERO, 1), balance(Address::ZERO, 2)];
    assert!(replay(&parent, &bal, compute_block_access_list_hash(&bal)).is_err());
}
