//! Authenticated BAL replay, branch isolation, and finality.

#![cfg(feature = "bal")]

use alloy_eip7928::{
    compute_block_access_list_hash, AccountChanges, BalanceChange, BlockAccessIndex, CodeChange,
    SlotChanges, StorageChange,
};
use alloy_primitives::{Address, B256, U256};
use reth_trie_pbt::{
    replay::{replay, ReplayChain, ReplayError},
    AccountData, Blake3Hasher, StateError, StateTree,
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

#[test]
fn noncanonical_storage_reads_are_rejected_without_retaining_a_layer() {
    let address = Address::repeat_byte(1);
    let anchor = B256::repeat_byte(1);
    let child = B256::repeat_byte(2);
    let mut parent = StateTree::<Blake3Hasher>::new();
    parent.set_account(address, AccountData { nonce: 1, ..Default::default() }).unwrap();
    let mut chain = ReplayChain::new(anchor, parent.clone());
    for reads in [vec![U256::from(2), U256::from(1)], vec![U256::from(1); 2]] {
        let mut changes = balance(address, 10);
        changes.storage_reads = reads;
        let bal = vec![changes];
        assert!(matches!(
            chain.insert(child, anchor, &bal, compute_block_access_list_hash(&bal)),
            Err(ReplayError::InvalidOrder)
        ));
        assert!(chain.state(child).is_none());
        assert_eq!(chain.state(anchor).unwrap().root(), parent.root());
    }

    let mut changes = balance(address, 10);
    changes.storage_reads = vec![U256::from(1), U256::from(2)];
    let bal = vec![changes];
    chain.insert(child, anchor, &bal, compute_block_access_list_hash(&bal)).unwrap();
    assert_eq!(chain.state(child).unwrap().account(address).unwrap().balance, U256::from(10));
    assert_eq!(chain.state(anchor).unwrap().root(), parent.root());
}

#[test]
fn storage_slots_cannot_be_both_reads_and_writes() {
    let address = Address::repeat_byte(1);
    let mut parent = StateTree::<Blake3Hasher>::new();
    parent.set_account(address, AccountData { nonce: 1, ..Default::default() }).unwrap();
    let root = parent.root();
    let mut changes = AccountChanges::new(address);
    changes.storage_reads = vec![U256::from(1), U256::from(2)];
    changes.storage_changes.push(SlotChanges::new(
        U256::from(2),
        vec![StorageChange::new(BlockAccessIndex::new(1), U256::from(10))],
    ));
    let bal = vec![changes];
    assert!(matches!(
        replay(&parent, &bal, compute_block_access_list_hash(&bal)),
        Err(ReplayError::InvalidOrder)
    ));
    assert_eq!(parent.root(), root);
}

#[test]
fn storage_write_lists_cannot_be_empty() {
    let mut changes = AccountChanges::new(Address::ZERO);
    changes.storage_changes.push(SlotChanges::new(U256::from(1), Vec::new()));
    let bal = vec![changes];
    assert!(matches!(
        replay(&StateTree::<Blake3Hasher>::new(), &bal, compute_block_access_list_hash(&bal)),
        Err(ReplayError::EmptyStorageChanges)
    ));
}

#[test]
fn nonzero_storage_requires_a_surviving_account() {
    let address = Address::repeat_byte(1);
    for exists in [false, true] {
        let mut parent = StateTree::<Blake3Hasher>::new();
        if exists {
            parent
                .set_account(address, AccountData { balance: U256::from(1), ..Default::default() })
                .unwrap();
            parent.set_storage(address, U256::from(1), B256::repeat_byte(1)).unwrap();
        }
        let root = parent.root();
        let mut changes = balance(address, 0);
        changes.storage_changes.push(SlotChanges::new(
            U256::from(1),
            vec![StorageChange::new(BlockAccessIndex::new(1), U256::from(10))],
        ));
        let bal = vec![changes];
        assert!(matches!(
            replay(&parent, &bal, compute_block_access_list_hash(&bal)),
            Err(ReplayError::State(StateError::MissingAccount(missing))) if missing == address
        ));
        assert_eq!(parent.root(), root);
    }
}

#[test]
fn zero_storage_for_an_absent_account_remains_a_deletion() {
    let mut changes = AccountChanges::new(Address::ZERO);
    changes.storage_changes.push(SlotChanges::new(
        U256::from(1),
        vec![StorageChange::new(BlockAccessIndex::new(1), U256::ZERO)],
    ));
    let bal = vec![changes];
    let state =
        replay(&StateTree::<Blake3Hasher>::new(), &bal, compute_block_access_list_hash(&bal))
            .unwrap();
    assert_eq!(state.root(), B256::ZERO);
    assert!(state.account(Address::ZERO).is_none());
}
