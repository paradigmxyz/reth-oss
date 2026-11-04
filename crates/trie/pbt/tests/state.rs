//! Ethereum state lifecycle and shared-code tests.

use alloy_primitives::{keccak256, Address, B256, U256};
use reth_trie_pbt::{key, AccountData, Blake3Hasher, StateError, StateTree};

fn account(code: Vec<u8>) -> AccountData {
    AccountData { nonce: 1, balance: U256::from(5), code: code.into() }
}

#[test]
fn deleting_shared_code_preserves_other_owner() {
    let a = Address::repeat_byte(1);
    let b = Address::repeat_byte(2);
    let code = vec![1; 31 * 257];
    let mut state = StateTree::<Blake3Hasher>::new();
    state.set_account(a, account(code.clone())).unwrap();
    let a_only = state.clone();
    state.set_account(b, account(code.clone())).unwrap();
    assert_eq!(state.tree().len(), 261);
    state.remove_account(b);
    assert_eq!(state.root(), a_only.root());
    assert!(state.tree().get(&key::code_key::<Blake3Hasher>(keccak256(&code), 256)).is_some());
    state.remove_account(a);
    assert_eq!(state.root(), B256::ZERO);
}

#[test]
fn delegation_replacement_and_clearing() {
    let a = Address::repeat_byte(1);
    let mut state = StateTree::<Blake3Hasher>::new();
    state.set_account(a, account(vec![1; 100])).unwrap();
    let mut indicator = vec![0xef, 1, 0];
    indicator.extend_from_slice(Address::repeat_byte(2).as_slice());
    state.set_account(a, account(indicator)).unwrap();
    assert_eq!(state.tree().len(), 2);
    assert!(state.tree().get(&key::header_key::<Blake3Hasher>(a, 1)).is_none());
    assert!(state.tree().get(&key::header_key::<Blake3Hasher>(a, 2)).is_some());
    state.set_account(a, account(Vec::new())).unwrap();
    assert_eq!(state.tree().len(), 2);
    assert_eq!(state.tree().get(&key::header_key::<Blake3Hasher>(a, 1)), Some(keccak256([])));
    assert!(state.tree().get(&key::header_key::<Blake3Hasher>(a, 2)).is_none());
}

#[test]
fn storage_wipe_and_account_clear_cover_both_zones() {
    let a = Address::repeat_byte(1);
    let b = Address::repeat_byte(2);
    let mut state = StateTree::<Blake3Hasher>::new();
    for address in [a, b] {
        state.set_account(address, account(Vec::new())).unwrap();
        for slot in [0, 63, 64, 255, 256] {
            state.set_storage(address, U256::from(slot), B256::repeat_byte(1)).unwrap();
        }
    }
    let snapshot = state.clone();
    state.clear_storage(a);
    for slot in [0, 63, 64, 255, 256] {
        assert_eq!(state.storage(a, U256::from(slot)), B256::ZERO);
        assert_eq!(state.storage(b, U256::from(slot)), B256::repeat_byte(1));
        assert_eq!(snapshot.storage(a, U256::from(slot)), B256::repeat_byte(1));
    }
    state.apply_account(b, Some(AccountData::default())).unwrap();
    let mut expected = StateTree::<Blake3Hasher>::new();
    expected.set_account(a, account(Vec::new())).unwrap();
    assert_eq!(state.root(), expected.root());
}

#[test]
fn zero_writes_do_not_leave_leaves() {
    let a = Address::repeat_byte(1);
    let mut state = StateTree::<Blake3Hasher>::new();
    state.set_account(a, account(vec![0; 62])).unwrap();
    assert_eq!(state.tree().len(), 2);
    let before = state.root();
    state.set_storage(a, U256::MAX, B256::repeat_byte(1)).unwrap();
    state.set_storage(a, U256::MAX, B256::ZERO).unwrap();
    assert_eq!(state.root(), before);
    assert!(state.tree().entries().all(|(_, value)| !value.is_zero()));
}

#[test]
fn invalid_account_is_rejected_atomically() {
    let a = Address::repeat_byte(1);
    let mut state = StateTree::<Blake3Hasher>::new();
    state.set_account(a, account(vec![1])).unwrap();
    let root = state.root();
    assert_eq!(
        state.set_account(a, AccountData { balance: U256::MAX, ..account(vec![2]) }),
        Err(StateError::BalanceOverflow)
    );
    assert_eq!(state.root(), root);
    assert_eq!(state.account(a).unwrap().code.as_ref(), [1]);
    assert_eq!(
        state.set_storage(Address::ZERO, U256::ZERO, B256::ZERO),
        Err(StateError::MissingAccount(Address::ZERO))
    );
}

#[test]
fn push32_can_continue_across_two_boundaries() {
    let mut code = vec![1; 30];
    code.push(0x7f);
    code.extend_from_slice(&[0x7f; 32]);
    code.push(0x00);
    let chunks = key::chunkify_code(&code);
    assert_eq!(chunks[0][0], 0);
    assert_eq!(chunks[1][0], 31);
    assert_eq!(chunks[2][0], 1);
}
