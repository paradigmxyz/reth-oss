//! Measures updating one account and slot while retaining the parent snapshot.

use alloy_primitives::{Address, B256, U256};
use reth_trie_pbt::{AccountData, Blake3Hasher, StateTree};
use std::{hint::black_box, time::Instant};

fn main() {
    for size in [1_000, 10_000] {
        let mut parent = StateTree::<Blake3Hasher>::new();
        for index in 0..size {
            let address = Address::from_word(B256::from(U256::from(index)));
            parent.set_account(address, AccountData { nonce: 1, ..Default::default() }).unwrap();
            parent.set_storage(address, U256::from(64), B256::from(U256::ONE)).unwrap();
        }
        let root = parent.root();
        let address = Address::from_word(B256::from(U256::from(size / 2)));
        let start = Instant::now();
        for _ in 0..100 {
            let mut child = parent.clone();
            child.set_account(address, AccountData { nonce: 2, ..Default::default() }).unwrap();
            child.set_storage(address, U256::from(64), B256::from(U256::from(2))).unwrap();
            black_box(child.root());
        }
        println!(
            "accounts={size}, child_account_and_slot_update_us={:.2}",
            start.elapsed().as_secs_f64() * 10_000.0
        );
        assert_eq!(parent.root(), root);
    }
}
