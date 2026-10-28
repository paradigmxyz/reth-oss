//! Canonical conversion, dual-check, and malformed-artifact tests.

#![cfg(feature = "migration")]

use alloy_primitives::{Address, B256, U256};
use reth_trie_pbt::{
    migration::{mpt_root, Artifacts, DecodeLimits},
    AccountData, Blake3Hasher, StateTree,
};

fn populated() -> StateTree {
    let mut state = StateTree::new();
    for byte in [1, 2] {
        let address = Address::repeat_byte(byte);
        state
            .set_account(
                address,
                AccountData { nonce: 1, balance: U256::from(byte), code: vec![1; 31 * 257].into() },
            )
            .unwrap();
        for slot in [0, 63, 64, 255, 256, 257] {
            state.set_storage(address, U256::from(slot), B256::repeat_byte(byte)).unwrap();
        }
    }
    let address = Address::repeat_byte(3);
    let mut code = vec![0xef, 1, 0];
    code.extend_from_slice(address.as_slice());
    state
        .set_account(address, AccountData { nonce: 2, balance: U256::ZERO, code: code.into() })
        .unwrap();
    state
}

#[test]
fn empty_artifact_has_only_end_tag_and_root() {
    let state = StateTree::<Blake3Hasher>::new();
    let artifacts = Artifacts::from_state(&state).unwrap();
    let mut expected = vec![7];
    expected.extend_from_slice(&[0; 32]);
    assert_eq!(artifacts.snapshot, expected);
    assert!(artifacts.preimages.is_empty());
    assert_eq!(
        artifacts.verify::<Blake3Hasher>(mpt_root(&state), DecodeLimits::default()).unwrap().root(),
        state.root()
    );
}

#[test]
fn populated_state_roundtrips_canonically_regardless_of_insertion_order() {
    let state = populated();
    let artifacts = Artifacts::from_state(&state).unwrap();
    let restored =
        artifacts.verify::<Blake3Hasher>(mpt_root(&state), DecodeLimits::default()).unwrap();
    assert_eq!(restored.root(), state.root());
    assert_eq!(artifacts, Artifacts::from_state(&restored).unwrap());
    let mut reverse = StateTree::<Blake3Hasher>::new();
    for (address, account) in state.accounts().collect::<Vec<_>>().into_iter().rev() {
        reverse.set_account(address, account.clone()).unwrap();
        for (slot, value) in state.storage_entries(address) {
            reverse.set_storage(address, slot, value).unwrap();
        }
    }
    assert_eq!(artifacts, Artifacts::from_state(&reverse).unwrap());
    assert_eq!(artifacts.digests(), Artifacts::from_state(&restored).unwrap().digests());
    assert_eq!(
        artifacts.restore::<Blake3Hasher>(state.root(), DecodeLimits::default()).unwrap().root(),
        state.root()
    );
    assert!(artifacts.restore::<Blake3Hasher>(B256::ZERO, DecodeLimits::default()).is_err());
    assert!(artifacts.verify::<Blake3Hasher>(B256::ZERO, DecodeLimits::default()).is_err());
}

#[test]
fn every_truncation_and_trailing_byte_is_rejected() {
    let mut state = StateTree::<Blake3Hasher>::new();
    let address = Address::repeat_byte(1);
    state
        .set_account(
            address,
            AccountData { nonce: 1, balance: U256::from(1), code: vec![0x7f; 33].into() },
        )
        .unwrap();
    state.set_storage(address, U256::from(64), B256::repeat_byte(1)).unwrap();
    let artifacts = Artifacts::from_state(&state).unwrap();
    let anchor = mpt_root(&state);
    for end in 0..artifacts.snapshot.len() {
        let malformed = Artifacts {
            snapshot: artifacts.snapshot[..end].to_vec(),
            preimages: artifacts.preimages.clone(),
        };
        assert!(
            malformed.verify::<Blake3Hasher>(anchor, DecodeLimits::default()).is_err(),
            "snapshot prefix {end}"
        );
    }
    for end in 0..artifacts.preimages.len() {
        let malformed = Artifacts {
            snapshot: artifacts.snapshot.clone(),
            preimages: artifacts.preimages[..end].to_vec(),
        };
        assert!(
            malformed.verify::<Blake3Hasher>(anchor, DecodeLimits::default()).is_err(),
            "preimage prefix {end}"
        );
    }
    let mut malformed = artifacts.clone();
    malformed.snapshot.push(0);
    assert!(malformed.verify::<Blake3Hasher>(anchor, DecodeLimits::default()).is_err());
    let mut malformed = artifacts;
    malformed.preimages.push(0);
    assert!(malformed.verify::<Blake3Hasher>(anchor, DecodeLimits::default()).is_err());
}

#[test]
fn corrupted_snapshot_and_preimages_fail_authentication() {
    let mut state = StateTree::<Blake3Hasher>::new();
    state
        .set_account(
            Address::ZERO,
            AccountData { nonce: 1, balance: U256::from(1), code: vec![0x7f; 65].into() },
        )
        .unwrap();
    let artifacts = Artifacts::from_state(&state).unwrap();
    let anchor = mpt_root(&state);
    for index in 0..artifacts.snapshot.len() {
        let mut malformed = artifacts.clone();
        malformed.snapshot[index] ^= 1;
        assert!(
            malformed.verify::<Blake3Hasher>(anchor, DecodeLimits::default()).is_err(),
            "snapshot byte {index}"
        );
    }
    for index in 0..artifacts.preimages.len() {
        let mut malformed = artifacts.clone();
        malformed.preimages[index] ^= 1;
        assert!(
            malformed.verify::<Blake3Hasher>(anchor, DecodeLimits::default()).is_err(),
            "preimage byte {index}"
        );
    }
}

#[test]
fn resource_bounds_are_checked_before_reconstruction() {
    let state = populated();
    let artifacts = Artifacts::from_state(&state).unwrap();
    for limits in [
        DecodeLimits { max_accounts: 1, ..Default::default() },
        DecodeLimits { max_leaves: 1, ..Default::default() },
        DecodeLimits { max_code_size: 1, ..Default::default() },
        DecodeLimits { max_total_code_size: 1, ..Default::default() },
    ] {
        assert!(artifacts.verify::<Blake3Hasher>(mpt_root(&state), limits).is_err());
    }
    let limits = DecodeLimits { max_total_code_size: 31 * 257, ..Default::default() };
    assert!(artifacts.verify::<Blake3Hasher>(mpt_root(&state), limits).is_ok());
}
