//! Tree mutation, snapshot, and adversarial-proof tests.

use alloy_primitives::B256;
use proptest::{collection, prelude::*};
use reth_trie_pbt::{
    BinaryTree, Blake3Hasher, ProofError, ProofTerminal, TreeError, MAX_KEY_LENGTH,
};
use std::collections::BTreeMap;

fn tree() -> BinaryTree {
    BinaryTree::new()
}

#[test]
fn prefix_conflicts_leave_snapshot_unchanged() {
    for (first, second) in [(vec![1], vec![1, 2]), (vec![1, 2], vec![1])] {
        let mut tree = tree();
        tree.insert(first, B256::ZERO).unwrap();
        let root = tree.root();
        assert_eq!(tree.insert(second, B256::repeat_byte(1)), Err(TreeError::PrefixConflict));
        assert_eq!(tree.root(), root);
        assert_eq!(tree.len(), 1);
    }
    let mut tree = tree();
    tree.insert(vec![1, 0], B256::ZERO).unwrap();
    tree.insert(vec![1, 128], B256::ZERO).unwrap();
    let root = tree.root();
    assert_eq!(tree.insert(vec![1], B256::ZERO), Err(TreeError::PrefixConflict));
    assert_eq!(tree.root(), root);
}

#[test]
fn maximum_prefix_and_key_length() {
    let mut tree = tree();
    assert_eq!(tree.insert(vec![], B256::ZERO), Err(TreeError::InvalidKeyLength));
    assert_eq!(
        tree.insert(vec![0; MAX_KEY_LENGTH + 1], B256::ZERO),
        Err(TreeError::InvalidKeyLength)
    );
    let a = vec![0; MAX_KEY_LENGTH];
    let mut b = a.clone();
    b[MAX_KEY_LENGTH - 1] = 1;
    tree.insert(a.clone(), B256::ZERO).unwrap();
    let snapshot = tree.clone();
    tree.insert(b.clone(), B256::repeat_byte(1)).unwrap();
    assert_eq!(tree.proof(&a).verify::<Blake3Hasher>(tree.root(), &a), Ok(Some(B256::ZERO)));
    tree.remove(&b);
    assert_eq!(tree.root(), snapshot.root());
}

#[test]
fn bulk_input_is_strictly_validated() {
    for keys in [vec![vec![2], vec![1]], vec![vec![1], vec![1]]] {
        assert_eq!(
            BinaryTree::<Blake3Hasher>::from_sorted_entries(
                keys.into_iter().map(|key| (key, B256::ZERO))
            )
            .unwrap_err(),
            TreeError::UnsortedKeys
        );
    }
    assert_eq!(
        BinaryTree::<Blake3Hasher>::from_sorted_entries([
            (vec![1], B256::ZERO),
            (vec![1, 2], B256::ZERO)
        ])
        .unwrap_err(),
        TreeError::PrefixConflict
    );
}

#[test]
fn absence_and_tampered_proofs() {
    let mut tree = tree();
    assert_eq!(tree.proof(&[0]).verify::<Blake3Hasher>(tree.root(), &[0]), Ok(None));
    for key in [vec![0, 0], vec![0, 1], vec![128, 0]] {
        tree.insert(key, B256::repeat_byte(7)).unwrap();
    }
    for key in [vec![0], vec![0, 2], vec![128, 1], vec![255]] {
        assert_eq!(tree.proof(&key).verify::<Blake3Hasher>(tree.root(), &key), Ok(None));
    }
    let mut proof = tree.proof(&[0, 0]);
    proof.steps[0].sibling = B256::ZERO;
    assert_eq!(proof.verify::<Blake3Hasher>(tree.root(), &[0, 0]), Err(ProofError::RootMismatch));
    let mut proof = tree.proof(&[0, 0]);
    proof.steps[0].right = !proof.steps[0].right;
    assert_eq!(proof.verify::<Blake3Hasher>(tree.root(), &[0, 0]), Err(ProofError::InvalidPath));
    let mut proof = tree.proof(&[0, 0]);
    proof.terminal = ProofTerminal::Empty;
    assert_eq!(proof.verify::<Blake3Hasher>(tree.root(), &[0, 0]), Err(ProofError::InvalidPath));
}

proptest! {
    #[test]
    fn mutations_match_sorted_rebuild(operations in collection::vec((any::<[u8; 4]>(), proptest::option::of(any::<[u8; 32]>())), 0..100)) {
        let mut tree = tree();
        let mut model = BTreeMap::new();
        for (key, value) in operations {
            let snapshot = tree.clone();
            let before = snapshot.root();
            if let Some(value) = value {
                let value = B256::from(value);
                prop_assert_eq!(tree.insert(key.to_vec(), value).unwrap(), model.insert(key.to_vec(), value));
            } else {
                prop_assert_eq!(tree.remove(&key), model.remove(key.as_slice()));
            }
            prop_assert_eq!(snapshot.root(), before);
            let rebuilt = BinaryTree::<Blake3Hasher>::from_sorted_entries(model.iter().map(|(key, value)| (key.clone(), *value))).unwrap();
            prop_assert_eq!(tree.root(), rebuilt.root());
            prop_assert_eq!(tree.len(), model.len());
            prop_assert_eq!(tree.get(&key), model.get(key.as_slice()).copied());
            prop_assert_eq!(tree.proof(&key).verify::<Blake3Hasher>(tree.root(), &key).unwrap(), model.get(key.as_slice()).copied());
        }
    }

    #[test]
    fn deleting_every_leaf_restores_empty(entries in collection::btree_map(any::<[u8; 2]>(), any::<[u8; 32]>(), 0..100)) {
        let mut tree = tree();
        for (key, value) in &entries { tree.insert(key.to_vec(), B256::from(*value)).unwrap(); }
        for (key, value) in entries { prop_assert_eq!(tree.remove(&key), Some(B256::from(value))); }
        prop_assert!(tree.is_empty());
        prop_assert_eq!(tree.root(), B256::ZERO);
    }
}
