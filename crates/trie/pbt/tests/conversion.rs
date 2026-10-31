//! Provider-backed conversion and exact preimage matching.
#![cfg(feature = "conversion")]

use alloy_primitives::{keccak256, Address, Bytes, B256, U256};
use reth_primitives_traits::{Account, Bytecode};
use reth_storage_api::{
    errors::provider::{ProviderError, ProviderResult},
    BytecodeReader, RangeEnd, RangeResponse, RangeResult, StateRangeProvider, StorageRangeResult,
};
use reth_trie_pbt::{
    conversion::{convert, ConversionError},
    migration::{mpt_root, ArtifactError, Artifacts, DecodeLimits},
    AccountData, Blake3Hasher, StateTree,
};
use std::collections::BTreeMap;

#[derive(Default)]
struct Source {
    accounts: BTreeMap<B256, Account>,
    storage: BTreeMap<B256, BTreeMap<B256, U256>>,
    code: BTreeMap<B256, Bytecode>,
}

impl Source {
    fn new(state: &StateTree) -> Self {
        let mut source = Self::default();
        for (address, account) in state.accounts() {
            let hash = keccak256(address);
            source.accounts.insert(
                hash,
                Account {
                    nonce: account.nonce,
                    balance: account.balance,
                    bytecode_hash: (!account.code.is_empty()).then(|| keccak256(&account.code)),
                },
            );
            source.storage.insert(
                hash,
                state
                    .storage_entries(address)
                    .map(|(slot, value)| {
                        (keccak256(B256::from(slot)), U256::from_be_bytes(value.0))
                    })
                    .collect(),
            );
            if !account.code.is_empty() {
                source
                    .code
                    .insert(keccak256(&account.code), Bytecode::new_raw(account.code.clone()));
            }
        }
        source
    }
}

impl BytecodeReader for Source {
    fn bytecode_by_hash(&self, hash: &B256) -> ProviderResult<Option<Bytecode>> {
        Ok(self.code.get(hash).cloned())
    }
}

impl StateRangeProvider for Source {
    fn account_range(
        &self,
        start: B256,
        _limit: B256,
        _bytes: usize,
    ) -> RangeResult<(B256, Account)> {
        Ok(page(&self.accounts, start))
    }

    fn storage_root_by_hash(&self, _address: B256) -> ProviderResult<B256> {
        Err(ProviderError::other(std::io::Error::other("unused conversion proof")))
    }

    fn storage_range(
        &self,
        address: B256,
        start: B256,
        _limit: B256,
        _bytes: usize,
    ) -> StorageRangeResult {
        Ok(self.storage.get(&address).map(|storage| page(storage, start)))
    }

    fn account_range_proof(&self, _keys: &[B256]) -> ProviderResult<Vec<Bytes>> {
        Err(ProviderError::other(std::io::Error::other("unused conversion proof")))
    }

    fn storage_range_proof(&self, _address: B256, _keys: &[B256]) -> ProviderResult<Vec<Bytes>> {
        Err(ProviderError::other(std::io::Error::other("unused conversion proof")))
    }
}

fn page<T: Clone>(entries: &BTreeMap<B256, T>, start: B256) -> RangeResponse<(B256, T)> {
    // A one-item page exercises cursor continuation across every account and slot.
    let items: Vec<_> =
        entries.range(start..).take(1).map(|(key, value)| (*key, value.clone())).collect();
    let end = if items.is_empty() { RangeEnd::Exhausted } else { RangeEnd::ByteLimit };
    RangeResponse { items, end }
}

fn populated() -> StateTree {
    let mut state = StateTree::new();
    for byte in [1, 2] {
        let address = Address::repeat_byte(byte);
        state
            .set_account(
                address,
                AccountData { nonce: 1, balance: U256::from(byte), code: vec![0x7f; 64].into() },
            )
            .unwrap();
        for slot in [0, 63, 64, 256] {
            state.set_storage(address, U256::from(slot), B256::repeat_byte(byte)).unwrap();
        }
    }
    state
}

#[test]
fn paged_mpt_conversion_matches_direct_artifacts() {
    let state = populated();
    let source = Source::new(&state);
    let expected = Artifacts::from_state(&state).unwrap();
    let actual = convert::<Blake3Hasher>(
        &source,
        &source,
        expected.preimages.as_slice(),
        mpt_root(&state),
        DecodeLimits::default(),
    )
    .unwrap();
    assert_eq!(actual, expected);
    assert_eq!(
        actual.verify::<Blake3Hasher>(mpt_root(&state), DecodeLimits::default()).unwrap().root(),
        state.root()
    );
}

#[test]
fn conversion_rejects_missing_and_surplus_preimages() {
    let state = populated();
    let source = Source::new(&state);
    let original = Artifacts::from_state(&state).unwrap().preimages;
    let one_record = 24 + 4 * 32;
    for input in [&[][..], &original[..one_record], &original[one_record..]] {
        assert!(matches!(
            convert::<Blake3Hasher>(
                &source,
                &source,
                input,
                mpt_root(&state),
                DecodeLimits::default()
            ),
            Err(ConversionError::Artifact(ArtifactError::Invalid(_)))
        ));
    }
    let mut missing_slot = original.clone();
    missing_slot[20..24].copy_from_slice(&3u32.to_be_bytes());
    missing_slot.drain(24 + 3 * 32..24 + 4 * 32);
    assert!(matches!(
        convert::<Blake3Hasher>(
            &source,
            &source,
            missing_slot.as_slice(),
            mpt_root(&state),
            DecodeLimits::default()
        ),
        Err(ConversionError::Artifact(ArtifactError::Invalid("missing slot preimage")))
    ));
    let mut duplicate = original.clone();
    duplicate.splice(one_record..one_record, original[..one_record].iter().copied());
    assert!(matches!(
        convert::<Blake3Hasher>(
            &source,
            &source,
            duplicate.as_slice(),
            mpt_root(&state),
            DecodeLimits::default()
        ),
        Err(ConversionError::Artifact(ArtifactError::Invalid(
            "account preimages do not match MPT paths"
        )))
    ));
}

#[test]
fn conversion_authenticates_code_anchor_and_resource_limits() {
    let state = populated();
    let mut source = Source::new(&state);
    let artifacts = Artifacts::from_state(&state).unwrap();
    let run = |source: &Source, root, limits| {
        convert::<Blake3Hasher>(source, source, artifacts.preimages.as_slice(), root, limits)
    };
    assert!(matches!(
        run(&source, B256::ZERO, DecodeLimits::default()),
        Err(ConversionError::Artifact(ArtifactError::AnchorRootMismatch))
    ));
    for limits in [
        DecodeLimits { max_accounts: 0, ..Default::default() },
        DecodeLimits { max_leaves: 1, ..Default::default() },
        DecodeLimits { max_code_size: 1, ..Default::default() },
        DecodeLimits { max_total_code_size: 1, ..Default::default() },
    ] {
        assert!(matches!(
            run(&source, mpt_root(&state), limits),
            Err(ConversionError::Artifact(ArtifactError::ResourceLimit))
        ));
    }
    let hash = *source.code.keys().next().unwrap();
    source.code.clear();
    assert!(matches!(
        run(&source, mpt_root(&state), DecodeLimits::default()),
        Err(ConversionError::Artifact(ArtifactError::Invalid("missing source bytecode")))
    ));
    source.code.insert(hash, Bytecode::new_raw(Bytes::from_static(&[1, 2, 3])));
    assert!(matches!(
        run(&source, mpt_root(&state), DecodeLimits::default()),
        Err(ConversionError::Artifact(ArtifactError::Invalid("source code hash mismatch")))
    ));
    assert!(
        matches!(convert::<Blake3Hasher>(&source, &source, &artifacts.preimages[..19], mpt_root(&state), DecodeLimits::default()), Err(ConversionError::Io(error)) if error.kind() == std::io::ErrorKind::UnexpectedEof)
    );
}

#[test]
fn empty_delegated_and_zero_code_states_convert_canonically() {
    let mut state = StateTree::new();
    for kind in 0..3 {
        if kind == 1 {
            let mut indicator = vec![0xef, 1, 0];
            indicator.extend_from_slice(Address::repeat_byte(9).as_slice());
            state
                .set_account(
                    Address::repeat_byte(3),
                    AccountData { nonce: 1, code: indicator.into(), ..Default::default() },
                )
                .unwrap();
        } else if kind == 2 {
            state
                .set_account(
                    Address::repeat_byte(4),
                    AccountData { nonce: 1, code: vec![0; 62].into(), ..Default::default() },
                )
                .unwrap();
        }
        let source = Source::new(&state);
        let expected = Artifacts::from_state(&state).unwrap();
        assert_eq!(
            convert::<Blake3Hasher>(
                &source,
                &source,
                expected.preimages.as_slice(),
                mpt_root(&state),
                DecodeLimits::default()
            )
            .unwrap(),
            expected
        );
    }
}
