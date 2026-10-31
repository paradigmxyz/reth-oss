//! Offline conversion from a pinned Reth MPT state view.
//!
//! Source ranges are paged and preimages are read sequentially. The resulting state and artifacts
//! still use the in-memory reference backend. Callers must pin the provider to a finalized anchor.

use crate::{
    migration::{mpt_root, ArtifactError, Artifacts, DecodeLimits},
    AccountData, StateTree, TreeHasher,
};
use alloy_primitives::{keccak256, Address, Bytes, B256, KECCAK256_EMPTY, U256};
use reth_storage_api::{RangeEnd, RangeResponse};
use std::{collections::BTreeMap, io::Read, vec::IntoIter};

pub use reth_storage_api::{errors::provider::ProviderError, BytecodeReader, StateRangeProvider};

/// Converts the complete MPT source into canonical PBT artifacts.
///
/// `source` and `codes` must refer to the same retained anchor state. `anchor_mpt_root` comes from
/// the caller's validated finalized header. Missing, duplicate, unordered and surplus account or
/// slot preimages are rejected, including omitted source accounts and storage. The complete result
/// is authenticated before it is returned; callers must not publish partial output on failure.
pub fn convert<H: TreeHasher>(
    source: &impl StateRangeProvider,
    codes: &(impl BytecodeReader + ?Sized),
    preimages: impl Read,
    anchor_mpt_root: B256,
    limits: DecodeLimits,
) -> Result<Artifacts, ConversionError> {
    let mut reader = Preimages(preimages);
    let mut accounts = HashedRange::default();
    let mut state = StateTree::<H>::new();
    let mut code_cache = BTreeMap::<B256, Bytes>::new();
    let mut code_bytes = 0usize;
    let mut count = 0usize;
    while let Some(address) = reader.address()? {
        count += 1;
        if count > limits.max_accounts {
            return Err(ArtifactError::ResourceLimit.into());
        }
        let hash = keccak256(address);
        let (path, account) = accounts
            .next(|start| source.account_range(start, B256::repeat_byte(0xff), RANGE_BYTES))?
            .ok_or(ArtifactError::Invalid("surplus account preimage"))?;
        if path != hash {
            return Err(ArtifactError::Invalid("account preimages do not match MPT paths").into());
        }
        let code_hash = account.bytecode_hash.unwrap_or(KECCAK256_EMPTY);
        let code = if code_hash == KECCAK256_EMPTY {
            Bytes::new()
        } else if let Some(code) = code_cache.get(&code_hash) {
            code.clone()
        } else {
            let code = codes
                .bytecode_by_hash(&code_hash)?
                .ok_or(ArtifactError::Invalid("missing source bytecode"))?
                .original_bytes();
            code_bytes = code_bytes.checked_add(code.len()).ok_or(ArtifactError::ResourceLimit)?;
            if code.len() > limits.max_code_size || code_bytes > limits.max_total_code_size {
                return Err(ArtifactError::ResourceLimit.into());
            }
            if keccak256(&code) != code_hash {
                return Err(ArtifactError::Invalid("source code hash mismatch").into());
            }
            code_cache.insert(code_hash, code.clone());
            code
        };
        let account = AccountData { nonce: account.nonce, balance: account.balance, code };
        if account.is_empty() {
            return Err(ArtifactError::Invalid("empty anchor account").into());
        }
        state.set_account(address, account).map_err(ArtifactError::from)?;
        check_leaves(&state, limits)?;
        let slot_count = u32::from_be_bytes(reader.fixed()?) as usize;
        if slot_count > limits.max_leaves {
            return Err(ArtifactError::ResourceLimit.into());
        }
        let mut storage = HashedRange::default();
        let fetch = |start| {
            source.storage_range(hash, start, B256::repeat_byte(0xff), RANGE_BYTES)?.ok_or_else(
                || ProviderError::other(std::io::Error::other("source account disappeared")),
            )
        };
        for _ in 0..slot_count {
            let slot = B256::from(reader.fixed::<32>()?);
            let (path, value) =
                storage.next(fetch)?.ok_or(ArtifactError::Invalid("surplus slot preimage"))?;
            if path != keccak256(slot) || value.is_zero() {
                return Err(ArtifactError::Invalid("slot preimages do not match MPT paths").into());
            }
            state
                .set_storage(address, U256::from_be_bytes(slot.0), B256::from(value))
                .map_err(ArtifactError::from)?;
            check_leaves(&state, limits)?;
        }
        if storage.next(fetch)?.is_some() {
            return Err(ArtifactError::Invalid("missing slot preimage").into());
        }
    }
    if accounts
        .next(|start| source.account_range(start, B256::repeat_byte(0xff), RANGE_BYTES))?
        .is_some()
    {
        return Err(ArtifactError::Invalid("missing account preimage").into());
    }
    if mpt_root(&state) != anchor_mpt_root {
        return Err(ArtifactError::AnchorRootMismatch.into());
    }
    Ok(Artifacts::from_state(&state)?)
}

/// Failed source read or invalid conversion input. No artifacts have been published.
#[derive(Debug, thiserror::Error)]
pub enum ConversionError {
    /// The source provider could not read the pinned MPT state or code.
    #[error(transparent)]
    Provider(#[from] ProviderError),
    /// A preimage stream could not be read.
    #[error("reading PBT conversion preimages: {0}")]
    Io(#[from] std::io::Error),
    /// The input is incomplete, noncanonical, over budget, or does not reproduce the anchor.
    #[error(transparent)]
    Artifact(#[from] ArtifactError),
}

const RANGE_BYTES: usize = 64 * 1024;

struct Preimages<R>(R);

impl<R: Read> Preimages<R> {
    fn address(&mut self) -> Result<Option<Address>, ConversionError> {
        let mut bytes = [0; 20];
        loop {
            match self.0.read(&mut bytes[..1]) {
                Ok(0) => return Ok(None),
                Ok(_) => break,
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error.into()),
            }
        }
        self.0.read_exact(&mut bytes[1..])?;
        Ok(Some(Address::from(bytes)))
    }

    fn fixed<const N: usize>(&mut self) -> Result<[u8; N], ConversionError> {
        let mut bytes = [0; N];
        self.0.read_exact(&mut bytes)?;
        Ok(bytes)
    }
}

struct HashedRange<T> {
    start: Option<B256>,
    entries: IntoIter<(B256, T)>,
}

impl<T> Default for HashedRange<T> {
    fn default() -> Self {
        Self { start: Some(B256::ZERO), entries: Vec::new().into_iter() }
    }
}

impl<T> HashedRange<T> {
    fn next(
        &mut self,
        fetch: impl FnOnce(B256) -> Result<RangeResponse<(B256, T)>, ProviderError>,
    ) -> Result<Option<(B256, T)>, ConversionError> {
        if let Some(entry) = self.entries.next() {
            return Ok(Some(entry));
        }
        let Some(start) = self.start else { return Ok(None) };
        let page = fetch(start)?;
        if page.items.first().is_some_and(|(key, _)| *key < start) ||
            page.items.windows(2).any(|pair| pair[0].0 >= pair[1].0) ||
            (page.items.is_empty() && page.end != RangeEnd::Exhausted)
        {
            return Err(ArtifactError::Invalid("source range did not advance in hash order").into());
        }
        self.start = if page.end == RangeEnd::Exhausted {
            None
        } else {
            page.items.last().and_then(|(hash, _)| {
                U256::from_be_bytes(hash.0).checked_add(U256::ONE).map(B256::from)
            })
        };
        self.entries = page.items.into_iter();
        Ok(self.entries.next())
    }
}

const fn check_leaves<H: TreeHasher>(
    state: &StateTree<H>,
    limits: DecodeLimits,
) -> Result<(), ArtifactError> {
    if state.tree().len() > limits.max_leaves {
        return Err(ArtifactError::ResourceLimit);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn range_cursor_rejects_nonprogressing_and_unordered_pages() {
        for items in [vec![], vec![(B256::repeat_byte(2), 1), (B256::repeat_byte(1), 2)]] {
            let mut range = HashedRange::default();
            assert!(matches!(
                range.next(|_| Ok(RangeResponse { items, end: RangeEnd::ByteLimit })),
                Err(ConversionError::Artifact(ArtifactError::Invalid(
                    "source range did not advance in hash order"
                )))
            ));
        }
        let mut range = HashedRange::default();
        assert_eq!(
            range
                .next(|_| Ok(RangeResponse {
                    items: vec![(B256::ZERO, 1)],
                    end: RangeEnd::ByteLimit
                }))
                .unwrap(),
            Some((B256::ZERO, 1))
        );
        assert!(matches!(
            range.next(|_| Ok(RangeResponse {
                items: vec![(B256::ZERO, 1)],
                end: RangeEnd::ByteLimit
            })),
            Err(ConversionError::Artifact(ArtifactError::Invalid(
                "source range did not advance in hash order"
            )))
        ));
    }

    #[test]
    fn range_cursor_handles_the_maximum_key_and_provider_failure() {
        let mut range = HashedRange::default();
        let last = B256::repeat_byte(0xff);
        assert_eq!(
            range
                .next(|_| Ok(RangeResponse { items: vec![(last, 7)], end: RangeEnd::HashLimit }))
                .unwrap(),
            Some((last, 7))
        );
        assert_eq!(range.next(|_| panic!("maximum key must exhaust the cursor")).unwrap(), None);
        let mut range = HashedRange::<u8>::default();
        assert!(matches!(
            range.next(|_| Err(ProviderError::other(std::io::Error::other("source failure")))),
            Err(ConversionError::Provider(_))
        ));
    }
}
