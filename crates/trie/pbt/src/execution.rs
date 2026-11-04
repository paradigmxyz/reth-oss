//! Applies finalized block output using plain address/slot preimages, never MPT hashed keys.

use crate::{AccountData, StateError, StateTree, TreeHasher};
use alloy_primitives::{keccak256, Bytes, B256, KECCAK256_EMPTY};
use reth_trie_common::bundle_state::BundleState;

/// Applies one block's execution output atomically to a parent PBT state.
///
/// The parent must contain all accounts and code. New code is obtained from the output's
/// contract table; missing or inconsistent code fails the block without changing the parent.
/// The finalized output is authoritative on serial and parallel execution paths alike.
pub fn apply_bundle<H: TreeHasher>(
    parent: &StateTree<H>,
    bundle: &BundleState,
) -> Result<StateTree<H>, ExecutionError> {
    let mut state = parent.clone();
    for (address, account) in bundle.state() {
        if account.status.is_not_modified() {
            continue;
        }
        let Some(info) = &account.info else {
            state.remove_account(*address);
            continue;
        };
        let code = if info.code_hash == KECCAK256_EMPTY || info.code_hash.is_zero() {
            Bytes::new()
        } else if let Some(code) =
            info.code.as_ref().or_else(|| bundle.contracts.get(&info.code_hash))
        {
            code.original_bytes()
        } else if let Some(account) =
            parent.account(*address).filter(|account| keccak256(&account.code) == info.code_hash)
        {
            account.code.clone()
        } else {
            return Err(ExecutionError::MissingCode(info.code_hash));
        };
        if !info.code_hash.is_zero() && keccak256(&code) != info.code_hash {
            return Err(ExecutionError::MissingCode(info.code_hash));
        }
        if account.was_destroyed() {
            state.clear_storage(*address);
        }
        state.apply_account(
            *address,
            Some(AccountData { nonce: info.nonce, balance: info.balance, code }),
        )?;
        if state.account(*address).is_some() {
            for (slot, value) in &account.storage {
                state.set_storage(*address, *slot, B256::from(value.present_value))?;
            }
        }
    }
    Ok(state)
}

/// Invalid or incomplete execution output.
#[derive(Debug, thiserror::Error)]
pub enum ExecutionError {
    /// The block refers to bytecode unavailable in the parent and output, or with a wrong hash.
    #[error("missing or inconsistent bytecode for {0}")]
    MissingCode(B256),
    /// Invalid Ethereum state encoding.
    #[error(transparent)]
    State(#[from] StateError),
}
