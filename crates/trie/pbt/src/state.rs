use crate::{
    key::{self, BASIC_DATA_LEAF_KEY, CODE_HASH_LEAF_KEY, DELEGATION_LEAF_KEY},
    BinaryTree, Blake3Hasher, TreeError, TreeHasher,
};
use alloy_primitives::{keccak256, Address, Bytes, B256, U256};
use imbl::OrdMap;

/// Ethereum state encoded into a single partitioned binary tree.
///
/// Account preimages and code references are retained alongside the commitment. Cloning gives
/// an independent state snapshot. This in-memory backend is intended for reference execution
/// and devnets; it does not replace Reth's database or historical-state providers.
#[derive(Clone, Debug)]
pub struct StateTree<H = Blake3Hasher> {
    tree: BinaryTree<H>,
    accounts: OrdMap<Address, AccountData>,
    code_references: OrdMap<B256, usize>,
    storages: OrdMap<Address, OrdMap<U256, B256>>,
}

impl<H: TreeHasher> Default for StateTree<H> {
    fn default() -> Self {
        Self {
            tree: BinaryTree::new(),
            accounts: OrdMap::new(),
            code_references: OrdMap::new(),
            storages: OrdMap::new(),
        }
    }
}

impl<H: TreeHasher> StateTree<H> {
    /// Creates empty state.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the complete state commitment.
    pub fn root(&self) -> B256 {
        self.tree.root()
    }

    /// Returns the underlying commitment for iteration and proof construction.
    pub const fn tree(&self) -> &BinaryTree<H> {
        &self.tree
    }

    /// Returns retained account data, or `None` for an absent account.
    pub fn account(&self, address: Address) -> Option<&AccountData> {
        self.accounts.get(&address)
    }

    /// Iterates over account preimages and data in address order.
    pub fn accounts(&self) -> impl Iterator<Item = (Address, &AccountData)> {
        self.accounts.iter().map(|(address, account)| (*address, account))
    }

    /// Iterates over an account's nonzero storage and retained plain slot preimages.
    pub fn storage_entries(&self, address: Address) -> impl Iterator<Item = (U256, B256)> + '_ {
        self.storages
            .get(&address)
            .into_iter()
            .flat_map(|slots| slots.iter().map(|(slot, value)| (*slot, *value)))
    }

    /// Replaces account header/code data while preserving its storage.
    ///
    /// This encodes even an explicitly supplied empty account, as required by the standalone
    /// embedding vectors. Use [`Self::apply_account`] for post-Merge state clearing. Invalid
    /// balances are rejected before mutation. Shared code survives deletion of one owner.
    pub fn set_account(
        &mut self,
        address: Address,
        account: AccountData,
    ) -> Result<(), StateError> {
        let basic = account.basic_data()?;
        let previous = self.accounts.get(&address).cloned();
        let previous_hash = previous.as_ref().and_then(AccountData::contract_hash);
        let next_hash = account.contract_hash();
        if previous_hash != next_hash {
            if let Some(hash) = previous_hash {
                self.release_code(hash, &previous.as_ref().unwrap().code);
            }
            if let Some(hash) = next_hash {
                let count = self.code_references.entry(hash).or_default();
                *count += 1;
                if *count == 1 {
                    for (index, chunk) in key::chunkify_code(&account.code).into_iter().enumerate()
                    {
                        self.write(key::code_key::<H>(hash, index as u64).to_vec(), chunk);
                    }
                }
            }
        }
        self.write(key::header_key::<H>(address, BASIC_DATA_LEAF_KEY).to_vec(), basic);
        if account.is_delegation() {
            self.tree.remove(&key::header_key::<H>(address, CODE_HASH_LEAF_KEY));
            let mut value = [0; 32];
            value[..23].copy_from_slice(&account.code);
            self.write(
                key::header_key::<H>(address, DELEGATION_LEAF_KEY).to_vec(),
                B256::from(value),
            );
        } else {
            self.tree.remove(&key::header_key::<H>(address, DELEGATION_LEAF_KEY));
            self.write(
                key::header_key::<H>(address, CODE_HASH_LEAF_KEY).to_vec(),
                keccak256(&account.code),
            );
        }
        self.accounts.insert(address, account);
        Ok(())
    }

    /// Applies an execution account update, clearing empty or destroyed accounts.
    pub fn apply_account(
        &mut self,
        address: Address,
        account: Option<AccountData>,
    ) -> Result<(), StateError> {
        match account {
            Some(account) if !account.is_empty() => self.set_account(address, account),
            _ => {
                self.remove_account(address);
                Ok(())
            }
        }
    }

    /// Deletes an account's header, all its storage, and code no remaining account uses.
    pub fn remove_account(&mut self, address: Address) -> Option<AccountData> {
        let account = self.accounts.remove(&address)?;
        if let Some(hash) = account.contract_hash() {
            self.release_code(hash, &account.code);
        }
        self.clear_storage(address);
        for index in [BASIC_DATA_LEAF_KEY, CODE_HASH_LEAF_KEY, DELEGATION_LEAF_KEY] {
            self.tree.remove(&key::header_key::<H>(address, index));
        }
        Some(account)
    }

    /// Reads a plain storage slot. Absent slots read as zero.
    pub fn storage(&self, address: Address, slot: U256) -> B256 {
        self.tree.get(&key::storage_key::<H>(address, slot)).unwrap_or_default()
    }

    /// Writes a plain slot; a zero value deletes the corresponding leaf.
    pub fn set_storage(
        &mut self,
        address: Address,
        slot: U256,
        value: B256,
    ) -> Result<(), StateError> {
        if !self.accounts.contains_key(&address) {
            return Err(StateError::MissingAccount(address));
        }
        self.write(key::storage_key::<H>(address, slot), value);
        let storages = &mut self.storages;
        if value.is_zero() {
            if let Some(slots) = storages.get_mut(&address) {
                slots.remove(&slot);
                if slots.is_empty() {
                    storages.remove(&address);
                }
            }
        } else {
            storages.entry(address).or_default().insert(slot, value);
        }
        Ok(())
    }

    /// Clears storage while retaining the account header and code.
    pub fn clear_storage(&mut self, address: Address) {
        if let Some(slots) = self.storages.remove(&address) {
            for (slot, _) in slots {
                self.tree.remove(&key::storage_key::<H>(address, slot));
            }
        }
    }

    fn release_code(&mut self, hash: B256, code: &[u8]) {
        let references = &mut self.code_references;
        let count = references.get_mut(&hash).expect("contract reference is tracked");
        *count -= 1;
        if *count == 0 {
            references.remove(&hash);
            for index in 0..code.len().div_ceil(31) {
                self.tree.remove(&key::code_key::<H>(hash, index as u64));
            }
        }
    }

    fn write(&mut self, key: Vec<u8>, value: B256) {
        if value.is_zero() {
            self.tree.remove(&key);
        } else {
            self.tree.insert(key, value).expect("zone-separated state keys are prefix-free");
        }
    }
}

/// Account data needed to encode a PBT header and its code leaves.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AccountData {
    /// Account nonce.
    pub nonce: u64,
    /// Balance, checked against the 128-bit PBT encoding before writing.
    pub balance: U256,
    /// Complete bytecode, or a 23-byte EIP-7702 delegation indicator.
    pub code: Bytes,
}

impl AccountData {
    /// Packs version/reserved bytes, code size, nonce, and balance, all big-endian.
    pub fn basic_data(&self) -> Result<B256, StateError> {
        let size = u32::try_from(self.code.len()).map_err(|_| StateError::CodeSizeOverflow)?;
        key::encode_basic_data(size, self.nonce, self.balance)
    }

    /// Whether the account is empty under the post-Merge state-clearing invariant.
    pub fn is_empty(&self) -> bool {
        self.nonce == 0 && self.balance.is_zero() && self.code.is_empty()
    }

    /// Whether this code is an EIP-7702 delegation indicator.
    pub fn is_delegation(&self) -> bool {
        self.code.len() == 23 && self.code.starts_with(&[0xef, 0x01, 0x00])
    }

    fn contract_hash(&self) -> Option<B256> {
        (!self.code.is_empty() && !self.is_delegation()).then(|| keccak256(&self.code))
    }
}

/// Invalid state transition input.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum StateError {
    /// The balance cannot fit the basic-data leaf's 16-byte balance field.
    #[error("balance exceeds the PBT 128-bit field")]
    BalanceOverflow,
    /// The code length cannot fit the basic-data leaf's four-byte size field.
    #[error("code length exceeds the PBT 32-bit field")]
    CodeSizeOverflow,
    /// Storage cannot be attached to an account absent from the state.
    #[error("storage write to absent account {0}")]
    MissingAccount(Address),
    /// Invalid generic tree input.
    #[error(transparent)]
    Tree(#[from] TreeError),
}
