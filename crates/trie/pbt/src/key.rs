//! EIP-8297's zone-separated key derivation. All integers and path bits are big-endian.

use crate::{StateError, TreeHasher};
use alloy_primitives::{Address, B256, U256};

/// Account header zone.
pub const ACCOUNT_ZONE: u8 = 0x00;
/// Content-addressed code zone.
pub const CODE_ZONE: u8 = 0x01;
/// Overflow storage zone.
pub const STORAGE_ZONE: u8 = 0xff;
/// Packed basic-data sub-index.
pub const BASIC_DATA_LEAF_KEY: u8 = 0;
/// Code-hash sub-index.
pub const CODE_HASH_LEAF_KEY: u8 = 1;
/// Delegation sub-index.
pub const DELEGATION_LEAF_KEY: u8 = 2;
/// First inline storage sub-index.
pub const HEADER_STORAGE_OFFSET: u8 = 64;
/// Number of storage slots colocated with the account header.
pub const HEADER_STORAGE_SLOTS: u64 = 64;
/// Number of sub-indices in one stem.
pub const STEM_SUBTREE_WIDTH: u64 = 256;
/// Account and code key length in bytes.
pub const ACCOUNT_KEY_LENGTH: usize = 34;
/// Overflow storage key length in bytes.
pub const STORAGE_KEY_LENGTH: usize = 66;

/// Pads a legacy address to the 32-byte address used for PBT key derivation.
pub fn address32(address: Address) -> B256 {
    let mut bytes = [0; 32];
    bytes[12..].copy_from_slice(address.as_slice());
    B256::from(bytes)
}

/// Hash-derived account position, shared by its header and overflow storage bucket.
pub fn account_hash<H: TreeHasher>(address: Address) -> B256 {
    H::hash(&[address32(address).as_slice()])
}

/// Key of an account header field.
pub fn header_key<H: TreeHasher>(address: Address, sub_index: u8) -> [u8; ACCOUNT_KEY_LENGTH] {
    let mut key = [0; ACCOUNT_KEY_LENGTH];
    key[0] = ACCOUNT_ZONE;
    key[1..33].copy_from_slice(account_hash::<H>(address).as_slice());
    key[33] = sub_index;
    key
}

/// Key of a plain EVM storage slot, including inline slots 0 through 63.
pub fn storage_key<H: TreeHasher>(address: Address, slot: U256) -> Vec<u8> {
    if slot < U256::from(HEADER_STORAGE_SLOTS) {
        return header_key::<H>(address, HEADER_STORAGE_OFFSET + slot.to::<u8>()).to_vec();
    }
    let group = (slot >> 8usize).to_be_bytes::<32>();
    let mut key = Vec::with_capacity(STORAGE_KEY_LENGTH);
    key.push(STORAGE_ZONE);
    key.extend_from_slice(account_hash::<H>(address).as_slice());
    key.extend_from_slice(H::hash(&[address32(address).as_slice(), &group]).as_slice());
    key.push(slot.byte(0));
    debug_assert_eq!(key.len(), STORAGE_KEY_LENGTH);
    key
}

/// Key of a 31-byte code chunk, addressed by Keccak code hash and chunk number.
pub fn code_key<H: TreeHasher>(code_hash: B256, chunk: u64) -> [u8; ACCOUNT_KEY_LENGTH] {
    let group = U256::from(chunk / STEM_SUBTREE_WIDTH).to_be_bytes::<32>();
    let mut key = [0; ACCOUNT_KEY_LENGTH];
    key[0] = CODE_ZONE;
    key[1..33].copy_from_slice(H::hash(&[code_hash.as_slice(), &group]).as_slice());
    key[33] = (chunk % STEM_SUBTREE_WIDTH) as u8;
    key
}

/// Chunks bytecode into 31 code bytes plus a leading PUSH-continuation byte.
///
/// Zero-valued chunks are retained here; the state writer removes them. PUSH data can span
/// more than one boundary, and PUSH-looking bytes inside data must not be interpreted as opcodes.
pub fn chunkify_code(code: &[u8]) -> Vec<B256> {
    let mut chunks = Vec::with_capacity(code.len().div_ceil(31));
    let mut remaining_push = 0usize;
    for bytes in code.chunks(31) {
        let mut chunk = [0; 32];
        chunk[0] = remaining_push.min(31) as u8;
        chunk[1..1 + bytes.len()].copy_from_slice(bytes);
        for byte in bytes {
            if remaining_push > 0 {
                remaining_push -= 1;
            } else if (0x60..=0x7f).contains(byte) {
                remaining_push = usize::from(*byte - 0x5f);
            }
        }
        chunks.push(B256::from(chunk));
    }
    chunks
}

/// Packs a basic-data leaf, rejecting balances that exceed its 128-bit balance field.
pub fn encode_basic_data(code_size: u32, nonce: u64, balance: U256) -> Result<B256, StateError> {
    let balance = u128::try_from(balance).map_err(|_| StateError::BalanceOverflow)?;
    let mut value = [0; 32];
    value[4..8].copy_from_slice(&code_size.to_be_bytes());
    value[8..16].copy_from_slice(&nonce.to_be_bytes());
    value[16..].copy_from_slice(&balance.to_be_bytes());
    Ok(B256::from(value))
}
