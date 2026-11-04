use alloy_primitives::B256;

/// The consensus-selected hash used for both key derivation and tree merkelization.
pub trait TreeHasher: Clone + std::fmt::Debug + Send + Sync + 'static {
    /// Hashes the concatenation of the supplied byte slices.
    fn hash(parts: &[&[u8]]) -> B256;
}

/// BLAKE3-256, as used by the EIP-8297 reference implementation.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Blake3Hasher;

impl TreeHasher for Blake3Hasher {
    fn hash(parts: &[&[u8]]) -> B256 {
        let mut hasher = blake3::Hasher::new();
        for part in parts {
            hasher.update(part);
        }
        B256::from(*hasher.finalize().as_bytes())
    }
}

pub(crate) fn leaf_hash<H: TreeHasher>(key: &[u8], value: B256) -> B256 {
    H::hash(&[&[0], key, value.as_slice()])
}

pub(crate) fn branch_hash<H: TreeHasher>(prefix: &[bool], left: B256, right: B256) -> B256 {
    let mut encoded = vec![0; 2 + prefix.len().div_ceil(8)];
    encoded[..2].copy_from_slice(&(prefix.len() as u16).to_be_bytes());
    for (index, bit) in prefix.iter().enumerate() {
        encoded[2 + index / 8] |= u8::from(*bit) << (7 - index % 8);
    }
    H::hash(&[&[1], &encoded, left.as_slice(), right.as_slice()])
}
