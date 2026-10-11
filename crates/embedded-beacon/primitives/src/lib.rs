//! Checked consensus types, canonical SSZ decoding, and Merkle primitives.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::sync::OnceLock;
use thiserror::Error;

pub mod gloas;

/// A consensus Merkle root.
pub type Root = [u8; 32];
/// A beacon slot.
pub type Slot = u64;
/// A beacon epoch.
pub type Epoch = u64;

/// Historical schemas, with Gloas the implementation target.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Fork {
    /// Initial consensus schema.
    Phase0,
    /// Altair consensus schema.
    Altair,
    /// Bellatrix consensus schema.
    Bellatrix,
    /// Capella consensus schema.
    Capella,
    /// Deneb consensus schema.
    Deneb,
    /// Electra consensus schema.
    Electra,
    /// Fulu historical consensus schema.
    Fulu,
    /// Gloas target consensus schema.
    Gloas,
}

/// An epoch checkpoint.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Checkpoint {
    /// Epoch number.
    pub epoch: Epoch,
    /// Consensus block root.
    pub root: Root,
}

/// The common beacon header schema.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeaconBlockHeader {
    /// Beacon slot number.
    pub slot: Slot,
    /// Index of the block proposer.
    pub proposer_index: u64,
    /// Parent beacon block root.
    pub parent_root: Root,
    /// Post-state Merkle root.
    pub state_root: Root,
    /// Beacon block body Merkle root.
    pub body_root: Root,
}

impl BeaconBlockHeader {
    /// Decode the exact 112-byte fixed-size SSZ representation.
    pub fn from_ssz(bytes: &[u8]) -> Result<Self, SszError> {
        if bytes.len() != 112 {
            return Err(SszError::InvalidLength { expected: 112, actual: bytes.len() });
        }
        Ok(Self {
            slot: read_u64(&bytes[..8])?,
            proposer_index: read_u64(&bytes[8..16])?,
            parent_root: read_root(&bytes[16..48])?,
            state_root: read_root(&bytes[48..80])?,
            body_root: read_root(&bytes[80..112])?,
        })
    }

    /// Encode the fixed-size SSZ representation.
    pub fn to_ssz(self) -> [u8; 112] {
        let mut bytes = [0; 112];
        bytes[..8].copy_from_slice(&self.slot.to_le_bytes());
        bytes[8..16].copy_from_slice(&self.proposer_index.to_le_bytes());
        bytes[16..48].copy_from_slice(&self.parent_root);
        bytes[48..80].copy_from_slice(&self.state_root);
        bytes[80..112].copy_from_slice(&self.body_root);
        bytes
    }

    /// Compute the header's SSZ root.
    pub fn hash_tree_root(self) -> Root {
        let leaves = [
            uint64_root(self.slot),
            uint64_root(self.proposer_index),
            self.parent_root,
            self.state_root,
            self.body_root,
        ];
        merkleize(&leaves, 5).expect("five fixed container fields")
    }
}

/// An SSZ bitlist with the delimiter removed from its packed data.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BitList {
    bytes: Vec<u8>,
    len: usize,
}

impl BitList {
    /// Bytes reserved by the owned packed-bit allocation, excluding inline metadata.
    pub const fn heap_bytes(&self) -> usize {
        self.bytes.capacity()
    }

    /// Decode canonical bitlist SSZ, enforcing a resource limit.
    pub fn from_ssz(encoded: &[u8], max_bits: usize) -> Result<Self, SszError> {
        let Some(last) = encoded.last().copied() else {
            return Err(SszError::MissingBitlistDelimiter);
        };
        if last == 0 {
            return Err(SszError::MissingBitlistDelimiter);
        }
        let delimiter = 7 - last.leading_zeros() as usize;
        let len = (encoded.len() - 1)
            .checked_mul(8)
            .and_then(|v| v.checked_add(delimiter))
            .ok_or(SszError::Overflow)?;
        if len > max_bits {
            return Err(SszError::LimitExceeded { actual: len, limit: max_bits });
        }
        let mut bytes = encoded.to_vec();
        bytes[encoded.len() - 1] &= !(1 << delimiter);
        bytes.truncate(len.div_ceil(8));
        Ok(Self { bytes, len })
    }

    /// Encode including the SSZ delimiter.
    pub fn to_ssz(&self) -> Vec<u8> {
        let mut bytes = self.bytes.clone();
        bytes.resize(self.len / 8 + 1, 0);
        bytes[self.len / 8] |= 1 << (self.len % 8);
        bytes
    }

    /// Number of bits.
    pub const fn len(&self) -> usize {
        self.len
    }

    /// Whether this bitlist contains no bits.
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Read a bit, returning none for an out-of-range index.
    pub fn get(&self, index: usize) -> Option<bool> {
        (index < self.len).then(|| self.bytes[index / 8] & (1 << (index % 8)) != 0)
    }

    /// Access packed bits without the delimiter.
    pub fn packed(&self) -> &[u8] {
        &self.bytes
    }

    /// Hash a bounded bitlist using the type's bit capacity.
    pub fn hash_tree_root(&self, max_bits: usize) -> Result<Root, SszError> {
        if self.len > max_bits {
            return Err(SszError::LimitExceeded { actual: self.len, limit: max_bits });
        }
        Ok(mix_in_length(
            merkleize(&pack_bytes(&self.bytes), max_bits.div_ceil(256))?,
            self.len as u64,
        ))
    }

    /// Hash a progressive bitlist.
    pub fn progressive_root(&self) -> Result<Root, SszError> {
        Ok(mix_in_length(merkleize_progressive(&pack_bytes(&self.bytes))?, self.len as u64))
    }
}

/// Canonically encoded fixed-size SSZ bitvector.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BitVector {
    bytes: Vec<u8>,
    len: usize,
}

impl BitVector {
    /// Bytes reserved by the owned packed-bit allocation, excluding inline metadata.
    pub const fn heap_bytes(&self) -> usize {
        self.bytes.capacity()
    }

    /// Decode an exact-length bitvector and reject nonzero unused high bits.
    pub fn from_ssz(bytes: &[u8], bits: usize) -> Result<Self, SszError> {
        if bits == 0 {
            return Err(SszError::InvalidBitVector);
        }
        let expected = bits.div_ceil(8);
        if bytes.len() != expected {
            return Err(SszError::InvalidLength { expected, actual: bytes.len() });
        }
        if !bits.is_multiple_of(8) && bytes[expected - 1] >> (bits % 8) != 0 {
            return Err(SszError::InvalidBitVector);
        }
        Ok(Self { bytes: bytes.to_vec(), len: bits })
    }

    /// Number of bits.
    pub const fn len(&self) -> usize {
        self.len
    }

    /// Fixed-size bitvectors cannot be empty.
    pub const fn is_empty(&self) -> bool {
        false
    }

    /// Read a bit with range checking.
    pub fn get(&self, index: usize) -> Option<bool> {
        (index < self.len).then(|| self.bytes[index / 8] & (1 << (index % 8)) != 0)
    }

    /// Set one logical bit in place without changing canonical padding or allocation size.
    pub fn set(&mut self, index: usize, value: bool) -> Result<(), SszError> {
        if index >= self.len {
            return Err(SszError::InvalidBitVector)
        }
        let byte = self.bytes.get_mut(index / 8).ok_or(SszError::InvalidBitVector)?;
        let mask = 1u8 << (index % 8);
        if value {
            *byte |= mask;
        } else {
            *byte &= !mask;
        }
        Ok(())
    }

    /// Access canonical packed bytes.
    pub fn as_ssz(&self) -> &[u8] {
        &self.bytes
    }

    /// Compute the SSZ root.
    pub fn hash_tree_root(&self) -> Result<Root, SszError> {
        merkleize(&pack_bytes(&self.bytes), self.len.div_ceil(256))
    }
}

/// Invalid SSZ data never produces a consensus root.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum SszError {
    /// Invalid Length.
    #[error("invalid length: expected {expected}, got {actual}")]
    InvalidLength {
        /// Required serialized length.
        expected: usize,
        /// Observed serialized length.
        actual: usize,
    },
    /// Limit Exceeded.
    #[error("SSZ size {actual} exceeds limit {limit}")]
    LimitExceeded {
        /// Observed serialized length or count.
        actual: usize,
        /// Maximum permitted length or count.
        limit: usize,
    },
    /// Overflow.
    #[error("SSZ size arithmetic overflow")]
    Overflow,
    /// Missing Bitlist Delimiter.
    #[error("bitlist delimiter is missing")]
    MissingBitlistDelimiter,
    /// Invalid Bit Vector.
    #[error("invalid bitvector or nonzero unused bits")]
    InvalidBitVector,
    /// Invalid Offset.
    #[error("invalid variable field offsets")]
    InvalidOffset,
    /// Invalid Active Fields.
    #[error("invalid progressive active-fields configuration")]
    InvalidActiveFields,
    /// Invalid Boolean.
    #[error("invalid boolean byte")]
    InvalidBoolean,
    /// The requested generalized-index path is unavailable for this SSZ type.
    #[error("unsupported SSZ Merkle path {0}")]
    UnsupportedMerklePath(u64),
    /// Invalid Hex Root.
    #[error("invalid hexadecimal root")]
    InvalidHexRoot,
}

/// SHA-256 without serialization or length mixing.
pub fn sha256(bytes: &[u8]) -> Root {
    Sha256::digest(bytes).into()
}

/// Hash two Merkle nodes.
pub fn hash_pair(left: Root, right: Root) -> Root {
    let mut hash = Sha256::new();
    hash.update(left);
    hash.update(right);
    hash.finalize().into()
}

/// Pack bytes into zero-padded chunks.
pub fn pack_bytes(bytes: &[u8]) -> Vec<Root> {
    bytes
        .chunks(32)
        .map(|bytes| {
            let mut root = [0; 32];
            root[..bytes.len()].copy_from_slice(bytes);
            root
        })
        .collect()
}

/// Merkleize up to the type's leaf limit without allocating virtual zero padding.
pub fn merkleize(chunks: &[Root], limit: usize) -> Result<Root, SszError> {
    if chunks.len() > limit {
        return Err(SszError::LimitExceeded { actual: chunks.len(), limit });
    }
    let capacity = limit.max(1).checked_next_power_of_two().ok_or(SszError::Overflow)?;
    let depth = capacity.trailing_zeros() as usize;
    Ok(merkle_subtree(chunks, depth, merkle_zero_hashes()))
}

fn merkle_subtree(chunks: &[Root], depth: usize, zero_hashes: &[Root]) -> Root {
    if chunks.is_empty() {
        return zero_hashes[depth];
    }
    if depth == 0 {
        return chunks[0];
    }
    let half = 1usize << (depth - 1);
    let split = chunks.len().min(half);
    hash_pair(
        merkle_subtree(&chunks[..split], depth - 1, zero_hashes),
        merkle_subtree(&chunks[split..], depth - 1, zero_hashes),
    )
}

/// Cached roots of empty complete subtrees, covering every representable Merkle depth.
fn merkle_zero_hashes() -> &'static [Root] {
    static ROOTS: OnceLock<[Root; usize::BITS as usize]> = OnceLock::new();
    ROOTS.get_or_init(|| {
        // checked_next_power_of_two permits a maximum depth of usize::BITS - 1.
        // Keep the table fixed and immutable instead of allocating it per validator.
        let mut roots = [[0; 32]; usize::BITS as usize];
        for depth in 1..roots.len() {
            roots[depth] = hash_pair(roots[depth - 1], roots[depth - 1]);
        }
        roots
    })
}

/// Mix an SSZ list's element count into its root.
pub fn mix_in_length(root: Root, length: u64) -> Root {
    hash_pair(root, uint64_root(length))
}

/// Root of an unsigned 64-bit scalar.
pub fn uint64_root(value: u64) -> Root {
    let mut root = [0; 32];
    root[..8].copy_from_slice(&value.to_le_bytes());
    root
}

/// Root of a byte vector or list with a byte capacity.
pub fn bytes_root(bytes: &[u8], limit: usize, list: bool) -> Result<Root, SszError> {
    if bytes.len() > limit || (!list && bytes.len() != limit) {
        return Err(SszError::InvalidLength { expected: limit, actual: bytes.len() });
    }
    let root = merkleize(&pack_bytes(bytes), limit.div_ceil(32))?;
    Ok(if list { mix_in_length(root, bytes.len() as u64) } else { root })
}

/// Gloas progressive Merkle shape: subtrees of 1, 4, 16, 64, ... leaves.
pub fn merkleize_progressive(chunks: &[Root]) -> Result<Root, SszError> {
    progressive_subtree(chunks, 1)
}

fn progressive_subtree(chunks: &[Root], capacity: usize) -> Result<Root, SszError> {
    if chunks.is_empty() {
        return Ok([0; 32]);
    }
    let split = chunks.len().min(capacity);
    let left = merkleize(&chunks[..split], capacity)?;
    let right = if split == chunks.len() {
        [0; 32]
    } else {
        progressive_subtree(&chunks[split..], capacity.checked_mul(4).ok_or(SszError::Overflow)?)?
    };
    Ok(hash_pair(left, right))
}

/// Root of a progressive container with absent-field gaps and active-field mixing.
pub fn progressive_container_root(
    fields: &[Root],
    active_fields: &[bool],
) -> Result<Root, SszError> {
    if active_fields.is_empty() ||
        active_fields.len() > 256 ||
        active_fields.last() != Some(&true) ||
        active_fields.iter().filter(|active| **active).count() != fields.len()
    {
        return Err(SszError::InvalidActiveFields);
    }
    let mut bitmap = [0; 32];
    let mut sparse = Vec::with_capacity(active_fields.len());
    let mut field = fields.iter();
    for (index, active) in active_fields.iter().copied().enumerate() {
        if active {
            bitmap[index / 8] |= 1 << (index % 8);
            sparse.push(*field.next().ok_or(SszError::InvalidActiveFields)?);
        } else {
            sparse.push([0; 32]);
        }
    }
    Ok(hash_pair(merkleize_progressive(&sparse)?, bitmap))
}

/// Decode a canonical boolean.
pub fn read_bool(bytes: &[u8]) -> Result<bool, SszError> {
    match bytes {
        [0] => Ok(false),
        [1] => Ok(true),
        [_] => Err(SszError::InvalidBoolean),
        _ => Err(SszError::InvalidLength { expected: 1, actual: bytes.len() }),
    }
}

/// Decode an exact 8-byte unsigned integer.
pub fn read_u64(bytes: &[u8]) -> Result<u64, SszError> {
    Ok(u64::from_le_bytes(
        bytes
            .try_into()
            .map_err(|_| SszError::InvalidLength { expected: 8, actual: bytes.len() })?,
    ))
}

/// Decode an exact root.
pub fn read_root(bytes: &[u8]) -> Result<Root, SszError> {
    bytes.try_into().map_err(|_| SszError::InvalidLength { expected: 32, actual: bytes.len() })
}

/// Decode container fields with canonical offsets.
///
/// Some(size) is a fixed field and None is a variable field.
pub fn container_fields<'a>(
    bytes: &'a [u8],
    schema: &[Option<usize>],
) -> Result<Vec<&'a [u8]>, SszError> {
    let fixed_size = schema.iter().try_fold(0usize, |sum, size| {
        sum.checked_add(size.unwrap_or(4)).ok_or(SszError::Overflow)
    })?;
    if bytes.len() < fixed_size {
        return Err(SszError::InvalidLength { expected: fixed_size, actual: bytes.len() });
    }
    let mut cursor = 0;
    let mut ranges = Vec::with_capacity(schema.len());
    let mut variable = Vec::new();
    for size in schema {
        if let Some(size) = size {
            ranges.push((cursor, cursor + size));
            cursor += size;
        } else {
            let offset = u32::from_le_bytes(
                bytes[cursor..cursor + 4].try_into().map_err(|_| SszError::InvalidOffset)?,
            ) as usize;
            variable.push((ranges.len(), offset));
            ranges.push((offset, offset));
            cursor += 4;
        }
    }
    if variable.is_empty() {
        if bytes.len() != fixed_size {
            return Err(SszError::InvalidLength { expected: fixed_size, actual: bytes.len() });
        }
    } else {
        if variable[0].1 != fixed_size {
            return Err(SszError::InvalidOffset);
        }
        for (position, (index, start)) in variable.iter().copied().enumerate() {
            let end = variable.get(position + 1).map_or(bytes.len(), |(_, offset)| *offset);
            if start > end || end > bytes.len() {
                return Err(SszError::InvalidOffset);
            }
            ranges[index] = (start, end);
        }
    }
    Ok(ranges.into_iter().map(|(start, end)| &bytes[start..end]).collect())
}

/// Parse exactly 64 hex digits, optionally prefixed.
pub fn root_from_hex(value: &str) -> Result<Root, SszError> {
    let value = value.strip_prefix("0x").unwrap_or(value);
    if value.len() != 64 {
        return Err(SszError::InvalidHexRoot);
    }
    let mut root = [0; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let digit = |byte: u8| match byte {
            b'0'..=b'9' => Some(byte - b'0'),
            b'a'..=b'f' => Some(byte - b'a' + 10),
            b'A'..=b'F' => Some(byte - b'A' + 10),
            _ => None,
        };
        root[index] = digit(pair[0]).ok_or(SszError::InvalidHexRoot)? * 16 +
            digit(pair[1]).ok_or(SszError::InvalidHexRoot)?;
    }
    Ok(root)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merkle_limit_preserves_virtual_padding() {
        let leaf = [7; 32];
        assert_eq!(
            merkleize(&[leaf], 4).unwrap(),
            hash_pair(hash_pair(leaf, [0; 32]), hash_pair([0; 32], [0; 32]))
        );
        assert_eq!(merkleize(&[], 0).unwrap(), [0; 32]);
        assert_eq!(merkleize(&[leaf], 0), Err(SszError::LimitExceeded { actual: 1, limit: 0 }));
    }

    #[test]
    fn huge_virtual_limit_does_not_allocate_it() {
        let depth = (usize::BITS - 1).min(40);
        let zero = merkleize(&[], 1usize << depth).unwrap();
        let mut expected = [0; 32];
        for _ in 0..depth {
            expected = hash_pair(expected, expected);
        }
        assert_eq!(zero, expected);
    }

    #[test]
    fn progressive_boundaries_and_active_fields() {
        let leaves = [[1; 32], [2; 32], [3; 32], [4; 32], [5; 32], [6; 32]];
        assert_eq!(merkleize_progressive(&[]).unwrap(), [0; 32]);
        assert_eq!(merkleize_progressive(&leaves[..1]).unwrap(), hash_pair(leaves[0], [0; 32]));
        assert_eq!(
            merkleize_progressive(&leaves).unwrap(),
            hash_pair(
                leaves[0],
                hash_pair(
                    merkleize(&leaves[1..5], 4).unwrap(),
                    hash_pair(merkleize(&leaves[5..], 16).unwrap(), [0; 32])
                )
            )
        );
        let sparse = [leaves[0], [0; 32], leaves[1]];
        let mut active = [0; 32];
        active[0] = 5;
        assert_eq!(
            progressive_container_root(&leaves[..2], &[true, false, true]).unwrap(),
            hash_pair(merkleize_progressive(&sparse).unwrap(), active)
        );
        assert!(progressive_container_root(&leaves[..1], &[true, false]).is_err());
    }

    #[test]
    fn bitlists_preserve_delimiter_at_byte_boundary() {
        for len in 0..1024usize {
            let mut bytes = vec![0xff; len / 8 + 1];
            bytes[len / 8] = ((1u16 << (len % 8 + 1)) - 1) as u8;
            let bits = BitList::from_ssz(&bytes, 1024).unwrap();
            assert_eq!(bits.len(), len);
            assert_eq!(bits.to_ssz(), bytes);
            assert_eq!(bits.get(len), None);
        }
        assert_eq!(BitList::from_ssz(&[0], 8), Err(SszError::MissingBitlistDelimiter));
        assert!(BitVector::from_ssz(&[0x80], 7).is_err());
    }

    #[test]
    fn bitvector_mutation_preserves_padding_and_rejects_out_of_range_atomically() {
        let mut bits = BitVector::from_ssz(&[0, 0], 10).unwrap();
        let capacity = bits.heap_bytes();
        bits.set(0, true).unwrap();
        bits.set(9, true).unwrap();
        assert_eq!(bits.as_ssz(), [1, 2]);
        let before = bits.clone();
        assert_eq!(bits.set(10, true), Err(SszError::InvalidBitVector));
        assert_eq!(bits.set(usize::MAX, false), Err(SszError::InvalidBitVector));
        assert_eq!(bits, before);
        bits.set(0, false).unwrap();
        assert_eq!(bits.as_ssz(), [0, 2]);
        assert_eq!(bits.heap_bytes(), capacity);
        assert_eq!(BitVector::from_ssz(bits.as_ssz(), 10).unwrap(), bits);
    }

    #[test]
    fn malformed_offsets_fail_closed() {
        let bytes = [1, 0, 8, 0, 0, 0, 8, 0, 0, 0];
        assert_eq!(container_fields(&bytes, &[Some(2), None, None]), Err(SszError::InvalidOffset));
        let bytes = [1, 0, 10, 0, 0, 0, 10, 0, 0, 0];
        assert_eq!(
            container_fields(&bytes, &[Some(2), None, None]).unwrap(),
            vec![&bytes[..2], &bytes[10..], &bytes[10..]]
        );
    }

    #[test]
    fn header_round_trip() {
        let header = BeaconBlockHeader {
            slot: 42,
            proposer_index: 9,
            parent_root: [1; 32],
            state_root: [2; 32],
            body_root: [3; 32],
        };
        assert_eq!(BeaconBlockHeader::from_ssz(&header.to_ssz()).unwrap(), header);
        assert!(BeaconBlockHeader::from_ssz(&header.to_ssz()[..111]).is_err());
    }

    fn reference_pair(left: Root, right: Root) -> Root {
        let mut bytes = [0; 64];
        bytes[..32].copy_from_slice(&left);
        bytes[32..].copy_from_slice(&right);
        Sha256::digest(bytes).into()
    }

    fn padded_reference(chunks: &[Root], limit: usize) -> Root {
        let mut level = chunks.to_vec();
        level.resize(limit.max(1).next_power_of_two(), [0; 32]);
        while level.len() > 1 {
            level = level.chunks_exact(2).map(|pair| reference_pair(pair[0], pair[1])).collect();
        }
        level[0]
    }

    #[test]
    fn cached_merkle_roots_equal_independent_materialized_padding() {
        for limit in 0..=65 {
            for count in 0..=limit {
                let leaves = (0..count)
                    .map(|index| {
                        let mut leaf = [index as u8; 32];
                        leaf[31] = 255 - index as u8;
                        leaf
                    })
                    .collect::<Vec<_>>();
                assert_eq!(
                    merkleize(&leaves, limit).unwrap(),
                    padded_reference(&leaves, limit),
                    "limit={limit}, count={count}"
                );
            }
        }
    }

    #[test]
    fn cached_zero_roots_cover_platform_maximum_depth_and_preserve_overflow_errors() {
        let maximum = 1usize << (usize::BITS - 1);
        let mut zero = [0; 32];
        let mut singleton = [7; 32];
        for depth in 0..usize::BITS {
            assert_eq!(merkleize(&[], 1usize << depth).unwrap(), zero);
            assert_eq!(merkleize(&[[7; 32]], 1usize << depth).unwrap(), singleton);
            if depth + 1 < usize::BITS {
                singleton = reference_pair(singleton, zero);
                zero = reference_pair(zero, zero);
            }
        }
        assert_eq!(merkleize(&[], maximum + 1), Err(SszError::Overflow));
        assert_eq!(merkleize(&[], usize::MAX), Err(SszError::Overflow));
        assert_eq!(
            merkleize(&[[1; 32], [2; 32]], 1),
            Err(SszError::LimitExceeded { actual: 2, limit: 1 })
        );
    }

    #[test]
    fn concurrent_merkle_calls_share_one_immutable_zero_root_table() {
        std::thread::scope(|scope| {
            let tasks = (0..8)
                .map(|worker| {
                    scope.spawn(move || {
                        let leaves =
                            (0..17).map(|index| [worker * 17 + index; 32]).collect::<Vec<_>>();
                        assert_eq!(merkleize(&leaves, 29).unwrap(), padded_reference(&leaves, 29));
                        merkle_zero_hashes()
                    })
                })
                .collect::<Vec<_>>();
            let tables = tasks.into_iter().map(|task| task.join().unwrap()).collect::<Vec<_>>();
            assert!(tables.iter().all(|table| std::ptr::eq(tables[0], *table)));
            assert_eq!(tables[0].len(), usize::BITS as usize);
        });
    }
}
