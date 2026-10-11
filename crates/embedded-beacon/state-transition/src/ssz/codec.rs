//! Canonical SSZ field codecs and preset-bound sequence types.

use reth_embedded_beacon_primitives::{
    bytes_root, container_fields, hash_pair, merkleize, mix_in_length, pack_bytes, read_bool,
    read_u64, uint64_root, BeaconBlockHeader, BitList, BitVector, Checkpoint, Root, SszError,
};
use std::marker::PhantomData;

/// Exact published SSZ preset; synthetic clocks do not change its schema bounds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Preset {
    /// Public-network collection sizes.
    Mainnet,
    /// Reduced published specification-test collection sizes.
    Minimal,
}

/// Canonical complete SSZ value; every encoding and root rechecks collection bounds.
pub trait Ssz: Sized {
    /// Whether every in-memory value has valid SSZ structure under every preset.
    /// Only intrinsically fixed scalar/container types opt in; sequences still check bounds.
    const STRUCTURE_INVARIANT: bool = false;

    /// Whether every value owns no heap allocations, excluding its inline representation.
    /// Opting in guarantees that `heap_bytes()` returns `Ok(0)` for every value.
    /// Fixed serialized width alone does not imply this: vectors and bitfields still allocate.
    /// Unknown implementations keep recursive accounting unless they establish this invariant.
    const HEAP_ALLOCATION_FREE: bool = false;

    /// Construct the declared structural SSZ default, including empty lists and exact vectors.
    /// This creates ordinary data and grants no consensus or genesis authentication.
    fn default_value(preset: Preset) -> Result<Self, SszError> {
        let width = Self::fixed_size(preset).ok_or(SszError::InvalidOffset)?;
        Self::from_ssz(&vec![0; width], preset)
    }
    /// Recursively count owned allocation capacities, excluding this value's inline size.
    /// Allocator bookkeeping and temporary validation buffers are outside this measurement.
    fn heap_bytes(&self) -> Result<usize, SszError>;
    /// Read a committed node at a generalized index. Root-only types fail explicitly for
    /// unavailable nested paths; containers route exact declared field trees recursively.
    fn tree_node(&self, gindex: u64, preset: Preset) -> Result<Root, SszError> {
        if gindex == 1 {
            self.hash_tree_root(preset)
        } else {
            Err(SszError::UnsupportedMerklePath(gindex))
        }
    }
    /// Serialized width when fixed-size, otherwise variable.
    fn fixed_size(preset: Preset) -> Option<usize>;
    /// Packed width for basic scalars, otherwise composite Merkleization.
    fn packed_width() -> Option<usize> {
        None
    }
    /// Decode exact canonical bytes.
    fn from_ssz(bytes: &[u8], preset: Preset) -> Result<Self, SszError>;
    /// Encode exact canonical bytes.
    fn to_ssz(&self, preset: Preset) -> Result<Vec<u8>, SszError>;
    /// Append canonical bytes to an existing buffer without retaining per-element encodings.
    fn write_ssz(&self, preset: Preset, output: &mut Vec<u8>) -> Result<(), SszError> {
        output.extend_from_slice(&self.to_ssz(preset)?);
        Ok(())
    }
    /// Compute the SSZ root under the specified preset.
    fn hash_tree_root(&self, preset: Preset) -> Result<Root, SszError>;
    /// Compute ordered composite leaf roots; each value retains its complete SSZ checks.
    /// The default is serial; only the fixed Validator container uses bounded workers.
    fn hash_sequence_roots(values: &[Self], preset: Preset) -> Result<Vec<Root>, SszError> {
        values.iter().map(|value| value.hash_tree_root(preset)).collect()
    }

    /// Recheck every structural constraint without computing an unused state commitment.
    /// This grants no signature, transition or authenticated-root authority. Unknown custom
    /// implementations retain their complete hash validation through this default fallback.
    fn validate_structure(&self, preset: Preset) -> Result<(), SszError> {
        self.hash_tree_root(preset).map(|_| ())
    }
}

impl Ssz for u64 {
    const STRUCTURE_INVARIANT: bool = true;
    const HEAP_ALLOCATION_FREE: bool = true;

    fn validate_structure(&self, _: Preset) -> Result<(), SszError> {
        Ok(())
    }

    fn heap_bytes(&self) -> Result<usize, SszError> {
        Ok(0)
    }

    fn fixed_size(_: Preset) -> Option<usize> {
        Some(8)
    }
    fn packed_width() -> Option<usize> {
        Some(8)
    }
    fn from_ssz(bytes: &[u8], _: Preset) -> Result<Self, SszError> {
        read_u64(bytes)
    }
    fn to_ssz(&self, _: Preset) -> Result<Vec<u8>, SszError> {
        Ok(self.to_le_bytes().to_vec())
    }
    fn write_ssz(&self, _: Preset, output: &mut Vec<u8>) -> Result<(), SszError> {
        output.extend_from_slice(&self.to_le_bytes());
        Ok(())
    }
    fn hash_tree_root(&self, _: Preset) -> Result<Root, SszError> {
        Ok(uint64_root(*self))
    }
}

impl Ssz for bool {
    const STRUCTURE_INVARIANT: bool = true;
    const HEAP_ALLOCATION_FREE: bool = true;

    fn validate_structure(&self, _: Preset) -> Result<(), SszError> {
        Ok(())
    }

    fn heap_bytes(&self) -> Result<usize, SszError> {
        Ok(0)
    }

    fn fixed_size(_: Preset) -> Option<usize> {
        Some(1)
    }
    fn packed_width() -> Option<usize> {
        Some(1)
    }
    fn from_ssz(bytes: &[u8], _: Preset) -> Result<Self, SszError> {
        read_bool(bytes)
    }
    fn to_ssz(&self, _: Preset) -> Result<Vec<u8>, SszError> {
        Ok(vec![u8::from(*self)])
    }
    fn write_ssz(&self, _: Preset, output: &mut Vec<u8>) -> Result<(), SszError> {
        output.push(u8::from(*self));
        Ok(())
    }
    fn hash_tree_root(&self, _: Preset) -> Result<Root, SszError> {
        Ok(uint64_root(u64::from(*self)))
    }
}

impl<const N: usize> Ssz for [u8; N] {
    const STRUCTURE_INVARIANT: bool = N <= 64;
    const HEAP_ALLOCATION_FREE: bool = true;

    fn validate_structure(&self, _: Preset) -> Result<(), SszError> {
        merkle_structure(N.div_ceil(32))
    }

    fn tree_node(&self, gindex: u64, _: Preset) -> Result<Root, SszError> {
        sparse_chunk_node(&pack_bytes(self), N.div_ceil(32), gindex)
    }
    fn heap_bytes(&self) -> Result<usize, SszError> {
        Ok(0)
    }

    fn fixed_size(_: Preset) -> Option<usize> {
        Some(N)
    }
    fn from_ssz(bytes: &[u8], _: Preset) -> Result<Self, SszError> {
        bytes.try_into().map_err(|_| SszError::InvalidLength { expected: N, actual: bytes.len() })
    }
    fn to_ssz(&self, _: Preset) -> Result<Vec<u8>, SszError> {
        Ok(self.to_vec())
    }
    fn write_ssz(&self, _: Preset, output: &mut Vec<u8>) -> Result<(), SszError> {
        output.extend_from_slice(self);
        Ok(())
    }
    fn hash_tree_root(&self, _: Preset) -> Result<Root, SszError> {
        // Validator public keys and withdrawal credentials are fixed one/two-chunk vectors.
        // Construct their exact SSZ chunks on the stack instead of allocating per validator.
        if N <= 32 {
            let mut root = [0; 32];
            root[..N].copy_from_slice(self);
            Ok(root)
        } else if N <= 64 {
            let mut left = [0; 32];
            let mut right = [0; 32];
            left.copy_from_slice(&self[..32]);
            right[..N - 32].copy_from_slice(&self[32..]);
            Ok(hash_pair(left, right))
        } else {
            bytes_root(self, N, false)
        }
    }
}

impl Ssz for BeaconBlockHeader {
    const STRUCTURE_INVARIANT: bool = true;
    const HEAP_ALLOCATION_FREE: bool = true;

    fn validate_structure(&self, _: Preset) -> Result<(), SszError> {
        Ok(())
    }

    fn heap_bytes(&self) -> Result<usize, SszError> {
        Ok(0)
    }

    fn fixed_size(_: Preset) -> Option<usize> {
        Some(112)
    }
    fn from_ssz(bytes: &[u8], _: Preset) -> Result<Self, SszError> {
        Self::from_ssz(bytes)
    }
    fn to_ssz(&self, _: Preset) -> Result<Vec<u8>, SszError> {
        Ok((*self).to_ssz().to_vec())
    }
    fn hash_tree_root(&self, _: Preset) -> Result<Root, SszError> {
        Ok((*self).hash_tree_root())
    }
}

impl Ssz for Checkpoint {
    const STRUCTURE_INVARIANT: bool = true;
    const HEAP_ALLOCATION_FREE: bool = true;

    fn validate_structure(&self, _: Preset) -> Result<(), SszError> {
        Ok(())
    }

    fn tree_node(&self, gindex: u64, preset: Preset) -> Result<Root, SszError> {
        match gindex {
            1 => Ssz::hash_tree_root(self, preset),
            2 => Ok(uint64_root(self.epoch)),
            3 => Ok(self.root),
            _ => Err(SszError::UnsupportedMerklePath(gindex)),
        }
    }
    fn heap_bytes(&self) -> Result<usize, SszError> {
        Ok(0)
    }

    fn fixed_size(_: Preset) -> Option<usize> {
        Some(40)
    }
    fn from_ssz(bytes: &[u8], preset: Preset) -> Result<Self, SszError> {
        let fields = container_fields(bytes, &[Some(8), Some(32)])?;
        Ok(Self {
            epoch: u64::from_ssz(fields[0], preset)?,
            root: Root::from_ssz(fields[1], preset)?,
        })
    }
    fn to_ssz(&self, preset: Preset) -> Result<Vec<u8>, SszError> {
        encode_container(&[
            (Some(8), self.epoch.to_ssz(preset)?),
            (Some(32), self.root.to_ssz(preset)?),
        ])
    }
    fn hash_tree_root(&self, _: Preset) -> Result<Root, SszError> {
        merkleize(&[uint64_root(self.epoch), self.root], 2)
    }
}

/// Collection bound determined by its SSZ type and preset.
pub trait Length {
    /// Exact vector length or maximum list length.
    fn length(preset: Preset) -> Result<usize, SszError>;
}

/// Preset-independent SSZ sequence bound.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Fixed<const N: u64>;
impl<const N: u64> Length for Fixed<N> {
    fn length(_: Preset) -> Result<usize, SszError> {
        usize::try_from(N).map_err(|_| SszError::Overflow)
    }
}

/// Two exact published preset lengths.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PresetLength<const MAINNET: u64, const MINIMAL: u64>;
impl<const M: u64, const N: u64> Length for PresetLength<M, N> {
    fn length(preset: Preset) -> Result<usize, SszError> {
        usize::try_from(match preset {
            Preset::Mainnet => M,
            Preset::Minimal => N,
        })
        .map_err(|_| SszError::Overflow)
    }
}

/// Typed bounded SSZ list; edited values are checked again before publication.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct List<T, L> {
    /// Elements in serialized order.
    pub values: Vec<T>,
    marker: PhantomData<L>,
}

impl<T, L> List<T, L> {
    /// Construct editable typed data; encoding and hashing enforce the declared bound.
    pub const fn from_vec(values: Vec<T>) -> Self {
        Self { values, marker: PhantomData }
    }
}

impl<T, L> Default for List<T, L> {
    fn default() -> Self {
        Self::from_vec(Vec::new())
    }
}

impl<T: Ssz, L: Length> Ssz for List<T, L> {
    fn validate_structure(&self, preset: Preset) -> Result<(), SszError> {
        let limit = L::length(preset)?;
        check_count(self.values.len(), limit, false)?;
        sequence_structure(&self.values, limit, preset)
    }

    fn tree_node(&self, gindex: u64, preset: Preset) -> Result<Root, SszError> {
        let limit = L::length(preset)?;
        check_count(self.values.len(), limit, false)?;
        if gindex == 0 {
            return Err(SszError::UnsupportedMerklePath(gindex));
        }
        if gindex == 1 {
            return self.hash_tree_root(preset);
        }
        let depth = 63 - gindex.leading_zeros();
        let leading = 1u64 << (depth - 1);
        if gindex & leading != 0 {
            return if gindex == 3 {
                Ok(uint64_root(u64::try_from(self.values.len()).map_err(|_| SszError::Overflow)?))
            } else {
                Err(SszError::UnsupportedMerklePath(gindex))
            };
        }
        sequence_tree_node(&self.values, limit, leading | (gindex & (leading - 1)), preset)
            .map_err(|error| merkle_path_error(error, gindex))
    }
    fn default_value(_: Preset) -> Result<Self, SszError> {
        Ok(Self::default())
    }
    fn heap_bytes(&self) -> Result<usize, SszError> {
        sequence_heap_bytes(&self.values)
    }

    fn fixed_size(_: Preset) -> Option<usize> {
        None
    }
    fn from_ssz(bytes: &[u8], preset: Preset) -> Result<Self, SszError> {
        Ok(Self::from_vec(decode_sequence(bytes, preset, L::length(preset)?, false)?))
    }
    fn to_ssz(&self, preset: Preset) -> Result<Vec<u8>, SszError> {
        check_count(self.values.len(), L::length(preset)?, false)?;
        encode_sequence(&self.values, preset)
    }
    fn hash_tree_root(&self, preset: Preset) -> Result<Root, SszError> {
        let limit = L::length(preset)?;
        check_count(self.values.len(), limit, false)?;
        Ok(mix_in_length(sequence_root(&self.values, limit, preset)?, self.values.len() as u64))
    }
}

/// Typed fixed-count SSZ vector with an explicit published preset length.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Vector<T, L> {
    /// Exact ordered vector contents; mutation is rechecked before publication.
    pub values: Vec<T>,
    marker: PhantomData<L>,
}

impl<T, L> Vector<T, L> {
    /// Construct editable typed data; encoding and hashing require the exact vector length.
    pub const fn from_vec(values: Vec<T>) -> Self {
        Self { values, marker: PhantomData }
    }
}

impl<T: Ssz, L: Length> Ssz for Vector<T, L> {
    fn validate_structure(&self, preset: Preset) -> Result<(), SszError> {
        let count = L::length(preset)?;
        check_count(self.values.len(), count, true)?;
        sequence_structure(&self.values, count, preset)
    }

    fn tree_node(&self, gindex: u64, preset: Preset) -> Result<Root, SszError> {
        let count = L::length(preset)?;
        check_count(self.values.len(), count, true)?;
        sequence_tree_node(&self.values, count, gindex, preset)
    }
    fn default_value(preset: Preset) -> Result<Self, SszError> {
        (0..L::length(preset)?)
            .map(|_| T::default_value(preset))
            .collect::<Result<Vec<_>, _>>()
            .map(Self::from_vec)
    }
    fn heap_bytes(&self) -> Result<usize, SszError> {
        sequence_heap_bytes(&self.values)
    }

    fn fixed_size(preset: Preset) -> Option<usize> {
        T::fixed_size(preset)?.checked_mul(L::length(preset).ok()?)
    }
    fn from_ssz(bytes: &[u8], preset: Preset) -> Result<Self, SszError> {
        Ok(Self::from_vec(decode_sequence(bytes, preset, L::length(preset)?, true)?))
    }
    fn to_ssz(&self, preset: Preset) -> Result<Vec<u8>, SszError> {
        check_count(self.values.len(), L::length(preset)?, true)?;
        encode_sequence(&self.values, preset)
    }
    fn hash_tree_root(&self, preset: Preset) -> Result<Root, SszError> {
        let count = L::length(preset)?;
        check_count(self.values.len(), count, true)?;
        sequence_root(&self.values, count, preset)
    }
}

/// Fixed-size SSZ bitvector.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Bits<const N: usize> {
    /// Checked packed bits including the exact logical length.
    pub bits: BitVector,
}

impl<const N: usize> Ssz for Bits<N> {
    fn heap_bytes(&self) -> Result<usize, SszError> {
        Ok(self.bits.heap_bytes())
    }

    fn fixed_size(_: Preset) -> Option<usize> {
        Some(N.div_ceil(8))
    }
    fn from_ssz(bytes: &[u8], _: Preset) -> Result<Self, SszError> {
        Ok(Self { bits: BitVector::from_ssz(bytes, N)? })
    }
    fn to_ssz(&self, _: Preset) -> Result<Vec<u8>, SszError> {
        if self.bits.len() != N {
            return Err(SszError::InvalidBitVector)
        }
        Ok(self.bits.as_ssz().to_vec())
    }
    fn hash_tree_root(&self, preset: Preset) -> Result<Root, SszError> {
        self.to_ssz(preset)?;
        self.bits.hash_tree_root()
    }
}

/// Bounded SSZ bitlist.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BitListValue<const N: usize> {
    /// Checked packed bits without the serialization delimiter.
    pub bits: BitList,
}

impl<const N: usize> Ssz for BitListValue<N> {
    fn default_value(preset: Preset) -> Result<Self, SszError> {
        Self::from_ssz(&[1], preset)
    }
    fn heap_bytes(&self) -> Result<usize, SszError> {
        Ok(self.bits.heap_bytes())
    }

    fn fixed_size(_: Preset) -> Option<usize> {
        None
    }
    fn from_ssz(bytes: &[u8], _: Preset) -> Result<Self, SszError> {
        Ok(Self { bits: BitList::from_ssz(bytes, N)? })
    }
    fn to_ssz(&self, _: Preset) -> Result<Vec<u8>, SszError> {
        check_count(self.bits.len(), N, false)?;
        Ok(self.bits.to_ssz())
    }
    fn hash_tree_root(&self, _: Preset) -> Result<Root, SszError> {
        self.bits.hash_tree_root(N)
    }
}

fn sequence_heap_bytes<T: Ssz>(values: &Vec<T>) -> Result<usize, SszError> {
    let allocated =
        values.capacity().checked_mul(std::mem::size_of::<T>()).ok_or(SszError::Overflow)?;
    if T::HEAP_ALLOCATION_FREE {
        return Ok(allocated)
    }
    values.iter().try_fold(allocated, |total, value| {
        total.checked_add(value.heap_bytes()?).ok_or(SszError::Overflow)
    })
}

const fn check_count(actual: usize, limit: usize, vector: bool) -> Result<(), SszError> {
    if actual > limit {
        return Err(SszError::LimitExceeded { actual, limit })
    }
    if vector && actual != limit {
        return Err(SszError::InvalidLength { expected: limit, actual })
    }
    Ok(())
}

fn decode_sequence<T: Ssz>(
    bytes: &[u8],
    preset: Preset,
    limit: usize,
    vector: bool,
) -> Result<Vec<T>, SszError> {
    if let Some(width) = T::fixed_size(preset) {
        if width == 0 || !bytes.len().is_multiple_of(width) {
            return Err(SszError::InvalidLength { expected: width, actual: bytes.len() })
        }
        let count = bytes.len() / width;
        check_count(count, limit, vector)?;
        // Fallible collection need not preserve the exact iterator length for allocation.
        // Reserve the already checked encoded count once, rather than geometrically growing
        // million-entry registries and charging their unused capacity to the decoded cache.
        let mut values = Vec::with_capacity(count);
        for record in bytes.chunks_exact(width) {
            values.push(T::from_ssz(record, preset)?);
        }
        return Ok(values)
    }
    if bytes.is_empty() {
        check_count(0, limit, vector)?;
        return Ok(Vec::new())
    }
    let first = bytes.get(..4).ok_or(SszError::InvalidOffset)?;
    let offset =
        u32::from_le_bytes(first.try_into().map_err(|_| SszError::InvalidOffset)?) as usize;
    if offset == 0 || !offset.is_multiple_of(4) || offset > bytes.len() {
        return Err(SszError::InvalidOffset)
    }
    let count = offset / 4;
    check_count(count, limit, vector)?;
    container_fields(bytes, &vec![None; count])?
        .into_iter()
        .map(|record| T::from_ssz(record, preset))
        .collect()
}

fn encode_sequence<T: Ssz>(values: &[T], preset: Preset) -> Result<Vec<u8>, SszError> {
    let width = T::fixed_size(preset);
    if let Some(width) = width {
        let total = values.len().checked_mul(width).ok_or(SszError::Overflow)?;
        u32::try_from(total).map_err(|_| SszError::Overflow)?;
        let mut bytes = Vec::with_capacity(total);
        for value in values {
            let start = bytes.len();
            value.write_ssz(preset, &mut bytes)?;
            let actual = bytes.len().checked_sub(start).ok_or(SszError::Overflow)?;
            if actual != width {
                return Err(SszError::InvalidLength { expected: width, actual })
            }
        }
        return Ok(bytes)
    }
    let fields = values
        .iter()
        .map(|value| Ok((width, value.to_ssz(preset)?)))
        .collect::<Result<Vec<_>, SszError>>()?;
    encode_container(&fields)
}

fn sequence_root<T: Ssz>(values: &[T], limit: usize, preset: Preset) -> Result<Root, SszError> {
    if let Some(width) = T::packed_width() {
        let bytes = encode_sequence(values, preset)?;
        let max_bytes = limit.checked_mul(width).ok_or(SszError::Overflow)?;
        merkleize(&pack_bytes(&bytes), max_bytes.div_ceil(32))
    } else {
        let roots = T::hash_sequence_roots(values, preset)?;
        merkleize(&roots, limit)
    }
}

// Generalized indices are relative to the public object queried, not its nested child.
pub(crate) const fn merkle_path_error(error: SszError, gindex: u64) -> SszError {
    match error {
        SszError::UnsupportedMerklePath(_) => SszError::UnsupportedMerklePath(gindex),
        error => error,
    }
}

// Compute only the requested subtree; declared bounds can exceed the materialized values.
// Missing leaves use the exact SSZ zero subtree without allocating the declared capacity.
fn sparse_chunk_node(chunks: &[Root], limit: usize, gindex: u64) -> Result<Root, SszError> {
    check_count(chunks.len(), limit, false)?;
    if gindex == 0 {
        return Err(SszError::UnsupportedMerklePath(gindex));
    }
    let capacity = limit.max(1).checked_next_power_of_two().ok_or(SszError::Overflow)?;
    let height = capacity.trailing_zeros();
    let depth = 63 - gindex.leading_zeros();
    if depth > height {
        return Err(SszError::UnsupportedMerklePath(gindex));
    }
    let width = capacity >> depth;
    let offset = usize::try_from(gindex - (1u64 << depth))
        .map_err(|_| SszError::Overflow)?
        .checked_mul(width)
        .ok_or(SszError::Overflow)?;
    let materialized = if offset < chunks.len() {
        &chunks[offset..offset + width.min(chunks.len() - offset)]
    } else {
        &[]
    };
    merkleize(materialized, width)
}

fn sequence_tree_node<T: Ssz>(
    values: &[T],
    limit: usize,
    gindex: u64,
    preset: Preset,
) -> Result<Root, SszError> {
    check_count(values.len(), limit, false)?;
    if gindex == 0 {
        return Err(SszError::UnsupportedMerklePath(gindex));
    }
    if let Some(width) = T::packed_width() {
        let bytes = encode_sequence(values, preset)?;
        let chunk_limit = limit.checked_mul(width).ok_or(SszError::Overflow)?.div_ceil(32);
        return sparse_chunk_node(&pack_bytes(&bytes), chunk_limit, gindex);
    }
    let capacity = limit.max(1).checked_next_power_of_two().ok_or(SszError::Overflow)?;
    let height = capacity.trailing_zeros();
    let depth = 63 - gindex.leading_zeros();
    if depth > height {
        let remaining = depth - height;
        let index = usize::try_from((gindex >> remaining) - (1u64 << height))
            .map_err(|_| SszError::Overflow)?;
        let value = values.get(index).ok_or(SszError::UnsupportedMerklePath(gindex))?;
        let leading = 1u64 << remaining;
        return value
            .tree_node(leading | (gindex & (leading - 1)), preset)
            .map_err(|error| merkle_path_error(error, gindex));
    }
    let roots =
        values.iter().map(|value| value.hash_tree_root(preset)).collect::<Result<Vec<_>, _>>()?;
    sparse_chunk_node(&roots, limit, gindex)
}

pub(crate) fn encode_container(fields: &[(Option<usize>, Vec<u8>)]) -> Result<Vec<u8>, SszError> {
    let fixed = fields.iter().try_fold(0usize, |n, (width, _)| {
        n.checked_add(width.unwrap_or(4)).ok_or(SszError::Overflow)
    })?;
    let total = fields
        .iter()
        .filter(|(width, _)| width.is_none())
        .try_fold(fixed, |n, (_, bytes)| n.checked_add(bytes.len()).ok_or(SszError::Overflow))?;
    u32::try_from(total).map_err(|_| SszError::Overflow)?;
    let mut bytes = Vec::with_capacity(total);
    let mut offset = fixed;
    for (width, field) in fields {
        if let Some(width) = width {
            if field.len() != *width {
                return Err(SszError::InvalidLength { expected: *width, actual: field.len() })
            }
            bytes.extend_from_slice(field);
        } else {
            bytes.extend_from_slice(
                &u32::try_from(offset).map_err(|_| SszError::Overflow)?.to_le_bytes(),
            );
            offset = offset.checked_add(field.len()).ok_or(SszError::Overflow)?;
        }
    }
    for (_, field) in fields.iter().filter(|(width, _)| width.is_none()) {
        bytes.extend_from_slice(field)
    }
    Ok(bytes)
}

impl Ssz for u8 {
    const STRUCTURE_INVARIANT: bool = true;
    const HEAP_ALLOCATION_FREE: bool = true;

    fn validate_structure(&self, _: Preset) -> Result<(), SszError> {
        Ok(())
    }

    fn heap_bytes(&self) -> Result<usize, SszError> {
        Ok(0)
    }

    fn fixed_size(_: Preset) -> Option<usize> {
        Some(1)
    }
    fn packed_width() -> Option<usize> {
        Some(1)
    }
    fn from_ssz(bytes: &[u8], _: Preset) -> Result<Self, SszError> {
        match bytes {
            [value] => Ok(*value),
            _ => Err(SszError::InvalidLength { expected: 1, actual: bytes.len() }),
        }
    }
    fn to_ssz(&self, _: Preset) -> Result<Vec<u8>, SszError> {
        Ok(vec![*self])
    }
    fn write_ssz(&self, _: Preset, output: &mut Vec<u8>) -> Result<(), SszError> {
        output.push(*self);
        Ok(())
    }
    fn hash_tree_root(&self, _: Preset) -> Result<Root, SszError> {
        Ok(uint64_root(u64::from(*self)))
    }
}

/// Preset-dependent fixed-size bitvector.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PresetBits<L> {
    /// Checked logical bits.
    pub bits: BitVector,
    marker: PhantomData<L>,
}

impl<L: Length> Ssz for PresetBits<L> {
    fn heap_bytes(&self) -> Result<usize, SszError> {
        Ok(self.bits.heap_bytes())
    }

    fn fixed_size(preset: Preset) -> Option<usize> {
        Some(L::length(preset).ok()?.div_ceil(8))
    }
    fn from_ssz(bytes: &[u8], preset: Preset) -> Result<Self, SszError> {
        Ok(Self { bits: BitVector::from_ssz(bytes, L::length(preset)?)?, marker: PhantomData })
    }
    fn to_ssz(&self, preset: Preset) -> Result<Vec<u8>, SszError> {
        if self.bits.len() != L::length(preset)? {
            return Err(SszError::InvalidBitVector)
        }
        Ok(self.bits.as_ssz().to_vec())
    }
    fn hash_tree_root(&self, preset: Preset) -> Result<Root, SszError> {
        self.to_ssz(preset)?;
        self.bits.hash_tree_root()
    }
}

/// Preset-dependent bounded bitlist.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PresetBitList<L> {
    /// Checked logical bits.
    pub bits: BitList,
    marker: PhantomData<L>,
}

impl<L: Length> Ssz for PresetBitList<L> {
    fn default_value(preset: Preset) -> Result<Self, SszError> {
        Self::from_ssz(&[1], preset)
    }
    fn heap_bytes(&self) -> Result<usize, SszError> {
        Ok(self.bits.heap_bytes())
    }

    fn fixed_size(_: Preset) -> Option<usize> {
        None
    }
    fn from_ssz(bytes: &[u8], preset: Preset) -> Result<Self, SszError> {
        Ok(Self { bits: BitList::from_ssz(bytes, L::length(preset)?)?, marker: PhantomData })
    }
    fn to_ssz(&self, preset: Preset) -> Result<Vec<u8>, SszError> {
        check_count(self.bits.len(), L::length(preset)?, false)?;
        Ok(self.bits.to_ssz())
    }
    fn hash_tree_root(&self, preset: Preset) -> Result<Root, SszError> {
        self.bits.hash_tree_root(L::length(preset)?)
    }
}

/// Progressive bitlist: no legacy SSZ capacity is mixed into the tree shape.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProgressiveBitList {
    /// Checked logical bits.
    pub bits: BitList,
}

impl Ssz for ProgressiveBitList {
    fn default_value(preset: Preset) -> Result<Self, SszError> {
        Self::from_ssz(&[1], preset)
    }
    fn heap_bytes(&self) -> Result<usize, SszError> {
        Ok(self.bits.heap_bytes())
    }

    fn fixed_size(_: Preset) -> Option<usize> {
        None
    }
    fn from_ssz(bytes: &[u8], _: Preset) -> Result<Self, SszError> {
        Ok(Self { bits: BitList::from_ssz(bytes, usize::MAX)? })
    }
    fn to_ssz(&self, _: Preset) -> Result<Vec<u8>, SszError> {
        Ok(self.bits.to_ssz())
    }
    fn hash_tree_root(&self, _: Preset) -> Result<Root, SszError> {
        self.bits.progressive_root()
    }
}

/// Progressive SSZ list; consensus operation limits remain contextual transition checks.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProgressiveList<T> {
    /// Elements in serialized order.
    pub values: Vec<T>,
}

impl<T> ProgressiveList<T> {
    /// Construct editable complete typed data.
    pub const fn from_vec(values: Vec<T>) -> Self {
        Self { values }
    }
}

impl<T> Default for ProgressiveList<T> {
    fn default() -> Self {
        Self::from_vec(Vec::new())
    }
}

impl<T: Ssz> Ssz for ProgressiveList<T> {
    fn validate_structure(&self, preset: Preset) -> Result<(), SszError> {
        let chunks = if T::packed_width().is_some() {
            packed_sequence_bytes(&self.values, preset)?.div_ceil(32)
        } else {
            validate_sequence_elements(&self.values, preset)?;
            self.values.len()
        };
        progressive_structure(chunks)
    }

    fn default_value(_: Preset) -> Result<Self, SszError> {
        Ok(Self::default())
    }
    fn heap_bytes(&self) -> Result<usize, SszError> {
        sequence_heap_bytes(&self.values)
    }

    fn fixed_size(_: Preset) -> Option<usize> {
        None
    }
    fn from_ssz(bytes: &[u8], preset: Preset) -> Result<Self, SszError> {
        Ok(Self::from_vec(decode_sequence(bytes, preset, usize::MAX, false)?))
    }
    fn to_ssz(&self, preset: Preset) -> Result<Vec<u8>, SszError> {
        encode_sequence(&self.values, preset)
    }
    fn hash_tree_root(&self, preset: Preset) -> Result<Root, SszError> {
        let chunks = if T::packed_width().is_some() {
            pack_bytes(&encode_sequence(&self.values, preset)?)
        } else {
            T::hash_sequence_roots(&self.values, preset)?
        };
        Ok(mix_in_length(
            reth_embedded_beacon_primitives::merkleize_progressive(&chunks)?,
            self.values.len() as u64,
        ))
    }
}

pub(crate) fn container_root(fields: &[Root], progressive: bool) -> Result<Root, SszError> {
    if progressive {
        reth_embedded_beacon_primitives::progressive_container_root(
            fields,
            &vec![true; fields.len()],
        )
    } else {
        merkleize(fields, fields.len())
    }
}

// These checks mirror root construction's error paths without materializing leaf roots.
// Fixed Validator fields are structurally invariant, so a registry needs only its list bound.
fn validate_sequence_elements<T: Ssz>(values: &[T], preset: Preset) -> Result<(), SszError> {
    if !T::STRUCTURE_INVARIANT {
        for value in values {
            value.validate_structure(preset)?;
        }
    }
    Ok(())
}

fn packed_sequence_bytes<T: Ssz>(values: &[T], preset: Preset) -> Result<usize, SszError> {
    if T::STRUCTURE_INVARIANT {
        let width = T::fixed_size(preset).ok_or(SszError::InvalidOffset)?;
        let bytes = values.len().checked_mul(width).ok_or(SszError::Overflow)?;
        u32::try_from(bytes).map_err(|_| SszError::Overflow)?;
        Ok(bytes)
    } else {
        // A custom packed implementation may reject encoding or have an incorrect width.
        // Preserve those checks rather than inferring validity from its declared layout.
        Ok(encode_sequence(values, preset)?.len())
    }
}

fn sequence_structure<T: Ssz>(values: &[T], limit: usize, preset: Preset) -> Result<(), SszError> {
    if let Some(width) = T::packed_width() {
        let bytes = packed_sequence_bytes(values, preset)?;
        let chunks = limit.checked_mul(width).ok_or(SszError::Overflow)?.div_ceil(32);
        check_count(bytes.div_ceil(32), chunks, false)?;
        merkle_structure(chunks)
    } else {
        validate_sequence_elements(values, preset)?;
        merkle_structure(limit)
    }
}

fn merkle_structure(limit: usize) -> Result<(), SszError> {
    limit.max(1).checked_next_power_of_two().ok_or(SszError::Overflow)?;
    Ok(())
}

fn progressive_structure(mut chunks: usize) -> Result<(), SszError> {
    let mut capacity = 1usize;
    while chunks != 0 {
        merkle_structure(capacity)?;
        chunks -= chunks.min(capacity);
        if chunks != 0 {
            capacity = capacity.checked_mul(4).ok_or(SszError::Overflow)?;
        }
    }
    Ok(())
}

pub(crate) fn container_structure(fields: usize, progressive: bool) -> Result<(), SszError> {
    if progressive {
        if fields == 0 || fields > 256 {
            return Err(SszError::InvalidActiveFields)
        }
        progressive_structure(fields)
    } else {
        merkle_structure(fields)
    }
}

/// Internal exact container-node traversal; opaque collection descendants fail closed.
#[derive(Debug)]
pub(crate) enum ContainerPath {
    Root(Root),
    Field { index: usize, gindex: u64 },
}
#[derive(Debug)]
struct ContainerTree {
    root: Root,
    kind: ContainerTreeKind,
}
#[derive(Debug)]
enum ContainerTreeKind {
    Opaque,
    Field(usize),
    Branch(Box<ContainerTree>, Box<ContainerTree>),
}
impl ContainerTree {
    const fn opaque(root: Root) -> Self {
        Self { root, kind: ContainerTreeKind::Opaque }
    }
    fn branch(left: Self, right: Self) -> Self {
        Self {
            root: reth_embedded_beacon_primitives::hash_pair(left.root, right.root),
            kind: ContainerTreeKind::Branch(Box::new(left), Box::new(right)),
        }
    }
}
fn regular_field_tree(fields: &[Root], capacity: usize, offset: usize) -> ContainerTree {
    if capacity == 1 {
        return fields.first().map_or_else(
            || ContainerTree::opaque([0; 32]),
            |root| ContainerTree { root: *root, kind: ContainerTreeKind::Field(offset) },
        );
    }
    let half = capacity / 2;
    let split = fields.len().min(half);
    ContainerTree::branch(
        regular_field_tree(&fields[..split], half, offset),
        regular_field_tree(&fields[split..], half, offset + half),
    )
}
fn progressive_field_tree(
    fields: &[Root],
    capacity: usize,
    offset: usize,
) -> Result<ContainerTree, SszError> {
    if fields.is_empty() {
        return Ok(ContainerTree::opaque([0; 32]));
    }
    let split = fields.len().min(capacity);
    Ok(ContainerTree::branch(
        regular_field_tree(&fields[..split], capacity, offset),
        progressive_field_tree(
            &fields[split..],
            capacity.checked_mul(4).ok_or(SszError::Overflow)?,
            offset + split,
        )?,
    ))
}
pub(crate) fn container_tree_path(
    fields: &[Root],
    progressive: bool,
    gindex: u64,
) -> Result<ContainerPath, SszError> {
    if gindex == 0 || fields.is_empty() {
        return Err(SszError::UnsupportedMerklePath(gindex));
    }
    let tree = if progressive {
        if fields.len() > 256 {
            return Err(SszError::InvalidActiveFields);
        }
        let mut bitmap = [0; 32];
        for index in 0..fields.len() {
            bitmap[index / 8] |= 1 << (index % 8);
        }
        ContainerTree::branch(progressive_field_tree(fields, 1, 0)?, ContainerTree::opaque(bitmap))
    } else {
        regular_field_tree(
            fields,
            fields.len().checked_next_power_of_two().ok_or(SszError::Overflow)?,
            0,
        )
    };
    let mut node = &tree;
    let mut remaining = 63 - gindex.leading_zeros();
    while remaining > 0 {
        match &node.kind {
            ContainerTreeKind::Branch(left, right) => {
                remaining -= 1;
                node = if (gindex >> remaining) & 1 == 0 { left } else { right };
            }
            ContainerTreeKind::Field(index) => {
                let leading = 1u64 << remaining;
                return Ok(ContainerPath::Field {
                    index: *index,
                    gindex: leading | (gindex & (leading - 1)),
                });
            }
            ContainerTreeKind::Opaque => return Err(SszError::UnsupportedMerklePath(gindex)),
        }
    }
    Ok(ContainerPath::Root(node.root))
}
macro_rules! ssz_container {
    (@progressive $(#[$doc:meta])* $name:ident { $($(#[$field_doc:meta])* $field:ident: $ty:ty),* $(,)? }) => {
        $crate::ssz::codec::ssz_container!(@build true; $(#[$doc])* $name { $($(#[$field_doc])* $field: $ty),* });
    };
    ($(#[$doc:meta])* $name:ident { $($(#[$field_doc:meta])* $field:ident: $ty:ty),* $(,)? }) => {
        $crate::ssz::codec::ssz_container!(@build false; $(#[$doc])* $name { $($(#[$field_doc])* $field: $ty),* });
    };
    (@sequence_roots Validator) => {
        fn hash_sequence_roots(values: &[Self], preset: Preset) -> Result<Vec<Root>, SszError> {
            $crate::ssz::parallel_roots::validator_roots(values, preset)
        }
    };
    (@sequence_roots $name:ident) => {};
    (@build $progressive:literal; $(#[$doc:meta])* $name:ident { $($(#[$field_doc:meta])* $field:ident: $ty:ty),* $(,)? }) => {
        $(#[$doc])*
        #[derive(Clone, Debug, PartialEq, Eq)]
        pub struct $name { $($(#[$field_doc])* pub $field: $ty),* }
        impl $name {
            /// Decode all fields with canonical offsets and preset-specific bounds.
            pub fn from_ssz(bytes: &[u8], preset: Preset) -> Result<Self, SszError> { <Self as Ssz>::from_ssz(bytes, preset) }
            /// Encode all fields, enforcing their SSZ type bounds.
            pub fn to_ssz(&self, preset: Preset) -> Result<Vec<u8>, SszError> { <Self as Ssz>::to_ssz(self, preset) }
            /// Compute the complete SSZ root with its declared sequence bounds.
            pub fn hash_tree_root(&self, preset: Preset) -> Result<Root, SszError> { <Self as Ssz>::hash_tree_root(self, preset) }
            /// Ordered field widths used by canonical SSZ offsets; `None` marks a variable field.
            /// This structural metadata grants no transition, signature or network authority.
            pub fn container_field_layout(preset: Preset) -> Vec<Option<usize>> {
                vec![$ (<$ty as Ssz>::fixed_size(preset)),*]
            }
        }
        impl $crate::ssz::json::JsonSsz for $name {
            fn write_json(&self, preset: Preset, output: &mut $crate::ssz::json::JsonEncoder) -> Result<(), $crate::ssz::json::JsonError> {
                output.raw(b"{")?;
                let mut first = true;
                $(output.field(stringify!($field), &mut first)?;
                  <$ty as $crate::ssz::json::JsonSsz>::write_json_field(&self.$field, stringify!($field), preset, output)?;)*
                output.raw(b"}")
            }
        }
        impl Ssz for $name {
            const HEAP_ALLOCATION_FREE: bool = true $(&& <$ty as Ssz>::HEAP_ALLOCATION_FREE)*;

            fn default_value(preset: Preset) -> Result<Self, SszError> {
                Ok(Self { $ ($field: <$ty as Ssz>::default_value(preset)?),* })
            }
            fn tree_node(&self, gindex: u64, preset: Preset) -> Result<Root, SszError> {
                let roots = [$ (<$ty as Ssz>::hash_tree_root(&self.$field, preset)?),*];
                match $crate::ssz::codec::container_tree_path(&roots, $progressive, gindex)? {
                    $crate::ssz::codec::ContainerPath::Root(root) => Ok(root),
                    $crate::ssz::codec::ContainerPath::Field { index, gindex: child } => {
                        let mut field_index = 0usize;
                        $(if index == field_index {
                            return <$ty as Ssz>::tree_node(&self.$field, child, preset)
                                .map_err(|error| $crate::ssz::codec::merkle_path_error(error, gindex));
                        }
                          field_index += 1;)*
                        let _ = field_index;
                        Err(SszError::UnsupportedMerklePath(gindex))
                    }
                }
            }
            fn heap_bytes(&self) -> Result<usize, SszError> {
                let mut bytes = 0usize;
                $(bytes = bytes.checked_add(<$ty as Ssz>::heap_bytes(&self.$field)?).ok_or(SszError::Overflow)?;)*
                Ok(bytes)
            }
            fn fixed_size(preset: Preset) -> Option<usize> {
                [$ (<$ty as Ssz>::fixed_size(preset)),*].into_iter().try_fold(0usize, |size, width| size.checked_add(width?))
            }
            fn from_ssz(bytes: &[u8], preset: Preset) -> Result<Self, SszError> {
                let schema = [$ (<$ty as Ssz>::fixed_size(preset)),*];
                let mut fields = reth_embedded_beacon_primitives::container_fields(bytes, &schema)?.into_iter();
                Ok(Self { $ ($field: <$ty as Ssz>::from_ssz(fields.next().ok_or(SszError::InvalidOffset)?, preset)?),* })
            }
            fn to_ssz(&self, preset: Preset) -> Result<Vec<u8>, SszError> {
                $crate::ssz::codec::encode_container(&[$ ((<$ty as Ssz>::fixed_size(preset), <$ty as Ssz>::to_ssz(&self.$field, preset)?)),*])
            }
            fn write_ssz(&self, preset: Preset, output: &mut Vec<u8>) -> Result<(), SszError> {
                let Some(width) = <Self as Ssz>::fixed_size(preset) else {
                    // Variable containers retain the canonical checked offset encoder.
                    output.extend_from_slice(&<Self as Ssz>::to_ssz(self, preset)?);
                    return Ok(())
                };
                // Match the ordinary container encoder's representable total-size bound.
                u32::try_from(width).map_err(|_| SszError::Overflow)?;
                output.len().checked_add(width).ok_or(SszError::Overflow)?;
                output.try_reserve(width).map_err(|_| SszError::Overflow)?;
                let start = output.len();
                let result = (|| {
                    $(let field_start = output.len();
                      <$ty as Ssz>::write_ssz(&self.$field, preset, output)?;
                      let expected = <$ty as Ssz>::fixed_size(preset).ok_or(SszError::InvalidOffset)?;
                      let actual = output.len().checked_sub(field_start).ok_or(SszError::Overflow)?;
                      if actual != expected {
                          return Err(SszError::InvalidLength { expected, actual })
                      })*
                    Ok(())
                })();
                if result.is_err() {
                    // Failed nested field validation must preserve the caller's existing bytes.
                    output.truncate(start);
                }
                result
            }
            fn hash_tree_root(&self, preset: Preset) -> Result<Root, SszError> {
                let fields = [$ (<$ty as Ssz>::hash_tree_root(&self.$field, preset)?),*];
                $crate::ssz::codec::container_root(&fields, $progressive)
            }
            const STRUCTURE_INVARIANT: bool =
                (!$progressive || ([$ (stringify!($field)),*].len() > 0 && [$ (stringify!($field)),*].len() <= 256))
                $(&& <$ty as Ssz>::STRUCTURE_INVARIANT)*;
            fn validate_structure(&self, preset: Preset) -> Result<(), SszError> {
                $(<$ty as Ssz>::validate_structure(&self.$field, preset)?;)*
                $crate::ssz::codec::container_structure([$ (stringify!($field)),*].len(), $progressive)
            }
            $crate::ssz::codec::ssz_container!(@sequence_roots $name);
        }
    };
}
pub(crate) use ssz_container;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collection_nodes_preserve_declared_padding_and_nested_paths() {
        let preset = Preset::Minimal;
        let first = [7u8; 48];
        let second = [9u8; 48];
        let value = List::<[u8; 48], Fixed<4>>::from_vec(vec![first, second]);
        let first_chunks = pack_bytes(&first);
        let roots = [first.hash_tree_root(preset).unwrap(), second.hash_tree_root(preset).unwrap()];
        assert_eq!(value.tree_node(2, preset).unwrap(), merkleize(&roots, 4).unwrap());
        assert_eq!(value.tree_node(3, preset).unwrap(), uint64_root(2));
        assert_eq!(value.tree_node(8, preset).unwrap(), roots[0]);
        assert_eq!(value.tree_node(16, preset).unwrap(), first_chunks[0]);
        assert_eq!(value.tree_node(17, preset).unwrap(), first_chunks[1]);
        assert_eq!(value.tree_node(10, preset).unwrap(), [0; 32]);
        assert_eq!(value.tree_node(5, preset).unwrap(), merkleize(&[], 2).unwrap());
        assert_eq!(value.tree_node(6, preset), Err(SszError::UnsupportedMerklePath(6)));
        assert_eq!(value.tree_node(20, preset), Err(SszError::UnsupportedMerklePath(20)));
        assert_eq!(value.tree_node(0, preset), Err(SszError::UnsupportedMerklePath(0)));
        let container = crate::ssz::phase0::DepositData {
            pubkey: first,
            withdrawal_credentials: [0; 32],
            amount: 0,
            signature: [0; 96],
        };
        let canonical = container.to_ssz(preset).unwrap();
        let decoded = crate::ssz::phase0::DepositData::from_ssz(&canonical, preset).unwrap();
        assert_eq!(
            decoded.hash_tree_root(preset).unwrap(),
            container.hash_tree_root(preset).unwrap()
        );
        assert_eq!(container.tree_node(16, preset), Err(SszError::UnsupportedMerklePath(16)));
        let proof = crate::light_client::merkle::compute_merkle_proof(&value, 17, preset).unwrap();
        let mut index = 17u64;
        let mut root = first_chunks[1];
        for sibling in proof {
            root = if index.is_multiple_of(2) {
                reth_embedded_beacon_primitives::hash_pair(root, sibling)
            } else {
                reth_embedded_beacon_primitives::hash_pair(sibling, root)
            };
            index /= 2;
        }
        assert_eq!(root, value.hash_tree_root(preset).unwrap());
    }

    #[test]
    fn collection_nodes_pack_basic_values_and_recheck_mutated_bounds() {
        let preset = Preset::Mainnet;
        let packed = Vector::<u64, Fixed<8>>::from_vec((1..=8).collect());
        let bytes = packed.to_ssz(preset).unwrap();
        let chunks = pack_bytes(&bytes);
        assert_eq!(packed.tree_node(2, preset).unwrap(), chunks[0]);
        assert_eq!(packed.tree_node(3, preset).unwrap(), chunks[1]);
        assert_eq!(packed.tree_node(4, preset), Err(SszError::UnsupportedMerklePath(4)));
        let invalid = List::<u64, Fixed<1>>::from_vec(vec![1, 2]);
        assert_eq!(
            invalid.tree_node(3, preset),
            Err(SszError::LimitExceeded { actual: 2, limit: 1 })
        );
        let short = Vector::<u64, Fixed<8>>::from_vec(vec![1]);
        assert_eq!(
            short.tree_node(2, preset),
            Err(SszError::InvalidLength { expected: 8, actual: 1 })
        );
    }

    #[test]
    fn sparse_collection_nodes_do_not_materialize_large_declared_bounds() {
        let preset = Preset::Mainnet;
        let value = List::<Root, Fixed<1_099_511_627_776>>::from_vec(vec![[3; 32]]);
        let first_leaf = 1u64 << 41;
        assert_eq!(value.tree_node(first_leaf, preset).unwrap(), [3; 32]);
        assert_eq!(value.tree_node(first_leaf + 1, preset).unwrap(), [0; 32]);
        assert_eq!(
            value.tree_node(2, preset).unwrap(),
            merkleize(&[[3; 32]], 1usize << 40).unwrap()
        );
        assert_eq!(value.tree_node(1, preset).unwrap(), value.hash_tree_root(preset).unwrap());
        assert_eq!(sparse_chunk_node(&[], usize::MAX, 1), Err(SszError::Overflow));
    }

    #[test]
    fn heap_accounting_counts_unused_capacity_and_nested_allocations() {
        let mut inner = Vec::<u8>::with_capacity(64);
        inner.extend_from_slice(&[1, 2, 3]);
        let inner_capacity = inner.capacity();
        let mut outer = Vec::with_capacity(4);
        outer.push(List::<u8, Fixed<128>>::from_vec(inner));
        let expected =
            outer.capacity() * std::mem::size_of::<List<u8, Fixed<128>>>() + inner_capacity;
        let value = List::<List<u8, Fixed<128>>, Fixed<8>>::from_vec(outer);
        assert_eq!(value.heap_bytes().unwrap(), expected);
        assert!(value.to_ssz(Preset::Minimal).unwrap().len() < expected);
        let bits = BitListValue::<512>::from_ssz(&[1], Preset::Minimal).unwrap();
        assert_eq!(bits.heap_bytes().unwrap(), bits.bits.heap_bytes());
    }

    #[test]
    fn fixed_sequences_stream_exact_canonical_bytes_and_recheck_bounds() {
        let preset = Preset::Minimal;
        let integers = List::<u64, Fixed<3>>::from_vec(vec![1, 0, u64::MAX]);
        let mut expected = vec![1, 0, 0, 0, 0, 0, 0, 0];
        expected.extend_from_slice(&[0; 8]);
        expected.extend_from_slice(&[255; 8]);
        assert_eq!(integers.to_ssz(preset).unwrap(), expected);
        assert_eq!(List::<u64, Fixed<3>>::from_ssz(&expected, preset).unwrap(), integers);
        let mut bytes = List::<u8, Fixed<4>>::from_vec(vec![0, 127, 128, 255]);
        assert_eq!(bytes.to_ssz(preset).unwrap(), [0, 127, 128, 255]);
        bytes.values.push(1);
        let error = SszError::LimitExceeded { actual: 5, limit: 4 };
        assert_eq!(bytes.to_ssz(preset), Err(error.clone()));
        assert_eq!(bytes.hash_tree_root(preset), Err(error));
        assert_eq!(
            List::<u64, Fixed<3>>::from_ssz(&[0; 7], preset),
            Err(SszError::InvalidLength { expected: 8, actual: 7 })
        );
    }

    #[test]
    fn fixed_registry_decode_reserves_actual_count_and_preserves_canonical_errors() {
        let preset = Preset::Mainnet;
        let mut validator = crate::ssz::phase0::Validator::default_value(preset).unwrap();
        validator.pubkey = [73; 48];
        validator.effective_balance = 32_000_000_000;
        let registry =
            List::<crate::ssz::phase0::Validator, Fixed<512>>::from_vec(vec![validator; 257]);
        let bytes = registry.to_ssz(preset).unwrap();
        let decoded =
            List::<crate::ssz::phase0::Validator, Fixed<512>>::from_ssz(&bytes, preset).unwrap();
        assert_eq!(decoded, registry);
        assert_eq!(decoded.values.capacity(), 257);
        assert_eq!(
            decoded.heap_bytes().unwrap(),
            257 * std::mem::size_of::<crate::ssz::phase0::Validator>()
        );
        assert_eq!(decoded.to_ssz(preset).unwrap(), bytes);
        assert_eq!(
            decoded.hash_tree_root(preset).unwrap(),
            registry.hash_tree_root(preset).unwrap()
        );
        let vector =
            Vector::<crate::ssz::phase0::Validator, Fixed<257>>::from_ssz(&bytes, preset).unwrap();
        assert_eq!(vector.values.capacity(), 257);
        assert_eq!(vector.to_ssz(preset).unwrap(), bytes);
        assert_eq!(
            List::<crate::ssz::phase0::Validator, Fixed<256>>::from_ssz(&bytes, preset),
            Err(SszError::LimitExceeded { actual: 257, limit: 256 })
        );
        assert_eq!(
            Vector::<crate::ssz::phase0::Validator, Fixed<256>>::from_ssz(&bytes, preset),
            Err(SszError::LimitExceeded { actual: 257, limit: 256 })
        );
        assert_eq!(
            Vector::<crate::ssz::phase0::Validator, Fixed<258>>::from_ssz(&bytes, preset),
            Err(SszError::InvalidLength { expected: 258, actual: 257 })
        );
        let mut invalid = bytes.clone();
        // The fixed SSZ Validator slashed Boolean follows pubkey, withdrawal credentials,
        // and effective balance. A reserved allocation never changes its decode checks.
        invalid[48 + 32 + 8] = 2;
        assert_eq!(
            List::<crate::ssz::phase0::Validator, Fixed<512>>::from_ssz(&invalid, preset),
            Err(SszError::InvalidBoolean)
        );
        let width = <crate::ssz::phase0::Validator as Ssz>::fixed_size(preset).unwrap();
        assert_eq!(
            List::<crate::ssz::phase0::Validator, Fixed<512>>::from_ssz(
                &bytes[..bytes.len() - 1],
                preset
            ),
            Err(SszError::InvalidLength { expected: width, actual: bytes.len() - 1 })
        );
    }

    #[test]
    fn nested_progressive_lists_reject_aliasing_and_accept_empty_elements() {
        let preset = Preset::Minimal;
        let values = ProgressiveList::from_vec(vec![
            ProgressiveList::<u8>::default(),
            ProgressiveList::from_vec(vec![1, 2]),
            ProgressiveList::from_vec(vec![3]),
        ]);
        let canonical = vec![12, 0, 0, 0, 12, 0, 0, 0, 14, 0, 0, 0, 1, 2, 3];
        assert_eq!(values.to_ssz(preset).unwrap(), canonical);
        assert_eq!(
            ProgressiveList::<ProgressiveList<u8>>::from_ssz(&canonical, preset).unwrap(),
            values
        );
        for (field, offset) in [(0, 0), (0, 9), (0, 16), (1, 11), (2, 11), (2, 16)] {
            let mut mutated = canonical.clone();
            mutated[field * 4..field * 4 + 4].copy_from_slice(&u32::to_le_bytes(offset));
            assert_eq!(
                ProgressiveList::<ProgressiveList<u8>>::from_ssz(&mutated, preset),
                Err(SszError::InvalidOffset)
            );
        }
        type BoundedNested = List<List<u8, Fixed<4>>, Fixed<2>>;
        assert_eq!(
            BoundedNested::from_ssz(&canonical, preset),
            Err(SszError::LimitExceeded { actual: 3, limit: 2 })
        );
    }

    #[test]
    fn appended_encodings_validate_type_width_before_success() {
        struct IncorrectWidth;
        impl Ssz for IncorrectWidth {
            fn heap_bytes(&self) -> Result<usize, SszError> {
                Ok(0)
            }

            fn fixed_size(_: Preset) -> Option<usize> {
                Some(1)
            }
            fn from_ssz(_: &[u8], _: Preset) -> Result<Self, SszError> {
                Ok(Self)
            }
            fn to_ssz(&self, _: Preset) -> Result<Vec<u8>, SszError> {
                Ok(vec![0, 1])
            }
            fn hash_tree_root(&self, _: Preset) -> Result<Root, SszError> {
                Ok([0; 32])
            }
        }
        assert_eq!(
            List::<IncorrectWidth, Fixed<1>>::from_vec(vec![IncorrectWidth])
                .to_ssz(Preset::Minimal),
            Err(SszError::InvalidLength { expected: 1, actual: 2 })
        );
    }
    #[test]
    fn fixed_byte_roots_match_canonical_packing_across_chunk_boundaries() {
        fn check<const N: usize>() {
            let value = std::array::from_fn::<_, N, _>(|index| index.wrapping_mul(37) as u8);
            let expected = bytes_root(&value, N, false).unwrap();
            for preset in [Preset::Minimal, Preset::Mainnet] {
                assert_eq!(value.hash_tree_root(preset).unwrap(), expected);
                assert_eq!(value.tree_node(1, preset).unwrap(), expected);
            }
        }
        check::<0>();
        check::<1>();
        check::<20>();
        check::<31>();
        check::<32>();
        check::<33>();
        check::<48>();
        check::<63>();
        check::<64>();
        check::<65>();
        check::<96>();
    }
    #[test]
    fn streamed_fixed_containers_match_canonical_bytes_and_preserve_prefix_on_error() {
        fn check<T: Ssz>(value: &T, preset: Preset) {
            let expected = value.to_ssz(preset).unwrap();
            let mut output = vec![0xa5, 0x5a];
            value.write_ssz(preset, &mut output).unwrap();
            assert_eq!(&output[..2], &[0xa5, 0x5a]);
            assert_eq!(&output[2..], expected);
            let decoded = T::from_ssz(&output[2..], preset).unwrap();
            assert_eq!(
                decoded.hash_tree_root(preset).unwrap(),
                value.hash_tree_root(preset).unwrap()
            );
        }
        for preset in [Preset::Minimal, Preset::Mainnet] {
            let mut validator = crate::ssz::phase0::Validator::default_value(preset).unwrap();
            validator.pubkey = [73; 48];
            validator.withdrawal_credentials = [19; 32];
            validator.effective_balance = 32_000_000_000;
            validator.exit_epoch = u64::MAX;
            validator.slashed = true;
            check(&validator, preset);
            check(&crate::ssz::phase0::SignedVoluntaryExit::default_value(preset).unwrap(), preset);
            check(&crate::ssz::electra::PendingDeposit::default_value(preset).unwrap(), preset);
            check(&crate::ssz::gloas::Builder::default_value(preset).unwrap(), preset);
            check(
                &crate::ssz::auxiliary::altair::SyncCommitteeContribution::default_value(preset)
                    .unwrap(),
                preset,
            );
            // Variable containers retain exact checked offsets and nested collection bounds.
            check(&crate::ssz::phase0::Attestation::default_value(preset).unwrap(), preset);
        }
        let mut malformed =
            crate::ssz::auxiliary::altair::SyncCommitteeContribution::default_value(
                Preset::Minimal,
            )
            .unwrap();
        malformed.aggregation_bits.bits = BitVector::from_ssz(&[0, 0], 16).unwrap();
        let expected = malformed.to_ssz(Preset::Minimal).unwrap_err();
        let mut output = vec![0xa5, 0x5a];
        assert_eq!(malformed.write_ssz(Preset::Minimal, &mut output), Err(expected));
        assert_eq!(output, vec![0xa5, 0x5a]);
    }

    #[test]
    #[ignore = "manual full-registry fixed-container serialization benchmark"]
    fn full_registry_streaming_matches_ordinary_container_encodings() {
        let preset = Preset::Mainnet;
        let mut validator = crate::ssz::phase0::Validator::default_value(preset).unwrap();
        validator.pubkey = [73; 48];
        validator.effective_balance = 32_000_000_000;
        let count = 1_144_679;
        let width = <crate::ssz::phase0::Validator as Ssz>::fixed_size(preset).unwrap();
        let mut expected = Vec::with_capacity(count * width);
        let started = std::time::Instant::now();
        for _ in 0..count {
            expected.extend_from_slice(&validator.to_ssz(preset).unwrap());
        }
        let ordinary = started.elapsed();
        let mut actual = Vec::with_capacity(count * width);
        let started = std::time::Instant::now();
        for _ in 0..count {
            validator.write_ssz(preset, &mut actual).unwrap();
        }
        let streaming = started.elapsed();
        assert_eq!(actual, expected);
        println!(
            "Validator serialization count={} bytes={} ordinary_ms={} streaming_ms={}",
            count,
            actual.len(),
            ordinary.as_millis(),
            streaming.as_millis()
        );
    }

    #[test]
    fn structural_guards_match_complete_fork_state_roots() {
        fn check<T: Ssz>(preset: Preset) {
            let state = T::default_value(preset).unwrap();
            assert_eq!(state.validate_structure(preset), state.hash_tree_root(preset).map(|_| ()));
        }
        for preset in [Preset::Minimal, Preset::Mainnet] {
            check::<crate::ssz::phase0::BeaconState>(preset);
            check::<crate::ssz::altair::BeaconState>(preset);
            check::<crate::ssz::bellatrix::BeaconState>(preset);
            check::<crate::ssz::capella::BeaconState>(preset);
            check::<crate::ssz::deneb::BeaconState>(preset);
            check::<crate::ssz::electra::BeaconState>(preset);
            check::<crate::ssz::fulu::BeaconState>(preset);
            check::<crate::ssz::gloas::BeaconState>(preset);
        }
        // No value-dependent SSZ constraints are hidden in a fixed Validator record.
        assert!(crate::ssz::phase0::Validator::STRUCTURE_INVARIANT);
    }

    #[test]
    fn structural_guards_recheck_nested_mutations_and_capacity_errors() {
        fn check<T: Ssz>(value: &T, preset: Preset) {
            let expected = value.hash_tree_root(preset).map(|_| ());
            assert!(expected.is_err());
            assert_eq!(value.validate_structure(preset), expected);
        }
        let preset = Preset::Minimal;
        check(&List::<u64, Fixed<1>>::from_vec(vec![1, 2]), preset);
        check(&Vector::<u64, Fixed<2>>::from_vec(vec![1]), preset);
        check(
            &List::<Vector<u64, Fixed<2>>, Fixed<2>>::from_vec(vec![
                Vector::from_vec(vec![1, 2]),
                Vector::from_vec(vec![1]),
            ]),
            preset,
        );
        check(&ProgressiveList::from_vec(vec![Vector::<u64, Fixed<2>>::from_vec(vec![1])]), preset);
        check(&List::<Root, Fixed<{ u64::MAX }>>::default(), preset);
        check(&List::<u64, Fixed<{ u64::MAX }>>::default(), preset);

        let state = crate::ssz::fulu::BeaconState::default_value(preset).unwrap();
        for mutation in 0..6 {
            let mut invalid = state.clone();
            match mutation {
                0 => {
                    invalid.block_roots.values.pop();
                }
                1 => {
                    invalid.current_sync_committee.pubkeys.values.pop();
                }
                2 => {
                    invalid.latest_execution_payload_header.logs_bloom.values.pop();
                }
                3 => {
                    invalid.latest_execution_payload_header.extra_data.values = vec![0; 33];
                }
                4 => {
                    invalid.proposer_lookahead.values.pop();
                }
                _ => {
                    invalid.next_sync_committee.pubkeys.values.push([0; 48]);
                }
            }
            check(&invalid, preset);
        }

        let state = crate::ssz::gloas::BeaconState::default_value(preset).unwrap();
        for mutation in 0..4 {
            let mut invalid = state.clone();
            match mutation {
                0 => {
                    invalid.ptc_window.values[0].values.pop();
                }
                1 => {
                    invalid.builder_pending_payments.values.pop();
                }
                2 => {
                    invalid.execution_payload_availability =
                        PresetBits::from_ssz(&[0; 1024], Preset::Mainnet).unwrap();
                }
                _ => {
                    invalid.ptc_window.values.pop();
                }
            }
            check(&invalid, preset);
        }
    }

    #[test]
    fn custom_structural_validation_falls_back_to_its_complete_hash_checks() {
        struct Custom;
        impl Ssz for Custom {
            fn heap_bytes(&self) -> Result<usize, SszError> {
                Ok(0)
            }
            fn fixed_size(_: Preset) -> Option<usize> {
                Some(1)
            }
            fn from_ssz(_: &[u8], _: Preset) -> Result<Self, SszError> {
                Ok(Self)
            }
            fn to_ssz(&self, _: Preset) -> Result<Vec<u8>, SszError> {
                Ok(vec![0])
            }
            fn hash_tree_root(&self, _: Preset) -> Result<Root, SszError> {
                Err(SszError::InvalidOffset)
            }
        }
        let preset = Preset::Minimal;
        assert!(!Custom::STRUCTURE_INVARIANT);
        assert_eq!(Custom.validate_structure(preset), Err(SszError::InvalidOffset));
        let regular = List::<Custom, Fixed<2>>::from_vec(vec![Custom]);
        assert_eq!(regular.validate_structure(preset), regular.hash_tree_root(preset).map(|_| ()));
        let progressive = ProgressiveList::from_vec(vec![Custom]);
        assert_eq!(
            progressive.validate_structure(preset),
            progressive.hash_tree_root(preset).map(|_| ())
        );
    }

    #[test]
    fn fixed_validator_heap_accounting_preserves_capacity_and_wire_validation() {
        let mut validators = Vec::with_capacity(32);
        validators
            .resize(3, crate::ssz::phase0::Validator::default_value(Preset::Minimal).unwrap());
        let expected = validators.capacity() * std::mem::size_of::<crate::ssz::phase0::Validator>();
        let list = List::<crate::ssz::phase0::Validator, Fixed<2>>::from_vec(validators);
        assert_eq!(list.heap_bytes().unwrap(), expected);
        // Capacity accounting cannot authorize an oversized schema or alter its wire error.
        assert_eq!(
            list.validate_structure(Preset::Minimal),
            Err(SszError::LimitExceeded { actual: 3, limit: 2 })
        );
        assert_eq!(
            list.hash_tree_root(Preset::Minimal),
            Err(SszError::LimitExceeded { actual: 3, limit: 2 })
        );
        let values = Vector::<[u8; 96], Fixed<2>>::from_vec(Vec::with_capacity(4));
        assert_eq!(values.heap_bytes().unwrap(), values.values.capacity() * 96);
        assert_eq!(
            values.validate_structure(Preset::Minimal),
            Err(SszError::InvalidLength { expected: 2, actual: 0 })
        );
    }

    #[test]
    fn fixed_wire_width_does_not_hide_custom_heap_allocations_or_errors() {
        let mut values = Vec::with_capacity(4);
        values.push(AllocatedFixedValue { bytes: Vec::with_capacity(37), invalid: false });
        let expected = values.capacity() * std::mem::size_of::<AllocatedFixedValue>() +
            values[0].bytes.capacity();
        let mut list = List::<AllocatedFixedValue, Fixed<8>>::from_vec(values);
        assert_eq!(AllocatedFixedValue::fixed_size(Preset::Minimal), Some(1));
        assert_eq!(list.heap_bytes().unwrap(), expected);
        list.values[0].invalid = true;
        assert_eq!(list.heap_bytes(), Err(SszError::Overflow));
    }

    struct AllocatedFixedValue {
        bytes: Vec<u8>,
        invalid: bool,
    }

    impl Ssz for AllocatedFixedValue {
        fn heap_bytes(&self) -> Result<usize, SszError> {
            if self.invalid {
                Err(SszError::Overflow)
            } else {
                Ok(self.bytes.capacity())
            }
        }

        fn fixed_size(_: Preset) -> Option<usize> {
            Some(1)
        }

        fn from_ssz(bytes: &[u8], _: Preset) -> Result<Self, SszError> {
            if bytes.len() != 1 {
                return Err(SszError::InvalidLength { expected: 1, actual: bytes.len() })
            }
            Ok(Self { bytes: bytes.to_vec(), invalid: false })
        }

        fn to_ssz(&self, _: Preset) -> Result<Vec<u8>, SszError> {
            Ok(vec![0])
        }

        fn hash_tree_root(&self, _: Preset) -> Result<Root, SszError> {
            Ok([0; 32])
        }
    }
}
