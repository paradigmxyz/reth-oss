//! Versioned dense and integer storage blobs. No neighboring blob is needed to decode a value.

use super::PackingMode;
use crate::DatabaseError;
use alloy_primitives::{B256, U256};
use reth_codecs::Compact;
use reth_primitives_traits::StorageEntry;
use std::sync::Arc;

const HEADER: usize = 26;
const MAGIC: &[u8; 8] = b"RSTPACK1";
const INLINE_HEADER: usize = 9;
const INLINE_LIMIT: usize = 256;

/// Validated borrowed view of a storage blob.
#[derive(Debug)]
pub(crate) struct Blob<'a> {
    bytes: &'a [u8],
    count: usize,
    width: usize,
    index: usize,
    payload: usize,
    mode: PackingMode,
    anchor: Option<B256>,
}

impl<'a> Blob<'a> {
    pub(crate) fn parse(bytes: &'a [u8]) -> Result<Self, DatabaseError> {
        if bytes.len() < HEADER || &bytes[..8] != MAGIC || !matches!(bytes[8], 1 | 2) {
            return Err(DatabaseError::Decode);
        }
        let width = bytes[9] as usize;
        if width != 2 && width != 4 {
            return Err(DatabaseError::Decode);
        }
        let count = u32::from_le_bytes(bytes[10..14].try_into().map_err(|_| DatabaseError::Decode)?)
            as usize;
        let len = u32::from_le_bytes(bytes[14..18].try_into().map_err(|_| DatabaseError::Decode)?)
            as usize;
        let expected =
            u64::from_le_bytes(bytes[18..26].try_into().map_err(|_| DatabaseError::Decode)?);
        if checksum(bytes) != expected {
            return Err(DatabaseError::Decode);
        }
        let index = HEADER
            .checked_add(count.checked_mul(32).ok_or(DatabaseError::Decode)?)
            .ok_or(DatabaseError::Decode)?;
        let mode = if bytes[8] == 1 { PackingMode::Dense } else { PackingMode::Integer32 };
        let entries = if mode == PackingMode::Dense { count } else { count.div_ceil(32) };
        let payload = index
            .checked_add(
                entries
                    .checked_add(1)
                    .and_then(|n| n.checked_mul(width))
                    .ok_or(DatabaseError::Decode)?,
            )
            .ok_or(DatabaseError::Decode)?;
        if payload.checked_add(len) != Some(bytes.len()) {
            return Err(DatabaseError::Decode);
        }
        let blob = Self { bytes, count, width, index, payload, mode, anchor: None };
        let mut previous = 0;
        for i in 0..=entries {
            let current = blob.offset(i)?;
            if current < previous || current > len || (i == 0 && current != 0) {
                return Err(DatabaseError::Decode);
            }
            if i > 0 && current - previous > if mode == PackingMode::Dense { 32 } else { 1060 } {
                return Err(DatabaseError::Decode);
            }
            previous = current;
        }
        if previous != len {
            return Err(DatabaseError::Decode);
        }
        for i in 1..count {
            if blob.key(i - 1) >= blob.key(i) {
                return Err(DatabaseError::Decode);
            }
        }
        Ok(blob)
    }

    pub(crate) const fn mode(&self) -> PackingMode {
        self.mode
    }

    pub(crate) const fn len(&self) -> usize {
        self.count
    }

    pub(crate) fn key(&self, row: usize) -> B256 {
        if let Some(anchor) = self.anchor {
            if row == 0 {
                return anchor;
            }
            let start = if self.count == 1 { INLINE_HEADER } else { INLINE_HEADER + 1 };
            return B256::from_slice(&self.bytes[start + (row - 1) * 32..start + row * 32]);
        }
        B256::from_slice(&self.bytes[HEADER + row * 32..HEADER + (row + 1) * 32])
    }

    pub(crate) fn row(&self, row: usize) -> Result<StorageEntry, DatabaseError> {
        if row >= self.count {
            return Err(DatabaseError::Decode);
        }
        let value = if self.mode == PackingMode::Dense || self.anchor.is_some() {
            compact(self.part(row)?)?
        } else {
            decode_group(self.part(row / 32)?, (self.count - row / 32 * 32).min(32))?[row % 32]
        };
        Ok(StorageEntry::new(self.key(row), value))
    }

    pub(crate) fn rows(&self) -> Result<Vec<StorageEntry>, DatabaseError> {
        if self.mode == PackingMode::Dense || self.anchor.is_some() {
            return (0..self.count).map(|i| self.row(i)).collect()
        }
        let mut rows = Vec::with_capacity(self.count);
        for group in 0..self.count.div_ceil(32) {
            let start = group * 32;
            let values = decode_group(self.part(group)?, (self.count - start).min(32))?;
            rows.extend(
                values
                    .into_iter()
                    .enumerate()
                    .map(|(i, v)| StorageEntry::new(self.key(start + i), v)),
            );
        }
        Ok(rows)
    }

    fn part(&self, index: usize) -> Result<&[u8], DatabaseError> {
        if self.anchor.is_some() {
            if self.count == 1 {
                return self.bytes.get(self.payload..).ok_or(DatabaseError::Decode);
            }
            let start: usize =
                self.bytes[self.index..self.index + index].iter().map(|v| *v as usize).sum();
            let len = *self.bytes.get(self.index + index).ok_or(DatabaseError::Decode)? as usize;
            return self
                .bytes
                .get(self.payload + start..self.payload + start + len)
                .ok_or(DatabaseError::Decode);
        }
        let start = self.offset(index)?;
        let end = self.offset(index + 1)?;
        self.bytes.get(self.payload + start..self.payload + end).ok_or(DatabaseError::Decode)
    }

    fn offset(&self, row: usize) -> Result<usize, DatabaseError> {
        let at = self.index + row * self.width;
        let bytes = self.bytes.get(at..at + self.width).ok_or(DatabaseError::Decode)?;
        Ok(if self.width == 2 {
            u16::from_le_bytes(bytes.try_into().map_err(|_| DatabaseError::Decode)?) as usize
        } else {
            u32::from_le_bytes(bytes.try_into().map_err(|_| DatabaseError::Decode)?) as usize
        })
    }

    /// Parse an anchored physical record, including the compact small-record tier.
    pub(crate) fn parse_record(
        bytes: &'a [u8],
        anchor: &[u8],
        mode: PackingMode,
    ) -> Result<Self, DatabaseError> {
        if anchor.len() != 64 {
            return Err(DatabaseError::Decode);
        }
        if bytes.first().is_some_and(|tag| (0x80..=0x83).contains(tag)) {
            if bytes.len() < INLINE_HEADER || bytes.len() > INLINE_LIMIT {
                return Err(DatabaseError::Decode);
            }
            let tag = bytes[0];
            let encoded_mode =
                if tag & 1 == 0 { PackingMode::Dense } else { PackingMode::Integer32 };
            let expected =
                u64::from_le_bytes(bytes[1..9].try_into().map_err(|_| DatabaseError::Decode)?);
            if encoded_mode != mode || inline_checksum(bytes, anchor) != expected {
                return Err(DatabaseError::Decode);
            }
            let singleton = tag & 2 == 0;
            let count = if singleton {
                1
            } else {
                *bytes.get(INLINE_HEADER).ok_or(DatabaseError::Decode)? as usize
            };
            if count == 0 || (!singleton && count < 2) {
                return Err(DatabaseError::Decode);
            }
            let index =
                if singleton { INLINE_HEADER } else { INLINE_HEADER + 1 + (count - 1) * 32 };
            let payload = index + if singleton { 0 } else { count };
            if payload > bytes.len() {
                return Err(DatabaseError::Decode);
            }
            let blob = Self {
                bytes,
                count,
                width: 0,
                index,
                payload,
                mode,
                anchor: Some(B256::from_slice(&anchor[32..])),
            };
            let len: usize = if singleton {
                bytes.len() - payload
            } else {
                bytes[index..payload].iter().map(|n| *n as usize).sum()
            };
            if payload + len != bytes.len() {
                return Err(DatabaseError::Decode);
            }
            for row in 0..count {
                compact(blob.part(row)?)?;
                if row > 0 && blob.key(row - 1) >= blob.key(row) {
                    return Err(DatabaseError::Decode);
                }
            }
            return Ok(blob);
        }
        let blob = Self::parse(bytes)?;
        if blob.mode != mode || blob.count == 0 || blob.key(0)[..] != anchor[32..] {
            return Err(DatabaseError::Decode);
        }
        Ok(blob)
    }
}

/// Owned validated blob. Its borrowed views need no repeated checksum or index scan.
#[derive(Debug, Clone)]
pub(crate) struct OwnedBlob {
    bytes: Arc<[u8]>,
    count: usize,
    width: usize,
    index: usize,
    payload: usize,
    mode: PackingMode,
    anchor: Option<B256>,
    group: Option<(usize, Vec<U256>)>,
}

impl OwnedBlob {
    pub(crate) fn parse(
        bytes: Vec<u8>,
        anchor: &[u8],
        mode: PackingMode,
    ) -> Result<Self, DatabaseError> {
        let blob = Blob::parse_record(&bytes, anchor, mode)?;
        let (count, width, index, payload) = (blob.count, blob.width, blob.index, blob.payload);
        let anchor = blob.anchor;
        Ok(Self { bytes: bytes.into(), count, width, index, payload, mode, anchor, group: None })
    }

    fn view(&self) -> Blob<'_> {
        Blob {
            bytes: &self.bytes,
            count: self.count,
            width: self.width,
            index: self.index,
            payload: self.payload,
            mode: self.mode,
            anchor: self.anchor,
        }
    }

    pub(crate) const fn len(&self) -> usize {
        self.count
    }

    pub(crate) fn byte_len(&self) -> usize {
        self.bytes.len()
    }

    pub(crate) fn key(&self, row: usize) -> B256 {
        self.view().key(row)
    }

    pub(crate) fn row(&mut self, row: usize) -> Result<StorageEntry, DatabaseError> {
        if row >= self.count {
            return Err(DatabaseError::Decode)
        }
        if self.mode == PackingMode::Dense || self.anchor.is_some() {
            return self.view().row(row)
        }
        let group = row / 32;
        if self.group.as_ref().is_none_or(|(g, _)| *g != group) {
            self.group = Some((
                group,
                decode_group(self.view().part(group)?, (self.count - group * 32).min(32))?,
            ));
        }
        let value = self.group.as_ref().ok_or(DatabaseError::Decode)?.1[row % 32];
        Ok(StorageEntry::new(self.key(row), value))
    }

    /// Position of the nearest qualifying key in this blob.
    pub(crate) fn find(
        &self,
        scope: B256,
        bound: Option<(B256, B256)>,
        inclusive: bool,
        reverse: bool,
    ) -> Option<usize> {
        let (mut lo, mut hi) = (0, self.count);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let key = (scope, self.key(mid));
            let before = bound.is_some_and(|b| key < b || (key == b && (inclusive == reverse)));
            if before {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if bound.is_none() {
            return if reverse { self.count.checked_sub(1) } else { (self.count > 0).then_some(0) }
        }
        if reverse {
            lo.checked_sub(1)
        } else {
            (lo < self.count).then_some(lo)
        }
    }
}

pub(crate) fn encode(rows: &[StorageEntry], mode: PackingMode) -> Result<Vec<u8>, DatabaseError> {
    if rows.windows(2).any(|p| p[0].key >= p[1].key) {
        return Err(DatabaseError::Decode);
    }
    let mut payload = Vec::new();
    let mut offsets = Vec::with_capacity(rows.len() + 1);
    if mode == PackingMode::Dense {
        for row in rows {
            offsets.push(payload.len());
            row.value.to_compact(&mut payload);
        }
    } else {
        for group in rows.chunks(32) {
            offsets.push(payload.len());
            encode_group(group, &mut payload);
        }
    }
    offsets.push(payload.len());
    let width = if payload.len() <= u16::MAX as usize { 2 } else { 4 };
    let mut out =
        Vec::with_capacity(HEADER + rows.len() * 32 + offsets.len() * width + payload.len());
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&[if mode == PackingMode::Dense { 1 } else { 2 }, width as u8]);
    out.extend_from_slice(
        &u32::try_from(rows.len()).map_err(|_| DatabaseError::Decode)?.to_le_bytes(),
    );
    out.extend_from_slice(
        &u32::try_from(payload.len()).map_err(|_| DatabaseError::Decode)?.to_le_bytes(),
    );
    out.extend_from_slice(&[0; 8]);
    for row in rows {
        out.extend_from_slice(row.key.as_slice());
    }
    for offset in offsets {
        if width == 2 {
            out.extend_from_slice(&(offset as u16).to_le_bytes());
        } else {
            out.extend_from_slice(&(offset as u32).to_le_bytes());
        }
    }
    out.extend_from_slice(&payload);
    let sum = checksum(&out);
    out[18..26].copy_from_slice(&sum.to_le_bytes());
    Ok(out)
}

/// Inline small records omit the first slot already stored in the physical key. Singleton
/// records also omit the count and lengths; larger inline records are bounded by encoded bytes.
pub(crate) fn encode_record(
    rows: &[StorageEntry],
    scope: B256,
    mode: PackingMode,
) -> Result<Vec<u8>, DatabaseError> {
    if rows.is_empty() || rows.windows(2).any(|p| p[0].key >= p[1].key) {
        return Err(DatabaseError::Decode);
    }
    // Even zero-width values cannot fit nine rows below the inline byte limit.
    if rows.len() > 8 {
        return encode(rows, mode);
    }
    let singleton = rows.len() == 1;
    let inline_size = INLINE_HEADER +
        if singleton { 0 } else { 1 + (rows.len() - 1) * 32 + rows.len() } +
        rows.iter().map(|r| r.value.byte_len()).sum::<usize>();
    if inline_size > INLINE_LIMIT {
        return encode(rows, mode);
    }
    let mut out = vec![0; INLINE_HEADER];
    out[0] = 0x80 | u8::from(mode == PackingMode::Integer32) | if singleton { 0 } else { 2 };
    if !singleton {
        out.push(rows.len() as u8);
        for row in &rows[1..] {
            out.extend_from_slice(row.key.as_slice());
        }
        for row in rows {
            out.push(row.value.byte_len() as u8);
        }
    }
    for row in rows {
        row.value.to_compact(&mut out);
    }
    let mut anchor = scope.to_vec();
    anchor.extend_from_slice(rows[0].key.as_slice());
    let sum = inline_checksum(&out, &anchor);
    out[1..9].copy_from_slice(&sum.to_le_bytes());
    // Integer groups can outperform Compact values, even for small records. Keep the
    // complete full-record candidate when it is smaller than the inline representation.
    if !singleton && mode == PackingMode::Integer32 {
        let full = encode(rows, mode)?;
        if full.len() < out.len() {
            return Ok(full);
        }
    }
    Ok(out)
}

fn inline_checksum(bytes: &[u8], anchor: &[u8]) -> u64 {
    anchor
        .iter()
        .chain(bytes[..1].iter())
        .chain(bytes[INLINE_HEADER..].iter())
        .fold(0xcbf29ce484222325, |h, b| (h ^ u64::from(*b)).wrapping_mul(0x100000001b3))
}

// Each 32-value group selects raw Compact values or base plus bit-packed U256 deltas.
// Raw fallback bounds overhead for unrelated high-entropy values.
fn encode_group(rows: &[StorageEntry], out: &mut Vec<u8>) {
    let mut raw = vec![0];
    for row in rows {
        let mut bytes = Vec::new();
        row.value.to_compact(&mut bytes);
        raw.push(bytes.len() as u8);
        raw.extend_from_slice(&bytes);
    }
    let base = rows.iter().map(|r| r.value).min().unwrap_or_default();
    let max = rows.iter().map(|r| r.value).max().unwrap_or_default();
    let width = (max - base).bit_len();
    let mut base_bytes = Vec::new();
    base.to_compact(&mut base_bytes);
    let size = 4 + base_bytes.len() + (width * rows.len()).div_ceil(8);
    if size >= raw.len() {
        out.extend_from_slice(&raw);
        return
    }
    out.push(1);
    out.push(base_bytes.len() as u8);
    out.extend_from_slice(&(width as u16).to_le_bytes());
    out.extend_from_slice(&base_bytes);
    let start = out.len();
    out.resize(start + (width * rows.len()).div_ceil(8), 0);
    for (i, row) in rows.iter().enumerate() {
        let delta = (row.value - base).to_le_bytes::<32>();
        for bit in 0..width {
            if (delta[bit / 8] >> (bit % 8)) & 1 != 0 {
                let at = i * width + bit;
                out[start + at / 8] |= 1 << (at % 8);
            }
        }
    }
}

fn compact(bytes: &[u8]) -> Result<U256, DatabaseError> {
    if bytes.len() > 32 {
        return Err(DatabaseError::Decode)
    }
    let value = U256::from_compact(bytes, bytes.len()).0;
    let mut canonical = Vec::new();
    value.to_compact(&mut canonical);
    if canonical != bytes {
        return Err(DatabaseError::Decode)
    }
    Ok(value)
}

fn decode_group(bytes: &[u8], count: usize) -> Result<Vec<U256>, DatabaseError> {
    let mut values = Vec::with_capacity(count);
    match bytes.first() {
        Some(0) => {
            let mut at = 1;
            for _ in 0..count {
                let len = *bytes.get(at).ok_or(DatabaseError::Decode)? as usize;
                at += 1;
                let end = at + len;
                values.push(compact(bytes.get(at..end).ok_or(DatabaseError::Decode)?)?);
                at = end;
            }
            if at != bytes.len() {
                return Err(DatabaseError::Decode)
            }
        }
        Some(1) => {
            if bytes.len() < 4 {
                return Err(DatabaseError::Decode)
            }
            let base_len = bytes[1] as usize;
            let width = u16::from_le_bytes([bytes[2], bytes[3]]) as usize;
            if width > 256 || base_len > 32 {
                return Err(DatabaseError::Decode)
            }
            let start = 4 + base_len;
            if bytes.len() != start + (count * width).div_ceil(8) {
                return Err(DatabaseError::Decode)
            }
            let base = compact(&bytes[4..start])?;
            for i in 0..count {
                let mut delta = [0u8; 32];
                for bit in 0..width {
                    let at = i * width + bit;
                    delta[bit / 8] |= ((bytes[start + at / 8] >> (at % 8)) & 1) << (bit % 8);
                }
                values.push(
                    base.checked_add(U256::from_le_bytes(delta)).ok_or(DatabaseError::Decode)?,
                );
            }
            let used = count * width;
            if !used.is_multiple_of(8) && bytes.last().is_some_and(|b| *b >> (used % 8) != 0) {
                return Err(DatabaseError::Decode)
            }
        }
        _ => return Err(DatabaseError::Decode),
    }
    Ok(values)
}

// FNV-1a over header and body, excluding the checksum field itself. This is corruption
// detection, not authentication; canonical state integrity is checked by Ethereum roots.
fn checksum(bytes: &[u8]) -> u64 {
    bytes[..18]
        .iter()
        .chain(bytes[26..].iter())
        .fold(0xcbf29ce484222325, |h, b| (h ^ u64::from(*b)).wrapping_mul(0x100000001b3))
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn both_codecs_roundtrip(values in prop::collection::vec(any::<[u8;32]>(),0..600)) {
            let rows:Vec<_>=values.iter().enumerate().map(|(i,v)|StorageEntry::new(B256::from(U256::from(i).to_be_bytes::<32>()),U256::from_le_bytes(*v))).collect();
            for mode in [PackingMode::Dense,PackingMode::Integer32] {
                let bytes=encode(&rows,mode).unwrap();
                let blob=Blob::parse(&bytes).unwrap();
                prop_assert_eq!(blob.rows().unwrap(),rows.clone());
                for i in (0..rows.len()).step_by(31) {prop_assert_eq!(blob.row(i).unwrap(),rows[i]);}
            }
        }

        #[test]
        fn malformed_blob_with_valid_checksum_never_panics(at in any::<usize>(),replacement in any::<u8>(),integer in any::<bool>()) {
            let rows:Vec<_>=(0..65).map(|i|StorageEntry::new(B256::from(U256::from(i).to_be_bytes::<32>()),U256::from(i))).collect();
            let mode=if integer {PackingMode::Integer32} else {PackingMode::Dense};
            let mut bytes=encode(&rows,mode).unwrap();
            let index=at%bytes.len();bytes[index]=replacement;
            let sum=checksum(&bytes);bytes[18..26].copy_from_slice(&sum.to_le_bytes());
            if let Ok(blob)=Blob::parse(&bytes) {let _=blob.rows();}
        }

        #[test]
        fn arbitrary_bytes_never_panic(bytes in prop::collection::vec(any::<u8>(),0..4096)) {
            if let Ok(blob)=Blob::parse(&bytes) {let _=blob.rows();}
            for mode in [PackingMode::Dense, PackingMode::Integer32] {
                if let Ok(blob)=Blob::parse_record(&bytes, &[0;64], mode) {let _=blob.rows();}
            }
        }

        #[test]
        fn anchored_records_roundtrip(values in prop::collection::vec(any::<[u8;32]>(),1..20), scope in any::<[u8;32]>()) {
            let scope=B256::from(scope);
            let rows:Vec<_>=values.iter().enumerate().map(|(i,v)|StorageEntry::new(B256::from(U256::from(i).to_be_bytes::<32>()),U256::from_le_bytes(*v))).collect();
            let mut anchor=scope.to_vec();anchor.extend_from_slice(rows[0].key.as_slice());
            for mode in [PackingMode::Dense,PackingMode::Integer32] {
                for bytes in [encode_record(&rows,scope,mode).unwrap(),encode(&rows,mode).unwrap()] {
                    prop_assert_eq!(Blob::parse_record(&bytes,&anchor,mode).unwrap().rows().unwrap(),rows.clone());
                    let mut owned=OwnedBlob::parse(bytes,&anchor,mode).unwrap();
                    for (i,row) in rows.iter().enumerate() {prop_assert_eq!(owned.row(i).unwrap(),*row);}
                }
            }
        }

        #[test]
        fn malformed_inline_with_valid_checksum_never_panics(at in any::<usize>(), replacement in any::<u8>()) {
            let rows:Vec<_>=(0..5).map(|i|StorageEntry::new(B256::from(U256::from(i).to_be_bytes::<32>()),U256::from(i))).collect();
            let anchor=[0;64];
            let mut bytes=encode_record(&rows,B256::ZERO,PackingMode::Dense).unwrap();
            let index=at%bytes.len();bytes[index]=replacement;
            let sum=inline_checksum(&bytes,&anchor);bytes[1..9].copy_from_slice(&sum.to_le_bytes());
            if let Ok(blob)=Blob::parse_record(&bytes,&anchor,PackingMode::Dense) {let _=blob.rows();}
        }
    }

    #[test]
    fn inline_records_bind_anchor_and_reject_corruption() {
        let scope = B256::repeat_byte(1);
        let slot = B256::repeat_byte(2);
        let mut anchor = scope.to_vec();
        anchor.extend_from_slice(slot.as_slice());
        for mode in [PackingMode::Dense, PackingMode::Integer32] {
            for value in [U256::ZERO, U256::ONE, U256::MAX] {
                let rows = [StorageEntry::new(slot, value)];
                let bytes = encode_record(&rows, scope, mode).unwrap();
                assert_eq!(bytes.len(), 9 + value.byte_len());
                assert_eq!(
                    Blob::parse_record(&bytes, &anchor, mode).unwrap().rows().unwrap(),
                    rows
                );
                for i in 0..bytes.len() {
                    let mut corrupt = bytes.clone();
                    corrupt[i] ^= 1;
                    assert!(Blob::parse_record(&corrupt, &anchor, mode).is_err());
                }
                for i in 0..anchor.len() {
                    let mut wrong = anchor.clone();
                    wrong[i] ^= 1;
                    assert!(Blob::parse_record(&bytes, &wrong, mode).is_err());
                }
                for len in 0..bytes.len() {
                    assert!(Blob::parse_record(&bytes[..len], &anchor, mode).is_err());
                }
                let wrong_mode = if mode == PackingMode::Dense {
                    PackingMode::Integer32
                } else {
                    PackingMode::Dense
                };
                assert!(Blob::parse_record(&bytes, &anchor, wrong_mode).is_err());
            }
        }
    }

    #[test]
    fn integer_groups_cover_constants_small_deltas_and_raw_fallback() {
        for n in [0, 1, 31, 32, 33, 511, 512, 513] {
            for kind in 0..4 {
                let rows: Vec<_> = (0..n)
                    .map(|i| {
                        StorageEntry::new(
                            B256::from(U256::from(i).to_be_bytes::<32>()),
                            match kind {
                                0 => U256::ZERO,
                                1 => U256::MAX,
                                2 => U256::MAX - U256::from(i % 17),
                                _ => U256::from(i),
                            },
                        )
                    })
                    .collect();
                let bytes = encode(&rows, PackingMode::Integer32).unwrap();
                assert_eq!(Blob::parse(&bytes).unwrap().rows().unwrap(), rows);
            }
        }
        for bad in
            [vec![], vec![2], vec![0, 33], vec![1, 0, 1, 1], vec![1, 33, 0, 0], vec![1, 0, 0, 0, 1]]
        {
            assert!(decode_group(&bad, 32).is_err());
        }
    }

    #[test]
    fn dense_roundtrip_boundaries_and_corruption() {
        let rows: Vec<_> = (0..2050)
            .map(|i| {
                StorageEntry::new(
                    B256::from(U256::from(i).to_be_bytes::<32>()),
                    if i % 3 == 0 { U256::MAX } else { U256::from(i) },
                )
            })
            .collect();
        let bytes = encode(&rows, PackingMode::Dense).unwrap();
        assert_eq!(Blob::parse(&bytes).unwrap().rows().unwrap(), rows);
        for len in [0, 8, 25, 26, bytes.len() - 1] {
            assert!(Blob::parse(&bytes[..len]).is_err());
        }
        for i in [8, 9, 10, 14, 18, 26, bytes.len() - 1] {
            let mut bad = bytes.clone();
            bad[i] ^= 1;
            assert!(Blob::parse(&bad).is_err());
        }
        assert!(Blob::parse(&encode(&[], PackingMode::Dense).unwrap())
            .unwrap()
            .rows()
            .unwrap()
            .is_empty());
        let large: Vec<_> = (0..3000)
            .map(|i| StorageEntry::new(B256::from(U256::from(i).to_be_bytes::<32>()), U256::MAX))
            .collect();
        let encoded = encode(&large, PackingMode::Dense).unwrap();
        assert_eq!(encoded[9], 4);
        assert_eq!(Blob::parse(&encoded).unwrap().rows().unwrap(), large);
    }
}
