//! Version-one persisted adaptive reconstruction boundaries.

use crate::DatabaseError;
use alloy_primitives::B256;
use alloy_trie::Nibbles;
use std::{collections::BTreeMap, ops::Bound, sync::Arc};

pub(crate) const TABLE: &str = "ExperimentalPackedTrieFrontiersV1";
pub(crate) const POLICY_VERSION: u8 = 1;
pub(crate) const LEAF_LIMIT: usize = 512;
pub(crate) const MERGE_LIMIT: usize = 256;
pub(crate) const REGION_LIMIT: usize = 4096;
/// Each decoded leaf is a 32-byte key and a 32-byte value.
pub(crate) const DECODED_LIMIT: usize = LEAF_LIMIT * 64;
/// Conservative bound for region branch maps, including allocation overhead.
pub(crate) const TEMPORARY_LIMIT: usize = 1024 * 1024;

/// Only occupied cuts are represented. Missing children are empty, not reconstructed regions.
/// Full fallback is sticky until a contract wipe, avoiding repeated adoption/removal of its trie.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) enum Frontier {
    #[default]
    Empty,
    Cuts(BTreeMap<Nibbles, u16>),
    Full,
}

impl Frontier {
    pub(crate) fn retains(&self, path: &Nibbles) -> bool {
        match self {
            Self::Full => !path.is_empty(),
            Self::Empty => false,
            Self::Cuts(cuts) => {
                // Root metadata links first-nibble cuts to reusable child hashes.
                // Logical cursors omit it, preserving pending overlay precedence.
                path.is_empty() ||
                    cuts.range(..=path).next_back().is_none_or(|(cut, _)| !path.starts_with(cut))
            }
        }
    }

    pub(crate) fn cuts(&self) -> impl DoubleEndedIterator<Item = Nibbles> + '_ {
        let cuts = match self {
            Self::Cuts(cuts) => Some(cuts),
            _ => None,
        };
        cuts.into_iter().flat_map(|cuts| cuts.keys().copied())
    }

    /// Find an existing cut or the first absent child of the implicit frontier tree.
    pub(crate) fn region(&self, slot: B256) -> Nibbles {
        let path = Nibbles::unpack(slot);
        let Self::Cuts(cuts) = self else { return Nibbles::default() };
        let previous = cuts.range(..=path).next_back().map(|(cut, _)| *cut);
        if let Some(cut) = previous.filter(|cut| path.starts_with(cut)) {
            return cut
        }
        let next =
            cuts.range((Bound::Excluded(path), Bound::Unbounded)).next().map(|(cut, _)| *cut);
        let length = previous
            .into_iter()
            .chain(next)
            .map(|cut| path.iter().zip(cut.iter()).take_while(|(a, b)| a == b).count() + 1)
            .max()
            .unwrap_or(0);
        path.slice(..length)
    }

    /// Count changes are applied once per logical insert/delete, not per value overwrite.
    pub(crate) fn change(&mut self, slot: B256, inserted: bool) -> Option<Nibbles> {
        if matches!(self, Self::Full) {
            return None
        }
        if matches!(self, Self::Empty) {
            *self = Self::Cuts(BTreeMap::new());
        }
        let prefix = self.region(slot);
        let Self::Cuts(cuts) = self else { unreachable!() };
        if inserted {
            let count = cuts.entry(prefix).or_default();
            *count += 1;
            if usize::from(*count) > LEAF_LIMIT {
                return Some(prefix)
            }
        } else if let Some(count) = cuts.get_mut(&prefix) {
            *count -= 1;
            if *count == 0 {
                cuts.remove(&prefix);
            }
        }
        None
    }

    /// A split reads at most 513 leaves, including a pending insertion.
    pub(crate) fn split(&mut self, prefix: Nibbles, slots: &[B256]) {
        let Self::Cuts(cuts) = self else { return };
        cuts.remove(&prefix);
        // A long common prefix advances without retaining another copied slot vector
        // at each nibble. The 513 unique leaves diverge before the end of the key.
        let mut split = prefix;
        while split.len() < 63 && !slots.is_empty() {
            let child = Nibbles::unpack(slots[0]).slice(..split.len() + 1);
            if !slots.iter().all(|slot| Nibbles::unpack(*slot).starts_with(&child)) {
                break
            }
            split = child;
        }
        let mut groups = BTreeMap::<Nibbles, Vec<B256>>::new();
        for slot in slots {
            groups.entry(Nibbles::unpack(slot).slice(..split.len() + 1)).or_default().push(*slot);
        }
        for (child, rows) in groups {
            if rows.len() > LEAF_LIMIT {
                // Unique 64-nibble keys necessarily terminate before prefix length 64.
                self.split(child, &rows);
            } else if let Self::Cuts(cuts) = self {
                cuts.insert(child, rows.len() as u16);
            }
        }
    }

    pub(crate) fn merge(&mut self, slot: B256) {
        let path = Nibbles::unpack(slot);
        let length = self.region(slot).len();
        let Self::Cuts(cuts) = self else { return };
        for length in (0..=length.saturating_sub(1)).rev() {
            let parent = path.slice(..length);
            let mut count = 0;
            for (_, leaves) in cuts.range(parent..).take_while(|(cut, _)| cut.starts_with(&parent))
            {
                count += usize::from(*leaves);
                if count > MERGE_LIMIT {
                    break
                }
            }
            if count > MERGE_LIMIT {
                break
            }
            if count != 0 {
                cuts.retain(|cut, _| !cut.starts_with(&parent));
                cuts.insert(parent, count as u16);
            }
        }
        if cuts.is_empty() {
            *self = Self::Empty;
        }
    }

    pub(crate) fn exceeds(&self, limit: usize) -> bool {
        matches!(self, Self::Cuts(cuts) if cuts.len() > limit)
    }

    pub(crate) fn encode(&self, scope: B256) -> Vec<u8> {
        let mut bytes = vec![POLICY_VERSION];
        match self {
            Self::Empty => bytes.push(0),
            Self::Full => bytes.push(1),
            Self::Cuts(cuts) => {
                bytes.push(2);
                bytes.extend_from_slice(&(cuts.len() as u16).to_be_bytes());
                for (path, count) in cuts {
                    bytes.push(path.len() as u8);
                    bytes.extend_from_slice(&path.pack());
                    bytes.extend_from_slice(&count.to_be_bytes());
                }
            }
        }
        bytes.extend_from_slice(&checksum(scope, &bytes).to_be_bytes());
        bytes
    }

    pub(crate) fn decode(scope: B256, bytes: &[u8]) -> Result<Self, DatabaseError> {
        if bytes.len() < 10 || bytes.len() > 4 + REGION_LIMIT * 35 + 8 {
            return Err(DatabaseError::Decode)
        }
        let (data, digest) = bytes.split_at(bytes.len() - 8);
        if data[0] != POLICY_VERSION || digest != checksum(scope, data).to_be_bytes() {
            return Err(DatabaseError::Decode)
        }
        if data.len() == 2 {
            return match data[1] {
                0 => Ok(Self::Empty),
                1 => Ok(Self::Full),
                _ => Err(DatabaseError::Decode),
            }
        }
        if data.len() < 4 || data[1] != 2 {
            return Err(DatabaseError::Decode)
        }
        let count = usize::from(u16::from_be_bytes([data[2], data[3]]));
        if count == 0 || count > REGION_LIMIT {
            return Err(DatabaseError::Decode)
        }
        let mut remaining = &data[4..];
        let mut cuts = BTreeMap::new();
        let mut previous: Option<Nibbles> = None;
        for _ in 0..count {
            let Some(&length) = remaining.first() else { return Err(DatabaseError::Decode) };
            let length = usize::from(length);
            let packed = length.div_ceil(2);
            if length > 64 || remaining.len() < packed + 3 {
                return Err(DatabaseError::Decode)
            }
            if !length.is_multiple_of(2) && remaining[packed] & 15 != 0 {
                return Err(DatabaseError::Decode)
            }
            let mut key = [0; 32];
            key[..packed].copy_from_slice(&remaining[1..packed + 1]);
            let prefix = Nibbles::unpack(key).slice(..length);
            let leaves = u16::from_be_bytes([remaining[packed + 1], remaining[packed + 2]]);
            if leaves == 0 ||
                usize::from(leaves) > LEAF_LIMIT ||
                previous.is_some_and(|last| last >= prefix || prefix.starts_with(&last))
            {
                return Err(DatabaseError::Decode)
            }
            cuts.insert(prefix, leaves);
            previous = Some(prefix);
            remaining = &remaining[packed + 3..];
        }
        if !remaining.is_empty() {
            return Err(DatabaseError::Decode)
        }
        Ok(Self::Cuts(cuts))
    }
}

fn checksum(scope: B256, bytes: &[u8]) -> u64 {
    scope.iter().chain(bytes).fold(0xcbf29ce484222325, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
    })
}

/// Snapshot-local metadata admission, separate from blob and derived-node budgets.
#[derive(Debug, Default)]
pub(crate) struct FrontierCache {
    entries: BTreeMap<B256, (u64, Arc<Frontier>)>,
    bytes: usize,
}

impl FrontierCache {
    pub(crate) fn get(&self, scope: B256, generation: u64) -> Option<Arc<Frontier>> {
        self.entries.get(&scope).filter(|(g, _)| *g == generation).map(|(_, f)| Arc::clone(f))
    }

    pub(crate) fn insert(&mut self, scope: B256, generation: u64, frontier: Arc<Frontier>) {
        if let Some((_, old)) = self.entries.remove(&scope) {
            self.bytes -= frontier_bytes(&old);
        }
        let bytes = frontier_bytes(&frontier);
        if bytes > super::cache::CACHE_BYTES {
            return
        }
        while self.entries.len() >= 64 || self.bytes + bytes > super::cache::CACHE_BYTES {
            if let Some((_, (_, old))) = self.entries.pop_first() {
                self.bytes -= frontier_bytes(&old);
            } else {
                break
            }
        }
        self.bytes += bytes;
        self.entries.insert(scope, (generation, frontier));
    }
}

fn frontier_bytes(frontier: &Frontier) -> usize {
    128 + frontier.cuts().count() * (std::mem::size_of::<(Nibbles, u16)>() + 96)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn parser_is_bounded(bytes in proptest::collection::vec(any::<u8>(), 0..2000)) {
            if let Ok(frontier) = Frontier::decode(B256::ZERO, &bytes) {
                prop_assert_eq!(frontier.encode(B256::ZERO), bytes);
            }
        }

        #[test]
        fn ordered_lookup_matches_prefix_model(indices in proptest::collection::btree_set(0u16..4096, 1..80), key in any::<[u8; 32]>()) {
            let cuts: BTreeMap<_, _> = indices.into_iter().map(|index| {
                let mut slot = [0; 32]; slot[..2].copy_from_slice(&(index << 4).to_be_bytes());
                (Nibbles::unpack(slot).slice(..3), 1)
            }).collect();
            let path = Nibbles::unpack(key);
            let expected = (0..=64).map(|length| path.slice(..length)).find(|prefix| cuts.contains_key(prefix) || !cuts.keys().any(|cut| cut.starts_with(prefix))).unwrap();
            let frontier = Frontier::Cuts(cuts);
            prop_assert_eq!(frontier.region(B256::from(key)), expected);
            prop_assert_eq!(Frontier::decode(B256::ZERO, &frontier.encode(B256::ZERO)).unwrap(), frontier);
        }
    }

    #[test]
    fn authenticated_but_invalid_boundaries_are_rejected() {
        let root = Nibbles::default();
        let child = Nibbles::from_nibbles_unchecked([1]);
        for cuts in [
            BTreeMap::from([(root, 0)]),
            BTreeMap::from([(root, 513)]),
            BTreeMap::from([(root, 1), (child, 1)]),
        ] {
            assert!(Frontier::decode(B256::ZERO, &Frontier::Cuts(cuts).encode(B256::ZERO)).is_err());
        }
    }

    #[test]
    fn boundaries_are_canonical_and_bound_to_contract() {
        for frontier in [
            Frontier::Empty,
            Frontier::Full,
            Frontier::Cuts(BTreeMap::from([(Nibbles::default(), 512)])),
        ] {
            let bytes = frontier.encode(B256::ZERO);
            assert_eq!(Frontier::decode(B256::ZERO, &bytes).unwrap(), frontier);
            assert!(Frontier::decode(B256::repeat_byte(1), &bytes).is_err());
            for index in 0..bytes.len() {
                let mut corrupt = bytes.clone();
                corrupt[index] ^= 1;
                assert!(Frontier::decode(B256::ZERO, &corrupt).is_err());
            }
        }
    }
}
