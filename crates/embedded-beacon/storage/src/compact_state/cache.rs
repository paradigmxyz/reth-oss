//! Cache ownership and active leases share one exact retained-schema budget.
//! A cold reconstruction temporary is separate and bounded by the store's serialized-state cap.

use super::{CompactStateError, CompactStateStats, StateLease};
use parking_lot::Mutex;
use reth_embedded_beacon_primitives::Root;
use reth_embedded_beacon_state_transition::ssz::BeaconState;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Weak},
};

#[derive(Debug)]
pub(super) struct Allocation {
    pub(super) state: Arc<BeaconState>,
    pub(super) root: Root,
    bytes: usize,
    resident: Arc<Mutex<usize>>,
}
impl Drop for Allocation {
    fn drop(&mut self) {
        let mut resident = self.resident.lock();
        debug_assert!(*resident >= self.bytes);
        *resident -= self.bytes;
    }
}

#[derive(Debug)]
struct Entry {
    allocation: Arc<Allocation>,
    touched: u64,
}

#[derive(Debug)]
pub(super) struct Cache {
    entries: BTreeMap<Root, Entry>,
    live: BTreeMap<Root, Weak<Allocation>>,
    resident: Arc<Mutex<usize>>,
    cached: usize,
    cache_limit: usize,
    resident_limit: usize,
    clock: u64,
}

impl Cache {
    pub(super) fn new(cache_limit: usize, resident_limit: usize) -> Self {
        Self {
            entries: BTreeMap::new(),
            live: BTreeMap::new(),
            resident: Arc::new(Mutex::new(0)),
            cached: 0,
            cache_limit,
            resident_limit,
            clock: 0,
        }
    }
    pub(super) fn get(&mut self, root: Root) -> Option<StateLease> {
        self.clock = self.clock.wrapping_add(1);
        if let Some(entry) = self.entries.get_mut(&root) {
            entry.touched = self.clock;
            return Some(StateLease { allocation: entry.allocation.clone() })
        }
        self.live.get(&root).and_then(Weak::upgrade).map(|allocation| StateLease { allocation })
    }
    pub(super) fn admit(
        &mut self,
        root: Root,
        state: BeaconState,
    ) -> Result<StateLease, CompactStateError> {
        self.admit_shared(root, Arc::new(state))
    }
    pub(super) fn admit_shared(
        &mut self,
        root: Root,
        state: Arc<BeaconState>,
    ) -> Result<StateLease, CompactStateError> {
        let bytes = state
            .heap_bytes()?
            .checked_add(std::mem::size_of::<BeaconState>())
            .ok_or(CompactStateError::Capacity)?;
        if bytes > self.resident_limit {
            return Err(CompactStateError::Capacity)
        }
        while self.resident_bytes() > self.resident_limit - bytes {
            if !self.evict_one() {
                return Err(CompactStateError::Capacity)
            }
        }
        {
            let mut resident = self.resident.lock();
            *resident = resident.checked_add(bytes).ok_or(CompactStateError::Capacity)?;
        }
        let allocation =
            Arc::new(Allocation { state, root, bytes, resident: self.resident.clone() });
        self.live.insert(root, Arc::downgrade(&allocation));
        if bytes <= self.cache_limit {
            while self.cached > self.cache_limit - bytes {
                if !self.evict_one() {
                    return Err(CompactStateError::Capacity)
                }
            }
            self.clock = self.clock.wrapping_add(1);
            self.cached += bytes;
            self.entries
                .insert(root, Entry { allocation: allocation.clone(), touched: self.clock });
        }
        Ok(StateLease { allocation })
    }
    pub(super) fn owns_cached_state(&self, root: Root, state: &BeaconState) -> bool {
        self.entries
            .get(&root)
            .is_some_and(|entry| std::ptr::eq(entry.allocation.state.as_ref(), state))
    }
    fn resident_bytes(&self) -> usize {
        *self.resident.lock()
    }
    fn evict_one(&mut self) -> bool {
        let Some(root) =
            self.entries.iter().min_by_key(|(_, entry)| entry.touched).map(|(root, _)| *root)
        else {
            return false
        };
        if let Some(entry) = self.entries.remove(&root) {
            self.cached -= entry.allocation.bytes;
        }
        true
    }
    pub(super) fn retain_roots(&mut self, keep: &BTreeSet<Root>) {
        self.entries.retain(|root, entry| {
            if keep.contains(root) {
                return true
            }
            self.cached -= entry.allocation.bytes;
            false
        });
        self.live.retain(|root, _| keep.contains(root));
    }
    pub(super) fn stats(&self, retained_states: usize) -> CompactStateStats {
        CompactStateStats {
            retained_states,
            cached_states: self.entries.len(),
            cached_schema_bytes: self.cached,
            resident_schema_bytes: self.resident_bytes(),
        }
    }
}
