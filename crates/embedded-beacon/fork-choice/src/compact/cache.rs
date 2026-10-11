//! Bounded decoded hot allocations; durable branch retention is independent of this cache.

use super::{StateBinding, StateSchema, StateSourceError, StateSourceIdentity};
use crate::StoreError;
use reth_embedded_beacon_state_transition::ssz::{gloas, BeaconState, Preset};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, Weak},
};

/// Resource policy for the graph's hot states and outstanding hot read owners.
///
/// Cold reconstruction/cache budgets are additionally enforced by the concrete source. This
/// measurement excludes block bodies, transition temporaries, allocator overhead and process RSS.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CompactGraphLimits {
    /// Complete states kept decoded by a live graph, counted by unique allocation capacity.
    pub max_hot_state_bytes: usize,
    /// Hot graph allocations and outstanding hot readers/snapshots together.
    pub max_resident_hot_state_bytes: usize,
    /// Maximum states in one explicitly prepared borrowed validation context.
    pub max_prepared_reads: usize,
}
impl Default for CompactGraphLimits {
    fn default() -> Self {
        Self {
            max_hot_state_bytes: 64 * 1024 * 1024,
            max_resident_hot_state_bytes: 512 * 1024 * 1024,
            max_prepared_reads: 32,
        }
    }
}
impl CompactGraphLimits {
    pub(crate) const fn validate(self) -> Result<(), StoreError> {
        if self.max_resident_hot_state_bytes == 0 ||
            self.max_hot_state_bytes > self.max_resident_hot_state_bytes ||
            self.max_prepared_reads == 0 ||
            self.max_prepared_reads > 128
        {
            return Err(StoreError::InvalidParameters)
        }
        Ok(())
    }
}

/// Measured decoded states and boxed binding metadata in the graph.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CompactGraphStats {
    /// Hot complete states retained by block/checkpoint entries, deduplicated by allocation.
    pub hot_states: usize,
    /// Private cold block/checkpoint bindings, independent of source cache residency.
    pub cold_entries: usize,
    /// Boxed binding payload bytes owned by this graph, including retained classic history.
    /// Excludes allocator overhead, source allocations and shared validation proofs.
    pub cold_binding_heap_bytes: usize,
    /// Complete unique hot allocation capacities currently held by graph entries.
    pub hot_schema_bytes: usize,
    /// Graph, snapshots and outstanding hot guards; source lease allocations are measured there.
    pub resident_hot_schema_bytes: usize,
}

#[derive(Debug)]
enum HotWeak {
    Classic(Weak<BeaconState>),
    Gloas(Weak<gloas::BeaconState>),
}
impl HotWeak {
    fn alive(&self) -> bool {
        match self {
            Self::Classic(s) => s.strong_count() != 0,
            Self::Gloas(s) => s.strong_count() != 0,
        }
    }
}
#[derive(Debug)]
struct Tracked {
    owner: HotWeak,
    bytes: usize,
    binding: Option<StateBinding>,
}

/// Shared weak allocation index across cloned graph snapshots and hot readers.
#[derive(Clone, Debug)]
pub(crate) struct HotBudget {
    entries: Arc<Mutex<BTreeMap<usize, Tracked>>>,
    maximum: usize,
}
impl HotBudget {
    pub(crate) fn new(maximum: usize) -> Self {
        Self { entries: Arc::new(Mutex::new(BTreeMap::new())), maximum }
    }
    pub(crate) fn classic(&self, state: &Arc<BeaconState>) -> Result<(), StoreError> {
        self.classic_bytes(state).map(|_| ())
    }
    /// Reuse measured capacity only while this exact immutable weak allocation remains live.
    pub(crate) fn classic_bytes(&self, state: &Arc<BeaconState>) -> Result<usize, StoreError> {
        if let Some(bytes) = self.tracked(
            Arc::as_ptr(state) as usize,
            |owner| matches!(owner, HotWeak::Classic(weak) if weak.ptr_eq(&Arc::downgrade(state))),
        )? {
            return Ok(bytes)
        }
        self.track(
            Arc::as_ptr(state) as usize,
            HotWeak::Classic(Arc::downgrade(state)),
            state.data(state.preset()).allocation_bytes()?,
        )
    }
    pub(crate) fn gloas(
        &self,
        state: &Arc<gloas::BeaconState>,
        preset: Preset,
    ) -> Result<(), StoreError> {
        if self.tracked(
            Arc::as_ptr(state) as usize,
            |owner| matches!(owner, HotWeak::Gloas(weak) if weak.ptr_eq(&Arc::downgrade(state))),
        )?.is_some() {
            return Ok(())
        }
        self.track(
            Arc::as_ptr(state) as usize,
            HotWeak::Gloas(Arc::downgrade(state)),
            state.data(preset).allocation_bytes()?,
        )
        .map(|_| ())
    }
    fn tracked(
        &self,
        key: usize,
        matches: impl FnOnce(&HotWeak) -> bool,
    ) -> Result<Option<usize>, StoreError> {
        let mut entries = self.entries.lock().map_err(|_| StateSourceError::Durability)?;
        entries.retain(|_, entry| entry.owner.alive());
        let Some(entry) = entries.get(&key) else { return Ok(None) };
        if !matches(&entry.owner) {
            return Err(StateSourceError::BindingMismatch.into())
        }
        // This exact live weak owner excludes mutable Arc access. Arc::make_mut dissociates weak
        // owners and requires full first-allocation accounting again; immutable capacities cannot
        // change while this identity remains live. No root or consensus proof is inferred here.
        Ok(Some(entry.bytes))
    }
    fn track(&self, key: usize, owner: HotWeak, bytes: usize) -> Result<usize, StoreError> {
        let mut entries = self.entries.lock().map_err(|_| StateSourceError::Durability)?;
        entries.retain(|_, entry| entry.owner.alive());
        if let Some(existing) = entries.get(&key) {
            if existing.bytes != bytes {
                return Err(StateSourceError::BindingMismatch.into())
            }
            return Ok(bytes)
        }
        let total = entries.values().try_fold(bytes, |sum, entry| {
            sum.checked_add(entry.bytes).ok_or(StoreError::Overflow)
        })?;
        if total > self.maximum {
            return Err(StateSourceError::ResourceLimit.into())
        }
        entries.insert(key, Tracked { owner, bytes, binding: None });
        Ok(bytes)
    }
    /// Memoize only a complete root proof tied to this exact tracked immutable allocation.
    /// Weak ownership never retains decoded data and durable lease checks remain separate.
    pub(crate) fn classic_binding(
        &self,
        state: &Arc<BeaconState>,
        root: reth_embedded_beacon_primitives::Root,
        committed_root: reth_embedded_beacon_primitives::Root,
        identity: StateSourceIdentity,
    ) -> Result<StateBinding, StoreError> {
        self.classic(state)?;
        self.binding(
            Arc::as_ptr(state) as usize,
            root,
            committed_root,
            state.data(state.preset()),
            identity,
        )
    }
    pub(crate) fn gloas_binding(
        &self,
        state: &Arc<gloas::BeaconState>,
        preset: Preset,
        root: reth_embedded_beacon_primitives::Root,
        committed_root: reth_embedded_beacon_primitives::Root,
        identity: StateSourceIdentity,
    ) -> Result<StateBinding, StoreError> {
        self.gloas(state, preset)?;
        self.binding(
            Arc::as_ptr(state) as usize,
            root,
            committed_root,
            state.data(preset),
            identity,
        )
    }
    fn binding(
        &self,
        key: usize,
        root: reth_embedded_beacon_primitives::Root,
        committed_root: reth_embedded_beacon_primitives::Root,
        data: super::StateData<'_>,
        identity: StateSourceIdentity,
    ) -> Result<StateBinding, StoreError> {
        let mut entries = self.entries.lock().map_err(|_| StateSourceError::Durability)?;
        let entry = entries.get_mut(&key).ok_or(StateSourceError::Unavailable)?;
        if let Some(binding) = &entry.binding &&
            binding.origin_block_root() == root &&
            binding.state_root() == committed_root &&
            binding.identity() == identity &&
            binding.preset() == data.preset() &&
            binding.fork() == data.fork()
        {
            return Ok(binding.clone())
        }
        let binding = StateBinding::admitted(root, committed_root, data, identity)?;
        entry.binding = Some(binding.clone());
        Ok(binding)
    }
    pub(crate) fn resident_bytes(&self) -> Result<usize, StoreError> {
        let mut entries = self.entries.lock().map_err(|_| StateSourceError::Durability)?;
        entries.retain(|_, entry| entry.owner.alive());
        entries
            .values()
            .try_fold(0usize, |sum, entry| sum.checked_add(entry.bytes).ok_or(StoreError::Overflow))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reth_embedded_beacon_state_transition::ssz::phase0;

    #[test]
    fn admitted_binding_reuse_is_exact_bounded_and_weak() {
        let bytes = include_bytes!("../../testdata/official-phase0-import/genesis.ssz");
        let original = Arc::new(BeaconState::Phase0(
            phase0::BeaconState::from_ssz(bytes, Preset::Minimal).unwrap(),
            Preset::Minimal,
        ));
        let cache = HotBudget::new(usize::MAX);
        let identity =
            StateSourceIdentity { profile_digest: [1; 32], consensus_config_digest: [2; 32] };
        let root = original.hash_tree_root().unwrap();
        let first = cache.classic_binding(&original, [7; 32], root, identity).unwrap();
        let second = cache.classic_binding(&original, [7; 32], root, identity).unwrap();
        assert_eq!(first, second);
        assert_eq!(cache.entries.lock().unwrap().len(), 1);
        assert_eq!(Arc::strong_count(&original), 1);
        assert!(matches!(
            cache.classic_binding(&original, [7; 32], [3; 32], identity),
            Err(StoreError::CompactState(StateSourceError::BindingMismatch))
        ));
        let changed_identity = StateSourceIdentity { consensus_config_digest: [4; 32], ..identity };
        assert_eq!(
            cache.classic_binding(&original, [7; 32], root, changed_identity).unwrap().identity(),
            changed_identity
        );
        let mut changed = original.as_ref().clone();
        let BeaconState::Phase0(state, _) = &mut changed else { unreachable!() };
        state.slot += 1;
        assert!(matches!(
            cache.classic_binding(&Arc::new(changed), [7; 32], root, identity),
            Err(StoreError::CompactState(StateSourceError::BindingMismatch))
        ));
        drop(original);
        assert_eq!(cache.resident_bytes().unwrap(), 0);
        assert!(cache.entries.lock().unwrap().is_empty());
    }
    #[test]
    fn repeated_hot_accounting_is_weak_and_make_mut_requires_new_budget_check() {
        let bytes = include_bytes!("../../testdata/official-phase0-import/genesis.ssz");
        let mut state = Arc::new(BeaconState::Phase0(
            phase0::BeaconState::from_ssz(bytes, Preset::Minimal).unwrap(),
            Preset::Minimal,
        ));
        let footprint = state.data(Preset::Minimal).allocation_bytes().unwrap();
        let budget = HotBudget::new(footprint);
        budget.classic(&state).unwrap();
        budget.classic(&state).unwrap();
        assert_eq!(budget.classic_bytes(&state).unwrap(), footprint);
        assert_eq!(budget.classic_bytes(&state).unwrap(), footprint);
        assert_eq!(budget.resident_bytes().unwrap(), footprint);
        assert_eq!(Arc::strong_count(&state), 1);
        let previous = Arc::downgrade(&state);
        let BeaconState::Phase0(changed, _) = Arc::make_mut(&mut state) else { unreachable!() };
        changed.balances.values.reserve_exact(10_000);
        assert!(previous.upgrade().is_none());
        assert!(matches!(
            budget.classic_bytes(&state),
            Err(StoreError::CompactState(StateSourceError::ResourceLimit))
        ));
        assert_eq!(budget.resident_bytes().unwrap(), 0);
    }
}
