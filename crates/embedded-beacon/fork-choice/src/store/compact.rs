//! Compact classic graph: state membership survives hot eviction and all reads own their leases.

use super::*;
use crate::compact::{
    AdmittedSnapshot, AdmittedState, CompactGraphLimits, CompactGraphStats, CompactStateSource,
    HotBudget, RetainedState, SignedBlockData, StateBinding, StateRead, StateSourceError,
};
use std::{collections::BTreeSet, time::Instant};

/// Strict classic event graph backed by authenticated durable state leases.
///
/// This wrapper never exposes its inner Store or an unowned borrowed cold state. The ordinary
/// Store constructors and borrowed getters remain hot-only. Every event runs the same private
/// strict kernels on a candidate; durable data/pins precede canonical publication and graph swap.
#[derive(Clone, Debug)]
pub struct CompactStore {
    pub(crate) graph: Store,
    pub(crate) source: Arc<dyn CompactStateSource>,
    pub(crate) limits: CompactGraphLimits,
    pub(crate) hot_budget: HotBudget,
    pub(crate) optimistic: Option<optimistic::CompactOptimisticTracking>,
}
impl CompactStore {
    /// Consume an actually admitted graph whose existing roots already have durable receipts.
    pub fn from_store(
        graph: Store,
        source: Arc<dyn CompactStateSource>,
        limits: CompactGraphLimits,
    ) -> Result<Self, StoreError> {
        limits.validate()?;
        let mut compact = Self {
            graph,
            source,
            limits,
            hot_budget: HotBudget::new(limits.max_resident_hot_state_bytes),
            optimistic: None,
        };
        compact.cool(limits.max_hot_state_bytes)?;
        compact.publish()?;
        Ok(compact)
    }

    /// Own the admitted complete post-state. Unknown roots are absent; known cold failures error.
    pub fn read_block_state(&self, root: Root) -> Result<Option<StateRead>, StoreError> {
        self.graph.read_block_state(root)
    }
    /// Own the separately retained actual checkpoint state.
    pub fn read_checkpoint_state(
        &self,
        checkpoint: Checkpoint,
    ) -> Result<Option<StateRead>, StoreError> {
        self.graph
            .checkpoint_states
            .get(&(checkpoint.epoch, checkpoint.root))
            .map(|s| s.acquire(self.graph.parameters.phase0.preset))
            .transpose()
    }
    /// Borrow immutable block data; complete state access always uses an owning guard.
    pub fn block(&self, root: Root) -> Option<&BeaconBlock> {
        self.graph.block(root)
    }
    /// Actual private graph head, including checkpoint state reads through owning guards.
    pub fn get_head(&self) -> Result<ForkChoiceNode, StoreError> {
        self.graph.get_head()
    }
    /// Actual admitted canonical graph metadata.
    pub fn snapshot(&self) -> Result<StoreSnapshot, StoreError> {
        self.graph.snapshot()
    }
    /// Actual realized graph checkpoints.
    pub const fn checkpoints(&self) -> (Checkpoint, Checkpoint) {
        self.graph.checkpoints()
    }
    /// Current checked clock.
    pub const fn current_slot(&self) -> u64 {
        self.graph.current_slot()
    }
    /// Current checked time.
    pub const fn time_ms(&self) -> u64 {
        self.graph.time_ms()
    }
    /// Exact checked transition schedule.
    pub const fn transition_parameters(&self) -> Option<&TransitionParameters> {
        self.graph.transition_parameters()
    }
    /// Execution association obtained from the strict private graph.
    pub fn verified_execution_block_hash(&self, root: Root) -> Result<Option<Root>, StoreError> {
        self.graph.verified_execution_block_hash(root)
    }
    /// Canonical finalized history uses actual retained block ancestry.
    pub fn canonical_finalized_epoch_roots(
        &self,
        epoch: u64,
        count: usize,
    ) -> Result<Vec<(u64, Root)>, StoreError> {
        self.graph.canonical_finalized_epoch_roots(epoch, count)
    }
    /// Compare actual private graph leaf weights without loading unrelated complete states.
    pub fn viable_leaf_weights(&self) -> Result<BTreeMap<Root, u128>, StoreError> {
        self.graph.viable_leaf_weights()
    }
    /// Inspect the native source constraint without loading or changing consensus state.
    pub fn block_voting_source(
        &self,
        root: Root,
    ) -> Result<Option<super::BlockVotingSource>, StoreError> {
        self.graph.block_voting_source(root)
    }

    /// Evict all decoded graph states; complete durable ancestry and every retained branch remain.
    pub fn evict_hot_states(&mut self) -> Result<(), StoreError> {
        let mut candidate = self.clone();
        candidate.cool(0)?;
        candidate.publish()?;
        *self = candidate;
        Ok(())
    }
    /// Measure graph and outstanding hot allocations; source cache leases have their own budget.
    pub fn stats(&self) -> Result<CompactGraphStats, StoreError> {
        let mut seen = BTreeSet::new();
        let mut hot_schema_bytes = 0usize;
        let mut cold_entries = 0usize;
        let mut cold_binding_heap_bytes = 0usize;
        for entry in self.graph.block_states.values().chain(self.graph.checkpoint_states.values()) {
            if let Some(state) = entry.hot() {
                if seen.insert(Arc::as_ptr(state) as usize) {
                    hot_schema_bytes = hot_schema_bytes
                        .checked_add(self.hot_budget.classic_bytes(state)?)
                        .ok_or(StoreError::Overflow)?;
                }
            } else {
                cold_entries = cold_entries.checked_add(1).ok_or(StoreError::Overflow)?;
                cold_binding_heap_bytes = cold_binding_heap_bytes
                    .checked_add(std::mem::size_of::<StateBinding>())
                    .ok_or(StoreError::Overflow)?;
            }
        }
        Ok(CompactGraphStats {
            hot_states: seen.len(),
            cold_entries,
            cold_binding_heap_bytes,
            hot_schema_bytes,
            resident_hot_schema_bytes: self.hot_budget.resident_bytes()?,
        })
    }

    /// Strict monotonic clock/finality realization, published transactionally.
    pub fn on_tick(&mut self, time_ms: u64) -> Result<(), StoreError> {
        self.event(|graph| graph.on_tick(time_ms))
    }
    /// Strict Phase0/Altair beacon import.
    pub fn on_block(&mut self, signed: &SignedBeaconBlock) -> Result<(), StoreError> {
        let mut candidate = self.clone();
        candidate.graph.on_block(signed)?;
        candidate.retain_block(signed)?;
        candidate.finish(self)
    }
    /// Strict complete block and authenticated contents import.
    pub fn on_block_and_contents(&mut self, signed: &SignedBeaconBlock) -> Result<(), StoreError> {
        let mut candidate = self.clone();
        candidate.graph.on_block_and_contents(signed)?;
        candidate.retain_block(signed)?;
        candidate.finish(self)
    }
    /// Strict scheduled full transition, mandatory Engine and required availability callbacks.
    pub async fn on_block_with_engine<
        E: fork_processing::ExecutionEngine,
        D: crate::data_availability::DataAvailability,
    >(
        &mut self,
        signed: &SignedBeaconBlock,
        engine: &E,
        availability: &D,
    ) -> Result<(), StoreError> {
        self.on_block_with_engine_timed(signed, engine, availability).await.map(|_| ())
    }
    /// Strict import with separate availability, transition, fork-choice and persistence timing.
    pub async fn on_block_with_engine_timed<
        E: fork_processing::ExecutionEngine,
        D: crate::data_availability::DataAvailability,
    >(
        &mut self,
        signed: &SignedBeaconBlock,
        engine: &E,
        availability: &D,
    ) -> Result<super::BlockImportTimings, StoreError> {
        let mut candidate = self.clone();
        let mut timings =
            candidate.graph.on_block_with_engine_timed(signed, engine, availability).await?;
        let started = Instant::now();
        candidate.retain_block_and_availability(signed, availability)?;
        candidate.finish(self)?;
        timings.persistence_micros = super::timing::elapsed_micros(started);
        Ok(timings)
    }
    /// Same strict atomic import with authenticated body observations.
    pub async fn on_block_and_contents_with_engine<
        E: fork_processing::ExecutionEngine,
        D: crate::data_availability::DataAvailability,
    >(
        &mut self,
        signed: &SignedBeaconBlock,
        engine: &E,
        availability: &D,
    ) -> Result<(), StoreError> {
        let mut candidate = self.clone();
        candidate.graph.on_block_and_contents_with_engine(signed, engine, availability).await?;
        candidate.retain_block_and_availability(signed, availability)?;
        candidate.finish(self)
    }
    /// Actual signed Phase0 attestation, with unchanged production committee and vote rules.
    pub fn on_attestation(&mut self, attestation: &phase0::Attestation) -> Result<(), StoreError> {
        self.event(|graph| graph.on_attestation(attestation))
    }
    /// Complete fork-selected wire vote, checked by the shared strict event kernel.
    pub fn on_attestation_wire(&mut self, bytes: &[u8], fork: Fork) -> Result<(), StoreError> {
        self.event(|graph| graph.on_attestation_wire(bytes, fork))
    }
    /// Complete strict wire slashing, preserving previous graph on any failure.
    pub fn on_attester_slashing_wire(
        &mut self,
        bytes: &[u8],
        fork: Fork,
    ) -> Result<(), StoreError> {
        self.event(|graph| graph.on_attester_slashing_wire(bytes, fork))
    }
    /// Reauthenticate complete original signed slashing evidence against this actual justified
    /// registry and the attached consensus domains. No saved equivocation flag is accepted.
    pub fn on_original_attester_slashing(
        &mut self,
        bytes: &[u8],
        fork: Fork,
        parameters: &reth_embedded_beacon_state_transition::fork_processing::TransitionParameters,
        verification_slot: u64,
    ) -> Result<(), StoreError> {
        self.event(|graph| {
            graph.original_attester_slashing(bytes, fork, parameters, verification_slot)
        })
    }
    /// Authenticate the actual item of a privately retained signed block body.
    pub fn on_retained_block_attestation(
        &mut self,
        root: Root,
        index: usize,
    ) -> Result<(), StoreError> {
        self.event(|graph| graph.on_retained_block_attestation(root, index))
    }

    /// Validate actual retained body votes in order and publish one checked graph candidate.
    /// Each rejected vote preserves the preceding successfully verified updates.
    pub fn on_retained_block_attestations(
        &mut self,
        root: Root,
    ) -> Result<Vec<(usize, Result<(), StoreError>)>, StoreError> {
        let mut candidate = self.clone();
        let mut results = Vec::new();
        let outcome = candidate.graph.retained_block_attestations_inner(root, &mut results);
        // A later infrastructure failure cannot roll back preceding verified scalar events.
        // Rejected votes alone never publish or alter the previous compact graph.
        if results.iter().any(|(_, result)| result.is_ok()) {
            candidate.finish(self)?;
        }
        outcome?;
        Ok(results)
    }
    /// Authenticate the actual slashing item of a privately retained block body.
    pub fn on_retained_block_slashing(
        &mut self,
        root: Root,
        index: usize,
    ) -> Result<(), StoreError> {
        self.event(|graph| graph.on_retained_block_slashing(root, index))
    }
    fn event(
        &mut self,
        operation: impl FnOnce(&mut Store) -> Result<(), StoreError>,
    ) -> Result<(), StoreError> {
        let mut candidate = self.clone();
        operation(&mut candidate.graph)?;
        candidate.finish(self)
    }
    pub(super) fn finish(mut self, previous: &mut Self) -> Result<(), StoreError> {
        if let Some(tracking) = &mut self.optimistic {
            tracking.retain_roots(&self.graph);
        }
        self.cool(self.limits.max_hot_state_bytes)?;
        self.publish()?;
        *previous = self;
        Ok(())
    }
    fn publish(&self) -> Result<(), StoreError> {
        self.source.publish_snapshot(AdmittedSnapshot {
            snapshot: self.graph.snapshot()?,
            time_ms: self.graph.time_ms(),
            retained_roots: self.graph.blocks.keys().copied().collect(),
            verified_envelope_roots: Default::default(),
            identity: self.source.identity(),
        })?;
        Ok(())
    }
    fn retain_block(&self, signed: &SignedBeaconBlock) -> Result<(), StoreError> {
        let root = signed.message_root()?;
        let state = self.graph.read_block_state(root)?.ok_or(StoreError::UnknownRoot)?;
        let binding = self.block_binding(root, &state)?;
        self.source.retain_block(AdmittedState {
            binding: &binding,
            state: state.data(),
            signed: SignedBlockData::Classic(signed),
            verified_execution_hash: self.admitted_execution_hash(root)?,
            execution_optimistic: self.graph.execution_status(root) == ExecutionStatus::Optimistic,
            shared_state: Some(&state),
        })?;
        Ok(())
    }
    fn block_binding(&self, root: Root, state: &StateRead) -> Result<StateBinding, StoreError> {
        let block = self.graph.blocks.get(&root).ok_or(StoreError::UnknownRoot)?;
        let header = block.header()?;
        if header.slot != state.slot() || block.schema_fork() != state.schema_fork() {
            return Err(StateSourceError::BindingMismatch.into())
        }
        if let Some(owner) = state.hot_arc() {
            return self.hot_budget.classic_binding(
                &owner,
                root,
                header.state_root,
                self.source.identity(),
            )
        }
        if let Some(entry) = self.graph.block_states.get(&root) &&
            let Some(binding) = entry.verified_binding_for(state)?
        {
            if binding.origin_block_root() != root ||
                binding.state_root() != header.state_root ||
                binding.origin_state_root() != header.state_root ||
                binding.checkpoint_epoch().is_some() ||
                binding.identity() != self.source.identity()
            {
                return Err(StateSourceError::BindingMismatch.into())
            }
            return Ok(binding)
        }
        StateBinding::admitted(root, header.state_root, state.data(), self.source.identity())
    }
    pub(crate) fn cool(&mut self, maximum: usize) -> Result<(), StoreError> {
        let preset = self.graph.parameters.phase0.preset;
        let mut ordered = self
            .graph
            .block_states
            .iter()
            .filter_map(|(&root, entry)| entry.hot().map(|s| (s.slot(), root, s.clone())))
            .collect::<Vec<_>>();
        ordered.sort_unstable_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
        let mut hot_bytes = 0usize;
        let mut kept = BTreeSet::new();
        for (_, root, state) in ordered {
            let bytes = self.hot_budget.classic_bytes(&state)?;
            let pointer = Arc::as_ptr(&state) as usize;
            if kept.contains(&pointer) {
                continue
            }
            let next = hot_bytes.checked_add(bytes).ok_or(StoreError::Overflow)?;
            if next <= maximum {
                kept.insert(pointer);
                hot_bytes = next;
                continue
            }
            let read = StateRead::hot(state, preset);
            let binding = self.block_binding(root, &read)?;
            let cold = RetainedState::cold_from_checked_read(binding, self.source.clone(), &read)?;
            self.graph.block_states.insert(root, cold);
        }
        let checkpoints = self
            .graph
            .checkpoint_states
            .iter()
            .filter_map(|(&key, entry)| entry.hot().map(|s| (key, s.clone())))
            .collect::<Vec<_>>();
        for ((epoch, root), state) in checkpoints {
            let bytes = self.hot_budget.classic_bytes(&state)?;
            let pointer = Arc::as_ptr(&state) as usize;
            if kept.contains(&pointer) {
                continue
            }
            let next = hot_bytes.checked_add(bytes).ok_or(StoreError::Overflow)?;
            if next <= maximum {
                kept.insert(pointer);
                hot_bytes = next;
                continue
            }
            let origin = self.graph.read_block_state(root)?.ok_or(StoreError::UnknownRoot)?;
            let origin_binding = self.block_binding(root, &origin)?;
            let read = StateRead::hot(state, preset);
            let binding = cooling_checkpoint_binding(&origin, &origin_binding, epoch, &read)?;
            if binding.checkpoint_epoch().is_some() {
                self.source.retain_checkpoint(&binding, read.data())?;
            }
            self.graph.checkpoint_states.insert(
                (epoch, root),
                RetainedState::cold_from_checked_read(binding, self.source.clone(), &read)?,
            );
        }
        self.graph.check_retention_budget()?;
        Ok(())
    }
}

impl crate::compact::owned_views::sealed::Sealed for CompactStore {}
impl crate::compact::ReadGossipView for CompactStore {
    const VERIFY_SIGNATURES: bool = true;
    fn read_block_state(
        &self,
        root: Root,
    ) -> Result<Option<crate::compact::StateAccess<'_, BeaconState>>, StoreError> {
        self.graph.read_block_state(root).map(|s| s.map(crate::compact::StateAccess::Owned))
    }
    fn block(&self, root: Root) -> Option<&BeaconBlock> {
        crate::gossip_view::GossipView::block(&self.graph, root)
    }
    fn get_head(&self) -> Result<ForkChoiceNode, StoreError> {
        crate::gossip_view::GossipView::get_head(&self.graph)
    }
    fn checkpoints(&self) -> (Checkpoint, Checkpoint) {
        crate::gossip_view::GossipView::checkpoints(&self.graph)
    }
    fn genesis_time_ms(&self) -> u64 {
        crate::gossip_view::GossipView::genesis_time_ms(&self.graph)
    }
    fn slot_duration_ms(&self) -> u64 {
        crate::gossip_view::GossipView::slot_duration_ms(&self.graph)
    }
    fn slots_per_epoch(&self) -> u64 {
        crate::gossip_view::GossipView::slots_per_epoch(&self.graph)
    }
    fn transition_parameters(&self) -> Option<&TransitionParameters> {
        crate::gossip_view::GossipView::transition_parameters(&self.graph)
    }
    fn store_parameters(&self) -> StoreParameters {
        crate::gossip_view::GossipView::store_parameters(&self.graph)
    }
    fn execution_status(&self, root: Root) -> ExecutionStatus {
        crate::gossip_view::GossipView::execution_status(&self.graph, root)
    }
    fn get_ancestor(&self, root: Root, slot: u64) -> Result<Root, StoreError> {
        crate::gossip_view::GossipView::get_ancestor(&self.graph, root, slot)
    }
    fn get_checkpoint_block(&self, root: Root, epoch: u64) -> Result<Root, StoreError> {
        crate::gossip_view::GossipView::get_checkpoint_block(&self.graph, root, epoch)
    }
}
impl CompactStore {
    /// Prepare the actual head/checkpoint owners for the unchanged sealed confirmation algorithm.
    pub fn prepare_fast_confirmation(
        &self,
        checkpoints: &[Checkpoint],
    ) -> Result<crate::compact::ClassicConfirmationView, StoreError> {
        if checkpoints.len().checked_add(1).ok_or(StoreError::Overflow)? >
            self.limits.max_prepared_reads
        {
            return Err(StoreError::ResourceLimit)
        }
        let head = self.graph.get_head()?.root;
        let read = self.graph.read_block_state(head)?.ok_or(StoreError::UnknownRoot)?;
        let mut prepared = crate::compact::ClassicConfirmationView {
            graph: self.graph.clone(),
            states: BTreeMap::from([(head, read)]),
            checkpoints: BTreeMap::new(),
        };
        for &checkpoint in checkpoints {
            let read =
                self.graph.read_checkpoint_state(checkpoint)?.ok_or(StoreError::UnknownRoot)?;
            prepared.checkpoints.insert((checkpoint.epoch, checkpoint.root), read);
        }
        Ok(prepared)
    }
}

impl Store {
    /// Read original block roots present in this actual retained native graph.
    pub fn retained_block_roots(&self) -> impl Iterator<Item = Root> + '_ {
        self.blocks.keys().copied()
    }
}

mod availability_retention;

impl CompactStore {
    /// Mint only the actual successful retained-body slashing's verification context.
    pub fn on_retained_block_slashing_with_context(
        &mut self,
        root: Root,
        index: usize,
    ) -> Result<crate::slashing_replay::SlashingContext, StoreError> {
        let mut candidate = self.clone();
        candidate.graph.on_retained_block_slashing(root, index)?;
        let context = candidate.graph.retained_slashing_context()?;
        candidate.finish(self)?;
        Ok(context)
    }
}

mod optimistic_import;

/// The caller supplies the exact origin read whose complete binding it independently checked.
/// Owning reads keep immutable allocations alive; different allocations always hash completely.
fn cooling_checkpoint_binding(
    origin: &StateRead,
    origin_binding: &StateBinding,
    epoch: u64,
    read: &StateRead,
) -> Result<StateBinding, StoreError> {
    if std::ptr::eq(read.as_ref(), origin.as_ref()) {
        return Ok(origin_binding.clone())
    }
    let derived = StateBinding::checkpoint(origin_binding, epoch, read.data())?;
    if derived.slot() == origin_binding.slot() &&
        derived.state_root() == origin_binding.state_root()
    {
        Ok(origin_binding.clone())
    } else {
        Ok(derived)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compact::{StateData, StateSourceIdentity};
    use reth_embedded_beacon_state_transition::ssz::Ssz;

    #[test]
    fn checkpoint_cooling_reuses_only_exact_origin_and_checks_replacement_data() {
        let state = Arc::new(BeaconState::Phase0(
            phase0::BeaconState::default_value(Preset::Minimal).unwrap(),
            Preset::Minimal,
        ));
        let identity =
            StateSourceIdentity { profile_digest: [1; 32], consensus_config_digest: [2; 32] };
        let binding = StateBinding::admitted(
            [3; 32],
            state.hash_tree_root().unwrap(),
            StateData::Classic(&state),
            identity,
        )
        .unwrap();
        let origin = StateRead::hot(state.clone(), Preset::Minimal);
        assert_eq!(cooling_checkpoint_binding(&origin, &binding, 0, &origin).unwrap(), binding);
        let copied = StateRead::hot(Arc::new(state.as_ref().clone()), Preset::Minimal);
        assert_eq!(cooling_checkpoint_binding(&origin, &binding, 0, &copied).unwrap(), binding);

        let mut changed = state.as_ref().clone();
        let BeaconState::Phase0(inner, _) = &mut changed else { unreachable!() };
        inner.balances.values.push(42);
        let replacement = StateRead::hot(Arc::new(changed), Preset::Minimal);
        let derived = cooling_checkpoint_binding(&origin, &binding, 0, &replacement).unwrap();
        assert_eq!(derived.checkpoint_epoch(), Some(0));
        assert_ne!(derived.state_root(), binding.state_root());
        assert_eq!(derived.state_root(), replacement.hash_tree_root().unwrap());

        let mut foreign = state.as_ref().clone();
        let BeaconState::Phase0(inner, _) = &mut foreign else { unreachable!() };
        inner.genesis_validators_root = [4; 32];
        let foreign = StateRead::hot(Arc::new(foreign), Preset::Minimal);
        assert!(matches!(
            cooling_checkpoint_binding(&origin, &binding, 0, &foreign),
            Err(StoreError::CompactState(StateSourceError::BindingMismatch))
        ));
    }
}
