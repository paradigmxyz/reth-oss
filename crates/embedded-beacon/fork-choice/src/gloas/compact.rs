//! Compact Gloas graph, retaining actual typed history and owning every cold-state read.

use super::*;
use crate::{
    compact::{
        AdmittedSnapshot, AdmittedState, CompactGraphLimits, CompactGraphStats, CompactStateSource,
        CompactStore, GloasStateRead, HotBudget, RetainedState, SignedBlockData, StateBinding,
        StateData, StateRead, StateSourceError, VerifiedEnvelope,
    },
    ExecutionStatus,
};
use reth_embedded_beacon_primitives::Fork;

/// Strict Gloas payload graph backed by durable authenticated state leases.
///
/// Classic history remains in its real schemas. Pending, empty and full payload authority is
/// retained exclusively by the shared strict graph; decoded data/cache membership grants none.
#[derive(Clone, Debug)]
pub struct CompactGloasStore {
    pub(super) graph: GloasStore,
    pub(super) source: Arc<dyn CompactStateSource>,
    pub(super) limits: CompactGraphLimits,
    pub(super) hot_budget: HotBudget,
    pub(super) execution_cut: Option<super::compact_retention::ExecutionCut>,
}
impl CompactGloasStore {
    /// Consume an actually admitted Gloas graph with already durable block/state receipts.
    pub fn from_store(
        graph: GloasStore,
        source: Arc<dyn CompactStateSource>,
        limits: CompactGraphLimits,
    ) -> Result<Self, StoreError> {
        limits.validate()?;
        let mut compact = Self {
            graph,
            execution_cut: None,
            source,
            limits,
            hot_budget: HotBudget::new(limits.max_resident_hot_state_bytes),
        };
        compact.cool(limits.max_hot_state_bytes)?;
        compact.publish()?;
        Ok(compact)
    }
    /// Preserve actual classic graph authority while importing the real first signed Gloas child.
    pub async fn migrate_from_classic<
        E: reth_embedded_beacon_state_transition::fork_processing::ExecutionEngine,
    >(
        previous: &CompactStore,
        signed: &reth_embedded_beacon_state_transition::ssz::SignedBeaconBlock,
        engine: &E,
    ) -> Result<Self, StoreError> {
        let graph = GloasStore::migrate_from_classic(&previous.graph, signed, engine).await?;
        let mut compact = Self {
            graph,
            execution_cut: None,
            source: previous.source.clone(),
            limits: previous.limits,
            hot_budget: previous.hot_budget.clone(),
        };
        let reth_embedded_beacon_state_transition::ssz::SignedBeaconBlock::Gloas(signed, preset) =
            signed
        else {
            return Err(StoreError::AnchorMismatch)
        };
        if *preset != compact.graph.processing.preset() {
            return Err(StoreError::AnchorMismatch)
        }
        compact.retain_block(signed)?;
        compact.cool(compact.limits.max_hot_state_bytes)?;
        compact.publish()?;
        Ok(compact)
    }
    /// Own a typed Gloas post-state; a known damaged cold root errors rather than disappearing.
    pub fn read_block_state(&self, root: Root) -> Result<Option<GloasStateRead>, StoreError> {
        self.graph.read_block_state(root)
    }
    /// Own a genuine earlier-schema state retained through authenticated migration.
    pub fn read_historical_block_state(&self, root: Root) -> Result<Option<StateRead>, StoreError> {
        self.graph.historical.as_ref().map_or(Ok(None), |h| h.read_block_state(root))
    }
    /// Own an exact typed Gloas checkpoint state; historical checkpoints retain their schema.
    pub fn read_checkpoint_state(
        &self,
        checkpoint: Checkpoint,
    ) -> Result<Option<GloasStateRead>, StoreError> {
        self.graph
            .checkpoint_states
            .get(&(checkpoint.epoch, checkpoint.root))
            .map(|s| s.acquire(self.graph.processing.preset()))
            .transpose()
    }
    /// Actual current head and payload branch.
    pub fn get_head(&self) -> Result<ForkChoiceNode, StoreError> {
        self.graph.get_head()
    }
    /// Actual canonical checkpoints/head derived from the strict graph.
    pub fn snapshot(&self) -> Result<StoreSnapshot, StoreError> {
        self.graph.snapshot()
    }
    /// Actual event clock used by the private admitted graph, in milliseconds.
    pub const fn time_ms(&self) -> u64 {
        self.graph.time_ms()
    }
    /// Exact retained branch checkpoint root at the selected epoch boundary.
    /// Missing ancestry and resource failures remain explicit strict graph errors.
    pub fn get_checkpoint_block(&self, root: Root, epoch: u64) -> Result<Root, StoreError> {
        self.graph.get_checkpoint_block(root, epoch)
    }
    /// Checked clock slot.
    pub const fn current_slot(&self) -> u64 {
        self.graph.current_slot()
    }
    /// Actual strict Engine-verified association for the chosen payload branch.
    pub fn verified_execution_block_hash(
        &self,
        node: ForkChoiceNode,
    ) -> Result<Option<Root>, StoreError> {
        match self.graph.verified_execution_block_hash(node) {
            Err(StoreError::UnknownRoot)
                if self.execution_cut.is_some() &&
                    node.payload_status == Some(PayloadStatus::Empty) =>
            {
                self.execution_hash_after_cut(node)
            }
            result => result,
        }
    }
    /// Actual finalized bid execution association.
    pub fn finalized_execution_block_hash(&self) -> Result<Option<Root>, StoreError> {
        self.graph.finalized_execution_block_hash()
    }
    /// Actual locally admitted payload; no reconstructed state can create this evidence.
    pub fn is_payload_verified(&self, root: Root) -> bool {
        self.graph.is_payload_verified(root)
    }
    /// Actual latest signed vote preserved by the private graph.
    pub fn latest_message(&self, index: u64) -> Option<LatestMessage> {
        self.graph.latest_message(index)
    }
    /// Actual signed timeliness votes held by the private admitted payload graph.
    pub fn payload_timeliness_votes(&self, root: Root) -> Option<&[Option<bool>]> {
        self.graph.payload_timeliness_votes(root)
    }
    /// Actual signed availability votes; retained bytes alone never establish these votes.
    pub fn payload_data_availability_votes(&self, root: Root) -> Option<&[Option<bool>]> {
        self.graph.payload_data_availability_votes(root)
    }
    /// Immutable Gloas block data.
    pub fn block(&self, root: Root) -> Option<&BeaconBlock> {
        self.graph.blocks.get(&root).map(AsRef::as_ref)
    }
    /// Release decoded states only, retaining all admitted ancestry and every unfinalized branch.
    pub fn evict_hot_states(&mut self) -> Result<(), StoreError> {
        let mut candidate = self.clone();
        candidate.cool(0)?;
        candidate.publish()?;
        *self = candidate;
        Ok(())
    }
    /// Unique decoded allocations; source-backed leases are measured by the source cache.
    pub fn stats(&self) -> Result<CompactGraphStats, StoreError> {
        let mut seen = BTreeSet::new();
        let (mut hot_schema_bytes, mut cold_entries, mut cold_binding_heap_bytes) =
            if let Some(history) = &self.graph.historical {
                let historical = CompactStore {
                    graph: history.as_ref().clone(),
                    source: self.source.clone(),
                    limits: self.limits,
                    hot_budget: self.hot_budget.clone(),
                    optimistic: None,
                }
                .stats()?;
                (
                    historical.hot_schema_bytes,
                    historical.cold_entries,
                    historical.cold_binding_heap_bytes,
                )
            } else {
                (0usize, 0usize, 0usize)
            };
        for entry in self.graph.block_states.values().chain(self.graph.checkpoint_states.values()) {
            if let Some(state) = entry.hot() {
                if seen.insert(Arc::as_ptr(state) as usize) {
                    hot_schema_bytes = hot_schema_bytes
                        .checked_add(
                            StateData::Gloas(state, self.graph.processing.preset())
                                .allocation_bytes()?,
                        )
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
    /// Same strict clock/checkpoint realization kernel, with durable publication before graph swap.
    pub fn on_tick(&mut self, time_ms: u64) -> Result<(), StoreError> {
        self.event(|graph| graph.on_tick(time_ms))
    }
    /// Same production signed beacon transition and committed post-state verification.
    pub fn on_block(&mut self, signed: &SignedBeaconBlock) -> Result<(), StoreError> {
        let mut candidate = self.clone();
        candidate.graph.on_block(signed)?;
        candidate.retain_block(signed)?;
        candidate.finish(self)
    }
    /// Same strict complete signed block and body-observation path.
    pub fn on_block_and_contents(&mut self, signed: &SignedBeaconBlock) -> Result<(), StoreError> {
        let mut candidate = self.clone();
        candidate.graph.on_block_and_contents(signed)?;
        candidate.retain_block(signed)?;
        candidate.finish(self)
    }
    /// Same genuine signed committee vote; missing known cold data errors before graph mutation.
    pub fn on_attestation(&mut self, attestation: &gloas::Attestation) -> Result<(), StoreError> {
        self.event(|graph| graph.on_attestation(attestation))
    }
    /// Same genuine indexed slashing validation.
    pub fn on_attester_slashing(
        &mut self,
        slashing: &gloas::AttesterSlashing,
    ) -> Result<(), StoreError> {
        self.event(|graph| graph.on_attester_slashing(slashing))
    }
    /// Same actual payload committee message; a lease never supplies payload authority.
    pub fn on_payload_attestation_message(
        &mut self,
        message: &PayloadAttestationMessage,
    ) -> Result<(), StoreError> {
        self.event(|graph| graph.on_payload_attestation_message(message))
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
    /// Authenticate the genuine retained block body item.
    pub fn on_retained_block_attestation(
        &mut self,
        root: Root,
        index: usize,
    ) -> Result<(), StoreError> {
        self.event(|graph| graph.on_retained_block_attestation(root, index))
    }

    /// Validate actual retained body votes in order and publish one checked graph candidate.
    /// Independent rejections preserve prior verified votes; infrastructure failures stop early.
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
    /// Authenticate the genuine retained block body slashing.
    pub fn on_retained_block_slashing(
        &mut self,
        root: Root,
        index: usize,
    ) -> Result<(), StoreError> {
        self.event(|graph| graph.on_retained_block_slashing(root, index))
    }
    /// Mandatory actual signature, Engine and custody verification, followed by durable evidence.
    pub async fn on_execution_payload_envelope<E: ExecutionEngine, D: DataAvailability>(
        &mut self,
        signed: &SignedExecutionPayloadEnvelope,
        engine: &E,
        availability: &D,
    ) -> Result<(), EnvelopeError> {
        let mut candidate = self.clone();
        candidate.graph.on_execution_payload_envelope(signed, engine, availability).await?;
        let root = signed.message.beacon_block_root;
        let state = candidate.graph.read_block_state(root)?.ok_or(StoreError::UnknownRoot)?;
        let binding = candidate.block_binding(root, &state)?;
        let hash = candidate
            .graph
            .verified_execution_block_hash(ForkChoiceNode {
                root,
                payload_status: Some(PayloadStatus::Full),
            })?
            .ok_or(StateSourceError::BindingMismatch)
            .map_err(StoreError::from)?;
        let admitted =
            VerifiedEnvelope { binding: &binding, signed, verified_execution_hash: hash };
        candidate.source.retain_envelope(admitted).map_err(StoreError::from)?;
        availability
            .retain_verified_envelope(&admitted)
            .map_err(|error| EnvelopeError::DataAvailability(error.to_string()))?;
        candidate.finish(self)?;
        Ok(())
    }
    fn event(
        &mut self,
        operation: impl FnOnce(&mut GloasStore) -> Result<(), StoreError>,
    ) -> Result<(), StoreError> {
        let mut candidate = self.clone();
        operation(&mut candidate.graph)?;
        candidate.finish(self)
    }
    pub(super) fn finish(mut self, previous: &mut Self) -> Result<(), StoreError> {
        self.cool(self.limits.max_hot_state_bytes)?;
        self.publish()?;
        *previous = self;
        Ok(())
    }
    fn publish(&self) -> Result<(), StoreError> {
        self.source.publish_snapshot(AdmittedSnapshot {
            snapshot: self.graph.snapshot()?,
            time_ms: self.graph.time_ms(),
            retained_roots: self.graph.retained_roots().collect(),
            verified_envelope_roots: self.graph.payloads.keys().copied().collect(),
            identity: self.source.identity(),
        })?;
        Ok(())
    }
    fn retain_block(&self, signed: &SignedBeaconBlock) -> Result<(), StoreError> {
        let root = signed.message.hash_tree_root(self.graph.processing.preset())?;
        let state = self.graph.read_block_state(root)?.ok_or(StoreError::UnknownRoot)?;
        let binding = self.block_binding(root, &state)?;
        self.source.retain_block(AdmittedState {
            binding: &binding,
            state: state.data(),
            signed: SignedBlockData::Gloas(signed, self.graph.processing.preset()),
            execution_optimistic: false,
            shared_state: None,
            verified_execution_hash: self.graph.verified_execution_block_hash(ForkChoiceNode {
                root,
                payload_status: Some(PayloadStatus::Pending),
            })?,
        })?;
        Ok(())
    }
    fn block_binding(
        &self,
        root: Root,
        state: &GloasStateRead,
    ) -> Result<StateBinding, StoreError> {
        let block = self.graph.blocks.get(&root).ok_or(StoreError::UnknownRoot)?;
        if block.slot != state.slot {
            return Err(StateSourceError::BindingMismatch.into())
        }
        if let Some(owner) = state.hot_arc() {
            return self.hot_budget.gloas_binding(
                &owner,
                self.graph.processing.preset(),
                root,
                block.state_root,
                self.source.identity(),
            )
        }
        if let Some(entry) = self.graph.block_states.get(&root) &&
            let Some(binding) = entry.verified_binding_for(state)?
        {
            if binding.origin_block_root() != root ||
                binding.state_root() != block.state_root ||
                binding.origin_state_root() != block.state_root ||
                binding.checkpoint_epoch().is_some() ||
                binding.identity() != self.source.identity()
            {
                return Err(StateSourceError::BindingMismatch.into())
            }
            return Ok(binding)
        }
        StateBinding::admitted(root, block.state_root, state.data(), self.source.identity())
    }
    fn cool(&mut self, maximum: usize) -> Result<(), StoreError> {
        if let Some(history) = &self.graph.historical {
            let mut historical = CompactStore {
                graph: history.as_ref().clone(),
                source: self.source.clone(),
                limits: self.limits,
                hot_budget: self.hot_budget.clone(),
                optimistic: None,
            };
            historical.cool(0)?;
            self.graph.historical = Some(Arc::new(historical.graph));
        }
        let preset = self.graph.processing.preset();
        let mut ordered = self
            .graph
            .block_states
            .iter()
            .filter_map(|(&root, entry)| entry.hot().map(|s| (s.slot, root, s.clone())))
            .collect::<Vec<_>>();
        ordered.sort_unstable_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
        let mut hot_bytes = 0usize;
        let mut kept = BTreeSet::new();
        for (_, root, state) in ordered {
            self.hot_budget.gloas(&state, preset)?;
            let bytes = StateData::Gloas(&state, preset).allocation_bytes()?;
            let next = hot_bytes.checked_add(bytes).ok_or(StoreError::Overflow)?;
            if next <= maximum && kept.insert(Arc::as_ptr(&state) as usize) {
                hot_bytes = next;
                continue
            }
            let read = GloasStateRead::hot(state, preset);
            let binding = self.block_binding(root, &read)?;
            self.graph.block_states.insert(
                root,
                RetainedState::cold_from_checked_read(binding, self.source.clone(), &read)?,
            );
        }
        let checkpoints = self
            .graph
            .checkpoint_states
            .iter()
            .filter_map(|(&key, entry)| entry.hot().map(|s| (key, s.clone())))
            .collect::<Vec<_>>();
        for ((epoch, root), state) in checkpoints {
            self.hot_budget.gloas(&state, preset)?;
            let pointer = Arc::as_ptr(&state) as usize;
            if kept.contains(&pointer) {
                continue
            }
            let bytes = StateData::Gloas(&state, preset).allocation_bytes()?;
            let next = hot_bytes.checked_add(bytes).ok_or(StoreError::Overflow)?;
            if next <= maximum {
                kept.insert(pointer);
                hot_bytes = next;
                continue
            }
            let origin = self.graph.read_block_state(root)?.ok_or(StoreError::UnknownRoot)?;
            let origin_binding = self.block_binding(root, &origin)?;
            let read = GloasStateRead::hot(state, preset);
            let binding = if read.slot == origin_binding.slot() &&
                read.hash_tree_root()? == origin_binding.state_root()
            {
                origin_binding
            } else {
                let derived = StateBinding::checkpoint(&origin_binding, epoch, read.data())?;
                self.source.retain_checkpoint(&derived, read.data())?;
                derived
            };
            self.graph.checkpoint_states.insert(
                (epoch, root),
                RetainedState::cold_from_checked_read(binding, self.source.clone(), &read)?,
            );
        }
        self.graph.check_budget()?;
        Ok(())
    }
}

impl crate::compact::owned_views::sealed::GloasSealed for CompactGloasStore {}
impl crate::compact::ReadGloasGossipView for CompactGloasStore {
    fn read_block_state(
        &self,
        root: Root,
    ) -> Result<Option<crate::compact::StateAccess<'_, BeaconState>>, StoreError> {
        self.graph.read_block_state(root).map(|s| s.map(crate::compact::StateAccess::Owned))
    }
    fn read_historical_block_state(
        &self,
        root: Root,
    ) -> Result<
        Option<
            crate::compact::StateAccess<
                '_,
                reth_embedded_beacon_state_transition::ssz::BeaconState,
            >,
        >,
        StoreError,
    > {
        self.read_historical_block_state(root).map(|s| s.map(crate::compact::StateAccess::Owned))
    }
    fn finalized_checkpoint(&self) -> Checkpoint {
        super::gossip_view::GloasGossipView::finalized_checkpoint(&self.graph)
    }
    fn replay_finalized_checkpoint(&self, root: Root) -> Option<Checkpoint> {
        super::gossip_view::GloasGossipView::replay_finalized_checkpoint(&self.graph, root)
    }
    fn store_parameters(&self) -> StoreParameters {
        super::gossip_view::GloasGossipView::store_parameters(&self.graph)
    }
    fn processing_parameters(&self) -> &GloasParameters {
        super::gossip_view::GloasGossipView::processing_parameters(&self.graph)
    }
    fn block(&self, root: Root) -> Option<&BeaconBlock> {
        super::gossip_view::GloasGossipView::block(&self.graph, root)
    }
    fn historical_block(
        &self,
        root: Root,
    ) -> Option<&reth_embedded_beacon_state_transition::ssz::BeaconBlock> {
        super::gossip_view::GloasGossipView::historical_block(&self.graph, root)
    }
    fn historical_transition_parameters(
        &self,
    ) -> Option<&reth_embedded_beacon_state_transition::fork_processing::TransitionParameters> {
        super::gossip_view::GloasGossipView::historical_transition_parameters(&self.graph)
    }
    fn node_for_root(&self, root: Root) -> Result<ForkChoiceNode, StoreError> {
        super::gossip_view::GloasGossipView::node_for_root(&self.graph, root)
    }
    fn payload(&self, root: Root) -> Option<&gloas::ExecutionPayloadEnvelope> {
        super::gossip_view::GloasGossipView::payload(&self.graph, root)
    }
    fn execution_status(&self, root: Root) -> ExecutionStatus {
        super::gossip_view::GloasGossipView::execution_status(&self.graph, root)
    }
    fn get_head(&self) -> Result<ForkChoiceNode, StoreError> {
        super::gossip_view::GloasGossipView::get_head(&self.graph)
    }
    fn get_ancestor(&self, node: ForkChoiceNode, slot: u64) -> Result<ForkChoiceNode, StoreError> {
        super::gossip_view::GloasGossipView::get_ancestor(&self.graph, node, slot)
    }
    fn is_valid_dependent_root(&self, root: Root, slot: u64) -> Result<bool, StoreError> {
        super::gossip_view::GloasGossipView::is_valid_dependent_root(&self.graph, root, slot)
    }
    fn should_build_on_full(&self, head: ForkChoiceNode, slot: u64) -> Result<bool, StoreError> {
        super::gossip_view::GloasGossipView::should_build_on_full(&self.graph, head, slot)
    }
    fn genesis_time_ms(&self) -> u64 {
        super::gossip_view::GloasGossipView::genesis_time_ms(&self.graph)
    }
    fn slot_duration_ms(&self) -> u64 {
        super::gossip_view::GloasGossipView::slot_duration_ms(&self.graph)
    }
    fn slots_per_epoch(&self) -> u64 {
        super::gossip_view::GloasGossipView::slots_per_epoch(&self.graph)
    }
}
impl CompactGloasStore {
    /// Own the required head/checkpoint reads before invoking borrowed confirmation.
    pub fn prepare_fast_confirmation(
        &self,
        checkpoints: &[Checkpoint],
    ) -> Result<crate::compact::GloasConfirmationView, StoreError> {
        if checkpoints.len().checked_add(1).ok_or(StoreError::Overflow)? >
            self.limits.max_prepared_reads
        {
            return Err(StoreError::ResourceLimit)
        }
        let head = self.graph.get_head()?.root;
        let read = self.graph.state_view(head)?;
        let mut prepared = crate::compact::GloasConfirmationView {
            graph: self.graph.clone(),
            states: BTreeMap::from([(head, read)]),
            checkpoints: BTreeMap::new(),
        };
        for &checkpoint in checkpoints {
            let read = self.graph.checkpoint_view(checkpoint)?;
            prepared.checkpoints.insert((checkpoint.epoch, checkpoint.root), read);
        }
        Ok(prepared)
    }
}

impl GloasStore {
    /// Read real historical and Gloas block roots present in this retained native graph.
    pub fn retained_block_roots(&self) -> impl Iterator<Item = Root> + '_ {
        self.retained_roots()
    }
    /// Read independently verified envelope roots present in this actual native graph.
    pub fn verified_envelope_roots(&self) -> impl Iterator<Item = Root> + '_ {
        self.payloads.keys().copied()
    }
}

impl CompactGloasStore {
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
