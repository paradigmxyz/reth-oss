//! Actual finalized Gloas retention with separately proven EMPTY execution ancestry.
use super::*;
use crate::compact::{
    CompactGloasStore, CompactStore, FinalizedRetention, FinalizedRetentionPolicy, PrunedGraph,
    RetainedExecutionAssociation, RetainedState, StateBinding, StateData, StateSourceError,
    UnresolvedExecutionAssociation,
};
use std::collections::BTreeSet;

/// Exact immutable cut proven by real pre-cut EMPTY traversal, never a payload receipt.
#[derive(Clone, Debug)]
pub(super) struct ExecutionCut {
    pub(super) root: Root,
    pub(super) state_root: Root,
    pub(super) parent_hash: Option<Root>,
}

impl CompactGloasStore {
    /// Read the exact eligible finalized cut before preparing a node-owned execution boundary.
    /// This selects actual retained consensus ancestry and grants no EL validity or payload status.
    pub fn finalized_retention_boundary(
        &self,
        policy: FinalizedRetentionPolicy,
    ) -> Result<Option<Checkpoint>, StoreError> {
        let actual = self.graph.snapshot()?;
        if actual.finalized_checkpoint.epoch <= policy.epochs() {
            return Ok(None)
        }
        let maximum_epoch = actual.finalized_checkpoint.epoch - policy.epochs();
        let epoch = self
            .source
            .retention_epoch_limit()?
            .map_or(maximum_epoch, |floor| floor.min(maximum_epoch));
        if epoch == 0 {
            return Ok(None)
        }
        let target = epoch.checked_mul(self.graph.slots_per_epoch()).ok_or(StoreError::Overflow)?;
        let first_slot = self.graph.retained_roots().try_fold(u64::MAX, |minimum, root| {
            Ok::<_, StoreError>(minimum.min(self.graph.block_header(root)?.slot))
        })?;
        // A genuine checkpoint constructor need not retain pre-checkpoint ancestry.
        if target < first_slot {
            return Ok(None)
        }
        let root = self.graph.get_checkpoint_block(actual.finalized_checkpoint.root, epoch)?;
        Ok(Some(Checkpoint { epoch, root }))
    }

    /// Advance finalized ancestry only with a strict independently proven EMPTY execution base.
    /// Missing original execution ancestry remains an error before any candidate graph cut.
    pub fn prune_finalized_history(
        &mut self,
        policy: FinalizedRetentionPolicy,
    ) -> Result<PrunedGraph, StoreError> {
        self.prune_finalized_inner(policy, false)
    }

    /// Advance proven consensus finality while explicitly preserving an unresolved trusted EL base.
    /// A node must prepare its sealed old-boundary-to-cut EMPTY receipt before calling this method.
    /// This method grants no execution authority: missing proof remains `UnknownRoot` in strict
    /// reads.
    pub fn prune_finalized_consensus_history(
        &mut self,
        policy: FinalizedRetentionPolicy,
    ) -> Result<PrunedGraph, StoreError> {
        self.prune_finalized_inner(policy, true)
    }

    fn prune_finalized_inner(
        &mut self,
        policy: FinalizedRetentionPolicy,
        consensus_only: bool,
    ) -> Result<PrunedGraph, StoreError> {
        let actual = self.graph.snapshot()?;
        let count = self.graph.block_count();
        let Some(boundary) = self.finalized_retention_boundary(policy)? else {
            return Ok(PrunedGraph { retained_blocks: count, ..Default::default() })
        };
        let epoch = boundary.epoch;
        let root = boundary.root;
        let mut candidate = self.clone();
        let genesis = {
            let state = candidate.graph.state_view(actual.head.root)?;
            (state.data().genesis_validators_root(), candidate.graph.genesis_time_ms / 1000)
        };
        let origin = candidate.retention_origin(root, genesis)?;
        let execution = if candidate.graph.blocks.contains_key(&root) {
            match candidate.verified_execution_block_hash(node(root, PayloadStatus::Empty)) {
                Ok(hash) => RetainedExecutionAssociation::VerifiedEmpty { hash },
                Err(StoreError::UnknownRoot) if consensus_only => {
                    RetainedExecutionAssociation::UnresolvedTrustedBoundary(
                        candidate.unresolved_execution_retention(root)?,
                    )
                }
                Err(error) => return Err(error),
            }
        } else {
            // Classic associations still require the genuine strict retained execution path.
            candidate
                .graph
                .verified_execution_block_hash(ForkChoiceNode { root, payload_status: None })?;
            RetainedExecutionAssociation::Classic
        };
        if !candidate.graph.blocks.contains_key(&root) {
            let history = candidate.graph.historical.as_mut().ok_or(StoreError::UnknownRoot)?;
            Arc::make_mut(history).restore_retention_origin(&origin, candidate.source.clone())?;
        }
        candidate.graph.ensure_checkpoint::<true>(boundary)?;
        let state = candidate.graph.checkpoint_view(boundary)?;
        let binding = StateBinding::checkpoint(&origin, epoch, state.data())?;
        candidate.source.retain_checkpoint(&binding, state.data())?;
        let boundary_slot = candidate.graph.block_header(root)?.slot;
        let mut retained = BTreeSet::new();
        let boundary_node = candidate.graph.root_node(root, PayloadStatus::Pending)?;
        for block_root in candidate.graph.retained_roots() {
            if candidate.graph.block_header(block_root)?.slot >= boundary_slot &&
                candidate.graph.is_ancestor(
                    candidate.graph.root_node(block_root, PayloadStatus::Pending)?,
                    boundary_node,
                )?
            {
                retained.insert(block_root);
            }
        }
        for needed in [
            actual.head.root,
            actual.justified_checkpoint.root,
            actual.finalized_checkpoint.root,
            candidate.graph.unrealized_justified.root,
            candidate.graph.unrealized_finalized.root,
        ] {
            if !retained.contains(&needed) {
                return Err(StoreError::ConflictsWithFinality)
            }
        }
        let states = candidate.retention_bindings(&retained, epoch, genesis)?;
        candidate.source.retain_finalized(FinalizedRetention {
            boundary,
            origin: &origin,
            binding: &binding,
            state: state.data(),
            roots: &retained,
            states: &states,
            snapshot: actual,
            execution,
        })?;
        if candidate.graph.blocks.contains_key(&root) {
            // This is now the genuine admitted retained anchor for optional lookahead/boost.
            // Its independently proven EMPTY base stays separate from FULL/payload authority.
            candidate.graph.execution_query_anchor = Some(root);
            candidate.execution_cut = match execution {
                RetainedExecutionAssociation::VerifiedEmpty { hash } => Some(ExecutionCut {
                    root,
                    state_root: origin.origin_state_root(),
                    parent_hash: hash,
                }),
                RetainedExecutionAssociation::UnresolvedTrustedBoundary(_) => None,
                RetainedExecutionAssociation::Classic => {
                    return Err(StateSourceError::BindingMismatch.into())
                }
            };
        }
        candidate.graph.blocks.retain(|r, _| retained.contains(r));
        candidate.graph.block_states.retain(|r, _| retained.contains(r));
        candidate.graph.checkpoint_states.retain(|(e, r), _| *e >= epoch && retained.contains(r));
        candidate.graph.block_timeliness.retain(|r, _| retained.contains(r));
        candidate.graph.unrealized_justifications.retain(|r, _| retained.contains(r));
        candidate.graph.payloads.retain(|r, _| retained.contains(r));
        // A skipped checkpoint retains its original pre-boundary block, but observations
        // before the constructor's epoch clock are excluded from original-event replay.
        let vote_slot =
            epoch.checked_mul(candidate.graph.slots_per_epoch()).ok_or(StoreError::Overflow)?;
        let vote_roots = candidate
            .graph
            .blocks
            .iter()
            .filter_map(|(root, block)| (block.slot >= vote_slot).then_some(*root))
            .collect::<BTreeSet<_>>();
        candidate.graph.timeliness_votes.retain(|r, _| retained.contains(r));
        candidate.graph.availability_votes.retain(|r, _| retained.contains(r));
        for (root, votes) in candidate
            .graph
            .timeliness_votes
            .iter_mut()
            .chain(candidate.graph.availability_votes.iter_mut())
        {
            if !vote_roots.contains(root) {
                // Keep native row shape for real PTC evidence in later retained signed bodies.
                votes.fill(None);
            }
        }
        if let Some(history) = &mut candidate.graph.historical {
            Arc::make_mut(history).retain_graph_roots(&retained, epoch);
            let empty = history.migration_roots().next().is_none();
            if empty {
                candidate.graph.historical = None;
            }
        }
        if candidate.graph.proposer_boost_root != [0; 32] &&
            !retained.contains(&candidate.graph.proposer_boost_root)
        {
            candidate.graph.proposer_boost_root = [0; 32];
        }
        // Latest signed slots remain private replay-protection tombstones.
        if candidate.graph.snapshot()? != actual {
            return Err(StateSourceError::BindingMismatch.into())
        }
        candidate.finish(self)?;
        Ok(PrunedGraph {
            removed_blocks: count - retained.len(),
            retained_blocks: retained.len(),
            boundary: Some(boundary),
        })
    }

    // Structural commitments only: stop at the exact private trusted execution boundary.
    // A previously verified cut cannot be downgraded to unresolved after losing genuine data.
    fn unresolved_execution_retention(
        &self,
        cut_root: Root,
    ) -> Result<UnresolvedExecutionAssociation, StoreError> {
        if self.execution_cut.is_some() {
            return Err(StoreError::UnknownRoot)
        }
        let anchor = self.graph.execution_query_anchor.ok_or(StoreError::UnknownRoot)?;
        let count = self.graph.block_count();
        if count == 0 || count > self.graph.parameters.max_blocks {
            return Err(StoreError::ResourceLimit)
        }
        let cut = self.graph.block(cut_root)?;
        let cut_state_root = cut.state_root;
        let expected = cut.body.signed_execution_payload_bid.message.parent_block_hash;
        let mut root = cut_root;
        let mut block = cut;
        for _ in 0..count {
            let state = self.graph.read_block_state(root)?.ok_or(StoreError::UnknownRoot)?;
            let bid = &block.body.signed_execution_payload_bid.message;
            if state.slot != block.slot ||
                state.latest_block_header.slot != block.slot ||
                state.latest_block_header.parent_root != block.parent_root ||
                state.latest_block_header.proposer_index != block.proposer_index ||
                state.latest_execution_payload_bid != *bid ||
                state.latest_block_hash != expected ||
                bid.parent_block_hash != expected
            {
                return Err(StateSourceError::BindingMismatch.into())
            }
            if root == anchor {
                if self.graph.blocks.contains_key(&block.parent_root) ||
                    self.graph
                        .historical
                        .as_ref()
                        .is_some_and(|history| history.block(block.parent_root).is_some())
                {
                    return Err(StoreError::UnknownRoot)
                }
                return Ok(UnresolvedExecutionAssociation {
                    origin_block_root: root,
                    origin_state_root: block.state_root,
                    origin_slot: block.slot,
                    cut_block_root: cut_root,
                    cut_state_root,
                    parent_hash: expected,
                })
            }
            let parent = self.graph.block(block.parent_root)?;
            if parent.slot >= block.slot {
                return Err(StoreError::InvalidBlockTime)
            }
            // An unverified FULL decision or a missing non-boundary parent remains an error.
            let parent_bid = &parent.body.signed_execution_payload_bid.message;
            let selects_unverified_parent_payload = bid.parent_block_hash == parent_bid.block_hash;
            let changes_carried_parent_payload =
                bid.parent_block_hash != parent_bid.parent_block_hash;
            if selects_unverified_parent_payload || changes_carried_parent_payload {
                return Err(StoreError::UnknownRoot)
            }
            root = block.parent_root;
            block = parent;
        }
        Err(StoreError::ResourceLimit)
    }

    fn retention_origin(
        &self,
        root: Root,
        genesis: (Root, u64),
    ) -> Result<StateBinding, StoreError> {
        if let Some(RetainedState::Cold { binding, .. }) = self.graph.block_states.get(&root) {
            return Ok(binding.as_ref().clone())
        }
        if let Some(block) = self.graph.blocks.get(&root) {
            let preset = self.graph.processing.preset();
            let header = reth_embedded_beacon_primitives::BeaconBlockHeader {
                slot: block.slot,
                proposer_index: block.proposer_index,
                parent_root: block.parent_root,
                state_root: block.state_root,
                body_root: block.body.hash_tree_root(preset)?,
            };
            return StateBinding::admitted_header(
                root,
                &header,
                reth_embedded_beacon_primitives::Fork::Gloas,
                preset,
                genesis,
                self.source.identity(),
            )
        }
        let history = self.graph.historical.as_ref().ok_or(StoreError::UnknownRoot)?;
        CompactStore {
            graph: history.as_ref().clone(),
            source: self.source.clone(),
            limits: self.limits,
            hot_budget: self.hot_budget.clone(),
            optimistic: None,
        }
        .retention_origin(root, genesis)
    }

    fn retention_bindings(
        &self,
        roots: &BTreeSet<Root>,
        minimum_epoch: u64,
        genesis: (Root, u64),
    ) -> Result<Vec<StateBinding>, StoreError> {
        let mut bindings = Vec::with_capacity(roots.len() + self.graph.checkpoint_states.len());
        for &root in roots {
            bindings.push(self.retention_origin(root, genesis)?);
        }
        if let Some(history) = &self.graph.historical {
            let classic = CompactStore {
                graph: history.as_ref().clone(),
                source: self.source.clone(),
                limits: self.limits,
                hot_budget: self.hot_budget.clone(),
                optimistic: None,
            };
            let historical_roots = roots
                .iter()
                .copied()
                .filter(|root| history.block(*root).is_some())
                .collect::<BTreeSet<_>>();
            for binding in classic.retention_bindings(&historical_roots, minimum_epoch, genesis)? {
                bindings.push(binding);
            }
        }
        for (&(epoch, root), entry) in &self.graph.checkpoint_states {
            if epoch >= minimum_epoch && roots.contains(&root) {
                let binding = match entry {
                    RetainedState::Cold { binding, .. } => binding.as_ref().clone(),
                    RetainedState::Hot(state) => StateBinding::checkpoint(
                        &self.retention_origin(root, genesis)?,
                        epoch,
                        StateData::Gloas(state, self.graph.processing.preset()),
                    )?,
                };
                bindings.push(binding);
            }
        }
        Ok(bindings)
    }

    pub(super) fn execution_hash_after_cut(
        &self,
        current: ForkChoiceNode,
    ) -> Result<Option<Root>, StoreError> {
        if current.payload_status != Some(PayloadStatus::Empty) {
            return Err(StoreError::InvalidAttestationTarget)
        }
        let cut = self.execution_cut.as_ref().ok_or(StoreError::UnknownRoot)?;
        let mut root = current.root;
        let mut block = self.graph.block(root)?;
        let expected = block.body.signed_execution_payload_bid.message.parent_block_hash;
        for _ in 0..self.graph.parameters.max_blocks {
            let state = self.graph.read_block_state(root)?.ok_or(StoreError::UnknownRoot)?;
            let bid = &block.body.signed_execution_payload_bid.message;
            if state.slot != block.slot ||
                state.latest_block_header.slot != block.slot ||
                state.latest_block_header.parent_root != block.parent_root ||
                state.latest_block_header.proposer_index != block.proposer_index ||
                state.latest_execution_payload_bid != *bid ||
                state.latest_block_hash != bid.parent_block_hash ||
                bid.parent_block_hash != expected
            {
                return Err(StateSourceError::BindingMismatch.into())
            }
            if root == cut.root {
                if block.state_root != cut.state_root ||
                    cut.parent_hash.unwrap_or([0; 32]) != expected
                {
                    return Err(StateSourceError::BindingMismatch.into())
                }
                return Ok(cut.parent_hash)
            }
            let parent = self.graph.block(block.parent_root)?;
            if parent.slot >= block.slot {
                return Err(StoreError::InvalidBlockTime)
            }
            let parent_bid = &parent.body.signed_execution_payload_bid.message;
            if bid.parent_block_hash == parent_bid.block_hash {
                let actual = self
                    .graph
                    .verified_execution_block_hash(node(block.parent_root, PayloadStatus::Full))?;
                if actual != Some(expected) {
                    return Err(StateSourceError::BindingMismatch.into())
                }
                return Ok(actual)
            }
            if bid.parent_block_hash != parent_bid.parent_block_hash {
                return Err(StateSourceError::BindingMismatch.into())
            }
            root = block.parent_root;
            block = parent;
        }
        Err(StoreError::ResourceLimit)
    }
}
