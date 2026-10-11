//! Transactional finalized graph retention for the compact classic wrapper.

use super::*;
use crate::compact::{
    CompactStore, FinalizedRetention, FinalizedRetentionPolicy, PrunedGraph, ReadGossipView,
    RetainedExecutionAssociation, StateBinding, StateData,
};

impl CompactStore {
    /// Stage an authenticated recovery boundary before removing proven finalized ancestry.
    /// Every descendant of the boundary remains, including noncanonical unfinalized branches.
    /// A source lacking durable roll-forward support fails closed without changing this graph.
    pub fn prune_finalized_history(
        &mut self,
        policy: FinalizedRetentionPolicy,
    ) -> Result<PrunedGraph, StoreError> {
        let actual = self.graph.snapshot()?;
        let count = self.graph.blocks.len();
        if actual.finalized_checkpoint.epoch <= policy.epochs() {
            return Ok(PrunedGraph { retained_blocks: count, ..Default::default() })
        }
        let maximum_epoch = actual.finalized_checkpoint.epoch - policy.epochs();
        let epoch = self
            .source
            .retention_epoch_limit()?
            .map_or(maximum_epoch, |floor| floor.min(maximum_epoch));
        if epoch == 0 {
            return Ok(PrunedGraph { retained_blocks: count, ..Default::default() })
        }
        let target = epoch
            .checked_mul(self.graph.parameters.phase0.slots_per_epoch())
            .ok_or(StoreError::Overflow)?;
        let first_slot = self.graph.blocks.values().try_fold(u64::MAX, |minimum, block| {
            Ok::<_, StoreError>(minimum.min(block.header()?.slot))
        })?;
        // A genuine checkpoint constructor need not retain history before its trusted seed.
        // Delay the cut until the desired horizon enters actual retained ancestry.
        if target < first_slot {
            return Ok(PrunedGraph { retained_blocks: count, ..Default::default() })
        }
        let root = self.graph.get_checkpoint_block(actual.finalized_checkpoint.root, epoch)?;
        let boundary = Checkpoint { epoch, root };
        let mut candidate = self.clone();
        let genesis = {
            let state = candidate
                .graph
                .read_block_state(actual.head.root)?
                .ok_or(StoreError::UnknownRoot)?;
            (state.genesis_validators_root(), state.genesis_time())
        };
        let origin = candidate.retention_origin(root, genesis)?;
        if !candidate.graph.block_states.contains_key(&root) {
            candidate
                .graph
                .block_states
                .insert(root, RetainedState::cold(origin.clone(), candidate.source.clone())?);
        }
        let state = candidate.graph.checkpoint_state_inner::<true>(boundary)?;
        let binding = StateBinding::checkpoint(&origin, epoch, state.data())?;
        candidate.source.retain_checkpoint(&binding, state.data())?;
        let boundary_slot =
            candidate.graph.blocks.get(&root).ok_or(StoreError::UnknownRoot)?.header()?.slot;
        let mut retained = BTreeSet::new();
        for (&block_root, block) in &candidate.graph.blocks {
            if block.header()?.slot >= boundary_slot &&
                candidate.graph.kernel.is_ancestor(block_root, root)?
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
            execution: RetainedExecutionAssociation::Classic,
        })?;
        candidate.graph.retain_graph_roots(&retained, epoch);
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

    pub(crate) fn retention_origin(
        &self,
        root: Root,
        genesis: (Root, u64),
    ) -> Result<StateBinding, StoreError> {
        if let Some(RetainedState::Cold { binding, .. }) = self.graph.block_states.get(&root) {
            return Ok(binding.as_ref().clone())
        }
        let block = self.graph.blocks.get(&root).ok_or(StoreError::UnknownRoot)?;
        StateBinding::admitted_header(
            root,
            &block.header()?,
            block.schema_fork(),
            self.graph.parameters.phase0.preset,
            genesis,
            self.source.identity(),
        )
    }

    pub(crate) fn retention_bindings(
        &self,
        roots: &BTreeSet<Root>,
        minimum_epoch: u64,
        genesis: (Root, u64),
    ) -> Result<Vec<StateBinding>, StoreError> {
        let mut bindings = Vec::with_capacity(roots.len() + self.graph.checkpoint_states.len());
        for &root in roots {
            bindings.push(self.retention_origin(root, genesis)?);
        }
        for (&(epoch, root), entry) in &self.graph.checkpoint_states {
            if epoch >= minimum_epoch && roots.contains(&root) {
                let binding = match entry {
                    RetainedState::Cold { binding, .. } => binding.as_ref().clone(),
                    RetainedState::Hot(state) => StateBinding::checkpoint(
                        &self.retention_origin(root, genesis)?,
                        epoch,
                        StateData::Classic(state),
                    )?,
                };
                bindings.push(binding);
            }
        }
        Ok(bindings)
    }
}
impl Store {
    pub(crate) fn restore_retention_origin(
        &mut self,
        binding: &StateBinding,
        source: Arc<dyn crate::compact::CompactStateSource>,
    ) -> Result<(), StoreError> {
        let root = binding.origin_block_root();
        let block = self.blocks.get(&root).ok_or(StoreError::UnknownRoot)?;
        let header = block.header()?;
        if header.state_root != binding.origin_state_root() ||
            header.slot != binding.slot() ||
            block.schema_fork() != binding.fork()
        {
            return Err(StateSourceError::BindingMismatch.into())
        }
        if let std::collections::btree_map::Entry::Vacant(entry) = self.block_states.entry(root) {
            entry.insert(RetainedState::cold(binding.clone(), source)?);
        }
        Ok(())
    }
    pub(crate) fn retain_graph_roots(&mut self, roots: &BTreeSet<Root>, minimum_epoch: u64) {
        self.blocks.retain(|root, _| roots.contains(root));
        self.block_states.retain(|root, _| roots.contains(root));
        self.checkpoint_states
            .retain(|(epoch, root), _| *epoch >= minimum_epoch && roots.contains(root));
        self.block_timeliness.retain(|root, _| roots.contains(root));
        self.kernel.blocks.retain(|root, _| roots.contains(root));
        self.kernel.children.retain(|root, children| {
            children.retain(|child| roots.contains(child));
            roots.contains(root) && !children.is_empty()
        });
        // Latest epochs and exact signed slots survive as replay-protection tombstones.
        // Their absent roots cannot contribute to subtree weights or prepared confirmation.
        if self.kernel.proposer_boost.is_some_and(|(root, _)| !roots.contains(&root)) {
            self.kernel.proposer_boost = None;
        }
    }
}
