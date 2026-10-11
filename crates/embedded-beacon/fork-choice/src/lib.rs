//! Validated-input LMD-GHOST kernel.
//!
//! This kernel does not authenticate attestations, execute state transitions, or
//! implement Gloas payload-presence/PTC event handlers. The import coordinator must
//! validate those inputs before they can enter the kernel.

use reth_embedded_beacon_primitives::{Checkpoint, Epoch, Root, Slot};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
use thiserror::Error;

pub mod compact;
pub mod data_availability;
pub mod fast_confirmation;
pub mod gloas;
pub mod gossip_view;

pub mod slashing_replay;
pub mod store;

pub use store::{ForkChoiceNode, PayloadStatus, Store, StoreError, StoreParameters, StoreSnapshot};

#[cfg(feature = "spec-tests")]
pub use store::spec_only::SpecStore;

/// Execution validity after the consensus transition has passed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExecutionStatus {
    /// Execution payload is fully validated.
    Valid,
    /// Execution validation is still pending.
    Optimistic,
    /// Execution payload was rejected.
    Invalid,
}

/// A block and its state-derived voting sources.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlockRecord {
    /// Consensus block root.
    pub root: Root,
    /// Parent beacon block root.
    pub parent_root: Root,
    /// Beacon slot number.
    pub slot: Slot,
    /// State-derived realized justified checkpoint.
    pub justified: Checkpoint,
    /// State-derived pulled-up justified checkpoint.
    pub unrealized_justified: Checkpoint,
    /// Execution engine validation status.
    pub execution_status: ExecutionStatus,
}

/// Weight at the justified checkpoint, indexed by validator.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ValidatorWeight {
    /// Checkpoint effective balance in gwei.
    pub effective_balance: u64,
    /// Whether the validator is active at the justified checkpoint.
    pub active: bool,
    /// Whether the validator is slashed at the justified checkpoint.
    pub slashed: bool,
}

/// An independently authenticated indexed attestation.
#[derive(Clone, Debug)]
pub struct VerifiedAttestation {
    /// Beacon slot number.
    pub slot: Slot,
    /// Authenticated epoch target checkpoint.
    pub target: Checkpoint,
    /// Authenticated voted beacon block root.
    pub beacon_block_root: Root,
    /// Sorted unique authenticated committee participants.
    pub validator_indices: Vec<u64>,
}

/// Latest message retained for a validator.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LatestMessage {
    /// Epoch number.
    pub epoch: Epoch,
    /// Consensus block root.
    pub root: Root,
}

/// An ancestry graph with deterministic LMD-GHOST weights and finality bounds.
#[derive(Clone, Debug)]
pub struct ForkChoice {
    blocks: BTreeMap<Root, BlockRecord>,
    children: BTreeMap<Root, BTreeSet<Root>>,
    latest_messages: BTreeMap<u64, LatestMessage>,
    equivocating: BTreeSet<u64>,
    // Checkpoint weights are immutable between verified checkpoint replacements. Sharing the
    // registry preserves transactional candidates without copying every validator on each event.
    weights: Arc<BTreeMap<u64, ValidatorWeight>>,
    justified: Checkpoint,
    finalized: Checkpoint,
    current_slot: Slot,
    slots_per_epoch: u64,
    proposer_boost: Option<(Root, u64)>,
}

impl ForkChoice {
    /// Start at a trusted, execution-valid anchor.
    pub fn new(
        anchor: BlockRecord,
        checkpoint: Checkpoint,
        weights: BTreeMap<u64, ValidatorWeight>,
        slots_per_epoch: u64,
    ) -> Result<Self, ForkChoiceError> {
        let checkpoint_slot =
            checkpoint.epoch.checked_mul(slots_per_epoch).ok_or(ForkChoiceError::Overflow)?;
        if slots_per_epoch == 0 ||
            anchor.root != checkpoint.root ||
            anchor.root == [0; 32] ||
            anchor.execution_status != ExecutionStatus::Valid ||
            anchor.slot / slots_per_epoch > checkpoint.epoch
        {
            return Err(ForkChoiceError::InvalidAnchor);
        }
        Ok(Self {
            blocks: BTreeMap::from([(anchor.root, anchor)]),
            children: BTreeMap::new(),
            latest_messages: BTreeMap::new(),
            equivocating: BTreeSet::new(),
            weights: Arc::new(weights),
            justified: checkpoint,
            finalized: checkpoint,
            current_slot: checkpoint_slot.max(anchor.slot),
            slots_per_epoch,
            proposer_boost: None,
        })
    }

    /// Advance time monotonically and remove the previous slot's proposer boost.
    pub const fn on_tick(&mut self, slot: Slot) -> Result<(), ForkChoiceError> {
        if slot < self.current_slot {
            return Err(ForkChoiceError::TimeRegression);
        }
        if slot > self.current_slot {
            self.proposer_boost = None;
            self.current_slot = slot;
        }
        Ok(())
    }

    /// Insert after full consensus validation; enforce graph and finality invariants.
    pub fn on_verified_block(&mut self, block: BlockRecord) -> Result<(), ForkChoiceError> {
        if let Some(existing) = self.blocks.get(&block.root) {
            return if existing == &block { Ok(()) } else { Err(ForkChoiceError::ConflictingBlock) };
        }
        let parent = self.blocks.get(&block.parent_root).ok_or(ForkChoiceError::UnknownRoot)?;
        if block.root == [0; 32] ||
            block.slot <= parent.slot ||
            block.slot > self.current_slot ||
            block.slot <=
                self.finalized
                    .epoch
                    .checked_mul(self.slots_per_epoch)
                    .ok_or(ForkChoiceError::Overflow)?
        {
            return Err(ForkChoiceError::InvalidBlockSlot);
        }
        if parent.execution_status == ExecutionStatus::Invalid ||
            block.execution_status == ExecutionStatus::Invalid
        {
            return Err(ForkChoiceError::InvalidExecutionBranch);
        }
        if !self.is_ancestor(block.parent_root, self.finalized.root)? {
            return Err(ForkChoiceError::ConflictsWithFinality);
        }
        self.children.entry(block.parent_root).or_default().insert(block.root);
        self.blocks.insert(block.root, block);
        Ok(())
    }

    /// Accept only an authenticated attestation after committee and BLS validation.
    ///
    /// Every input is checked before modifying any latest message.
    pub fn on_verified_attestation(
        &mut self,
        attestation: &VerifiedAttestation,
    ) -> Result<(), ForkChoiceError> {
        let current_epoch = self.current_slot / self.slots_per_epoch;
        if attestation.target.epoch > current_epoch ||
            current_epoch.saturating_sub(attestation.target.epoch) > 1 ||
            attestation.slot / self.slots_per_epoch != attestation.target.epoch ||
            attestation.slot >= self.current_slot
        {
            return Err(ForkChoiceError::InvalidAttestationTime);
        }
        let block =
            self.blocks.get(&attestation.beacon_block_root).ok_or(ForkChoiceError::UnknownRoot)?;
        if block.slot > attestation.slot || block.execution_status == ExecutionStatus::Invalid {
            return Err(ForkChoiceError::InvalidExecutionBranch);
        }
        if self.checkpoint_root(attestation.beacon_block_root, attestation.target.epoch)? !=
            attestation.target.root
        {
            return Err(ForkChoiceError::InvalidAttestationTarget);
        }
        if !self.is_ancestor(attestation.beacon_block_root, self.finalized.root)? {
            return Err(ForkChoiceError::ConflictsWithFinality);
        }
        let indices = attestation.validator_indices.iter().copied().collect::<BTreeSet<_>>();
        if indices.is_empty() ||
            indices.len() != attestation.validator_indices.len() ||
            indices.iter().any(|index| !self.weights.contains_key(index))
        {
            return Err(ForkChoiceError::InvalidValidatorIndices);
        }
        for index in indices {
            let message = LatestMessage {
                epoch: attestation.target.epoch,
                root: attestation.beacon_block_root,
            };
            if self
                .latest_messages
                .get(&index)
                .is_none_or(|previous| message.epoch > previous.epoch)
            {
                self.latest_messages.insert(index, message);
            }
        }
        Ok(())
    }

    /// Exclude equivocators after an independently verified attester slashing.
    pub fn on_verified_equivocation(&mut self, indices: &[u64]) -> Result<(), ForkChoiceError> {
        if indices.iter().any(|index| !self.weights.contains_key(index)) {
            return Err(ForkChoiceError::InvalidValidatorIndices);
        }
        self.equivocating.extend(indices.iter().copied());
        Ok(())
    }

    /// Set a timely boost computed from the justified checkpoint's active balance.
    pub fn set_proposer_boost(&mut self, root: Root, score: u64) -> Result<(), ForkChoiceError> {
        let block = self.blocks.get(&root).ok_or(ForkChoiceError::UnknownRoot)?;
        if block.slot != self.current_slot || block.execution_status == ExecutionStatus::Invalid {
            return Err(ForkChoiceError::InvalidBlockSlot);
        }
        if self.proposer_boost.is_none() {
            self.proposer_boost = Some((root, score));
        }
        Ok(())
    }

    /// Update checkpoints and weights after verified state processing.
    pub fn update_checkpoints(
        &mut self,
        justified: Checkpoint,
        finalized: Checkpoint,
        weights: BTreeMap<u64, ValidatorWeight>,
    ) -> Result<(), ForkChoiceError> {
        if finalized.epoch < self.finalized.epoch ||
            justified.epoch < self.justified.epoch ||
            finalized.epoch > justified.epoch
        {
            return Err(ForkChoiceError::CheckpointRegression);
        }
        self.require_valid_checkpoint(finalized)?;
        self.require_valid_checkpoint(justified)?;
        if !self.is_ancestor(finalized.root, self.finalized.root)? ||
            !self.is_ancestor(justified.root, finalized.root)? ||
            (finalized.epoch == self.finalized.epoch && finalized.root != self.finalized.root) ||
            (justified.epoch == self.justified.epoch && justified.root != self.justified.root)
        {
            return Err(ForkChoiceError::ConflictsWithFinality);
        }
        self.finalized = finalized;
        self.justified = justified;
        self.weights = Arc::new(weights);
        Ok(())
    }

    /// Invalidate descendants atomically; invalidating a checkpoint requires coordinator recovery.
    pub fn invalidate(&mut self, root: Root) -> Result<Vec<Root>, ForkChoiceError> {
        if !self.blocks.contains_key(&root) {
            return Err(ForkChoiceError::UnknownRoot);
        }
        let invalid = self
            .blocks
            .keys()
            .copied()
            .filter(|candidate| self.is_ancestor(*candidate, root).unwrap_or(false))
            .collect::<Vec<_>>();
        if invalid.contains(&self.finalized.root) || invalid.contains(&self.justified.root) {
            return Err(ForkChoiceError::InvalidatesCheckpoint);
        }
        for root in &invalid {
            self.blocks.get_mut(root).expect("collected graph entry").execution_status =
                ExecutionStatus::Invalid;
        }
        self.latest_messages.retain(|_, message| !invalid.contains(&message.root));
        if self.proposer_boost.is_some_and(|(root, _)| invalid.contains(&root)) {
            self.proposer_boost = None;
        }
        Ok(invalid)
    }

    /// Compute the head, breaking ties in favor of the lexicographically greatest root.
    pub fn head(&self) -> Result<Root, ForkChoiceError> {
        let ordered = self.ordered_blocks();
        let viable = self.filtered_tree(&ordered)?;
        // An excluded historical prefix cannot win LMD-GHOST regardless of vote weights.
        // Preserve the actual source/finality filter before avoiding this registry-wide scan.
        if self
            .children
            .get(&self.justified.root)
            .is_none_or(|children| children.iter().all(|root| !viable.contains(root)))
        {
            return Ok(self.justified.root)
        }
        let scores = self.subtree_weights(&ordered)?;
        let mut head = self.justified.root;
        loop {
            let Some(children) = self.children.get(&head) else { return Ok(head) };
            let mut best = None;
            for root in children.iter().filter(|root| viable.contains(*root)) {
                let candidate = (scores.get(root).copied().unwrap_or_default(), *root);
                if best.is_none_or(|previous| candidate > previous) {
                    best = Some(candidate);
                }
            }
            let Some((_, next)) = best else { return Ok(head) };
            head = next;
        }
    }

    /// Return every viable leaf reached from the justified root and its actual LMD weight.
    /// Leaves have no children in the filtered tree, even if excluded children exist in the graph.
    pub fn viable_leaf_weights(&self) -> Result<BTreeMap<Root, u128>, ForkChoiceError> {
        let (viable, scores) = self.filtered_tree_and_scores()?;
        let mut pending = vec![self.justified.root];
        let mut leaves = BTreeMap::new();
        while let Some(root) = pending.pop() {
            let children = self
                .children
                .get(&root)
                .map(|children| {
                    children
                        .iter()
                        .copied()
                        .filter(|child| viable.contains(child))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            if children.is_empty() {
                leaves.insert(root, scores.get(&root).copied().unwrap_or_default());
            } else {
                pending.extend(children);
            }
        }
        Ok(leaves)
    }

    fn filtered_tree_and_scores(
        &self,
    ) -> Result<(BTreeSet<Root>, BTreeMap<Root, u128>), ForkChoiceError> {
        let ordered = self.ordered_blocks();
        let scores = self.subtree_weights(&ordered)?;
        let viable = self.filtered_tree(&ordered)?;
        Ok((viable, scores))
    }

    fn ordered_blocks(&self) -> Vec<BlockRecord> {
        let mut ordered = self
            .blocks
            .values()
            .filter(|block| block.execution_status != ExecutionStatus::Invalid)
            .copied()
            .collect::<Vec<_>>();
        ordered.sort_by_key(|block| std::cmp::Reverse(block.slot));
        ordered
    }

    fn filtered_tree(&self, ordered: &[BlockRecord]) -> Result<BTreeSet<Root>, ForkChoiceError> {
        let mut viable = BTreeSet::new();
        for &block in ordered {
            let has_children = self.children.get(&block.root).is_some_and(|children| {
                children
                    .iter()
                    .any(|root| self.blocks[root].execution_status != ExecutionStatus::Invalid)
            });
            let child_viable = self
                .children
                .get(&block.root)
                .is_some_and(|children| children.iter().any(|root| viable.contains(root)));
            if child_viable || (!has_children && self.viable_leaf(block)?) {
                viable.insert(block.root);
            }
        }
        Ok(viable)
    }

    /// Follow parents to the latest block at or before a slot.
    pub fn ancestor(&self, mut root: Root, slot: Slot) -> Result<Root, ForkChoiceError> {
        loop {
            let block = self.blocks.get(&root).ok_or(ForkChoiceError::UnknownRoot)?;
            if block.slot <= slot {
                return Ok(root);
            }
            root = block.parent_root;
        }
    }

    /// Test ancestry at the ancestor's exact slot.
    pub fn is_ancestor(&self, descendant: Root, ancestor: Root) -> Result<bool, ForkChoiceError> {
        let slot = self.blocks.get(&ancestor).ok_or(ForkChoiceError::UnknownRoot)?.slot;
        Ok(self.ancestor(descendant, slot)? == ancestor)
    }

    /// Latest messages for persistence or diagnostics.
    pub const fn latest_messages(&self) -> &BTreeMap<u64, LatestMessage> {
        &self.latest_messages
    }

    /// Checkpoints for execution fork-choice propagation.
    pub const fn checkpoints(&self) -> (Checkpoint, Checkpoint) {
        (self.justified, self.finalized)
    }

    fn checkpoint_root(&self, root: Root, epoch: Epoch) -> Result<Root, ForkChoiceError> {
        self.ancestor(
            root,
            epoch.checked_mul(self.slots_per_epoch).ok_or(ForkChoiceError::Overflow)?,
        )
    }

    fn require_valid_checkpoint(&self, checkpoint: Checkpoint) -> Result<(), ForkChoiceError> {
        let block = self.blocks.get(&checkpoint.root).ok_or(ForkChoiceError::UnknownRoot)?;
        if block.execution_status != ExecutionStatus::Valid ||
            block.slot >
                checkpoint
                    .epoch
                    .checked_mul(self.slots_per_epoch)
                    .ok_or(ForkChoiceError::Overflow)?
        {
            return Err(ForkChoiceError::InvalidCheckpoint);
        }
        Ok(())
    }

    fn viable_leaf(&self, block: BlockRecord) -> Result<bool, ForkChoiceError> {
        let current_epoch = self.current_slot / self.slots_per_epoch;
        let source = if current_epoch > block.slot / self.slots_per_epoch {
            block.unrealized_justified
        } else {
            block.justified
        };
        let justified = self.justified.epoch == 0 ||
            source.epoch == self.justified.epoch ||
            source.epoch.saturating_add(2) >= current_epoch;
        let finalized =
            self.checkpoint_root(block.root, self.finalized.epoch)? == self.finalized.root;
        Ok(justified && finalized)
    }

    fn subtree_weights(
        &self,
        descending_blocks: &[BlockRecord],
    ) -> Result<BTreeMap<Root, u128>, ForkChoiceError> {
        // Aggregate direct votes once, then propagate child sums toward the anchor.
        // This avoids an ancestry walk for every validator at every candidate node.
        let mut scores = BTreeMap::<Root, u128>::new();
        for (index, message) in &self.latest_messages {
            if let Some(validator) = self.weights.get(index) &&
                validator.active &&
                !validator.slashed &&
                !self.equivocating.contains(index) &&
                self.blocks
                    .get(&message.root)
                    .is_some_and(|block| block.execution_status != ExecutionStatus::Invalid)
            {
                let score = scores.entry(message.root).or_default();
                *score = score
                    .checked_add(u128::from(validator.effective_balance))
                    .ok_or(ForkChoiceError::Overflow)?;
            }
        }
        if let Some((root, boost)) = self.proposer_boost {
            let score = scores.entry(root).or_default();
            *score = score.checked_add(u128::from(boost)).ok_or(ForkChoiceError::Overflow)?;
        }
        for block in descending_blocks {
            let score = scores.get(&block.root).copied().unwrap_or_default();
            if self.blocks.contains_key(&block.parent_root) {
                let parent = scores.entry(block.parent_root).or_default();
                *parent = parent.checked_add(score).ok_or(ForkChoiceError::Overflow)?;
            }
        }
        Ok(scores)
    }
}

/// Invalid events leave the store unchanged.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub enum ForkChoiceError {
    /// Invalid Anchor.
    #[error("invalid trusted anchor")]
    InvalidAnchor,
    /// Unknown Root.
    #[error("unknown block root")]
    UnknownRoot,
    /// Time Regression.
    #[error("slot clock moved backwards")]
    TimeRegression,
    /// Invalid Block Slot.
    #[error("block slot is not after its parent or is in the future")]
    InvalidBlockSlot,
    /// Conflicting Block.
    #[error("conflicting data for a known block root")]
    ConflictingBlock,
    /// Invalid Execution Branch.
    #[error("branch contains invalid execution")]
    InvalidExecutionBranch,
    /// Conflicts With Finality.
    #[error("branch conflicts with finalized ancestry")]
    ConflictsWithFinality,
    /// Invalid Attestation Time.
    #[error("attestation is from an invalid epoch or is too early")]
    InvalidAttestationTime,
    /// Invalid Attestation Target.
    #[error("attestation target is not its epoch checkpoint ancestor")]
    InvalidAttestationTarget,
    /// Invalid Validator Indices.
    #[error("validator indices are empty, duplicate, or unknown")]
    InvalidValidatorIndices,
    /// Checkpoint Regression.
    #[error("checkpoint epoch regressed")]
    CheckpointRegression,
    /// Invalid Checkpoint.
    #[error("checkpoint is not execution valid or predates its block")]
    InvalidCheckpoint,
    /// Invalidates Checkpoint.
    #[error("invalidation touches a checkpoint; coordinator rollback required")]
    InvalidatesCheckpoint,
    /// Overflow.
    #[error("fork-choice arithmetic overflow")]
    Overflow,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn root(id: u8) -> Root {
        [id; 32]
    }

    fn setup() -> ForkChoice {
        let checkpoint = Checkpoint { epoch: 0, root: root(1) };
        let anchor = BlockRecord {
            root: root(1),
            parent_root: [0; 32],
            slot: 0,
            justified: checkpoint,
            unrealized_justified: checkpoint,
            execution_status: ExecutionStatus::Valid,
        };
        ForkChoice::new(
            anchor,
            checkpoint,
            BTreeMap::from([
                (0, ValidatorWeight { effective_balance: 32, active: true, slashed: false }),
                (1, ValidatorWeight { effective_balance: 64, active: true, slashed: false }),
            ]),
            32,
        )
        .unwrap()
    }

    fn block(id: u8, parent: u8, slot: u64) -> BlockRecord {
        let checkpoint = Checkpoint { epoch: 0, root: root(1) };
        BlockRecord {
            root: root(id),
            parent_root: root(parent),
            slot,
            justified: checkpoint,
            unrealized_justified: checkpoint,
            execution_status: ExecutionStatus::Valid,
        }
    }

    fn vote(id: u8, indices: Vec<u64>) -> VerifiedAttestation {
        VerifiedAttestation {
            slot: 2,
            target: Checkpoint { epoch: 0, root: root(1) },
            beacon_block_root: root(id),
            validator_indices: indices,
        }
    }

    #[test]
    fn latest_balance_and_ties_follow_ancestry() {
        let mut store = setup();
        store.on_tick(4).unwrap();
        store.on_verified_block(block(2, 1, 1)).unwrap();
        store.on_verified_block(block(3, 1, 1)).unwrap();
        store.on_verified_block(block(4, 2, 2)).unwrap();
        assert_eq!(store.head().unwrap(), root(3));
        store.on_verified_attestation(&vote(4, vec![1])).unwrap();
        assert_eq!(store.head().unwrap(), root(4));
        store.on_verified_attestation(&vote(3, vec![1])).unwrap();
        assert_eq!(store.head().unwrap(), root(4));
        store.on_verified_equivocation(&[1]).unwrap();
        assert_eq!(store.head().unwrap(), root(3));
    }

    #[test]
    fn invalidation_removes_boost_and_descendants() {
        let mut store = setup();
        store.on_tick(3).unwrap();
        store.on_verified_block(block(2, 1, 1)).unwrap();
        store.on_verified_block(block(3, 1, 3)).unwrap();
        store.set_proposer_boost(root(3), 100).unwrap();
        assert_eq!(store.head().unwrap(), root(3));
        assert_eq!(store.invalidate(root(1)), Err(ForkChoiceError::InvalidatesCheckpoint));
        store.invalidate(root(3)).unwrap();
        assert_eq!(store.head().unwrap(), root(2));
        store.on_tick(4).unwrap();
        assert_eq!(
            store.on_verified_block(block(4, 3, 4)),
            Err(ForkChoiceError::InvalidExecutionBranch)
        );
    }

    #[test]
    fn malformed_vote_is_atomic() {
        let mut store = setup();
        store.on_tick(4).unwrap();
        store.on_verified_block(block(2, 1, 1)).unwrap();
        assert_eq!(
            store.on_verified_attestation(&vote(2, vec![0, 99])),
            Err(ForkChoiceError::InvalidValidatorIndices)
        );
        assert!(store.latest_messages().is_empty());
        assert_eq!(
            store.on_verified_attestation(&vote(2, vec![0, 0])),
            Err(ForkChoiceError::InvalidValidatorIndices)
        );
        assert!(store.latest_messages().is_empty());
    }

    #[test]
    fn checkpoint_anchor_may_precede_an_empty_epoch_boundary() {
        let checkpoint = Checkpoint { epoch: 2, root: root(9) };
        let anchor = BlockRecord {
            root: root(9),
            parent_root: root(8),
            slot: 63,
            justified: checkpoint,
            unrealized_justified: checkpoint,
            execution_status: ExecutionStatus::Valid,
        };
        let mut store = ForkChoice::new(anchor, checkpoint, BTreeMap::new(), 32).unwrap();
        assert_eq!(store.head().unwrap(), root(9));
        let mut boundary_block = block(10, 9, 64);
        boundary_block.justified = checkpoint;
        boundary_block.unrealized_justified = checkpoint;
        assert_eq!(store.on_verified_block(boundary_block), Err(ForkChoiceError::InvalidBlockSlot));
        store.on_tick(65).unwrap();
        boundary_block.slot = 65;
        store.on_verified_block(boundary_block).unwrap();
        assert_eq!(store.head().unwrap(), root(10));
    }

    #[test]
    fn finality_and_clock_never_regress() {
        let mut store = setup();
        store.on_tick(33).unwrap();
        store.on_verified_block(block(2, 1, 32)).unwrap();
        let checkpoint = Checkpoint { epoch: 1, root: root(2) };
        store.update_checkpoints(checkpoint, checkpoint, BTreeMap::new()).unwrap();
        assert_eq!(store.on_tick(32), Err(ForkChoiceError::TimeRegression));
        assert_eq!(
            store.on_verified_block(block(3, 1, 33)),
            Err(ForkChoiceError::ConflictsWithFinality)
        );
        assert_eq!(
            store.update_checkpoints(
                Checkpoint { epoch: 0, root: root(1) },
                checkpoint,
                BTreeMap::new()
            ),
            Err(ForkChoiceError::CheckpointRegression)
        );
    }

    #[test]
    fn excluded_historical_children_do_not_select_head_from_retained_votes() {
        let mut store = setup();
        store.on_tick(35).unwrap();
        store.on_verified_block(block(2, 1, 32)).unwrap();
        store.on_verified_block(block(3, 2, 33)).unwrap();
        store.on_verified_block(block(4, 2, 33)).unwrap();
        let justified = Checkpoint { epoch: 1, root: root(2) };
        for (beacon_block_root, validator) in [(root(3), 0), (root(4), 1)] {
            store
                .on_verified_attestation(&VerifiedAttestation {
                    slot: 33,
                    target: justified,
                    beacon_block_root,
                    validator_indices: vec![validator],
                })
                .unwrap();
        }
        store
            .update_checkpoints(justified, store.finalized, store.weights.as_ref().clone())
            .unwrap();
        assert_eq!(store.head().unwrap(), root(4));

        // Wall-clock catch-up excludes both old voting sources. Existing votes remain intact,
        // and the diagnostic weight query still reports their complete ancestor weight.
        store.on_tick(160).unwrap();
        assert_eq!(store.head().unwrap(), justified.root);
        assert_eq!(store.latest_messages().len(), 2);
        assert_eq!(store.viable_leaf_weights().unwrap(), BTreeMap::from([(justified.root, 96)]));

        // A strictly admitted descendant with the required source reopens this branch. The
        // heavier excluded sibling cannot override the native viability filter.
        let mut descendant = block(5, 3, 160);
        descendant.justified = justified;
        descendant.unrealized_justified = justified;
        store.on_verified_block(descendant).unwrap();
        assert_eq!(store.head().unwrap(), root(5));
    }

    #[test]
    fn checkpoint_weight_replacement_keeps_prior_candidate_vote_results() {
        let mut original = setup();
        original.on_tick(35).unwrap();
        original.on_verified_block(block(2, 1, 32)).unwrap();
        original.on_verified_block(block(3, 2, 33)).unwrap();
        original.on_verified_block(block(4, 2, 33)).unwrap();
        let next_justified = Checkpoint { epoch: 1, root: root(2) };
        for (beacon_block_root, validator) in [(root(3), 0), (root(4), 1)] {
            original
                .on_verified_attestation(&VerifiedAttestation {
                    slot: 33,
                    target: next_justified,
                    beacon_block_root,
                    validator_indices: vec![validator],
                })
                .unwrap();
        }
        assert_eq!(original.head().unwrap(), root(4));
        let previous_checkpoints = original.checkpoints();
        let mut candidate = original.clone();

        // A newly justified state's balances reverse the competing branches' vote weights.
        // The earlier candidate must still use its own authenticated checkpoint balances.
        candidate
            .update_checkpoints(
                next_justified,
                previous_checkpoints.1,
                BTreeMap::from([
                    (0, ValidatorWeight { effective_balance: 96, active: true, slashed: false }),
                    (1, ValidatorWeight { effective_balance: 8, active: true, slashed: false }),
                ]),
            )
            .unwrap();

        assert_eq!(candidate.head().unwrap(), root(3));
        assert_eq!(candidate.checkpoints(), (next_justified, previous_checkpoints.1));
        assert_eq!(original.head().unwrap(), root(4));
        assert_eq!(original.checkpoints(), previous_checkpoints);
        assert_eq!(original.latest_messages(), candidate.latest_messages());
        assert_eq!(
            candidate.update_checkpoints(
                previous_checkpoints.0,
                previous_checkpoints.1,
                BTreeMap::new()
            ),
            Err(ForkChoiceError::CheckpointRegression)
        );
        assert_eq!(candidate.head().unwrap(), root(3));
        assert_eq!(original.head().unwrap(), root(4));
    }
}
