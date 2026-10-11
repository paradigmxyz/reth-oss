//! Sealed finalized retention plans; decoded data never establish consensus finality.
//!
//! A source stages/pins the exact new recovery boundary before a candidate removes ancestry.
//! Canonical recovery pointers and obsolete-owner releases belong to the import transaction.
use super::{StateBinding, StateData};
use crate::{StoreError, StoreSnapshot};
use reth_embedded_beacon_primitives::{Checkpoint, Root};
use std::collections::BTreeSet;

/// Retain a conservative finalized epoch horizon in addition to every unfinalized branch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FinalizedRetentionPolicy {
    epochs: u64,
}
impl Default for FinalizedRetentionPolicy {
    fn default() -> Self {
        Self { epochs: 32 }
    }
}
impl FinalizedRetentionPolicy {
    /// Finality-compatible competing branches are never dropped by this horizon.
    /// Two full prior epochs remain mandatory for authenticated historical committee reads.
    pub const fn new(epochs: u64) -> Result<Self, StoreError> {
        if epochs < 2 || epochs > 64 {
            return Err(StoreError::InvalidParameters)
        }
        Ok(Self { epochs })
    }
    /// Complete finalized epochs kept before the current finalized checkpoint.
    pub const fn epochs(self) -> u64 {
        self.epochs
    }
}

/// Execution evidence carried by a finalized consensus retention receipt.
/// Unresolved commitments never establish Engine validity, zero absence, or FULL delivery.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetainedExecutionAssociation {
    /// Earlier-schema execution remains bound to its existing admitted block receipts.
    Classic,
    /// The strict pre-cut EMPTY traversal independently proved this execution association.
    VerifiedEmpty {
        /// None is proven zero-hash absence, rather than missing execution proof.
        hash: Option<Root>,
    },
    /// Consensus ancestry is finalized, but its original trusted EL base is node-owned.
    UnresolvedTrustedBoundary(UnresolvedExecutionAssociation),
}

/// Graph-created structural EMPTY lineage ending at an authenticated constructor boundary.
/// These exact commitments require the node's separate sealed same-adapter VALID base.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UnresolvedExecutionAssociation {
    pub(crate) origin_block_root: Root,
    pub(crate) origin_state_root: Root,
    pub(crate) origin_slot: u64,
    pub(crate) cut_block_root: Root,
    pub(crate) cut_state_root: Root,
    pub(crate) parent_hash: Root,
}
impl UnresolvedExecutionAssociation {
    /// Actual retained execution-boundary block at which the structural lineage ends.
    pub const fn origin_block_root(&self) -> Root {
        self.origin_block_root
    }
    /// Actual beacon post-state commitment of that boundary, distinct from the EL state root.
    pub const fn origin_state_root(&self) -> Root {
        self.origin_state_root
    }
    /// Actual slot of the original boundary block.
    pub const fn origin_slot(&self) -> u64 {
        self.origin_slot
    }
    /// Actual finalized cut block selected by the private consensus graph.
    pub const fn cut_block_root(&self) -> Root {
        self.cut_block_root
    }
    /// Actual beacon post-state commitment in the selected cut block.
    pub const fn cut_state_root(&self) -> Root {
        self.cut_state_root
    }
    /// Raw committed EMPTY parent hash; this value alone proves no execution validity.
    pub const fn parent_hash(&self) -> Root {
        self.parent_hash
    }
}

/// Exact data-only roll-forward receipt created exclusively by the strict private graph.
#[derive(Debug)]
pub struct FinalizedRetention<'a> {
    pub(crate) boundary: Checkpoint,
    pub(crate) origin: &'a StateBinding,
    pub(crate) binding: &'a StateBinding,
    pub(crate) state: StateData<'a>,
    pub(crate) roots: &'a BTreeSet<Root>,
    pub(crate) states: &'a [StateBinding],
    pub(crate) snapshot: StoreSnapshot,
    pub(crate) execution: RetainedExecutionAssociation,
}
impl<'a> FinalizedRetention<'a> {
    /// Canonical checkpoint proven to precede the actual admitted finalized checkpoint.
    pub const fn boundary_checkpoint(&self) -> Checkpoint {
        self.boundary
    }
    /// Actual original block commitment; original complete signed bytes stay in the repository.
    pub const fn origin_binding(&self) -> &'a StateBinding {
        self.origin
    }
    /// Exact separately production-replayed epoch boundary.
    pub const fn boundary_binding(&self) -> &'a StateBinding {
        self.binding
    }
    /// Complete boundary data, held by its owning read throughout this receipt.
    pub const fn boundary_state(&self) -> StateData<'a> {
        self.state
    }
    /// Every retained block root; parent closure ends at the exact boundary block.
    pub const fn retained_roots(&self) -> &'a BTreeSet<Root> {
        self.roots
    }
    /// Every retained block/checkpoint state binding; all required pins remain independent.
    pub const fn retained_states(&self) -> &'a [StateBinding] {
        self.states
    }
    /// Actual admitted head/justified/finalized values, without any saved-hint authority.
    pub const fn actual_snapshot(&self) -> StoreSnapshot {
        self.snapshot
    }
    /// Distinguish proven EMPTY execution from an explicitly unresolved trusted boundary.
    /// No variant establishes a new FULL payload receipt.
    pub const fn execution_association(&self) -> RetainedExecutionAssociation {
        self.execution
    }
}

/// Actual graph cut measurements; these do not include disk garbage collection or RSS.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PrunedGraph {
    /// Actually removed block/schema graph entries.
    pub removed_blocks: usize,
    /// Remaining genuine typed block roots.
    pub retained_blocks: usize,
    /// New exact finalized recovery boundary, if it advanced.
    pub boundary: Option<Checkpoint>,
}
