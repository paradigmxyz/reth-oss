//! EIP-8347 BAL catch-up. A replayed block produces a new immutable parent snapshot.

use crate::{StateError, StateTree, TreeHasher};
use alloy_eip7928::{compute_block_access_list_hash, AccountChanges, BlockAccessIndex};
use alloy_primitives::B256;
use std::{collections::BTreeMap, num::NonZeroUsize};

/// Replays authenticated post-values without EVM execution.
///
/// `expected_bal_hash` must come from the caller's validated canonical header chain. A failed
/// hash, ordering check, or state update leaves `parent` unchanged. Reads never create accounts.
pub fn replay<H: TreeHasher>(
    parent: &StateTree<H>,
    bal: &[AccountChanges],
    expected_bal_hash: B256,
) -> Result<StateTree<H>, ReplayError> {
    if compute_block_access_list_hash(bal) != expected_bal_hash {
        return Err(ReplayError::BalHashMismatch);
    }
    let mut previous = None;
    let mut state = parent.clone();
    for changes in bal {
        if previous.is_some_and(|address| address >= changes.address) {
            return Err(ReplayError::InvalidOrder);
        }
        previous = Some(changes.address);
        validate_indices(changes.balance_changes.iter().map(|change| change.block_access_index))?;
        validate_indices(changes.nonce_changes.iter().map(|change| change.block_access_index))?;
        validate_indices(changes.code_changes.iter().map(|change| change.block_access_index))?;
        if changes.storage_changes.windows(2).any(|pair| pair[0].slot >= pair[1].slot) ||
            changes.storage_reads.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(ReplayError::InvalidOrder);
        }
        for slot in &changes.storage_changes {
            if slot.changes.is_empty() {
                return Err(ReplayError::EmptyStorageChanges);
            }
            if changes.storage_reads.binary_search(&slot.slot).is_ok() {
                return Err(ReplayError::InvalidOrder);
            }
            validate_indices(slot.changes.iter().map(|change| change.block_access_index))?;
        }
        if !changes.has_changes() {
            continue;
        }
        let mut account = state.account(changes.address).cloned().unwrap_or_default();
        if let Some(balance) = changes.balance_post_state() {
            account.balance = balance;
        }
        if let Some(nonce) = changes.nonce_post_state() {
            account.nonce = nonce;
        }
        if let Some(code) = changes.code_post_state() {
            account.code = code.clone();
        }
        state.apply_account(changes.address, Some(account))?;
        let has_account = state.account(changes.address).is_some();
        for (slot, value) in changes.storage_post_states() {
            // A zero post-value only deletes a leaf, but a nonzero value requires an account.
            if has_account || !value.is_zero() {
                state.set_storage(changes.address, slot, B256::from(value))?;
            }
        }
    }
    Ok(state)
}

/// Discardable replay layers keyed by block hash, with a finalized base.
///
/// Switching branches selects a stored parent snapshot, rather than inverting BAL post-values.
/// Persistence of the finalized base is the caller's responsibility. Retention is bounded:
/// inserting a new layer at the limit fails without evicting any unfinalized branch. Advance
/// finality or obtain a recent finalized anchor before continuing replay.
#[derive(Clone, Debug)]
pub struct ReplayChain<H> {
    finalized: B256,
    blocks: BTreeMap<B256, ReplayLayer<H>>,
    max_layers: NonZeroUsize,
}

impl<H: TreeHasher> ReplayChain<H> {
    /// Starts from a verified finalized anchor, retaining at most 1024 layers including it.
    pub fn new(anchor: B256, state: StateTree<H>) -> Self {
        Self::with_limit(anchor, state, DEFAULT_MAX_LAYERS)
    }

    /// Starts from a verified finalized anchor with an explicit retained-layer limit.
    ///
    /// The anchor counts toward `max_layers`. A limit of one retains only the anchor and
    /// rejects every insertion. This is a local operator budget, not a protocol limit. It
    /// bounds the number of snapshots, not their byte size.
    pub fn with_limit(anchor: B256, state: StateTree<H>, max_layers: NonZeroUsize) -> Self {
        Self {
            finalized: anchor,
            blocks: BTreeMap::from([(anchor, ReplayLayer { parent: None, state })]),
            max_layers,
        }
    }

    /// Returns the replayed state associated with a specific block hash.
    pub fn state(&self, hash: B256) -> Option<&StateTree<H>> {
        self.blocks.get(&hash).map(|layer| &layer.state)
    }

    /// Returns the finalized anchor hash.
    pub const fn finalized(&self) -> B256 {
        self.finalized
    }

    /// Replays a child of any retained block. Competing branches are independent.
    ///
    /// At the retained-layer limit, fails before authenticating or applying the BAL. Existing
    /// layers remain available for branch selection and finalization.
    pub fn insert(
        &mut self,
        hash: B256,
        parent: B256,
        bal: &[AccountChanges],
        expected_bal_hash: B256,
    ) -> Result<B256, ReplayError> {
        if self.blocks.contains_key(&hash) {
            return Err(ReplayError::DuplicateBlock);
        }
        let parent_state = self.state(parent).ok_or(ReplayError::UnknownParent)?;
        if self.blocks.len() >= self.max_layers.get() {
            return Err(ReplayError::LayerLimitExceeded { max_layers: self.max_layers.get() });
        }
        let state = replay(parent_state, bal, expected_bal_hash)?;
        let root = state.root();
        self.blocks.insert(hash, ReplayLayer { parent: Some(parent), state });
        Ok(root)
    }

    /// Advances finality and drops every layer that cannot descend from it.
    ///
    /// Returns the finalized snapshot so the caller can durably checkpoint it. Calls for an
    /// unknown block fail without dropping layers. A finality violation requires a new anchor.
    pub fn finalize(&mut self, hash: B256) -> Result<StateTree<H>, ReplayError> {
        let state = self.state(hash).ok_or(ReplayError::UnknownParent)?.clone();
        let retained: Vec<_> = self
            .blocks
            .keys()
            .copied()
            .filter(|candidate| self.descends_from(*candidate, hash))
            .collect();
        self.blocks.retain(|candidate, _| retained.binary_search(candidate).is_ok());
        self.blocks.get_mut(&hash).unwrap().parent = None;
        self.finalized = hash;
        Ok(state)
    }

    fn descends_from(&self, mut candidate: B256, ancestor: B256) -> bool {
        loop {
            if candidate == ancestor {
                return true;
            }
            let Some(parent) = self.blocks.get(&candidate).and_then(|layer| layer.parent) else {
                return false;
            };
            candidate = parent;
        }
    }
}

/// BAL replay could not be authenticated or applied.
#[derive(Debug, thiserror::Error)]
pub enum ReplayError {
    /// Received BAL does not match the trusted header commitment.
    #[error("block access list hash mismatch")]
    BalHashMismatch,
    /// Addresses, slots, or change indices are duplicated or not strictly ascending, including
    /// a slot present in both reads and writes.
    #[error("block access list changes are not strictly ordered")]
    InvalidOrder,
    /// The parent or finality target is outside retained state.
    #[error("replay parent is unavailable; obtain a recent finalized anchor")]
    UnknownParent,
    /// The supplied block hash already identifies a retained layer.
    #[error("replay block hash already exists")]
    DuplicateBlock,
    /// Invalid state encoding.
    #[error(transparent)]
    State(#[from] StateError),
    /// A storage write slot has no recorded post-values.
    #[error("block access list storage change list is empty")]
    EmptyStorageChanges,
    /// Retaining another replay snapshot would exceed the configured layer budget.
    #[error("replay retained-layer limit of {max_layers} reached; finalize or obtain a recent finalized anchor")]
    LayerLimitExceeded {
        /// Maximum retained layers, including the finalized anchor.
        max_layers: usize,
    },
}

#[derive(Clone, Debug)]
struct ReplayLayer<H> {
    parent: Option<B256>,
    state: StateTree<H>,
}

fn validate_indices(
    indices: impl IntoIterator<Item = BlockAccessIndex>,
) -> Result<(), ReplayError> {
    let mut previous = None;
    for index in indices {
        if previous.is_some_and(|previous| previous >= index) {
            return Err(ReplayError::InvalidOrder);
        }
        previous = Some(index);
    }
    Ok(())
}

const DEFAULT_MAX_LAYERS: NonZeroUsize =
    NonZeroUsize::new(1024).expect("replay layer limit is nonzero");
