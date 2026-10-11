//! Unforgeable node-local peer advertisement derived only by the actual admitted graph owner.

use super::{Graph, ImportError};
use crate::bootstrap::checked_config_digest;
use reth_embedded_beacon_config::ChainConfig;
use reth_embedded_beacon_fork_choice::{StoreError, StoreSnapshot};
use reth_embedded_beacon_network::{peer::TrustedStatusSnapshot, Status, StatusV2};
use reth_embedded_beacon_primitives::Root;

/// Actual graph status evidence; public snapshot copies cannot construct or mutate this token.
#[derive(Clone, Debug)]
pub struct AuthenticatedStatus {
    digest: Root,
    current_slot: u64,
    snapshot: StoreSnapshot,
    canonical_roots: Vec<(u64, Root)>,
    fork_digest: [u8; 4],
    earliest_available_slot: u64,
    pub(super) origin_genesis: Option<Root>,
}
impl AuthenticatedStatus {
    /// Exact consensus configuration authenticated by the actual importer.
    pub const fn config_digest(&self) -> Root {
        self.digest
    }
    /// Graph clock, or the local retrieval clock while original history is replayed.
    pub const fn current_slot(&self) -> u64 {
        self.current_slot
    }
    /// Real chosen graph head and checkpoints; reading a copy grants no token constructor.
    pub const fn snapshot(&self) -> StoreSnapshot {
        self.snapshot
    }
    pub(crate) const fn genesis_origin(&self) -> Option<Root> {
        self.origin_genesis
    }
    // The replay owner supplies a current discovery clock without publishing a replayed head.
    pub(super) fn replay_retrieval_clock(
        &mut self,
        config: &ChainConfig,
        now: u64,
    ) -> Result<(), ImportError> {
        if self.digest != checked_config_digest(config)? {
            return Err(ImportError::Configuration)
        }
        let current_slot = config
            .clock()
            .map_err(|_| ImportError::Configuration)?
            .slot_at(now)
            .ok_or(ImportError::Clock)?;
        if current_slot < self.current_slot {
            return Err(ImportError::Clock)
        }
        let fork_digest = config
            .fork_digest(current_slot / config.slots_per_epoch)
            .map_err(|_| ImportError::Configuration)?;
        self.current_slot = current_slot;
        self.fork_digest = fork_digest;
        Ok(())
    }
    pub(crate) fn apply_custody_history(
        &mut self,
        receipt: Option<crate::live_sync::availability::coverage::CustodyReceipt>,
    ) -> Result<(), ImportError> {
        // Proof-serving history belongs to this same native head and configured owner.
        self.earliest_available_slot = receipt
            .filter(|receipt| receipt.config_and_head(self.digest, self.snapshot.head.root))
            .map_or_else(
                || self.snapshot.head_slot.saturating_add(1),
                |receipt| receipt.earliest(),
            );
        Ok(())
    }
    pub(crate) fn into_network(self) -> TrustedStatusSnapshot {
        TrustedStatusSnapshot {
            current_slot: self.current_slot,
            finalized: self.snapshot.finalized_checkpoint,
            canonical_roots: self.canonical_roots,
            status: StatusV2 {
                status: Status {
                    fork_digest: self.fork_digest,
                    finalized_root: self.snapshot.finalized_checkpoint.root,
                    finalized_epoch: self.snapshot.finalized_checkpoint.epoch,
                    head_root: self.snapshot.head.root,
                    head_slot: self.snapshot.head_slot,
                },
                earliest_available_slot: self.earliest_available_slot,
            },
        }
    }
}

impl Graph {
    pub(super) fn authenticated_status(
        &self,
        config: &ChainConfig,
        earliest_available_slot: u64,
    ) -> Result<AuthenticatedStatus, ImportError> {
        let snapshot = self.snapshot()?;
        let finalized = snapshot.finalized_checkpoint;
        let mut canonical_roots = vec![(finalized.epoch, finalized.root)];
        // Earlier finalized roots are included only when actual retained ancestry resolves them.
        for epoch in (finalized.epoch.saturating_sub(31)..finalized.epoch).rev() {
            let result = match self {
                Self::Classic { store, .. } => {
                    store.canonical_finalized_epoch_roots(epoch, 1).map(|roots| roots[0].1)
                }
                Self::Gloas { store, .. } => store.get_checkpoint_block(finalized.root, epoch),
            };
            match result {
                Ok(root) => canonical_roots.push((epoch, root)),
                Err(StoreError::UnavailableHistory | StoreError::UnknownRoot) => break,
                Err(error) => return Err(error.into()),
            }
        }
        canonical_roots.sort_unstable();
        let current_slot = self.current_slot();
        Ok(AuthenticatedStatus {
            digest: checked_config_digest(config)?,
            current_slot,
            snapshot,
            canonical_roots,
            fork_digest: config
                .fork_digest(current_slot / config.slots_per_epoch)
                .map_err(|_| ImportError::Configuration)?,
            earliest_available_slot,
            origin_genesis: None,
        })
    }
}
