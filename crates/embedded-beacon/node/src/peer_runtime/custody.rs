//! Signed custody publication accepts only the local retained-proof receipt.

use super::*;
use crate::live_sync::availability::coverage::CustodyReceipt;
use reth_embedded_beacon_primitives::Fork;

impl CheckpointPeerRuntime {
    pub(crate) fn apply_custody(
        &mut self,
        receipt: Option<CustodyReceipt>,
    ) -> Result<(), PeerStartupError> {
        self.identity.ensure_healthy()?;
        let node_id = self.peer.local_enr().node_id().raw();
        let count = if receipt.is_some_and(|receipt| {
            receipt.matches(
                self.identity.digest,
                node_id,
                self.trusted_status.status.status.head_root,
            )
        }) && self.config.fork_at_slot(self.current_slot)?.fork >= Fork::Fulu
        {
            4
        } else {
            0
        };
        let mut persistence_error = None;
        let store = &mut self.identity;
        let slot = self.current_slot;
        let policy = self.listeners.policy;
        let result = self.peer.update_custody(count, |record| {
            store.persist(record, slot, policy).map_err(|error| {
                persistence_error = Some(error);
                DiscoveryError::Persistence
            })
        });
        if let Some(error) = persistence_error {
            self.shutdown();
            return Err(error)
        }
        if let Err(error) = result {
            self.shutdown();
            return Err(error.into())
        }
        Ok(())
    }
}
