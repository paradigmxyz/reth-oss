//! A single background coverage turn lets the actual sidecar broker continue polling.

use super::*;
use availability::coverage::CustodyReceipt;
use tokio::task::JoinHandle;

#[derive(Debug, Default)]
pub(super) struct CustodyTask {
    work: Option<JoinHandle<Result<Option<CustodyReceipt>, ImportError>>>,
    receipt: Option<CustodyReceipt>,
}
impl Drop for CustodyTask {
    fn drop(&mut self) {
        if let Some(work) = self.work.take() {
            work.abort()
        }
    }
}
impl LiveSync {
    pub(super) async fn custody_receipt(
        &mut self,
        head: Root,
        current_slot: u64,
        head_slot: u64,
    ) -> Result<Option<CustodyReceipt>, LiveSyncError> {
        if self.custody.work.as_ref().is_some_and(JoinHandle::is_finished) {
            let work = self.custody.work.take().ok_or(LiveSyncError::Bounds)?;
            self.custody.receipt = match work.await.map_err(|_| LiveSyncError::PeerUnavailable)? {
                Ok(receipt) => receipt,
                Err(
                    ImportError::Bounds | ImportError::Backpressure | ImportError::GossipIgnore(_),
                ) => None,
                Err(error) => return Err(error.into()),
            };
        }

        if self.custody.work.is_none() && self.custody_foreground_idle() {
            let availability = self.availability.clone();
            let importer = self.importer.clone();
            self.custody.work = Some(tokio::spawn(async move {
                availability.custody_coverage(importer, head, current_slot).await
            }));
        }
        Ok(self.availability.custody_startup(head, head_slot, current_slot).await?)
    }

    fn custody_foreground_idle(&self) -> bool {
        !self.rebase
            && self.active.is_none()
            && self.deferred_import.is_none()
            && self.block_request.is_none()
            && self.envelopes.requested().is_none()
            && self.envelopes.waiting().is_none()
            && self.sidecars.requested().is_none()
            && self.sidecars.waiting().is_none()
            && self.broker_envelope.is_none()
            && self.broker_envelope_retry.is_none()
            && self.branch.is_none()
            && self.gossip_active.is_none()
            && self.gossip_queue.is_empty()
            && self.history.requests() == 0
            // Peer targets are scheduling hints, not consensus authority.
            && !self.targets.iter().any(|target| {
                (target.status.head_slot >= self.cursor
                    && target.earliest.is_none_or(|slot| slot <= self.cursor))
                    || (target.status.head_root != self.admitted_root
                        && target.examined_root != Some(target.status.head_root))
            })
    }
}

#[cfg(test)]
impl CustodyTask {
    pub(super) fn has_work(&self) -> bool {
        self.work.is_some()
    }
}
