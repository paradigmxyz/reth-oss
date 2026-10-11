//! One bounded historical `BlocksByRoot` job runs alongside the actual forward importer.

use super::*;
use availability::history_retrieval::HistoricalBlockJob;
use reth_embedded_beacon_network::{peer::PeerRuntimeError, transport::TransportError};

#[derive(Default)]
pub(super) struct HistoryBroker {
    waiting: Option<WaitingHistory>,
    pending: Option<PendingHistory>,
}
struct WaitingHistory {
    job: HistoricalBlockJob,
    started: Instant,
    tried: Vec<PeerId>,
}
struct PendingHistory {
    peer: PeerId,
    id: BlockRequestId,
    job: HistoricalBlockJob,
    started: Instant,
    tried: Vec<PeerId>,
}
impl HistoryBroker {
    pub(super) fn matches(&self, peer: PeerId, id: BlockRequestId) -> bool {
        self.pending.as_ref().is_some_and(|pending| pending.peer == peer && pending.id == id)
    }
    pub(super) fn requests(&self) -> usize {
        usize::from(self.waiting.is_some()) + usize::from(self.pending.is_some())
    }
    pub(super) fn expired_provider(&self, now: Instant, deadline: Duration) -> Option<PeerId> {
        self.pending
            .as_ref()
            .filter(|pending| now.saturating_duration_since(pending.started) >= deadline)
            .map(|pending| pending.peer)
    }
}
impl LiveSync {
    pub(super) fn history_request(&mut self, job: HistoricalBlockJob) -> Result<(), LiveSyncError> {
        if self.history.requests() != 0 {
            job.complete(Err(RetrievalError::Capacity));
            return Ok(());
        }
        let started = job.created();
        self.history.waiting = Some(WaitingHistory { job, started, tried: Vec::new() });
        self.schedule_history()
    }
    pub(super) fn schedule_history(&mut self) -> Result<(), LiveSyncError> {
        // Retain this peer/id and admission charge until the real transport deadline event,
        // including cancelled receivers. A late reply remains a historical response.
        let Some(mut waiting) = self.history.waiting.take() else { return Ok(()) };
        if waiting.job.closed() || waiting.started.elapsed() >= Duration::from_secs(18) {
            waiting.job.complete(Err(RetrievalError::Missing));
            return Ok(());
        }
        let peer = self
            .targets
            .iter()
            .filter(|target| {
                !waiting.tried.contains(&target.peer) &&
                    target.status.head_slot >= waiting.job.upper_slot().saturating_sub(1) &&
                    target.earliest.is_none_or(|slot| slot < waiting.job.upper_slot())
            })
            .map(|target| target.peer)
            .next();
        let Some(peer) = peer else {
            self.history.waiting = Some(waiting);
            return Ok(());
        };
        match self
            .peer
            .as_mut()
            .ok_or(LiveSyncError::PeerUnavailable)?
            .request_blocks(peer, BlockRequest::Roots(vec![waiting.job.root()]))
        {
            Ok(id) => {
                waiting.tried.push(peer);
                self.history.pending = Some(PendingHistory {
                    peer,
                    id,
                    job: waiting.job,
                    started: waiting.started,
                    tried: waiting.tried,
                })
            }
            Err(PeerStartupError::Peer(PeerRuntimeError::Transport(TransportError::Capacity))) => {
                self.history.waiting = Some(waiting)
            }
            Err(PeerStartupError::Peer(PeerRuntimeError::Transport(
                TransportError::Configuration,
            ))) => {
                waiting.tried.push(peer);
                self.remove_peer(peer);
                self.history.waiting = Some(waiting)
            }
            Err(error) => return Err(error.into()),
        }
        Ok(())
    }
    pub(super) fn history_unavailable(
        &mut self,
        peer: PeerId,
        id: BlockRequestId,
    ) -> Result<(), LiveSyncError> {
        if !self.history.matches(peer, id) {
            return Err(LiveSyncError::Response);
        }
        let pending = self.history.pending.take().ok_or(LiveSyncError::Response)?;
        self.history.waiting = Some(WaitingHistory {
            job: pending.job,
            started: pending.started,
            tried: pending.tried,
        });
        self.schedule_history()
    }
    pub(super) fn history_disconnected(&mut self, peer: PeerId) -> Result<(), LiveSyncError> {
        if let Some(pending) = self.history.pending.take() {
            if pending.peer == peer {
                self.history.waiting = Some(WaitingHistory {
                    job: pending.job,
                    started: pending.started,
                    tried: pending.tried,
                })
            } else {
                self.history.pending = Some(pending)
            }
        }
        self.schedule_history()
    }
    pub(super) fn history_received(
        &mut self,
        peer: PeerId,
        id: BlockRequestId,
        batch: BlockBatch,
    ) -> Result<(), LiveSyncError> {
        if !self.history.matches(peer, id) {
            return Err(LiveSyncError::Response);
        }
        let pending = self.history.pending.take().ok_or(LiveSyncError::Response)?;
        if batch.blocks().is_empty() {
            self.history.waiting = Some(WaitingHistory {
                job: pending.job,
                started: pending.started,
                tried: pending.tried,
            });
            return self.schedule_history();
        }
        let request = BlockRequest::Roots(vec![pending.job.root()]);
        let result = request
            .matches(batch.blocks())
            .map(|()| batch)
            .map_err(|_| RetrievalError::Association);
        pending.job.complete(result);
        Ok(())
    }
}
