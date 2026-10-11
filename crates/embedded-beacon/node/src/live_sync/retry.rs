//! Alternate-peer retries preserve the original bounded availability job and deadline.

use super::*;
use reth_embedded_beacon_network::{
    availability_rpc::SidecarRequest, peer::PeerRuntimeError, transport::TransportError,
};

pub(super) struct RetryingSidecar {
    pub(super) job: RetrievalJob,
    pub(super) started: Instant,
    pub(super) tried: Vec<PeerId>,
}
impl LiveSync {
    /// Defer local transport pressure and retire peers whose status admission has expired.
    /// Validate our own request first so malformed requests remain fatal.
    pub(super) fn try_request_blocks(
        &mut self,
        peer: PeerId,
        request: BlockRequest,
    ) -> Result<Option<BlockRequestId>, LiveSyncError> {
        request.validate_outbound().map_err(|_| LiveSyncError::Configuration)?;
        let result =
            self.peer.as_mut().ok_or(LiveSyncError::PeerUnavailable)?.request_blocks(peer, request);
        match result {
            Ok(id) => Ok(Some(id)),
            Err(PeerStartupError::Peer(PeerRuntimeError::Transport(TransportError::Capacity))) => {
                Ok(None)
            }
            Err(PeerStartupError::Peer(PeerRuntimeError::Transport(
                TransportError::Configuration,
            ))) => {
                // The transport no longer trusts this peer. Drop its retrieval hints only;
                // admitted blocks and the forward cursor retain their original authority.
                self.remove_peer(peer);
                tracing::debug!(target: "beacon::sync", %peer,
                    "[beacon] Retired expired block retrieval peer; selecting another peer");
                Ok(None)
            }
            Err(error) => Err(error.into()),
        }
    }
    pub(super) fn schedule_sidecar_retry(&mut self) -> Result<(), LiveSyncError> {
        let Some(mut retry) = self.sidecar_retry.take() else { return Ok(()) };
        if self.sidecar_request.is_some() {
            return Err(LiveSyncError::Bounds)
        }
        if retry.job.reply.is_closed() {
            retry.job.complete(Err(RetrievalError::Missing));
            return Ok(())
        }
        if retry.started.elapsed() >= Duration::from_secs(18) || retry.tried.len() >= MAX_PEERS {
            // A waiting job owns no transport channel. Return a known missing outcome before
            // its original broker timeout, without claiming availability or invalidating peers.
            retry.job.complete(Err(RetrievalError::Missing));
            return Ok(())
        }
        if select_data_peer(&self.targets, &retry.tried, retry.job.slot, |_| 0).is_none() {
            // No eligible provider means no transport access or consumed fairness turn. Keep
            // the exact original job queued, including genuine detached custody recovery.
            self.sidecar_retry = Some(retry);
            return Ok(())
        }
        let runtime = self.peer.as_ref().ok_or(LiveSyncError::PeerUnavailable)?;
        let selected = self.sidecar_cycle.select(
            (retry.job.root, retry.job.slot),
            &self.targets,
            &retry.tried,
            |peer| sidecar_custody_overlap(runtime, peer, &retry.job.request),
        );
        let Some(peer) = selected else {
            self.sidecar_retry = Some(retry);
            return Ok(())
        };
        let result = self
            .peer
            .as_mut()
            .ok_or(LiveSyncError::PeerUnavailable)?
            .request_sidecars(peer, retry.job.request.clone());
        match result {
            Ok(id) => {
                self.sidecar_cycle.issued(peer);
                rotate_data_peer(&mut self.targets, peer);
                retry.tried.push(peer);
                self.sidecar_request = Some(SidecarPending {
                    peer,
                    id,
                    job: retry.job,
                    started: retry.started,
                    tried: retry.tried,
                });
            }
            Err(PeerStartupError::Peer(PeerRuntimeError::Transport(TransportError::Capacity))) => {
                // Existing codec/proof jobs retain permits through cancellation. Keep this
                // single original job rather than duplicating it or extending its deadline.
                self.sidecar_retry = Some(retry);
            }
            Err(PeerStartupError::Peer(PeerRuntimeError::Transport(
                TransportError::Configuration,
            ))) => {
                self.remove_peer(peer);
                retry.tried.push(peer);
                self.sidecar_retry = Some(retry);
            }
            Err(error) => return Err(error.into()),
        }
        Ok(())
    }
    pub(super) fn retry_sidecar(&mut self, pending: SidecarPending) -> Result<(), LiveSyncError> {
        if self.sidecar_retry.is_some() || pending.tried.len() > MAX_PEERS {
            return Err(LiveSyncError::Bounds)
        }
        self.sidecar_retry = Some(RetryingSidecar {
            job: pending.job,
            started: pending.started,
            tried: pending.tried,
        });
        self.schedule_sidecar_retry()
    }
}
/// Rotate only retrieval preference. Peer hints confer no column or consensus authority.
pub(super) fn rotate_data_peer(targets: &mut Vec<Target>, peer: PeerId) {
    if let Some(index) = targets.iter().position(|target| target.peer == peer) {
        let target = targets.remove(index);
        targets.push(target);
    }
}

/// Prefer the largest advisory overlap; equal scores retain the original rotation order.
/// Unknown, expired and unavailable custody claims remain ordinary eligible fallbacks.
pub(super) fn select_data_peer(
    targets: &[Target],
    tried: &[PeerId],
    slot: u64,
    mut score: impl FnMut(PeerId) -> usize,
) -> Option<PeerId> {
    let mut selected = None;
    for target in targets {
        if !tried.contains(&target.peer) &&
            target.status.head_slot >= slot &&
            target.earliest.is_none_or(|earliest| earliest <= slot)
        {
            let overlap = score(target.peer);
            if selected.is_none_or(|(_, previous)| overlap > previous) {
                selected = Some((target.peer, overlap));
            }
        }
    }
    selected.map(|(peer, _)| peer)
}

pub(super) fn sidecar_custody_overlap(
    runtime: &CheckpointPeerRuntime,
    peer: PeerId,
    request: &SidecarRequest,
) -> usize {
    match request {
        SidecarRequest::ColumnRange { columns, .. } => runtime.custody_overlap(peer, columns),
        SidecarRequest::ColumnRoots(roots) => {
            roots.iter().map(|root| runtime.custody_overlap(peer, &root.columns)).sum()
        }
        SidecarRequest::BlobRange { .. } | SidecarRequest::BlobRoots(_) => 0,
    }
}

/// One turn per eligible provider for the exact candidate prevents inflated custody claims
/// from monopolizing successive empty or partial sampling replies. No validity is remembered.
#[derive(Default)]
pub(super) struct DataPeerCycle {
    candidate: Option<(Root, u64)>,
    issued: Vec<PeerId>,
}

impl DataPeerCycle {
    pub(super) fn select(
        &mut self,
        candidate: (Root, u64),
        targets: &[Target],
        tried: &[PeerId],
        mut score: impl FnMut(PeerId) -> usize,
    ) -> Option<PeerId> {
        if self.candidate != Some(candidate) {
            self.candidate = Some(candidate);
            self.issued.clear();
        }
        self.issued.retain(|peer| targets.iter().any(|target| target.peer == *peer));
        let selected = select_data_peer(targets, tried, candidate.1, |peer| {
            if self.issued.contains(&peer) {
                0
            } else {
                score(peer).saturating_add(1)
            }
        });
        if selected.is_some_and(|peer| !self.issued.contains(&peer)) {
            return selected
        }
        // Every remaining eligible provider has received a turn. Start another bounded cycle;
        // the current original job's own tried inventory still prevents duplicate requests.
        self.issued.clear();
        select_data_peer(targets, tried, candidate.1, score)
    }

    pub(super) fn issued(&mut self, peer: PeerId) {
        if self.issued.len() < MAX_PEERS && !self.issued.contains(&peer) {
            self.issued.push(peer);
        }
    }
}

#[cfg(test)]
mod tests;
