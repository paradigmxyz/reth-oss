//! Bounded alternate-peer envelope retrieval preserves exact admitted linkage and candidate pins.

use super::*;
use reth_embedded_beacon_network::{peer::PeerRuntimeError, transport::TransportError};

pub(super) struct WaitingEnvelope {
    pub(super) root: Root,
    pub(super) slot: u64,
    pub(super) metadata: EnvelopeBlockReference,
    pub(super) candidate: CandidateGuard,
    pub(super) started: Instant,
    pub(super) tried: Vec<PeerId>,
    pub(super) preferred: PeerId,
}
impl LiveSync {
    pub(super) fn schedule_envelope_retry(&mut self) -> Result<(), LiveSyncError> {
        let Some(mut retry) = self.envelopes.take_waiting() else { return Ok(()) };
        if retry.started.elapsed() >= Duration::from_secs(18) {
            // Every previous stream had a known response or disconnect. A missing envelope
            // grants no FULL branch; an actual later signed child can still select EMPTY.
            tracing::debug!(target: "beacon::sync", root = %B256::from(retry.root),
                "[beacon] Original envelope unavailable within bounded peer retry window");
            return Ok(())
        }
        let selected = self
            .targets
            .iter()
            .filter(|target| {
                !retry.tried.contains(&target.peer) &&
                    target.status.head_slot >= retry.slot &&
                    target.earliest.is_none_or(|slot| slot <= retry.slot)
            })
            .min_by_key(|target| usize::from(target.peer != retry.preferred))
            .map(|target| target.peer);
        let Some(peer) = selected else {
            self.envelopes.wait(retry)?;
            return Ok(())
        };
        let result = self.peer.as_mut().ok_or(LiveSyncError::PeerUnavailable)?.request_envelopes(
            peer,
            EnvelopeRequest::Roots(vec![retry.root]),
            vec![retry.metadata],
        );
        match result {
            Ok(id) => {
                retry.tried.push(peer);
                self.envelopes.request(EnvelopePending {
                    peer,
                    id,
                    root: retry.root,
                    slot: retry.slot,
                    metadata: retry.metadata,
                    candidate: retry.candidate,
                    started: retry.started,
                    tried: retry.tried,
                })?;
            }
            Err(PeerStartupError::Peer(PeerRuntimeError::Transport(TransportError::Capacity))) => {
                self.envelopes.wait(retry)?;
            }
            Err(PeerStartupError::Peer(PeerRuntimeError::Transport(
                TransportError::Configuration,
            ))) => {
                self.remove_peer(peer);
                retry.tried.push(peer);
                self.envelopes.wait(retry)?;
            }
            Err(error) => return Err(error.into()),
        }
        Ok(())
    }
    pub(super) fn retry_envelope(&mut self, pending: EnvelopePending) -> Result<(), LiveSyncError> {
        if self.envelopes.waiting().is_some() || pending.tried.len() > MAX_PEERS {
            return Err(LiveSyncError::Bounds)
        }
        self.envelopes.wait(WaitingEnvelope {
            root: pending.root,
            slot: pending.slot,
            metadata: pending.metadata,
            candidate: pending.candidate,
            started: pending.started,
            tried: pending.tried,
            preferred: pending.peer,
        })?;
        self.schedule_envelope_retry()
    }
}
