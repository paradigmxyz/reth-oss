//! Live envelope broker driving keeps original recovery jobs and transport leases alive.

use super::*;
use reth_embedded_beacon_network::{peer::PeerRuntimeError, transport::TransportError};

pub(super) struct BrokerEnvelopePending {
    pub(super) peer: PeerId,
    pub(super) id: EnvelopeRequestId,
    pub(super) job: EnvelopeRetrievalJob,
    pub(super) started: Instant,
    tried: Vec<PeerId>,
}
pub(super) struct RetryingBrokerEnvelope {
    job: EnvelopeRetrievalJob,
    started: Instant,
    tried: Vec<PeerId>,
}
impl LiveSync {
    pub(super) fn retrieve_envelope(
        &mut self,
        job: EnvelopeRetrievalJob,
    ) -> Result<(), LiveSyncError> {
        let active = self.active.as_ref().ok_or(LiveSyncError::Response)?;
        if job.metadata.root() != active.root ||
            job.metadata.slot() != active.slot ||
            self.broker_envelope.is_some() ||
            self.broker_envelope_retry.is_some() ||
            self.envelope_request.is_some() ||
            self.envelope_retry.is_some() ||
            self.sidecar_request.is_some() ||
            self.sidecar_retry.is_some()
        {
            return Err(LiveSyncError::Response)
        }
        let started = job.created;
        self.broker_envelope_retry =
            Some(RetryingBrokerEnvelope { job, started, tried: Vec::new() });
        self.schedule_broker_envelope()
    }
    pub(super) fn schedule_broker_envelope(&mut self) -> Result<(), LiveSyncError> {
        let Some(mut retry) = self.broker_envelope_retry.take() else { return Ok(()) };
        if self.broker_envelope.is_some() {
            return Err(LiveSyncError::Bounds)
        }
        if retry.job.reply.is_closed() || retry.started.elapsed() >= Duration::from_secs(18) {
            retry.job.complete(Err(RetrievalError::Missing));
            return Ok(())
        }
        let preferred = self.active.as_ref().map(|active| active.peer);
        let selected = self
            .targets
            .iter()
            .filter(|target| {
                !retry.tried.contains(&target.peer) &&
                    target.status.head_slot >= retry.job.metadata.slot() &&
                    target.earliest.is_none_or(|slot| slot <= retry.job.metadata.slot())
            })
            .min_by_key(|target| usize::from(Some(target.peer) != preferred))
            .map(|target| target.peer);
        let Some(peer) = selected else {
            self.broker_envelope_retry = Some(retry);
            return Ok(())
        };
        let result = self.peer.as_mut().ok_or(LiveSyncError::PeerUnavailable)?.request_envelopes(
            peer,
            EnvelopeRequest::Roots(vec![retry.job.metadata.root()]),
            vec![retry.job.metadata],
        );
        match result {
            Ok(id) => {
                retry.tried.push(peer);
                self.broker_envelope = Some(BrokerEnvelopePending {
                    peer,
                    id,
                    job: retry.job,
                    started: retry.started,
                    tried: retry.tried,
                });
            }
            Err(PeerStartupError::Peer(PeerRuntimeError::Transport(TransportError::Capacity))) => {
                self.broker_envelope_retry = Some(retry);
            }
            Err(PeerStartupError::Peer(PeerRuntimeError::Transport(
                TransportError::Configuration,
            ))) => {
                self.remove_peer(peer);
                retry.tried.push(peer);
                self.broker_envelope_retry = Some(retry);
            }
            Err(error) => return Err(error.into()),
        }
        Ok(())
    }
    pub(super) fn retry_broker_envelope(
        &mut self,
        pending: BrokerEnvelopePending,
    ) -> Result<(), LiveSyncError> {
        if self.broker_envelope_retry.is_some() || pending.tried.len() > MAX_PEERS {
            return Err(LiveSyncError::Bounds)
        }
        self.broker_envelope_retry = Some(RetryingBrokerEnvelope {
            job: pending.job,
            started: pending.started,
            tried: pending.tried,
        });
        self.schedule_broker_envelope()
    }
    pub(super) fn broker_envelope_result(
        &mut self,
        peer: PeerId,
        id: EnvelopeRequestId,
        batch: EnvelopeBatch,
    ) -> Result<(), LiveSyncError> {
        let pending = self.broker_envelope.take().ok_or(LiveSyncError::Response)?;
        if pending.peer != peer || pending.id != id {
            return Err(LiveSyncError::Response)
        }
        let request = EnvelopeRequest::Roots(vec![pending.job.metadata.root()]);
        let result = request.matches(&self.config, &batch, |root| {
            (root == pending.job.metadata.root()).then_some(&pending.job.metadata)
        });
        if batch.envelopes().is_empty() {
            self.retry_broker_envelope(pending)?;
        } else {
            pending.job.complete(result.map(|()| batch).map_err(|_| RetrievalError::Association));
        }
        Ok(())
    }
}
