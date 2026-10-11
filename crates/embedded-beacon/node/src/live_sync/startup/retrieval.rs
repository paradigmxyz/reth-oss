//! Startup data retries keep one original deadline and unresolved transport ownership.

use super::{
    super::retry::{select_data_peer, sidecar_custody_overlap, DataPeerCycle},
    *,
};
use reth_embedded_beacon_network::{peer::PeerRuntimeError, transport::TransportError};

const DATA_DEADLINE: Duration = Duration::from_secs(18);

/// One waiting original plus one issued transport token; neither admission can be duplicated.
pub(super) struct StartupRetrieval<J, I> {
    waiting: Option<Waiting<J>>,
    pending: Option<Pending<J, I>>,
    cycle: DataPeerCycle,
}
impl<J, I> Default for StartupRetrieval<J, I> {
    fn default() -> Self {
        Self { waiting: None, pending: None, cycle: DataPeerCycle::default() }
    }
}
impl<J: StartupJob, I: Copy + Eq> StartupRetrieval<J, I> {
    pub(super) fn admit(&mut self, job: J) -> Result<(), LiveSyncError> {
        if self.waiting.is_some() ||
            self.pending.as_ref().is_some_and(|pending| pending.work.is_some())
        {
            return Err(LiveSyncError::Bounds);
        }
        let started = job.created();
        self.waiting = Some(Waiting { job, started, tried: Vec::new() });
        Ok(())
    }

    pub(super) fn schedule(
        &mut self,
        targets: &mut Vec<Target>,
        peer: &mut CheckpointPeerRuntime,
        request: impl FnOnce(&J, &mut CheckpointPeerRuntime, PeerId) -> Result<I, PeerStartupError>,
    ) -> Result<(), LiveSyncError> {
        self.expire();
        if self.pending.is_some() {
            return Ok(());
        }
        let Some(mut work) = self.waiting.take() else { return Ok(()) };
        if work.tried.len() >= MAX_PEERS {
            work.job.unavailable();
            return Ok(());
        }
        let selected = if let Some(candidate) = work.job.candidate() {
            self.cycle.select(candidate, targets, &work.tried, |selected| {
                work.job.provider_score(peer, selected)
            })
        } else {
            select_data_peer(targets, &work.tried, work.job.slot(), |selected| {
                work.job.provider_score(peer, selected)
            })
        };
        let Some(selected) = selected else {
            self.waiting = Some(work);
            return Ok(());
        };
        match request(&work.job, peer, selected) {
            Ok(id) => {
                if work.job.candidate().is_some() {
                    self.cycle.issued(selected);
                }
                rotate_data_peer(targets, selected);
                work.tried.push(selected);
                self.pending =
                    Some(Pending { peer: selected, id, issued: Instant::now(), work: Some(work) });
            }
            Err(PeerStartupError::Peer(PeerRuntimeError::Transport(TransportError::Capacity))) => {
                self.waiting = Some(work);
            }
            Err(PeerStartupError::Peer(PeerRuntimeError::Transport(
                TransportError::Configuration,
            ))) => {
                work.tried.push(selected);
                targets.retain(|target| target.peer != selected);
                self.waiting = Some(work);
            }
            Err(error) => return Err(error.into()),
        }
        Ok(())
    }

    /// A data deadline resolves Missing before the caller's 20-second timeout. An issued
    /// transport token stays retained until that exact channel produces a terminal event.
    pub(super) fn expire(&mut self) {
        if self.waiting.as_ref().is_some_and(Waiting::expired) {
            self.waiting.take().expect("checked original startup job").job.unavailable();
        }
        if let Some(pending) = &mut self.pending &&
            pending.work.as_ref().is_some_and(Waiting::expired)
        {
            pending.work.take().expect("checked issued startup job").job.unavailable();
        }
    }

    pub(super) fn has_work(&self) -> bool {
        self.waiting.is_some() ||
            self.pending.as_ref().is_some_and(|pending| pending.work.is_some())
    }

    pub(super) fn has_request(&self) -> bool {
        self.pending.is_some()
    }

    /// The outer option identifies the exact transport owner. The inner option distinguishes
    /// its cancelled data receiver from a still-live original; neither accepts an unrelated reply.
    pub(super) fn retire(&mut self, peer: PeerId, id: I) -> Option<Option<Waiting<J>>> {
        if !self.pending.as_ref().is_some_and(|pending| pending.peer == peer && pending.id == id) {
            return None;
        }
        Some(self.pending.take().expect("checked startup transport owner").work)
    }

    pub(super) fn unavailable(&mut self, peer: PeerId, id: I) -> Result<bool, LiveSyncError> {
        let Some(work) = self.retire(peer, id) else { return Ok(false) };
        self.retry(work)?;
        Ok(true)
    }

    pub(super) fn disconnected(&mut self, peer: PeerId) -> Result<(), LiveSyncError> {
        if self.pending.as_ref().is_some_and(|pending| pending.peer == peer) {
            let pending = self.pending.take().expect("checked disconnected startup owner");
            self.retry(pending.work)?;
        }
        Ok(())
    }

    pub(super) fn timed_out(&self) -> bool {
        self.pending.as_ref().is_some_and(|pending| pending.issued.elapsed() >= REQUEST_TIMEOUT)
    }

    fn retry(&mut self, work: Option<Waiting<J>>) -> Result<(), LiveSyncError> {
        if let Some(work) = work {
            if self.waiting.is_some() {
                return Err(LiveSyncError::Bounds);
            }
            // Keep the exact original job, deadline, and bounded attempted-peer inventory.
            self.waiting = Some(work);
        }
        Ok(())
    }
}

pub(super) struct Waiting<J> {
    pub(super) job: J,
    started: Instant,
    tried: Vec<PeerId>,
}
impl<J: StartupJob> Waiting<J> {
    fn expired(&self) -> bool {
        self.job.closed() || self.started.elapsed() >= DATA_DEADLINE
    }
}
struct Pending<J, I> {
    peer: PeerId,
    id: I,
    issued: Instant,
    work: Option<Waiting<J>>,
}

pub(super) trait StartupJob {
    fn slot(&self) -> u64;
    fn created(&self) -> Instant;
    fn closed(&self) -> bool;
    fn unavailable(self);
    fn provider_score(&self, _runtime: &CheckpointPeerRuntime, _peer: PeerId) -> usize {
        0
    }
    fn candidate(&self) -> Option<(Root, u64)> {
        None
    }
}
impl StartupJob for RetrievalJob {
    fn slot(&self) -> u64 {
        self.slot
    }
    fn created(&self) -> Instant {
        self.created
    }
    fn closed(&self) -> bool {
        self.reply.is_closed()
    }
    fn unavailable(self) {
        self.complete(Err(RetrievalError::Missing));
    }
    fn provider_score(&self, runtime: &CheckpointPeerRuntime, peer: PeerId) -> usize {
        sidecar_custody_overlap(runtime, peer, &self.request)
    }
    fn candidate(&self) -> Option<(Root, u64)> {
        Some((self.root, self.slot))
    }
}
impl StartupJob for EnvelopeRetrievalJob {
    fn slot(&self) -> u64 {
        self.metadata.slot()
    }
    fn created(&self) -> Instant {
        self.created
    }
    fn closed(&self) -> bool {
        self.reply.is_closed()
    }
    fn unavailable(self) {
        self.complete(Err(RetrievalError::Missing));
    }
}

#[cfg(test)]
mod tests;
