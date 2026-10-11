//! Retire unresolved data providers before their original broker caller can time out.
//!
//! Socket retirement keeps the exact pending peer/id and job charged until the real
//! Closed/Rejected event resolves it. It never cancels proof workers or reuses a live token.

use super::*;

const DATA_DEADLINE: Duration = Duration::from_secs(18);

impl LiveSync {
    pub(super) fn retire_expired_data_provider(&mut self) -> Result<(), LiveSyncError> {
        let Some(peer) = self.expired_data_provider(Instant::now()) else { return Ok(()) };
        self.peer.as_mut().ok_or(LiveSyncError::PeerUnavailable)?.retire_peer(peer)?;
        tracing::debug!(target: "beacon::sync", %peer,
            "[beacon] Retired expired data provider while preserving pending response ownership");
        Ok(())
    }

    fn expired_data_provider(&self, now: Instant) -> Option<PeerId> {
        self.sidecar_request
            .as_ref()
            .filter(|pending| data_expired(pending.started, now))
            .map(|pending| pending.peer)
            .or_else(|| {
                self.broker_envelope
                    .as_ref()
                    .filter(|pending| data_expired(pending.started, now))
                    .map(|pending| pending.peer)
            })
            .or_else(|| self.history.expired_provider(now, DATA_DEADLINE))
    }
}

fn data_expired(started: Instant, now: Instant) -> bool {
    now.saturating_duration_since(started) >= DATA_DEADLINE
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn data_deadline_precedes_caller_timeout_without_extending_original_start() {
        let started = Instant::now();
        assert!(!data_expired(started, started + DATA_DEADLINE - Duration::from_nanos(1)));
        assert!(data_expired(started, started + DATA_DEADLINE));
        assert!(data_expired(started, started + Duration::from_secs(20)));
        assert!(!data_expired(started, started - Duration::from_secs(1)));
        assert!(DATA_DEADLINE < availability::RETRIEVAL_TIMEOUT);
        assert!(DATA_DEADLINE + Duration::from_secs(1) < availability::RETRIEVAL_TIMEOUT);
    }
}
