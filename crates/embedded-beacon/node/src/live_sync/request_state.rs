//! Ownership transitions for one mutually exclusive waiting or requested retrieval job.

use super::LiveSyncError;

#[derive(Default)]
pub(super) enum RequestState<W, R> {
    #[default]
    Idle,
    Waiting(W),
    Requested(R),
}

impl<W, R> RequestState<W, R> {
    pub(super) const fn is_idle(&self) -> bool {
        matches!(self, Self::Idle)
    }

    pub(super) const fn waiting(&self) -> Option<&W> {
        match self {
            Self::Waiting(job) => Some(job),
            _ => None,
        }
    }

    pub(super) const fn requested(&self) -> Option<&R> {
        match self {
            Self::Requested(job) => Some(job),
            _ => None,
        }
    }

    pub(super) fn take_waiting(&mut self) -> Option<W> {
        self.waiting()?;
        match std::mem::take(self) {
            Self::Waiting(job) => Some(job),
            _ => unreachable!("checked waiting request state"),
        }
    }

    pub(super) fn take_requested(&mut self) -> Option<R> {
        self.requested()?;
        match std::mem::take(self) {
            Self::Requested(job) => Some(job),
            _ => unreachable!("checked outstanding request state"),
        }
    }

    pub(super) fn wait(&mut self, job: W) -> Result<(), LiveSyncError> {
        if !self.is_idle() {
            return Err(LiveSyncError::Bounds);
        }
        *self = Self::Waiting(job);
        Ok(())
    }

    pub(super) fn request(&mut self, job: R) -> Result<(), LiveSyncError> {
        if !self.is_idle() {
            return Err(LiveSyncError::Bounds);
        }
        *self = Self::Requested(job);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn outstanding_ownership_survives_wrong_phase_and_conflicting_transitions() {
        let mut state = RequestState::<u64, u64>::default();
        state.wait(10).unwrap();
        assert!(state.request(20).is_err());
        assert_eq!(state.take_requested(), None);
        assert_eq!(state.take_waiting(), Some(10));
        state.request(20).unwrap();
        assert!(state.wait(30).is_err());
        assert_eq!(state.take_waiting(), None);
        assert_eq!(state.requested(), Some(&20));
        assert_eq!(state.take_requested(), Some(20));
        assert!(state.is_idle());
    }
}
