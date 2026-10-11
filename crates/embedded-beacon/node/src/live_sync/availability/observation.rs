//! Observations of existing bounded availability leases and owner flags.
//! No snapshot grants mathematical proof, retained custody or serving authority.

use super::*;

/// Actual occupied availability worker reservations and existing broker state.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AvailabilityObservation {
    /// Sidecar/envelope request reservations, including jobs whose replies were cancelled.
    pub retrieval_jobs: u64,
    /// Blocking proof reservations held until mandatory verification actually finishes.
    pub proof_jobs: u64,
    /// Original historical-block retrieval reservations.
    pub history_jobs: u64,
    /// The actual custody retrieval owner has begun its bounded work.
    pub custody_retrieving: bool,
    /// The current exact candidate was deferred for missing original data.
    pub deferred_data: bool,
    /// Existing availability admission has halted and requires reconciliation.
    pub halted: bool,
    /// A real shared sidecar archive is bound; this does not assert any retained coverage.
    pub archive_enabled: bool,
}

impl RetrievalAvailability {
    /// O(1) lease/flag snapshot without filesystem reads, archive scans or proof inference.
    pub fn observation(&self) -> AvailabilityObservation {
        AvailabilityObservation {
            retrieval_jobs: 1usize.saturating_sub(self.requests.available_permits()) as u64,
            proof_jobs: 1usize.saturating_sub(self.proofs.available_permits()) as u64,
            history_jobs: 1usize.saturating_sub(self.history_requests.available_permits()) as u64,
            custody_retrieving: self.custody_retrieving.load(Ordering::Acquire),
            deferred_data: self.deferred_data.load(Ordering::Acquire),
            halted: self.halted.load(Ordering::Acquire),
            archive_enabled: self.archive.is_some(),
        }
    }
}
