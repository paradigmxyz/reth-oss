//! Fixed-cardinality observations of real import work and counted command reservations.
//! Counters never confer consensus, execution or availability authority.

use super::*;
use std::sync::atomic::{AtomicU64, AtomicUsize};

/// Per-owner measured import attempts, including exact-original recovery replay.
#[derive(Clone, Copy, Debug, Default)]
pub struct ImportObservation {
    /// Original block/envelope attempts actually processed by the owner.
    pub attempts_total: u64,
    /// Signed beacon-block attempts, including gossip and retained duplicate acknowledgements.
    pub blocks_total: u64,
    /// Signed execution-envelope attempts, including gossip and retained duplicates.
    pub envelopes_total: u64,
    /// Successful checked import attempts; this is not an Engine VALID counter.
    pub accepted_total: u64,
    /// Rejected attempts other than explicit execution deferrals.
    pub rejected_total: u64,
    /// Exact attempts deferred with the native ExecutionPending outcome.
    pub execution_deferred_total: u64,
    /// Original input bytes attempted, including retries and duplicate acknowledgements.
    pub attempted_bytes_total: u64,
    /// Cumulative owner processing time, including mandatory Engine/DA/storage work.
    pub duration_micros_total: u64,
    /// Duration of the most recently completed attempt.
    pub last_duration_micros: u64,
    /// Largest observed completed attempt duration.
    pub max_duration_micros: u64,
    /// Attempts performed during original historical/recovery replay.
    pub replay_attempts_total: u64,
    /// Successfully reapplied ordered original events, including clocks and off-block votes.
    pub replayed_events_total: u64,
    /// Successful block transitions, excluding already admitted duplicates.
    pub newly_admitted_blocks_total: u64,
    /// Valid proposer signatures rechecked for already admitted block roots.
    pub duplicate_blocks_total: u64,
    /// Cumulative successful new block processing time, excluding duplicate acknowledgements.
    pub new_block_duration_micros_total: u64,
    /// Duration of the last successful new block, excluding duplicate acknowledgements.
    pub last_new_block_duration_micros: u64,
    /// Successful consensus-verified classic imports with measured native phases, including
    /// strict execution and explicitly tracked optimistic execution.
    pub measured_native_imports_total: u64,
    /// Cumulative required availability verification duration of those measured imports.
    pub availability_micros_total: u64,
    /// Cumulative transition duration, including the mandatory typed execution callback.
    pub transition_with_execution_micros_total: u64,
    /// Cumulative payload callback waits for successful measured classic imports. Genuine
    /// deferred execution responses contribute time without granting execution authority.
    pub execution_wait_micros_total: u64,
    /// Cumulative native graph and checkpoint processing duration.
    pub native_fork_choice_micros_total: u64,
    /// Cumulative compact state retention, persistence and cooling duration.
    pub state_persistence_micros_total: u64,
}
impl ImportObservation {
    pub(super) fn record(
        &mut self,
        is_block: bool,
        bytes: usize,
        replaying: bool,
        micros: u64,
        result: &Result<ImportReceipt, ImportError>,
    ) {
        self.attempts_total = self.attempts_total.saturating_add(1);
        if is_block {
            self.blocks_total = self.blocks_total.saturating_add(1);
        } else {
            self.envelopes_total = self.envelopes_total.saturating_add(1);
        }
        self.attempted_bytes_total = self.attempted_bytes_total.saturating_add(bytes as u64);
        if replaying {
            self.replay_attempts_total = self.replay_attempts_total.saturating_add(1);
        }
        match result {
            Ok(_) => self.accepted_total = self.accepted_total.saturating_add(1),
            Err(ImportError::ExecutionPending(_)) => {
                self.execution_deferred_total = self.execution_deferred_total.saturating_add(1)
            }
            Err(_) => self.rejected_total = self.rejected_total.saturating_add(1),
        }
        self.last_duration_micros = micros;
        self.max_duration_micros = self.max_duration_micros.max(micros);
        self.duration_micros_total = self.duration_micros_total.saturating_add(micros);
    }

    pub(super) fn record_phases(
        &mut self,
        native: Option<reth_embedded_beacon_fork_choice::store::BlockImportTimings>,
        execution_micros: Option<u64>,
    ) {
        if let Some(native) = native {
            self.measured_native_imports_total =
                self.measured_native_imports_total.saturating_add(1);
            self.availability_micros_total =
                self.availability_micros_total.saturating_add(native.availability_micros);
            self.transition_with_execution_micros_total = self
                .transition_with_execution_micros_total
                .saturating_add(native.state_transition_micros);
            self.execution_wait_micros_total = self
                .execution_wait_micros_total
                .saturating_add(execution_micros.unwrap_or_default());
            self.native_fork_choice_micros_total =
                self.native_fork_choice_micros_total.saturating_add(native.fork_choice_micros);
            self.state_persistence_micros_total =
                self.state_persistence_micros_total.saturating_add(native.persistence_micros);
        }
    }
}

/// Semaphore-backed command reservations, including the active job and cancelled replies.
#[derive(Clone, Copy, Debug)]
pub struct ImportQueueObservation {
    /// Currently held command reservations.
    pub reserved_jobs: usize,
    /// Currently held original-input byte reservations.
    pub reserved_bytes: usize,
    /// Fixed maximum command reservations.
    pub job_capacity: usize,
    /// Fixed maximum original-input byte reservations.
    pub byte_capacity: usize,
    /// Highest occupied command reservations sampled at successful command admission.
    pub jobs_high_water: usize,
    /// Highest occupied input-byte reservations sampled at successful command admission.
    pub bytes_high_water: usize,
    /// Successfully enqueued block/envelope attempts, independently of cancelled replies.
    pub imports_enqueued_total: u64,
    /// Block/envelope enqueue attempts rejected by bounded queue backpressure.
    pub import_backpressure_total: u64,
}

#[derive(Debug, Default)]
pub(super) struct QueueHighWater {
    jobs: AtomicUsize,
    bytes: AtomicUsize,
    pub(super) imports_enqueued: AtomicU64,
    pub(super) import_backpressure: AtomicU64,
}
impl QueueHighWater {
    pub(super) fn increment(counter: &AtomicU64) {
        let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
            Some(value.saturating_add(1))
        });
    }
}
impl ImportCoordinator {
    /// O(1) counters read independently of the graph owner; adjacent admissions may be visible.
    pub fn queue_observation(&self) -> ImportQueueObservation {
        let (reserved_jobs, reserved_bytes) = self.reserved_resources();
        ImportQueueObservation {
            reserved_jobs,
            reserved_bytes,
            job_capacity: self.limits.command_capacity,
            byte_capacity: self.limits.max_queued_bytes,
            jobs_high_water: self.queue_high_water.jobs.load(Ordering::Relaxed),
            bytes_high_water: self.queue_high_water.bytes.load(Ordering::Relaxed),
            imports_enqueued_total: self.queue_high_water.imports_enqueued.load(Ordering::Relaxed),
            import_backpressure_total: self
                .queue_high_water
                .import_backpressure
                .load(Ordering::Relaxed),
        }
    }
    pub(super) fn observe_queue_admission(&self) {
        let (jobs, bytes) = self.reserved_resources();
        self.queue_high_water.jobs.fetch_max(jobs, Ordering::Relaxed);
        self.queue_high_water.bytes.fetch_max(bytes, Ordering::Relaxed);
    }
}
