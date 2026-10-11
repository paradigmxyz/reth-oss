//! Export the actual importer and queue counters without executing imports.

use reth_embedded_beacon_node::import_coordinator::{ImportObservation, ImportQueueObservation};
use reth_metrics::{
    metrics::{self, Counter, Gauge},
    Metrics,
};

#[derive(Metrics)]
#[metrics(scope = "embedded_beacon.import")]
pub(super) struct ImportMetrics {
    /// Native import attempts, including rejected and execution-deferred outcomes.
    attempts_total: Counter,
    /// Cumulative blocks since this owner started.
    blocks_total: Counter,
    /// Cumulative envelopes since this owner started.
    envelopes_total: Counter,
    /// Native consensus admissions; this does not assert execution validity.
    accepted_total: Counter,
    /// Successful new block transitions, excluding duplicate acknowledgements.
    newly_admitted_blocks_total: Counter,
    /// Successfully rechecked signatures of already admitted blocks.
    duplicate_blocks_total: Counter,
    /// Cumulative processing time of successful new blocks, in microseconds.
    new_block_duration_micros_total: Counter,
    /// Latest successful new block duration, in seconds.
    last_new_block_duration_seconds: Gauge,
    /// Successful strict classic imports with native phase measurements.
    measured_native_imports_total: Counter,
    /// Cumulative required availability retrieval and verification, in microseconds.
    availability_micros_total: Counter,
    /// Cumulative transition time including execution verification, in microseconds.
    transition_with_execution_micros_total: Counter,
    /// Cumulative payload API wait time in measured imports, in microseconds.
    execution_wait_micros_total: Counter,
    /// Cumulative native graph and checkpoint processing, in microseconds.
    native_fork_choice_micros_total: Counter,
    /// Cumulative state retention, durable writes and cooling, in microseconds.
    state_persistence_micros_total: Counter,
    /// Native import attempts that returned a definitive error.
    rejected_total: Counter,
    /// Attempts deferred while required execution data was unavailable.
    execution_deferred_total: Counter,
    /// Cumulative attempted bytes since this owner started.
    attempted_bytes_total: Counter,
    /// Cumulative native import time, in microseconds.
    duration_micros_total: Counter,
    /// Cumulative replay attempts since this owner started.
    replay_attempts_total: Counter,
    /// Original retained recovery events successfully replayed through native checks.
    replayed_events_total: Counter,
    /// Duration of the last completed native import attempt, in seconds.
    last_duration_seconds: Gauge,
    /// Longest completed native import attempt since this owner started, in seconds.
    max_duration_seconds: Gauge,
    /// Configured bounded importer command capacity.
    queue_job_capacity: Gauge,
    /// Configured bounded importer input byte capacity.
    queue_byte_capacity: Gauge,
    /// Maximum actual held command permits observed at successful admission.
    queue_jobs_high_water: Gauge,
    /// Maximum actual held input byte permits observed at successful admission.
    queue_bytes_high_water: Gauge,
    /// Cumulative enqueued since this owner started.
    enqueued_total: Counter,
    /// Import admissions rejected by actual bounded command or byte limits.
    backpressure_total: Counter,
}

impl ImportMetrics {
    pub(super) fn record(&self, imports: ImportObservation, queue: ImportQueueObservation) {
        self.attempts_total.absolute(imports.attempts_total);
        self.blocks_total.absolute(imports.blocks_total);
        self.envelopes_total.absolute(imports.envelopes_total);
        self.accepted_total.absolute(imports.accepted_total);
        self.newly_admitted_blocks_total.absolute(imports.newly_admitted_blocks_total);
        self.duplicate_blocks_total.absolute(imports.duplicate_blocks_total);
        self.new_block_duration_micros_total.absolute(imports.new_block_duration_micros_total);
        self.last_new_block_duration_seconds
            .set(imports.last_new_block_duration_micros as f64 / 1_000_000.0);
        self.measured_native_imports_total.absolute(imports.measured_native_imports_total);
        self.availability_micros_total.absolute(imports.availability_micros_total);
        self.transition_with_execution_micros_total
            .absolute(imports.transition_with_execution_micros_total);
        self.execution_wait_micros_total.absolute(imports.execution_wait_micros_total);
        self.native_fork_choice_micros_total.absolute(imports.native_fork_choice_micros_total);
        self.state_persistence_micros_total.absolute(imports.state_persistence_micros_total);
        self.rejected_total.absolute(imports.rejected_total);
        self.execution_deferred_total.absolute(imports.execution_deferred_total);
        self.attempted_bytes_total.absolute(imports.attempted_bytes_total);
        self.duration_micros_total.absolute(imports.duration_micros_total);
        self.replay_attempts_total.absolute(imports.replay_attempts_total);
        self.replayed_events_total.absolute(imports.replayed_events_total);
        self.last_duration_seconds.set(imports.last_duration_micros as f64 / 1_000_000.0);
        self.max_duration_seconds.set(imports.max_duration_micros as f64 / 1_000_000.0);
        self.queue_job_capacity.set(queue.job_capacity as f64);
        self.queue_byte_capacity.set(queue.byte_capacity as f64);
        self.queue_jobs_high_water.set(queue.jobs_high_water as f64);
        self.queue_bytes_high_water.set(queue.bytes_high_water as f64);
        self.enqueued_total.absolute(queue.imports_enqueued_total);
        self.backpressure_total.absolute(queue.import_backpressure_total);
    }
}
