//! Export bounded resource samples; partial scans are never exported as storage totals.

use super::resources::{DirectoryUsage, FileUsage, ResourceCollector, ResourceSample};
use reth_metrics::{
    metrics::{self, Counter, Gauge, Histogram},
    Metrics,
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Metrics)]
#[metrics(scope = "embedded_beacon.resources")]
struct ResourceMetrics {
    /// Whether the most recent blocking resource sample completed successfully.
    sample_available: Gauge,
    /// Elapsed seconds between collection start and recording the resource sample.
    sample_age_seconds: Gauge,
    /// Unix timestamp of the latest successful resource observation.
    last_sample_timestamp_seconds: Gauge,
    /// Observed sampling seconds.
    sampling_seconds: Histogram,
    /// Whether all process I/O counters were available in the latest sample.
    process_io_available: Gauge,
    /// Current measured process memory available.
    process_memory_available: Gauge,
    /// Whether both page-fault counters were available in the latest sample.
    process_faults_available: Gauge,
    /// Current measured process resident bytes.
    process_resident_bytes: Gauge,
    /// Current measured process virtual bytes.
    process_virtual_bytes: Gauge,
    /// Current measured process threads.
    process_threads: Gauge,
    /// Current measured process open fds.
    process_open_fds: Gauge,
    /// Process CPU seconds per wall second; may exceed one on multiple cores.
    cpu_cores: Gauge,
    /// Current measured process swap bytes.
    process_swap_bytes: Gauge,
    /// Current measured process anonymous bytes.
    process_anonymous_bytes: Gauge,
    /// Current measured process file backed bytes.
    process_file_backed_bytes: Gauge,
    /// Current measured process shared bytes.
    process_shared_bytes: Gauge,
    /// Current measured process peak resident bytes.
    process_peak_resident_bytes: Gauge,
    /// Current measured process peak virtual bytes.
    process_peak_virtual_bytes: Gauge,
    /// Cumulative process read bytes since this owner started.
    process_read_bytes_total: Counter,
    /// Cumulative process write bytes since this owner started.
    process_write_bytes_total: Counter,
    /// Cumulative process read chars since this owner started.
    process_read_chars_total: Counter,
    /// Cumulative process write chars since this owner started.
    process_write_chars_total: Counter,
    /// Cumulative process read syscalls since this owner started.
    process_read_syscalls_total: Counter,
    /// Cumulative process write syscalls since this owner started.
    process_write_syscalls_total: Counter,
    /// Cumulative process cancelled write bytes since this owner started.
    process_cancelled_write_bytes_total: Counter,
    /// Cumulative process minor faults since this owner started.
    process_minor_faults_total: Counter,
    /// Cumulative process major faults since this owner started.
    process_major_faults_total: Counter,
    /// Whether the bounded file descriptor enumeration was complete.
    file_descriptors_complete: Gauge,
    /// Whether current filesystem capacity was successfully measured.
    filesystem_available: Gauge,
    /// Current measured filesystem total bytes.
    filesystem_total_bytes: Gauge,
    /// Current measured filesystem free bytes.
    filesystem_free_bytes: Gauge,
    /// Current measured filesystem available bytes.
    filesystem_available_bytes: Gauge,
    /// Current measured filesystem total inodes.
    filesystem_total_inodes: Gauge,
    /// Current measured filesystem free inodes.
    filesystem_free_inodes: Gauge,
    /// Current measured filesystem available inodes.
    filesystem_available_inodes: Gauge,
}

impl ResourceMetrics {
    fn record(&self, sample: &ResourceSample) {
        self.sample_available.set(1.0);
        self.sample_age_seconds.set(sample.captured_at.elapsed().as_secs_f64());
        self.last_sample_timestamp_seconds.set(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|time| time.as_secs_f64())
                .unwrap_or(f64::NAN),
        );
        self.sampling_seconds.record(sample.sampling_seconds);
        self.cpu_cores.set(sample.cpu_cores.unwrap_or(f64::NAN));
        let process = &sample.process;
        self.process_memory_available.set(u8::from(process.memory.is_some()) as f64);
        set_optional(
            &self.process_resident_bytes,
            process.memory.as_ref().and_then(|memory| memory.resident_bytes),
        );
        set_optional(
            &self.process_virtual_bytes,
            process.memory.as_ref().and_then(|memory| memory.virtual_bytes),
        );
        set_optional(
            &self.process_threads,
            process.memory.as_ref().and_then(|memory| memory.threads),
        );
        set_optional(&self.process_open_fds, process.open_fds.and_then(|fds| fds.total()));
        self.process_faults_available
            .set(u8::from(process.minor_faults.is_some() && process.major_faults.is_some()) as f64);
        set_optional(
            &self.process_swap_bytes,
            process.memory.as_ref().and_then(|memory| memory.swap_bytes),
        );
        set_optional(
            &self.process_anonymous_bytes,
            process.memory.as_ref().and_then(|memory| memory.anonymous_bytes),
        );
        set_optional(
            &self.process_file_backed_bytes,
            process.memory.as_ref().and_then(|memory| memory.file_backed_bytes),
        );
        set_optional(
            &self.process_shared_bytes,
            process.memory.as_ref().and_then(|memory| memory.shared_bytes),
        );
        set_optional(
            &self.process_peak_resident_bytes,
            process.memory.as_ref().and_then(|memory| memory.peak_resident_bytes),
        );
        set_optional(
            &self.process_peak_virtual_bytes,
            process.memory.as_ref().and_then(|memory| memory.peak_virtual_bytes),
        );
        let io = process.io.as_ref();
        let complete_io = io.is_some_and(|io| {
            io.read_bytes.is_some() &&
                io.write_bytes.is_some() &&
                io.rchar.is_some() &&
                io.wchar.is_some() &&
                io.syscr.is_some() &&
                io.syscw.is_some() &&
                io.cancelled_write_bytes.is_some()
        });
        self.process_io_available.set(u8::from(complete_io) as f64);
        if let Some(value) = io.and_then(|io| io.read_bytes) {
            self.process_read_bytes_total.absolute(value);
        }
        if let Some(value) = io.and_then(|io| io.write_bytes) {
            self.process_write_bytes_total.absolute(value);
        }
        if let Some(value) = io.and_then(|io| io.rchar) {
            self.process_read_chars_total.absolute(value);
        }
        if let Some(value) = io.and_then(|io| io.wchar) {
            self.process_write_chars_total.absolute(value);
        }
        if let Some(value) = io.and_then(|io| io.syscr) {
            self.process_read_syscalls_total.absolute(value);
        }
        if let Some(value) = io.and_then(|io| io.syscw) {
            self.process_write_syscalls_total.absolute(value);
        }
        if let Some(value) = io.and_then(|io| io.cancelled_write_bytes) {
            self.process_cancelled_write_bytes_total.absolute(value);
        }
        if let Some(value) = process.minor_faults {
            self.process_minor_faults_total.absolute(value);
        }
        if let Some(value) = process.major_faults {
            self.process_major_faults_total.absolute(value);
        }
        self.file_descriptors_complete
            .set(u8::from(process.open_fds.is_some_and(|fds| fds.complete)) as f64);
        self.filesystem_available.set(u8::from(sample.filesystem.is_some()) as f64);
        set_optional(
            &self.filesystem_total_bytes,
            sample.filesystem.map(|filesystem| filesystem.total_bytes),
        );
        set_optional(
            &self.filesystem_free_bytes,
            sample.filesystem.map(|filesystem| filesystem.free_bytes),
        );
        set_optional(
            &self.filesystem_available_bytes,
            sample.filesystem.map(|filesystem| filesystem.available_bytes),
        );
        set_optional(
            &self.filesystem_total_inodes,
            sample.filesystem.and_then(|filesystem| filesystem.total_inodes),
        );
        set_optional(
            &self.filesystem_free_inodes,
            sample.filesystem.and_then(|filesystem| filesystem.free_inodes),
        );
        set_optional(
            &self.filesystem_available_inodes,
            sample.filesystem.and_then(|filesystem| filesystem.available_inodes),
        );
        let storage = sample.storage.as_ref();
        let age =
            storage.map(|storage| storage.captured_at.elapsed().as_secs_f64()).unwrap_or(f64::NAN);
        record_area("database", storage.and_then(|storage| storage.database), None, age);
        record_directory("static_files", storage.and_then(|storage| storage.static_files), age);
        record_directory("shared_data", storage.and_then(|storage| storage.shared_data), age);
    }
}

pub(in crate::reth) async fn observe_resources(mut collector: ResourceCollector) {
    let metrics = ResourceMetrics::default();
    let mut tick = tokio::time::interval(Duration::from_secs(30));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tick.tick().await;
        if let Some(sample) = collector.sample().await {
            metrics.record(&sample);
        } else {
            metrics.sample_available.set(0.0);
        }
    }
}

fn set_optional(gauge: &Gauge, value: Option<u64>) {
    gauge.set(value.map(|value| value as f64).unwrap_or(f64::NAN));
}

fn record_directory(area: &'static str, usage: Option<DirectoryUsage>, age: f64) {
    record_area(area, usage.and_then(DirectoryUsage::total_usage), usage, age);
}

fn record_area(
    area: &'static str,
    total: Option<FileUsage>,
    directory: Option<DirectoryUsage>,
    age: f64,
) {
    metrics::gauge!("embedded_beacon.resources.storage_scan_complete", "area" => area)
        .set(u8::from(total.is_some()) as f64);
    metrics::gauge!("embedded_beacon.resources.storage_scan_age_seconds", "area" => area).set(age);
    set_optional(
        &metrics::gauge!("embedded_beacon.resources.storage_logical_bytes", "area" => area),
        total.map(|usage| usage.logical_bytes),
    );
    set_optional(
        &metrics::gauge!("embedded_beacon.resources.storage_allocated_bytes", "area" => area),
        total.and_then(|usage| usage.allocated_bytes),
    );
    set_optional(
        &metrics::gauge!("embedded_beacon.resources.storage_scan_entries", "area" => area),
        directory.map(|usage| usage.entries),
    );
    set_optional(
        &metrics::gauge!("embedded_beacon.resources.storage_scanned_lower_bound_bytes", "area" => area),
        directory.map(|usage| usage.observed.logical_bytes),
    );
}
