//! Sample the existing shared writer in constant time without acquiring a blocking lock.

use reth_embedded_beacon_shared_data::SharedDataStore;
use reth_metrics::{
    metrics::{self, Counter, Gauge},
    Metrics,
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Metrics)]
#[metrics(scope = "embedded_beacon.shared_data")]
struct SharedMetrics {
    /// Whether the most recent owner observation completed successfully.
    observation_available: Gauge,
    /// Unix timestamp of the latest successful shared writer observation.
    last_observation_timestamp_seconds: Gauge,
    /// Whether the original shared writer journal remains healthy.
    healthy: Gauge,
    /// Objects recorded in the successfully committed reference map.
    tracked_objects: Gauge,
    /// Committed object lengths, excluding journal files and untracked orphans.
    tracked_bytes: Gauge,
    /// Original object lengths counted once per independent durable pin.
    pinned_logical_bytes: Gauge,
    /// Original object lengths counted once per object having a durable pin.
    pinned_unique_bytes: Gauge,
    /// Current measured owner pins.
    owner_pins: Gauge,
    /// Current measured owners.
    owners: Gauge,
    /// Current measured cache entries.
    cache_entries: Gauge,
    /// Current measured cache bytes.
    cache_bytes: Gauge,
    /// Current measured cache capacity bytes.
    cache_capacity_bytes: Gauge,
    /// Cumulative cache hits since this owner started.
    cache_hits_total: Counter,
    /// Cumulative cache misses since this owner started.
    cache_misses_total: Counter,
    /// Cumulative cache evictions since this owner started.
    cache_evictions_total: Counter,
    /// Original bytes successfully read and integrity checked from disk.
    verified_disk_read_bytes_total: Counter,
    /// Cumulative missing reads since this owner started.
    missing_reads_total: Counter,
    /// Cumulative oversized reads since this owner started.
    oversized_reads_total: Counter,
    /// Cumulative journal commits since this owner started.
    journal_commits_total: Counter,
    /// Cumulative successful shared reference journal commit time, in microseconds.
    journal_commit_micros_total: Counter,
    /// Cumulative garbage collected objects since this owner started.
    garbage_collected_objects_total: Counter,
    /// Cumulative garbage collected bytes since this owner started.
    garbage_collected_bytes_total: Counter,
}

pub(in crate::reth) async fn observe_shared(store: SharedDataStore) {
    let metrics = SharedMetrics::default();
    let mut tick = tokio::time::interval(Duration::from_secs(5));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tick.tick().await;
        let Some(observation) = store.try_observe() else {
            metrics.observation_available.set(0.0);
            continue;
        };
        metrics.observation_available.set(1.0);
        metrics.last_observation_timestamp_seconds.set(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|time| time.as_secs_f64())
                .unwrap_or(f64::NAN),
        );
        metrics.healthy.set(u8::from(observation.healthy) as f64);
        metrics.tracked_objects.set(observation.tracked_objects as f64);
        metrics.tracked_bytes.set(observation.tracked_bytes as f64);
        metrics.pinned_logical_bytes.set(observation.pinned_logical_bytes as f64);
        metrics.pinned_unique_bytes.set(observation.pinned_unique_bytes as f64);
        metrics.owner_pins.set(observation.owner_pins as f64);
        metrics.owners.set(observation.owners as f64);
        metrics.cache_entries.set(observation.cache_entries as f64);
        metrics.cache_bytes.set(observation.cache_bytes as f64);
        metrics.cache_capacity_bytes.set(observation.cache_capacity_bytes as f64);
        metrics.cache_hits_total.absolute(observation.cache_hits_total);
        metrics.cache_misses_total.absolute(observation.cache_misses_total);
        metrics.cache_evictions_total.absolute(observation.cache_evictions_total);
        metrics.verified_disk_read_bytes_total.absolute(observation.verified_disk_read_bytes_total);
        metrics.missing_reads_total.absolute(observation.missing_reads_total);
        metrics.oversized_reads_total.absolute(observation.oversized_reads_total);
        metrics.journal_commits_total.absolute(observation.journal_commits_total);
        metrics.journal_commit_micros_total.absolute(observation.journal_commit_micros_total);
        metrics
            .garbage_collected_objects_total
            .absolute(observation.garbage_collected_objects_total);
        metrics.garbage_collected_bytes_total.absolute(observation.garbage_collected_bytes_total);
    }
}
