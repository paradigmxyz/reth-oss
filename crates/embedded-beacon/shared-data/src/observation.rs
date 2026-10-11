//! Constant-time observations of the existing shared writer, without filesystem scans.

use super::{SharedDataStore, StoreInner};

/// Reference metadata and process-local cache/maintenance counters.
/// Tracked bytes exclude orphan files, journals and external caller-held byte buffers.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StoreObservation {
    /// Whether the original writer's journal remains healthy.
    pub healthy: bool,
    /// Objects in the last successfully committed reference map.
    pub tracked_objects: u64,
    /// Original object lengths counted once per committed object.
    pub tracked_bytes: u64,
    /// Original lengths counted once per independent durable pin.
    pub pinned_logical_bytes: u64,
    /// Original lengths counted once per object with at least one durable pin.
    pub pinned_unique_bytes: u64,
    /// Independent durable pins.
    pub owner_pins: u64,
    /// Distinct owners in the committed reference map.
    pub owners: u64,
    /// Objects held by the byte cache.
    pub cache_entries: u64,
    /// Original lengths held by the byte cache.
    pub cache_bytes: u64,
    /// Configured cache ceiling; caller-held evicted buffers are additional.
    pub cache_capacity_bytes: u64,
    /// Successful reads from the shared byte cache.
    pub cache_hits_total: u64,
    /// Known-object reads requiring disk access.
    pub cache_misses_total: u64,
    /// Cache entries evicted to respect the configured byte ceiling.
    pub cache_evictions_total: u64,
    /// Original bytes successfully read and integrity checked from disk.
    pub verified_disk_read_bytes_total: u64,
    /// Reads of absent committed objects.
    pub missing_reads_total: u64,
    /// Reads rejected before allocation because the supplied bound was exceeded.
    pub oversized_reads_total: u64,
    /// Successful reference journal commits since this writer opened.
    pub journal_commits_total: u64,
    /// Total successful reference journal commit time in microseconds.
    pub journal_commit_micros_total: u64,
    /// Object files successfully removed by garbage collection since open.
    pub garbage_collected_objects_total: u64,
    /// Original file lengths successfully removed by garbage collection since open.
    pub garbage_collected_bytes_total: u64,
}

impl SharedDataStore {
    /// Obtain an O(1) view without waiting behind a writer or performing disk operations.
    /// A busy writer returns `None`, rather than a stale or fabricated healthy sample.
    pub fn try_observe(&self) -> Option<StoreObservation> {
        let inner = self.inner.try_lock()?;
        Some(StoreObservation {
            healthy: inner.journal.ensure_healthy().is_ok(),
            tracked_objects: inner.objects.len() as u64,
            tracked_bytes: inner.references.tracked_bytes,
            pinned_logical_bytes: inner.references.pinned_logical_bytes,
            pinned_unique_bytes: inner.references.pinned_unique_bytes,
            owner_pins: inner.references.owner_pins,
            owners: inner.owner_index.len() as u64,
            cache_entries: inner.cache.len() as u64,
            cache_bytes: inner.cache_bytes as u64,
            cache_capacity_bytes: inner.cache_limit as u64,
            cache_hits_total: inner.observations.cache_hits_total,
            cache_misses_total: inner.observations.cache_misses_total,
            cache_evictions_total: inner.observations.cache_evictions_total,
            verified_disk_read_bytes_total: inner.observations.verified_disk_read_bytes_total,
            missing_reads_total: inner.observations.missing_reads_total,
            oversized_reads_total: inner.observations.oversized_reads_total,
            journal_commits_total: inner.observations.journal_commits_total,
            journal_commit_micros_total: inner.observations.journal_commit_micros_total,
            garbage_collected_objects_total: inner.observations.garbage_collected_objects_total,
            garbage_collected_bytes_total: inner.observations.garbage_collected_bytes_total,
        })
    }
}

impl StoreInner {
    pub(super) fn record_collection(&mut self, bytes: u64) {
        self.observations.garbage_collected_objects_total =
            self.observations.garbage_collected_objects_total.saturating_add(1);
        self.observations.garbage_collected_bytes_total =
            self.observations.garbage_collected_bytes_total.saturating_add(bytes);
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct ReferenceObservation {
    pub(super) tracked_bytes: u64,
    pub(super) pinned_logical_bytes: u64,
    pub(super) pinned_unique_bytes: u64,
    pub(super) owner_pins: u64,
}

#[derive(Debug, Default)]
pub(super) struct StoreCounters {
    pub(super) cache_hits_total: u64,
    pub(super) cache_misses_total: u64,
    pub(super) cache_evictions_total: u64,
    pub(super) verified_disk_read_bytes_total: u64,
    pub(super) missing_reads_total: u64,
    pub(super) oversized_reads_total: u64,
    pub(super) journal_commits_total: u64,
    pub(super) journal_commit_micros_total: u64,
    pub(super) garbage_collected_objects_total: u64,
    pub(super) garbage_collected_bytes_total: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Encoding, ObjectKind, Owner, StoreIdentity};

    #[test]
    fn observes_independent_pins_eviction_gc_and_restart_without_a_disk_scan() {
        let directory = tempfile::tempdir().unwrap();
        let identity = StoreIdentity { genesis_root: [1; 32], config_digest: [2; 32] };
        let store = SharedDataStore::open(directory.path(), identity, 4).unwrap();
        let first = Owner::Consensus([3; 32]);
        let second = Owner::Consensus([4; 32]);
        let id = store.put(ObjectKind::BeaconBlock, Encoding::Raw, b"abcd", first).unwrap();
        store.pin(second, id).unwrap();
        let observed = store.try_observe().unwrap();
        assert_eq!(
            (
                observed.tracked_bytes,
                observed.pinned_unique_bytes,
                observed.pinned_logical_bytes,
                observed.owner_pins
            ),
            (4, 4, 8, 2)
        );
        store.get(id).unwrap();
        assert_eq!(store.try_observe().unwrap().cache_hits_total, 1);
        store.release(first, id).unwrap();
        assert_eq!(store.try_observe().unwrap().pinned_logical_bytes, 4);
        store.release(second, id).unwrap();
        assert_eq!(store.try_observe().unwrap().pinned_unique_bytes, 0);
        let locked = store.inner.lock();
        assert!(store.try_observe().is_none());
        drop(locked);
        store.collect_garbage().unwrap();
        let observed = store.try_observe().unwrap();
        assert_eq!(
            (
                observed.tracked_objects,
                observed.garbage_collected_objects_total,
                observed.garbage_collected_bytes_total
            ),
            (0, 1, 4)
        );
        drop(store);
        let reopened = SharedDataStore::open(directory.path(), identity, 4).unwrap();
        let observed = reopened.try_observe().unwrap();
        assert_eq!(
            (observed.tracked_objects, observed.cache_hits_total, observed.journal_commits_total),
            (0, 0, 0)
        );
    }
}
