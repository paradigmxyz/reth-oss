//! Bounded range scheduling and integrity-checked local historical archives.
//!
//! Archive integrity is independent of consensus validation. Every returned block must still
//! traverse the full fork-specific state transition before import.

use reth_embedded_beacon_primitives::{sha256, Root, Slot};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, VecDeque},
    io::Read,
    path::{Path, PathBuf},
};
use thiserror::Error;

/// Bounds download work and reassigns expired leases without advancing the imported head.
#[derive(Debug)]
pub struct RangeScheduler {
    next_slot: Slot,
    end_slot: Slot,
    batch_slots: u64,
    max_inflight: usize,
    retry_limit: u32,
    next_id: u64,
    retry: VecDeque<(Slot, u64, u32)>,
    inflight: BTreeMap<u64, RangeLease>,
}

impl RangeScheduler {
    /// Schedules the half-open slot range; empty slots need no artificial blocks.
    pub const fn new(
        start: Slot,
        end: Slot,
        batch_slots: u64,
        max_inflight: usize,
        retry_limit: u32,
    ) -> Result<Self, SyncError> {
        if start > end ||
            batch_slots == 0 ||
            batch_slots > 1024 ||
            max_inflight == 0 ||
            max_inflight > 128
        {
            return Err(SyncError::InvalidBounds);
        }
        Ok(Self {
            next_slot: start,
            end_slot: end,
            batch_slots,
            max_inflight,
            retry_limit,
            next_id: 0,
            retry: VecDeque::new(),
            inflight: BTreeMap::new(),
        })
    }

    /// Reserves one bounded request for a peer, using monotonic milliseconds.
    pub fn lease(
        &mut self,
        peer: Root,
        now_ms: u64,
        timeout_ms: u64,
    ) -> Result<Option<RangeLease>, SyncError> {
        if timeout_ms == 0 {
            return Err(SyncError::InvalidBounds);
        }
        if self.inflight.len() >= self.max_inflight {
            return Ok(None);
        }
        let (start, count, attempt) = if let Some(range) = self.retry.front().copied() {
            range
        } else if self.next_slot < self.end_slot {
            (self.next_slot, (self.end_slot - self.next_slot).min(self.batch_slots), 0)
        } else {
            return Ok(None)
        };
        let id = self.next_id.checked_add(1).ok_or(SyncError::Overflow)?;
        let deadline_ms = now_ms.checked_add(timeout_ms).ok_or(SyncError::Overflow)?;
        if self.retry.front().is_some() {
            self.retry.pop_front();
        } else {
            self.next_slot += count;
        }
        self.next_id = id;
        let lease = RangeLease { id, peer, start, count, attempt, deadline_ms };
        self.inflight.insert(id, lease);
        tracing::debug!(target: "beacon::sync", request_id = id, start_slot = start, count, attempt, deadline_ms, inflight = self.inflight.len(), "[beacon] Leased bounded block range");
        Ok(Some(lease))
    }

    /// Finishes a fully verified response; sparse responses are valid for empty slots.
    pub fn complete(
        &mut self,
        id: u64,
        peer: Root,
        verified_slots: &[Slot],
    ) -> Result<(), SyncError> {
        let lease = self.inflight.get(&id).ok_or(SyncError::UnknownLease)?;
        if lease.peer != peer {
            return Err(SyncError::WrongPeer);
        }
        let end = lease.start.checked_add(lease.count).ok_or(SyncError::Overflow)?;
        if verified_slots.iter().any(|slot| *slot < lease.start || *slot >= end) ||
            verified_slots.windows(2).any(|slots| slots[0] >= slots[1])
        {
            return Err(SyncError::InvalidResponse);
        }
        self.inflight.remove(&id);
        tracing::debug!(target: "beacon::sync", request_id = id, verified_blocks = verified_slots.len(), inflight = self.inflight.len(), "[beacon] Completed verified block range");
        if self.is_complete() {
            tracing::info!(target: "beacon::sync", end_slot = self.end_slot, "[beacon] Completed scheduled block-range work");
        }
        Ok(())
    }

    /// Retries failed work and errors once the configured retry budget is exhausted.
    pub fn fail(&mut self, id: u64, peer: Root) -> Result<(), SyncError> {
        let lease = *self.inflight.get(&id).ok_or(SyncError::UnknownLease)?;
        if lease.peer != peer {
            return Err(SyncError::WrongPeer);
        }
        if lease.attempt >= self.retry_limit {
            tracing::warn!(target: "beacon::sync", request_id = id, start_slot = lease.start, attempt = lease.attempt, "[beacon] Block range retry budget exhausted");
            return Err(SyncError::RetriesExhausted { id });
        }
        self.inflight.remove(&id);
        self.retry.push_back((lease.start, lease.count, lease.attempt + 1));
        tracing::debug!(target: "beacon::sync", request_id = id, start_slot = lease.start, next_attempt = lease.attempt + 1, "[beacon] Requeued failed block range");
        Ok(())
    }

    /// Reclaims expired leases. An exhausted lease remains visible for operator recovery.
    pub fn expire(&mut self, now_ms: u64) -> Result<usize, SyncError> {
        let expired = self
            .inflight
            .values()
            .filter(|lease| lease.deadline_ms <= now_ms)
            .copied()
            .collect::<Vec<_>>();
        for lease in &expired {
            self.fail(lease.id, lease.peer)?;
        }
        Ok(expired.len())
    }

    /// True only when all scheduled ranges have finished consensus verification.
    pub fn is_complete(&self) -> bool {
        self.next_slot == self.end_slot && self.retry.is_empty() && self.inflight.is_empty()
    }
    /// Number of concurrent leased requests.
    pub fn inflight(&self) -> usize {
        self.inflight.len()
    }
}

/// Owned, bounded request work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RangeLease {
    /// Unique local lease identifier.
    pub id: u64,
    /// Peer identity digest.
    pub peer: Root,
    /// First requested slot.
    pub start: Slot,
    /// Number of requested slots.
    pub count: u64,
    /// Retry count.
    pub attempt: u32,
    /// Monotonic deadline in milliseconds.
    pub deadline_ms: u64,
}

/// Local archive with checksummed SSZ files and a network-bound manifest.
#[derive(Debug)]
pub struct HistoricalArchive {
    root: PathBuf,
    manifest: ArchiveManifest,
}

impl HistoricalArchive {
    /// Opens a manifest after bounding its size and validating ordering and identity.
    pub fn open(root: impl AsRef<Path>, genesis_root: Root) -> Result<Self, SyncError> {
        let root = root
            .as_ref()
            .canonicalize()
            .map_err(|err| reth_fs_util::FsPathError::read(err, root.as_ref()))?;
        let manifest_path = root.join("manifest.json");
        if reth_fs_util::metadata(&manifest_path)?.len() > 16 * 1024 * 1024 {
            return Err(SyncError::ArchiveTooLarge);
        }
        let manifest: ArchiveManifest =
            serde_json::from_slice(&read_bounded(&manifest_path, 16 * 1024 * 1024)?)?;
        if manifest.version != 1 {
            return Err(SyncError::ArchiveVersion(manifest.version));
        }
        if manifest.genesis_validators_root != genesis_root {
            return Err(SyncError::WrongNetwork);
        }
        if manifest.entries.windows(2).any(|pair| pair[0].slot >= pair[1].slot) ||
            manifest
                .entries
                .iter()
                .any(|entry| entry.bytes == 0 || entry.bytes > 16 * 1024 * 1024)
        {
            return Err(SyncError::InvalidArchive);
        }
        tracing::info!(target: "beacon::archive", blocks = manifest.entries.len(), "[beacon] Opened integrity-checked historical archive");
        Ok(Self { root, manifest })
    }

    /// Returns ascending block descriptors, including historical skipped slots.
    pub fn entries(&self) -> &[ArchiveEntry] {
        &self.manifest.entries
    }

    /// Reads raw SSZ after checksum verification. This does not prove block validity.
    pub fn read_block(&self, entry: &ArchiveEntry) -> Result<Vec<u8>, SyncError> {
        if self
            .manifest
            .entries
            .binary_search_by_key(&entry.slot, |known| known.slot)
            .ok()
            .and_then(|index| self.manifest.entries.get(index)) !=
            Some(entry)
        {
            return Err(SyncError::InvalidArchive);
        }
        let name = entry.root.iter().map(|byte| format!("{byte:02x}")).collect::<String>();
        let path = self.root.join(format!("{name}.ssz"));
        let canonical =
            path.canonicalize().map_err(|err| reth_fs_util::FsPathError::read(err, &path))?;
        if !canonical.starts_with(&self.root) {
            return Err(SyncError::ArchiveEscape);
        }
        if reth_fs_util::metadata(&canonical)?.len() != entry.bytes {
            return Err(SyncError::ArchiveChecksum);
        }
        let bytes = read_bounded(&canonical, entry.bytes)?;
        if bytes.len() as u64 != entry.bytes || sha256(&bytes) != entry.ssz_sha256 {
            return Err(SyncError::ArchiveChecksum);
        }
        tracing::debug!(target: "beacon::archive", slot = entry.slot, bytes = bytes.len(), "[beacon] Read checksum-verified archive block; consensus import still required");
        Ok(bytes)
    }
}

/// Versioned local archive format. Checksums are not trusted consensus roots.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArchiveManifest {
    /// Archive format version.
    pub version: u32,
    /// Consensus network identity.
    pub genesis_validators_root: Root,
    /// Ordered signed-block SSZ descriptors.
    pub entries: Vec<ArchiveEntry>,
}

/// Integrity descriptor for one signed block.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArchiveEntry {
    /// Claimed consensus root, verified by the import pipeline after decoding.
    pub root: Root,
    /// Claimed slot.
    pub slot: Slot,
    /// SSZ file SHA-256 digest.
    pub ssz_sha256: Root,
    /// Exact bounded SSZ file size.
    pub bytes: u64,
}

/// Scheduling and archive failures.
#[derive(Debug, Error)]
pub enum SyncError {
    /// Invalid resource or range limits.
    #[error("invalid sync bounds")]
    InvalidBounds,
    /// Integer overflow.
    #[error("sync counter overflow")]
    Overflow,
    /// Lease already finished or never reserved.
    #[error("unknown range lease")]
    UnknownLease,
    /// Peer cannot finish another peer's request.
    #[error("range response from wrong peer")]
    WrongPeer,
    /// Duplicate, unordered or out-of-range response.
    #[error("invalid range response slots")]
    InvalidResponse,
    /// Retry budget exhausted.
    #[error("retry budget exhausted for lease {id}")]
    RetriesExhausted {
        /// Exhausted request identifier.
        id: u64,
    },
    /// Unsupported file format.
    #[error("unsupported archive version {0}")]
    ArchiveVersion(u32),
    /// Network mismatch.
    #[error("archive genesis root does not match network")]
    WrongNetwork,
    /// Invalid ordering or bounds.
    #[error("invalid archive manifest")]
    InvalidArchive,
    /// Path escapes namespace.
    #[error("archive file resolves outside archive directory")]
    ArchiveEscape,
    /// Checksum or exact length mismatch.
    #[error("archive SSZ checksum or size mismatch")]
    ArchiveChecksum,
    /// Manifest exceeds resource bound.
    #[error("archive manifest is too large")]
    ArchiveTooLarge,
    /// Filesystem error with path context.
    #[error(transparent)]
    Fs(#[from] reth_fs_util::FsPathError),
    /// Invalid manifest JSON.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

fn read_bounded(path: &Path, limit: u64) -> Result<Vec<u8>, SyncError> {
    let file = reth_fs_util::open(path)?;
    let mut bytes = Vec::new();
    file.take(limit.checked_add(1).ok_or(SyncError::Overflow)?)
        .read_to_end(&mut bytes)
        .map_err(|error| reth_fs_util::FsPathError::read(error, path))?;
    if bytes.len() as u64 > limit {
        return Err(SyncError::ArchiveTooLarge);
    }
    Ok(bytes)
}
#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn leases_bound_work_and_retry_without_skipping() {
        let mut sync = RangeScheduler::new(5, 12, 4, 1, 1).unwrap();
        let first = sync.lease([1; 32], 0, 10).unwrap().unwrap();
        assert_eq!((first.start, first.count), (5, 4));
        assert!(sync.lease([2; 32], 0, 10).unwrap().is_none());
        assert!(matches!(sync.complete(first.id, [2; 32], &[]), Err(SyncError::WrongPeer)));
        assert_eq!(sync.expire(10).unwrap(), 1);
        let retry = sync.lease([2; 32], 10, 10).unwrap().unwrap();
        assert_eq!((retry.start, retry.count, retry.attempt), (5, 4, 1));
        assert!(matches!(
            sync.complete(retry.id, [2; 32], &[6, 6]),
            Err(SyncError::InvalidResponse)
        ));
        sync.complete(retry.id, [2; 32], &[6, 8]).unwrap();
        let last = sync.lease([1; 32], 10, 10).unwrap().unwrap();
        assert_eq!((last.start, last.count), (9, 3));
        sync.complete(last.id, [1; 32], &[]).unwrap();
        assert!(sync.is_complete());
    }

    #[test]
    fn archive_checksums_do_not_establish_consensus_validity() {
        let dir = tempfile::tempdir().unwrap();
        let raw = b"intentionally not a valid beacon block";
        let entry = ArchiveEntry {
            root: [3; 32],
            slot: 1,
            ssz_sha256: sha256(raw),
            bytes: raw.len() as u64,
        };
        let manifest = ArchiveManifest {
            version: 1,
            genesis_validators_root: [1; 32],
            entries: vec![entry.clone()],
        };
        reth_fs_util::write(
            dir.path().join("manifest.json"),
            serde_json::to_vec(&manifest).unwrap(),
        )
        .unwrap();
        let filename = entry.root.iter().map(|b| format!("{b:02x}")).collect::<String>();
        reth_fs_util::write(dir.path().join(format!("{filename}.ssz")), raw).unwrap();
        let archive = HistoricalArchive::open(dir.path(), [1; 32]).unwrap();
        assert_eq!(archive.read_block(&entry).unwrap(), raw);
        assert!(matches!(
            HistoricalArchive::open(dir.path(), [2; 32]),
            Err(SyncError::WrongNetwork)
        ));
        reth_fs_util::write(dir.path().join(format!("{filename}.ssz")), b"bad").unwrap();
        assert!(matches!(archive.read_block(&entry), Err(SyncError::ArchiveChecksum)));
    }

    proptest! {
        #[test]
        fn every_slot_scheduled_once(start in 0u64..10000,length in 0u64..3000,batch in 1u64..1025){
            let mut sync=RangeScheduler::new(start,start+length,batch,2,1).unwrap();
            let mut covered=start;
            while let Some(lease)=sync.lease([1;32],0,10).unwrap(){
                prop_assert_eq!(lease.start,covered);
                prop_assert!(lease.count<=batch);
                covered+=lease.count;
                sync.complete(lease.id,[1;32],&[]).unwrap();
            }
            prop_assert_eq!(covered,start+length);
            prop_assert!(sync.is_complete());
        }
    }
}
