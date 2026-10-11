//! Atomic beacon metadata and immutable snapshots in a private namespace.
//!
//! All operations are blocking. This store does not alter execution database schemas.

use parking_lot::Mutex;
use reth_embedded_beacon_shared_data::{
    journal::{publish_file, Journal},
    StoreError,
};
use reth_fs_util as fs;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fmt::Write as _,
    path::{Path, PathBuf},
    sync::Arc,
};

pub use reth_embedded_beacon_shared_data::StoreIdentity;

mod compact_state;
pub use compact_state::{
    CompactStateError, CompactStateLimits, CompactStateStats, CompactStateStore, StateLease,
    WeakStateLease,
};

/// An isolated metadata store supporting atomic import and checkpoint records.
#[derive(Debug, Clone)]
pub struct MetadataStore {
    inner: Arc<Mutex<Inner>>,
}

impl MetadataStore {
    /// Opens a store, rejecting unsupported schemas, corruption and network changes.
    pub fn open(root: impl AsRef<Path>, identity: StoreIdentity) -> Result<Self, MetadataError> {
        let root = root.as_ref().join("beacon-metadata-v1");
        let (mut journal, restored) = Journal::open::<State>(root.join("journal"), identity)?;
        let state = restored.unwrap_or_default();
        let snapshot_dir = root.join("snapshots");
        fs::create_dir_all(&snapshot_dir)?;
        for snapshot in state.snapshots.values() {
            verify_snapshot(&snapshot_dir, snapshot)?;
        }
        if state.values.is_empty() && state.snapshots.is_empty() {
            journal.commit(&state)?;
        }
        Ok(Self { inner: Arc::new(Mutex::new(Inner { journal, snapshot_dir, state })) })
    }

    /// Network and configuration identity checked at store initialization.
    pub fn identity(&self) -> StoreIdentity {
        self.inner.lock().journal.identity()
    }
    /// Rejects access after an uncertain journal commit until all handles are dropped and reopened.
    pub fn ensure_healthy(&self) -> Result<(), MetadataError> {
        self.inner.lock().journal.ensure_healthy()?;
        Ok(())
    }

    /// Applies all entries as one generation.
    /// A failure after publication requires reopening before reading or maintenance.
    pub fn commit(&self, batch: MetadataBatch) -> Result<(), MetadataError> {
        let mut inner = self.inner.lock();
        inner.journal.ensure_healthy()?;
        let mut next = inner.state.clone();
        for (key, value) in batch.entries {
            validate_key(&key)?;
            if let Some(value) = value {
                if value.len() > MAX_VALUE_BYTES {
                    return Err(MetadataError::ValueTooLarge);
                }
                next.values.insert(key, value);
            } else {
                next.values.remove(&key);
            }
        }
        inner.journal.commit(&next)?;
        inner.state = next;
        Ok(())
    }

    /// Looks up one committed metadata entry.
    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>, MetadataError> {
        let inner = self.inner.lock();
        inner.journal.ensure_healthy()?;
        Ok(inner.state.values.get(key).cloned())
    }

    /// Persists immutable state bytes before atomically recording their root and slot.
    ///
    /// The caller must validate the SSZ state root. This store verifies exact byte integrity.
    pub fn put_snapshot(
        &self,
        root: [u8; 32],
        slot: u64,
        bytes: &[u8],
    ) -> Result<(), MetadataError> {
        let mut inner = self.inner.lock();
        inner.journal.ensure_healthy()?;
        let record = SnapshotRecord {
            root,
            slot,
            digest: Sha256::digest(bytes).into(),
            size: bytes.len() as u64,
        };
        if let Some(existing) = inner.state.snapshots.get(&root) {
            return if existing == &record {
                Ok(())
            } else {
                Err(MetadataError::ConflictingSnapshot)
            };
        }
        let path = inner.snapshot_dir.join(snapshot_filename(record.digest));
        let publication = if path.exists() {
            verify_snapshot(&inner.snapshot_dir, &record)?;
            reth_embedded_beacon_shared_data::journal::ensure_published_durability(&path)
        } else {
            publish_file(&path, bytes)
        };
        if matches!(publication, Err(StoreError::PublicationUncertain(_))) {
            inner.journal.mark_publication_uncertain();
        }
        publication?;
        let mut next = inner.state.clone();
        next.snapshots.insert(root, record);
        inner.journal.commit(&next)?;
        inner.state = next;
        Ok(())
    }

    /// Retrieves a committed immutable snapshot, verifying its digest on every disk read.
    pub fn get_snapshot(&self, root: [u8; 32]) -> Result<Option<(u64, Vec<u8>)>, MetadataError> {
        let inner = self.inner.lock();
        inner.journal.ensure_healthy()?;
        let Some(record) = inner.state.snapshots.get(&root) else { return Ok(None) };
        let bytes = verify_snapshot(&inner.snapshot_dir, record)?;
        Ok(Some((record.slot, bytes)))
    }

    /// Removes a snapshot reference atomically. Orphan bytes can then be collected.
    pub fn remove_snapshot(&self, root: [u8; 32]) -> Result<(), MetadataError> {
        let mut inner = self.inner.lock();
        inner.journal.ensure_healthy()?;
        let mut next = inner.state.clone();
        next.snapshots.remove(&root);
        inner.journal.commit(&next)?;
        inner.state = next;
        Ok(())
    }

    /// Collects only files that match known snapshot names and have no committed references.
    pub fn collect_garbage(&self) -> Result<usize, MetadataError> {
        let inner = self.inner.lock();
        inner.journal.ensure_healthy()?;
        let retained = inner
            .state
            .snapshots
            .values()
            .map(|entry| snapshot_filename(entry.digest))
            .collect::<std::collections::BTreeSet<_>>();
        let mut removed = 0;
        for entry in fs::read_dir(&inner.snapshot_dir)? {
            let entry = entry.map_err(|err| fs::FsPathError::read_dir(err, &inner.snapshot_dir))?;
            if let Some(name) = entry.file_name().to_str() &&
                name.len() == 68 &&
                name.ends_with(".ssz") &&
                name.as_bytes()[..64].iter().all(u8::is_ascii_hexdigit) &&
                !retained.contains(name)
            {
                fs::remove_file(entry.path())?;
                removed += 1;
            }
        }
        Ok(removed)
    }
}

/// Maximum individual metadata value; large SSZ state data belongs in snapshots.
pub const MAX_VALUE_BYTES: usize = 1024 * 1024;

/// A batch of atomic key mutations.
#[derive(Debug, Default)]
pub struct MetadataBatch {
    entries: Vec<(String, Option<Vec<u8>>)>,
}

impl MetadataBatch {
    /// Creates an empty batch.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds or replaces a namespaced value.
    pub fn put(mut self, key: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
        self.entries.push((key.into(), Some(value.into())));
        self
    }

    /// Removes a namespaced value.
    pub fn delete(mut self, key: impl Into<String>) -> Self {
        self.entries.push((key.into(), None));
        self
    }
}

/// Metadata validation failures and contextual storage errors.
#[derive(Debug, thiserror::Error)]
pub enum MetadataError {
    /// Shared integrity or persistence error.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// Filesystem error with path context.
    #[error(transparent)]
    Fs(#[from] fs::FsPathError),
    /// Snapshot bytes failed their saved digest or length.
    #[error("corrupt beacon snapshot")]
    CorruptSnapshot,
    /// The same state root was assigned different bytes or slot metadata.
    #[error("conflicting snapshot for existing state root")]
    ConflictingSnapshot,
    /// Keys require a short ASCII namespace to avoid accidental schema ambiguity.
    #[error("metadata key must be 1..=128 ASCII bytes with a namespace separator")]
    InvalidKey,
    /// A metadata value should be moved into the immutable snapshot store.
    #[error("metadata value exceeds 1 MiB")]
    ValueTooLarge,
}

#[derive(Debug)]
struct Inner {
    journal: Journal,
    snapshot_dir: PathBuf,
    state: State,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct State {
    values: BTreeMap<String, Vec<u8>>,
    #[serde(with = "snapshot_map")]
    snapshots: BTreeMap<[u8; 32], SnapshotRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct SnapshotRecord {
    root: [u8; 32],
    slot: u64,
    digest: [u8; 32],
    size: u64,
}

fn validate_key(key: &str) -> Result<(), MetadataError> {
    if key.is_empty() || key.len() > 128 || !key.is_ascii() || !key.contains('/') {
        return Err(MetadataError::InvalidKey);
    }
    Ok(())
}

fn snapshot_filename(digest: [u8; 32]) -> String {
    let mut name = String::with_capacity(68);
    for byte in digest {
        let _ = write!(name, "{byte:02x}");
    }
    name.push_str(".ssz");
    name
}

fn verify_snapshot(directory: &Path, record: &SnapshotRecord) -> Result<Vec<u8>, MetadataError> {
    let path = directory.join(snapshot_filename(record.digest));
    if fs::metadata(&path)?.len() != record.size {
        return Err(MetadataError::CorruptSnapshot);
    }
    let bytes = fs::read(path)?;
    if <[u8; 32]>::from(Sha256::digest(&bytes)) != record.digest {
        return Err(MetadataError::CorruptSnapshot);
    }
    Ok(bytes)
}

mod snapshot_map {
    use super::*;

    pub(super) fn serialize<S: serde::Serializer>(
        map: &BTreeMap<[u8; 32], SnapshotRecord>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        map.values().collect::<Vec<_>>().serialize(serializer)
    }

    pub(super) fn deserialize<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<BTreeMap<[u8; 32], SnapshotRecord>, D::Error> {
        let records = Vec::<SnapshotRecord>::deserialize(deserializer)?;
        let mut map = BTreeMap::new();
        for record in records {
            if map.insert(record.root, record).is_some() {
                return Err(serde::de::Error::custom("duplicate snapshot root"));
            }
        }
        Ok(map)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn identity() -> StoreIdentity {
        StoreIdentity { genesis_root: [1; 32], config_digest: [2; 32] }
    }

    #[test]
    fn batch_and_snapshot_survive_restart() {
        let dir = TempDir::new().unwrap();
        {
            let store = MetadataStore::open(dir.path(), identity()).unwrap();
            store
                .commit(
                    MetadataBatch::new()
                        .put("head/root", vec![3; 32])
                        .put("finalized/epoch", 17u64.to_le_bytes().to_vec()),
                )
                .unwrap();
            store.put_snapshot([4; 32], 544, b"state bytes").unwrap();
        }
        let store = MetadataStore::open(dir.path(), identity()).unwrap();
        assert_eq!(store.get("head/root").unwrap(), Some(vec![3; 32]));
        assert_eq!(store.get_snapshot([4; 32]).unwrap(), Some((544, b"state bytes".to_vec())));
    }

    #[test]
    fn invalid_batch_never_partially_commits() {
        let dir = TempDir::new().unwrap();
        let store = MetadataStore::open(dir.path(), identity()).unwrap();
        assert!(matches!(
            store.commit(MetadataBatch::new().put("head/root", vec![3]).put("bad", vec![4])),
            Err(MetadataError::InvalidKey)
        ));
        assert_eq!(store.get("head/root").unwrap(), None);
    }

    #[test]
    fn removal_is_recoverable_and_snapshot_conflicts_fail() {
        let dir = TempDir::new().unwrap();
        let store = MetadataStore::open(dir.path(), identity()).unwrap();
        store.put_snapshot([4; 32], 2, b"state").unwrap();
        assert!(matches!(
            store.put_snapshot([4; 32], 3, b"different"),
            Err(MetadataError::ConflictingSnapshot)
        ));
        store.remove_snapshot([4; 32]).unwrap();
        drop(store);
        let store = MetadataStore::open(dir.path(), identity()).unwrap();
        assert_eq!(store.collect_garbage().unwrap(), 1);
        assert_eq!(store.get_snapshot([4; 32]).unwrap(), None);
    }

    #[test]
    fn identity_and_corrupt_snapshot_are_rejected() {
        let dir = TempDir::new().unwrap();
        let store = MetadataStore::open(dir.path(), identity()).unwrap();
        store.put_snapshot([4; 32], 2, b"state").unwrap();
        drop(store);
        assert!(matches!(
            MetadataStore::open(dir.path(), StoreIdentity { config_digest: [9; 32], ..identity() }),
            Err(MetadataError::Store(StoreError::IdentityMismatch))
        ));
        let digest = Sha256::digest(b"state").into();
        fs::write(
            dir.path().join("beacon-metadata-v1/snapshots").join(snapshot_filename(digest)),
            b"wrong",
        )
        .unwrap();
        assert!(matches!(
            MetadataStore::open(dir.path(), identity()),
            Err(MetadataError::CorruptSnapshot)
        ));
    }
    #[test]
    fn uncertain_snapshot_file_requires_reopen_and_verified_orphan_resync() {
        let directory = TempDir::new().unwrap();
        let root = [4; 32];
        let bytes = b"snapshot before its root metadata";
        let digest = Sha256::digest(bytes).into();
        let path =
            directory.path().join("beacon-metadata-v1/snapshots").join(snapshot_filename(digest));
        for attempt in 0..2 {
            let store = MetadataStore::open(directory.path(), identity()).unwrap();
            assert_eq!(store.get_snapshot(root).unwrap(), None);
            reth_embedded_beacon_shared_data::journal::inject_post_rename_failure(&path);
            assert!(matches!(
                store.put_snapshot(root, 1, bytes),
                Err(MetadataError::Store(StoreError::PublicationUncertain(_)))
            ));
            assert!(matches!(
                store.get_snapshot(root),
                Err(MetadataError::Store(StoreError::JournalPoisoned))
            ));
            assert!(matches!(
                store.collect_garbage(),
                Err(MetadataError::Store(StoreError::JournalPoisoned))
            ));
            assert!(path.exists(), "attempt {attempt}");
        }
        let store = MetadataStore::open(directory.path(), identity()).unwrap();
        store.put_snapshot(root, 1, bytes).unwrap();
        assert_eq!(store.get_snapshot(root).unwrap(), Some((1, bytes.to_vec())));
    }

    #[test]
    fn uncertain_publication_blocks_stale_reads_snapshot_fast_paths_and_gc_until_reopen() {
        let directory = TempDir::new().unwrap();
        let first = [4; 32];
        let second = [5; 32];
        {
            let store = MetadataStore::open(directory.path(), identity()).unwrap();
            store.put_snapshot(first, 1, b"first snapshot").unwrap();
            reth_embedded_beacon_shared_data::journal::inject_post_rename_failure(
                directory
                    .path()
                    .join("beacon-metadata-v1/journal/snapshot-00000000000000000003.commit"),
            );
            assert!(matches!(
                store.put_snapshot(second, 2, b"second snapshot"),
                Err(MetadataError::Store(StoreError::PublicationUncertain(_)))
            ));
            assert!(matches!(
                store.ensure_healthy(),
                Err(MetadataError::Store(StoreError::JournalPoisoned))
            ));
            assert!(matches!(
                store.get("absent/value"),
                Err(MetadataError::Store(StoreError::JournalPoisoned))
            ));
            assert!(matches!(
                store.get_snapshot(first),
                Err(MetadataError::Store(StoreError::JournalPoisoned))
            ));
            assert!(matches!(
                store.get_snapshot([9; 32]),
                Err(MetadataError::Store(StoreError::JournalPoisoned))
            ));
            assert!(matches!(
                store.put_snapshot(first, 1, b"first snapshot"),
                Err(MetadataError::Store(StoreError::JournalPoisoned))
            ));
            assert!(matches!(
                store.remove_snapshot([9; 32]),
                Err(MetadataError::Store(StoreError::JournalPoisoned))
            ));
            assert!(matches!(
                store.commit(MetadataBatch::new()),
                Err(MetadataError::Store(StoreError::JournalPoisoned))
            ));
            assert!(matches!(
                store.collect_garbage(),
                Err(MetadataError::Store(StoreError::JournalPoisoned))
            ));
        }
        let reopened = MetadataStore::open(directory.path(), identity()).unwrap();
        reopened.ensure_healthy().unwrap();
        assert_eq!(reopened.get_snapshot(second).unwrap(), Some((2, b"second snapshot".to_vec())));
        assert_eq!(reopened.collect_garbage().unwrap(), 0);
    }
}
