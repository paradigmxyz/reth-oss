//! Append-only atomic snapshots shared by the object and beacon metadata stores.

use crate::{StoreError, StoreIdentity};
use reth_fs_util as fs;
use serde::{de::DeserializeOwned, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::{File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

const MAGIC: &[u8; 8] = b"EBJNL001";
const HEADER_LEN: usize = 124;
const MAX_SNAPSHOT_BYTES: u64 = 64 * 1024 * 1024;

/// An exclusive append-only journal. Only fully renamed snapshots are committed.
#[derive(Debug)]
pub struct Journal {
    root: PathBuf,
    identity: StoreIdentity,
    generation: u64,
    poisoned: bool,
    // The open handle holds the operating-system lock until the journal is dropped.
    _lock: File,
}

impl Journal {
    /// Opens the newest complete snapshot, rejecting corruption and identity mismatches.
    pub fn open<T: DeserializeOwned>(
        root: impl AsRef<Path>,
        identity: StoreIdentity,
    ) -> Result<(Self, Option<T>), StoreError> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root)?;
        let lock_path = root.join("writer.lock");
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .map_err(|err| fs::FsPathError::open(err, &lock_path))?;
        lock.try_lock().map_err(|_| StoreError::AlreadyOpen(root.clone()))?;
        let mut generations = Vec::new();
        for entry in fs::read_dir(&root)? {
            let entry = entry.map_err(|err| fs::FsPathError::read_dir(err, &root))?;
            if let Some(name) = entry.file_name().to_str() &&
                let Some(number) =
                    name.strip_prefix("snapshot-").and_then(|v| v.strip_suffix(".commit")) &&
                number.len() == 20 &&
                let Ok(generation) = number.parse::<u64>()
            {
                generations.push(generation);
            }
        }
        let mut journal = Self { root, identity, generation: 0, poisoned: false, _lock: lock };
        let Some(generation) = generations.into_iter().max() else {
            return Ok((journal, None));
        };
        let path = journal.snapshot_path(generation);
        if fs::metadata(&path)?.len() > MAX_SNAPSHOT_BYTES + HEADER_LEN as u64 {
            return Err(StoreError::SnapshotTooLarge);
        }
        let bytes = fs::read(&path)?;
        let payload = decode_snapshot(&bytes, generation, identity)?;
        let state = serde_json::from_slice(payload)?;
        // Reopening is the recovery fence for a previously uncertain rename.
        // Flush the verified generation before returning references or permitting collection.
        ensure_published_durability(&path)?;
        journal.generation = generation;
        Ok((journal, Some(state)))
    }

    /// Network and configuration identity checked when this journal was opened.
    pub const fn identity(&self) -> StoreIdentity {
        self.identity
    }
    /// Rejects operations after a publication with uncertain durability until reopen.
    pub const fn ensure_healthy(&self) -> Result<(), StoreError> {
        if self.poisoned {
            Err(StoreError::JournalPoisoned)
        } else {
            Ok(())
        }
    }

    /// Reject all subsequent access after any publication in this namespace became uncertain.
    /// The caller must drop every handle and reopen before reading or maintaining references.
    pub const fn mark_publication_uncertain(&mut self) {
        self.poisoned = true;
    }
    /// Persists a complete generation before publishing it with an atomic rename.
    /// A post-rename failure poisons this handle, preventing stale-reference maintenance.
    pub fn commit<T: Serialize>(&mut self, state: &T) -> Result<(), StoreError> {
        self.ensure_healthy()?;
        let payload = serde_json::to_vec(state)?;
        if payload.len() as u64 > MAX_SNAPSHOT_BYTES {
            return Err(StoreError::SnapshotTooLarge);
        }
        let generation = self.generation.checked_add(1).ok_or(StoreError::GenerationOverflow)?;
        let mut bytes = Vec::with_capacity(HEADER_LEN + payload.len());
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&1u32.to_le_bytes());
        bytes.extend_from_slice(&generation.to_le_bytes());
        bytes.extend_from_slice(&self.identity.genesis_root);
        bytes.extend_from_slice(&self.identity.config_digest);
        bytes.extend_from_slice(&(payload.len() as u64).to_le_bytes());
        bytes.extend_from_slice(&Sha256::digest(&payload));
        bytes.extend_from_slice(&payload);
        if let Err(error) = publish_file(&self.snapshot_path(generation), &bytes) {
            if matches!(error, StoreError::PublicationUncertain(_)) {
                self.poisoned = true;
            }
            return Err(error);
        }
        self.generation = generation;
        // Keep two generations. Garbage collection never precedes publishing its references.
        if generation > 2 {
            let _ = fs::remove_file_if_exists(self.snapshot_path(generation - 2));
        }
        Ok(())
    }

    fn snapshot_path(&self, generation: u64) -> PathBuf {
        self.root.join(format!("snapshot-{generation:020}.commit"))
    }
}

/// Publishes immutable bytes, syncing the file before the name becomes visible.
pub fn publish_file(path: &Path, bytes: &[u8]) -> Result<(), StoreError> {
    let pending = path.with_extension("pending");
    fs::remove_file_if_exists(&pending)?;
    let mut file = fs::create_file(&pending)?;
    file.write_all(bytes).map_err(|err| fs::FsPathError::write(err, &pending))?;
    file.sync_all().map_err(|err| fs::FsPathError::fsync(err, &pending))?;
    drop(file);
    fs::rename(&pending, path)?;
    sync_published(path).map_err(StoreError::PublicationUncertain)?;
    Ok(())
}

/// Reestablish file and directory durability before pinning an existing verified orphan.
/// Callers must check the exact digest and byte length before invoking this operation.
pub fn ensure_published_durability(path: &Path) -> Result<(), StoreError> {
    sync_published(path).map_err(StoreError::PublicationUncertain)
}
fn sync_published(path: &Path) -> Result<(), fs::FsPathError> {
    #[cfg(any(test, feature = "test-utils"))]
    if POST_RENAME_FAILURE.with(|target| {
        let mut target = target.borrow_mut();
        if target.as_deref() == Some(path) {
            target.take();
            true
        } else {
            false
        }
    }) {
        return Err(fs::FsPathError::fsync(
            std::io::Error::other("injected post-rename sync failure"),
            path,
        ));
    }
    // Sync exact file bytes on every platform before making owner references durable.
    // Windows cannot open directory handles through std::fs; the file flush applies there.
    OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .map_err(|err| fs::FsPathError::open(err, path))?
        .sync_all()
        .map_err(|err| fs::FsPathError::fsync(err, path))?;
    #[cfg(unix)]
    if let Some(parent) = path.parent() {
        fs::open(parent)?.sync_all().map_err(|err| fs::FsPathError::fsync(err, parent))?;
    }
    Ok(())
}

#[cfg(any(test, feature = "test-utils"))]
std::thread_local! {
    static POST_RENAME_FAILURE: std::cell::RefCell<Option<PathBuf>> = const { std::cell::RefCell::new(None) };
}

/// Inject one post-rename sync failure on this thread for the exact target path.
/// The hook is unavailable in production builds unless explicitly enabled.
#[doc(hidden)]
#[cfg(any(test, feature = "test-utils"))]
pub fn inject_post_rename_failure(path: impl Into<PathBuf>) {
    POST_RENAME_FAILURE.with(|target| *target.borrow_mut() = Some(path.into()));
}

fn decode_snapshot(
    bytes: &[u8],
    generation: u64,
    identity: StoreIdentity,
) -> Result<&[u8], StoreError> {
    if bytes.len() < HEADER_LEN || &bytes[..8] != MAGIC {
        return Err(StoreError::CorruptJournal);
    }
    if bytes[8..12] != 1u32.to_le_bytes() {
        return Err(StoreError::UnsupportedSchema);
    }
    if bytes[12..20] != generation.to_le_bytes() {
        return Err(StoreError::CorruptJournal);
    }
    if bytes[20..52] != identity.genesis_root || bytes[52..84] != identity.config_digest {
        return Err(StoreError::IdentityMismatch);
    }
    let payload_len =
        u64::from_le_bytes(bytes[84..92].try_into().map_err(|_| StoreError::CorruptJournal)?);
    let payload = &bytes[HEADER_LEN..];
    if payload_len != payload.len() as u64 || bytes[92..124] != Sha256::digest(payload)[..] {
        return Err(StoreError::CorruptJournal);
    }
    Ok(payload)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn reopen_flushes_verified_generation_before_exposing_recovered_references() {
        let directory = TempDir::new().unwrap();
        let identity = StoreIdentity { genesis_root: [1; 32], config_digest: [2; 32] };
        let (mut journal, _) = Journal::open::<u64>(directory.path(), identity).unwrap();
        journal.commit(&10u64).unwrap();
        let path = journal.snapshot_path(1);
        drop(journal);
        inject_post_rename_failure(&path);
        assert!(matches!(
            Journal::open::<u64>(directory.path(), identity),
            Err(StoreError::PublicationUncertain(_))
        ));
        let (journal, state) = Journal::open::<u64>(directory.path(), identity).unwrap();
        assert_eq!(state, Some(10));
        journal.ensure_healthy().unwrap();
    }

    #[test]
    fn post_rename_failure_poison_prevents_generation_reuse_and_reopen_resolves_it() {
        let directory = TempDir::new().unwrap();
        let identity = StoreIdentity { genesis_root: [1; 32], config_digest: [2; 32] };
        let (mut journal, state) = Journal::open::<u64>(directory.path(), identity).unwrap();
        assert_eq!(state, None);
        journal.commit(&10u64).unwrap();
        let published = journal.snapshot_path(2);
        inject_post_rename_failure(&published);
        publish_file(&directory.path().join("unrelated.bin"), b"unrelated").unwrap();
        assert!(matches!(journal.commit(&20u64), Err(StoreError::PublicationUncertain(_))));
        assert!(published.exists());
        assert!(matches!(journal.ensure_healthy(), Err(StoreError::JournalPoisoned)));
        assert!(matches!(journal.commit(&30u64), Err(StoreError::JournalPoisoned)));
        assert!(!journal.snapshot_path(3).exists());
        drop(journal);
        let (mut reopened, state) = Journal::open::<u64>(directory.path(), identity).unwrap();
        assert_eq!(state, Some(20));
        reopened.ensure_healthy().unwrap();
        reopened.commit(&30u64).unwrap();
    }
}
