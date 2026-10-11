//! Beacon-only physical maintenance after durable canonical publication.
//!
//! This module does not release any owner or execute recovery-deletion descriptors.

use super::*;
use reth_embedded_beacon_shared_data::beacon_gc::BeaconGcReport;
use reth_fs_util as fs;
use std::path::Path;

#[cfg(windows)]
use std::os::windows::fs::MetadataExt;

impl AdmittedStateRepository {
    /// The sole blocking owner calls this only after strict Engine/canonical/retention success.
    pub(crate) fn collect_published_beacon_objects(
        &self,
        actual: StoreSnapshot,
        actual_slot: u64,
    ) -> Result<Option<BeaconGcReport>, PersistenceError> {
        let guard = self.lock()?;
        let Some(current) = &guard.current else { return Ok(None) };
        if guard.prepared.is_some() ||
            guard.cleanup.is_some() ||
            guard.event_cleanup.is_some() ||
            guard.current_closure.is_none() ||
            current.block_root != actual.head.root ||
            current.slot != actual.head_slot ||
            current.payload != actual.head.payload_status.map(SavedPayloadStatus::from_status) ||
            current.justified != actual.justified_checkpoint ||
            current.finalized != actual.finalized_checkpoint ||
            actual_slot < actual.head_slot
        {
            return Ok(None)
        }
        // Check the independent compact metadata journal too; stats does not sweep objects.
        self.compact.stats()?;
        for domain in
            [retention_gc::RetentionDomain::Sidecars, retention_gc::RetentionDomain::Contexts]
        {
            let directory = domain.directory(&self.shared);
            if !clean_directory(&directory)? {
                return Ok(None)
            }
            for name in ["pruning.json", "pruning.pending"] {
                if path_exists(&directory.join(name))? {
                    return Ok(None)
                }
            }
        }
        // Hold the admission lock through shared reference removal and physical unlink.
        // Current/previous/prepared closure pins and every independent owner remain untouched.
        match self.shared.collect_beacon_garbage(actual_slot) {
            Ok(report) => Ok(Some(report)),
            Err(SharedError::Fs(error @ fs::FsPathError::RemoveFile { .. })) => {
                // Failed unlink leaves harmless orphan bytes. A later bounded scan retries;
                // uncertain reference publication still fails this health fence and halts.
                self.shared.ensure_healthy()?;
                tracing::warn!(target: "beacon::storage", slot = actual_slot, %error,
                    "[beacon] Deferred beacon byte collection after filesystem removal failure");
                Ok(None)
            }
            Err(error) => Err(error.into()),
        }
    }
}

fn path_exists(path: &Path) -> Result<bool, PersistenceError> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(SharedError::from(fs::FsPathError::metadata(error, path)).into()),
    }
}

fn clean_directory(path: &Path) -> Result<bool, PersistenceError> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) => {
            #[cfg(windows)]
            if metadata.file_attributes() & 0x400 != 0 {
                return Ok(false)
            }
            Ok(metadata.is_dir() && !metadata.file_type().is_symlink())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(true),
        Err(error) => Err(SharedError::from(fs::FsPathError::metadata(error, path)).into()),
    }
}
