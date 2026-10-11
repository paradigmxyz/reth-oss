//! Bounded beacon-only physical maintenance in the existing identity-bound shared writer.
//!
//! Filename and unlink limits do not bound the full reference-map journal serialization.
//! This blocking operation must follow successful canonical publication and completed cleanup.

use super::*;
use std::{
    fs::{Metadata, ReadDir},
    path::Component,
    time::Instant,
};

#[cfg(windows)]
use std::os::windows::fs::MetadataExt;

const MAX_EXAMINED: usize = 1024;
const MAX_REMOVED: usize = 256;

/// Physical progress for one bounded beacon maintenance turn.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BeaconGcReport {
    /// Directory entries inspected, including excluded and noncanonical names.
    pub examined: usize,
    /// Exact unowned beacon object files unlinked.
    pub removed: usize,
    /// Physical bytes unlinked in this turn.
    pub removed_bytes: u64,
    /// Full reference-map entries serialized; this work is not filename-budget bounded.
    pub journal_objects: usize,
    /// Time spent committing the complete reference map, excluding scanning and unlink.
    pub journal_commit_micros: u128,
}

/// Runtime-only progress; no persisted cursor or filename ever grants deletion authority.
#[derive(Debug, Default)]
pub(super) struct BeaconGcCursor {
    last_slot: Option<u64>,
    reader: Option<ReadDir>,
}

impl SharedDataStore {
    /// Examines at most 1024 names and unlinks at most 256 unowned beacon objects per slot.
    ///
    /// The caller supplies the actual native owner slot only after canonical publication,
    /// required strict Engine acknowledgement and all dependent cleanup have succeeded.
    /// Every independent owner is checked under the same writer lock as reference removal.
    /// The complete reference-map snapshot is serialized when tracked objects are retired;
    /// these filesystem budgets do not bound that metadata work.
    pub fn collect_beacon_garbage(&self, actual_slot: u64) -> Result<BeaconGcReport, StoreError> {
        let mut inner = self.inner.lock();
        inner.journal.ensure_healthy()?;
        if inner.beacon_gc.last_slot.is_some_and(|slot| actual_slot <= slot) {
            return Ok(BeaconGcReport::default())
        }
        // A failed turn still consumes its slot; reopen or a later actual slot is required.
        inner.beacon_gc.last_slot = Some(actual_slot);
        check_directory(&inner.namespace_dir)?;
        check_directory(&inner.object_dir)?;
        if inner.beacon_gc.reader.is_none() {
            inner.beacon_gc.reader = Some(fs::read_dir(&inner.object_dir)?);
        }
        let mut report = BeaconGcReport::default();
        let mut selected = Vec::new();
        while report.examined < MAX_EXAMINED && selected.len() < MAX_REMOVED {
            let entry = inner.beacon_gc.reader.as_mut().and_then(Iterator::next);
            let Some(entry) = entry else {
                inner.beacon_gc.reader = None;
                break
            };
            report.examined += 1;
            let entry = entry.map_err(|err| fs::FsPathError::read_dir(err, &inner.object_dir))?;
            if let Some(name) = entry.file_name().to_str() &&
                let Some(id) = ObjectId::from_filename(name) &&
                id.filename() == name &&
                beacon_kind(id.kind) &&
                inner.objects.get(&id).is_none_or(|object| object.owners.is_empty())
            {
                // Derive the path ourselves; neither entries nor stored descriptors supply paths.
                let path = inner.object_dir.join(id.filename());
                if let Some(metadata) = plain_file(&path)? {
                    selected.push((id, metadata.len()));
                }
            }
        }
        if selected.is_empty() {
            return Ok(report)
        }
        if selected.iter().any(|(id, _)| inner.objects.contains_key(id)) {
            let mut next = inner.objects.clone();
            for (id, _) in &selected {
                if next.get(id).is_some_and(|object| object.owners.is_empty()) {
                    next.remove(id);
                }
            }
            report.journal_objects = next.len();
            let started = Instant::now();
            // Durably remove references first. An uncertain commit aborts every unlink.
            inner.persist(next)?;
            report.journal_commit_micros = started.elapsed().as_micros();
        }
        for (id, _) in selected {
            inner.journal.ensure_healthy()?;
            // Recheck all domains under the original lock, including pins installed before turn.
            if inner.objects.get(&id).is_some_and(|object| !object.owners.is_empty()) {
                continue
            }
            check_directory(&inner.namespace_dir)?;
            check_directory(&inner.object_dir)?;
            let path = inner.object_dir.join(id.filename());
            if let Some(metadata) = plain_file(&path)? {
                unlink(&path)?;
                inner.record_collection(metadata.len());
                if let Some(cached) = inner.cache.remove(&id) {
                    inner.cache_bytes -= cached.bytes.len();
                }
                report.removed += 1;
                report.removed_bytes = report.removed_bytes.saturating_add(metadata.len());
            }
        }
        Ok(report)
    }
}

fn beacon_kind(kind: ObjectKind) -> bool {
    matches!(
        kind,
        ObjectKind::BeaconState |
            ObjectKind::BeaconBlock |
            ObjectKind::BeaconStateComponent |
            ObjectKind::BeaconStateBranch |
            ObjectKind::BeaconStateManifest |
            ObjectKind::BeaconPayloadEnvelope |
            ObjectKind::BeaconDataSidecar |
            ObjectKind::BeaconObservation
    )
}

fn no_links(metadata: &Metadata) -> bool {
    if metadata.file_type().is_symlink() {
        return false
    }
    #[cfg(windows)]
    if metadata.file_attributes() & 0x400 != 0 {
        return false
    }
    true
}

fn check_directory(path: &Path) -> Result<(), StoreError> {
    let absolute = std::path::absolute(path).map_err(|err| fs::FsPathError::metadata(err, path))?;
    let mut ancestor = PathBuf::new();
    // Inspect root-to-leaf, rejecting a link before resolving any child under its target.
    for component in absolute.components() {
        if component == Component::ParentDir {
            return Err(StoreError::InvalidPublication)
        }
        ancestor.push(component);
        if !ancestor.has_root() {
            continue
        }
        let metadata = std::fs::symlink_metadata(&ancestor)
            .map_err(|err| fs::FsPathError::metadata(err, &ancestor))?;
        if !metadata.is_dir() || !no_links(&metadata) {
            return Err(StoreError::InvalidPublication)
        }
    }
    Ok(())
}

fn plain_file(path: &Path) -> Result<Option<Metadata>, StoreError> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) => Ok((metadata.is_file() && no_links(&metadata)).then_some(metadata)),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(fs::FsPathError::metadata(err, path).into()),
    }
}

fn unlink(path: &Path) -> Result<(), StoreError> {
    #[cfg(test)]
    if UNLINK_FAILURE.with(|target| {
        let mut target = target.borrow_mut();
        if target.as_deref() == Some(path) {
            target.take();
            true
        } else {
            false
        }
    }) {
        return Err(fs::FsPathError::remove_file(
            std::io::Error::other("injected beacon unlink interruption"),
            path,
        )
        .into())
    }
    fs::remove_file(path)?;
    Ok(())
}

#[cfg(test)]
std::thread_local! {
    static UNLINK_FAILURE: std::cell::RefCell<Option<PathBuf>> = const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[cfg(unix)]
    use std::os::unix::fs::symlink;

    fn identity() -> StoreIdentity {
        StoreIdentity { genesis_root: [1; 32], config_digest: [2; 32] }
    }

    #[test]
    fn allowlist_preserves_all_independent_owner_domains_and_reader_bytes() {
        let dir = TempDir::new().unwrap();
        let store = SharedDataStore::open(dir.path(), identity(), 4096).unwrap();
        let temporary = Owner::Recovery([7; 32]);
        let owners = [
            Owner::Pool([8; 32]),
            Owner::Checkpoint([9; 32]),
            Owner::Consensus([10; 32]),
            Owner::Recovery([11; 32]),
            Owner::CompactState([12; 32]),
            Owner::CompactNode(ObjectId::new(
                ObjectKind::BeaconStateBranch,
                Encoding::Raw,
                b"node",
            )),
        ];
        let mut retained = Vec::new();
        for (index, owner) in owners.into_iter().enumerate() {
            let bytes = vec![index as u8; 64];
            let id =
                store.put(ObjectKind::BeaconStateComponent, Encoding::Raw, &bytes, owner).unwrap();
            store.pin(temporary, id).unwrap();
            retained.push((id, owner, bytes));
        }
        let kinds = [
            ObjectKind::Blob,
            ObjectKind::Cell,
            ObjectKind::Proof,
            ObjectKind::ExecutionPayload,
            ObjectKind::BeaconState,
            ObjectKind::BlobSidecar,
            ObjectKind::Commitment,
            ObjectKind::BeaconBlock,
            ObjectKind::BeaconStateComponent,
            ObjectKind::BeaconStateBranch,
            ObjectKind::BeaconStateManifest,
            ObjectKind::BeaconPayloadEnvelope,
            ObjectKind::BeaconDataSidecar,
            ObjectKind::BeaconObservation,
        ];
        let mut objects = Vec::new();
        for kind in kinds {
            let id = store.put(kind, Encoding::Ssz, b"original", temporary).unwrap();
            objects.push(id);
        }
        let held = store.get(objects[4]).unwrap().unwrap();
        store.release_owners(&[temporary]).unwrap();
        let before = store.stats().unwrap().physical_bytes;
        let report = store.collect_beacon_garbage(1).unwrap();
        assert_eq!(report.removed, 8);
        assert!(store.stats().unwrap().physical_bytes < before);
        assert_eq!(held.as_ref(), b"original");
        for (id, owner, bytes) in retained {
            assert_eq!(store.owner_objects(owner), vec![id]);
            assert_eq!(store.get(id).unwrap().unwrap().as_ref(), bytes);
        }
        for id in objects {
            assert_eq!(store.get(id).unwrap().is_none(), beacon_kind(id.kind));
        }
        drop(store);
        let store = SharedDataStore::open(dir.path(), identity(), 0).unwrap();
        assert_eq!(store.stats().unwrap().physical_objects, 12);
    }

    #[test]
    fn canonical_names_only_never_descend_into_nested_or_foreign_paths() {
        let dir = TempDir::new().unwrap();
        let store = SharedDataStore::open(dir.path(), identity(), 0).unwrap();
        let object_dir = store.namespace_root().join("objects");
        let id = ObjectId::new(ObjectKind::BeaconObservation, Encoding::Raw, b"orphan");
        let canonical = object_dir.join(id.filename());
        fs::write(&canonical, b"orphan").unwrap();
        // Use a distinct digest: on Windows, a case-only alias of the canonical object
        // addresses that same file instead of creating an adversarial directory entry.
        let noncanonical = ObjectId::new(
            ObjectKind::BeaconObservation,
            Encoding::Raw,
            b"distinct noncanonical identity",
        );
        let mut upper_name = noncanonical.filename();
        upper_name.get_mut(6..70).unwrap().make_ascii_uppercase();
        assert_ne!(upper_name, noncanonical.filename());
        assert_eq!(ObjectId::from_filename(&upper_name), Some(noncanonical));
        let upper = object_dir.join(upper_name);
        fs::write(&upper, b"noncanonical").unwrap();
        let nested = object_dir.join("nested");
        fs::create_dir_all(&nested).unwrap();
        fs::write(nested.join(id.filename()), b"nested").unwrap();
        let foreign = dir.path().join(id.filename());
        fs::write(&foreign, b"personal").unwrap();
        let descriptor = store.namespace_root().join("pruning.json");
        fs::write(&descriptor, b"descriptor").unwrap();
        assert_eq!(store.collect_beacon_garbage(1).unwrap().removed, 1);
        assert!(!canonical.exists());
        assert_eq!(fs::read(upper).unwrap(), b"noncanonical");
        assert_eq!(fs::read(nested.join(id.filename())).unwrap(), b"nested");
        assert_eq!(fs::read(foreign).unwrap(), b"personal");
        assert_eq!(fs::read(descriptor).unwrap(), b"descriptor");
    }

    #[test]
    fn uncertain_reference_removal_aborts_all_unlinks_until_reopen() {
        let dir = TempDir::new().unwrap();
        let owner = Owner::Recovery([3; 32]);
        let id;
        let path;
        {
            let store = SharedDataStore::open(dir.path(), identity(), 0).unwrap();
            id = store
                .put(ObjectKind::BeaconState, Encoding::Ssz, b"keep until durable", owner)
                .unwrap();
            path = store.namespace_root().join("objects").join(id.filename());
            store.release(owner, id).unwrap();
            journal::inject_post_rename_failure(
                store.namespace_root().join("refs/snapshot-00000000000000000004.commit"),
            );
            assert!(matches!(
                store.collect_beacon_garbage(1),
                Err(StoreError::PublicationUncertain(_))
            ));
            assert!(path.exists());
            assert!(matches!(store.collect_beacon_garbage(2), Err(StoreError::JournalPoisoned)));
            assert!(path.exists());
        }
        let store = SharedDataStore::open(dir.path(), identity(), 0).unwrap();
        assert_eq!(store.collect_beacon_garbage(1).unwrap().removed, 1);
        assert!(!path.exists());
        assert_eq!(store.get(id).unwrap(), None);
    }

    #[test]
    fn interrupted_unlink_leaves_recoverable_orphan_after_durable_reference_removal() {
        let dir = TempDir::new().unwrap();
        let owner = Owner::Recovery([3; 32]);
        let id;
        let path;
        {
            let store = SharedDataStore::open(dir.path(), identity(), 0).unwrap();
            id = store.put(ObjectKind::BeaconBlock, Encoding::Ssz, b"orphan", owner).unwrap();
            path = store.namespace_root().join("objects").join(id.filename());
            store.release(owner, id).unwrap();
            UNLINK_FAILURE.with(|target| *target.borrow_mut() = Some(path.clone()));
            assert!(matches!(store.collect_beacon_garbage(1), Err(StoreError::Fs(_))));
            store.ensure_healthy().unwrap();
            assert_eq!(store.get(id).unwrap(), None);
            assert!(path.exists());
        }
        let store = SharedDataStore::open(dir.path(), identity(), 0).unwrap();
        assert_eq!(store.collect_beacon_garbage(1).unwrap().removed, 1);
        assert!(!path.exists());
    }

    #[test]
    fn cursor_advances_across_pinned_prefix_and_respects_slot_and_unlink_budgets() {
        let dir = TempDir::new().unwrap();
        let store = SharedDataStore::open(dir.path(), identity(), 0).unwrap();
        let pinned = Owner::Pool([4; 32]);
        let released = Owner::Recovery([5; 32]);
        let bytes = (0u64..1900).map(|value| value.to_le_bytes()).collect::<Vec<_>>();
        let objects = bytes
            .iter()
            .enumerate()
            .map(|(index, bytes)| PinnedObject {
                kind: ObjectKind::BeaconStateComponent,
                encoding: Encoding::Raw,
                bytes,
                owners: if index < 1300 {
                    std::slice::from_ref(&pinned)
                } else {
                    std::slice::from_ref(&released)
                },
            })
            .collect::<Vec<_>>();
        let ids = store.put_pinned_batch(&objects).unwrap();
        store.release_owners(&[released]).unwrap();
        // A new owner installed before collection protects a formerly retired candidate.
        let newly_pinned = ids[1300];
        store.pin(Owner::Checkpoint([6; 32]), newly_pinned).unwrap();
        let mut total = 0;
        for slot in 1..=20 {
            let report = store.collect_beacon_garbage(slot).unwrap();
            assert!(report.examined <= MAX_EXAMINED);
            assert!(report.removed <= MAX_REMOVED);
            total += report.removed;
            assert_eq!(store.collect_beacon_garbage(slot).unwrap(), BeaconGcReport::default());
            if total == 599 {
                break
            }
        }
        assert_eq!(total, 599);
        assert_eq!(store.stats().unwrap().physical_objects, 1301);
        assert_eq!(store.get(newly_pinned).unwrap().unwrap().as_ref(), bytes[1300]);
        assert_eq!(store.owner_objects(pinned).len(), 1300);
    }

    #[cfg(unix)]
    #[test]
    fn symlink_object_and_namespace_are_rejected_without_following_targets() {
        let dir = TempDir::new().unwrap();
        let store = SharedDataStore::open(dir.path(), identity(), 0).unwrap();
        let object_dir = store.namespace_root().join("objects");
        let target = dir.path().join("personal");
        fs::write(&target, b"personal").unwrap();
        let id = ObjectId::new(ObjectKind::BeaconObservation, Encoding::Raw, b"linked");
        symlink(&target, object_dir.join(id.filename())).unwrap();
        assert_eq!(store.collect_beacon_garbage(1).unwrap().removed, 0);
        assert_eq!(fs::read(&target).unwrap(), b"personal");
        fs::remove_file(object_dir.join(id.filename())).unwrap();
        fs::remove_dir_all(&object_dir).unwrap();
        symlink(dir.path(), &object_dir).unwrap();
        assert!(matches!(store.collect_beacon_garbage(2), Err(StoreError::InvalidPublication)));
        assert_eq!(fs::read(target).unwrap(), b"personal");
    }
    #[cfg(unix)]
    #[test]
    fn symlink_datadir_ancestor_never_authorizes_collection_in_its_target() {
        let directory = TempDir::new().unwrap();
        let real = directory.path().join("node-data");
        let alias = directory.path().join("linked-node-data");
        fs::create_dir_all(&real).unwrap();
        symlink(&real, &alias).unwrap();
        let store = SharedDataStore::open(&alias, identity(), 0).unwrap();
        let owner = Owner::Recovery([19; 32]);
        let id = store
            .put(ObjectKind::BeaconObservation, Encoding::Raw, b"retained target bytes", owner)
            .unwrap();
        store.release(owner, id).unwrap();
        let target = real.join("shared-data-v1/objects").join(id.filename());
        assert!(matches!(store.collect_beacon_garbage(1), Err(StoreError::InvalidPublication)));
        assert_eq!(fs::read(target).unwrap(), b"retained target bytes");
        assert_eq!(store.get(id).unwrap().unwrap().as_ref(), b"retained target bytes");
    }
}
