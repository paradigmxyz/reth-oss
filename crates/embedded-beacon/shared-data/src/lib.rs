//! Shared immutable bytes with independent durable ownership and bounded hot caching.
//!
//! These APIs perform blocking disk operations. Async callers must use a blocking task.

use parking_lot::Mutex;
use reth_fs_util as fs;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
    io::Read as _,
    path::{Path, PathBuf},
    sync::Arc,
};

pub mod beacon_gc;
pub mod journal;
mod observation;

pub use observation::StoreObservation;

#[cfg(feature = "reth")]
mod blobstore;

#[cfg(feature = "reth")]
pub use blobstore::SharedBlobStore;

/// Default shared byte-cache capacity, excluding data held by active callers.
pub const DEFAULT_CACHE_BYTES: usize = 256 * 1024 * 1024;

/// A cloneable single-writer content-addressed store.
#[derive(Debug, Clone)]
pub struct SharedDataStore {
    inner: Arc<Mutex<StoreInner>>,
}

impl SharedDataStore {
    /// Opens an isolated namespace without deleting retained objects.
    pub fn open(
        root: impl AsRef<Path>,
        identity: StoreIdentity,
        cache_bytes: usize,
    ) -> Result<Self, StoreError> {
        let root = root.as_ref().join("shared-data-v1");
        let (mut journal, restored) =
            journal::Journal::open::<ReferenceState>(root.join("refs"), identity)?;
        let state = restored.unwrap_or_default();
        let object_dir = root.join("objects");
        fs::create_dir_all(&object_dir)?;
        let mut objects = BTreeMap::new();
        for object in state.objects {
            verify_object_file(&object_dir.join(object.id.filename()), object.id, object.size)?;
            if objects.insert(object.id, object).is_some() {
                return Err(StoreError::CorruptJournal);
            }
        }
        if objects.is_empty() {
            // Persist identity even for a freshly initialized empty store.
            journal.commit(&ReferenceState::default())?;
        }
        let (owner_index, references) = build_owner_index(&objects);
        Ok(Self {
            inner: Arc::new(Mutex::new(StoreInner {
                namespace_dir: root,
                object_dir,
                journal,
                owner_index,
                references,
                observations: Default::default(),
                objects,
                cache: BTreeMap::new(),
                cache_bytes: 0,
                cache_limit: cache_bytes,
                clock: 0,
                beacon_gc: beacon_gc::BeaconGcCursor::default(),
            })),
        })
    }

    /// Network and configuration identity checked at store initialization.
    pub fn identity(&self) -> StoreIdentity {
        self.inner.lock().journal.identity()
    }

    /// Isolated backend namespace for additional metadata using this exact shared writer.
    /// Companion stores must derive their own constant child path here, not accept a foreign root.
    pub fn namespace_root(&self) -> PathBuf {
        self.inner.lock().namespace_dir.clone()
    }
    /// Rejects access after an uncertain journal commit until all handles are dropped and reopened.
    pub fn ensure_healthy(&self) -> Result<(), StoreError> {
        self.inner.lock().journal.ensure_healthy()
    }
    /// Writes immutable bytes before atomically adding a durable owner pin.
    pub fn put(
        &self,
        kind: ObjectKind,
        encoding: Encoding,
        bytes: &[u8],
        owner: Owner,
    ) -> Result<ObjectId, StoreError> {
        let mut inner = self.inner.lock();
        inner.journal.ensure_healthy()?;
        let id = ObjectId::new(kind, encoding, bytes);
        if let Some(existing) = inner.objects.get(&id) {
            if existing.owners.contains(&owner) {
                return Ok(id);
            }
        } else {
            let path = inner.object_dir.join(id.filename());
            inner.publish_object(&path, id, bytes)?;
        }
        let mut next = inner.objects.clone();
        next.entry(id)
            .or_insert_with(|| ObjectEntry {
                id,
                size: bytes.len() as u64,
                owners: BTreeSet::new(),
            })
            .owners
            .insert(owner);
        inner.persist(next)?;
        inner.cache_borrowed(id, bytes);
        Ok(id)
    }

    /// Adds an independent idempotent pin to already persisted bytes.
    pub fn pin(&self, owner: Owner, id: ObjectId) -> Result<(), StoreError> {
        let mut inner = self.inner.lock();
        inner.journal.ensure_healthy()?;
        let object = inner.objects.get(&id).ok_or(StoreError::MissingObject(id))?;
        if object.owners.contains(&owner) {
            return Ok(());
        }
        let mut next = inner.objects.clone();
        next.get_mut(&id).ok_or(StoreError::MissingObject(id))?.owners.insert(owner);
        inner.persist(next)
    }

    /// Releases one owner only; other owners continue retaining the same bytes.
    pub fn release(&self, owner: Owner, id: ObjectId) -> Result<(), StoreError> {
        let mut inner = self.inner.lock();
        inner.journal.ensure_healthy()?;
        if !inner.objects.get(&id).is_some_and(|object| object.owners.contains(&owner)) {
            return Ok(());
        }
        let mut next = inner.objects.clone();
        next.get_mut(&id).ok_or(StoreError::MissingObject(id))?.owners.remove(&owner);
        inner.persist(next)
    }

    /// Replaces all references held by an owner in a single atomic commit.
    pub fn replace_owner(&self, owner: Owner, ids: &[ObjectId]) -> Result<(), StoreError> {
        let mut inner = self.inner.lock();
        inner.journal.ensure_healthy()?;
        for id in ids {
            if !inner.objects.contains_key(id) {
                return Err(StoreError::MissingObject(*id));
            }
        }
        let mut next = inner.objects.clone();
        for entry in next.values_mut() {
            entry.owners.remove(&owner);
        }
        for id in ids {
            next.get_mut(id).ok_or(StoreError::MissingObject(*id))?.owners.insert(owner);
        }
        inner.persist(next)
    }

    /// Atomically replaces an owner's object set after persisting all immutable bytes.
    pub fn put_owned_batch(
        &self,
        owner: Owner,
        objects: &[(ObjectKind, Encoding, Vec<u8>)],
    ) -> Result<Vec<ObjectId>, StoreError> {
        let mut inner = self.inner.lock();
        inner.journal.ensure_healthy()?;
        let mut next = inner.objects.clone();
        for entry in next.values_mut() {
            entry.owners.remove(&owner);
        }
        let mut ids = Vec::with_capacity(objects.len());
        for (kind, encoding, bytes) in objects {
            let id = ObjectId::new(*kind, *encoding, bytes);
            let path = inner.object_dir.join(id.filename());
            inner.publish_object(&path, id, bytes)?;
            next.entry(id)
                .or_insert_with(|| ObjectEntry {
                    id,
                    size: bytes.len() as u64,
                    owners: BTreeSet::new(),
                })
                .owners
                .insert(owner);
            ids.push(id);
        }
        inner.persist(next)?;
        for ((_, _, bytes), id) in objects.iter().zip(&ids) {
            inner.cache_borrowed(*id, bytes.as_slice());
        }
        Ok(ids)
    }

    /// Publishes borrowed immutable objects and independent owner edges in one generation.
    /// Files are durable before the journal changes. A failed journal commit can leave orphan
    /// bytes, but cannot delete existing pins. This avoids a full reference-map commit per leaf.
    pub fn put_pinned_batch(
        &self,
        objects: &[PinnedObject<'_>],
    ) -> Result<Vec<ObjectId>, StoreError> {
        if objects.len() > 65_536 || objects.iter().any(|object| object.owners.is_empty()) {
            return Err(StoreError::InvalidPublication);
        }
        let pins = objects.iter().try_fold(0usize, |count, object| {
            count.checked_add(object.owners.len()).ok_or(StoreError::InvalidPublication)
        })?;
        if pins > 262_144 {
            return Err(StoreError::InvalidPublication);
        }
        let committed = objects
            .iter()
            .map(|object| {
                CommittedObject::new(object.kind, object.encoding, Cow::Borrowed(object.bytes))
            })
            .collect::<Vec<_>>();
        let pinned = committed
            .iter()
            .zip(objects)
            .map(|(object, input)| object.pinned(input.owners))
            .collect::<Vec<_>>();
        self.put_committed_pinned_batch(&pinned)
    }

    /// Publish exact immutable objects whose content commitments were computed during assembly.
    /// Only `CommittedObject::new` can create a commitment, and each pinned view borrows those
    /// same bytes immutably. This reuses content hashing, never a durable or consensus receipt.
    /// File publication, duplicate sizes, independent pins and atomic journal ordering are the
    /// same as `put_pinned_batch`; uncertain publication still requires reopening the store.
    pub fn put_committed_pinned_batch(
        &self,
        objects: &[CommittedPinnedObject<'_>],
    ) -> Result<Vec<ObjectId>, StoreError> {
        if objects.len() > 65_536 || objects.iter().any(|object| object.owners.is_empty()) {
            return Err(StoreError::InvalidPublication);
        }
        let pins = objects.iter().try_fold(0usize, |count, object| {
            count.checked_add(object.owners.len()).ok_or(StoreError::InvalidPublication)
        })?;
        if pins > 262_144 {
            return Err(StoreError::InvalidPublication);
        }
        let mut inner = self.inner.lock();
        inner.journal.ensure_healthy()?;
        let mut next = inner.objects.clone();
        let mut ids = Vec::with_capacity(objects.len());
        for object in objects {
            let id = object.id;
            if let Some(existing) = next.get(&id) {
                // Previously journaled immutable objects were verified on open and made durable
                // before their original pins. Reused leaves need no repeat fsync per state.
                if existing.size != object.bytes.len() as u64 {
                    return Err(StoreError::CorruptObject(id));
                }
            } else {
                let path = inner.object_dir.join(id.filename());
                inner.publish_object(&path, id, object.bytes)?;
            }

            next.entry(id)
                .or_insert_with(|| ObjectEntry {
                    id,
                    size: object.bytes.len() as u64,
                    owners: BTreeSet::new(),
                })
                .owners
                .extend(object.owners.iter().copied());
            ids.push(id);
        }
        inner.persist(next)?;
        for (object, id) in objects.iter().zip(&ids) {
            inner.cache_borrowed(*id, object.bytes);
        }
        Ok(ids)
    }
    /// Releases every pin belonging to the supplied owners in a single atomic commit.
    pub fn release_owners(&self, owners: &[Owner]) -> Result<(), StoreError> {
        let mut inner = self.inner.lock();
        inner.journal.ensure_healthy()?;
        let owners = owners.iter().copied().collect::<BTreeSet<_>>();
        let mut next = inner.objects.clone();
        for entry in next.values_mut() {
            entry.owners.retain(|owner| !owners.contains(owner));
        }
        inner.persist(next)
    }
    /// Returns independently owned bytes. A reader remains valid after disk garbage collection.
    pub fn get(&self, id: ObjectId) -> Result<Option<Arc<[u8]>>, StoreError> {
        self.get_bounded(id, usize::MAX)
    }

    /// Checks the saved length before allocating or returning a cached object.
    pub fn get_bounded(
        &self,
        id: ObjectId,
        max_bytes: usize,
    ) -> Result<Option<Arc<[u8]>>, StoreError> {
        let mut inner = self.inner.lock();
        inner.journal.ensure_healthy()?;
        let Some(object) = inner.objects.get(&id) else {
            inner.observations.missing_reads_total =
                inner.observations.missing_reads_total.saturating_add(1);
            return Ok(None)
        };
        let size = object.size;
        if size > max_bytes as u64 {
            inner.observations.oversized_reads_total =
                inner.observations.oversized_reads_total.saturating_add(1);
            return Err(StoreError::ObjectTooLarge { size, limit: max_bytes })
        }
        inner.clock = inner.clock.wrapping_add(1);
        let touched = inner.clock;
        if let Some(entry) = inner.cache.get_mut(&id) {
            entry.touched = touched;
            let bytes = entry.bytes.clone();
            inner.observations.cache_hits_total =
                inner.observations.cache_hits_total.saturating_add(1);
            return Ok(Some(bytes))
        }
        inner.observations.cache_misses_total =
            inner.observations.cache_misses_total.saturating_add(1);
        let path = inner.object_dir.join(id.filename());
        let file = fs::open(&path)?;
        if file.metadata().map_err(|err| fs::FsPathError::metadata(err, &path))?.len() != size {
            return Err(StoreError::CorruptObject(id))
        }
        let mut bytes = Vec::new();
        file.take(size.saturating_add(1))
            .read_to_end(&mut bytes)
            .map_err(|err| fs::FsPathError::read(err, &path))?;
        if bytes.len() as u64 != size || !id.matches(&bytes) {
            return Err(StoreError::CorruptObject(id))
        }
        let bytes = Arc::<[u8]>::from(bytes);
        inner.observations.verified_disk_read_bytes_total =
            inner.observations.verified_disk_read_bytes_total.saturating_add(size);
        inner.cache(id, bytes.clone());
        Ok(Some(bytes))
    }
    /// Lists objects pinned by one logical owner.
    /// Call `ensure_healthy` before using this infallible view for recovery or mutation.
    pub fn owner_objects(&self, owner: Owner) -> Vec<ObjectId> {
        self.inner
            .lock()
            .owner_index
            .get(&owner)
            .map(|ids| ids.iter().copied().collect())
            .unwrap_or_default()
    }

    /// Lists all durable pins, useful for rebuilding typed lookup indexes after restart.
    /// Call `ensure_healthy` before using this infallible view for recovery or mutation.
    pub fn owners(&self) -> BTreeSet<Owner> {
        self.inner.lock().owner_index.keys().copied().collect()
    }
    /// Removes only known object names with no remaining durable owners.
    pub fn collect_garbage(&self) -> Result<usize, StoreError> {
        self.collect_garbage_detailed().map(|(objects, _)| objects)
    }

    pub(crate) fn collect_garbage_detailed(&self) -> Result<(usize, usize), StoreError> {
        let mut inner = self.inner.lock();
        inner.journal.ensure_healthy()?;
        let dead = inner
            .objects
            .values()
            .filter(|object| object.owners.is_empty())
            .map(|object| object.id)
            .collect::<Vec<_>>();
        let mut next = inner.objects.clone();
        for id in &dead {
            next.remove(id);
        }
        // An interrupted deletion leaves an unreferenced orphan, never a dangling reference.
        inner.persist(next)?;
        let mut deleted = 0;
        let mut sidecars = 0;
        let object_dir = inner.object_dir.clone();
        for entry in fs::read_dir(&object_dir)? {
            let entry = entry.map_err(|err| fs::FsPathError::read_dir(err, &object_dir))?;
            if let Some(id) = entry.file_name().to_str().and_then(ObjectId::from_filename) &&
                !inner.objects.contains_key(&id)
            {
                let removed_bytes = std::fs::symlink_metadata(entry.path())
                    .map_err(|error| fs::FsPathError::metadata(error, entry.path()))?
                    .len();
                fs::remove_file(entry.path())?;
                inner.record_collection(removed_bytes);
                if let Some(cached) = inner.cache.remove(&id) {
                    inner.cache_bytes -= cached.bytes.len();
                }
                deleted += 1;
                sidecars += usize::from(id.kind == ObjectKind::BlobSidecar);
            }
        }
        Ok((deleted, sidecars))
    }

    /// Returns physical byte counts and logical ownership separately.
    pub fn stats(&self) -> Result<StoreStats, StoreError> {
        let inner = self.inner.lock();
        inner.journal.ensure_healthy()?;
        let mut stats = StoreStats { cached_bytes: inner.cache_bytes as u64, ..Default::default() };
        for object in inner.objects.values() {
            stats.logical_bytes += object.size * object.owners.len() as u64;
            stats.owner_pins += object.owners.len() as u64;
        }
        for entry in fs::read_dir(&inner.object_dir)? {
            let entry = entry.map_err(|err| fs::FsPathError::read_dir(err, &inner.object_dir))?;
            if entry.file_name().to_str().and_then(ObjectId::from_filename).is_some() {
                stats.physical_bytes += fs::metadata(entry.path())?.len();
                stats.physical_objects += 1;
            }
        }
        Ok(stats)
    }
}

/// Persisted network identity. Configuration hashes must cover preset and fork schedule.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreIdentity {
    /// Genesis validators root for the selected chain.
    pub genesis_root: [u8; 32],
    /// Digest of the complete consensus configuration.
    pub config_digest: [u8; 32],
}

/// Borrowed bytes and independent pins for one atomic multi-owner publication.
#[derive(Debug)]
pub struct PinnedObject<'a> {
    /// Exact semantic kind; fragments do not masquerade as complete states.
    pub kind: ObjectKind,
    /// Encoding belongs to the content identity.
    pub encoding: Encoding,
    /// Caller-owned bytes remain alive until the blocking batch completes.
    pub bytes: &'a [u8],
    /// At least one independent owner must retain the published object.
    pub owners: &'a [Owner],
}

/// A content commitment tied to exact immutable serialized bytes.
///
/// Borrowed input stays immutably borrowed for the full lifetime of this object. Owned input is
/// moved into it and never exposes mutable access. This is an ordinary SHA-256 commitment, not
/// proof of SSZ validity, consensus admission, durable storage or independent ownership.
///
/// ```compile_fail
/// use reth_embedded_beacon_shared_data::{CommittedObject, Encoding, ObjectKind};
/// use std::borrow::Cow;
/// let mut bytes = vec![1, 2, 3];
/// let object = CommittedObject::new(ObjectKind::BeaconStateComponent, Encoding::Raw,
///     Cow::Borrowed(&bytes));
/// bytes[0] = 4;
/// assert_eq!(object.bytes(), [1, 2, 3]);
/// ```
#[derive(Debug)]
pub struct CommittedObject<'a> {
    id: ObjectId,
    bytes: Cow<'a, [u8]>,
}

impl<'a> CommittedObject<'a> {
    /// Hash exact original bytes once while taking immutable ownership of their contents.
    pub fn new(kind: ObjectKind, encoding: Encoding, bytes: Cow<'a, [u8]>) -> Self {
        Self { id: ObjectId::new(kind, encoding, &bytes), bytes }
    }

    /// Exact kind, encoding and content digest established by this object's constructor.
    pub const fn id(&self) -> ObjectId {
        self.id
    }

    /// Borrow the same immutable contents without copying or recalculating their commitment.
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Associate independent owners with this exact content for one blocking publication.
    pub fn pinned<'b>(&'b self, owners: &'b [Owner]) -> CommittedPinnedObject<'b> {
        CommittedPinnedObject { id: self.id, bytes: &self.bytes, owners }
    }
}

/// Private-field view of an exact immutable content commitment and its independent pins.
/// Only `CommittedObject::pinned` creates these views; callers cannot substitute bytes or IDs.
#[derive(Debug)]
pub struct CommittedPinnedObject<'a> {
    id: ObjectId,
    bytes: &'a [u8],
    owners: &'a [Owner],
}

/// Immutable object identity including semantic kind and byte encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ObjectId {
    /// Semantic type prevents accidental reuse of differently interpreted bytes.
    pub kind: ObjectKind,
    /// Encoding belongs to identity; a conversion creates a distinct object.
    pub encoding: Encoding,
    /// SHA-256 digest of the exact serialized bytes.
    pub digest: [u8; 32],
}

impl ObjectId {
    /// Computes an identity without performing disk writes.
    pub fn new(kind: ObjectKind, encoding: Encoding, bytes: &[u8]) -> Self {
        Self { kind, encoding, digest: Sha256::digest(bytes).into() }
    }

    fn matches(&self, bytes: &[u8]) -> bool {
        self.digest == <[u8; 32]>::from(Sha256::digest(bytes))
    }

    fn filename(self) -> String {
        let mut name = format!("{:02x}-{:02x}-", self.kind as u8, self.encoding as u8);
        for byte in self.digest {
            let _ = write!(name, "{byte:02x}");
        }
        name.push_str(".obj");
        name
    }

    fn from_filename(name: &str) -> Option<Self> {
        if name.len() != 74 ||
            !name.ends_with(".obj") ||
            name.as_bytes()[2] != b'-' ||
            name.as_bytes()[5] != b'-'
        {
            return None;
        }
        let kind = match name.get(..2)? {
            "00" => ObjectKind::Blob,
            "01" => ObjectKind::Cell,
            "02" => ObjectKind::Proof,
            "03" => ObjectKind::ExecutionPayload,
            "04" => ObjectKind::BeaconState,
            "05" => ObjectKind::BlobSidecar,
            "06" => ObjectKind::Commitment,
            "07" => ObjectKind::BeaconBlock,
            "08" => ObjectKind::BeaconStateComponent,
            "09" => ObjectKind::BeaconStateBranch,
            "0a" => ObjectKind::BeaconStateManifest,
            "0b" => ObjectKind::BeaconPayloadEnvelope,
            "0c" => ObjectKind::BeaconDataSidecar,
            "0d" => ObjectKind::BeaconObservation,
            _ => return None,
        };
        let encoding = match name.get(3..5)? {
            "00" => Encoding::Raw,
            "01" => Encoding::Ssz,
            "02" => Encoding::Rlp,
            "03" => Encoding::Json,
            _ => return None,
        };
        let mut digest = [0; 32];
        for (index, byte) in digest.iter_mut().enumerate() {
            *byte = u8::from_str_radix(name.get(6 + index * 2..8 + index * 2)?, 16).ok()?;
        }
        Some(Self { kind, encoding, digest })
    }
}

/// Semantic types admitted to the shared store.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[repr(u8)]
pub enum ObjectKind {
    /// Complete blob bytes.
    Blob,
    /// One data-availability cell; presence does not imply a complete blob.
    Cell,
    /// A mathematical commitment proof, excluding contextual beacon validity.
    Proof,
    /// Exact serialized execution payload bytes.
    ExecutionPayload,
    /// Exact serialized beacon state bytes.
    BeaconState,
    /// Full execution-pool sidecar serialization.
    BlobSidecar,
    /// Polynomial commitment bytes, distinct from proofs.
    Commitment,
    /// Complete unsigned or signed beacon block bytes.
    BeaconBlock,
    /// Exact SSZ field fragment; never a complete or authoritative beacon state.
    BeaconStateComponent,
    /// Bounded immutable tree of field fragments.
    BeaconStateBranch,
    /// Complete reconstruction descriptor committed by a compact state record.
    BeaconStateManifest,
    /// Complete signed beacon execution-payload envelope, distinct from an EL payload.
    BeaconPayloadEnvelope,
    /// Complete original beacon blob or data-column sidecar SSZ, distinct from EL pool data.
    BeaconDataSidecar,
    /// Complete original validated off-block observation; replay still runs native checks.
    BeaconObservation,
}

/// Exact byte encoding of an object.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[repr(u8)]
pub enum Encoding {
    /// Unwrapped bytes.
    Raw,
    /// Simple serialize bytes.
    Ssz,
    /// Recursive length prefix bytes.
    Rlp,
    /// JSON metadata descriptor bytes.
    Json,
}

/// Independent logical retainers of shared bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Owner {
    /// Execution pool transaction hash.
    Pool([u8; 32]),
    /// Beacon block or state root.
    Consensus([u8; 32]),
    /// Recovery record identifier.
    Recovery([u8; 32]),
    /// Immutable bootstrap generation, separate from other recovery owners.
    Checkpoint([u8; 32]),
    /// Committed compact state root, separate from all execution and bootstrap pins.
    CompactState([u8; 32]),
    /// Immutable compact descriptor owning its direct children once.
    CompactNode(ObjectId),
}

/// Byte and pin statistics. Metadata files and caller-held Arcs are excluded.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StoreStats {
    /// Bytes in all immutable object files, including uncollected orphans.
    pub physical_bytes: u64,
    /// Number of immutable object files.
    pub physical_objects: u64,
    /// Bytes counted once per durable owner pin.
    pub logical_bytes: u64,
    /// Number of durable owner pins.
    pub owner_pins: u64,
    /// Byte cache ownership; active readers may additionally retain evicted bytes.
    pub cached_bytes: u64,
}

/// Filesystem, integrity and reference errors.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// Filesystem error with path context.
    #[error(transparent)]
    Fs(#[from] fs::FsPathError),
    /// Invalid serialized metadata.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    /// Another process already holds the exclusive writer lock.
    #[error("store is already open: {0:?}")]
    AlreadyOpen(PathBuf),
    /// Genesis or configuration differs from the persisted store.
    #[error("persisted genesis or consensus configuration differs")]
    IdentityMismatch,
    /// An unsupported on-disk schema was encountered.
    #[error("unsupported embedded beacon storage schema")]
    UnsupportedSchema,
    /// Committed metadata failed checksum or framing validation.
    #[error("corrupt committed storage journal")]
    CorruptJournal,
    /// Object contents differ from the digest or saved byte length.
    #[error("corrupt immutable object: {0:?}")]
    CorruptObject(ObjectId),
    /// A reference points to an absent object.
    #[error("missing immutable object: {0:?}")]
    MissingObject(ObjectId),
    /// An object exceeds the caller-requested byte limit.
    #[error("immutable object size {size} exceeds read limit {limit}")]
    ObjectTooLarge {
        /// Declared immutable object length in bytes.
        size: u64,
        /// Maximum byte length permitted by the caller.
        limit: usize,
    },
    /// A journal snapshot exceeds the explicit size limit.
    #[error("storage snapshot exceeds 64 MiB")]
    SnapshotTooLarge,
    /// Borrowed graph publication has no owner or exceeds its explicit edge budget.
    #[error("invalid or oversized multi-owner object publication")]
    InvalidPublication,
    /// Generation identifier cannot wrap.
    #[error("storage generation overflow")]
    GenerationOverflow,
    /// Atomic rename completed, but the post-publication durability sync failed.
    #[error("file publication durability is uncertain; reopen journal before reuse: {0}")]
    PublicationUncertain(#[source] fs::FsPathError),
    /// Journal memory may differ from its published generation; reopening is required.
    #[error("journal requires reopening after an uncertain publication")]
    JournalPoisoned,
}

// Verify retained objects at startup without allocating each full state or payload.
fn verify_object_file(path: &Path, id: ObjectId, size: u64) -> Result<(), StoreError> {
    let file = fs::open(path)?;
    if file.metadata().map_err(|err| fs::FsPathError::metadata(err, path))?.len() != size {
        return Err(StoreError::CorruptObject(id))
    }
    let mut reader = file.take(size.saturating_add(1));
    let mut hasher = Sha256::new();
    let mut total = 0u64;
    let mut buffer = [0u8; 65536];
    loop {
        let read = reader.read(&mut buffer).map_err(|err| fs::FsPathError::read(err, path))?;
        if read == 0 {
            break
        }
        total += read as u64;
        hasher.update(&buffer[..read]);
    }
    if total != size || <[u8; 32]>::from(hasher.finalize()) != id.digest {
        return Err(StoreError::CorruptObject(id))
    }
    Ok(())
}
#[derive(Debug)]
struct StoreInner {
    namespace_dir: PathBuf,
    object_dir: PathBuf,
    journal: journal::Journal,
    objects: BTreeMap<ObjectId, ObjectEntry>,
    owner_index: BTreeMap<Owner, BTreeSet<ObjectId>>,
    references: observation::ReferenceObservation,
    observations: observation::StoreCounters,
    cache: BTreeMap<ObjectId, CacheEntry>,
    cache_bytes: usize,
    cache_limit: usize,
    clock: u64,
    beacon_gc: beacon_gc::BeaconGcCursor,
}

impl StoreInner {
    fn publish_object(
        &mut self,
        path: &Path,
        id: ObjectId,
        bytes: &[u8],
    ) -> Result<(), StoreError> {
        let result = if path.exists() {
            verify_object_file(path, id, bytes.len() as u64)?;
            journal::ensure_published_durability(path)
        } else {
            journal::publish_file(path, bytes)
        };
        if matches!(result, Err(StoreError::PublicationUncertain(_))) {
            self.journal.mark_publication_uncertain();
        }
        result
    }
    fn persist(&mut self, next: BTreeMap<ObjectId, ObjectEntry>) -> Result<(), StoreError> {
        self.journal.ensure_healthy()?;
        // Immutable bytes already precede the reference generation. An identical complete pin
        // map adds no new durability obligation; uncertain publication must still fail closed.
        if next == self.objects {
            return Ok(())
        }
        let started = std::time::Instant::now();
        self.journal.commit(&ReferenceState { objects: next.values().cloned().collect() })?;
        self.observations.journal_commits_total =
            self.observations.journal_commits_total.saturating_add(1);
        self.observations.journal_commit_micros_total = self
            .observations
            .journal_commit_micros_total
            .saturating_add(u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX));
        (self.owner_index, self.references) = build_owner_index(&next);
        self.objects = next;
        Ok(())
    }

    fn cache(&mut self, id: ObjectId, bytes: Arc<[u8]>) {
        if bytes.len() > self.cache_limit || self.cache.contains_key(&id) {
            return;
        }
        while self.cache_bytes.saturating_add(bytes.len()) > self.cache_limit {
            let Some(oldest) =
                self.cache.iter().min_by_key(|(_, entry)| entry.touched).map(|(id, _)| *id)
            else {
                break
            };
            if let Some(evicted) = self.cache.remove(&oldest) {
                self.cache_bytes -= evicted.bytes.len();
                self.observations.cache_evictions_total =
                    self.observations.cache_evictions_total.saturating_add(1);
            }
        }
        self.clock = self.clock.wrapping_add(1);
        self.cache_bytes += bytes.len();
        self.cache.insert(id, CacheEntry { bytes, touched: self.clock });
    }

    fn cache_borrowed(&mut self, id: ObjectId, bytes: &[u8]) {
        if bytes.len() > self.cache_limit {
            return
        }
        // The batch writer independently hashed the exact input and committed its lengths and
        // pins before this call. Retain a matching immutable cached allocation instead of copying
        // every unchanged state leaf into a temporary Arc that cache() would immediately drop.
        if let Some(entry) = self.cache.get_mut(&id) {
            debug_assert_eq!(entry.bytes.len(), bytes.len());
            self.clock = self.clock.wrapping_add(1);
            entry.touched = self.clock;
            return
        }
        self.cache(id, Arc::from(bytes));
    }
}

#[derive(Debug)]
struct CacheEntry {
    bytes: Arc<[u8]>,
    touched: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ObjectEntry {
    id: ObjectId,
    size: u64,
    owners: BTreeSet<Owner>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct ReferenceState {
    objects: Vec<ObjectEntry>,
}

fn build_owner_index(
    objects: &BTreeMap<ObjectId, ObjectEntry>,
) -> (BTreeMap<Owner, BTreeSet<ObjectId>>, observation::ReferenceObservation) {
    let mut index = BTreeMap::<Owner, BTreeSet<ObjectId>>::new();
    let mut references = observation::ReferenceObservation::default();
    for object in objects.values() {
        references.tracked_bytes = references.tracked_bytes.saturating_add(object.size);
        references.owner_pins = references.owner_pins.saturating_add(object.owners.len() as u64);
        references.pinned_logical_bytes = references
            .pinned_logical_bytes
            .saturating_add(object.size.saturating_mul(object.owners.len() as u64));
        if !object.owners.is_empty() {
            references.pinned_unique_bytes =
                references.pinned_unique_bytes.saturating_add(object.size);
        }
        for owner in &object.owners {
            index.entry(*owner).or_default().insert(object.id);
        }
    }
    (index, references)
}
#[cfg(test)]
mod tests;
