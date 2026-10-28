//! Atomic devnet checkpoints. Each state is addressed by its PBT root; block references are
//! published only after the corresponding state file has been synced and renamed into place.

use crate::{
    migration::{ArtifactError, Artifacts, DecodeLimits},
    Blake3Hasher, StateTree, TreeHasher,
};
use alloy_primitives::B256;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::Mutex,
};

/// A durable state-snapshot store for experimental nodes and migration replay.
///
/// This writes full canonical artifacts, not incremental database pages. Checkpoints remain
/// until the caller explicitly prunes blocks proven obsolete by finality. It is not a mainnet
/// persistence backend.
#[derive(Debug)]
pub struct SnapshotStore<H = Blake3Hasher> {
    path: PathBuf,
    cache: Mutex<BTreeMap<B256, StateTree<H>>>,
    limits: DecodeLimits,
}

impl<H: TreeHasher> SnapshotStore<H> {
    /// Opens a checkpoint directory. Use one shared store per node directory.
    pub fn open(path: impl Into<PathBuf>, limits: DecodeLimits) -> Result<Self, StoreError> {
        let path = path.into();
        reth_fs_util::create_dir_all(path.join("states"))?;
        reth_fs_util::create_dir_all(path.join("blocks"))?;
        Ok(Self { path, cache: Mutex::default(), limits })
    }

    /// Returns the checkpoint directory.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Loads and verifies state using its trusted PBT root.
    pub fn load(&self, root: B256) -> Result<StateTree<H>, StoreError> {
        let mut cache = self.cache.lock().map_err(|_| StoreError::Poisoned)?;
        if let Some(state) = cache.get(&root) {
            return Ok(state.clone());
        }
        let state = self.load_state(root)?;
        cache_state(&mut cache, root, state.clone());
        Ok(state)
    }

    /// Persists a state before returning its root. No block reference is implied.
    pub fn save(&self, state: &StateTree<H>) -> Result<B256, StoreError> {
        let mut cache = self.cache.lock().map_err(|_| StoreError::Poisoned)?;
        self.save_state(state, &mut cache)
    }

    /// Publishes a block-hash-to-PBT-root reference after durably saving its state.
    ///
    /// This also supports pre-activation blocks whose header commits to the MPT instead.
    pub fn save_block(&self, block: B256, state: &StateTree<H>) -> Result<B256, StoreError> {
        let mut cache = self.cache.lock().map_err(|_| StoreError::Poisoned)?;
        let root = self.save_state(state, &mut cache)?;
        reth_fs_util::atomic_write_file(
            &self.path.join("blocks").join(block.to_string()),
            |file| file.write_all(root.as_slice()),
        )?;
        Ok(root)
    }

    /// Resolves an accepted block's shadow or canonical PBT state.
    pub fn load_block(&self, block: B256) -> Result<StateTree<H>, StoreError> {
        self.load(self.block_root(block)?)
    }

    /// Imports bounded artifacts against a trusted finalized MPT header and checkpoints them.
    ///
    /// The caller must obtain `block` and `anchor_mpt_root` from its validated, finalized header
    /// chain. Both artifacts are authenticated before any checkpoint or block reference is saved.
    pub fn import_anchor(
        &self,
        block: B256,
        anchor_mpt_root: B256,
        snapshot_path: &Path,
        preimages_path: &Path,
    ) -> Result<B256, StoreError> {
        let artifacts = Artifacts {
            snapshot: read_bounded(snapshot_path, snapshot_byte_limit(self.limits)?)?,
            preimages: read_bounded(preimages_path, preimage_byte_limit(self.limits)?)?,
        };
        let state = artifacts.verify::<H>(anchor_mpt_root, self.limits)?;
        self.save_block(block, &state)
    }

    /// Deletes explicitly obsolete block references and their otherwise unreferenced states.
    ///
    /// The caller must authenticate the finalized checkpoint against its validated header and prove
    /// that each `obsolete_blocks` entry is obsolete under it; this store has no header ancestry
    /// information. Every other block reference
    /// and its state are retained, including competing branches and states shared with an obsolete
    /// block. Root-only checkpoints outside the obsolete candidates are also retained.
    ///
    /// All references, candidate states, and the durable finalized state are checked before the
    /// first deletion. A failure during deletion may leave obsolete files behind, but never removes
    /// a retained state's checkpoint. The finalized block cannot itself be pruned.
    pub fn prune_finalized(
        &self,
        finalized: B256,
        obsolete_blocks: &[B256],
    ) -> Result<PruneOutcome, StoreError> {
        let mut cache = self.cache.lock().map_err(|_| StoreError::Poisoned)?;
        let obsolete: BTreeSet<_> = obsolete_blocks.iter().copied().collect();
        if obsolete.contains(&finalized) {
            return Err(StoreError::PruneFinalized);
        }
        let blocks_path = self.path.join("blocks");
        let mut references = BTreeMap::new();
        for entry in reth_fs_util::read_dir(&blocks_path)? {
            let entry =
                entry.map_err(|error| reth_fs_util::FsPathError::read_dir(error, &blocks_path))?;
            if entry.path().extension().is_some_and(|extension| extension == "tmp") {
                continue;
            }
            let name = entry.file_name();
            let name =
                name.to_str().ok_or(ArtifactError::Invalid("invalid block checkpoint filename"))?;
            let block = name
                .parse::<B256>()
                .map_err(|_| ArtifactError::Invalid("invalid block checkpoint filename"))?;
            if name != block.to_string() {
                return Err(ArtifactError::Invalid("invalid block checkpoint filename").into());
            }
            let root = self.block_root(block)?;
            references.insert(block, root);
        }
        let finalized_root =
            *references.get(&finalized).ok_or(StoreError::MissingBlockReference(finalized))?;
        // Bypass the cache so the one checkpoint pruning must preserve is verified on disk.
        self.load_state(finalized_root)?;
        let mut candidate_roots = BTreeSet::new();
        for block in &obsolete {
            let root = *references.get(block).ok_or(StoreError::MissingBlockReference(*block))?;
            if candidate_roots.insert(root) {
                self.load_state(root)?;
            }
        }
        let retained_roots: BTreeSet<_> = references
            .iter()
            .filter(|(block, _)| !obsolete.contains(*block))
            .map(|(_, root)| *root)
            .collect();
        for root in &retained_roots {
            let path = self.state_path(*root);
            if !reth_fs_util::metadata(&path)?.is_file() {
                return Err(ArtifactError::Invalid("invalid retained state checkpoint").into());
            }
        }
        let mut outcome = PruneOutcome::default();
        // Remove references first. Any interrupted prune leaves every surviving reference usable.
        for block in obsolete {
            reth_fs_util::remove_file(blocks_path.join(block.to_string()))?;
            outcome.block_references += 1;
        }
        // Persist reference removal before any state file can disappear after a crash.
        reth_fs_util::open(&blocks_path)?
            .sync_all()
            .map_err(|error| reth_fs_util::FsPathError::fsync(error, &blocks_path))?;
        for root in candidate_roots.difference(&retained_roots) {
            reth_fs_util::remove_file(self.state_path(*root))?;
            cache.remove(root);
            outcome.state_files += 1;
        }
        let states_path = self.path.join("states");
        reth_fs_util::open(&states_path)?
            .sync_all()
            .map_err(|error| reth_fs_util::FsPathError::fsync(error, &states_path))?;
        Ok(outcome)
    }

    fn state_path(&self, root: B256) -> PathBuf {
        self.path.join("states").join(root.to_string())
    }

    fn save_state(
        &self,
        state: &StateTree<H>,
        cache: &mut BTreeMap<B256, StateTree<H>>,
    ) -> Result<B256, StoreError> {
        if state.accounts().count() > self.limits.max_accounts ||
            state.tree().len() > self.limits.max_leaves
        {
            return Err(ArtifactError::ResourceLimit.into());
        }
        let mut codes = BTreeSet::new();
        let mut total_code_size = 0usize;
        for (_, account) in state
            .accounts()
            .filter(|(_, account)| !account.code.is_empty() && !account.is_delegation())
        {
            if account.code.len() > self.limits.max_code_size {
                return Err(ArtifactError::ResourceLimit.into());
            }
            if codes.insert(alloy_primitives::keccak256(&account.code)) {
                total_code_size = total_code_size
                    .checked_add(account.code.len())
                    .ok_or(ArtifactError::ResourceLimit)?;
                if total_code_size > self.limits.max_total_code_size {
                    return Err(ArtifactError::ResourceLimit.into());
                }
            }
        }
        let root = state.root();
        let artifacts = Artifacts::from_state(state)?;
        reth_fs_util::atomic_write_file(&self.state_path(root), |file| -> std::io::Result<()> {
            file.write_all(&(artifacts.preimages.len() as u64).to_be_bytes())?;
            file.write_all(&artifacts.preimages)?;
            file.write_all(&artifacts.snapshot)
        })?;
        cache_state(cache, root, state.clone());
        Ok(root)
    }

    fn block_root(&self, block: B256) -> Result<B256, StoreError> {
        let bytes = read_bounded(&self.path.join("blocks").join(block.to_string()), HASH_BYTES)?;
        if bytes.len() != HASH_BYTES {
            return Err(ArtifactError::Invalid("invalid block checkpoint reference").into());
        }
        Ok(B256::from_slice(&bytes))
    }

    fn load_state(&self, root: B256) -> Result<StateTree<H>, StoreError> {
        let path = self.state_path(root);
        let preimage_limit = preimage_byte_limit(self.limits)?;
        let snapshot_limit = snapshot_byte_limit(self.limits)?;
        let limit = CHECKPOINT_LENGTH_BYTES
            .checked_add(preimage_limit)
            .and_then(|limit| limit.checked_add(snapshot_limit))
            .ok_or(ArtifactError::ResourceLimit)?;
        let (mut file, length) = open_bounded(&path, limit)?;
        let mut prefix = [0; CHECKPOINT_LENGTH_BYTES];
        read_exact(&mut file, &mut prefix, &path)?;
        let preimage_length = usize::try_from(u64::from_be_bytes(prefix))
            .map_err(|_| ArtifactError::ResourceLimit)?;
        if preimage_length > preimage_limit {
            return Err(ArtifactError::ResourceLimit.into());
        }
        let snapshot_length = length
            .checked_sub(CHECKPOINT_LENGTH_BYTES)
            .and_then(|length| length.checked_sub(preimage_length))
            .ok_or(ArtifactError::Truncated)?;
        if snapshot_length > snapshot_limit {
            return Err(ArtifactError::ResourceLimit.into());
        }
        let mut artifacts =
            Artifacts { preimages: vec![0; preimage_length], snapshot: vec![0; snapshot_length] };
        read_exact(&mut file, &mut artifacts.preimages, &path)?;
        read_exact(&mut file, &mut artifacts.snapshot, &path)?;
        check_eof(&mut file, &path)?;
        Ok(artifacts.restore::<H>(root, self.limits)?)
    }
}

/// Files removed by an explicit finalized-checkpoint prune.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PruneOutcome {
    /// Number of obsolete block references deleted.
    pub block_references: usize,
    /// Number of state files no remaining block reference uses that were deleted.
    pub state_files: usize,
}

/// Checkpoint read/write or verification failure.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// Filesystem operation failed, including its path.
    #[error(transparent)]
    Io(#[from] reth_fs_util::FsPathError),
    /// Checkpoint failed canonical artifact or root verification.
    #[error(transparent)]
    Artifact(#[from] ArtifactError),
    /// A panicking store writer poisoned the serialization lock.
    #[error("PBT snapshot store lock poisoned")]
    Poisoned,
    /// A prune request attempted to remove the trusted finalized block itself.
    #[error("cannot prune the finalized PBT checkpoint")]
    PruneFinalized,
    /// A requested finalized or obsolete block has no checkpoint reference.
    #[error("PBT checkpoint reference is unavailable for block {0}")]
    MissingBlockReference(B256),
}

fn cache_state<H>(cache: &mut BTreeMap<B256, StateTree<H>>, root: B256, state: StateTree<H>) {
    // Cache eviction never removes the durable checkpoint needed by a fork.
    if cache.len() >= 64 {
        cache.pop_first();
    }
    cache.insert(root, state);
}

const HASH_BYTES: usize = 32;
const CHECKPOINT_LENGTH_BYTES: usize = 8;

fn preimage_byte_limit(limits: DecodeLimits) -> Result<usize, ArtifactError> {
    // Each account carries address[20] + slotCount[4], and each storage leaf at most one slot[32].
    limits
        .max_accounts
        .checked_mul(24)
        .and_then(|accounts| limits.max_leaves.checked_mul(HASH_BYTES)?.checked_add(accounts))
        .ok_or(ArtifactError::ResourceLimit)
}

fn snapshot_byte_limit(limits: DecodeLimits) -> Result<usize, ArtifactError> {
    // A header uses at most 97 bytes excluding inline slot values; its storage marker uses 33.
    // Each leaf adds at most 34 bytes of group framing and 34 bytes for its index and value.
    limits
        .max_accounts
        .checked_mul(130)
        .and_then(|accounts| limits.max_leaves.checked_mul(68)?.checked_add(accounts))
        .and_then(|records| records.checked_add(1 + HASH_BYTES))
        .ok_or(ArtifactError::ResourceLimit)
}

fn open_bounded(path: &Path, limit: usize) -> Result<(File, usize), StoreError> {
    let file = reth_fs_util::open(path)?;
    let metadata =
        file.metadata().map_err(|error| reth_fs_util::FsPathError::metadata(error, path))?;
    let length = usize::try_from(metadata.len()).map_err(|_| ArtifactError::ResourceLimit)?;
    if !metadata.is_file() || length > limit {
        return Err(ArtifactError::ResourceLimit.into());
    }
    Ok((file, length))
}

fn read_bounded(path: &Path, limit: usize) -> Result<Vec<u8>, StoreError> {
    let (mut file, length) = open_bounded(path, limit)?;
    let mut bytes = vec![0; length];
    read_exact(&mut file, &mut bytes, path)?;
    check_eof(&mut file, path)?;
    Ok(bytes)
}

fn read_exact(file: &mut File, bytes: &mut [u8], path: &Path) -> Result<(), StoreError> {
    file.read_exact(bytes).map_err(|error| {
        if error.kind() == std::io::ErrorKind::UnexpectedEof {
            ArtifactError::Truncated.into()
        } else {
            reth_fs_util::FsPathError::read(error, path).into()
        }
    })
}

fn check_eof(file: &mut File, path: &Path) -> Result<(), StoreError> {
    let mut trailing = [0];
    if file.read(&mut trailing).map_err(|error| reth_fs_util::FsPathError::read(error, path))? != 0
    {
        return Err(ArtifactError::Invalid("checkpoint file changed while reading").into());
    }
    Ok(())
}
