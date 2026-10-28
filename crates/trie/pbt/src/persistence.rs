//! Atomic devnet checkpoints. Each state is addressed by its PBT root; block references are
//! published only after the corresponding state file has been synced and renamed into place.

use crate::{
    migration::{ArtifactError, Artifacts, DecodeLimits},
    Blake3Hasher, StateTree, TreeHasher,
};
use alloy_primitives::B256;
use std::{
    collections::{BTreeMap, BTreeSet},
    io::Write,
    path::{Path, PathBuf},
    sync::Mutex,
};

/// A durable state-snapshot store for experimental nodes and migration replay.
///
/// This writes full canonical artifacts, not incremental database pages. It deliberately
/// retains every checkpoint so any unfinalized branch remains recoverable. Operators must
/// budget for this devnet-only space cost; it is not a mainnet persistence backend.
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
        let bytes = reth_fs_util::read(self.state_path(root))?;
        let length: [u8; 8] = bytes.get(..8).ok_or(ArtifactError::Truncated)?.try_into().unwrap();
        let length = usize::try_from(u64::from_be_bytes(length))
            .map_err(|_| ArtifactError::ResourceLimit)?;
        if length > bytes.len() - 8 {
            return Err(ArtifactError::Truncated.into());
        }
        let artifacts = Artifacts {
            preimages: bytes[8..8 + length].to_vec(),
            snapshot: bytes[8 + length..].to_vec(),
        };
        let state = artifacts.restore::<H>(root, self.limits)?;
        cache_state(&mut cache, root, state.clone());
        Ok(state)
    }

    /// Persists a state before returning its root. No block reference is implied.
    pub fn save(&self, state: &StateTree<H>) -> Result<B256, StoreError> {
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
        let mut cache = self.cache.lock().map_err(|_| StoreError::Poisoned)?;
        let artifacts = Artifacts::from_state(state)?;
        atomic_write(&self.state_path(root), |file| -> std::io::Result<()> {
            file.write_all(&(artifacts.preimages.len() as u64).to_be_bytes())?;
            file.write_all(&artifacts.preimages)?;
            file.write_all(&artifacts.snapshot)
        })?;
        cache_state(&mut cache, root, state.clone());
        Ok(root)
    }

    /// Publishes a block-hash-to-PBT-root reference after durably saving its state.
    ///
    /// This also supports pre-activation blocks whose header commits to the MPT instead.
    pub fn save_block(&self, block: B256, state: &StateTree<H>) -> Result<B256, StoreError> {
        let root = self.save(state)?;
        let _guard = self.cache.lock().map_err(|_| StoreError::Poisoned)?;
        atomic_write(&self.path.join("blocks").join(block.to_string()), |file| {
            file.write_all(root.as_slice())
        })?;
        Ok(root)
    }

    /// Resolves an accepted block's shadow or canonical PBT state.
    pub fn load_block(&self, block: B256) -> Result<StateTree<H>, StoreError> {
        let bytes = reth_fs_util::read(self.path.join("blocks").join(block.to_string()))?;
        if bytes.len() != 32 {
            return Err(ArtifactError::Invalid("invalid block checkpoint reference").into());
        }
        self.load(B256::from_slice(&bytes))
    }

    fn state_path(&self, root: B256) -> PathBuf {
        self.path.join("states").join(root.to_string())
    }
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
}

fn cache_state<H>(cache: &mut BTreeMap<B256, StateTree<H>>, root: B256, state: StateTree<H>) {
    // Only the cache is bounded. Eviction never removes the durable checkpoint needed by a fork.
    if cache.len() >= 64 {
        cache.pop_first();
    }
    cache.insert(root, state);
}

fn atomic_write(
    path: &Path,
    write: impl FnOnce(&mut std::fs::File) -> std::io::Result<()>,
) -> Result<(), reth_fs_util::FsPathError> {
    #[cfg(unix)]
    {
        reth_fs_util::atomic_write_file(path, write)
    }
    // Windows cannot open a directory through File::open for the parent-directory flush
    // performed by atomic_write_file. File contents are still flushed before replacement.
    #[cfg(not(unix))]
    {
        let temporary = path.with_extension("tmp");
        let mut file = reth_fs_util::create_file(&temporary)?;
        if let Err(error) = write(&mut file).and_then(|()| file.sync_all()) {
            drop(file);
            let _ = reth_fs_util::remove_file_if_exists(&temporary);
            return Err(reth_fs_util::FsPathError::write(error, &temporary));
        }
        drop(file);
        reth_fs_util::rename(&temporary, path)
    }
}
