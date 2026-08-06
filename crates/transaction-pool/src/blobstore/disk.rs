//! A simple diskstore for blobs

use crate::blobstore::{
    BlobStore, BlobStoreCleanupStat, BlobStoreError, BlobStoreSize, PooledBlobSidecar,
    PooledBlobSidecarData, SparseBlobSidecar,
};
use alloy_eips::{
    eip4844::{BlobAndProofV1, BlobAndProofV2, BlobCellsAndProofsV1, Bytes48},
    eip7594::{BlobCellMask, BlobTransactionSidecarVariant, Cell, BYTES_PER_CELL},
    eip7840::BlobParams,
    merge::EPOCH_SLOTS,
};
use alloy_primitives::{map::B256Set, TxHash, B128, B256};
use parking_lot::{Mutex, RwLock};
use schnellru::{ByLength, LruMap};
use std::{fmt, fs, io, path::PathBuf, sync::Arc};
use tracing::{debug, trace};

/// How many pooled blob sidecars to cache in memory.
pub const DEFAULT_MAX_CACHED_BLOBS: u32 = 100;

/// A cache size heuristic based on the highest blob params
///
/// This uses the max blobs per tx and max blobs per block over 16 epochs: `21 * 6 * 512 = 64512`
/// This should be ~4MB
const VERSIONED_HASH_TO_TX_HASH_CACHE_SIZE: u64 =
    BlobParams::bpo2().max_blobs_per_tx * BlobParams::bpo2().max_blob_count * EPOCH_SLOTS * 16;

/// A blob store that stores blob data on disk.
///
/// The type uses deferred deletion, meaning that blobs are not immediately deleted from disk, but
/// it's expected that the maintenance task will call [`BlobStore::cleanup`] to remove the deleted
/// blobs from disk.
#[derive(Clone, Debug)]
pub struct DiskFileBlobStore {
    inner: Arc<DiskFileBlobStoreInner>,
}

impl DiskFileBlobStore {
    /// Opens and initializes a new disk file blob store according to the given options.
    pub fn open(
        blob_dir: impl Into<PathBuf>,
        opts: DiskFileBlobStoreConfig,
    ) -> Result<Self, DiskFileBlobStoreError> {
        let blob_dir = blob_dir.into();
        let DiskFileBlobStoreConfig { max_cached_entries, .. } = opts;
        let inner = DiskFileBlobStoreInner::new(blob_dir, max_cached_entries);

        // initialize the blob store
        inner.delete_all()?;
        inner.create_blob_dir()?;

        Ok(Self { inner: Arc::new(inner) })
    }

    #[cfg(test)]
    fn is_cached(&self, tx: &B256) -> bool {
        self.inner.blob_cache.lock().get(tx).is_some()
    }

    #[cfg(test)]
    fn clear_cache(&self) {
        self.inner.blob_cache.lock().clear()
    }

    /// Look up EIP-7594 blobs by their versioned hashes.
    ///
    /// This returns a result vector with the **same length and order** as the input
    /// `versioned_hashes`. Each element is `Some(BlobAndProofV2)` if the blob is available, or
    /// `None` if it is missing or an older sidecar version.
    ///
    /// The lookup first scans the in-memory cache and, if not all blobs are found, falls back to
    /// reading candidate sidecars from disk using the `versioned_hash -> tx_hash` index.
    fn get_by_versioned_hashes_eip7594(
        &self,
        versioned_hashes: &[B256],
    ) -> Result<Vec<Option<BlobAndProofV2>>, BlobStoreError> {
        // we must return the blobs in order but we don't necessarily find them in the requested
        // order
        let mut result: Vec<Option<BlobAndProofV2>> = vec![None; versioned_hashes.len()];
        let mut missing_count = result.len();
        // first scan all cached full sidecars
        for (_tx_hash, data) in self.inner.blob_cache.lock().iter() {
            if let Some(blob_sidecar) =
                data.sidecar().and_then(BlobTransactionSidecarVariant::as_eip7594)
            {
                for (hash_idx, match_result) in
                    blob_sidecar.match_versioned_hashes(versioned_hashes)
                {
                    let slot = &mut result[hash_idx];
                    if slot.is_none() {
                        missing_count -= 1;
                    }
                    *slot = Some(match_result);
                }
            }

            // return early if all blobs are found.
            if missing_count == 0 {
                // since versioned_hashes may have duplicates, we double check here
                if result.iter().all(|blob| blob.is_some()) {
                    return Ok(result);
                }
            }
        }

        // not all versioned hashes were found, try to look up a matching tx
        let mut missing_tx_hashes = Vec::new();
        let mut seen_missing_tx_hashes = B256Set::default();

        {
            let mut versioned_to_txhashes = self.inner.versioned_hashes_to_txhash.lock();
            for (idx, _) in
                result.iter().enumerate().filter(|(_, blob_and_proof)| blob_and_proof.is_none())
            {
                // this is safe because the result vec has the same len
                let versioned_hash = versioned_hashes[idx];
                if let Some(tx_hash) = versioned_to_txhashes.get(&versioned_hash).copied() &&
                    seen_missing_tx_hashes.insert(tx_hash)
                {
                    missing_tx_hashes.push(tx_hash);
                }
            }
        }

        // if we have missing blobs, try to read them from disk and try again
        if !missing_tx_hashes.is_empty() {
            let blobs_from_disk = self.inner.read_many_decoded(missing_tx_hashes);
            for (_, data) in blobs_from_disk {
                if let Some(blob_sidecar) =
                    data.sidecar().and_then(BlobTransactionSidecarVariant::as_eip7594)
                {
                    for (hash_idx, match_result) in
                        blob_sidecar.match_versioned_hashes(versioned_hashes)
                    {
                        if result[hash_idx].is_none() {
                            result[hash_idx] = Some(match_result);
                        }
                    }
                }
            }
        }

        Ok(result)
    }

    /// Look up EIP-7594 blob cells by their versioned hashes.
    fn get_by_versioned_hashes_cells_eip7594(
        &self,
        versioned_hashes: &[B256],
        indices_bitarray: B128,
    ) -> Result<Vec<Option<BlobCellsAndProofsV1>>, BlobStoreError> {
        let cell_mask = BlobCellMask::new(indices_bitarray);
        let mut result: Vec<Option<BlobCellsAndProofsV1>> = vec![None; versioned_hashes.len()];
        let mut missing_count = result.len();

        let cached_blob_sidecars = self
            .inner
            .blob_cache
            .lock()
            .iter()
            .map(|(_, blob_sidecar)| Arc::clone(blob_sidecar))
            .collect::<Vec<_>>();
        for data in cached_blob_sidecars {
            if let Some(sparse) = data.sparse_sidecar() {
                for (hash_idx, match_result) in
                    sparse.match_versioned_hashes_cells(versioned_hashes, indices_bitarray)
                {
                    let slot = &mut result[hash_idx];
                    if slot.is_none() {
                        missing_count -= 1;
                    }
                    *slot = Some(match_result);
                }
            } else if let Some(blob_sidecar) =
                data.sidecar().and_then(BlobTransactionSidecarVariant::as_eip7594)
            {
                for (hash_idx, match_result) in blob_sidecar
                    .match_versioned_hashes_cells(versioned_hashes, cell_mask)
                    .map_err(|err| BlobStoreError::Other(Box::new(err)))?
                {
                    let slot = &mut result[hash_idx];
                    if slot.is_none() {
                        missing_count -= 1;
                    }
                    *slot = Some(match_result);
                }
            }

            if missing_count == 0 && result.iter().all(Option::is_some) {
                return Ok(result)
            }
        }

        let mut missing_tx_hashes = Vec::new();
        let mut seen_missing_tx_hashes = B256Set::default();
        {
            let mut versioned_to_txhashes = self.inner.versioned_hashes_to_txhash.lock();
            for (idx, _) in
                result.iter().enumerate().filter(|(_, cells_and_proofs)| cells_and_proofs.is_none())
            {
                let versioned_hash = versioned_hashes[idx];
                if let Some(tx_hash) = versioned_to_txhashes.get(&versioned_hash).copied() &&
                    seen_missing_tx_hashes.insert(tx_hash)
                {
                    missing_tx_hashes.push(tx_hash);
                }
            }
        }

        if !missing_tx_hashes.is_empty() {
            let blobs_from_disk = self.inner.read_many_decoded(missing_tx_hashes);
            for (_, data) in blobs_from_disk {
                if let Some(sparse) = data.sparse_sidecar() {
                    for (hash_idx, match_result) in
                        sparse.match_versioned_hashes_cells(versioned_hashes, indices_bitarray)
                    {
                        if result[hash_idx].is_none() {
                            result[hash_idx] = Some(match_result);
                        }
                    }
                } else if let Some(blob_sidecar) =
                    data.sidecar().and_then(BlobTransactionSidecarVariant::as_eip7594)
                {
                    for (hash_idx, match_result) in blob_sidecar
                        .match_versioned_hashes_cells(versioned_hashes, cell_mask)
                        .map_err(|err| BlobStoreError::Other(Box::new(err)))?
                    {
                        if result[hash_idx].is_none() {
                            result[hash_idx] = Some(match_result);
                        }
                    }
                }
            }
        }

        Ok(result)
    }
}

impl BlobStore for DiskFileBlobStore {
    fn insert(&self, tx: B256, data: PooledBlobSidecar) -> Result<(), BlobStoreError> {
        self.inner.insert_one(tx, data)
    }

    fn insert_all(&self, txs: Vec<(B256, PooledBlobSidecar)>) -> Result<(), BlobStoreError> {
        if txs.is_empty() {
            return Ok(())
        }
        self.inner.insert_many(txs)
    }

    fn delete(&self, tx: B256) -> Result<(), BlobStoreError> {
        if self.inner.contains(tx)? {
            self.inner.txs_to_delete.write().insert(tx);
        }
        Ok(())
    }

    fn delete_all(&self, txs: Vec<B256>) -> Result<(), BlobStoreError> {
        if txs.is_empty() {
            return Ok(())
        }
        let txs = self.inner.retain_existing(txs)?;
        self.inner.txs_to_delete.write().extend(txs);
        Ok(())
    }

    fn cleanup(&self) -> BlobStoreCleanupStat {
        let txs_to_delete = std::mem::take(&mut *self.inner.txs_to_delete.write());
        let mut stat = BlobStoreCleanupStat::default();
        let mut subsize = 0;
        debug!(target:"txpool::blob", num_blobs=%txs_to_delete.len(), "Removing blobs from disk");
        for tx in txs_to_delete {
            let path = self.inner.blob_disk_file(tx);
            let filesize = fs::metadata(&path).map_or(0, |meta| meta.len());
            match fs::remove_file(&path) {
                Ok(_) => {
                    stat.delete_succeed += 1;
                    subsize += filesize;
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    // Already deleted by a concurrent cleanup task
                    stat.delete_succeed += 1;
                }
                Err(e) => {
                    stat.delete_failed += 1;
                    let err = DiskFileBlobStoreError::DeleteFile(tx, path, e);
                    debug!(target:"txpool::blob", %err);
                }
            };
        }
        self.inner.size_tracker.sub_size(subsize as usize);
        self.inner.size_tracker.sub_len(stat.delete_succeed);
        stat
    }

    fn get(&self, tx: B256) -> Result<Option<Arc<BlobTransactionSidecarVariant>>, BlobStoreError> {
        Ok(self.inner.get_one(tx)?.and_then(|data| data.sidecar().cloned().map(Arc::new)))
    }

    fn contains(&self, tx: B256) -> Result<bool, BlobStoreError> {
        self.inner.contains(tx)
    }

    fn get_all(
        &self,
        txs: Vec<B256>,
    ) -> Result<Vec<(B256, Arc<BlobTransactionSidecarVariant>)>, BlobStoreError> {
        if txs.is_empty() {
            return Ok(Vec::new())
        }
        Ok(self
            .inner
            .get_all(txs)?
            .into_iter()
            .filter_map(|(tx, data)| data.sidecar().cloned().map(|data| (tx, Arc::new(data))))
            .collect())
    }

    fn get_exact(
        &self,
        txs: Vec<B256>,
    ) -> Result<Vec<Arc<BlobTransactionSidecarVariant>>, BlobStoreError> {
        if txs.is_empty() {
            return Ok(Vec::new())
        }
        self.inner
            .get_exact(txs)?
            .into_iter()
            .map(|(tx, data)| {
                data.sidecar().cloned().map(Arc::new).ok_or(BlobStoreError::IncompleteSidecar(tx))
            })
            .collect()
    }

    fn get_by_versioned_hashes_v1(
        &self,
        versioned_hashes: &[B256],
    ) -> Result<Vec<Option<BlobAndProofV1>>, BlobStoreError> {
        // the response must always be the same len as the request, misses must be None
        let mut result: Vec<Option<BlobAndProofV1>> = vec![None; versioned_hashes.len()];

        // first scan all cached full sidecars
        for (_tx_hash, data) in self.inner.blob_cache.lock().iter() {
            if let Some(blob_sidecar) =
                data.sidecar().and_then(BlobTransactionSidecarVariant::as_eip4844)
            {
                for (hash_idx, match_result) in
                    blob_sidecar.match_versioned_hashes(versioned_hashes)
                {
                    result[hash_idx] = Some(match_result);
                }
            }

            // return early if all blobs are found.
            if result.iter().all(|blob| blob.is_some()) {
                return Ok(result);
            }
        }

        // not all versioned hashes were be found, try to look up a matching tx

        let mut missing_tx_hashes = Vec::new();
        let mut seen_missing_tx_hashes = B256Set::default();

        {
            let mut versioned_to_txhashes = self.inner.versioned_hashes_to_txhash.lock();
            for (idx, _) in
                result.iter().enumerate().filter(|(_, blob_and_proof)| blob_and_proof.is_none())
            {
                // this is safe because the result vec has the same len
                let versioned_hash = versioned_hashes[idx];
                if let Some(tx_hash) = versioned_to_txhashes.get(&versioned_hash).copied() &&
                    seen_missing_tx_hashes.insert(tx_hash)
                {
                    missing_tx_hashes.push(tx_hash);
                }
            }
        }

        // if we have missing blobs, try to read them from disk and try again
        if !missing_tx_hashes.is_empty() {
            let blobs_from_disk = self.inner.read_many_decoded(missing_tx_hashes);
            for (_, data) in blobs_from_disk {
                if let Some(blob_sidecar) =
                    data.sidecar().and_then(BlobTransactionSidecarVariant::as_eip4844)
                {
                    for (hash_idx, match_result) in
                        blob_sidecar.match_versioned_hashes(versioned_hashes)
                    {
                        if result[hash_idx].is_none() {
                            result[hash_idx] = Some(match_result);
                        }
                    }
                }
            }
        }

        Ok(result)
    }

    fn get_by_versioned_hashes_v2(
        &self,
        versioned_hashes: &[B256],
    ) -> Result<Option<Vec<BlobAndProofV2>>, BlobStoreError> {
        let result = self.get_by_versioned_hashes_eip7594(versioned_hashes)?;

        // only return the blobs if we found all requested versioned hashes
        if result.iter().all(|blob| blob.is_some()) {
            Ok(Some(result.into_iter().map(Option::unwrap).collect()))
        } else {
            Ok(None)
        }
    }

    fn get_by_versioned_hashes_v3(
        &self,
        versioned_hashes: &[B256],
    ) -> Result<Vec<Option<BlobAndProofV2>>, BlobStoreError> {
        self.get_by_versioned_hashes_eip7594(versioned_hashes)
    }

    fn get_by_versioned_hashes_v4(
        &self,
        versioned_hashes: &[B256],
        indices_bitarray: B128,
    ) -> Result<Vec<Option<BlobCellsAndProofsV1>>, BlobStoreError> {
        self.get_by_versioned_hashes_cells_eip7594(versioned_hashes, indices_bitarray)
    }

    fn has_versioned_hashes(&self, versioned_hashes: &[B256]) -> Result<Vec<bool>, BlobStoreError> {
        let mut result = vec![false; versioned_hashes.len()];
        for (_tx_hash, data) in self.inner.blob_cache.lock().iter() {
            for available_hash in data.data().versioned_hashes() {
                for (idx, requested_hash) in versioned_hashes.iter().enumerate() {
                    if !result[idx] && *requested_hash == available_hash {
                        result[idx] = true;
                    }
                }
            }

            if result.iter().all(|available| *available) {
                return Ok(result)
            }
        }

        let mut missing_tx_hashes = Vec::new();
        {
            let mut versioned_to_txhashes = self.inner.versioned_hashes_to_txhash.lock();
            for (idx, requested_hash) in versioned_hashes.iter().enumerate() {
                if !result[idx] &&
                    let Some(tx_hash) = versioned_to_txhashes.get(requested_hash).copied()
                {
                    missing_tx_hashes.push((idx, tx_hash));
                }
            }
        }

        for (idx, tx_hash) in missing_tx_hashes {
            if self.inner.contains(tx_hash)? {
                result[idx] = true;
            }
        }

        Ok(result)
    }

    fn get_cells(
        &self,
        tx: B256,
        indices_bitarray: B128,
    ) -> Result<Option<Vec<Cell>>, BlobStoreError> {
        let Some(data) = self.inner.get_one(tx)? else {
            return Ok(None);
        };

        if let Some(sparse) = data.sparse_sidecar() {
            return Ok(sparse.cells_for_mask(indices_bitarray));
        }

        let Some(sidecar) = data.sidecar().and_then(BlobTransactionSidecarVariant::as_eip7594)
        else {
            return Ok(None);
        };

        sidecar
            .compute_matching_cells(BlobCellMask::new(indices_bitarray))
            .map(Some)
            .map_err(|err| BlobStoreError::Other(Box::new(err)))
    }

    fn data_size_hint(&self) -> Option<usize> {
        Some(self.inner.size_tracker.data_size())
    }

    fn get_availability(&self, tx_hash: B256) -> Result<Option<BlobCellMask>, BlobStoreError> {
        Ok(self.inner.get_one(tx_hash)?.map(|data| data.availability().get()))
    }

    fn blobs_len(&self) -> usize {
        self.inner.size_tracker.blobs_len()
    }
}

struct DiskFileBlobStoreInner {
    blob_dir: PathBuf,
    blob_cache: Mutex<LruMap<TxHash, Arc<PooledBlobSidecar>, ByLength>>,
    size_tracker: BlobStoreSize,
    file_lock: RwLock<()>,
    txs_to_delete: RwLock<B256Set>,
    /// Tracks of known versioned hashes and a transaction they exist in
    ///
    /// Note: It is possible that one blob can appear in multiple transactions but this only tracks
    /// the most recent one.
    versioned_hashes_to_txhash: Mutex<LruMap<B256, B256>>,
}

impl DiskFileBlobStoreInner {
    /// Creates a new empty disk file blob store with the given maximum length of the blob cache.
    fn new(blob_dir: PathBuf, max_length: u32) -> Self {
        Self {
            blob_dir,
            blob_cache: Mutex::new(LruMap::new(ByLength::new(max_length))),
            size_tracker: Default::default(),
            file_lock: Default::default(),
            txs_to_delete: Default::default(),
            versioned_hashes_to_txhash: Mutex::new(LruMap::new(ByLength::new(
                VERSIONED_HASH_TO_TX_HASH_CACHE_SIZE as u32,
            ))),
        }
    }

    /// Creates the directory where blobs will be stored on disk.
    fn create_blob_dir(&self) -> Result<(), DiskFileBlobStoreError> {
        debug!(target:"txpool::blob", blob_dir = ?self.blob_dir, "Creating blob store");
        fs::create_dir_all(&self.blob_dir)
            .map_err(|e| DiskFileBlobStoreError::Open(self.blob_dir.clone(), e))
    }

    /// Deletes the entire blob store.
    fn delete_all(&self) -> Result<(), DiskFileBlobStoreError> {
        match fs::remove_dir_all(&self.blob_dir) {
            Ok(_) => {
                debug!(target:"txpool::blob", blob_dir = ?self.blob_dir, "Removed blob store directory");
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => return Err(DiskFileBlobStoreError::Open(self.blob_dir.clone(), err)),
        }
        Ok(())
    }

    /// Ensures a pooled sidecar is in the cache and written to disk.
    fn insert_one(&self, tx: B256, mut data: PooledBlobSidecar) -> Result<(), BlobStoreError> {
        // Multiple network responses may carry disjoint cell subsets for the same transaction.
        // Merge those subsets before replacing the on-disk entry so a cache eviction cannot lose
        // cells that were received earlier.
        let existing = self.get_one(tx)?;
        let replace = match existing {
            Some(existing) if existing.is_materialized() && data.is_sparse() => {
                data = (*existing).clone();
                false
            }
            Some(existing) if existing.is_sparse() && data.is_sparse() => {
                let mut merged = (*existing).clone();
                merged.merge_sparse(data.sparse_sidecar().expect("sparse sidecar"))?;
                data = merged;
                true
            }
            Some(_) => true,
            None => true,
        };

        {
            let mut map = self.versioned_hashes_to_txhash.lock();
            for hash in data.data().versioned_hashes() {
                map.insert(hash, tx);
            }
        }

        let encoded = encode_pooled(tx, &data)?;
        let (old_size, new_size) = self.write_one_encoded(tx, &encoded, replace)?;
        self.blob_cache.lock().insert(tx, Arc::new(data));
        self.size_tracker.sub_size(old_size);
        self.size_tracker.add_size(new_size);
        if old_size == 0 && new_size != 0 {
            self.size_tracker.inc_len(1);
        }
        Ok(())
    }

    /// Ensures pooled sidecars are in the cache and written to disk.
    fn insert_many(&self, txs: Vec<(B256, PooledBlobSidecar)>) -> Result<(), BlobStoreError> {
        // Keep the prototype's merge semantics identical for single and batched inserts. This can
        // be optimized into one locked write pass once the sparse wire format is finalized.
        for (tx, data) in txs {
            self.insert_one(tx, data)?;
        }
        Ok(())
    }

    /// Returns true if the blob for the given transaction hash is in the blob cache or on disk.
    fn contains(&self, tx: B256) -> Result<bool, BlobStoreError> {
        if self.blob_cache.lock().get(&tx).is_some() {
            return Ok(true)
        }
        // we only check if the file exists and assume it's valid
        Ok(self.blob_disk_file(tx).is_file())
    }

    /// Returns all the blob transactions which are in the cache or on the disk.
    fn retain_existing(&self, txs: Vec<B256>) -> Result<Vec<B256>, BlobStoreError> {
        let (in_cache, not_in_cache): (Vec<B256>, Vec<B256>) = {
            let mut cache = self.blob_cache.lock();
            txs.into_iter().partition(|tx| cache.get(tx).is_some())
        };

        let mut existing = in_cache;
        for tx in not_in_cache {
            if self.blob_disk_file(tx).is_file() {
                existing.push(tx);
            }
        }

        Ok(existing)
    }

    /// Retrieves the blob for the given transaction hash from the blob cache or disk.
    fn get_one(&self, tx: B256) -> Result<Option<Arc<PooledBlobSidecar>>, BlobStoreError> {
        if let Some(data) = self.blob_cache.lock().get(&tx) {
            return Ok(Some(data.clone()))
        }

        if let Some(data) = self.read_one(tx)? {
            let data = Arc::new(data);
            self.blob_cache.lock().insert(tx, data.clone());
            return Ok(Some(data))
        }

        Ok(None)
    }

    /// Returns the path to the blob file for the given transaction hash.
    #[inline]
    fn blob_disk_file(&self, tx: B256) -> PathBuf {
        self.blob_dir.join(format!("{tx:x}"))
    }

    /// Retrieves the blob data for the given transaction hash.
    #[inline]
    fn read_one(&self, tx: B256) -> Result<Option<PooledBlobSidecar>, BlobStoreError> {
        let path = self.blob_disk_file(tx);
        let data = {
            let _lock = self.file_lock.read();
            match fs::read(&path) {
                Ok(data) => data,
                Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
                Err(e) => {
                    return Err(BlobStoreError::Other(Box::new(DiskFileBlobStoreError::ReadFile(
                        tx, path, e,
                    ))))
                }
            }
        };
        decode_pooled(tx, &data).map(Some)
    }

    /// Returns decoded pooled sidecars read from disk.
    ///
    /// Only returns sidecars that were found and successfully decoded.
    fn read_many_decoded(&self, txs: Vec<TxHash>) -> Vec<(TxHash, PooledBlobSidecar)> {
        self.read_many_raw(txs)
            .into_iter()
            .filter_map(|(tx, data)| decode_pooled(tx, &data).ok().map(|data| (tx, data)))
            .collect()
    }

    /// Retrieves the raw blob data for the given transaction hashes.
    ///
    /// Only returns the blobs that were found in file.
    #[inline]
    fn read_many_raw(&self, txs: Vec<TxHash>) -> Vec<(TxHash, Vec<u8>)> {
        let mut res = Vec::with_capacity(txs.len());
        let _lock = self.file_lock.read();
        for tx in txs {
            let path = self.blob_disk_file(tx);
            match fs::read(&path) {
                Ok(data) => {
                    res.push((tx, data));
                }
                Err(err) => {
                    debug!(target:"txpool::blob", %err, ?tx, "Failed to read blob file");
                }
            };
        }
        res
    }

    /// Writes the blob data for the given transaction hash to the disk.
    #[inline]
    fn write_one_encoded(
        &self,
        tx: B256,
        data: &[u8],
        replace: bool,
    ) -> Result<(usize, usize), DiskFileBlobStoreError> {
        trace!(target:"txpool::blob", "[{:?}] writing blob file", tx);
        let path = self.blob_disk_file(tx);
        let _lock = self.file_lock.write();
        let old_size = fs::metadata(&path).map_or(0, |metadata| metadata.len() as usize);
        if old_size != 0 && !replace {
            return Ok((0, 0))
        }
        fs::write(&path, data).map_err(|e| DiskFileBlobStoreError::WriteFile(tx, path, e))?;
        Ok((old_size, data.len()))
    }

    /// Retrieves pooled sidecars for the given transaction hashes from the cache or disk.
    ///
    /// This will not return an error if there are missing blobs. Therefore, the result may be a
    /// subset of the request or an empty vector if none of the blobs were found.
    #[inline]
    fn get_all(
        &self,
        txs: Vec<B256>,
    ) -> Result<Vec<(B256, Arc<PooledBlobSidecar>)>, BlobStoreError> {
        let mut res = Vec::with_capacity(txs.len());
        let mut cache_miss = Vec::new();
        {
            let mut cache = self.blob_cache.lock();
            for tx in txs {
                if let Some(data) = cache.get(&tx) {
                    res.push((tx, data.clone()));
                } else {
                    cache_miss.push(tx)
                }
            }
        }
        if cache_miss.is_empty() {
            return Ok(res)
        }
        let from_disk = self.read_many_decoded(cache_miss);
        if from_disk.is_empty() {
            return Ok(res)
        }
        let from_disk = from_disk
            .into_iter()
            .map(|(tx, data)| {
                let data = Arc::new(data);
                res.push((tx, data.clone()));
                (tx, data)
            })
            .collect::<Vec<_>>();

        let mut cache = self.blob_cache.lock();
        for (tx, data) in from_disk {
            cache.insert(tx, data);
        }

        Ok(res)
    }

    /// Retrieves blobs for the given transaction hashes from the blob cache or disk.
    ///
    /// Returns an error if there are any missing blobs.
    #[inline]
    fn get_exact(
        &self,
        txs: Vec<B256>,
    ) -> Result<Vec<(B256, Arc<PooledBlobSidecar>)>, BlobStoreError> {
        txs.into_iter()
            .map(|tx| {
                self.get_one(tx).map(|data| (tx, data)).and_then(|(tx, data)| {
                    data.map(|data| (tx, data)).ok_or(BlobStoreError::MissingSidecar(tx))
                })
            })
            .collect()
    }
}

/// Prefix used for sparse sidecar files. Complete sidecars retain the existing raw Alloy-RLP
/// encoding, while this tag lets the reader distinguish sparse metadata/cell files.
const SPARSE_MAGIC: [u8; 4] = *b"RSP2";

fn encode_pooled(tx: B256, data: &PooledBlobSidecar) -> Result<Vec<u8>, BlobStoreError> {
    match data.data() {
        PooledBlobSidecarData::Complete(sidecar) => {
            let mut buf = Vec::with_capacity(sidecar.rlp_encoded_fields_length());
            sidecar.rlp_encode_fields(&mut buf);
            Ok(buf)
        }
        PooledBlobSidecarData::Sparse(sidecar) => {
            encode_sparse(tx, sidecar).map_err(BlobStoreError::from)
        }
    }
}

fn encode_sparse(tx: B256, sidecar: &SparseBlobSidecar) -> Result<Vec<u8>, DiskFileBlobStoreError> {
    let mut encoded = Vec::new();
    encoded.extend_from_slice(&SPARSE_MAGIC);
    push_sparse_len(&mut encoded, tx, "commitments", sidecar.commitments.len())?;
    push_sparse_len(&mut encoded, tx, "cell proofs", sidecar.cell_proofs.len())?;
    push_sparse_len(&mut encoded, tx, "cells", sidecar.cells.len())?;
    encoded.extend_from_slice(&u128::from(sidecar.custody).to_le_bytes());
    for commitment in &sidecar.commitments {
        encoded.extend_from_slice(commitment.as_slice());
    }
    for proof in &sidecar.cell_proofs {
        encoded.extend_from_slice(proof.as_slice());
    }
    for cell in &sidecar.cells {
        encoded.extend_from_slice(cell.as_ref());
    }
    Ok(encoded)
}

fn push_sparse_len(
    encoded: &mut Vec<u8>,
    tx: B256,
    name: &'static str,
    len: usize,
) -> Result<(), DiskFileBlobStoreError> {
    let len = u32::try_from(len)
        .map_err(|_| DiskFileBlobStoreError::EncodeSparse(tx, format!("{name} are too long")))?;
    encoded.extend_from_slice(&len.to_le_bytes());
    Ok(())
}

fn decode_pooled(tx: B256, encoded: &[u8]) -> Result<PooledBlobSidecar, BlobStoreError> {
    if !encoded.starts_with(&SPARSE_MAGIC) {
        return BlobTransactionSidecarVariant::rlp_decode_fields(&mut encoded.as_ref())
            .map(PooledBlobSidecar::from)
            .map_err(BlobStoreError::DecodeError);
    }

    let mut offset = SPARSE_MAGIC.len();
    let commitments_len = read_sparse_len(tx, encoded, &mut offset, "commitments")?;
    let proofs_len = read_sparse_len(tx, encoded, &mut offset, "cell proofs")?;
    let cells_len = read_sparse_len(tx, encoded, &mut offset, "cells")?;
    let custody = B128::from(u128::from_le_bytes(
        take_sparse_bytes(tx, encoded, &mut offset, 16, "custody")?
            .try_into()
            .expect("sparse custody is sixteen bytes"),
    ));

    let mut commitments = Vec::with_capacity(commitments_len);
    for _ in 0..commitments_len {
        let bytes = take_sparse_bytes(tx, encoded, &mut offset, 48, "commitment")?;
        let commitment = Bytes48::try_from(bytes)
            .map_err(|_| sparse_decode_error(tx, "invalid commitment length"))?;
        commitments.push(commitment);
    }

    let mut cell_proofs = Vec::with_capacity(proofs_len);
    for _ in 0..proofs_len {
        let bytes = take_sparse_bytes(tx, encoded, &mut offset, 48, "cell proof")?;
        let proof = Bytes48::try_from(bytes)
            .map_err(|_| sparse_decode_error(tx, "invalid cell proof length"))?;
        cell_proofs.push(proof);
    }

    let mut cells = Vec::with_capacity(cells_len);
    for _ in 0..cells_len {
        let bytes = take_sparse_bytes(tx, encoded, &mut offset, BYTES_PER_CELL, "cell")?;
        let cell =
            Cell::try_from(bytes).map_err(|_| sparse_decode_error(tx, "invalid cell length"))?;
        cells.push(cell);
    }

    if offset != encoded.len() {
        return Err(sparse_decode_error(tx, "trailing bytes"));
    }

    SparseBlobSidecar::try_new(commitments, cell_proofs, cells, custody)
        .map(PooledBlobSidecar::from_sparse)
        .map_err(|err| sparse_decode_error(tx, err.to_string()))
}

fn read_sparse_len(
    tx: B256,
    encoded: &[u8],
    offset: &mut usize,
    name: &'static str,
) -> Result<usize, BlobStoreError> {
    let bytes = take_sparse_bytes(tx, encoded, offset, 4, name)?;
    Ok(u32::from_le_bytes(bytes.try_into().expect("sparse length is four bytes")) as usize)
}

fn take_sparse_bytes<'a>(
    tx: B256,
    encoded: &'a [u8],
    offset: &mut usize,
    len: usize,
    name: &'static str,
) -> Result<&'a [u8], BlobStoreError> {
    let end = offset
        .checked_add(len)
        .ok_or_else(|| sparse_decode_error(tx, format!("{name} offset overflow")))?;
    let bytes = encoded
        .get(*offset..end)
        .ok_or_else(|| sparse_decode_error(tx, format!("truncated {name}")))?;
    *offset = end;
    Ok(bytes)
}

fn sparse_decode_error(tx: B256, reason: impl Into<String>) -> BlobStoreError {
    BlobStoreError::Other(Box::new(DiskFileBlobStoreError::DecodeSparse(tx, reason.into())))
}

impl fmt::Debug for DiskFileBlobStoreInner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DiskFileBlobStoreInner")
            .field("blob_dir", &self.blob_dir)
            .field("cached_blobs", &self.blob_cache.try_lock().map(|lock| lock.len()))
            .field("txs_to_delete", &self.txs_to_delete.try_read())
            .finish()
    }
}

/// Errors that can occur when interacting with a disk file blob store.
#[derive(Debug, thiserror::Error)]
pub enum DiskFileBlobStoreError {
    /// Thrown during [`DiskFileBlobStore::open`] if the blob store directory cannot be opened.
    #[error("failed to open blobstore at {0}: {1}")]
    /// Indicates a failure to open the blob store directory.
    Open(PathBuf, io::Error),
    /// Failure while reading a blob file.
    #[error("[{0}] failed to read blob file at {1}: {2}")]
    /// Indicates a failure while reading a blob file.
    ReadFile(TxHash, PathBuf, io::Error),
    /// Failure while writing a blob file.
    #[error("[{0}] failed to write blob file at {1}: {2}")]
    /// Indicates a failure while writing a blob file.
    WriteFile(TxHash, PathBuf, io::Error),
    /// Failure while deleting a blob file.
    #[error("[{0}] failed to delete blob file at {1}: {2}")]
    /// Indicates a failure while deleting a blob file.
    DeleteFile(TxHash, PathBuf, io::Error),
    /// Failure while encoding a sparse sidecar.
    #[error("[{0}] failed to encode sparse blob data: {1}")]
    EncodeSparse(TxHash, String),
    /// Failure while decoding a sparse sidecar.
    #[error("[{0}] failed to decode sparse blob data: {1}")]
    DecodeSparse(TxHash, String),
}

impl From<DiskFileBlobStoreError> for BlobStoreError {
    fn from(value: DiskFileBlobStoreError) -> Self {
        Self::Other(Box::new(value))
    }
}

/// Configuration for a disk file blob store.
#[derive(Debug, Clone)]
pub struct DiskFileBlobStoreConfig {
    /// The maximum number of blobs to keep in the in memory blob cache.
    pub max_cached_entries: u32,
    /// How to open the blob store.
    pub open: OpenDiskFileBlobStore,
}

impl Default for DiskFileBlobStoreConfig {
    fn default() -> Self {
        Self { max_cached_entries: DEFAULT_MAX_CACHED_BLOBS, open: Default::default() }
    }
}

impl DiskFileBlobStoreConfig {
    /// Set maximum number of blobs to keep in the in memory blob cache.
    pub const fn with_max_cached_entries(mut self, max_cached_entries: u32) -> Self {
        self.max_cached_entries = max_cached_entries;
        self
    }
}

/// How to open a disk file blob store.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OpenDiskFileBlobStore {
    /// Clear everything in the blob store.
    #[default]
    Clear,
    /// Keep the existing blob store and index
    ReIndex,
}

#[cfg(test)]
mod tests {
    use alloy_consensus::BlobTransactionSidecar;
    use alloy_eips::{
        eip4844::{kzg_to_versioned_hash, Blob, BlobAndProofV2, Bytes48},
        eip7594::{
            BlobTransactionSidecarEip7594, BlobTransactionSidecarVariant, CELLS_PER_EXT_BLOB,
        },
    };

    use super::*;
    use std::sync::atomic::Ordering;

    fn tmp_store() -> (DiskFileBlobStore, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let store = DiskFileBlobStore::open(dir.path(), Default::default()).unwrap();
        (store, dir)
    }

    fn rng_blobs(num: usize) -> Vec<(TxHash, BlobTransactionSidecarVariant)> {
        let mut rng = rand::rng();
        (0..num)
            .map(|_| {
                let tx = TxHash::random_with(&mut rng);
                let blob = BlobTransactionSidecarVariant::Eip4844(BlobTransactionSidecar {
                    blobs: vec![],
                    commitments: vec![],
                    proofs: vec![],
                });
                (tx, blob)
            })
            .collect()
    }

    fn wrapped_blobs(
        blobs: Vec<(TxHash, BlobTransactionSidecarVariant)>,
    ) -> Vec<(TxHash, PooledBlobSidecar)> {
        blobs.into_iter().map(|(tx, blob)| (tx, blob.into())).collect()
    }

    fn eip7594_single_blob_sidecar() -> (BlobTransactionSidecarVariant, B256, BlobAndProofV2) {
        let blob = Blob::default();
        let commitment = Bytes48::default();
        let cell_proofs = vec![Bytes48::default(); CELLS_PER_EXT_BLOB];

        let versioned_hash = kzg_to_versioned_hash(commitment.as_slice());

        let expected =
            BlobAndProofV2 { blob: Box::new(Blob::default()), proofs: cell_proofs.clone() };
        let sidecar = BlobTransactionSidecarEip7594::new(vec![blob], vec![commitment], cell_proofs);

        (BlobTransactionSidecarVariant::Eip7594(sidecar), versioned_hash, expected)
    }

    #[test]
    fn disk_insert_all_get_all() {
        let (store, _dir) = tmp_store();

        let blobs = rng_blobs(10);
        let all_hashes = blobs.iter().map(|(tx, _)| *tx).collect::<Vec<_>>();
        store.insert_all(wrapped_blobs(blobs.clone())).unwrap();

        // all cached
        for (tx, blob) in &blobs {
            assert!(store.is_cached(tx));
            let b = store.get(*tx).unwrap().map(Arc::unwrap_or_clone).unwrap();
            assert_eq!(b, *blob);
        }

        let all = store.get_all(all_hashes.clone()).unwrap();
        for (tx, blob) in all {
            assert!(blobs.contains(&(tx, Arc::unwrap_or_clone(blob))), "missing blob {tx:?}");
        }

        assert!(store.contains(all_hashes[0]).unwrap());
        store.delete_all(all_hashes.clone()).unwrap();
        assert!(store.inner.txs_to_delete.read().contains(&all_hashes[0]));
        store.clear_cache();
        store.cleanup();

        assert!(store.get(blobs[0].0).unwrap().is_none());

        let all = store.get_all(all_hashes.clone()).unwrap();
        assert!(all.is_empty());

        assert!(!store.contains(all_hashes[0]).unwrap());
        assert!(store.get_exact(all_hashes).is_err());

        assert_eq!(store.data_size_hint(), Some(0));
        assert_eq!(store.inner.size_tracker.num_blobs.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn disk_insert_and_retrieve() {
        let (store, _dir) = tmp_store();

        let (tx, blob) = rng_blobs(1).into_iter().next().unwrap();
        store.insert(tx, blob.clone().into()).unwrap();

        assert!(store.is_cached(&tx));
        let retrieved_blob = store.get(tx).unwrap().map(Arc::unwrap_or_clone).unwrap();
        assert_eq!(retrieved_blob, blob);
    }

    #[test]
    fn disk_delete_blob() {
        let (store, _dir) = tmp_store();

        let (tx, blob) = rng_blobs(1).into_iter().next().unwrap();
        store.insert(tx, blob.into()).unwrap();
        assert!(store.is_cached(&tx));

        store.delete(tx).unwrap();
        assert!(store.inner.txs_to_delete.read().contains(&tx));
        store.cleanup();

        let result = store.get(tx).unwrap();
        assert_eq!(
            result,
            Some(Arc::new(BlobTransactionSidecarVariant::Eip4844(BlobTransactionSidecar {
                blobs: vec![],
                commitments: vec![],
                proofs: vec![]
            })))
        );
    }

    #[test]
    fn disk_insert_all_and_delete_all() {
        let (store, _dir) = tmp_store();

        let blobs = rng_blobs(5);
        let txs = blobs.iter().map(|(tx, _)| *tx).collect::<Vec<_>>();
        store.insert_all(wrapped_blobs(blobs.clone())).unwrap();

        for (tx, _) in &blobs {
            assert!(store.is_cached(tx));
        }

        store.delete_all(txs.clone()).unwrap();
        store.cleanup();

        for tx in txs {
            let result = store.get(tx).unwrap();
            assert_eq!(
                result,
                Some(Arc::new(BlobTransactionSidecarVariant::Eip4844(BlobTransactionSidecar {
                    blobs: vec![],
                    commitments: vec![],
                    proofs: vec![]
                })))
            );
        }
    }

    #[test]
    fn disk_get_all_blobs() {
        let (store, _dir) = tmp_store();

        let blobs = rng_blobs(3);
        let txs = blobs.iter().map(|(tx, _)| *tx).collect::<Vec<_>>();
        store.insert_all(wrapped_blobs(blobs.clone())).unwrap();

        let retrieved_blobs = store.get_all(txs.clone()).unwrap();
        for (tx, blob) in retrieved_blobs {
            assert!(blobs.contains(&(tx, Arc::unwrap_or_clone(blob))));
        }

        store.delete_all(txs).unwrap();
        store.cleanup();
    }

    #[test]
    fn disk_get_exact_blobs_success() {
        let (store, _dir) = tmp_store();

        let blobs = rng_blobs(3);
        let txs = blobs.iter().map(|(tx, _)| *tx).collect::<Vec<_>>();
        store.insert_all(wrapped_blobs(blobs.clone())).unwrap();

        let retrieved_blobs = store.get_exact(txs).unwrap();
        for (retrieved_blob, (_, original_blob)) in retrieved_blobs.into_iter().zip(blobs) {
            assert_eq!(Arc::unwrap_or_clone(retrieved_blob), original_blob);
        }
    }

    #[test]
    fn disk_get_exact_blobs_failure() {
        let (store, _dir) = tmp_store();

        let blobs = rng_blobs(2);
        let txs = blobs.iter().map(|(tx, _)| *tx).collect::<Vec<_>>();
        store.insert_all(wrapped_blobs(blobs)).unwrap();

        // Try to get a blob that was never inserted
        let missing_tx = TxHash::random();
        let result = store.get_exact(vec![txs[0], missing_tx]);
        assert!(result.is_err());
    }

    #[test]
    fn disk_data_size_hint() {
        let (store, _dir) = tmp_store();
        assert_eq!(store.data_size_hint(), Some(0));

        let blobs = rng_blobs(2);
        store.insert_all(wrapped_blobs(blobs)).unwrap();
        assert!(store.data_size_hint().unwrap() > 0);
    }

    #[test]
    fn disk_cleanup_stat() {
        let (store, _dir) = tmp_store();

        let blobs = rng_blobs(3);
        let txs = blobs.iter().map(|(tx, _)| *tx).collect::<Vec<_>>();
        store.insert_all(wrapped_blobs(blobs)).unwrap();

        store.delete_all(txs).unwrap();
        let stat = store.cleanup();
        assert_eq!(stat.delete_succeed, 3);
        assert_eq!(stat.delete_failed, 0);
    }

    #[test]
    fn disk_get_blobs_v3_returns_partial_results() {
        let (store, _dir) = tmp_store();

        let (sidecar, versioned_hash, expected) = eip7594_single_blob_sidecar();
        store.insert(TxHash::random(), sidecar.into()).unwrap();

        assert_ne!(versioned_hash, B256::ZERO);

        let request = vec![versioned_hash, B256::ZERO];
        let v2 = store.get_by_versioned_hashes_v2(&request).unwrap();
        assert!(v2.is_none(), "v2 must return null if any requested blob is missing");

        let v3 = store.get_by_versioned_hashes_v3(&request).unwrap();
        assert_eq!(v3, vec![Some(expected), None]);
    }

    #[test]
    fn disk_has_blobs_returns_ordered_availability() {
        let (store, _dir) = tmp_store();

        let (sidecar, versioned_hash, _) = eip7594_single_blob_sidecar();
        store.insert(TxHash::random(), sidecar.into()).unwrap();

        let request = vec![B256::ZERO, versioned_hash, versioned_hash];
        assert_eq!(store.has_versioned_hashes(&request).unwrap(), vec![false, true, true]);
    }

    #[test]
    fn disk_get_blobs_v4_returns_requested_cells() {
        let (store, _dir) = tmp_store();

        let (sidecar, versioned_hash, _) = eip7594_single_blob_sidecar();
        store.insert(TxHash::random(), sidecar.into()).unwrap();

        let indices_bitarray = B128::from((1u128 << 0) | (1u128 << 7));
        let request = vec![versioned_hash, B256::ZERO];

        let v4 = store.get_by_versioned_hashes_v4(&request, indices_bitarray).unwrap();
        assert_eq!(v4.len(), request.len());
        assert!(v4[1].is_none());

        let cells_and_proofs = v4[0].as_ref().unwrap();
        assert_eq!(cells_and_proofs.blob_cells.len(), 2);
        assert_eq!(cells_and_proofs.proofs.len(), 2);
        assert!(cells_and_proofs.blob_cells.iter().all(Option::is_some));
        assert_eq!(cells_and_proofs.proofs, vec![Some(Bytes48::default()); 2]);
    }

    #[test]
    fn disk_get_blobs_v3_can_fallback_to_disk() {
        let (store, _dir) = tmp_store();

        let (sidecar, versioned_hash, expected) = eip7594_single_blob_sidecar();
        store.insert(TxHash::random(), sidecar.into()).unwrap();
        store.clear_cache();

        let v3 = store.get_by_versioned_hashes_v3(&[versioned_hash]).unwrap();
        assert_eq!(v3, vec![Some(expected)]);
    }

    #[test]
    fn disk_has_blobs_can_fallback_to_disk() {
        let (store, _dir) = tmp_store();

        let (sidecar, versioned_hash, _) = eip7594_single_blob_sidecar();
        store.insert(TxHash::random(), sidecar.into()).unwrap();
        store.clear_cache();

        assert_eq!(store.has_versioned_hashes(&[versioned_hash]).unwrap(), vec![true]);
    }

    #[test]
    fn disk_has_blobs_ignores_stale_index_entries() {
        let (store, _dir) = tmp_store();

        let tx_hash = TxHash::random();
        let (sidecar, versioned_hash, _) = eip7594_single_blob_sidecar();
        store.insert(tx_hash, sidecar.into()).unwrap();
        store.clear_cache();

        store.delete(tx_hash).unwrap();
        store.cleanup();

        assert_eq!(store.has_versioned_hashes(&[versioned_hash]).unwrap(), vec![false]);
    }

    #[test]
    fn disk_get_blobs_v4_can_fallback_to_disk() {
        let (store, _dir) = tmp_store();

        let (sidecar, versioned_hash, _) = eip7594_single_blob_sidecar();
        store.insert(TxHash::random(), sidecar.into()).unwrap();
        store.clear_cache();

        let v4 = store.get_by_versioned_hashes_v4(&[versioned_hash], B128::from(1u128)).unwrap();
        let cells_and_proofs = v4[0].as_ref().unwrap();
        assert_eq!(cells_and_proofs.blob_cells.len(), 1);
        assert_eq!(cells_and_proofs.proofs, vec![Some(Bytes48::default())]);
    }

    #[test]
    fn disk_get_cells_can_fallback_to_disk() {
        let (store, _dir) = tmp_store();

        let tx_hash = TxHash::random();
        let (sidecar, versioned_hash, _) = eip7594_single_blob_sidecar();
        store.insert(tx_hash, sidecar.into()).unwrap();

        let indices_bitarray = B128::from((1u128 << 0) | (1u128 << 7));
        let expected = store
            .get_by_versioned_hashes_v4(&[versioned_hash], indices_bitarray)
            .unwrap()
            .pop()
            .unwrap()
            .unwrap()
            .blob_cells
            .into_iter()
            .collect::<Option<Vec<_>>>()
            .unwrap();

        store.clear_cache();

        assert_eq!(store.get_cells(tx_hash, indices_bitarray).unwrap(), Some(expected));
    }

    #[test]
    fn disk_double_cleanup_no_failure() {
        let (store, _dir) = tmp_store();

        let blobs = rng_blobs(5);
        let all_hashes: Vec<_> = blobs.iter().map(|(tx, _)| *tx).collect();
        store.insert_all(wrapped_blobs(blobs)).unwrap();
        store.clear_cache();

        // Schedule blobs for deletion
        store.delete_all(all_hashes.clone()).unwrap();

        // First cleanup: files exist, all should succeed
        let stat1 = store.cleanup();
        assert_eq!(stat1.delete_succeed, 5);
        assert_eq!(stat1.delete_failed, 0);

        // Manually re-enqueue the same hashes to simulate a concurrent cleanup race
        store.inner.txs_to_delete.write().extend(all_hashes);

        // Second cleanup: files already deleted, should still report success (NotFound)
        let stat2 = store.cleanup();
        assert_eq!(stat2.delete_succeed, 5);
        assert_eq!(stat2.delete_failed, 0);
    }
}
