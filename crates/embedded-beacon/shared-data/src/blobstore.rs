//! Execution-pool facade sharing exact blob, commitment and proof bytes with consensus owners.

use crate::{Encoding, ObjectId, ObjectKind, OwnedObjectBatch, Owner, SharedDataStore, StoreError};
use alloy_eips::{
    eip4844::{
        kzg_to_versioned_hash, Blob, BlobAndProofV1, BlobAndProofV2, BlobCellsAndProofsV1,
        BlobTransactionSidecar, Bytes48,
    },
    eip7594::{
        BlobCellMask, BlobTransactionSidecarEip7594, BlobTransactionSidecarVariant, Cell,
        CELLS_PER_EXT_BLOB,
    },
};
use alloy_primitives::{TxHash, B256};
use parking_lot::Mutex;
use reth_transaction_pool::blobstore::{
    BlobStore, BlobStoreCleanupStat, BlobStoreError, PooledBlobSidecar,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Weak},
};

/// `BlobStore` facade whose pool deletion releases only the transaction's independent owner pins.
#[derive(Debug, Clone)]
pub struct SharedBlobStore {
    shared: SharedDataStore,
    gate: Arc<Mutex<BlobCache>>,
}

impl SharedBlobStore {
    /// Creates a facade around an already opened, isolated shared-data store.
    pub fn new(shared: SharedDataStore) -> Self {
        Self { shared, gate: Arc::new(Mutex::new(BlobCache::default())) }
    }

    /// Returns the underlying byte store for consensus pinning and physical statistics.
    pub const fn shared(&self) -> &SharedDataStore {
        &self.shared
    }

    fn durable_transaction_hashes(&self) -> Vec<B256> {
        self.shared
            .owners()
            .into_iter()
            .filter_map(|owner| {
                if let Owner::Pool(tx) = owner &&
                    self.shared.owner_objects(owner).iter().any(|id| {
                        id.kind == ObjectKind::BlobSidecar && id.encoding == Encoding::Json
                    })
                {
                    Some(B256::from(tx))
                } else {
                    None
                }
            })
            .collect()
    }

    fn read_sidecar(
        &self,
        tx: B256,
    ) -> Result<Option<Arc<BlobTransactionSidecarVariant>>, BlobStoreError> {
        self.shared.ensure_healthy().map_err(blob_error)?;
        let mut cache = self.gate.lock();
        self.refresh_index(&mut cache)?;
        let owner = Owner::Pool(tx.0);
        let ids = self.shared.owner_objects(owner);
        let Some(id) = ids
            .into_iter()
            .find(|id| id.kind == ObjectKind::BlobSidecar && id.encoding == Encoding::Json)
        else {
            cache.decoded.remove(&tx);
            return Ok(None);
        };
        if let Some(decoded) = cache.decoded.get(&tx).and_then(Weak::upgrade) {
            return Ok(Some(decoded));
        }
        let bytes = self
            .shared
            .get(id)
            .map_err(blob_error)?
            .ok_or_else(|| blob_error(StoreError::MissingObject(id)))?;
        let descriptor = serde_json::from_slice::<Descriptor>(&bytes).map_err(blob_error)?;
        descriptor.validate().map_err(blob_error)?;
        // Allocate the complete vector first; returning 128 KiB FixedBytes values through
        // iterator adapters creates excessive debug stack copies on native Windows threads.
        let mut blobs = vec![Blob::ZERO; descriptor.blobs.len()];
        for (blob, id) in blobs.iter_mut().zip(&descriptor.blobs) {
            let bytes = self.read_bytes(*id)?;
            let source = <&Blob>::try_from(bytes.as_ref()).map_err(blob_error)?;
            blob.as_mut_slice().copy_from_slice(source.as_slice());
        }
        let commitments = descriptor
            .commitments
            .iter()
            .map(|id| {
                let bytes = self.read_bytes(*id)?;
                Bytes48::try_from(bytes.as_ref()).map_err(blob_error)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let proofs = descriptor
            .proofs
            .iter()
            .map(|id| {
                let bytes = self.read_bytes(*id)?;
                Bytes48::try_from(bytes.as_ref()).map_err(blob_error)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let sidecar = Arc::new(match descriptor.flavor {
            Flavor::Eip4844 => BlobTransactionSidecarVariant::Eip4844(BlobTransactionSidecar {
                blobs,
                commitments,
                proofs,
            }),
            Flavor::Eip7594 => BlobTransactionSidecarVariant::Eip7594(
                BlobTransactionSidecarEip7594::new(blobs, commitments, proofs),
            ),
        });
        if logical_size(&sidecar) != descriptor.logical_size {
            return Err(blob_error(FacadeError::MalformedSidecar));
        }
        cache.decoded.insert(tx, Arc::downgrade(&sidecar));
        Ok(Some(sidecar))
    }

    fn read_bytes(&self, id: ObjectId) -> Result<Arc<[u8]>, BlobStoreError> {
        self.shared.ensure_healthy().map_err(blob_error)?;
        self.shared
            .get(id)
            .map_err(blob_error)?
            .ok_or_else(|| blob_error(StoreError::MissingObject(id)))
    }

    fn v3(&self, hashes: &[B256]) -> Result<Vec<Option<BlobAndProofV2>>, BlobStoreError> {
        self.shared.ensure_healthy().map_err(blob_error)?;
        let mut result = vec![None; hashes.len()];
        for tx in self.lookup_transactions(hashes)? {
            if let Some(sidecar) = self.read_sidecar(tx)? &&
                let Some(sidecar) = sidecar.as_eip7594()
            {
                for (index, value) in sidecar.match_versioned_hashes(hashes) {
                    result[index] = Some(value);
                }
            }
            if result.iter().all(Option::is_some) {
                break;
            }
        }
        Ok(result)
    }
    fn refresh_index(&self, cache: &mut BlobCache) -> Result<(), BlobStoreError> {
        let generation = self.shared.reference_generation().map_err(blob_error)?;
        if cache.generation == Some(generation) {
            return Ok(());
        }
        cache.generation = None;
        let mut index = BlobIndex::default();
        for tx in self.durable_transaction_hashes() {
            let ids = self.shared.owner_objects(Owner::Pool(tx.0));
            let Some(id) = ids
                .into_iter()
                .find(|id| id.kind == ObjectKind::BlobSidecar && id.encoding == Encoding::Json)
            else {
                continue;
            };
            let bytes = self.read_bytes(id)?;
            let descriptor = serde_json::from_slice::<Descriptor>(&bytes).map_err(blob_error)?;
            descriptor.validate().map_err(blob_error)?;
            let hashes = descriptor
                .commitments
                .iter()
                .map(|id| {
                    let bytes = self.read_bytes(*id)?;
                    let commitment = Bytes48::try_from(bytes.as_ref()).map_err(blob_error)?;
                    Ok(kzg_to_versioned_hash(commitment.as_slice()))
                })
                .collect::<Result<Vec<_>, BlobStoreError>>()?;
            index.replace(
                tx,
                IndexedSidecar { descriptor: id, logical_size: descriptor.logical_size, hashes },
            );
        }
        cache.decoded.retain(|tx, _| {
            cache.index.entries.get(tx).map(|entry| entry.descriptor) ==
                index.entries.get(tx).map(|entry| entry.descriptor)
        });
        cache.index = index;
        // Keep the generation observed before reading. Concurrent mutations force a fresh lookup.
        cache.generation = Some(generation);
        Ok(())
    }

    fn lookup_transactions(&self, hashes: &[B256]) -> Result<Vec<B256>, BlobStoreError> {
        let mut cache = self.gate.lock();
        self.refresh_index(&mut cache)?;
        Ok(hashes
            .iter()
            .filter_map(|hash| cache.index.hashes.get(hash))
            .flatten()
            .copied()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect())
    }
}

impl BlobStore for SharedBlobStore {
    fn insert(&self, tx: B256, data: PooledBlobSidecar) -> Result<(), BlobStoreError> {
        self.insert_all(vec![(tx, data)])
    }

    fn insert_all(&self, txs: Vec<(B256, PooledBlobSidecar)>) -> Result<(), BlobStoreError> {
        self.shared.ensure_healthy().map_err(blob_error)?;
        let prepared = txs
            .into_iter()
            .map(|(tx, data)| {
                let sidecar = prepare_sidecar(data)?;
                Ok((tx, sidecar))
            })
            .collect::<Result<Vec<_>, BlobStoreError>>()?;
        if prepared.is_empty() {
            return Ok(());
        }
        let batches = prepared
            .iter()
            .map(|(tx, sidecar)| OwnedObjectBatch {
                owner: Owner::Pool(tx.0),
                objects: &sidecar.objects,
            })
            .collect::<Vec<_>>();
        let mut cache = self.gate.lock();
        self.refresh_index(&mut cache)?;
        let (_, commit) = self.shared.put_owned_batches_tracked(&batches).map_err(blob_error)?;
        if cache.generation == Some(commit.before) {
            for (tx, sidecar) in prepared {
                cache.index.replace(tx, sidecar.entry);
                cache.decoded.remove(&tx);
            }
            cache.generation = Some(commit.after);
        } else {
            cache.invalidate();
        }
        Ok(())
    }

    fn delete(&self, tx: B256) -> Result<(), BlobStoreError> {
        self.delete_all(vec![tx])
    }

    fn delete_all(&self, txs: Vec<B256>) -> Result<(), BlobStoreError> {
        let mut cache = self.gate.lock();
        self.refresh_index(&mut cache)?;
        let owners = txs.iter().map(|tx| Owner::Pool(tx.0)).collect::<Vec<_>>();
        let commit = self.shared.release_owners_tracked(&owners).map_err(blob_error)?;
        if cache.generation == Some(commit.before) {
            for tx in txs {
                cache.index.remove(tx);
                cache.decoded.remove(&tx);
            }
            cache.generation = Some(commit.after);
        } else {
            cache.invalidate();
        }
        Ok(())
    }

    fn cleanup(&self) -> BlobStoreCleanupStat {
        match self.shared.collect_garbage_detailed() {
            Ok((_, delete_succeed)) => BlobStoreCleanupStat { delete_succeed, delete_failed: 0 },
            Err(_) => BlobStoreCleanupStat { delete_succeed: 0, delete_failed: 1 },
        }
    }

    fn get(&self, tx: B256) -> Result<Option<Arc<BlobTransactionSidecarVariant>>, BlobStoreError> {
        self.shared.ensure_healthy().map_err(blob_error)?;
        self.read_sidecar(tx)
    }

    fn contains(&self, tx: B256) -> Result<bool, BlobStoreError> {
        self.shared.ensure_healthy().map_err(blob_error)?;
        Ok(self
            .shared
            .owner_objects(Owner::Pool(tx.0))
            .iter()
            .any(|id| id.kind == ObjectKind::BlobSidecar && id.encoding == Encoding::Json))
    }

    fn get_all(
        &self,
        txs: Vec<B256>,
    ) -> Result<Vec<(B256, Arc<BlobTransactionSidecarVariant>)>, BlobStoreError> {
        self.shared.ensure_healthy().map_err(blob_error)?;
        let mut result = Vec::new();
        for tx in txs {
            if let Some(sidecar) = self.read_sidecar(tx)? {
                result.push((tx, sidecar));
            }
        }
        Ok(result)
    }

    fn get_exact(
        &self,
        txs: Vec<B256>,
    ) -> Result<Vec<Arc<BlobTransactionSidecarVariant>>, BlobStoreError> {
        self.shared.ensure_healthy().map_err(blob_error)?;
        txs.into_iter()
            .map(|tx| self.read_sidecar(tx)?.ok_or(BlobStoreError::MissingSidecar(tx)))
            .collect()
    }

    fn get_by_versioned_hashes_v1(
        &self,
        hashes: &[B256],
    ) -> Result<Vec<Option<BlobAndProofV1>>, BlobStoreError> {
        self.shared.ensure_healthy().map_err(blob_error)?;
        let mut result = vec![None; hashes.len()];
        for tx in self.lookup_transactions(hashes)? {
            if let Some(sidecar) = self.read_sidecar(tx)? &&
                let Some(sidecar) = sidecar.as_eip4844()
            {
                for (index, value) in sidecar.match_versioned_hashes(hashes) {
                    result[index] = Some(value);
                }
            }
            if result.iter().all(Option::is_some) {
                break;
            }
        }
        Ok(result)
    }

    fn get_by_versioned_hashes_v2(
        &self,
        hashes: &[B256],
    ) -> Result<Option<Vec<BlobAndProofV2>>, BlobStoreError> {
        self.shared.ensure_healthy().map_err(blob_error)?;
        Ok(self.v3(hashes)?.into_iter().collect())
    }

    fn get_by_versioned_hashes_v3(
        &self,
        hashes: &[B256],
    ) -> Result<Vec<Option<BlobAndProofV2>>, BlobStoreError> {
        self.shared.ensure_healthy().map_err(blob_error)?;
        self.v3(hashes)
    }

    fn get_by_versioned_hashes_v4(
        &self,
        hashes: &[B256],
        mask: BlobCellMask,
    ) -> Result<Vec<Option<BlobCellsAndProofsV1>>, BlobStoreError> {
        self.shared.ensure_healthy().map_err(blob_error)?;
        let mut result = vec![None; hashes.len()];
        for tx in self.lookup_transactions(hashes)? {
            if let Some(sidecar) = self.read_sidecar(tx)? &&
                let Some(sidecar) = sidecar.as_eip7594()
            {
                for (index, value) in
                    sidecar.match_versioned_hashes_cells(hashes, mask).map_err(blob_error)?
                {
                    result[index] = Some(value);
                }
            }
            if result.iter().all(Option::is_some) {
                break;
            }
        }
        Ok(result)
    }

    fn has_versioned_hashes(&self, hashes: &[B256]) -> Result<Vec<bool>, BlobStoreError> {
        let mut cache = self.gate.lock();
        self.refresh_index(&mut cache)?;
        Ok(hashes.iter().map(|hash| cache.index.hashes.contains_key(hash)).collect())
    }

    fn get_cells(
        &self,
        tx: TxHash,
        mask: BlobCellMask,
    ) -> Result<Option<Vec<Cell>>, BlobStoreError> {
        self.shared.ensure_healthy().map_err(blob_error)?;
        let Some(sidecar) = self.read_sidecar(tx)? else { return Ok(None) };
        let Some(sidecar) = sidecar.as_eip7594() else { return Ok(None) };
        sidecar.compute_matching_cells(mask).map(Some).map_err(blob_error)
    }

    fn data_size_hint(&self) -> Option<usize> {
        let mut cache = self.gate.lock();
        self.refresh_index(&mut cache).ok()?;
        usize::try_from(cache.index.logical_bytes).ok()
    }

    fn blobs_len(&self) -> usize {
        let mut cache = self.gate.lock();
        if self.refresh_index(&mut cache).is_err() {
            return 0;
        }
        cache.index.entries.len()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Descriptor {
    flavor: Flavor,
    blobs: Vec<ObjectId>,
    commitments: Vec<ObjectId>,
    proofs: Vec<ObjectId>,
    logical_size: usize,
}

impl Descriptor {
    fn validate(&self) -> Result<(), FacadeError> {
        let proof_count = match self.flavor {
            Flavor::Eip4844 => self.blobs.len(),
            Flavor::Eip7594 => self
                .blobs
                .len()
                .checked_mul(CELLS_PER_EXT_BLOB)
                .ok_or(FacadeError::MalformedSidecar)?,
        };
        if self.commitments.len() != self.blobs.len() ||
            self.proofs.len() != proof_count ||
            self.blobs
                .iter()
                .any(|id| id.kind != ObjectKind::Blob || id.encoding != Encoding::Raw) ||
            self.commitments
                .iter()
                .any(|id| id.kind != ObjectKind::Commitment || id.encoding != Encoding::Raw) ||
            self.proofs
                .iter()
                .any(|id| id.kind != ObjectKind::Proof || id.encoding != Encoding::Raw)
        {
            return Err(FacadeError::MalformedSidecar);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
enum Flavor {
    Eip4844,
    Eip7594,
}

#[derive(Debug, thiserror::Error)]
enum FacadeError {
    #[error("sparse sidecar cannot be exposed as a complete execution-pool sidecar")]
    PartialSidecar,
    #[error("malformed sidecar cardinalities or object namespaces")]
    MalformedSidecar,
}

#[derive(Debug, Default)]
struct BlobCache {
    generation: Option<u64>,
    index: BlobIndex,
    decoded: BTreeMap<B256, Weak<BlobTransactionSidecarVariant>>,
}

impl BlobCache {
    fn invalidate(&mut self) {
        self.generation = None;
        self.decoded.clear();
    }
}

#[derive(Debug, Default)]
struct BlobIndex {
    entries: BTreeMap<B256, IndexedSidecar>,
    hashes: BTreeMap<B256, BTreeSet<B256>>,
    logical_bytes: u128,
}

impl BlobIndex {
    fn replace(&mut self, tx: B256, entry: IndexedSidecar) {
        self.remove(tx);
        self.logical_bytes += entry.logical_size as u128;
        for hash in &entry.hashes {
            self.hashes.entry(*hash).or_default().insert(tx);
        }
        self.entries.insert(tx, entry);
    }

    fn remove(&mut self, tx: B256) {
        if let Some(entry) = self.entries.remove(&tx) {
            self.logical_bytes -= entry.logical_size as u128;
            for hash in entry.hashes {
                if let std::collections::btree_map::Entry::Occupied(mut holders) =
                    self.hashes.entry(hash)
                {
                    holders.get_mut().remove(&tx);
                    if holders.get().is_empty() {
                        holders.remove();
                    }
                }
            }
        }
    }
}

#[derive(Debug)]
struct IndexedSidecar {
    descriptor: ObjectId,
    logical_size: usize,
    hashes: Vec<B256>,
}

struct PreparedSidecar {
    objects: Vec<(ObjectKind, Encoding, Vec<u8>)>,
    entry: IndexedSidecar,
}

fn prepare_sidecar(data: PooledBlobSidecar) -> Result<PreparedSidecar, BlobStoreError> {
    if !data.availability().is_full() {
        return Err(blob_error(FacadeError::PartialSidecar));
    }
    let sidecar = data.into_sidecar();
    let proofs = match &sidecar {
        BlobTransactionSidecarVariant::Eip4844(value) => &value.proofs,
        BlobTransactionSidecarVariant::Eip7594(value) => &value.cell_proofs,
    };
    let mut objects = Vec::new();
    let descriptor = Descriptor {
        flavor: if sidecar.is_eip7594() { Flavor::Eip7594 } else { Flavor::Eip4844 },
        blobs: sidecar
            .blobs()
            .iter()
            .map(|blob| add_object(&mut objects, ObjectKind::Blob, blob.as_slice()))
            .collect(),
        commitments: sidecar
            .commitments()
            .iter()
            .map(|value| add_object(&mut objects, ObjectKind::Commitment, value.as_slice()))
            .collect(),
        proofs: proofs
            .iter()
            .map(|value| add_object(&mut objects, ObjectKind::Proof, value.as_slice()))
            .collect(),
        logical_size: logical_size(&sidecar),
    };
    descriptor.validate().map_err(blob_error)?;
    let bytes = serde_json::to_vec(&descriptor).map_err(blob_error)?;
    let id = ObjectId::new(ObjectKind::BlobSidecar, Encoding::Json, &bytes);
    objects.push((ObjectKind::BlobSidecar, Encoding::Json, bytes));
    Ok(PreparedSidecar {
        objects,
        entry: IndexedSidecar {
            descriptor: id,
            logical_size: descriptor.logical_size,
            hashes: sidecar.versioned_hashes().collect(),
        },
    })
}

fn logical_size(sidecar: &BlobTransactionSidecarVariant) -> usize {
    let proof_count = match sidecar {
        BlobTransactionSidecarVariant::Eip4844(value) => value.proofs.len(),
        BlobTransactionSidecarVariant::Eip7594(value) => value.cell_proofs.len(),
    };
    sidecar.blobs().len() * alloy_eips::eip4844::BYTES_PER_BLOB +
        (sidecar.commitments().len() + proof_count) * 48
}
fn add_object(
    objects: &mut Vec<(ObjectKind, Encoding, Vec<u8>)>,
    kind: ObjectKind,
    bytes: &[u8],
) -> ObjectId {
    objects.push((kind, Encoding::Raw, bytes.to_vec()));
    ObjectId::new(kind, Encoding::Raw, bytes)
}

fn blob_error(error: impl std::error::Error + Send + Sync + 'static) -> BlobStoreError {
    BlobStoreError::Other(Box::new(error))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{StoreIdentity, DEFAULT_CACHE_BYTES};
    use alloy_eips::eip4844::kzg_to_versioned_hash;
    use tempfile::TempDir;

    fn open(path: &std::path::Path) -> SharedBlobStore {
        SharedBlobStore::new(
            SharedDataStore::open(
                path,
                StoreIdentity { genesis_root: [1; 32], config_digest: [2; 32] },
                DEFAULT_CACHE_BYTES,
            )
            .unwrap(),
        )
    }

    #[test]
    fn restart_lookup_order_and_pool_eviction_preserve_consensus_blob() {
        let dir = TempDir::new().unwrap();
        let tx = B256::from([3; 32]);
        let blob = Blob::default();
        let commitment = Bytes48::from([4; 48]);
        let hash = kzg_to_versioned_hash(commitment.as_slice());
        let id = ObjectId::new(ObjectKind::Blob, Encoding::Raw, blob.as_slice());
        {
            let store = open(dir.path());
            let sidecar = BlobTransactionSidecar {
                blobs: vec![blob],
                commitments: vec![commitment],
                proofs: vec![Bytes48::default()],
            };
            store.insert(tx, BlobTransactionSidecarVariant::Eip4844(sidecar).into()).unwrap();
            store.shared.pin(Owner::Consensus([5; 32]), id).unwrap();
            assert_eq!(
                store.has_versioned_hashes(&[hash, B256::ZERO, hash]).unwrap(),
                vec![true, false, true]
            );
        }
        let store = open(dir.path());
        assert!(store.get_by_versioned_hashes_v1(&[hash]).unwrap()[0].is_some());
        assert_eq!(store.get_exact(vec![tx, tx]).unwrap().len(), 2);
        store.delete(tx).unwrap();
        store.cleanup();
        assert!(!store.contains(tx).unwrap());
        assert!(store.shared.get(id).unwrap().is_some());
    }

    #[test]
    fn v2_requires_all_while_v3_and_v4_preserve_missing_positions() {
        let dir = TempDir::new().unwrap();
        let store = open(dir.path());
        let commitment = Bytes48::from([4; 48]);
        let hash = kzg_to_versioned_hash(commitment.as_slice());
        let sidecar = BlobTransactionSidecarEip7594::new(
            vec![Blob::default()],
            vec![commitment],
            vec![Bytes48::default(); CELLS_PER_EXT_BLOB],
        );
        store
            .insert(B256::from([3; 32]), BlobTransactionSidecarVariant::Eip7594(sidecar).into())
            .unwrap();
        assert!(store.get_by_versioned_hashes_v2(&[hash, B256::ZERO]).unwrap().is_none());
        let values = store.get_by_versioned_hashes_v3(&[hash, B256::ZERO, hash]).unwrap();
        assert!(values[0].is_some() && values[1].is_none() && values[2].is_some());
        let values = store
            .get_by_versioned_hashes_v4(&[hash, B256::ZERO], BlobCellMask::from_bits(1))
            .unwrap();
        assert_eq!(values[0].as_ref().unwrap().blob_cells.len(), 1);
        assert!(values[1].is_none());
    }

    #[test]
    fn standalone_sparse_cells_are_never_exposed_as_complete_pool_sidecars() {
        let dir = TempDir::new().unwrap();
        let store = open(dir.path());
        let tx = B256::from([3; 32]);
        store.shared.put(ObjectKind::Cell, Encoding::Raw, b"partial", Owner::Pool(tx.0)).unwrap();
        assert!(!store.contains(tx).unwrap());
        assert!(store.get(tx).unwrap().is_none());
        let malformed = BlobTransactionSidecar {
            blobs: vec![Blob::default()],
            commitments: vec![],
            proofs: vec![],
        };
        assert!(store
            .insert(tx, BlobTransactionSidecarVariant::Eip4844(malformed).into())
            .is_err());
    }

    fn sidecar(commitment: Bytes48) -> PooledBlobSidecar {
        BlobTransactionSidecarVariant::Eip4844(BlobTransactionSidecar {
            blobs: vec![Blob::default()],
            commitments: vec![commitment],
            proofs: vec![Bytes48::default()],
        })
        .into()
    }

    #[test]
    fn batched_indexes_track_duplicate_holders_replacement_and_restart() {
        let directory = TempDir::new().unwrap();
        let first = B256::from([1; 32]);
        let second = B256::from([2; 32]);
        let commitment = Bytes48::from([7; 48]);
        let hash = kzg_to_versioned_hash(commitment.as_slice());
        let replacement = Bytes48::from([8; 48]);
        let replacement_hash = kzg_to_versioned_hash(replacement.as_slice());
        let size = alloy_eips::eip4844::BYTES_PER_BLOB + 96;
        {
            let store = open(directory.path());
            let before = store.shared.try_observe().unwrap().journal_commits_total;
            store
                .insert_all(vec![(first, sidecar(commitment)), (second, sidecar(commitment))])
                .unwrap();
            assert_eq!(store.shared.try_observe().unwrap().journal_commits_total, before + 1);
            assert_eq!(store.blobs_len(), 2);
            assert_eq!(store.data_size_hint(), Some(2 * size));
            store.insert(first, sidecar(replacement)).unwrap();
            assert_eq!(
                store.has_versioned_hashes(&[hash, replacement_hash]).unwrap(),
                vec![true, true]
            );
            store.delete(second).unwrap();
            assert_eq!(
                store.has_versioned_hashes(&[hash, replacement_hash, hash]).unwrap(),
                vec![false, true, false]
            );
            assert_eq!(store.blobs_len(), 1);
            assert_eq!(store.data_size_hint(), Some(size));
        }
        let store = open(directory.path());
        assert_eq!(store.blobs_len(), 1);
        assert_eq!(store.data_size_hint(), Some(size));
        assert_eq!(
            store
                .get_by_versioned_hashes_v1(&[hash, replacement_hash])
                .unwrap()
                .iter()
                .map(Option::is_some)
                .collect::<Vec<_>>(),
            vec![false, true]
        );
    }

    #[test]
    fn metrics_do_not_read_objects_and_external_pool_changes_invalidate_indexes() {
        let directory = TempDir::new().unwrap();
        let store = open(directory.path());
        let tx = B256::from([1; 32]);
        let commitment = Bytes48::from([7; 48]);
        let hash = kzg_to_versioned_hash(commitment.as_slice());
        store.insert(tx, sidecar(commitment)).unwrap();
        let decoded = store.get(tx).unwrap().unwrap();
        let blob = ObjectId::new(ObjectKind::Blob, Encoding::Raw, decoded.blobs()[0].as_slice());
        store.shared.pin(Owner::Consensus([2; 32]), blob).unwrap();
        let before = store.shared.try_observe().unwrap();
        assert_eq!(store.blobs_len(), 1);
        assert_eq!(store.data_size_hint(), Some(logical_size(&decoded)));
        assert_eq!(store.has_versioned_hashes(&[hash]).unwrap(), vec![true]);
        assert_eq!(store.shared.try_observe().unwrap(), before);
        store.shared.replace_owner(Owner::Pool(tx.0), &[]).unwrap();
        assert_eq!(store.blobs_len(), 0);
        assert_eq!(store.data_size_hint(), Some(0));
        assert_eq!(store.has_versioned_hashes(&[hash]).unwrap(), vec![false]);
        assert!(store.get(tx).unwrap().is_none());
        assert!(store.shared.get(blob).unwrap().is_some());
        store.shared.inner.lock().journal.mark_publication_uncertain();
        assert_eq!(store.data_size_hint(), None);
        assert_eq!(store.blobs_len(), 0);
        assert!(store.has_versioned_hashes(&[hash]).is_err());
    }

    #[test]
    fn malformed_later_batch_member_cannot_publish_an_earlier_transaction() {
        let directory = TempDir::new().unwrap();
        let store = open(directory.path());
        let first = B256::from([1; 32]);
        let malformed = BlobTransactionSidecarVariant::Eip4844(BlobTransactionSidecar {
            blobs: vec![Blob::default()],
            commitments: vec![],
            proofs: vec![],
        });
        let before = store.shared.try_observe().unwrap();
        assert!(store
            .insert_all(vec![
                (first, sidecar(Bytes48::default())),
                (B256::from([2; 32]), malformed.into())
            ])
            .is_err());
        assert!(!store.contains(first).unwrap());
        assert_eq!(store.shared.try_observe().unwrap(), before);
    }
}
