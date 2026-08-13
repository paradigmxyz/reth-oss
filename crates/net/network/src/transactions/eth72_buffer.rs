//! Temporary storage for blobless eth/72 transaction responses.
//!
//! Geth keeps transaction bodies and cell deliveries in separate two-minute buffers because either
//! can arrive first. Reth uses the same lifecycle, with [`SparseBlobSidecar`] providing the
//! metadata and sparse cell storage. Complete sidecars are still handed to the regular blobstore
//! by the pool integration layer.

use alloy_eips::eip7594::Cell;
use alloy_primitives::{B128, B256};
use reth_network_peers::PeerId;
use reth_transaction_pool::blobstore::{SparseBlobSidecar, SparseBlobSidecarError};
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

/// Default lifetime of incomplete eth/72 state, matching geth's `bufferLifetime`.
pub const DEFAULT_ETH72_BLOB_BUFFER_TTL: Duration = Duration::from_secs(2 * 60);

/// Default bound on the number of transaction bodies retained in memory.
pub const DEFAULT_ETH72_BLOB_BUFFER_CAPACITY: usize = 4096;

/// A body or cell delivery that is waiting for its counterpart.
#[derive(Debug)]
struct PendingCells {
    deliveries: Vec<(PeerId, B128, Vec<Cell>)>,
    expires_at: Instant,
}

/// Temporary state for one structurally validated blobless transaction.
#[derive(Debug)]
pub struct PendingEth72Blob<T> {
    /// Signed transaction body without the blob payload.
    pub transaction: T,
    /// Commitments, cell proofs, and cells received so far.
    pub sidecar: SparseBlobSidecar,
    /// Peers that announced or delivered data for this transaction.
    pub announcers: Vec<PeerId>,
    /// Fixed expiry. Duplicate announcements do not extend it.
    pub expires_at: Instant,
}

/// Result of merging a `Cells` response.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MergedEth72Cells {
    /// Number of previously missing cells inserted.
    pub inserted: usize,
    /// Whether every cell for every blob is now present.
    pub complete: bool,
    /// Whether the body was not present yet and the delivery was buffered cells-first.
    pub buffered_without_body: bool,
}

/// Errors raised while accepting cell deliveries.
#[derive(Debug, thiserror::Error)]
pub enum Eth72BlobBufferError {
    /// No body or cell delivery is currently retained for the hash.
    #[error("transaction {0:?} is not pending in the eth/72 blob buffer")]
    UnknownTransaction(B256),
    /// A peer returned a cell index that was not requested from it.
    #[error("peer {peer:?} returned unrequested cells for transaction {tx_hash:?}")]
    UnrequestedCells {
        /// Transaction whose response was rejected.
        tx_hash: B256,
        /// Peer that sent the response.
        peer: PeerId,
    },
    /// The sparse sidecar rejected the response layout or a conflicting cell.
    #[error(transparent)]
    InvalidCells(#[from] SparseBlobSidecarError),
}

/// Bounded hash-keyed storage for in-flight eth/72 reassembly.
#[derive(Debug)]
pub struct Eth72BlobBuffer<T> {
    entries: HashMap<B256, PendingEth72Blob<T>>,
    cells_first: HashMap<B256, PendingCells>,
    requested: HashMap<(B256, PeerId), B128>,
    capacity: usize,
    ttl: Duration,
}

impl<T> Default for Eth72BlobBuffer<T> {
    fn default() -> Self {
        Self::new(DEFAULT_ETH72_BLOB_BUFFER_CAPACITY, DEFAULT_ETH72_BLOB_BUFFER_TTL)
    }
}

impl<T> Eth72BlobBuffer<T> {
    /// Creates a buffer with an explicit body capacity and entry lifetime.
    pub fn new(capacity: usize, ttl: Duration) -> Self {
        Self {
            entries: HashMap::new(),
            cells_first: HashMap::new(),
            requested: HashMap::new(),
            capacity,
            ttl,
        }
    }

    /// Returns the number of body entries currently retained.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns whether no body or cell-only entries are retained.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty() && self.cells_first.is_empty()
    }

    /// Returns whether a hash is known to the temporary buffer, including cells-first state.
    pub fn contains(&mut self, hash: &B256, now: Instant) -> bool {
        self.purge_expired(now);
        self.entries.contains_key(hash) || self.cells_first.contains_key(hash)
    }

    /// Inserts a validated blobless body and its sidecar metadata.
    ///
    /// If cells arrived first, they are replayed into the new sparse sidecar. The expiry is not
    /// extended when an existing body receives another announcer.
    pub fn insert(
        &mut self,
        hash: B256,
        transaction: T,
        sidecar: SparseBlobSidecar,
        announcer: PeerId,
        now: Instant,
    ) -> Result<bool, Eth72BlobBufferError> {
        self.purge_expired(now);
        if let Some(entry) = self.entries.get_mut(&hash) {
            add_announcer(entry, announcer);
            return Ok(false)
        }
        if self.capacity == 0 {
            return Ok(false)
        }
        if self.entries.len() >= self.capacity &&
            let Some(oldest) = self
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.expires_at)
                .map(|(hash, _)| *hash)
        {
            self.entries.remove(&oldest);
            self.requested.retain(|(pending, _), _| pending != &oldest);
        }

        let expires_at = now.checked_add(self.ttl).unwrap_or(now);
        let mut entry =
            PendingEth72Blob { transaction, sidecar, announcers: vec![announcer], expires_at };
        if let Some(pending) = self.cells_first.remove(&hash) {
            for (peer, mask, cells) in pending.deliveries {
                entry.sidecar.merge_cells(mask, cells)?;
                add_announcer(&mut entry, peer);
            }
        }
        self.entries.insert(hash, entry);
        Ok(true)
    }

    /// Records a request sent to a peer. Responses are accepted only for requested indices.
    pub fn record_request(
        &mut self,
        hash: B256,
        peer: PeerId,
        mask: B128,
        now: Instant,
    ) -> Result<(), Eth72BlobBufferError> {
        self.purge_expired(now);
        if !self.contains(&hash, now) {
            return Err(Eth72BlobBufferError::UnknownTransaction(hash))
        }
        self.requested
            .entry((hash, peer))
            .and_modify(|old| *old = B128::from(u128::from(*old) | u128::from(mask)))
            .or_insert(mask);
        Ok(())
    }

    /// Merges one `Cells` group. The response mask must be a subset of the outstanding request.
    pub fn merge_cells(
        &mut self,
        hash: B256,
        peer: PeerId,
        mask: B128,
        cells: Vec<Cell>,
        now: Instant,
    ) -> Result<MergedEth72Cells, Eth72BlobBufferError> {
        self.purge_expired(now);
        let requested = self.requested.get(&(hash, peer)).copied().unwrap_or_default();
        if u128::from(mask) & !u128::from(requested) != 0 {
            return Err(Eth72BlobBufferError::UnrequestedCells { tx_hash: hash, peer })
        }

        if let Some(entry) = self.entries.get_mut(&hash) {
            let inserted = entry.sidecar.merge_cells(mask, cells)?;
            add_announcer(entry, peer);
            let complete = entry.sidecar.is_complete();
            self.consume_request(hash, peer, mask);
            return Ok(MergedEth72Cells { inserted, complete, buffered_without_body: false })
        }

        if !self.contains(&hash, now) && requested == B128::default() {
            return Err(Eth72BlobBufferError::UnknownTransaction(hash))
        }
        let pending = self.cells_first.entry(hash).or_insert_with(|| PendingCells {
            deliveries: Vec::new(),
            expires_at: now.checked_add(self.ttl).unwrap_or(now),
        });
        pending.deliveries.push((peer, mask, cells));
        self.consume_request(hash, peer, mask);
        Ok(MergedEth72Cells { inserted: 0, complete: false, buffered_without_body: true })
    }

    /// Returns the common cell mask still missing from a body entry.
    pub fn missing_mask(&mut self, hash: B256, now: Instant) -> Result<B128, Eth72BlobBufferError> {
        self.purge_expired(now);
        let entry =
            self.entries.get(&hash).ok_or(Eth72BlobBufferError::UnknownTransaction(hash))?;
        Ok(B128::from(!u128::from(entry.sidecar.cell_mask())))
    }

    /// Removes and returns a complete entry for reconstruction and pool import.
    pub fn take_complete(&mut self, hash: B256, now: Instant) -> Option<PendingEth72Blob<T>> {
        self.purge_expired(now);
        if self.entries.get(&hash)?.sidecar.is_complete() {
            self.requested.retain(|(pending, _), _| pending != &hash);
            self.entries.remove(&hash)
        } else {
            None
        }
    }

    /// Drops body and cells-first entries whose fixed lifetime elapsed.
    pub fn purge_expired(&mut self, now: Instant) -> usize {
        let before = self.entries.len() + self.cells_first.len();
        self.entries.retain(|_, entry| entry.expires_at > now);
        self.cells_first.retain(|_, entry| entry.expires_at > now);
        let live = self.entries.keys().chain(self.cells_first.keys()).copied().collect::<Vec<_>>();
        self.requested.retain(|(hash, _), _| live.contains(hash));
        before - self.entries.len() - self.cells_first.len()
    }

    fn consume_request(&mut self, hash: B256, peer: PeerId, mask: B128) {
        let key = (hash, peer);
        let remaining =
            self.requested.get(&key).map(|requested| u128::from(*requested) & !u128::from(mask));
        match remaining {
            Some(0) | None => {
                self.requested.remove(&key);
            }
            Some(mask) => {
                self.requested.insert(key, B128::from(mask));
            }
        }
    }
}

fn add_announcer<T>(entry: &mut PendingEth72Blob<T>, peer: PeerId) {
    if !entry.announcers.contains(&peer) {
        entry.announcers.push(peer);
    }
}
