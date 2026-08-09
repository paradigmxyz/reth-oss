//! Reth-owned sparse blob sidecar representation.

use alloy_eips::{
    eip4844::{kzg_to_versioned_hash, BlobCellsAndProofsV1, Bytes48},
    eip7594::{Cell, CELLS_PER_EXT_BLOB},
};
use alloy_primitives::{B128, B256};

/// A sparse EIP-7594 sidecar.
///
/// Cells are flattened in blob-major order, but only for the cell indices set in
/// [`Self::custody`]. For `n` blobs and custody mask `{0, 3}`, their layout is
/// `[blob_0_cell_0, blob_0_cell_3, blob_1_cell_0, blob_1_cell_3]`.
///
/// This mirrors Geth's cell sidecar representation: custody describes the common set of cells
/// held for every blob, while the vectors contain only those cells. Missing cells therefore do
/// not require allocating `Option<Cell>` slots or manufacturing zero-filled blob data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SparseBlobSidecar {
    /// One commitment per blob.
    pub commitments: Vec<Bytes48>,
    /// Complete cell-proof metadata: one proof for every cell of every blob.
    ///
    /// Proofs arrive with the eth/72 pooled transaction response, independently of the cells
    /// subsequently fetched with `GetCells`. They therefore remain blob-major with
    /// `CELLS_PER_EXT_BLOB` entries per blob even when [`Self::cells`] is sparse.
    pub cell_proofs: Vec<Bytes48>,
    /// Received cells in blob-major order, filtered by [`Self::custody`].
    pub cells: Vec<Cell>,
    /// Common cell custody mask for every blob in this sidecar.
    pub custody: B128,
}

impl SparseBlobSidecar {
    /// Creates an empty sparse sidecar from complete transaction metadata.
    pub fn empty(
        commitments: Vec<Bytes48>,
        cell_proofs: Vec<Bytes48>,
    ) -> Result<Self, SparseBlobSidecarError> {
        Self::try_new(commitments, cell_proofs, Vec::new(), B128::from(0u128))
    }

    /// Creates a sparse sidecar from commitments, cells, proofs, and a common custody mask.
    ///
    /// Cells must be ordered blob-major and contain exactly one entry for each set custody bit in
    /// each blob. Proofs must contain the complete blob-major proof vector.
    pub fn try_new(
        commitments: Vec<Bytes48>,
        cell_proofs: Vec<Bytes48>,
        cells: Vec<Cell>,
        custody: B128,
    ) -> Result<Self, SparseBlobSidecarError> {
        let expected_cells = expected_cell_count(commitments.len(), custody)?;
        let expected_proofs = expected_cell_proof_count(commitments.len())?;
        if cell_proofs.len() != expected_proofs || cells.len() != expected_cells {
            return Err(SparseBlobSidecarError::InvalidLayout {
                blobs: commitments.len(),
                cell_proofs: cell_proofs.len(),
                cells: cells.len(),
                custody,
                expected_cells,
                expected_proofs,
            });
        }

        Ok(Self { commitments, cell_proofs, cells, custody })
    }

    /// Returns the number of blobs represented by this sidecar.
    pub const fn blob_count(&self) -> usize {
        self.commitments.len()
    }

    /// Returns the common cell mask available for every blob in this sidecar.
    pub fn cell_mask(&self) -> B128 {
        self.custody
    }

    /// Returns whether every cell for every blob has been received.
    pub fn is_complete(&self) -> bool {
        !self.commitments.is_empty() && self.custody == B128::from(u128::MAX)
    }

    /// Merges cells selected by `cell_mask` into the sidecar.
    ///
    /// Cells must be ordered by blob first and cell index second, matching the compact Geth-style
    /// representation. The operation is all-or-nothing.
    pub fn merge_cells(
        &mut self,
        cell_mask: B128,
        cells: Vec<Cell>,
    ) -> Result<usize, SparseBlobSidecarError> {
        let expected = expected_cell_count(self.blob_count(), cell_mask)?;
        if cells.len() != expected {
            return Err(SparseBlobSidecarError::CellCountMismatch { expected, actual: cells.len() })
        }
        let incoming = Self {
            commitments: self.commitments.clone(),
            cell_proofs: self.cell_proofs.clone(),
            cells,
            custody: cell_mask,
        };
        self.merge_from(&incoming)
    }

    /// Merges all cells present in another sparse sidecar.
    ///
    /// The commitments must match exactly. The two sidecars may have different custody masks; the
    /// result stores the union and keeps the compact blob-major ordering.
    pub fn merge_from(&mut self, other: &Self) -> Result<usize, SparseBlobSidecarError> {
        if self.commitments != other.commitments || self.cell_proofs != other.cell_proofs {
            return Err(SparseBlobSidecarError::MetadataMismatch);
        }

        let merged_custody = B128::from(u128::from(self.custody) | u128::from(other.custody));
        let merged_indices = cell_indices(merged_custody);
        let cells_per_blob = merged_indices.len();
        let mut merged_cells = Vec::with_capacity(cells_per_blob.saturating_mul(self.blob_count()));
        let mut inserted = 0;

        for blob_index in 0..self.blob_count() {
            for &cell_index in &merged_indices {
                let existing = self.cell_at(blob_index, cell_index);
                let incoming = other.cell_at(blob_index, cell_index);
                let was_present = existing.is_some();

                match (existing, incoming) {
                    (Some(existing_cell), Some(incoming_cell)) => {
                        if existing_cell != incoming_cell {
                            return Err(SparseBlobSidecarError::ConflictingCell(
                                blob_index * CELLS_PER_EXT_BLOB + cell_index,
                            ));
                        }
                        merged_cells.push(existing_cell);
                    }
                    (Some(cell), None) | (None, Some(cell)) => {
                        if !was_present {
                            inserted += 1;
                        }
                        merged_cells.push(cell);
                    }
                    (None, None) => unreachable!("merged custody must contain the cell"),
                }
            }
        }

        self.cells = merged_cells;
        self.custody = merged_custody;
        Ok(inserted)
    }

    /// Returns whether this sidecar contains a blob matching the versioned hash.
    pub fn contains_versioned_hash(&self, versioned_hash: B256) -> bool {
        self.commitments
            .iter()
            .any(|commitment| kzg_to_versioned_hash(commitment.as_slice()) == versioned_hash)
    }

    /// Returns requested cells and proofs for matching versioned hashes.
    ///
    /// Missing cells are represented as `None`, which is the shape required by
    /// `engine_getBlobsV4`.
    pub fn match_versioned_hashes_cells(
        &self,
        versioned_hashes: &[B256],
        cell_mask: B128,
    ) -> Vec<(usize, BlobCellsAndProofsV1)> {
        let indices = cell_indices(cell_mask);
        let mut matches = Vec::new();

        for (blob_index, commitment) in self.commitments.iter().enumerate() {
            let versioned_hash = kzg_to_versioned_hash(commitment.as_slice());
            for (matched_index, requested_hash) in versioned_hashes.iter().enumerate() {
                if versioned_hash != *requested_hash {
                    continue;
                }

                matches.push((
                    matched_index,
                    BlobCellsAndProofsV1 {
                        blob_cells: indices
                            .iter()
                            .map(|&index| self.cell_at(blob_index, index))
                            .collect(),
                        proofs: indices
                            .iter()
                            .map(|&index| {
                                self.cell_at(blob_index, index)
                                    .map(|_| self.proof_at(blob_index, index))
                            })
                            .collect(),
                    },
                ));
            }
        }

        matches
    }

    /// Returns the requested cells in blob-major, cell-index order if all are available.
    pub fn cells_for_mask(&self, cell_mask: B128) -> Option<Vec<Cell>> {
        let indices = cell_indices(cell_mask);
        let mut result = Vec::with_capacity(indices.len().saturating_mul(self.blob_count()));
        for blob_index in 0..self.blob_count() {
            for cell_index in &indices {
                result.push(self.cell_at(blob_index, *cell_index)?);
            }
        }
        Some(result)
    }

    /// Returns a stored cell for a blob/cell pair.
    fn cell_at(&self, blob_index: usize, cell_index: usize) -> Option<Cell> {
        if u128::from(self.custody) & (1u128 << cell_index) == 0 {
            return None;
        }

        let indices = cell_indices(self.custody);
        let cells_per_blob = indices.len();
        let offset =
            blob_index * cells_per_blob + indices.iter().position(|&index| index == cell_index)?;
        Some(self.cells[offset])
    }

    /// Returns the full transaction-metadata proof for a blob/cell pair.
    fn proof_at(&self, blob_index: usize, cell_index: usize) -> Bytes48 {
        self.cell_proofs[blob_index * CELLS_PER_EXT_BLOB + cell_index]
    }
}

/// Errors produced while constructing or merging sparse sidecars.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SparseBlobSidecarError {
    /// The metadata or cell vectors do not have the required compact blob-major layout.
    #[error(
        "invalid sparse sidecar layout: {blobs} blobs and custody {custody:?} require {expected_proofs} cell proofs and {expected_cells} cells, got {cell_proofs} proofs and {cells} cells"
    )]
    InvalidLayout {
        /// Number of commitments/blobs.
        blobs: usize,
        /// Number of supplied cell proofs.
        cell_proofs: usize,
        /// Number of supplied cells.
        cells: usize,
        /// Common custody mask.
        custody: B128,
        /// Expected number of cells.
        expected_cells: usize,
        /// Expected number of cell proofs.
        expected_proofs: usize,
    },
    /// A cell vector did not contain exactly one entry for every requested pair.
    #[error("invalid sparse cell count: expected {expected}, got {actual}")]
    CellCountMismatch {
        /// Expected number of cells.
        expected: usize,
        /// Actual number of cells.
        actual: usize,
    },
    /// A cell already stored at this index conflicted with a newly received cell.
    #[error("conflicting cell at flattened index {0}")]
    ConflictingCell(usize),
    /// The commitments did not match the existing sidecar.
    #[error("sparse sidecar metadata does not match")]
    MetadataMismatch,
}

fn expected_cell_count(blobs: usize, custody: B128) -> Result<usize, SparseBlobSidecarError> {
    blobs.checked_mul(u128::from(custody).count_ones() as usize).ok_or(
        SparseBlobSidecarError::InvalidLayout {
            blobs,
            cell_proofs: 0,
            cells: 0,
            custody,
            expected_cells: usize::MAX,
            expected_proofs: usize::MAX,
        },
    )
}

fn expected_cell_proof_count(blobs: usize) -> Result<usize, SparseBlobSidecarError> {
    blobs.checked_mul(CELLS_PER_EXT_BLOB).ok_or(SparseBlobSidecarError::InvalidLayout {
        blobs,
        cell_proofs: 0,
        cells: 0,
        custody: B128::from(0u128),
        expected_cells: 0,
        expected_proofs: usize::MAX,
    })
}

fn cell_indices(mask: B128) -> Vec<usize> {
    let bits = u128::from(mask);
    (0..CELLS_PER_EXT_BLOB).filter(|index| bits & (1u128 << index) != 0).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sidecar(blob_count: usize) -> SparseBlobSidecar {
        SparseBlobSidecar::empty(
            vec![Bytes48::default(); blob_count],
            vec![Bytes48::default(); blob_count * CELLS_PER_EXT_BLOB],
        )
        .unwrap()
    }

    #[test]
    fn empty_requires_complete_cell_proof_metadata() {
        let err = SparseBlobSidecar::empty(vec![Bytes48::default()], Vec::new()).unwrap_err();
        assert!(matches!(
            err,
            SparseBlobSidecarError::InvalidLayout {
                expected_cells: 0,
                expected_proofs: CELLS_PER_EXT_BLOB,
                ..
            }
        ));
    }

    #[test]
    fn merges_cells_in_blob_major_order() {
        let mut sidecar = sidecar(2);
        let mask = B128::from((1u128 << 0) | (1u128 << 3));
        let cells = vec![
            Cell::repeat_byte(1),
            Cell::repeat_byte(2),
            Cell::repeat_byte(3),
            Cell::repeat_byte(4),
        ];

        assert_eq!(sidecar.merge_cells(mask, cells.clone()).unwrap(), 4);
        assert_eq!(sidecar.cells, cells);
        assert_eq!(sidecar.custody, B128::from(0b1001u128));
        assert_eq!(sidecar.cell_mask(), B128::from(0b1001u128));
    }

    #[test]
    fn cell_merge_rejects_conflicts_without_partial_updates() {
        let mut sidecar = sidecar(1);
        let mask = B128::from(1u128);
        sidecar.merge_cells(mask, vec![Cell::repeat_byte(1)]).unwrap();

        let err = sidecar.merge_cells(mask, vec![Cell::repeat_byte(2)]).unwrap_err();
        assert_eq!(err, SparseBlobSidecarError::ConflictingCell(0));
        assert_eq!(sidecar.cells[0], Cell::repeat_byte(1));
    }

    #[test]
    fn cells_for_mask_requires_all_requested_cells() {
        let mut sidecar = sidecar(1);
        let mask = B128::from(0b11u128);
        assert!(sidecar.cells_for_mask(mask).is_none());

        sidecar.merge_cells(mask, vec![Cell::repeat_byte(1), Cell::repeat_byte(2)]).unwrap();
        assert_eq!(
            sidecar.cells_for_mask(mask),
            Some(vec![Cell::repeat_byte(1), Cell::repeat_byte(2)])
        );
    }

    #[test]
    fn v4_uses_full_proof_metadata_for_sparse_cells() {
        let commitment = Bytes48::from([1u8; 48]);
        let versioned_hash = kzg_to_versioned_hash(commitment.as_slice());
        let proofs = (0..CELLS_PER_EXT_BLOB)
            .map(|index| Bytes48::from([index as u8; 48]))
            .collect::<Vec<_>>();
        let mut sidecar = SparseBlobSidecar::empty(vec![commitment], proofs.clone()).unwrap();
        let stored_mask = B128::from(1u128 << 7);
        sidecar.merge_cells(stored_mask, vec![Cell::repeat_byte(7)]).unwrap();

        let (_, result) = sidecar
            .match_versioned_hashes_cells(
                &[versioned_hash],
                B128::from((1u128 << 0) | (1u128 << 7)),
            )
            .pop()
            .unwrap();

        assert_eq!(result.blob_cells, vec![None, Some(Cell::repeat_byte(7))]);
        assert_eq!(result.proofs, vec![None, Some(proofs[7])]);
    }
}
