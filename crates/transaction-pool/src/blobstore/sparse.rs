//! Reth-owned sparse blob sidecar representation.

use alloy_eips::{
    eip4844::{kzg_to_versioned_hash, BlobCellsAndProofsV1, Bytes48},
    eip7594::{Cell, CELLS_PER_EXT_BLOB},
};
use alloy_primitives::{B128, B256};

/// A sparse EIP-7594 sidecar.
///
/// The cells and proofs are flattened in blob-major order, but only for the cell indices set in
/// [`Self::custody`]. For `n` blobs and custody mask `{0, 3}`, the layout is
/// `[blob_0_cell_0, blob_0_cell_3, blob_1_cell_0, blob_1_cell_3]`.
///
/// This mirrors Geth's cell sidecar representation: custody describes the common set of cells
/// held for every blob, while the vectors contain only those cells. Missing cells therefore do
/// not require allocating `Option<Cell>` slots or manufacturing zero-filled blob data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SparseBlobSidecar {
    /// One commitment per blob.
    pub commitments: Vec<Bytes48>,
    /// One proof per stored cell, in the same flattened layout as [`Self::cells`].
    pub cell_proofs: Vec<Bytes48>,
    /// Received cells in blob-major order, filtered by [`Self::custody`].
    pub cells: Vec<Cell>,
    /// Common cell custody mask for every blob in this sidecar.
    pub custody: B128,
}

impl SparseBlobSidecar {
    /// Creates an empty sparse sidecar with the given commitments.
    pub fn empty(commitments: Vec<Bytes48>) -> Self {
        Self { commitments, cell_proofs: Vec::new(), cells: Vec::new(), custody: B128::from(0u128) }
    }

    /// Creates a sparse sidecar from commitments, cells, proofs, and a common custody mask.
    ///
    /// Cells and proofs must be ordered blob-major and contain exactly one entry for each set
    /// custody bit in each blob.
    pub fn try_new(
        commitments: Vec<Bytes48>,
        cell_proofs: Vec<Bytes48>,
        cells: Vec<Cell>,
        custody: B128,
    ) -> Result<Self, SparseBlobSidecarError> {
        let expected = expected_cell_count(commitments.len(), custody)?;
        if cell_proofs.len() != expected || cells.len() != expected {
            return Err(SparseBlobSidecarError::InvalidLayout {
                blobs: commitments.len(),
                cell_proofs: cell_proofs.len(),
                cells: cells.len(),
                custody,
                expected,
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

    /// Merges cells and proofs selected by `cell_mask` into the sidecar.
    ///
    /// Cells and proofs must be ordered by blob first and cell index second, matching the
    /// compact Geth-style representation. The operation is all-or-nothing.
    pub fn merge_cells(
        &mut self,
        cell_mask: B128,
        cells: Vec<Cell>,
        cell_proofs: Vec<Bytes48>,
    ) -> Result<usize, SparseBlobSidecarError> {
        let incoming = Self::try_new(self.commitments.clone(), cell_proofs, cells, cell_mask)?;
        self.merge_from(&incoming)
    }

    /// Merges all cells present in another sparse sidecar.
    ///
    /// The commitments must match exactly. The two sidecars may have different custody masks; the
    /// result stores the union and keeps the compact blob-major ordering.
    pub fn merge_from(&mut self, other: &Self) -> Result<usize, SparseBlobSidecarError> {
        if self.commitments != other.commitments {
            return Err(SparseBlobSidecarError::MetadataMismatch);
        }

        let merged_custody = B128::from(u128::from(self.custody) | u128::from(other.custody));
        let merged_indices = cell_indices(merged_custody);
        let cells_per_blob = merged_indices.len();
        let mut merged_cells = Vec::with_capacity(cells_per_blob.saturating_mul(self.blob_count()));
        let mut merged_proofs = Vec::with_capacity(merged_cells.capacity());
        let mut inserted = 0;

        for blob_index in 0..self.blob_count() {
            for &cell_index in &merged_indices {
                let existing = self.cell_and_proof(blob_index, cell_index);
                let incoming = other.cell_and_proof(blob_index, cell_index);
                let was_present = existing.is_some();

                match (existing, incoming) {
                    (
                        Some((existing_cell, existing_proof)),
                        Some((incoming_cell, incoming_proof)),
                    ) => {
                        if existing_cell != incoming_cell || existing_proof != incoming_proof {
                            return Err(SparseBlobSidecarError::ConflictingCell(
                                blob_index * CELLS_PER_EXT_BLOB + cell_index,
                            ));
                        }
                        merged_cells.push(existing_cell);
                        merged_proofs.push(existing_proof);
                    }
                    (Some((cell, proof)), None) | (None, Some((cell, proof))) => {
                        if !was_present {
                            inserted += 1;
                        }
                        merged_cells.push(cell);
                        merged_proofs.push(proof);
                    }
                    (None, None) => unreachable!("merged custody must contain the cell"),
                }
            }
        }

        self.cells = merged_cells;
        self.cell_proofs = merged_proofs;
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
                            .map(|&index| {
                                self.cell_and_proof(blob_index, index).map(|(cell, _)| cell)
                            })
                            .collect(),
                        proofs: indices
                            .iter()
                            .map(|&index| {
                                self.cell_and_proof(blob_index, index).map(|(_, proof)| proof)
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
                result.push(self.cell_and_proof(blob_index, *cell_index)?.0);
            }
        }
        Some(result)
    }

    /// Returns a stored cell and proof for a blob/cell pair.
    fn cell_and_proof(&self, blob_index: usize, cell_index: usize) -> Option<(Cell, Bytes48)> {
        if u128::from(self.custody) & (1u128 << cell_index) == 0 {
            return None;
        }

        let indices = cell_indices(self.custody);
        let cells_per_blob = indices.len();
        let offset =
            blob_index * cells_per_blob + indices.iter().position(|&index| index == cell_index)?;
        Some((self.cells[offset], self.cell_proofs[offset]))
    }
}

/// Errors produced while constructing or merging sparse sidecars.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SparseBlobSidecarError {
    /// The metadata or cell vectors do not have the required compact blob-major layout.
    #[error(
        "invalid sparse sidecar layout: {blobs} blobs and custody {custody:?} require {expected} cell proofs and cells, got {cell_proofs} proofs and {cells} cells"
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
        /// Expected number of proofs and cells.
        expected: usize,
    },
    /// A cell or proof vector did not contain exactly one entry for every requested pair.
    #[error(
        "invalid sparse cell count: expected {expected} cells and proofs, got {cells} cells and {proofs} proofs"
    )]
    CellCountMismatch {
        /// Expected number of cells and proofs.
        expected: usize,
        /// Actual number of cells.
        cells: usize,
        /// Actual number of proofs.
        proofs: usize,
    },
    /// A cell already stored at this index conflicted with a newly received cell or proof.
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
            expected: usize::MAX,
        },
    )
}

fn cell_indices(mask: B128) -> Vec<usize> {
    let bits = u128::from(mask);
    (0..CELLS_PER_EXT_BLOB).filter(|index| bits & (1u128 << index) != 0).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sidecar(blob_count: usize) -> SparseBlobSidecar {
        SparseBlobSidecar::empty(vec![Bytes48::default(); blob_count])
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

        assert_eq!(
            sidecar
                .merge_cells(mask, cells.clone(), vec![Bytes48::default(); cells.len()])
                .unwrap(),
            4
        );
        assert_eq!(sidecar.cells, cells);
        assert_eq!(sidecar.custody, B128::from(0b1001u128));
        assert_eq!(sidecar.cell_mask(), B128::from(0b1001u128));
    }

    #[test]
    fn cell_merge_rejects_conflicts_without_partial_updates() {
        let mut sidecar = sidecar(1);
        let mask = B128::from(1u128);
        sidecar.merge_cells(mask, vec![Cell::repeat_byte(1)], vec![Bytes48::default()]).unwrap();

        let err = sidecar
            .merge_cells(mask, vec![Cell::repeat_byte(2)], vec![Bytes48::default()])
            .unwrap_err();
        assert_eq!(err, SparseBlobSidecarError::ConflictingCell(0));
        assert_eq!(sidecar.cells[0], Cell::repeat_byte(1));
    }

    #[test]
    fn cells_for_mask_requires_all_requested_cells() {
        let mut sidecar = sidecar(1);
        let mask = B128::from(0b11u128);
        assert!(sidecar.cells_for_mask(mask).is_none());

        sidecar
            .merge_cells(
                mask,
                vec![Cell::repeat_byte(1), Cell::repeat_byte(2)],
                vec![Bytes48::default(); 2],
            )
            .unwrap();
        assert_eq!(
            sidecar.cells_for_mask(mask),
            Some(vec![Cell::repeat_byte(1), Cell::repeat_byte(2)])
        );
    }
}
