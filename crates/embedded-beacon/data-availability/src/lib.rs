//! Proof-checked data columns with explicit custody and reconstruction state.

use c_kzg::{Bytes48, Cell, KzgSettings, CELLS_PER_EXT_BLOB};
use std::{collections::BTreeMap, sync::Arc};

pub mod columns;
pub mod admission;

/// All locally verified cells belonging to one KZG commitment.
#[derive(Clone, Debug)]
pub struct VerifiedCells {
    commitment: Bytes48,
    cells: BTreeMap<u64, (Arc<Cell>, Bytes48)>,
}

/// A declaration of what the store actually owns, independent of fork-choice validity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Custody {
    /// Only these columns are present and cryptographically checked.
    Partial {
        /// Number of physically retained and verified columns.
        verified_columns: usize,
    },
    /// Every extended cell is present and checked.
    Complete,
}

/// Data availability rejection before storage ownership can be acquired.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum AvailabilityError {
    /// The supplied cell is outside the extended blob.
    #[error("column index outside extended blob")]
    ColumnIndex,
    /// The batch is malformed or does not verify against its commitment.
    #[error("cell proof verification failed")]
    InvalidProof,
    /// Repeated indices in one batch are ambiguous.
    #[error("duplicate column index")]
    DuplicateColumn,
    /// A reconstruction needs at least half of the extended cells.
    #[error("insufficient cells for reconstruction")]
    InsufficientCells,
    /// Recovery failed or generated data did not verify.
    #[error("cell reconstruction failed")]
    Reconstruction,
    /// The column is not bound to the validated block and slot.
    #[error("column does not match validated beacon block")]
    BlockAssociation,
    /// SSZ offsets, counts or fork-specific limits are malformed.
    #[error("invalid column sidecar structure")]
    Structure,
}

impl VerifiedCells {
    /// Start with no availability; a commitment alone grants no custody.
    pub const fn new(commitment: Bytes48) -> Self {
        Self { commitment, cells: BTreeMap::new() }
    }

    /// Validate a batch before publishing any cell. Invalid batches leave the store unchanged.
    /// Beacon inclusion, block signature and fork-specific custody checks remain separate.
    pub fn insert_verified(
        &mut self,
        settings: &KzgSettings,
        batch: Vec<(u64, Arc<Cell>, Bytes48)>,
    ) -> Result<(), AvailabilityError> {
        if batch.is_empty() {
            return Ok(());
        }
        let mut unique = std::collections::BTreeSet::new();
        for (index, _, _) in &batch {
            if *index >= CELLS_PER_EXT_BLOB as u64 {
                return Err(AvailabilityError::ColumnIndex);
            }
            if !unique.insert(*index) {
                return Err(AvailabilityError::DuplicateColumn);
            }
        }
        let commitments = vec![self.commitment; batch.len()];
        let indices = batch.iter().map(|(index, _, _)| *index).collect::<Vec<_>>();
        let cells = batch.iter().map(|(_, cell, _)| **cell).collect::<Vec<_>>();
        let proofs = batch.iter().map(|(_, _, proof)| *proof).collect::<Vec<_>>();
        let verified = settings
            .verify_cell_kzg_proof_batch(&commitments, &indices, &cells, &proofs)
            .map_err(|_| AvailabilityError::InvalidProof)?;
        if !verified {
            return Err(AvailabilityError::InvalidProof);
        }
        for (index, cell, proof) in batch {
            self.cells.entry(index).or_insert((cell, proof));
        }
        Ok(())
    }

    /// Report physical custody without promoting a partial collection to a complete sidecar.
    pub fn custody(&self) -> Custody {
        if self.cells.len() == CELLS_PER_EXT_BLOB {
            Custody::Complete
        } else {
            Custody::Partial { verified_columns: self.cells.len() }
        }
    }

    /// Borrow verified cells. The shared allocation can be retained by independent owners.
    pub fn get(&self, column: u64) -> Option<&Arc<Cell>> {
        self.cells.get(&column).map(|(cell, _)| cell)
    }

    /// Reconstruct the complete extended blob, verify it, then publish atomically.
    pub fn reconstruct(&mut self, settings: &KzgSettings) -> Result<(), AvailabilityError> {
        if self.custody() == Custody::Complete {
            return Ok(());
        }
        if self.cells.len() < CELLS_PER_EXT_BLOB / 2 {
            return Err(AvailabilityError::InsufficientCells);
        }
        let indices = self.cells.keys().copied().collect::<Vec<_>>();
        let cells = self.cells.values().map(|(cell, _)| **cell).collect::<Vec<_>>();
        let (recovered, proofs) = settings
            .recover_cells_and_kzg_proofs(&indices, &cells)
            .map_err(|_| AvailabilityError::Reconstruction)?;
        let batch = recovered
            .iter()
            .zip(proofs.iter())
            .enumerate()
            .map(|(index, (cell, proof))| (index as u64, Arc::new(*cell), proof.to_bytes()))
            .collect();
        self.insert_verified(settings, batch).map_err(|_| AvailabilityError::Reconstruction)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use c_kzg::{ethereum_kzg_settings, Blob, BYTES_PER_BLOB};

    #[test]
    fn commitment_alone_is_not_availability() {
        let mut cells = VerifiedCells::new(Bytes48::new([0; 48]));
        assert_eq!(cells.custody(), Custody::Partial { verified_columns: 0 });
        assert_eq!(
            cells.reconstruct(ethereum_kzg_settings(0)),
            Err(AvailabilityError::InsufficientCells)
        );
    }

    #[test]
    fn invalid_batch_is_atomic() {
        let settings = ethereum_kzg_settings(0);
        let blob = Blob::new([0; BYTES_PER_BLOB]);
        let commitment = settings.blob_to_kzg_commitment(&blob).unwrap().to_bytes();
        let (cells, proofs) = settings.compute_cells_and_kzg_proofs(&blob).unwrap();
        let mut store = VerifiedCells::new(commitment);
        let proof = proofs[0].to_bytes();
        assert_eq!(
            store.insert_verified(settings, vec![(128, Arc::new(cells[0]), proof)]),
            Err(AvailabilityError::ColumnIndex)
        );
        assert_eq!(
            store.insert_verified(
                settings,
                vec![(0, Arc::new(cells[0]), proof), (0, Arc::new(cells[0]), proof)]
            ),
            Err(AvailabilityError::DuplicateColumn)
        );
        let mut bad = cells[0].to_bytes();
        bad[31] = 1;
        assert_eq!(
            store.insert_verified(settings, vec![(0, Arc::new(Cell::new(bad)), proof)]),
            Err(AvailabilityError::InvalidProof)
        );
        assert_eq!(store.custody(), Custody::Partial { verified_columns: 0 });
    }

    #[test]
    fn partial_cells_reconstruct_and_retain_shared_allocation() {
        let settings = ethereum_kzg_settings(0);
        let blob = Blob::new([0; BYTES_PER_BLOB]);
        let commitment = settings.blob_to_kzg_commitment(&blob).unwrap().to_bytes();
        let (cells, proofs) = settings.compute_cells_and_kzg_proofs(&blob).unwrap();
        let retained = Arc::new(cells[0]);
        let mut store = VerifiedCells::new(commitment);
        let batch = (0..64)
            .map(|index| {
                (
                    index as u64,
                    if index == 0 { retained.clone() } else { Arc::new(cells[index]) },
                    proofs[index].to_bytes(),
                )
            })
            .collect();
        store.insert_verified(settings, batch).unwrap();
        assert_eq!(store.custody(), Custody::Partial { verified_columns: 64 });
        assert!(Arc::ptr_eq(store.get(0).unwrap(), &retained));
        store.reconstruct(settings).unwrap();
        assert_eq!(store.custody(), Custody::Complete);
        assert!(Arc::ptr_eq(store.get(0).unwrap(), &retained));
        for (index, cell) in cells.iter().enumerate() {
            assert_eq!(store.get(index as u64).unwrap().to_bytes(), cell.to_bytes());
        }
    }
}

/// Gloas sidecar, whose commitments come from the associated block's validated bid.
#[derive(Clone, Debug)]
pub struct ColumnSidecar {
    /// Extended cell index, also identifying the data column.
    pub index: u64,
    /// One cell per blob named by the block's bid.
    pub cells: Vec<Arc<Cell>>,
    /// One KZG cell proof per blob.
    pub proofs: Vec<Bytes48>,
    /// Associated beacon block slot.
    pub slot: u64,
    /// Consensus root of the associated beacon block.
    pub beacon_block_root: [u8; 32],
}

/// Immutable proof-checked column bound to a particular validated block.
#[derive(Clone, Debug)]
pub struct VerifiedColumn {
    sidecar: ColumnSidecar,
}

impl ColumnSidecar {
    /// Decode canonical Gloas SSZ offsets with allocation and shape limits enforced first.
    pub fn from_ssz(bytes: &[u8], max_blobs: usize) -> Result<Self, AvailabilityError> {
        const FIXED_SIZE: usize = 56;
        if bytes.len() < FIXED_SIZE {
            return Err(AvailabilityError::Structure);
        }
        let index =
            u64::from_le_bytes(bytes[..8].try_into().map_err(|_| AvailabilityError::Structure)?);
        let column_offset =
            u32::from_le_bytes(bytes[8..12].try_into().map_err(|_| AvailabilityError::Structure)?)
                as usize;
        let proofs_offset =
            u32::from_le_bytes(bytes[12..16].try_into().map_err(|_| AvailabilityError::Structure)?)
                as usize;
        if index >= CELLS_PER_EXT_BLOB as u64 ||
            column_offset != FIXED_SIZE ||
            proofs_offset < column_offset ||
            proofs_offset > bytes.len()
        {
            return Err(AvailabilityError::Structure);
        }
        let column = &bytes[column_offset..proofs_offset];
        let proofs = &bytes[proofs_offset..];
        let count = column.len() / c_kzg::BYTES_PER_CELL;
        if !column.len().is_multiple_of(c_kzg::BYTES_PER_CELL) ||
            !proofs.len().is_multiple_of(48) ||
            count == 0 ||
            count > max_blobs ||
            proofs.len() / 48 != count
        {
            return Err(AvailabilityError::Structure);
        }
        let cells = column
            .chunks_exact(c_kzg::BYTES_PER_CELL)
            .map(|bytes| {
                Cell::from_bytes(bytes).map(Arc::new).map_err(|_| AvailabilityError::Structure)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let proofs = proofs
            .chunks_exact(48)
            .map(|bytes| Bytes48::from_bytes(bytes).map_err(|_| AvailabilityError::Structure))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            index,
            cells,
            proofs,
            slot: u64::from_le_bytes(
                bytes[16..24].try_into().map_err(|_| AvailabilityError::Structure)?,
            ),
            beacon_block_root: bytes[24..56]
                .try_into()
                .map_err(|_| AvailabilityError::Structure)?,
        })
    }

    /// Verify against commitments obtained from a fully validated block's bid.
    /// Calling code must additionally enforce fork timing, subnet and gossip duplicate rules.
    pub fn verify_against_bid(
        self,
        settings: &KzgSettings,
        block_root: [u8; 32],
        block_slot: u64,
        commitments: &[Bytes48],
        max_blobs: usize,
    ) -> Result<VerifiedColumn, AvailabilityError> {
        if self.beacon_block_root != block_root || self.slot != block_slot {
            return Err(AvailabilityError::BlockAssociation);
        }
        if self.index >= CELLS_PER_EXT_BLOB as u64 ||
            self.cells.is_empty() ||
            self.cells.len() > max_blobs ||
            self.cells.len() != commitments.len() ||
            self.proofs.len() != commitments.len()
        {
            return Err(AvailabilityError::Structure);
        }
        let indices = vec![self.index; self.cells.len()];
        let cells = self.cells.iter().map(|cell| **cell).collect::<Vec<_>>();
        let valid = settings
            .verify_cell_kzg_proof_batch(commitments, &indices, &cells, &self.proofs)
            .map_err(|_| AvailabilityError::InvalidProof)?;
        if !valid {
            return Err(AvailabilityError::InvalidProof);
        }
        Ok(VerifiedColumn { sidecar: self })
    }
}

impl VerifiedColumn {
    /// Read the immutable, proof-checked sidecar.
    pub const fn sidecar(&self) -> &ColumnSidecar {
        &self.sidecar
    }
}

#[cfg(test)]
mod column_tests {
    use super::*;
    use c_kzg::{ethereum_kzg_settings, Blob, BYTES_PER_BLOB, BYTES_PER_CELL};

    fn encoded_column(
        index: u64,
        slot: u64,
        root: [u8; 32],
        cell: &Cell,
        proof: &Bytes48,
    ) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend(index.to_le_bytes());
        bytes.extend(56u32.to_le_bytes());
        bytes.extend((56u32 + BYTES_PER_CELL as u32).to_le_bytes());
        bytes.extend(slot.to_le_bytes());
        bytes.extend(root);
        bytes.extend(cell.to_bytes());
        bytes.extend(proof.into_inner());
        bytes
    }

    #[test]
    fn gloas_column_checks_shape_block_binding_and_proof() {
        let settings = ethereum_kzg_settings(0);
        let blob = Blob::new([0; BYTES_PER_BLOB]);
        let commitment = settings.blob_to_kzg_commitment(&blob).unwrap().to_bytes();
        let (cells, proofs) = settings.compute_cells_and_kzg_proofs(&blob).unwrap();
        let encoded = encoded_column(7, 100, [8; 32], &cells[7], &proofs[7].to_bytes());
        let sidecar = ColumnSidecar::from_ssz(&encoded, 21).unwrap();
        assert_eq!(
            sidecar
                .clone()
                .verify_against_bid(settings, [9; 32], 100, &[commitment], 21)
                .unwrap_err(),
            AvailabilityError::BlockAssociation
        );
        assert_eq!(
            sidecar
                .clone()
                .verify_against_bid(settings, [8; 32], 99, &[commitment], 21)
                .unwrap_err(),
            AvailabilityError::BlockAssociation
        );
        assert_eq!(
            sidecar.clone().verify_against_bid(settings, [8; 32], 100, &[], 21).unwrap_err(),
            AvailabilityError::Structure
        );
        let verified =
            sidecar.verify_against_bid(settings, [8; 32], 100, &[commitment], 21).unwrap();
        assert_eq!(verified.sidecar().index, 7);
        assert_eq!(verified.sidecar().beacon_block_root, [8; 32]);
        assert_eq!(ColumnSidecar::from_ssz(&encoded, 0).unwrap_err(), AvailabilityError::Structure);
        let mut bad = encoded.clone();
        bad[8..12].copy_from_slice(&55u32.to_le_bytes());
        assert_eq!(ColumnSidecar::from_ssz(&bad, 21).unwrap_err(), AvailabilityError::Structure);
        bad = encoded;
        bad[56 + 31] = 1;
        assert_eq!(
            ColumnSidecar::from_ssz(&bad, 21)
                .unwrap()
                .verify_against_bid(settings, [8; 32], 100, &[commitment], 21)
                .unwrap_err(),
            AvailabilityError::InvalidProof
        );
    }
}
