//! Fork-specific data-column proof helpers using complete consensus schemas.
//! Proof success does not authenticate a proposer, validate ancestry or admit gossip.

use crate::AvailabilityError;
use c_kzg::{Bytes48, Cell, KzgSettings, BYTES_PER_CELL, CELLS_PER_EXT_BLOB};
use reth_embedded_beacon_primitives::{hash_pair, BitList, Root};
use reth_embedded_beacon_state_transition::ssz::{
    auxiliary::{fulu, gloas},
    fulu::BlobKZGCommitments,
    Preset, Ssz,
};

const MAX_COLUMN_CELLS: usize = 4096;

/// Check Fulu dimensions against the blob limit active at its header's slot.
pub fn verify_fulu_column_structure(
    sidecar: &fulu::DataColumnSidecar,
    max_blobs_per_block: usize,
) -> Result<(), AvailabilityError> {
    check_column(
        sidecar.index,
        sidecar.column.values.len(),
        sidecar.kzg_commitments.values.len(),
        sidecar.kzg_proofs.values.len(),
    )?;
    if sidecar.kzg_commitments.values.len() > max_blobs_per_block {
        return Err(AvailabilityError::Structure);
    }
    Ok(())
}

/// Verify the Fulu column against the ordered commitments carried in its header.
pub fn verify_fulu_column_kzg_proofs(
    settings: &KzgSettings,
    sidecar: &fulu::DataColumnSidecar,
) -> Result<(), AvailabilityError> {
    check_column(
        sidecar.index,
        sidecar.column.values.len(),
        sidecar.kzg_commitments.values.len(),
        sidecar.kzg_proofs.values.len(),
    )?;
    verify_cells(
        settings,
        sidecar.index,
        &sidecar.kzg_commitments.values,
        &sidecar.column.values,
        &sidecar.kzg_proofs.values,
    )
}

/// Verify the bounded commitments list at its exact Fulu beacon-body position.
pub fn verify_fulu_column_inclusion_proof(
    sidecar: &fulu::DataColumnSidecar,
    preset: Preset,
) -> Result<(), AvailabilityError> {
    verify_commitments_inclusion(
        &sidecar.kzg_commitments,
        &sidecar.kzg_commitments_inclusion_proof.values,
        sidecar.signed_block_header.message.body_root,
        preset,
    )
}

/// Verify a Fulu partial header's commitments inclusion without authenticating its signature.
pub fn verify_fulu_partial_header_inclusion_proof(
    header: &fulu::PartialDataColumnHeader,
    preset: Preset,
) -> Result<(), AvailabilityError> {
    verify_commitments_inclusion(
        &header.kzg_commitments,
        &header.kzg_commitments_inclusion_proof.values,
        header.signed_block_header.message.body_root,
        preset,
    )
}

/// Check Gloas dimensions against commitments taken from its validated bid.
pub const fn verify_gloas_column_structure(
    sidecar: &gloas::DataColumnSidecar,
    commitments: &[[u8; 48]],
) -> Result<(), AvailabilityError> {
    check_column(
        sidecar.index,
        sidecar.column.values.len(),
        commitments.len(),
        sidecar.kzg_proofs.values.len(),
    )
}

/// Verify the complete Gloas column against its associated bid's ordered commitments.
pub fn verify_gloas_column_kzg_proofs(
    settings: &KzgSettings,
    sidecar: &gloas::DataColumnSidecar,
    commitments: &[[u8; 48]],
) -> Result<(), AvailabilityError> {
    verify_gloas_column_structure(sidecar, commitments)?;
    verify_cells(
        settings,
        sidecar.index,
        commitments,
        &sidecar.column.values,
        &sidecar.kzg_proofs.values,
    )
}

/// Verify Fulu partial cells selected in ascending blob order by the complete bitmap.
/// The caller must first authenticate the corresponding partial header.
pub fn verify_fulu_partial_column_kzg_proofs(
    settings: &KzgSettings,
    sidecar: &fulu::PartialDataColumnSidecar,
    commitments: &[[u8; 48]],
    column_index: u64,
) -> Result<(), AvailabilityError> {
    verify_partial_cells(
        settings,
        column_index,
        &sidecar.cells_present_bitmap.bits,
        commitments,
        &sidecar.partial_column.values,
        &sidecar.kzg_proofs.values,
    )
}

/// Verify Gloas partial cells selected in ascending blob order by the complete bitmap.
/// The caller must bind the group ID and slot to the fully validated beacon block's bid.
pub fn verify_gloas_partial_column_kzg_proofs(
    settings: &KzgSettings,
    sidecar: &gloas::PartialDataColumnSidecar,
    commitments: &[[u8; 48]],
    column_index: u64,
) -> Result<(), AvailabilityError> {
    verify_partial_cells(
        settings,
        column_index,
        &sidecar.cells_present_bitmap.bits,
        commitments,
        &sidecar.partial_column.values,
        &sidecar.kzg_proofs.values,
    )
}

const fn check_column(
    index: u64,
    cells: usize,
    commitments: usize,
    proofs: usize,
) -> Result<(), AvailabilityError> {
    if index >= CELLS_PER_EXT_BLOB as u64 {
        return Err(AvailabilityError::ColumnIndex);
    }
    if cells == 0 || cells > MAX_COLUMN_CELLS || cells != commitments || cells != proofs {
        return Err(AvailabilityError::Structure);
    }
    Ok(())
}

fn verify_commitments_inclusion(
    commitments: &BlobKZGCommitments,
    branch: &[Root],
    body_root: Root,
    preset: Preset,
) -> Result<(), AvailabilityError> {
    if branch.len() != 4 {
        return Err(AvailabilityError::Structure);
    }
    let mut root = commitments.hash_tree_root(preset).map_err(|_| AvailabilityError::Structure)?;
    let mut index = 11u64;
    for sibling in branch {
        root = if index & 1 == 0 { hash_pair(root, *sibling) } else { hash_pair(*sibling, root) };
        index >>= 1;
    }
    if root != body_root {
        return Err(AvailabilityError::InvalidProof);
    }
    Ok(())
}

fn verify_partial_cells(
    settings: &KzgSettings,
    column_index: u64,
    bitmap: &BitList,
    commitments: &[[u8; 48]],
    cells: &[fulu::Cell],
    proofs: &[[u8; 48]],
) -> Result<(), AvailabilityError> {
    if column_index >= CELLS_PER_EXT_BLOB as u64 {
        return Err(AvailabilityError::ColumnIndex);
    }
    if commitments.len() > MAX_COLUMN_CELLS || bitmap.len() != commitments.len() {
        return Err(AvailabilityError::Structure);
    }
    let selected = commitments
        .iter()
        .enumerate()
        .filter_map(|(index, commitment)| (bitmap.get(index) == Some(true)).then_some(*commitment))
        .collect::<Vec<_>>();
    if selected.len() != cells.len() || selected.len() != proofs.len() {
        return Err(AvailabilityError::Structure);
    }
    if selected.is_empty() {
        return Ok(());
    }
    verify_cells(settings, column_index, &selected, cells, proofs)
}

fn verify_cells(
    settings: &KzgSettings,
    column_index: u64,
    commitments: &[[u8; 48]],
    cells: &[fulu::Cell],
    proofs: &[[u8; 48]],
) -> Result<(), AvailabilityError> {
    let cells = cells
        .iter()
        .map(|cell| {
            if cell.values.len() != BYTES_PER_CELL {
                return Err(AvailabilityError::Structure);
            }
            Cell::from_bytes(&cell.values).map_err(|_| AvailabilityError::Structure)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let commitments = commitments.iter().copied().map(Bytes48::new).collect::<Vec<_>>();
    let proofs = proofs.iter().copied().map(Bytes48::new).collect::<Vec<_>>();
    let indices = vec![column_index; cells.len()];
    let valid = settings
        .verify_cell_kzg_proof_batch(&commitments, &indices, &cells, &proofs)
        .map_err(|_| AvailabilityError::InvalidProof)?;
    if !valid {
        return Err(AvailabilityError::InvalidProof);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use c_kzg::{ethereum_kzg_settings, Blob, BYTES_PER_BLOB};
    use reth_embedded_beacon_state_transition::ssz::{
        phase0::SignedBeaconBlockHeader, ProgressiveList, Vector,
    };

    #[test]
    fn bounded_commitments_root_is_bound_to_the_exact_body_position() {
        let preset = Preset::Minimal;
        let commitments = BlobKZGCommitments::from_vec(vec![[7; 48]]);
        let branch = [[1; 32], [2; 32], [3; 32], [4; 32]];
        let leaf = commitments.hash_tree_root(preset).unwrap();
        // Position eleven has the path right, right, left, right.
        let body_root = hash_pair(
            branch[3],
            hash_pair(hash_pair(branch[1], hash_pair(branch[0], leaf)), branch[2]),
        );
        let mut header = fulu::PartialDataColumnHeader {
            kzg_commitments: commitments,
            signed_block_header: SignedBeaconBlockHeader::from_ssz(&[0; 208], preset).unwrap(),
            kzg_commitments_inclusion_proof: Vector::from_vec(branch.to_vec()),
        };
        header.signed_block_header.message.body_root = body_root;
        verify_fulu_partial_header_inclusion_proof(&header, preset).unwrap();
        header.kzg_commitments.values[0][0] ^= 1;
        assert_eq!(
            verify_fulu_partial_header_inclusion_proof(&header, preset),
            Err(AvailabilityError::InvalidProof)
        );
        header.kzg_commitments_inclusion_proof.values.pop();
        assert_eq!(
            verify_fulu_partial_header_inclusion_proof(&header, preset),
            Err(AvailabilityError::Structure)
        );
    }

    #[test]
    fn partial_cells_use_bitmap_blob_order_and_real_kzg_proofs() {
        let settings = ethereum_kzg_settings(0);
        let first = Blob::new([0; BYTES_PER_BLOB]);
        let mut second_bytes = [0; BYTES_PER_BLOB];
        second_bytes[31] = 1;
        let second = Blob::new(second_bytes);
        let commitments = [
            settings.blob_to_kzg_commitment(&first).unwrap().to_bytes().into_inner(),
            settings.blob_to_kzg_commitment(&second).unwrap().to_bytes().into_inner(),
        ];
        let (cells, proofs) = settings.compute_cells_and_kzg_proofs(&second).unwrap();
        let index = 7;
        let mut sidecar = gloas::PartialDataColumnSidecar {
            cells_present_bitmap: gloas::CellsBitList::from_ssz(&[6], Preset::Minimal).unwrap(),
            partial_column: ProgressiveList::from_vec(vec![Vector::from_vec(
                cells[index].to_bytes().to_vec(),
            )]),
            kzg_proofs: ProgressiveList::from_vec(vec![proofs[index].to_bytes().into_inner()]),
        };
        verify_gloas_partial_column_kzg_proofs(settings, &sidecar, &commitments, index as u64)
            .unwrap();
        assert_eq!(
            verify_gloas_partial_column_kzg_proofs(settings, &sidecar, &commitments, 128),
            Err(AvailabilityError::ColumnIndex)
        );
        assert_eq!(
            verify_gloas_partial_column_kzg_proofs(settings, &sidecar, &commitments, 8),
            Err(AvailabilityError::InvalidProof)
        );
        sidecar.cells_present_bitmap =
            gloas::CellsBitList::from_ssz(&[5], Preset::Minimal).unwrap();
        assert_eq!(
            verify_gloas_partial_column_kzg_proofs(settings, &sidecar, &commitments, index as u64),
            Err(AvailabilityError::InvalidProof)
        );
        sidecar.cells_present_bitmap =
            gloas::CellsBitList::from_ssz(&[2], Preset::Minimal).unwrap();
        assert_eq!(
            verify_gloas_partial_column_kzg_proofs(settings, &sidecar, &commitments, index as u64),
            Err(AvailabilityError::Structure)
        );
    }

    #[test]
    fn complete_gloas_column_cannot_substitute_commitments_or_proofs() {
        let settings = ethereum_kzg_settings(0);
        let blob = Blob::new([0; BYTES_PER_BLOB]);
        let commitment = settings.blob_to_kzg_commitment(&blob).unwrap().to_bytes().into_inner();
        let (cells, proofs) = settings.compute_cells_and_kzg_proofs(&blob).unwrap();
        let mut sidecar = gloas::DataColumnSidecar {
            index: 3,
            column: ProgressiveList::from_vec(vec![Vector::from_vec(cells[3].to_bytes().to_vec())]),
            kzg_proofs: ProgressiveList::from_vec(vec![proofs[3].to_bytes().into_inner()]),
            slot: 42,
            beacon_block_root: [9; 32],
        };
        verify_gloas_column_kzg_proofs(settings, &sidecar, &[commitment]).unwrap();
        assert_eq!(verify_gloas_column_structure(&sidecar, &[]), Err(AvailabilityError::Structure));
        sidecar.kzg_proofs.values[0][0] ^= 1;
        assert_eq!(
            verify_gloas_column_kzg_proofs(settings, &sidecar, &[commitment]),
            Err(AvailabilityError::InvalidProof)
        );
    }
}
