//! Actual blob and sampled-column availability checks for fork-choice admission.
//! These blocking mathematical checks do not authenticate the block's transition or proposer.

use crate::{columns, AvailabilityError};
use c_kzg::{Blob, Bytes48, KzgSettings, CELLS_PER_EXT_BLOB};
use reth_embedded_beacon_primitives::Root;
use reth_embedded_beacon_state_transition::ssz::{
    auxiliary::{deneb, fulu, gloas},
    fulu::BeaconBlock,
    gloas::BeaconBlock as GloasBeaconBlock,
    Preset,
};
use std::collections::BTreeSet;

/// Availability is distinct from candidate consensus or execution validity.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum AdmissionError {
    /// Structural collection dimensions, limits or configured sampling indices failed.
    #[error("invalid availability dimensions or sample configuration")]
    Structure,
    /// The proof input exceeds the local bounded-worker allocation policy.
    #[error("availability proof batch exceeds the local worker limit")]
    ResourceLimit,
    /// A sampled column was absent or duplicated.
    #[error("required availability column missing or duplicated")]
    MissingColumn,
    /// A header, commitment or supplied candidate root did not match the complete block.
    #[error("availability data is not bound to the candidate block")]
    BlockAssociation,
    /// Complete encoding or hashing failed.
    #[error("invalid candidate block encoding")]
    Encoding,
    /// The supplied blob or cell proof did not verify.
    #[error(transparent)]
    Proof(#[from] AvailabilityError),
}

/// Verify every blob against the complete block's ordered commitments and actual KZG proofs.
/// The caller must retrieve the full batch for the exact candidate root; an empty commitment
/// list requires an empty batch and proves only that no blob data is required.
pub fn verify_blob_batch(
    settings: &KzgSettings,
    commitments: &[[u8; 48]],
    blobs: &[deneb::Blob],
    proofs: &[[u8; 48]],
) -> Result<(), AdmissionError> {
    if commitments.len() > 4096 || commitments.len() != blobs.len() || blobs.len() != proofs.len() {
        return Err(AdmissionError::Structure);
    }
    if blobs.len() > MAX_BATCH_BYTES / c_kzg::BYTES_PER_BLOB {
        return Err(AdmissionError::ResourceLimit);
    }
    if commitments.is_empty() {
        return Ok(());
    }
    let blobs = blobs
        .iter()
        .map(|blob| Blob::from_bytes(&blob.values).map_err(|_| AdmissionError::Structure))
        .collect::<Result<Vec<_>, _>>()?;
    let commitments = commitments.iter().copied().map(Bytes48::new).collect::<Vec<_>>();
    let proofs = proofs.iter().copied().map(Bytes48::new).collect::<Vec<_>>();
    let verified = settings
        .verify_blob_kzg_proof_batch(&blobs, &commitments, &proofs)
        .map_err(|_| AdmissionError::Proof(AvailabilityError::InvalidProof))?;
    if !verified {
        return Err(AdmissionError::Proof(AvailabilityError::InvalidProof));
    }
    Ok(())
}

/// Verify all configured Fulu samples, binding every sidecar to the exact candidate block.
/// Required indices are an explicit nonempty sampling policy; uniqueness and the 128-column
/// bound are enforced here. This reports sampled availability, without claiming full custody.
pub fn verify_fulu_sampling_batch(
    settings: &KzgSettings,
    candidate_root: Root,
    block: &BeaconBlock,
    sidecars: &[fulu::DataColumnSidecar],
    preset: Preset,
    max_blobs_per_block: usize,
    required_columns: &[u64],
) -> Result<(), AdmissionError> {
    if required_columns.is_empty() ||
        required_columns.len() > CELLS_PER_EXT_BLOB ||
        max_blobs_per_block == 0 ||
        max_blobs_per_block > 4096
    {
        return Err(AdmissionError::Structure);
    }
    let required = required_columns.iter().copied().collect::<BTreeSet<_>>();
    if required.len() != required_columns.len() ||
        required.iter().any(|index| *index >= CELLS_PER_EXT_BLOB as u64)
    {
        return Err(AdmissionError::Structure);
    }
    if block.hash_tree_root(preset).map_err(|_| AdmissionError::Encoding)? != candidate_root {
        return Err(AdmissionError::BlockAssociation);
    }
    let commitments = &block.body.blob_kzg_commitments.values;
    if commitments.len() > max_blobs_per_block {
        return Err(AdmissionError::Structure);
    }
    if commitments.is_empty() {
        return if sidecars.is_empty() { Ok(()) } else { Err(AdmissionError::Structure) };
    }
    if sidecars.len() > CELLS_PER_EXT_BLOB {
        return Err(AdmissionError::Structure);
    }
    check_cell_budget(sidecars.iter().map(|sidecar| sidecar.column.values.len()))?;
    let body_root = block.body.hash_tree_root(preset).map_err(|_| AdmissionError::Encoding)?;
    let mut seen = BTreeSet::new();
    for sidecar in sidecars {
        if !seen.insert(sidecar.index) {
            return Err(AdmissionError::MissingColumn);
        }
        let header = &sidecar.signed_block_header.message;
        if header.slot != block.slot ||
            header.proposer_index != block.proposer_index ||
            header.parent_root != block.parent_root ||
            header.state_root != block.state_root ||
            header.body_root != body_root ||
            &sidecar.kzg_commitments.values != commitments
        {
            return Err(AdmissionError::BlockAssociation);
        }
        columns::verify_fulu_column_structure(sidecar, max_blobs_per_block)?;
        columns::verify_fulu_column_inclusion_proof(sidecar, preset)?;
        columns::verify_fulu_column_kzg_proofs(settings, sidecar)?;
    }
    if !required.is_subset(&seen) {
        return Err(AdmissionError::MissingColumn);
    }
    Ok(())
}

// Bounds temporary native cryptographic copies on a single bounded verification worker.
const MAX_BATCH_BYTES: usize = 64 * 1024 * 1024;

/// Verify every configured Gloas custody/sample column against the actual retained bid.
/// The caller supplies the immutable strictly imported block; a successful mathematical
/// batch cannot independently authenticate that block or the execution envelope.
pub fn verify_gloas_sampling_batch(
    settings: &KzgSettings,
    candidate_root: Root,
    block: &GloasBeaconBlock,
    sidecars: &[gloas::DataColumnSidecar],
    preset: Preset,
    required_columns: &[u64],
) -> Result<(), AdmissionError> {
    if required_columns.is_empty() ||
        required_columns.len() > CELLS_PER_EXT_BLOB ||
        sidecars.len() > CELLS_PER_EXT_BLOB
    {
        return Err(AdmissionError::Structure);
    }
    let required = required_columns.iter().copied().collect::<BTreeSet<_>>();
    if required.len() != required_columns.len() ||
        required.iter().any(|index| *index >= CELLS_PER_EXT_BLOB as u64)
    {
        return Err(AdmissionError::Structure);
    }
    if block.hash_tree_root(preset).map_err(|_| AdmissionError::Encoding)? != candidate_root {
        return Err(AdmissionError::BlockAssociation);
    }
    let commitments = &block.body.signed_execution_payload_bid.message.blob_kzg_commitments.values;
    if commitments.is_empty() {
        return if sidecars.is_empty() { Ok(()) } else { Err(AdmissionError::Structure) };
    }
    if commitments.len() > 4096 {
        return Err(AdmissionError::ResourceLimit);
    }
    check_cell_budget(sidecars.iter().map(|sidecar| sidecar.column.values.len()))?;
    let mut seen = BTreeSet::new();
    for sidecar in sidecars {
        if !seen.insert(sidecar.index) {
            return Err(AdmissionError::MissingColumn);
        }
        if sidecar.slot != block.slot || sidecar.beacon_block_root != candidate_root {
            return Err(AdmissionError::BlockAssociation);
        }
        columns::verify_gloas_column_structure(sidecar, commitments)?;
        columns::verify_gloas_column_kzg_proofs(settings, sidecar, commitments)?;
    }
    if !required.is_subset(&seen) {
        return Err(AdmissionError::MissingColumn);
    }
    Ok(())
}

fn check_cell_budget(mut counts: impl Iterator<Item = usize>) -> Result<(), AdmissionError> {
    let cells = counts
        .try_fold(0usize, |total, count| total.checked_add(count))
        .ok_or(AdmissionError::ResourceLimit)?;
    if cells > MAX_BATCH_BYTES / c_kzg::BYTES_PER_CELL {
        return Err(AdmissionError::ResourceLimit);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use c_kzg::{ethereum_kzg_settings, BYTES_PER_BLOB};
    use reth_embedded_beacon_state_transition::ssz::Vector;

    #[test]
    fn blob_batch_requires_exact_order_shape_and_real_proofs() {
        // Native debug builds reserve large inline cryptographic input temporaries.
        // Keep this real two-blob regression on one explicitly bounded worker stack.
        std::thread::Builder::new()
            .stack_size(8 * 1024 * 1024)
            .spawn(blob_batch_regression)
            .unwrap()
            .join()
            .unwrap();
    }

    fn blob_batch_regression() {
        let settings = ethereum_kzg_settings(0);
        let first = Blob::new([0; BYTES_PER_BLOB]);
        let mut second_bytes = [0; BYTES_PER_BLOB];
        second_bytes[31] = 1;
        let second = Blob::new(second_bytes);
        let native = [first, second];
        let commitments = native
            .iter()
            .map(|blob| settings.blob_to_kzg_commitment(blob).unwrap().to_bytes().into_inner())
            .collect::<Vec<_>>();
        let proofs = native
            .iter()
            .zip(&commitments)
            .map(|(blob, commitment)| {
                settings
                    .compute_blob_kzg_proof(blob, &Bytes48::new(*commitment))
                    .unwrap()
                    .to_bytes()
                    .into_inner()
            })
            .collect::<Vec<_>>();
        let blobs =
            native.iter().map(|blob| Vector::from_vec(blob.as_ref().to_vec())).collect::<Vec<_>>();
        verify_blob_batch(settings, &commitments, &blobs, &proofs).unwrap();
        assert_eq!(
            verify_blob_batch(settings, &commitments, &blobs[..1], &proofs),
            Err(AdmissionError::Structure)
        );
        let mut swapped = commitments;
        swapped.swap(0, 1);
        assert_eq!(
            verify_blob_batch(settings, &swapped, &blobs, &proofs),
            Err(AdmissionError::Proof(AvailabilityError::InvalidProof))
        );
        assert!(verify_blob_batch(settings, &[], &[], &[]).is_ok());
        assert_eq!(
            verify_blob_batch(settings, &[], &blobs, &proofs),
            Err(AdmissionError::Structure)
        );
    }
}
