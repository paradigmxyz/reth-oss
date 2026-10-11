//! Canonical encodings for normalized fork-aware light-client values.

use super::{
    types::{branch_depths, committee_size},
    Bootstrap, FinalityUpdate, Header, LightClientError, OptimisticUpdate, Update,
};
use crate::ssz::{encode_container, Preset};
use reth_embedded_beacon_primitives::{merkleize, uint64_root, Root};

const fn header_width(header: &Header) -> Option<usize> {
    match header {
        Header::Altair(_) => Some(112),
        Header::Capella(_) | Header::Deneb(_) => None,
        Header::Gloas(_) => Some(496),
    }
}
fn proof_bytes(proof: &[Root], depth: usize) -> Result<Vec<u8>, LightClientError> {
    if proof.len() != depth {
        return Err(LightClientError::Invalid("incorrect wire proof length"))
    }
    Ok(proof.iter().flat_map(|root| root.iter().copied()).collect())
}
fn proof_root(proof: &[Root], depth: usize) -> Result<Root, LightClientError> {
    if proof.len() != depth {
        return Err(LightClientError::Invalid("incorrect wire proof length"))
    }
    Ok(merkleize(proof, depth)?)
}
const fn check_header(
    header: &Header,
    schema: reth_embedded_beacon_primitives::Fork,
) -> Result<(), LightClientError> {
    if !header.matches_schema(schema) {
        return Err(LightClientError::Invalid("wire header schema mismatch"))
    }
    Ok(())
}
impl Bootstrap {
    /// Encode the exact declared fork container, including normalized proof padding.
    pub fn to_ssz(&self, preset: Preset) -> Result<Vec<u8>, LightClientError> {
        let (depth, _) = branch_depths(self.schema)?;
        check_header(&self.header, self.schema)?;
        Ok(encode_container(&[
            (header_width(&self.header), self.header.to_ssz(preset)?),
            (Some(committee_size(preset) * 48 + 48), self.current_sync_committee.to_ssz(preset)?),
            (Some(depth * 32), proof_bytes(&self.current_sync_committee_branch, depth)?),
        ])?)
    }
    /// Exact regular container root under the declared proof bounds.
    pub fn hash_tree_root(&self, preset: Preset) -> Result<Root, LightClientError> {
        let (depth, _) = branch_depths(self.schema)?;
        check_header(&self.header, self.schema)?;
        Ok(merkleize(
            &[
                self.header.hash_tree_root(preset)?,
                self.current_sync_committee.hash_tree_root(preset)?,
                proof_root(&self.current_sync_committee_branch, depth)?,
            ],
            3,
        )?)
    }
}
impl Update {
    /// Encode every update field using the exact declared fork and preset.
    pub fn to_ssz(&self, preset: Preset) -> Result<Vec<u8>, LightClientError> {
        let (depth, finality_depth) = branch_depths(self.schema)?;
        check_header(&self.attested_header, self.schema)?;
        check_header(&self.finalized_header, self.schema)?;
        Ok(encode_container(&[
            (header_width(&self.attested_header), self.attested_header.to_ssz(preset)?),
            (Some(committee_size(preset) * 48 + 48), self.next_sync_committee.to_ssz(preset)?),
            (Some(depth * 32), proof_bytes(&self.next_sync_committee_branch, depth)?),
            (header_width(&self.finalized_header), self.finalized_header.to_ssz(preset)?),
            (Some(finality_depth * 32), proof_bytes(&self.finality_branch, finality_depth)?),
            (Some(committee_size(preset) / 8 + 96), self.sync_aggregate.to_ssz(preset)?),
            (Some(8), self.signature_slot.to_le_bytes().to_vec()),
        ])?)
    }
    /// Complete seven-field container root with exact branch widths.
    pub fn hash_tree_root(&self, preset: Preset) -> Result<Root, LightClientError> {
        let (depth, finality_depth) = branch_depths(self.schema)?;
        check_header(&self.attested_header, self.schema)?;
        check_header(&self.finalized_header, self.schema)?;
        Ok(merkleize(
            &[
                self.attested_header.hash_tree_root(preset)?,
                self.next_sync_committee.hash_tree_root(preset)?,
                proof_root(&self.next_sync_committee_branch, depth)?,
                self.finalized_header.hash_tree_root(preset)?,
                proof_root(&self.finality_branch, finality_depth)?,
                self.sync_aggregate.hash_tree_root(preset)?,
                uint64_root(self.signature_slot),
            ],
            7,
        )?)
    }
}
impl FinalityUpdate {
    /// Encode the exact compact finality container without manufacturing a validation verdict.
    pub fn to_ssz(&self, preset: Preset) -> Result<Vec<u8>, LightClientError> {
        let (_, depth) = branch_depths(self.schema)?;
        check_header(&self.attested_header, self.schema)?;
        check_header(&self.finalized_header, self.schema)?;
        Ok(encode_container(&[
            (header_width(&self.attested_header), self.attested_header.to_ssz(preset)?),
            (header_width(&self.finalized_header), self.finalized_header.to_ssz(preset)?),
            (Some(depth * 32), proof_bytes(&self.finality_branch, depth)?),
            (Some(committee_size(preset) / 8 + 96), self.sync_aggregate.to_ssz(preset)?),
            (Some(8), self.signature_slot.to_le_bytes().to_vec()),
        ])?)
    }
    /// Exact five-field compact finality container root.
    pub fn hash_tree_root(&self, preset: Preset) -> Result<Root, LightClientError> {
        let (_, depth) = branch_depths(self.schema)?;
        check_header(&self.attested_header, self.schema)?;
        check_header(&self.finalized_header, self.schema)?;
        Ok(merkleize(
            &[
                self.attested_header.hash_tree_root(preset)?,
                self.finalized_header.hash_tree_root(preset)?,
                proof_root(&self.finality_branch, depth)?,
                self.sync_aggregate.hash_tree_root(preset)?,
                uint64_root(self.signature_slot),
            ],
            5,
        )?)
    }
}
impl OptimisticUpdate {
    /// Encode the exact compact optimistic container.
    pub fn to_ssz(&self, preset: Preset) -> Result<Vec<u8>, LightClientError> {
        branch_depths(self.schema)?;
        check_header(&self.attested_header, self.schema)?;
        Ok(encode_container(&[
            (header_width(&self.attested_header), self.attested_header.to_ssz(preset)?),
            (Some(committee_size(preset) / 8 + 96), self.sync_aggregate.to_ssz(preset)?),
            (Some(8), self.signature_slot.to_le_bytes().to_vec()),
        ])?)
    }
    /// Exact three-field compact optimistic container root.
    pub fn hash_tree_root(&self, preset: Preset) -> Result<Root, LightClientError> {
        branch_depths(self.schema)?;
        check_header(&self.attested_header, self.schema)?;
        Ok(merkleize(
            &[
                self.attested_header.hash_tree_root(preset)?,
                self.sync_aggregate.hash_tree_root(preset)?,
                uint64_root(self.signature_slot),
            ],
            3,
        )?)
    }
}
