//! Fixture retrieval with real production blob and column proof verification.
//!
//! Supplied bytes and proofs are never converted into a caller-asserted validity flag.
//! The full-column retrieval policy here is confined to the published fixture context.

use super::{fixed_hex, fork_choice_fixture_path, read_snappy_ssz, Case};
use c_kzg::ethereum_kzg_settings;
use reth_embedded_beacon_data_availability::admission::{
    verify_blob_batch, verify_fulu_sampling_batch,
};
use reth_embedded_beacon_fork_choice::data_availability::DataAvailability;
use reth_embedded_beacon_primitives::{Fork, Root};
use reth_embedded_beacon_state_transition::{
    fulu_processing::FuluParameters,
    ssz::{
        auxiliary::{deneb::Blob, fulu::DataColumnSidecar},
        BeaconBlock, Preset, Ssz,
    },
};
use serde_yaml::Value;

pub(super) struct FixtureAvailability {
    blobs: Result<Option<Vec<Blob>>, String>,
    proofs: Result<Option<Vec<[u8; 48]>>, String>,
    columns: Result<Option<Vec<DataColumnSidecar>>, String>,
    parameters: FuluParameters,
}

impl FixtureAvailability {
    pub(super) fn new(
        case: &Case,
        step: &Value,
        preset: Preset,
        parameters: FuluParameters,
        max_bytes: usize,
    ) -> Self {
        Self {
            blobs: read_blobs(case, &step["blobs"], preset, max_bytes),
            proofs: read_proofs(&step["proofs"]),
            columns: read_columns(case, &step["columns"], preset, max_bytes),
            parameters,
        }
    }

    fn verify_inner(&self, root: Root, block: &BeaconBlock) -> Result<(), String> {
        if block.schema_fork() == Fork::Fulu {
            let BeaconBlock::Fulu(block, preset) = block else {
                return Err("unexpected Fulu block schema".into());
            };
            let columns = self.columns.as_ref().map_err(Clone::clone)?;
            if !block.body.blob_kzg_commitments.values.is_empty() && columns.is_none() {
                return Err("required column retrieval is unavailable".into());
            }
            let required = (0..128).collect::<Vec<_>>();
            let max_blobs = usize::try_from(
                self.parameters
                    .blob_parameters_at_epoch(block.slot / self.parameters.slots_per_epoch())
                    .max_blobs_per_block,
            )
            .map_err(|_| "blob limit does not fit resource bounds")?;
            verify_fulu_sampling_batch(
                ethereum_kzg_settings(0),
                root,
                block,
                columns.as_deref().unwrap_or(&[]),
                *preset,
                max_blobs,
                &required,
            )
            .map_err(|error| error.to_string())
        } else {
            let commitments = match block {
                BeaconBlock::Deneb(block, _) => &block.body.blob_kzg_commitments.values,
                BeaconBlock::Electra(block, _) => &block.body.blob_kzg_commitments.values,
                _ => return Ok(()),
            };
            let blobs = self.blobs.as_ref().map_err(Clone::clone)?;
            let proofs = self.proofs.as_ref().map_err(Clone::clone)?;
            if !commitments.is_empty() && (blobs.is_none() || proofs.is_none()) {
                return Err("required blob retrieval is unavailable".into());
            }
            verify_blob_batch(
                ethereum_kzg_settings(0),
                commitments,
                blobs.as_deref().unwrap_or(&[]),
                proofs.as_deref().unwrap_or(&[]),
            )
            .map_err(|error| error.to_string())
        }
    }
}

impl DataAvailability for FixtureAvailability {
    type Error = std::io::Error;
    async fn verify(&self, root: Root, block: &BeaconBlock) -> Result<(), Self::Error> {
        self.verify_inner(root, block).map_err(std::io::Error::other)
    }
}

fn read_blobs(
    case: &Case,
    value: &Value,
    preset: Preset,
    max_bytes: usize,
) -> Result<Option<Vec<Blob>>, String> {
    if value.is_null() {
        return Ok(None);
    }
    let bytes = read_snappy_ssz(&fork_choice_fixture_path(case, value)?, max_bytes)?;
    if !bytes.len().is_multiple_of(131_072) || bytes.len() / 131_072 > 4096 {
        return Err("blob batch is not canonical bounded fixed-element SSZ".into());
    }
    bytes
        .chunks_exact(131_072)
        .map(|bytes| Blob::from_ssz(bytes, preset).map_err(|error| error.to_string()))
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
}

fn read_proofs(value: &Value) -> Result<Option<Vec<[u8; 48]>>, String> {
    if value.is_null() {
        return Ok(None);
    }
    let values = value.as_sequence().ok_or("blob proofs must be a list")?;
    if values.len() > 4096 {
        return Err("blob proof count exceeds resource limit".into());
    }
    values.iter().map(fixed_hex::<48>).collect::<Result<Vec<_>, _>>().map(Some)
}

fn read_columns(
    case: &Case,
    value: &Value,
    preset: Preset,
    max_bytes: usize,
) -> Result<Option<Vec<DataColumnSidecar>>, String> {
    if value.is_null() {
        return Ok(None);
    }
    let names = value.as_sequence().ok_or("column retrieval must be a list")?;
    if names.len() > 128 {
        return Err("column retrieval exceeds 128 columns".into());
    }
    names
        .iter()
        .map(|name| {
            let bytes = read_snappy_ssz(&fork_choice_fixture_path(case, name)?, max_bytes)?;
            DataColumnSidecar::from_ssz(&bytes, preset).map_err(|error| error.to_string())
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
}
