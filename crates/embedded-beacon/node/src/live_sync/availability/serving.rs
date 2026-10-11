//! Physical archive reads acquire no consensus authority: original native owner selection and
//! every mathematical availability proof are independently checked again before peer serving.

use super::*;
use crate::import_coordinator::{
    ImportCoordinator, ImportError, ServingBatch, ServingRequest, UnsignedCustodyAnchor,
};
use reth_embedded_beacon_crypto::{compute_domain, compute_signing_root, verify_signature};
use reth_embedded_beacon_network::{
    availability_rpc::{SidecarKind, WireSidecar},
    block_rpc::{BlockRequest, MAX_BATCH_BYTES, MAX_BLOCK_BATCH},
};
use reth_embedded_beacon_state_transition::ssz::{
    deneb as deneb_block, electra as electra_block, SignedBeaconBlock,
};
use std::collections::BTreeSet;

mod unsigned;

impl RetrievalAvailability {
    pub(crate) fn serve_sidecars(
        &self,
        importer: ImportCoordinator,
        request: SidecarRequest,
    ) -> impl Future<Output = Result<Option<ServingBatch>, ImportError>> + Send + 'static {
        let availability = self.clone();
        async move {
            if request.validate_outbound().is_err() {
                return Ok(None)
            }
            let Some(archive) = availability.archive.clone() else { return Ok(None) };
            let blocks_request = match &request {
                SidecarRequest::BlobRange { start_slot, count } |
                SidecarRequest::ColumnRange { start_slot, count, .. } => {
                    BlockRequest::Range { start_slot: *start_slot, count: *count }
                }
                SidecarRequest::BlobRoots(ids) => BlockRequest::Roots(
                    ids.iter()
                        .map(|id| id.block_root)
                        .collect::<BTreeSet<_>>()
                        .into_iter()
                        .collect(),
                ),
                SidecarRequest::ColumnRoots(ids) => BlockRequest::Roots(
                    ids.iter()
                        .map(|id| id.block_root)
                        .collect::<BTreeSet<_>>()
                        .into_iter()
                        .collect(),
                ),
            };
            if blocks_request.validate_outbound().is_err() {
                return Ok(None)
            }
            let Some(ServingBatch::Blocks(blocks)) =
                importer.serve(ServingRequest::Blocks(blocks_request)).await?
            else {
                return Ok(None)
            };
            let anchors =
                availability.unsigned_serving_contexts(&importer, &request, &blocks).await?;
            if blocks.len().checked_add(anchors.len()).is_none_or(|count| count > MAX_BLOCK_BATCH) {
                return Err(ImportError::Bounds)
            }
            let Ok(permit) = availability.proofs.clone().try_acquire_owned() else {
                return Ok(None)
            };
            let config = availability.config.clone();
            tokio::task::spawn_blocking(move || {
                let _permit = permit;
                let mut retained = 0usize;
                let mut output = Vec::new();
                for wire in blocks {
                    let column = matches!(
                        &request,
                        SidecarRequest::ColumnRange { .. } | SidecarRequest::ColumnRoots(_)
                    );
                    if column && wire.fork() < Fork::Fulu ||
                        !column && !matches!(wire.fork(), Fork::Deneb | Fork::Electra)
                    {
                        continue
                    }
                    let indices = match &request {
                        SidecarRequest::BlobRange { .. } => (0..config
                            .blob_parameters(wire.slot() / config.slots_per_epoch)
                            .map_err(|_| ImportError::Configuration)?
                            .max_blobs_per_block)
                            .collect::<Vec<_>>(),
                        SidecarRequest::BlobRoots(ids) => ids
                            .iter()
                            .filter(|id| id.block_root == wire.root())
                            .map(|id| id.index)
                            .collect(),
                        SidecarRequest::ColumnRange { columns, .. } => columns.clone(),
                        SidecarRequest::ColumnRoots(ids) => ids
                            .iter()
                            .filter(|id| id.block_root == wire.root())
                            .flat_map(|id| id.columns.iter().copied())
                            .collect(),
                    };
                    let mut sidecars = Vec::new();
                    for index in indices {
                        if let Some(sidecar) = archive
                            .read(wire.root(), wire.slot(), index, column)
                            .map_err(retention_error)?
                        {
                            retained = retained
                                .checked_add(sidecar.ssz().len())
                                .ok_or(ImportError::Bounds)?;
                            if retained > MAX_BATCH_BYTES {
                                return Ok(None)
                            }
                            sidecars.push(sidecar);
                        }
                    }
                    if sidecars.is_empty() {
                        continue
                    }
                    let signed =
                        SignedBeaconBlock::from_ssz(wire.ssz(), wire.fork(), preset(&config))
                            .map_err(|_| {
                                ImportError::PublicationInfrastructure(
                                    "retained original sidecar block failed native decoding".into(),
                                )
                            })?;
                    verify_original(&signed, wire.root(), &sidecars, &config)
                        .map_err(retention_error)?;
                    output.extend(sidecars);
                }
                for anchor in anchors {
                    let indices = unsigned::requested_indices(&request, &anchor);
                    let mut sidecars = Vec::new();
                    for index in indices {
                        if let Some(wire) = archive
                            .read(anchor.root(), anchor.slot(), index, true)
                            .map_err(retention_error)?
                        {
                            retained = retained
                                .checked_add(wire.ssz().len())
                                .ok_or(ImportError::Bounds)?;
                            if retained > MAX_BATCH_BYTES {
                                return Ok(None)
                            }
                            sidecars.push(wire);
                        }
                    }
                    if !sidecars.is_empty() {
                        verify_unsigned(&anchor, &sidecars, &config).map_err(retention_error)?;
                        output.extend(sidecars);
                    }
                }
                output.sort_by_key(|wire| (wire.slot(), wire.index(), wire.block_root()));
                Ok(Some(ServingBatch::Sidecars(output)))
            })
            .await
            .map_err(|_| {
                ImportError::PublicationInfrastructure("retained sidecar proof task failed".into())
            })?
        }
    }
}

pub(super) fn retention_error(error: RetrievalError) -> ImportError {
    ImportError::PublicationInfrastructure(format!("retained sidecar revalidation failed: {error}"))
}

pub(crate) fn verify_original(
    signed: &SignedBeaconBlock,
    root: Root,
    wires: &[WireSidecar],
    config: &ChainConfig,
) -> Result<(), RetrievalError> {
    let preset = preset(config);
    if signed.message_root().map_err(|_| RetrievalError::Encoding)? != root {
        return Err(RetrievalError::Association)
    }
    match signed {
        SignedBeaconBlock::Deneb(block, _) => {
            verify_blob_original(root, &block.message, block.signature, wires, config, preset)
        }
        SignedBeaconBlock::Electra(block, _) => verify_electra_blob_original(
            root,
            &block.message,
            block.signature,
            wires,
            config,
            preset,
        ),
        SignedBeaconBlock::Fulu(block, _) => {
            let columns = wires.iter().map(WireSidecar::index).collect::<Vec<_>>();
            let sidecars = wires
                .iter()
                .map(|wire| {
                    if wire.kind() != SidecarKind::Column ||
                        wire.block_root() != root ||
                        wire.slot() != block.message.slot ||
                        wire.fork() != Fork::Fulu
                    {
                        return Err(RetrievalError::Association)
                    }
                    let sidecar = fulu::DataColumnSidecar::from_ssz(wire.ssz(), preset)
                        .map_err(|_| RetrievalError::Encoding)?;
                    if sidecar.signed_block_header.signature != block.signature {
                        return Err(RetrievalError::Association)
                    }
                    Ok(sidecar)
                })
                .collect::<Result<Vec<_>, RetrievalError>>()?;
            let maximum = config
                .blob_parameters(block.message.slot / config.slots_per_epoch)
                .map_err(|_| RetrievalError::Configuration)?
                .max_blobs_per_block as usize;
            verify_fulu_sampling_batch(
                ethereum_kzg_settings(0),
                root,
                &block.message,
                &sidecars,
                preset,
                maximum,
                &columns,
            )
            .map_err(|_| RetrievalError::Proof)
        }
        SignedBeaconBlock::Gloas(block, _) => {
            let columns = wires.iter().map(WireSidecar::index).collect::<Vec<_>>();
            let sidecars = wires
                .iter()
                .map(|wire| {
                    if wire.kind() != SidecarKind::Column ||
                        wire.block_root() != root ||
                        wire.slot() != block.message.slot ||
                        wire.fork() != Fork::Gloas
                    {
                        return Err(RetrievalError::Association)
                    }
                    gloas::DataColumnSidecar::from_ssz(wire.ssz(), preset)
                        .map_err(|_| RetrievalError::Encoding)
                })
                .collect::<Result<Vec<_>, RetrievalError>>()?;
            verify_gloas_sampling_batch(
                ethereum_kzg_settings(0),
                root,
                &block.message,
                &sidecars,
                preset,
                &columns,
            )
            .map_err(|_| RetrievalError::Proof)
        }
        _ => Err(RetrievalError::Association),
    }
}

macro_rules! blob_original {
    ($function:ident, $block:ident, $sidecar:ident, $fork:ident) => {
        fn $function(
            root: Root,
            block: &$block::BeaconBlock,
            signature: [u8; 96],
            wires: &[WireSidecar],
            _config: &ChainConfig,
            preset: Preset,
        ) -> Result<(), RetrievalError> {
            let mut commitments = Vec::new();
            let mut blobs = Vec::new();
            let mut proofs = Vec::new();
            for wire in wires {
                if wire.kind() != SidecarKind::Blob ||
                    wire.block_root() != root ||
                    wire.slot() != block.slot ||
                    wire.fork() != Fork::$fork
                {
                    return Err(RetrievalError::Association)
                }
                let sidecar = $sidecar::BlobSidecar::from_ssz(wire.ssz(), preset)
                    .map_err(|_| RetrievalError::Encoding)?;
                let index =
                    usize::try_from(sidecar.index).map_err(|_| RetrievalError::Association)?;
                if block.body.blob_kzg_commitments.values.get(index) !=
                    Some(&sidecar.kzg_commitment) ||
                    sidecar.signed_block_header.signature != signature ||
                    sidecar.signed_block_header.message.hash_tree_root() != root
                {
                    return Err(RetrievalError::Association)
                }
                verify_blob_inclusion(
                    sidecar.index,
                    sidecar.kzg_commitment,
                    &sidecar.kzg_commitment_inclusion_proof.values,
                    &sidecar.signed_block_header,
                )?;
                commitments.push(sidecar.kzg_commitment);
                blobs.push(sidecar.blob);
                proofs.push(sidecar.kzg_proof);
            }
            verify_blob_batch(ethereum_kzg_settings(0), &commitments, &blobs, &proofs)
                .map_err(|_| RetrievalError::Proof)
        }
    };
}
blob_original!(verify_blob_original, deneb_block, deneb, Deneb);
blob_original!(verify_electra_blob_original, electra_block, electra, Electra);

#[cfg(test)]
mod tests;

/// Fresh proof validation of the genuine unsigned native anchor, without synthesizing a block
/// signature.
pub(crate) fn verify_unsigned(
    anchor: &UnsignedCustodyAnchor,
    wires: &[WireSidecar],
    config: &ChainConfig,
) -> Result<(), RetrievalError> {
    if anchor.config_digest() !=
        checked_config_digest(config).map_err(|_| RetrievalError::Configuration)?
    {
        return Err(RetrievalError::Association)
    }
    let block = anchor.block().map_err(|_| RetrievalError::Association)?;
    let root = anchor.root();
    let required = wires.iter().map(WireSidecar::index).collect::<Vec<_>>();
    if required.is_empty() {
        return Err(RetrievalError::Missing)
    }
    let preset = preset(config);
    match block {
        BeaconBlock::Fulu(block, _) => {
            let key = anchor.proposer_key().ok_or(RetrievalError::Missing)?;
            let version =
                config.fork_at_slot(block.slot).map_err(|_| RetrievalError::Configuration)?.version;
            let signing_root = compute_signing_root(
                root,
                compute_domain([0; 4], version, config.genesis_validators_root),
            );
            let sidecars = wires
                .iter()
                .map(|wire| {
                    if wire.kind() != SidecarKind::Column ||
                        wire.block_root() != root ||
                        wire.slot() != block.slot ||
                        wire.fork() != Fork::Fulu
                    {
                        return Err(RetrievalError::Association)
                    }
                    let sidecar = fulu::DataColumnSidecar::from_ssz(wire.ssz(), preset)
                        .map_err(|_| RetrievalError::Encoding)?;
                    verify_signature(&key, &signing_root, &sidecar.signed_block_header.signature)
                        .map_err(|_| RetrievalError::Association)?;
                    Ok(sidecar)
                })
                .collect::<Result<Vec<_>, RetrievalError>>()?;
            let maximum = config
                .blob_parameters(block.slot / config.slots_per_epoch)
                .map_err(|_| RetrievalError::Configuration)?
                .max_blobs_per_block as usize;
            verify_fulu_sampling_batch(
                ethereum_kzg_settings(0),
                root,
                &block,
                &sidecars,
                preset,
                maximum,
                &required,
            )
            .map_err(|_| RetrievalError::Proof)
        }
        BeaconBlock::Gloas(block, _) => {
            let sidecars = wires
                .iter()
                .map(|wire| {
                    if wire.kind() != SidecarKind::Column ||
                        wire.block_root() != root ||
                        wire.slot() != block.slot ||
                        wire.fork() != Fork::Gloas
                    {
                        return Err(RetrievalError::Association)
                    }
                    gloas::DataColumnSidecar::from_ssz(wire.ssz(), preset)
                        .map_err(|_| RetrievalError::Encoding)
                })
                .collect::<Result<Vec<_>, RetrievalError>>()?;
            verify_gloas_sampling_batch(
                ethereum_kzg_settings(0),
                root,
                &block,
                &sidecars,
                preset,
                &required,
            )
            .map_err(|_| RetrievalError::Proof)
        }
        _ => Err(RetrievalError::Association),
    }
}
