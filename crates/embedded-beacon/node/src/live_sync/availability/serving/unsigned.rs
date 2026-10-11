//! Unsigned anchor sidecar serving retains its native context and never invents signed SSZ.

use super::*;

impl RetrievalAvailability {
    pub(super) async fn unsigned_serving_contexts(
        &self,
        importer: &ImportCoordinator,
        request: &SidecarRequest,
        blocks: &[reth_embedded_beacon_network::block_rpc::WireBlock],
    ) -> Result<Vec<UnsignedCustodyAnchor>, ImportError> {
        let mut roots = match request {
            SidecarRequest::ColumnRoots(ids) => {
                ids.iter().map(|id| id.block_root).collect::<BTreeSet<_>>()
            }
            SidecarRequest::ColumnRange { .. } => {
                let snapshot = importer.snapshot().await?;
                BTreeSet::from([snapshot.head.root, snapshot.finalized_checkpoint.root])
            }
            _ => return Ok(Vec::new()),
        };
        if matches!(request, SidecarRequest::ColumnRange { .. }) &&
            let Some(anchor) = importer.retained_unsigned_custody_anchor().await?
        {
            roots.insert(anchor.root());
        }
        let mut anchors = Vec::new();
        let mut bytes = blocks
            .iter()
            .try_fold(0usize, |total, block| total.checked_add(block.ssz().len()))
            .ok_or(ImportError::Bounds)?;
        for root in roots {
            if blocks.iter().any(|wire| wire.root() == root) {
                continue;
            }
            if let Some(anchor) = importer.unsigned_custody_anchor(root).await? {
                if requested_indices(request, &anchor).is_empty() {
                    continue;
                }
                bytes = bytes.checked_add(anchor.original_len()).ok_or(ImportError::Bounds)?;
                if bytes > MAX_BATCH_BYTES {
                    return Err(ImportError::Bounds);
                }
                anchors.push(anchor);
            }
        }
        Ok(anchors)
    }
}

pub(super) fn requested_indices(
    request: &SidecarRequest,
    anchor: &UnsignedCustodyAnchor,
) -> Vec<u64> {
    match request {
        SidecarRequest::ColumnRange { start_slot, count, columns }
            if anchor.slot().checked_sub(*start_slot).is_some_and(|delta| delta < *count) =>
        {
            columns.clone()
        }
        SidecarRequest::ColumnRoots(ids) => ids
            .iter()
            .filter(|id| id.block_root == anchor.root())
            .flat_map(|id| id.columns.iter().copied())
            .collect(),
        _ => Vec::new(),
    }
}
