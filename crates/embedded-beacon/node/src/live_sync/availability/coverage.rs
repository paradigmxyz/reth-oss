//! Incremental proof-checked custody coverage of actual selected native ancestry.
//!
//! Reopen starts empty. Neither archive indexes nor stored selected hints can authorize custody.
//! A missing original or column leaves coverage incomplete without rejecting a remote block.

use super::*;
use crate::import_coordinator::{ImportCoordinator, ImportError, ServingBatch, ServingRequest};
use reth_embedded_beacon_network::block_rpc::BlockRequest;
use reth_embedded_beacon_state_transition::ssz::SignedBeaconBlock;
use std::collections::BTreeMap;

const BLOCKS_PER_TURN: usize = 16;
const RETENTION_EPOCHS: u64 = 4096;

/// Private receipt derived only from native selected ancestry and actual retained proofs.
#[derive(Clone, Copy, Debug)]
pub(crate) struct CustodyReceipt {
    config_digest: Root,
    node_id: Root,
    head: Root,
    earliest_available_slot: u64,
}
impl CustodyReceipt {
    pub(crate) fn config_and_head(&self, digest: Root, head: Root) -> bool {
        self.config_digest == digest && self.head == head
    }
    pub(crate) const fn earliest(&self) -> u64 {
        self.earliest_available_slot
    }

    pub(crate) fn matches(&self, digest: Root, node_id: Root, head: Root) -> bool {
        self.config_digest == digest && self.node_id == node_id && self.head == head
    }
}

#[derive(Debug, Default)]
pub(super) struct CustodyCoverage {
    head: Option<Root>,
    cursor: Option<Root>,
    last_slot: Option<u64>,
    checked: BTreeMap<Root, CheckedBlock>,
    path: Vec<Root>,
    complete_floor: Option<u64>,
    serving_floor: Option<u64>,
    unsigned_gap: Option<u64>,
    genuine_genesis_terminal: bool,
}
#[derive(Clone, Copy, Debug)]
struct CheckedBlock {
    slot: u64,
    parent: Root,
    covered_floor: Option<u64>,
    minimum_serving_slot: u64,
}

impl RetrievalAvailability {
    /// Perform one finite turn, keeping cryptography and archive IO off executor threads.
    pub(crate) async fn custody_coverage(
        &self,
        importer: ImportCoordinator,
        head: Root,
        current_slot: u64,
    ) -> Result<Option<CustodyReceipt>, ImportError> {
        let Some(archive) = self.archive.clone() else { return Ok(None) };
        let selector = archive.clone();
        let selection = tokio::task::spawn_blocking(move || selector.selection())
            .await
            .map_err(|_| ImportError::Closed)?
            .map_err(serving::retention_error)?;
        let Some((node_id, columns)) = selection else { return Ok(None) };
        let floor = (current_slot / self.config.slots_per_epoch).saturating_sub(RETENTION_EPOCHS) *
            self.config.slots_per_epoch;

        {
            let mut coverage = self.coverage.lock().map_err(|_| ImportError::Closed)?;
            coverage.checked.retain(|_, block| block.slot >= floor);
            if coverage.head != Some(head) {
                coverage.head = Some(head);
                coverage.cursor = Some(head);
                coverage.last_slot = None;
                coverage.path.clear();
                coverage.complete_floor = None;
                coverage.serving_floor = None;
                coverage.unsigned_gap = None;
                coverage.genuine_genesis_terminal = false;
            }
            let terminal_floor =
                if coverage.genuine_genesis_terminal { floor.max(1) } else { floor };
            if coverage.complete_floor.is_some_and(|prior| prior <= terminal_floor) {
                coverage.complete_floor = Some(terminal_floor);
                return Ok(Some(CustodyReceipt {
                    config_digest: self.digest,
                    node_id,
                    head,
                    earliest_available_slot: terminal_floor,
                }));
            }
        }
        for _ in 0..BLOCKS_PER_TURN {
            let (root, last, cached) = {
                let coverage = self.coverage.lock().map_err(|_| ImportError::Closed)?;
                let Some(root) = coverage.cursor else { return Ok(None) };
                (root, coverage.last_slot, coverage.checked.get(&root).copied())
            };
            if root == [0; 32] {
                if self.acquire_historical_parent(&importer).await? {
                    self.restart_coverage(head)?;
                }
                return Ok(None);
            }
            if let Some(block) = cached {
                if block.minimum_serving_slot > block.slot {
                    let original = importer
                        .serve(ServingRequest::Blocks(BlockRequest::Roots(vec![root])))
                        .await?;
                    if matches!(original, Some(ServingBatch::Blocks(ref blocks)) if blocks.len()==1 && blocks[0].root()==root && blocks[0].slot()==block.slot)
                    {
                        let mut coverage = self.coverage.lock().map_err(|_| ImportError::Closed)?;
                        // Clear this gap only after the owner freshly serves the genuine signed
                        // original. A pin, archive index or public verdict is insufficient.
                        coverage.checked.remove(&root);
                        coverage.cursor = Some(head);
                        coverage.last_slot = None;
                        coverage.path.clear();
                        coverage.complete_floor = None;
                        coverage.serving_floor = None;
                        coverage.unsigned_gap = None;
                        continue;
                    }
                }
                if last.is_some_and(|slot| block.slot >= slot) {
                    return Err(ImportError::Wire);
                }
                if block.covered_floor.is_some_and(|prior| prior <= floor) {
                    self.complete_coverage(floor)?;
                    return Ok(Some(CustodyReceipt {
                        config_digest: self.digest,
                        node_id,
                        head,
                        earliest_available_slot: self.coverage_floor(floor)?,
                    }));
                }
                {
                    let mut coverage = self.coverage.lock().map_err(|_| ImportError::Closed)?;
                    coverage.serving_floor = Some(block.minimum_serving_slot);
                    if block.minimum_serving_slot > block.slot {
                        coverage.unsigned_gap = Some(
                            coverage.unsigned_gap.unwrap_or(0).max(block.minimum_serving_slot),
                        );
                    }
                }
                self.advance_coverage(root, block)?;
                if block.slot <= floor {
                    self.complete_coverage(floor)?;
                    return Ok(None);
                }
                continue;
            }
            let Some(ServingBatch::Blocks(mut blocks)) =
                importer.serve(ServingRequest::Blocks(BlockRequest::Roots(vec![root]))).await?
            else {
                return Ok(None);
            };
            if blocks.len() > 1 {
                return Err(ImportError::Bounds);
            }
            let Some(wire) = blocks.pop() else {
                let Some(anchor) = importer.unsigned_custody_anchor(root).await? else {
                    if self.acquire_historical_parent(&importer).await? {
                        // Revisit cached unsigned contexts only through a fresh owner serve.
                        // Acquisition alone must not clear any coverage gap or proof seal.
                        self.restart_coverage(head)?;
                    }
                    return Ok(None);
                };
                {
                    let mut coverage = self.coverage.lock().map_err(|_| ImportError::Closed)?;
                    // Original unsigned checkpoints cannot be served as signed blocks.
                    coverage.unsigned_gap = Some(anchor.slot().saturating_add(1));
                }
                let block = anchor.block()?;
                let header = block.header().map_err(|_| ImportError::Wire)?;
                if last.is_some_and(|slot| header.slot >= slot) {
                    return Err(ImportError::Wire);
                }
                if header.slot < floor || empty_columns(&block) {
                    if anchor.is_genuine_genesis() {
                        // This sealed original was reconstructed from complete native genesis.
                        // Its actual empty body ends ancestry without fabricating signed bytes.
                        self.coverage
                            .lock()
                            .map_err(|_| ImportError::Closed)?
                            .genuine_genesis_terminal = true;
                    }
                    let checked = CheckedBlock {
                        slot: header.slot,
                        parent: header.parent_root,
                        covered_floor: None,
                        minimum_serving_slot: header.slot.saturating_add(1),
                    };
                    self.advance_coverage(root, checked)?;
                    if header.slot <= floor ||
                        header.parent_root == [0; 32] ||
                        block.schema_fork() < Fork::Fulu
                    {
                        self.complete_coverage(floor)?;
                        return Ok(Some(CustodyReceipt {
                            config_digest: self.digest,
                            node_id,
                            head,
                            earliest_available_slot: self.coverage_floor(floor)?,
                        }));
                    }
                    continue;
                }
                let Ok(permit) = self.proofs.clone().try_acquire_owned() else { return Ok(None) };
                let config = self.config.clone();
                let archive = archive.clone();
                let acquisition_anchor = anchor.clone();
                let proven = tokio::task::spawn_blocking(move || {
                    let _permit = permit;
                    let mut sidecars = Vec::with_capacity(4);
                    for column in columns {
                        let Some(sidecar) = archive.read(root, header.slot, column, true)? else {
                            return Ok(false);
                        };
                        sidecars.push(sidecar);
                    }
                    serving::verify_unsigned(&anchor, &sidecars, &config)?;
                    Ok::<_, RetrievalError>(true)
                })
                .await
                .map_err(|_| ImportError::Closed)?
                .map_err(serving::retention_error)?;
                if !proven {
                    self.acquire_anchor_columns(&importer, acquisition_anchor, columns).await?;
                    return Ok(None);
                }
                let checked = CheckedBlock {
                    slot: header.slot,
                    parent: header.parent_root,
                    covered_floor: None,
                    minimum_serving_slot: header.slot.saturating_add(1),
                };
                self.advance_coverage(root, checked)?;
                if header.slot <= floor || header.parent_root == [0; 32] {
                    self.complete_coverage(floor)?;
                    return Ok(Some(CustodyReceipt {
                        config_digest: self.digest,
                        node_id,
                        head,
                        earliest_available_slot: self.coverage_floor(floor)?,
                    }));
                }
                continue;
            };
            if wire.root() != root || last.is_some_and(|slot| wire.slot() >= slot) {
                return Err(ImportError::Wire);
            }
            let block = CheckedBlock {
                slot: wire.slot(),
                parent: wire.parent_root(),
                covered_floor: None,
                minimum_serving_slot: wire.slot(),
            };
            if wire.slot() < floor {
                self.complete_coverage(floor)?;
                return Ok(Some(CustodyReceipt {
                    config_digest: self.digest,
                    node_id,
                    head,
                    earliest_available_slot: self.coverage_floor(floor)?,
                }));
            }
            let Ok(permit) = self.proofs.clone().try_acquire_owned() else { return Ok(None) };
            let config = self.config.clone();
            let archive = archive.clone();
            let acquisition_wire = wire.clone();
            let proven = tokio::task::spawn_blocking(move || {
                let _permit = permit;
                let signed = SignedBeaconBlock::from_ssz(wire.ssz(), wire.fork(), preset(&config))
                    .map_err(|_| RetrievalError::Encoding)?;
                let (indices, column) = required_sidecars(&signed, columns);
                if indices.is_empty() {
                    return Ok(true);
                }
                let mut sidecars = Vec::with_capacity(indices.len());
                for index in indices {
                    let Some(sidecar) = archive.read(root, wire.slot(), index, column)? else {
                        return Ok(false);
                    };
                    sidecars.push(sidecar);
                }
                serving::verify_original(&signed, root, &sidecars, &config)?;
                Ok::<_, RetrievalError>(true)
            })
            .await
            .map_err(|_| ImportError::Closed)?
            .map_err(serving::retention_error)?;
            if !proven {
                self.acquire_signed_columns(&importer, acquisition_wire, columns).await?;
                return Ok(None);
            }
            self.coverage.lock().map_err(|_| ImportError::Closed)?.serving_floor = Some(block.slot);
            self.advance_coverage(root, block)?;
            if block.slot <= floor ||
                block.parent == [0; 32] ||
                self.config
                    .fork_at_slot(block.slot)
                    .map_err(|_| ImportError::Configuration)?
                    .fork <
                    Fork::Deneb
            {
                self.complete_coverage(floor)?;
                return Ok(Some(CustodyReceipt {
                    config_digest: self.digest,
                    node_id,
                    head,
                    earliest_available_slot: self.coverage_floor(floor)?,
                }));
            }
        }
        Ok(None)
    }
    pub(crate) async fn custody_startup(
        &self,
        head: Root,
        head_slot: u64,
        current_slot: u64,
    ) -> Result<Option<CustodyReceipt>, ImportError> {
        let Some(archive) = self.archive.clone() else { return Ok(None) };
        let selection = tokio::task::spawn_blocking(move || archive.selection())
            .await
            .map_err(|_| ImportError::Closed)?
            .map_err(serving::retention_error)?;
        let Some((node_id, _columns)) = selection else { return Ok(None) };
        let coverage = self.coverage.lock().map_err(|_| ImportError::Closed)?;
        let floor = (current_slot / self.config.slots_per_epoch).saturating_sub(RETENTION_EPOCHS) *
            self.config.slots_per_epoch;
        let earliest_available_slot = if coverage.head == Some(head) {
            coverage
                .complete_floor
                .or(coverage.serving_floor)
                .unwrap_or_else(|| head_slot.saturating_add(1))
                .max(coverage.unsigned_gap.unwrap_or(0))
        } else {
            head_slot.saturating_add(1)
        }
        .max(floor);
        Ok(Some(CustodyReceipt {
            config_digest: self.digest,
            node_id,
            head,
            earliest_available_slot,
        }))
    }
    fn coverage_floor(&self, floor: u64) -> Result<u64, ImportError> {
        let coverage = self.coverage.lock().map_err(|_| ImportError::Closed)?;
        Ok(floor.max(coverage.unsigned_gap.unwrap_or(0)))
    }
    fn restart_coverage(&self, head: Root) -> Result<(), ImportError> {
        let mut coverage = self.coverage.lock().map_err(|_| ImportError::Closed)?;
        coverage.cursor = Some(head);
        coverage.last_slot = None;
        coverage.path.clear();
        coverage.complete_floor = None;
        Ok(())
    }
    fn advance_coverage(&self, root: Root, block: CheckedBlock) -> Result<(), ImportError> {
        let mut coverage = self.coverage.lock().map_err(|_| ImportError::Closed)?;
        let limit = RETENTION_EPOCHS
            .checked_add(2)
            .and_then(|epochs| epochs.checked_mul(self.config.slots_per_epoch))
            .ok_or(ImportError::Bounds)?;
        if coverage.path.len() as u64 >= limit {
            return Err(ImportError::Bounds);
        }
        if coverage.checked.len() as u64 >= limit * 2 && !coverage.checked.contains_key(&root) {
            return Err(ImportError::Bounds);
        }
        coverage.checked.insert(root, block);
        coverage.path.push(root);
        coverage.cursor = Some(block.parent);
        coverage.last_slot = Some(block.slot);
        Ok(())
    }
    fn complete_coverage(&self, floor: u64) -> Result<(), ImportError> {
        let mut coverage = self.coverage.lock().map_err(|_| ImportError::Closed)?;
        let floor = floor.max(coverage.unsigned_gap.unwrap_or(0));
        let path = std::mem::take(&mut coverage.path);
        for root in path {
            if let Some(block) = coverage.checked.get_mut(&root) {
                block.covered_floor = Some(floor);
            }
        }
        coverage.complete_floor = Some(floor.max(coverage.unsigned_gap.unwrap_or(0)));
        Ok(())
    }
}

const fn empty_columns(block: &BeaconBlock) -> bool {
    match block {
        BeaconBlock::Fulu(block, _) => block.body.blob_kzg_commitments.values.is_empty(),
        BeaconBlock::Gloas(block, _) => {
            block.body.signed_execution_payload_bid.message.blob_kzg_commitments.values.is_empty()
        }
        _ => true,
    }
}
const fn empty_signed_columns(block: &SignedBeaconBlock) -> bool {
    match block {
        SignedBeaconBlock::Fulu(block, _) => {
            block.message.body.blob_kzg_commitments.values.is_empty()
        }
        SignedBeaconBlock::Gloas(block, _) => block
            .message
            .body
            .signed_execution_payload_bid
            .message
            .blob_kzg_commitments
            .values
            .is_empty(),
        _ => true,
    }
}

pub(super) fn required_sidecars(signed: &SignedBeaconBlock, columns: [u64; 4]) -> (Vec<u64>, bool) {
    match signed {
        SignedBeaconBlock::Deneb(block, _) => {
            ((0..block.message.body.blob_kzg_commitments.values.len() as u64).collect(), false)
        }
        SignedBeaconBlock::Electra(block, _) => {
            ((0..block.message.body.blob_kzg_commitments.values.len() as u64).collect(), false)
        }
        _ if !empty_signed_columns(signed) => (columns.to_vec(), true),
        _ => (Vec::new(), true),
    }
}
