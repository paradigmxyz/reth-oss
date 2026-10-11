//! Bounded catch-up ranges retain exact originals without reserving transport through import.
//!
//! Missing slots and partial responses remain retrieval hints. Only individually admitted
//! range members earn a forward cursor; fetched earlier ancestors cannot earn that progress.

use super::*;
use reth_embedded_beacon_network::block_rpc::{MAX_BATCH_BYTES, MAX_BLOCK_BATCH};

pub(super) fn catch_up_request(cursor: u64, head_slot: u64) -> Result<BlockRequest, LiveSyncError> {
    let remaining = head_slot.checked_sub(cursor).ok_or(LiveSyncError::Bounds)?;
    let count = remaining.saturating_add(1).min(MAX_BLOCK_BATCH as u64);
    cursor.checked_add(count).ok_or(LiveSyncError::Bounds)?;
    Ok(BlockRequest::Range { start_slot: cursor, count })
}

impl BranchRecovery {
    pub(super) fn from_range(peer: PeerId, originals: &[WireBlock]) -> Result<Self, LiveSyncError> {
        let first = originals.first().ok_or(LiveSyncError::Bounds)?;
        let last = originals.last().ok_or(LiveSyncError::Bounds)?;
        let count = last
            .slot()
            .checked_sub(first.slot())
            .and_then(|distance| distance.checked_add(1))
            .ok_or(LiveSyncError::Bounds)?;
        let request = BlockRequest::Range { start_slot: first.slot(), count };
        request.validate_outbound().map_err(|_| LiveSyncError::Bounds)?;
        request.matches(originals).map_err(|_| LiveSyncError::Response)?;
        let bytes = originals.iter().try_fold(0usize, |bytes, wire| {
            bytes.checked_add(wire.ssz().len()).ok_or(LiveSyncError::Bounds)
        })?;
        if bytes > MAX_BATCH_BYTES || bytes > MAX_ANCESTRY_BYTES {
            return Err(LiveSyncError::Bounds);
        }
        Ok(Self {
            peer,
            target_root: last.root(),
            next_root: Some(first.parent_root()),
            blocks: originals.iter().cloned().collect(),
            bytes,
            range_next_slot: Some(last.slot().checked_add(1).ok_or(LiveSyncError::Bounds)?),
            range_start_slot: Some(first.slot()),
        })
    }

    pub(super) fn range_next_slot_for(
        &self,
        wire: &WireBlock,
    ) -> Result<Option<u64>, LiveSyncError> {
        if self.range_start_slot.is_some_and(|start| wire.slot() >= start) &&
            self.range_next_slot.is_some_and(|end| wire.slot() < end)
        {
            return Ok(Some(wire.slot().checked_add(1).ok_or(LiveSyncError::Bounds)?));
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests;
