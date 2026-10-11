//! Read-only Gloas execution associations from private authenticated store data.
//!
//! PENDING grants no execution authority. FULL requires the actual envelope admitted by the
//! mandatory signed, Engine and availability path. EMPTY checks every retained parent decision.
//! The immutable constructor anchor can prove zero-hash absence only; it cannot supply a payload.
//! These methods do not import, relabel or mutate blocks, votes, states or checkpoints.

use super::*;

impl GloasStore {
    /// Resolve the selected node's execution hash using only retained verified payload authority.
    ///
    /// Unknown ancestry, missing delivery, malformed root/status or inconsistent commitments
    /// fail explicitly. PENDING and an exact authenticated zero anchor return absence.
    pub fn verified_execution_block_hash(
        &self,
        current: ForkChoiceNode,
    ) -> Result<Option<Root>, StoreError> {
        if let Some(history) = &self.historical &&
            history.block(current.root).is_some()
        {
            if current.payload_status.is_some() {
                return Err(StoreError::InvalidAttestationTarget)
            }
            return history.verified_execution_block_hash(current.root)
        }
        let block = self.block(current.root)?;
        match status(current)? {
            PayloadStatus::Pending => Ok(None),
            PayloadStatus::Full => self.execution_query_full(current.root).map(Some),
            PayloadStatus::Empty => self.execution_query_empty(current.root, block),
        }
    }

    /// Return the actual finalized beacon bid's committed parent execution association.
    ///
    /// Zero means absence. This specification-defined finalized association does not claim
    /// independent validation of unavailable EL ancestry; actual Engine dispatch still applies.
    pub fn finalized_execution_block_hash(&self) -> Result<Option<Root>, StoreError> {
        if let Some(history) = &self.historical &&
            history.block(self.finalized.root).is_some()
        {
            return history.verified_execution_block_hash(self.finalized.root)
        }
        let block = self.block(self.finalized.root)?;
        self.execution_query_state(self.finalized.root, block)?;
        let hash = block.body.signed_execution_payload_bid.message.parent_block_hash;
        Ok((hash != [0; 32]).then_some(hash))
    }

    fn execution_query_state(
        &self,
        root: Root,
        block: &BeaconBlock,
    ) -> Result<GloasStateRead, StoreError> {
        let state = self.read_block_state(root)?.ok_or(StoreError::UnknownRoot)?;
        let bid = &block.body.signed_execution_payload_bid.message;
        if state.slot != block.slot ||
            state.latest_block_header.slot != block.slot ||
            state.latest_block_header.parent_root != block.parent_root ||
            state.latest_block_header.proposer_index != block.proposer_index ||
            state.latest_execution_payload_bid != *bid ||
            state.latest_block_hash != bid.parent_block_hash
        {
            return Err(query_mismatch("retained block and post-state execution commitments differ"))
        }
        Ok(state)
    }

    fn execution_query_full(&self, root: Root) -> Result<Root, StoreError> {
        let block = self.block(root)?;
        self.execution_query_state(root, block)?;
        let envelope = self
            .payloads
            .get(&root)
            .ok_or_else(|| query_mismatch("full payload envelope is not locally verified"))?;
        let bid = &block.body.signed_execution_payload_bid.message;
        if envelope.beacon_block_root != root ||
            envelope.parent_beacon_block_root != block.parent_root ||
            envelope.payload.slot_number != block.slot ||
            envelope.payload.block_hash != bid.block_hash ||
            envelope.payload.parent_hash != bid.parent_block_hash
        {
            return Err(query_mismatch("verified envelope and retained beacon bid differ"))
        }
        if envelope.payload.block_hash == [0; 32] {
            return Err(query_mismatch("zero execution sentinel cannot establish a full payload"))
        }
        Ok(envelope.payload.block_hash)
    }

    fn execution_query_empty(
        &self,
        root: Root,
        block: &BeaconBlock,
    ) -> Result<Option<Root>, StoreError> {
        let count = self.block_count();
        let limit = count.min(self.parameters.max_blocks);
        if limit == 0 || count > self.parameters.max_blocks {
            return Err(StoreError::ResourceLimit)
        }
        let expected = block.body.signed_execution_payload_bid.message.parent_block_hash;
        let mut root = root;
        let mut block = block;
        for _ in 0..limit {
            let state = self.execution_query_state(root, block)?;
            let bid = &block.body.signed_execution_payload_bid.message;
            if bid.parent_block_hash != expected {
                return Err(query_mismatch("empty ancestry changes the committed execution parent"))
            }
            if Some(root) == self.execution_query_anchor &&
                bid.block_hash == [0; 32] &&
                bid.parent_block_hash == [0; 32] &&
                state.latest_block_hash == [0; 32]
            {
                return Ok(None)
            }
            if let Some(history) = &self.historical &&
                let Some(parent) = history.block(block.parent_root)
            {
                if parent.header()?.slot >= block.slot {
                    return Err(StoreError::InvalidBlockTime)
                }
                let hash = history.verified_execution_block_hash(block.parent_root)?;
                if hash.unwrap_or([0; 32]) != expected {
                    return Err(query_mismatch(
                        "verified historical parent differs from the committed execution parent",
                    ))
                }
                return Ok(hash)
            }
            let parent = self.block(block.parent_root)?;
            if parent.slot >= block.slot {
                return Err(StoreError::InvalidBlockTime)
            }
            let parent_bid = &parent.body.signed_execution_payload_bid.message;
            if bid.parent_block_hash == parent_bid.block_hash {
                let hash = self.execution_query_full(block.parent_root)?;
                if hash != expected {
                    return Err(query_mismatch(
                        "full ancestor differs from the committed execution parent",
                    ))
                }
                return Ok(Some(hash))
            }
            if bid.parent_block_hash != parent_bid.parent_block_hash {
                return Err(query_mismatch(
                    "empty parent decision does not preserve its execution parent",
                ))
            }
            root = block.parent_root;
            block = parent;
        }
        Err(StoreError::ResourceLimit)
    }
}

fn query_mismatch(message: &'static str) -> StoreError {
    StoreError::ExecutionImport(message.into())
}

#[cfg(test)]
mod tests;
