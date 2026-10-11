//! Privately borrowed classic graph queries for the shared fast-confirmation algorithm.

use super::*;
use crate::fast_confirmation::{
    self, BlockInfo, FastConfirmationView, Message, ProcessingRef, StateRef,
};

impl fast_confirmation::sealed::Sealed for Store {}
impl FastConfirmationView for Store {
    const VERIFY_SIGNATURES: bool = true;
    fn parameters(&self) -> StoreParameters {
        self.parameters
    }
    fn processing(&self) -> ProcessingRef<'_> {
        self.transition_parameters.as_ref().map_or(ProcessingRef::Legacy, ProcessingRef::Scheduled)
    }
    fn current_slot(&self) -> u64 {
        Self::current_slot(self)
    }
    fn time_ms(&self) -> u64 {
        Self::time_ms(self)
    }
    fn genesis_time_ms(&self) -> u64 {
        Self::genesis_time_ms(self)
    }
    fn checkpoints(&self) -> (Checkpoint, Checkpoint) {
        Self::checkpoints(self)
    }
    fn unrealized_justified(&self) -> Checkpoint {
        self.unrealized_justified
    }
    fn head(&self) -> Result<ForkChoiceNode, StoreError> {
        Self::get_head(self)
    }
    fn block_info(&self, root: Root) -> Result<BlockInfo, StoreError> {
        let block = self.blocks.get(&root).ok_or(StoreError::UnknownRoot)?;
        let header = block.header()?;
        let safe_execution_hash = match block.as_ref() {
            BeaconBlock::Phase0(..) | BeaconBlock::Altair(..) => None,
            BeaconBlock::Bellatrix(block, _) => Some(block.body.execution_payload.block_hash),
            BeaconBlock::Capella(block, _) => Some(block.body.execution_payload.block_hash),
            BeaconBlock::Deneb(block, _) => Some(block.body.execution_payload.block_hash),
            BeaconBlock::Electra(block, _) => Some(block.body.execution_payload.block_hash),
            BeaconBlock::Fulu(block, _) => Some(block.body.execution_payload.block_hash),
            BeaconBlock::Gloas(block, _) => {
                Some(block.body.signed_execution_payload_bid.message.parent_block_hash)
            }
        };
        Ok(BlockInfo { slot: header.slot, parent_root: header.parent_root, safe_execution_hash })
    }
    fn state(&self, root: Root) -> Result<StateRef<'_>, StoreError> {
        Ok(StateRef::Classic(
            self.block_states.get(&root).ok_or(StoreError::UnknownRoot)?.hot_required()?,
        ))
    }
    fn checkpoint_state(&self, checkpoint: Checkpoint) -> Result<StateRef<'_>, StoreError> {
        Ok(StateRef::Classic(
            self.checkpoint_states
                .get(&(checkpoint.epoch, checkpoint.root))
                .ok_or(StoreError::UnknownRoot)?
                .hot_required()?,
        ))
    }
    fn latest(&self, index: u64) -> Result<Option<Message>, StoreError> {
        Ok(self
            .kernel
            .latest_messages
            .get(&index)
            .filter(|message| self.kernel.blocks.contains_key(&message.root))
            .map(|message| Message {
                epoch: message.epoch,
                root: message.root,
                supported_node: ForkChoiceNode { root: message.root, payload_status: None },
            }))
    }
    fn equivocating(&self, index: u64) -> bool {
        self.kernel.equivocating.contains(&index)
    }
    fn is_ancestor(
        &self,
        current: ForkChoiceNode,
        ancestor: ForkChoiceNode,
    ) -> Result<bool, StoreError> {
        Ok(self.kernel.is_ancestor(current.root, ancestor.root)?)
    }
    fn ancestor(&self, current: ForkChoiceNode, slot: u64) -> Result<ForkChoiceNode, StoreError> {
        Ok(ForkChoiceNode { root: self.kernel.ancestor(current.root, slot)?, payload_status: None })
    }
    fn voting_source(&self, root: Root) -> Result<Checkpoint, StoreError> {
        let record = self.kernel.blocks.get(&root).ok_or(StoreError::UnknownRoot)?;
        Ok(if self.current_slot() / self.slots_per_epoch() > record.slot / self.slots_per_epoch() {
            record.unrealized_justified
        } else {
            record.justified
        })
    }
    fn unrealized_justification(&self, root: Root) -> Result<Checkpoint, StoreError> {
        Ok(self.kernel.blocks.get(&root).ok_or(StoreError::UnknownRoot)?.unrealized_justified)
    }
    fn is_gloas(&self) -> bool {
        false
    }
}
