//! Privately borrowed Gloas payload graph queries for the shared fast-confirmation algorithm.

use super::*;
use crate::fast_confirmation::{
    self, BlockInfo, FastConfirmationView, Message, ProcessingRef, StateRef,
};

impl fast_confirmation::sealed::Sealed for GloasStore {}
impl FastConfirmationView for GloasStore {
    const VERIFY_SIGNATURES: bool = true;
    fn parameters(&self) -> StoreParameters {
        self.parameters
    }
    fn processing(&self) -> ProcessingRef<'_> {
        self.historical
            .as_ref()
            .and_then(|history| history.transition_parameters())
            .map_or(ProcessingRef::Gloas(&self.processing), ProcessingRef::Scheduled)
    }
    fn current_slot(&self) -> u64 {
        Self::current_slot(self)
    }
    fn time_ms(&self) -> u64 {
        self.time_ms
    }
    fn genesis_time_ms(&self) -> u64 {
        Self::genesis_time_ms(self)
    }
    fn checkpoints(&self) -> (Checkpoint, Checkpoint) {
        (self.justified, self.finalized)
    }
    fn unrealized_justified(&self) -> Checkpoint {
        self.unrealized_justified
    }
    fn head(&self) -> Result<ForkChoiceNode, StoreError> {
        Self::get_head(self)
    }
    fn block_info(&self, root: Root) -> Result<BlockInfo, StoreError> {
        if let Some(block) = self.blocks.get(&root) {
            return Ok(BlockInfo {
                slot: block.slot,
                parent_root: block.parent_root,
                safe_execution_hash: Some(
                    block.body.signed_execution_payload_bid.message.parent_block_hash,
                ),
            })
        }
        let history = self.historical.as_ref().ok_or(StoreError::UnknownRoot)?;
        history.verified_execution_block_hash(root)?;
        FastConfirmationView::block_info(history.as_ref(), root)
    }
    fn state(&self, root: Root) -> Result<StateRef<'_>, StoreError> {
        if let Some(entry) = self.block_states.get(&root) {
            return Ok(StateRef::Gloas(entry.hot_required()?, self.processing.preset()))
        }
        FastConfirmationView::state(
            self.historical.as_ref().ok_or(StoreError::UnknownRoot)?.as_ref(),
            root,
        )
    }
    fn checkpoint_state(&self, checkpoint: Checkpoint) -> Result<StateRef<'_>, StoreError> {
        if let Some(entry) = self.checkpoint_states.get(&(checkpoint.epoch, checkpoint.root)) {
            return Ok(StateRef::Gloas(entry.hot_required()?, self.processing.preset()))
        }
        FastConfirmationView::checkpoint_state(
            self.historical.as_ref().ok_or(StoreError::UnknownRoot)?.as_ref(),
            checkpoint,
        )
    }
    fn latest(&self, index: u64) -> Result<Option<Message>, StoreError> {
        self.latest_messages
            .get(&index)
            .filter(|message| self.has_retained_message_root(message.root))
            .map(|&message| {
                Ok(Message {
                    epoch: message.slot / self.slots_per_epoch(),
                    root: message.root,
                    supported_node: self.get_supported_node(message)?,
                })
            })
            .transpose()
    }
    fn equivocating(&self, index: u64) -> bool {
        self.equivocating.contains(&index)
    }
    fn is_ancestor(
        &self,
        current: ForkChoiceNode,
        ancestor: ForkChoiceNode,
    ) -> Result<bool, StoreError> {
        Self::is_ancestor(self, current, ancestor)
    }
    fn ancestor(&self, current: ForkChoiceNode, slot: u64) -> Result<ForkChoiceNode, StoreError> {
        Self::get_ancestor(self, current, slot)
    }
    fn voting_source(&self, root: Root) -> Result<Checkpoint, StoreError> {
        Self::voting_source(self, root)
    }
    fn unrealized_justification(&self, root: Root) -> Result<Checkpoint, StoreError> {
        self.unrealized_justifications.get(&root).copied().ok_or(StoreError::UnknownRoot)
    }
    fn is_gloas(&self) -> bool {
        true
    }
    fn has_payload_branches(&self, root: Root) -> bool {
        self.blocks.contains_key(&root)
    }
}
