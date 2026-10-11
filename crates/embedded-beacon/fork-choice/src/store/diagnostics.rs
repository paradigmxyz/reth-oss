//! Read-only metadata explaining the strict kernel's voting-source filter.

use super::*;

/// Actual native voting-source constraints for a retained block.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlockVotingSource {
    /// Epoch used to select realized or unrealized justification.
    pub current_epoch: u64,
    /// Epoch of the candidate block.
    pub block_epoch: u64,
    /// Native realized justified checkpoint epoch.
    pub realized_justified_epoch: u64,
    /// Native unrealized justified checkpoint epoch.
    pub unrealized_justified_epoch: u64,
    /// Actual source epoch selected by the native viability rule.
    pub voting_source_epoch: u64,
    /// Native store justified checkpoint epoch.
    pub required_justified_epoch: u64,
    /// Whether this candidate satisfies the native leaf source and finalized-ancestry rules.
    /// This is not a statement that it wins LMD-GHOST or has no viable children.
    pub viable: bool,
}

impl Store {
    /// Inspect source constraints without loading a complete state or overriding head selection.
    pub fn block_voting_source(&self, root: Root) -> Result<Option<BlockVotingSource>, StoreError> {
        let Some(block) = self.kernel.blocks.get(&root) else { return Ok(None) };
        let current_epoch = self.kernel.current_slot / self.parameters.phase0.slots_per_epoch();
        let block_epoch = block.slot / self.parameters.phase0.slots_per_epoch();
        let source =
            if current_epoch > block_epoch { block.unrealized_justified } else { block.justified };
        Ok(Some(BlockVotingSource {
            current_epoch,
            block_epoch,
            realized_justified_epoch: block.justified.epoch,
            unrealized_justified_epoch: block.unrealized_justified.epoch,
            voting_source_epoch: source.epoch,
            required_justified_epoch: self.kernel.justified.epoch,
            viable: self.kernel.viable_leaf(*block)?,
        }))
    }
}
