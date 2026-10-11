//! Read-only participation checkpoint calculation for immutable, completely checked states.

use crate::{
    phase0_processing::{
        add, is_active, mul, position, Constants, ProcessingError, EFFECTIVE_BALANCE_INCREMENT,
    },
    ssz::phase0::Validator,
};
use reth_embedded_beacon_primitives::{Checkpoint, Root};

pub(crate) struct Inputs<'a> {
    pub(crate) slot: u64,
    pub(crate) validators: &'a [Validator],
    pub(crate) previous_participation: &'a [u8],
    pub(crate) current_participation: &'a [u8],
    pub(crate) block_roots: &'a [Root],
    pub(crate) bits: u8,
    pub(crate) previous: Checkpoint,
    pub(crate) current: Checkpoint,
    pub(crate) finalized: Checkpoint,
}

/// The caller validates its complete fork schema before constructing these private inputs.
/// Only the four checkpoint fields can change under the shared Altair justification kernel.
pub(crate) fn checkpoints(
    state: Inputs<'_>,
    constants: &Constants,
) -> Result<(Checkpoint, Checkpoint), ProcessingError> {
    let epoch = state.slot / constants.slots_per_epoch;
    if epoch <= 1 {
        return Ok((state.current, state.finalized))
    }
    let previous = epoch - 1;
    // Preserve the original balance-summation order and its overflow checks.
    let participating_balance = |participation: &[u8], target| {
        state
            .validators
            .iter()
            .zip(participation)
            .filter(|(validator, flags)| {
                is_active(validator, target) && !validator.slashed && **flags & 2 != 0
            })
            .try_fold(0u64, |sum, (validator, _)| {
                add(sum, validator.effective_balance, "total effective balance")
            })
            .map(|balance| balance.max(EFFECTIVE_BALANCE_INCREMENT))
    };
    let previous_balance = participating_balance(state.previous_participation, previous)?;
    let current_balance = participating_balance(state.current_participation, epoch)?;
    let total = state
        .validators
        .iter()
        .filter(|validator| is_active(validator, epoch))
        .try_fold(0u64, |sum, validator| {
            add(sum, validator.effective_balance, "total active balance")
        })?
        .max(EFFECTIVE_BALANCE_INCREMENT);
    let block_root = |target| {
        let slot = mul(target, constants.slots_per_epoch, "epoch start slot")?;
        if slot >= state.slot || state.slot - slot > constants.historical_slots {
            return Err(ProcessingError::InvalidState(
                "block root is outside historical slot window",
            ))
        }
        state
            .block_roots
            .get(position(slot, constants.historical_slots)?)
            .copied()
            .ok_or(ProcessingError::InvalidState("block-root ring is truncated"))
    };
    let mut bits = (state.bits << 1) & 15;
    let mut current = state.current;
    let mut finalized = state.finalized;
    if mul(previous_balance, 3, "justification participation")? >=
        mul(total, 2, "justification threshold")?
    {
        current = Checkpoint { epoch: previous, root: block_root(previous)? };
        bits |= 2;
    }
    if mul(current_balance, 3, "justification participation")? >=
        mul(total, 2, "justification threshold")?
    {
        current = Checkpoint { epoch, root: block_root(epoch)? };
        bits |= 1;
    }
    if bits & 14 == 14 && add(state.previous.epoch, 3, "previous checkpoint finality")? == epoch {
        finalized = state.previous;
    }
    if bits & 6 == 6 && add(state.previous.epoch, 2, "previous checkpoint finality")? == epoch {
        finalized = state.previous;
    }
    if bits & 7 == 7 && add(state.current.epoch, 2, "current checkpoint finality")? == epoch {
        finalized = state.current;
    }
    if bits & 3 == 3 && add(state.current.epoch, 1, "current checkpoint finality")? == epoch {
        finalized = state.current;
    }
    Ok((current, finalized))
}

#[cfg(test)]
mod tests;
