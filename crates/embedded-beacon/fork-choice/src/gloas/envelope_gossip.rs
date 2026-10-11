//! Strict Gloas envelope gossip component checks, distinct from full execution/state import.
//! Success verifies propagation prerequisites and real BLS; it produces no runtime proof or FULL
//! graph admission and does not validate gas-limit commitment, execution or data availability.

use crate::{compact::ReadGloasGossipView, StoreError};
use reth_embedded_beacon_primitives::SszError;
use reth_embedded_beacon_state_transition::{
    gloas_processing,
    phase0_processing::ProcessingError,
    ssz::{gloas::SignedExecutionPayloadEnvelope, Preset, Ssz},
};

/// Propagation outcomes remain separate from unavailable infrastructure or arithmetic bounds.
#[derive(Debug, thiserror::Error)]
pub enum GossipEnvelopeError {
    /// A missing or obsolete prerequisite should not be propagated yet.
    #[error("ignore envelope gossip: {0}")]
    Ignore(&'static str),
    /// An invalid candidate should not be propagated.
    #[error("reject envelope gossip: {0}")]
    Reject(&'static str),
    /// Complete structural encoding or hashing failed.
    #[error(transparent)]
    Ssz(#[from] SszError),
    /// The state failed a concrete processing invariant.
    #[error(transparent)]
    Processing(#[from] ProcessingError),
    /// Local configuration or retention could not satisfy this operation.
    #[error(transparent)]
    Store(#[from] StoreError),
}

/// Check the pinned ordered envelope propagation rules with real signatures.
/// A successful result cannot admit execution, establish DA or publish a FULL branch.
pub fn validate<V: ReadGloasGossipView>(
    store: &V,
    signed: &SignedExecutionPayloadEnvelope,
) -> Result<(), GossipEnvelopeError> {
    let envelope = &signed.message;
    let payload = &envelope.payload;
    let block = store
        .block(envelope.beacon_block_root)
        .ok_or(GossipEnvelopeError::Ignore("envelope's block has not been seen"))?;
    let state = store
        .read_block_state(envelope.beacon_block_root)?
        .ok_or(GossipEnvelopeError::Reject("envelope's block failed validation"))?;
    let finalized_slot = store
        .finalized_checkpoint()
        .epoch
        .checked_mul(store.slots_per_epoch())
        .ok_or(StoreError::Overflow)?;
    if payload.slot_number < finalized_slot {
        return Err(GossipEnvelopeError::Ignore(
            "envelope is from a slot before the latest finalized slot",
        ))
    }
    if block.slot != payload.slot_number {
        return Err(GossipEnvelopeError::Reject("block's slot does not match payload's slot number"))
    }
    let bid = &block.body.signed_execution_payload_bid.message;
    if envelope.builder_index != bid.builder_index {
        return Err(GossipEnvelopeError::Reject(
            "envelope's builder index does not match the bid's builder index",
        ))
    }
    if payload.block_hash != bid.block_hash {
        return Err(GossipEnvelopeError::Reject(
            "payload's block hash does not match the bid's block hash",
        ))
    }
    // The pinned rules permit checking request limits during decoding. Check them before hashing
    // typed progressive lists as well, so candidate construction cannot bypass these CPU bounds.
    let requests = &envelope.execution_requests;
    for (count, limit, reason) in [
        (requests.withdrawals.values.len(), 16, "too many withdrawal requests"),
        (requests.consolidations.values.len(), 2, "too many consolidation requests"),
        (requests.builder_deposits.values.len(), 64, "too many builder deposit requests"),
        (requests.builder_exits.values.len(), 16, "too many builder exit requests"),
    ] {
        if count > limit {
            return Err(GossipEnvelopeError::Reject(reason))
        }
    }
    let parameters = store.processing_parameters();
    if signed
        .heap_bytes()?
        .checked_add(std::mem::size_of_val(signed))
        .ok_or(StoreError::Overflow)? >
        64 * 1024 * 1024
    {
        return Err(StoreError::ResourceLimit.into())
    }
    if requests.hash_tree_root(parameters.preset())? != bid.execution_requests_root {
        return Err(GossipEnvelopeError::Reject(
            "envelope's execution requests root does not match the bid's",
        ))
    }
    let withdrawals_limit = match parameters.preset() {
        Preset::Mainnet => 16,
        Preset::Minimal => 4,
    };
    if payload.withdrawals.values.len() > withdrawals_limit {
        return Err(GossipEnvelopeError::Reject("too many withdrawals"))
    }
    gloas_processing::verify_execution_payload_envelope_signature(&state, signed, parameters)
        .map_err(|_| GossipEnvelopeError::Reject("invalid envelope signature"))?;
    Ok(())
}
