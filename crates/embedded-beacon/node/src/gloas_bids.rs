//! Ordered Gloas builder bid and proposer-preference gossip admission.
//!
//! A cache entry requires the actual retained fork-choice state and a mandatory signature.
//! Payload gas metadata requires real envelope gossip signatures or full local verification.
//! Neither an observed block nor an accepted bid authorizes block import or payload delivery.

use reth_embedded_beacon_crypto::{compute_domain, compute_signing_root, verify_signature};
use reth_embedded_beacon_fork_choice::{
    compact::ReadGloasGossipView,
    gloas::envelope_gossip::{self, GossipEnvelopeError},
    ForkChoiceNode, PayloadStatus, StoreError,
};
use reth_embedded_beacon_primitives::{Root, SszError};
use reth_embedded_beacon_state_transition::{
    gloas_processing::{self, GloasParameters},
    phase0_processing::ProcessingError,
    ssz::{
        auxiliary::gloas::{ProposerPreferences, SignedProposerPreferences},
        gloas::{
            BeaconBlock, BeaconState, ExecutionPayloadBid, SignedExecutionPayloadBid,
            SignedExecutionPayloadEnvelope,
        },
        Preset,
    },
};
use std::collections::{BTreeMap, BTreeSet};

mod context;
pub(crate) use context::GossipContext;

/// Bounded cache of received block identities and cryptographically accepted gossip.
#[derive(Clone, Debug)]
pub struct SeenBids {
    observed: BTreeMap<Root, u64>,
    bids: BTreeSet<(u64, Root, Root, u64)>,
    best: BTreeMap<(u64, Root, Root), u64>,
    preferences: BTreeMap<(u64, Root), ProposerPreferences>,
    payloads: BTreeMap<Root, (u64, u64)>,
    envelopes: BTreeMap<(Root, u64), u64>,
    max_entries: usize,
    max_replay_slots: u64,
    context: Option<GossipContext>,
}

/// Normative propagation outcomes stay separate from local capacity and clock failures.
#[derive(Debug, thiserror::Error)]
pub enum BidGossipError {
    /// Harmless duplicate, early/late input or unavailable prerequisite.
    #[error("ignore bid gossip: {0}")]
    Ignore(&'static str),
    /// Invalid signed input, state association or operation bound.
    #[error("reject bid gossip: {0}")]
    Reject(&'static str),
    /// Local bounded capacity or replay budget was exceeded.
    #[error("bid gossip resource limit exceeded")]
    ResourceLimit,
    /// Local clock or index arithmetic overflowed.
    #[error("bid gossip arithmetic overflow")]
    Overflow,
    /// The authoritative fork-choice graph could not answer a required query.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// The immutable imported state failed a production processing invariant.
    #[error(transparent)]
    Processing(#[from] ProcessingError),
    /// An observed block could not be structurally decoded or hashed.
    #[error(transparent)]
    Ssz(#[from] SszError),
}

impl SeenBids {
    /// Each identity collection is bounded separately; replay runs on bounded blocking workers.
    pub const fn new(max_entries: usize, max_replay_slots: u64) -> Result<Self, BidGossipError> {
        if max_entries == 0 || max_replay_slots == 0 {
            return Err(BidGossipError::ResourceLimit)
        }
        Ok(Self {
            observed: BTreeMap::new(),
            bids: BTreeSet::new(),
            best: BTreeMap::new(),
            preferences: BTreeMap::new(),
            payloads: BTreeMap::new(),
            envelopes: BTreeMap::new(),
            max_entries,
            max_replay_slots,
            context: None,
        })
    }

    fn check_context<V: ReadGloasGossipView>(&self, store: &V) -> Result<(), BidGossipError> {
        if let Some(context) = &self.context &&
            !context.matches(store)?
        {
            return Err(BidGossipError::Reject(
                "gossip cache belongs to another chain or configuration",
            ))
        }
        Ok(())
    }
    fn bind_context<V: ReadGloasGossipView>(&mut self, store: &V) -> Result<(), BidGossipError> {
        if self.context.is_none() {
            self.context = Some(GossipContext::capture(store)?);
        }
        Ok(())
    }
    /// Record an actual received block's root and slot without asserting successful import.
    pub fn observe_block(
        &mut self,
        block: &BeaconBlock,
        preset: Preset,
    ) -> Result<(), BidGossipError> {
        let root = block.hash_tree_root(preset)?;
        if !self.observed.contains_key(&root) && self.observed.len() == self.max_entries {
            return Err(BidGossipError::ResourceLimit)
        }
        self.observed.insert(root, block.slot);
        Ok(())
    }

    /// Record only an envelope which passed the store's mandatory BLS, Engine and DA checks.
    /// This stores its gas limit, rather than copying transactions or the execution payload.
    pub fn record_verified_payload<V: ReadGloasGossipView>(
        &mut self,
        store: &V,
        root: Root,
    ) -> Result<(), BidGossipError> {
        self.record_payload_with_view(store, root)
    }
    fn record_payload_with_view<V: ReadGloasGossipView>(
        &mut self,
        store: &V,
        root: Root,
    ) -> Result<(), BidGossipError> {
        self.check_context(store)?;
        let envelope = store
            .payload(root)
            .ok_or(BidGossipError::Ignore("execution payload envelope has not been seen"))?;
        let block = store.block(root).ok_or(StoreError::UnknownRoot)?;
        let hash = envelope.payload.block_hash;
        if !self.payloads.contains_key(&hash) && self.payloads.len() == self.max_entries {
            return Err(BidGossipError::ResourceLimit)
        }
        self.bind_context(store)?;
        self.payloads.insert(hash, (block.slot, envelope.payload.gas_limit));
        Ok(())
    }

    /// Prune retired slots; retention policy must keep the node's required gossip horizon.
    pub fn retain_from_slot(&mut self, slot: u64) {
        self.observed.retain(|_, value| *value >= slot);
        self.bids.retain(|(value, _, _, _)| *value >= slot);
        self.best.retain(|(value, _, _), _| *value >= slot);
        self.preferences.retain(|(value, _), _| *value >= slot);
        self.payloads.retain(|_, (value, _)| *value >= slot);
        self.envelopes.retain(|_, value| *value >= slot);
    }

    /// Validate envelope propagation and cache only its gas metadata after a real signature.
    /// Success grants no execution, state-transition or DA validity and never publishes FULL.
    pub fn validate_envelope<V: ReadGloasGossipView>(
        &mut self,
        store: &V,
        signed: &SignedExecutionPayloadEnvelope,
    ) -> Result<(), BidGossipError> {
        self.validate_envelope_with_view(store, signed)
    }
    fn validate_envelope_with_view<V: ReadGloasGossipView>(
        &mut self,
        store: &V,
        signed: &SignedExecutionPayloadEnvelope,
    ) -> Result<(), BidGossipError> {
        self.check_context(store)?;
        let envelope = &signed.message;
        let key = (envelope.beacon_block_root, envelope.builder_index);
        if self.envelopes.contains_key(&key) {
            return Err(BidGossipError::Ignore(
                "already seen envelope for this block root from this builder",
            ))
        }
        if store.block(envelope.beacon_block_root).is_none() &&
            self.observed.contains_key(&envelope.beacon_block_root)
        {
            return Err(BidGossipError::Reject("envelope's block failed validation"))
        }
        envelope_gossip::validate(store, signed).map_err(|error| match error {
            GossipEnvelopeError::Ignore(reason) => BidGossipError::Ignore(reason),
            GossipEnvelopeError::Reject(reason) => BidGossipError::Reject(reason),
            GossipEnvelopeError::Ssz(error) => BidGossipError::Ssz(error),
            GossipEnvelopeError::Processing(error) => BidGossipError::Processing(error),
            GossipEnvelopeError::Store(error) => BidGossipError::Store(error),
        })?;
        if self.envelopes.len() == self.max_entries ||
            (!self.payloads.contains_key(&envelope.payload.block_hash) &&
                self.payloads.len() == self.max_entries)
        {
            return Err(BidGossipError::ResourceLimit)
        }
        self.bind_context(store)?;
        self.envelopes.insert(key, envelope.payload.slot_number);
        self.payloads.insert(
            envelope.payload.block_hash,
            (envelope.payload.slot_number, envelope.payload.gas_limit),
        );
        Ok(())
    }

    /// Rejected/ignored bids never reserve a seen identity or advance the highest bid.
    /// Apply the exact bid propagation checks against the private authoritative Gloas graph.
    pub fn validate_bid<V: ReadGloasGossipView>(
        &mut self,
        store: &V,
        signed: &SignedExecutionPayloadBid,
        current_time_ms: u64,
    ) -> Result<(), BidGossipError> {
        self.validate_bid_with_view(store, signed, current_time_ms)
    }
    fn validate_bid_with_view<V: ReadGloasGossipView>(
        &mut self,
        store: &V,
        signed: &SignedExecutionPayloadBid,
        current_time_ms: u64,
    ) -> Result<(), BidGossipError> {
        self.check_context(store)?;
        let parameters = store.processing_parameters();
        let bid = &signed.message;
        let key = (bid.slot, bid.parent_block_hash, bid.parent_block_root, bid.builder_index);
        let best_key = (bid.slot, bid.parent_block_hash, bid.parent_block_root);
        if self.bids.contains(&key) {
            return Err(BidGossipError::Ignore(
                "already seen valid bid for this slot, parent, and builder",
            ))
        }
        if self.best.get(&best_key).is_some_and(|value| bid.value <= *value) {
            return Err(BidGossipError::Ignore(
                "bid is not the highest value bid seen for this slot and parent",
            ))
        }
        if !current_or_next_slot(store, bid.slot, current_time_ms)? {
            return Err(BidGossipError::Ignore("bid's slot is not the current or next slot"))
        }
        if bid.execution_payment != 0 {
            return Err(BidGossipError::Reject("bid's execution payment must be zero"))
        }
        if bid.block_hash == bid.parent_block_hash {
            return Err(BidGossipError::Reject("bid's block hash equals its parent block hash"))
        }
        let proposal_epoch = bid.slot / store.slots_per_epoch();
        if bid.blob_kzg_commitments.values.len() as u64 >
            parameters.fulu.blob_parameters_at_epoch(proposal_epoch).max_blobs_per_block
        {
            return Err(BidGossipError::Reject("too many blob kzg commitments"))
        }
        signed
            .hash_tree_root(parameters.preset())
            .map_err(|_| BidGossipError::Reject("invalid bid encoding"))?;
        let Some(parent_slot) = store
            .block(bid.parent_block_root)
            .map(|block| block.slot)
            .or_else(|| self.observed.get(&bid.parent_block_root).copied())
        else {
            return Err(BidGossipError::Ignore(
                "bid's parent block root is not a known beacon block",
            ))
        };
        if bid.slot <= parent_slot {
            return Err(BidGossipError::Reject("bid's slot is not higher than its parent's slot"))
        }
        let state = store
            .read_block_state(bid.parent_block_root)?
            .ok_or(BidGossipError::Ignore("bid's parent block post-state is unavailable"))?;
        if proposal_epoch >
            (state.slot / store.slots_per_epoch())
                .checked_add(1)
                .ok_or(BidGossipError::Overflow)?
        {
            return Err(BidGossipError::Ignore("bid's slot is past the parent's proposer lookahead"))
        }
        let lookahead_start = lookahead_start(proposal_epoch, store.slots_per_epoch())?;
        let dependent_slot = lookahead_start.saturating_sub(1);
        let dependent_root = store
            .get_ancestor(
                ForkChoiceNode {
                    root: bid.parent_block_root,
                    payload_status: Some(PayloadStatus::Pending),
                },
                dependent_slot,
            )?
            .root;
        let preferences = self
            .preferences
            .get(&(bid.slot, dependent_root))
            .ok_or(BidGossipError::Ignore("matching proposer preferences have not been seen"))?;
        if bid.fee_recipient != preferences.fee_recipient {
            return Err(BidGossipError::Ignore(
                "bid's fee recipient does not match the proposer's preference",
            ))
        }
        let parent_gas_limit = self
            .payloads
            .get(&bid.parent_block_hash)
            .ok_or(BidGossipError::Ignore(
                "bid's parent block hash is not a known execution payload",
            ))?
            .1;
        if !is_gas_limit_target_compatible(
            parent_gas_limit,
            bid.gas_limit,
            preferences.target_gas_limit,
        )? {
            return Err(BidGossipError::Ignore(
                "bid's gas limit is not compatible with the proposer's target",
            ))
        }
        if !bid_compatible_with_view(store, bid)? {
            return Err(BidGossipError::Ignore("bid is not compatible with the current head branch"))
        }
        let mix = state
            .randao_mixes
            .values
            .get((state.slot / store.slots_per_epoch()) as usize % state.randao_mixes.values.len())
            .ok_or(BidGossipError::Reject("invalid randao ring"))?;
        if bid.prev_randao != *mix {
            return Err(BidGossipError::Reject("bid's previous randao is incorrect"))
        }
        let mut state = state.as_ref().clone();
        if let Some(checkpoint) = store.replay_finalized_checkpoint(bid.parent_block_root) {
            state.finalized_checkpoint = checkpoint;
        }
        replay(&mut state, bid.slot, parameters, self.max_replay_slots)?;
        let builder = state
            .builders
            .values
            .get(
                usize::try_from(bid.builder_index)
                    .map_err(|_| BidGossipError::Reject("builder index out of range"))?,
            )
            .ok_or(BidGossipError::Reject("builder index out of range"))?;
        if builder.version != 0 {
            return Err(BidGossipError::Reject("builder is not a payload builder"))
        }
        if !gloas_processing::is_active_builder(&state, bid.builder_index, parameters)? {
            return Err(BidGossipError::Reject("builder is not active"))
        }
        if !gloas_processing::can_builder_cover_bid(
            &state,
            bid.builder_index,
            bid.value,
            parameters,
        )? {
            return Err(BidGossipError::Ignore("builder cannot cover bid value"))
        }
        if bid.parent_block_hash == state.latest_execution_payload_bid.block_hash {
            let envelope = store
                .payload(bid.parent_block_root)
                .ok_or(BidGossipError::Ignore("execution payload envelope has not been seen"))?;
            let pubkey = builder.pubkey;
            let address = builder.execution_address;
            if envelope
                .execution_requests
                .builder_exits
                .values
                .iter()
                .any(|request| request.pubkey == pubkey && request.source_address == address)
            {
                return Err(BidGossipError::Ignore("builder may exit"))
            }
        }
        gloas_processing::verify_execution_payload_bid_signature(&state, signed, parameters)
            .map_err(|_| BidGossipError::Reject("invalid bid signature"))?;
        if self.bids.len() == self.max_entries ||
            (!self.best.contains_key(&best_key) && self.best.len() == self.max_entries)
        {
            return Err(BidGossipError::ResourceLimit)
        }
        self.bind_context(store)?;
        self.bids.insert(key);
        self.best.insert(best_key, bid.value);
        Ok(())
    }

    /// Validate proposer lookahead, exact future fork domain and BLS before caching preferences.
    pub fn validate_preferences<V: ReadGloasGossipView>(
        &mut self,
        store: &V,
        signed: &SignedProposerPreferences,
        current_time_ms: u64,
    ) -> Result<(), BidGossipError> {
        self.validate_preferences_with_view(store, signed, current_time_ms)
    }
    fn validate_preferences_with_view<V: ReadGloasGossipView>(
        &mut self,
        store: &V,
        signed: &SignedProposerPreferences,
        current_time_ms: u64,
    ) -> Result<(), BidGossipError> {
        self.check_context(store)?;
        let parameters = store.processing_parameters();
        let preferences = &signed.message;
        let key = (preferences.proposal_slot, preferences.dependent_root);
        if self.preferences.contains_key(&key) {
            return Err(BidGossipError::Ignore(
                "already seen preferences for this dependent root and proposal slot",
            ))
        }
        let proposal_epoch = preferences.proposal_slot / store.slots_per_epoch();
        if proposal_epoch < parameters.fork_epoch {
            return Err(BidGossipError::Ignore("proposal epoch is pre-gloas"))
        }
        if current_time_ms >
            slot_time(store, preferences.proposal_slot)?
                .checked_add(500)
                .ok_or(BidGossipError::Overflow)?
        {
            return Err(BidGossipError::Ignore("proposal slot has already started"))
        }
        let start = lookahead_start(proposal_epoch, store.slots_per_epoch())?;
        if current_time_ms.checked_add(500).ok_or(BidGossipError::Overflow)? <
            slot_time(store, start)?
        {
            return Err(BidGossipError::Ignore("proposer for the proposal slot is not yet known"))
        }
        if store.block(preferences.dependent_root).is_none() &&
            !self.observed.contains_key(&preferences.dependent_root)
        {
            return Err(BidGossipError::Ignore("dependent block has not been seen"))
        }
        let state = store
            .read_block_state(preferences.dependent_root)?
            .ok_or(BidGossipError::Ignore("dependent block failed validation"))?;
        let dependent_slot = start.saturating_sub(1);
        if store.block(preferences.dependent_root).ok_or(StoreError::UnknownRoot)?.slot >
            dependent_slot
        {
            return Err(BidGossipError::Reject(
                "dependent block is after the shuffling dependent slot",
            ))
        }
        if !store.is_valid_dependent_root(preferences.dependent_root, dependent_slot)? {
            return Err(BidGossipError::Ignore("dependent block is not a possible dependent block"))
        }
        let mut state = state.as_ref().clone();
        if let Some(checkpoint) = store.replay_finalized_checkpoint(preferences.dependent_root) {
            state.finalized_checkpoint = checkpoint;
        }
        let target = start.max(state.slot);
        replay(&mut state, target, parameters, self.max_replay_slots)?;
        let index = preferences
            .proposal_slot
            .checked_sub(start)
            .and_then(|value| usize::try_from(value).ok())
            .ok_or(BidGossipError::Reject("invalid proposer lookahead index"))?;
        if state.proposer_lookahead.values.get(index) != Some(&preferences.validator_index) {
            return Err(BidGossipError::Reject("validator is not the proposer for the given slot"))
        }
        let validator = state
            .validators
            .values
            .get(usize::try_from(preferences.validator_index).map_err(|_| {
                BidGossipError::Reject("validator is not the proposer for the given slot")
            })?)
            .ok_or(BidGossipError::Reject("validator is not the proposer for the given slot"))?;
        let signing_root = compute_signing_root(
            preferences.hash_tree_root(parameters.preset())?,
            compute_domain([13, 0, 0, 0], parameters.fork_version, state.genesis_validators_root),
        );
        verify_signature(&validator.pubkey, &signing_root, &signed.signature)
            .map_err(|_| BidGossipError::Reject("invalid proposer preferences signature"))?;
        if self.preferences.len() == self.max_entries {
            return Err(BidGossipError::ResourceLimit)
        }
        self.bind_context(store)?;
        self.preferences.insert(key, preferences.clone());
        Ok(())
    }
}

fn slot_time<V: ReadGloasGossipView>(store: &V, slot: u64) -> Result<u64, BidGossipError> {
    slot.checked_mul(store.slot_duration_ms())
        .and_then(|time| store.genesis_time_ms().checked_add(time))
        .ok_or(BidGossipError::Overflow)
}
fn current_or_next_slot<V: ReadGloasGossipView>(
    store: &V,
    slot: u64,
    time: u64,
) -> Result<bool, BidGossipError> {
    let first = slot.saturating_sub(1);
    let end = slot.checked_add(1).ok_or(BidGossipError::Overflow)?;
    Ok(time.checked_add(500).ok_or(BidGossipError::Overflow)? >= slot_time(store, first)? &&
        time <= slot_time(store, end)?.checked_add(500).ok_or(BidGossipError::Overflow)?)
}
fn lookahead_start(epoch: u64, slots_per_epoch: u64) -> Result<u64, BidGossipError> {
    epoch.saturating_sub(1).checked_mul(slots_per_epoch).ok_or(BidGossipError::Overflow)
}
fn replay(
    state: &mut BeaconState,
    slot: u64,
    parameters: &GloasParameters,
    max: u64,
) -> Result<(), BidGossipError> {
    if slot < state.slot || slot - state.slot > max {
        return Err(BidGossipError::ResourceLimit)
    }
    if slot != state.slot {
        gloas_processing::process_slots(state, slot, parameters)?;
    }
    Ok(())
}

/// Exact EIP-1559 target clamping, including integer truncation at the transition boundary.
pub const fn is_gas_limit_target_compatible(
    parent: u64,
    gas: u64,
    target: u64,
) -> Result<bool, BidGossipError> {
    let difference = (parent / 1024).saturating_sub(1);
    let minimum = parent - difference;
    let Some(maximum) = parent.checked_add(difference) else {
        return Err(BidGossipError::Overflow)
    };
    Ok(gas ==
        if target < minimum {
            minimum
        } else if target > maximum {
            maximum
        } else {
            target
        })
}

/// Query the precise FULL/EMPTY canonical branch, retaining the previous block fallback.
pub fn is_bid_compatible_with_head<V: ReadGloasGossipView>(
    store: &V,
    bid: &ExecutionPayloadBid,
) -> Result<bool, BidGossipError> {
    bid_compatible_with_view(store, bid)
}
fn bid_compatible_with_view<V: ReadGloasGossipView>(
    store: &V,
    bid: &ExecutionPayloadBid,
) -> Result<bool, BidGossipError> {
    let head = store.get_head()?;
    let block = store.block(head.root).ok_or(StoreError::UnknownRoot)?;
    let parent = &block.body.signed_execution_payload_bid.message;
    let parent_root = block.parent_root;
    let parent_hash = parent.parent_block_hash;
    if bid.parent_block_root == parent_root && bid.parent_block_hash == parent_hash {
        return Ok(true)
    }
    if bid.parent_block_root != head.root {
        return Ok(false)
    }
    Ok(if store.should_build_on_full(head, bid.slot)? {
        bid.parent_block_hash == parent.block_hash
    } else {
        bid.parent_block_hash == parent.parent_block_hash
    })
}

#[cfg(feature = "spec-tests")]
pub mod spec_only;

#[cfg(test)]
pub(crate) mod tests;
