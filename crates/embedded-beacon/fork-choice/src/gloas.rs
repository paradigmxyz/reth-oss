//! Private Gloas ROOT/FULL/EMPTY graph with strict beacon, payload and PTC admission.
//!
//! The anchor is caller-trusted genesis/checkpoint data. Subsequent beacon blocks require
//! mandatory BLS and complete post-state verification. Payload envelopes require actual
//! Engine and custody/data-availability verification; PTC observations cannot supply either.

use crate::{
    compact::{GloasStateRead, GraphStateRead, RetainedState},
    ForkChoiceNode, PayloadStatus, StoreError, StoreParameters, StoreSnapshot,
};
use reth_embedded_beacon_primitives::{Checkpoint, Root};
use reth_embedded_beacon_state_transition::{
    gloas_processing::{self, ExecutionEngine, GloasParameters, IndexedPayloadAttestation},
    phase0_processing::ProcessingError,
    ssz::{
        auxiliary::gloas::PayloadAttestationMessage,
        gloas::{
            self, BeaconBlock, BeaconState, SignedBeaconBlock, SignedExecutionPayloadEnvelope,
        },
        Preset, Ssz,
    },
};
use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    sync::Arc,
};
use thiserror::Error;

pub mod envelope_gossip;
pub mod gossip_view;

mod body_attestations;
pub(crate) mod compact;
mod compact_retention;
mod execution_queries;
mod fast_confirmation;
mod migration;
mod policy;

#[cfg(feature = "spec-tests")]
pub mod spec_only;

#[cfg(feature = "spec-tests")]
pub mod artificial;

/// Required custody verification against the exact locally retained signed beacon block.
///
/// Implementations must retrieve the configured custody/sample columns, bind their slot/root
/// and commitments to `block`, and validate every required structural and KZG proof. Missing
/// columns must fail. PTC votes and asserted availability flags cannot satisfy this contract.
/// There is no default implementation; deployment must configure its actual verification path.
pub trait DataAvailability: Send + Sync {
    /// Missing data, invalid proofs, resource limits or retrieval errors.
    type Error: std::error::Error + Send + Sync + 'static;
    /// Verify the custody data for this exact root and immutable authenticated block.
    fn verify<'a>(
        &'a self,
        root: Root,
        block: &'a BeaconBlock,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;
    /// Retain staged custody data only after native BLS, Engine and envelope validation succeeded.
    fn retain_verified_envelope(
        &self,
        _admitted: &crate::compact::VerifiedEnvelope<'_>,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// Payload admission failures never publish a partially verified envelope.
#[derive(Debug, Error)]
pub enum EnvelopeError {
    /// Consensus graph or complete structural validation failed.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// Mandatory actual custody/proof verification failed.
    #[error("mandatory data availability verification failed: {0}")]
    DataAvailability(String),
    /// Mandatory signature, linkage or execution verification failed.
    #[error("mandatory execution envelope verification failed: {0}")]
    Verification(String),
}

/// Latest authenticated vote, ordered by attestation slot rather than epoch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LatestMessage {
    /// Exact attestation slot.
    pub slot: u64,
    /// Signed voted beacon block root.
    pub root: Root,
    /// Whether this vote supports the block's FULL payload branch.
    pub payload_present: bool,
}

/// Bounded authoritative Gloas event store; every graph and vote map stays private.
#[derive(Clone, Debug)]
pub struct GloasStore {
    // Checked constructor or proven finalized retained boundary; migrated history stays genuine.
    execution_query_anchor: Option<Root>,
    // Genuine earlier schemas are retained only by authenticated migration.
    historical: Option<Arc<crate::Store>>,
    blocks: BTreeMap<Root, Arc<BeaconBlock>>,
    block_states: BTreeMap<Root, RetainedState<BeaconState>>,
    checkpoint_states: BTreeMap<(u64, Root), RetainedState<BeaconState>>,
    block_timeliness: BTreeMap<Root, [bool; 2]>,
    unrealized_justifications: BTreeMap<Root, Checkpoint>,
    payloads: BTreeMap<Root, Arc<gloas::ExecutionPayloadEnvelope>>,
    timeliness_votes: BTreeMap<Root, Vec<Option<bool>>>,
    availability_votes: BTreeMap<Root, Vec<Option<bool>>>,
    latest_messages: BTreeMap<u64, LatestMessage>,
    equivocating: BTreeSet<u64>,
    justified: Checkpoint,
    finalized: Checkpoint,
    unrealized_justified: Checkpoint,
    unrealized_finalized: Checkpoint,
    proposer_boost_root: Root,
    time_ms: u64,
    genesis_time_ms: u64,
    parameters: StoreParameters,
    processing: GloasParameters,
}

impl GloasStore {
    /// Start from explicitly trusted complete Gloas genesis/checkpoint data.
    /// This verifies schema, root linkage, preset, fork identity and clock; it does not infer
    /// trust.
    pub fn get_store(
        state: BeaconState,
        block: BeaconBlock,
        parameters: StoreParameters,
        processing: GloasParameters,
    ) -> Result<Self, StoreError> {
        parameters.validate()?;
        processing.validate()?;
        let inherited = processing.fulu.electra.deneb.capella.bellatrix.altair;
        if processing.preset() != parameters.phase0.preset ||
            inherited.phase0 != parameters.phase0 ||
            inherited.fork_version != parameters.altair_fork_version ||
            inherited.inactivity_score_bias != parameters.inactivity_score_bias ||
            inherited.inactivity_score_recovery_rate != parameters.inactivity_score_recovery_rate ||
            processing.fulu.electra.deneb.capella.bellatrix.seconds_per_slot.checked_mul(1000) !=
                Some(parameters.slot_duration_ms) ||
            state.fork.current_version != processing.fork_version ||
            state.fork.epoch != processing.fork_epoch ||
            state.fork.epoch > state.slot / processing.slots_per_epoch() ||
            block.slot != state.slot
        {
            return Err(StoreError::AnchorMismatch)
        }
        let preset = processing.preset();
        if block.state_root != state.hash_tree_root(preset)? {
            return Err(StoreError::AnchorMismatch)
        }
        let root = block.hash_tree_root(preset)?;
        let checkpoint = Checkpoint { epoch: state.slot / processing.slots_per_epoch(), root };
        let genesis_time_ms = state.genesis_time.checked_mul(1000).ok_or(StoreError::Overflow)?;
        let time_ms = state
            .slot
            .checked_mul(parameters.slot_duration_ms)
            .and_then(|time| genesis_time_ms.checked_add(time))
            .ok_or(StoreError::Overflow)?;
        let state = Arc::new(state);
        let store = Self {
            execution_query_anchor: Some(root),
            historical: None,
            blocks: BTreeMap::from([(root, Arc::new(block))]),
            block_states: BTreeMap::from([(root, state.clone().into())]),
            checkpoint_states: BTreeMap::from([((checkpoint.epoch, root), state.into())]),
            block_timeliness: BTreeMap::from([(root, [true, true])]),
            unrealized_justifications: BTreeMap::from([(root, checkpoint)]),
            payloads: BTreeMap::new(),
            timeliness_votes: BTreeMap::from([(root, vec![None; ptc_size(preset)])]),
            availability_votes: BTreeMap::from([(root, vec![None; ptc_size(preset)])]),
            latest_messages: BTreeMap::new(),
            equivocating: BTreeSet::new(),
            justified: checkpoint,
            finalized: checkpoint,
            unrealized_justified: checkpoint,
            unrealized_finalized: checkpoint,
            proposer_boost_root: [0; 32],
            time_ms,
            genesis_time_ms,
            parameters,
            processing,
        };
        store.check_budget()?;
        Ok(store)
    }

    /// Exact checked genesis clock, in milliseconds.
    pub const fn genesis_time_ms(&self) -> u64 {
        self.genesis_time_ms
    }
    /// Exact checked slot duration, in milliseconds.
    pub const fn slot_duration_ms(&self) -> u64 {
        self.parameters.slot_duration_ms
    }
    /// Immutable fork, BPO, balance and signature parameters bound to the trusted anchor.
    pub const fn processing_parameters(&self) -> &GloasParameters {
        &self.processing
    }
    /// Exact mainnet/minimal epoch length.
    pub const fn slots_per_epoch(&self) -> u64 {
        self.processing.slots_per_epoch()
    }
    /// Current event clock, in milliseconds.
    pub const fn time_ms(&self) -> u64 {
        self.time_ms
    }
    /// Current event-clock slot.
    pub const fn current_slot(&self) -> u64 {
        (self.time_ms - self.genesis_time_ms) / self.parameters.slot_duration_ms
    }
    /// First timely block's boost root, or zero after the slot changes.
    pub const fn proposer_boost_root(&self) -> Root {
        self.proposer_boost_root
    }
    /// Own the admitted typed Gloas post-state; known cold failures are explicit errors.
    pub fn read_block_state(&self, root: Root) -> Result<Option<GloasStateRead>, StoreError> {
        self.block_states
            .get(&root)
            .map(|state| state.acquire(self.processing.preset()))
            .transpose()
    }

    /// Borrow a retained hot post-state; ordinary constructors keep this borrowed API.
    /// Compact owners use checked owning reads for cold states.
    pub fn block_state(&self, root: Root) -> Option<&BeaconState> {
        self.block_states.get(&root).and_then(RetainedState::hot).map(AsRef::as_ref)
    }
    /// Share an immutable post-state without copying its registry.
    pub fn block_state_arc(&self, root: Root) -> Option<Arc<BeaconState>> {
        self.block_states.get(&root).and_then(RetainedState::hot).cloned()
    }
    /// Share a beacon block admitted by mandatory signed transition, or the trusted anchor.
    pub fn block_arc(&self, root: Root) -> Option<Arc<BeaconBlock>> {
        self.blocks.get(&root).cloned()
    }
    /// Share an actual retained earlier block without changing its fork schema.
    /// Trusted Gloas-only anchors have no earlier graph.
    pub fn historical_block_arc(
        &self,
        root: Root,
    ) -> Option<Arc<reth_embedded_beacon_state_transition::ssz::BeaconBlock>> {
        self.historical.as_ref()?.block_arc(root)
    }
    /// Share an actual retained earlier post-state without replaying or relabeling it.
    pub fn historical_block_state_arc(
        &self,
        root: Root,
    ) -> Option<Arc<reth_embedded_beacon_state_transition::ssz::BeaconState>> {
        self.historical.as_ref()?.block_state_arc(root)
    }
    /// Share an envelope admitted by mandatory signature, Engine and data verification.
    pub fn payload_arc(&self, root: Root) -> Option<Arc<gloas::ExecutionPayloadEnvelope>> {
        self.payloads.get(&root).cloned()
    }
    /// Whether this root has a locally delivered and verified envelope.
    pub fn is_payload_verified(&self, root: Root) -> bool {
        self.payloads.contains_key(&root)
    }
    /// Borrow an authenticated latest vote, without granting mutation of the graph.
    pub fn latest_message(&self, index: u64) -> Option<LatestMessage> {
        self.latest_messages.get(&index).copied()
    }
    /// Realized and pulled-up checkpoints derived from actual transitions.
    pub const fn checkpoints(&self) -> (Checkpoint, Checkpoint, Checkpoint, Checkpoint) {
        (self.justified, self.finalized, self.unrealized_justified, self.unrealized_finalized)
    }
    /// A stable authoritative canonical head/checkpoint snapshot.
    pub fn snapshot(&self) -> Result<StoreSnapshot, StoreError> {
        let head = self.get_head()?;
        Ok(StoreSnapshot {
            head,
            head_slot: self.block_header(head.root)?.slot,
            justified_checkpoint: self.justified,
            finalized_checkpoint: self.finalized,
        })
    }

    fn block(&self, root: Root) -> Result<&BeaconBlock, StoreError> {
        self.blocks.get(&root).map(AsRef::as_ref).ok_or(StoreError::UnknownRoot)
    }
    fn deadline(&self, bps: u64) -> Result<u64, StoreError> {
        self.slot_duration_ms()
            .checked_mul(bps)
            .map(|value| value / 10_000)
            .ok_or(StoreError::Overflow)
    }
    const fn time_into_slot(&self) -> u64 {
        (self.time_ms - self.genesis_time_ms) % self.slot_duration_ms()
    }
    fn checkpoint_state<const VERIFY_SIGNATURES: bool>(
        &mut self,
        checkpoint: Checkpoint,
    ) -> Result<GloasStateRead, StoreError> {
        policy::require::<VERIFY_SIGNATURES>()?;
        let key = (checkpoint.epoch, checkpoint.root);
        if let Some(state) = self.checkpoint_states.get(&key) {
            return state.acquire(self.processing.preset())
        }
        let base = self.read_block_state(checkpoint.root)?.ok_or(StoreError::UnknownRoot)?;
        let target =
            checkpoint.epoch.checked_mul(self.slots_per_epoch()).ok_or(StoreError::Overflow)?;
        if base.slot >= target {
            let entry =
                self.block_states.get(&checkpoint.root).ok_or(StoreError::UnknownRoot)?.clone();
            self.checkpoint_states.insert(key, entry);
            return Ok(base)
        }
        let state = {
            if target - base.slot > self.parameters.max_tick_slots {
                return Err(StoreError::ResourceLimit)
            }
            let mut state = base.as_ref().clone();
            policy::process_slots::<VERIFY_SIGNATURES>(&mut state, target, &self.processing)?;
            Arc::new(state)
        };
        self.checkpoint_states.insert(key, state.clone().into());
        self.check_budget()?;
        Ok(GloasStateRead::hot(state, self.processing.preset()))
    }
    fn realized<const VERIFY_SIGNATURES: bool>(
        &mut self,
        justified: Checkpoint,
        finalized: Checkpoint,
    ) -> Result<(), StoreError> {
        policy::require::<VERIFY_SIGNATURES>()?;
        if justified.epoch > self.justified.epoch {
            self.ensure_checkpoint::<VERIFY_SIGNATURES>(justified)?;
            self.justified = justified;
        }
        if finalized.epoch > self.finalized.epoch {
            self.ensure_checkpoint::<VERIFY_SIGNATURES>(finalized)?;
            self.finalized = finalized;
        }
        Ok(())
    }
    /// Count retained unique schema allocations; temporary work and external Arc holders are
    /// excluded.
    pub fn retained_schema_bytes(&self) -> Result<usize, StoreError> {
        let mut bytes =
            self.historical.as_ref().map_or(Ok(0), |history| history.retained_schema_bytes())?;
        let mut seen = BTreeSet::new();
        for entry in self.block_states.values().chain(self.checkpoint_states.values()) {
            if let Some(state) = entry.hot() &&
                seen.insert(Arc::as_ptr(state) as usize)
            {
                bytes = allocation::<BeaconState>(bytes, state.heap_bytes()?)?;
            }
        }
        for block in self.blocks.values() {
            bytes = allocation::<BeaconBlock>(bytes, block.heap_bytes()?)?;
        }
        for envelope in self.payloads.values() {
            bytes = allocation::<gloas::ExecutionPayloadEnvelope>(bytes, envelope.heap_bytes()?)?;
        }
        Ok(bytes)
    }
    fn check_budget(&self) -> Result<(), StoreError> {
        if self.block_count() > self.parameters.max_blocks ||
            self.retained_schema_bytes()? > self.parameters.max_retained_schema_bytes
        {
            return Err(StoreError::ResourceLimit)
        }
        Ok(())
    }
}

const fn ptc_size(preset: Preset) -> usize {
    match preset {
        Preset::Mainnet => 512,
        Preset::Minimal => 16,
    }
}
const fn node(root: Root, status: PayloadStatus) -> ForkChoiceNode {
    ForkChoiceNode { root, payload_status: Some(status) }
}
fn status(node: ForkChoiceNode) -> Result<PayloadStatus, StoreError> {
    node.payload_status.ok_or(StoreError::InvalidAttestationTarget)
}
fn allocation<T>(bytes: usize, heap: usize) -> Result<usize, StoreError> {
    bytes
        .checked_add(heap)
        .and_then(|bytes| bytes.checked_add(std::mem::size_of::<T>()))
        .and_then(|bytes| bytes.checked_add(2 * std::mem::size_of::<usize>()))
        .ok_or(StoreError::Overflow)
}

impl GloasStore {
    /// Derive the payload decision committed by a strictly validated child beacon block.
    pub fn parent_payload_status(&self, block: &BeaconBlock) -> Result<PayloadStatus, StoreError> {
        let parent = self.block(block.parent_root)?;
        Ok(
            if block.body.signed_execution_payload_bid.message.parent_block_hash ==
                parent.body.signed_execution_payload_bid.message.block_hash
            {
                PayloadStatus::Full
            } else {
                PayloadStatus::Empty
            },
        )
    }
    /// Walk exact payload-aware ancestry at or before `slot` without recursion.
    pub fn get_ancestor(
        &self,
        mut current: ForkChoiceNode,
        slot: u64,
    ) -> Result<ForkChoiceNode, StoreError> {
        self.block_header(current.root)?;
        if self.blocks.contains_key(&current.root) != current.payload_status.is_some() {
            return Err(StoreError::InvalidAttestationTarget)
        }
        for _ in 0..=self.block_count() {
            let block = self.block_header(current.root)?;
            if block.slot <= slot {
                return Ok(current)
            }
            current = if let Some(gloas) = self.blocks.get(&current.root) {
                self.parent_node(gloas)?
            } else {
                self.root_node(block.parent_root, PayloadStatus::Pending)?
            };
        }
        Err(StoreError::ResourceLimit)
    }
    /// PENDING covers both decided branches; FULL and EMPTY remain mutually exclusive.
    pub fn is_ancestor(
        &self,
        current: ForkChoiceNode,
        ancestor: ForkChoiceNode,
    ) -> Result<bool, StoreError> {
        if self.blocks.contains_key(&ancestor.root) != ancestor.payload_status.is_some() {
            return Err(StoreError::InvalidAttestationTarget)
        }
        let found = self.get_ancestor(current, self.block_header(ancestor.root)?.slot)?;
        Ok(found.root == ancestor.root &&
            (found.payload_status == ancestor.payload_status ||
                ancestor.payload_status == Some(PayloadStatus::Pending)))
    }
    /// Exact branch checkpoint root at the start of the selected epoch.
    pub fn get_checkpoint_block(&self, root: Root, epoch: u64) -> Result<Root, StoreError> {
        Ok(self
            .get_ancestor(
                self.root_node(root, PayloadStatus::Pending)?,
                epoch.checked_mul(self.slots_per_epoch()).ok_or(StoreError::Overflow)?,
            )?
            .root)
    }
    /// Convert a signed slot vote into its precise payload branch.
    pub fn get_supported_node(&self, message: LatestMessage) -> Result<ForkChoiceNode, StoreError> {
        if self.historical.as_ref().is_some_and(|history| history.block(message.root).is_some()) {
            return self.root_node(message.root, PayloadStatus::Pending)
        }
        let block = self.block(message.root)?;
        Ok(node(
            message.root,
            if block.slot < message.slot {
                if message.payload_present {
                    PayloadStatus::Full
                } else {
                    PayloadStatus::Empty
                }
            } else {
                PayloadStatus::Pending
            },
        ))
    }
    /// Derive availability/timeliness from authenticated PTC votes and local verified delivery.
    pub fn payload_timeliness(&self, root: Root, timely: bool) -> Result<bool, StoreError> {
        self.payload_vote(root, timely, &self.timeliness_votes)
    }
    /// PTC observations are conditional on local verified envelope delivery.
    pub fn payload_data_availability(
        &self,
        root: Root,
        available: bool,
    ) -> Result<bool, StoreError> {
        self.payload_vote(root, available, &self.availability_votes)
    }
    fn payload_vote(
        &self,
        root: Root,
        value: bool,
        votes: &BTreeMap<Root, Vec<Option<bool>>>,
    ) -> Result<bool, StoreError> {
        let votes = votes.get(&root).ok_or(StoreError::UnknownRoot)?;
        if !self.is_payload_verified(root) {
            return Ok(!value)
        }
        Ok(votes.iter().filter(|vote| **vote == Some(value)).count() >
            ptc_size(self.processing.preset()) / 2)
    }
    /// Whether a node represents the unresolved previous-slot payload decision.
    pub fn is_previous_slot_payload_decision(
        &self,
        current: ForkChoiceNode,
    ) -> Result<bool, StoreError> {
        Ok(self.block_header(current.root)?.slot.checked_add(1) == Some(self.current_slot()) &&
            current.payload_status.is_some_and(|status| status != PayloadStatus::Pending))
    }
    /// Decide extension using both signed PTC observations and the boosted child's commitment.
    pub fn should_extend_payload(&self, root: Root) -> Result<bool, StoreError> {
        if self.block(root)?.slot.checked_add(1) != Some(self.current_slot()) {
            return Err(StoreError::InvalidBlockTime)
        }
        if !self.is_payload_verified(root) {
            return Ok(false)
        }
        if (self.payload_timeliness(root, true)? && self.payload_data_availability(root, true)?) ||
            self.proposer_boost_root == [0; 32]
        {
            return Ok(true)
        }
        let boost = self.block(self.proposer_boost_root)?;
        Ok(boost.parent_root != root || self.parent_payload_status(boost)? == PayloadStatus::Full)
    }
    /// Choose whether a proposer can extend this decided head at its exact proposal slot.
    pub fn should_build_on_full(
        &self,
        head: ForkChoiceNode,
        slot: u64,
    ) -> Result<bool, StoreError> {
        if head.payload_status.is_none() {
            self.block_header(head.root)?;
            return Ok(false)
        }
        if status(head)? == PayloadStatus::Pending {
            return Err(StoreError::InvalidAttestationTarget)
        }
        if self.block(head.root)?.slot.checked_add(1) != Some(slot) {
            return Ok(status(head)? == PayloadStatus::Full)
        }
        Ok(status(head)? == PayloadStatus::Full &&
            !self.payload_timeliness(head.root, false)? &&
            !self.payload_data_availability(head.root, false)?)
    }
    fn tiebreaker(&self, current: ForkChoiceNode) -> Result<u8, StoreError> {
        if current.payload_status.is_none() {
            self.block_header(current.root)?;
            return Ok(0)
        }
        if self.is_previous_slot_payload_decision(current)? {
            return Ok(if status(current)? == PayloadStatus::Empty {
                1
            } else if self.should_extend_payload(current.root)? {
                2
            } else {
                0
            })
        }
        Ok(match status(current)? {
            PayloadStatus::Empty => 0,
            PayloadStatus::Full => 1,
            PayloadStatus::Pending => 2,
        })
    }
    /// Child nodes include separate EMPTY/FULL decisions followed by matching beacon children.
    pub fn get_node_children(
        &self,
        current: ForkChoiceNode,
    ) -> Result<Vec<ForkChoiceNode>, StoreError> {
        self.block_header(current.root)?;
        if self.blocks.contains_key(&current.root) != current.payload_status.is_some() {
            return Err(StoreError::InvalidAttestationTarget)
        }
        if current.payload_status == Some(PayloadStatus::Pending) {
            let mut children = vec![node(current.root, PayloadStatus::Empty)];
            if self.is_payload_verified(current.root) {
                children.push(node(current.root, PayloadStatus::Full));
            }
            return Ok(children)
        }
        let mut children = Vec::new();
        if let Some(history) = &self.historical {
            for root in history.migration_roots() {
                if root != current.root && self.block_header(root)?.parent_root == current.root {
                    children.push(self.root_node(root, PayloadStatus::Pending)?);
                }
            }
        }
        for (&root, block) in &self.blocks {
            if root != current.root &&
                block.parent_root == current.root &&
                self.parent_node(block)? == current
            {
                children.push(node(root, PayloadStatus::Pending));
            }
        }
        Ok(children)
    }
    fn voting_source(&self, root: Root) -> Result<Checkpoint, StoreError> {
        if self.current_slot() / self.slots_per_epoch() >
            self.block_header(root)?.slot / self.slots_per_epoch()
        {
            self.unrealized_justifications.get(&root).copied().ok_or(StoreError::UnknownRoot)
        } else {
            if self.blocks.contains_key(&root) {
                Ok(self
                    .read_block_state(root)?
                    .ok_or(StoreError::UnknownRoot)?
                    .current_justified_checkpoint)
            } else {
                crate::fast_confirmation::FastConfirmationView::voting_source(
                    self.historical.as_ref().ok_or(StoreError::UnknownRoot)?.as_ref(),
                    root,
                )
            }
        }
    }
    fn viable(&self, root: Root) -> Result<bool, StoreError> {
        let source = self.voting_source(root)?;
        let epoch = self.current_slot() / self.slots_per_epoch();
        let justified = self.justified.epoch == 0 ||
            source.epoch == self.justified.epoch ||
            source.epoch.saturating_add(2) >= epoch;
        let finalized = self.finalized.epoch == 0 ||
            self.get_checkpoint_block(root, self.finalized.epoch)? == self.finalized.root;
        Ok(justified && finalized)
    }
    /// Filter leaves by actual voting source/finality using a bounded iterative traversal.
    pub fn get_filtered_node_tree(&self) -> Result<BTreeSet<ForkChoiceNode>, StoreError> {
        let base = self.root_node(self.justified.root, PayloadStatus::Pending)?;
        let mut stack = vec![(base, false)];
        let mut viable = BTreeSet::new();
        let limit = self
            .block_count()
            .checked_mul(6)
            .and_then(|n| n.checked_add(2))
            .ok_or(StoreError::Overflow)?;
        let mut visited = 0usize;
        while let Some((current, expanded)) = stack.pop() {
            visited = visited.checked_add(1).ok_or(StoreError::Overflow)?;
            if visited > limit {
                return Err(StoreError::ResourceLimit)
            }
            let children = self.get_node_children(current)?;
            if children.is_empty() {
                if self.viable(current.root)? {
                    viable.insert(current);
                }
            } else if expanded {
                if children.iter().any(|child| viable.contains(child)) {
                    viable.insert(current);
                }
            } else {
                stack.push((current, true));
                stack.extend(children.into_iter().map(|child| (child, false)));
            }
        }
        Ok(viable)
    }
    fn justified_state(&self) -> Result<GraphStateRead, StoreError> {
        self.checkpoint_view(self.justified)
    }
    fn total_active(&self) -> Result<u64, StoreError> {
        let state = self.justified_state()?;
        let epoch = state.slot() / self.slots_per_epoch();
        state
            .validators()
            .iter()
            .filter(|v| v.activation_epoch <= epoch && epoch < v.exit_epoch)
            .try_fold(0u64, |sum, v| {
                sum.checked_add(v.effective_balance).ok_or(StoreError::Overflow)
            })
            .map(|sum| sum.max(1_000_000_000))
    }
    pub(super) fn has_retained_message_root(&self, root: Root) -> bool {
        self.blocks.contains_key(&root) ||
            self.historical.as_ref().is_some_and(|history| history.block(root).is_some())
    }
    /// Compute support from the actual justified registry and authenticated latest slot messages.
    pub fn get_attestation_score(&self, current: ForkChoiceNode) -> Result<u64, StoreError> {
        let state = self.justified_state()?;
        let epoch = state.slot() / self.slots_per_epoch();
        let mut weight = 0u64;
        for (index, validator) in state.validators().iter().enumerate() {
            let index = u64::try_from(index).map_err(|_| StoreError::Overflow)?;
            if validator.activation_epoch <= epoch &&
                epoch < validator.exit_epoch &&
                !validator.slashed &&
                !self.equivocating.contains(&index) &&
                let Some(&message) = self.latest_messages.get(&index) &&
                self.has_retained_message_root(message.root) &&
                self.is_ancestor(self.get_supported_node(message)?, current)?
            {
                weight =
                    weight.checked_add(validator.effective_balance).ok_or(StoreError::Overflow)?;
            }
        }
        Ok(weight)
    }
    fn committee_fraction(&self, percent: u64) -> Result<u64, StoreError> {
        (self.total_active()? / self.slots_per_epoch())
            .checked_mul(percent)
            .map(|n| n / 100)
            .ok_or(StoreError::Overflow)
    }
    /// Include equivocations from this exact head's committees when measuring weakness.
    pub fn is_head_weak(&self, root: Root) -> Result<bool, StoreError> {
        let mut weight =
            self.get_attestation_score(self.root_node(root, PayloadStatus::Pending)?)?;
        if self.historical.as_ref().is_some_and(|history| history.block(root).is_some()) {
            return self.historical_head_weak(root, weight)
        }
        let head = self.read_block_state(root)?.ok_or(StoreError::UnknownRoot)?;
        let block = self.block(root)?;
        let epoch = block.slot / self.slots_per_epoch();
        let active = head
            .validators
            .values
            .iter()
            .filter(|v| v.activation_epoch <= epoch && epoch < v.exit_epoch)
            .count() as u64;
        let (target, max) = match self.processing.preset() {
            Preset::Mainnet => (128, 64),
            Preset::Minimal => (4, 4),
        };
        let committees = (active / self.slots_per_epoch() / target).clamp(1, max);
        let justified = self.justified_state()?;
        for index in 0..committees {
            for validator in
                gloas_processing::get_beacon_committee(&head, block.slot, index, &self.processing)?
            {
                if self.equivocating.contains(&validator) {
                    let validator = justified
                        .validators()
                        .get(validator as usize)
                        .ok_or(StoreError::InvalidAttestationTarget)?;
                    weight = weight
                        .checked_add(validator.effective_balance)
                        .ok_or(StoreError::Overflow)?;
                }
            }
        }
        Ok(weight < self.committee_fraction(self.parameters.reorg_head_weight_threshold)?)
    }
    fn should_apply_boost(&self) -> Result<bool, StoreError> {
        if self.proposer_boost_root == [0; 32] {
            return Ok(false)
        }
        let block = self.block_header(self.proposer_boost_root)?;
        let parent = self.block_header(block.parent_root)?;
        if parent.slot.checked_add(1).ok_or(StoreError::Overflow)? < block.slot ||
            !self.is_head_weak(block.parent_root)?
        {
            return Ok(true)
        }
        for root in self.retained_roots() {
            let candidate = self.block_header(root)?;
            if root != block.parent_root &&
                candidate.slot.checked_add(1) == Some(block.slot) &&
                candidate.proposer_index == parent.proposer_index &&
                self.block_timeliness.get(&root).is_some_and(|timeliness| timeliness[1])
            {
                return Ok(false)
            }
        }
        Ok(true)
    }
    /// Exact branch weight; previous-slot payload decisions have zero weight and use PTC ties.
    pub fn get_weight(&self, current: ForkChoiceNode) -> Result<u64, StoreError> {
        if self.is_previous_slot_payload_decision(current)? {
            return Ok(0)
        }
        let mut weight = self.get_attestation_score(current)?;
        if self.should_apply_boost()? &&
            self.is_ancestor(
                self.root_node(self.proposer_boost_root, PayloadStatus::Pending)?,
                current,
            )?
        {
            weight = weight
                .checked_add(self.committee_fraction(self.parameters.proposer_score_boost)?)
                .ok_or(StoreError::Overflow)?;
        }
        Ok(weight)
    }
    /// LMD-GHOST selects a decided payload node with exact root/status tiebreaking.
    pub fn get_head(&self) -> Result<ForkChoiceNode, StoreError> {
        let viable = self.get_filtered_node_tree()?;
        if viable.is_empty() {
            return self.root_node(self.justified.root, PayloadStatus::Empty)
        }
        let mut head = self.root_node(self.justified.root, PayloadStatus::Pending)?;
        for _ in 0..=self.block_count().saturating_mul(2) {
            let mut best = None;
            for child in
                self.get_node_children(head)?.into_iter().filter(|child| viable.contains(child))
            {
                let score = (self.get_weight(child)?, child.root, self.tiebreaker(child)?);
                if best.as_ref().is_none_or(|(previous, _)| score > *previous) {
                    best = Some((score, child));
                }
            }
            if let Some((_, child)) = best {
                head = child;
            } else {
                return Ok(head)
            }
        }
        Err(StoreError::ResourceLimit)
    }
    /// Optional proposer reorganization uses exact payload branch, real weights and two deadlines.
    pub fn get_proposer_head(&self) -> Result<ForkChoiceNode, StoreError> {
        let head = self.get_head()?;
        if head.payload_status.is_none() {
            return Ok(head)
        }
        let block = self.block(head.root)?;
        let parent = self.block_header(block.parent_root)?;
        if self.proposer_boost_root == head.root {
            return Err(StoreError::ProposerBoostActive)
        }
        let head_weak = self.is_head_weak(head.root)?;
        let current_time_ok = block.slot.checked_add(1) == Some(self.current_slot());
        let mut same_proposals = 0usize;
        for root in self.retained_roots() {
            let candidate = self.block_header(root)?;
            if candidate.slot == block.slot && candidate.proposer_index == block.proposer_index {
                same_proposals += 1;
            }
        }
        let equivocation = same_proposals > 1;
        let ordinary = !self.block_timeliness.get(&head.root).ok_or(StoreError::UnknownRoot)?[0] &&
            self.unrealized_justifications.get(&head.root) ==
                self.unrealized_justifications.get(&block.parent_root) &&
            self.current_slot() / self.slots_per_epoch() - self.finalized.epoch <=
                self.parameters.reorg_max_epochs_since_finalization &&
            self.time_into_slot() <= self.deadline(self.parameters.proposer_reorg_cutoff_bps)? &&
            parent.slot.checked_add(1) == Some(block.slot) &&
            current_time_ok &&
            head_weak &&
            self.get_attestation_score(
                self.root_node(block.parent_root, PayloadStatus::Pending)?,
            )? > self.committee_fraction(self.parameters.reorg_parent_weight_threshold)?;
        if ordinary || (head_weak && current_time_ok && equivocation) {
            self.parent_node(block)
        } else {
            Ok(head)
        }
    }
    /// Determine whether a retained root can be the dependent block for this slot.
    pub fn is_valid_dependent_root(
        &self,
        root: Root,
        dependent_slot: u64,
    ) -> Result<bool, StoreError> {
        if self.get_head()?.root == root {
            return Ok(true)
        }
        for candidate in self.retained_roots() {
            let block = self.block_header(candidate)?;
            if block.parent_root == root && block.slot > dependent_slot {
                return Ok(true)
            }
        }
        Ok(false)
    }
}

impl GloasStore {
    /// Advance the real clock monotonically and pull up checkpoints at every crossed epoch.
    pub fn on_tick(&mut self, time_ms: u64) -> Result<(), StoreError> {
        self.on_tick_inner::<true>(time_ms)
    }
    fn on_tick_inner<const VERIFY_SIGNATURES: bool>(
        &mut self,
        time_ms: u64,
    ) -> Result<(), StoreError> {
        policy::require::<VERIFY_SIGNATURES>()?;
        if time_ms < self.time_ms || time_ms < self.genesis_time_ms {
            return Err(StoreError::TimeRegression)
        }
        let target = (time_ms - self.genesis_time_ms) / self.slot_duration_ms();
        let current = self.current_slot();
        if target - current > self.parameters.max_tick_slots {
            return Err(StoreError::ResourceLimit)
        }
        let mut candidate = self.clone();
        if target > current {
            candidate.proposer_boost_root = [0; 32];
            for slot in current.checked_add(1).ok_or(StoreError::Overflow)?..=target {
                if slot.is_multiple_of(self.slots_per_epoch()) {
                    candidate.realized::<VERIFY_SIGNATURES>(
                        candidate.unrealized_justified,
                        candidate.unrealized_finalized,
                    )?;
                }
            }
        }
        candidate.time_ms = time_ms;
        candidate.check_budget()?;
        *self = candidate;
        Ok(())
    }
    /// Admit the exact signed beacon block, including authenticated in-block PTC observations.
    /// FULL-parent children require a previously verified local execution envelope.
    pub fn on_block(&mut self, signed: &SignedBeaconBlock) -> Result<(), StoreError> {
        self.on_block_inner::<true>(signed)
    }
    fn on_block_inner<const VERIFY_SIGNATURES: bool>(
        &mut self,
        signed: &SignedBeaconBlock,
    ) -> Result<(), StoreError> {
        policy::require::<VERIFY_SIGNATURES>()?;
        let block = &signed.message;
        let root = block.hash_tree_root(self.processing.preset())?;
        if self.blocks.contains_key(&root) {
            return Ok(())
        }
        let parent_node = self.parent_node(block)?;
        if parent_node.payload_status == Some(PayloadStatus::Full) &&
            !self.is_payload_verified(block.parent_root)
        {
            return Err(ProcessingError::InvalidState(
                "full parent envelope is not locally verified",
            )
            .into())
        }
        let parent_slot = self.state_view(block.parent_root)?.slot();
        self.check_block_time_and_ancestry(block, parent_slot)?;
        let state = if let Some(parent) = self.read_block_state(block.parent_root)? {
            parent.as_ref().clone()
        } else {
            let history = self.historical.as_ref().ok_or(StoreError::UnknownRoot)?;
            history.verified_execution_block_hash(block.parent_root)?;
            let parameters = history.transition_parameters().ok_or(StoreError::UnknownRoot)?;
            if parameters.fork_at_slot(block.slot)? != reth_embedded_beacon_primitives::Fork::Gloas
            {
                return Err(StoreError::AnchorMismatch)
            }
            let mut state = history
                .read_block_state(block.parent_root)?
                .ok_or(StoreError::UnknownRoot)?
                .as_ref()
                .clone();
            reth_embedded_beacon_state_transition::fork_processing::process_slots(
                &mut state, block.slot, parameters,
            )?;
            let reth_embedded_beacon_state_transition::ssz::BeaconState::Gloas(state, _) = state
            else {
                return Err(StoreError::AnchorMismatch)
            };
            state
        };
        let state =
            policy::state_transition_owned::<VERIFY_SIGNATURES>(state, signed, &self.processing)?;
        self.finish_block::<VERIFY_SIGNATURES>(signed, state)
    }

    // Only an authenticated complete post-state from the mandatory signed transition enters.
    fn finish_block<const VERIFY_SIGNATURES: bool>(
        &mut self,
        signed: &SignedBeaconBlock,
        state: BeaconState,
    ) -> Result<(), StoreError> {
        let block = &signed.message;
        let root = block.hash_tree_root(self.processing.preset())?;
        let head = self.get_head()?;
        let mut candidate = self.clone();
        candidate.blocks.insert(root, Arc::new(block.clone()));
        candidate.block_states.insert(root, Arc::new(state).into());
        candidate.timeliness_votes.insert(root, vec![None; ptc_size(self.processing.preset())]);
        candidate.availability_votes.insert(root, vec![None; ptc_size(self.processing.preset())]);
        let state = candidate.read_block_state(root)?.ok_or(StoreError::UnknownRoot)?;
        if state.slot != 0 {
            for attestation in &block.body.payload_attestations.values {
                let indexed = gloas_processing::get_indexed_payload_attestation(
                    &state,
                    attestation,
                    &candidate.processing,
                )?;
                for index in indexed.attesting_indices {
                    candidate.payload_message_inner::<VERIFY_SIGNATURES>(
                        &PayloadAttestationMessage {
                            validator_index: index,
                            data: indexed.data.clone(),
                            signature: [0; 96],
                        },
                        true,
                    )?;
                }
            }
        }
        let current_slot = block.slot == candidate.current_slot();
        candidate.block_timeliness.insert(
            root,
            [
                current_slot &&
                    candidate.time_into_slot() <
                        candidate.deadline(candidate.parameters.gloas_attestation_due_bps)?,
                current_slot &&
                    candidate.time_into_slot() <
                        candidate
                            .deadline(candidate.parameters.payload_attestation_due_bps)?,
            ],
        );
        let epoch = candidate.current_slot() / candidate.slots_per_epoch();
        let dependent_slot = epoch
            .saturating_sub(1)
            .checked_mul(candidate.slots_per_epoch())
            .ok_or(StoreError::Overflow)?
            .saturating_sub(1);
        // A trusted checkpoint may begin after the shuffling dependency with no retained
        // pre-checkpoint parent. Mandatory block admission remains valid, but that absence
        // cannot establish the common dependency required for optional proposer boost.
        // Genuine migrated graphs retain their original history and use the exact lookup.
        let boost_dependency_available = candidate
            .execution_query_anchor
            .and_then(|anchor| candidate.blocks.get(&anchor))
            .is_none_or(|anchor| {
                dependent_slot >= anchor.slot ||
                    candidate.blocks.contains_key(&anchor.parent_root) ||
                    candidate
                        .historical
                        .as_ref()
                        .is_some_and(|history| history.block(anchor.parent_root).is_some())
            });
        if boost_dependency_available &&
            candidate.proposer_boost_root == [0; 32] &&
            candidate.block_timeliness[&root][0] &&
            candidate
                .get_ancestor(
                    candidate.root_node(head.root, PayloadStatus::Pending)?,
                    dependent_slot,
                )?
                .root ==
                candidate
                    .get_ancestor(node(root, PayloadStatus::Pending), dependent_slot)?
                    .root
        {
            candidate.proposer_boost_root = root;
        }
        candidate.realized::<VERIFY_SIGNATURES>(
            state.current_justified_checkpoint,
            state.finalized_checkpoint,
        )?;
        let (unrealized_justified, unrealized_finalized) =
            gloas_processing::get_unrealized_checkpoints(&state, &candidate.processing)?;
        candidate.unrealized_justifications.insert(root, unrealized_justified);
        if unrealized_justified.epoch > candidate.unrealized_justified.epoch {
            candidate.unrealized_justified = unrealized_justified;
        }
        if unrealized_finalized.epoch > candidate.unrealized_finalized.epoch {
            candidate.unrealized_finalized = unrealized_finalized;
        }
        if block.slot / candidate.slots_per_epoch() < epoch {
            candidate.realized::<VERIFY_SIGNATURES>(unrealized_justified, unrealized_finalized)?;
        }
        candidate.check_budget()?;
        *self = candidate;
        Ok(())
    }
    /// Admit a signed block and feed only its real retained attestations/slashings into LMD.
    pub fn on_block_and_contents(&mut self, signed: &SignedBeaconBlock) -> Result<(), StoreError> {
        self.on_block_and_contents_inner::<true>(signed)
    }
    fn on_block_and_contents_inner<const VERIFY_SIGNATURES: bool>(
        &mut self,
        signed: &SignedBeaconBlock,
    ) -> Result<(), StoreError> {
        policy::require::<VERIFY_SIGNATURES>()?;
        let mut candidate = self.clone();
        candidate.on_block_inner::<VERIFY_SIGNATURES>(signed)?;
        let root = signed.message.hash_tree_root(self.processing.preset())?;
        let block = candidate.blocks.get(&root).ok_or(StoreError::UnknownRoot)?.clone();
        for attestation in &block.body.attestations.values {
            candidate.attestation_inner::<VERIFY_SIGNATURES>(attestation, true)?;
        }
        for slashing in &block.body.attester_slashings.values {
            candidate.on_attester_slashing_inner::<VERIFY_SIGNATURES>(slashing)?;
        }
        candidate.check_budget()?;
        *self = candidate;
        Ok(())
    }
    /// Authenticate an ordinary wire vote against its exact checkpoint state and committees.
    pub fn on_attestation(&mut self, attestation: &gloas::Attestation) -> Result<(), StoreError> {
        self.on_attestation_inner::<true>(attestation)
    }
    fn on_attestation_inner<const VERIFY_SIGNATURES: bool>(
        &mut self,
        attestation: &gloas::Attestation,
    ) -> Result<(), StoreError> {
        policy::require::<VERIFY_SIGNATURES>()?;
        let mut candidate = self.clone();
        candidate.attestation_inner::<VERIFY_SIGNATURES>(attestation, false)?;
        candidate.check_budget()?;
        *self = candidate;
        Ok(())
    }
    fn attestation_inner<const VERIFY_SIGNATURES: bool>(
        &mut self,
        attestation: &gloas::Attestation,
        from_block: bool,
    ) -> Result<(), StoreError> {
        attestation.hash_tree_root(self.processing.preset())?;
        let data = &attestation.data;
        self.validate_attestation_target(data, from_block)?;
        self.ensure_checkpoint::<VERIFY_SIGNATURES>(data.target)?;
        let indices =
            self.validate_checkpoint_attestation::<VERIFY_SIGNATURES>(data.target, attestation)?;
        for index in indices {
            if !self.equivocating.contains(&index) &&
                self.latest_messages.get(&index).is_none_or(|previous| data.slot > previous.slot)
            {
                self.latest_messages.insert(
                    index,
                    LatestMessage {
                        slot: data.slot,
                        root: data.beacon_block_root,
                        payload_present: data.index == 1,
                    },
                );
            }
        }
        Ok(())
    }
    /// Require a real slashable pair and both aggregate signatures before excluding validators.
    pub fn on_attester_slashing(
        &mut self,
        slashing: &gloas::AttesterSlashing,
    ) -> Result<(), StoreError> {
        self.on_attester_slashing_inner::<true>(slashing)
    }
    fn on_attester_slashing_inner<const VERIFY_SIGNATURES: bool>(
        &mut self,
        slashing: &gloas::AttesterSlashing,
    ) -> Result<(), StoreError> {
        policy::require::<VERIFY_SIGNATURES>()?;
        slashing.hash_tree_root(self.processing.preset())?;
        let first = &slashing.attestation_1;
        let second = &slashing.attestation_2;
        let a = &first.data;
        let b = &second.data;
        if !((a != b && a.target.epoch == b.target.epoch) ||
            (a.source.epoch < b.source.epoch && b.target.epoch < a.target.epoch))
        {
            return Err(StoreError::InvalidSlashing)
        }
        if let Some(state) = self.read_block_state(self.justified.root)? {
            policy::indexed_attestation::<VERIFY_SIGNATURES>(&state, first, &self.processing)?;
            policy::indexed_attestation::<VERIFY_SIGNATURES>(&state, second, &self.processing)?;
        } else {
            self.validate_historical_indexed(first)?;
            self.validate_historical_indexed(second)?;
        }
        self.equivocating.extend(
            first
                .attesting_indices
                .values
                .iter()
                .copied()
                .filter(|index| second.attesting_indices.values.binary_search(index).is_ok()),
        );
        Ok(())
    }
    /// Authenticate a current-slot PTC wire message; all repeated committee positions update.
    pub fn on_payload_attestation_message(
        &mut self,
        message: &PayloadAttestationMessage,
    ) -> Result<(), StoreError> {
        self.on_payload_attestation_message_inner::<true>(message)
    }
    fn on_payload_attestation_message_inner<const VERIFY_SIGNATURES: bool>(
        &mut self,
        message: &PayloadAttestationMessage,
    ) -> Result<(), StoreError> {
        policy::require::<VERIFY_SIGNATURES>()?;
        let mut candidate = self.clone();
        candidate.payload_message_inner::<VERIFY_SIGNATURES>(message, false)?;
        *self = candidate;
        Ok(())
    }
    fn payload_message_inner<const VERIFY_SIGNATURES: bool>(
        &mut self,
        message: &PayloadAttestationMessage,
        from_block: bool,
    ) -> Result<(), StoreError> {
        message.hash_tree_root(self.processing.preset())?;
        let data = &message.data;
        let state =
            self.read_block_state(data.beacon_block_root)?.ok_or(StoreError::UnknownRoot)?;
        if data.slot != state.slot {
            return Ok(())
        }
        let ptc = gloas_processing::get_ptc(&state, data.slot, &self.processing)?;
        let positions = ptc
            .values
            .iter()
            .enumerate()
            .filter_map(|(position, &index)| (index == message.validator_index).then_some(position))
            .collect::<Vec<_>>();
        if positions.is_empty() {
            return Err(
                ProcessingError::InvalidState("payload attester is not in the assigned PTC").into()
            )
        }
        if !from_block {
            if data.slot != self.current_slot() {
                return Err(StoreError::InvalidAttestationTime)
            }
            policy::indexed_payload_attestation::<VERIFY_SIGNATURES>(
                &state,
                &IndexedPayloadAttestation {
                    attesting_indices: vec![message.validator_index],
                    data: data.clone(),
                    signature: message.signature,
                },
                &self.processing,
            )?;
        }
        let timely = self
            .timeliness_votes
            .get_mut(&data.beacon_block_root)
            .ok_or(StoreError::UnknownRoot)?;
        let available = self
            .availability_votes
            .get_mut(&data.beacon_block_root)
            .ok_or(StoreError::UnknownRoot)?;
        for position in positions {
            timely[position] = Some(data.payload_present);
            available[position] = Some(data.blob_data_available);
        }
        Ok(())
    }
    /// Verify custody/proofs, signature/linkage and actual Engine result before publishing FULL.
    /// Cancellation or either verifier's failure leaves every retained graph/vote map unchanged.
    pub async fn on_execution_payload_envelope<E: ExecutionEngine, D: DataAvailability>(
        &mut self,
        signed: &SignedExecutionPayloadEnvelope,
        engine: &E,
        availability: &D,
    ) -> Result<(), EnvelopeError> {
        self.on_execution_payload_envelope_inner::<true, E, D>(signed, engine, availability).await
    }
    async fn on_execution_payload_envelope_inner<
        const VERIFY_SIGNATURES: bool,
        E: ExecutionEngine,
        D: DataAvailability,
    >(
        &mut self,
        signed: &SignedExecutionPayloadEnvelope,
        engine: &E,
        availability: &D,
    ) -> Result<(), EnvelopeError> {
        policy::require::<VERIFY_SIGNATURES>()?;
        let root = signed.message.beacon_block_root;
        let state = self.read_block_state(root)?.ok_or(StoreError::UnknownRoot)?;
        let block = self.blocks.get(&root).ok_or(StoreError::UnknownRoot)?;
        signed.hash_tree_root(self.processing.preset()).map_err(StoreError::from)?;
        availability
            .verify(root, block)
            .await
            .map_err(|error| EnvelopeError::DataAvailability(error.to_string()))?;
        policy::verify_envelope::<VERIFY_SIGNATURES, E>(&state, signed, &self.processing, engine)
            .await
            .map_err(|error| EnvelopeError::Verification(error.to_string()))?;
        let mut candidate = self.clone();
        candidate.payloads.insert(root, Arc::new(signed.message.clone()));
        candidate.check_budget()?;
        *self = candidate;
        Ok(())
    }

    /// Authenticate one actual attestation from an already admitted retained Gloas block.
    /// Only the privately resolved body item receives the retained-block timing rules.
    pub fn on_retained_block_attestation(
        &mut self,
        root: Root,
        index: usize,
    ) -> Result<(), StoreError> {
        let block = self.blocks.get(&root).ok_or(StoreError::UnknownRoot)?.clone();
        let attestation = block
            .body
            .attestations
            .values
            .get(index)
            .ok_or(StoreError::InvalidAttestationTarget)?;
        let mut candidate = self.clone();
        candidate.attestation_inner::<true>(attestation, true)?;
        candidate.check_budget()?;
        *self = candidate;
        Ok(())
    }

    /// Authenticate one actual slashing from an already admitted retained Gloas block.
    /// Any rejection leaves the graph, votes, equivocation set and caches unchanged.
    pub fn on_retained_block_slashing(
        &mut self,
        root: Root,
        index: usize,
    ) -> Result<(), StoreError> {
        let block = self.blocks.get(&root).ok_or(StoreError::UnknownRoot)?.clone();
        let slashing =
            block.body.attester_slashings.values.get(index).ok_or(StoreError::InvalidSlashing)?;
        let mut candidate = self.clone();
        candidate.on_attester_slashing_inner::<true>(slashing)?;
        candidate.check_budget()?;
        *self = candidate;
        Ok(())
    }
}

impl GloasStore {
    /// Immutable PTC timeliness observations, preserving every duplicate committee position.
    pub fn payload_timeliness_votes(&self, root: Root) -> Option<&[Option<bool>]> {
        self.timeliness_votes.get(&root).map(Vec::as_slice)
    }
    /// Immutable PTC data observations; these cannot grant local payload verification.
    pub fn payload_data_availability_votes(&self, root: Root) -> Option<&[Option<bool>]> {
        self.availability_votes.get(&root).map(Vec::as_slice)
    }
}

#[cfg(test)]
mod tests;

mod checkpoint_anchor;

mod original_slashing;
