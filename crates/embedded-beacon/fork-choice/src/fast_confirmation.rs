//! Fork-aware probabilistic fast confirmation over sealed authenticated consensus graphs.
//! Confirmations depend on the configured Byzantine bound and timely delivery assumptions.
//! They do not replace finality, block import, execution or data availability verification.

use crate::{ForkChoiceNode, PayloadStatus, StoreError, StoreParameters};
use reth_embedded_beacon_primitives::{Checkpoint, Root};
use reth_embedded_beacon_state_transition::{
    altair_processing, bellatrix_processing, capella_processing, deneb_processing,
    electra_processing,
    fork_processing::TransitionParameters,
    fulu_processing, gloas_processing, phase0_processing,
    ssz::{gloas, phase0::Validator, BeaconState, Preset},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    marker::PhantomData,
};

pub(crate) mod sealed {
    pub trait Sealed {}
}

/// Actual borrowed registry data from a privately validated graph, without shared Arc extraction.
#[derive(Clone, Copy, Debug)]
pub enum StateRef<'a> {
    /// Earlier complete fork-aware state wrapper.
    Classic(&'a BeaconState),
    /// Dedicated complete Gloas state and checked preset.
    Gloas(&'a gloas::BeaconState, Preset),
}
impl<'a> StateRef<'a> {
    /// Actual state slot.
    pub const fn slot(self) -> u64 {
        match self {
            Self::Classic(s) => s.slot(),
            Self::Gloas(s, _) => s.slot,
        }
    }
    /// Borrow the complete actual validator registry.
    pub const fn validators(self) -> &'a [Validator] {
        match self {
            Self::Classic(BeaconState::Phase0(s, _)) => s.validators.values.as_slice(),
            Self::Classic(BeaconState::Altair(s, _)) => s.validators.values.as_slice(),
            Self::Classic(BeaconState::Bellatrix(s, _)) => s.validators.values.as_slice(),
            Self::Classic(BeaconState::Capella(s, _)) => s.validators.values.as_slice(),
            Self::Classic(BeaconState::Deneb(s, _)) => s.validators.values.as_slice(),
            Self::Classic(BeaconState::Electra(s, _)) => s.validators.values.as_slice(),
            Self::Classic(BeaconState::Fulu(s, _)) => s.validators.values.as_slice(),
            Self::Classic(BeaconState::Gloas(s, _)) | Self::Gloas(s, _) => {
                s.validators.values.as_slice()
            }
        }
    }
    fn owned(self) -> BeaconState {
        match self {
            Self::Classic(s) => s.clone(),
            Self::Gloas(s, p) => BeaconState::Gloas(s.clone(), p),
        }
    }
}

/// Checked fork configuration borrowed from the exact graph.
#[derive(Clone, Copy, Debug)]
pub enum ProcessingRef<'a> {
    /// All scheduled fork configurations.
    Scheduled(&'a TransitionParameters),
    /// Dedicated Gloas configuration.
    Gloas(&'a gloas_processing::GloasParameters),
    /// Original Phase0/Altair graph parameters.
    Legacy,
}

/// Exact immutable block identity required by the confirmation algorithm.
#[derive(Clone, Copy, Debug)]
pub struct BlockInfo {
    /// Actual slot of the block.
    pub slot: u64,
    /// Actual committed parent root.
    pub parent_root: Root,
    /// Execution hash safe when this beacon block is confirmed; Gloas uses the parent hash.
    pub safe_execution_hash: Option<Root>,
}

/// A privately admitted latest vote and its fork-aware supported node.
#[derive(Clone, Copy, Debug)]
pub struct Message {
    /// Actual target epoch of this vote.
    pub epoch: u64,
    /// Actual signed beacon root.
    pub root: Root,
    /// Exact supported payload branch, where relevant.
    pub supported_node: ForkChoiceNode,
}

/// Sealed immutable graph queries; public callers cannot implement a forged confirmation view.
/// Fixture-only graphs remain distinct generic types, with no conversion into runtime graphs.
pub trait FastConfirmationView: sealed::Sealed + Clone {
    /// Runtime views are strict; an isolated artificial fixture view fixes this to false.
    const VERIFY_SIGNATURES: bool;
    /// Privately checked clock, preset and retention parameters.
    fn parameters(&self) -> StoreParameters;
    /// Checked fork parameters for committee derivation and slot replay.
    fn processing(&self) -> ProcessingRef<'_>;
    /// Current checked genesis-relative slot.
    fn current_slot(&self) -> u64;
    /// Current clock in milliseconds.
    fn time_ms(&self) -> u64;
    /// Checked genesis clock in milliseconds.
    fn genesis_time_ms(&self) -> u64;
    /// Exact current realized checkpoints.
    fn checkpoints(&self) -> (Checkpoint, Checkpoint);
    /// Greatest privately observed unrealized justified checkpoint.
    fn unrealized_justified(&self) -> Checkpoint;
    /// Actual ordinary head.
    fn head(&self) -> Result<ForkChoiceNode, StoreError>;
    /// Actual retained block data.
    fn block_info(&self, root: Root) -> Result<BlockInfo, StoreError>;
    /// Actual complete retained post-state.
    fn state(&self, root: Root) -> Result<StateRef<'_>, StoreError>;
    /// Complete actual replayed checkpoint state.
    fn checkpoint_state(&self, checkpoint: Checkpoint) -> Result<StateRef<'_>, StoreError>;
    /// Latest admitted message for this validator, without exporting the private message map.
    fn latest(&self, index: u64) -> Result<Option<Message>, StoreError>;
    /// Actual recorded equivocation.
    fn equivocating(&self, index: u64) -> bool;
    /// Exact fork-aware ancestry relation.
    fn is_ancestor(
        &self,
        node: ForkChoiceNode,
        ancestor: ForkChoiceNode,
    ) -> Result<bool, StoreError>;
    /// Exact fork-aware ancestor at a slot.
    fn ancestor(&self, node: ForkChoiceNode, slot: u64) -> Result<ForkChoiceNode, StoreError>;
    /// State-derived realized/pulled-up voting source.
    fn voting_source(&self, root: Root) -> Result<Checkpoint, StoreError>;
    /// Exact privately derived unrealized justification for this block.
    fn unrealized_justification(&self, root: Root) -> Result<Checkpoint, StoreError>;
    /// Whether this graph has the separate Gloas payload branches.
    fn is_gloas(&self) -> bool;
    /// Only actual Gloas blocks have separate payload branches; historical schemas remain typed.
    fn has_payload_branches(&self, _root: Root) -> bool {
        self.is_gloas()
    }
}

/// Explicit assumptions and bounded work for probabilistic confirmation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Parameters {
    /// Maximum assumed Byzantine effective balance percentage, bounded by the pinned maximum of
    /// 25.
    pub byzantine_threshold: u64,
    /// Maximum slots in one committee/support query.
    pub max_committee_slots: u64,
}
impl Default for Parameters {
    fn default() -> Self {
        Self { byzantine_threshold: 25, max_committee_slots: 8192 }
    }
}
impl Parameters {
    const fn validate(self) -> Result<(), StoreError> {
        if self.byzantine_threshold > 25 || self.max_committee_slots == 0 {
            return Err(StoreError::InvalidParameters)
        }
        Ok(())
    }
}

/// Small retained confirmation observations; plain values confer no import or finality authority.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Snapshot {
    /// Most recently probabilistically confirmed unsigned beacon root.
    pub confirmed_root: Root,
    /// Observed justified checkpoint at the start of the previous epoch.
    pub previous_epoch_observed_justified_checkpoint: Checkpoint,
    /// Observed justified checkpoint at the start of the current epoch.
    pub current_epoch_observed_justified_checkpoint: Checkpoint,
    /// Greatest unrealized checkpoint observed at the start of the last epoch slot.
    pub previous_epoch_greatest_unrealized_checkpoint: Checkpoint,
    /// Head captured at the start of the previous slot.
    pub previous_slot_head: Root,
    /// Head captured at the start of the current slot.
    pub current_slot_head: Root,
}

// Fork configuration is retained once, so replacing the schedule or kernel policy cannot
// transplant prior confirmations even if the same genesis and clock are reused.
#[derive(Clone, Debug, PartialEq, Eq)]
enum ProcessingIdentity {
    Scheduled(Box<TransitionParameters>),
    Gloas(Box<gloas_processing::GloasParameters>),
    Legacy,
}
impl ProcessingIdentity {
    fn capture(parameters: ProcessingRef<'_>) -> Self {
        match parameters {
            ProcessingRef::Scheduled(parameters) => Self::Scheduled(Box::new(parameters.clone())),
            ProcessingRef::Gloas(parameters) => Self::Gloas(Box::new(parameters.clone())),
            ProcessingRef::Legacy => Self::Legacy,
        }
    }
    fn matches(&self, parameters: ProcessingRef<'_>) -> bool {
        match (self, parameters) {
            (Self::Scheduled(expected), ProcessingRef::Scheduled(actual)) => {
                expected.as_ref() == actual
            }
            (Self::Gloas(expected), ProcessingRef::Gloas(actual)) => expected.as_ref() == actual,
            (Self::Legacy, ProcessingRef::Legacy) => true,
            _ => false,
        }
    }
}

/// Confirmation observations bound to one graph type and genesis identity.
/// An artificial fixture graph cannot turn this value into runtime confirmation state.
#[derive(Clone, Debug)]
pub struct FastConfirmation<G> {
    snapshot: Snapshot,
    parameters: Parameters,
    anchor: Root,
    genesis_time_ms: u64,
    graph_parameters: StoreParameters,
    processing_identity: ProcessingIdentity,
    updated_slot: Option<u64>,
    graph_type: PhantomData<fn(&G)>,
}
impl<G: FastConfirmationView> FastConfirmation<G> {
    /// Conservatively initialize all observations at the same graph's trusted finalized checkpoint.
    pub fn new(graph: &G, parameters: Parameters) -> Result<Self, StoreError> {
        parameters.validate()?;
        let finalized = graph.checkpoints().1;
        graph.block_info(finalized.root)?;
        Ok(Self {
            snapshot: Snapshot {
                confirmed_root: finalized.root,
                previous_epoch_observed_justified_checkpoint: finalized,
                current_epoch_observed_justified_checkpoint: finalized,
                previous_epoch_greatest_unrealized_checkpoint: finalized,
                previous_slot_head: finalized.root,
                current_slot_head: finalized.root,
            },
            parameters,
            anchor: finalized.root,
            genesis_time_ms: graph.genesis_time_ms(),
            graph_parameters: graph.parameters(),
            processing_identity: ProcessingIdentity::capture(graph.processing()),
            updated_slot: None,
            graph_type: PhantomData,
        })
    }
    /// Borrow-free readout of the exact retained confirmation variables.
    pub const fn snapshot(&self) -> Snapshot {
        self.snapshot
    }
    fn bind(&self, graph: &G) -> Result<(), StoreError> {
        if graph.genesis_time_ms() != self.genesis_time_ms ||
            graph.parameters() != self.graph_parameters ||
            !self.processing_identity.matches(graph.processing())
        {
            return Err(StoreError::AnchorMismatch)
        }
        graph.block_info(self.anchor)?;
        Ok(())
    }
    /// Update slot/epoch observations exactly once, before the actual attestation deadline.
    pub fn update_variables(&mut self, graph: &G) -> Result<(), StoreError> {
        let next = self.next_variables(graph)?;
        self.snapshot = next;
        self.updated_slot = Some(graph.current_slot());
        Ok(())
    }

    /// Plan exact source-state reads for the next timely native observation without updating it.
    /// Epoch rotation can discard an older observed source already retired by ordinary native
    /// pruning. Only the rotated sources and actual realized checkpoints need complete leases.
    pub fn checkpoint_states_for_update(&self, graph: &G) -> Result<Vec<Checkpoint>, StoreError> {
        let next = self.next_variables(graph)?;
        let (justified, finalized) = graph.checkpoints();
        Ok([
            justified,
            finalized,
            next.previous_epoch_observed_justified_checkpoint,
            next.current_epoch_observed_justified_checkpoint,
        ]
        .into_iter()
        .map(|checkpoint| (checkpoint.epoch, checkpoint.root))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(|(epoch, root)| Checkpoint { epoch, root })
        .collect())
    }

    fn next_variables(&self, graph: &G) -> Result<Snapshot, StoreError> {
        self.bind(graph)?;
        let slot = graph.current_slot();
        if self.updated_slot.is_some_and(|last| last >= slot) {
            return Err(StoreError::InvalidParameters)
        }
        let since_start =
            graph.time_ms().checked_sub(graph.genesis_time_ms()).ok_or(StoreError::Overflow)? %
                self.graph_parameters.slot_duration_ms;
        let due_bps = if graph.is_gloas() {
            self.graph_parameters.gloas_attestation_due_bps
        } else {
            self.graph_parameters.attestation_due_bps
        };
        let due = mul(self.graph_parameters.slot_duration_ms, due_bps)? / 10_000;
        if since_start >= due {
            return Err(StoreError::InvalidParameters)
        }
        let mut next = self.snapshot;
        next.previous_slot_head = next.current_slot_head;
        next.current_slot_head = graph.head()?.root;
        let slots = self.graph_parameters.phase0.slots_per_epoch();
        if add(slot, 1)?.is_multiple_of(slots) {
            next.previous_epoch_greatest_unrealized_checkpoint = graph.unrealized_justified();
        }
        if slot.is_multiple_of(slots) {
            next.previous_epoch_observed_justified_checkpoint =
                next.current_epoch_observed_justified_checkpoint;
            next.current_epoch_observed_justified_checkpoint =
                next.previous_epoch_greatest_unrealized_checkpoint;
        }
        Ok(next)
    }
    /// Compute the latest confirmation at any point in the current slot without changing
    /// observations.
    pub fn latest_confirmed(&self, graph: &G) -> Result<Root, StoreError> {
        self.bind(graph)?;
        Computation::new(graph, self.parameters)?.latest(self.snapshot)
    }
    /// Apply the mandatory observation-update then confirmation-computation sequence atomically.
    pub fn on_fast_confirmation(&mut self, graph: &G) -> Result<(), StoreError> {
        let mut next = self.clone();
        next.update_variables(graph)?;
        next.snapshot.confirmed_root = next.latest_confirmed(graph)?;
        *self = next;
        Ok(())
    }
    /// Exact safe execution hash; Gloas confirms only the parent payload of the beacon block.
    pub fn safe_execution_block_hash(&self, graph: &G) -> Result<Root, StoreError> {
        self.bind(graph)?;
        graph
            .block_info(self.snapshot.confirmed_root)?
            .safe_execution_hash
            .ok_or(StoreError::InvalidParameters)
    }
    /// Recover a prior confirmation only after sync and the normal confirmation handler have run.
    pub fn restart_resilient_confirmed_root(
        &self,
        graph: &G,
        before_restart: Root,
    ) -> Result<Root, StoreError> {
        self.bind(graph)?;
        if self.updated_slot.is_none() {
            return Err(StoreError::InvalidParameters)
        }
        let old = graph.block_info(before_restart)?.slot;
        let current = graph.block_info(self.snapshot.confirmed_root)?.slot;
        let slots = self.graph_parameters.phase0.slots_per_epoch();
        let next_epoch = add(old, slots - 1)? / slots;
        if old <= current ||
            mul(add(next_epoch, 2)?, slots)? <= graph.current_slot() ||
            !graph.is_ancestor(graph.head()?, node(graph, before_restart))?
        {
            return Ok(self.snapshot.confirmed_root)
        }
        Ok(before_restart)
    }
}

fn add(a: u64, b: u64) -> Result<u64, StoreError> {
    a.checked_add(b).ok_or(StoreError::Overflow)
}
fn mul(a: u64, b: u64) -> Result<u64, StoreError> {
    a.checked_mul(b).ok_or(StoreError::Overflow)
}
fn node<G: FastConfirmationView>(g: &G, root: Root) -> ForkChoiceNode {
    ForkChoiceNode {
        root,
        payload_status: g.has_payload_branches(root).then_some(PayloadStatus::Pending),
    }
}
const fn active(v: &Validator, epoch: u64) -> bool {
    v.activation_epoch <= epoch && epoch < v.exit_epoch
}
fn total(state: StateRef<'_>, slots: u64) -> Result<u64, StoreError> {
    state
        .validators()
        .iter()
        .filter(|v| active(v, state.slot() / slots))
        .try_fold(0, |sum, v| add(sum, v.effective_balance))
        .map(|balance| balance.max(1_000_000_000))
}

/// Whether an inclusive interval includes every slot of at least one complete epoch.
pub fn is_full_validator_set_covered(
    start: u64,
    end: u64,
    slots_per_epoch: u64,
) -> Result<bool, StoreError> {
    if slots_per_epoch == 0 {
        return Err(StoreError::InvalidParameters)
    }
    Ok(add(start, slots_per_epoch - 1)? / slots_per_epoch < add(end, 1)? / slots_per_epoch)
}
/// Conservative exact pinned committee weight estimate, including cross-epoch safety adjustment.
pub fn estimate_committee_weight_between_slots(
    total_active_balance: u64,
    start: u64,
    end: u64,
    slots: u64,
) -> Result<u64, StoreError> {
    if slots == 0 {
        return Err(StoreError::InvalidParameters)
    }
    if start > end {
        return Ok(0)
    }
    if is_full_validator_set_covered(start, end, slots)? {
        return Ok(total_active_balance)
    }
    let committee = total_active_balance / slots;
    if start / slots == end / slots {
        return mul(committee, add(end - start, 1)?)
    }
    let end_count = end % slots + 1;
    let start_weight = mul(committee, slots - start % slots)?;
    let estimate = add(mul(start_weight / slots, slots - end_count)?, mul(committee, end_count)?)?;
    mul(estimate.div_ceil(1000), 1005)
}

struct Computation<'a, G> {
    graph: &'a G,
    parameters: Parameters,
    slots: u64,
    committees: BTreeMap<u64, BTreeSet<u64>>,
    head: ForkChoiceNode,
}
impl<'a, G: FastConfirmationView> Computation<'a, G> {
    fn new(graph: &'a G, parameters: Parameters) -> Result<Self, StoreError> {
        // The graph is immutably borrowed for this computation, so its actual validated
        // head and committee derivations are shared across every candidate comparison.
        Ok(Self {
            graph,
            parameters,
            slots: graph.parameters().phase0.slots_per_epoch(),
            committees: BTreeMap::new(),
            head: graph.head()?,
        })
    }
    fn epoch(&self) -> u64 {
        self.graph.current_slot() / self.slots
    }
    fn start(&self, epoch: u64) -> Result<u64, StoreError> {
        mul(epoch, self.slots)
    }
    fn block_epoch(&self, root: Root) -> Result<u64, StoreError> {
        Ok(self.graph.block_info(root)?.slot / self.slots)
    }
    fn checkpoint(&self, root: Root, epoch: u64) -> Result<Checkpoint, StoreError> {
        Ok(Checkpoint {
            epoch,
            root: self.graph.ancestor(node(self.graph, root), self.start(epoch)?)?.root,
        })
    }
    fn target(&self) -> Result<Checkpoint, StoreError> {
        self.checkpoint(self.head.root, self.epoch())
    }
    fn ancestors(&self, mut root: Root, terminal: Root) -> Result<Vec<Root>, StoreError> {
        let boundary = self.graph.block_info(terminal)?.slot;
        let mut result = Vec::new();
        for _ in 0..=self.graph.parameters().max_blocks {
            let block = self.graph.block_info(root)?;
            if block.slot <= boundary {
                return Ok(Vec::new())
            }
            result.push(root);
            root = block.parent_root;
            if root == terminal {
                result.reverse();
                return Ok(result)
            }
        }
        Err(StoreError::ResourceLimit)
    }
    fn participants(&mut self, start: u64, end: Option<u64>) -> Result<BTreeSet<u64>, StoreError> {
        let Some(end) = end else { return Ok(BTreeSet::new()) };
        if start > end {
            return Ok(BTreeSet::new())
        }
        if end - start >= self.parameters.max_committee_slots {
            return Err(StoreError::ResourceLimit)
        }
        let mut all = BTreeSet::new();
        for slot in start..=end {
            if let std::collections::btree_map::Entry::Vacant(entry) = self.committees.entry(slot) {
                let state = self.graph.state(self.head.root)?;
                entry.insert(slot_committee(
                    state,
                    slot,
                    self.graph.parameters(),
                    self.graph.processing(),
                )?);
            }
            all.extend(&self.committees[&slot]);
        }
        Ok(all)
    }
    fn estimate(&self, balance: u64, start: u64, end: Option<u64>) -> Result<u64, StoreError> {
        end.map_or(Ok(0), |end| {
            estimate_committee_weight_between_slots(balance, start, end, self.slots)
        })
    }
    fn equivocation(
        &mut self,
        balance: StateRef<'_>,
        start: u64,
        end: Option<u64>,
    ) -> Result<u64, StoreError> {
        let validators = balance.validators();
        let epoch = balance.slot() / self.slots;
        self.participants(start, end)?.into_iter().filter(|i| self.graph.equivocating(*i)).try_fold(
            0,
            |sum, i| {
                let validator = validators
                    .get(usize::try_from(i).map_err(|_| StoreError::Overflow)?)
                    .ok_or(StoreError::UnknownRoot)?;
                if active(validator, epoch) {
                    add(sum, validator.effective_balance)
                } else {
                    Ok(sum)
                }
            },
        )
    }
    fn adversarial(
        &mut self,
        balance: StateRef<'_>,
        start: u64,
        end: Option<u64>,
    ) -> Result<u64, StoreError> {
        let maximum = mul(
            self.estimate(total(balance, self.slots)?, start, end)? / 100,
            self.parameters.byzantine_threshold,
        )?;
        Ok(maximum.saturating_sub(self.equivocation(balance, start, end)?))
    }
    fn support(
        &mut self,
        balance: StateRef<'_>,
        target: ForkChoiceNode,
        start: u64,
        end: Option<u64>,
    ) -> Result<u64, StoreError> {
        let validators = balance.validators();
        let epoch = balance.slot() / self.slots;
        self.participants(start, end)?.into_iter().try_fold(0, |sum, i| {
            let validator = validators
                .get(usize::try_from(i).map_err(|_| StoreError::Overflow)?)
                .ok_or(StoreError::UnknownRoot)?;
            if active(validator, epoch) &&
                !validator.slashed &&
                !self.graph.equivocating(i) &&
                self.graph.latest(i)?.is_some_and(|message| message.supported_node == target)
            {
                add(sum, validator.effective_balance)
            } else {
                Ok(sum)
            }
        })
    }
    fn score(&self, balance: StateRef<'_>, target: ForkChoiceNode) -> Result<u64, StoreError> {
        let epoch = balance.slot() / self.slots;
        balance.validators().iter().enumerate().try_fold(0, |sum, (i, v)| {
            let i = u64::try_from(i).map_err(|_| StoreError::Overflow)?;
            if active(v, epoch) &&
                !v.slashed &&
                !self.graph.equivocating(i) &&
                let Some(message) = self.graph.latest(i)? &&
                self.graph.is_ancestor(message.supported_node, target)?
            {
                add(sum, v.effective_balance)
            } else {
                Ok(sum)
            }
        })
    }
    fn one_confirmed(&mut self, balance: StateRef<'_>, root: Root) -> Result<bool, StoreError> {
        let block = self.graph.block_info(root)?;
        let parent = self.graph.block_info(block.parent_root)?;
        let end = self.graph.current_slot().checked_sub(1);
        let total = total(balance, self.slots)?;
        let maximum = self.estimate(total, add(parent.slot, 1)?, end)?;
        let proposer = mul(total / self.slots, self.graph.parameters().proposer_score_boost)? / 100;
        let adversarial_start = if block.slot / self.slots > parent.slot / self.slots {
            self.start(block.slot / self.slots)?
        } else {
            block.slot
        };
        let adversarial = self.adversarial(balance, adversarial_start, end)?;
        let discount = if add(parent.slot, 1)? == block.slot {
            0
        } else {
            let target = self.graph.ancestor(node(self.graph, root), parent.slot)?;
            let support =
                self.support(balance, target, add(parent.slot, 1)?, block.slot.checked_sub(1))?;
            support.saturating_sub(self.adversarial(
                balance,
                add(parent.slot, 1)?,
                block.slot.checked_sub(1),
            )?)
        };
        let threshold =
            add(add(maximum, proposer)?, mul(2, adversarial)?)?.saturating_sub(discount) / 2;
        Ok(self.score(balance, node(self.graph, root))? > threshold)
    }
    fn pulled(&self) -> Result<BalanceData<'a>, StoreError> {
        let head = self.graph.state(self.head.root)?;
        if head.slot() / self.slots >= self.epoch() {
            return Ok(BalanceData::Borrowed(head))
        }
        let mut owned = head.owned();
        replay::<G>(
            &mut owned,
            self.start(self.epoch())?,
            self.graph.parameters(),
            self.graph.processing(),
        )?;
        Ok(BalanceData::Owned(Box::new(owned)))
    }
    fn honest_ffg(&mut self) -> Result<u64, StoreError> {
        let pulled = self.pulled()?;
        let balance = pulled.borrow();
        let target = self.target()?;
        let epoch = balance.slot() / self.slots;
        let mut support = 0;
        for (i, validator) in balance.validators().iter().enumerate() {
            let i = u64::try_from(i).map_err(|_| StoreError::Overflow)?;
            if active(validator, epoch) &&
                !validator.slashed &&
                !self.graph.equivocating(i) &&
                let Some(message) = self.graph.latest(i)? &&
                self.checkpoint(message.root, message.epoch)? == target
            {
                support = add(support, validator.effective_balance)?;
            }
        }
        let total = total(balance, self.slots)?;
        let start = self.start(self.epoch())?;
        let end = self.graph.current_slot().checked_sub(1);
        let remaining =
            total.checked_sub(self.estimate(total, start, end)?).ok_or(StoreError::Overflow)?;
        let remaining_honest = mul(remaining / 100, 100 - self.parameters.byzantine_threshold)?;
        add(support.saturating_sub(self.adversarial(balance, start, end)?), remaining_honest)
    }
    fn no_conflict(&mut self) -> Result<bool, StoreError> {
        if self.target()? == self.graph.unrealized_justified() {
            return Ok(true)
        }
        let state = self.pulled()?;
        Ok(u128::from(self.honest_ffg()?) * 3 > u128::from(total(state.borrow(), self.slots)?))
    }
    fn will_justify(&mut self) -> Result<bool, StoreError> {
        let state = self.pulled()?;
        Ok(u128::from(self.honest_ffg()?) * 3 >= u128::from(total(state.borrow(), self.slots)?) * 2)
    }
    fn chain_safe(&mut self, snapshot: Snapshot, root: Root) -> Result<bool, StoreError> {
        let checkpoint = snapshot.current_epoch_observed_justified_checkpoint;
        if checkpoint != self.checkpoint(root, checkpoint.epoch)? {
            return Ok(false)
        }
        let terminal = if add(checkpoint.epoch, 1)? >= self.epoch() {
            checkpoint.root
        } else {
            let ancestor = self
                .graph
                .ancestor(
                    node(self.graph, root),
                    self.start(self.epoch().checked_sub(1).ok_or(StoreError::Overflow)?)?,
                )?
                .root;
            if add(self.block_epoch(ancestor)?, 1)? == self.epoch() {
                self.graph.block_info(ancestor)?.parent_root
            } else {
                ancestor
            }
        };
        let source =
            self.graph.checkpoint_state(snapshot.previous_epoch_observed_justified_checkpoint)?;
        for block in self.ancestors(root, terminal)? {
            if !self.one_confirmed(source, block)? {
                return Ok(false)
            }
        }
        Ok(true)
    }
    fn descendants(&mut self, snapshot: Snapshot, mut confirmed: Root) -> Result<Root, StoreError> {
        let head = self.head.root;
        let epoch = self.epoch();
        let start = self.graph.current_slot().is_multiple_of(self.slots);
        let source =
            self.graph.checkpoint_state(snapshot.current_epoch_observed_justified_checkpoint)?;
        if add(self.block_epoch(confirmed)?, 1)? == epoch &&
            add(self.graph.voting_source(snapshot.previous_slot_head)?.epoch, 2)? >= epoch &&
            (start ||
                self.no_conflict()? &&
                    (add(
                        self.graph.unrealized_justification(snapshot.previous_slot_head)?.epoch,
                        1,
                    )? >= epoch ||
                        add(self.graph.unrealized_justification(head)?.epoch, 1)? >=
                            epoch))
        {
            for root in self.ancestors(head, confirmed)? {
                if self.block_epoch(root)? == epoch ||
                    !self.graph.is_ancestor(
                        node(self.graph, snapshot.previous_slot_head),
                        node(self.graph, root),
                    )? ||
                    !self.one_confirmed(source, root)?
                {
                    break
                }
                confirmed = root;
            }
        }
        if start || add(self.graph.unrealized_justification(head)?.epoch, 1)? >= epoch {
            let mut tentative = confirmed;
            for root in self.ancestors(head, confirmed)? {
                if self.block_epoch(root)? > self.block_epoch(tentative)? && !self.will_justify()? ||
                    !self.one_confirmed(source, root)?
                {
                    break
                }
                tentative = root;
            }
            if self.block_epoch(tentative)? == epoch ||
                add(self.graph.voting_source(tentative)?.epoch, 2)? >= epoch &&
                    (start || self.no_conflict()?)
            {
                confirmed = tentative;
            }
        }
        Ok(confirmed)
    }
    fn latest(&mut self, snapshot: Snapshot) -> Result<Root, StoreError> {
        let mut confirmed = snapshot.confirmed_root;
        let epoch = self.epoch();
        let start = self.graph.current_slot().is_multiple_of(self.slots);
        if add(self.block_epoch(confirmed)?, 1)? < epoch ||
            !self
                .graph
                .is_ancestor(node(self.graph, self.head.root), node(self.graph, confirmed))? ||
            start && !self.chain_safe(snapshot, confirmed)?
        {
            confirmed = self.graph.checkpoints().1.root;
        }
        let observed = snapshot.current_epoch_observed_justified_checkpoint;
        if start &&
            add(self.block_epoch(observed.root)?, 1)? == epoch &&
            observed == self.graph.unrealized_justification(self.head.root)? &&
            self.graph.block_info(confirmed)?.slot < self.graph.block_info(observed.root)?.slot
        {
            confirmed = observed.root;
        }
        if add(self.block_epoch(confirmed)?, 1)? >= epoch {
            self.descendants(snapshot, confirmed)
        } else {
            Ok(confirmed)
        }
    }
}

enum BalanceData<'a> {
    Borrowed(StateRef<'a>),
    Owned(Box<BeaconState>),
}
impl BalanceData<'_> {
    fn borrow(&self) -> StateRef<'_> {
        match self {
            Self::Borrowed(state) => *state,
            Self::Owned(state) => StateRef::Classic(state),
        }
    }
}

fn slot_committee(
    state: StateRef<'_>,
    slot: u64,
    store: StoreParameters,
    processing: ProcessingRef<'_>,
) -> Result<BTreeSet<u64>, StoreError> {
    let slots = store.phase0.slots_per_epoch();
    let epoch = slot / slots;
    let count = state.validators().iter().filter(|v| active(v, epoch)).count() as u64;
    let (maximum, target) = match store.phase0.preset {
        Preset::Mainnet => (64, 128),
        Preset::Minimal => (4, 4),
    };
    let count = (count / slots / target).clamp(1, maximum);
    let mut result = BTreeSet::new();
    let inherited = match processing {
        ProcessingRef::Scheduled(parameters) => Some(&parameters.gloas),
        ProcessingRef::Gloas(parameters) => Some(parameters),
        ProcessingRef::Legacy => None,
    };
    for i in 0..count {
        let committee = match state {
            StateRef::Classic(BeaconState::Phase0(s, _)) => {
                phase0_processing::get_beacon_committee(s, slot, i, &store.phase0)?
            }
            StateRef::Classic(BeaconState::Altair(s, _)) => {
                altair_processing::get_beacon_committee(
                    s,
                    slot,
                    i,
                    &altair_processing::AltairParameters {
                        phase0: store.phase0,
                        fork_version: store.altair_fork_version,
                        inactivity_score_bias: store.inactivity_score_bias,
                        inactivity_score_recovery_rate: store.inactivity_score_recovery_rate,
                    },
                )?
            }
            StateRef::Classic(BeaconState::Bellatrix(s, _)) => {
                bellatrix_processing::get_beacon_committee(
                    s,
                    slot,
                    i,
                    &inherited
                        .ok_or(StoreError::InvalidParameters)?
                        .fulu
                        .electra
                        .deneb
                        .capella
                        .bellatrix,
                )?
            }
            StateRef::Classic(BeaconState::Capella(s, _)) => {
                capella_processing::get_beacon_committee(
                    s,
                    slot,
                    i,
                    &inherited.ok_or(StoreError::InvalidParameters)?.fulu.electra.deneb.capella,
                )?
            }
            StateRef::Classic(BeaconState::Deneb(s, _)) => deneb_processing::get_beacon_committee(
                s,
                slot,
                i,
                &inherited.ok_or(StoreError::InvalidParameters)?.fulu.electra.deneb,
            )?,
            StateRef::Classic(BeaconState::Electra(s, _)) => {
                electra_processing::get_beacon_committee(
                    s,
                    slot,
                    i,
                    &inherited.ok_or(StoreError::InvalidParameters)?.fulu.electra,
                )?
            }
            StateRef::Classic(BeaconState::Fulu(s, _)) => fulu_processing::get_beacon_committee(
                s,
                slot,
                i,
                &inherited.ok_or(StoreError::InvalidParameters)?.fulu,
            )?,
            StateRef::Classic(BeaconState::Gloas(s, _)) | StateRef::Gloas(s, _) => {
                gloas_processing::get_beacon_committee(
                    s,
                    slot,
                    i,
                    inherited.ok_or(StoreError::InvalidParameters)?,
                )?
            }
        };
        result.extend(committee);
    }
    Ok(result)
}
fn replay<G: FastConfirmationView>(
    state: &mut BeaconState,
    target: u64,
    store: StoreParameters,
    processing: ProcessingRef<'_>,
) -> Result<(), StoreError> {
    match processing {
        ProcessingRef::Scheduled(parameters) => {
            if G::VERIFY_SIGNATURES {
                reth_embedded_beacon_state_transition::fork_processing::process_slots(
                    state, target, parameters,
                )?;
            } else {
                #[cfg(feature = "spec-tests")]
                reth_embedded_beacon_state_transition::fork_processing::spec_only::process_slots(
                    state, target, parameters,
                )?;
                #[cfg(not(feature = "spec-tests"))]
                return Err(StoreError::InvalidParameters);
            }
        }
        ProcessingRef::Gloas(parameters) => {
            let BeaconState::Gloas(state, _) = state else {
                return Err(StoreError::InvalidParameters)
            };
            if G::VERIFY_SIGNATURES {
                gloas_processing::process_slots(state, target, parameters)?;
            } else {
                #[cfg(feature = "spec-tests")]
                gloas_processing::spec_only::process_slots(state, target, parameters)?;
                #[cfg(not(feature = "spec-tests"))]
                return Err(StoreError::InvalidParameters);
            }
        }
        ProcessingRef::Legacy => match state {
            BeaconState::Phase0(state, _) => {
                phase0_processing::process_slots(state, target, &store.phase0)?
            }
            BeaconState::Altair(state, _) => altair_processing::process_slots(
                state,
                target,
                &altair_processing::AltairParameters {
                    phase0: store.phase0,
                    fork_version: store.altair_fork_version,
                    inactivity_score_bias: store.inactivity_score_bias,
                    inactivity_score_recovery_rate: store.inactivity_score_recovery_rate,
                },
            )?,
            _ => return Err(StoreError::InvalidParameters),
        },
    }
    Ok(())
}

#[cfg(test)]
mod tests;
