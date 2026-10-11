//! Private event store with mandatory typed transitions and cryptographic validation.
//!
//! Complete signed imports through Fulu use checked fork schedules and mandatory execution
//! and availability callbacks. Gloas uses its dedicated payload graph. No production event
//! accepts a claimed validation flag or an externally fabricated post-state.

use crate::{
    compact::{RetainedState, StateRead, StateSourceError},
    BlockRecord, ExecutionStatus, ForkChoice, ForkChoiceError, LatestMessage, ValidatorWeight,
};
use reth_embedded_beacon_primitives::{Checkpoint, Fork, Root, SszError};
use reth_embedded_beacon_state_transition::{
    altair_processing::{self, AltairParameters},
    bellatrix_processing, capella_processing, deneb_processing, electra_processing,
    fork_processing::{self, TransitionParameters},
    fulu_processing,
    phase0_processing::{self, Phase0Parameters, ProcessingError},
    ssz::{
        altair, bellatrix, capella, deneb, electra, fulu, phase0, BeaconBlock, BeaconState, Preset,
        SignedBeaconBlock,
    },
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Instant,
};
use thiserror::Error;

pub(crate) mod body_attestations;
pub(crate) mod compact;
mod compact_retention;
mod diagnostics;
mod fast_confirmation;
mod timing;

pub use diagnostics::BlockVotingSource;
pub use timing::BlockImportTimings;

pub mod block_gossip;

pub mod optimistic;

#[cfg(feature = "spec-tests")]
pub mod spec_only;

#[cfg(feature = "spec-tests")]
pub mod strict_gossip_fixture;

macro_rules! validate_indexed {
    ($verify:expr, $module:ident, $state:expr, $indexed:expr, $parameters:expr $(,)?) => {{
        if $verify {
            $module::is_valid_indexed_attestation($state, $indexed, $parameters)
        } else {
            #[cfg(feature = "spec-tests")]
            {
                $module::spec_only::is_valid_indexed_attestation($state, $indexed, $parameters)
            }
            #[cfg(not(feature = "spec-tests"))]
            {
                Err(ProcessingError::InvalidState("fixture signature policy is unavailable"))
            }
        }
    }};
}

/// A beacon graph node, including the Gloas payload decision.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ForkChoiceNode {
    /// Unsigned consensus block root.
    pub root: Root,
    /// Gloas payload branch; earlier forks have no separate payload decision.
    pub payload_status: Option<PayloadStatus>,
}

/// Immutable identity and checkpoint values derived from validated retained store data.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StoreSnapshot {
    /// Current canonical fork-choice node.
    pub head: ForkChoiceNode,
    /// Actual slot of the retained canonical block.
    pub head_slot: u64,
    /// Current realized justified checkpoint.
    pub justified_checkpoint: Checkpoint,
    /// Current realized finalized checkpoint.
    pub finalized_checkpoint: Checkpoint,
}

/// Gloas graph status, in the exact specification order.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum PayloadStatus {
    /// The chain does not include this block's payload.
    Empty,
    /// The chain includes a locally verified payload.
    Full,
    /// The beacon block is known and its payload branch is undecided.
    Pending,
}

/// Explicit preset, clock and resource parameters for the event store.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StoreParameters {
    /// Production Phase0 parameters, including exact mainnet/minimal preset.
    pub phase0: Phase0Parameters,
    /// Configured Altair fork domain version.
    pub altair_fork_version: [u8; 4],
    /// Configured Altair inactivity score bias.
    pub inactivity_score_bias: u64,
    /// Configured Altair inactivity score recovery rate.
    pub inactivity_score_recovery_rate: u64,
    /// Actual network slot duration in milliseconds.
    pub slot_duration_ms: u64,
    /// Attestation deadline as a fraction of one slot, in basis points.
    pub attestation_due_bps: u64,
    /// Gloas attestation deadline in basis points.
    pub gloas_attestation_due_bps: u64,
    /// Gloas payload-attestation deadline in basis points.
    pub payload_attestation_due_bps: u64,
    /// Proposer boost as a percentage of a committee's active balance.
    pub proposer_score_boost: u64,
    /// Optional proposer reorganization cutoff in basis points of the current slot.
    pub proposer_reorg_cutoff_bps: u64,
    /// Weak-head balance threshold as a percentage of one committee.
    pub reorg_head_weight_threshold: u64,
    /// Strong-parent balance threshold as a percentage of one committee.
    pub reorg_parent_weight_threshold: u64,
    /// Maximum epochs since finalization for an ordinary proposer reorganization.
    pub reorg_max_epochs_since_finalization: u64,
    /// Maximum blocks retained before explicit pruning is required.
    pub max_blocks: usize,
    /// Maximum owned state/block allocation bytes retained by this store, counted by capacity.
    /// Compact graph/index metadata, allocator overhead, caller-held Arcs and temporary
    /// transition buffers are separately bounded or outside this retention measurement.
    pub max_retained_schema_bytes: usize,
    /// Maximum clock catch-up gap accepted in one event.
    pub max_tick_slots: u64,
}

impl StoreParameters {
    /// Exact published preset defaults; private networks must set their own clock.
    pub const fn for_preset(preset: Preset) -> Self {
        Self {
            phase0: Phase0Parameters::for_preset(preset),
            altair_fork_version: match preset {
                Preset::Mainnet => [1, 0, 0, 0],
                Preset::Minimal => [1, 0, 0, 1],
            },
            inactivity_score_bias: 4,
            inactivity_score_recovery_rate: 16,
            slot_duration_ms: match preset {
                Preset::Mainnet => 12_000,
                Preset::Minimal => 6_000,
            },
            attestation_due_bps: 3333,
            gloas_attestation_due_bps: 2500,
            payload_attestation_due_bps: 7500,
            proposer_score_boost: 40,
            proposer_reorg_cutoff_bps: 1667,
            reorg_head_weight_threshold: 20,
            reorg_parent_weight_threshold: 160,
            reorg_max_epochs_since_finalization: 2,
            max_blocks: 4096,
            max_retained_schema_bytes: 2 * 1024 * 1024 * 1024,
            max_tick_slots: 8192,
        }
    }

    const fn altair_parameters(&self) -> AltairParameters {
        AltairParameters {
            phase0: self.phase0,
            fork_version: self.altair_fork_version,
            inactivity_score_bias: self.inactivity_score_bias,
            inactivity_score_recovery_rate: self.inactivity_score_recovery_rate,
        }
    }

    pub(super) fn validate(&self) -> Result<(), StoreError> {
        self.altair_parameters().validate()?;
        if self.slot_duration_ms == 0 ||
            self.max_blocks == 0 ||
            self.max_retained_schema_bytes == 0 ||
            self.max_tick_slots == 0 ||
            self.attestation_due_bps == 0 ||
            self.attestation_due_bps > 10_000 ||
            self.gloas_attestation_due_bps == 0 ||
            self.gloas_attestation_due_bps > 10_000 ||
            self.payload_attestation_due_bps == 0 ||
            self.payload_attestation_due_bps > 10_000 ||
            self.proposer_score_boost > 100 ||
            self.proposer_reorg_cutoff_bps > 10_000
        {
            return Err(StoreError::InvalidParameters)
        }
        Ok(())
    }
}

/// Production fork-choice inputs and state stay private behind strict event handlers.
#[derive(Clone, Debug)]
pub struct Store {
    kernel: ForkChoice,
    blocks: BTreeMap<Root, Arc<BeaconBlock>>,
    block_states: BTreeMap<Root, RetainedState<BeaconState>>,
    checkpoint_states: BTreeMap<(u64, Root), RetainedState<BeaconState>>,
    block_timeliness: BTreeMap<Root, [bool; 2]>,
    unrealized_justified: Checkpoint,
    unrealized_finalized: Checkpoint,
    time_ms: u64,
    genesis_time_ms: u64,
    parameters: StoreParameters,
    transition_parameters: Option<TransitionParameters>,
    // Preserve the actual signed slot selected by the existing epoch-based vote update.
    latest_message_slots: BTreeMap<u64, u64>,
}

impl Store {
    /// Start from a caller-trusted complete anchor whose actual state and block roots match.
    ///
    /// Trust establishment belongs to genesis/checkpoint bootstrap. This method verifies
    /// the supplied complete schemas, preset, slot and root linkage; it does not infer trust.
    pub fn get_store(
        anchor_state: BeaconState,
        anchor_block: BeaconBlock,
        parameters: StoreParameters,
    ) -> Result<Self, StoreError> {
        parameters.validate()?;
        if anchor_state.schema_fork() != anchor_block.schema_fork() ||
            state_preset(&anchor_state) != parameters.phase0.preset ||
            block_preset(&anchor_block) != parameters.phase0.preset
        {
            return Err(StoreError::AnchorMismatch)
        }
        let header = anchor_block.header()?;
        let state_root = anchor_state.hash_tree_root()?;
        if header.state_root != state_root || header.slot != anchor_state.slot() {
            return Err(StoreError::AnchorMismatch)
        }
        let root = anchor_block.hash_tree_root()?;
        let epoch = anchor_state.slot() / parameters.phase0.slots_per_epoch();
        let checkpoint = Checkpoint { epoch, root };
        let genesis_time_ms =
            anchor_state.genesis_time().checked_mul(1000).ok_or(StoreError::Overflow)?;
        let time_ms = genesis_time_ms
            .checked_add(
                anchor_state
                    .slot()
                    .checked_mul(parameters.slot_duration_ms)
                    .ok_or(StoreError::Overflow)?,
            )
            .ok_or(StoreError::Overflow)?;
        let record = BlockRecord {
            root,
            parent_root: header.parent_root,
            slot: header.slot,
            justified: checkpoint,
            unrealized_justified: checkpoint,
            execution_status: ExecutionStatus::Valid,
        };
        let mut kernel = ForkChoice::new(
            record,
            checkpoint,
            state_weights(&anchor_state, epoch)?,
            parameters.phase0.slots_per_epoch(),
        )?;
        // A checkpoint anchor can be in the middle of an epoch; the wall clock is at its actual
        // slot.
        kernel.on_tick(anchor_state.slot())?;
        let state = Arc::new(anchor_state);
        let block = Arc::new(anchor_block);
        let store = Self {
            kernel,
            blocks: BTreeMap::from([(root, block)]),
            block_states: BTreeMap::from([(root, state.clone().into())]),
            checkpoint_states: BTreeMap::from([((epoch, root), state.into())]),
            block_timeliness: BTreeMap::from([(root, [true, true])]),
            unrealized_justified: checkpoint,
            unrealized_finalized: checkpoint,
            time_ms,
            genesis_time_ms,
            parameters,
            transition_parameters: None,
            latest_message_slots: BTreeMap::new(),
        };
        store.check_retention_budget()?;
        Ok(store)
    }

    /// Advance a monotonic millisecond clock, realizing pulled-up checkpoints at every epoch
    /// boundary.
    pub fn on_tick(&mut self, time_ms: u64) -> Result<(), StoreError> {
        self.on_tick_inner::<true>(time_ms)
    }

    fn on_tick_inner<const VERIFY_SIGNATURES: bool>(
        &mut self,
        time_ms: u64,
    ) -> Result<(), StoreError> {
        if time_ms < self.time_ms || time_ms < self.genesis_time_ms {
            return Err(StoreError::TimeRegression)
        }
        let target = (time_ms - self.genesis_time_ms) / self.parameters.slot_duration_ms;
        let current = self.current_slot();
        if target.saturating_sub(current) > self.parameters.max_tick_slots {
            return Err(StoreError::ResourceLimit)
        }
        let mut candidate = self.clone();
        for slot in current.saturating_add(1)..=target {
            candidate.kernel.on_tick(slot)?;
            if slot.is_multiple_of(candidate.parameters.phase0.slots_per_epoch()) {
                candidate.realize_checkpoints_inner::<VERIFY_SIGNATURES>(
                    candidate.unrealized_justified,
                    candidate.unrealized_finalized,
                )?;
            }
        }
        candidate.time_ms = time_ms;
        candidate.prune_retained_states()?;
        candidate.check_retention_budget()?;
        *self = candidate;
        Ok(())
    }

    /// Import only after the real production signed transition validates BLS and the complete
    /// post-state root.
    pub fn on_block(&mut self, signed: &SignedBeaconBlock) -> Result<(), StoreError> {
        let root = signed.message_root()?;
        if self.blocks.contains_key(&root) {
            return Ok(())
        }
        let (block, preset) = match signed {
            SignedBeaconBlock::Phase0(value, preset) => {
                (BeaconBlock::Phase0(value.message.clone(), *preset), *preset)
            }
            SignedBeaconBlock::Altair(value, preset) => {
                (BeaconBlock::Altair(value.message.clone(), *preset), *preset)
            }
            _ => return Err(StoreError::ValidationUnavailable(signed.schema_fork())),
        };
        if preset != self.parameters.phase0.preset {
            return Err(StoreError::AnchorMismatch)
        }
        let header = block.header()?;
        let parent = self.read_block_state(header.parent_root)?.ok_or(StoreError::UnknownRoot)?;
        if state_preset(&parent) != preset {
            return Err(StoreError::AnchorMismatch)
        }
        if parent.schema_fork() != signed.schema_fork() {
            // Fork upgrades require an explicit validated network schedule.
            return Err(StoreError::ValidationUnavailable(signed.schema_fork()))
        }
        let (_, finalized) = self.kernel.checkpoints();
        if header.slot > self.current_slot() ||
            header.slot <=
                finalized
                    .epoch
                    .checked_mul(self.parameters.phase0.slots_per_epoch())
                    .ok_or(StoreError::Overflow)?
        {
            return Err(StoreError::InvalidBlockTime)
        }
        if self.kernel.ancestor(
            header.parent_root,
            finalized
                .epoch
                .checked_mul(self.parameters.phase0.slots_per_epoch())
                .ok_or(StoreError::Overflow)?,
        )? != finalized.root
        {
            return Err(StoreError::ConflictsWithFinality)
        }
        if self.blocks.len() >= self.parameters.max_blocks {
            return Err(StoreError::ResourceLimit)
        }
        if header.slot.saturating_sub(parent.slot()) > self.parameters.max_tick_slots {
            return Err(StoreError::ResourceLimit);
        }
        let mut state = parent.as_ref().clone();
        match (&mut state, signed) {
            (BeaconState::Phase0(state, _), SignedBeaconBlock::Phase0(block, _)) => {
                phase0_processing::process_signed_block(state, block, &self.parameters.phase0)?;
            }
            (BeaconState::Altair(state, _), SignedBeaconBlock::Altair(block, _)) => {
                altair_processing::process_signed_block(
                    state,
                    block,
                    &self.parameters.altair_parameters(),
                )?;
            }
            _ => return Err(StoreError::ValidationUnavailable(signed.schema_fork())),
        }
        self.finish_block(root, block, state)
    }

    fn finish_block(
        &mut self,
        root: Root,
        block: BeaconBlock,
        state: BeaconState,
    ) -> Result<(), StoreError> {
        self.finish_block_inner::<true>(root, block, state)
    }

    fn finish_block_inner<const VERIFY_SIGNATURES: bool>(
        &mut self,
        root: Root,
        block: BeaconBlock,
        state: BeaconState,
    ) -> Result<(), StoreError> {
        let header = block.header()?;
        let head = self.kernel.head()?;
        let (realized_justified, realized_finalized) = state_checkpoints(&state)?;
        let (unrealized_justified, unrealized_finalized) =
            if let BeaconState::Fulu(inner, _) = &state {
                let parameters = self
                    .transition_parameters
                    .as_ref()
                    .ok_or(StoreError::ValidationUnavailable(Fork::Fulu))?;
                fulu_processing::get_unrealized_checkpoints(inner, &parameters.gloas.fulu)?
            } else {
                let mut pulled = state.clone();
                match &mut pulled {
                    BeaconState::Phase0(state, _) => {
                        phase0_processing::process_justification_and_finalization(
                            state,
                            &self.parameters.phase0,
                        )?;
                    }
                    BeaconState::Altair(state, _) => {
                        altair_processing::process_justification_and_finalization(
                            state,
                            &self.parameters.altair_parameters(),
                        )?;
                    }
                    BeaconState::Bellatrix(state, _) => {
                        let parameters = self
                            .transition_parameters
                            .as_ref()
                            .ok_or(StoreError::ValidationUnavailable(Fork::Bellatrix))?;
                        bellatrix_processing::process_justification_and_finalization(
                            state,
                            &parameters.gloas.fulu.electra.deneb.capella.bellatrix,
                        )?;
                    }
                    BeaconState::Capella(state, _) => {
                        let parameters = self
                            .transition_parameters
                            .as_ref()
                            .ok_or(StoreError::ValidationUnavailable(Fork::Capella))?;
                        capella_processing::process_justification_and_finalization(
                            state,
                            &parameters.gloas.fulu.electra.deneb.capella,
                        )?;
                    }
                    BeaconState::Deneb(state, _) => {
                        let parameters = self
                            .transition_parameters
                            .as_ref()
                            .ok_or(StoreError::ValidationUnavailable(Fork::Deneb))?;
                        deneb_processing::process_justification_and_finalization(
                            state,
                            &parameters.gloas.fulu.electra.deneb,
                        )?;
                    }
                    BeaconState::Electra(state, _) => {
                        let parameters = self
                            .transition_parameters
                            .as_ref()
                            .ok_or(StoreError::ValidationUnavailable(Fork::Electra))?;
                        electra_processing::process_justification_and_finalization(
                            state,
                            &parameters.gloas.fulu.electra,
                        )?;
                    }
                    _ => return Err(StoreError::ValidationUnavailable(pulled.schema_fork())),
                }
                state_checkpoints(&pulled)?
            };
        let record = BlockRecord {
            root,
            parent_root: header.parent_root,
            slot: header.slot,
            justified: realized_justified,
            unrealized_justified,
            execution_status: ExecutionStatus::Valid,
        };
        let mut candidate = self.clone();
        candidate.kernel.on_verified_block(record)?;
        candidate.blocks.insert(root, Arc::new(block));
        candidate.block_states.insert(root, Arc::new(state).into());
        let timely = header.slot == self.current_slot() &&
            self.time_into_slot_ms() < self.deadline_ms(self.parameters.attestation_due_bps)?;
        candidate.block_timeliness.insert(root, [timely, false]);
        let epoch = self.current_slot() / self.parameters.phase0.slots_per_epoch();
        let dependent_slot = epoch
            .saturating_sub(1)
            .checked_mul(self.parameters.phase0.slots_per_epoch())
            .ok_or(StoreError::Overflow)?
            .saturating_sub(1);
        if timely &&
            let Some(head_dependency) =
                candidate.retained_proposer_dependency(head, dependent_slot)? &&
            let Some(block_dependency) =
                candidate.retained_proposer_dependency(root, dependent_slot)? &&
            head_dependency == block_dependency
        {
            let (justified, _) = candidate.kernel.checkpoints();
            let checkpoint_state =
                candidate.checkpoint_state_inner::<VERIFY_SIGNATURES>(justified)?;
            let total = active_balance(&checkpoint_state, justified.epoch)?;
            let score = (total / candidate.parameters.phase0.slots_per_epoch())
                .checked_mul(candidate.parameters.proposer_score_boost)
                .ok_or(StoreError::Overflow)? /
                100;
            candidate.kernel.set_proposer_boost(root, score)?;
        }
        candidate.realize_checkpoints_inner::<VERIFY_SIGNATURES>(
            realized_justified,
            realized_finalized,
        )?;
        if unrealized_justified.epoch > candidate.unrealized_justified.epoch {
            candidate.unrealized_justified = unrealized_justified;
        }
        if unrealized_finalized.epoch > candidate.unrealized_finalized.epoch {
            candidate.unrealized_finalized = unrealized_finalized;
        }
        if header.slot / candidate.parameters.phase0.slots_per_epoch() < epoch {
            candidate.realize_checkpoints_inner::<VERIFY_SIGNATURES>(
                unrealized_justified,
                unrealized_finalized,
            )?;
        }
        candidate.prune_retained_states()?;
        candidate.check_retention_budget()?;
        *self = candidate;
        Ok(())
    }

    /// Import a signed block and its contained votes/slashings as one atomic network event.
    ///
    /// Only the actual contents of a block retained by the mandatory signed transition can
    /// enter the block-attestation timing path; callers cannot claim this provenance.
    pub fn on_block_and_contents(&mut self, signed: &SignedBeaconBlock) -> Result<(), StoreError> {
        let mut candidate = self.clone();
        candidate.on_block(signed)?;
        candidate.on_block_contents::<true>(signed.message_root()?)?;
        *self = candidate;
        Ok(())
    }

    /// Attach authenticated network parameters before importing later forks or crossing upgrades.
    /// The anchor's complete fork identity, preset, clock and existing kernel settings must agree.
    pub fn set_transition_parameters(
        &mut self,
        parameters: TransitionParameters,
    ) -> Result<(), StoreError> {
        parameters.validate()?;
        if self.transition_parameters.is_some() ||
            parameters.preset() != self.parameters.phase0.preset ||
            parameters.gloas.fulu.electra.deneb.capella.bellatrix.altair.phase0 !=
                self.parameters.phase0 ||
            parameters.gloas.fulu.electra.deneb.capella.bellatrix.altair !=
                self.parameters.altair_parameters() ||
            parameters
                .gloas
                .fulu
                .electra
                .deneb
                .capella
                .bellatrix
                .seconds_per_slot
                .checked_mul(1000) !=
                Some(self.parameters.slot_duration_ms)
        {
            return Err(StoreError::InvalidParameters)
        }
        let (justified, _) = self.kernel.checkpoints();
        let state = self.read_block_state(justified.root)?.ok_or(StoreError::UnknownRoot)?;
        let mut checked = state.as_ref().clone();
        fork_processing::upgrade_state_at_slot(&mut checked, &parameters)?;
        if checked.schema_fork() != state.schema_fork() || checked.fork_info() != state.fork_info()
        {
            return Err(StoreError::AnchorMismatch)
        }
        self.transition_parameters = Some(parameters);
        Ok(())
    }

    /// Read the authenticated fork schedule and processing parameters attached to this store.
    pub const fn transition_parameters(&self) -> Option<&TransitionParameters> {
        self.transition_parameters.as_ref()
    }

    /// Atomically import complete signed blocks through the checked fork schedule and real engines.
    /// Every versioned Engine/terminal callback is mandatory. Gloas payload-branch admission
    /// remains unavailable until its separate envelope/timeliness/data-availability graph exists.
    pub async fn on_block_and_contents_with_engine<
        E: fork_processing::ExecutionEngine,
        D: crate::data_availability::DataAvailability,
    >(
        &mut self,
        signed: &SignedBeaconBlock,
        engine: &E,
        availability: &D,
    ) -> Result<(), StoreError> {
        self.on_block_and_contents_inner::<true, false, E, D>(signed, engine, availability).await
    }

    /// Strictly import the complete signed block through real Engine and availability verification.
    /// Contained fork-choice observations are admitted separately, so an ineligible FC vote cannot
    /// undo a valid normative signed state transition. This grants no payload-envelope authority.
    pub async fn on_block_with_engine<
        E: fork_processing::ExecutionEngine,
        D: crate::data_availability::DataAvailability,
    >(
        &mut self,
        signed: &SignedBeaconBlock,
        engine: &E,
        availability: &D,
    ) -> Result<(), StoreError> {
        self.on_block_with_engine_timed(signed, engine, availability).await.map(|_| ())
    }

    /// Measure the strict kernels without changing their validation or publication order.
    pub(crate) async fn on_block_with_engine_timed<
        E: fork_processing::ExecutionEngine,
        D: crate::data_availability::DataAvailability,
    >(
        &mut self,
        signed: &SignedBeaconBlock,
        engine: &E,
        availability: &D,
    ) -> Result<BlockImportTimings, StoreError> {
        let mut timings = BlockImportTimings::default();
        self.on_block_with_engine_inner::<true, E, D>(signed, engine, availability, &mut timings)
            .await?;
        Ok(timings)
    }

    async fn on_block_and_contents_inner<
        const VERIFY_SIGNATURES: bool,
        const SUPPRESS_INVALID_BODY_VOTES: bool,
        E: fork_processing::ExecutionEngine,
        D: crate::data_availability::DataAvailability,
    >(
        &mut self,
        signed: &SignedBeaconBlock,
        engine: &E,
        availability: &D,
    ) -> Result<(), StoreError> {
        if signed.schema_fork() == Fork::Gloas {
            return Err(StoreError::ValidationUnavailable(Fork::Gloas))
        }
        let root = signed.message_root()?;
        if self.blocks.contains_key(&root) {
            return Ok(())
        }
        let mut candidate = self.clone();
        let mut timings = BlockImportTimings::default();
        candidate
            .on_block_with_engine_inner::<VERIFY_SIGNATURES, E, D>(
                signed,
                engine,
                availability,
                &mut timings,
            )
            .await?;
        candidate
            .on_block_contents_inner::<VERIFY_SIGNATURES, SUPPRESS_INVALID_BODY_VOTES>(root)?;
        *self = candidate;
        Ok(())
    }
    async fn on_block_with_engine_inner<
        const VERIFY_SIGNATURES: bool,
        E: fork_processing::ExecutionEngine,
        D: crate::data_availability::DataAvailability,
    >(
        &mut self,
        signed: &SignedBeaconBlock,
        engine: &E,
        availability: &D,
        timings: &mut BlockImportTimings,
    ) -> Result<(), StoreError> {
        if signed.schema_fork() == Fork::Gloas {
            return Err(StoreError::ValidationUnavailable(Fork::Gloas))
        }
        let root = signed.message_root()?;
        if self.blocks.contains_key(&root) {
            return Ok(())
        }
        let block = match signed {
            SignedBeaconBlock::Phase0(value, preset) => {
                BeaconBlock::Phase0(value.message.clone(), *preset)
            }
            SignedBeaconBlock::Altair(value, preset) => {
                BeaconBlock::Altair(value.message.clone(), *preset)
            }
            SignedBeaconBlock::Bellatrix(value, preset) => {
                BeaconBlock::Bellatrix(value.message.clone(), *preset)
            }
            SignedBeaconBlock::Capella(value, preset) => {
                BeaconBlock::Capella(value.message.clone(), *preset)
            }
            SignedBeaconBlock::Deneb(value, preset) => {
                BeaconBlock::Deneb(value.message.clone(), *preset)
            }
            SignedBeaconBlock::Electra(value, preset) => {
                BeaconBlock::Electra(value.message.clone(), *preset)
            }
            SignedBeaconBlock::Fulu(value, preset) => {
                BeaconBlock::Fulu(value.message.clone(), *preset)
            }
            SignedBeaconBlock::Gloas(value, preset) => {
                BeaconBlock::Gloas(value.message.clone(), *preset)
            }
        };
        if block_preset(&block) != self.parameters.phase0.preset {
            return Err(StoreError::AnchorMismatch)
        }
        let header = block.header()?;
        let parent = self.read_block_state(header.parent_root)?.ok_or(StoreError::UnknownRoot)?;
        let (_, finalized) = self.kernel.checkpoints();
        let finalized_slot = finalized
            .epoch
            .checked_mul(self.parameters.phase0.slots_per_epoch())
            .ok_or(StoreError::Overflow)?;
        if header.slot > self.current_slot() || header.slot <= finalized_slot {
            return Err(StoreError::InvalidBlockTime)
        }
        if self.kernel.ancestor(header.parent_root, finalized_slot)? != finalized.root {
            return Err(StoreError::ConflictsWithFinality)
        }
        if self.blocks.len() >= self.parameters.max_blocks ||
            header.slot.saturating_sub(parent.slot()) > self.parameters.max_tick_slots
        {
            return Err(StoreError::ResourceLimit)
        }
        let parameters = self
            .transition_parameters
            .as_ref()
            .ok_or_else(|| StoreError::ValidationUnavailable(signed.schema_fork()))?;
        if matches!(block.schema_fork(), Fork::Deneb | Fork::Electra | Fork::Fulu) {
            let started = Instant::now();
            availability
                .verify(root, &block)
                .await
                .map_err(|error| StoreError::DataAvailability(error.to_string()))?;
            timings.availability_micros = timing::elapsed_micros(started);
        }
        let started = Instant::now();
        let state = if VERIFY_SIGNATURES {
            fork_processing::state_transition_owned(
                parent.as_ref().clone(),
                signed,
                parameters,
                engine,
            )
            .await
            .map_err(|error| StoreError::ExecutionImport(error.to_string()))?
        } else {
            #[cfg(feature = "spec-tests")]
            {
                let mut state = parent.as_ref().clone();
                fork_processing::spec_only::state_transition(
                    &mut state, signed, parameters, engine,
                )
                .await
                .map_err(|error| StoreError::ExecutionImport(error.to_string()))?;
                state
            }
            #[cfg(not(feature = "spec-tests"))]
            return Err(StoreError::ValidationUnavailable(signed.schema_fork()));
        };
        timings.state_transition_micros = timing::elapsed_micros(started);
        let started = Instant::now();
        let mut candidate = self.clone();
        candidate.finish_block_inner::<VERIFY_SIGNATURES>(root, block, state)?;

        *self = candidate;
        timings.fork_choice_micros = timing::elapsed_micros(started);
        Ok(())
    }

    fn on_block_contents<const VERIFY_SIGNATURES: bool>(
        &mut self,
        root: Root,
    ) -> Result<(), StoreError> {
        self.on_block_contents_inner::<VERIFY_SIGNATURES, false>(root)
    }

    fn on_block_contents_inner<
        const VERIFY_SIGNATURES: bool,
        const SUPPRESS_INVALID_BODY_VOTES: bool,
    >(
        &mut self,
        root: Root,
    ) -> Result<(), StoreError> {
        if SUPPRESS_INVALID_BODY_VOTES && !cfg!(feature = "spec-tests") {
            return Err(StoreError::ValidationUnavailable(Fork::Phase0));
        }
        let block = self.blocks.get(&root).ok_or(StoreError::UnknownRoot)?.clone();
        let preset = self.parameters.phase0.preset;
        if matches!(block.schema_fork(), Fork::Electra | Fork::Fulu) {
            let (attestations, slashings) = match block.as_ref() {
                BeaconBlock::Electra(value, _) => (
                    value
                        .body
                        .attestations
                        .values
                        .iter()
                        .map(|v| v.to_ssz(preset))
                        .collect::<Result<Vec<_>, _>>()?,
                    value
                        .body
                        .attester_slashings
                        .values
                        .iter()
                        .map(|v| v.to_ssz(preset))
                        .collect::<Result<Vec<_>, _>>()?,
                ),
                BeaconBlock::Fulu(value, _) => (
                    value
                        .body
                        .attestations
                        .values
                        .iter()
                        .map(|v| v.to_ssz(preset))
                        .collect::<Result<Vec<_>, _>>()?,
                    value
                        .body
                        .attester_slashings
                        .values
                        .iter()
                        .map(|v| v.to_ssz(preset))
                        .collect::<Result<Vec<_>, _>>()?,
                ),
                _ => unreachable!("later fork selected above"),
            };
            for attestation in attestations {
                let result = self.on_attestation_wire_inner::<VERIFY_SIGNATURES>(
                    &attestation,
                    block.schema_fork(),
                    true,
                );
                Self::body_vote_result::<SUPPRESS_INVALID_BODY_VOTES>(result)?;
            }
            for slashing in slashings {
                let result = self.on_attester_slashing_wire_inner::<VERIFY_SIGNATURES>(
                    &slashing,
                    block.schema_fork(),
                );
                Self::body_vote_result::<SUPPRESS_INVALID_BODY_VOTES>(result)?;
            }
            return Ok(())
        }

        let (attestations, slashings) = match block.as_ref() {
            BeaconBlock::Phase0(block, _) => (
                block.body.attestations.values.clone(),
                block.body.attester_slashings.values.clone(),
            ),
            BeaconBlock::Altair(block, _) => (
                block
                    .body
                    .attestations
                    .values
                    .iter()
                    .map(|value| phase0::Attestation::from_ssz(&value.to_ssz(preset)?, preset))
                    .collect::<Result<Vec<_>, SszError>>()?,
                block
                    .body
                    .attester_slashings
                    .values
                    .iter()
                    .map(|value| phase0::AttesterSlashing::from_ssz(&value.to_ssz(preset)?, preset))
                    .collect::<Result<Vec<_>, SszError>>()?,
            ),
            BeaconBlock::Bellatrix(block, _) => (
                block
                    .body
                    .attestations
                    .values
                    .iter()
                    .map(|value| phase0::Attestation::from_ssz(&value.to_ssz(preset)?, preset))
                    .collect::<Result<Vec<_>, SszError>>()?,
                block
                    .body
                    .attester_slashings
                    .values
                    .iter()
                    .map(|value| phase0::AttesterSlashing::from_ssz(&value.to_ssz(preset)?, preset))
                    .collect::<Result<Vec<_>, SszError>>()?,
            ),
            BeaconBlock::Capella(block, _) => (
                block
                    .body
                    .attestations
                    .values
                    .iter()
                    .map(|value| phase0::Attestation::from_ssz(&value.to_ssz(preset)?, preset))
                    .collect::<Result<Vec<_>, SszError>>()?,
                block
                    .body
                    .attester_slashings
                    .values
                    .iter()
                    .map(|value| phase0::AttesterSlashing::from_ssz(&value.to_ssz(preset)?, preset))
                    .collect::<Result<Vec<_>, SszError>>()?,
            ),
            BeaconBlock::Deneb(block, _) => (
                block
                    .body
                    .attestations
                    .values
                    .iter()
                    .map(|value| phase0::Attestation::from_ssz(&value.to_ssz(preset)?, preset))
                    .collect::<Result<Vec<_>, SszError>>()?,
                block
                    .body
                    .attester_slashings
                    .values
                    .iter()
                    .map(|value| phase0::AttesterSlashing::from_ssz(&value.to_ssz(preset)?, preset))
                    .collect::<Result<Vec<_>, SszError>>()?,
            ),
            _ => return Err(StoreError::ValidationUnavailable(block.schema_fork())),
        };
        for attestation in &attestations {
            let result = self.on_attestation_inner::<VERIFY_SIGNATURES>(attestation, true);
            Self::body_vote_result::<SUPPRESS_INVALID_BODY_VOTES>(result)?;
        }
        for slashing in &slashings {
            let result = self.on_attester_slashing_inner::<VERIFY_SIGNATURES>(slashing);
            Self::body_vote_result::<SUPPRESS_INVALID_BODY_VOTES>(result)?;
        }
        Ok(())
    }

    // The pinned compliance event format independently ignores assertion-invalid body votes.
    // Resource/configuration failures are never equivalent to a normative vote rejection.
    fn body_vote_result<const SUPPRESS_INVALID_BODY_VOTES: bool>(
        result: Result<(), StoreError>,
    ) -> Result<(), StoreError> {
        #[cfg(feature = "spec-tests")]
        if SUPPRESS_INVALID_BODY_VOTES &&
            matches!(
                &result,
                Err(StoreError::UnknownRoot |
                    StoreError::InvalidAttestationTime |
                    StoreError::InvalidAttestationTarget |
                    StoreError::InvalidSlashing |
                    StoreError::Processing(ProcessingError::Crypto(_)) |
                    StoreError::Kernel(
                        ForkChoiceError::UnknownRoot |
                            ForkChoiceError::InvalidAttestationTime |
                            ForkChoiceError::InvalidAttestationTarget |
                            ForkChoiceError::InvalidValidatorIndices
                    ))
            )
        {
            return Ok(());
        }
        if SUPPRESS_INVALID_BODY_VOTES && !cfg!(feature = "spec-tests") {
            return Err(StoreError::ValidationUnavailable(Fork::Phase0));
        }
        result
    }

    /// Authenticate a wire attestation against its actual target checkpoint state and committee.
    pub fn on_attestation(&mut self, attestation: &phase0::Attestation) -> Result<(), StoreError> {
        self.on_attestation_inner::<true>(attestation, false)
    }

    fn on_attestation_inner<const VERIFY_SIGNATURES: bool>(
        &mut self,
        attestation: &phase0::Attestation,
        is_from_retained_block: bool,
    ) -> Result<(), StoreError> {
        let data = &attestation.data;
        let epoch = self.current_slot() / self.parameters.phase0.slots_per_epoch();
        if (!is_from_retained_block &&
            ![epoch, epoch.saturating_sub(1)].contains(&data.target.epoch)) ||
            data.target.epoch != data.slot / self.parameters.phase0.slots_per_epoch() ||
            data.slot >= self.current_slot()
        {
            return Err(StoreError::InvalidAttestationTime)
        }
        let target_block = self.blocks.get(&data.target.root).ok_or(StoreError::UnknownRoot)?;
        let vote_block = self.blocks.get(&data.beacon_block_root).ok_or(StoreError::UnknownRoot)?;
        if !matches!(
            target_block.schema_fork(),
            Fork::Phase0 | Fork::Altair | Fork::Bellatrix | Fork::Capella | Fork::Deneb
        ) || !matches!(
            vote_block.schema_fork(),
            Fork::Phase0 | Fork::Altair | Fork::Bellatrix | Fork::Capella | Fork::Deneb
        ) {
            return Err(StoreError::ValidationUnavailable(vote_block.schema_fork()))
        }
        let target_slot = data
            .target
            .epoch
            .checked_mul(self.parameters.phase0.slots_per_epoch())
            .ok_or(StoreError::Overflow)?;
        if vote_block.header()?.slot > data.slot ||
            self.kernel.ancestor(data.beacon_block_root, target_slot)? != data.target.root
        {
            return Err(StoreError::InvalidAttestationTarget)
        }
        let mut candidate = self.clone();
        let state = candidate.checkpoint_state_inner::<VERIFY_SIGNATURES>(data.target)?;
        let indices = match state.as_ref() {
            BeaconState::Phase0(state, _) => {
                let indexed = phase0_processing::get_indexed_attestation(
                    state,
                    attestation,
                    &candidate.parameters.phase0,
                )?;
                validate_indexed!(
                    VERIFY_SIGNATURES,
                    phase0_processing,
                    state,
                    &indexed,
                    &candidate.parameters.phase0,
                )?;
                indexed.attesting_indices.values
            }
            BeaconState::Altair(state, preset) => {
                // The pre-Electra wire attestation has the same canonical schema.
                // Decode it into the target state's fork type before real committee/BLS checks.
                let bytes = attestation.to_ssz(*preset)?;
                let attestation = altair::Attestation::from_ssz(&bytes, *preset)?;
                let parameters = candidate.parameters.altair_parameters();
                let indexed =
                    altair_processing::get_indexed_attestation(state, &attestation, &parameters)?;
                validate_indexed!(
                    VERIFY_SIGNATURES,
                    altair_processing,
                    state,
                    &indexed,
                    &parameters
                )?;
                indexed.attesting_indices.values
            }
            BeaconState::Bellatrix(state, preset) => {
                let bytes = attestation.to_ssz(*preset)?;
                let attestation = bellatrix::Attestation::from_ssz(&bytes, *preset)?;
                let parameters = &candidate
                    .transition_parameters
                    .as_ref()
                    .ok_or(StoreError::ValidationUnavailable(Fork::Bellatrix))?
                    .gloas
                    .fulu
                    .electra
                    .deneb
                    .capella
                    .bellatrix;
                let indexed =
                    bellatrix_processing::get_indexed_attestation(state, &attestation, parameters)?;
                validate_indexed!(
                    VERIFY_SIGNATURES,
                    bellatrix_processing,
                    state,
                    &indexed,
                    parameters
                )?;
                indexed.attesting_indices.values
            }
            BeaconState::Capella(state, preset) => {
                let bytes = attestation.to_ssz(*preset)?;
                let attestation = capella::Attestation::from_ssz(&bytes, *preset)?;
                let parameters = &candidate
                    .transition_parameters
                    .as_ref()
                    .ok_or(StoreError::ValidationUnavailable(Fork::Capella))?
                    .gloas
                    .fulu
                    .electra
                    .deneb
                    .capella;
                let indexed =
                    capella_processing::get_indexed_attestation(state, &attestation, parameters)?;
                validate_indexed!(
                    VERIFY_SIGNATURES,
                    capella_processing,
                    state,
                    &indexed,
                    parameters
                )?;
                indexed.attesting_indices.values
            }
            BeaconState::Deneb(state, preset) => {
                let bytes = attestation.to_ssz(*preset)?;
                let attestation = deneb::Attestation::from_ssz(&bytes, *preset)?;
                let parameters = &candidate
                    .transition_parameters
                    .as_ref()
                    .ok_or(StoreError::ValidationUnavailable(Fork::Deneb))?
                    .gloas
                    .fulu
                    .electra
                    .deneb;
                let indexed =
                    deneb_processing::get_indexed_attestation(state, &attestation, parameters)?;
                validate_indexed!(
                    VERIFY_SIGNATURES,
                    deneb_processing,
                    state,
                    &indexed,
                    parameters
                )?;
                indexed.attesting_indices.values
            }
            _ => return Err(StoreError::ValidationUnavailable(state.schema_fork())),
        };
        // Committee and BLS validation above are the trusted admission path. An
        // authenticated vote on a retained non-finalized branch still updates LMD;
        // viability filtering decides whether that branch can affect the head.
        for index in indices {
            if !candidate.kernel.equivocating.contains(&index) &&
                candidate
                    .kernel
                    .latest_messages
                    .get(&index)
                    .is_none_or(|previous| data.target.epoch > previous.epoch)
            {
                candidate.kernel.latest_messages.insert(
                    index,
                    LatestMessage { epoch: data.target.epoch, root: data.beacon_block_root },
                );
                candidate.latest_message_slots.insert(index, data.slot);
            }
        }
        *self = candidate;
        Ok(())
    }

    /// Decode and authenticate Electra/Fulu wire attestations against actual retained target state.
    /// Earlier wire schemas use the ordinary attestation entrypoint. Cross-schema previous
    /// checkpoint validation remains explicit until its wider committee/domain helper is present.
    pub fn on_attestation_wire(&mut self, bytes: &[u8], fork: Fork) -> Result<(), StoreError> {
        self.on_attestation_wire_inner::<true>(bytes, fork, false)
    }

    fn on_attestation_wire_inner<const VERIFY_SIGNATURES: bool>(
        &mut self,
        bytes: &[u8],
        fork: Fork,
        from_block: bool,
    ) -> Result<(), StoreError> {
        if fork < Fork::Electra {
            return self.on_attestation_inner::<VERIFY_SIGNATURES>(
                &phase0::Attestation::from_ssz(bytes, self.parameters.phase0.preset)?,
                from_block,
            )
        }
        if fork == Fork::Gloas {
            return Err(StoreError::ValidationUnavailable(fork))
        }
        let preset = self.parameters.phase0.preset;
        let vote = electra::Attestation::from_ssz(bytes, preset)?;
        let data = vote.data.clone();
        self.validate_later_attestation_target(&data, from_block)?;
        let mut candidate = self.clone();
        let state = candidate.checkpoint_state_inner::<VERIFY_SIGNATURES>(data.target)?;
        let parameters = candidate
            .transition_parameters
            .as_ref()
            .ok_or(StoreError::ValidationUnavailable(fork))?;
        let indices = match state.as_ref() {
            BeaconState::Electra(state, _) => {
                let indexed = electra_processing::get_indexed_attestation(
                    state,
                    &vote,
                    &parameters.gloas.fulu.electra,
                )?;
                validate_indexed!(
                    VERIFY_SIGNATURES,
                    electra_processing,
                    state,
                    &indexed,
                    &parameters.gloas.fulu.electra,
                )?;
                indexed.attesting_indices.values
            }
            BeaconState::Fulu(state, _) => {
                let vote = fulu::Attestation::from_ssz(bytes, preset)?;
                let indexed = if VERIFY_SIGNATURES {
                    fulu_processing::get_validated_indexed_attestation(
                        state,
                        &vote,
                        &parameters.gloas.fulu,
                    )
                } else {
                    #[cfg(feature = "spec-tests")]
                    {
                        fulu_processing::spec_only::get_validated_indexed_attestation(
                            state,
                            &vote,
                            &parameters.gloas.fulu,
                        )
                    }
                    #[cfg(not(feature = "spec-tests"))]
                    {
                        Err(ProcessingError::InvalidState(
                            "fixture signature policy is unavailable",
                        ))
                    }
                }?;
                indexed.attesting_indices.values
            }
            BeaconState::Deneb(state, _) => {
                if fork == Fork::Electra {
                    reth_embedded_beacon_state_transition::attestation_validation::validate_electra_attestation_at_deneb_checkpoint(state, &vote, &parameters.gloas.fulu.electra.deneb)?
                } else {
                    let vote = fulu::Attestation::from_ssz(bytes, preset)?;
                    reth_embedded_beacon_state_transition::attestation_validation::validate_fulu_attestation_at_deneb_checkpoint(state,&vote,&parameters.gloas.fulu.electra.deneb)?
                }
            }
            _ => return Err(StoreError::ValidationUnavailable(state.schema_fork())),
        };
        for index in indices {
            if !candidate.kernel.equivocating.contains(&index) &&
                candidate
                    .kernel
                    .latest_messages
                    .get(&index)
                    .is_none_or(|previous| data.target.epoch > previous.epoch)
            {
                candidate.kernel.latest_messages.insert(
                    index,
                    LatestMessage { epoch: data.target.epoch, root: data.beacon_block_root },
                );
                candidate.latest_message_slots.insert(index, data.slot);
            }
        }
        *self = candidate;
        Ok(())
    }

    /// Authenticate the complete Electra/Fulu slashing wire schema before marking equivocation.
    pub fn on_attester_slashing_wire(
        &mut self,
        bytes: &[u8],
        fork: Fork,
    ) -> Result<(), StoreError> {
        self.on_attester_slashing_wire_inner::<true>(bytes, fork)
    }

    fn on_attester_slashing_wire_inner<const VERIFY_SIGNATURES: bool>(
        &mut self,
        bytes: &[u8],
        fork: Fork,
    ) -> Result<(), StoreError> {
        if fork < Fork::Electra {
            return self.on_attester_slashing_inner::<VERIFY_SIGNATURES>(
                &phase0::AttesterSlashing::from_ssz(bytes, self.parameters.phase0.preset)?,
            )
        }
        if fork == Fork::Gloas {
            return Err(StoreError::ValidationUnavailable(fork))
        }
        let preset = self.parameters.phase0.preset;
        let value = electra::AttesterSlashing::from_ssz(bytes, preset)?;
        let first = &value.attestation_1;
        let second = &value.attestation_2;
        let a = &first.data;
        let b = &second.data;
        if !((a != b && a.target.epoch == b.target.epoch) ||
            (a.source.epoch < b.source.epoch && b.target.epoch < a.target.epoch))
        {
            return Err(StoreError::InvalidSlashing)
        }
        let (justified, _) = self.kernel.checkpoints();
        let state = self.read_block_state(justified.root)?.ok_or(StoreError::UnknownRoot)?;
        let parameters =
            self.transition_parameters.as_ref().ok_or(StoreError::ValidationUnavailable(fork))?;
        match state.as_ref() {
            BeaconState::Electra(state, _) => {
                validate_indexed!(
                    VERIFY_SIGNATURES,
                    electra_processing,
                    state,
                    first,
                    &parameters.gloas.fulu.electra,
                )?;
                validate_indexed!(
                    VERIFY_SIGNATURES,
                    electra_processing,
                    state,
                    second,
                    &parameters.gloas.fulu.electra,
                )?;
            }
            BeaconState::Fulu(state, _) => {
                let value = fulu::AttesterSlashing::from_ssz(bytes, preset)?;
                validate_indexed!(
                    VERIFY_SIGNATURES,
                    fulu_processing,
                    state,
                    &value.attestation_1,
                    &parameters.gloas.fulu,
                )?;
                validate_indexed!(
                    VERIFY_SIGNATURES,
                    fulu_processing,
                    state,
                    &value.attestation_2,
                    &parameters.gloas.fulu,
                )?;
            }
            _ => return Err(StoreError::ValidationUnavailable(state.schema_fork())),
        }
        self.kernel.equivocating.extend(
            first
                .attesting_indices
                .values
                .iter()
                .copied()
                .filter(|index| second.attesting_indices.values.binary_search(index).is_ok()),
        );
        Ok(())
    }

    /// Authenticate both slashing attestations before recording any equivocator.
    pub fn on_attester_slashing(
        &mut self,
        slashing: &phase0::AttesterSlashing,
    ) -> Result<(), StoreError> {
        self.on_attester_slashing_inner::<true>(slashing)
    }

    fn on_attester_slashing_inner<const VERIFY_SIGNATURES: bool>(
        &mut self,
        slashing: &phase0::AttesterSlashing,
    ) -> Result<(), StoreError> {
        let first = &slashing.attestation_1;
        let second = &slashing.attestation_2;
        let a = &first.data;
        let b = &second.data;
        let slashable = (a != b && a.target.epoch == b.target.epoch) ||
            (a.source.epoch < b.source.epoch && b.target.epoch < a.target.epoch);
        if !slashable {
            return Err(StoreError::InvalidSlashing)
        }
        let (justified, _) = self.kernel.checkpoints();
        let state = self.read_block_state(justified.root)?.ok_or(StoreError::UnknownRoot)?;
        match state.as_ref() {
            BeaconState::Phase0(state, _) => {
                validate_indexed!(
                    VERIFY_SIGNATURES,
                    phase0_processing,
                    state,
                    first,
                    &self.parameters.phase0,
                )?;
                validate_indexed!(
                    VERIFY_SIGNATURES,
                    phase0_processing,
                    state,
                    second,
                    &self.parameters.phase0,
                )?;
            }
            BeaconState::Altair(state, preset) => {
                let first = altair::IndexedAttestation::from_ssz(&first.to_ssz(*preset)?, *preset)?;
                let second =
                    altair::IndexedAttestation::from_ssz(&second.to_ssz(*preset)?, *preset)?;
                let parameters = self.parameters.altair_parameters();
                validate_indexed!(
                    VERIFY_SIGNATURES,
                    altair_processing,
                    state,
                    &first,
                    &parameters
                )?;
                validate_indexed!(
                    VERIFY_SIGNATURES,
                    altair_processing,
                    state,
                    &second,
                    &parameters
                )?;
            }
            BeaconState::Bellatrix(state, preset) => {
                let first =
                    bellatrix::IndexedAttestation::from_ssz(&first.to_ssz(*preset)?, *preset)?;
                let second =
                    bellatrix::IndexedAttestation::from_ssz(&second.to_ssz(*preset)?, *preset)?;
                let parameters = &self
                    .transition_parameters
                    .as_ref()
                    .ok_or(StoreError::ValidationUnavailable(Fork::Bellatrix))?
                    .gloas
                    .fulu
                    .electra
                    .deneb
                    .capella
                    .bellatrix;
                validate_indexed!(
                    VERIFY_SIGNATURES,
                    bellatrix_processing,
                    state,
                    &first,
                    parameters
                )?;
                validate_indexed!(
                    VERIFY_SIGNATURES,
                    bellatrix_processing,
                    state,
                    &second,
                    parameters
                )?;
            }
            BeaconState::Capella(state, preset) => {
                let first =
                    capella::IndexedAttestation::from_ssz(&first.to_ssz(*preset)?, *preset)?;
                let second =
                    capella::IndexedAttestation::from_ssz(&second.to_ssz(*preset)?, *preset)?;
                let parameters = &self
                    .transition_parameters
                    .as_ref()
                    .ok_or(StoreError::ValidationUnavailable(Fork::Capella))?
                    .gloas
                    .fulu
                    .electra
                    .deneb
                    .capella;
                validate_indexed!(
                    VERIFY_SIGNATURES,
                    capella_processing,
                    state,
                    &first,
                    parameters
                )?;
                validate_indexed!(
                    VERIFY_SIGNATURES,
                    capella_processing,
                    state,
                    &second,
                    parameters
                )?;
            }
            BeaconState::Deneb(state, preset) => {
                let first = deneb::IndexedAttestation::from_ssz(&first.to_ssz(*preset)?, *preset)?;
                let second =
                    deneb::IndexedAttestation::from_ssz(&second.to_ssz(*preset)?, *preset)?;
                let parameters = &self
                    .transition_parameters
                    .as_ref()
                    .ok_or(StoreError::ValidationUnavailable(Fork::Deneb))?
                    .gloas
                    .fulu
                    .electra
                    .deneb;
                validate_indexed!(VERIFY_SIGNATURES, deneb_processing, state, &first, parameters)?;
                validate_indexed!(VERIFY_SIGNATURES, deneb_processing, state, &second, parameters)?;
            }
            _ => return Err(StoreError::ValidationUnavailable(state.schema_fork())),
        }
        let intersection = first
            .attesting_indices
            .values
            .iter()
            .copied()
            .filter(|index| second.attesting_indices.values.binary_search(index).is_ok())
            .collect::<Vec<_>>();
        self.kernel.equivocating.extend(intersection);
        Ok(())
    }

    /// Return the canonical unsigned node. Gloas anchors initially select the empty branch.
    ///
    /// Gloas event acceptance and full payload nodes remain unavailable, so this cannot
    /// be presented as complete Gloas fork-choice support.
    pub fn get_head(&self) -> Result<ForkChoiceNode, StoreError> {
        let root = self.kernel.head()?;
        let fork = self.blocks.get(&root).ok_or(StoreError::UnknownRoot)?.schema_fork();
        Ok(ForkChoiceNode {
            root,
            payload_status: (fork == Fork::Gloas).then_some(PayloadStatus::Empty),
        })
    }

    /// Observe every justified-root-reachable viable leaf and its actual authenticated vote weight.
    pub fn viable_leaf_weights(&self) -> Result<BTreeMap<Root, u128>, StoreError> {
        Ok(self.kernel.viable_leaf_weights()?)
    }

    /// Apply the optional pinned proposer-head optimization to the actual current head/clock.
    ///
    /// This derives committee weights, equivocations and timing from authenticated private
    /// store data. It does not select a supplied head or accept a caller-asserted vote weight.
    pub fn get_proposer_head(&self) -> Result<ForkChoiceNode, StoreError> {
        let head = self.get_head()?;
        let block = self.blocks.get(&head.root).ok_or(StoreError::UnknownRoot)?;
        if block.schema_fork() == Fork::Gloas {
            return Err(StoreError::ValidationUnavailable(block.schema_fork()))
        }
        let header = block.header()?;
        let parent = self.blocks.get(&header.parent_root).ok_or(StoreError::UnknownRoot)?;
        let parent_header = parent.header()?;
        if self.proposer_boost_root() == head.root {
            return Err(StoreError::ProposerBoostActive)
        }
        let slot = self.current_slot();
        let slots_per_epoch = self.parameters.phase0.slots_per_epoch();
        let (justified, finalized) = self.kernel.checkpoints();
        let justified_state = self
            .checkpoint_states
            .get(&(justified.epoch, justified.root))
            .ok_or(StoreError::UnknownRoot)?
            .acquire(self.parameters.phase0.preset)?;
        let committee_weight = active_balance(&justified_state, justified.epoch)? / slots_per_epoch;
        let head_threshold = u128::from(committee_weight) *
            u128::from(self.parameters.reorg_head_weight_threshold) /
            100;
        let parent_threshold = u128::from(committee_weight) *
            u128::from(self.parameters.reorg_parent_weight_threshold) /
            100;
        let mut head_weight = self.attestation_weight(head.root)?;
        let head_state = self.read_block_state(head.root)?.ok_or(StoreError::UnknownRoot)?;
        let epoch = header.slot / slots_per_epoch;
        let active = validators(&head_state)
            .iter()
            .filter(|validator| validator.activation_epoch <= epoch && epoch < validator.exit_epoch)
            .count();
        let (target_committee_size, max_committees) = match self.parameters.phase0.preset {
            Preset::Mainnet => (128, 64),
            Preset::Minimal => (4, 4),
        };
        let committee_count = (u64::try_from(active).map_err(|_| StoreError::Overflow)? /
            slots_per_epoch /
            target_committee_size)
            .clamp(1, max_committees);
        for index in 0..committee_count {
            let committee = match head_state.as_ref() {
                BeaconState::Phase0(state, _) => phase0_processing::get_beacon_committee(
                    state,
                    header.slot,
                    index,
                    &self.parameters.phase0,
                )?,
                BeaconState::Altair(state, _) => altair_processing::get_beacon_committee(
                    state,
                    header.slot,
                    index,
                    &self.parameters.altair_parameters(),
                )?,
                BeaconState::Bellatrix(state, _) => bellatrix_processing::get_beacon_committee(
                    state,
                    header.slot,
                    index,
                    &self
                        .transition_parameters
                        .as_ref()
                        .ok_or(StoreError::ValidationUnavailable(Fork::Bellatrix))?
                        .gloas
                        .fulu
                        .electra
                        .deneb
                        .capella
                        .bellatrix,
                )?,
                BeaconState::Capella(state, _) => capella_processing::get_beacon_committee(
                    state,
                    header.slot,
                    index,
                    &self
                        .transition_parameters
                        .as_ref()
                        .ok_or(StoreError::ValidationUnavailable(Fork::Capella))?
                        .gloas
                        .fulu
                        .electra
                        .deneb
                        .capella,
                )?,
                BeaconState::Deneb(state, _) => deneb_processing::get_beacon_committee(
                    state,
                    header.slot,
                    index,
                    &self
                        .transition_parameters
                        .as_ref()
                        .ok_or(StoreError::ValidationUnavailable(Fork::Deneb))?
                        .gloas
                        .fulu
                        .electra
                        .deneb,
                )?,
                BeaconState::Electra(state, _) => electra_processing::get_beacon_committee(
                    state,
                    header.slot,
                    index,
                    &self
                        .transition_parameters
                        .as_ref()
                        .ok_or(StoreError::ValidationUnavailable(Fork::Electra))?
                        .gloas
                        .fulu
                        .electra,
                )?,
                BeaconState::Fulu(state, _) => fulu_processing::get_beacon_committee(
                    state,
                    header.slot,
                    index,
                    &self
                        .transition_parameters
                        .as_ref()
                        .ok_or(StoreError::ValidationUnavailable(Fork::Fulu))?
                        .gloas
                        .fulu,
                )?,
                _ => return Err(StoreError::ValidationUnavailable(head_state.schema_fork())),
            };
            for index in committee {
                if self.kernel.equivocating.contains(&index) {
                    let weight = self
                        .kernel
                        .weights
                        .get(&index)
                        .ok_or(StoreError::InvalidAttestationTarget)?;
                    head_weight = head_weight
                        .checked_add(u128::from(weight.effective_balance))
                        .ok_or(StoreError::Overflow)?;
                }
            }
        }
        let head_weak = head_weight < head_threshold;
        let current_time_ok = header.slot.checked_add(1).ok_or(StoreError::Overflow)? == slot;
        let equivocation = self
            .kernel
            .blocks
            .values()
            .filter(|record| record.slot == header.slot)
            .filter_map(|record| self.blocks.get(&record.root))
            .map(|block| block.header())
            .collect::<Result<Vec<_>, _>>()?
            .iter()
            .filter(|candidate| candidate.proposer_index == header.proposer_index)
            .count() >
            1;
        let ordinary_reorg = !self
            .block_timeliness
            .get(&head.root)
            .ok_or(StoreError::UnknownRoot)?[0] &&
            (header.slot / slots_per_epoch == slot / slots_per_epoch ||
                block.schema_fork() >= Fork::Fulu) &&
            self.kernel.blocks[&head.root].unrealized_justified ==
                self.kernel.blocks[&header.parent_root].unrealized_justified &&
            (slot / slots_per_epoch).checked_sub(finalized.epoch).ok_or(StoreError::Overflow)? <=
                self.parameters.reorg_max_epochs_since_finalization &&
            self.time_into_slot_ms() <=
                self.deadline_ms(self.parameters.proposer_reorg_cutoff_bps)? &&
            parent_header.slot.checked_add(1).ok_or(StoreError::Overflow)? == header.slot &&
            current_time_ok &&
            head_weak &&
            self.attestation_weight(header.parent_root)? > parent_threshold;
        if ordinary_reorg || head_weak && current_time_ok && equivocation {
            Ok(ForkChoiceNode { root: header.parent_root, payload_status: None })
        } else {
            Ok(head)
        }
    }

    fn attestation_weight(&self, root: Root) -> Result<u128, StoreError> {
        let mut total = 0u128;
        for (index, message) in &self.kernel.latest_messages {
            if let Some(validator) = self.kernel.weights.get(index) &&
                validator.active &&
                !validator.slashed &&
                !self.kernel.equivocating.contains(index) &&
                self.kernel.is_ancestor(message.root, root)?
            {
                total = total
                    .checked_add(u128::from(validator.effective_balance))
                    .ok_or(StoreError::Overflow)?;
            }
        }
        Ok(total)
    }

    /// Actual trusted genesis clock identity in Unix milliseconds.
    pub const fn genesis_time_ms(&self) -> u64 {
        self.genesis_time_ms
    }

    /// Current slot from the actual genesis-relative millisecond clock.
    pub const fn current_slot(&self) -> u64 {
        (self.time_ms - self.genesis_time_ms) / self.parameters.slot_duration_ms
    }
    /// Current Unix time in milliseconds.
    pub const fn time_ms(&self) -> u64 {
        self.time_ms
    }
    /// Realized justified and finalized checkpoints.
    pub const fn checkpoints(&self) -> (Checkpoint, Checkpoint) {
        self.kernel.checkpoints()
    }
    /// Read-only retained state at an imported unsigned block root.
    /// Own an admitted complete post-state; known cold data failure is an error.
    pub fn read_block_state(&self, root: Root) -> Result<Option<StateRead>, StoreError> {
        self.block_states
            .get(&root)
            .map(|state| state.acquire(self.parameters.phase0.preset))
            .transpose()
    }
    pub(crate) fn read_checkpoint_state(
        &self,
        checkpoint: Checkpoint,
    ) -> Result<Option<StateRead>, StoreError> {
        self.checkpoint_states
            .get(&(checkpoint.epoch, checkpoint.root))
            .map(|state| state.acquire(self.parameters.phase0.preset))
            .transpose()
    }

    /// Borrow a retained hot post-state; ordinary constructors keep this borrowed API.
    /// Compact owners use checked owning reads for cold states.
    pub fn block_state(&self, root: Root) -> Option<&BeaconState> {
        self.block_states.get(&root).and_then(RetainedState::hot).map(AsRef::as_ref)
    }
    /// Borrow an immutable complete block at its trusted anchor or admitted block identity.
    pub fn block(&self, root: Root) -> Option<&BeaconBlock> {
        self.blocks.get(&root).map(AsRef::as_ref)
    }
    /// Share an immutable retained state allocation at its validated block identity.
    pub fn block_state_arc(&self, root: Root) -> Option<Arc<BeaconState>> {
        self.block_states.get(&root).and_then(RetainedState::hot).cloned()
    }

    /// Share an immutable complete block allocation admitted by the trusted anchor or import path.
    pub fn block_arc(&self, root: Root) -> Option<Arc<BeaconBlock>> {
        self.blocks.get(&root).cloned()
    }

    pub(crate) fn execution_status(&self, root: Root) -> ExecutionStatus {
        self.kernel
            .blocks
            .get(&root)
            .map_or(ExecutionStatus::Optimistic, |record| record.execution_status)
    }

    /// Return an exact execution hash only when this retained block is actually valid.
    /// Optimistic or invalid execution cannot authorize a fork-choice update.
    pub fn verified_execution_block_hash(&self, root: Root) -> Result<Option<Root>, StoreError> {
        let record = self.kernel.blocks.get(&root).ok_or(StoreError::UnknownRoot)?;
        if record.execution_status != ExecutionStatus::Valid {
            return Err(StoreError::ExecutionImport("execution is not verified".into()))
        }
        let hash = match self.blocks.get(&root).ok_or(StoreError::UnknownRoot)?.as_ref() {
            BeaconBlock::Phase0(..) | BeaconBlock::Altair(..) => return Ok(None),
            BeaconBlock::Bellatrix(block, _) => block.body.execution_payload.block_hash,
            BeaconBlock::Capella(block, _) => block.body.execution_payload.block_hash,
            BeaconBlock::Deneb(block, _) => block.body.execution_payload.block_hash,
            BeaconBlock::Electra(block, _) => block.body.execution_payload.block_hash,
            BeaconBlock::Fulu(block, _) => block.body.execution_payload.block_hash,
            BeaconBlock::Gloas(..) => return Err(StoreError::ValidationUnavailable(Fork::Gloas)),
        };
        Ok((hash != [0; 32]).then_some(hash))
    }

    pub(super) const fn migration_parameters(&self) -> StoreParameters {
        self.parameters
    }

    pub(super) fn migration_roots(&self) -> impl Iterator<Item = Root> + '_ {
        self.blocks.keys().copied()
    }

    pub(super) const fn migration_checkpoints(
        &self,
    ) -> (Checkpoint, Checkpoint, Checkpoint, Checkpoint) {
        let (justified, finalized) = self.kernel.checkpoints();
        (justified, finalized, self.unrealized_justified, self.unrealized_finalized)
    }

    pub(super) fn migration_votes(&self) -> Result<Vec<(u64, u64, Root)>, StoreError> {
        self.kernel
            .latest_messages
            .iter()
            .map(|(&index, message)| {
                let slot = *self
                    .latest_message_slots
                    .get(&index)
                    .ok_or(StoreError::InvalidAttestationTarget)?;
                if slot / self.slots_per_epoch() != message.epoch {
                    return Err(StoreError::InvalidAttestationTarget)
                }
                Ok((index, slot, message.root))
            })
            .collect()
    }

    pub(super) fn migration_equivocations(&self) -> BTreeSet<u64> {
        self.kernel.equivocating.clone()
    }

    pub(super) fn migration_timeliness(&self) -> BTreeMap<Root, [bool; 2]> {
        self.block_timeliness.clone()
    }

    pub(super) fn migration_unrealized(&self) -> BTreeMap<Root, Checkpoint> {
        self.kernel
            .blocks
            .iter()
            .map(|(&root, record)| (root, record.unrealized_justified))
            .collect()
    }

    pub(super) fn migration_committee(
        &self,
        root: Root,
        slot: u64,
        index: u64,
    ) -> Result<Vec<u64>, StoreError> {
        let state = self.read_block_state(root)?.ok_or(StoreError::UnknownRoot)?;
        let parameters = self
            .transition_parameters
            .as_ref()
            .ok_or_else(|| StoreError::ValidationUnavailable(state.schema_fork()))?;
        Ok(match state.as_ref() {
            BeaconState::Phase0(s, _) => phase0_processing::get_beacon_committee(
                s,
                slot,
                index,
                &parameters.gloas.fulu.electra.deneb.capella.bellatrix.altair.phase0,
            )?,
            BeaconState::Altair(s, _) => altair_processing::get_beacon_committee(
                s,
                slot,
                index,
                &parameters.gloas.fulu.electra.deneb.capella.bellatrix.altair,
            )?,
            BeaconState::Bellatrix(s, _) => bellatrix_processing::get_beacon_committee(
                s,
                slot,
                index,
                &parameters.gloas.fulu.electra.deneb.capella.bellatrix,
            )?,
            BeaconState::Capella(s, _) => capella_processing::get_beacon_committee(
                s,
                slot,
                index,
                &parameters.gloas.fulu.electra.deneb.capella,
            )?,
            BeaconState::Deneb(s, _) => deneb_processing::get_beacon_committee(
                s,
                slot,
                index,
                &parameters.gloas.fulu.electra.deneb,
            )?,
            BeaconState::Electra(s, _) => electra_processing::get_beacon_committee(
                s,
                slot,
                index,
                &parameters.gloas.fulu.electra,
            )?,
            BeaconState::Fulu(s, _) => {
                fulu_processing::get_beacon_committee(s, slot, index, &parameters.gloas.fulu)?
            }
            BeaconState::Gloas(..) => return Err(StoreError::AnchorMismatch),
        })
    }

    pub(super) fn ensure_migration_checkpoint(
        &mut self,
        checkpoint: Checkpoint,
    ) -> Result<(), StoreError> {
        self.checkpoint_state_inner::<true>(checkpoint)?;
        Ok(())
    }

    /// Derive a consistent read-only head/checkpoint view from this store.
    pub fn snapshot(&self) -> Result<StoreSnapshot, StoreError> {
        let head = self.get_head()?;
        let head_slot = self.blocks.get(&head.root).ok_or(StoreError::UnknownRoot)?.header()?.slot;
        let (justified_checkpoint, finalized_checkpoint) = self.kernel.checkpoints();
        Ok(StoreSnapshot { head, head_slot, justified_checkpoint, finalized_checkpoint })
    }

    /// Return at most 1,024 finalized epoch roots, derived from actual canonical ancestry.
    ///
    /// A checkpoint-only anchor may not retain earlier ancestors. Such history is explicitly
    /// unavailable; this method never invents roots or accepts caller-supplied checkpoint data.
    pub fn canonical_finalized_epoch_roots(
        &self,
        start_epoch: u64,
        count: usize,
    ) -> Result<Vec<(u64, Root)>, StoreError> {
        if count == 0 || count > 1024 {
            return Err(StoreError::ResourceLimit)
        }
        let end = start_epoch
            .checked_add(u64::try_from(count).map_err(|_| StoreError::Overflow)? - 1)
            .ok_or(StoreError::Overflow)?;
        let (_, finalized) = self.kernel.checkpoints();
        if end > finalized.epoch {
            return Err(StoreError::UnavailableHistory)
        }
        (start_epoch..=end)
            .map(|epoch| {
                let slot = epoch
                    .checked_mul(self.parameters.phase0.slots_per_epoch())
                    .ok_or(StoreError::Overflow)?;
                let root = self.kernel.ancestor(finalized.root, slot).map_err(|error| {
                    if error == ForkChoiceError::UnknownRoot {
                        StoreError::UnavailableHistory
                    } else {
                        StoreError::Kernel(error)
                    }
                })?;
                Ok((epoch, root))
            })
            .collect()
    }

    /// First timely proposer boost root, or zero if no boost is active.
    pub fn proposer_boost_root(&self) -> Root {
        self.kernel.proposer_boost.map_or([0; 32], |(root, _)| root)
    }

    const fn time_into_slot_ms(&self) -> u64 {
        (self.time_ms - self.genesis_time_ms) % self.parameters.slot_duration_ms
    }
    fn deadline_ms(&self, bps: u64) -> Result<u64, StoreError> {
        self.parameters
            .slot_duration_ms
            .checked_mul(bps)
            .ok_or(StoreError::Overflow)
            .map(|value| value / 10_000)
    }

    #[cfg(test)]
    fn checkpoint_state(&mut self, checkpoint: Checkpoint) -> Result<Arc<BeaconState>, StoreError> {
        self.checkpoint_state_inner::<true>(checkpoint)?
            .hot_arc()
            .ok_or_else(|| StateSourceError::Unavailable.into())
    }

    fn checkpoint_state_inner<const VERIFY_SIGNATURES: bool>(
        &mut self,
        checkpoint: Checkpoint,
    ) -> Result<StateRead, StoreError> {
        let (entry, read) = self.prepare_checkpoint_state::<VERIFY_SIGNATURES>(checkpoint)?;
        self.checkpoint_states.entry((checkpoint.epoch, checkpoint.root)).or_insert(entry);
        Ok(read)
    }

    /// Prepare an owning checked checkpoint view without publishing prospective state entries.
    /// Vote validation can hold this immutable allocation through individual commits without
    /// copying the kernel or its latest-message maps. The caller retains rollback authority.
    fn prepare_checkpoint_state<const VERIFY_SIGNATURES: bool>(
        &self,
        checkpoint: Checkpoint,
    ) -> Result<(RetainedState<BeaconState>, StateRead), StoreError> {
        let key = (checkpoint.epoch, checkpoint.root);
        if let Some(state) = self.checkpoint_states.get(&key) {
            return Ok((state.clone(), state.acquire(self.parameters.phase0.preset)?))
        }
        let base = self.read_block_state(checkpoint.root)?.ok_or(StoreError::UnknownRoot)?;
        let target = checkpoint
            .epoch
            .checked_mul(self.parameters.phase0.slots_per_epoch())
            .ok_or(StoreError::Overflow)?;
        if base.slot() >= target {
            let entry =
                self.block_states.get(&checkpoint.root).ok_or(StoreError::UnknownRoot)?.clone();
            return Ok((entry, base))
        }
        let mut state = base.as_ref().clone();
        if state.slot() < target {
            if target - state.slot() > self.parameters.max_tick_slots {
                return Err(StoreError::ResourceLimit)
            }
            if let Some(parameters) = &self.transition_parameters {
                if VERIFY_SIGNATURES {
                    fork_processing::process_slots(&mut state, target, parameters)?;
                } else {
                    #[cfg(feature = "spec-tests")]
                    fork_processing::spec_only::process_slots(&mut state, target, parameters)?;
                    #[cfg(not(feature = "spec-tests"))]
                    return Err(StoreError::ValidationUnavailable(state.schema_fork()));
                }
            } else {
                match &mut state {
                    BeaconState::Phase0(value, _) => {
                        phase0_processing::process_slots(value, target, &self.parameters.phase0)?;
                    }
                    BeaconState::Altair(value, _) => {
                        altair_processing::process_slots(
                            value,
                            target,
                            &self.parameters.altair_parameters(),
                        )?;
                    }
                    _ => return Err(StoreError::ValidationUnavailable(state.schema_fork())),
                }
            }
        }
        let extra = state
            .heap_bytes()?
            .checked_add(std::mem::size_of::<BeaconState>())
            .and_then(|n| n.checked_add(2 * std::mem::size_of::<usize>()))
            .ok_or(StoreError::Overflow)?;
        if self.retained_schema_bytes()?.checked_add(extra).ok_or(StoreError::Overflow)? >
            self.parameters.max_retained_schema_bytes
        {
            return Err(StoreError::ResourceLimit)
        }
        let state = Arc::new(state);
        Ok((state.clone().into(), StateRead::hot(state, self.parameters.phase0.preset)))
    }

    /// Configured network slot duration, already checked before constructing this store.
    pub const fn slot_duration_ms(&self) -> u64 {
        self.parameters.slot_duration_ms
    }

    /// Preset epoch length, already checked before constructing this store.
    pub const fn slots_per_epoch(&self) -> u64 {
        self.parameters.phase0.slots_per_epoch()
    }

    /// Exact owned allocation capacities of retained unique immutable state/block Arcs.
    /// This excludes temporary transition work, external Arc holders, allocator bookkeeping
    /// and bounded compact graph/index metadata; it is not a process-RSS measurement.
    pub fn retained_schema_bytes(&self) -> Result<usize, StoreError> {
        let mut seen = BTreeSet::new();
        let mut bytes = 0usize;
        for entry in self.block_states.values().chain(self.checkpoint_states.values()) {
            if let Some(state) = entry.hot() &&
                seen.insert(Arc::as_ptr(state) as usize)
            {
                bytes = bytes
                    .checked_add(state.heap_bytes()?)
                    .and_then(|n| n.checked_add(std::mem::size_of::<BeaconState>()))
                    .and_then(|n| n.checked_add(2 * std::mem::size_of::<usize>()))
                    .ok_or(StoreError::Overflow)?;
            }
        }
        for block in self.blocks.values() {
            bytes = bytes
                .checked_add(block.heap_bytes()?)
                .and_then(|n| n.checked_add(std::mem::size_of::<BeaconBlock>()))
                .and_then(|n| n.checked_add(2 * std::mem::size_of::<usize>()))
                .ok_or(StoreError::Overflow)?;
        }
        Ok(bytes)
    }

    fn check_retention_budget(&self) -> Result<(), StoreError> {
        if self.retained_schema_bytes()? > self.parameters.max_retained_schema_bytes {
            return Err(StoreError::ResourceLimit)
        }
        Ok(())
    }

    fn prune_retained_states(&mut self) -> Result<(), StoreError> {
        let (justified, finalized) = self.kernel.checkpoints();
        // Retain the previous finalized epoch for valid historical in-block attestations.
        // Compact block ancestry remains available for canonical finalized-root queries.
        let minimum_epoch = finalized.epoch.saturating_sub(1);
        let minimum_slot = minimum_epoch
            .checked_mul(self.parameters.phase0.slots_per_epoch())
            .ok_or(StoreError::Overflow)?;
        self.block_states.retain(|root, state| {
            state.slot(self.parameters.phase0.preset) >= minimum_slot ||
                *root == justified.root ||
                *root == finalized.root
        });
        self.checkpoint_states.retain(|(epoch, root), _| {
            *epoch >= minimum_epoch || *root == justified.root || *root == finalized.root
        });
        Ok(())
    }

    fn realize_checkpoints_inner<const VERIFY_SIGNATURES: bool>(
        &mut self,
        justified: Checkpoint,
        finalized: Checkpoint,
    ) -> Result<(), StoreError> {
        let (previous_justified, previous_finalized) = self.kernel.checkpoints();
        let justified =
            if justified.epoch > previous_justified.epoch { justified } else { previous_justified };
        let finalized =
            if finalized.epoch > previous_finalized.epoch { finalized } else { previous_finalized };
        if (justified, finalized) == (previous_justified, previous_finalized) {
            return Ok(())
        }
        let state = self.checkpoint_state_inner::<VERIFY_SIGNATURES>(justified)?;
        self.kernel.update_checkpoints(
            justified,
            finalized,
            state_weights(&state, justified.epoch)?,
        )?;
        Ok(())
    }

    /// Authenticate one actual attestation from an already admitted retained block.
    ///
    /// The body index is resolved privately, so callers cannot assert block provenance.
    /// A rejection leaves this store unchanged, including checkpoint replay caches.
    pub fn on_retained_block_attestation(
        &mut self,
        root: Root,
        index: usize,
    ) -> Result<(), StoreError> {
        let block = self.blocks.get(&root).ok_or(StoreError::UnknownRoot)?.clone();
        let preset = self.parameters.phase0.preset;
        macro_rules! wire {
            ($value:expr) => {
                $value
                    .body
                    .attestations
                    .values
                    .get(index)
                    .ok_or(StoreError::InvalidAttestationTarget)?
                    .to_ssz(preset)?
            };
        }
        let bytes = match block.as_ref() {
            BeaconBlock::Phase0(value, _) => wire!(value),
            BeaconBlock::Altair(value, _) => wire!(value),
            BeaconBlock::Bellatrix(value, _) => wire!(value),
            BeaconBlock::Capella(value, _) => wire!(value),
            BeaconBlock::Deneb(value, _) => wire!(value),
            BeaconBlock::Electra(value, _) => wire!(value),
            BeaconBlock::Fulu(value, _) => wire!(value),
            BeaconBlock::Gloas(..) => return Err(StoreError::ValidationUnavailable(Fork::Gloas)),
        };
        let mut candidate = self.clone();
        candidate.on_attestation_wire_inner::<true>(&bytes, block.schema_fork(), true)?;
        candidate.check_retention_budget()?;
        *self = candidate;
        Ok(())
    }

    /// Authenticate one actual slashing from an already admitted retained block.
    /// Any rejection leaves the graph, votes, equivocation set and caches unchanged.
    pub fn on_retained_block_slashing(
        &mut self,
        root: Root,
        index: usize,
    ) -> Result<(), StoreError> {
        let block = self.blocks.get(&root).ok_or(StoreError::UnknownRoot)?.clone();
        let preset = self.parameters.phase0.preset;
        macro_rules! wire {
            ($value:expr) => {
                $value
                    .body
                    .attester_slashings
                    .values
                    .get(index)
                    .ok_or(StoreError::InvalidSlashing)?
                    .to_ssz(preset)?
            };
        }
        let bytes = match block.as_ref() {
            BeaconBlock::Phase0(value, _) => wire!(value),
            BeaconBlock::Altair(value, _) => wire!(value),
            BeaconBlock::Bellatrix(value, _) => wire!(value),
            BeaconBlock::Capella(value, _) => wire!(value),
            BeaconBlock::Deneb(value, _) => wire!(value),
            BeaconBlock::Electra(value, _) => wire!(value),
            BeaconBlock::Fulu(value, _) => wire!(value),
            BeaconBlock::Gloas(..) => return Err(StoreError::ValidationUnavailable(Fork::Gloas)),
        };
        let mut candidate = self.clone();
        candidate.on_attester_slashing_wire_inner::<true>(&bytes, block.schema_fork())?;
        candidate.check_retention_budget()?;
        *self = candidate;
        Ok(())
    }
}

const fn state_checkpoints(state: &BeaconState) -> Result<(Checkpoint, Checkpoint), StoreError> {
    match state {
        BeaconState::Phase0(value, _) => {
            Ok((value.current_justified_checkpoint, value.finalized_checkpoint))
        }
        BeaconState::Altair(value, _) => {
            Ok((value.current_justified_checkpoint, value.finalized_checkpoint))
        }
        BeaconState::Bellatrix(value, _) => {
            Ok((value.current_justified_checkpoint, value.finalized_checkpoint))
        }
        BeaconState::Capella(value, _) => {
            Ok((value.current_justified_checkpoint, value.finalized_checkpoint))
        }
        BeaconState::Deneb(value, _) => {
            Ok((value.current_justified_checkpoint, value.finalized_checkpoint))
        }
        BeaconState::Electra(value, _) => {
            Ok((value.current_justified_checkpoint, value.finalized_checkpoint))
        }
        BeaconState::Fulu(value, _) => {
            Ok((value.current_justified_checkpoint, value.finalized_checkpoint))
        }
        BeaconState::Gloas(value, _) => {
            Ok((value.current_justified_checkpoint, value.finalized_checkpoint))
        }
    }
}

const fn state_preset(state: &BeaconState) -> Preset {
    match state {
        BeaconState::Phase0(_, p) |
        BeaconState::Altair(_, p) |
        BeaconState::Bellatrix(_, p) |
        BeaconState::Capella(_, p) |
        BeaconState::Deneb(_, p) |
        BeaconState::Electra(_, p) |
        BeaconState::Fulu(_, p) |
        BeaconState::Gloas(_, p) => *p,
    }
}

const fn block_preset(block: &BeaconBlock) -> Preset {
    match block {
        BeaconBlock::Phase0(_, p) |
        BeaconBlock::Altair(_, p) |
        BeaconBlock::Bellatrix(_, p) |
        BeaconBlock::Capella(_, p) |
        BeaconBlock::Deneb(_, p) |
        BeaconBlock::Electra(_, p) |
        BeaconBlock::Fulu(_, p) |
        BeaconBlock::Gloas(_, p) => *p,
    }
}

fn validators(state: &BeaconState) -> &[phase0::Validator] {
    match state {
        BeaconState::Phase0(s, _) => &s.validators.values,
        BeaconState::Altair(s, _) => &s.validators.values,
        BeaconState::Bellatrix(s, _) => &s.validators.values,
        BeaconState::Capella(s, _) => &s.validators.values,
        BeaconState::Deneb(s, _) => &s.validators.values,
        BeaconState::Electra(s, _) => &s.validators.values,
        BeaconState::Fulu(s, _) => &s.validators.values,
        BeaconState::Gloas(s, _) => &s.validators.values,
    }
}

fn state_weights(
    state: &BeaconState,
    epoch: u64,
) -> Result<BTreeMap<u64, ValidatorWeight>, StoreError> {
    validators(state)
        .iter()
        .enumerate()
        .map(|(index, validator)| {
            Ok((
                u64::try_from(index).map_err(|_| StoreError::Overflow)?,
                ValidatorWeight {
                    effective_balance: validator.effective_balance,
                    active: validator.activation_epoch <= epoch && epoch < validator.exit_epoch,
                    slashed: validator.slashed,
                },
            ))
        })
        .collect()
}

fn active_balance(state: &BeaconState, epoch: u64) -> Result<u64, StoreError> {
    let total = validators(state)
        .iter()
        .filter(|validator| validator.activation_epoch <= epoch && epoch < validator.exit_epoch)
        .try_fold(0u64, |total, validator| {
            total.checked_add(validator.effective_balance).ok_or(StoreError::Overflow)
        })?;
    Ok(total.max(1_000_000_000))
}

/// Invalid input is rejected before any production-store mutation.
#[derive(Debug, Error)]
pub enum StoreError {
    /// Known retained compact data failed reconstruction, binding, durability or resource checks.
    #[error(transparent)]
    CompactState(#[from] StateSourceError),
    /// Invalid clock, preset or resource configuration.
    #[error("invalid fork-choice parameters")]
    InvalidParameters,
    /// Complete state and block do not form the same trusted anchor.
    #[error("trusted anchor fork, preset, slot or state root mismatch")]
    AnchorMismatch,
    /// Time moved backwards.
    #[error("fork-choice time moved backwards")]
    TimeRegression,
    /// A bounded operation exceeded its resource budget.
    #[error("fork-choice resource limit exceeded")]
    ResourceLimit,
    /// Required production validation has not been implemented for this fork.
    #[error("production fork-choice transition or event validation unavailable for {0:?}")]
    ValidationUnavailable(Fork),
    /// Missing parent, target or checkpoint block.
    #[error("unknown fork-choice root")]
    UnknownRoot,
    /// The block is too early or in the future.
    #[error("invalid fork-choice block time")]
    InvalidBlockTime,
    /// The block contradicts finalized ancestry.
    #[error("block conflicts with finalized checkpoint")]
    ConflictsWithFinality,
    /// The wire attestation cannot affect the current store time.
    #[error("invalid fork-choice attestation time")]
    InvalidAttestationTime,
    /// The vote and its target ancestry disagree.
    #[error("invalid fork-choice attestation target")]
    InvalidAttestationTarget,
    /// The supplied attestation pair is not slashable.
    #[error("attestation pair is not slashable")]
    InvalidSlashing,
    /// The optional proposer optimization requires the head's previous boost to have expired.
    #[error("canonical head still has active proposer boost")]
    ProposerBoostActive,
    /// Requested epoch roots are not finalized or precede retained canonical history.
    #[error("canonical finalized history is unavailable")]
    UnavailableHistory,
    /// Checked arithmetic overflow.
    #[error("fork-choice arithmetic overflow")]
    Overflow,
    /// Complete structural SSZ failure.
    #[error(transparent)]
    Ssz(#[from] SszError),
    /// Actual production transition or BLS failure.
    #[error(transparent)]
    Processing(#[from] ProcessingError),
    /// Mandatory execution/import verification failed; no graph mutation was committed.
    #[error("mandatory execution/import verification failed: {0}")]
    ExecutionImport(String),
    /// Mandatory blob or configured column retrieval/proof verification failed.
    #[error("data availability verification failed: {0}")]
    DataAvailability(String),
    /// Graph invariant failure.
    #[error(transparent)]
    Kernel(#[from] ForkChoiceError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use reth_embedded_beacon_state_transition::ssz::List;

    fn published_phase0(
    ) -> (phase0::BeaconState, phase0::SignedBeaconBlock, phase0::SignedBeaconBlock) {
        let preset = Preset::Minimal;
        let base = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("testdata/official-phase0-import");
        let state = phase0::BeaconState::from_ssz(
            &std::fs::read(base.join("genesis.ssz")).unwrap(),
            preset,
        )
        .unwrap();
        let first = phase0::SignedBeaconBlock::from_ssz(
            &std::fs::read(base.join("blocks_0.ssz")).unwrap(),
            preset,
        )
        .unwrap();
        let second = phase0::SignedBeaconBlock::from_ssz(
            &std::fs::read(base.join("blocks_1.ssz")).unwrap(),
            preset,
        )
        .unwrap();
        (state, first, second)
    }

    fn phase0_store(state: phase0::BeaconState) -> Store {
        // The published sanity fixture supplies a genesis state; build its complete
        // empty trusted block exactly as genesis initialization does.
        let body = phase0::BeaconBlockBody {
            randao_reveal: [0; 96],
            eth1_data: phase0::Eth1Data {
                deposit_root: [0; 32],
                deposit_count: 0,
                block_hash: [0; 32],
            },
            graffiti: [0; 32],
            proposer_slashings: List::default(),
            attester_slashings: List::default(),
            attestations: List::default(),
            deposits: List::default(),
            voluntary_exits: List::default(),
        };
        assert_eq!(
            body.hash_tree_root(Preset::Minimal).unwrap(),
            state.latest_block_header.body_root
        );
        let block = phase0::BeaconBlock {
            slot: state.slot,
            proposer_index: state.latest_block_header.proposer_index,
            parent_root: state.latest_block_header.parent_root,
            state_root: state.hash_tree_root(Preset::Minimal).unwrap(),
            body,
        };
        Store::get_store(
            BeaconState::Phase0(state, Preset::Minimal),
            BeaconBlock::Phase0(block, Preset::Minimal),
            StoreParameters::for_preset(Preset::Minimal),
        )
        .unwrap()
    }

    #[cfg(feature = "spec-tests")]
    #[test]
    fn artificial_body_vote_rejections_do_not_hide_infrastructure_failures() {
        assert!(matches!(
            Store::body_vote_result::<false>(Err(StoreError::UnknownRoot)),
            Err(StoreError::UnknownRoot)
        ));
        assert!(Store::body_vote_result::<true>(Err(StoreError::UnknownRoot)).is_ok());
        assert!(Store::body_vote_result::<true>(Err(StoreError::InvalidAttestationTarget)).is_ok());
        for error in [
            StoreError::Overflow,
            StoreError::ResourceLimit,
            StoreError::InvalidParameters,
            StoreError::ValidationUnavailable(Fork::Altair),
            StoreError::Kernel(ForkChoiceError::Overflow),
            StoreError::Processing(ProcessingError::Arithmetic("bounded failure")),
        ] {
            assert!(Store::body_vote_result::<true>(Err(error)).is_err());
        }
    }

    #[test]
    fn published_later_anchors_retain_unsigned_identity_and_clock() {
        let base =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("testdata/official-anchors");
        for (name, fork) in [
            ("altair", Fork::Altair),
            ("bellatrix", Fork::Bellatrix),
            ("capella", Fork::Capella),
            ("deneb", Fork::Deneb),
            ("electra", Fork::Electra),
            ("fulu", Fork::Fulu),
            ("gloas", Fork::Gloas),
        ] {
            let state = BeaconState::from_ssz(
                &std::fs::read(base.join(name).join("anchor_state.ssz")).unwrap(),
                fork,
                Preset::Minimal,
            )
            .unwrap();
            let block = BeaconBlock::from_ssz(
                &std::fs::read(base.join(name).join("anchor_block.ssz")).unwrap(),
                fork,
                Preset::Minimal,
            )
            .unwrap();
            let root = block.hash_tree_root().unwrap();
            let mut store =
                Store::get_store(state, block, StoreParameters::for_preset(Preset::Minimal))
                    .unwrap();
            assert_eq!(
                store.get_head().unwrap(),
                ForkChoiceNode {
                    root,
                    payload_status: (fork == Fork::Gloas).then_some(PayloadStatus::Empty)
                }
            );
            assert_eq!(
                store.checkpoints(),
                (Checkpoint { epoch: 0, root }, Checkpoint { epoch: 0, root })
            );
            store.on_tick(store.time_ms() + 6000).unwrap();
            assert_eq!(store.current_slot(), 1);
            assert_eq!(store.get_head().unwrap().root, root);
            assert_eq!(store.proposer_boost_root(), [0; 32]);
        }
    }

    #[test]
    fn genuine_phase0_blocks_and_attestations_enter_only_through_production_validation() {
        let (state, first, second) = published_phase0();
        let mut store = phase0_store(state);
        assert_eq!(first.message.parent_root, store.get_head().unwrap().root);
        let first_root = first.message.hash_tree_root(Preset::Minimal).unwrap();
        store.on_tick(store.genesis_time_ms + first.message.slot * 6000).unwrap();
        store.on_block(&SignedBeaconBlock::Phase0(first.clone(), Preset::Minimal)).unwrap();
        assert_eq!(store.get_head().unwrap().root, first_root);
        assert_eq!(store.proposer_boost_root(), first_root);
        store.on_tick(store.genesis_time_ms + second.message.slot * 6000).unwrap();
        store.on_block(&SignedBeaconBlock::Phase0(second.clone(), Preset::Minimal)).unwrap();
        let second_root = second.message.hash_tree_root(Preset::Minimal).unwrap();
        assert_eq!(store.get_head().unwrap().root, second_root);
        let expected = phase0::BeaconState::from_ssz(
            &std::fs::read(
                std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("testdata/official-phase0-import/post.ssz"),
            )
            .unwrap(),
            Preset::Minimal,
        )
        .unwrap();
        assert_eq!(
            store.block_state(second_root),
            Some(&BeaconState::Phase0(expected, Preset::Minimal))
        );
        let attestations = first
            .message
            .body
            .attestations
            .values
            .iter()
            .chain(&second.message.body.attestations.values)
            .collect::<Vec<_>>();
        assert!(!attestations.is_empty());
        store.on_tick(store.time_ms() + 6000).unwrap();
        for attestation in attestations {
            store.on_attestation(attestation).unwrap();
        }
        assert!(!store.kernel.latest_messages().is_empty());
    }

    #[test]
    fn genuine_checkpoint_child_without_pre_seed_dependency_has_no_optional_boost() {
        let (state, first, second) = published_phase0();
        let mut full = phase0_store(state);
        let first_root = first.message.hash_tree_root(Preset::Minimal).unwrap();
        full.on_tick(first.message.slot * 6000).unwrap();
        full.on_block(&SignedBeaconBlock::Phase0(first.clone(), Preset::Minimal)).unwrap();
        // The complete retained genesis ancestry grants ordinary normative proposer boost.
        assert_eq!(full.proposer_boost_root(), first_root);
        let parameters = TransitionParameters::for_preset(Preset::Minimal);
        let post = full.block_state(first_root).unwrap().clone();
        let boundary = fork_processing::replay_slots(&post, 16, &parameters, 8).unwrap();
        let mut checkpoint = Store::get_store_at_checkpoint(
            post,
            BeaconBlock::Phase0(first.message, Preset::Minimal),
            Checkpoint { epoch: 2, root: first_root },
            &boundary.to_ssz().unwrap(),
            StoreParameters::for_preset(Preset::Minimal),
            parameters,
        )
        .unwrap();
        let dependency = 7;
        assert_eq!(checkpoint.retained_proposer_dependency(first_root, dependency).unwrap(), None);
        assert!(matches!(
            checkpoint.retained_proposer_dependency([255; 32], dependency),
            Err(StoreError::Kernel(ForkChoiceError::UnknownRoot))
        ));
        checkpoint.on_tick(second.message.slot * 6000).unwrap();
        let before = checkpoint.snapshot().unwrap();
        let mut invalid = second.clone();
        invalid.signature = [0; 96];
        assert!(matches!(
            checkpoint.on_block(&SignedBeaconBlock::Phase0(invalid, Preset::Minimal)),
            Err(StoreError::Processing(_))
        ));
        assert_eq!(checkpoint.snapshot().unwrap(), before);
        let second_root = second.message.hash_tree_root(Preset::Minimal).unwrap();
        checkpoint.on_block(&SignedBeaconBlock::Phase0(second, Preset::Minimal)).unwrap();
        assert_eq!(checkpoint.get_head().unwrap().root, second_root);
        assert_eq!(checkpoint.proposer_boost_root(), [0; 32]);
        let expected = phase0::BeaconState::from_ssz(
            &std::fs::read(
                std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("testdata/official-phase0-import/post.ssz"),
            )
            .unwrap(),
            Preset::Minimal,
        )
        .unwrap();
        assert_eq!(
            checkpoint.block_state(second_root),
            Some(&BeaconState::Phase0(expected, Preset::Minimal))
        );
    }

    #[test]
    fn genuine_altair_basic_fork_choice_steps_use_mandatory_validation() {
        let base = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("testdata/official-altair-import");
        let preset = Preset::Minimal;
        let state = BeaconState::from_ssz(
            &std::fs::read(base.join("anchor_state.ssz")).unwrap(),
            Fork::Altair,
            preset,
        )
        .unwrap();
        let block = BeaconBlock::from_ssz(
            &std::fs::read(base.join("anchor_block.ssz")).unwrap(),
            Fork::Altair,
            preset,
        )
        .unwrap();
        let anchor = block.hash_tree_root().unwrap();
        let mut store =
            Store::get_store(state, block, StoreParameters::for_preset(preset)).unwrap();
        assert_eq!(store.get_head().unwrap().root, anchor);
        assert_eq!(
            store.checkpoints(),
            (Checkpoint { epoch: 0, root: anchor }, Checkpoint { epoch: 0, root: anchor })
        );
        for (time, slot, root_text) in [
            (6_000, 1, "d8073ffdd11c559cb3d12141c5a5253b92dcf6d22deb5316704dba72313e1380"),
            (54_000, 9, "f01d17ac7ecfd1a489b991cf344c66339dfeba528189696ce98249178ce2c751"),
        ] {
            store.on_tick(time).unwrap();
            assert_eq!(store.proposer_boost_root(), [0; 32]);
            let signed = SignedBeaconBlock::from_ssz(
                &std::fs::read(base.join(format!("block_0x{root_text}.ssz"))).unwrap(),
                Fork::Altair,
                preset,
            )
            .unwrap();
            let expected =
                reth_embedded_beacon_primitives::root_from_hex(&format!("0x{root_text}")).unwrap();
            assert_eq!(signed.message_root().unwrap(), expected);
            let before = store.get_head().unwrap();
            let mut invalid = signed.clone();
            let SignedBeaconBlock::Altair(value, _) = &mut invalid else {
                panic!("Altair fixture")
            };
            value.signature = [0; 96];
            assert!(matches!(store.on_block(&invalid), Err(StoreError::Processing(_))));
            assert_eq!(store.get_head().unwrap(), before);
            assert!(store.block_state(expected).is_none());
            store.on_block(&signed).unwrap();
            assert_eq!(store.get_head().unwrap().root, expected);
            assert_eq!(store.proposer_boost_root(), expected);
            assert_eq!(store.block_state(expected).unwrap().slot(), slot);
            assert_eq!(
                store.checkpoints(),
                (Checkpoint { epoch: 0, root: anchor }, Checkpoint { epoch: 0, root: anchor })
            );
        }
    }

    #[test]
    fn actual_retention_budget_counts_shared_allocations_once_and_rejects_atomically() {
        let (state, first, _) = published_phase0();
        let mut store = phase0_store(state);
        let snapshot = store.snapshot().unwrap();
        let state = store.block_state_arc(snapshot.head.root).unwrap();
        let block = store.block_arc(snapshot.head.root).unwrap();
        let expected = state.heap_bytes().unwrap() +
            std::mem::size_of::<BeaconState>() +
            block.heap_bytes().unwrap() +
            std::mem::size_of::<BeaconBlock>() +
            4 * std::mem::size_of::<usize>();
        assert_eq!(store.retained_schema_bytes().unwrap(), expected);
        let checkpoint = store.checkpoint_state(snapshot.justified_checkpoint).unwrap();
        assert!(Arc::ptr_eq(&state, &checkpoint));
        assert_eq!(store.retained_schema_bytes().unwrap(), expected);
        store.parameters.max_retained_schema_bytes = expected;
        store.on_tick(first.message.slot * 6000).unwrap();
        let before = store.snapshot().unwrap();
        let signed = SignedBeaconBlock::Phase0(first, Preset::Minimal);
        let root = signed.message_root().unwrap();
        assert!(matches!(store.on_block_and_contents(&signed), Err(StoreError::ResourceLimit)));
        assert_eq!(store.snapshot().unwrap(), before);
        assert_eq!(store.retained_schema_bytes().unwrap(), expected);
        assert!(store.block_arc(root).is_none());
        assert!(store.block_state_arc(root).is_none());
    }

    #[test]
    fn snapshots_share_validated_allocations_and_bound_finalized_history() {
        let (state, first, _) = published_phase0();
        let mut store = phase0_store(state);
        let anchor = store.snapshot().unwrap();
        assert_eq!(anchor.head_slot, 0);
        assert_eq!(
            store.canonical_finalized_epoch_roots(0, 1).unwrap(),
            vec![(0, anchor.head.root)]
        );
        assert!(matches!(
            store.canonical_finalized_epoch_roots(1, 1),
            Err(StoreError::UnavailableHistory)
        ));
        assert!(matches!(
            store.canonical_finalized_epoch_roots(0, 1025),
            Err(StoreError::ResourceLimit)
        ));
        let state_a = store.block_state_arc(anchor.head.root).unwrap();
        let state_b = store.block_state_arc(anchor.head.root).unwrap();
        assert!(Arc::ptr_eq(&state_a, &state_b));
        let block_a = store.block_arc(anchor.head.root).unwrap();
        let block_b = store.block_arc(anchor.head.root).unwrap();
        assert!(Arc::ptr_eq(&block_a, &block_b));
        store.on_tick(first.message.slot * 6000).unwrap();
        store
            .on_block_and_contents(&SignedBeaconBlock::Phase0(first.clone(), Preset::Minimal))
            .unwrap();
        let imported = store.snapshot().unwrap();
        assert_eq!(imported.head_slot, first.message.slot);
        assert_eq!(imported.head.root, first.message.hash_tree_root(Preset::Minimal).unwrap());
        assert_eq!(anchor.head_slot, 0);
        assert_eq!(state_a.slot(), 0);
        assert_eq!(block_a.hash_tree_root().unwrap(), anchor.head.root);
        assert!(store.block_state_arc([0xff; 32]).is_none());
        assert!(store.block_arc([0xff; 32]).is_none());
    }

    #[test]
    fn invalid_signature_and_post_state_cannot_change_store() {
        let (state, first, _) = published_phase0();
        let mut store = phase0_store(state);
        store.on_tick(store.genesis_time_ms + first.message.slot * 6000).unwrap();
        let original = store.get_head().unwrap();
        let checkpoints = store.checkpoints();
        let mut invalid = first.clone();
        invalid.signature[0] ^= 1;
        assert!(matches!(
            store.on_block(&SignedBeaconBlock::Phase0(invalid, Preset::Minimal)),
            Err(StoreError::Processing(_))
        ));
        let mut invalid = first;
        invalid.message.state_root[0] ^= 1;
        assert!(matches!(
            store.on_block(&SignedBeaconBlock::Phase0(invalid, Preset::Minimal)),
            Err(StoreError::Processing(_))
        ));
        assert_eq!(store.get_head().unwrap(), original);
        assert_eq!(store.checkpoints(), checkpoints);
        assert_eq!(store.blocks.len(), 1);
        assert_eq!(store.proposer_boost_root(), [0; 32]);
    }

    #[cfg(feature = "spec-tests")]
    #[test]
    fn artificial_signature_policy_preserves_structural_checks_and_runtime_rejection() {
        let (state, _, _) = published_phase0();
        let parameters = Phase0Parameters::for_preset(Preset::Minimal);
        let mut vote = phase0::IndexedAttestation {
            attesting_indices: List::from_vec(vec![0]),
            data: phase0::AttestationData {
                slot: 0,
                index: 0,
                beacon_block_root: [0; 32],
                source: Checkpoint { epoch: 0, root: [0; 32] },
                target: Checkpoint { epoch: 0, root: [0; 32] },
            },
            signature: [0; 96],
        };
        assert!(validate_indexed!(true, phase0_processing, &state, &vote, &parameters).is_err());
        validate_indexed!(false, phase0_processing, &state, &vote, &parameters).unwrap();
        vote.attesting_indices.values = vec![0, 0];
        assert!(validate_indexed!(false, phase0_processing, &state, &vote, &parameters).is_err());
        vote.attesting_indices.values.clear();
        assert!(validate_indexed!(false, phase0_processing, &state, &vote, &parameters).is_err());
        vote.attesting_indices.values = vec![state.validators.values.len() as u64];
        assert!(validate_indexed!(false, phase0_processing, &state, &vote, &parameters).is_err());
    }

    #[cfg(feature = "spec-tests")]
    #[test]
    fn fixture_observation_cannot_create_a_post_state_or_head_node() {
        let (state, first, _) = published_phase0();
        let store = phase0_store(state);
        let expected_head = store.get_head().unwrap();
        let anchor = expected_head.root;
        let mut fixture = spec_only::SpecStore::get_store(
            store.block_state(anchor).unwrap().clone(),
            store.block(anchor).unwrap().clone(),
            store.parameters,
        )
        .unwrap();
        let block = BeaconBlock::Phase0(first.message, Preset::Minimal);
        let root = block.hash_tree_root().unwrap();
        fixture.fixture_observe_block_without_state(block).unwrap();
        assert!(fixture.block(root).is_some());
        assert!(fixture.block_state(root).is_none());
        assert_eq!(fixture.get_head().unwrap(), expected_head);
        assert!(!std::hint::black_box(
            <spec_only::SpecStore as crate::gossip_view::GossipView>::VERIFY_SIGNATURES
        ));
        assert!(std::hint::black_box(<Store as crate::gossip_view::GossipView>::VERIFY_SIGNATURES));
    }

    #[test]
    fn clock_regressions_resource_bombs_and_anchor_mismatches_are_atomic() {
        let (state, _, _) = published_phase0();
        let mut store = phase0_store(state);
        let original = store.time_ms();
        assert!(matches!(store.on_tick(original + 6000 * 8193), Err(StoreError::ResourceLimit)));
        assert_eq!(store.time_ms(), original);
        store.on_tick(original + 1).unwrap();
        assert!(matches!(store.on_tick(original), Err(StoreError::TimeRegression)));
        assert_eq!(store.time_ms(), original + 1);
        let root = store.get_head().unwrap().root;
        let mut block = store.blocks[&root].as_ref().clone();
        let BeaconBlock::Phase0(value, _) = &mut block else { panic!("phase0 fixture") };
        value.state_root[0] ^= 1;
        assert!(matches!(
            Store::get_store(
                store.block_states[&root].hot_required().unwrap().as_ref().clone(),
                block,
                store.parameters
            ),
            Err(StoreError::AnchorMismatch)
        ));
    }
}

mod checkpoint_anchor;

mod original_slashing;
