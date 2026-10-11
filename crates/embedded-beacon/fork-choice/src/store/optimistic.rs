//! Opt-in execution optimism over a private, fully consensus-verified block graph.
//!
//! Deferred execution never creates a validated Store, runtime graph view or Arc escape.
//! Every payload decision is obtained through the configured typed engine callback; block
//! signatures, consensus transitions, post-state roots and required data availability remain
//! mandatory. This module covers payload-bearing Bellatrix through Fulu blocks. Gloas uses
//! its separately authenticated envelope and payload-branch graph.

use super::{block_preset, timing, BlockImportTimings, Store, StoreError, StoreParameters};
use crate::{data_availability::DataAvailability, ExecutionStatus, ForkChoice};
use reth_embedded_beacon_primitives::{Fork, Root};
use reth_embedded_beacon_state_transition::{
    bellatrix_processing, capella_processing, deneb_processing, electra_processing,
    fork_processing::TransitionParameters,
    fulu_processing, gloas_processing,
    ssz::{
        bellatrix, capella, deneb, electra, fulu, gloas, BeaconBlock, BeaconState,
        SignedBeaconBlock, Ssz,
    },
};
use std::{
    collections::BTreeMap,
    future::Future,
    mem::size_of,
    sync::{Arc, Mutex},
    time::Instant,
};
use thiserror::Error;

/// An opt-in follower graph whose consensus data is not extractable as execution authority.
pub struct OptimisticStore<E> {
    engine: Arc<E>,
    consensus: Store,
    statuses: BTreeMap<Root, StatusRecord>,
    parameters: OptimisticParameters,
    halted: bool,
}

impl<E> Clone for OptimisticStore<E> {
    fn clone(&self) -> Self {
        Self {
            engine: self.engine.clone(),
            consensus: self.consensus.clone(),
            statuses: self.statuses.clone(),
            parameters: self.parameters,
            halted: self.halted,
        }
    }
}
impl<E> std::fmt::Debug for OptimisticStore<E> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OptimisticStore")
            .field("retained_status_count", &self.statuses.len())
            .field("parameters", &self.parameters)
            .field("halted", &self.halted)
            .finish_non_exhaustive()
    }
}

/// Configuration independent from normal strict block import.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OptimisticParameters {
    /// Minimum age of a first Bellatrix execution payload when its parent has execution disabled.
    pub safe_slots_to_import_optimistically: u64,
    /// Maximum owned status/context allocation bytes in addition to the Store schema budget.
    pub max_status_bytes: usize,
}

impl Default for OptimisticParameters {
    fn default() -> Self {
        Self { safe_slots_to_import_optimistically: 128, max_status_bytes: 16 * 1024 * 1024 }
    }
}

/// Engine status for the exact submitted execution payload.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PayloadStatus {
    /// Exact payload and its execution ancestry are validated.
    Valid,
    /// The engine is still synchronizing the required execution ancestry.
    Syncing,
    /// The engine accepted the request without finishing execution validation.
    Accepted,
    /// The payload or its execution ancestry was rejected.
    Invalid,
    /// The execution block hash itself was rejected.
    InvalidBlockHash,
}

/// A structured response obtained from the typed engine, never a caller-supplied event flag.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PayloadResponse {
    /// Exact engine validation status.
    pub status: PayloadStatus,
    /// Hash identifying the latest valid execution ancestor, if supplied.
    pub latest_valid_hash: Option<Root>,
    /// Optional bounded engine diagnostic; it cannot supply validation authority.
    pub validation_error: Option<String>,
}

impl PayloadResponse {
    fn validate(&self, hash: Root) -> Result<(), OptimisticError> {
        if self.validation_error.as_ref().is_some_and(|error| error.len() > 1024) {
            return Err(OptimisticError::MalformedResponse)
        }
        match self.status {
            PayloadStatus::Valid
                if self.latest_valid_hash != Some(hash) || self.validation_error.is_some() =>
            {
                Err(OptimisticError::MalformedResponse)
            }
            PayloadStatus::Syncing | PayloadStatus::Accepted | PayloadStatus::InvalidBlockHash
                if self.latest_valid_hash.is_some() =>
            {
                Err(OptimisticError::MalformedResponse)
            }
            _ => Ok(()),
        }
    }

    const fn execution_status(&self) -> ExecutionStatus {
        match self.status {
            PayloadStatus::Valid => ExecutionStatus::Valid,
            PayloadStatus::Syncing | PayloadStatus::Accepted => ExecutionStatus::Optimistic,
            PayloadStatus::Invalid | PayloadStatus::InvalidBlockHash => ExecutionStatus::Invalid,
        }
    }
}

/// Result of the mandatory first-payload terminal verification.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TerminalResponse {
    /// The activated override or authenticated terminal block/parent difficulty proof passed.
    Verified,
    /// Both required `PoW` ancestry blocks are unavailable. All configured override and activation
    /// assertions have still passed; invalid, malformed or mismatched proofs must be errors.
    UnknownPowAncestry,
    /// Authenticated terminal/override evidence deterministically failed the required assertions.
    Rejected,
}

/// A complete immutable payload in its exact fork schema.
#[derive(Clone, Copy, Debug)]
pub enum Payload<'a> {
    /// Version-one payload.
    Bellatrix(&'a bellatrix::ExecutionPayload),
    /// Version-two payload with withdrawals.
    Capella(&'a capella::ExecutionPayload),
    /// Version-three payload with blob gas fields.
    Deneb(&'a deneb::ExecutionPayload),
    /// Version-four payload with external execution requests.
    Electra(&'a electra::ExecutionPayload),
    /// Version-four payload under the checked blob schedule.
    Fulu(&'a fulu::ExecutionPayload),
}

impl Payload<'_> {
    /// Actual execution block hash committed by the consensus object.
    pub const fn block_hash(self) -> Root {
        match self {
            Self::Bellatrix(value) => value.block_hash,
            Self::Capella(value) => value.block_hash,
            Self::Deneb(value) => value.block_hash,
            Self::Electra(value) => value.block_hash,
            Self::Fulu(value) => value.block_hash,
        }
    }

    fn is_empty(
        self,
        preset: reth_embedded_beacon_state_transition::ssz::Preset,
    ) -> Result<bool, OptimisticError> {
        let result = match self {
            Self::Bellatrix(value) => {
                *value ==
                    bellatrix::ExecutionPayload::default_value(preset)
                        .map_err(StoreError::from)?
            }
            Self::Capella(value) => {
                *value ==
                    capella::ExecutionPayload::default_value(preset).map_err(StoreError::from)?
            }
            Self::Deneb(value) => {
                *value ==
                    deneb::ExecutionPayload::default_value(preset).map_err(StoreError::from)?
            }
            Self::Electra(value) => {
                *value ==
                    electra::ExecutionPayload::default_value(preset).map_err(StoreError::from)?
            }
            Self::Fulu(value) => {
                *value == fulu::ExecutionPayload::default_value(preset).map_err(StoreError::from)?
            }
        };
        Ok(result)
    }
}

/// Context derived during the real typed consensus transition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PayloadContext {
    /// Bellatrix ancestry and slot.
    Bellatrix(bellatrix_processing::ExecutionContext),
    /// Capella ancestry and slot.
    Capella(bellatrix_processing::ExecutionContext),
    /// Deneb ancestry, parent beacon root and ordered blob hashes.
    Deneb(deneb_processing::ExecutionContext),
    /// Electra ancestry, ordered blob hashes and canonical execution requests.
    Electra(electra_processing::ExecutionContext),
    /// Fulu ancestry, ordered blob hashes and canonical execution requests.
    Fulu(fulu_processing::ExecutionContext),
}

impl PayloadContext {
    fn heap_bytes(&self) -> Result<usize, OptimisticError> {
        let hashes = match self {
            Self::Bellatrix(_) | Self::Capella(_) => 0,
            Self::Deneb(value) => value.expected_blob_versioned_hashes.capacity(),
            Self::Electra(value) => value.expected_blob_versioned_hashes.capacity(),
            Self::Fulu(value) => value.expected_blob_versioned_hashes.capacity(),
        };
        let requests = match self {
            Self::Electra(value) => Some(&value.execution_requests_list),
            Self::Fulu(value) => Some(&value.execution_requests_list),
            _ => None,
        };
        let mut bytes =
            hashes.checked_mul(size_of::<Root>()).ok_or(OptimisticError::ResourceLimit)?;
        if let Some(requests) = requests {
            bytes = bytes
                .checked_add(
                    requests
                        .capacity()
                        .checked_mul(size_of::<Vec<u8>>())
                        .ok_or(OptimisticError::ResourceLimit)?,
                )
                .ok_or(OptimisticError::ResourceLimit)?;
            for request in requests {
                bytes =
                    bytes.checked_add(request.capacity()).ok_or(OptimisticError::ResourceLimit)?;
            }
        }
        Ok(bytes)
    }
}

/// Mandatory transport contract for exact candidate submission and terminal ancestry checks.
///
/// Implementations must enforce real engine request version, computed block hash and complete
/// context identity. Transport errors and malformed responses cannot be represented as SYNCING.
/// This interface enables deferred execution only inside `OptimisticStore`; it does not satisfy
/// the strict transition engine contracts used by normal Store or execution publication.
pub trait ExecutionEngine: Send + Sync {
    /// Typed transport, invalid terminal proof or request/response identity failure.
    type Error: std::error::Error + Send + Sync + 'static;
    /// Check configured terminal eligibility; unknown `PoW` ancestry is the only deferred result.
    fn verify_terminal_block<'a>(
        &'a self,
        payload: Payload<'a>,
        context: PayloadContext,
    ) -> impl Future<Output = Result<TerminalResponse, Self::Error>> + Send + 'a;
    /// Submit the exact immutable payload/context and return its real structured status.
    fn notify_new_payload<'a>(
        &'a self,
        payload: Payload<'a>,
        context: PayloadContext,
    ) -> impl Future<Output = Result<PayloadResponse, Self::Error>> + Send + 'a;
}

/// Canonical follower head; consumers must preserve its explicit execution status.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OptimisticHead {
    /// Actual unsigned beacon block root.
    pub root: Root,
    /// Actual retained block slot.
    pub slot: u64,
    /// Actual execution block hash, absent before execution is enabled.
    pub execution_block_hash: Option<Root>,
    /// True while payload validity is deferred; it never grants execution authority.
    pub execution_optimistic: bool,
}

/// Outcome after the complete consensus validation and real engine callback.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ImportOutcome {
    /// The consensus-valid branch is retained with the explicit execution status.
    Accepted {
        /// Execution validation remains deferred.
        execution_optimistic: bool,
    },
    /// Consensus checks passed but the engine rejected the candidate or its ancestry.
    ExecutionRejected,
}

#[derive(Clone, Debug)]
struct StatusRecord {
    status: ExecutionStatus,
    context: Option<PayloadContext>,
    terminal_unverified: bool,
}

impl<E: ExecutionEngine> OptimisticStore<E> {
    /// Start from a caller-trusted, execution-valid anchor with full state/block root linkage.
    /// There is no conversion from this wrapper into a validated Store or graph view.
    pub fn get_store(
        state: BeaconState,
        block: BeaconBlock,
        store_parameters: StoreParameters,
        transitions: TransitionParameters,
        parameters: OptimisticParameters,
        engine: Arc<E>,
    ) -> Result<Self, OptimisticError> {
        if parameters.max_status_bytes == 0 || state.schema_fork() == Fork::Gloas {
            return Err(OptimisticError::InvalidParameters)
        }
        let mut consensus = Store::get_store(state, block, store_parameters)?;
        consensus.set_transition_parameters(transitions)?;
        let root = consensus.get_head()?.root;
        let result = Self {
            engine,
            consensus,
            statuses: BTreeMap::from([(
                root,
                StatusRecord {
                    status: ExecutionStatus::Valid,
                    context: None,
                    terminal_unverified: false,
                },
            )]),
            parameters,
            halted: false,
        };
        result.check_status_budget()?;
        Ok(result)
    }

    /// Advance the checked clock without publishing an optimistic checkpoint as authority.
    pub fn on_tick(&mut self, time_ms: u64) -> Result<(), OptimisticError> {
        self.require_healthy()?;
        let mut candidate = self.clone();
        candidate.consensus.on_tick(time_ms)?;
        if let Err(error) = candidate.require_checkpoints_not_invalid() {
            self.halted = true;
            return Err(error);
        }
        *self = candidate;
        Ok(())
    }

    /// Validate complete signatures, transition and post-state root before retaining any branch.
    /// The mandatory availability callback is unchanged for data-bearing forks.
    pub async fn on_block<D: DataAvailability>(
        &mut self,
        signed: &SignedBeaconBlock,
        availability: &D,
    ) -> Result<ImportOutcome, OptimisticError> {
        self.on_block_inner(signed, availability, true, &mut BlockImportTimings::default()).await
    }

    pub(super) async fn on_block_consensus_only_timed<D: DataAvailability>(
        &mut self,
        signed: &SignedBeaconBlock,
        availability: &D,
    ) -> Result<(ImportOutcome, BlockImportTimings), OptimisticError> {
        let mut timings = BlockImportTimings::default();
        let outcome = self.on_block_inner(signed, availability, false, &mut timings).await?;
        Ok((outcome, timings))
    }

    async fn on_block_inner<D: DataAvailability>(
        &mut self,
        signed: &SignedBeaconBlock,
        availability: &D,
        contents: bool,
        timings: &mut BlockImportTimings,
    ) -> Result<ImportOutcome, OptimisticError> {
        self.require_healthy()?;
        if signed.schema_fork() == Fork::Gloas {
            return Err(OptimisticError::UnsupportedFork(Fork::Gloas))
        }
        let root = signed.message_root().map_err(StoreError::from)?;
        if let Some(record) = self.statuses.get(&root) {
            return match record.status {
                ExecutionStatus::Invalid => Ok(ImportOutcome::ExecutionRejected),
                status => Ok(ImportOutcome::Accepted {
                    execution_optimistic: status == ExecutionStatus::Optimistic,
                }),
            }
        }
        let header = signed.header().map_err(StoreError::from)?;
        if self
            .statuses
            .get(&header.parent_root)
            .is_none_or(|status| status.status == ExecutionStatus::Invalid)
        {
            return Err(OptimisticError::InvalidParent)
        }
        let parent =
            self.consensus.blocks.get(&header.parent_root).ok_or(StoreError::UnknownRoot)?;
        let parent_execution_enabled = payload(parent)
            .map(|value| value.is_empty(block_preset(parent)).map(|empty| !empty))
            .transpose()?
            .unwrap_or(false);
        let eligible = signed.schema_fork() >= Fork::Capella ||
            parent_execution_enabled ||
            header
                .slot
                .checked_add(self.parameters.safe_slots_to_import_optimistically)
                .ok_or(OptimisticError::Overflow)? <=
                self.consensus.current_slot();
        let engine = self.engine.clone();
        let adapter = TransitionAdapter::new(engine.as_ref(), eligible);
        let mut candidate = self.clone();
        if contents {
            candidate
                .consensus
                .on_block_and_contents_with_engine(signed, &adapter, availability)
                .await?;
        } else {
            // The owner admits independently eligible FC observations separately. Full block
            // operation signatures and state-root validation remain mandatory in this transition.
            *timings = candidate
                .consensus
                .on_block_with_engine_timed(signed, &adapter, availability)
                .await?;
        }
        let status_started = Instant::now();
        let observation =
            adapter.observation.into_inner().map_err(|_| OptimisticError::Poisoned)?;
        timings.execution_wait_micros = observation.execution_wait_micros;
        let (response, context, terminal_unverified) =
            if let Some((response, context)) = observation.payload {
                (
                    response,
                    Some(context),
                    observation.terminal == Some(TerminalResponse::UnknownPowAncestry),
                )
            } else {
                (
                    PayloadResponse {
                        status: PayloadStatus::Valid,
                        latest_valid_hash: None,
                        validation_error: None,
                    },
                    None,
                    false,
                )
            };
        candidate.statuses.insert(
            root,
            StatusRecord {
                status: if response.status == PayloadStatus::Valid && context.is_some() {
                    ExecutionStatus::Optimistic
                } else {
                    response.execution_status()
                },
                context,
                terminal_unverified,
            },
        );
        let outcome = match response.status {
            PayloadStatus::Valid => {
                if let Err(error) = candidate.apply_valid_feedback(root).await {
                    if matches!(
                        error,
                        OptimisticError::EngineContradiction |
                            OptimisticError::InvalidatedCheckpoint
                    ) {
                        self.halted = true;
                    }
                    return Err(error);
                }
                if candidate.statuses[&root].status == ExecutionStatus::Invalid {
                    ImportOutcome::ExecutionRejected
                } else {
                    ImportOutcome::Accepted { execution_optimistic: false }
                }
            }
            PayloadStatus::Syncing | PayloadStatus::Accepted => {
                if !eligible {
                    return Err(OptimisticError::NotOptimisticCandidate)
                }
                ImportOutcome::Accepted { execution_optimistic: true }
            }
            PayloadStatus::Invalid | PayloadStatus::InvalidBlockHash => {
                if let Err(error) = candidate.invalidate_ancestry(root, response.latest_valid_hash)
                {
                    if matches!(
                        error,
                        OptimisticError::EngineContradiction |
                            OptimisticError::InvalidatedCheckpoint
                    ) {
                        self.halted = true;
                    }
                    return Err(error)
                }
                ImportOutcome::ExecutionRejected
            }
        };
        candidate.check_status_budget()?;
        candidate.require_checkpoints_not_invalid()?;
        *self = candidate;
        timings.fork_choice_micros =
            timings.fork_choice_micros.saturating_add(timing::elapsed_micros(status_started));
        Ok(outcome)
    }

    /// Obtain subsequent status from the real typed engine and apply bounded ancestry feedback.
    /// No public method accepts an externally asserted engine status.
    pub async fn refresh_execution_status(&mut self, root: Root) -> Result<(), OptimisticError> {
        self.require_healthy()?;
        let record = self.statuses.get(&root).ok_or(StoreError::UnknownRoot)?;
        let context = record.context.clone().ok_or(OptimisticError::NoExecutionPayload)?;
        let block = self.consensus.blocks.get(&root).ok_or(StoreError::UnknownRoot)?;
        let payload = payload(block).ok_or(OptimisticError::NoExecutionPayload)?;
        let response =
            self.engine.notify_new_payload(payload, context).await.map_err(engine_error)?;
        response.validate(payload.block_hash())?;
        let status = response.execution_status();
        if (record.status == ExecutionStatus::Valid && status != ExecutionStatus::Valid) ||
            (record.status == ExecutionStatus::Invalid && status != ExecutionStatus::Invalid)
        {
            self.halted = true;
            return Err(OptimisticError::EngineContradiction)
        }
        let mut candidate = self.clone();
        match status {
            ExecutionStatus::Valid => {
                if let Err(error) = candidate.apply_valid_feedback(root).await {
                    if matches!(
                        error,
                        OptimisticError::EngineContradiction |
                            OptimisticError::InvalidatedCheckpoint
                    ) {
                        self.halted = true;
                    }
                    return Err(error);
                }
            }
            ExecutionStatus::Optimistic => {}
            ExecutionStatus::Invalid => {
                if let Err(error) = candidate.invalidate_ancestry(root, response.latest_valid_hash)
                {
                    if matches!(
                        error,
                        OptimisticError::EngineContradiction |
                            OptimisticError::InvalidatedCheckpoint
                    ) {
                        self.halted = true;
                    }
                    return Err(error)
                }
            }
        }
        candidate.require_checkpoints_not_invalid()?;
        *self = candidate;
        Ok(())
    }

    /// Read canonical identity with an explicit deferred-execution marker, without graph escape.
    pub fn head(&self) -> Result<OptimisticHead, OptimisticError> {
        self.require_healthy()?;
        let root = self.filtered_kernel()?.head().map_err(StoreError::from)?;
        let block = self.consensus.blocks.get(&root).ok_or(StoreError::UnknownRoot)?;
        let record = self.statuses.get(&root).ok_or(StoreError::UnknownRoot)?;
        Ok(OptimisticHead {
            root,
            slot: block.header().map_err(StoreError::from)?.slot,
            execution_block_hash: payload(block)
                .map(Payload::block_hash)
                .filter(|hash| *hash != [0; 32]),
            execution_optimistic: record.status == ExecutionStatus::Optimistic,
        })
    }

    /// Return publication identity only after the head and both checkpoints have real VALID.
    /// This method never exposes private consensus-valid states as a normal validated Store.
    pub fn execution_verified_head(&self) -> Result<OptimisticHead, OptimisticError> {
        let head = self.head()?;
        let (justified, finalized) = self.consensus.checkpoints();
        if head.execution_optimistic ||
            [justified.root, finalized.root].into_iter().any(|root| {
                self.statuses.get(&root).is_none_or(|record| {
                    record.status != ExecutionStatus::Valid || record.terminal_unverified
                })
            })
        {
            return Err(OptimisticError::ExecutionDeferred)
        }
        Ok(head)
    }

    /// Consensus-derived checkpoints remain observations until `execution_verified_head` succeeds.
    pub fn checkpoints(
        &self,
    ) -> Result<
        (reth_embedded_beacon_primitives::Checkpoint, reth_embedded_beacon_primitives::Checkpoint),
        OptimisticError,
    > {
        self.require_healthy()?;
        Ok(self.consensus.checkpoints())
    }

    /// Checked monotonic follower clock.
    pub const fn time_ms(&self) -> u64 {
        self.consensus.time_ms()
    }

    /// Proposer boost derived from consensus validation, excluding execution-invalid branches.
    pub fn proposer_boost_root(&self) -> Result<Root, OptimisticError> {
        self.require_healthy()?;
        Ok(self.filtered_kernel()?.proposer_boost.map_or([0; 32], |(root, _)| root))
    }

    /// Authenticate an actual wire vote; execution-invalid voted branches cannot acquire weight.
    pub fn on_attestation_wire(&mut self, bytes: &[u8], fork: Fork) -> Result<(), OptimisticError> {
        self.require_healthy()?;
        let preset = self.consensus.parameters.phase0.preset;
        let root = match fork {
            Fork::Phase0 | Fork::Altair | Fork::Bellatrix | Fork::Capella | Fork::Deneb => {
                reth_embedded_beacon_state_transition::ssz::phase0::Attestation::from_ssz(
                    bytes, preset,
                )
                .map_err(StoreError::from)?
                .data
                .beacon_block_root
            }
            Fork::Electra | Fork::Fulu => {
                electra::Attestation::from_ssz(bytes, preset)
                    .map_err(StoreError::from)?
                    .data
                    .beacon_block_root
            }
            Fork::Gloas => return Err(OptimisticError::UnsupportedFork(Fork::Gloas)),
        };
        if self.statuses.get(&root).is_none_or(|record| record.status == ExecutionStatus::Invalid) {
            return Err(OptimisticError::InvalidParent)
        }
        self.consensus.on_attestation_wire(bytes, fork)?;
        Ok(())
    }

    /// Independently authenticate real signed slashing evidence against the consensus checkpoint.
    pub fn on_attester_slashing_wire(
        &mut self,
        bytes: &[u8],
        fork: Fork,
    ) -> Result<(), OptimisticError> {
        self.require_healthy()?;
        self.consensus.on_attester_slashing_wire(bytes, fork)?;
        Ok(())
    }

    /// Explicit halt after an engine contradiction or invalidated trusted checkpoint.
    pub const fn is_halted(&self) -> bool {
        self.halted
    }

    const fn require_healthy(&self) -> Result<(), OptimisticError> {
        if self.halted {
            Err(OptimisticError::Halted)
        } else {
            Ok(())
        }
    }

    fn require_checkpoints_not_invalid(&self) -> Result<(), OptimisticError> {
        let (justified, finalized) = self.consensus.checkpoints();
        if [justified.root, finalized.root].into_iter().any(|root| {
            self.statuses.get(&root).is_none_or(|record| record.status == ExecutionStatus::Invalid)
        }) {
            return Err(OptimisticError::InvalidatedCheckpoint)
        }
        Ok(())
    }

    pub(super) fn filtered_kernel(&self) -> Result<ForkChoice, OptimisticError> {
        self.require_checkpoints_not_invalid()?;
        let mut graph = self.consensus.kernel.clone();
        for (root, record) in &mut graph.blocks {
            record.execution_status =
                self.statuses.get(root).ok_or(StoreError::UnknownRoot)?.status;
        }
        graph.latest_messages.retain(|_, vote| {
            graph
                .blocks
                .get(&vote.root)
                .is_some_and(|block| block.execution_status != ExecutionStatus::Invalid)
        });
        if graph.proposer_boost.is_some_and(|(root, _)| {
            graph
                .blocks
                .get(&root)
                .is_none_or(|block| block.execution_status == ExecutionStatus::Invalid)
        }) {
            graph.proposer_boost = None;
        }
        Ok(graph)
    }

    async fn apply_valid_feedback(&mut self, root: Root) -> Result<(), OptimisticError> {
        match self.promote_ancestors(root).await {
            Err(OptimisticError::InvalidTerminalBlock(merge_root)) => {
                self.invalidate_ancestry(merge_root, None)?;
                Ok(())
            }
            result => result,
        }
    }

    async fn promote_ancestors(&mut self, mut root: Root) -> Result<(), OptimisticError> {
        let engine = self.engine.clone();
        let mut chain = Vec::new();
        loop {
            if chain.len() >= self.statuses.len() {
                return Err(OptimisticError::InvalidParameters)
            }
            let record = self.statuses.get(&root).ok_or(StoreError::UnknownRoot)?;
            if record.status == ExecutionStatus::Invalid {
                return Err(OptimisticError::EngineContradiction)
            }
            let block = self.consensus.blocks.get(&root).ok_or(StoreError::UnknownRoot)?;
            if record.terminal_unverified {
                let context = record.context.clone().ok_or(OptimisticError::NoExecutionPayload)?;
                let payload = payload(block).ok_or(OptimisticError::NoExecutionPayload)?;
                match engine.verify_terminal_block(payload, context).await.map_err(engine_error)? {
                    TerminalResponse::Verified => {}
                    TerminalResponse::UnknownPowAncestry => {
                        return Err(OptimisticError::TerminalEvidenceUnavailable)
                    }
                    TerminalResponse::Rejected => {
                        return Err(OptimisticError::InvalidTerminalBlock(root))
                    }
                }
            }
            chain.push(root);
            let parent = block.header().map_err(StoreError::from)?.parent_root;
            if !self.statuses.contains_key(&parent) {
                break
            }
            root = parent;
        }
        // Verify every first-payload proof before changing any descendant to VALID.
        for root in chain {
            let record = self.statuses.get_mut(&root).ok_or(StoreError::UnknownRoot)?;
            record.status = ExecutionStatus::Valid;
            record.terminal_unverified = false;
        }
        Ok(())
    }

    fn invalidate_ancestry(
        &mut self,
        root: Root,
        latest: Option<Root>,
    ) -> Result<(), OptimisticError> {
        let mut branch = vec![root];
        let mut cursor = root;
        loop {
            let block = self.consensus.blocks.get(&cursor).ok_or(StoreError::UnknownRoot)?;
            let parent = block.header().map_err(StoreError::from)?.parent_root;
            if !self.statuses.contains_key(&parent) {
                break
            }
            branch.push(parent);
            cursor = parent;
        }
        let invalid_root = match latest {
            Some(hash) if hash != [0; 32] => branch
                .iter()
                .position(|root| {
                    self.consensus
                        .blocks
                        .get(root)
                        .and_then(|block| payload(block))
                        .is_some_and(|value| value.block_hash() == hash)
                })
                .map(|index| {
                    if index == 0 {
                        Err(OptimisticError::MalformedResponse)
                    } else {
                        Ok(branch[index - 1])
                    }
                })
                .transpose()?
                .unwrap_or(root),
            Some(_) => {
                let mut first = None;
                for root in branch.iter().rev() {
                    let block = self.consensus.blocks.get(root).ok_or(StoreError::UnknownRoot)?;
                    if let Some(value) = payload(block) &&
                        !value.is_empty(block_preset(block))?
                    {
                        first = Some(*root);
                        break;
                    }
                }
                first.ok_or(OptimisticError::NoExecutionPayload)?
            }
            None => root,
        };
        let mut invalid = Vec::new();
        for candidate in self.statuses.keys() {
            if self
                .consensus
                .kernel
                .is_ancestor(*candidate, invalid_root)
                .map_err(StoreError::from)?
            {
                invalid.push(*candidate);
            }
        }
        let (justified, finalized) = self.consensus.checkpoints();
        if invalid.contains(&justified.root) || invalid.contains(&finalized.root) {
            return Err(OptimisticError::InvalidatedCheckpoint)
        }
        if invalid.iter().any(|root| self.statuses[root].status == ExecutionStatus::Valid) {
            return Err(OptimisticError::EngineContradiction)
        }
        for root in invalid {
            self.statuses.get_mut(&root).ok_or(StoreError::UnknownRoot)?.status =
                ExecutionStatus::Invalid;
        }
        Ok(())
    }

    fn check_status_budget(&self) -> Result<(), OptimisticError> {
        if self.statuses.len() > self.consensus.parameters.max_blocks {
            return Err(OptimisticError::ResourceLimit)
        }
        let mut bytes = self
            .statuses
            .len()
            .checked_mul(size_of::<(Root, StatusRecord)>())
            .ok_or(OptimisticError::ResourceLimit)?;
        for record in self.statuses.values() {
            if let Some(context) = &record.context {
                bytes = bytes
                    .checked_add(context.heap_bytes()?)
                    .ok_or(OptimisticError::ResourceLimit)?;
            }
        }
        if bytes > self.parameters.max_status_bytes {
            return Err(OptimisticError::ResourceLimit)
        }
        Ok(())
    }
}

/// Private status/context ownership retained by the compact graph without retaining an engine.
#[derive(Clone, Debug)]
pub(crate) struct CompactOptimisticTracking {
    statuses: BTreeMap<Root, StatusRecord>,
    parameters: OptimisticParameters,
    halted: bool,
}
impl CompactOptimisticTracking {
    pub(super) fn trusted_anchor(
        consensus: &Store,
        parameters: OptimisticParameters,
    ) -> Result<Self, OptimisticError> {
        if parameters.max_status_bytes == 0 ||
            consensus.blocks.len() != 1 ||
            consensus.blocks.values().any(|block| block.schema_fork() == Fork::Gloas)
        {
            return Err(OptimisticError::InvalidParameters)
        }
        let statuses = consensus
            .kernel
            .blocks
            .iter()
            .map(|(&root, record)| {
                (
                    root,
                    StatusRecord {
                        status: record.execution_status,
                        context: None,
                        terminal_unverified: false,
                    },
                )
            })
            .collect();
        Ok(Self { statuses, parameters, halted: false })
    }
    pub(super) fn retain_roots(&mut self, consensus: &Store) {
        self.statuses.retain(|root, _| consensus.blocks.contains_key(root));
    }
    pub(super) fn all_valid(&self) -> bool {
        !self.halted &&
            self.statuses.values().all(|record| {
                record.status == ExecutionStatus::Valid && !record.terminal_unverified
            })
    }
}
impl<E: ExecutionEngine> OptimisticStore<E> {
    pub(super) fn from_compact(
        consensus: Store,
        tracking: CompactOptimisticTracking,
        engine: Arc<E>,
    ) -> Result<Self, OptimisticError> {
        let store = Self {
            engine,
            consensus,
            statuses: tracking.statuses,
            parameters: tracking.parameters,
            halted: tracking.halted,
        };
        store.require_healthy()?;
        store.require_checkpoints_not_invalid()?;
        store.check_status_budget()?;
        Ok(store)
    }
    pub(super) fn into_compact(
        mut self,
    ) -> Result<(Store, CompactOptimisticTracking), OptimisticError> {
        // The overlay becomes the private kernel before any compact view or durable seal exists.
        // Consequently gossip, confirmation and execution-hash getters cannot observe a hidden
        // VALID.
        self.consensus.kernel = self.filtered_kernel()?;
        let tracking = CompactOptimisticTracking {
            statuses: self.statuses,
            parameters: self.parameters,
            halted: self.halted,
        };
        Ok((self.consensus, tracking))
    }
}

const fn payload(block: &BeaconBlock) -> Option<Payload<'_>> {
    match block {
        BeaconBlock::Bellatrix(value, _) => Some(Payload::Bellatrix(&value.body.execution_payload)),
        BeaconBlock::Capella(value, _) => Some(Payload::Capella(&value.body.execution_payload)),
        BeaconBlock::Deneb(value, _) => Some(Payload::Deneb(&value.body.execution_payload)),
        BeaconBlock::Electra(value, _) => Some(Payload::Electra(&value.body.execution_payload)),
        BeaconBlock::Fulu(value, _) => Some(Payload::Fulu(&value.body.execution_payload)),
        _ => None,
    }
}

#[derive(Debug, Default)]
struct Observation {
    terminal: Option<TerminalResponse>,
    payload: Option<(PayloadResponse, PayloadContext)>,
    execution_wait_micros: Option<u64>,
}

struct TransitionAdapter<'a, E> {
    engine: &'a E,
    eligible: bool,
    observation: Mutex<Observation>,
}

impl<'a, E: ExecutionEngine> TransitionAdapter<'a, E> {
    fn new(engine: &'a E, eligible: bool) -> Self {
        Self { engine, eligible, observation: Mutex::default() }
    }
    async fn terminal(
        &self,
        payload: Payload<'_>,
        context: PayloadContext,
    ) -> Result<(), OptimisticError> {
        let result =
            self.engine.verify_terminal_block(payload, context).await.map_err(engine_error)?;
        if result == TerminalResponse::Rejected {
            return Err(OptimisticError::TerminalRejected)
        }
        if result == TerminalResponse::UnknownPowAncestry && !self.eligible {
            return Err(OptimisticError::NotOptimisticCandidate)
        }
        let mut observation = self.observation.lock().map_err(|_| OptimisticError::Poisoned)?;
        if observation.terminal.replace(result).is_some() {
            return Err(OptimisticError::MalformedResponse)
        }
        Ok(())
    }
    async fn submit(
        &self,
        payload: Payload<'_>,
        context: PayloadContext,
    ) -> Result<(), OptimisticError> {
        let started = Instant::now();
        let response =
            self.engine.notify_new_payload(payload, context.clone()).await.map_err(engine_error)?;
        let execution_wait_micros = timing::elapsed_micros(started);
        response.validate(payload.block_hash())?;
        if response.execution_status() == ExecutionStatus::Optimistic && !self.eligible {
            return Err(OptimisticError::NotOptimisticCandidate)
        }
        let mut observation = self.observation.lock().map_err(|_| OptimisticError::Poisoned)?;
        if response.status == PayloadStatus::Valid &&
            observation.terminal == Some(TerminalResponse::UnknownPowAncestry)
        {
            return Err(OptimisticError::TerminalEvidenceUnavailable)
        }
        if observation.payload.replace((response, context)).is_some() {
            return Err(OptimisticError::MalformedResponse)
        }
        observation.execution_wait_micros = Some(execution_wait_micros);
        // This private adapter finishes mandatory consensus validation only. Its enclosing
        // wrapper records the actual engine status before any externally visible decision.
        Ok(())
    }
}

macro_rules! transition_adapter {
    ($module:ident,$schema:ident,$variant:ident) => {
        impl<E: ExecutionEngine> $module::ExecutionEngine for TransitionAdapter<'_, E> {
            type Error = OptimisticError;
            async fn verify_terminal_block(
                &self,
                value: &$schema::ExecutionPayload,
                context: $module::ExecutionContext,
            ) -> Result<(), Self::Error> {
                self.terminal(Payload::$variant(value), PayloadContext::$variant(context)).await
            }
            async fn verify_payload(
                &self,
                value: &$schema::ExecutionPayload,
                context: $module::ExecutionContext,
            ) -> Result<(), Self::Error> {
                self.submit(Payload::$variant(value), PayloadContext::$variant(context)).await
            }
        }
    };
}
transition_adapter!(bellatrix_processing, bellatrix, Bellatrix);
transition_adapter!(capella_processing, capella, Capella);
transition_adapter!(deneb_processing, deneb, Deneb);
transition_adapter!(electra_processing, electra, Electra);
transition_adapter!(fulu_processing, fulu, Fulu);
impl<E: ExecutionEngine> gloas_processing::ExecutionEngine for TransitionAdapter<'_, E> {
    type Error = OptimisticError;
    async fn verify_payload(
        &self,
        _: &gloas::ExecutionPayload,
        _: gloas_processing::ExecutionContext,
    ) -> Result<(), Self::Error> {
        Err(OptimisticError::UnsupportedFork(Fork::Gloas))
    }
}

fn engine_error(error: impl std::fmt::Display) -> OptimisticError {
    OptimisticError::Engine(error.to_string().chars().take(1024).collect())
}

/// Execution optimism errors never satisfy expected normative fixture rejection automatically.
#[derive(Debug, Error)]
pub enum OptimisticError {
    /// Full consensus, fork-choice, data-availability or resource validation failed.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// Real engine callback failed; transport errors cannot become optimistic admission.
    #[error("execution callback failed: {0}")]
    Engine(String),
    /// Optional optimism configuration is invalid.
    #[error("invalid optimistic parameters")]
    InvalidParameters,
    /// This fork requires the independent Gloas envelope graph.
    #[error("optimistic payload wrapper does not support {0:?}")]
    UnsupportedFork(Fork),
    /// Structured response contradicts its exact submitted payload or required fields.
    #[error("malformed exact-candidate payload response")]
    MalformedResponse,
    /// First execution block is too recent and its parent has execution disabled.
    #[error("block is not an optimistic import candidate")]
    NotOptimisticCandidate,
    /// A retained parent is unknown or execution-invalid.
    #[error("unknown or execution-invalid parent")]
    InvalidParent,
    /// Engine feedback contradicted previously validated or invalidated execution.
    #[error("contradictory execution status requires manual recovery")]
    EngineContradiction,
    /// Invalidation touches trusted/current checkpoints and requires explicit recovery.
    #[error("execution invalidation touches a checkpoint")]
    InvalidatedCheckpoint,
    /// Normal execution publication still requires real VALID.
    #[error("execution authority remains deferred")]
    ExecutionDeferred,
    /// Actual terminal proof failed before first-payload consensus admission.
    #[error("terminal execution block was rejected")]
    TerminalRejected,
    /// Later terminal proof invalidates this already consensus-verified merge branch.
    #[error("terminal execution proof invalidates merge block {0:?}")]
    InvalidTerminalBlock(Root),
    /// Unknown terminal proof cannot be promoted to validated execution.
    #[error("terminal execution ancestry remains unavailable")]
    TerminalEvidenceUnavailable,
    /// Feedback query requires an actual retained execution payload.
    #[error("block has no execution payload request")]
    NoExecutionPayload,
    /// Context retention exceeded the explicit bound.
    #[error("optimistic status/context resource limit exceeded")]
    ResourceLimit,
    /// Arithmetic overflow is infrastructure failure.
    #[error("optimistic slot arithmetic overflow")]
    Overflow,
    /// Poisoned synchronization state cannot authorize import.
    #[error("optimistic callback synchronization failed")]
    Poisoned,
    /// No further head/publication/event decisions are allowed after a contradiction.
    #[error("optimistic store is halted pending manual recovery")]
    Halted,
}

#[cfg(test)]
mod tests;
