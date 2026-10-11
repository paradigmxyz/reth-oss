//! Bounded original-data reconstruction hints; no saved root is accepted as graph authority.
//! Every retained branch is replayed by the actual single-owner signed importer, including
//! original Gloas envelopes. The final graph and complete state must match the saved hint
//! before any canonical Engine call or head publication. Missing observations remain an error.

use super::{Graph, ImportError};
use crate::{
    compact_persistence::{AdmittedStateRepository, PersistedSnapshot},
    execution_checkpoint_boundary::ExecutionCheckpointBoundary,
    CheckpointBlockFormat,
};
use reth_embedded_beacon_config::ChainConfig;
use reth_embedded_beacon_network::{
    block_rpc::WireBlock,
    envelope_rpc::{EnvelopeBlockReference, WireEnvelope},
};
use reth_embedded_beacon_primitives::Root;
use reth_embedded_beacon_state_transition::ssz::SignedBeaconBlock;

/// Bounded complete durable admission inventory, separate from ordinary command admission.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecoveryLimits {
    /// Includes the concretely reauthenticated anchor and every retained branch receipt.
    pub max_records: usize,
    /// Complete original signed block plus optional envelope bytes retained for one replay step.
    pub max_original_bytes: usize,
    /// Aggregate authenticated original-data work across the bounded metadata inventory.
    pub max_total_original_bytes: u64,
}
impl Default for RecoveryLimits {
    fn default() -> Self {
        Self {
            max_records: 4096,
            max_original_bytes: 64 * 1024 * 1024,
            max_total_original_bytes: 64 * 1024 * 1024 * 1024,
        }
    }
}
impl RecoveryLimits {
    pub(super) const fn validate(self) -> Result<(), ImportError> {
        if self.max_records == 0 ||
            self.max_records > 4096 ||
            self.max_original_bytes == 0 ||
            self.max_original_bytes > 256 * 1024 * 1024 ||
            self.max_total_original_bytes < self.max_original_bytes as u64 ||
            self.max_total_original_bytes > 256 * 1024 * 1024 * 1024
        {
            return Err(ImportError::Bounds)
        }
        Ok(())
    }
}

pub(super) struct ReplayPlan {
    pub(super) expected: PersistedSnapshot,
    pub(super) blocks: Vec<ReplayBlock>,
    pub(super) anchor_envelope: Option<ReplayBlock>,
    pub(super) limits: RecoveryLimits,
    pub(super) transcript: Option<crate::compact_persistence::events::ReplayTranscript>,
}
pub(super) struct ReplayBlock {
    pub(super) slot: u64,
    pub(super) root: Root,
}
pub(super) struct ReplayInput {
    pub(super) wire: WireBlock,
    pub(super) envelope: Option<WireEnvelope>,
    pub(super) state_root: Root,
    pub(super) original_bytes: u64,
}
impl ReplayPlan {
    pub(super) fn prepare(
        repository: &AdmittedStateRepository,
        _config: &ChainConfig,
        anchor_root: Root,
        limits: RecoveryLimits,
    ) -> Result<Self, ImportError> {
        limits.validate()?;
        let expected = repository
            .replay_snapshot()
            .map_err(persistence)?
            .ok_or(ImportError::RecoveryRequired)?;
        let anchor_root = repository.recovery_root().map_err(persistence)?.unwrap_or(anchor_root);
        let inventory = repository.replay_inventory(limits.max_records).map_err(persistence)?;
        let blocks = inventory
            .into_iter()
            .filter(|(_, root)| *root != anchor_root)
            .map(|(slot, root)| ReplayBlock { slot, root })
            .collect::<Vec<_>>();
        if expected.head().root != anchor_root &&
            !blocks.iter().any(|block| block.root == expected.head().root)
        {
            return Err(ImportError::RecoveryRequired)
        }
        let anchor_envelope =
            if repository.replay_contains_envelope(anchor_root).map_err(persistence)? {
                let slot = repository
                    .block_state(anchor_root)
                    .map_err(persistence)?
                    .ok_or(ImportError::RecoveryRequired)?
                    .state()
                    .slot();
                Some(ReplayBlock { slot, root: anchor_root })
            } else {
                None
            };
        let transcript = repository.replay_transcript().map_err(persistence)?;
        Ok(Self { expected, blocks, anchor_envelope, limits, transcript })
    }
    pub(super) fn load(
        &self,
        repository: &AdmittedStateRepository,
        config: &ChainConfig,
        record: &ReplayBlock,
    ) -> Result<ReplayInput, ImportError> {
        let (format, original) = repository
            .original_block(record.root)
            .map_err(persistence)?
            .ok_or(ImportError::RecoveryRequired)?;
        if format != CheckpointBlockFormat::Signed ||
            original.len() > self.limits.max_original_bytes
        {
            return Err(ImportError::Bounds)
        }
        let mut bytes = original.len();
        let wire = WireBlock::from_ssz(original.to_vec(), config).map_err(|_| ImportError::Wire)?;
        drop(original);
        if wire.root() != record.root || wire.slot() != record.slot {
            return Err(ImportError::RecoveryRequired)
        }
        let envelope = if repository.replay_contains_envelope(record.root).map_err(persistence)? {
            repository.original_envelope(record.root).map_err(persistence)?
        } else {
            None
        }
        .map(|original| {
            bytes = bytes
                .checked_add(original.len())
                .filter(|bytes| *bytes <= self.limits.max_original_bytes)
                .ok_or(ImportError::Bounds)?;
            WireEnvelope::from_ssz(original, &wire, config).map_err(|_| ImportError::Wire)
        })
        .transpose()?;
        let state_root = repository
            .block_state(record.root)
            .map_err(persistence)?
            .ok_or(ImportError::RecoveryRequired)?
            .state()
            .hash_tree_root()
            .map_err(|_| ImportError::Wire)?;
        let signed = SignedBeaconBlock::from_ssz(wire.ssz(), wire.fork(), super::preset(config))
            .map_err(|_| ImportError::Wire)?;
        if signed.header().map_err(|_| ImportError::Wire)?.state_root != state_root {
            return Err(ImportError::RecoveryRequired)
        }
        Ok(ReplayInput { wire, envelope, state_root, original_bytes: bytes as u64 })
    }

    pub(super) fn load_anchor_envelope(
        &self,
        repository: &AdmittedStateRepository,
        config: &ChainConfig,
        record: &ReplayBlock,
    ) -> Result<
        (WireEnvelope, reth_embedded_beacon_state_transition::ssz::BeaconBlock, u64),
        ImportError,
    > {
        let (format, original) = repository
            .original_block(record.root)
            .map_err(persistence)?
            .ok_or(ImportError::RecoveryRequired)?;
        let fork = config.fork_at_slot(record.slot).map_err(|_| ImportError::Configuration)?.fork;
        let block = match format {
            CheckpointBlockFormat::Signed => {
                SignedBeaconBlock::from_ssz(&original, fork, super::preset(config))
                    .map_err(|_| ImportError::Wire)?
                    .message()
            }
            CheckpointBlockFormat::Unsigned => {
                reth_embedded_beacon_state_transition::ssz::BeaconBlock::from_ssz(
                    &original,
                    fork,
                    super::preset(config),
                )
                .map_err(|_| ImportError::Wire)?
            }
        };
        let reference =
            EnvelopeBlockReference::from_block(&block, config).map_err(|_| ImportError::Wire)?;
        if reference.root() != record.root || reference.slot() != record.slot {
            return Err(ImportError::RecoveryRequired)
        }
        let envelope = repository
            .original_envelope(record.root)
            .map_err(persistence)?
            .ok_or(ImportError::RecoveryRequired)?;
        let bytes = original
            .len()
            .checked_add(envelope.len())
            .filter(|bytes| *bytes <= self.limits.max_original_bytes)
            .ok_or(ImportError::Bounds)?;
        Ok((
            WireEnvelope::from_reference(envelope, &reference, config)
                .map_err(|_| ImportError::Wire)?,
            block,
            bytes as u64,
        ))
    }

    #[cfg(test)]
    pub(super) fn check(&self, graph: &Graph) -> Result<(), ImportError> {
        self.check_with_boundary(graph, None)
    }

    pub(super) fn check_with_boundary(
        &self,
        graph: &Graph,
        boundary: Option<&ExecutionCheckpointBoundary>,
    ) -> Result<(), ImportError> {
        let snapshot = graph.snapshot()?;
        if snapshot.head != self.expected.head() ||
            (snapshot.justified_checkpoint, snapshot.finalized_checkpoint) !=
                self.expected.checkpoints() ||
            state_root(graph, snapshot.head.root)? != self.expected.state_root()
        {
            return Err(ImportError::RecoveryRequired)
        }
        let execution = graph
            .execution_state_with_boundary(boundary)?
            .map(|(_, state)| state.head_block_hash.0);
        if execution != self.expected.execution().map(|association| association.block_hash) &&
            !(self.expected.execution().is_none() &&
                snapshot.head.payload_status ==
                    Some(reth_embedded_beacon_fork_choice::PayloadStatus::Empty))
        {
            return Err(ImportError::RecoveryRequired)
        }
        Ok(())
    }
}

pub(super) fn state_root(graph: &Graph, root: Root) -> Result<Root, ImportError> {
    match graph {
        Graph::Classic { store, .. } => store
            .read_block_state(root)?
            .ok_or(ImportError::RecoveryRequired)?
            .hash_tree_root()
            .map_err(|_| ImportError::Wire),
        Graph::Gloas { store, .. } => {
            if let Some(state) = store.read_block_state(root)? {
                state.hash_tree_root().map_err(|_| ImportError::Wire)
            } else {
                store
                    .read_historical_block_state(root)?
                    .ok_or(ImportError::RecoveryRequired)?
                    .hash_tree_root()
                    .map_err(|_| ImportError::Wire)
            }
        }
    }
}
fn persistence(error: crate::compact_persistence::PersistenceError) -> ImportError {
    ImportError::Persistence(error.to_string())
}
