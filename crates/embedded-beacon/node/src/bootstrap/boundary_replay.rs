//! Authenticate empty-slot checkpoint boundaries by replaying the committed post-block state.

use super::*;
use crate::{runtime_parameters::RuntimeParameters, VerifiedCheckpoint};
use reth_embedded_beacon_primitives::{sha256, Checkpoint, Fork, Root};
use reth_embedded_beacon_state_transition::fork_processing::replay_slots;

impl SszCheckpointVerifier {
    /// Verify an epoch-boundary state by replaying from the exact state committed by its block.
    /// The original post-block state is mandatory; a copied latest header confers no authority.
    /// This is blocking SSZ and transition work and must run outside an asynchronous IO worker.
    #[allow(clippy::too_many_arguments)]
    pub fn verify_checkpoint_with_replay(
        &self,
        boundary_state_ssz: &[u8],
        post_block_state_ssz: &[u8],
        block_ssz: &[u8],
        trusted: Checkpoint,
        now_unix_ms: u64,
        max_age_epochs: u64,
        max_replay_slots: u64,
    ) -> Result<VerifiedCheckpoint, BootstrapError> {
        if max_age_epochs == 0 || max_replay_slots == 0 || max_replay_slots > 8192 {
            return Err(BootstrapError::InvalidConfiguration);
        }
        let clock = self.config.clock().map_err(|_| BootstrapError::InvalidConfiguration)?;
        let current_slot =
            clock.slot_at(now_unix_ms).ok_or(BootstrapError::InvalidConfiguration)?;
        let current_epoch = clock.epoch_at_slot(current_slot);
        if trusted.epoch > current_epoch || current_epoch - trusted.epoch > max_age_epochs {
            return Err(BootstrapError::StaleCheckpoint);
        }
        let boundary_slot = trusted
            .epoch
            .checked_mul(self.config.slots_per_epoch)
            .ok_or(BootstrapError::InvalidConfiguration)?;
        let (boundary, evidence) = self.decode_state_and_block(boundary_state_ssz, block_ssz)?;
        // Identical original bytes have one immutable decoded state and evidence. Distinct
        // inputs still require independent decoding, commitments and complete slot replay.
        let distinct_post = if boundary_state_ssz == post_block_state_ssz {
            None
        } else {
            Some(self.decode_state_and_block(post_block_state_ssz, block_ssz)?)
        };
        let committed = distinct_post.as_ref().map_or(&evidence, |(_, evidence)| evidence);
        if evidence.block_root != trusted.root || committed.block_root != trusted.root {
            return Err(BootstrapError::WrongTrustedRoot);
        }
        if evidence.state_slot != boundary_slot ||
            evidence.latest_block_root != trusted.root ||
            evidence.block_slot > boundary_slot ||
            committed.state_slot != committed.block_slot ||
            committed.latest_block_root != trusted.root ||
            committed.state_root != committed.block_state_root
        {
            return Err(BootstrapError::StateBlockMismatch);
        }
        let distance = boundary_slot
            .checked_sub(committed.state_slot)
            .ok_or(BootstrapError::StateBlockMismatch)?;
        if distance > max_replay_slots {
            return Err(BootstrapError::CheckpointReplayLimit);
        }
        let parameters = RuntimeParameters::from_config(&self.config)
            .map_err(|error| BootstrapError::CheckpointReplay(error.to_string()))?;
        let replayed = if let Some((post_block, _)) = distinct_post {
            Some(if distance == 0 {
                post_block
            } else {
                replay_slots(&post_block, boundary_slot, &parameters.transitions, max_replay_slots)
                    .map_err(|error| BootstrapError::CheckpointReplay(error.to_string()))?
            })
        } else {
            // All existing linkage checks imply zero distance for the same decoded state.
            // Keep the explicit bound so a future refactor cannot skip required replay.
            if distance != 0 {
                return Err(BootstrapError::StateBlockMismatch);
            }
            None
        };
        let checked_state = replayed.as_ref().unwrap_or(&boundary);
        let checked_root = if replayed.is_some() {
            checked_state.hash_tree_root().map_err(ssz_error)?
        } else {
            evidence.state_root
        };
        if checked_root != evidence.state_root ||
            checked_state.to_ssz().map_err(ssz_error)?.as_slice() != boundary_state_ssz
        {
            return Err(BootstrapError::UnverifiedBoundaryState);
        }
        crate::weak_subjectivity::verify_checkpoint_age(&boundary, &self.config, current_epoch)?;
        let verified = VerifiedCheckpoint {
            checkpoint: trusted,
            state_root: evidence.state_root,
            slot: boundary_slot,
            config_digest: Some(checked_config_digest(&self.config)?),
        };
        tracing::info!(target: "beacon::bootstrap", network = %self.config.name,
            slot = boundary_slot, replayed_slots = distance,
            "[beacon] Authenticated checkpoint boundary through scheduled empty-slot replay");
        Ok(verified)
    }
}

#[cfg(test)]
pub(crate) mod tests;

/// Concrete replay authority bound to every exact original input and its block-slot schema.
/// There is no public constructor or mutation method; saved metadata cannot recreate this proof.
#[derive(Clone, Debug)]
pub struct ReplayedCheckpoint {
    verified: VerifiedCheckpoint,
    post_block_slot: u64,
    post_block_fork: Fork,
    input_hashes: [Root; 3],
    block_format: CheckpointBlockFormat,
    max_replay_slots: u64,
}

impl ReplayedCheckpoint {
    /// Authenticated epoch boundary and exact checked network identity.
    pub const fn verified(&self) -> &VerifiedCheckpoint {
        &self.verified
    }

    /// Actual original block slot, which may precede the authenticated epoch boundary.
    pub const fn post_block_slot(&self) -> u64 {
        self.post_block_slot
    }

    /// Scheduled schema of the original block and its committed post-state.
    pub const fn post_block_fork(&self) -> Fork {
        self.post_block_fork
    }

    /// The exact work cap used by the concrete replay verifier.
    pub const fn max_replay_slots(&self) -> u64 {
        self.max_replay_slots
    }

    /// Match the exact original input bytes and wire container; no field grants authority alone.
    pub fn matches_inputs(
        &self,
        boundary: &[u8],
        post_block: &[u8],
        block: &[u8],
        format: CheckpointBlockFormat,
    ) -> bool {
        self.block_format == format &&
            self.input_hashes == checkpoint_input_hashes(boundary, post_block, block)
    }
}

impl SszCheckpointVerifier {
    /// Return sealed replay evidence for admission and persistence of the original earlier block.
    /// This performs the same complete verification as `verify_checkpoint_with_replay`.
    #[allow(clippy::too_many_arguments)]
    pub fn verify_checkpoint_replay(
        &self,
        boundary_state_ssz: &[u8],
        post_block_state_ssz: &[u8],
        block_ssz: &[u8],
        trusted: Checkpoint,
        now_unix_ms: u64,
        max_age_epochs: u64,
        max_replay_slots: u64,
    ) -> Result<ReplayedCheckpoint, BootstrapError> {
        let verified = self.verify_checkpoint_with_replay(
            boundary_state_ssz,
            post_block_state_ssz,
            block_ssz,
            trusted,
            now_unix_ms,
            max_age_epochs,
            max_replay_slots,
        )?;
        let post_block_slot = slot_at(post_block_state_ssz, 40)?;
        let post_block_fork = self
            .config
            .fork_at_slot(post_block_slot)
            .map_err(|_| BootstrapError::InvalidConfiguration)?
            .fork;
        Ok(ReplayedCheckpoint {
            verified,
            post_block_slot,
            post_block_fork,
            input_hashes: checkpoint_input_hashes(
                boundary_state_ssz,
                post_block_state_ssz,
                block_ssz,
            ),
            block_format: self.block_format,
            max_replay_slots,
        })
    }
}

// Exact byte equality only reuses work; the proof retains one digest for each original input.
fn checkpoint_input_hashes(boundary: &[u8], post_block: &[u8], block: &[u8]) -> [Root; 3] {
    let boundary_hash = sha256(boundary);
    let post_block_hash = if boundary == post_block { boundary_hash } else { sha256(post_block) };
    [boundary_hash, post_block_hash, sha256(block)]
}
