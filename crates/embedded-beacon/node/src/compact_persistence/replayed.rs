//! Concrete durable bootstrap of an independently replay-authenticated skipped-slot boundary.
//! Original block/post-state retain their actual slot/schema/commitment. The separate complete
//! boundary state is durably pinned before the trusted boundary descriptor is published.

use super::*;
use crate::ReplayedCheckpoint;
use reth_embedded_beacon_state_transition::fork_processing::replay_slots;

impl AdmittedStateRepository {
    /// Compare the actual durable recovery boundary only after rechecking all original and
    /// complete-state pins and the independently reproduced scheduled boundary state.
    #[cfg(all(feature = "peer-runtime", feature = "execution-verification"))]
    pub(crate) fn retained_execution_boundary(
        &self,
        checkpoint: Checkpoint,
        original_state_root: Root,
        original_slot: u64,
        boundary_state_root: Root,
        boundary_slot: u64,
    ) -> Result<bool, PersistenceError> {
        let inner = self.lock()?;
        let Some(anchor) =
            inner.replay_recovery().map(|generation| &generation.anchor).or(inner.anchor.as_ref())
        else {
            return Ok(false)
        };
        let Some(boundary) = &anchor.boundary else { return Ok(false) };
        if anchor.genesis ||
            anchor.checkpoint != checkpoint ||
            anchor.state_root != original_state_root ||
            anchor.slot != original_slot ||
            boundary.state_root != boundary_state_root ||
            boundary.slot != boundary_slot
        {
            return Ok(false)
        }
        let origin = inner.records.get(&checkpoint.root).ok_or(PersistenceError::Linkage)?;
        self.check_saved_record(origin)?;
        self.check_saved_boundary(anchor)?;
        Ok(true)
    }

    /// Exact checked consensus configuration captured by the existing repository writer.
    #[cfg(all(feature = "peer-runtime", feature = "execution-verification"))]
    pub(crate) const fn consensus_config_digest(&self) -> Root {
        self.network.consensus_digest
    }

    /// Reauthenticate complete scheduled replay and persist both distinct complete states.
    /// This is blocking work for the dedicated import/bootstrap owner.
    #[allow(clippy::too_many_arguments)]
    pub fn persist_replayed_checkpoint(
        &self,
        verifier: &SszCheckpointVerifier,
        proof: &ReplayedCheckpoint,
        boundary_ssz: &[u8],
        post_block_ssz: &[u8],
        original_block_ssz: &[u8],
        now_unix_ms: u64,
        max_age_epochs: u64,
    ) -> Result<(), PersistenceError> {
        if proof.verified().config_digest() != Some(self.network.consensus_digest) ||
            !proof.matches_inputs(
                boundary_ssz,
                post_block_ssz,
                original_block_ssz,
                verifier.block_format(),
            )
        {
            return Err(PersistenceError::Identity)
        }
        let checked = verifier.verify_checkpoint_replay(
            boundary_ssz,
            post_block_ssz,
            original_block_ssz,
            proof.verified().checkpoint(),
            now_unix_ms,
            max_age_epochs,
            proof.max_replay_slots(),
        )?;
        let actual = checked.verified();
        if actual.config_digest() != Some(self.network.consensus_digest) ||
            actual.slot() != proof.verified().slot() ||
            actual.state_root() != proof.verified().state_root() ||
            checked.post_block_slot() != proof.post_block_slot() ||
            checked.post_block_fork() != proof.post_block_fork()
        {
            return Err(PersistenceError::Identity)
        }
        self.check_bytes(original_block_ssz)?;
        let post = BeaconState::from_ssz(
            post_block_ssz,
            checked.post_block_fork(),
            self.network.preset(),
        )?;
        let boundary_fork = self
            .network
            .config
            .fork_at_slot(actual.slot())
            .map_err(|_| PersistenceError::Identity)?
            .fork;
        let boundary = BeaconState::from_ssz(boundary_ssz, boundary_fork, self.network.preset())?;
        self.check_state(&post)?;
        self.check_state(&boundary)?;
        let block = match verifier.block_format() {
            CheckpointBlockFormat::Unsigned => BeaconBlock::from_ssz(
                original_block_ssz,
                checked.post_block_fork(),
                self.network.preset(),
            )?,
            CheckpointBlockFormat::Signed => {
                let signed = SignedBeaconBlock::from_ssz(
                    original_block_ssz,
                    checked.post_block_fork(),
                    self.network.preset(),
                )?;
                verify_original_signature(&post, &signed, self.network.config.slots_per_epoch)?;
                signed.message()
            }
        };
        let header = block.header()?;
        let post_root = post.hash_tree_root()?;
        if header.slot != checked.post_block_slot() ||
            header.slot != post.slot() ||
            header.state_root != post_root ||
            block.hash_tree_root()? != actual.checkpoint().root
        {
            return Err(PersistenceError::Linkage)
        }
        let record = Admission {
            block_root: actual.checkpoint().root,
            parent_root: header.parent_root,
            state_root: post_root,
            slot: post.slot(),
            fork: checked.post_block_fork(),
            original: ObjectId::new(ObjectKind::BeaconBlock, Encoding::Ssz, original_block_ssz),
            format: verifier.block_format(),
            execution: execution(&block),
            envelope: None,
        };
        let anchor = Anchor {
            checkpoint: actual.checkpoint(),
            state_root: post_root,
            slot: post.slot(),
            genesis: false,
            boundary: Some(BoundaryAnchor {
                state_root: actual.state_root(),
                slot: actual.slot(),
                fork: boundary_fork,
                max_replay_slots: checked.max_replay_slots(),
            }),
        };
        let mut inner = self.lock()?;
        if self.existing_seed(&inner, &anchor, &record)? {
            return Ok(())
        }
        self.check_capacity(&inner, record.block_root)?;
        self.compact.persist_state(&post, post_root)?;
        self.compact.persist_state(&boundary, actual.state_root())?;
        self.shared.put(
            ObjectKind::BeaconBlock,
            Encoding::Ssz,
            original_block_ssz,
            Owner::Checkpoint(record.block_root),
        )?;
        let mut next = Saved::capture(self.network.consensus_digest, &inner);
        next.records.push(record.clone());
        next.anchor = Some(anchor.clone());
        inner.journal.commit(&next)?;
        inner.records.insert(record.block_root, record);
        inner.anchor = Some(anchor);
        Ok(())
    }

    pub(super) fn check_saved_boundary(&self, anchor: &Anchor) -> Result<(), PersistenceError> {
        let Some(boundary) = &anchor.boundary else { return Ok(()) };
        if boundary.slot !=
            anchor
                .checkpoint
                .epoch
                .checked_mul(self.network.config.slots_per_epoch)
                .ok_or(PersistenceError::Identity)? ||
            boundary.slot < anchor.slot ||
            boundary.slot - anchor.slot > boundary.max_replay_slots ||
            boundary.max_replay_slots == 0 ||
            boundary.max_replay_slots > 8192 ||
            self.network
                .config
                .fork_at_slot(boundary.slot)
                .map_err(|_| PersistenceError::Identity)?
                .fork !=
                boundary.fork
        {
            return Err(PersistenceError::Linkage)
        }
        self.compact.ensure_retained(boundary.state_root)?;
        if boundary.state_root != anchor.state_root {
            self.compact.ensure_retained(anchor.state_root)?;
        }
        let exact = self
            .compact
            .lease(boundary.state_root, boundary.fork, self.network.preset())?
            .ok_or(PersistenceError::Linkage)?;
        let post_fork = self
            .network
            .config
            .fork_at_slot(anchor.slot)
            .map_err(|_| PersistenceError::Identity)?
            .fork;
        let post = self
            .compact
            .lease(anchor.state_root, post_fork, self.network.preset())?
            .ok_or(PersistenceError::Linkage)?;
        self.check_state(exact.state())?;
        self.check_state(post.state())?;
        if post.state().slot() != anchor.slot ||
            exact.state().slot() != boundary.slot ||
            post.root() != anchor.state_root ||
            exact.root() != boundary.state_root ||
            self.shared.identity() != self.network.identity()
        {
            return Err(PersistenceError::Linkage)
        }
        if boundary.slot == anchor.slot {
            // Both private immutable leases were independently checked against their complete
            // canonical commitments by storage. Fresh graph/pin and network checks above still
            // run; comparing their exact roots avoids serializing two complete no-op states.
            if anchor.state_root != boundary.state_root ||
                post.root() != anchor.state_root ||
                exact.root() != boundary.state_root
            {
                return Err(PersistenceError::Linkage)
            }
        } else {
            // Callers hold the healthy repository journal guard and recheck the exact original
            // signed admission before reaching this helper. Both complete graph/pin checks and
            // private lease roots were checked above on this call. Only native replay is memoized,
            // under the full unchanged descriptor/config and the same two immutable allocations.
            let mut proof =
                self.boundary_proof.lock().map_err(|_| PersistenceError::CoordinatorPoisoned)?;
            if proof.as_ref().is_some_and(|proof| {
                proof.anchor == *anchor &&
                    proof.identity == self.network.identity() &&
                    proof.consensus_digest == self.network.consensus_digest &&
                    proof.post.matches(&post) &&
                    proof.boundary.matches(&exact)
            }) {
                return Ok(())
            }
            let actual = replay_slots(
                post.state(),
                boundary.slot,
                &self.runtime.transitions,
                boundary.max_replay_slots,
            )?;
            if actual.hash_tree_root()? != boundary.state_root ||
                actual.to_ssz()? != exact.state().to_ssz()?
            {
                return Err(PersistenceError::Linkage)
            }
            *proof = Some(BoundaryProof {
                anchor: anchor.clone(),
                identity: self.network.identity(),
                consensus_digest: self.network.consensus_digest,
                post: post.downgrade(),
                boundary: exact.downgrade(),
            });
        }
        Ok(())
    }

    pub(super) fn recover_replayed_checkpoint(
        &self,
        verifier: &SszCheckpointVerifier,
        anchor: &Anchor,
        record: &Admission,
        now_unix_ms: u64,
        max_age_epochs: u64,
    ) -> Result<RecoveredCompactCheckpoint, PersistenceError> {
        let boundary = anchor.boundary.as_ref().ok_or(PersistenceError::Linkage)?;
        self.check_saved_boundary(anchor)?;
        if boundary.slot == anchor.slot {
            let state = self
                .compact
                .lease(record.state_root, record.fork, self.network.preset())?
                .ok_or(PersistenceError::Linkage)?;
            let block_ssz = self
                .shared
                .get_bounded(record.original, self.network.config.max_payload_size)?
                .ok_or(PersistenceError::Linkage)?;
            let verified = verifier.verify_checkpoint(
                &state.state().to_ssz()?,
                &block_ssz,
                anchor.checkpoint,
                now_unix_ms,
                max_age_epochs,
            )?;
            if verified.config_digest() != Some(self.network.consensus_digest) ||
                verified.state_root() != boundary.state_root ||
                verified.slot() != boundary.slot
            {
                return Err(PersistenceError::Identity)
            }
            return Ok(RecoveredCompactCheckpoint {
                checkpoint: verified,
                state,
                block_ssz,
                block_format: record.format,
                replay: None,
            })
        }
        let state = self
            .compact
            .lease(boundary.state_root, boundary.fork, self.network.preset())?
            .ok_or(PersistenceError::Linkage)?;
        let post_block_state = self
            .compact
            .lease(record.state_root, record.fork, self.network.preset())?
            .ok_or(PersistenceError::Linkage)?;
        let block_ssz = self
            .shared
            .get_bounded(record.original, self.network.config.max_payload_size)?
            .ok_or(PersistenceError::Linkage)?;
        let proof = verifier.verify_checkpoint_replay(
            &state.state().to_ssz()?,
            &post_block_state.state().to_ssz()?,
            &block_ssz,
            anchor.checkpoint,
            now_unix_ms,
            max_age_epochs,
            boundary.max_replay_slots,
        )?;
        if proof.verified().config_digest() != Some(self.network.consensus_digest) ||
            proof.verified().state_root() != boundary.state_root ||
            proof.post_block_slot() != record.slot ||
            proof.post_block_fork() != record.fork
        {
            return Err(PersistenceError::Identity)
        }
        Ok(RecoveredCompactCheckpoint {
            checkpoint: proof.verified().clone(),
            state,
            block_ssz,
            block_format: record.format,
            replay: Some(RecoveredReplayCheckpoint { proof, post_block_state }),
        })
    }
}

/// Separate complete original post-block lease and sealed scheduled boundary proof.
#[derive(Debug)]
pub struct RecoveredReplayCheckpoint {
    /// Freshly concrete reauthenticated scheduled replay, never reconstructed from metadata alone.
    pub proof: ReplayedCheckpoint,
    /// Complete actual original block-slot state, preserving its original schema.
    pub post_block_state: StateLease,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct BoundaryAnchor {
    pub(super) state_root: Root,
    pub(super) slot: u64,
    pub(super) fork: Fork,
    pub(super) max_replay_slots: u64,
}

/// One weak native replay proof. It neither retains decoded states nor survives a reopen.
#[derive(Debug)]
pub(super) struct BoundaryProof {
    anchor: Anchor,
    identity: StoreIdentity,
    consensus_digest: Root,
    post: WeakStateLease,
    boundary: WeakStateLease,
}
#[cfg(test)]
mod tests;
