//! Durable authenticated bootstrap records retaining shared immutable bytes.
//!
//! All methods perform blocking storage and SSZ work. Runtime callers must use
//! a blocking task and one import coordinator.

use crate::{BootstrapError, CheckpointBlockFormat, SszCheckpointVerifier, VerifiedCheckpoint};
use reth_embedded_beacon_primitives::{sha256, Checkpoint, Root};
use reth_embedded_beacon_shared_data::{
    Encoding, ObjectId, ObjectKind, Owner, SharedDataStore, StoreError, StoreIdentity,
};
use reth_embedded_beacon_storage::{MetadataBatch, MetadataError, MetadataStore};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

const CURRENT: &str = "checkpoint/current";
const PREVIOUS: &str = "checkpoint/previous";

/// Checkpoint persistence errors; none permits a partially verified recovery.
#[derive(Debug, thiserror::Error)]
pub enum CheckpointStoreError {
    /// State and block authentication failed.
    #[error(transparent)]
    Verification(#[from] BootstrapError),
    /// Immutable bytes or durable ownership failed.
    #[error(transparent)]
    Shared(#[from] StoreError),
    /// Atomic metadata persistence failed.
    #[error(transparent)]
    Metadata(#[from] MetadataError),
    /// Persisted record encoding is malformed.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    /// Store namespaces disagree with the checked network/profile identity.
    #[error("checkpoint stores do not match the expected network and configuration")]
    IdentityMismatch,
    /// Unsupported schema, container interpretation or inconsistent record fields.
    #[error("invalid checkpoint recovery record")]
    InvalidRecord,
}

/// Verified checkpoint and its shared immutable encodings after restart.
#[derive(Debug)]
pub struct RecoveredCheckpoint {
    /// Trusted identity authenticated against the decoded state and block.
    pub checkpoint: VerifiedCheckpoint,
    /// Exact state bytes; a caller may keep these across object garbage collection.
    pub state_ssz: Arc<[u8]>,
    /// Exact block container bytes under the configured explicit format.
    pub block_ssz: Arc<[u8]>,
    /// Shared object identity for consensus retention without duplicating state bytes.
    pub state_object: ObjectId,
    /// Shared object identity for consensus retention without duplicating block bytes.
    pub block_object: ObjectId,
}

/// One coordinator publishing authenticated checkpoints into existing shared stores.
///
/// Immutable data and owner pins are durable before the current pointer changes.
/// Two committed generations stay pinned. Interrupted unpublished generations can
/// be released separately without touching pool, consensus or other recovery pins.
#[derive(Debug)]
pub struct CheckpointRepository {
    metadata: MetadataStore,
    shared: SharedDataStore,
    verifier: SszCheckpointVerifier,
    identity: StoreIdentity,
}

impl CheckpointRepository {
    /// Reuse the node's stores under its complete checked profile identity.
    ///
    /// The profile digest must include every consensus setting and private
    /// execution/terminal anchor before these stores are opened.
    pub fn from_stores(
        metadata: MetadataStore,
        shared: SharedDataStore,
        verifier: SszCheckpointVerifier,
        identity: StoreIdentity,
    ) -> Result<Self, CheckpointStoreError> {
        if metadata.identity() != identity ||
            shared.identity() != identity ||
            identity.genesis_root != verifier.network_root()
        {
            return Err(CheckpointStoreError::IdentityMismatch)
        }
        Ok(Self { metadata, shared, verifier, identity })
    }

    /// Authenticate and atomically publish one bootstrap checkpoint.
    ///
    /// A failed write may leave an unpublished owner, but never replaces the
    /// prior checkpoint with an incomplete state/block pair.
    pub fn persist(
        &mut self,
        state_ssz: &[u8],
        block_ssz: &[u8],
        trusted: Checkpoint,
        now_unix_ms: u64,
        max_age_epochs: u64,
    ) -> Result<VerifiedCheckpoint, CheckpointStoreError> {
        let verified = self.verifier.verify_checkpoint(
            state_ssz,
            block_ssz,
            trusted,
            now_unix_ms,
            max_age_epochs,
        )?;
        let prior = self.record(CURRENT)?;
        // Validate both pointers before altering any ownership.
        let _ = self.record(PREVIOUS)?;
        let record = CheckpointRecord {
            schema_version: 1,
            identity: self.identity,
            trusted,
            state_root: verified.state_root(),
            slot: verified.slot(),
            block_format: self.verifier.block_format(),
            state_object: ObjectId::new(ObjectKind::BeaconState, Encoding::Ssz, state_ssz),
            block_object: ObjectId::new(ObjectKind::BeaconBlock, Encoding::Ssz, block_ssz),
        };
        let owner = record.owner()?;
        self.shared.put(ObjectKind::BeaconState, Encoding::Ssz, state_ssz, owner)?;
        self.shared.put(ObjectKind::BeaconBlock, Encoding::Ssz, block_ssz, owner)?;
        let bytes = serde_json::to_vec(&record)?;
        let mut batch = MetadataBatch::new().put(CURRENT, bytes);
        if prior.as_ref() != Some(&record) {
            batch = match prior {
                Some(prior) => batch.put(PREVIOUS, serde_json::to_vec(&prior)?),
                None => batch.delete(PREVIOUS),
            };
        }
        self.metadata.commit(batch)?;
        self.reconcile_checkpoint_owners()?;
        tracing::info!(target: "beacon::recovery", slot = verified.slot(), epoch = trusted.epoch, "[beacon] Published authenticated checkpoint");
        tracing::debug!(target: "beacon::recovery", state_bytes = state_ssz.len(), block_bytes = block_ssz.len(), "[beacon] Retained checkpoint objects and reconciled owners");
        Ok(verified)
    }

    /// Reload a previously authenticated checkpoint without contacting its source.
    ///
    /// Recompute complete SSZ roots and linkage and enforce the current trust-age
    /// cap again. This does not replay later blocks or complete live recovery.
    pub fn recover(
        &mut self,
        now_unix_ms: u64,
        max_age_epochs: u64,
    ) -> Result<Option<RecoveredCheckpoint>, CheckpointStoreError> {
        let Some(record) = self.record(CURRENT)? else {
            self.reconcile_checkpoint_owners()?;
            tracing::debug!(target: "beacon::recovery", "[beacon] No persisted checkpoint available");
            return Ok(None)
        };
        let state = self
            .shared
            .get_bounded(record.state_object, self.verifier.decode_limits().max_state_bytes)?
            .ok_or(StoreError::MissingObject(record.state_object))?;
        let block = self
            .shared
            .get_bounded(record.block_object, self.verifier.decode_limits().max_block_bytes)?
            .ok_or(StoreError::MissingObject(record.block_object))?;
        let verified = self.verifier.verify_checkpoint(
            &state,
            &block,
            record.trusted,
            now_unix_ms,
            max_age_epochs,
        )?;
        if verified.state_root() != record.state_root || verified.slot() != record.slot {
            return Err(CheckpointStoreError::InvalidRecord)
        }
        self.reconcile_checkpoint_owners()?;
        tracing::info!(target: "beacon::recovery", slot = verified.slot(), epoch = record.trusted.epoch, "[beacon] Recovered and reauthenticated checkpoint");
        Ok(Some(RecoveredCheckpoint {
            checkpoint: verified,
            state_ssz: state,
            block_ssz: block,
            state_object: record.state_object,
            block_object: record.block_object,
        }))
    }

    fn record(&self, key: &str) -> Result<Option<CheckpointRecord>, CheckpointStoreError> {
        let Some(bytes) = self.metadata.get(key)? else { return Ok(None) };
        let record: CheckpointRecord = serde_json::from_slice(&bytes)?;
        if record.schema_version != 1 ||
            record.identity != self.identity ||
            record.block_format != self.verifier.block_format() ||
            record.state_object.kind != ObjectKind::BeaconState ||
            record.block_object.kind != ObjectKind::BeaconBlock ||
            record.state_object.encoding != Encoding::Ssz ||
            record.block_object.encoding != Encoding::Ssz
        {
            return Err(CheckpointStoreError::InvalidRecord)
        }
        Ok(Some(record))
    }

    fn reconcile_checkpoint_owners(&self) -> Result<(), CheckpointStoreError> {
        self.shared.ensure_healthy()?;
        let retained = [self.record(CURRENT)?, self.record(PREVIOUS)?]
            .into_iter()
            .flatten()
            .map(|record| record.owner())
            .collect::<Result<std::collections::BTreeSet<_>, _>>()?;
        let abandoned = self
            .shared
            .owners()
            .into_iter()
            .filter(|owner| matches!(owner, Owner::Checkpoint(_)) && !retained.contains(owner))
            .collect::<Vec<_>>();
        if !abandoned.is_empty() {
            self.shared.release_owners(&abandoned)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CheckpointRecord {
    schema_version: u32,
    identity: StoreIdentity,
    trusted: Checkpoint,
    state_root: Root,
    slot: u64,
    block_format: CheckpointBlockFormat,
    state_object: ObjectId,
    block_object: ObjectId,
}

impl CheckpointRecord {
    fn owner(&self) -> Result<Owner, serde_json::Error> {
        let mut bytes = b"embedded-beacon-checkpoint-v1".to_vec();
        bytes.extend_from_slice(&serde_json::to_vec(self)?);
        Ok(Owner::Checkpoint(sha256(&bytes)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{bootstrap::tests::linked_fixture, BootstrapDecodeLimits};
    use reth_embedded_beacon_config::ChainConfig;
    use reth_embedded_beacon_state_transition::ssz::Preset;
    use tempfile::TempDir;

    fn identity() -> StoreIdentity {
        StoreIdentity { genesis_root: [1; 32], config_digest: [2; 32] }
    }

    fn open(
        directory: &std::path::Path,
        config: ChainConfig,
    ) -> (CheckpointRepository, MetadataStore, SharedDataStore) {
        let metadata = MetadataStore::open(directory, identity()).unwrap();
        let shared = SharedDataStore::open(directory, identity(), 0).unwrap();
        let verifier = SszCheckpointVerifier::new(
            config,
            BootstrapDecodeLimits::default(),
            CheckpointBlockFormat::Unsigned,
        )
        .unwrap();
        let repository = CheckpointRepository::from_stores(
            metadata.clone(),
            shared.clone(),
            verifier,
            identity(),
        )
        .unwrap();
        (repository, metadata, shared)
    }

    #[test]
    fn authenticated_checkpoint_reopens_without_source_or_duplicate_snapshot() {
        let directory = TempDir::new().unwrap();
        let (state, block, config, trusted) = linked_fixture();
        let state_bytes = state.to_ssz(Preset::Minimal).unwrap();
        let block_bytes = block.to_ssz(Preset::Minimal).unwrap();
        let now = config.clock().unwrap().start_of_slot(72).unwrap();
        {
            let (mut repository, _, shared) = open(directory.path(), config.clone());
            repository.persist(&state_bytes, &block_bytes, trusted, now, 4).unwrap();
            repository.persist(&state_bytes, &block_bytes, trusted, now, 4).unwrap();
            assert_eq!(shared.stats().unwrap().physical_objects, 2);
            assert_eq!(shared.owners().len(), 1);
            assert_eq!(
                std::fs::read_dir(directory.path().join("beacon-metadata-v1/snapshots"))
                    .unwrap()
                    .count(),
                0,
            );
        }
        let (mut repository, _, shared) = open(directory.path(), config);
        let recovered = repository.recover(now, 4).unwrap().unwrap();
        assert_eq!(recovered.checkpoint.checkpoint(), trusted);
        assert_eq!(recovered.state_ssz.as_ref(), state_bytes);
        assert_eq!(recovered.block_ssz.as_ref(), block_bytes);
        assert_eq!(shared.stats().unwrap().physical_objects, 2);
    }

    #[test]
    fn failed_authentication_preserves_current_and_recovery_keeps_other_owners() {
        let directory = TempDir::new().unwrap();
        let (mut state, block, config, trusted) = linked_fixture();
        let now = config.clock().unwrap().start_of_slot(72).unwrap();
        let (mut repository, metadata, shared) = open(directory.path(), config);
        let block_bytes = block.to_ssz(Preset::Minimal).unwrap();
        repository
            .persist(&state.to_ssz(Preset::Minimal).unwrap(), &block_bytes, trusted, now, 4)
            .unwrap();
        let pointer = metadata.get(CURRENT).unwrap();
        state.balances.values[0] ^= 1;
        assert!(repository
            .persist(&state.to_ssz(Preset::Minimal).unwrap(), &block_bytes, trusted, now, 4,)
            .is_err());
        assert_eq!(metadata.get(CURRENT).unwrap(), pointer);
        let orphan = shared
            .put(
                ObjectKind::BeaconState,
                Encoding::Ssz,
                b"interrupted pending generation",
                Owner::Checkpoint([9; 32]),
            )
            .unwrap();
        let survivors = [Owner::Pool([4; 32]), Owner::Consensus([5; 32]), Owner::Recovery([6; 32])];
        for owner in survivors {
            shared.pin(owner, orphan).unwrap();
        }
        assert!(repository.recover(now, 4).unwrap().is_some());
        assert!(shared.owner_objects(Owner::Checkpoint([9; 32])).is_empty());
        for owner in survivors {
            assert_eq!(shared.owner_objects(owner), vec![orphan]);
        }
        assert_eq!(shared.collect_garbage().unwrap(), 0);
    }

    #[test]
    fn recovery_recomputes_ssz_roots_and_rejects_inconsistent_record_before_cleanup() {
        let directory = TempDir::new().unwrap();
        let (state, block, config, trusted) = linked_fixture();
        let now = config.clock().unwrap().start_of_slot(72).unwrap();
        let (mut repository, metadata, shared) = open(directory.path(), config);
        repository
            .persist(
                &state.to_ssz(Preset::Minimal).unwrap(),
                &block.to_ssz(Preset::Minimal).unwrap(),
                trusted,
                now,
                4,
            )
            .unwrap();
        let orphan = shared
            .put(ObjectKind::BeaconState, Encoding::Ssz, b"pending", Owner::Checkpoint([9; 32]))
            .unwrap();
        let mut record = repository.record(CURRENT).unwrap().unwrap();
        record.state_root[0] ^= 1;
        metadata
            .commit(MetadataBatch::new().put(CURRENT, serde_json::to_vec(&record).unwrap()))
            .unwrap();
        assert!(matches!(repository.recover(now, 4), Err(CheckpointStoreError::InvalidRecord)));
        assert_eq!(shared.owner_objects(Owner::Checkpoint([9; 32])), vec![orphan]);
    }

    #[test]
    fn checkpoint_generations_retain_only_current_and_previous_after_publication() {
        let directory = TempDir::new().unwrap();
        let (mut state, mut block, config, _) = linked_fixture();
        let now = config.clock().unwrap().start_of_slot(72).unwrap();
        let (mut repository, _, shared) = open(directory.path(), config);
        let mut trusted = Checkpoint { epoch: 8, root: [0; 32] };
        for nonce in 0..3u8 {
            state.eth1_deposit_index = u64::from(nonce);
            block.parent_root = [nonce; 32];
            block.state_root = [0; 32];
            state.latest_block_header = block.header(Preset::Minimal).unwrap();
            block.state_root = state.hash_tree_root(Preset::Minimal).unwrap();
            trusted.root = block.hash_tree_root(Preset::Minimal).unwrap();
            repository
                .persist(
                    &state.to_ssz(Preset::Minimal).unwrap(),
                    &block.to_ssz(Preset::Minimal).unwrap(),
                    trusted,
                    now,
                    4,
                )
                .unwrap();
        }
        assert_eq!(shared.owners().len(), 2);
        assert_eq!(shared.collect_garbage().unwrap(), 2);
        assert_eq!(shared.stats().unwrap().physical_objects, 4);
        assert_eq!(repository.recover(now, 4).unwrap().unwrap().checkpoint.checkpoint(), trusted);
    }

    #[test]
    fn uncertain_published_pointer_blocks_cleanup_until_reopen() {
        let directory = TempDir::new().unwrap();
        let (mut state, mut block, config, original) = linked_fixture();
        let now = config.clock().unwrap().start_of_slot(72).unwrap();
        let replacement;
        {
            let (mut repository, metadata, shared) = open(directory.path(), config.clone());
            repository
                .persist(
                    &state.to_ssz(Preset::Minimal).unwrap(),
                    &block.to_ssz(Preset::Minimal).unwrap(),
                    original,
                    now,
                    4,
                )
                .unwrap();
            state.eth1_deposit_index ^= 1;
            block.state_root = [0; 32];
            state.latest_block_header = block.header(Preset::Minimal).unwrap();
            block.state_root = state.hash_tree_root(Preset::Minimal).unwrap();
            replacement = Checkpoint {
                epoch: original.epoch,
                root: block.hash_tree_root(Preset::Minimal).unwrap(),
            };
            reth_embedded_beacon_shared_data::journal::inject_post_rename_failure(
                directory
                    .path()
                    .join("beacon-metadata-v1/journal/snapshot-00000000000000000003.commit"),
            );
            assert!(matches!(
                repository.persist(
                    &state.to_ssz(Preset::Minimal).unwrap(),
                    &block.to_ssz(Preset::Minimal).unwrap(),
                    replacement,
                    now,
                    4,
                ),
                Err(CheckpointStoreError::Metadata(MetadataError::Store(
                    StoreError::PublicationUncertain(_)
                ))),
            ));
            assert!(matches!(
                metadata.get(CURRENT),
                Err(MetadataError::Store(StoreError::JournalPoisoned)),
            ));
            assert!(matches!(
                repository.recover(now, 4),
                Err(CheckpointStoreError::Metadata(MetadataError::Store(
                    StoreError::JournalPoisoned
                ))),
            ));
            assert_eq!(shared.owners().len(), 2);
            assert_eq!(shared.collect_garbage().unwrap(), 0);
            assert_eq!(shared.stats().unwrap().physical_objects, 4);
        }
        let (mut repository, _, shared) = open(directory.path(), config);
        let recovered = repository.recover(now, 4).unwrap().unwrap();
        assert_eq!(recovered.checkpoint.checkpoint(), replacement);
        assert_eq!(shared.stats().unwrap().physical_objects, 4);
    }
}
