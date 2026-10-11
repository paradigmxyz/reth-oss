//! Durable sealed genesis originals, distinctly recovered without checkpoint/WS conversion.
//!
//! Complete data and their owners become durable before the private genesis descriptor. Saved
//! flags and roots are only hints; recovery freshly runs the concrete full genesis authenticator.

use super::*;
use crate::{BootstrapDecodeLimits, VerifiedGenesis};

impl AdmittedStateRepository {
    /// Independently reauthenticate sealed original genesis under this repository's exact network.
    /// Full SSZ/crypto/storage work belongs on the dedicated import owner or a blocking task.
    pub fn persist_genesis(
        &self,
        genesis: &VerifiedGenesis,
        limits: BootstrapDecodeLimits,
    ) -> Result<(), PersistenceError> {
        if genesis.config_digest() != self.network.consensus_digest {
            return Err(PersistenceError::Identity)
        }
        let actual = VerifiedGenesis::authenticate(
            self.network.config.clone(),
            genesis.original_state_ssz(),
            limits,
        )?;
        if actual.config_digest() != self.network.consensus_digest ||
            actual.state_root() != genesis.state_root() ||
            actual.block_root() != genesis.block_root() ||
            actual.fork() != genesis.fork() ||
            actual.block_ssz() != genesis.block_ssz()
        {
            return Err(PersistenceError::Identity)
        }
        let original = actual.block_ssz();
        let root = actual.block_root();
        let state_root = actual.state_root();
        let fork = actual.fork();
        let (state, block) = actual.into_import_inputs(&self.network.config)?;
        self.check_state(&state)?;
        self.check_bytes(&original)?;
        let record = Admission {
            block_root: root,
            parent_root: [0; 32],
            state_root,
            slot: 0,
            fork,
            original: ObjectId::new(ObjectKind::BeaconBlock, Encoding::Ssz, &original),
            format: CheckpointBlockFormat::Unsigned,
            execution: execution(&block),
            envelope: None,
        };
        let anchor = Anchor {
            checkpoint: Checkpoint { epoch: 0, root },
            state_root,
            slot: 0,
            boundary: None,
            genesis: true,
        };
        let mut inner = self.lock()?;
        if inner.anchor.as_ref().is_some_and(|existing| existing != &anchor) {
            return Err(PersistenceError::ConflictingAnchor)
        }
        self.check_capacity(&inner, root)?;
        if let Some(existing) = inner.records.get(&root) {
            if existing != &record {
                return Err(PersistenceError::Linkage)
            }
            self.check_saved_record(existing)?;
            return Ok(())
        }
        self.compact.persist_state(&state, state_root)?;
        self.shared.put(
            ObjectKind::BeaconBlock,
            Encoding::Ssz,
            &original,
            Owner::Checkpoint(root),
        )?;
        let mut next = Saved::capture(self.network.consensus_digest, &inner);
        next.records.push(record.clone());
        next.anchor = Some(anchor.clone());
        inner.journal.commit(&next)?;
        inner.records.insert(root, record);
        inner.anchor = Some(anchor);
        Ok(())
    }

    /// Recover original slot-zero trust input through fresh complete native genesis validation.
    /// No current-time or max-age argument can turn it into a weak-subjectivity checkpoint.
    pub fn recover_genesis(
        &self,
        limits: BootstrapDecodeLimits,
    ) -> Result<Option<VerifiedGenesis>, PersistenceError> {
        let inner = self.lock()?;
        let Some(anchor) = &inner.anchor else { return Ok(None) };
        if !anchor.genesis {
            return Err(PersistenceError::WrongAnchorKind)
        }
        if anchor.slot != 0 || anchor.checkpoint.epoch != 0 || anchor.boundary.is_some() {
            return Err(PersistenceError::Linkage)
        }
        let record = inner.records.get(&anchor.checkpoint.root).ok_or(PersistenceError::Linkage)?;
        self.check_saved_record(record)?;
        if record.slot != 0 ||
            record.state_root != anchor.state_root ||
            record.format != CheckpointBlockFormat::Unsigned ||
            record.parent_root != [0; 32] ||
            record.execution.is_some() ||
            record.envelope.is_some()
        {
            return Err(PersistenceError::Linkage)
        }
        let state = self
            .compact
            .lease(record.state_root, record.fork, self.network.preset())?
            .ok_or(PersistenceError::Linkage)?;
        let exact = state.state().to_ssz()?;
        if exact.len() > limits.max_state_bytes {
            return Err(crate::GenesisError::Bounds.into())
        }
        let original = self
            .shared
            .get_bounded(
                record.original,
                self.network.config.max_payload_size.min(limits.max_block_bytes),
            )?
            .ok_or(PersistenceError::Linkage)?;
        let actual =
            VerifiedGenesis::authenticate(self.network.config.clone(), exact.into(), limits)?;
        if actual.config_digest() != self.network.consensus_digest ||
            actual.state_root() != record.state_root ||
            actual.block_root() != record.block_root ||
            actual.fork() != record.fork ||
            actual.block_ssz().as_ref() != original.as_ref()
        {
            return Err(PersistenceError::Linkage)
        }
        Ok(Some(actual))
    }
}

#[cfg(test)]
mod tests;
