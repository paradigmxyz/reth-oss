//! Independently pinned originals extend only the already committed finalized serving chain.

use super::*;
use crate::import_coordinator::{HistoricalContextReceipt, UnsignedCustodyAnchor};

impl RepositoryStateSource {
    pub(crate) fn retain_unsigned_context(
        &self,
        context: &UnsignedCustodyAnchor,
    ) -> Result<(), PersistenceError> {
        if context.config_digest() != self.repository.network.consensus_digest {
            return Err(PersistenceError::Identity)
        }
        let block = context.block().map_err(|_| PersistenceError::Linkage)?;
        if block.hash_tree_root()? != context.root() || block.header()?.slot != context.slot() {
            return Err(PersistenceError::Linkage)
        }
        self.repository.check_bytes(context.original())?;
        // One original stays pinned independently of hot graph pruning. Each reopen must mint
        // this context from native initialization; the durable owner grants no registry authority.
        self.repository.shared.put_owned_batch(
            unsigned_context_owner(),
            &[(ObjectKind::BeaconBlock, Encoding::Ssz, context.original().to_vec())],
        )?;
        Ok(())
    }
    pub(crate) fn ensure_unsigned_context(
        &self,
        context: &UnsignedCustodyAnchor,
    ) -> Result<(), PersistenceError> {
        self.repository.shared.ensure_healthy()?;
        let object = ObjectId::new(ObjectKind::BeaconBlock, Encoding::Ssz, context.original());
        if self.repository.shared.owner_objects(unsigned_context_owner()) != vec![object] {
            return Err(PersistenceError::Linkage)
        }
        let original = self
            .repository
            .shared
            .get_bounded(object, self.repository.network.config.max_payload_size)?
            .ok_or(PersistenceError::Linkage)?;
        if original.as_ref() != context.original() {
            return Err(PersistenceError::Linkage)
        }
        Ok(())
    }

    pub(crate) fn custody_missing_signed_anchor(
        &self,
        actual: StoreSnapshot,
        context: &UnsignedCustodyAnchor,
        floor: u64,
    ) -> Result<Option<(Root, u64)>, PersistenceError> {
        let contexts =
            self.sidecar_contexts.lock().map_err(|_| PersistenceError::CoordinatorPoisoned)?;
        contexts.healthy(&self.repository)?;
        if contexts.last_anchor != Some(actual.finalized_checkpoint.root) ||
            contexts.staged_snapshot.is_some() ||
            context.slot() < floor ||
            context.is_genuine_genesis()
        {
            return Ok(None)
        }
        if context.config_digest() != self.repository.network.consensus_digest {
            return Err(PersistenceError::Identity)
        }
        if contexts.selected.get(&context.slot()) == Some(&context.root()) &&
            contexts.verified_keys.contains_key(&context.root()) &&
            contexts
                .serve(&self.repository, &BlockRequest::Roots(vec![context.root()]))?
                .is_some_and(|blocks| blocks.len() == 1)
        {
            return Ok(None)
        }
        // After finality advances, the nearest selected signed descendant must commit directly
        // to this missing bootstrap original. Stored inventories alone cannot select the root.
        let connected = if actual.finalized_checkpoint.root == context.root() {
            true
        } else {
            let Some((_, descendant)) =
                contexts.selected.range(context.slot().saturating_add(1)..).next()
            else {
                return Ok(None)
            };
            if !contexts.verified_keys.contains_key(descendant) {
                return Ok(None)
            }
            contexts
                .original(&self.repository, *descendant)?
                .is_some_and(|(wire, _)| wire.parent_root() == context.root())
        };
        Ok(connected.then_some((context.root(), context.slot().saturating_add(1))))
    }
    pub(crate) fn custody_missing_parent(
        &self,
        actual: StoreSnapshot,
        floor: u64,
    ) -> Result<Option<(Root, u64)>, PersistenceError> {
        let contexts =
            self.sidecar_contexts.lock().map_err(|_| PersistenceError::CoordinatorPoisoned)?;
        contexts.healthy(&self.repository)?;
        if contexts.last_anchor != Some(actual.finalized_checkpoint.root) ||
            contexts.staged_snapshot.is_some()
        {
            return Ok(None)
        }
        let Some((slot, root)) = contexts.selected.first_key_value() else {
            let Some((format, original)) =
                self.repository.original_block(actual.finalized_checkpoint.root)?
            else {
                return Ok(None)
            };
            let header = match format {
                CheckpointBlockFormat::Signed => {
                    let wire =
                        WireBlock::from_ssz(original.to_vec(), &self.repository.network.config)
                            .map_err(|_| PersistenceError::Linkage)?;
                    if wire.root() != actual.finalized_checkpoint.root {
                        return Err(PersistenceError::Linkage)
                    }
                    return Ok(Some((wire.parent_root(), wire.slot())))
                }
                CheckpointBlockFormat::Unsigned => {
                    let slot = u64::from_le_bytes(
                        original
                            .get(..8)
                            .ok_or(PersistenceError::Linkage)?
                            .try_into()
                            .map_err(|_| PersistenceError::Linkage)?,
                    );
                    let fork = self
                        .repository
                        .network
                        .config
                        .fork_at_slot(slot)
                        .map_err(|_| PersistenceError::Identity)?
                        .fork;
                    let block =
                        BeaconBlock::from_ssz(&original, fork, self.repository.network.preset())?;
                    if block.hash_tree_root()? != actual.finalized_checkpoint.root {
                        return Err(PersistenceError::Linkage)
                    }
                    block.header()?
                }
            };
            return Ok(Some((actual.finalized_checkpoint.root, header.slot.saturating_add(1))))
        };
        if *slot <= floor {
            return Ok(Some(([0; 32], *slot)))
        }
        let Some((wire, _)) = contexts.original(&self.repository, *root)? else {
            return Err(PersistenceError::Linkage)
        };
        Ok(Some((wire.parent_root(), wire.slot())))
    }
    pub(crate) fn retain_historical_context(
        &self,
        receipt: &HistoricalContextReceipt,
    ) -> Result<(), PersistenceError> {
        let mut contexts =
            self.sidecar_contexts.lock().map_err(|_| PersistenceError::CoordinatorPoisoned)?;
        contexts.healthy(&self.repository)?;
        if receipt.digest() != self.repository.network.consensus_digest ||
            contexts.last_anchor != Some(receipt.boundary()) ||
            contexts.staged_snapshot.is_some()
        {
            return Err(PersistenceError::Identity)
        }
        let wire = receipt.original();
        // This private seal has already checked this owner's exact expected parent and a fresh
        // proposer signature against its actual native registry. Persist the original before
        // extending the data-serving index; no native graph or execution state is created.
        contexts.persist_wire(&self.repository, wire, receipt.key())?;
        if contexts
            .selected
            .insert(wire.slot(), wire.root())
            .is_some_and(|root| root != wire.root())
        {
            return Err(PersistenceError::Linkage)
        }
        Ok(())
    }
}

fn unsigned_context_owner() -> Owner {
    Owner::Recovery(sha256(b"embedded-beacon-unsigned-sidecar-context-v1"))
}
