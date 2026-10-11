//! Ordered retained-body observations reuse only checked immutable committee preparation.

use std::collections::btree_map::Entry;

use super::*;

/// Infrastructure failures stop exactly as the native importer's independent scalar path does.
pub(crate) fn independently_rejected(error: &StoreError) -> bool {
    matches!(
        error,
        StoreError::UnknownRoot |
            StoreError::InvalidAttestationTime |
            StoreError::InvalidAttestationTarget |
            StoreError::InvalidSlashing |
            StoreError::Processing(ProcessingError::Crypto(_)) |
            StoreError::Kernel(
                ForkChoiceError::UnknownRoot |
                    ForkChoiceError::InvalidAttestationTime |
                    ForkChoiceError::InvalidAttestationTarget |
                    ForkChoiceError::InvalidValidatorIndices
            )
    )
}

pub(crate) fn retain_result(
    results: &mut Vec<(usize, Result<(), StoreError>)>,
    index: usize,
    result: Result<(), StoreError>,
) -> Result<(), StoreError> {
    match result {
        Err(error) if !independently_rejected(&error) => Err(error),
        result => {
            results.push((index, result));
            Ok(())
        }
    }
}

impl Store {
    /// Check genuine retained body votes in original wire order with separate vote rollback.
    /// Consecutive Fulu votes on one checkpoint reuse a fully checked immutable borrowed view.
    /// Independent rejections retain their original indices; infrastructure errors stop early.
    pub fn on_retained_block_attestations(
        &mut self,
        root: Root,
    ) -> Result<Vec<(usize, Result<(), StoreError>)>, StoreError> {
        let mut results = Vec::new();
        self.retained_block_attestations_inner(root, &mut results)?;
        Ok(results)
    }

    pub(crate) fn retained_block_attestations_inner(
        &mut self,
        root: Root,
        results: &mut Vec<(usize, Result<(), StoreError>)>,
    ) -> Result<(), StoreError> {
        let block = self.blocks.get(&root).ok_or(StoreError::UnknownRoot)?.clone();
        let count = match block.as_ref() {
            BeaconBlock::Phase0(value, _) => value.body.attestations.values.len(),
            BeaconBlock::Altair(value, _) => value.body.attestations.values.len(),
            BeaconBlock::Bellatrix(value, _) => value.body.attestations.values.len(),
            BeaconBlock::Capella(value, _) => value.body.attestations.values.len(),
            BeaconBlock::Deneb(value, _) => value.body.attestations.values.len(),
            BeaconBlock::Electra(value, _) => value.body.attestations.values.len(),
            BeaconBlock::Fulu(value, _) => value.body.attestations.values.len(),
            BeaconBlock::Gloas(..) => return Err(StoreError::ValidationUnavailable(Fork::Gloas)),
        };
        results.reserve(count);
        if count == 0 {
            return Ok(())
        }
        let BeaconBlock::Fulu(block, _) = block.as_ref() else {
            for index in 0..count {
                let result = self.on_retained_block_attestation(root, index);
                retain_result(results, index, result)?;
            }
            return Ok(())
        };

        let preset = self.parameters.phase0.preset;
        let mut cursor = 0;
        while cursor < count {
            // Match scalar canonical wire validation before target or timing rejection.
            let first_wire = block.body.attestations.values[cursor].to_ssz(preset)?;
            let first = fulu::Attestation::from_ssz(&first_wire, preset)?;
            let target = first.data.target;
            if let Err(error) = self.validate_later_attestation_target(
                &block.body.attestations.values[cursor].data,
                true,
            ) {
                retain_result(results, cursor, Err(error))?;
                cursor += 1;
                continue
            }
            let end = cursor +
                block.body.attestations.values[cursor..]
                    .iter()
                    .take_while(|vote| vote.data.target == target)
                    .count();
            let (entry, owner) = match self.prepare_checkpoint_state::<true>(target) {
                Ok(prepared) => prepared,
                Err(error) => {
                    retain_result(results, cursor, Err(error))?;
                    cursor += 1;
                    continue
                }
            };
            let BeaconState::Fulu(state, _) = owner.as_ref() else {
                for index in cursor..end {
                    let result = self.on_retained_block_attestation(root, index);
                    retain_result(results, index, result)?;
                }
                cursor = end;
                continue
            };
            let parameters = self
                .transition_parameters
                .clone()
                .ok_or(StoreError::ValidationUnavailable(Fork::Fulu))?;
            let mut checked =
                fulu_processing::CheckedAttestationState::new(state, &parameters.gloas.fulu)?;
            let mut commits = RetainedAttestationCommits {
                store: self,
                key: (target.epoch, target.root),
                entry,
                budget_checked: false,
            };
            for index in cursor..end {
                // Keep canonical original wire bounds before using this exact borrowed proof.
                let wire = block.body.attestations.values[index].to_ssz(preset)?;
                let vote = fulu::Attestation::from_ssz(&wire, preset)?;
                let result = commits
                    .store
                    .validate_later_attestation_target(&vote.data, true)
                    .and_then(|()| {
                        let indexed = checked.validate(&vote)?;
                        commits.commit(&vote.data, indexed.attesting_indices.values)
                    });
                retain_result(results, index, result)?;
            }
            cursor = end;
        }
        Ok(())
    }

    pub(super) fn validate_later_attestation_target(
        &self,
        data: &phase0::AttestationData,
        from_block: bool,
    ) -> Result<(), StoreError> {
        let epoch = self.current_slot() / self.parameters.phase0.slots_per_epoch();
        if (!from_block && ![epoch, epoch.saturating_sub(1)].contains(&data.target.epoch)) ||
            data.target.epoch != data.slot / self.parameters.phase0.slots_per_epoch() ||
            data.slot >= self.current_slot()
        {
            return Err(StoreError::InvalidAttestationTime)
        }
        let block = self.blocks.get(&data.beacon_block_root).ok_or(StoreError::UnknownRoot)?;
        let target_slot = data
            .target
            .epoch
            .checked_mul(self.parameters.phase0.slots_per_epoch())
            .ok_or(StoreError::Overflow)?;
        if block.header()?.slot > data.slot ||
            self.kernel.ancestor(data.beacon_block_root, target_slot)? != data.target.root
        {
            return Err(StoreError::InvalidAttestationTarget)
        }
        Ok(())
    }

    fn record_later_attestation(&mut self, data: &phase0::AttestationData, indices: Vec<u64>) {
        for index in indices {
            if !self.kernel.equivocating.contains(&index) &&
                self.kernel
                    .latest_messages
                    .get(&index)
                    .is_none_or(|previous| data.target.epoch > previous.epoch)
            {
                self.kernel.latest_messages.insert(
                    index,
                    LatestMessage { epoch: data.target.epoch, root: data.beacon_block_root },
                );
                self.latest_message_slots.insert(index, data.slot);
            }
        }
    }

    fn commit_retained_attestation(
        &mut self,
        key: (u64, Root),
        entry: RetainedState<BeaconState>,
        data: &phase0::AttestationData,
        indices: Vec<u64>,
    ) -> Result<(), StoreError> {
        let inserted = match self.checkpoint_states.entry(key) {
            Entry::Vacant(vacant) => {
                vacant.insert(entry);
                true
            }
            Entry::Occupied(_) => false,
        };
        // The preceding caller verified canonical wire, target, committee and every signature.
        // Schema retention is the only remaining fallible operation and does not depend on
        // latest-message maps. Check it before those infallible updates, rolling back only the
        // prospective checkpoint entry on failure. Earlier successfully verified votes remain.
        if let Err(error) = self.check_retention_budget() {
            if inserted {
                self.checkpoint_states.remove(&key);
            }
            return Err(error)
        }
        self.record_later_attestation(data, indices);
        Ok(())
    }
}

/// Exclusive commit scope for one exact checkpoint and its immutable preparation owner.
/// Only latest-message maps change after the first successful commit. Those maps do not
/// contribute to the schema budget, so its checked allocation total remains unchanged.
/// The scope cannot insert another checkpoint or mutate any retained schema between votes.
struct RetainedAttestationCommits<'a> {
    store: &'a mut Store,
    key: (u64, Root),
    entry: RetainedState<BeaconState>,
    budget_checked: bool,
}

impl RetainedAttestationCommits<'_> {
    fn commit(
        &mut self,
        data: &phase0::AttestationData,
        indices: Vec<u64>,
    ) -> Result<(), StoreError> {
        debug_assert_eq!(self.key, (data.target.epoch, data.target.root));
        if self.budget_checked {
            self.store.record_later_attestation(data, indices);
        } else {
            // This first commit retains the scalar preflight and prospective-entry rollback,
            // including when the checkpoint was already occupied or no message will change.
            self.store.commit_retained_attestation(self.key, self.entry.clone(), data, indices)?;
            self.budget_checked = true;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fulu_store() -> Store {
        let base =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("testdata/official-anchors/fulu");
        let state = BeaconState::from_ssz(
            &std::fs::read(base.join("anchor_state.ssz")).unwrap(),
            Fork::Fulu,
            Preset::Minimal,
        )
        .unwrap();
        let block = BeaconBlock::from_ssz(
            &std::fs::read(base.join("anchor_block.ssz")).unwrap(),
            Fork::Fulu,
            Preset::Minimal,
        )
        .unwrap();
        Store::get_store(state, block, StoreParameters::for_preset(Preset::Minimal)).unwrap()
    }

    fn data(root: Root, epoch: u64) -> phase0::AttestationData {
        phase0::AttestationData {
            slot: epoch * Phase0Parameters::for_preset(Preset::Minimal).slots_per_epoch(),
            index: 0,
            beacon_block_root: root,
            source: Checkpoint { epoch: 0, root },
            target: Checkpoint { epoch, root },
        }
    }

    #[test]
    fn checkpoint_preparation_owns_exact_state_without_publishing_before_vote_commit() {
        let mut store = fulu_store();
        let root = store.get_head().unwrap().root;
        let target = Checkpoint { epoch: 0, root };
        store.checkpoint_states.remove(&(target.epoch, target.root));
        let checkpoints = store.checkpoint_states.keys().copied().collect::<Vec<_>>();
        let messages = store.kernel.latest_messages.clone();
        let mut scalar = store.clone();
        let scalar_read = scalar.checkpoint_state_inner::<true>(target).unwrap();
        let (entry, read) = store.prepare_checkpoint_state::<true>(target).unwrap();
        assert_eq!(read.hash_tree_root().unwrap(), scalar_read.hash_tree_root().unwrap());
        assert_eq!(store.checkpoint_states.keys().copied().collect::<Vec<_>>(), checkpoints);
        assert_eq!(store.kernel.latest_messages, messages);

        // An infrastructure rejection cannot publish the prospective checkpoint, even while
        // its immutable preparation guard remains live. Ordinary successful commit still can.
        store.parameters.max_retained_schema_bytes = 1;
        assert!(matches!(
            store.commit_retained_attestation((0, root), entry.clone(), &data(root, 0), vec![0]),
            Err(StoreError::ResourceLimit)
        ));
        assert_eq!(store.checkpoint_states.keys().copied().collect::<Vec<_>>(), checkpoints);
        assert_eq!(store.kernel.latest_messages, messages);
        store.parameters.max_retained_schema_bytes = scalar.parameters.max_retained_schema_bytes;
        store.commit_retained_attestation((0, root), entry, &data(root, 0), vec![0]).unwrap();
        let committed = store.read_checkpoint_state(target).unwrap().unwrap();
        assert_eq!(committed.hash_tree_root().unwrap(), read.hash_tree_root().unwrap());
    }

    #[test]
    fn retained_vote_commit_matches_ordered_latest_messages_without_full_store_copy() {
        let mut store = fulu_store();
        let root = store.get_head().unwrap().root;
        let entry = store.checkpoint_states.values().next().unwrap().clone();
        store.kernel.equivocating.insert(2);
        store.kernel.latest_messages.insert(1, LatestMessage { epoch: 2, root });
        store.latest_message_slots.insert(1, 16);
        let mut scalar = store.clone();
        for epoch in [1, 1, 3] {
            let data = data(root, epoch);
            let key = (epoch, root);
            let indices = vec![0, 1, 2, 0];
            let mut candidate = scalar.clone();
            candidate.checkpoint_states.entry(key).or_insert_with(|| entry.clone());
            candidate.record_later_attestation(&data, indices.clone());
            candidate.check_retention_budget().unwrap();
            scalar = candidate;
            store.commit_retained_attestation(key, entry.clone(), &data, indices).unwrap();
            assert_eq!(store.kernel.latest_messages, scalar.kernel.latest_messages);
            assert_eq!(store.latest_message_slots, scalar.latest_message_slots);
            assert_eq!(
                store.checkpoint_states.keys().collect::<Vec<_>>(),
                scalar.checkpoint_states.keys().collect::<Vec<_>>()
            );
        }
        assert!(!store.kernel.latest_messages.contains_key(&2));
    }

    #[test]
    fn failed_retained_vote_removes_only_new_checkpoint_and_preserves_prior_votes() {
        let mut store = fulu_store();
        let root = store.get_head().unwrap().root;
        let occupied = *store.checkpoint_states.keys().next().unwrap();
        let entry = store.checkpoint_states.get(&occupied).unwrap().clone();
        store
            .commit_retained_attestation((1, root), entry.clone(), &data(root, 1), vec![0])
            .unwrap();
        let messages = store.kernel.latest_messages.clone();
        let slots = store.latest_message_slots.clone();
        let checkpoints = store.checkpoint_states.keys().copied().collect::<Vec<_>>();
        store.parameters.max_retained_schema_bytes = 1;
        for key in [(3, root), occupied] {
            assert!(matches!(
                store.commit_retained_attestation(key, entry.clone(), &data(root, 3), vec![0, 1]),
                Err(StoreError::ResourceLimit)
            ));
            assert_eq!(store.kernel.latest_messages, messages);
            assert_eq!(store.latest_message_slots, slots);
            assert_eq!(store.checkpoint_states.keys().copied().collect::<Vec<_>>(), checkpoints);
        }
    }

    #[test]
    fn group_budget_preflight_preserves_scalar_message_order_and_exact_schema_total() {
        let mut store = fulu_store();
        let root = store.get_head().unwrap().root;
        let entry = store.checkpoint_states.values().next().unwrap().clone();
        store.kernel.equivocating.insert(2);
        store.kernel.latest_messages.insert(1, LatestMessage { epoch: 2, root });
        let mut scalar = store.clone();
        let vote = data(root, 1);
        let key = (vote.target.epoch, vote.target.root);
        let mut commits = RetainedAttestationCommits {
            store: &mut store,
            key,
            entry: entry.clone(),
            budget_checked: false,
        };
        let mut checked_bytes = None;
        for indices in [vec![0, 1, 2, 0], vec![3, 0], vec![3, 4, 2]] {
            scalar.commit_retained_attestation(key, entry.clone(), &vote, indices.clone()).unwrap();
            commits.commit(&vote, indices).unwrap();
            assert!(commits.budget_checked);
            let bytes = commits.store.retained_schema_bytes().unwrap();
            assert_eq!(*checked_bytes.get_or_insert(bytes), bytes);
            assert_eq!(commits.store.kernel.latest_messages, scalar.kernel.latest_messages);
            assert_eq!(commits.store.latest_message_slots, scalar.latest_message_slots);
            assert_eq!(
                commits.store.checkpoint_states.keys().copied().collect::<Vec<_>>(),
                scalar.checkpoint_states.keys().copied().collect::<Vec<_>>()
            );
        }
        assert!(!commits.store.kernel.latest_messages.contains_key(&2));
    }

    #[test]
    fn rejected_first_group_vote_never_authorizes_later_commits() {
        for occupied in [false, true] {
            let mut store = fulu_store();
            let root = store.get_head().unwrap().root;
            let entry = store.checkpoint_states.values().next().unwrap().clone();
            let epoch = u64::from(!occupied);
            let key = (epoch, root);
            let vote = data(root, epoch);
            let checkpoints = store.checkpoint_states.keys().copied().collect::<Vec<_>>();
            let messages = store.kernel.latest_messages.clone();
            store.parameters.max_retained_schema_bytes = 1;
            let mut commits =
                RetainedAttestationCommits { store: &mut store, key, entry, budget_checked: false };
            for indices in [vec![0], vec![1]] {
                assert!(matches!(commits.commit(&vote, indices), Err(StoreError::ResourceLimit)));
                assert!(!commits.budget_checked);
                assert_eq!(commits.store.kernel.latest_messages, messages);
                assert_eq!(
                    commits.store.checkpoint_states.keys().copied().collect::<Vec<_>>(),
                    checkpoints
                );
            }
        }
    }
}
