//! Ordered Gloas body votes share only checked immutable native checkpoint preparation.

use super::*;
use crate::store::body_attestations::retain_result;

impl GloasStore {
    /// Authenticate actual retained votes in wire order with independent rollback per vote.
    /// Migrated historical checkpoint schemas keep their original strict scalar validation path.
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
        let count = block.body.attestations.values.len();
        results.reserve(count);
        let parameters = self.processing.clone();
        let mut cursor = 0;
        while cursor < count {
            let first = &block.body.attestations.values[cursor];
            first.hash_tree_root(parameters.preset())?;
            let target = first.data.target;
            if let Err(error) = self.validate_attestation_target(&first.data, true) {
                retain_result(results, cursor, Err(error))?;
                cursor += 1;
                continue
            }
            let end = cursor +
                block.body.attestations.values[cursor..]
                    .iter()
                    .take_while(|vote| vote.data.target == target)
                    .count();
            let mut prepared = self.clone();
            if let Err(error) = prepared.ensure_checkpoint::<true>(target) {
                retain_result(results, cursor, Err(error))?;
                cursor += 1;
                continue
            }
            let owner = prepared.checkpoint_view(target)?;
            let GraphStateRead::Gloas(state) = &owner else {
                for index in cursor..end {
                    let result = self.on_retained_block_attestation(root, index);
                    retain_result(results, index, result)?;
                }
                cursor = end;
                continue
            };
            let mut checked = gloas_processing::CheckedAttestationState::new(state, &parameters)?;
            for index in cursor..end {
                let vote = &block.body.attestations.values[index];
                vote.hash_tree_root(parameters.preset())?;
                let result = self.validate_attestation_target(&vote.data, true).and_then(|()| {
                    let indexed = checked.validate(vote)?;
                    let mut candidate = self.clone();
                    let key = (target.epoch, target.root);
                    let entry = prepared
                        .checkpoint_states
                        .get(&key)
                        .ok_or(StoreError::UnknownRoot)?
                        .clone();
                    candidate.checkpoint_states.entry(key).or_insert(entry);
                    candidate.record_verified_body_attestation(
                        &vote.data,
                        indexed.attesting_indices.values,
                    );
                    candidate.check_budget()?;
                    *self = candidate;
                    Ok(())
                });
                retain_result(results, index, result)?;
            }
            cursor = end;
        }
        Ok(())
    }

    pub(super) fn validate_attestation_target(
        &self,
        data: &gloas::AttestationData,
        from_block: bool,
    ) -> Result<(), StoreError> {
        let epoch = self.current_slot() / self.slots_per_epoch();
        if (!from_block && ![epoch, epoch.saturating_sub(1)].contains(&data.target.epoch)) ||
            data.target.epoch != data.slot / self.slots_per_epoch() ||
            data.slot >= self.current_slot()
        {
            return Err(StoreError::InvalidAttestationTime)
        }
        self.block_header(data.target.root)?;
        let block = self.block_header(data.beacon_block_root)?;
        if block.slot > data.slot ||
            data.index > 1 ||
            (block.slot == data.slot && data.index != 0) ||
            (data.index == 1 && !self.is_payload_verified(data.beacon_block_root)) ||
            (!self.blocks.contains_key(&data.beacon_block_root) && data.index != 0) ||
            self.get_checkpoint_block(data.beacon_block_root, data.target.epoch)? !=
                data.target.root
        {
            return Err(StoreError::InvalidAttestationTarget)
        }
        Ok(())
    }

    fn record_verified_body_attestation(
        &mut self,
        data: &gloas::AttestationData,
        indices: Vec<u64>,
    ) {
        for index in indices {
            if !self.equivocating.contains(&index) &&
                self.latest_messages.get(&index).is_none_or(|previous| data.slot > previous.slot)
            {
                self.latest_messages.insert(
                    index,
                    LatestMessage {
                        slot: data.slot,
                        root: data.beacon_block_root,
                        payload_present: data.index == 1,
                    },
                );
            }
        }
    }
}

#[cfg(test)]
pub(super) mod tests;
