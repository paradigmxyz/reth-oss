//! Preparation owned only by the exact post-slashing, pre-exit native attestation loop.
//!
//! Attestations mutate participation, balances and Gloas payments, never the validator
//! registry, effective balances, RANDAO, fork or slot used by this private preparation.
//! No signature, participant result, reward or mutable participation is cached.

use super::*;
use std::collections::BTreeMap;

#[derive(Default)]
pub(super) struct AttestationInputs {
    epochs: BTreeMap<u64, CommitteeInputs>,
    active_balance: Option<u64>,
}

impl AttestationInputs {
    pub(super) fn indices(
        &mut self,
        state: &BeaconState,
        operation: &Attestation,
        constants: &Constants,
    ) -> Result<Vec<u64>, ProcessingError> {
        attesting_indices_with(operation, true, |committee_index| {
            let epoch = operation.data.slot / constants.slots_per_epoch;
            if !self.epochs.contains_key(&epoch) && self.epochs.len() == 2 {
                self.epochs.clear();
            }
            let inputs = match self.epochs.entry(epoch) {
                std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(CommitteeInputs::new(state, operation.data.slot, constants)?)
                }
            };
            inputs.committee(operation.data.slot, committee_index, constants)
        })
    }

    pub(super) fn active_balance(
        &mut self,
        state: &BeaconState,
        constants: &Constants,
    ) -> Result<u64, ProcessingError> {
        if let Some(balance) = self.active_balance {
            return Ok(balance)
        }
        let balance = common::total_active_balance(state, constants)?;
        self.active_balance = Some(balance);
        Ok(balance)
    }
}
