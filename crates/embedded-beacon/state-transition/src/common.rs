//! Owned temporary views share common epoch/operation kernels without cloning registries.
//!
//! A guard restores every moved field on success, error or unwinding. The common view's
//! container root must never stand in for the complete fork-specific state root.

use crate::{
    phase0_processing::ProcessingError,
    ssz::{altair, bellatrix, capella, deneb, electra, fulu, gloas, List, Vector},
};

/// False cryptographic policy is available only in the explicitly selected fixture build.
/// Runtime dependency graphs cannot activate it through any private generic invocation.
pub(crate) const fn require_signature_policy<const VERIFY_BLS: bool>() -> Result<(), ProcessingError>
{
    if VERIFY_BLS {
        return Ok(())
    }
    #[cfg(feature = "spec-tests")]
    {
        Ok(())
    }
    #[cfg(not(feature = "spec-tests"))]
    {
        Err(ProcessingError::InvalidParameters(
            "artificial signature policy requires spec-tests feature",
        ))
    }
}

pub(crate) trait AltairFields {
    fn take_common(&mut self) -> altair::BeaconState;
    fn restore_common(&mut self, common: &mut altair::BeaconState);
}

struct CommonGuard<'a, S: AltairFields> {
    target: &'a mut S,
    common: altair::BeaconState,
}

impl<S: AltairFields> Drop for CommonGuard<'_, S> {
    fn drop(&mut self) {
        self.target.restore_common(&mut self.common);
    }
}

pub(crate) fn with_altair<S: AltairFields, T>(
    state: &mut S,
    operation: impl FnOnce(&mut altair::BeaconState) -> Result<T, ProcessingError>,
) -> Result<T, ProcessingError> {
    let common = state.take_common();
    let mut guard = CommonGuard { target: state, common };
    operation(&mut guard.common)
}

macro_rules! impl_altair_fields {
    ($state:ty) => {
        impl AltairFields for $state {
            fn take_common(&mut self) -> altair::BeaconState {
                altair::BeaconState {
                    genesis_time: self.genesis_time,
                    genesis_validators_root: self.genesis_validators_root,
                    slot: self.slot,
                    latest_block_header: self.latest_block_header,
                    eth1_deposit_index: self.eth1_deposit_index,
                    previous_justified_checkpoint: self.previous_justified_checkpoint,
                    current_justified_checkpoint: self.current_justified_checkpoint,
                    finalized_checkpoint: self.finalized_checkpoint,
                    fork: self.fork.clone(),
                    eth1_data: self.eth1_data.clone(),
                    justification_bits: self.justification_bits.clone(),
                    block_roots: Vector::from_vec(std::mem::take(&mut self.block_roots.values)),
                    state_roots: Vector::from_vec(std::mem::take(&mut self.state_roots.values)),
                    historical_roots: List::from_vec(std::mem::take(
                        &mut self.historical_roots.values,
                    )),
                    eth1_data_votes: List::from_vec(std::mem::take(
                        &mut self.eth1_data_votes.values,
                    )),
                    validators: List::from_vec(std::mem::take(&mut self.validators.values)),
                    balances: List::from_vec(std::mem::take(&mut self.balances.values)),
                    randao_mixes: Vector::from_vec(std::mem::take(&mut self.randao_mixes.values)),
                    slashings: Vector::from_vec(std::mem::take(&mut self.slashings.values)),
                    previous_epoch_participation: List::from_vec(std::mem::take(
                        &mut self.previous_epoch_participation.values,
                    )),
                    current_epoch_participation: List::from_vec(std::mem::take(
                        &mut self.current_epoch_participation.values,
                    )),
                    inactivity_scores: List::from_vec(std::mem::take(
                        &mut self.inactivity_scores.values,
                    )),
                    current_sync_committee: altair::SyncCommittee {
                        pubkeys: Vector::from_vec(std::mem::take(
                            &mut self.current_sync_committee.pubkeys.values,
                        )),
                        aggregate_pubkey: self.current_sync_committee.aggregate_pubkey,
                    },
                    next_sync_committee: altair::SyncCommittee {
                        pubkeys: Vector::from_vec(std::mem::take(
                            &mut self.next_sync_committee.pubkeys.values,
                        )),
                        aggregate_pubkey: self.next_sync_committee.aggregate_pubkey,
                    },
                }
            }

            fn restore_common(&mut self, common: &mut altair::BeaconState) {
                self.genesis_time = common.genesis_time;
                self.genesis_validators_root = common.genesis_validators_root;
                self.slot = common.slot;
                self.latest_block_header = common.latest_block_header;
                self.eth1_deposit_index = common.eth1_deposit_index;
                self.previous_justified_checkpoint = common.previous_justified_checkpoint;
                self.current_justified_checkpoint = common.current_justified_checkpoint;
                self.finalized_checkpoint = common.finalized_checkpoint;
                self.fork = common.fork.clone();
                self.eth1_data = common.eth1_data.clone();
                self.justification_bits = common.justification_bits.clone();
                self.block_roots.values = std::mem::take(&mut common.block_roots.values);
                self.state_roots.values = std::mem::take(&mut common.state_roots.values);
                self.historical_roots.values = std::mem::take(&mut common.historical_roots.values);
                self.eth1_data_votes.values = std::mem::take(&mut common.eth1_data_votes.values);
                self.validators.values = std::mem::take(&mut common.validators.values);
                self.balances.values = std::mem::take(&mut common.balances.values);
                self.randao_mixes.values = std::mem::take(&mut common.randao_mixes.values);
                self.slashings.values = std::mem::take(&mut common.slashings.values);
                self.previous_epoch_participation.values =
                    std::mem::take(&mut common.previous_epoch_participation.values);
                self.current_epoch_participation.values =
                    std::mem::take(&mut common.current_epoch_participation.values);
                self.inactivity_scores.values =
                    std::mem::take(&mut common.inactivity_scores.values);
                self.current_sync_committee.pubkeys.values =
                    std::mem::take(&mut common.current_sync_committee.pubkeys.values);
                self.current_sync_committee.aggregate_pubkey =
                    common.current_sync_committee.aggregate_pubkey;
                self.next_sync_committee.pubkeys.values =
                    std::mem::take(&mut common.next_sync_committee.pubkeys.values);
                self.next_sync_committee.aggregate_pubkey =
                    common.next_sync_committee.aggregate_pubkey;
            }
        }
    };
}

impl_altair_fields!(bellatrix::BeaconState);
impl_altair_fields!(capella::BeaconState);
impl_altair_fields!(deneb::BeaconState);

impl_altair_fields!(electra::BeaconState);

impl_altair_fields!(fulu::BeaconState);

impl_altair_fields!(gloas::BeaconState);

// Specification merge completeness compares the entire logical header against empty.
// A nonzero hash is sufficient but not necessary for a nonempty header.
macro_rules! execution_header_is_empty {
    ($header:expr) => {{
        let header = $header;
        header.parent_hash == [0; 32] &&
            header.fee_recipient == [0; 20] &&
            header.state_root == [0; 32] &&
            header.receipts_root == [0; 32] &&
            header.logs_bloom.values.iter().all(|&b| b == 0) &&
            header.prev_randao == [0; 32] &&
            header.block_number == 0 &&
            header.gas_limit == 0 &&
            header.gas_used == 0 &&
            header.timestamp == 0 &&
            header.extra_data.values.is_empty() &&
            header.base_fee_per_gas == [0; 32] &&
            header.block_hash == [0; 32] &&
            header.transactions_root == [0; 32] &&
            header.withdrawals_root == [0; 32]
    }};
    ($header:expr, blob) => {{
        let header = $header;
        $crate::common::execution_header_is_empty!(header) &&
            header.blob_gas_used == 0 &&
            header.excess_blob_gas == 0
    }};
}
pub(crate) use execution_header_is_empty;

/// Borrow only immutable committee source fields from the complete typed state.
pub(crate) trait CommitteeFields {
    fn committee_validators(&self) -> &[crate::ssz::phase0::Validator];
    fn committee_randao_mixes(&self) -> &[reth_embedded_beacon_primitives::Root];
}
macro_rules! impl_committee_fields {
    ($($state:ty),+ $(,)?) => {$(
        impl CommitteeFields for $state {
            fn committee_validators(&self) -> &[crate::ssz::phase0::Validator] { &self.validators.values }
            fn committee_randao_mixes(&self) -> &[reth_embedded_beacon_primitives::Root] { &self.randao_mixes.values }
        }
    )+};
}
impl_committee_fields!(
    crate::ssz::phase0::BeaconState,
    altair::BeaconState,
    bellatrix::BeaconState,
    capella::BeaconState,
    deneb::BeaconState,
    electra::BeaconState,
    fulu::BeaconState,
    gloas::BeaconState
);
/// Compute the actual committee directly from borrowed registry and RANDAO slices.
pub(crate) fn beacon_committee(
    state: &impl CommitteeFields,
    slot: u64,
    index: u64,
    constants: &crate::phase0_processing::Constants,
) -> Result<Vec<u64>, ProcessingError> {
    let epoch = slot / constants.slots_per_epoch;
    let active = state
        .committee_validators()
        .iter()
        .enumerate()
        .filter_map(|(i, v)| crate::phase0_processing::is_active(v, epoch).then_some(i as u64))
        .collect::<Vec<_>>();
    let count = ((active.len() as u64 / constants.slots_per_epoch) /
        constants.target_committee_size)
        .clamp(1, constants.maximum_committees);
    if index >= count {
        return Err(ProcessingError::InvalidState("attestation committee index is out of range"))
    }
    let mix_epoch = crate::phase0_processing::sub(
        crate::phase0_processing::add(epoch, constants.historical_epochs, "committee seed epoch")?,
        2,
        "committee seed lookback",
    )?;
    let mix = *state
        .committee_randao_mixes()
        .get(crate::phase0_processing::position(mix_epoch, constants.historical_epochs)?)
        .ok_or(ProcessingError::InvalidState("RANDAO ring is truncated"))?;
    let seed = crate::compute_seed([1, 0, 0, 0], epoch, mix);
    let position = crate::phase0_processing::add(
        crate::phase0_processing::mul(
            slot % constants.slots_per_epoch,
            count,
            "committee slot position",
        )?,
        index,
        "committee position",
    )?;
    Ok(crate::compute_committee(
        &active,
        seed,
        position,
        crate::phase0_processing::mul(count, constants.slots_per_epoch, "committee count")?,
        constants.shuffle_rounds,
    )?)
}

#[cfg(test)]
mod signature_policy_tests {
    #[test]
    fn artificial_policy_requires_explicit_compile_feature() {
        assert!(super::require_signature_policy::<true>().is_ok());
        assert_eq!(
            super::require_signature_policy::<false>().is_ok(),
            cfg!(feature = "spec-tests")
        );
    }
}

pub(crate) mod sync_committee;

/// Read-only checkpoint calculation for fully validated immutable states.
pub(crate) mod justification;
