//! Atomic Bellatrix processing with mandatory execution verification during signed import.
//!
//! Callers enforce the trusted fork schedule, terminal-block fork-choice rules and resource
//! limits. Common epoch and operation data move through a restoring guard; complete state
//! commitments always use the actual Bellatrix schema.

use crate::{
    altair_processing::{self, AltairParameters},
    common::with_altair,
    phase0_processing::{add, mul, position, Constants, ProcessingError},
    ssz::{
        altair,
        bellatrix::{BeaconState, ExecutionPayload, ExecutionPayloadHeader},
        List, Preset, Ssz, Vector,
    },
};
use reth_embedded_beacon_primitives::Root;
use std::future::Future;
use thiserror::Error;

mod block;
mod slot_cache;

#[cfg(feature = "spec-tests")]
pub use block::spec_only;

pub use block::{
    get_attesting_indices, get_beacon_proposer_index, get_indexed_attestation,
    is_valid_indexed_attestation, process_attestation, process_attester_slashing,
    process_block_header, process_deposit, process_eth1_data, process_execution_payload,
    process_operations, process_proposer_slashing, process_randao, process_signed_block,
    process_sync_aggregate, process_voluntary_exit, state_transition, verify_block_signature,
};

/// Trusted network configuration required by Bellatrix processing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BellatrixParameters {
    /// Inherited churn, participation and inactivity configuration.
    pub altair: AltairParameters,
    /// Version written during the scheduled Bellatrix upgrade.
    pub fork_version: [u8; 4],
    /// Clock duration used for the exact execution payload timestamp.
    pub seconds_per_slot: u64,
}

impl BellatrixParameters {
    /// Exact pinned preset and default network configuration.
    pub const fn for_preset(preset: Preset) -> Self {
        Self {
            altair: AltairParameters::for_preset(preset),
            fork_version: match preset {
                Preset::Mainnet => [2, 0, 0, 0],
                Preset::Minimal => [2, 0, 0, 1],
            },
            seconds_per_slot: match preset {
                Preset::Mainnet => 12,
                Preset::Minimal => 6,
            },
        }
    }

    /// Reject invalid clock configuration and inherited zero divisors.
    pub fn validate(&self) -> Result<(), ProcessingError> {
        self.altair.validate()?;
        if self.seconds_per_slot == 0 {
            return Err(ProcessingError::InvalidParameters("seconds per slot must be nonzero"))
        }
        Ok(())
    }

    /// Exact slots per epoch under the selected preset.
    pub const fn slots_per_epoch(&self) -> u64 {
        self.altair.slots_per_epoch()
    }

    /// Exact SSZ and deterministic algorithm preset.
    pub const fn preset(&self) -> Preset {
        self.altair.phase0.preset
    }
}

/// Consensus context bound to the exact payload submitted during import.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExecutionContext {
    /// Slot of the candidate beacon block.
    pub slot: u64,
    /// Actual parent root of the candidate beacon block.
    pub parent_beacon_block_root: Root,
    /// Committed previous execution hash, absent before the merge transition.
    pub previous_execution_payload_hash: Option<Root>,
}

/// Mandatory execution verification invoked inside the atomic signed transition.
///
/// `Ok(())` means the actual execution engine returned VALID for this exact candidate,
/// with a computed block hash and matching latest valid hash. ACCEPTED, SYNCING, timeout,
/// and any malformed or mismatched response are errors. An implementation must also
/// enforce its configured execution chain identity and versioned request policy.
/// Production adapters call the real typed execution API; artificial adapters belong
/// exclusively to the specification runner and cannot select a launcher runtime path.
pub trait ExecutionEngine: Send + Sync {
    /// Execution rejection, transport failure or invalid response.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Verify first-payload terminal eligibility using configured override activation or
    /// authenticated proof of work block/parent total difficulties across the terminal threshold.
    /// This check is separate from new-payload VALID and must fail if evidence is absent.
    fn verify_terminal_block<'a>(
        &'a self,
        payload: &'a ExecutionPayload,
        context: ExecutionContext,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;

    /// Verify the exact candidate with the actual execution engine before publishing state.
    fn verify_payload<'a>(
        &'a self,
        payload: &'a ExecutionPayload,
        context: ExecutionContext,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;
}

/// Failure leaves the caller's beacon state unchanged.
#[derive(Debug, Error)]
pub enum ImportError<E: std::error::Error + 'static> {
    /// Consensus structure, signature or transition rejection.
    #[error(transparent)]
    Consensus(#[from] ProcessingError),
    /// The mandatory execution engine did not validate the exact candidate.
    #[error("execution verification failed: {0}")]
    Execution(E),
}

const fn constants_for_preset(preset: Preset) -> Constants {
    let mut constants = Constants::for_preset(preset);
    constants.inactivity_penalty_quotient = 16_777_216;
    constants.minimum_slashing_penalty_quotient = 32;
    constants.proportional_slashing_multiplier = 3;
    constants
}

fn checked_constants(
    state: &BeaconState,
    parameters: &BellatrixParameters,
) -> Result<Constants, ProcessingError> {
    parameters.validate()?;
    state.hash_tree_root(parameters.preset())?;
    let count = state.validators.values.len();
    if [
        state.balances.values.len(),
        state.previous_epoch_participation.values.len(),
        state.current_epoch_participation.values.len(),
        state.inactivity_scores.values.len(),
    ]
    .iter()
    .any(|&len| len != count)
    {
        return Err(ProcessingError::InvalidState("Bellatrix validator-indexed list lengths differ"))
    }
    Ok(constants_for_preset(parameters.preset()))
}

fn transact<T>(
    state: &mut BeaconState,
    parameters: &BellatrixParameters,
    operation: impl FnOnce(&mut BeaconState, &Constants) -> Result<T, ProcessingError>,
) -> Result<T, ProcessingError> {
    let constants = checked_constants(state, parameters)?;
    let mut candidate = state.clone();
    let result = operation(&mut candidate, &constants)?;
    checked_constants(&candidate, parameters)?;
    *state = candidate;
    Ok(result)
}

/// Exact irregular Altair-to-Bellatrix upgrade; the caller enforces the scheduled boundary.
pub fn upgrade_to_bellatrix(
    pre: &altair::BeaconState,
    parameters: &BellatrixParameters,
) -> Result<BeaconState, ProcessingError> {
    parameters.validate()?;
    altair_processing::validate_state(pre, &parameters.altair)?;
    let post = BeaconState {
        genesis_time: pre.genesis_time,
        genesis_validators_root: pre.genesis_validators_root,
        slot: pre.slot,
        fork: crate::ssz::phase0::ForkInfo {
            previous_version: pre.fork.current_version,
            current_version: parameters.fork_version,
            epoch: pre.slot / parameters.slots_per_epoch(),
        },
        latest_block_header: pre.latest_block_header,
        block_roots: pre.block_roots.clone(),
        state_roots: pre.state_roots.clone(),
        historical_roots: pre.historical_roots.clone(),
        eth1_data: pre.eth1_data.clone(),
        eth1_data_votes: pre.eth1_data_votes.clone(),
        eth1_deposit_index: pre.eth1_deposit_index,
        validators: pre.validators.clone(),
        balances: pre.balances.clone(),
        randao_mixes: pre.randao_mixes.clone(),
        slashings: pre.slashings.clone(),
        previous_epoch_participation: pre.previous_epoch_participation.clone(),
        current_epoch_participation: pre.current_epoch_participation.clone(),
        justification_bits: pre.justification_bits.clone(),
        previous_justified_checkpoint: pre.previous_justified_checkpoint,
        current_justified_checkpoint: pre.current_justified_checkpoint,
        finalized_checkpoint: pre.finalized_checkpoint,
        inactivity_scores: pre.inactivity_scores.clone(),
        current_sync_committee: crate::ssz::bellatrix::SyncCommittee {
            pubkeys: pre.current_sync_committee.pubkeys.clone(),
            aggregate_pubkey: pre.current_sync_committee.aggregate_pubkey,
        },
        next_sync_committee: crate::ssz::bellatrix::SyncCommittee {
            pubkeys: pre.next_sync_committee.pubkeys.clone(),
            aggregate_pubkey: pre.next_sync_committee.aggregate_pubkey,
        },
        latest_execution_payload_header: empty_execution_header(),
    };
    checked_constants(&post, parameters)?;
    Ok(post)
}

fn empty_execution_header() -> ExecutionPayloadHeader {
    ExecutionPayloadHeader {
        parent_hash: [0; 32],
        fee_recipient: [0; 20],
        state_root: [0; 32],
        receipts_root: [0; 32],
        logs_bloom: Vector::from_vec(vec![0; 256]),
        prev_randao: [0; 32],
        block_number: 0,
        gas_limit: 0,
        gas_used: 0,
        timestamp: 0,
        extra_data: List::default(),
        base_fee_per_gas: [0; 32],
        block_hash: [0; 32],
        transactions_root: [0; 32],
    }
}

/// Whether the full committed execution header differs from the specified empty value.
pub fn is_merge_transition_complete(state: &BeaconState) -> bool {
    state.latest_execution_payload_header != empty_execution_header()
}

/// Whether execution processing is enabled for this exact state and block payload.
pub fn is_execution_enabled(state: &BeaconState, payload: &ExecutionPayload) -> bool {
    is_merge_transition_complete(state) ||
        payload.parent_hash != [0; 32] ||
        payload.fee_recipient != [0; 20] ||
        payload.state_root != [0; 32] ||
        payload.receipts_root != [0; 32] ||
        payload.logs_bloom.values.len() != 256 ||
        payload.logs_bloom.values.iter().any(|&byte| byte != 0) ||
        payload.prev_randao != [0; 32] ||
        payload.block_number != 0 ||
        payload.gas_limit != 0 ||
        payload.gas_used != 0 ||
        payload.timestamp != 0 ||
        !payload.extra_data.values.is_empty() ||
        payload.base_fee_per_gas != [0; 32] ||
        payload.block_hash != [0; 32] ||
        !payload.transactions.values.is_empty()
}

/// Check pure consensus payload invariants and derive its complete header.
///
/// This result does not establish execution validity or mutate the state. Complete import
/// must invoke the mandatory execution adapter before accepting the derived header.
pub fn validate_execution_payload_consensus(
    state: &BeaconState,
    payload: &ExecutionPayload,
    parameters: &BellatrixParameters,
) -> Result<ExecutionPayloadHeader, ProcessingError> {
    let constants = checked_constants(state, parameters)?;
    payload.hash_tree_root(parameters.preset())?;
    if is_merge_transition_complete(state) &&
        payload.parent_hash != state.latest_execution_payload_header.block_hash
    {
        return Err(ProcessingError::InvalidState("execution payload parent hash mismatch"))
    }
    let current = state.slot / constants.slots_per_epoch;
    let mix = state
        .randao_mixes
        .values
        .get(position(current, constants.historical_epochs)?)
        .ok_or(ProcessingError::InvalidState("RANDAO ring is truncated"))?;
    if payload.prev_randao != *mix {
        return Err(ProcessingError::InvalidState("execution payload RANDAO mismatch"))
    }
    let time = add(
        state.genesis_time,
        mul(state.slot, parameters.seconds_per_slot, "execution timestamp slot duration")?,
        "execution timestamp",
    )?;
    if payload.timestamp != time {
        return Err(ProcessingError::InvalidState("execution payload timestamp mismatch"))
    }
    Ok(ExecutionPayloadHeader {
        parent_hash: payload.parent_hash,
        fee_recipient: payload.fee_recipient,
        state_root: payload.state_root,
        receipts_root: payload.receipts_root,
        logs_bloom: payload.logs_bloom.clone(),
        prev_randao: payload.prev_randao,
        block_number: payload.block_number,
        gas_limit: payload.gas_limit,
        gas_used: payload.gas_used,
        timestamp: payload.timestamp,
        extra_data: payload.extra_data.clone(),
        base_fee_per_gas: payload.base_fee_per_gas,
        block_hash: payload.block_hash,
        transactions_root: payload.transactions.hash_tree_root(parameters.preset())?,
    })
}

/// Cache the complete Bellatrix pre-state commitment without advancing the slot.
pub fn process_slot(
    state: &mut BeaconState,
    parameters: &BellatrixParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, slot_kernel)
}

/// Replay empty Bellatrix slots, including every ordered epoch kernel.
pub fn process_slots(
    state: &mut BeaconState,
    target_slot: u64,
    parameters: &BellatrixParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, constants| {
        slots_kernel(candidate, target_slot, parameters, constants)
    })
}

/// Apply the complete ordered Bellatrix epoch sequence atomically.
pub fn process_epoch(
    state: &mut BeaconState,
    parameters: &BellatrixParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, constants| {
        epoch_kernel(candidate, parameters, constants)
    })
}

fn slot_kernel(state: &mut BeaconState, constants: &Constants) -> Result<(), ProcessingError> {
    let state_root = state.hash_tree_root(constants.preset())?;
    let index = position(state.slot, constants.historical_slots)?;
    *state
        .state_roots
        .values
        .get_mut(index)
        .ok_or(ProcessingError::InvalidState("state-root ring is truncated"))? = state_root;
    if state.latest_block_header.state_root == [0; 32] {
        state.latest_block_header.state_root = state_root;
    }
    *state
        .block_roots
        .values
        .get_mut(index)
        .ok_or(ProcessingError::InvalidState("block-root ring is truncated"))? =
        state.latest_block_header.hash_tree_root();
    Ok(())
}

fn slots_kernel(
    state: &mut BeaconState,
    target_slot: u64,
    parameters: &BellatrixParameters,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    if state.slot >= target_slot {
        return Err(ProcessingError::SlotNotIncreasing { current: state.slot, target: target_slot })
    }
    let mut cache = slot_cache::SlotCache::new(state, constants)?;
    while state.slot < target_slot {
        cache.process_slot(state)?;
        let next = add(state.slot, 1, "next slot")?;
        if next.is_multiple_of(constants.slots_per_epoch) {
            epoch_kernel(state, parameters, constants)?;
            cache.refresh_epoch(state)?;
        }
        state.slot = next;
        cache.advance_slot(next);
    }
    Ok(())
}

fn epoch_kernel(
    state: &mut BeaconState,
    parameters: &BellatrixParameters,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    with_altair(state, |common| {
        altair_processing::epoch_kernel(common, &parameters.altair, constants)
    })
}

/// Apply the exact `process_justification_and_finalization` Bellatrix component atomically.
pub fn process_justification_and_finalization(
    state: &mut BeaconState,
    parameters: &BellatrixParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, constants| {
        with_altair(candidate, |common| altair_processing::justification_kernel(common, constants))
    })
}

/// Apply the exact `process_inactivity_updates` Bellatrix component atomically.
pub fn process_inactivity_updates(
    state: &mut BeaconState,
    parameters: &BellatrixParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, constants| {
        with_altair(candidate, |common| {
            altair_processing::inactivity_kernel(common, &parameters.altair, constants)
        })
    })
}

/// Apply the exact `process_rewards_and_penalties` Bellatrix component atomically.
pub fn process_rewards_and_penalties(
    state: &mut BeaconState,
    parameters: &BellatrixParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, constants| {
        with_altair(candidate, |common| {
            altair_processing::rewards_kernel(common, &parameters.altair, constants)
        })
    })
}

/// Apply the exact `process_registry_updates` Bellatrix component atomically.
pub fn process_registry_updates(
    state: &mut BeaconState,
    parameters: &BellatrixParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, constants| {
        with_altair(candidate, |common| {
            altair_processing::registry_kernel(common, &parameters.altair, constants)
        })
    })
}

/// Apply the exact `process_slashings` Bellatrix component atomically.
pub fn process_slashings(
    state: &mut BeaconState,
    parameters: &BellatrixParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, constants| {
        with_altair(candidate, |common| altair_processing::slashings_kernel(common, constants))
    })
}

/// Apply the exact `process_eth1_data_reset` Bellatrix component atomically.
pub fn process_eth1_data_reset(
    state: &mut BeaconState,
    parameters: &BellatrixParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, constants| {
        with_altair(candidate, |common| altair_processing::eth1_reset_kernel(common, constants))
    })
}

/// Apply the exact `process_effective_balance_updates` Bellatrix component atomically.
pub fn process_effective_balance_updates(
    state: &mut BeaconState,
    parameters: &BellatrixParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, constants| {
        with_altair(candidate, |common| {
            altair_processing::effective_balances_kernel(common, constants)
        })
    })
}

/// Apply the exact `process_slashings_reset` Bellatrix component atomically.
pub fn process_slashings_reset(
    state: &mut BeaconState,
    parameters: &BellatrixParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, constants| {
        with_altair(candidate, |common| {
            altair_processing::slashings_reset_kernel(common, constants)
        })
    })
}

/// Apply the exact `process_randao_mixes_reset` Bellatrix component atomically.
pub fn process_randao_mixes_reset(
    state: &mut BeaconState,
    parameters: &BellatrixParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, constants| {
        with_altair(candidate, |common| altair_processing::randao_reset_kernel(common, constants))
    })
}

/// Apply the exact `process_historical_roots_update` Bellatrix component atomically.
pub fn process_historical_roots_update(
    state: &mut BeaconState,
    parameters: &BellatrixParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, constants| {
        with_altair(candidate, |common| {
            altair_processing::historical_roots_kernel(common, constants)
        })
    })
}

/// Apply the exact `process_participation_flag_updates` Bellatrix component atomically.
pub fn process_participation_flag_updates(
    state: &mut BeaconState,
    parameters: &BellatrixParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, constants| {
        with_altair(candidate, |common| altair_processing::participation_kernel(common, constants))
    })
}

/// Apply the exact `process_sync_committee_updates` Bellatrix component atomically.
pub fn process_sync_committee_updates(
    state: &mut BeaconState,
    parameters: &BellatrixParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, constants| {
        with_altair(candidate, |common| altair_processing::sync_committee_kernel(common, constants))
    })
}

/// Derive the exact committee from borrowed registry and RANDAO data without cloning state.
pub fn get_beacon_committee(
    state: &BeaconState,
    slot: u64,
    index: u64,
    parameters: &BellatrixParameters,
) -> Result<Vec<u64>, ProcessingError> {
    let constants = checked_constants(state, parameters)?;
    crate::common::beacon_committee(state, slot, index, &constants)
}

/// Replay into a private temporary candidate without allocating a second complete state copy.
/// Errors may modify this candidate; callers must discard it and never pass retained state.
pub(crate) fn process_slots_in_place(
    state: &mut BeaconState,
    target: u64,
    parameters: &BellatrixParameters,
) -> Result<(), ProcessingError> {
    let constants = checked_constants(state, parameters)?;
    slots_kernel(state, target, parameters, &constants)?;
    checked_constants(state, parameters)?;
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::{
        altair_processing::upgrade_to_altair,
        ssz::{
            bellatrix::{BeaconBlock, BeaconBlockBody, SignedBeaconBlock, SyncAggregate},
            phase0, Bits, PresetBits,
        },
    };
    use blst::min_pk::SecretKey;
    use reth_embedded_beacon_crypto::{compute_domain, compute_signing_root, ETHEREUM_BLS_DST};
    use reth_embedded_beacon_primitives::{uint64_root, BeaconBlockHeader, Checkpoint};
    use std::{
        sync::{
            atomic::{AtomicUsize, Ordering},
            Mutex,
        },
        task::{Context, Poll, Waker},
    };

    pub(crate) fn state_and_keys() -> (BeaconState, Vec<SecretKey>) {
        let keys = (1u8..=32)
            .map(|index| SecretKey::key_gen(&[index; 32], &[]).unwrap())
            .collect::<Vec<_>>();
        let validators = keys
            .iter()
            .map(|key| phase0::Validator {
                pubkey: key.sk_to_pk().to_bytes(),
                withdrawal_credentials: [0; 32],
                effective_balance: 32_000_000_000,
                slashed: false,
                activation_eligibility_epoch: 0,
                activation_epoch: 0,
                exit_epoch: u64::MAX,
                withdrawable_epoch: u64::MAX,
            })
            .collect();
        let pre = phase0::BeaconState {
            genesis_time: 0,
            genesis_validators_root: [1; 32],
            slot: 0,
            fork: phase0::ForkInfo {
                previous_version: [0, 0, 0, 1],
                current_version: [0, 0, 0, 1],
                epoch: 0,
            },
            latest_block_header: BeaconBlockHeader::default(),
            block_roots: Vector::from_vec(vec![[0; 32]; 64]),
            state_roots: Vector::from_vec(vec![[0; 32]; 64]),
            historical_roots: List::default(),
            eth1_data: phase0::Eth1Data {
                deposit_root: [0; 32],
                deposit_count: 0,
                block_hash: [0; 32],
            },
            eth1_data_votes: List::default(),
            eth1_deposit_index: 0,
            validators: List::from_vec(validators),
            balances: List::from_vec(vec![32_000_000_000; 32]),
            randao_mixes: Vector::from_vec(vec![[0; 32]; 64]),
            slashings: Vector::from_vec(vec![0; 64]),
            previous_epoch_attestations: List::default(),
            current_epoch_attestations: List::default(),
            justification_bits: Bits::from_ssz(&[0], Preset::Minimal).unwrap(),
            previous_justified_checkpoint: Checkpoint::default(),
            current_justified_checkpoint: Checkpoint::default(),
            finalized_checkpoint: Checkpoint::default(),
        };
        let parameters = BellatrixParameters::for_preset(Preset::Minimal);
        let altair = upgrade_to_altair(&pre, &parameters.altair).unwrap();
        (upgrade_to_bellatrix(&altair, &parameters).unwrap(), keys)
    }

    fn empty_payload() -> ExecutionPayload {
        ExecutionPayload {
            parent_hash: [0; 32],
            fee_recipient: [0; 20],
            state_root: [0; 32],
            receipts_root: [0; 32],
            logs_bloom: Vector::from_vec(vec![0; 256]),
            prev_randao: [0; 32],
            block_number: 0,
            gas_limit: 0,
            gas_used: 0,
            timestamp: 0,
            extra_data: List::default(),
            base_fee_per_gas: [0; 32],
            block_hash: [0; 32],
            transactions: List::default(),
        }
    }

    fn candidate(
        pre: &BeaconState,
        keys: &[SecretKey],
        parameters: &BellatrixParameters,
    ) -> (SignedBeaconBlock, BeaconState) {
        let mut expected = pre.clone();
        process_slots(&mut expected, pre.slot + 1, parameters).unwrap();
        let proposer = get_beacon_proposer_index(&expected, parameters).unwrap();
        let key = &keys[proposer as usize];
        let current = expected.slot / parameters.slots_per_epoch();
        let randao_root = compute_signing_root(
            uint64_root(current),
            compute_domain(
                [2, 0, 0, 0],
                expected.fork.current_version,
                expected.genesis_validators_root,
            ),
        );
        let reveal = key.sign(&randao_root, ETHEREUM_BLS_DST, &[]).to_bytes();
        let mut payload = empty_payload();
        payload.parent_hash = if is_merge_transition_complete(pre) {
            pre.latest_execution_payload_header.block_hash
        } else {
            [7; 32]
        };
        payload.timestamp = expected.genesis_time + expected.slot * parameters.seconds_per_slot;
        payload.prev_randao = expected.randao_mixes.values[(current % 64) as usize];
        payload.block_number = pre.latest_execution_payload_header.block_number + 1;
        payload.block_hash = [payload.block_number as u8; 32];
        payload.gas_limit = 30_000_000;
        let mut infinity = [0; 96];
        infinity[0] = 0xc0;
        let body = BeaconBlockBody {
            randao_reveal: reveal,
            eth1_data: expected.eth1_data.clone(),
            graffiti: [0; 32],
            proposer_slashings: List::default(),
            attester_slashings: List::default(),
            attestations: List::default(),
            deposits: List::default(),
            voluntary_exits: List::default(),
            sync_aggregate: SyncAggregate {
                sync_committee_bits: PresetBits::from_ssz(&[0; 4], Preset::Minimal).unwrap(),
                sync_committee_signature: infinity,
            },
            execution_payload: payload,
        };
        let mut message = BeaconBlock {
            slot: expected.slot,
            proposer_index: proposer,
            parent_root: expected.latest_block_header.hash_tree_root(),
            state_root: [0; 32],
            body,
        };
        process_block_header(&mut expected, &message, parameters).unwrap();
        expected.latest_execution_payload_header = validate_execution_payload_consensus(
            &expected,
            &message.body.execution_payload,
            parameters,
        )
        .unwrap();
        process_randao(&mut expected, &message.body, parameters).unwrap();
        process_eth1_data(&mut expected, &message.body, parameters).unwrap();
        process_operations(&mut expected, &message.body, parameters).unwrap();
        process_sync_aggregate(&mut expected, &message.body.sync_aggregate, parameters).unwrap();
        message.state_root = expected.hash_tree_root(Preset::Minimal).unwrap();
        let root = compute_signing_root(
            message.hash_tree_root(Preset::Minimal).unwrap(),
            compute_domain([0; 4], expected.fork.current_version, expected.genesis_validators_root),
        );
        (
            SignedBeaconBlock {
                message,
                signature: key.sign(&root, ETHEREUM_BLS_DST, &[]).to_bytes(),
            },
            expected,
        )
    }

    fn ready<F: Future>(future: F) -> F::Output {
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        let mut future = std::pin::pin!(future);
        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => value,
            Poll::Pending => panic!("private test adapter must be immediately ready"),
        }
    }

    // This private test adapter exercises transaction boundaries, never actual execution validity.
    struct RecordingEngine {
        terminal_allowed: bool,
        payload_allowed: bool,
        terminal_calls: AtomicUsize,
        payload_calls: AtomicUsize,
        observed: Mutex<Option<(ExecutionPayload, ExecutionContext)>>,
    }
    impl RecordingEngine {
        fn new(terminal_allowed: bool, payload_allowed: bool) -> Self {
            Self {
                terminal_allowed,
                payload_allowed,
                terminal_calls: AtomicUsize::new(0),
                payload_calls: AtomicUsize::new(0),
                observed: Mutex::new(None),
            }
        }
    }
    impl ExecutionEngine for RecordingEngine {
        type Error = std::io::Error;
        async fn verify_terminal_block(
            &self,
            _payload: &ExecutionPayload,
            _context: ExecutionContext,
        ) -> Result<(), Self::Error> {
            self.terminal_calls.fetch_add(1, Ordering::SeqCst);
            if self.terminal_allowed {
                Ok(())
            } else {
                Err(std::io::Error::other("missing terminal anchor"))
            }
        }
        async fn verify_payload(
            &self,
            payload: &ExecutionPayload,
            context: ExecutionContext,
        ) -> Result<(), Self::Error> {
            self.payload_calls.fetch_add(1, Ordering::SeqCst);
            *self.observed.lock().unwrap() = Some((payload.clone(), context));
            if self.payload_allowed {
                Ok(())
            } else {
                Err(std::io::Error::other("execution rejected"))
            }
        }
    }

    #[test]
    fn exact_candidate_requires_terminal_then_payload_verification_before_commit() {
        let parameters = BellatrixParameters::for_preset(Preset::Minimal);
        let (mut state, keys) = state_and_keys();
        let (block, expected) = candidate(&state, &keys, &parameters);
        let engine = RecordingEngine::new(true, true);
        ready(state_transition(&mut state, &block, &parameters, &engine)).unwrap();
        assert_eq!(state, expected);
        assert_eq!(engine.terminal_calls.load(Ordering::SeqCst), 1);
        assert_eq!(engine.payload_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            *engine.observed.lock().unwrap(),
            Some((
                block.message.body.execution_payload.clone(),
                ExecutionContext {
                    slot: block.message.slot,
                    parent_beacon_block_root: block.message.parent_root,
                    previous_execution_payload_hash: None
                }
            ))
        );
        let (next, expected) = candidate(&state, &keys, &parameters);
        let engine = RecordingEngine::new(false, true);
        ready(state_transition(&mut state, &next, &parameters, &engine)).unwrap();
        assert_eq!(state, expected);
        assert_eq!(engine.terminal_calls.load(Ordering::SeqCst), 0);
        assert_eq!(engine.payload_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            engine.observed.lock().unwrap().as_ref().unwrap().1.previous_execution_payload_hash,
            Some(block.message.body.execution_payload.block_hash)
        );
    }

    #[test]
    fn missing_terminal_proof_and_execution_rejection_leave_state_unchanged() {
        let parameters = BellatrixParameters::for_preset(Preset::Minimal);
        let (state, keys) = state_and_keys();
        let (block, _) = candidate(&state, &keys, &parameters);
        for (terminal, payload, expected_calls) in [(false, true, 0), (true, false, 1)] {
            let mut candidate = state.clone();
            let engine = RecordingEngine::new(terminal, payload);
            assert!(matches!(
                ready(state_transition(&mut candidate, &block, &parameters, &engine)),
                Err(ImportError::Execution(_))
            ));
            assert_eq!(candidate, state);
            assert_eq!(engine.payload_calls.load(Ordering::SeqCst), expected_calls);
        }
    }

    #[test]
    fn outer_signature_rejection_and_post_root_mismatch_are_atomic() {
        let parameters = BellatrixParameters::for_preset(Preset::Minimal);
        let (state, keys) = state_and_keys();
        let (mut block, _) = candidate(&state, &keys, &parameters);
        block.signature = [0; 96];
        let mut candidate = state.clone();
        let engine = RecordingEngine::new(true, true);
        assert!(matches!(
            ready(state_transition(&mut candidate, &block, &parameters, &engine)),
            Err(ImportError::Consensus(ProcessingError::Crypto(_)))
        ));
        assert_eq!(candidate, state);
        assert_eq!(engine.payload_calls.load(Ordering::SeqCst), 0);
        block.message.state_root = [9; 32];
        let proposer = block.message.proposer_index as usize;
        let root = compute_signing_root(
            block.message.hash_tree_root(Preset::Minimal).unwrap(),
            compute_domain([0; 4], state.fork.current_version, state.genesis_validators_root),
        );
        block.signature = keys[proposer].sign(&root, ETHEREUM_BLS_DST, &[]).to_bytes();
        assert!(matches!(
            ready(state_transition(&mut candidate, &block, &parameters, &engine)),
            Err(ImportError::Consensus(ProcessingError::InvalidState(
                "block post-state root mismatch"
            )))
        ));
        assert_eq!(candidate, state);
        assert_eq!(engine.payload_calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn payload_consensus_checks_parent_randao_timestamp_and_forwards_opaque_transactions() {
        let parameters = BellatrixParameters::for_preset(Preset::Minimal);
        let (state, keys) = state_and_keys();
        let (block, _) = candidate(&state, &keys, &parameters);
        let mut state = state;
        process_slots(&mut state, 1, &parameters).unwrap();
        let mut payload = block.message.body.execution_payload.clone();
        payload.timestamp = 0;
        assert!(matches!(
            validate_execution_payload_consensus(&state, &payload, &parameters),
            Err(ProcessingError::InvalidState("execution payload timestamp mismatch"))
        ));
        payload = block.message.body.execution_payload.clone();
        payload.prev_randao = [1; 32];
        assert!(matches!(
            validate_execution_payload_consensus(&state, &payload, &parameters),
            Err(ProcessingError::InvalidState("execution payload RANDAO mismatch"))
        ));
        payload = block.message.body.execution_payload.clone();
        payload.transactions.values.push(List::default());
        let header = validate_execution_payload_consensus(&state, &payload, &parameters).unwrap();
        assert_eq!(
            header.transactions_root,
            payload.transactions.hash_tree_root(Preset::Minimal).unwrap()
        );
        let engine = RecordingEngine::new(true, true);
        let mut body = block.message.body.clone();
        body.execution_payload = payload.clone();
        ready(process_execution_payload(&mut state, &body, &parameters, &engine)).unwrap();
        assert_eq!(
            engine.observed.lock().unwrap().as_ref().unwrap().0.transactions,
            payload.transactions
        );
        payload = block.message.body.execution_payload;
        state.latest_execution_payload_header = empty_execution_header();
        state.latest_execution_payload_header.gas_limit = 1;
        assert!(is_merge_transition_complete(&state));
        assert!(matches!(
            validate_execution_payload_consensus(&state, &payload, &parameters),
            Err(ProcessingError::InvalidState("execution payload parent hash mismatch"))
        ));
    }

    #[test]
    fn cached_bellatrix_roots_include_execution_header_across_epochs_and_sync_boundary() {
        let parameters = BellatrixParameters::for_preset(Preset::Minimal);
        let (mut state, _) = state_and_keys();
        state.slot = 7;
        state.latest_execution_payload_header.block_hash = [9; 32];
        let mut full = state.clone();
        let constants = checked_constants(&state, &parameters).unwrap();
        let mut cache = slot_cache::SlotCache::new(&state, &constants).unwrap();
        for _ in 0..73 {
            assert_eq!(cache.root().unwrap(), state.hash_tree_root(Preset::Minimal).unwrap());
            cache.process_slot(&mut state).unwrap();
            slot_kernel(&mut full, &constants).unwrap();
            let next = state.slot + 1;
            if next.is_multiple_of(parameters.slots_per_epoch()) {
                epoch_kernel(&mut state, &parameters, &constants).unwrap();
                epoch_kernel(&mut full, &parameters, &constants).unwrap();
                cache.refresh_epoch(&state).unwrap();
            }
            state.slot = next;
            full.slot = next;
            cache.advance_slot(next);
            assert_eq!(state, full);
        }
        assert_eq!(cache.root().unwrap(), state.hash_tree_root(Preset::Minimal).unwrap());
        assert_eq!(state.latest_execution_payload_header.block_hash, [9; 32]);
        state.slot = 63;
        state.finalized_checkpoint.epoch = 0;
        state.inactivity_scores.values[0] = u64::MAX;
        let before = state.clone();
        assert!(matches!(
            process_inactivity_updates(&mut state, &parameters),
            Err(ProcessingError::Arithmetic(_))
        ));
        assert_eq!(state, before);
    }
}
