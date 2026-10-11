//! Exact irregular Deneb-to-Electra state upgrade and pending-balance migration.

use super::{checked_constants, common, ElectraParameters, ProcessingError};
use crate::{
    altair_processing::activation_exit_epoch,
    phase0_processing::add,
    ssz::{
        deneb,
        electra::{
            BeaconState, ExecutionPayloadHeader, HistoricalSummary, PendingDeposit, SyncCommittee,
        },
        List,
    },
};

/// Upgrade a complete authenticated Deneb state at the caller's trusted fork boundary.
/// Pre-activation deposits retain eligibility order and compounding excess enters churn.
pub fn upgrade_to_electra(
    pre: &deneb::BeaconState,
    parameters: &ElectraParameters,
) -> Result<BeaconState, ProcessingError> {
    parameters.validate()?;
    pre.hash_tree_root(parameters.preset())?;
    let epoch = pre.slot / parameters.slots_per_epoch();
    let activation = activation_exit_epoch(epoch)?;
    let earliest = pre
        .validators
        .values
        .iter()
        .filter(|v| v.exit_epoch != u64::MAX)
        .fold(activation, |epoch, v| epoch.max(v.exit_epoch));
    let mut post = BeaconState {
        genesis_time: pre.genesis_time,
        genesis_validators_root: pre.genesis_validators_root,
        slot: pre.slot,
        fork: crate::ssz::phase0::ForkInfo {
            previous_version: pre.fork.current_version,
            current_version: parameters.fork_version,
            epoch,
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
        current_sync_committee: SyncCommittee {
            pubkeys: pre.current_sync_committee.pubkeys.clone(),
            aggregate_pubkey: pre.current_sync_committee.aggregate_pubkey,
        },
        next_sync_committee: SyncCommittee {
            pubkeys: pre.next_sync_committee.pubkeys.clone(),
            aggregate_pubkey: pre.next_sync_committee.aggregate_pubkey,
        },
        latest_execution_payload_header: ExecutionPayloadHeader::from_ssz(
            &pre.latest_execution_payload_header.to_ssz(parameters.preset())?,
            parameters.preset(),
        )?,
        next_withdrawal_index: pre.next_withdrawal_index,
        next_withdrawal_validator_index: pre.next_withdrawal_validator_index,
        historical_summaries: List::from_vec(
            pre.historical_summaries
                .values
                .iter()
                .map(|s| HistoricalSummary {
                    block_summary_root: s.block_summary_root,
                    state_summary_root: s.state_summary_root,
                })
                .collect(),
        ),
        deposit_requests_start_index: u64::MAX,
        deposit_balance_to_consume: 0,
        exit_balance_to_consume: 0,
        earliest_exit_epoch: add(earliest, 1, "upgraded earliest exit epoch")?,
        consolidation_balance_to_consume: 0,
        earliest_consolidation_epoch: activation,
        pending_deposits: List::default(),
        pending_partial_withdrawals: List::default(),
        pending_consolidations: List::default(),
    };
    let constants = checked_constants(&post, parameters)?;
    post.exit_balance_to_consume =
        common::get_activation_exit_churn_limit(&post, parameters, &constants)?;
    post.consolidation_balance_to_consume =
        common::get_consolidation_churn_limit(&post, parameters, &constants)?;
    let mut queued = post
        .validators
        .values
        .iter()
        .enumerate()
        .filter_map(|(index, v)| (v.activation_epoch == u64::MAX).then_some(index))
        .collect::<Vec<_>>();
    queued.sort_unstable_by_key(|&index| {
        (post.validators.values[index].activation_eligibility_epoch, index)
    });
    let mut infinity = [0; 96];
    infinity[0] = 0xc0;
    for index in queued {
        if post.pending_deposits.values.len() >= 134_217_728 {
            return Err(ProcessingError::InvalidState("pending deposits limit reached"))
        }
        let amount = post.balances.values[index];
        post.balances.values[index] = 0;
        let validator = &mut post.validators.values[index];
        validator.effective_balance = 0;
        validator.activation_eligibility_epoch = u64::MAX;
        post.pending_deposits.values.push(PendingDeposit {
            pubkey: validator.pubkey,
            withdrawal_credentials: validator.withdrawal_credentials,
            amount,
            signature: infinity,
            slot: 0,
        });
    }
    for index in 0..post.validators.values.len() {
        if common::has_compounding_withdrawal_credential(&post.validators.values[index]) {
            common::queue_excess_active_balance(&mut post, index as u64)?;
        }
    }
    checked_constants(&post, parameters)?;
    Ok(post)
}
