//! Exact Electra-to-Fulu upgrade with deterministic current/next proposer lookahead.

use super::{checked_constants, initialize_proposer_lookahead, FuluParameters, ProcessingError};
use crate::ssz::{
    electra,
    fulu::{
        BeaconState, ExecutionPayloadHeader, HistoricalSummary, PendingConsolidation,
        PendingDeposit, PendingPartialWithdrawal, SyncCommittee,
    },
    List, Vector,
};

/// Upgrade every state field and initialize the exact available proposer lookahead.
pub fn upgrade_to_fulu(
    pre: &electra::BeaconState,
    parameters: &FuluParameters,
) -> Result<BeaconState, ProcessingError> {
    parameters.validate()?;
    pre.hash_tree_root(parameters.preset())?;
    let mut post = BeaconState {
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
        deposit_requests_start_index: pre.deposit_requests_start_index,
        deposit_balance_to_consume: pre.deposit_balance_to_consume,
        exit_balance_to_consume: pre.exit_balance_to_consume,
        earliest_exit_epoch: pre.earliest_exit_epoch,
        consolidation_balance_to_consume: pre.consolidation_balance_to_consume,
        earliest_consolidation_epoch: pre.earliest_consolidation_epoch,
        pending_deposits: List::from_vec(
            pre.pending_deposits
                .values
                .iter()
                .map(|d| PendingDeposit {
                    pubkey: d.pubkey,
                    withdrawal_credentials: d.withdrawal_credentials,
                    amount: d.amount,
                    signature: d.signature,
                    slot: d.slot,
                })
                .collect(),
        ),
        pending_partial_withdrawals: List::from_vec(
            pre.pending_partial_withdrawals
                .values
                .iter()
                .map(|w| PendingPartialWithdrawal {
                    validator_index: w.validator_index,
                    amount: w.amount,
                    withdrawable_epoch: w.withdrawable_epoch,
                })
                .collect(),
        ),
        pending_consolidations: List::from_vec(
            pre.pending_consolidations
                .values
                .iter()
                .map(|c| PendingConsolidation {
                    source_index: c.source_index,
                    target_index: c.target_index,
                })
                .collect(),
        ),
        proposer_lookahead: Vector::from_vec(vec![0; (parameters.slots_per_epoch() * 2) as usize]),
    };
    checked_constants(&post, parameters)?;
    post.proposer_lookahead = initialize_proposer_lookahead(&post, parameters)?;
    checked_constants(&post, parameters)?;
    Ok(post)
}
