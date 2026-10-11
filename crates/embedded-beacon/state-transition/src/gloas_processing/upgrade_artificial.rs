//! Exact artificial-fixture upgrade; excluded from runtime builds.
#![cfg(feature = "spec-tests")]
use super::*;

pub(crate) fn upgrade_to_gloas(
    pre: &fulu::BeaconState,
    parameters: &GloasParameters,
) -> Result<BeaconState, ProcessingError> {
    parameters.validate()?;
    pre.hash_tree_root(parameters.preset())?;
    let constants = crate::fulu_processing::checked_constants(pre, &parameters.fulu)?;
    let empty_requests = ExecutionRequests {
        deposits: ProgressiveList::default(),
        withdrawals: ProgressiveList::default(),
        consolidations: ProgressiveList::default(),
        builder_deposits: ProgressiveList::default(),
        builder_exits: ProgressiveList::default(),
    };
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
        validators: ProgressiveList::from_vec(pre.validators.values.clone()),
        balances: ProgressiveList::from_vec(pre.balances.values.clone()),
        randao_mixes: pre.randao_mixes.clone(),
        slashings: pre.slashings.clone(),
        previous_epoch_participation: ProgressiveList::from_vec(
            pre.previous_epoch_participation.values.clone(),
        ),
        current_epoch_participation: ProgressiveList::from_vec(
            pre.current_epoch_participation.values.clone(),
        ),
        justification_bits: pre.justification_bits.clone(),
        previous_justified_checkpoint: pre.previous_justified_checkpoint,
        current_justified_checkpoint: pre.current_justified_checkpoint,
        finalized_checkpoint: pre.finalized_checkpoint,
        inactivity_scores: ProgressiveList::from_vec(pre.inactivity_scores.values.clone()),
        current_sync_committee: SyncCommittee {
            pubkeys: pre.current_sync_committee.pubkeys.clone(),
            aggregate_pubkey: pre.current_sync_committee.aggregate_pubkey,
        },
        next_sync_committee: SyncCommittee {
            pubkeys: pre.next_sync_committee.pubkeys.clone(),
            aggregate_pubkey: pre.next_sync_committee.aggregate_pubkey,
        },
        latest_block_hash: pre.latest_execution_payload_header.block_hash,
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
        pending_deposits: ProgressiveList::from_vec(
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
        pending_partial_withdrawals: ProgressiveList::from_vec(
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
        pending_consolidations: ProgressiveList::from_vec(
            pre.pending_consolidations
                .values
                .iter()
                .map(|c| PendingConsolidation {
                    source_index: c.source_index,
                    target_index: c.target_index,
                })
                .collect(),
        ),
        proposer_lookahead: pre.proposer_lookahead.clone(),
        builders: ProgressiveList::default(),
        next_withdrawal_builder_index: 0,
        execution_payload_availability: ExecutionPayloadAvailability::from_ssz(
            &vec![0xff; (constants.historical_slots / 8) as usize],
            parameters.preset(),
        )?,
        builder_pending_payments: Vector::from_vec(vec![
            super::super::builder::empty_payment();
            (parameters.slots_per_epoch() * 2) as usize
        ]),
        builder_pending_withdrawals: ProgressiveList::default(),
        latest_execution_payload_bid: ExecutionPayloadBid {
            parent_block_hash: pre.latest_execution_payload_header.parent_hash,
            parent_block_root: pre.latest_block_header.parent_root,
            block_hash: pre.latest_execution_payload_header.block_hash,
            prev_randao: pre.latest_execution_payload_header.prev_randao,
            fee_recipient: [0; 20],
            gas_limit: pre.latest_execution_payload_header.gas_limit,
            builder_index: u64::MAX,
            slot: pre.latest_block_header.slot,
            value: 0,
            execution_payment: 0,
            blob_kzg_commitments: ProgressiveList::default(),
            execution_requests_root: empty_requests.hash_tree_root(parameters.preset())?,
        },
        payload_expected_withdrawals: ProgressiveList::default(),
        ptc_window: Vector::from_vec(vec![
            Vector::from_vec(vec![
                0;
                match parameters.preset() {
                    Preset::Mainnet => 512,
                    Preset::Minimal => 16,
                }
            ]);
            (parameters.slots_per_epoch() * 3) as usize
        ]),
    };
    checked_constants(&post, parameters)?;
    post.ptc_window = initialize_ptc_window(&post, parameters)?;
    super::super::builder::artificial::onboard_builders_from_pending_deposits_kernel(
        &mut post, parameters,
    )?;
    checked_constants(&post, parameters)?;
    Ok(post)
}
