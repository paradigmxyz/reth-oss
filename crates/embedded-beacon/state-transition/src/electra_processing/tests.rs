//! Electra upgrade, churn, pending queue, signature and complete-root regressions.

use super::*;
use crate::{
    capella_processing::upgrade_to_capella,
    deneb_processing::upgrade_to_deneb,
    ssz::{
        electra::{PendingConsolidation, PendingDeposit, PendingPartialWithdrawal},
        List,
    },
};
use blst::min_pk::SecretKey;
use reth_embedded_beacon_crypto::{compute_domain, compute_signing_root, ETHEREUM_BLS_DST};
use reth_embedded_beacon_primitives::{bytes_root, merkleize, uint64_root};

pub(crate) fn state_and_keys() -> (BeaconState, Vec<SecretKey>, ElectraParameters) {
    let (mut pre, keys) = crate::bellatrix_processing::tests::state_and_keys();
    pre.latest_execution_payload_header.block_hash = [9; 32];
    let parameters = ElectraParameters::for_preset(Preset::Minimal);
    let pre = upgrade_to_capella(&pre, &parameters.deneb.capella).unwrap();
    let pre = upgrade_to_deneb(&pre, &parameters.deneb).unwrap();
    (upgrade_to_electra(&pre, &parameters).unwrap(), keys, parameters)
}
fn pending(state: &BeaconState, index: usize, amount: u64) -> PendingDeposit {
    let mut infinity = [0; 96];
    infinity[0] = 0xc0;
    PendingDeposit {
        pubkey: state.validators.values[index].pubkey,
        withdrawal_credentials: state.validators.values[index].withdrawal_credentials,
        amount,
        signature: infinity,
        slot: 0,
    }
}
#[test]
fn upgrade_orders_pre_activation_and_compounding_balances_and_preserves_header() {
    let (mut pre, keys) = crate::bellatrix_processing::tests::state_and_keys();
    pre.latest_execution_payload_header.block_hash = [9; 32];
    let parameters = ElectraParameters::for_preset(Preset::Minimal);
    let pre = upgrade_to_capella(&pre, &parameters.deneb.capella).unwrap();
    let mut pre = upgrade_to_deneb(&pre, &parameters.deneb).unwrap();
    pre.validators.values[0].activation_epoch = u64::MAX;
    pre.validators.values[0].activation_eligibility_epoch = 5;
    pre.validators.values[1].activation_epoch = u64::MAX;
    pre.validators.values[1].activation_eligibility_epoch = 3;
    pre.validators.values[2].withdrawal_credentials[0] = 2;
    pre.balances.values[2] = 35_000_000_000;
    pre.validators.values[3].exit_epoch = 99;
    pre.latest_execution_payload_header.blob_gas_used = 262_144;
    let post = upgrade_to_electra(&pre, &parameters).unwrap();
    assert_eq!(post.earliest_exit_epoch, 100);
    assert_eq!(post.earliest_consolidation_epoch, 5);
    assert_eq!(post.exit_balance_to_consume, 64_000_000_000);
    assert_eq!(post.consolidation_balance_to_consume, 0);
    assert_eq!(post.pending_deposits.values.len(), 3);
    assert_eq!(post.pending_deposits.values[0].pubkey, keys[1].sk_to_pk().to_bytes());
    assert_eq!(post.pending_deposits.values[1].pubkey, keys[0].sk_to_pk().to_bytes());
    assert_eq!(post.pending_deposits.values[2].amount, 3_000_000_000);
    assert_eq!(post.balances.values[..3], [0, 0, 32_000_000_000]);
    assert_eq!(post.validators.values[0].effective_balance, 0);
    assert_eq!(post.validators.values[0].activation_eligibility_epoch, u64::MAX);
    assert_eq!(post.latest_execution_payload_header.block_hash, [9; 32]);
    assert_eq!(post.latest_execution_payload_header.blob_gas_used, 262_144);
    assert_eq!(pre.balances.values[0], 32_000_000_000);
}
#[test]
fn exited_deposits_postpone_and_withdrawn_topups_do_not_consume_churn() {
    let (mut state, _, parameters) = state_and_keys();
    state.slot = 16;
    state.finalized_checkpoint.epoch = 2;
    state.validators.values[0].exit_epoch = 1;
    state.validators.values[0].withdrawable_epoch = 2;
    state.validators.values[1].exit_epoch = 1;
    state.validators.values[1].withdrawable_epoch = 5;
    let first = pending(&state, 0, 100_000_000_000);
    let postponed = pending(&state, 1, 11);
    let large = pending(&state, 2, 70_000_000_000);
    let small = pending(&state, 3, 1);
    state.pending_deposits =
        List::from_vec(vec![first, postponed.clone(), large.clone(), small.clone()]);
    process_pending_deposits(&mut state, &parameters).unwrap();
    assert_eq!(state.balances.values[0], 132_000_000_000);
    assert_eq!(state.balances.values[1], 32_000_000_000);
    assert_eq!(state.pending_deposits.values, vec![large, small, postponed.clone()]);
    assert_eq!(state.deposit_balance_to_consume, 64_000_000_000);
    process_pending_deposits(&mut state, &parameters).unwrap();
    assert_eq!(state.balances.values[2], 102_000_000_000);
    assert_eq!(state.balances.values[3], 32_000_000_001);
    assert_eq!(state.pending_deposits.values, vec![postponed]);
    assert_eq!(state.deposit_balance_to_consume, 0);
}
#[test]
fn bridge_finality_and_per_epoch_count_precede_deposit_application() {
    let (mut state, _, parameters) = state_and_keys();
    state.slot = 16;
    state.finalized_checkpoint.epoch = 2;
    let mut request = pending(&state, 0, 5);
    request.slot = 1;
    state.pending_deposits.values.push(request.clone());
    let before = state.clone();
    process_pending_deposits(&mut state, &parameters).unwrap();
    assert_eq!(state, before);
    state.deposit_requests_start_index = 0;
    state.finalized_checkpoint.epoch = 0;
    let before = state.clone();
    process_pending_deposits(&mut state, &parameters).unwrap();
    assert_eq!(state, before);
    state.finalized_checkpoint.epoch = 2;
    state.pending_deposits = List::from_vec(vec![request.clone(); 17]);
    process_pending_deposits(&mut state, &parameters).unwrap();
    assert_eq!(state.pending_deposits.values, vec![request]);
    assert_eq!(state.balances.values[0], 32_000_000_080);
}
#[test]
fn pending_new_compounding_key_requires_real_deposit_signature() {
    let (mut state, _, mut parameters) = state_and_keys();
    parameters.min_per_epoch_churn_limit = common::MAX_EFFECTIVE;
    parameters.max_per_epoch_activation_exit_churn_limit = common::MAX_EFFECTIVE;
    let key = SecretKey::key_gen(&[66; 32], &[]).unwrap();
    let pubkey = key.sk_to_pk().to_bytes();
    let mut credentials = [0; 32];
    credentials[0] = 2;
    let amount = common::MAX_EFFECTIVE;
    let message =
        merkleize(&[bytes_root(&pubkey, 48, false).unwrap(), credentials, uint64_root(amount)], 3)
            .unwrap();
    let root = compute_signing_root(
        message,
        compute_domain(
            [3, 0, 0, 0],
            parameters.deneb.capella.bellatrix.altair.phase0.genesis_fork_version,
            [0; 32],
        ),
    );
    let good = PendingDeposit {
        pubkey,
        withdrawal_credentials: credentials,
        amount,
        signature: key.sign(&root, ETHEREUM_BLS_DST, &[]).to_bytes(),
        slot: 0,
    };
    let mut bad = good.clone();
    bad.signature = [0; 96];
    state.pending_deposits.values.push(bad);
    process_pending_deposits(&mut state, &parameters).unwrap();
    assert_eq!(state.validators.values.len(), 32);
    assert!(state.pending_deposits.values.is_empty());
    state.pending_deposits.values.push(good);
    process_pending_deposits(&mut state, &parameters).unwrap();
    assert_eq!(state.validators.values.len(), 33);
    assert_eq!(state.validators.values[32].pubkey, pubkey);
    assert_eq!(state.validators.values[32].effective_balance, common::MAX_EFFECTIVE);
    assert_eq!(state.balances.values[32], common::MAX_EFFECTIVE);
    assert_eq!(state.inactivity_scores.values.len(), 33);
    assert_eq!(state.current_epoch_participation.values[32], 0);
}
#[test]
fn balance_exit_and_consolidation_churn_round_exactly_and_reject_zero_capacity() {
    let (mut state, _, mut parameters) = state_and_keys();
    parameters.min_per_epoch_churn_limit = 128_000_000_000;
    parameters.max_per_epoch_activation_exit_churn_limit = 64_000_000_000;
    state.earliest_exit_epoch = 5;
    state.exit_balance_to_consume = 64_000_000_000;
    assert_eq!(
        compute_exit_epoch_and_update_churn(&mut state, 64_000_000_001, &parameters).unwrap(),
        6
    );
    assert_eq!(state.exit_balance_to_consume, 63_999_999_999);
    assert_eq!(
        compute_exit_epoch_and_update_churn(&mut state, 63_999_999_999, &parameters).unwrap(),
        6
    );
    assert_eq!(state.exit_balance_to_consume, 0);
    state.earliest_consolidation_epoch = 5;
    state.consolidation_balance_to_consume = 64_000_000_000;
    assert_eq!(
        compute_consolidation_epoch_and_update_churn(&mut state, 128_000_000_001, &parameters)
            .unwrap(),
        7
    );
    assert_eq!(state.consolidation_balance_to_consume, 63_999_999_999);
    parameters.max_per_epoch_activation_exit_churn_limit = 128_000_000_000;
    state.consolidation_balance_to_consume = 0;
    let before = state.clone();
    assert!(matches!(
        compute_consolidation_epoch_and_update_churn(&mut state, 1, &parameters),
        Err(ProcessingError::InvalidState("consolidation churn is zero"))
    ));
    assert_eq!(state, before);
}
#[test]
fn pending_consolidations_skip_slashed_and_preserve_excess_until_withdrawal() {
    let (mut state, _, parameters) = state_and_keys();
    state.slot = 16;
    state.validators.values[0].slashed = true;
    state.validators.values[1].withdrawable_epoch = 3;
    state.balances.values[1] = 32_000_000_009;
    state.validators.values[2].withdrawable_epoch = 4;
    let later = PendingConsolidation { source_index: 2, target_index: 3 };
    state.pending_consolidations = List::from_vec(vec![
        PendingConsolidation { source_index: 0, target_index: 3 },
        PendingConsolidation { source_index: 1, target_index: 3 },
        later.clone(),
    ]);
    process_pending_consolidations(&mut state, &parameters).unwrap();
    assert_eq!(state.pending_consolidations.values, vec![later]);
    assert_eq!(state.balances.values[0], 32_000_000_000);
    assert_eq!(state.balances.values[1], 9);
    assert_eq!(state.balances.values[3], 64_000_000_000);
}
#[test]
fn credential_limits_registry_priority_and_epoch_overflow_are_atomic() {
    let (mut state, _, parameters) = state_and_keys();
    state.validators.values[0].withdrawal_credentials[0] = 2;
    state.balances.values[0] = common::MAX_EFFECTIVE;
    state.balances.values[1] = common::MAX_EFFECTIVE;
    process_effective_balance_updates(&mut state, &parameters).unwrap();
    assert_eq!(state.validators.values[0].effective_balance, common::MAX_EFFECTIVE);
    assert_eq!(state.validators.values[1].effective_balance, common::MIN_ACTIVATION);
    state.validators.values[2].activation_epoch = u64::MAX;
    state.validators.values[2].activation_eligibility_epoch = u64::MAX;
    process_registry_updates(&mut state, &parameters).unwrap();
    assert_eq!(state.validators.values[2].activation_eligibility_epoch, 1);
    assert_eq!(state.validators.values[2].activation_epoch, u64::MAX);
    state.finalized_checkpoint.epoch = 1;
    process_registry_updates(&mut state, &parameters).unwrap();
    assert_eq!(state.validators.values[2].activation_epoch, 5);
    state.balances.values[0] = u64::MAX;
    state.pending_deposits.values.push(pending(&state, 0, 1));
    let before = state.clone();
    assert!(matches!(
        process_pending_deposits(&mut state, &parameters),
        Err(ProcessingError::Arithmetic("pending deposit balance"))
    ));
    assert_eq!(state, before);
}
#[test]
fn complete_root_cache_tracks_all_pending_fields_across_epochs_and_sync_boundary() {
    let (mut state, _, parameters) = state_and_keys();
    state.slot = 7;
    state.next_withdrawal_index = 123;
    state.next_withdrawal_validator_index = 9;
    state.historical_roots.values.push([7; 32]);
    state.pending_partial_withdrawals.values.push(PendingPartialWithdrawal {
        validator_index: 0,
        amount: 1,
        withdrawable_epoch: 99,
    });
    state.pending_deposits.values.push(pending(&state, 0, 5));
    state
        .pending_consolidations
        .values
        .push(PendingConsolidation { source_index: 1, target_index: 2 });
    state.validators.values[1].withdrawable_epoch = 1;
    let mut full = state.clone();
    let constants = checked_constants(&state, &parameters).unwrap();
    let mut cache = slots::SlotCache::new(&state, &constants).unwrap();
    for _ in 0..73 {
        assert_eq!(cache.root().unwrap(), state.hash_tree_root(Preset::Minimal).unwrap());
        cache.process_slot(&mut state).unwrap();
        slots::slot_kernel(&mut full, &constants).unwrap();
        let next = state.slot + 1;
        if next.is_multiple_of(parameters.slots_per_epoch()) {
            epochs::epoch_kernel(&mut state, &parameters, &constants).unwrap();
            epochs::epoch_kernel(&mut full, &parameters, &constants).unwrap();
            cache.refresh_epoch(&state).unwrap();
        }
        state.slot = next;
        full.slot = next;
        cache.advance_slot(next);
        assert_eq!(state, full);
    }
    assert_eq!(cache.root().unwrap(), state.hash_tree_root(Preset::Minimal).unwrap());
    assert_eq!(state.historical_roots.values, vec![[7; 32]]);
    assert_eq!(state.historical_summaries.values.len(), 1);
    assert_eq!(state.next_withdrawal_index, 123);
    assert_eq!(state.pending_partial_withdrawals.values.len(), 1);
    assert!(state.pending_deposits.values.is_empty());
    assert!(state.pending_consolidations.values.is_empty());
}
