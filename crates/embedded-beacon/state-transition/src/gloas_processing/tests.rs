//! Changed Gloas rules, progressive state roots and fork initialization regressions.

use super::*;
use crate::ssz::{
    gloas::{BuilderPendingWithdrawal, PendingDeposit},
    ProgressiveList,
};
use blst::min_pk::SecretKey;
use reth_embedded_beacon_crypto::{compute_domain, compute_signing_root, ETHEREUM_BLS_DST};
use reth_embedded_beacon_primitives::{merkleize, uint64_root};

fn state_and_keys() -> (BeaconState, Vec<SecretKey>, GloasParameters) {
    let (pre, keys, fulu) = crate::fulu_processing::tests::state_and_keys();
    let mut parameters = GloasParameters::for_preset(Preset::Minimal);
    parameters.fulu = fulu;
    parameters.fork_epoch = 0;
    (upgrade_to_gloas(&pre, &parameters).unwrap(), keys, parameters)
}

#[test]
fn upgrade_preserves_prefix_and_initializes_progressive_payload_and_ptc() {
    let (pre, _, fulu) = crate::fulu_processing::tests::state_and_keys();
    let mut parameters = GloasParameters::for_preset(Preset::Minimal);
    parameters.fulu = fulu;
    parameters.fork_epoch = 0;
    let post = upgrade_to_gloas(&pre, &parameters).unwrap();
    assert_eq!(post.validators.values, pre.validators.values);
    assert_eq!(post.balances.values, pre.balances.values);
    assert_eq!(post.proposer_lookahead, pre.proposer_lookahead);
    assert_eq!(post.latest_block_hash, pre.latest_execution_payload_header.block_hash);
    assert_eq!(post.latest_execution_payload_bid.builder_index, u64::MAX);
    assert_eq!(post.latest_execution_payload_bid.block_hash, post.latest_block_hash);
    assert_eq!(
        post.latest_execution_payload_bid.parent_block_root,
        pre.latest_block_header.parent_root
    );
    assert_eq!(post.execution_payload_availability.bits.as_ssz(), &[0xff; 8]);
    assert!(post.builders.values.is_empty());
    assert_eq!(post.builder_pending_payments.values, vec![builder::empty_payment(); 16]);
    assert_eq!(post.ptc_window.values.len(), 24);
    assert!(post.ptc_window.values[..8].iter().all(|c| c.values == vec![0; 16]));
    for slot in 0..16 {
        assert_eq!(
            post.ptc_window.values[slot + 8],
            compute_ptc(&post, slot as u64, &parameters).unwrap()
        );
    }
    assert_eq!(
        BeaconState::from_ssz(&post.to_ssz(Preset::Minimal).unwrap(), Preset::Minimal).unwrap(),
        post
    );
    assert_eq!(GloasParameters::for_preset(Preset::Mainnet).fork_epoch, u64::MAX);
    assert_eq!(parameters.fulu.electra.deneb.capella.bellatrix.seconds_per_slot, 6);
}

#[test]
fn activation_is_capped_while_exit_and_consolidation_are_independent() {
    let (mut state, _, parameters) = state_and_keys();
    for (v, balance) in state.validators.values.iter_mut().zip(&mut state.balances.values) {
        v.withdrawal_credentials[0] = 2;
        v.effective_balance = 2_048_000_000_000;
        *balance = v.effective_balance;
    }
    assert_eq!(get_activation_churn_limit(&state, &parameters).unwrap(), 128_000_000_000);
    assert_eq!(get_exit_churn_limit(&state, &parameters).unwrap(), 4_096_000_000_000);
    assert_eq!(get_consolidation_churn_limit(&state, &parameters).unwrap(), 2_048_000_000_000);
    state.earliest_exit_epoch = 0;
    state.exit_balance_to_consume = 0;
    let baseline = crate::altair_processing::activation_exit_epoch(0).unwrap();
    assert_eq!(
        compute_exit_epoch_and_update_churn(&mut state, 256_000_000_000, &parameters).unwrap(),
        baseline
    );
    assert_eq!(state.exit_balance_to_consume, 3_840_000_000_000);
    let before = state.clone();
    let mut invalid = parameters.clone();
    invalid.churn_limit_quotient = 0;
    assert!(compute_exit_epoch_and_update_churn(&mut state, 1, &invalid).is_err());
    assert_eq!(state, before);
}

#[test]
fn recomputed_proposers_exclude_slashed_but_preserve_stored_lookahead() {
    let (mut state, _, parameters) = state_and_keys();
    let stored = state.proposer_lookahead.clone();
    for validator in &mut state.validators.values[1..] {
        validator.slashed = true;
    }
    let indices = get_beacon_proposer_indices(&state, 1, &parameters).unwrap();
    assert_eq!(indices.values, vec![0; 8]);
    assert_eq!(state.proposer_lookahead, stored);
    state.validators.values[0].slashed = true;
    assert!(get_beacon_proposer_indices(&state, 1, &parameters).is_err());
}

#[test]
fn builder_quorum_keeps_zero_amount_and_rotates_exactly() {
    let (mut state, _, parameters) = state_and_keys();
    let constants = checked_constants(&state, &parameters).unwrap();
    let quorum = common::total_active_balance(&state, &constants).unwrap() / 8 * 6 / 10;
    state.builder_pending_payments.values[0].weight = quorum - 1;
    state.builder_pending_payments.values[0].withdrawal.amount = 100;
    state.builder_pending_payments.values[1].weight = quorum;
    state.builder_pending_payments.values[1].withdrawal =
        BuilderPendingWithdrawal { fee_recipient: [9; 20], amount: 0, builder_index: 3 };
    state.builder_pending_payments.values[8].withdrawal.amount = 7;
    let carried = state.builder_pending_payments.values[8].clone();
    process_builder_pending_payments(&mut state, &parameters).unwrap();
    assert_eq!(
        state.builder_pending_withdrawals.values,
        vec![BuilderPendingWithdrawal { fee_recipient: [9; 20], amount: 0, builder_index: 3 }]
    );
    assert_eq!(state.builder_pending_payments.values[0], carried);
    assert_eq!(state.builder_pending_payments.values[8..], vec![builder::empty_payment(); 8]);
}

#[test]
fn cached_progressive_roots_match_naive_epochs_and_availability_wraps() {
    let (pre, _, parameters) = state_and_keys();
    let mut cached = pre.clone();
    let mut naive = pre;
    let constants = checked_constants(&cached, &parameters).unwrap();
    let mut cache = slots::SlotCache::new(&cached, &constants).unwrap();
    for _ in 0..66 {
        assert_eq!(cache.root().unwrap(), cached.hash_tree_root(Preset::Minimal).unwrap());
        cache.process_slot(&mut cached).unwrap();
        slots::slot_kernel(&mut naive, &constants).unwrap();
        assert_eq!(cached, naive);
        assert_eq!(cache.root().unwrap(), cached.hash_tree_root(Preset::Minimal).unwrap());
        let next = cached.slot + 1;
        assert_eq!(
            cached.execution_payload_availability.bits.get((next % 64) as usize),
            Some(false)
        );
        if next.is_multiple_of(8) {
            epochs::epoch_kernel(&mut cached, &parameters, &constants).unwrap();
            epochs::epoch_kernel(&mut naive, &parameters, &constants).unwrap();
            cache.refresh_epoch(&cached).unwrap();
            assert_eq!(cached, naive);
            assert_eq!(cache.root().unwrap(), cached.hash_tree_root(Preset::Minimal).unwrap());
        }
        cached.slot = next;
        naive.slot = next;
        cache.advance_slot(next);
    }
    assert_eq!(cached.execution_payload_availability.bits.as_ssz(), &[0; 8]);
}

#[test]
fn upgrade_builder_onboarding_uses_valid_pop_and_retains_validator_precedence() {
    let (mut pre, _, fulu) = crate::fulu_processing::tests::state_and_keys();
    let mut parameters = GloasParameters::for_preset(Preset::Minimal);
    parameters.fulu = fulu;
    parameters.fork_epoch = 0;
    let key = SecretKey::key_gen(&[42; 32], &[]).unwrap();
    let pubkey = key.sk_to_pk().to_bytes();
    let mut builder_credential = [0; 32];
    builder_credential[0] = 0xb0;
    builder_credential[12..].fill(7);
    let sign = |credentials, amount| {
        let root = merkleize(
            &[
                reth_embedded_beacon_primitives::bytes_root(&pubkey, 48, false).unwrap(),
                credentials,
                uint64_root(amount),
            ],
            3,
        )
        .unwrap();
        let domain = compute_domain(
            [3, 0, 0, 0],
            parameters.fulu.electra.deneb.capella.bellatrix.altair.phase0.genesis_fork_version,
            [0; 32],
        );
        key.sign(&compute_signing_root(root, domain), ETHEREUM_BLS_DST, &[]).to_bytes()
    };
    let deposit = crate::ssz::fulu::PendingDeposit {
        pubkey,
        withdrawal_credentials: builder_credential,
        amount: 1_000_000_000,
        signature: sign(builder_credential, 1_000_000_000),
        slot: 0,
    };
    pre.pending_deposits.values.push(deposit.clone());
    let mut topup = deposit;
    topup.amount = 2_000_000_000;
    topup.signature = [0; 96];
    pre.pending_deposits.values.push(topup);
    let post = upgrade_to_gloas(&pre, &parameters).unwrap();
    assert_eq!(post.builders.values.len(), 1);
    assert_eq!(post.builders.values[0].balance, 3_000_000_000);
    assert_eq!(post.builders.values[0].execution_address, [7; 20]);
    assert!(post.pending_deposits.values.is_empty());
    let mut credential = [0; 32];
    credential[0] = 1;
    let validator_deposit = crate::ssz::fulu::PendingDeposit {
        pubkey,
        withdrawal_credentials: credential,
        amount: 32_000_000_000,
        signature: sign(credential, 32_000_000_000),
        slot: 0,
    };
    pre.pending_deposits.values.insert(0, validator_deposit);
    let post = upgrade_to_gloas(&pre, &parameters).unwrap();
    assert!(post.builders.values.is_empty());
    assert_eq!(post.pending_deposits.values.len(), 3);
}

#[test]
fn pending_deposit_uses_activation_only_budget_without_legacy_bridge() {
    let (mut state, _, parameters) = state_and_keys();
    let pubkey = state.validators.values[0].pubkey;
    state.pending_deposits = ProgressiveList::from_vec(vec![PendingDeposit {
        pubkey,
        withdrawal_credentials: [0; 32],
        amount: 65_000_000_000,
        signature: [0; 96],
        slot: 0,
    }]);
    let balance = state.balances.values[0];
    process_pending_deposits(&mut state, &parameters).unwrap();
    assert_eq!(state.pending_deposits.values.len(), 1);
    assert_eq!(state.balances.values[0], balance);
    assert_eq!(state.deposit_balance_to_consume, 64_000_000_000);
    process_pending_deposits(&mut state, &parameters).unwrap();
    assert!(state.pending_deposits.values.is_empty());
    assert_eq!(state.balances.values[0], balance + 65_000_000_000);
    assert_eq!(state.deposit_requests_start_index, u64::MAX);
}

#[test]
fn checked_post_root_tracks_changed_state_and_rejects_indexed_length_mismatch() {
    let (mut state, _, parameters) = state_and_keys();
    let (_, original) = checked_constants_with_root(&state, &parameters).unwrap();
    assert_eq!(original, state.hash_tree_root(parameters.preset()).unwrap());
    state.balances.values[0] += 1;
    let (_, changed) = checked_constants_with_root(&state, &parameters).unwrap();
    assert_ne!(changed, original);
    assert_eq!(changed, state.hash_tree_root(parameters.preset()).unwrap());
    state.balances.values.pop();
    assert!(state.hash_tree_root(parameters.preset()).is_ok());
    assert!(matches!(
        checked_constants_with_root(&state, &parameters),
        Err(ProcessingError::InvalidState("Gloas validator-indexed list lengths differ"))
    ));
}

#[test]
fn structural_guards_preserve_progressive_root_errors_and_atomic_replay() {
    let (state, _, parameters) = state_and_keys();
    for mutation in 0..5 {
        let mut invalid = state.clone();
        match mutation {
            0 => {
                invalid.ptc_window.values[0].values.pop();
            }
            1 => {
                invalid.builder_pending_payments.values.pop();
            }
            2 => {
                invalid.proposer_lookahead.values.pop();
            }
            3 => {
                invalid.current_epoch_participation.values.pop();
            }
            _ => {
                invalid.balances.values.pop();
            }
        }
        let guard = checked_constants(&invalid, &parameters).map(|_| ());
        let committed = checked_constants_with_root(&invalid, &parameters).map(|_| ());
        assert!(guard.is_err());
        assert_eq!(format!("{guard:?}"), format!("{committed:?}"));
        let before = invalid.clone();
        let target = invalid.slot + 1;
        assert!(process_slots(&mut invalid, target, &parameters).is_err());
        assert_eq!(invalid, before);
    }
}
