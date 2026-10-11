//! Gloas builder identities, dedicated deposit signatures, reserved payments and signed bids.

use super::{checked_constants, common, transact, GloasParameters};
use crate::{
    phase0_processing::{add, position, Constants, ProcessingError},
    ssz::gloas::{
        self, BeaconState, BuilderDepositRequest, BuilderExitRequest, BuilderPendingPayment,
        SignedExecutionPayloadBid,
    },
};
use reth_embedded_beacon_crypto::{compute_domain, compute_signing_root, verify_signature};
use reth_embedded_beacon_primitives::{bytes_root, merkleize, uint64_root, Root};

/// Bit separating builder withdrawals from validator indices.
pub const BUILDER_INDEX_FLAG: u64 = 1 << 40;
/// Reserved builder index for proposer self-builds.
pub const BUILDER_INDEX_SELF_BUILD: u64 = u64::MAX;
const MIN_DEPOSIT_AMOUNT: u64 = 1_000_000_000;
/// Whether the withdrawal index encodes a builder registry entry.
pub const fn is_builder_index(index: u64) -> bool {
    index & BUILDER_INDEX_FLAG != 0
}
/// Encode a builder withdrawal using the specification's index flag.
pub const fn convert_builder_index_to_validator_index(index: u64) -> u64 {
    index | BUILDER_INDEX_FLAG
}
/// Decode the builder index flag without changing the remaining index bits.
pub const fn convert_validator_index_to_builder_index(index: u64) -> u64 {
    index & !BUILDER_INDEX_FLAG
}
/// Whether deposit credentials use the dedicated builder prefix.
pub const fn is_builder_withdrawal_credential(credentials: Root) -> bool {
    credentials[0] == 0xb0
}

pub(crate) fn active_builder_kernel(
    state: &BeaconState,
    index: u64,
) -> Result<bool, ProcessingError> {
    let builder = &state.builders.values[common::index(index, state.builders.values.len())?];
    Ok(builder.deposit_epoch < state.finalized_checkpoint.epoch &&
        builder.withdrawable_epoch == u64::MAX)
}
/// Whether the builder's registry placement is finalized and it has not initiated exit.
pub fn is_active_builder(
    state: &BeaconState,
    index: u64,
    parameters: &GloasParameters,
) -> Result<bool, ProcessingError> {
    checked_constants(state, parameters)?;
    active_builder_kernel(state, index)
}
pub(crate) fn pending_builder_balance_kernel(
    state: &BeaconState,
    index: u64,
) -> Result<u64, ProcessingError> {
    common::index(index, state.builders.values.len())?;
    let queued = state
        .builder_pending_withdrawals
        .values
        .iter()
        .filter(|w| w.builder_index == index)
        .try_fold(0, |sum, w| add(sum, w.amount, "builder pending withdrawals"))?;
    state
        .builder_pending_payments
        .values
        .iter()
        .filter(|p| p.withdrawal.builder_index == index)
        .try_fold(queued, |sum, p| add(sum, p.withdrawal.amount, "builder pending payments"))
}
/// Sum both pending payment rings and queued withdrawals without double-spending builder funds.
pub fn get_pending_balance_to_withdraw_for_builder(
    state: &BeaconState,
    index: u64,
    parameters: &GloasParameters,
) -> Result<u64, ProcessingError> {
    checked_constants(state, parameters)?;
    pending_builder_balance_kernel(state, index)
}
pub(crate) fn can_cover_kernel(
    state: &BeaconState,
    index: u64,
    amount: u64,
) -> Result<bool, ProcessingError> {
    let balance = state.builders.values[common::index(index, state.builders.values.len())?].balance;
    let reserved = add(
        MIN_DEPOSIT_AMOUNT,
        pending_builder_balance_kernel(state, index)?,
        "builder minimum and reserved payments",
    )?;
    Ok(balance >= reserved && balance - reserved >= amount)
}
/// Whether unreserved builder funds cover the exact bid while retaining the minimum deposit.
pub fn can_builder_cover_bid(
    state: &BeaconState,
    index: u64,
    amount: u64,
    parameters: &GloasParameters,
) -> Result<bool, ProcessingError> {
    checked_constants(state, parameters)?;
    can_cover_kernel(state, index, amount)
}
pub(crate) const fn empty_payment() -> BuilderPendingPayment {
    BuilderPendingPayment {
        weight: 0,
        withdrawal: gloas::BuilderPendingWithdrawal {
            fee_recipient: [0; 20],
            amount: 0,
            builder_index: 0,
        },
        proposer_index: 0,
    }
}
pub(crate) fn settle_builder_payment_kernel(
    state: &mut BeaconState,
    payment_index: u64,
    _parameters: &GloasParameters,
) -> Result<(), ProcessingError> {
    let index = common::index(payment_index, state.builder_pending_payments.values.len())?;
    let payment = &state.builder_pending_payments.values[index];
    if payment.withdrawal.amount > 0 {
        state.builder_pending_withdrawals.values.push(payment.withdrawal.clone());
    }
    state.builder_pending_payments.values[index] = empty_payment();
    Ok(())
}
/// Move a payment into the withdrawal queue and clear its ring position atomically.
pub fn settle_builder_payment(
    state: &mut BeaconState,
    index: u64,
    parameters: &GloasParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, _| {
        settle_builder_payment_kernel(candidate, index, parameters)
    })
}
pub(crate) fn initiate_builder_exit_kernel(
    state: &mut BeaconState,
    index: u64,
    parameters: &GloasParameters,
) -> Result<(), ProcessingError> {
    let index = common::index(index, state.builders.values.len())?;
    state.builders.values[index].withdrawable_epoch = add(
        state.slot / parameters.slots_per_epoch(),
        parameters.min_builder_withdrawability_delay,
        "builder withdrawable epoch",
    )?;
    Ok(())
}
/// Initiate only the builder exit state mutation; authorized request processing performs
/// eligibility.
pub fn initiate_builder_exit(
    state: &mut BeaconState,
    index: u64,
    parameters: &GloasParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, _| {
        initiate_builder_exit_kernel(candidate, index, parameters)
    })
}

/// Dedicated builder proof of possession; a validator deposit signature cannot be replayed here.
pub fn is_valid_builder_deposit_signature(
    request: &BuilderDepositRequest,
    parameters: &GloasParameters,
) -> Result<bool, ProcessingError> {
    parameters.validate()?;
    let message = merkleize(
        &[
            bytes_root(&request.pubkey, 48, false)?,
            request.withdrawal_credentials,
            uint64_root(request.amount),
        ],
        3,
    )?;
    let version =
        parameters.fulu.electra.deneb.capella.bellatrix.altair.phase0.genesis_fork_version;
    let root = compute_signing_root(message, compute_domain([14, 0, 0, 0], version, [0; 32]));
    Ok(verify_signature(&request.pubkey, &root, &request.signature).is_ok())
}
pub(crate) fn new_builder_index_kernel(state: &BeaconState, parameters: &GloasParameters) -> usize {
    let epoch = state.slot / parameters.slots_per_epoch();
    state
        .builders
        .values
        .iter()
        .position(|builder| builder.withdrawable_epoch <= epoch && builder.balance == 0)
        .unwrap_or(state.builders.values.len())
}
/// Find the first exited empty registry position, or append a new builder entry.
pub fn get_index_for_new_builder(
    state: &BeaconState,
    parameters: &GloasParameters,
) -> Result<u64, ProcessingError> {
    checked_constants(state, parameters)?;
    Ok(new_builder_index_kernel(state, parameters) as u64)
}
pub(crate) fn builder_deposit_kernel(
    state: &mut BeaconState,
    request: &BuilderDepositRequest,
    parameters: &GloasParameters,
) -> Result<(), ProcessingError> {
    if !is_builder_withdrawal_credential(request.withdrawal_credentials) {
        return Ok(());
    }
    if let Some(index) =
        state.builders.values.iter().position(|builder| builder.pubkey == request.pubkey)
    {
        let builder = &mut state.builders.values[index];
        if builder.withdrawable_epoch != u64::MAX && builder.balance == 0 {
            builder.withdrawable_epoch = add(
                state.slot / parameters.slots_per_epoch(),
                parameters.min_builder_withdrawability_delay,
                "exited builder top-up maturity",
            )?;
        }
        builder.balance = add(builder.balance, request.amount, "builder deposit top-up")?;
    } else if is_valid_builder_deposit_signature(request, parameters)? {
        let builder = gloas::Builder {
            pubkey: request.pubkey,
            version: 0,
            execution_address: request.withdrawal_credentials[12..]
                .try_into()
                .map_err(|_| ProcessingError::InvalidState("builder execution address length"))?,
            balance: request.amount,
            deposit_epoch: state.slot / parameters.slots_per_epoch(),
            withdrawable_epoch: u64::MAX,
        };
        let index = new_builder_index_kernel(state, parameters);
        if index == state.builders.values.len() {
            state.builders.values.push(builder);
        } else {
            state.builders.values[index] = builder;
        }
    }
    Ok(())
}
/// Process only the isolated builder deposit operation, with strict new-key proof of possession.
pub fn process_builder_deposit_request(
    state: &mut BeaconState,
    request: &BuilderDepositRequest,
    parameters: &GloasParameters,
) -> Result<(), ProcessingError> {
    request.hash_tree_root(parameters.preset())?;
    transact(state, parameters, |candidate, _| {
        builder_deposit_kernel(candidate, request, parameters)
    })
}
pub(crate) fn builder_exit_kernel(
    state: &mut BeaconState,
    request: &BuilderExitRequest,
    parameters: &GloasParameters,
) -> Result<(), ProcessingError> {
    let Some(index) =
        state.builders.values.iter().position(|builder| builder.pubkey == request.pubkey)
    else {
        return Ok(());
    };
    if !active_builder_kernel(state, index as u64)? ||
        state.builders.values[index].execution_address != request.source_address ||
        pending_builder_balance_kernel(state, index as u64)? != 0
    {
        return Ok(());
    }
    initiate_builder_exit_kernel(state, index as u64, parameters)
}
/// Honor only a finalized active builder's authorized exit with no outstanding payments.
pub fn process_builder_exit_request(
    state: &mut BeaconState,
    request: &BuilderExitRequest,
    parameters: &GloasParameters,
) -> Result<(), ProcessingError> {
    request.hash_tree_root(parameters.preset())?;
    transact(state, parameters, |candidate, _| builder_exit_kernel(candidate, request, parameters))
}

pub(crate) fn verify_bid_signature_kernel(
    state: &BeaconState,
    signed: &SignedExecutionPayloadBid,
    parameters: &GloasParameters,
) -> Result<(), ProcessingError> {
    let builder = &state.builders.values
        [common::index(signed.message.builder_index, state.builders.values.len())?];
    let root = compute_signing_root(
        signed.message.hash_tree_root(parameters.preset())?,
        common::domain(state, [11, 0, 0, 0], state.slot / parameters.slots_per_epoch()),
    );
    Ok(verify_signature(&builder.pubkey, &root, &signed.signature)?)
}
/// Verify a registered builder's bid signature only; self-build infinity rules are checked on
/// import.
pub fn verify_execution_payload_bid_signature(
    state: &BeaconState,
    signed: &SignedExecutionPayloadBid,
    parameters: &GloasParameters,
) -> Result<(), ProcessingError> {
    checked_constants(state, parameters)?;
    signed.hash_tree_root(parameters.preset())?;
    verify_bid_signature_kernel(state, signed, parameters)
}
pub(crate) fn bid_kernel(
    state: &mut BeaconState,
    signed: &SignedExecutionPayloadBid,
    parameters: &GloasParameters,
    constants: &Constants,
) -> Result<(), ProcessingError> {
    let bid = &signed.message;
    if bid.builder_index == BUILDER_INDEX_SELF_BUILD {
        let mut infinity = [0; 96];
        infinity[0] = 0xc0;
        if bid.value != 0 || signed.signature != infinity {
            return Err(ProcessingError::InvalidState(
                "self-build bid requires zero value and infinity signature",
            ));
        }
    } else {
        if !active_builder_kernel(state, bid.builder_index)? {
            return Err(ProcessingError::InvalidState("bid builder is not finalized and active"));
        }
        let builder =
            &state.builders.values[common::index(bid.builder_index, state.builders.values.len())?];
        if builder.version != 0 || !can_cover_kernel(state, bid.builder_index, bid.value)? {
            return Err(ProcessingError::InvalidState(
                "builder version or unreserved bid balance is invalid",
            ));
        }
        verify_bid_signature_kernel(state, signed, parameters)?;
    }
    let epoch = state.slot / parameters.slots_per_epoch();
    if bid.blob_kzg_commitments.values.len() as u64 >
        parameters.fulu.blob_parameters_at_epoch(epoch).max_blobs_per_block
    {
        return Err(ProcessingError::InvalidState(
            "bid blob commitment count exceeds active schedule",
        ));
    }
    if state.slot == 0 || bid.slot != state.slot {
        return Err(ProcessingError::InvalidState("bid slot is invalid"));
    }
    if bid.parent_block_hash != state.latest_block_hash {
        return Err(ProcessingError::InvalidState("bid execution parent linkage is invalid"));
    }
    if bid.block_hash == bid.parent_block_hash {
        return Err(ProcessingError::InvalidState(
            "bid block hash must differ from execution parent",
        ));
    }
    let prior = state
        .block_roots
        .values
        .get(position(state.slot - 1, constants.historical_slots)?)
        .ok_or(ProcessingError::InvalidState("block root ring is truncated"))?;
    if bid.parent_block_root != *prior {
        return Err(ProcessingError::InvalidState("bid parent beacon root differs"));
    }
    let randao = state
        .randao_mixes
        .values
        .get(position(epoch, constants.historical_epochs)?)
        .ok_or(ProcessingError::InvalidState("RANDAO ring is truncated"))?;
    if bid.prev_randao != *randao {
        return Err(ProcessingError::InvalidState("bid RANDAO differs"));
    }
    if bid.value > 0 {
        let payment_index = parameters.slots_per_epoch() + bid.slot % parameters.slots_per_epoch();
        let index = common::index(payment_index, state.builder_pending_payments.values.len())?;
        state.builder_pending_payments.values[index] = BuilderPendingPayment {
            weight: 0,
            withdrawal: gloas::BuilderPendingWithdrawal {
                fee_recipient: bid.fee_recipient,
                amount: bid.value,
                builder_index: bid.builder_index,
            },
            proposer_index: common::get_beacon_proposer_index(state, parameters, constants)?,
        };
    }
    state.latest_execution_payload_bid = bid.clone();
    Ok(())
}
/// Atomically verify the signed bid and reserve its payment; this does not verify its payload.
pub fn process_execution_payload_bid(
    state: &mut BeaconState,
    signed: &SignedExecutionPayloadBid,
    parameters: &GloasParameters,
) -> Result<(), ProcessingError> {
    signed.hash_tree_root(parameters.preset())?;
    transact(state, parameters, |candidate, constants| {
        bid_kernel(candidate, signed, parameters, constants)
    })
}

/// Whether an earlier queued validator deposit has a valid proof of possession for this key.
pub fn is_pending_validator(
    deposits: &[gloas::PendingDeposit],
    pubkey: &[u8; 48],
    parameters: &GloasParameters,
) -> Result<bool, ProcessingError> {
    for deposit in deposits {
        if &deposit.pubkey == pubkey &&
            super::execution_requests::is_valid_deposit_signature(
                deposit.pubkey,
                deposit.withdrawal_credentials,
                deposit.amount,
                deposit.signature,
                parameters,
            )?
        {
            return Ok(true);
        }
    }
    Ok(false)
}
pub(crate) fn add_builder_to_registry_kernel(
    state: &mut BeaconState,
    pubkey: [u8; 48],
    version: u8,
    address: [u8; 20],
    amount: u64,
    slot: u64,
    parameters: &GloasParameters,
) {
    let builder = gloas::Builder {
        pubkey,
        version,
        execution_address: address,
        balance: amount,
        deposit_epoch: slot / parameters.slots_per_epoch(),
        withdrawable_epoch: u64::MAX,
    };
    let index = new_builder_index_kernel(state, parameters);
    if index == state.builders.values.len() {
        state.builders.values.push(builder);
    } else {
        state.builders.values[index] = builder;
    }
}
pub(crate) fn onboard_builders_from_pending_deposits_kernel(
    state: &mut BeaconState,
    parameters: &GloasParameters,
) -> Result<(), ProcessingError> {
    let deposits = std::mem::take(&mut state.pending_deposits.values);
    let mut retained = Vec::new();
    for deposit in deposits {
        if state.validators.values.iter().any(|validator| validator.pubkey == deposit.pubkey) {
            retained.push(deposit);
            continue;
        }
        if let Some(index) =
            state.builders.values.iter().position(|builder| builder.pubkey == deposit.pubkey)
        {
            state.builders.values[index].balance = add(
                state.builders.values[index].balance,
                deposit.amount,
                "fork builder deposit top-up",
            )?;
        } else if !is_builder_withdrawal_credential(deposit.withdrawal_credentials) ||
            is_pending_validator(&retained, &deposit.pubkey, parameters)?
        {
            retained.push(deposit);
        } else if super::execution_requests::is_valid_deposit_signature(
            deposit.pubkey,
            deposit.withdrawal_credentials,
            deposit.amount,
            deposit.signature,
            parameters,
        )? {
            let address = deposit.withdrawal_credentials[12..]
                .try_into()
                .map_err(|_| ProcessingError::InvalidState("builder execution address length"))?;
            add_builder_to_registry_kernel(
                state,
                deposit.pubkey,
                0,
                address,
                deposit.amount,
                deposit.slot,
                parameters,
            );
        }
    }
    state.pending_deposits.values = retained;
    Ok(())
}
/// One-time fork onboarding uses validator-deposit proofs; later builder requests use DOMAIN14.
pub fn onboard_builders_from_pending_deposits(
    state: &mut BeaconState,
    parameters: &GloasParameters,
) -> Result<(), ProcessingError> {
    transact(state, parameters, |candidate, _| {
        onboard_builders_from_pending_deposits_kernel(candidate, parameters)
    })
}

#[cfg(test)]
mod tests {
    use super::{super::blocks::tests::state_and_keys, *};
    use crate::ssz::Preset;
    use blst::min_pk::SecretKey;
    use reth_embedded_beacon_crypto::ETHEREUM_BLS_DST;

    fn request(
        key: &SecretKey,
        amount: u64,
        domain_type: [u8; 4],
        parameters: &GloasParameters,
    ) -> BuilderDepositRequest {
        let mut credentials = [0; 32];
        credentials[0] = 0xb0;
        credentials[12..].fill(9);
        let pubkey = key.sk_to_pk().to_bytes();
        let message = merkleize(
            &[bytes_root(&pubkey, 48, false).unwrap(), credentials, uint64_root(amount)],
            3,
        )
        .unwrap();
        let domain = compute_domain(
            domain_type,
            parameters.fulu.electra.deneb.capella.bellatrix.altair.phase0.genesis_fork_version,
            [0; 32],
        );
        BuilderDepositRequest {
            pubkey,
            withdrawal_credentials: credentials,
            amount,
            signature: key
                .sign(&compute_signing_root(message, domain), ETHEREUM_BLS_DST, &[])
                .to_bytes(),
        }
    }
    #[test]
    fn new_builder_requires_its_dedicated_domain_and_exited_topups_never_reactivate() {
        let (mut state, _, parameters) = state_and_keys();
        let key = SecretKey::key_gen(&[110; 32], &[]).unwrap();
        let validator_deposit = request(&key, 1_000_000_000, [3, 0, 0, 0], &parameters);
        assert!(!is_valid_builder_deposit_signature(&validator_deposit, &parameters).unwrap());
        process_builder_deposit_request(&mut state, &validator_deposit, &parameters).unwrap();
        assert!(state.builders.values.is_empty());
        let valid = request(&key, 1_000_000_000, [14, 0, 0, 0], &parameters);
        process_builder_deposit_request(&mut state, &valid, &parameters).unwrap();
        assert_eq!(state.builders.values[0].execution_address, [9; 20]);
        state.finalized_checkpoint.epoch = 1;
        assert!(is_active_builder(&state, 0, &parameters).unwrap());
        state.builders.values[0].withdrawable_epoch = 0;
        state.builders.values[0].balance = 0;
        let mut topup = valid.clone();
        topup.amount = 7;
        topup.signature = [0; 96];
        process_builder_deposit_request(&mut state, &topup, &parameters).unwrap();
        assert_eq!(state.builders.values[0].balance, 7);
        assert_eq!(state.builders.values[0].withdrawable_epoch, 2);
        assert!(!is_active_builder(&state, 0, &parameters).unwrap());
        state.builders.values[0].balance = 0;
        let other = SecretKey::key_gen(&[111; 32], &[]).unwrap();
        state.slot = 16;
        let fresh = request(&other, 22, [14, 0, 0, 0], &parameters);
        process_builder_deposit_request(&mut state, &fresh, &parameters).unwrap();
        assert_eq!(state.builders.values.len(), 1);
        assert_eq!(state.builders.values[0].pubkey, fresh.pubkey);
        let before = state.clone();
        process_builder_deposit_request(&mut state, &topup, &parameters).unwrap();
        assert_eq!(state, before);
    }
    #[test]
    fn pending_payment_and_queued_withdrawal_are_both_reserved_and_exit_is_authorized() {
        let (mut state, keys, parameters) = state_and_keys();
        state.finalized_checkpoint.epoch = 1;
        state.builders.values.push(gloas::Builder {
            pubkey: keys[0].sk_to_pk().to_bytes(),
            version: 0,
            execution_address: [9; 20],
            balance: 1_000_000_100,
            deposit_epoch: 0,
            withdrawable_epoch: u64::MAX,
        });
        state.builder_pending_payments.values[8] = BuilderPendingPayment {
            weight: 0,
            withdrawal: gloas::BuilderPendingWithdrawal {
                fee_recipient: [1; 20],
                amount: 40,
                builder_index: 0,
            },
            proposer_index: 0,
        };
        state.builder_pending_withdrawals.values.push(gloas::BuilderPendingWithdrawal {
            fee_recipient: [2; 20],
            amount: 30,
            builder_index: 0,
        });
        assert_eq!(
            get_pending_balance_to_withdraw_for_builder(&state, 0, &parameters).unwrap(),
            70
        );
        assert!(can_builder_cover_bid(&state, 0, 30, &parameters).unwrap());
        assert!(!can_builder_cover_bid(&state, 0, 31, &parameters).unwrap());
        let request =
            BuilderExitRequest { pubkey: keys[0].sk_to_pk().to_bytes(), source_address: [9; 20] };
        process_builder_exit_request(&mut state, &request, &parameters).unwrap();
        assert_eq!(state.builders.values[0].withdrawable_epoch, u64::MAX);
        state.builder_pending_withdrawals.values.clear();
        state.builder_pending_payments.values[8] = empty_payment();
        let mut wrong = request.clone();
        wrong.source_address = [8; 20];
        process_builder_exit_request(&mut state, &wrong, &parameters).unwrap();
        assert_eq!(state.builders.values[0].withdrawable_epoch, u64::MAX);
        process_builder_exit_request(&mut state, &request, &parameters).unwrap();
        assert_eq!(state.builders.values[0].withdrawable_epoch, 2);
    }
    #[test]
    fn fork_onboarding_keeps_existing_and_earlier_valid_validator_deposits_and_recomputes_builder_keys(
    ) {
        let (mut state, keys, parameters) = state_and_keys();
        let new = SecretKey::key_gen(&[112; 32], &[]).unwrap();
        let other = SecretKey::key_gen(&[113; 32], &[]).unwrap();
        let to_pending = |request: BuilderDepositRequest, slot| gloas::PendingDeposit {
            pubkey: request.pubkey,
            withdrawal_credentials: request.withdrawal_credentials,
            amount: request.amount,
            signature: request.signature,
            slot,
        };
        let existing = to_pending(request(&keys[0], 3, [3, 0, 0, 0], &parameters), 1);
        let mut validator = request(&new, 7, [3, 0, 0, 0], &parameters);
        validator.withdrawal_credentials = [0; 32];
        let message = merkleize(
            &[
                bytes_root(&validator.pubkey, 48, false).unwrap(),
                validator.withdrawal_credentials,
                uint64_root(7),
            ],
            3,
        )
        .unwrap();
        validator.signature = new
            .sign(
                &compute_signing_root(
                    message,
                    compute_domain(
                        [3, 0, 0, 0],
                        parameters
                            .fulu
                            .electra
                            .deneb
                            .capella
                            .bellatrix
                            .altair
                            .phase0
                            .genesis_fork_version,
                        [0; 32],
                    ),
                ),
                ETHEREUM_BLS_DST,
                &[],
            )
            .to_bytes();
        let validator = to_pending(validator, 2);
        let retained_builder = to_pending(request(&new, 9, [3, 0, 0, 0], &parameters), 3);
        let builder = to_pending(request(&other, 11, [3, 0, 0, 0], &parameters), 4);
        let mut builder_topup = builder.clone();
        builder_topup.withdrawal_credentials = [0; 32];
        builder_topup.signature = [0; 96];
        builder_topup.amount = 5;
        state.pending_deposits.values = vec![
            existing.clone(),
            validator.clone(),
            retained_builder.clone(),
            builder,
            builder_topup,
        ];
        onboard_builders_from_pending_deposits(&mut state, &parameters).unwrap();
        assert_eq!(state.pending_deposits.values, vec![existing, validator, retained_builder]);
        assert_eq!(state.builders.values.len(), 1);
        assert_eq!(state.builders.values[0].pubkey, other.sk_to_pk().to_bytes());
        assert_eq!(state.builders.values[0].balance, 16);
        assert_eq!(state.builders.values[0].deposit_epoch, 0);
        assert_eq!(parameters.preset(), Preset::Minimal);
    }
    #[test]
    fn real_builder_bid_requires_reserved_funds_signature_and_active_blob_schedule() {
        let (mut state, keys, mut parameters) = state_and_keys();
        state.slot = 8;
        state.finalized_checkpoint.epoch = 1;
        parameters.fulu.fork_epoch = 0;
        parameters.fulu.blob_schedule =
            vec![crate::fulu_processing::BlobParameters { epoch: 0, max_blobs_per_block: 1 }];
        state.builders.values.push(gloas::Builder {
            pubkey: keys[0].sk_to_pk().to_bytes(),
            version: 0,
            execution_address: [9; 20],
            balance: 1_000_000_100,
            deposit_epoch: 0,
            withdrawable_epoch: u64::MAX,
        });
        state.builder_pending_withdrawals.values.push(gloas::BuilderPendingWithdrawal {
            fee_recipient: [9; 20],
            amount: 60,
            builder_index: 0,
        });
        state.builder_pending_payments.values[0] = BuilderPendingPayment {
            weight: 0,
            withdrawal: gloas::BuilderPendingWithdrawal {
                fee_recipient: [9; 20],
                amount: 30,
                builder_index: 0,
            },
            proposer_index: 0,
        };
        let mut message = gloas::ExecutionPayloadBid {
            parent_block_hash: state.latest_block_hash,
            parent_block_root: state.block_roots.values[7],
            block_hash: [11; 32],
            prev_randao: state.randao_mixes.values[1],
            fee_recipient: [17; 20],
            gas_limit: 30_000_000,
            builder_index: 0,
            slot: 8,
            value: 10,
            execution_payment: 0,
            blob_kzg_commitments: crate::ssz::ProgressiveList::from_vec(vec![[1; 48]]),
            execution_requests_root: super::super::blocks::tests::empty_requests()
                .hash_tree_root(Preset::Minimal)
                .unwrap(),
        };
        let domain = common::domain(&state, [11, 0, 0, 0], 1);
        let sign = |message: gloas::ExecutionPayloadBid| {
            let signature = keys[0]
                .sign(
                    &compute_signing_root(
                        message.hash_tree_root(parameters.preset()).unwrap(),
                        domain,
                    ),
                    ETHEREUM_BLS_DST,
                    &[],
                )
                .to_bytes();
            SignedExecutionPayloadBid { message, signature }
        };
        let valid = sign(message.clone());
        let mut bad = valid.clone();
        bad.signature = [0; 96];
        let before = state.clone();
        assert!(process_execution_payload_bid(&mut state, &bad, &parameters).is_err());
        assert_eq!(state, before);
        message.value = 11;
        let bad = sign(message.clone());
        assert!(process_execution_payload_bid(&mut state, &bad, &parameters).is_err());
        assert_eq!(state, before);
        message.value = 10;
        message.blob_kzg_commitments.values.push([2; 48]);
        let bad = sign(message);
        assert!(matches!(
            process_execution_payload_bid(&mut state, &bad, &parameters),
            Err(ProcessingError::InvalidState("bid blob commitment count exceeds active schedule"))
        ));
        assert_eq!(state, before);
        process_execution_payload_bid(&mut state, &valid, &parameters).unwrap();
        assert_eq!(state.latest_execution_payload_bid, valid.message);
        assert_eq!(
            state.builder_pending_payments.values[8].withdrawal,
            gloas::BuilderPendingWithdrawal {
                fee_recipient: [17; 20],
                amount: 10,
                builder_index: 0
            }
        );
        assert_eq!(
            state.builder_pending_payments.values[0],
            before.builder_pending_payments.values[0]
        );
    }
}

#[cfg(feature = "spec-tests")]
#[path = "builder_artificial.rs"]
pub(crate) mod artificial;
