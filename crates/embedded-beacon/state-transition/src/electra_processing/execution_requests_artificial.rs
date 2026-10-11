//! Explicit artificial published-fixture kernels; absent from runtime builds.
#![cfg(feature = "spec-tests")]
use super::*;
use crate::spec_crypto::skip_signature_for_artificial_fixture as verify_signature;

pub(crate) fn is_valid_deposit_signature(
    pubkey: [u8; 48],
    credentials: Root,
    amount: u64,
    signature: [u8; 96],
    parameters: &ElectraParameters,
) -> Result<bool, ProcessingError> {
    parameters.validate()?;
    let message =
        merkleize(&[bytes_root(&pubkey, 48, false)?, credentials, uint64_root(amount)], 3)?;
    let domain = compute_domain(
        [3, 0, 0, 0],
        parameters.deneb.capella.bellatrix.altair.phase0.genesis_fork_version,
        [0; 32],
    );
    Ok(verify_signature(&pubkey, &compute_signing_root(message, domain), &signature).is_ok())
}

pub(crate) fn apply_deposit_kernel(
    state: &mut BeaconState,
    pubkey: [u8; 48],
    credentials: Root,
    amount: u64,
    signature: [u8; 96],
    parameters: &ElectraParameters,
) -> Result<(), ProcessingError> {
    if validator_index(state, &pubkey).is_none() {
        if !is_valid_deposit_signature(pubkey, credentials, amount, signature, parameters)? {
            return Ok(());
        }
        require_pending_capacity(state)?;
        add_validator(state, pubkey, credentials, 0)?;
    }
    require_pending_capacity(state)?;
    state.pending_deposits.values.push(PendingDeposit {
        pubkey,
        withdrawal_credentials: credentials,
        amount,
        signature,
        slot: 0,
    });
    Ok(())
}

pub(crate) fn deposit_kernel(
    state: &mut BeaconState,
    deposit: &Deposit,
    parameters: &ElectraParameters,
) -> Result<(), ProcessingError> {
    if !is_valid_merkle_branch(
        deposit.data.hash_tree_root(parameters.preset())?,
        &deposit.proof.values,
        33,
        state.eth1_deposit_index,
        state.eth1_data.deposit_root,
    ) {
        return Err(ProcessingError::InvalidState("deposit Merkle proof is invalid"));
    }
    state.eth1_deposit_index = add(state.eth1_deposit_index, 1, "ordered deposit index")?;
    apply_deposit_kernel(
        state,
        deposit.data.pubkey,
        deposit.data.withdrawal_credentials,
        deposit.data.amount,
        deposit.data.signature,
        parameters,
    )
}
