//! Explicit artificial published-fixture kernels; absent from runtime builds.
#![cfg(feature = "spec-tests")]
use super::*;
use crate::spec_crypto::skip_signature_for_artificial_fixture as verify_signature;

pub(crate) fn is_valid_deposit_signature(
    pubkey: [u8; 48],
    credentials: Root,
    amount: u64,
    signature: [u8; 96],
    parameters: &GloasParameters,
) -> Result<bool, ProcessingError> {
    parameters.validate()?;
    let message =
        merkleize(&[bytes_root(&pubkey, 48, false)?, credentials, uint64_root(amount)], 3)?;
    let domain = compute_domain(
        [3, 0, 0, 0],
        parameters.fulu.electra.deneb.capella.bellatrix.altair.phase0.genesis_fork_version,
        [0; 32],
    );
    Ok(verify_signature(&pubkey, &compute_signing_root(message, domain), &signature).is_ok())
}
