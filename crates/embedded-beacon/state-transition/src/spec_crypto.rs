//! Explicit published-fixture cryptographic policy; excluded from every runtime build.
#![cfg(feature = "spec-tests")]
use reth_embedded_beacon_crypto::CryptoError;
use reth_embedded_beacon_primitives::Root;
/// Fixture metadata disables signature verification; this returns no runtime validation proof.
pub(crate) const fn skip_signature_for_artificial_fixture(
    _: &[u8; 48],
    _: &Root,
    _: &[u8; 96],
) -> Result<(), CryptoError> {
    Ok(())
}
/// Fixture-only aggregate counterpart; index/committee checks remain in the fixture kernel.
pub(crate) const fn skip_aggregate_for_artificial_fixture(
    _: &[[u8; 48]],
    _: &Root,
    _: &[u8; 96],
) -> Result<(), CryptoError> {
    Ok(())
}
