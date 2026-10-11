//! Re-serving repeats real KZG and complete original signed-header association checks.

use super::{
    super::tests::{candidate, config},
    *,
};
use reth_embedded_beacon_network::Protocol;

#[test]
fn original_body_and_real_proof_revalidate_while_changed_proof_or_signature_fails() {
    let config = config();
    let (wire, _, sidecar) = candidate(&config);
    let signed = SignedBeaconBlock::from_ssz(wire.ssz(), Fork::Deneb, Preset::Minimal).unwrap();
    let original = WireSidecar::from_ssz(
        sidecar.to_ssz(Preset::Minimal).unwrap(),
        wire.context(),
        Protocol::BlobSidecarsByRootV1,
        &config,
    )
    .unwrap();
    verify_original(&signed, wire.root(), &[original], &config).unwrap();
    let mut bad_proof = sidecar.clone();
    bad_proof.kzg_proof[0] ^= 1;
    let bad_proof = WireSidecar::from_ssz(
        bad_proof.to_ssz(Preset::Minimal).unwrap(),
        wire.context(),
        Protocol::BlobSidecarsByRootV1,
        &config,
    )
    .unwrap();
    assert!(matches!(
        verify_original(&signed, wire.root(), &[bad_proof], &config),
        Err(RetrievalError::Proof)
    ));
    let mut bad_signature = sidecar;
    bad_signature.signed_block_header.signature = [0; 96];
    let bad_signature = WireSidecar::from_ssz(
        bad_signature.to_ssz(Preset::Minimal).unwrap(),
        wire.context(),
        Protocol::BlobSidecarsByRootV1,
        &config,
    )
    .unwrap();
    assert!(matches!(
        verify_original(&signed, wire.root(), &[bad_signature], &config),
        Err(RetrievalError::Association)
    ));
}
