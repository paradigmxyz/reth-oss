//! Ethereum BLS verification and fork-aware signing-domain hashing.

use blst::{min_pk::Signature, BLST_ERROR};
use reth_embedded_beacon_primitives::{hash_pair, Root};
use thiserror::Error;

mod parallel_keys;
mod validated_keys;

/// Ethereum proof-of-possession ciphersuite.
pub const ETHEREUM_BLS_DST: &[u8] = b"BLS_SIG_BLS12381G2_XMD:SHA-256_SSWU_RO_POP_";

/// Cryptographic validation failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub enum CryptoError {
    /// Invalid Public Key.
    #[error("invalid BLS public key")]
    InvalidPublicKey,
    /// Invalid Signature.
    #[error("invalid BLS signature")]
    InvalidSignature,
    /// Verification Failed.
    #[error("BLS signature verification failed")]
    VerificationFailed,
    /// Empty Aggregate.
    #[error("aggregate must contain at least one participant")]
    EmptyAggregate,
    /// Aggregate Length Mismatch.
    #[error("aggregate public key and message lengths disagree")]
    AggregateLengthMismatch,
}

/// Hash the SSZ `ForkData` container.
pub fn compute_fork_data_root(fork_version: [u8; 4], genesis_validators_root: Root) -> Root {
    let mut version = [0; 32];
    version[..4].copy_from_slice(&fork_version);
    hash_pair(version, genesis_validators_root)
}

/// Domain type followed by the first 28 bytes of `ForkData`'s root.
pub fn compute_domain(
    domain_type: [u8; 4],
    fork_version: [u8; 4],
    genesis_validators_root: Root,
) -> Root {
    let mut domain = [0; 32];
    domain[..4].copy_from_slice(&domain_type);
    domain[4..]
        .copy_from_slice(&compute_fork_data_root(fork_version, genesis_validators_root)[..28]);
    domain
}

/// Hash the SSZ `SigningData` container.
pub fn compute_signing_root(object_root: Root, domain: Root) -> Root {
    hash_pair(object_root, domain)
}

/// Decode and validate a compressed public key, rejecting infinity and wrong subgroups.
pub fn validate_public_key(bytes: &[u8; 48]) -> Result<(), CryptoError> {
    validated_keys::validate(bytes).map(|_| ())
}

/// Verify a single signature with subgroup and infinity checks.
pub fn verify_signature(
    public_key: &[u8; 48],
    message: &[u8],
    signature: &[u8; 96],
) -> Result<(), CryptoError> {
    let public_key = validated_keys::validate(public_key)?;
    let signature =
        Signature::sig_validate(signature, true).map_err(|_| CryptoError::InvalidSignature)?;
    verification_result(signature.verify(false, message, ETHEREUM_BLS_DST, &[], &public_key, false))
}

/// Verify an aggregate over one message.
///
/// Public keys must originate from validators with established proofs of possession.
/// Subgroup checks here do not establish proof of possession.
pub fn fast_aggregate_verify(
    public_keys: &[[u8; 48]],
    message: &[u8],
    signature: &[u8; 96],
) -> Result<(), CryptoError> {
    if public_keys.is_empty() {
        return Err(CryptoError::EmptyAggregate);
    }
    let keys = parallel_keys::validate_keys(public_keys)?;
    let references = keys.iter().collect::<Vec<_>>();
    let signature =
        Signature::sig_validate(signature, true).map_err(|_| CryptoError::InvalidSignature)?;
    verification_result(signature.fast_aggregate_verify(
        false,
        message,
        ETHEREUM_BLS_DST,
        &references,
    ))
}

/// Verify an aggregate over distinct messages with validated public keys.
pub fn aggregate_verify(
    public_keys: &[[u8; 48]],
    messages: &[&[u8]],
    signature: &[u8; 96],
) -> Result<(), CryptoError> {
    if public_keys.is_empty() {
        return Err(CryptoError::EmptyAggregate);
    }
    if public_keys.len() != messages.len() {
        return Err(CryptoError::AggregateLengthMismatch);
    }
    let keys = parallel_keys::validate_keys(public_keys)?;
    let references = keys.iter().collect::<Vec<_>>();
    let signature =
        Signature::sig_validate(signature, true).map_err(|_| CryptoError::InvalidSignature)?;
    verification_result(signature.aggregate_verify(
        false,
        messages,
        ETHEREUM_BLS_DST,
        &references,
        false,
    ))
}

fn verification_result(result: BLST_ERROR) -> Result<(), CryptoError> {
    if result == BLST_ERROR::BLST_SUCCESS {
        Ok(())
    } else {
        Err(CryptoError::VerificationFailed)
    }
}

/// Aggregate nonempty public keys with subgroup and infinity checks on every input.
pub fn aggregate_pubkeys(public_keys: &[[u8; 48]]) -> Result<[u8; 48], CryptoError> {
    if public_keys.is_empty() {
        return Err(CryptoError::EmptyAggregate);
    }
    let keys = parallel_keys::validate_keys(public_keys)?;
    let references = keys.iter().collect::<Vec<_>>();
    blst::min_pk::AggregatePublicKey::aggregate(&references, false)
        .map(|key| key.to_public_key().to_bytes())
        .map_err(|_| CryptoError::InvalidPublicKey)
}

/// Consensus Altair aggregate verification with its empty-participant infinity exception.
///
/// Public keys must have established proofs of possession, as in a validated registry.
pub fn eth_fast_aggregate_verify(
    public_keys: &[[u8; 48]],
    message: &[u8],
    signature: &[u8; 96],
) -> Result<(), CryptoError> {
    if public_keys.is_empty() {
        let mut infinity = [0; 96];
        infinity[0] = 0xc0;
        return if *signature == infinity { Ok(()) } else { Err(CryptoError::VerificationFailed) };
    }
    fast_aggregate_verify(public_keys, message, signature)
}

/// Subtract a nonempty validated public-key aggregate from a validated total key.
///
/// Inputs are subgroup-checked. The result may be infinity; its eventual signature
/// verifier must apply the consensus nonempty-participant rule.
pub fn subtract_aggregate_pubkeys(
    total: &[u8; 48],
    removed: &[[u8; 48]],
) -> Result<[u8; 48], CryptoError> {
    if removed.is_empty() {
        return Err(CryptoError::EmptyAggregate)
    }
    let total = validated_keys::validate(total)?;
    let keys = parallel_keys::validate_keys(removed)?;
    let references = keys.iter().collect::<Vec<_>>();
    let removed = blst::min_pk::AggregatePublicKey::aggregate(&references, false)
        .map_err(|_| CryptoError::InvalidPublicKey)?;
    let mut total = blst::min_pk::AggregatePublicKey::from_public_key(&total);
    total.sub_aggregate(&removed);
    Ok(total.to_public_key().to_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use blst::min_pk::{AggregateSignature, PublicKey, SecretKey};
    use std::{hint::black_box, time::Instant};

    #[test]
    fn checks_valid_signature_and_wrong_domain() {
        let key = SecretKey::key_gen(&[42; 32], &[]).unwrap();
        let domain = compute_domain([0, 0, 0, 0], [1, 0, 0, 0], [9; 32]);
        let root = compute_signing_root([7; 32], domain);
        let signature = key.sign(&root, ETHEREUM_BLS_DST, &[]).to_bytes();
        assert_eq!(verify_signature(&key.sk_to_pk().to_bytes(), &root, &signature), Ok(()));
        let wrong_root =
            compute_signing_root([7; 32], compute_domain([0; 4], [2, 0, 0, 0], [9; 32]));
        assert_eq!(
            verify_signature(&key.sk_to_pk().to_bytes(), &wrong_root, &signature),
            Err(CryptoError::VerificationFailed)
        );
        assert_eq!(validate_public_key(&[0; 48]), Err(CryptoError::InvalidPublicKey));
    }

    #[test]
    fn aggregate_checks_participants() {
        let first = SecretKey::key_gen(&[1; 32], &[]).unwrap();
        let second = SecretKey::key_gen(&[2; 32], &[]).unwrap();
        let signatures = [
            first.sign(b"message", ETHEREUM_BLS_DST, &[]),
            second.sign(b"message", ETHEREUM_BLS_DST, &[]),
        ];
        let aggregate = AggregateSignature::aggregate(&signatures.iter().collect::<Vec<_>>(), true)
            .unwrap()
            .to_signature()
            .to_bytes();
        let keys = [first.sk_to_pk().to_bytes(), second.sk_to_pk().to_bytes()];
        assert_eq!(fast_aggregate_verify(&keys, b"message", &aggregate), Ok(()));
        assert_eq!(
            fast_aggregate_verify(&keys[..1], b"message", &aggregate),
            Err(CryptoError::VerificationFailed)
        );
        assert_eq!(
            fast_aggregate_verify(&[], b"message", &aggregate),
            Err(CryptoError::EmptyAggregate)
        );
    }
    #[test]
    fn aggregate_subtraction_preserves_duplicate_multiplicity() {
        let first = SecretKey::key_gen(&[1; 32], &[]).unwrap().sk_to_pk().to_bytes();
        let second = SecretKey::key_gen(&[2; 32], &[]).unwrap().sk_to_pk().to_bytes();
        let total = aggregate_pubkeys(&[first, second, first]).unwrap();
        assert_eq!(
            subtract_aggregate_pubkeys(&total, &[first]),
            Ok(aggregate_pubkeys(&[first, second]).unwrap())
        );
        assert_eq!(subtract_aggregate_pubkeys(&total, &[first, first]), Ok(second));
        assert_eq!(
            subtract_aggregate_pubkeys(&[0; 48], &[first]),
            Err(CryptoError::InvalidPublicKey)
        );
        assert_eq!(
            subtract_aggregate_pubkeys(&total, &[[0; 48]]),
            Err(CryptoError::InvalidPublicKey)
        );
        assert_eq!(subtract_aggregate_pubkeys(&total, &[]), Err(CryptoError::EmptyAggregate));
    }

    /// Use a real-size slot aggregate with valid keys and signatures, preserving multiplicity.
    #[test]
    #[ignore = "explicit Hoodi-size mandatory BLS preparation measurement"]
    fn hoodi_attestation_bls_preparation_latency() {
        let participants = 35_772;
        let message = [73; 32];
        // Key generation is outside the timed section. Distinct fixture keys repeat only
        // to keep setup small. The scalar reference checks each participant; the bounded cache
        // may reuse exact checked keys, so this repeated-key fixture is not a cold live-block
        // throughput estimate.
        let distinct = 256;
        let secrets = (0..distinct)
            .map(|index: u64| {
                let mut seed = [0; 32];
                seed[..8].copy_from_slice(&index.to_le_bytes());
                SecretKey::key_gen(&seed, &[]).unwrap()
            })
            .collect::<Vec<_>>();
        let public = secrets.iter().map(|secret| secret.sk_to_pk().to_bytes()).collect::<Vec<_>>();
        let signatures = secrets
            .iter()
            .map(|secret| secret.sign(&message, ETHEREUM_BLS_DST, &[]))
            .collect::<Vec<_>>();
        let keys =
            (0..participants).map(|index| public[index % distinct as usize]).collect::<Vec<_>>();
        let references = (0..participants)
            .map(|index| &signatures[index % distinct as usize])
            .collect::<Vec<_>>();
        let signature =
            AggregateSignature::aggregate(&references, false).unwrap().to_signature().to_bytes();
        let started = Instant::now();
        let decoded = keys
            .iter()
            .map(|key| PublicKey::key_validate(black_box(key)).unwrap())
            .collect::<Vec<_>>();
        let preparation = started.elapsed();
        let decoded_references = decoded.iter().collect::<Vec<_>>();
        let checked_signature = Signature::sig_validate(&signature, true).unwrap();
        let started = Instant::now();
        assert_eq!(
            verification_result(checked_signature.fast_aggregate_verify(
                false,
                &message,
                ETHEREUM_BLS_DST,
                &decoded_references,
            )),
            Ok(())
        );
        let prepared_verification = started.elapsed();
        let started = Instant::now();
        let parallel = parallel_keys::validate_keys(black_box(&keys)).unwrap();
        let parallel_preparation = started.elapsed();
        assert_eq!(parallel.iter().map(PublicKey::to_bytes).collect::<Vec<_>>(), keys);
        let started = Instant::now();
        assert_eq!(serial_fast_aggregate_verify(black_box(&keys), &message, &signature), Ok(()));
        let serial_complete = started.elapsed();
        let started = Instant::now();
        assert_eq!(fast_aggregate_verify(black_box(&keys), &message, &signature), Ok(()));
        let complete = started.elapsed();
        eprintln!("Hoodi-size repeated-key BLS aggregate: participants={participants} distinct_fixture_keys={distinct} serial_key_validation_ms={} cached_parallel_key_preparation_ms={} prepared_verification_ms={} serial_strict_verification_ms={} cached_parallel_strict_verification_ms={}",
            preparation.as_millis(), parallel_preparation.as_millis(), prepared_verification.as_millis(), serial_complete.as_millis(), complete.as_millis());
    }

    fn serial_fast_aggregate_verify(
        public_keys: &[[u8; 48]],
        message: &[u8],
        signature: &[u8; 96],
    ) -> Result<(), CryptoError> {
        if public_keys.is_empty() {
            return Err(CryptoError::EmptyAggregate)
        }
        let keys = parallel_keys::serial_keys(public_keys)?;
        let references = keys.iter().collect::<Vec<_>>();
        let signature =
            Signature::sig_validate(signature, true).map_err(|_| CryptoError::InvalidSignature)?;
        verification_result(signature.fast_aggregate_verify(
            false,
            message,
            ETHEREUM_BLS_DST,
            &references,
        ))
    }

    fn signed_aggregate(participants: usize, message: &[u8]) -> (Vec<[u8; 48]>, [u8; 96]) {
        let first = SecretKey::key_gen(&[7; 32], &[]).unwrap();
        let second = SecretKey::key_gen(&[19; 32], &[]).unwrap();
        let public = [first.sk_to_pk().to_bytes(), second.sk_to_pk().to_bytes()];
        let signatures = [
            first.sign(message, ETHEREUM_BLS_DST, &[]),
            second.sign(message, ETHEREUM_BLS_DST, &[]),
        ];
        let keys = (0..participants).map(|index| public[usize::from(index % 3 == 0)]).collect();
        let references = (0..participants)
            .map(|index| &signatures[usize::from(index % 3 == 0)])
            .collect::<Vec<_>>();
        let signature =
            AggregateSignature::aggregate(&references, false).unwrap().to_signature().to_bytes();
        (keys, signature)
    }

    #[test]
    fn parallel_fast_aggregate_preserves_serial_signature_domain_and_error_precedence() {
        let domain = compute_domain([1, 0, 0, 0], [7; 4], [19; 32]);
        let root = compute_signing_root([73; 32], domain);
        let wrong_root =
            compute_signing_root([73; 32], compute_domain([1, 0, 0, 0], [8; 4], [19; 32]));
        for participants in [1, 2_049] {
            let (keys, signature) = signed_aggregate(participants, &root);
            for (message, signature) in
                [(&root, signature), (&wrong_root, signature), (&root, [0; 96])]
            {
                assert_eq!(
                    fast_aggregate_verify(&keys, message, &signature),
                    serial_fast_aggregate_verify(&keys, message, &signature)
                );
            }
            assert_eq!(fast_aggregate_verify(&keys, &root, &signature), Ok(()));
            assert_eq!(
                fast_aggregate_verify(&keys, &wrong_root, &signature),
                Err(CryptoError::VerificationFailed)
            );
            assert_eq!(
                fast_aggregate_verify(&keys, &root, &[0; 96]),
                Err(CryptoError::InvalidSignature)
            );
            for position in [0, participants / 2, participants - 1] {
                let mut malformed = keys.clone();
                malformed[position] = [0; 48];
                assert_eq!(
                    fast_aggregate_verify(&malformed, &root, &[0; 96]),
                    Err(CryptoError::InvalidPublicKey)
                );
                assert_eq!(
                    fast_aggregate_verify(&malformed, &root, &[0; 96]),
                    serial_fast_aggregate_verify(&malformed, &root, &[0; 96])
                );
            }
            assert_eq!(
                fast_aggregate_verify(&keys[..participants - 1], &root, &signature),
                serial_fast_aggregate_verify(&keys[..participants - 1], &root, &signature)
            );
        }
        assert_eq!(fast_aggregate_verify(&[], &root, &[0; 96]), Err(CryptoError::EmptyAggregate));
    }
}
