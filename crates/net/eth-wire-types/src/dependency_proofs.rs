//! EIP-8512 dependency proof exchange over eth/74.

use alloc::vec::Vec;
use alloy_eips::eip8288::MAX_DEPENDENCY_PROOF_BYTES;
use alloy_primitives::{keccak256, Bytes, B256};
use alloy_rlp::{Decodable, Header, RlpEncodableWrapper};

/// Local bound on hashes and response entries in one dependency proof exchange.
pub const MAX_DEPENDENCY_PROOFS: usize = 1024;

/// Requests proofs in block-hash order. Request IDs are carried by `RequestPair`.
#[derive(Clone, Debug, Default, PartialEq, Eq, RlpEncodableWrapper)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct GetDependencyProofs(
    /// Block hashes in the requested response order.
    pub Vec<B256>,
);

impl Decodable for GetDependencyProofs {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = Header::decode_bytes(buf, true)?;
        let mut hashes = Vec::new();
        while !payload.is_empty() {
            if hashes.len() == MAX_DEPENDENCY_PROOFS {
                return Err(alloy_rlp::Error::Custom("too many dependency proof hashes"))
            }
            hashes.push(B256::decode(&mut payload)?);
        }
        Ok(Self(hashes))
    }
}

/// Proof byte strings for a prefix of the request, including empty unavailable entries.
#[derive(Clone, Debug, Default, PartialEq, Eq, RlpEncodableWrapper)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct DependencyProofs(
    /// Proof byte strings in request order; empty entries may mean unavailable data.
    pub Vec<Bytes>,
);

impl DependencyProofs {
    /// Authenticates the entire response before any entry may be consumed.
    ///
    /// Commitments must be in request order and obtained from trusted headers. Empty entries
    /// whose commitments differ from `keccak256("")` mean unavailable data, not invalid proofs.
    /// A shorter response covers only the corresponding prefix of the request.
    pub fn validate_response(&self, commitments: &[B256]) -> Result<(), DependencyProofError> {
        if self.0.len() > commitments.len() {
            return Err(DependencyProofError::TooManyProofs)
        }
        for (proof, commitment) in self.0.iter().zip(commitments) {
            if proof.len() > MAX_DEPENDENCY_PROOF_BYTES {
                return Err(DependencyProofError::ProofTooLarge)
            }
            if !proof.is_empty() && keccak256(proof) != *commitment {
                return Err(DependencyProofError::CommitmentMismatch)
            }
        }
        Ok(())
    }
}

impl Decodable for DependencyProofs {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = Header::decode_bytes(buf, true)?;
        let mut proofs = Vec::new();
        while !payload.is_empty() {
            if proofs.len() == MAX_DEPENDENCY_PROOFS {
                return Err(alloy_rlp::Error::Custom("too many dependency proofs"))
            }
            // Check the borrowed byte string before allocating or hashing peer-controlled data.
            let proof = Header::decode_bytes(&mut payload, false)?;
            if proof.len() > MAX_DEPENDENCY_PROOF_BYTES {
                return Err(alloy_rlp::Error::Custom("dependency proof too large"))
            }
            proofs.push(Bytes::copy_from_slice(proof));
        }
        Ok(Self(proofs))
    }
}

/// An invalid dependency proof response. Unavailability is not an error.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum DependencyProofError {
    /// More entries than requested.
    #[error("too many dependency proofs")]
    TooManyProofs,
    /// A proof exceeds the EIP-8288 size limit.
    #[error("dependency proof too large")]
    ProofTooLarge,
    /// A non-empty proof does not match its header commitment.
    #[error("dependency proof commitment mismatch")]
    CommitmentMismatch,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        message::{MessageError, RequestPair},
        EthMessage, EthMessageID, EthNetworkPrimitives, EthVersion, ProtocolMessage,
    };
    use alloc::vec;
    use alloy_rlp::Encodable;
    use proptest::prelude::*;

    #[test]
    fn canonical_wire_encoding_and_version_gate() {
        let response = ProtocolMessage::from(EthMessage::<EthNetworkPrimitives>::DependencyProofs(
            RequestPair {
                request_id: 1,
                message: DependencyProofs(vec![Bytes::new(), Bytes::from_static(&[0xaa, 0xbb])]),
            },
        ));
        let encoded = alloy_rlp::encode(&response);
        assert_eq!(encoded, [0x17, 0xc6, 0x01, 0xc4, 0x80, 0x82, 0xaa, 0xbb]);
        assert_eq!(
            ProtocolMessage::decode_message(EthVersion::Eth74, &mut encoded.as_slice()).unwrap(),
            response
        );
        for version in [EthVersion::Eth71, EthVersion::Eth72, EthVersion::Eth73] {
            assert!(
                matches!(ProtocolMessage::<EthNetworkPrimitives>::decode_message(version, &mut encoded.as_slice()), Err(MessageError::Invalid(v, EthMessageID::DependencyProofs)) if v == version)
            );
        }
        assert_eq!(EthMessageID::message_count(EthVersion::Eth74), 24);
        assert_eq!(EthVersion::try_from(74).unwrap(), EthVersion::Eth74);
    }

    #[test]
    fn unavailable_and_unserved_are_valid_but_bad_proofs_discard_the_batch() {
        let proof = Bytes::from_static(b"proof");
        let commitments = [keccak256(&proof), keccak256(b"other"), keccak256(b"")];
        assert_eq!(
            DependencyProofs(vec![proof.clone(), Bytes::new(), Bytes::new()])
                .validate_response(&commitments),
            Ok(())
        );
        assert_eq!(DependencyProofs(vec![proof.clone()]).validate_response(&commitments), Ok(()));
        assert_eq!(DependencyProofs::default().validate_response(&commitments), Ok(()));
        assert_eq!(
            DependencyProofs(vec![proof.clone(), proof]).validate_response(&commitments),
            Err(DependencyProofError::CommitmentMismatch)
        );
        assert_eq!(
            DependencyProofs(vec![Bytes::new(); 4]).validate_response(&commitments),
            Err(DependencyProofError::TooManyProofs)
        );
        assert_eq!(
            DependencyProofs(vec![vec![0; MAX_DEPENDENCY_PROOF_BYTES + 1].into()])
                .validate_response(&commitments),
            Err(DependencyProofError::ProofTooLarge)
        );
    }

    #[test]
    fn reject_oversized_and_nested_proof_entries() {
        assert_eq!(
            DependencyProofs::decode(&mut [0xc1, 0xc0].as_slice()),
            Err(alloy_rlp::Error::UnexpectedList)
        );
        let oversized =
            alloy_rlp::encode(DependencyProofs(vec![
                vec![0; MAX_DEPENDENCY_PROOF_BYTES + 1].into()
            ]));
        assert_eq!(
            DependencyProofs::decode(&mut oversized.as_slice()),
            Err(alloy_rlp::Error::Custom("dependency proof too large"))
        );
        let maximum = DependencyProofs(vec![vec![1; MAX_DEPENDENCY_PROOF_BYTES].into()]);
        assert_eq!(
            alloy_rlp::decode_exact::<DependencyProofs>(alloy_rlp::encode(&maximum)).unwrap(),
            maximum
        );
        let many =
            alloy_rlp::encode(DependencyProofs(vec![Bytes::new(); MAX_DEPENDENCY_PROOFS + 1]));
        assert_eq!(
            DependencyProofs::decode(&mut many.as_slice()),
            Err(alloy_rlp::Error::Custom("too many dependency proofs"))
        );
        let many =
            alloy_rlp::encode(GetDependencyProofs(vec![B256::ZERO; MAX_DEPENDENCY_PROOFS + 1]));
        assert_eq!(
            GetDependencyProofs::decode(&mut many.as_slice()),
            Err(alloy_rlp::Error::Custom("too many dependency proof hashes"))
        );
    }

    #[test]
    fn headers_preserve_dependency_commitments() {
        for enabled in [false, true] {
            let header = alloy_consensus::Header {
                base_fee_per_gas: enabled.then_some(0),
                withdrawals_root: enabled.then_some(B256::ZERO),
                blob_gas_used: enabled.then_some(0),
                excess_blob_gas: enabled.then_some(0),
                parent_beacon_block_root: enabled.then_some(B256::ZERO),
                requests_hash: enabled.then_some(B256::ZERO),
                block_access_list_hash: enabled.then_some(B256::ZERO),
                slot_number: enabled.then_some(0),
                dependency_proof_hash: enabled.then(|| keccak256(b"proof")),
                block_deps_hash: enabled.then_some(B256::repeat_byte(2)),
                ..Default::default()
            };
            let message = ProtocolMessage::from(EthMessage::<EthNetworkPrimitives>::BlockHeaders(
                RequestPair { request_id: 7, message: crate::BlockHeaders(vec![header]) },
            ));
            let encoded = alloy_rlp::encode(&message);
            assert_eq!(
                ProtocolMessage::decode_message(EthVersion::Eth74, &mut encoded.as_slice())
                    .unwrap(),
                message
            );
        }
    }

    proptest! {
        #[test]
        fn proof_strings_roundtrip(proofs in proptest::collection::vec(proptest::collection::vec(any::<u8>(), 0..512), 0..16)) {
            let proofs = DependencyProofs(proofs.into_iter().map(Bytes::from).collect());
            let encoded = alloy_rlp::encode(&proofs);
            prop_assert_eq!(proofs.length(), encoded.len());
            prop_assert_eq!(alloy_rlp::decode_exact::<DependencyProofs>(&encoded).unwrap(), proofs);
        }
    }
}
