//! Ethereum pooled transaction representation used by Reth across supported forks.

use alloy_consensus::{error::ValueError, transaction::TxEip8141Variant, Signed};
use alloy_primitives::Sealed;

use crate::TransactionSigned;

/// Reth's fork-aware pooled transaction representation.
///
/// The protocol representation is defined in Alloy; this alias keeps the Reth API stable.
pub type PooledTransactionVariant = alloy_consensus::PooledTransactionWithSidecarVariant;

impl TryFrom<TransactionSigned> for PooledTransactionVariant {
    type Error = ValueError<TransactionSigned>;

    fn try_from(value: TransactionSigned) -> Result<Self, Self::Error> {
        match value {
            TransactionSigned::Legacy(tx) => Ok(Self::Legacy(tx)),
            TransactionSigned::Eip2930(tx) => Ok(Self::Eip2930(tx)),
            TransactionSigned::Eip1559(tx) => Ok(Self::Eip1559(tx)),
            TransactionSigned::Eip4844(tx) => Err(ValueError::new_static(
                TransactionSigned::Eip4844(tx),
                "pooled transaction requires a blob sidecar",
            )),
            TransactionSigned::Eip7702(tx) => Ok(Self::Eip7702(tx)),
            TransactionSigned::Eip8141(tx) => {
                let (tx, hash) = tx.into_parts();
                Ok(Self::Eip8141(Sealed::new_unchecked(tx.into(), hash)))
            }
        }
    }
}

impl From<PooledTransactionVariant> for TransactionSigned {
    fn from(value: PooledTransactionVariant) -> Self {
        match value {
            PooledTransactionVariant::Legacy(tx) => Self::Legacy(tx),
            PooledTransactionVariant::Eip2930(tx) => Self::Eip2930(tx),
            PooledTransactionVariant::Eip1559(tx) => Self::Eip1559(tx),
            PooledTransactionVariant::Eip4844(tx) => {
                let (tx, signature, hash) = tx.into_parts();
                let (tx, _) = tx.into_parts();
                Self::Eip4844(Signed::new_unchecked(tx, signature, hash))
            }
            PooledTransactionVariant::Eip7702(tx) => Self::Eip7702(tx),
            PooledTransactionVariant::Eip8141(tx) => {
                let (tx, hash) = tx.into_parts();
                let tx = match tx {
                    TxEip8141Variant::TxEip8141(tx) => tx,
                    TxEip8141Variant::TxEip8141WithSidecar(tx) => tx.into_parts().0,
                };
                Self::Eip8141(Sealed::new_unchecked(tx, hash))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_consensus::{TxEip8141, TxEip8141WithSidecar};
    use alloy_eips::{
        eip2718::{Decodable2718, Encodable2718},
        eip4844::{Blob, Bytes48},
        eip7594::{BlobTransactionSidecarEip7594, CELLS_PER_EXT_BLOB},
    };
    use alloy_primitives::{Address, Sealable, B256};

    #[test]
    fn eip8141_consensus_pooled_roundtrip() {
        let tx = TxEip8141 { sender: Address::repeat_byte(0x41), ..Default::default() };
        let consensus = TransactionSigned::Eip8141(tx.seal_slow());
        let pooled = PooledTransactionVariant::try_from(consensus.clone()).unwrap();

        assert!(pooled.as_eip8141().unwrap().sidecar().is_none());
        assert_eq!(TransactionSigned::from(pooled), consensus);
    }

    #[test]
    fn eip8141_sidecar_network_roundtrip() {
        let mut versioned_hash = B256::ZERO;
        versioned_hash[0] = 0x01;
        let tx = TxEip8141 {
            sender: Address::repeat_byte(0x41),
            blob_versioned_hashes: vec![versioned_hash],
            ..Default::default()
        };
        let sidecar = BlobTransactionSidecarEip7594::new(
            vec![Blob::default()],
            vec![Bytes48::default()],
            vec![Bytes48::default(); CELLS_PER_EXT_BLOB],
        );
        let pooled: PooledTransactionVariant = TxEip8141WithSidecar::new(tx, sidecar).into();

        let encoded = pooled.encoded_2718();
        let decoded = PooledTransactionVariant::decode_2718(&mut encoded.as_ref()).unwrap();

        assert_eq!(decoded, pooled);
        assert!(decoded.as_eip8141().unwrap().sidecar().is_some());
    }
}
