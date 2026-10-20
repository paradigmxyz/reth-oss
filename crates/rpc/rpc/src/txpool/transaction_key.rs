use alloy_eips::eip8141::nonce_keys_hash;
use alloy_primitives::U256;
use reth_transaction_pool::PoolTransaction;

/// Nonzero keyed domains can share a sequence with each other and with the legacy lane.
pub(super) fn transaction_key<T: PoolTransaction>(tx: &T) -> String {
    match tx.eip8250_nonce_keys() {
        Some(keys) if keys != [U256::ZERO] => format!("{}:{}", nonce_keys_hash(keys), tx.nonce()),
        _ => tx.nonce().to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_consensus::{transaction::Recovered, TxEip8141};
    use alloy_primitives::{Address, Sealable};
    use reth_ethereum_primitives::TransactionSigned;
    use reth_transaction_pool::EthPooledTransaction;

    fn frame(keys: Option<Vec<U256>>) -> EthPooledTransaction {
        let sender = Address::repeat_byte(1);
        EthPooledTransaction::new(
            Recovered::new_unchecked(
                TransactionSigned::Eip8141(
                    TxEip8141 { sender, nonce: 7, nonce_keys: keys, ..Default::default() }
                        .seal_slow(),
                ),
                sender,
            ),
            0,
        )
    }

    #[test]
    fn distinguishes_nonce_domains_without_changing_legacy_keys() {
        assert_eq!(transaction_key(&frame(None)), "7");
        assert_eq!(transaction_key(&frame(Some(vec![U256::ZERO]))), "7");
        let first = transaction_key(&frame(Some(vec![U256::from(1)])));
        let second = transaction_key(&frame(Some(vec![U256::from(2)])));
        assert_eq!(first, format!("{}:7", nonce_keys_hash(&[U256::from(1)])));
        assert_ne!(first, second);
        assert_ne!(first, "7");
    }
}
