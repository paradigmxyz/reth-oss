//! Cancellation-aware ownership of temporarily withdrawn frame transactions.

use crate::{identifier::TransactionId, PoolTransaction, ValidPoolTransaction};
use alloy_primitives::{Address, B256, U256};
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

pub(super) struct FrameRevalidationQueue<T: PoolTransaction> {
    transactions: HashMap<B256, Arc<ValidPoolTransaction<T>>>,
    senders: HashMap<Address, BTreeMap<TransactionId, B256>>,
}

impl<T: PoolTransaction> Default for FrameRevalidationQueue<T> {
    fn default() -> Self {
        Self { transactions: HashMap::new(), senders: HashMap::new() }
    }
}

impl<T: PoolTransaction> FrameRevalidationQueue<T> {
    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.transactions.is_empty()
    }

    pub(super) fn insert(&mut self, tx: Arc<ValidPoolTransaction<T>>) {
        if let Some(existing) = self.get(tx.sender(), tx.id()) {
            let hash = *existing.hash();
            self.cancel(&hash);
        }
        self.senders.entry(tx.sender()).or_default().insert(*tx.id(), *tx.hash());
        self.transactions.insert(*tx.hash(), tx);
    }

    pub(super) fn cancel(&mut self, hash: &B256) {
        let _ = self.remove(hash);
    }

    pub(super) fn remove(&mut self, hash: &B256) -> Option<Arc<ValidPoolTransaction<T>>> {
        let tx = self.transactions.remove(hash)?;
        let nonces = self.senders.get_mut(&tx.sender())?;
        nonces.remove(tx.id());
        if nonces.is_empty() {
            self.senders.remove(&tx.sender());
        }
        Some(tx)
    }

    pub(super) fn get(
        &self,
        sender: Address,
        id: &TransactionId,
    ) -> Option<&Arc<ValidPoolTransaction<T>>> {
        let hash = self.senders.get(&sender)?.get(id)?;
        self.transactions.get(hash)
    }

    pub(super) fn cancel_sender(&mut self, sender: Address) -> Vec<Arc<ValidPoolTransaction<T>>> {
        self.senders
            .remove(&sender)
            .into_iter()
            .flat_map(|nonces| nonces.into_values())
            .filter_map(|hash| self.transactions.remove(&hash))
            .collect()
    }

    pub(super) fn cancel_descendants(
        &mut self,
        sender: Address,
        id: &TransactionId,
    ) -> Vec<Arc<ValidPoolTransaction<T>>> {
        // Nonces in other keyed domains are independent of this transaction.
        let hashes: Vec<_> = self
            .senders
            .get(&sender)
            .into_iter()
            .flat_map(|nonces| nonces.range(*id..).take_while(|(next, _)| next.sender == id.sender))
            .map(|(_, hash)| *hash)
            .collect();
        hashes.into_iter().filter_map(|hash| self.remove(&hash)).collect()
    }

    pub(super) fn contains_sender(&self, sender: Address) -> bool {
        self.senders.contains_key(&sender)
    }

    pub(super) fn replacement(&self, tx: &T) -> Option<&Arc<ValidPoolTransaction<T>>> {
        self.senders.get(&tx.sender())?.iter().find_map(|(id, hash)| {
            let existing = self.transactions.get(hash)?;
            let keys = existing.transaction.eip8250_nonce_keys();
            // Ordinary transactions share the legacy lane with key-zero frames, including
            // replacement fees and cancellation of an in-flight revalidation.
            let same_domain = keys == tx.eip8250_nonce_keys() ||
                (!tx.is_eip8141() && keys == Some(&[U256::ZERO][..]));
            (id.nonce == tx.nonce() && same_domain).then_some(existing)
        })
    }

    pub(super) fn snapshot(&self) -> Vec<Arc<ValidPoolTransaction<T>>> {
        let mut transactions: Vec<_> = self.transactions.values().cloned().collect();
        // Blob admission requires ancestors to be restored before their descendants.
        transactions.sort_unstable_by_key(|tx| *tx.id());
        transactions
    }

    pub(super) fn take_current(&mut self, tx: &Arc<ValidPoolTransaction<T>>) -> bool {
        if self.transactions.get(tx.hash()).is_some_and(|current| Arc::ptr_eq(current, tx)) {
            self.cancel(tx.hash());
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{MockTransaction, MockTransactionFactory};

    fn transaction(
        factory: &mut MockTransactionFactory,
        sender: Address,
        hash: B256,
        nonce: u64,
    ) -> Arc<ValidPoolTransaction<MockTransaction>> {
        factory.validated_arc(
            MockTransaction::legacy().with_sender(sender).with_hash(hash).with_nonce(nonce),
        )
    }

    #[test]
    fn cancelled_snapshot_cannot_be_restored() {
        let mut factory = MockTransactionFactory::default();
        let tx = transaction(&mut factory, Address::repeat_byte(1), B256::repeat_byte(1), 0);
        let mut queue = FrameRevalidationQueue::default();
        queue.insert(Arc::clone(&tx));
        let snapshot = queue.snapshot();

        queue.cancel(tx.hash());

        assert!(!queue.take_current(&snapshot[0]));
        assert!(queue.is_empty());
    }

    #[test]
    fn replacing_nonce_cancels_stale_snapshot() {
        let mut factory = MockTransactionFactory::default();
        let sender = Address::repeat_byte(2);
        let old = transaction(&mut factory, sender, B256::repeat_byte(1), 0);
        let new = transaction(&mut factory, sender, B256::repeat_byte(2), 0);
        let mut queue = FrameRevalidationQueue::default();
        queue.insert(Arc::clone(&old));
        let snapshot = queue.snapshot();

        queue.insert(Arc::clone(&new));

        assert!(!queue.take_current(&snapshot[0]));
        assert!(queue.take_current(&new));
        assert!(queue.is_empty());
    }

    #[test]
    fn identical_hash_with_new_arc_cannot_be_taken_via_stale_arc() {
        let mut factory = MockTransactionFactory::default();
        let sender = Address::repeat_byte(3);
        let hash = B256::repeat_byte(3);
        let old = transaction(&mut factory, sender, hash, 0);
        let new = transaction(&mut factory, sender, hash, 0);
        let mut queue = FrameRevalidationQueue::default();
        queue.insert(Arc::clone(&old));
        queue.insert(Arc::clone(&new));

        assert!(!queue.take_current(&old));
        assert!(queue.take_current(&new));
        assert!(queue.is_empty());
    }

    #[test]
    fn successful_take_removes_transaction_once() {
        let mut factory = MockTransactionFactory::default();
        let tx = transaction(&mut factory, Address::repeat_byte(4), B256::repeat_byte(4), 0);
        let mut queue = FrameRevalidationQueue::default();
        queue.insert(Arc::clone(&tx));

        assert!(queue.take_current(&tx));
        assert!(!queue.take_current(&tx));
        assert!(queue.is_empty());
    }

    #[test]
    fn different_nonces_survive_withdrawal_and_restore_in_order() {
        let mut factory = MockTransactionFactory::default();
        let sender = Address::repeat_byte(5);
        let first = transaction(&mut factory, sender, B256::repeat_byte(1), 0);
        let second = transaction(&mut factory, sender, B256::repeat_byte(2), 1);
        let mut queue = FrameRevalidationQueue::default();
        queue.insert(Arc::clone(&second));
        queue.insert(Arc::clone(&first));

        let snapshot = queue.snapshot();
        assert_eq!(snapshot.len(), 2);
        assert_eq!(snapshot[0].nonce(), 0);
        assert_eq!(snapshot[1].nonce(), 1);
        assert!(queue.take_current(&first));
        assert!(queue.take_current(&second));
        assert!(queue.is_empty());
    }

    #[test]
    fn cancellation_preserves_lower_nonces_and_other_senders() {
        let mut factory = MockTransactionFactory::default();
        let sender = Address::repeat_byte(6);
        let first = transaction(&mut factory, sender, B256::repeat_byte(1), 0);
        let second = transaction(&mut factory, sender, B256::repeat_byte(2), 1);
        let other = transaction(&mut factory, Address::repeat_byte(7), B256::repeat_byte(3), 0);
        let mut queue = FrameRevalidationQueue::default();
        for tx in [&first, &second, &other] {
            queue.insert(Arc::clone(tx));
        }

        assert_eq!(queue.cancel_descendants(sender, second.id()).len(), 1);
        assert!(!queue.take_current(&second));
        assert!(queue.get(sender, first.id()).is_some());
        assert_eq!(queue.cancel_sender(sender).len(), 1);
        assert!(!queue.take_current(&first));
        assert!(queue.take_current(&other));
        assert!(queue.is_empty());
    }

    #[test]
    fn cancellation_preserves_other_nonce_domains() {
        let mut factory = MockTransactionFactory::default();
        let sender = Address::repeat_byte(8);
        let first = transaction(&mut factory, sender, B256::repeat_byte(1), 0);
        let second = transaction(&mut factory, sender, B256::repeat_byte(2), 1);
        let mut other_domain = transaction(&mut factory, sender, B256::repeat_byte(3), 1);
        Arc::make_mut(&mut other_domain).transaction_id.sender =
            crate::identifier::SenderId::from(100);
        let mut queue = FrameRevalidationQueue::default();
        for tx in [&first, &second, &other_domain] {
            queue.insert(Arc::clone(tx));
        }

        assert_eq!(queue.snapshot().len(), 3);
        assert_eq!(queue.cancel_descendants(sender, second.id()).len(), 1);
        assert!(!queue.take_current(&second));
        assert!(queue.get(sender, first.id()).is_some());
        assert!(queue.get(sender, other_domain.id()).is_some());
        assert_eq!(queue.cancel_sender(sender).len(), 2);
        assert!(queue.is_empty());
    }
}
