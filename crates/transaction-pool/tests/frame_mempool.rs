//! Public-pool regressions using supplied validation metadata to isolate pool admission.

use alloy_consensus::{transaction::Recovered, TxEip1559, TxEip8141};
use alloy_eips::eip8141::{Frame, FrameMode, TransactionFees};
use alloy_primitives::{Address, Sealable, Signature, TxKind, B256, U256};
use reth_ethereum_primitives::{Block, TransactionSigned};
use reth_primitives_traits::InMemorySize;
use reth_transaction_pool::{
    blobstore::InMemoryBlobStore,
    validate::{
        FrameDependencies, FrameValidation, TransactionValidationOutcome, ValidTransaction,
    },
    BestTransactionsAttributes, BlockInfo, CoinbaseTipOrdering, EthPooledTransaction, Pool,
    PoolConfig, PoolTransaction, TransactionOrigin, TransactionPool, TransactionPoolExt,
    TransactionValidator,
};
use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
};

#[derive(Clone, Debug, Default)]
struct Validator(Arc<Mutex<B256>>);

impl TransactionValidator for Validator {
    type Transaction = EthPooledTransaction;
    type Block = Block;
    async fn validate_transaction(
        &self,
        _: TransactionOrigin,
        mut tx: EthPooledTransaction,
    ) -> TransactionValidationOutcome<EthPooledTransaction> {
        if let Some(metadata) = tx.frame_validation.as_mut() {
            Arc::make_mut(metadata).head_hash = *self.0.lock().unwrap();
        }
        valid(tx)
    }
}

type AuditPool = Pool<Validator, CoinbaseTipOrdering<EthPooledTransaction>, InMemoryBlobStore>;

fn pool() -> AuditPool {
    pool_with_config(PoolConfig::default())
}

fn pool_with_config(config: PoolConfig) -> AuditPool {
    Pool::new(
        Validator::default(),
        CoinbaseTipOrdering::default(),
        InMemoryBlobStore::default(),
        config,
    )
}

fn frame(sender: Address, payer: Address, key: u64, cost: u64, fee: u64) -> EthPooledTransaction {
    frame_with_blob_fee(sender, payer, key, cost, fee, None)
}

fn frame_with_blob_fee(
    sender: Address,
    payer: Address,
    key: u64,
    cost: u64,
    fee: u64,
    blob_fee: Option<u64>,
) -> EthPooledTransaction {
    let frames = if sender == payer {
        vec![Frame { mode: FrameMode::Verify, flags: 3, ..Default::default() }]
    } else {
        vec![
            Frame { mode: FrameMode::Verify, flags: 2, ..Default::default() },
            Frame { mode: FrameMode::Verify, flags: 1, target: payer.into(), ..Default::default() },
        ]
    };
    let tx = TxEip8141 {
        chain_id: 1,
        sender,
        nonce_keys: Some(vec![U256::from(key)]),
        frames,
        fees: TransactionFees {
            max_fee_per_gas: U256::from(fee),
            max_priority_fee_per_gas: U256::from(fee / 10),
            max_fee_per_blob_gas: U256::from(blob_fee.unwrap_or_default()),
        },
        blob_versioned_hashes: if blob_fee.is_some() { vec![B256::repeat_byte(1)] } else { vec![] },
        ..Default::default()
    };
    let mut pooled = EthPooledTransaction::new(
        Recovered::new_unchecked(TransactionSigned::Eip8141(tx.seal_slow()), sender),
        200,
    );
    pooled
        .set_frame_validation(Arc::new(FrameValidation {
            sender,
            sender_nonce: 0,
            nonce_keys: Some(vec![U256::from(key)]),
            state_nonce: 0,
            sender_balance: U256::from(100_000_000),
            sender_code_hash: None,
            payer,
            max_cost: U256::from(cost),
            payer_balance: U256::from(100_000_000),
            head_hash: B256::ZERO,
            dependencies: FrameDependencies::default(),
            expires_at: None,
            recent_root_dependencies: vec![],
            exclusive_payer: false,
        }))
        .unwrap();
    pooled
}

fn ordinary(sender: Address) -> EthPooledTransaction {
    ordinary_with_fee(sender, 100)
}

fn ordinary_with_fee(sender: Address, fee: u128) -> EthPooledTransaction {
    let tx = TxEip1559 {
        chain_id: 1,
        nonce: 0,
        gas_limit: 21_000,
        max_fee_per_gas: fee,
        max_priority_fee_per_gas: fee / 10,
        to: TxKind::Call(Address::ZERO),
        ..Default::default()
    };
    EthPooledTransaction::new(
        Recovered::new_unchecked(
            TransactionSigned::new_unhashed(tx.into(), Signature::test_signature()),
            sender,
        ),
        100,
    )
}

fn valid(tx: EthPooledTransaction) -> TransactionValidationOutcome<EthPooledTransaction> {
    TransactionValidationOutcome::Valid {
        balance: U256::from(100_000_000),
        state_nonce: 0,
        bytecode_hash: None,
        transaction: ValidTransaction::Valid(tx),
        propagate: true,
        authorities: None,
    }
}

fn admit(pool: &AuditPool, tx: EthPooledTransaction) -> Result<B256, String> {
    pool.inner()
        .add_transactions(TransactionOrigin::External, [valid(tx)])
        .remove(0)
        .map(|outcome| outcome.hash)
        .map_err(|error| format!("{error:?}"))
}

#[test]
fn keyed_frames_appear_in_sender_snapshot() {
    let pool = pool();
    let sender = Address::repeat_byte(1);
    let hash = admit(&pool, frame(sender, sender, 1, 2_000_000, 100)).unwrap();
    assert!(pool.get(&hash).is_some());
    assert_eq!(pool.get_pending_transactions_by_sender(sender).len(), 1);
    let by_sender = pool.all_transactions_by_sender(sender);
    assert_eq!(by_sender.pending.len(), 1);
    assert_eq!(*by_sender.pending[0].hash(), hash);
    assert!(by_sender.queued.is_empty());
    let legacy_hash = admit(&pool, ordinary(sender)).unwrap();
    let by_sender = pool.all_transactions_by_sender(sender);
    let hashes: BTreeSet<_> = by_sender.pending.iter().map(|tx| *tx.hash()).collect();
    assert_eq!(hashes, BTreeSet::from([hash, legacy_hash]));
}

#[test]
fn keyed_lanes_preserve_ordinary_payer_exposure_on_insert_and_removal() {
    let pool = pool();
    let payer = Address::repeat_byte(2);
    let sender = Address::repeat_byte(3);
    let ordinary_hash = admit(&pool, ordinary(payer)).unwrap();
    let keyed_hash = admit(&pool, frame(payer, payer, 1, 2_000_000, 100)).unwrap();
    assert!(admit(&pool, frame(sender, payer, 2, 98_000_000, 100)).is_err());
    assert!(pool.get(&ordinary_hash).is_some());

    pool.remove_transactions(vec![keyed_hash]);
    assert!(admit(&pool, frame(sender, payer, 2, 98_000_000, 100)).is_err());
    pool.remove_transactions(vec![ordinary_hash]);
    assert!(admit(&pool, frame(sender, payer, 2, 98_000_000, 100)).is_ok());
}

#[test]
fn keyed_replacement_during_withdrawal_checks_identity_and_fees() {
    let pool = pool();
    let sender = Address::repeat_byte(4);
    let old_hash = admit(&pool, frame(sender, sender, 1, 2_000_000, 100)).unwrap();
    let head = B256::repeat_byte(1);
    pool.set_block_info(BlockInfo { last_seen_block_hash: head, ..pool.block_info() });
    assert!(pool.get(&old_hash).is_none());
    for (key, fee, reason) in [(1, 109, "ReplacementUnderpriced"), (2, 200, "awaits revalidation")]
    {
        let mut invalid = frame(sender, sender, key, 4_000_000, fee);
        Arc::make_mut(invalid.frame_validation.as_mut().unwrap()).head_hash = head;
        assert!(admit(&pool, invalid).unwrap_err().contains(reason));
    }
    let mut replacement = frame(sender, sender, 1, 4_000_000, 200);
    Arc::make_mut(replacement.frame_validation.as_mut().unwrap()).head_hash = head;
    let replacement_hash = admit(&pool, replacement).unwrap();
    revalidate(&pool);
    assert!(pool.get(&old_hash).is_none());
    assert!(pool.get(&replacement_hash).is_some());
}

#[test]
fn ordinary_replacement_of_withdrawn_key_zero_frame_checks_fees_and_cancels() {
    let pool = pool();
    let sender = Address::repeat_byte(5);
    let old_hash = admit(&pool, frame(sender, sender, 0, 2_000_000, 100)).unwrap();
    let head = B256::repeat_byte(1);
    *pool.inner().validator().0.lock().unwrap() = head;
    pool.set_block_info(BlockInfo { last_seen_block_hash: head, ..pool.block_info() });
    assert!(admit(&pool, ordinary(sender)).unwrap_err().contains("ReplacementUnderpriced"));
    let replacement_hash = admit(&pool, ordinary_with_fee(sender, 200)).unwrap();
    revalidate(&pool);
    assert!(pool.get(&old_hash).is_none());
    assert!(pool.get(&replacement_hash).is_some());
}

#[test]
fn revalidation_preserves_first_seen_timestamp() {
    let pool = pool();
    let sender = Address::repeat_byte(7);
    let hash = admit(&pool, frame(sender, sender, 1, 2_000_000, 100)).unwrap();
    let first_seen = pool.get(&hash).unwrap().timestamp;
    std::thread::sleep(std::time::Duration::from_millis(5));
    let head = B256::repeat_byte(1);
    *pool.inner().validator().0.lock().unwrap() = head;
    pool.set_block_info(BlockInfo { last_seen_block_hash: head, ..pool.block_info() });
    revalidate(&pool);
    let restored = pool.get(&hash).unwrap();
    assert_eq!(restored.timestamp, first_seen);
}

fn revalidate(pool: &AuditPool) {
    let future = pool.inner().revalidate_frame_transactions();
    let mut future = std::pin::pin!(future);
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(std::future::Future::poll(future.as_mut(), &mut context).is_ready());
}

#[test]
fn unpaid_validation_default_evicts_and_releases_payer_exposure() {
    let pool = pool();
    let sender = Address::repeat_byte(30);
    let payer = Address::repeat_byte(31);
    let hash = admit(&pool, frame(sender, payer, 1, 100_000_000, 100)).unwrap();
    let tx = pool.get(&hash).unwrap();
    assert!(admit(&pool, frame(Address::repeat_byte(32), payer, 1, 1, 100)).is_err());

    pool.on_frame_validation_result(&tx, false);

    assert!(!pool.contains(&hash));
    assert!(admit(&pool, frame(Address::repeat_byte(32), payer, 1, 100_000_000, 100)).is_ok());
}

#[test]
fn unpaid_validation_limit_counts_consecutive_failures() {
    let pool = pool_with_config(PoolConfig {
        max_frame_unpaid_validation_attempts: 2,
        ..Default::default()
    });
    let sender = Address::repeat_byte(33);
    let hash = admit(&pool, frame(sender, sender, 1, 2_000_000, 100)).unwrap();
    let tx = pool.best_transactions().next().expect("frame is pending");
    let other_builder = pool.clone();

    pool.on_frame_validation_result(&tx, false);
    assert!(pool.contains(&hash));
    other_builder.on_frame_validation_result(&tx, true);
    pool.on_frame_validation_result(&tx, false);
    assert!(pool.contains(&hash));
    other_builder.on_frame_validation_result(&tx, false);
    assert!(!pool.contains(&hash));
}

#[test]
fn unpaid_validation_count_survives_revalidation_and_ignores_old_builds() {
    let pool = pool_with_config(PoolConfig {
        max_frame_unpaid_validation_attempts: 2,
        ..Default::default()
    });
    let sender = Address::repeat_byte(34);
    let hash = admit(&pool, frame(sender, sender, 1, 2_000_000, 100)).unwrap();
    let old = pool.get(&hash).unwrap();
    pool.on_frame_validation_result(&old, false);

    let head = B256::repeat_byte(1);
    *pool.inner().validator().0.lock().unwrap() = head;
    pool.set_block_info(BlockInfo { last_seen_block_hash: head, ..pool.block_info() });
    pool.on_frame_validation_result(&old, false);
    revalidate(&pool);
    let current = pool.get(&hash).unwrap();
    assert!(!Arc::ptr_eq(&old, &current));
    pool.on_frame_validation_result(&old, true);
    pool.on_frame_validation_result(&current, false);
    assert!(!pool.contains(&hash));
}

#[test]
fn unpaid_validation_replacement_and_readmission_start_fresh() {
    let pool = pool_with_config(PoolConfig {
        max_frame_unpaid_validation_attempts: 2,
        ..Default::default()
    });
    let sender = Address::repeat_byte(35);
    let hash = admit(&pool, frame(sender, sender, 1, 2_000_000, 100)).unwrap();
    let old = pool.get(&hash).unwrap();
    pool.on_frame_validation_result(&old, false);
    let replacement = frame(sender, sender, 1, 2_000_000, 200);
    let replaced_hash = admit(&pool, replacement.clone()).unwrap();
    let replaced = pool.get(&replaced_hash).unwrap();
    pool.on_frame_validation_result(&old, false);
    pool.on_frame_validation_result(&replaced, false);
    assert!(pool.contains(&replaced_hash));
    pool.remove_transaction(replaced_hash).unwrap();
    admit(&pool, replacement).unwrap();
    let readmitted = pool.get(&replaced_hash).unwrap();
    pool.on_frame_validation_result(&replaced, false);
    pool.on_frame_validation_result(&readmitted, false);
    assert!(pool.contains(&replaced_hash));
    pool.on_frame_validation_result(&readmitted, false);
    assert!(!pool.contains(&replaced_hash));
}

#[test]
fn unpaid_validation_does_not_evict_ordinary_transactions() {
    let pool = pool();
    let hash = admit(&pool, ordinary(Address::repeat_byte(36))).unwrap();
    let tx = pool.get(&hash).unwrap();
    pool.on_frame_validation_result(&tx, false);
    assert!(pool.contains(&hash));
}

#[test]
fn failed_keyed_replacement_preserves_withdrawn_transaction() {
    let pool = pool();
    let sender = Address::repeat_byte(8);
    let old_hash = admit(&pool, frame(sender, sender, 1, 2_000_000, 100)).unwrap();
    let head = B256::repeat_byte(1);
    *pool.inner().validator().0.lock().unwrap() = head;
    pool.set_block_info(BlockInfo { last_seen_block_hash: head, ..pool.block_info() });
    // This candidate clears the fee check but still carries metadata for the old head.
    assert!(admit(&pool, frame(sender, sender, 1, 4_000_000, 200)).is_err());
    revalidate(&pool);
    assert!(pool.get(&old_hash).is_some());
}

#[test]
fn memory_accounting_includes_recent_root_dependencies() {
    let sender = Address::repeat_byte(9);
    let mut tx = frame(sender, sender, 1, 2_000_000, 100);
    let before = tx.size();
    let metadata = Arc::make_mut(tx.frame_validation.as_mut().unwrap());
    metadata.recent_root_dependencies = Vec::with_capacity(4);
    metadata.recent_root_dependencies.push(reth_transaction_pool::validate::RecentRootDependency {
        storage_key: U256::from(1),
        expires_at_slot: 10,
    });
    let expected = metadata.recent_root_dependencies.capacity() *
        std::mem::size_of::<reth_transaction_pool::validate::RecentRootDependency>();
    assert_eq!(tx.size() - before, expected);
}

#[test]
fn selecting_transactions_checks_both_fee_dimensions() {
    let pool = pool();
    pool.set_block_info(BlockInfo {
        pending_basefee: 100,
        pending_blob_fee: Some(100),
        ..pool.block_info()
    });
    let mut candidates = Vec::new();
    for (index, (fee, blob_fee)) in
        [(100, None), (200, None), (100, Some(100)), (200, Some(200))].into_iter().enumerate()
    {
        let sender = Address::with_last_byte(20 + index as u8);
        let hash = admit(&pool, frame_with_blob_fee(sender, sender, 1, 50_000_000, fee, blob_fee))
            .unwrap();
        candidates.push((hash, fee, blob_fee));
    }
    for base_fee in [50, 100, 150] {
        for blob_fee in [50, 100, 150] {
            let expected: BTreeSet<_> = candidates
                .iter()
                .filter(|(_, fee, blob)| *fee >= base_fee && blob.is_none_or(|fee| fee >= blob_fee))
                .map(|(hash, _, _)| *hash)
                .collect();
            let actual: BTreeSet<_> = pool
                .best_transactions_with_attributes(BestTransactionsAttributes::new(
                    base_fee,
                    Some(blob_fee),
                ))
                .map(|tx| *tx.hash())
                .collect();
            assert_eq!(actual, expected, "base fee {base_fee}, blob fee {blob_fee}");
        }
    }
}
