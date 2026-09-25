//! Frame adapter regressions: gas schedules, owned buffers and fallible receipts.

use alloy_consensus::{transaction::Recovered, TxEip4844, TxEip8141, TxEnvelope, TxType};
use alloy_eips::{
    eip2718::WithEncoded,
    eip8141::{
        Frame, FrameLimits, FrameSignature, SignatureScheme, NONCE_MANAGER, NONCE_MANAGER_CODE,
    },
};
use alloy_evm::{
    block::{
        BlockExecutionError, BlockExecutor, ExecutableTxParts, InternalBlockExecutionError,
        SystemCaller,
    },
    eth::{
        receipt_builder::{AlloyReceiptBuilder, ReceiptBuilder, ReceiptBuilderCtx},
        spec::{EthExecutorSpec, EthSpec},
        EthBlockExecutionCtx, EthBlockExecutor, EthEvmFactory, EthTxResult,
    },
    tx_env_from_eip8141, Evm, EvmEnv, EvmFactory, FromRecoveredTx, IntoTxEnv, ToTxEnv,
};
use alloy_hardforks::{EthereumHardfork, EthereumHardforks, ForkCondition};
use alloy_primitives::{address, Address, Bytes, Sealable, B256, U256};
use revm::{
    context::{
        result::{ExecutionResult, HaltReason, ResultAndState, ResultGas},
        CfgEnv, TxEnv,
    },
    context_interface::cfg::gas_params::GasId,
    database::{CacheDB, State},
    database_interface::EmptyDB,
    primitives::hardfork::SpecId,
    state::{AccountInfo, Bytecode},
};

const SENDER: Address = address!("1000000000000000000000000000000000000001");

fn frame_tx() -> TxEip8141 {
    TxEip8141 {
        chain_id: 1,
        sender: SENDER,
        frames: vec![Frame {
            flags: 3,
            data: vec![1].into(),
            limits: FrameLimits { execution: 50_000, state: 0 },
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn custom_env() -> EvmEnv {
    let mut cfg = CfgEnv::new_with_spec(SpecId::BOGOTA);
    cfg.gas_params.override_gas([(GasId::tx_token_cost(), 20)]);
    EvmEnv::new(cfg, Default::default())
}

fn db() -> CacheDB<EmptyDB> {
    let mut db: CacheDB<EmptyDB> = CacheDB::default();
    // PUSH1 3, PUSH0, PUSH0, APPROVE: approve execution and payment.
    db.insert_account_info(
        SENDER,
        AccountInfo::default()
            .with_code(Bytecode::new_legacy(Bytes::from_static(&[0x60, 3, 0x5f, 0x5f, 0xaa]))),
    );
    db
}

fn recovered() -> Recovered<TxEnvelope> {
    Recovered::new_unchecked(TxEnvelope::Eip8141(frame_tx().seal_slow()), SENDER)
}

const fn context() -> EthBlockExecutionCtx<'static> {
    EthBlockExecutionCtx {
        parent_hash: B256::ZERO,
        parent_beacon_block_root: None,
        ommers: &[],
        withdrawals: None,
        extra_data: Bytes::new(),
        tx_count_hint: None,
        slot_number: None,
    }
}

#[derive(Clone)]
struct BogotaSpec;

impl EthereumHardforks for BogotaSpec {
    fn ethereum_fork_activation(&self, fork: EthereumHardfork) -> ForkCondition {
        if fork == EthereumHardfork::Bogota {
            ForkCondition::Timestamp(0)
        } else {
            ForkCondition::Never
        }
    }
}

impl EthExecutorSpec for BogotaSpec {
    fn deposit_contract_address(&self) -> Option<Address> {
        None
    }
}

#[test]
fn nonce_manager_transition_preserves_balance_and_consumed_slots() {
    let mut db: CacheDB<EmptyDB> = CacheDB::default();
    db.insert_account_info(
        NONCE_MANAGER,
        AccountInfo { nonce: 2, balance: U256::from(17), ..Default::default() },
    );
    let mut evm = EthEvmFactory::default().create_evm(db, custom_env());
    let mut caller = SystemCaller::new(BogotaSpec);
    caller.apply_eip8141_fork_transition(&mut evm).unwrap();

    let installed = revm::Database::basic(evm.db_mut(), NONCE_MANAGER).unwrap().unwrap();
    assert_eq!(installed.nonce, 2);
    assert_eq!(installed.balance, U256::from(17));
    assert_eq!(installed.code_hash, alloy_primitives::keccak256(NONCE_MANAGER_CODE));

    let slot = U256::from(123);
    evm.db_mut().insert_account_storage(NONCE_MANAGER, slot, U256::from(7)).unwrap();
    caller.apply_eip8141_fork_transition(&mut evm).unwrap();
    assert_eq!(revm::Database::storage(evm.db_mut(), NONCE_MANAGER, slot).unwrap(), U256::from(7));
}

#[test]
fn recovered_frame_uses_custom_schedule_during_execution() {
    let mut evm = EthEvmFactory::default().create_evm(db(), custom_env());
    let result = evm.transact(recovered()).unwrap();
    assert!(matches!(result.result, ExecutionResult::FrameTransaction { .. }));
    assert_eq!(result.state[&SENDER].info.nonce, 1);
}

#[test]
fn wrappers_forward_the_custom_schedule() {
    let env = custom_env();
    let gas = &env.cfg_env.gas_params;
    let tx = recovered();
    let expected = 62_555;
    let check = |env: TxEnv| assert_eq!(env.gas_limit, expected);
    check(tx.to_tx_env_with_gas_params(gas));
    check((&tx).into_tx_env_with_gas_params(gas));
    let borrowed = Recovered::new_unchecked(tx.inner(), SENDER);
    check(borrowed.to_tx_env_with_gas_params(gas));
    let encoded = WithEncoded::new(Bytes::new(), tx.clone());
    check(encoded.to_tx_env_with_gas_params(gas));
    let encoded_borrowed = WithEncoded::new(Bytes::new(), &tx);
    check(encoded_borrowed.to_tx_env_with_gas_params(gas));
    check(
        <_ as ExecutableTxParts<TxEnv, TxEnvelope>>::into_parts_with_gas_params(tx.clone(), gas).0,
    );
    check(<_ as ExecutableTxParts<TxEnv, TxEnvelope>>::into_parts_with_gas_params(&tx, gas).0);
    check(<_ as ExecutableTxParts<TxEnv, TxEnvelope>>::into_parts_with_gas_params(borrowed, gas).0);
    check(<_ as ExecutableTxParts<TxEnv, TxEnvelope>>::into_parts_with_gas_params(encoded, gas).0);
    check(
        <_ as ExecutableTxParts<TxEnv, TxEnvelope>>::into_parts_with_gas_params(
            encoded_borrowed,
            gas,
        )
        .0,
    );
    let either: revm::context_interface::either::Either<_, Recovered<TxEnvelope>> =
        revm::context_interface::either::Either::Left(tx.clone());
    check(either.to_tx_env_with_gas_params(gas));
    check(<_ as ExecutableTxParts<TxEnv, TxEnvelope>>::into_parts_with_gas_params(either, gas).0);
}

#[test]
fn explicit_environment_is_not_repaired() {
    let env = custom_env();
    let mut tx = tx_env_from_eip8141(frame_tx(), &env.cfg_env.gas_params);
    tx.gas_limit = 1;
    let explicit = tx.clone().into_tx_env_with_gas_params(&env.cfg_env.gas_params);
    assert_eq!(explicit.gas_limit, 1);
    let tuple = <_ as ExecutableTxParts<TxEnv, TxEnvelope>>::into_parts_with_gas_params(
        (tx.clone(), recovered()),
        &env.cfg_env.gas_params,
    );
    assert_eq!(tuple.0.gas_limit, 1);
    let mut evm = EthEvmFactory::default().create_evm(db(), env);
    assert!(evm.transact(tx).is_err());
}

#[test]
fn standard_transaction_conversion_does_not_change() {
    let tx = TxEip4844 { gas_limit: 100_000, ..Default::default() };
    assert_eq!(
        TxEnv::from_recovered_tx(&tx, SENDER),
        TxEnv::from_recovered_tx_with_gas_params(&tx, SENDER, &custom_env().cfg_env.gas_params)
    );
}

#[test]
fn owned_frame_conversion_moves_buffers() {
    let mut tx = frame_tx();
    tx.signatures =
        vec![FrameSignature { scheme: SignatureScheme::Arbitrary, ..Default::default() }];
    tx.blob_versioned_hashes = vec![B256::with_last_byte(1)];
    let frames = tx.frames.as_ptr();
    let signatures = tx.signatures.as_ptr();
    let blobs = tx.blob_versioned_hashes.as_ptr();
    let signature_hash = tx.signature_hash();
    let expected =
        TxEnv::from_recovered_tx_with_gas_params(&tx, SENDER, &custom_env().cfg_env.gas_params);
    let converted = tx_env_from_eip8141(tx, &custom_env().cfg_env.gas_params);
    let payload = converted.frame_transaction.as_ref().unwrap();
    assert_eq!(payload.frames.as_ptr(), frames);
    assert_eq!(payload.signatures.as_ptr(), signatures);
    assert_eq!(converted.blob_hashes.as_ptr(), blobs);
    assert_eq!(payload.signature_hash, signature_hash);
    assert_eq!(converted, expected);
}

#[test]
fn custom_schedule_is_used_before_block_admission() {
    let mut env = custom_env();
    // This transaction fits exactly. The actual consumption is lower.
    env.block_env.gas_limit = 62_555;
    let state = State::builder().with_database(db()).build();
    let evm = EthEvmFactory::default().create_evm(state, env.clone());
    let mut executor =
        EthBlockExecutor::new(evm, context(), EthSpec::mainnet(), AlloyReceiptBuilder::default());
    executor.execute_transaction(recovered()).unwrap();
    assert_eq!(executor.receipts.len(), 1);

    env.block_env.gas_limit -= 1;
    let state = State::builder().with_database(db()).build();
    let evm = EthEvmFactory::default().create_evm(state, env);
    let mut executor =
        EthBlockExecutor::new(evm, context(), EthSpec::mainnet(), AlloyReceiptBuilder::default());
    assert!(matches!(
        executor.execute_transaction(recovered()),
        Err(BlockExecutionError::Validation(
            alloy_evm::block::BlockValidationError::TransactionGasLimitMoreThanAvailableBlockGas {
                transaction_gas_limit: 62_555,
                block_available_gas: 62_554,
            }
        ))
    ));
    assert!(executor.receipts.is_empty());
}

fn revert() -> ExecutionResult<HaltReason> {
    ExecutionResult::Revert {
        gas: ResultGas::default().with_total_gas_spent(42).with_state_gas_spent(4),
        output: Bytes::new(),
        logs: vec![],
    }
}

#[test]
fn ordinary_receipt_variants_are_unchanged() {
    let evm = EthEvmFactory::default().create_evm(db(), custom_env());
    for tx_type in
        [TxType::Legacy, TxType::Eip2930, TxType::Eip1559, TxType::Eip4844, TxType::Eip7702]
    {
        let receipt = AlloyReceiptBuilder::default()
            .build_receipt(ReceiptBuilderCtx {
                tx_type,
                evm: &evm,
                result: revert(),
                state: &Default::default(),
                cumulative_gas_used: 42,
            })
            .unwrap();
        use alloy_eips::Typed2718;
        assert_eq!(receipt.ty(), tx_type as u8);
        assert_eq!(receipt.cumulative_gas_used(), 42);
        assert!(!receipt.status());
    }
}

#[test]
fn conversion_uses_custom_floor_and_detects_overflow() {
    let mut env = custom_env();
    env.cfg_env.gas_params.override_gas([(GasId::tx_floor_cost_per_token(), 32)]);
    let mut tx = frame_tx();
    tx.frames[0].data = vec![0; 1_000].into();
    let converted = tx_env_from_eip8141(tx.clone(), &env.cfg_env.gas_params);
    let frame = converted.frame_transaction.as_ref().unwrap();
    assert_eq!(
        converted.gas_limit,
        frame.calldata_floor_gas_with_params(SENDER, &env.cfg_env.gas_params).unwrap()
    );
    assert_ne!(converted.gas_limit, frame.gas_limit(SENDER).unwrap());
    tx.frames[0].limits.execution = u64::MAX;
    let converted = tx_env_from_eip8141(tx, &env.cfg_env.gas_params);
    assert_eq!(converted.gas_limit, u64::MAX);
    let mut evm = EthEvmFactory::default().create_evm(db(), env);
    assert!(evm.transact(converted).is_err());
}

#[test]
fn receipt_mismatches_return_errors() {
    let evm = EthEvmFactory::default().create_evm(db(), custom_env());
    for tx_type in [
        TxType::Legacy,
        TxType::Eip2930,
        TxType::Eip1559,
        TxType::Eip4844,
        TxType::Eip7702,
        TxType::Eip8141,
    ] {
        let result = if tx_type == TxType::Eip8141 {
            revert()
        } else {
            ExecutionResult::FrameTransaction {
                gas: ResultGas::default(),
                payer: SENDER,
                logs: vec![],
                frame_receipts: vec![],
            }
        };
        let result = AlloyReceiptBuilder::default().build_receipt(ReceiptBuilderCtx {
            tx_type,
            evm: &evm,
            result,
            state: &Default::default(),
            cumulative_gas_used: 42,
        });
        assert!(matches!(
            result,
            Err(BlockExecutionError::Internal(
                InternalBlockExecutionError::ReceiptTypeMismatch { .. }
            ))
        ));
    }
}

#[test]
fn failed_receipt_does_not_commit_state_or_counters() {
    let state = State::builder().with_database(db()).build();
    let evm = EthEvmFactory::default().create_evm(state, custom_env());
    let mut executor =
        EthBlockExecutor::new(evm, context(), EthSpec::mainnet(), AlloyReceiptBuilder::default());
    let mut account = revm::state::Account::from(AccountInfo {
        nonce: 99,
        balance: U256::from(123),
        ..Default::default()
    });
    account.mark_touch();
    let state = [(SENDER, account)].into_iter().collect();
    let result = executor.commit_transaction(EthTxResult {
        result: ResultAndState { result: revert(), state },
        blob_gas_used: 131_072,
        tx_type: TxType::Eip8141,
    });
    assert!(result.is_err());
    assert_eq!(executor.cumulative_tx_gas_used, 0);
    assert_eq!(executor.block_regular_gas_used, 0);
    assert_eq!(executor.block_state_gas_used, 0);
    assert_eq!(executor.blob_gas_used, 0);
    assert!(executor.receipts.is_empty());
    let account = revm::Database::basic(executor.evm.db_mut(), SENDER).unwrap().unwrap();
    assert_eq!(account.nonce, 0);
    assert_eq!(account.balance, U256::ZERO);
}

#[cfg(feature = "rpc")]
#[test]
fn rpc_frame_uses_custom_schedule_and_moves_request_buffers() {
    use alloy_evm::rpc::TryIntoTxEnv;
    let tx = frame_tx();
    let request: alloy_rpc_types_eth::TransactionRequest = tx.into();
    let frames = request.frames.as_ref().unwrap().as_ptr();
    let env = request.try_into_tx_env(&custom_env()).unwrap();
    assert_eq!(env.gas_limit, 62_555);
    assert_eq!(env.frame_transaction.as_ref().unwrap().frames.as_ptr(), frames);
    let mut evm = EthEvmFactory::default().create_evm(db(), custom_env());
    assert!(evm.transact(env).is_ok());
}
