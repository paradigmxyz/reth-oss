//! Utilities for serving `eth_simulateV1`

use crate::{
    error::{api::FromEthApiError, FromEvmError, ToRpcError},
    EthApiError,
};
use alloy_consensus::{transaction::TxHashRef, BlockHeader, Transaction};
use alloy_eips::eip2718::WithEncoded;
use alloy_evm::{block::TxResult, precompiles::PrecompilesMap};
use alloy_network::{NetworkTransactionBuilder, TransactionBuilder};
use alloy_primitives::{Log, LogData, B256};
use alloy_rpc_types_eth::{
    simulate::{SimCallResult, SimulateError, SimulatedBlock},
    state::StateOverride,
    BlockTransactionsKind,
};
use alloy_sol_types::SolValue;
use jsonrpsee_types::ErrorObject;
use reth_evm::{
    execute::{BlockBuilder, BlockBuilderOutcome, BlockExecutor},
    Evm, HaltReasonFor,
};
use reth_primitives_traits::{BlockBody as _, BlockTy, NodePrimitives, Recovered, RecoveredBlock};
use reth_rpc_convert::{RpcBlock, RpcConvert, RpcTxReq};
use reth_rpc_server_types::result::rpc_err;
use reth_storage_api::StateProvider;
use revm::{
    context::{Block, JournalTr},
    context_interface::{result::ExecutionResult, ContextTr, CreateScheme},
    interpreter::{CallInputs, CallOutcome, CreateInputs, CreateOutcome},
    primitives::{Address, Bytes, TxKind, U256},
    Database, Inspector,
};
use revm_inspectors::transfer::{
    TransferKind, TransferOperation, TRANSFER_EVENT_TOPIC, TRANSFER_LOG_EMITTER,
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

/// Error code for execution reverted in `eth_simulateV1`.
///
/// Consistent with `eth_call` revert error code.
///
/// <https://github.com/ethereum/execution-apis/pull/748>
pub const SIMULATE_REVERT_CODE: i32 = 3;

/// Error code for VM execution errors (e.g., out of gas) in `eth_simulateV1`.
///
/// <https://github.com/ethereum/execution-apis>
pub const SIMULATE_VM_ERROR_CODE: i32 = -32015;

/// Errors which may occur during `eth_simulateV1` execution.
#[derive(Debug, thiserror::Error)]
pub enum EthSimulateError {
    /// Total gas limit of transactions for the block exceeds the block gas limit.
    #[error("Block gas limit exceeded by the block's transactions")]
    BlockGasLimitExceeded,
    /// Number of simulated blocks exceeds the configured client limit.
    #[error("too many blocks")]
    TooManyBlocks,
    /// Max gas limit for entire operation exceeded.
    #[error("Client adjustable limit reached")]
    GasLimitReached,
    /// Block number in sequence did not increase.
    #[error("block numbers must be in order: {got} <= {parent}")]
    BlockNumberInvalid {
        /// The block number that was provided.
        got: u64,
        /// The parent block number.
        parent: u64,
    },
    /// Block timestamp in sequence did not increase.
    #[error("block timestamps must be in order: {got} <= {parent}")]
    BlockTimestampInvalid {
        /// The block timestamp that was provided.
        got: u64,
        /// The parent block timestamp.
        parent: u64,
    },
    /// Transaction nonce is too low.
    #[error("nonce too low: next nonce {state}, tx nonce {tx}")]
    NonceTooLow {
        /// Transaction nonce.
        tx: u64,
        /// Current state nonce.
        state: u64,
    },
    /// Transaction nonce is too high.
    #[error("nonce too high")]
    NonceTooHigh,
    /// Transaction's baseFeePerGas is too low.
    #[error("max fee per gas less than block base fee")]
    BaseFeePerGasTooLow,
    /// Not enough gas provided to pay for intrinsic gas.
    #[error("intrinsic gas too low")]
    IntrinsicGasTooLow,
    /// Insufficient funds to pay for gas fees and value.
    #[error("insufficient funds for gas * price + value: have {balance} want {cost}")]
    InsufficientFunds {
        /// Transaction cost.
        cost: U256,
        /// Sender balance.
        balance: U256,
    },
    /// Sender is not an EOA.
    #[error("sender is not an EOA")]
    SenderNotEOA,
    /// Max init code size exceeded.
    #[error("max initcode size exceeded")]
    MaxInitCodeSizeExceeded,
    /// Attempted to move a non-precompile address.
    #[error("account {0} is not a precompile")]
    NotAPrecompile(Address),
    /// Attempted to move a precompile to its own address.
    #[error("cannot move precompile {0} to itself")]
    MovePrecompileToSelf(Address),
}

/// Collects ETH transfers for `eth_simulateV1` without inserting them into the EVM journal.
#[derive(Clone, Debug, Default)]
pub struct SimulateTransferInspector {
    internal_only: bool,
    transfers: Vec<TransferOperation>,
    checkpoints: Vec<usize>,
    collector: TransferLogCollector,
}

impl SimulateTransferInspector {
    /// Creates a new transfer inspector and a collector for synthetic transfer logs.
    pub fn new(internal_only: bool) -> (Self, TransferLogCollector) {
        let collector = TransferLogCollector::default();
        (
            Self {
                internal_only,
                transfers: Vec::new(),
                checkpoints: Vec::new(),
                collector: collector.clone(),
            },
            collector,
        )
    }

    fn sync_collector(&self) {
        *self.collector.transfers.lock().expect("transfer collector lock poisoned") =
            self.transfers.clone();
    }

    fn push_transfer<DB: Database, JOURNAL: JournalTr<Database = DB>>(
        &mut self,
        from: Address,
        to: Address,
        value: U256,
        kind: TransferKind,
        journaled_state: &JOURNAL,
    ) {
        if self.internal_only && journaled_state.depth() == 0 {
            return
        }

        if value.is_zero() {
            return
        }

        self.transfers.push(TransferOperation { kind, from, to, value });
        self.sync_collector();
    }
}

impl<CTX> Inspector<CTX> for SimulateTransferInspector
where
    CTX: ContextTr,
{
    fn call(&mut self, context: &mut CTX, inputs: &mut CallInputs) -> Option<CallOutcome> {
        self.checkpoints.push(self.transfers.len());

        if let Some(value) = inputs.transfer_value() {
            self.push_transfer(
                inputs.transfer_from(),
                inputs.transfer_to(),
                value,
                TransferKind::Call,
                context.journal(),
            );
        }

        None
    }

    fn call_end(&mut self, _context: &mut CTX, _inputs: &CallInputs, outcome: &mut CallOutcome) {
        let checkpoint = self.checkpoints.pop().unwrap_or_default();
        if !outcome.instruction_result().is_ok() {
            self.transfers.truncate(checkpoint);
            self.sync_collector();
        }
    }

    fn create(&mut self, context: &mut CTX, inputs: &mut CreateInputs) -> Option<CreateOutcome> {
        self.checkpoints.push(self.transfers.len());

        let kind = match inputs.scheme() {
            CreateScheme::Create => TransferKind::Create,
            CreateScheme::Create2 { .. } => TransferKind::Create2,
            CreateScheme::Custom { .. } => return None,
        };

        let nonce = match context.journal_mut().load_account(inputs.caller()) {
            Ok(account) => account.data.info.nonce,
            Err(_) => return None,
        };

        self.push_transfer(
            inputs.caller(),
            inputs.created_address(nonce),
            inputs.value(),
            kind,
            context.journal(),
        );

        None
    }

    fn create_end(
        &mut self,
        _context: &mut CTX,
        _inputs: &CreateInputs,
        outcome: &mut CreateOutcome,
    ) {
        let checkpoint = self.checkpoints.pop().unwrap_or_default();
        if !outcome.instruction_result().is_ok() {
            self.transfers.truncate(checkpoint);
            self.sync_collector();
        }
    }

    fn selfdestruct(&mut self, contract: Address, target: Address, value: U256) {
        self.transfers.push(TransferOperation {
            kind: TransferKind::SelfDestruct,
            from: contract,
            to: target,
            value,
        });
        self.sync_collector();
    }
}

/// Shared synthetic transfer log collector.
#[derive(Clone, Debug, Default)]
pub struct TransferLogCollector {
    transfers: Arc<Mutex<Vec<TransferOperation>>>,
}

impl TransferLogCollector {
    fn append_new_logs<HaltReasonTy>(
        &self,
        result: &mut ExecutionResult<HaltReasonTy>,
        next_transfer: &mut usize,
    ) {
        let transfers = self.transfers.lock().expect("transfer collector lock poisoned");
        if *next_transfer >= transfers.len() {
            return
        }

        let logs = match result {
            ExecutionResult::Success { logs, .. } |
            ExecutionResult::Revert { logs, .. } |
            ExecutionResult::Halt { logs, .. } => logs,
        };

        logs.extend(transfers[*next_transfer..].iter().map(transfer_to_log));
        *next_transfer = transfers.len();
    }
}

fn transfer_to_log(transfer: &TransferOperation) -> Log {
    let from = B256::from_slice(&transfer.from.abi_encode());
    let to = B256::from_slice(&transfer.to.abi_encode());
    let data = transfer.value.abi_encode();

    Log {
        address: TRANSFER_LOG_EMITTER,
        data: LogData::new_unchecked(vec![TRANSFER_EVENT_TOPIC, from, to], data.into()),
    }
}

impl EthSimulateError {
    /// Returns the JSON-RPC error code for a `eth_simulateV1` error.
    pub const fn error_code(&self) -> i32 {
        match self {
            Self::NonceTooLow { .. } => -38010,
            Self::NonceTooHigh => -38011,
            Self::BaseFeePerGasTooLow => -38012,
            Self::IntrinsicGasTooLow => -38013,
            Self::InsufficientFunds { .. } => -38014,
            Self::BlockGasLimitExceeded => -38015,
            Self::BlockNumberInvalid { .. } => -38020,
            Self::BlockTimestampInvalid { .. } => -38021,
            Self::SenderNotEOA => -38024,
            Self::MaxInitCodeSizeExceeded => -38025,
            Self::TooManyBlocks | Self::GasLimitReached => -38026,
            Self::MovePrecompileToSelf(_) => -38022,
            Self::NotAPrecompile(_) => -32000,
        }
    }
}

impl ToRpcError for EthSimulateError {
    fn to_rpc_error(&self) -> ErrorObject<'static> {
        rpc_err(self.error_code(), self.to_string(), None)
    }
}

/// Applies precompile move overrides from state overrides to the EVM's precompiles map.
///
/// This function processes `movePrecompileToAddress` entries from the state overrides and
/// moves precompiles from their original addresses to new addresses. The original address
/// is cleared (precompile removed) and the precompile is installed at the destination address.
pub fn apply_precompile_overrides(
    state_overrides: &StateOverride,
    precompiles: &mut PrecompilesMap,
) -> Result<(), EthSimulateError> {
    let moves: Vec<_> = state_overrides
        .iter()
        .filter_map(|(source, account_override)| {
            account_override.move_precompile_to.map(|dest| (*source, dest))
        })
        .collect();

    for (source, dest) in moves {
        if source == dest {
            if precompiles.get(&source).is_none() {
                return Err(EthSimulateError::NotAPrecompile(source))
            }
            return Err(EthSimulateError::MovePrecompileToSelf(source))
        }

        precompiles.move_precompiles([(source, dest)]).map_err(
            |alloy_evm::precompiles::MovePrecompileError::NotAPrecompile(addr)| {
                EthSimulateError::NotAPrecompile(addr)
            },
        )?;
    }

    Ok(())
}

/// Converts all [`TransactionRequest`]s into [`Recovered`] transactions and applies them to the
/// given [`BlockExecutor`].
///
/// Returns all executed transactions and the result of the execution.
///
/// [`TransactionRequest`]: alloy_rpc_types_eth::TransactionRequest
#[expect(clippy::type_complexity)]
pub fn execute_transactions<S, T>(
    mut builder: S,
    calls: Vec<RpcTxReq<T::Network>>,
    remaining_call_gas_limit: &mut u64,
    chain_id: u64,
    state_provider: &dyn StateProvider,
    converter: &T,
    enforce_value_balance: bool,
    base_nonces: &mut HashMap<Address, u64>,
    transfer_logs: Option<&TransferLogCollector>,
) -> Result<
    (
        BlockBuilderOutcome<S::Primitives>,
        Vec<ExecutionResult<<<S::Executor as BlockExecutor>::Evm as Evm>::HaltReason>>,
    ),
    EthApiError,
>
where
    S: BlockBuilder<Executor: BlockExecutor<Evm: Evm<DB: Database<Error: Into<EthApiError>>>>>,
    T: RpcConvert<Primitives = S::Primitives>,
{
    builder.apply_pre_execution_changes()?;

    let mut results = Vec::with_capacity(calls.len());
    let block_gas_limit = builder.evm().block().gas_limit();
    let mut cumulative_gas_used = 0u64;
    let mut next_nonces = HashMap::new();
    let mut next_transfer = 0;
    for mut call in calls {
        let block_remaining_gas = block_gas_limit.saturating_sub(cumulative_gas_used);
        let default_gas_limit = if *remaining_call_gas_limit > 0 {
            block_remaining_gas.min(*remaining_call_gas_limit)
        } else {
            block_remaining_gas
        };
        let from = call.as_ref().from().unwrap_or_default();
        if call.as_ref().nonce().is_none() {
            let nonce = if let Some(nonce) = next_nonces.get(&from).copied() {
                nonce
            } else if let Some(nonce) = base_nonces.get(&from).copied() {
                nonce
            } else {
                let nonce = builder
                    .evm_mut()
                    .db_mut()
                    .basic(from)
                    .map_err(Into::into)?
                    .map(|acc| acc.nonce)
                    .unwrap_or_default();
                base_nonces.insert(from, nonce);
                nonce
            };
            call.as_mut().set_nonce(nonce);
            next_nonces.insert(from, next_simulated_nonce(nonce));
        }
        if let Some(gas_limit) = call.as_ref().gas_limit() &&
            *remaining_call_gas_limit > 0 &&
            gas_limit > *remaining_call_gas_limit
        {
            call.as_mut().set_gas_limit(*remaining_call_gas_limit);
        }

        // Resolve transaction, populate missing fields and enforce calls
        // correctness.
        let tx = resolve_transaction(
            call,
            default_gas_limit,
            builder.evm().block().basefee(),
            chain_id,
            builder.evm_mut().db_mut(),
            converter,
            enforce_value_balance,
        )?;
        let next_nonce = next_simulated_nonce(tx.nonce());

        // Create transaction with an empty envelope.
        // The effect for a layer-2 execution client is that it does not charge L1 cost.
        let tx = WithEncoded::new(Default::default(), tx);

        let gas_output = builder.execute_transaction_with_result_closure(tx, |result| {
            let mut result = result.result().result.clone();
            if let Some(transfer_logs) = transfer_logs {
                transfer_logs.append_new_logs(&mut result, &mut next_transfer);
            }
            results.push(result)
        })?;
        next_nonces.insert(from, next_nonce);
        cumulative_gas_used = cumulative_gas_used.saturating_add(gas_output.tx_gas_used());
        if *remaining_call_gas_limit > 0 {
            *remaining_call_gas_limit =
                remaining_call_gas_limit.saturating_sub(gas_output.tx_gas_used());
        }
    }

    base_nonces.extend(next_nonces);

    let result = builder.finish(state_provider, None)?;

    Ok((result, results))
}

const fn next_simulated_nonce(nonce: u64) -> u64 {
    nonce.wrapping_add(1)
}

/// Goes over the list of [`TransactionRequest`]s and populates missing fields trying to resolve
/// them into primitive transactions.
///
/// This will set the defaults as defined in <https://github.com/ethereum/execution-apis/blob/e56d3208789259d0b09fa68e9d8594aa4d73c725/docs/ethsimulatev1-notes.md#default-values-for-transactions>
///
/// [`TransactionRequest`]: alloy_rpc_types_eth::TransactionRequest
pub fn resolve_transaction<DB: Database, Tx, T>(
    mut tx: RpcTxReq<T::Network>,
    default_gas_limit: u64,
    block_base_fee_per_gas: u64,
    chain_id: u64,
    db: &mut DB,
    converter: &T,
    enforce_value_balance: bool,
) -> Result<Recovered<Tx>, EthApiError>
where
    DB::Error: Into<EthApiError>,
    T: RpcConvert<Primitives: NodePrimitives<SignedTx = Tx>>,
    Tx: Transaction,
{
    // If we're missing any fields we try to fill nonce, gas and
    // gas price.
    let tx_type = tx.as_ref().output_tx_type();

    let from = if let Some(from) = tx.as_ref().from() {
        from
    } else {
        tx.as_mut().set_from(Address::ZERO);
        Address::ZERO
    };

    if tx.as_ref().nonce().is_none() {
        tx.as_mut().set_nonce(
            db.basic(from).map_err(Into::into)?.map(|acc| acc.nonce).unwrap_or_default(),
        );
    }

    if tx.as_ref().gas_limit().is_none() {
        tx.as_mut().set_gas_limit(default_gas_limit);
    }

    if tx.as_ref().chain_id().is_none() {
        tx.as_mut().set_chain_id(chain_id);
    }

    if tx.as_ref().kind().is_none() {
        tx.as_mut().set_kind(TxKind::Create);
    }

    // if we can't build the _entire_ transaction yet, we need to check the fee values
    if tx.as_ref().output_tx_type_checked().is_none() {
        if tx_type.is_legacy() || tx_type.is_eip2930() {
            if tx.as_ref().gas_price().is_none() {
                tx.as_mut().set_gas_price(block_base_fee_per_gas as u128);
            }
        } else {
            // set dynamic 1559 fees
            if tx.as_ref().max_fee_per_gas().is_none() {
                let mut max_fee_per_gas = block_base_fee_per_gas as u128;
                if let Some(prio_fee) = tx.as_ref().max_priority_fee_per_gas() {
                    // if a prio fee is provided we need to select the max fee accordingly
                    // because the base fee must be higher than the prio fee.
                    max_fee_per_gas = prio_fee.max(max_fee_per_gas);
                }
                tx.as_mut().set_max_fee_per_gas(max_fee_per_gas);
            }
            if tx.as_ref().max_priority_fee_per_gas().is_none() {
                tx.as_mut().set_max_priority_fee_per_gas(0);
            }
        }
    }

    let tx =
        converter.build_simulate_v1_transaction(tx).map_err(|e| EthApiError::other(e.into()))?;

    if enforce_value_balance {
        let value = tx.value();
        if !value.is_zero() {
            let balance =
                db.basic(from).map_err(Into::into)?.map(|acc| acc.balance).unwrap_or_default();
            if balance < value {
                return Err(EthApiError::other(EthSimulateError::InsufficientFunds {
                    cost: value,
                    balance,
                }))
            }
        }
    }

    Ok(Recovered::new_unchecked(tx, from))
}

/// Handles outputs of the calls execution and builds a [`SimulatedBlock`].
pub fn build_simulated_block<Err, T>(
    block: RecoveredBlock<BlockTy<T::Primitives>>,
    results: Vec<ExecutionResult<HaltReasonFor<T::Evm>>>,
    txs_kind: BlockTransactionsKind,
    converter: &T,
) -> Result<SimulatedBlock<RpcBlock<T::Network>>, Err>
where
    Err: std::error::Error
        + FromEthApiError
        + FromEvmError<T::Evm>
        + From<T::Error>
        + Into<jsonrpsee_types::ErrorObject<'static>>,
    T: RpcConvert,
{
    let mut calls: Vec<SimCallResult> = Vec::with_capacity(results.len());

    let mut log_index = 0;
    for (index, (result, tx)) in results.into_iter().zip(block.body().transactions()).enumerate() {
        let call = match result {
            ExecutionResult::Halt { reason, gas, .. } => {
                let error = Err::from_evm_halt(reason, tx.gas_limit());
                SimCallResult {
                    return_data: Bytes::new(),
                    error: Some(SimulateError {
                        message: error.to_string(),
                        code: SIMULATE_VM_ERROR_CODE,
                        ..SimulateError::invalid_params()
                    }),
                    gas_used: gas.tx_gas_used(),
                    max_used_gas: Some(gas.tx_gas_used()),
                    logs: Vec::new(),
                    status: false,
                }
            }
            ExecutionResult::Revert { output, gas, .. } => {
                let error = Err::from_revert(output.clone());
                SimCallResult {
                    return_data: Bytes::new(),
                    error: Some(SimulateError {
                        message: error.to_string(),
                        code: SIMULATE_REVERT_CODE,
                        data: Some(output),
                        ..SimulateError::invalid_params()
                    }),
                    gas_used: gas.tx_gas_used(),
                    max_used_gas: Some(gas.tx_gas_used()),
                    status: false,
                    logs: Vec::new(),
                }
            }
            ExecutionResult::Success { output, gas, logs, .. } => SimCallResult {
                return_data: output.into_data(),
                error: None,
                gas_used: gas.tx_gas_used(),
                max_used_gas: Some(gas.tx_gas_used()),
                logs: logs
                    .into_iter()
                    .map(|log| {
                        log_index += 1;
                        alloy_rpc_types_eth::Log {
                            inner: log,
                            log_index: Some(log_index - 1),
                            transaction_index: Some(index as u64),
                            transaction_hash: Some(*tx.tx_hash()),
                            block_hash: Some(block.hash()),
                            block_number: Some(block.header().number()),
                            block_timestamp: Some(block.header().timestamp()),
                            ..Default::default()
                        }
                    })
                    .collect(),
                status: true,
            },
        };

        calls.push(call);
    }

    let block = block.into_rpc_block(
        txs_kind,
        |tx, tx_info| converter.fill(tx, tx_info),
        |header, size| converter.convert_header(header, size),
    )?;
    Ok(SimulatedBlock { inner: block, calls })
}

#[cfg(test)]
mod tests {
    use super::{apply_precompile_overrides, next_simulated_nonce, EthSimulateError};
    use alloy_evm::precompiles::PrecompilesMap;
    use alloy_primitives::address;
    use alloy_rpc_types_eth::state::{AccountOverride, StateOverride};
    use revm::precompile::Precompiles;

    #[test]
    fn simulated_nonce_wraps_at_max() {
        assert_eq!(next_simulated_nonce(0), 1);
        assert_eq!(next_simulated_nonce(u64::MAX), 0);
    }

    #[test]
    fn precompile_self_move_requires_existing_precompile() {
        let address = address!("c100000000000000000000000000000000000000");
        let mut state_overrides = StateOverride::default();
        state_overrides.insert(
            address,
            AccountOverride { move_precompile_to: Some(address), ..Default::default() },
        );
        let mut precompiles = PrecompilesMap::from_static(Precompiles::prague());

        let err = apply_precompile_overrides(&state_overrides, &mut precompiles).unwrap_err();

        assert!(matches!(err, EthSimulateError::NotAPrecompile(addr) if addr == address));
    }

    #[test]
    fn precompile_self_move_errors_for_existing_precompile() {
        let address = address!("0000000000000000000000000000000000000001");
        let mut state_overrides = StateOverride::default();
        state_overrides.insert(
            address,
            AccountOverride { move_precompile_to: Some(address), ..Default::default() },
        );
        let mut precompiles = PrecompilesMap::from_static(Precompiles::prague());

        let err = apply_precompile_overrides(&state_overrides, &mut precompiles).unwrap_err();

        assert!(matches!(err, EthSimulateError::MovePrecompileToSelf(addr) if addr == address));
    }
}
