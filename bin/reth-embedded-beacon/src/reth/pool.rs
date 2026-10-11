//! Shared blob-store injection, retaining execution pool validation and maintenance.

use reth_chainspec::ChainSpec;
use reth_embedded_beacon_shared_data::{SharedBlobStore, SharedDataStore, StoreIdentity};
use reth_ethereum_primitives::EthPrimitives;
use reth_evm::ConfigureEvm;
use reth_node_api::{FullNodeTypes, NodeTypes};
use reth_node_builder::{
    components::{PoolBuilder, TxPoolBuilder},
    BuilderContext,
};
use reth_transaction_pool::{EthTransactionPool, TransactionValidationTaskExecutor};

/// Replaces only blob byte storage through the existing public pool component boundary.
#[derive(Debug, Clone)]
pub(super) struct SharedPoolBuilder {
    /// Consensus network identity.
    pub(super) genesis_root: [u8; 32],
    /// Digest of the effective consensus configuration.
    pub(super) config_digest: [u8; 32],
    /// Shared immutable-byte cache capacity.
    pub(super) cache_bytes: usize,
}

impl<N, Evm> PoolBuilder<N, Evm> for SharedPoolBuilder
where
    N: FullNodeTypes<Types: NodeTypes<ChainSpec = ChainSpec, Primitives = EthPrimitives>>,
    Evm: ConfigureEvm<Primitives = EthPrimitives> + Clone + 'static,
{
    type Pool = EthTransactionPool<N::Provider, SharedBlobStore, Evm>;
    async fn build_pool(
        self,
        ctx: &BuilderContext<N>,
        evm_config: Evm,
    ) -> eyre::Result<Self::Pool> {
        let directory = ctx.config().datadir().data_dir().join("embedded-beacon");
        let identity =
            StoreIdentity { genesis_root: self.genesis_root, config_digest: self.config_digest };
        // Recovery and immutable-object verification can scan disk; keep it off runtime workers.
        let shared = tokio::task::spawn_blocking(move || {
            SharedDataStore::open(directory, identity, self.cache_bytes)
        })
        .await??;
        let blob_store = SharedBlobStore::new(shared);
        let config = ctx.pool_config();
        let blobs_disabled = ctx.config().txpool.disable_blobs_support ||
            ctx.config().txpool.blobpool_max_count == 0;
        let validator =
            TransactionValidationTaskExecutor::eth_builder(ctx.provider().clone(), evm_config)
                .set_eip4844(!blobs_disabled)
                .kzg_settings(ctx.kzg_settings()?)
                .with_max_tx_input_bytes(ctx.config().txpool.max_tx_input_bytes)
                .with_local_transactions_config(config.local_transactions_config.clone())
                .set_tx_fee_cap(ctx.config().rpc.rpc_tx_fee_cap)
                .with_max_tx_gas_limit(ctx.config().txpool.max_tx_gas_limit)
                .with_minimum_priority_fee(ctx.config().txpool.minimum_priority_fee)
                .with_additional_tasks(ctx.config().txpool.additional_validation_tasks)
                .build_with_tasks(ctx.task_executor().clone(), blob_store.clone());
        if validator.validator().eip4844() {
            let settings = validator.validator().kzg_settings().clone();
            ctx.task_executor().spawn_blocking_task(async move {
                let _ = settings.get();
            });
        }
        TxPoolBuilder::new(ctx)
            .with_validator(validator)
            .build_and_spawn_maintenance_task(blob_store, config)
    }
}
