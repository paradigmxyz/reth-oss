//! A PBT-from-genesis dev node using EIP-8297's BLAKE3 reference configuration.
//!
//! Run with `cargo run -p example-pbt-node`. The node and checkpoint directory are temporary.
//! This example exercises live engine processing, not staged or snap synchronization.

use alloy_primitives::U256;
use reth_engine_tree::{
    backfill::{BackfillAction, BackfillEvent, BackfillSync},
    tree::{
        state_root_strategy::{pbt::PbtStateRootStrategy, StateRootStrategy},
        BasicEngineValidator, TreeConfig,
    },
};
use reth_ethereum::{
    chainspec::ChainSpec,
    node::{
        builder::{
            rpc::{
                BasicEngineApiBuilder, BasicEngineValidatorBuilder, EngineValidatorBuilder,
                EthApiBuilder, Identity, RpcAddOns,
            },
            sync::{BackfillContext, BackfillSyncBuilder},
            FullNodeComponents, NodeBuilder,
        },
        core::{args::RpcServerArgs, node_config::NodeConfig},
        EthereumAddOns, EthereumEngineValidatorBuilder, EthereumEthApiBuilder, EthereumNode,
    },
    provider::{providers::ProviderNodeTypes, ProviderError},
    tasks::Runtime,
    EthPrimitives,
};
use reth_storage_overlay::OverlayManager;
use reth_trie_pbt::{migration::DecodeLimits, persistence::SnapshotStore, AccountData, StateTree};
use std::{
    sync::Arc,
    task::{Context, Poll},
};

#[derive(Clone, Debug)]
struct PbtValidatorBuilder {
    strategy: Arc<PbtStateRootStrategy>,
}

impl<N> EngineValidatorBuilder<N> for PbtValidatorBuilder
where
    N: FullNodeComponents<
        Types = EthereumNode,
        Evm: reth_ethereum::node::builder::ConfigureEngineEvm<
            alloy_rpc_types_engine::ExecutionData,
        >,
    >,
{
    type EngineValidator = BasicEngineValidator<N::Provider, N::Evm,
        <EthereumEngineValidatorBuilder as reth_ethereum::node::builder::rpc::PayloadValidatorBuilder<N>>::Validator>;

    async fn build_tree_validator(
        self,
        ctx: &reth_ethereum::node::builder::AddOnsContext<'_, N>,
        tree_config: TreeConfig,
        overlay_manager: OverlayManager<EthPrimitives>,
    ) -> eyre::Result<Self::EngineValidator> {
        let validator = BasicEngineValidatorBuilder::<EthereumEngineValidatorBuilder>::default()
            .build_tree_validator(ctx, tree_config, overlay_manager)
            .await?;
        let strategy: Arc<dyn StateRootStrategy<EthPrimitives, N::Provider, N::Evm>> =
            self.strategy;
        Ok(validator.with_state_root_strategy(strategy))
    }
}

type PbtAddOns<N> = EthereumAddOns<
    N,
    EthereumEthApiBuilder,
    EthereumEngineValidatorBuilder,
    BasicEngineApiBuilder<EthereumEngineValidatorBuilder>,
    PbtValidatorBuilder,
>;

fn addons<N>(strategy: Arc<PbtStateRootStrategy>) -> PbtAddOns<N>
where
    N: FullNodeComponents<
        Types = EthereumNode,
        Evm: reth_ethereum::node::builder::ConfigureEngineEvm<
            alloy_rpc_types_engine::ExecutionData,
        >,
    >,
    EthereumEthApiBuilder: EthApiBuilder<N>,
{
    EthereumAddOns::new(RpcAddOns::new(
        EthereumEthApiBuilder::default(),
        EthereumEngineValidatorBuilder::default(),
        BasicEngineApiBuilder::default(),
        PbtValidatorBuilder { strategy },
        Default::default(),
        Identity::new(),
    ))
}

fn genesis_state(chain: &ChainSpec) -> eyre::Result<StateTree> {
    let mut state = StateTree::new();
    for (address, account) in &chain.genesis.alloc {
        let account_data = AccountData {
            nonce: account.nonce.unwrap_or_default(),
            balance: account.balance,
            code: account.code.clone().unwrap_or_default(),
        };
        eyre::ensure!(!account_data.is_empty(), "PBT devnet genesis cannot contain empty accounts");
        state.set_account(*address, account_data)?;
        for (slot, value) in account.storage.iter().flatten() {
            state.set_storage(*address, U256::from_be_slice(slot.as_slice()), *value)?;
        }
    }
    Ok(state)
}

fn prepare_chain(
    mut chain: ChainSpec,
    activation: u64,
    store: &SnapshotStore,
) -> eyre::Result<Arc<ChainSpec>> {
    let state = genesis_state(&chain)?;
    if chain.genesis_header().timestamp >= activation {
        chain = chain.map_header(|mut header| {
            header.state_root = state.root();
            header
        });
    }
    store.save_block(chain.genesis_hash(), &state)?;
    Ok(Arc::new(chain))
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let directory = tempfile::tempdir()?;
    let store = Arc::new(SnapshotStore::open(directory.path(), DecodeLimits::default())?);
    let chain = prepare_chain((**reth_ethereum::chainspec::DEV).clone(), 0, &store)?;
    let config =
        NodeConfig::test().dev().with_rpc(RpcServerArgs::default().with_http()).with_chain(chain);
    let handle = NodeBuilder::new(config)
        .testing_node(Runtime::test())
        .with_types::<EthereumNode>()
        .with_components(EthereumNode::components())
        .with_add_ons(addons(Arc::new(PbtStateRootStrategy::new(store, 0))))
        .extend_rpc_modules(|ctx| {
            // MPT proof responses cannot authenticate against a PBT header commitment.
            ctx.modules.remove_method_from_configured("eth_getProof");
            ctx.auth_module.remove_auth_method("eth_getProof");
            Ok(())
        })
        .launch_with_debug_capabilities_and_backfill(PbtBackfill::default())
        .await?;
    println!(
        "PBT dev node running; HTTP RPC: {:?}",
        handle.node.rpc_server_handles.rpc.http_local_addr()
    );
    handle.wait_for_node_exit().await
}

/// Refuses sync paths that still validate MPT roots outside the installed live strategy.
#[derive(Debug, Default)]
struct PbtBackfill {
    requested: bool,
}

impl<N: ProviderNodeTypes, C> BackfillSyncBuilder<N, C> for PbtBackfill {
    type Backfill = Self;

    fn build(self, _ctx: BackfillContext<N, C>) -> eyre::Result<Self::Backfill> {
        Ok(self)
    }
}

impl BackfillSync for PbtBackfill {
    fn on_action(&mut self, action: BackfillAction) {
        if matches!(action, BackfillAction::Start(_)) {
            self.requested = true;
        }
    }

    fn poll(&mut self, _cx: &mut Context<'_>) -> Poll<BackfillEvent> {
        if !std::mem::take(&mut self.requested) {
            return Poll::Pending
        }
        Poll::Ready(BackfillEvent::Finished(Err(ProviderError::other(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "PBT dev node does not support staged/backfill sync",
        ))
        .into())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{hex, Address, Bytes, B256};
    use reth_e2e_test_utils::{
        eth_payload_attributes,
        node::{Finality, NodeTestContext},
        test_chain_spec,
        wallet::Wallet,
        Adapter,
    };
    use reth_ethereum::{chainspec::EthereumHardfork, node::builder::NodeHandle};
    use reth_payload_primitives::BuiltPayload;

    type TestNode = NodeTestContext<Adapter<EthereumNode>, PbtAddOns<Adapter<EthereumNode>>>;

    async fn setup(
        activation: u64,
    ) -> eyre::Result<(TestNode, Arc<SnapshotStore>, tempfile::TempDir)> {
        setup_for(activation, EthereumHardfork::Cancun).await
    }

    async fn setup_for(
        activation: u64,
        fork: EthereumHardfork,
    ) -> eyre::Result<(TestNode, Arc<SnapshotStore>, tempfile::TempDir)> {
        let directory = tempfile::tempdir()?;
        let store = Arc::new(SnapshotStore::open(directory.path(), DecodeLimits::default())?);
        let chain = prepare_chain((*test_chain_spec(fork)).clone(), activation, &store)?;
        let node = launch_test_node(chain, store.clone(), activation).await?;
        Ok((node, store, directory))
    }

    async fn launch_test_node(
        chain: Arc<ChainSpec>,
        store: Arc<SnapshotStore>,
        activation: u64,
    ) -> eyre::Result<TestNode> {
        let config = NodeConfig::test().with_chain(chain.clone());
        let NodeHandle { node, node_exit_future: _ } = NodeBuilder::new(config)
            .testing_node(Runtime::test())
            .with_types::<EthereumNode>()
            .with_components(EthereumNode::components())
            .with_add_ons(addons(Arc::new(PbtStateRootStrategy::new(store.clone(), activation))))
            .extend_rpc_modules(|ctx| {
                ctx.modules.remove_method_from_configured("eth_getProof");
                ctx.auth_module.remove_auth_method("eth_getProof");
                Ok(())
            })
            .launch_with_debug_capabilities_and_backfill(PbtBackfill::default())
            .await?;
        let node =
            NodeTestContext::new(node, move |timestamp| eth_payload_attributes(&*chain, timestamp))
                .await?;
        Ok(node)
    }

    #[tokio::test]
    async fn builds_imports_and_reorgs_pbt_blocks() -> eyre::Result<()> {
        reth_tracing::init_test_tracing();
        let (mut node, store, _directory) = setup(0).await?;
        let genesis = node.block_hash(0);
        node.set_finality(Finality::Keep);
        let mut sender = Wallet::new(1).account(0).with_gas_limit(1_000_000);
        let recipient = Address::repeat_byte(0x77);
        let transfer = sender.transfer(recipient, U256::from(7)).await;
        let mined = node.mine([transfer]).await?.ensure_success()?;
        assert_eq!(
            store.load_block(mined.block().hash())?.account(recipient).unwrap().balance,
            U256::from(7)
        );
        let first = node.advance_blocks(2).await?;
        for block in &first {
            assert_eq!(store.load_block(block.block().hash())?.root(), block.block().state_root);
        }
        let fork = node.advance_fork(genesis, 3).await?;
        node.reorg_to(first[1].block().hash()).await?;
        assert_eq!(node.current_forkchoice_state()?.head_block_hash, first[1].block().hash());
        node.reorg_to(fork[2].block().hash()).await?;
        assert_eq!(node.current_forkchoice_state()?.head_block_hash, fork[2].block().hash());
        Ok(())
    }

    #[tokio::test]
    async fn activation_switches_commitment_and_keeps_pre_fork_state() -> eyre::Result<()> {
        reth_tracing::init_test_tracing();
        let activation = 1_710_338_137;
        let (mut node, store, _directory) = setup(activation).await?;
        node.set_finality(Finality::Keep);
        let first = node.advance_block().await?;
        let pre = store.load_block(first.block().hash())?;
        assert_eq!(reth_trie_pbt::migration::mpt_root(&pre), first.block().state_root);
        let second = node.advance_block().await?;
        assert_eq!(store.load_block(second.block().hash())?.root(), second.block().state_root);
        assert_ne!(
            reth_trie_pbt::migration::mpt_root(&store.load_block(second.block().hash())?),
            second.block().state_root
        );
        let fork = node.advance_fork(node.block_hash(0), 3).await?;
        assert_eq!(store.load_block(fork[2].block().hash())?.root(), fork[2].block().state_root);
        Ok(())
    }

    #[tokio::test]
    async fn deployment_and_storage_write_commit_code_and_overflow_storage() -> eyre::Result<()> {
        reth_tracing::init_test_tracing();
        let (mut node, store, _directory) = setup(0).await?;
        let mut sender = Wallet::new(1).account(0).with_gas_limit(1_000_000);
        // Runtime writes 1 to slot 64, which belongs to the overflow storage zone.
        let contract = sender.next_contract_address();
        let deployment = sender.deploy(hex!("6006600c60003960066000f3600160405500").to_vec()).await;
        node.mine([deployment]).await?.ensure_success()?;
        let call = sender.call(contract, Bytes::new()).await;
        let mined = node.mine([call]).await?.ensure_success()?;
        let state = store.load_block(mined.block().hash())?;
        assert_eq!(state.account(contract).unwrap().code.as_ref(), hex!("600160405500"));
        assert_eq!(state.storage(contract, U256::from(64)), B256::from(U256::from(1)));
        assert_eq!(state.root(), mined.block().state_root);
        Ok(())
    }

    #[tokio::test]
    async fn builds_and_validates_pbt_with_block_access_lists() -> eyre::Result<()> {
        reth_tracing::init_test_tracing();
        let (mut node, store, _directory) = setup_for(0, EthereumHardfork::Amsterdam).await?;
        let recipient = Address::repeat_byte(0x77);
        let mut sender = Wallet::new(1).account(0);
        let transfer = sender.transfer(recipient, U256::from(9)).await;
        let mined = node.mine([transfer]).await?.ensure_success()?;
        let state = store.load_block(mined.block().hash())?;
        assert_eq!(state.root(), mined.block().state_root);
        assert_eq!(state.account(recipient).unwrap().balance, U256::from(9));
        let bytes = mined.payload.block_access_list().expect("Amsterdam payload carries a BAL");
        let mut input = bytes.as_ref();
        let bal: Vec<alloy_eip7928::AccountChanges> = alloy_rlp::Decodable::decode(&mut input)?;
        assert!(input.is_empty());
        let parent = store.load_block(mined.block().parent_hash)?;
        let replayed = reth_trie_pbt::replay::replay(
            &parent,
            &bal,
            mined.block().block_access_list_hash.expect("Amsterdam header commits to its BAL"),
        )?;
        assert_eq!(replayed.root(), state.root());
        Ok(())
    }

    #[tokio::test]
    async fn earliest_payload_uses_pbt_even_when_racing_an_empty_build() -> eyre::Result<()> {
        reth_tracing::init_test_tracing();
        let (mut node, store, _directory) = setup(0).await?;
        let attributes = node.payload.next_attributes();
        let choice = node
            .engine
            .forkchoice_updated_with_attributes(node.current_forkchoice_state()?, attributes)
            .await?;
        let id = choice.payload_id.ok_or_else(|| eyre::eyre!("missing payload id"))?;
        let payload = node
            .inner
            .payload_builder_handle
            .resolve_kind(id, reth_payload_primitives::PayloadKind::Earliest)
            .await
            .ok_or_else(|| eyre::eyre!("missing payload job"))??;
        assert_eq!(store.load(payload.block().state_root)?.root(), payload.block().state_root);
        node.import_payload(payload).await?;
        Ok(())
    }

    #[tokio::test]
    async fn missing_checkpoint_fails_payload_building_without_an_mpt_fallback() -> eyre::Result<()>
    {
        reth_tracing::init_test_tracing();
        let directory = tempfile::tempdir()?;
        let anchor = SnapshotStore::open(directory.path().join("anchor"), DecodeLimits::default())?;
        let chain =
            prepare_chain((*test_chain_spec(EthereumHardfork::Cancun)).clone(), 0, &anchor)?;
        let missing = Arc::new(SnapshotStore::open(
            directory.path().join("missing"),
            DecodeLimits::default(),
        )?);
        let mut node = launch_test_node(chain, missing, 0).await?;
        let attributes = node.payload.next_attributes();
        let choice = node
            .engine
            .forkchoice_updated_with_attributes(node.current_forkchoice_state()?, attributes)
            .await?;
        let id = choice.payload_id.ok_or_else(|| eyre::eyre!("missing payload id"))?;
        let result = node
            .inner
            .payload_builder_handle
            .resolve_kind(id, reth_payload_primitives::PayloadKind::Earliest)
            .await;
        assert!(matches!(result, Some(Err(_))));
        Ok(())
    }

    #[test]
    fn unsupported_backfill_fails_before_running_pipeline() {
        let mut backfill = PbtBackfill::default();
        let mut cx = Context::from_waker(std::task::Waker::noop());
        assert!(backfill.poll(&mut cx).is_pending());
        backfill.on_action(BackfillAction::UpdateFinalized(B256::repeat_byte(1)));
        backfill.on_action(BackfillAction::UpdateTarget(B256::repeat_byte(2)));
        assert!(backfill.poll(&mut cx).is_pending());

        backfill.on_action(BackfillAction::Start(B256::repeat_byte(3).into()));
        let Poll::Ready(BackfillEvent::Finished(Err(error))) = backfill.poll(&mut cx) else {
            panic!("unsupported backfill must fail immediately");
        };
        assert_eq!(error.to_string(), "PBT dev node does not support staged/backfill sync");
        assert!(backfill.poll(&mut cx).is_pending());
    }
}
