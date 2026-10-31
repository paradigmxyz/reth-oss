//! A PBT-from-genesis dev node using EIP-8297's BLAKE3 reference configuration.
//!
//! Run with `cargo run -p example-pbt-node`. Set `PBT_DATADIR` to retain the node and checkpoints.
//! This example exercises live engine processing, not staged or snap synchronization.

mod rpc;

use alloy_eip7928::bal::DecodedBal;
use alloy_primitives::{B256, U256};
use eyre::WrapErr;
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
            FullNodeComponents, NodeBuilder, NodeTypes,
        },
        core::{
            args::{DatadirArgs, RpcServerArgs},
            node_config::NodeConfig,
        },
        EthereumAddOns, EthereumEngineValidatorBuilder, EthereumEthApiBuilder, EthereumNode,
    },
    primitives::{AlloyBlockHeader, SealedHeader},
    provider::{
        providers::ProviderNodeTypes, BalProvider, BlockIdReader, BlockNumReader, HeaderProvider,
        ProviderError, RawBal, StateProviderFactory, StateRangeProviderFactory,
    },
    tasks::Runtime,
    EthPrimitives,
};
use reth_network_api::BlockDownloaderProvider;
use reth_network_p2p::{BalRequirement, BlockAccessListsClient};
use reth_storage_overlay::OverlayManager;
use reth_trie_pbt::{
    migration::{mpt_root, DecodeLimits},
    persistence::SnapshotStore,
    replay::replay,
    AccountData, StateTree,
};
use std::{
    collections::BTreeSet,
    ffi::OsString,
    fs::{File, OpenOptions, TryLockError},
    num::NonZeroUsize,
    path::{Path, PathBuf},
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

#[derive(Clone, Debug)]
struct PbtValidatorBuilder {
    strategy: Arc<PbtStateRootStrategy>,
    startup: Startup,
}

impl<N> EngineValidatorBuilder<N> for PbtValidatorBuilder
where
    N: FullNodeComponents<
        Types: NodeTypes<
            Primitives = EthPrimitives,
            ChainSpec = ChainSpec,
            Payload = <EthereumNode as NodeTypes>::Payload,
        >,
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
        let strategy = self.strategy.clone();
        let provider = ctx.node.provider().clone();
        if self.startup.migration.is_some() {
            let client = tokio::select! {
                result = tokio::time::timeout(Duration::from_secs(30), ctx.node.network().fetch_client()) => result??,
                _ = ctx.node.task_executor().on_shutdown_signal().clone() => eyre::bail!("PBT onboarding interrupted by shutdown"),
            };
            tokio::select! {
                result = self.startup.fetch_missing_bals(&provider, &client) => result?,
                _ = ctx.node.task_executor().on_shutdown_signal().clone() => eyre::bail!("PBT onboarding interrupted by shutdown"),
            }
        }
        tokio::task::spawn_blocking(move || self.startup.initialize(&strategy, &provider))
            .await??;
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

fn addons<N>(strategy: Arc<PbtStateRootStrategy>, startup: Startup) -> PbtAddOns<N>
where
    N: FullNodeComponents<
        Types: NodeTypes<
            Primitives = EthPrimitives,
            ChainSpec = ChainSpec,
            Payload = <EthereumNode as NodeTypes>::Payload,
        >,
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
        PbtValidatorBuilder { strategy, startup },
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
    let configuration =
        [b"PBT1".as_slice(), chain.genesis_hash().as_slice(), &activation.to_be_bytes()].concat();
    let path = store.path().join("chain-config");
    if path.try_exists()? {
        let mut file = reth_fs_util::open(&path)?;
        eyre::ensure!(
            file.metadata()?.len() == CONFIGURATION_LENGTH as u64,
            "invalid PBT checkpoint configuration length"
        );
        let mut saved = [0; CONFIGURATION_LENGTH];
        std::io::Read::read_exact(&mut file, &mut saved)
            .map_err(|error| reth_fs_util::FsPathError::read(error, &path))?;
        eyre::ensure!(
            saved.as_slice() == configuration,
            "PBT checkpoint directory belongs to a different genesis or activation timestamp"
        );
    } else {
        for name in ["states", "blocks"] {
            eyre::ensure!(
                reth_fs_util::read_dir(store.path().join(name))?.next().transpose()?.is_none(),
                "PBT checkpoint directory is populated but chain-config is missing"
            );
        }
        store.save_block(chain.genesis_hash(), &state)?;
        reth_fs_util::atomic_write_file(&path, |file| {
            std::io::Write::write_all(file, &configuration)
        })?;
    }
    Ok(Arc::new(chain))
}

const CONFIGURATION_LENGTH: usize = 4 + 32 + 8;

fn lock_directory(directory: &Path) -> eyre::Result<File> {
    reth_fs_util::create_dir_all(directory)?;
    let path = directory.join("pbt.lock");
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)
        .map_err(|error| reth_fs_util::FsPathError::open(error, &path))?;
    match file.try_lock() {
        Ok(()) => Ok(file),
        Err(TryLockError::WouldBlock) => eyre::bail!("PBT data directory is already in use"),
        Err(TryLockError::Error(error)) => Err(reth_fs_util::FsPathError::open(error, path).into()),
    }
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let configured_directory = std::env::var_os("PBT_DATADIR");
    let temporary = configured_directory.is_none().then(tempfile::tempdir).transpose()?;
    let directory = configured_directory
        .map(PathBuf::from)
        .or_else(|| temporary.as_ref().map(|directory| directory.path().to_path_buf()))
        .ok_or_else(|| eyre::eyre!("missing PBT data directory"))?;
    let activation = match std::env::var("PBT_ACTIVATION_TIMESTAMP") {
        Ok(value) => value.parse::<u64>()?,
        Err(std::env::VarError::NotPresent) => 0,
        Err(error) => return Err(error.into()),
    };
    let mut startup = Startup::new(
        std::env::var_os("PBT_MIGRATION_SNAPSHOT"),
        std::env::var_os("PBT_MIGRATION_PREIMAGES"),
        std::env::var_os("PBT_MIGRATION_ANCHOR"),
        std::env::var_os("PBT_PRUNE_BLOCKS"),
        CatchupLimits::new(
            std::env::var_os("PBT_CATCHUP_MAX_BLOCKS"),
            std::env::var_os("PBT_CATCHUP_MAX_BAL_BYTES"),
        )?,
    )?;
    startup.self_convert = match std::env::var("PBT_SELF_CONVERT") {
        Ok(value) => value.parse::<bool>()?,
        Err(std::env::VarError::NotPresent) => false,
        Err(error) => return Err(error.into()),
    };
    eyre::ensure!(
        !startup.self_convert || startup.migration.is_some(),
        "PBT_SELF_CONVERT requires migration anchor and artifact paths"
    );
    // Keep the inode and its OS lock for the whole node lifetime, including shutdown.
    let _directory_lock = lock_directory(&directory)?;
    let store = Arc::new(SnapshotStore::open(directory.join("pbt"), DecodeLimits::default())?);
    let chain = prepare_chain((**reth_ethereum::chainspec::DEV).clone(), activation, &store)?;
    let config = NodeConfig::test()
        .dev()
        .with_datadir_args(DatadirArgs { datadir: directory.into(), ..Default::default() })
        .with_rpc(RpcServerArgs::default().with_http())
        .with_chain(chain);
    let database = Arc::new(reth_ethereum::provider::db::init_db(
        config.datadir().db(),
        reth_ethereum::provider::db::mdbx::DatabaseArguments::default(),
    )?);
    let runtime = Runtime::test();
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let mut handle = NodeBuilder::new(config)
        .with_database(database)
        .with_launch_context(runtime.clone())
        .with_types::<EthereumNode>()
        .with_components(EthereumNode::components())
        .with_add_ons(addons(
            Arc::new(PbtStateRootStrategy::new(store.clone(), activation)),
            startup,
        ))
        .extend_rpc_modules(move |ctx| {
            // MPT proof responses cannot authenticate against a PBT header commitment.
            ctx.modules.remove_method_from_configured("eth_getProof");
            ctx.auth_module.remove_auth_method("eth_getProof");
            ctx.auth_module.merge_auth_methods(rpc::ProofRpc::new(store).into_rpc()?)?;
            Ok(())
        })
        .launch_with_debug_capabilities_and_backfill(PbtBackfill::default())
        .await?;
    println!(
        "PBT dev node running; HTTP RPC: {:?}; authenticated RPC: {}",
        handle.node.rpc_server_handles.rpc.http_local_addr(),
        handle.node.auth_server_handle().http_url()
    );
    let result = tokio::select! {
        result = &mut handle.node_exit_future => result,
        _ = interrupt.recv() => Ok(()),
        _ = terminate.recv() => Ok(()),
    };
    let rpc_stopped = handle.node.rpc_server_handle().clone().stop();
    let auth_stopped = handle.node.auth_server_handle().clone().stop();
    let stopped = tokio::task::spawn_blocking(move || {
        runtime.graceful_shutdown_with_timeout(Duration::from_secs(30))
    })
    .await?;
    eyre::ensure!(stopped, "PBT dev node did not finish shutdown within 30 seconds");
    result?;
    rpc_stopped?;
    auth_stopped?;
    Ok(())
}

#[derive(Clone, Debug, Default)]
struct Startup {
    migration: Option<MigrationFiles>,
    obsolete_checkpoints: Vec<B256>,
    catchup: CatchupLimits,
    self_convert: bool,
}

impl Startup {
    fn new(
        snapshot: Option<OsString>,
        preimages: Option<OsString>,
        anchor: Option<OsString>,
        obsolete: Option<OsString>,
        catchup: CatchupLimits,
    ) -> eyre::Result<Self> {
        let migration = match (snapshot, preimages, anchor) {
            (Some(snapshot), Some(preimages), Some(anchor)) => {
                let anchor = anchor
                    .into_string()
                    .map_err(|_| eyre::eyre!("PBT_MIGRATION_ANCHOR must be a UTF-8 block hash"))?
                    .parse()?;
                Some(MigrationFiles { snapshot: snapshot.into(), preimages: preimages.into(), anchor })
            }
            (None, None, None) => None,
            _ => eyre::bail!(
                "PBT_MIGRATION_SNAPSHOT, PBT_MIGRATION_PREIMAGES and PBT_MIGRATION_ANCHOR must be set together"
            ),
        };
        let obsolete_checkpoints = obsolete
            .map(|hashes| {
                let hashes = hashes
                    .into_string()
                    .map_err(|_| eyre::eyre!("PBT_PRUNE_BLOCKS must contain UTF-8 block hashes"))?;
                hashes
                    .split(',')
                    .map(|hash| hash.parse::<B256>().map_err(Into::into))
                    .collect::<eyre::Result<Vec<_>>>()
            })
            .transpose()?
            .unwrap_or_default();
        Ok(Self { migration, obsolete_checkpoints, catchup, self_convert: false })
    }

    fn initialize<P>(&self, strategy: &PbtStateRootStrategy, provider: &P) -> eyre::Result<()>
    where
        P: BalProvider
            + BlockIdReader
            + BlockNumReader
            + HeaderProvider
            + StateProviderFactory
            + StateRangeProviderFactory,
    {
        let finalized = if self.migration.is_some() || !self.obsolete_checkpoints.is_empty() {
            let hash = provider.finalized_block_hash()?.ok_or_else(|| {
                eyre::eyre!("PBT onboarding or pruning requires a locally finalized block")
            })?;
            Some(
                provider
                    .sealed_header_by_hash(hash)?
                    .ok_or_else(|| eyre::eyre!("missing local finalized header"))?,
            )
        } else {
            None
        };
        let number = provider.last_block_number()?;
        let tip = provider
            .sealed_header(number)?
            .ok_or_else(|| eyre::eyre!("missing persisted tip header"))?;
        if let Some(migration) = &self.migration {
            let finalized =
                finalized.as_ref().ok_or_else(|| eyre::eyre!("missing local finalized header"))?;
            let header = provider
                .sealed_header_by_hash(migration.anchor)?
                .ok_or_else(|| eyre::eyre!("missing PBT migration anchor header"))?;
            eyre::ensure!(
                header.number() <= finalized.number() &&
                    provider
                        .sealed_header(header.number())?
                        .is_some_and(|canonical| { canonical.hash() == migration.anchor }),
                "PBT migration anchor must be canonical and finalized"
            );
            eyre::ensure!(
                header.timestamp() < strategy.activation_timestamp(),
                "PBT migration anchor must precede activation"
            );
            if self.self_convert {
                eyre::ensure!(
                    !migration.snapshot.try_exists()? ||
                        migration.snapshot.canonicalize()? !=
                            migration.preimages.canonicalize()?,
                    "PBT snapshot output must differ from the preimage input"
                );
                let source = provider
                    .state_range_provider(header.state_root())?
                    .ok_or_else(|| eyre::eyre!("PBT conversion anchor state is not retained"))?;
                let codes = provider.state_by_block_hash(header.hash())?;
                let preimages = reth_fs_util::open(&migration.preimages)?;
                let artifacts = reth_trie_pbt::conversion::convert::<reth_trie_pbt::Blake3Hasher>(
                    &source,
                    codes.as_ref(),
                    preimages,
                    header.state_root(),
                    DecodeLimits::default(),
                )?;
                reth_fs_util::atomic_write_file(&migration.snapshot, |file| {
                    std::io::Write::write_all(file, &artifacts.snapshot)
                })?;
            }
            strategy.store().import_anchor(
                header.hash(),
                header.state_root(),
                &migration.snapshot,
                &migration.preimages,
            )?;
            self.catch_up(strategy, provider, &header, &tip)?;
        }
        strategy
            .restore_parent(tip.hash(), tip.state_root(), tip.timestamp())
            .wrap_err("PBT persisted tip checkpoint is missing or inconsistent")?;
        if !self.obsolete_checkpoints.is_empty() {
            let finalized =
                finalized.ok_or_else(|| eyre::eyre!("missing local finalized header"))?;
            let trusted = strategy.restore_parent(
                finalized.hash(),
                finalized.state_root(),
                finalized.timestamp(),
            )?;
            eyre::ensure!(
                strategy.store().load_block(finalized.hash())?.root() == trusted.root(),
                "PBT finalized checkpoint reference does not match its header"
            );
            let mut unmatched = self.obsolete_checkpoints.iter().copied().collect::<BTreeSet<_>>();
            eyre::ensure!(
                !unmatched.contains(&finalized.hash()),
                "cannot prune the finalized PBT checkpoint"
            );
            let mut ancestor = finalized.clone();
            while !unmatched.is_empty() && ancestor.number() != 0 {
                let parent = provider
                    .sealed_header_by_hash(ancestor.parent_hash())?
                    .ok_or_else(|| eyre::eyre!("missing finalized ancestor header"))?;
                eyre::ensure!(
                    parent.number().checked_add(1) == Some(ancestor.number()),
                    "inconsistent finalized header ancestry"
                );
                unmatched.remove(&parent.hash());
                ancestor = parent;
            }
            eyre::ensure!(
                unmatched.is_empty(),
                "PBT prune candidates must be strict ancestors of the finalized block"
            );
            strategy.store().prune_finalized(finalized.hash(), &self.obsolete_checkpoints)?;
        }
        Ok(())
    }

    fn catch_up<P>(
        &self,
        strategy: &PbtStateRootStrategy,
        provider: &P,
        anchor: &SealedHeader<P::Header>,
        tip: &SealedHeader<P::Header>,
    ) -> eyre::Result<()>
    where
        P: BalProvider + HeaderProvider,
    {
        let headers = self.catchup_headers(provider, anchor, tip)?;
        let mut state =
            strategy.restore_parent(anchor.hash(), anchor.state_root(), anchor.timestamp())?;
        let mut remaining_bytes = self.catchup.max_bal_bytes.get();
        for header in headers {
            let commitment = header
                .block_access_list_hash()
                .ok_or_else(|| eyre::eyre!("PBT catch-up header has no BAL commitment"))?;
            let bytes = provider
                .bal_store()
                .get_by_hash(header.hash())?
                .ok_or_else(|| eyre::eyre!("PBT catch-up BAL is unavailable"))?;
            remaining_bytes = remaining_bytes
                .checked_sub(bytes.len())
                .ok_or_else(|| eyre::eyre!("PBT catch-up BAL byte budget exceeded"))?;
            let raw = RawBal::new(bytes);
            raw.ensure_hash(commitment)?;
            let bal = DecodedBal::from_raw_bal(raw)?;
            let next = replay(&state, bal.as_bal().as_slice(), commitment)?;
            let root = if header.timestamp() >= strategy.activation_timestamp() {
                next.root()
            } else {
                mpt_root(&next)
            };
            eyre::ensure!(
                root == header.state_root(),
                "PBT catch-up state root does not match its header"
            );
            strategy.store().save_block(header.hash(), &next)?;
            state = next;
        }
        Ok(())
    }

    fn catchup_headers<P: HeaderProvider>(
        &self,
        provider: &P,
        anchor: &SealedHeader<P::Header>,
        tip: &SealedHeader<P::Header>,
    ) -> eyre::Result<Vec<SealedHeader<P::Header>>> {
        let mut headers = Vec::new();
        let mut ancestor = tip.clone();
        while ancestor.hash() != anchor.hash() {
            eyre::ensure!(
                ancestor.number() > anchor.number(),
                "PBT migration anchor is not an ancestor of the persisted tip"
            );
            eyre::ensure!(
                headers.len() < self.catchup.max_blocks.get(),
                "PBT catch-up block budget exceeded"
            );
            let parent = provider
                .sealed_header_by_hash(ancestor.parent_hash())?
                .ok_or_else(|| eyre::eyre!("PBT catch-up header is unavailable"))?;
            eyre::ensure!(
                parent.number().checked_add(1) == Some(ancestor.number()) &&
                    parent.timestamp() < ancestor.timestamp(),
                "inconsistent PBT catch-up header ancestry"
            );
            headers.push(ancestor);
            ancestor = parent;
        }
        headers.reverse();
        Ok(headers)
    }

    async fn fetch_missing_bals<P, C>(&self, provider: &P, client: &C) -> eyre::Result<()>
    where
        P: BalProvider + BlockNumReader + HeaderProvider,
        C: BlockAccessListsClient,
    {
        let Some(migration) = &self.migration else { return Ok(()) };
        let anchor = provider
            .sealed_header_by_hash(migration.anchor)?
            .ok_or_else(|| eyre::eyre!("missing PBT migration anchor header"))?;
        let tip = provider
            .sealed_header(provider.last_block_number()?)?
            .ok_or_else(|| eyre::eyre!("missing persisted tip header"))?;
        let headers = self.catchup_headers(provider, &anchor, &tip)?;
        let mut remaining_bytes = self.catchup.max_bal_bytes.get();
        for header in headers {
            let commitment = header
                .block_access_list_hash()
                .ok_or_else(|| eyre::eyre!("PBT catch-up header has no BAL commitment"))?;
            if let Some(bytes) = provider.bal_store().get_by_hash(header.hash())? {
                remaining_bytes = remaining_bytes
                    .checked_sub(bytes.len())
                    .ok_or_else(|| eyre::eyre!("PBT catch-up BAL byte budget exceeded"))?;
                RawBal::new(bytes).ensure_hash(commitment)?;
                continue;
            }
            let mut downloaded = None;
            for _ in 0..3 {
                let response = tokio::time::timeout(
                    Duration::from_secs(10),
                    client.get_block_access_lists_with_requirement(
                        vec![header.hash()],
                        BalRequirement::Mandatory,
                    ),
                )
                .await;
                if let Ok(Ok(response)) = response {
                    let mut entries = response.into_data().0;
                    if entries.len() == 1 &&
                        let Some(bytes) = entries.pop().flatten()
                    {
                        let raw = RawBal::new(bytes);
                        if raw.ensure_hash(commitment).is_ok() {
                            downloaded = Some(raw);
                            break;
                        }
                    }
                }
            }
            let raw = downloaded.ok_or_else(|| {
                eyre::eyre!("PBT catch-up BAL could not be fetched after 3 attempts")
            })?;
            remaining_bytes = remaining_bytes
                .checked_sub(raw.as_raw().len())
                .ok_or_else(|| eyre::eyre!("PBT catch-up BAL byte budget exceeded"))?;
            provider.bal_store().insert(header.num_hash(), raw)?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
struct CatchupLimits {
    max_blocks: NonZeroUsize,
    max_bal_bytes: NonZeroUsize,
}

impl Default for CatchupLimits {
    fn default() -> Self {
        Self { max_blocks: DEFAULT_CATCHUP_BLOCKS, max_bal_bytes: DEFAULT_CATCHUP_BAL_BYTES }
    }
}

impl CatchupLimits {
    fn new(blocks: Option<OsString>, bytes: Option<OsString>) -> eyre::Result<Self> {
        let parse = |value: Option<OsString>, default| -> eyre::Result<NonZeroUsize> {
            match value {
                Some(value) => Ok(value
                    .into_string()
                    .map_err(|_| eyre::eyre!("PBT catch-up budgets must be UTF-8 integers"))?
                    .parse()?),
                None => Ok(default),
            }
        };
        Ok(Self {
            max_blocks: parse(blocks, DEFAULT_CATCHUP_BLOCKS)?,
            max_bal_bytes: parse(bytes, DEFAULT_CATCHUP_BAL_BYTES)?,
        })
    }
}

const DEFAULT_CATCHUP_BLOCKS: NonZeroUsize =
    NonZeroUsize::new(1024).expect("block budget is nonzero");
const DEFAULT_CATCHUP_BAL_BYTES: NonZeroUsize =
    NonZeroUsize::new(64 * 1024 * 1024).expect("BAL byte budget is nonzero");

#[derive(Clone, Debug)]
struct MigrationFiles {
    snapshot: PathBuf,
    preimages: PathBuf,
    anchor: B256,
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
    use alloy_primitives::{hex, Address, Bytes};
    use alloy_rpc_types_engine::ForkchoiceState;
    use jsonrpsee::{core::client::ClientT, rpc_params};
    use reth_e2e_test_utils::{
        eth_payload_attributes,
        node::{Finality, NodeTestContext},
        test_chain_spec,
        wallet::Wallet,
        Adapter, E2ETestSetupBuilder,
    };
    use reth_ethereum::{
        chainspec::EthereumHardfork,
        node::{
            builder::{
                components::{BasicPayloadServiceBuilder, ComponentsBuilder},
                FullNodeTypes, Node, NodeAdapter, NodeHandle,
            },
            EthereumConsensusBuilder, EthereumExecutorBuilder, EthereumNetworkBuilder,
            EthereumPayloadBuilder, EthereumPoolBuilder,
        },
        provider::{
            test_utils::{MockEthProvider, NoopProvider},
            BalStoreHandle, InMemoryBalStore,
        },
    };
    use reth_payload_primitives::BuiltPayload;
    use reth_trie_pbt::{key, migration::Artifacts, Blake3Hasher};
    use std::sync::Mutex;

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
        let config = NodeConfig::test()
            .with_chain(chain.clone())
            .with_rpc(RpcServerArgs::default().with_unused_ports().with_http());
        let NodeHandle { node, node_exit_future: _ } = NodeBuilder::new(config)
            .testing_node(Runtime::test())
            .with_types::<EthereumNode>()
            .with_components(EthereumNode::components())
            .with_add_ons(addons(
                Arc::new(PbtStateRootStrategy::new(store.clone(), activation)),
                Startup::default(),
            ))
            .extend_rpc_modules(move |ctx| {
                ctx.modules.remove_method_from_configured("eth_getProof");
                ctx.auth_module.remove_auth_method("eth_getProof");
                ctx.auth_module.merge_auth_methods(rpc::ProofRpc::new(store).into_rpc()?)?;
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
    async fn missing_checkpoint_rejects_startup_without_an_mpt_fallback() -> eyre::Result<()> {
        reth_tracing::init_test_tracing();
        let directory = tempfile::tempdir()?;
        let anchor = SnapshotStore::open(directory.path().join("anchor"), DecodeLimits::default())?;
        let chain =
            prepare_chain((*test_chain_spec(EthereumHardfork::Cancun)).clone(), 0, &anchor)?;
        let missing = Arc::new(SnapshotStore::open(
            directory.path().join("missing"),
            DecodeLimits::default(),
        )?);
        let error = match launch_test_node(chain, missing, 0).await {
            Ok(_) => eyre::bail!("startup accepted a missing PBT checkpoint"),
            Err(error) => error,
        };
        assert_eq!(error.to_string(), "PBT persisted tip checkpoint is missing or inconsistent");
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

    #[derive(Clone, Debug, Default)]
    struct RestartNode {
        checkpoints: PathBuf,
        activation: u64,
        startup: Startup,
    }

    impl NodeTypes for RestartNode {
        type Primitives = <EthereumNode as NodeTypes>::Primitives;
        type ChainSpec = <EthereumNode as NodeTypes>::ChainSpec;
        type Storage = <EthereumNode as NodeTypes>::Storage;
        type Payload = <EthereumNode as NodeTypes>::Payload;
    }

    impl<N: FullNodeTypes<Types = Self>> Node<N> for RestartNode {
        type ComponentsBuilder = ComponentsBuilder<
            N,
            EthereumPoolBuilder,
            BasicPayloadServiceBuilder<EthereumPayloadBuilder>,
            EthereumNetworkBuilder,
            EthereumExecutorBuilder,
            EthereumConsensusBuilder,
        >;
        type AddOns = PbtAddOns<NodeAdapter<N>>;

        fn components_builder(&self) -> Self::ComponentsBuilder {
            EthereumNode::components()
        }

        fn add_ons(&self) -> Self::AddOns {
            let store =
                Arc::new(SnapshotStore::open(&self.checkpoints, DecodeLimits::default()).unwrap());
            addons(
                Arc::new(PbtStateRootStrategy::new(store, self.activation)),
                self.startup.clone(),
            )
        }
    }

    #[tokio::test]
    async fn finalized_anchor_onboarding_survives_restart_and_activation_reorg() -> eyre::Result<()>
    {
        reth_tracing::init_test_tracing();
        let activation = 1_710_338_138;
        let directory = tempfile::tempdir()?;
        let checkpoints = directory.path().join("pbt");
        let store = SnapshotStore::open(&checkpoints, DecodeLimits::default())?;
        let chain = prepare_chain(
            (*test_chain_spec(EthereumHardfork::Amsterdam)).clone(),
            activation,
            &store,
        )?;
        let startup = Arc::new(Mutex::new(Startup::default()));
        let node_checkpoints = checkpoints.clone();
        let node_startup = startup.clone();
        let (mut node, _) = E2ETestSetupBuilder::<RestartNode>::new(1, chain)
            .with_node(move |_| RestartNode {
                checkpoints: node_checkpoints.clone(),
                activation,
                startup: node_startup.lock().unwrap().clone(),
            })
            .with_restartable_nodes()
            .with_backfill(|_| PbtBackfill::default())
            .with_node_builder_modifier(|builder| {
                builder.extend_rpc_modules(|ctx| {
                    ctx.modules.remove_method_from_configured("eth_getProof");
                    ctx.auth_module.remove_auth_method("eth_getProof");
                    Ok(())
                })
            })
            .build_single()
            .await?;
        let mut sender = Wallet::new(1).account(0);
        let recipient = Address::repeat_byte(0x77);
        let transaction = sender.transfer(recipient, U256::from(9)).await;
        let anchor = node.mine([transaction]).await?.ensure_success()?;
        assert!(anchor.block().timestamp < activation);
        let anchor_hash = anchor.block().hash();
        node.set_finality(Finality::Keep);
        let catchup = node.advance_blocks(3).await?;
        assert!(catchup[0].block().timestamp < activation);
        assert!(catchup[2].block().timestamp >= activation);
        let persisted_tip = catchup[2].block().hash();
        let finalized_hash = catchup[0].block().hash();
        let response = node
            .engine
            .forkchoice_updated(ForkchoiceState {
                head_block_hash: persisted_tip,
                safe_block_hash: finalized_hash,
                finalized_block_hash: finalized_hash,
            })
            .await?;
        assert!(response.payload_status.is_valid());
        let stopped = node.stop().await?;
        let state = store.load_block(anchor_hash)?;
        let artifacts = Artifacts::from_state(&state)?;
        let files = MigrationFiles {
            snapshot: directory.path().join("snapshot"),
            preimages: directory.path().join("preimages"),
            anchor: anchor_hash,
        };
        reth_fs_util::write(&files.snapshot, &artifacts.snapshot)?;
        reth_fs_util::write(&files.preimages, &artifacts.preimages)?;
        drop(store);
        // Remove the old shadow checkpoints: onboarding must reconstruct from the artifacts.
        reth_fs_util::remove_dir_all(checkpoints.join("states"))?;
        reth_fs_util::remove_dir_all(checkpoints.join("blocks"))?;
        startup.lock().unwrap().migration = Some(files);
        let mut node = stopped.start().await?;
        assert_eq!(node.current_forkchoice_state()?.head_block_hash, persisted_tip);
        node.set_finality(Finality::Keep);
        let canonical = node.advance_block().await?;
        let reopened = SnapshotStore::<Blake3Hasher>::open(&checkpoints, DecodeLimits::default())?;
        assert_eq!(reopened.load_block(anchor_hash)?.root(), state.root());
        assert_eq!(
            mpt_root(&reopened.load_block(catchup[0].block().hash())?),
            catchup[0].block().state_root
        );
        assert_eq!(reopened.load_block(persisted_tip)?.root(), catchup[2].block().state_root);
        assert_eq!(
            reopened.load_block(canonical.block().hash())?.root(),
            canonical.block().state_root
        );
        assert_eq!(
            reopened.load_block(canonical.block().hash())?.account(recipient).unwrap().balance,
            U256::from(9)
        );
        let fork = node.advance_fork(finalized_hash, 2).await?;
        node.reorg_to(canonical.block().hash()).await?;
        node.reorg_to(fork[1].block().hash()).await?;
        let tip = fork[1].block().hash();
        let stopped = node.stop().await?;
        startup.lock().unwrap().migration = None;
        drop(reopened);
        let mut node = stopped.start().await?;
        assert_eq!(node.current_forkchoice_state()?.head_block_hash, tip);
        let next = node.advance_block().await?;
        assert_eq!(
            SnapshotStore::<Blake3Hasher>::open(&checkpoints, DecodeLimits::default())?
                .load_block(next.block().hash())?
                .root(),
            next.block().state_root
        );
        node.stop().await?;
        Ok(())
    }

    #[test]
    fn startup_requires_paired_migration_files() {
        for (snapshot, preimages) in
            [(Some(OsString::from("snapshot")), None), (None, Some(OsString::from("preimages")))]
        {
            assert_eq!(
                Startup::new(snapshot, preimages, None, None, CatchupLimits::default())
                    .unwrap_err()
                    .to_string(),
                "PBT_MIGRATION_SNAPSHOT, PBT_MIGRATION_PREIMAGES and PBT_MIGRATION_ANCHOR must be set together"
            );
        }
        assert!(Startup::new(None, None, None, None, CatchupLimits::default())
            .unwrap()
            .migration
            .is_none());
        assert!(Startup::new(
            Some("snapshot".into()),
            Some("preimages".into()),
            Some(B256::ZERO.to_string().into()),
            None,
            CatchupLimits::default()
        )
        .unwrap()
        .migration
        .is_some());
        for (snapshot, preimages, anchor) in [
            (None, None, Some(B256::ZERO.to_string().into())),
            (Some("snapshot".into()), Some("preimages".into()), None),
        ] {
            assert_eq!(Startup::new(snapshot, preimages, anchor, None, CatchupLimits::default()).unwrap_err().to_string(),
                "PBT_MIGRATION_SNAPSHOT, PBT_MIGRATION_PREIMAGES and PBT_MIGRATION_ANCHOR must be set together");
        }
        assert!(Startup::new(
            Some("snapshot".into()),
            Some("preimages".into()),
            Some("bad-hash".into()),
            None,
            CatchupLimits::default()
        )
        .is_err());
    }

    #[tokio::test]
    async fn older_anchor_import_and_self_conversion_survive_post_fork_finality() -> eyre::Result<()>
    {
        let activation = 1_710_338_138;
        let directory = tempfile::tempdir()?;
        let checkpoints = directory.path().join("pbt");
        let store = Arc::new(SnapshotStore::open(&checkpoints, DecodeLimits::default())?);
        let chain = prepare_chain(
            (*test_chain_spec(EthereumHardfork::Amsterdam)).clone(),
            activation,
            &store,
        )?;
        let (mut node, _) = E2ETestSetupBuilder::<RestartNode>::new(1, chain)
            .with_node(move |_| RestartNode {
                checkpoints: checkpoints.clone(),
                activation,
                startup: Startup::default(),
            })
            .with_backfill(|_| PbtBackfill::default())
            .with_tree_config_modifier(|config| {
                config.with_persistence_threshold(0).with_memory_block_buffer_target(0)
            })
            .build_single()
            .await?;
        let anchor = node.advance_block().await?;
        let anchor_hash = anchor.block().hash();
        assert!(anchor.block().timestamp < activation);
        let artifacts = Artifacts::from_state(&store.load_block(anchor_hash)?)?;
        let files = MigrationFiles {
            snapshot: directory.path().join("migration.snapshot"),
            preimages: directory.path().join("migration.preimages"),
            anchor: anchor_hash,
        };
        reth_fs_util::write(&files.snapshot, &artifacts.snapshot)?;
        reth_fs_util::write(&files.preimages, &artifacts.preimages)?;
        let blocks = node.advance_blocks(3).await?;
        let tip = &blocks[2];
        assert!(tip.block().timestamp >= activation);
        node.wait_for_persisted_block(tip.block().number).await?;
        let strategy = PbtStateRootStrategy::new(store.clone(), activation);
        let startup = Startup { migration: Some(files.clone()), ..Default::default() };
        startup.initialize(&strategy, &node.inner.provider)?;
        let convert = Startup { self_convert: true, ..startup.clone() };
        reth_fs_util::remove_file(&files.snapshot)?;
        convert.initialize(&strategy, &node.inner.provider)?;
        assert_eq!(reth_fs_util::read(&files.snapshot)?, artifacts.snapshot);
        assert_eq!(store.load_block(tip.block().hash())?.root(), tip.block().state_root);
        let alias_directory = directory.path().join("alias");
        reth_fs_util::create_dir_all(&alias_directory)?;
        for snapshot in
            [files.preimages.clone(), alias_directory.join("..").join("migration.preimages")]
        {
            let alias = Startup {
                migration: Some(MigrationFiles { snapshot, ..files.clone() }),
                ..convert.clone()
            };
            assert_eq!(
                alias.initialize(&strategy, &node.inner.provider).unwrap_err().to_string(),
                "PBT snapshot output must differ from the preimage input"
            );
            assert_eq!(reth_fs_util::read(&files.preimages)?, artifacts.preimages);
        }
        let missing = Startup {
            migration: Some(MigrationFiles { anchor: B256::repeat_byte(0xff), ..files.clone() }),
            ..Default::default()
        };
        assert_eq!(
            missing.initialize(&strategy, &node.inner.provider).unwrap_err().to_string(),
            "missing PBT migration anchor header"
        );
        node.set_finality(Finality::Keep);
        let unfinalized = node.advance_block().await?;
        let future = Startup {
            migration: Some(MigrationFiles { anchor: unfinalized.block().hash(), ..files }),
            ..Default::default()
        };
        assert_eq!(
            future.initialize(&strategy, &node.inner.provider).unwrap_err().to_string(),
            "PBT migration anchor must be canonical and finalized"
        );
        Ok(())
    }

    #[test]
    fn checkpoint_directory_pins_genesis_and_activation() -> eyre::Result<()> {
        let directory = tempfile::tempdir()?;
        let store = SnapshotStore::open(directory.path(), DecodeLimits::default())?;
        let chain = (*test_chain_spec(EthereumHardfork::Cancun)).clone();
        prepare_chain(chain.clone(), 1, &store)?;
        prepare_chain(chain.clone(), 1, &store)?;
        assert_eq!(
            prepare_chain(chain.clone(), 2, &store).unwrap_err().to_string(),
            "PBT checkpoint directory belongs to a different genesis or activation timestamp"
        );
        let changed = chain.map_header(|mut header| {
            header.extra_data = Bytes::from_static(b"other");
            header
        });
        assert_eq!(
            prepare_chain(changed, 1, &store).unwrap_err().to_string(),
            "PBT checkpoint directory belongs to a different genesis or activation timestamp"
        );
        for bytes in [vec![0; CONFIGURATION_LENGTH - 1], vec![0; CONFIGURATION_LENGTH + 1]] {
            reth_fs_util::write(store.path().join("chain-config"), bytes)?;
            assert_eq!(
                prepare_chain((*test_chain_spec(EthereumHardfork::Cancun)).clone(), 1, &store)
                    .unwrap_err()
                    .to_string(),
                "invalid PBT checkpoint configuration length"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn finalized_pruning_only_removes_proven_ancestors() -> eyre::Result<()> {
        let (mut node, store, _directory) = setup(0).await?;
        node.set_finality(Finality::Keep);
        let genesis = node.block_hash(0);
        let sibling = node.advance_block().await?;
        let finalized = node.advance_fork(genesis, 1).await?;
        let finalized_hash = finalized[0].block().hash();
        let response = node
            .engine
            .forkchoice_updated(ForkchoiceState {
                head_block_hash: finalized_hash,
                safe_block_hash: finalized_hash,
                finalized_block_hash: finalized_hash,
            })
            .await?;
        assert!(response.payload_status.is_valid());
        let strategy = PbtStateRootStrategy::new(store.clone(), 0);
        let active_anchor = Startup {
            migration: Some(MigrationFiles {
                snapshot: "snapshot".into(),
                preimages: "preimages".into(),
                anchor: finalized_hash,
            }),
            ..Default::default()
        };
        assert_eq!(
            active_anchor.initialize(&strategy, &node.inner.provider).unwrap_err().to_string(),
            "PBT migration anchor must precede activation"
        );
        for (candidate, error) in [
            (finalized_hash, "cannot prune the finalized PBT checkpoint"),
            (
                sibling.block().hash(),
                "PBT prune candidates must be strict ancestors of the finalized block",
            ),
        ] {
            let root = store.load_block(candidate)?.root();
            let startup = Startup { obsolete_checkpoints: vec![candidate], ..Default::default() };
            assert_eq!(
                startup.initialize(&strategy, &node.inner.provider).unwrap_err().to_string(),
                error
            );
            assert_eq!(store.load_block(candidate)?.root(), root);
        }
        let canonical = store.load_block(finalized_hash)?;
        let mut forged = canonical.clone();
        forged.set_account(
            Address::repeat_byte(0x88),
            AccountData { balance: U256::ONE, ..Default::default() },
        )?;
        store.save_block(finalized_hash, &forged)?;
        let prune = Startup { obsolete_checkpoints: vec![genesis], ..Default::default() };
        assert_eq!(
            prune.initialize(&strategy, &node.inner.provider).unwrap_err().to_string(),
            "PBT finalized checkpoint reference does not match its header"
        );
        assert!(store.path().join("blocks").join(genesis.to_string()).try_exists()?);
        store.save_block(finalized_hash, &canonical)?;
        prune.initialize(&strategy, &node.inner.provider)?;
        assert!(!store.path().join("blocks").join(genesis.to_string()).try_exists()?);
        assert_eq!(store.load_block(finalized_hash)?.root(), finalized[0].block().state_root);
        assert_eq!(store.load_block(sibling.block().hash())?.root(), sibling.block().state_root);
        Ok(())
    }

    #[test]
    fn onboarding_and_pruning_require_local_finality() -> eyre::Result<()> {
        let directory = tempfile::tempdir()?;
        let store = Arc::new(SnapshotStore::open(directory.path(), DecodeLimits::default())?);
        let strategy = PbtStateRootStrategy::new(store, 1);
        let provider = NoopProvider::default();
        for startup in [
            Startup {
                migration: Some(MigrationFiles {
                    snapshot: "snapshot".into(),
                    preimages: "preimages".into(),
                    anchor: B256::ZERO,
                }),
                ..Default::default()
            },
            Startup { obsolete_checkpoints: vec![B256::repeat_byte(1)], ..Default::default() },
        ] {
            assert_eq!(
                startup.initialize(&strategy, &provider).unwrap_err().to_string(),
                "PBT onboarding or pruning requires a locally finalized block"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn checkpoint_proofs_are_exposed_only_on_authenticated_rpc() -> eyre::Result<()> {
        let (mut node, store, _directory) = setup(0).await?;
        let recipient = Address::repeat_byte(0x77);
        let mut sender = Wallet::new(1).account(0);
        let transaction = sender.transfer(recipient, U256::from(9)).await;
        let mined = node.mine([transaction]).await?.ensure_success()?;
        let state = store.load_block(mined.block().hash())?;
        let key = Bytes::copy_from_slice(&key::header_key::<Blake3Hasher>(
            recipient,
            key::BASIC_DATA_LEAF_KEY,
        ));
        let response: serde_json::Value = node
            .inner
            .auth_server_handle()
            .http_client()
            .request("pbt_getProof", rpc_params![mined.block().hash(), vec![key.clone()]])
            .await?;
        assert_eq!(response["version"], 1);
        assert_eq!(
            serde_json::from_value::<B256>(response["blockHash"].clone())?,
            mined.block().hash()
        );
        assert_eq!(
            serde_json::from_value::<B256>(response["pbtStateRoot"].clone())?,
            mined.block().state_root
        );
        assert_eq!(serde_json::from_value::<Bytes>(response["proofs"][0]["key"].clone())?, key);
        assert_eq!(
            serde_json::from_value::<B256>(response["proofs"][0]["value"].clone())?,
            state.account(recipient).unwrap().basic_data()?
        );
        let public = node.rpc_client().ok_or_else(|| eyre::eyre!("HTTP RPC is not enabled"))?;
        let error = public
            .request::<serde_json::Value, _>(
                "pbt_getProof",
                rpc_params![mined.block().hash(), vec![key]],
            )
            .await
            .unwrap_err();
        let jsonrpsee::core::client::Error::Call(error) = error else {
            eyre::bail!("public RPC returned a transport error instead of method-not-found");
        };
        assert_eq!(error.code(), jsonrpsee::types::error::METHOD_NOT_FOUND_CODE);
        assert_eq!(error.message(), "Method not found");
        assert!(error.data().is_none());
        Ok(())
    }

    #[test]
    fn directory_lock_excludes_concurrent_launches_and_releases_on_drop() -> eyre::Result<()> {
        let directory = tempfile::tempdir()?;
        let lock = lock_directory(directory.path())?;
        assert_eq!(
            lock_directory(directory.path()).unwrap_err().to_string(),
            "PBT data directory is already in use"
        );
        drop(lock);
        let reopened = lock_directory(directory.path())?;
        assert_eq!(
            lock_directory(directory.path()).unwrap_err().to_string(),
            "PBT data directory is already in use"
        );
        drop(reopened);
        Ok(())
    }

    #[test]
    fn populated_checkpoint_directory_requires_configuration_marker() -> eyre::Result<()> {
        let directory = tempfile::tempdir()?;
        let store = SnapshotStore::open(directory.path(), DecodeLimits::default())?;
        let chain = (*test_chain_spec(EthereumHardfork::Cancun)).clone();
        let prepared = prepare_chain(chain.clone(), 1, &store)?;
        let root = store.load_block(prepared.genesis_hash())?.root();
        reth_fs_util::remove_file(store.path().join("chain-config"))?;
        assert_eq!(
            prepare_chain(chain, 2, &store).unwrap_err().to_string(),
            "PBT checkpoint directory is populated but chain-config is missing"
        );
        assert!(!store.path().join("chain-config").try_exists()?);
        assert_eq!(store.load_block(prepared.genesis_hash())?.root(), root);
        Ok(())
    }

    fn catchup_provider(
        anchor: &SealedHeader,
        tip: &SealedHeader,
        bytes: Option<Bytes>,
    ) -> eyre::Result<MockEthProvider> {
        let mut provider = MockEthProvider::new();
        provider.bal_store = BalStoreHandle::new(InMemoryBalStore::default());
        provider.add_header(anchor.hash(), anchor.clone().into_header());
        provider.add_header(tip.hash(), tip.clone().into_header());
        if let Some(bytes) = bytes {
            provider.bal_store().insert(tip.num_hash(), RawBal::new(bytes))?;
        }
        Ok(provider)
    }

    #[tokio::test]
    async fn migration_download_authenticates_and_budgets_missing_bals() -> eyre::Result<()> {
        let (mut source, _store, _directory) = setup_for(0, EthereumHardfork::Amsterdam).await?;
        let anchor = source.inner.provider.sealed_header(0)?.unwrap();
        let mut sender = Wallet::new(1).account(0);
        let first = source
            .mine([sender.transfer(Address::repeat_byte(0x77), U256::ONE).await])
            .await?
            .ensure_success()?;
        let tip = source.inner.provider.sealed_header_by_hash(first.block().hash())?.unwrap();
        let bytes = first.payload.block_access_list().unwrap().clone();
        let provider = catchup_provider(&anchor, &tip, None)?;
        let client = reth_network_p2p::test_utils::TestFullBlockClient::default();
        let startup = Startup {
            migration: Some(MigrationFiles {
                snapshot: "unused".into(),
                preimages: "unused".into(),
                anchor: anchor.hash(),
            }),
            ..Default::default()
        };
        assert_eq!(
            startup.fetch_missing_bals(&provider, &client).await.unwrap_err().to_string(),
            "PBT catch-up BAL could not be fetched after 3 attempts"
        );
        client.insert_access_list(tip.hash(), Bytes::from_static(&[0xc0, 0xc0]));
        assert_eq!(
            startup.fetch_missing_bals(&provider, &client).await.unwrap_err().to_string(),
            "PBT catch-up BAL could not be fetched after 3 attempts"
        );
        assert!(provider.bal_store().get_by_hash(tip.hash())?.is_none());
        client.insert_access_list(tip.hash(), bytes.clone());
        let limited = Startup {
            catchup: CatchupLimits {
                max_bal_bytes: NonZeroUsize::new(bytes.len() - 1).unwrap(),
                ..Default::default()
            },
            ..startup.clone()
        };
        assert_eq!(
            limited.fetch_missing_bals(&provider, &client).await.unwrap_err().to_string(),
            "PBT catch-up BAL byte budget exceeded"
        );
        assert!(provider.bal_store().get_by_hash(tip.hash())?.is_none());
        startup.fetch_missing_bals(&provider, &client).await?;
        assert_eq!(provider.bal_store().get_by_hash(tip.hash())?, Some(bytes));
        startup.fetch_missing_bals(&provider, &client).await?;
        Ok(())
    }

    #[tokio::test]
    async fn local_catchup_rejects_missing_tampered_and_over_budget_inputs() -> eyre::Result<()> {
        let (mut source, source_store, _source_directory) =
            setup_for(0, EthereumHardfork::Amsterdam).await?;
        let anchor = source.inner.provider.sealed_header(0)?.unwrap();
        let mut sender = Wallet::new(1).account(0);
        let recipient = Address::repeat_byte(0x77);
        let transaction = sender.transfer(recipient, U256::from(9)).await;
        let first = source.mine([transaction]).await?.ensure_success()?;
        let tip = source.inner.provider.sealed_header_by_hash(first.block().hash())?.unwrap();
        let bytes = first.payload.block_access_list().unwrap().clone();
        let directory = tempfile::tempdir()?;
        let store = Arc::new(SnapshotStore::open(directory.path(), DecodeLimits::default())?);
        store.save_block(anchor.hash(), &source_store.load_block(anchor.hash())?)?;
        let strategy = PbtStateRootStrategy::new(store.clone(), 0);
        let startup = Startup::default();
        let missing_header = catchup_provider(&anchor, &tip, Some(bytes.clone()))?;
        assert!(missing_header.headers.lock().remove(&anchor.hash()).is_some());
        assert_eq!(
            startup.catch_up(&strategy, &missing_header, &anchor, &tip).unwrap_err().to_string(),
            "PBT catch-up header is unavailable"
        );
        let missing_bal = catchup_provider(&anchor, &tip, None)?;
        assert_eq!(
            startup.catch_up(&strategy, &missing_bal, &anchor, &tip).unwrap_err().to_string(),
            "PBT catch-up BAL is unavailable"
        );
        let short_budget = Startup {
            catchup: CatchupLimits {
                max_bal_bytes: NonZeroUsize::new(bytes.len() - 1).unwrap(),
                ..Default::default()
            },
            ..Default::default()
        };
        let valid = catchup_provider(&anchor, &tip, Some(bytes.clone()))?;
        assert_eq!(
            short_budget.catch_up(&strategy, &valid, &anchor, &tip).unwrap_err().to_string(),
            "PBT catch-up BAL byte budget exceeded"
        );
        let corrupted = Bytes::from_static(&[0xc0]);
        let tampered = catchup_provider(&anchor, &tip, Some(corrupted.clone()))?;
        let error = startup.catch_up(&strategy, &tampered, &anchor, &tip).unwrap_err();
        let error = error.downcast_ref::<alloy_eip7928::BlockAccessListHashMismatch>().unwrap();
        assert_eq!(error.computed, alloy_primitives::keccak256(corrupted));
        assert_eq!(error.expected, tip.block_access_list_hash().unwrap());
        let mut bad_root = tip.clone().into_header();
        bad_root.state_root = B256::repeat_byte(0xff);
        let bad_root = SealedHeader::seal_slow(bad_root);
        let invalid = catchup_provider(&anchor, &bad_root, Some(bytes.clone()))?;
        assert_eq!(
            startup.catch_up(&strategy, &invalid, &anchor, &bad_root).unwrap_err().to_string(),
            "PBT catch-up state root does not match its header"
        );
        assert!(!store.path().join("blocks").join(bad_root.hash().to_string()).try_exists()?);
        let mut no_commitment = tip.clone().into_header();
        no_commitment.block_access_list_hash = None;
        let no_commitment = SealedHeader::seal_slow(no_commitment);
        let invalid = catchup_provider(&anchor, &no_commitment, None)?;
        assert_eq!(
            startup.catch_up(&strategy, &invalid, &anchor, &no_commitment).unwrap_err().to_string(),
            "PBT catch-up header has no BAL commitment"
        );
        let next = source.advance_block().await?;
        let next = source.inner.provider.sealed_header_by_hash(next.block().hash())?.unwrap();
        valid.add_header(next.hash(), next.clone().into_header());
        let short_chain = Startup {
            catchup: CatchupLimits {
                max_blocks: NonZeroUsize::new(1).unwrap(),
                ..Default::default()
            },
            ..Default::default()
        };
        assert_eq!(
            short_chain.catch_up(&strategy, &valid, &anchor, &next).unwrap_err().to_string(),
            "PBT catch-up block budget exceeded"
        );
        assert!(!store.path().join("blocks").join(tip.hash().to_string()).try_exists()?);
        startup.catch_up(&strategy, &valid, &anchor, &tip)?;
        assert_eq!(store.load_block(tip.hash())?.root(), tip.state_root());
        assert_eq!(
            store.load_block(tip.hash())?.account(recipient).unwrap().balance,
            U256::from(9)
        );
        Ok(())
    }

    #[test]
    fn catchup_budgets_are_nonzero_operator_limits() {
        let defaults = CatchupLimits::new(None, None).unwrap();
        assert_eq!(defaults.max_blocks.get(), 1024);
        assert_eq!(defaults.max_bal_bytes.get(), 64 * 1024 * 1024);
        let configured = CatchupLimits::new(Some("3".into()), Some("4096".into())).unwrap();
        assert_eq!(configured.max_blocks.get(), 3);
        assert_eq!(configured.max_bal_bytes.get(), 4096);
        for (blocks, bytes) in [(Some("0".into()), None), (None, Some("0".into()))] {
            let error = CatchupLimits::new(blocks, bytes).unwrap_err();
            assert_eq!(
                error.downcast_ref::<std::num::ParseIntError>().unwrap().kind(),
                &std::num::IntErrorKind::Zero
            );
        }
    }
}
