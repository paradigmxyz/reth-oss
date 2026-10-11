//! Experimental extension of the existing launcher through public builders.

use clap::{parser::ValueSource, ArgMatches, CommandFactory, FromArgMatches};
use reth_chainspec::{ChainSpec, EthChainSpec, EthereumHardfork, ForkCondition, HOODI, MAINNET};
use reth_cli_commands::launcher::FnLauncher;
use reth_embedded_beacon_config::{ChainConfig, DevnetConfig, EmbeddedProfile};
use reth_embedded_beacon_execution_proxy::{ExecutionProxy, RethTypedEngine};
use reth_embedded_beacon_node::Readiness;
use reth_embedded_beacon_primitives::{root_from_hex, sha256, Checkpoint, Fork};
use reth_ethereum_cli::{chainspec::EthereumChainSpecParser, interface::Cli};
use reth_node_builder::{rpc::BasicEngineApiBuilder, EngineApiExt};
use reth_node_ethereum::{
    node::{EthereumAddOns, EthereumEngineValidatorBuilder},
    EthereumNode,
};
use std::{
    io::IsTerminal,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::Duration,
};

mod args;
mod pool;
mod presentation;
mod public_network;
mod shutdown;
mod startup;
mod telemetry;
mod terminal;

use args::{BeaconArgs, SyncMode};

fn parse_checkpoint(value: &str) -> Result<Checkpoint, String> {
    let (epoch, root) =
        value.split_once(':').ok_or_else(|| "checkpoint must be EPOCH:0xROOT".to_owned())?;
    let epoch = epoch.parse().map_err(|_| "invalid checkpoint epoch".to_owned())?;
    let root = root_from_hex(root).map_err(|err| err.to_string())?;
    Ok(Checkpoint { epoch, root })
}

/// Runs the separate executable; consensus admission precedes execution DB initialization.
pub(super) fn run() -> eyre::Result<()> {
    reth_primitives_traits::ensure_no_account_extensions()?;
    let matches = Cli::<EthereumChainSpecParser, BeaconArgs>::command().get_matches();
    let mut cli = Cli::<EthereumChainSpecParser, BeaconArgs>::from_arg_matches(&matches)?;
    let output = presentation::OutputOptions::resolve(
        &cli.logs.color.to_string(),
        &cli.logs.log_stdout_format.to_string(),
        std::io::stdout().is_terminal(),
        std::env::var("RUST_LOG_STYLE").ok().as_deref(),
    )
    .with_quiet(matches.get_flag("quiet"));
    let Some(node) = cli.as_node_command_mut() else {
        return cli.run(async move |builder, _| {
            builder
                .node(EthereumNode::default())
                .launch_with_debug_capabilities()
                .await?
                .wait_for_node_exit()
                .await
        });
    };
    if node.ext.enabled {
        let mut profile = EmbeddedProfile::resolve(node.pruning.minimal, node.ext.cache_mib)?
            .with_state_cache_mib(node.ext.state_cache_mib)?;
        let args = matches
            .subcommand_matches("node")
            .ok_or_else(|| eyre::eyre!("missing node command arguments"))?;
        if profile.minimal {
            profile.rpc_block_cache_bytes = Some(apply_cache_default(
                &mut node.rpc.rpc_state_cache.max_blocks_bytes,
                profile.rpc_block_cache_bytes,
                args,
                "max_blocks_bytes",
            ));
            profile.rpc_receipt_cache_bytes = Some(apply_cache_default(
                &mut node.rpc.rpc_state_cache.max_receipts_bytes,
                profile.rpc_receipt_cache_bytes,
                args,
                "max_receipts_bytes",
            ));
            profile.rpc_bal_cache_bytes = Some(apply_cache_default(
                &mut node.rpc.rpc_state_cache.max_bals_bytes,
                profile.rpc_bal_cache_bytes,
                args,
                "max_bals_bytes",
            ));
        }
        node.ext.effective_profile = Some(profile);
    }
    if node.ext.profile_status {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "profile": node.ext.effective_profile,
                "readiness": Readiness::current(),
                "state_diff_persistence_enabled": false,
            }))?
        );
        return Ok(());
    }
    if node.ext.status {
        println!("{}", serde_json::to_string_pretty(&Readiness::current())?);
        return Ok(());
    }
    let consensus_config = if node.ext.enabled {
        if !node.datadir.datadir.is_some() {
            eyre::bail!("embedded beacon requires an explicit isolated --datadir");
        }
        if node.dev.dev ||
            node.debug.etherscan.is_some() ||
            node.debug.rpc_consensus_url.is_some() ||
            node.debug.tip.is_some() ||
            node.debug.skip_fcu.is_some() ||
            node.debug.skip_new_payload.is_some() ||
            node.debug.skip_state_root ||
            node.debug.skip_genesis_validation ||
            node.debug.reorg_frequency.is_some() ||
            node.rpc.testing_skip_invalid_transactions
        {
            eyre::bail!("embedded beacon conflicts with mining, synthetic consensus and validation bypass options");
        }
        let devnet = node
            .ext
            .devnet_config
            .as_ref()
            .map(|path| {
                let config = DevnetConfig::from_reader(reth_fs_util::open(path)?)?;
                validate_devnet_execution(&config, &node.chain)?;
                Ok::<_, eyre::Report>(config)
            })
            .transpose()?;
        let config = if let Some(devnet) = &devnet {
            devnet.consensus.clone()
        } else {
            consensus_config_for_execution(&node.chain)?
        };
        if devnet.is_none() {
            node.chain = public_network::for_consensus(&node.chain, &config)?;
        }
        validate_args(&node.ext)?;
        startup::validate_topology(&node.ext, devnet.as_ref())?;
        require_sync_launch(&node.ext, &config)?;
        node.rpc.disable_auth_server = true;
        Some((config, devnet))
    } else {
        None
    };
    let embedded_enabled = consensus_config.is_some();
    let mut app = cli.configure();
    if embedded_enabled {
        app.access_tracing_layers()?.set_stdout_bright_cyan_target("beacon");
    }
    app.run(FnLauncher::new::<EthereumChainSpecParser, BeaconArgs>(async move |builder, args| {
        if !args.enabled {
            return builder
                .node(EthereumNode::default())
                .launch_with_debug_capabilities()
                .await?
                .wait_for_node_exit()
                .await;
        }
        if args.experimental_sync {
            tracing::warn!(target: "beacon::node",
                "[beacon] Attempting experimental Hoodi checkpoint sync; independent runtime acceptance remains unverified");
        }
        tracing::info!(target: "beacon::node", "[beacon] Starting isolated embedded beacon execution integration");
        tracing::debug!(target: "beacon::node", sync_mode = ?args.sync_mode, port = args.port, cache_mib = ?args.cache_mib, "[beacon] Embedded startup configuration");
        let (config, devnet) = consensus_config
            .ok_or_else(|| eyre::eyre!("missing consensus network configuration"))?;
        tracing::info!(target: "beacon::node", network = %config.name, chain_id = config.chain_id, target_fork = "gloas", "[beacon] Validated embedded consensus network");
        // Private identity includes the explicit terminal selection in addition to the fork
        // schedule.
        let config_digest = sha256(&if let Some(devnet) = &devnet {
            serde_json::to_vec(devnet)?
        } else {
            serde_json::to_vec(&config)?
        });
        let profile = args.effective_profile.clone()
            .ok_or_else(|| eyre::eyre!("missing checked embedded resource profile"))?;
        tracing::info!(target: "beacon::storage", minimal = profile.minimal,
            shared_cache_bytes = profile.shared_cache_bytes,
            state_cache_bytes = profile.state_cache_bytes,
            independent_retention_owners = profile.separate_el_cl_pins,
            "[beacon] Configured shared immutable storage");
        let cache_bytes = profile.shared_cache_bytes;
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let addons = EthereumAddOns::default().with_engine_api(EngineApiExt::new(
            BasicEngineApiBuilder::<EthereumEngineValidatorBuilder>::default(),
            move |api| {
                // Static positive deadline; typed API validates versions and updates custody.
                let proxy = ExecutionProxy::new(RethTypedEngine::new(api), Duration::from_secs(30))
                    .expect("static positive deadline");
                let _ = sender.send(proxy);
            },
        ));
        let builder = builder
            .with_types::<EthereumNode>()
            .with_components(EthereumNode::components().pool(pool::SharedPoolBuilder {
                genesis_root: config.genesis_validators_root,
                config_digest,
                cache_bytes,
            }))
            .with_add_ons(addons);
        let launcher = builder.engine_api_launcher().with_in_process_consensus();
        let handle = builder.launch_with(launcher).await?;
        telemetry::install(&handle.node);
        let execution_proxy =
            receiver.await.map_err(|_| eyre::eyre!("typed engine construction failed"))?;
        tracing::info!(target: "beacon::execution", "[beacon] Connected typed execution proxy");
        let shared = handle.node.pool.blob_store().shared().clone();
        let directories = handle.node.config.datadir();
        let resources = telemetry::ResourceCollector::new(telemetry::StoragePaths {
            database: directories.db().join("mdbx.dat"),
            static_files: directories.static_files(),
            shared_data: shared.namespace_root(),
            volume: directories.data_dir().to_path_buf(),
        });
        handle.node.task_executor.spawn_task(telemetry::observe_resources(resources));
        handle.node.task_executor.spawn_task(telemetry::observe_shared(shared.clone()));
        handle.node.task_executor.spawn_task(telemetry::observe_transport(execution_proxy.clone()));
        let provider = handle.node.provider.clone();
        let engine_config = config.clone();
        let engine_devnet = devnet.clone();
        let engine = tokio::task::spawn_blocking(move || {
            let terminal = reth_embedded_beacon_node::terminal_verification::TerminalVerifier::capture(
                terminal::TerminalProvider(provider),
                engine_config.clone(),
                engine_devnet.as_ref(),
                Default::default(),
            )?;
            Ok::<_, eyre::Report>(
                reth_embedded_beacon_node::runtime_engine::RuntimeEngineAdapter::with_terminal_verifier(
                    execution_proxy, engine_config, terminal,
                )?,
            )
        }).await??;
        let api_config = args.http.then(|| (
            SocketAddr::new(args.http_addr.unwrap_or(IpAddr::V4(Ipv4Addr::LOCALHOST)), args.http_port),
            config.clone(), devnet.clone(),
        ));
        let fork_banner = presentation::ForkBanner::new(output);
        let execution_exit = handle.wait_for_node_exit();
        tokio::pin!(execution_exit);
        let (live, cancellation) = tokio::select! {
            startup = Box::pin(startup::prepare(args, config, devnet, shared, engine, output)) => startup?,
            result = &mut execution_exit => return result,
        };
        let owner = live.observation_handle();
        let peers = live.peer_observation_handle();
        let api = if let Some((address, api_config, private)) = api_config {
            Some(reth_embedded_beacon_node::beacon_api::BeaconApi::bind(
                address, api_config, owner.clone(), peers.clone(), private,
            ).await?)
        } else { None };
        let api_enabled = api.is_some();
        let (stop_api, api_stopped) = tokio::sync::oneshot::channel();
        let api = async move {
            if let Some(api) = api {
                api.run(async move { let _ = api_stopped.await; }).await.map_err(eyre::Report::new)
            } else {
                std::future::pending::<eyre::Result<()>>().await
            }
        };
        let observer = telemetry::observe_consensus(owner, peers, fork_banner);
        // The concrete live driver has a large state machine; poll it from the heap.
        let consensus = Box::pin(live.run());
        tokio::pin!(api, observer, consensus);
        let (result, consensus_done, api_done) = tokio::select! {
            result = &mut consensus => (result.map_err(eyre::Report::new), true, false),
            result = &mut execution_exit => (result, false, false),
            result = &mut api => (result, false, true),
            _ = &mut observer => (Err(eyre::eyre!("consensus observer exited unexpectedly")), false, false),
        };
        cancellation.cancel();
        let _ = stop_api.send(());
        let consensus_cleanup =
            (!consensus_done).then_some(async { (&mut consensus).await.map_err(eyre::Report::new) });
        let api_cleanup = (api_enabled && !api_done).then_some(&mut api);
        shutdown::finish(result, consensus_cleanup, api_cleanup, Duration::from_secs(5)).await
    }))
}

fn consensus_config_for_execution(execution: &ChainSpec) -> eyre::Result<ChainConfig> {
    let (canonical, config) = match execution.chain().id() {
        1 => (MAINNET.clone(), ChainConfig::mainnet()),
        560048 => (HOODI.clone(), ChainConfig::hoodi()),
        chain_id => eyre::bail!(
            "embedded beacon supports execution chain IDs 1 and 560048; received {chain_id}"
        ),
    };
    // A chain ID alone does not identify a network: custom specifications can reuse it.
    // Match every consensus-relevant execution setting before selecting consensus identity.
    if execution.genesis_hash() != canonical.genesis_hash() ||
        execution.genesis != canonical.genesis ||
        execution.genesis_header != canonical.genesis_header ||
        execution.hardforks != canonical.hardforks ||
        execution.paris_block_and_final_difficulty != canonical.paris_block_and_final_difficulty ||
        execution.deposit_contract != canonical.deposit_contract ||
        execution.base_fee_params != canonical.base_fee_params ||
        execution.blob_params != canonical.blob_params
    {
        eyre::bail!("execution chain specification does not match canonical {}", config.name);
    }
    config.validate()?;
    Ok(config)
}
fn validate_devnet_execution(config: &DevnetConfig, execution: &ChainSpec) -> eyre::Result<()> {
    config.validate_execution_identity(execution.chain().id(), execution.genesis_hash().0)?;
    if execution.genesis.config.chain_id != execution.chain().id() ||
        execution.genesis.timestamp > config.consensus.genesis_time ||
        execution
            .genesis
            .config
            .terminal_total_difficulty
            .is_none_or(|difficulty| !difficulty.is_zero())
    {
        eyre::bail!("private beacon requires an execution genesis before consensus genesis and terminal difficulty zero");
    }
    // These pairings are the explicit local Gloas execution schedule, not public activation
    // metadata.
    for (consensus_fork, execution_fork) in [
        (Fork::Capella, EthereumHardfork::Shanghai),
        (Fork::Deneb, EthereumHardfork::Cancun),
        (Fork::Electra, EthereumHardfork::Prague),
        (Fork::Fulu, EthereumHardfork::Osaka),
        (Fork::Gloas, EthereumHardfork::Amsterdam),
    ] {
        let activation = config
            .consensus
            .forks
            .activations()
            .iter()
            .find(|activation| activation.fork == consensus_fork)
            .ok_or_else(|| eyre::eyre!("missing private consensus fork"))?;
        let timestamp = config
            .consensus
            .genesis_time
            .checked_add(
                activation
                    .epoch
                    .checked_mul(config.consensus.slots_per_epoch)
                    .and_then(|slot| slot.checked_mul(config.consensus.seconds_per_slot))
                    .ok_or_else(|| eyre::eyre!("private execution activation overflow"))?,
            )
            .ok_or_else(|| eyre::eyre!("private execution activation overflow"))?;
        let condition = execution.hardforks.fork(execution_fork);
        let matches = if activation.epoch == 0 {
            matches!(condition, ForkCondition::Timestamp(time) if time <= execution.genesis.timestamp)
        } else {
            condition == ForkCondition::Timestamp(timestamp)
        };
        if !matches {
            eyre::bail!("private execution fork {execution_fork:?} does not match consensus {consensus_fork:?} activation");
        }
    }
    Ok(())
}

fn validate_args(args: &BeaconArgs) -> eyre::Result<()> {
    if args.port == 0 || args.cache_mib == Some(0) || (args.http && args.http_port == 0) {
        eyre::bail!("beacon port and shared cache size must be positive");
    }
    match args.sync_mode {
        Some(SyncMode::Genesis) => {
            if args.genesis.is_none() || (args.devnet_config.is_none() && args.archive.is_none()) {
                eyre::bail!("genesis sync requires --beacon.genesis and a public history archive; only an explicit verified devnet may omit --beacon.archive");
            }
            if args.checkpoint_url.is_some() {
                eyre::bail!("checkpoint URL conflicts with genesis mode");
            }
        }
        Some(SyncMode::Checkpoint) => {
            if args.checkpoint_url.is_none() || args.trusted_checkpoint.is_none() {
                eyre::bail!("checkpoint sync requires source URL and explicit trusted checkpoint");
            }
        }
        None => eyre::bail!("fresh embedded sync requires --beacon.sync-mode"),
    }
    Ok(())
}

/// Apply profile defaults only when the user did not explicitly select the native cache flag.
fn apply_cache_default(
    value: &mut usize,
    ceiling: Option<usize>,
    args: &ArgMatches,
    id: &str,
) -> usize {
    if let Some(ceiling) = ceiling &&
        !matches!(
            args.value_source(id),
            Some(ValueSource::CommandLine | ValueSource::EnvVariable)
        )
    {
        *value = (*value).min(ceiling);
    }
    *value
}

/// Admit the first public experiment without changing compiled acceptance evidence.
///
/// Full bootstrap, signature, state, availability and execution validation still run through
/// the same concrete startup owners. The explicit exception is limited to Hoodi checkpoints.
fn require_sync_launch(args: &BeaconArgs, config: &ChainConfig) -> eyre::Result<()> {
    if !args.experimental_sync {
        Readiness::current().require_live_sync()?;
        return Ok(());
    }
    if !args.enabled ||
        args.devnet_config.is_some() ||
        !matches!(args.sync_mode, Some(SyncMode::Checkpoint)) ||
        config != &ChainConfig::hoodi()
    {
        eyre::bail!("--beacon.experimental-sync requires canonical Hoodi checkpoint sync");
    }
    Ok(())
}

#[cfg(test)]
mod tests;
