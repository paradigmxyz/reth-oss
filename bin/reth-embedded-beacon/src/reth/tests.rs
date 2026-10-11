//! CLI and network identity regression controls.

use super::*;
use clap::{error::ErrorKind, CommandFactory, Parser};
use reth_embedded_beacon_config::{ForkActivation, ForkSchedule};

fn private_profile(all_at_genesis: bool) -> (DevnetConfig, ChainSpec) {
    let mut execution = MAINNET.as_ref().clone();
    execution.chain = 3151908u64.into();
    execution.genesis.config.chain_id = 3151908;
    execution.genesis.config.terminal_total_difficulty = Some(Default::default());
    let mut consensus = ChainConfig::mainnet();
    consensus.name = "embedded-devnet".to_owned();
    consensus.chain_id = 3151908;
    consensus.genesis_time = execution.genesis.timestamp;
    consensus.genesis_validators_root = [2; 32];
    consensus.seconds_per_slot = 6;
    consensus.blob_schedule.clear();
    consensus.forks = ForkSchedule::new(
        [
            Fork::Phase0,
            Fork::Altair,
            Fork::Bellatrix,
            Fork::Capella,
            Fork::Deneb,
            Fork::Electra,
            Fork::Fulu,
            Fork::Gloas,
        ]
        .into_iter()
        .enumerate()
        .map(|(index, fork)| ForkActivation {
            fork,
            epoch: if all_at_genesis { 0 } else { index as u64 },
            version: [index as u8, 0, 0, 42],
        })
        .collect(),
    )
    .unwrap();
    for (index, fork) in [
        EthereumHardfork::Shanghai,
        EthereumHardfork::Cancun,
        EthereumHardfork::Prague,
        EthereumHardfork::Osaka,
        EthereumHardfork::Amsterdam,
    ]
    .into_iter()
    .enumerate()
    {
        let timestamp =
            if all_at_genesis { 0 } else { consensus.genesis_time + (index as u64 + 3) * 32 * 6 };
        execution.hardforks.insert(fork, ForkCondition::Timestamp(timestamp));
    }
    let root = execution.genesis_hash().0;
    (
        DevnetConfig {
            schema_version: 1,
            execution_genesis_hash: root,
            terminal_block_hash: root,
            terminal_block_hash_activation_epoch: if all_at_genesis { 0 } else { 2 },
            consensus,
        },
        execution,
    )
}

#[test]
fn private_network_requires_matching_execution_schedule_and_identity() {
    for all_at_genesis in [false, true] {
        let (config, execution) = private_profile(all_at_genesis);
        validate_devnet_execution(&config, &execution).unwrap();
        let mut changed = execution.clone();
        changed.hardforks.insert(EthereumHardfork::Amsterdam, ForkCondition::Timestamp(u64::MAX));
        assert_eq!(
            validate_devnet_execution(&config, &changed).unwrap_err().to_string(),
            "private execution fork Amsterdam does not match consensus Gloas activation"
        );
        let mut wrong_root = config.clone();
        wrong_root.execution_genesis_hash = [3; 32];
        wrong_root.terminal_block_hash = [3; 32];
        assert_eq!(
            validate_devnet_execution(&wrong_root, &execution).unwrap_err().to_string(),
            "private beacon configuration does not match execution chain ID and genesis hash"
        );
        let mut nonterminal = execution.clone();
        nonterminal.genesis.config.terminal_total_difficulty = Some("0x1".parse().unwrap());
        assert!(validate_devnet_execution(&config, &nonterminal).is_err());
    }
}

#[test]
fn genesis_archive_is_optional_only_for_explicit_checked_private_configuration() {
    let mut args = BeaconArgs {
        enabled: true,
        sync_mode: Some(SyncMode::Genesis),
        genesis: Some("genesis.ssz".into()),
        port: 9000,
        cache_mib: Some(256),
        ..Default::default()
    };
    assert!(validate_args(&args).is_err());
    args.devnet_config = Some("embedded-beacon.json".into());
    validate_args(&args).unwrap();
    // This is only argument admission; the independently checked config precedes it in run().
    assert!(Readiness::current().require_live_sync().is_err());
    args.genesis = None;
    assert!(validate_args(&args).is_err());
}

#[test]
fn private_config_flag_requires_explicit_embedded_mode() {
    let error = Cli::<EthereumChainSpecParser, BeaconArgs>::try_parse_from([
        "embedded-beacon",
        "node",
        "--beacon.devnet-config",
        "embedded-beacon.json",
    ])
    .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::MissingRequiredArgument);
    let mut cli = Cli::<EthereumChainSpecParser, BeaconArgs>::try_parse_from([
        "embedded-beacon",
        "node",
        "--embedded-beacon",
        "--beacon.devnet-config",
        "embedded-beacon.json",
        "--beacon.sync-mode",
        "genesis",
        "--beacon.genesis",
        "genesis.ssz",
    ])
    .unwrap();
    let node = cli.as_node_command_mut().unwrap();
    assert_eq!(
        node.ext.devnet_config.as_deref(),
        Some(std::path::Path::new("embedded-beacon.json"))
    );
    validate_args(&node.ext).unwrap();
}
#[test]
fn beacon_topology_requires_checked_private_identity_and_separate_udp_ports() {
    let (devnet, _) = private_profile(false);
    let mut args = BeaconArgs {
        enabled: true,
        listen_ip: Some("172.18.0.3".parse().unwrap()),
        bootnodes_file: Some("bootnodes.txt".into()),
        port: 9000,
        checkpoint_max_age_epochs: 2048,
        max_replay_slots: 8192,
        ..Default::default()
    };
    assert!(startup::validate_topology(&args, None).is_err());
    let sockets = startup::validate_topology(&args, Some(&devnet)).unwrap();
    assert_eq!(sockets.discovery_port, 9000);
    assert_eq!(sockets.tcp_port, 9000);
    assert_eq!(sockets.quic_port, 9001);
    assert_eq!(sockets.policy, reth_embedded_beacon_network::EndpointPolicy::PrivateNetwork);
    args.quic_port = Some(9000);
    assert!(startup::validate_topology(&args, Some(&devnet)).is_err());
    args.port = u16::MAX;
    args.quic_port = None;
    assert!(startup::validate_topology(&args, Some(&devnet)).is_err());
    args.port = 9000;
    args.listen_ip = Some("0.0.0.0".parse().unwrap());
    assert!(startup::validate_topology(&args, Some(&devnet)).is_err());
    args.listen_ip = Some("8.8.8.8".parse().unwrap());
    assert!(startup::validate_topology(&args, None).is_ok());
    assert!(startup::validate_topology(&args, Some(&devnet)).is_err());
    args.bind_ip = Some("0.0.0.0".parse().unwrap());
    let translated = startup::validate_topology(&args, None).unwrap();
    assert_eq!(translated.ip, "8.8.8.8".parse::<IpAddr>().unwrap());
    assert_eq!(translated.bind_ip, args.bind_ip);
    args.bind_ip = Some("::".parse().unwrap());
    assert!(startup::validate_topology(&args, None).is_err());
    args.bind_ip = None;
    let mut altered = devnet;
    altered.consensus.chain_id = 1;
    args.listen_ip = Some("127.0.0.1".parse().unwrap());
    assert!(startup::validate_topology(&args, Some(&altered)).is_err());
}

#[test]
fn execution_network_selection_checks_canonical_consensus_settings() {
    assert_eq!(consensus_config_for_execution(&MAINNET).unwrap(), ChainConfig::mainnet());
    assert_eq!(consensus_config_for_execution(&HOODI).unwrap(), ChainConfig::hoodi());

    let mut changed_schedule = MAINNET.as_ref().clone();
    changed_schedule.hardforks.insert(EthereumHardfork::Shanghai, ForkCondition::Timestamp(1));
    assert_eq!(changed_schedule.genesis_hash(), MAINNET.genesis_hash());
    assert_eq!(
        consensus_config_for_execution(&changed_schedule).unwrap_err().to_string(),
        "execution chain specification does not match canonical mainnet"
    );

    let mut changed_genesis = HOODI.as_ref().clone();
    changed_genesis.genesis.timestamp += 1;
    assert_eq!(changed_genesis.genesis_hash(), HOODI.genesis_hash());
    assert_eq!(
        consensus_config_for_execution(&changed_genesis).unwrap_err().to_string(),
        "execution chain specification does not match canonical hoodi"
    );

    let mut unknown_chain = MAINNET.as_ref().clone();
    unknown_chain.chain = 42u64.into();
    assert_eq!(
        consensus_config_for_execution(&unknown_chain).unwrap_err().to_string(),
        "embedded beacon supports execution chain IDs 1 and 560048; received 42"
    );
}
#[test]
fn checkpoint_arguments_are_exact() {
    let value = format!("2:0x{}", "03".repeat(32));
    assert_eq!(parse_checkpoint(&value).unwrap(), Checkpoint { epoch: 2, root: [3; 32] });
    assert!(parse_checkpoint("2:0x03").is_err());
    assert!(parse_checkpoint("bad").is_err());
}

#[test]
fn extension_does_not_enable_consensus_by_default() {
    let cli = Cli::<EthereumChainSpecParser, BeaconArgs>::try_parse_from([
        "reth-embedded-beacon",
        "node",
    ])
    .unwrap();
    let mut cli = cli;
    assert!(!cli.as_node_command_mut().unwrap().ext.enabled);
}

#[test]
fn flattened_command_ids_are_unique() {
    Cli::<EthereumChainSpecParser, BeaconArgs>::command().debug_assert();
}

#[test]
fn beacon_arguments_parse_in_the_existing_node_command() {
    let mut cli = Cli::<EthereumChainSpecParser, BeaconArgs>::try_parse_from([
        "embedded-beacon",
        "node",
        "--beacon.status",
    ])
    .unwrap();
    let node = cli.as_node_command_mut().unwrap();
    assert!(node.ext.status);
    assert!(!node.ext.enabled);

    let mut cli = Cli::<EthereumChainSpecParser, BeaconArgs>::try_parse_from([
        "embedded-beacon",
        "node",
        "--embedded-beacon",
        "--beacon.sync-mode",
        "genesis",
        "--beacon.archive",
        "history",
        "--beacon.genesis",
        "genesis.ssz",
        "--beacon.port",
        "9001",
    ])
    .unwrap();
    let node = cli.as_node_command_mut().unwrap();
    assert!(node.ext.enabled);
    assert!(!node.ext.status);
    assert!(matches!(node.ext.sync_mode, Some(SyncMode::Genesis)));
    assert_eq!(node.ext.archive.as_deref(), Some(std::path::Path::new("history")));
    assert_eq!(node.ext.genesis.as_deref(), Some(std::path::Path::new("genesis.ssz")));
    assert_eq!(node.ext.port, 9001);

    let error = Cli::<EthereumChainSpecParser, BeaconArgs>::try_parse_from([
        "embedded-beacon",
        "node",
        "--beacon.sync-mode",
        "genesis",
    ])
    .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::MissingRequiredArgument);
}
#[test]
fn native_minimal_flag_selects_resource_defaults_and_preserves_explicit_cache_overrides() {
    Cli::<EthereumChainSpecParser, BeaconArgs>::command().debug_assert();
    let matches = Cli::<EthereumChainSpecParser, BeaconArgs>::command()
        .try_get_matches_from([
            "embedded-beacon",
            "node",
            "--embedded-beacon",
            "--minimal",
            "--beacon.profile",
            "--beacon.cache-mib",
            "128",
            "--rpc-cache.max-blocks-bytes",
            "32MB",
        ])
        .unwrap();
    let mut cli = Cli::<EthereumChainSpecParser, BeaconArgs>::from_arg_matches(&matches).unwrap();
    let node = cli.as_node_command_mut().unwrap();
    assert!(node.pruning.minimal);
    assert!(node.ext.profile_status);
    let profile = EmbeddedProfile::resolve(node.pruning.minimal, node.ext.cache_mib).unwrap();
    assert_eq!(profile.shared_cache_bytes, 128 * 1024 * 1024);
    let args = matches.subcommand_matches("node").unwrap();
    apply_cache_default(
        &mut node.rpc.rpc_state_cache.max_blocks_bytes,
        profile.rpc_block_cache_bytes,
        args,
        "max_blocks_bytes",
    );
    assert_eq!(node.rpc.rpc_state_cache.max_blocks_bytes, 32 * 1024 * 1024);
    apply_cache_default(
        &mut node.rpc.rpc_state_cache.max_receipts_bytes,
        profile.rpc_receipt_cache_bytes,
        args,
        "max_receipts_bytes",
    );
    assert_eq!(node.rpc.rpc_state_cache.max_receipts_bytes, 8 * 1024 * 1024);
    assert_eq!(
        consensus_config_for_execution(&node.chain).unwrap().preset,
        reth_embedded_beacon_config::Preset::Mainnet
    );
    assert!(Readiness::current().require_live_sync().is_err());
}

#[test]
fn read_only_beacon_api_is_explicit_and_defaults_to_loopback() {
    let mut parsed = Cli::<EthereumChainSpecParser, BeaconArgs>::try_parse_from([
        "reth-embedded-beacon",
        "node",
        "--embedded-beacon",
        "--beacon.http",
    ])
    .unwrap();
    let args = &parsed.as_node_command_mut().unwrap().ext;
    assert!(args.enabled && args.http);
    assert_eq!(args.http_addr, Some(IpAddr::V4(Ipv4Addr::LOCALHOST)));
    assert_eq!(args.http_port, 5052);
    let invalid = Cli::<EthereumChainSpecParser, BeaconArgs>::try_parse_from([
        "reth-embedded-beacon",
        "node",
        "--beacon.http",
    ])
    .unwrap_err();
    assert_eq!(invalid.kind(), ErrorKind::MissingRequiredArgument);
}

mod standalone;

#[test]
fn experimental_sync_requires_explicit_embedded_owner_and_keeps_readiness_unverified() {
    let rejected = Cli::<EthereumChainSpecParser, BeaconArgs>::try_parse_from([
        "reth-embedded-beacon",
        "node",
        "--beacon.experimental-sync",
    ])
    .unwrap_err();
    assert_eq!(rejected.kind(), ErrorKind::MissingRequiredArgument);
    let mut admitted = Cli::<EthereumChainSpecParser, BeaconArgs>::try_parse_from([
        "reth-embedded-beacon",
        "node",
        "--chain",
        "hoodi",
        "--embedded-beacon",
        "--beacon.experimental-sync",
        "--beacon.sync-mode",
        "checkpoint",
        "--beacon.checkpoint-url",
        "https://checkpoint.example.test",
        "--beacon.trusted-checkpoint",
        "2:0x0303030303030303030303030303030303030303030303030303030303030303",
    ])
    .unwrap();
    let node = admitted.as_node_command_mut().unwrap();
    let config = consensus_config_for_execution(&node.chain).unwrap();
    assert!(node.ext.experimental_sync);
    validate_args(&node.ext).unwrap();
    require_sync_launch(&node.ext, &config).unwrap();
    assert!(Readiness::current().require_live_sync().is_err());
    // Opt-in admission does not synthesize required bootstrap or networking inputs.
    node.ext.trusted_checkpoint = None;
    assert!(validate_args(&node.ext).is_err());
    assert!(startup::validate_topology(&node.ext, None).is_err());
}

#[test]
fn experimental_sync_admission_rejects_other_routes_and_default_remains_gated() {
    let mut args =
        BeaconArgs { enabled: true, sync_mode: Some(SyncMode::Checkpoint), ..Default::default() };
    let config = ChainConfig::hoodi();
    assert_eq!(
        require_sync_launch(&args, &config).unwrap_err().to_string(),
        Readiness::current().require_live_sync().unwrap_err().to_string()
    );
    args.experimental_sync = true;
    require_sync_launch(&args, &config).unwrap();
    let refusal = "--beacon.experimental-sync requires canonical Hoodi checkpoint sync";
    assert_eq!(
        require_sync_launch(&args, &ChainConfig::mainnet()).unwrap_err().to_string(),
        refusal
    );
    let mut changed_identity = config.clone();
    changed_identity.genesis_validators_root = [3; 32];
    assert_eq!(require_sync_launch(&args, &changed_identity).unwrap_err().to_string(), refusal);
    args.sync_mode = Some(SyncMode::Genesis);
    assert_eq!(require_sync_launch(&args, &config).unwrap_err().to_string(), refusal);
    args.sync_mode = Some(SyncMode::Checkpoint);
    args.devnet_config = Some("embedded-beacon.json".into());
    assert_eq!(require_sync_launch(&args, &config).unwrap_err().to_string(), refusal);
    args.devnet_config = None;
    args.enabled = false;
    assert_eq!(require_sync_launch(&args, &config).unwrap_err().to_string(), refusal);
}

#[test]
fn embedded_terminal_banner_requires_no_additional_cli_flag() {
    let mut parsed = Cli::<EthereumChainSpecParser, BeaconArgs>::try_parse_from([
        "reth-embedded-beacon",
        "node",
        "--embedded-beacon",
        "--color",
        "always",
    ])
    .unwrap();
    assert!(parsed.as_node_command_mut().unwrap().ext.enabled);
    assert_eq!(parsed.logs.color.to_string(), "always");
    let removed = Cli::<EthereumChainSpecParser, BeaconArgs>::try_parse_from([
        "reth-embedded-beacon",
        "node",
        "--embedded-beacon",
        "--beacon.fork-banner",
    ])
    .unwrap_err();
    assert_eq!(removed.kind(), ErrorKind::UnknownArgument);
}

#[test]
fn fcr_is_opt_in_and_requires_the_embedded_owner() {
    let mut ordinary = Cli::<EthereumChainSpecParser, BeaconArgs>::try_parse_from([
        "reth-embedded-beacon",
        "node",
    ])
    .unwrap();
    assert!(!ordinary.as_node_command_mut().unwrap().ext.fcr);
    let rejected = Cli::<EthereumChainSpecParser, BeaconArgs>::try_parse_from([
        "reth-embedded-beacon",
        "node",
        "--fcr",
    ])
    .unwrap_err();
    assert_eq!(rejected.kind(), ErrorKind::MissingRequiredArgument);
    let mut embedded = Cli::<EthereumChainSpecParser, BeaconArgs>::try_parse_from([
        "reth-embedded-beacon",
        "node",
        "--embedded-beacon",
        "--fcr",
    ])
    .unwrap();
    assert!(embedded.as_node_command_mut().unwrap().ext.fcr);
}

#[test]
fn decoded_state_cache_override_is_bounded_and_requires_the_embedded_owner() {
    let rejected = Cli::<EthereumChainSpecParser, BeaconArgs>::try_parse_from([
        "reth-embedded-beacon",
        "node",
        "--beacon.state-cache-mib",
        "512",
    ])
    .unwrap_err();
    assert_eq!(rejected.kind(), ErrorKind::MissingRequiredArgument);
    for value in ["0", "2049", "not-a-size"] {
        let rejected = Cli::<EthereumChainSpecParser, BeaconArgs>::try_parse_from([
            "reth-embedded-beacon",
            "node",
            "--embedded-beacon",
            "--beacon.state-cache-mib",
            value,
        ])
        .unwrap_err();
        assert_eq!(rejected.kind(), ErrorKind::ValueValidation);
    }
    let mut parsed = Cli::<EthereumChainSpecParser, BeaconArgs>::try_parse_from([
        "reth-embedded-beacon",
        "node",
        "--minimal",
        "--embedded-beacon",
        "--beacon.state-cache-mib",
        "512",
    ])
    .unwrap();
    let node = parsed.as_node_command_mut().unwrap();
    let profile = EmbeddedProfile::resolve(node.pruning.minimal, node.ext.cache_mib)
        .unwrap()
        .with_state_cache_mib(node.ext.state_cache_mib)
        .unwrap();
    assert_eq!(profile.state_cache_bytes, 512 * 1024 * 1024);
    assert_eq!(profile.shared_cache_bytes, 64 * 1024 * 1024);
    assert!(profile.minimal && profile.retain_all_unfinalized && profile.separate_el_cl_pins);
}
