//! The extension preserves native execution-node arguments unless explicitly enabled.

use super::*;

fn assert_native_node_arguments(args: &[&str]) {
    let mut native = Cli::<EthereumChainSpecParser>::try_parse_from(args.iter().copied()).unwrap();
    let mut extended =
        Cli::<EthereumChainSpecParser, BeaconArgs>::try_parse_from(args.iter().copied()).unwrap();
    let native = native.as_node_command_mut().unwrap();
    let extended = extended.as_node_command_mut().unwrap();

    assert_eq!(extended.chain, native.chain);
    assert_eq!(extended.datadir, native.datadir);
    assert_eq!(extended.network, native.network);
    assert_eq!(extended.rpc, native.rpc);
    assert_eq!(extended.pruning, native.pruning);
    assert_eq!(extended.instance, native.instance);
    assert_eq!(extended.with_unused_ports, native.with_unused_ports);
    assert!(!extended.ext.enabled);
    assert!(!extended.ext.experimental_sync);
    assert!(!extended.ext.status);
    assert!(!extended.ext.profile_status);
    assert!(extended.ext.effective_profile.is_none());
}

#[test]
fn native_network_rpc_auth_and_datadir_defaults_survive_the_extension() {
    for chain in ["mainnet", "hoodi"] {
        assert_native_node_arguments(&["reth", "node", "--chain", chain]);
        assert_native_node_arguments(&[
            "reth",
            "node",
            "--chain",
            chain,
            "--datadir",
            "standalone-node-data",
            "--http",
            "--http.port",
            "18854",
            "--authrpc.port",
            "18851",
            "--port",
            "30305",
            "--max-peers",
            "24",
        ]);
    }
}

#[test]
fn native_minimal_and_explicit_cache_settings_do_not_activate_embedded_profile() {
    assert_native_node_arguments(&[
        "reth",
        "node",
        "--chain",
        "hoodi",
        "--minimal",
        "--rpc-cache.max-blocks-bytes",
        "32MB",
        "--rpc-cache.max-receipts-bytes",
        "12MB",
        "--rpc-cache.max-bals-bytes",
        "4MB",
    ]);
}

#[test]
fn native_binary_parser_keeps_the_embedded_switch_out_of_its_interface() {
    let native =
        Cli::<EthereumChainSpecParser>::try_parse_from(["reth", "node", "--embedded-beacon"])
            .unwrap_err();
    assert_eq!(native.kind(), ErrorKind::UnknownArgument);

    let mut extended = Cli::<EthereumChainSpecParser, BeaconArgs>::try_parse_from([
        "reth-embedded-beacon",
        "node",
        "--embedded-beacon",
    ])
    .unwrap();
    assert!(extended.as_node_command_mut().unwrap().ext.enabled);
}

#[test]
fn native_non_node_commands_keep_their_arguments_and_no_node_startup() {
    let mut native = Cli::<EthereumChainSpecParser>::try_parse_from([
        "reth",
        "dump-genesis",
        "--chain",
        "hoodi",
    ])
    .unwrap();
    let mut extended = Cli::<EthereumChainSpecParser, BeaconArgs>::try_parse_from([
        "reth-embedded-beacon",
        "dump-genesis",
        "--chain",
        "hoodi",
    ])
    .unwrap();
    assert!(native.as_node_command_mut().is_none());
    assert!(extended.as_node_command_mut().is_none());
    assert_eq!(format!("{:?}", extended.command), format!("{:?}", native.command));
}

#[test]
fn explicit_beacon_resource_flags_require_the_embedded_owner() {
    for flag in ["--beacon.port", "--beacon.checkpoint-max-age-epochs", "--beacon.max-replay-slots"]
    {
        let rejected = Cli::<EthereumChainSpecParser, BeaconArgs>::try_parse_from([
            "reth-embedded-beacon",
            "node",
            flag,
            "64",
        ])
        .unwrap_err();
        assert_eq!(rejected.kind(), ErrorKind::MissingRequiredArgument);
        let mut accepted = Cli::<EthereumChainSpecParser, BeaconArgs>::try_parse_from([
            "reth-embedded-beacon",
            "node",
            "--embedded-beacon",
            flag,
            "64",
        ])
        .unwrap();
        assert!(accepted.as_node_command_mut().unwrap().ext.enabled);
    }
}
