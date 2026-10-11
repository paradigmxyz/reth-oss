//! Optional beacon arguments kept separate from ordinary execution startup.

use super::parse_checkpoint;
use clap::{Args, ValueEnum};
use reth_embedded_beacon_config::EmbeddedProfile;
use reth_embedded_beacon_primitives::Checkpoint;
use std::{net::IpAddr, path::PathBuf};

/// Dedicated experimental arguments; the ordinary binary does not register these flags.
#[derive(Debug, Clone, Args, Default)]
pub(super) struct BeaconArgs {
    /// Follow consensus using the embedded beacon.
    #[arg(id = "beacon_enabled", long = "embedded-beacon")]
    pub(super) enabled: bool,
    /// Enable probabilistic fast confirmation after catch-up; diagnostics use beacon::fcr.
    #[arg(id = "beacon_fcr", long = "fcr", requires = "beacon_enabled")]
    pub(super) fcr: bool,
    /// Attempt Hoodi checkpoint sync before independent runtime acceptance is established.
    #[arg(
        id = "beacon_experimental_sync",
        long = "beacon.experimental-sync",
        requires = "beacon_enabled"
    )]
    pub(super) experimental_sync: bool,
    /// Enable the read-only Beacon API from the same authenticated consensus owner.
    #[arg(id = "beacon_http", long = "beacon.http", requires = "beacon_enabled")]
    pub(super) http: bool,
    /// Bind the optional read-only Beacon API; defaults to loopback.
    #[arg(
        id = "beacon_http_addr",
        long = "beacon.http-addr",
        default_value = "127.0.0.1",
        requires = "beacon_http"
    )]
    pub(super) http_addr: Option<IpAddr>,
    /// Read-only Beacon API port.
    #[arg(
        id = "beacon_http_port",
        long = "beacon.http-port",
        default_value_t = 5052,
        requires = "beacon_http"
    )]
    pub(super) http_port: u16,
    /// Print honest readiness without opening any database.
    #[arg(id = "beacon_status", long = "beacon.status")]
    pub(super) status: bool,
    /// Fresh synchronization strategy.
    #[arg(
        id = "beacon_sync_mode",
        long = "beacon.sync-mode",
        value_enum,
        requires = "beacon_enabled"
    )]
    pub(super) sync_mode: Option<SyncMode>,
    /// Strict JSON configuration for an explicitly isolated private network.
    #[arg(id = "beacon_devnet_config", long = "beacon.devnet-config", requires = "beacon_enabled")]
    pub(super) devnet_config: Option<PathBuf>,
    /// Integrity-indexed local history directory for genesis replay.
    #[arg(id = "beacon_archive", long = "beacon.archive", requires = "beacon_enabled")]
    pub(super) archive: Option<PathBuf>,
    /// Verified network genesis SSZ artifact.
    #[arg(id = "beacon_genesis", long = "beacon.genesis", requires = "beacon_enabled")]
    pub(super) genesis: Option<PathBuf>,
    /// Checkpoint artifact source; its response never establishes trust.
    #[arg(
        id = "beacon_checkpoint_url",
        long = "beacon.checkpoint-url",
        requires = "beacon_enabled"
    )]
    pub(super) checkpoint_url: Option<String>,
    /// Explicit epoch and trusted block root, formatted EPOCH:0xROOT.
    #[arg(id = "beacon_trusted_checkpoint", long = "beacon.trusted-checkpoint", value_parser = parse_checkpoint, requires = "beacon_enabled")]
    pub(super) trusted_checkpoint: Option<Checkpoint>,
    /// Beacon listener port, separate from execution networking.
    #[arg(
        id = "beacon_port",
        long = "beacon.port",
        default_value_t = 9000,
        requires = "beacon_enabled"
    )]
    pub(super) port: u16,
    /// Literal advertised beacon address used by the signed discovery record.
    #[arg(id = "beacon_listen_ip", long = "beacon.listen-ip", requires = "beacon_enabled")]
    pub(super) listen_ip: Option<IpAddr>,
    /// Local socket binding behind explicit address translation; defaults to the advertised IP.
    #[arg(id = "beacon_bind_ip", long = "beacon.bind-ip", requires = "beacon_enabled")]
    pub(super) bind_ip: Option<IpAddr>,
    /// Separate QUIC UDP port; defaults to the checked beacon port plus one.
    #[arg(id = "beacon_quic_port", long = "beacon.quic-port", requires = "beacon_enabled")]
    pub(super) quic_port: Option<u16>,
    /// Bounded UTF-8 file containing one signed discovery record per line.
    #[arg(
        id = "beacon_bootnodes_file",
        long = "beacon.bootnodes-file",
        requires = "beacon_enabled"
    )]
    pub(super) bootnodes_file: Option<PathBuf>,
    /// Additional checkpoint age cap; state-dependent trust checks still apply.
    #[arg(
        id = "beacon_checkpoint_max_age_epochs",
        long = "beacon.checkpoint-max-age-epochs",
        default_value_t = 2048,
        requires = "beacon_enabled"
    )]
    pub(super) checkpoint_max_age_epochs: u64,
    /// Maximum authenticated empty-slot replay at a checkpoint boundary.
    #[arg(
        id = "beacon_max_replay_slots",
        long = "beacon.max-replay-slots",
        default_value_t = 8192,
        requires = "beacon_enabled"
    )]
    pub(super) max_replay_slots: u64,
    /// Shared immutable-byte cache limit in MiB; defaults to 64 with --minimal, otherwise 256.
    #[arg(id = "beacon_cache_mib", long = "beacon.cache-mib", requires = "beacon_enabled")]
    pub(super) cache_mib: Option<usize>,
    /// Decoded-state cache in MiB; defaults to 64 with --minimal, otherwise 256.
    #[arg(
        id = "beacon_state_cache_mib",
        long = "beacon.state-cache-mib",
        requires = "beacon_enabled",
        value_parser = parse_state_cache_mib
    )]
    pub(super) state_cache_mib: Option<usize>,
    /// Print the effective embedded resource policy and readiness without opening databases.
    #[arg(id = "beacon_profile_status", long = "beacon.profile", requires = "beacon_enabled")]
    pub(super) profile_status: bool,
    /// Resource policy selected from the existing execution --minimal flag.
    #[arg(skip)]
    pub(super) effective_profile: Option<EmbeddedProfile>,
}

/// Fresh synchronization strategy for the isolated experiment.
#[derive(Debug, Clone, Copy, ValueEnum)]
pub(super) enum SyncMode {
    /// Replay from verified network genesis and archived history.
    Genesis,
    /// Bootstrap from an explicitly trusted checkpoint.
    Checkpoint,
}

fn parse_state_cache_mib(value: &str) -> Result<usize, String> {
    let mib = value.parse::<usize>().map_err(|_| "state cache must be between 1 and 2048 MiB")?;
    if !(1..=2048).contains(&mib) {
        return Err("state cache must be between 1 and 2048 MiB".to_owned())
    }
    Ok(mib)
}
