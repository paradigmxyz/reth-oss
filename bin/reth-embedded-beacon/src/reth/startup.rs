//! Concrete startup joins one shared writer, captured Engine API and strict peer/import owners.
//! File and complete SSZ work run off the async IO workers. Provider responses never select trust.

use super::{
    presentation::{observe_checkpoint_authentication, CheckpointWork, OutputOptions},
    BeaconArgs, SyncMode,
};
use alloy_rpc_types_engine::{ExecutionData, ForkchoiceState};
use reth_embedded_beacon_config::{ChainConfig, DevnetConfig};
use reth_embedded_beacon_execution_proxy::{ExecutionEngine, ForkchoiceReply, PayloadReply};
use reth_embedded_beacon_network::EndpointPolicy;
use reth_embedded_beacon_node::{
    checkpoint_download::CheckpointSource,
    compact_persistence::{AdmittedStateRepository, PersistenceNetwork},
    execution_bootstrap::{
        BootstrapLimits, BootstrapRunError, ExecutionBootstrap, ReconciledCheckpoint,
    },
    import_coordinator::{
        ImportAnchor, ImportCoordinator, ImportLimits, RecoveryLimits, ReplayAnchor,
    },
    live_sync::{
        availability::{RetrievalAvailability, RetrievalBroker},
        LiveSync, LiveSyncCancellation,
    },
    peer_runtime::{CheckpointPeerRuntime, PeerListenConfig},
    runtime_engine::RuntimeEngineAdapter,
    BootstrapDecodeLimits, CheckpointBlockFormat, SszCheckpointVerifier,
};
use reth_embedded_beacon_shared_data::{SharedDataStore, StoreIdentity};
use reth_embedded_beacon_storage::CompactStateLimits;
use std::{
    future::Future,
    io::Read,
    path::{Path, PathBuf},
    pin::Pin,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const MAX_SEED_BYTES: u64 = 1024 * 1024;
const MAX_SEEDS: usize = 128;
const MAX_SEED_LINE: usize = 4096;

mod checkpoint;
mod genesis;

/// Already checked owners shared by the distinct startup routes.
struct StartupContext {
    config: ChainConfig,
    repository: AdmittedStateRepository,
    availability: RetrievalAvailability,
    broker: RetrievalBroker,
    directory: PathBuf,
    identity: StoreIdentity,
    listeners: PeerListenConfig,
    seeds: Vec<String>,
    limits: ImportLimits,
    experimental_sync: bool,
}

/// Construct a future in this synchronous frame before its caller polls the heap allocation.
#[inline(never)]
fn boxed_future<'a, F>(
    factory: impl FnOnce() -> F,
) -> Pin<Box<dyn Future<Output = F::Output> + Send + 'a>>
where
    F: Future + Send + 'a,
{
    Box::pin(factory())
}

/// Validate actual socket topology separately from printed profile/status commands.
pub(super) fn validate_topology(
    args: &BeaconArgs,
    devnet: Option<&DevnetConfig>,
) -> eyre::Result<PeerListenConfig> {
    let ip = args.listen_ip.ok_or_else(|| {
        eyre::eyre!(
            "embedded networking requires --beacon.listen-ip with a literal advertised address"
        )
    })?;
    let policy = if let Some(config) = devnet {
        config.validate()?;
        EndpointPolicy::PrivateNetwork
    } else {
        EndpointPolicy::Public
    };
    let quic_port = args
        .quic_port
        .or_else(|| args.port.checked_add(1))
        .ok_or_else(|| eyre::eyre!("beacon QUIC port overflow; select --beacon.quic-port"))?;
    let bind_ip = args.bind_ip.unwrap_or(ip);
    if !policy.permits(ip) ||
        !policy.permits_bind(bind_ip) ||
        bind_ip.is_ipv4() != ip.is_ipv4() ||
        args.port == 0 ||
        quic_port == 0 ||
        quic_port == args.port
    {
        eyre::bail!("beacon address or UDP ports do not match the checked network routing policy");
    }
    if args.bootnodes_file.is_none() {
        eyre::bail!(
            "embedded networking requires --beacon.bootnodes-file with signed discovery records"
        );
    }
    if args.checkpoint_max_age_epochs == 0 || !(1..=8192).contains(&args.max_replay_slots) {
        eyre::bail!("checkpoint trust and replay limits must be positive and bounded");
    }
    Ok(PeerListenConfig {
        ip,
        bind_ip: args.bind_ip,
        discovery_port: args.port,
        tcp_port: args.port,
        quic_port,
        policy,
    })
}

/// Prepare the actual owners using the byte backend already captured from the execution pool.
pub(super) async fn prepare<E>(
    args: BeaconArgs,
    config: ChainConfig,
    devnet: Option<DevnetConfig>,
    shared: SharedDataStore,
    engine: RuntimeEngineAdapter<E>,
    output: OutputOptions,
) -> eyre::Result<(LiveSync, LiveSyncCancellation)>
where
    E: ExecutionEngine<
            Payload = ExecutionData,
            PayloadReply = PayloadReply,
            Forkchoice = ForkchoiceState,
            ForkchoiceReply = ForkchoiceReply,
        > + Clone
        + 'static,
    E::Error: std::error::Error + Send + Sync + 'static,
{
    let listeners = validate_topology(&args, devnet.as_ref())?;
    if matches!(args.sync_mode, Some(SyncMode::Genesis)) {
        return boxed_future(|| genesis::prepare(args, config, devnet, shared, engine, listeners))
            .await;
    }
    let network = if let Some(devnet) = &devnet {
        PersistenceNetwork::from_devnet(devnet.clone())?
    } else {
        PersistenceNetwork::from_config(config.clone())?
    };
    let identity = network.identity();
    if shared.identity() != identity || engine.config() != &config {
        eyre::bail!("captured execution and consensus owners have different network identities");
    }
    // Availability retains original proof-checked sidecars in this exact captured writer.
    let availability_shared = shared.clone();
    let directory = shared.namespace_root();
    let max_age = args.checkpoint_max_age_epochs;
    let decode_limits = BootstrapDecodeLimits::default();
    let cache_bytes = args
        .effective_profile
        .as_ref()
        .ok_or_else(|| eyre::eyre!("missing checked embedded resource profile"))?
        .state_cache_bytes;
    let repository_config = config.clone();
    let seed_file = args.bootnodes_file.clone().expect("checked seed file");
    let (repository, recovered, seeds) = tokio::task::spawn_blocking(move || {
        let repository = AdmittedStateRepository::open(
            shared,
            network,
            CompactStateLimits { hot_cache_schema_bytes: cache_bytes, ..Default::default() },
        )?;
        let verifier = SszCheckpointVerifier::new(
            repository_config,
            decode_limits,
            CheckpointBlockFormat::Signed,
        )?;
        let recovered = repository
            .recover_checkpoint(&verifier, now()?, max_age)?
            .map(|recovered| recovered.into_import_anchor(max_age, decode_limits))
            .transpose()?;
        let seeds = read_seeds(&seed_file)?;
        Ok::<_, eyre::Report>((repository, recovered, seeds))
    })
    .await??;
    let (availability, broker) =
        RetrievalAvailability::with_shared_store(config.clone(), availability_shared)?;
    let limits = ImportLimits { fast_confirmation: args.fcr, ..ImportLimits::default() };
    let context = StartupContext {
        config,
        repository,
        availability,
        broker,
        directory,
        identity,
        listeners,
        seeds,
        limits,
        experimental_sync: args.experimental_sync,
    };
    if let Some(anchor) = recovered {
        checkpoint::recover(context, engine, anchor).await
    } else {
        checkpoint::fresh(context, engine, args, devnet, output).await
    }
}

/// Continue known execution download work in bounded, freshly authenticated batches.
///
/// Space and work per batch remain bounded even when initial execution synchronization takes
/// hours. Each batch independently rechecks original SSZ and the current state-dependent trust
/// window; expired trust, ambiguous outcomes and closed admission never receive a reset.
async fn reconcile_checkpoint<E>(
    config: &ChainConfig,
    anchor: &ImportAnchor,
    engine: &RuntimeEngineAdapter<E>,
    output: OutputOptions,
) -> eyre::Result<ReconciledCheckpoint>
where
    E: ExecutionEngine<
            Payload = ExecutionData,
            PayloadReply = PayloadReply,
            Forkchoice = ForkchoiceState,
            ForkchoiceReply = ForkchoiceReply,
        > + Clone
        + 'static,
    E::Error: std::error::Error + Send + Sync + 'static,
{
    let work = CheckpointWork {
        epoch: anchor.verified.checkpoint().epoch,
        state_bytes: anchor.state_ssz.len(),
        post_block_state_bytes: anchor
            .replay
            .as_ref()
            .map_or(0, |replay| replay.post_block_state_ssz.len()),
        block_bytes: anchor.block_ssz.len(),
    };
    let mut batch = 0u64;
    loop {
        let batch_config = config.clone();
        let batch_anchor = anchor.clone();
        let authentication = tokio::task::spawn_blocking(move || {
            let started = Instant::now();
            tracing::info!(target: "beacon::bootstrap",
                phase = "execution_bootstrap_authentication", epoch = work.epoch,
                state_bytes = work.state_bytes, post_block_state_bytes = work.post_block_state_bytes,
                block_bytes = work.block_bytes,
                "[beacon] Authenticating checkpoint for execution bootstrap");
            let authenticated = ExecutionBootstrap::authenticate(
                batch_config,
                &batch_anchor,
                now()?,
                BootstrapLimits::default(),
            )?;
            tracing::info!(target: "beacon::bootstrap",
                phase = "execution_bootstrap_authentication", epoch = work.epoch,
                elapsed_ms = started.elapsed().as_millis(),
                "[beacon] Completed execution bootstrap checkpoint authentication");
            Ok::<_, eyre::Report>(authenticated)
        });
        let (mut bootstrap, cancellation) =
            observe_checkpoint_authentication(authentication, output, work).await??;
        batch = batch.saturating_add(1);
        let result = bootstrap.reconcile(engine).await;
        drop(cancellation);
        match result {
            Ok(receipt) => return Ok(receipt),
            Err(BootstrapRunError::Attempts | BootstrapRunError::Deadline)
                if !bootstrap.requires_reconciliation() && !engine.is_closed() =>
            {
                tracing::info!(target: "beacon::bootstrap", batch,
                    "[beacon] Execution download is continuing; rechecking checkpoint trust before the next bounded batch");
            }
            Err(error) => return Err(error.into()),
        }
    }
}

/// Read bounded original signed seed text; verification still belongs to the network owner.
fn read_seeds(path: &Path) -> eyre::Result<Vec<String>> {
    let mut bytes = Vec::new();
    reth_fs_util::open(path)?.take(MAX_SEED_BYTES + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_SEED_BYTES {
        eyre::bail!("beacon seed file exceeds its byte limit");
    }
    let text = std::str::from_utf8(&bytes)?;
    let mut seeds = Vec::new();
    for line in
        text.lines().map(str::trim).filter(|line| !line.is_empty() && !line.starts_with('#'))
    {
        if line.len() > MAX_SEED_LINE || seeds.len() == MAX_SEEDS || !line.starts_with("enr:") {
            eyre::bail!("beacon seed file exceeds its limit or contains a malformed record");
        }
        if !seeds.iter().any(|seed| seed == line) {
            seeds.push(line.to_owned());
        }
    }
    if seeds.is_empty() {
        eyre::bail!("beacon seed file contains no signed discovery records");
    }
    Ok(seeds)
}

fn now() -> eyre::Result<u64> {
    Ok(u64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn original_seed_file_has_finite_bytes_entries_and_text_decoding() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("seeds.txt");
        // Original records remain untrusted text until the actual network owner's verifier.
        reth_fs_util::write(&path, b"# comment\n\nenr:untrusted-one\nenr:untrusted-one\n").unwrap();
        assert_eq!(read_seeds(&path).unwrap(), vec!["enr:untrusted-one"]);
        for invalid in [
            vec![0xff],
            vec![b'x'; MAX_SEED_LINE + 1],
            vec![b' '; MAX_SEED_BYTES as usize + 1],
            b"https://example.test/\n".to_vec(),
        ] {
            reth_fs_util::write(&path, invalid).unwrap();
            assert!(read_seeds(&path).is_err());
        }
        let many =
            (0..=MAX_SEEDS).map(|index| format!("enr:untrusted-{index}\n")).collect::<String>();
        reth_fs_util::write(&path, many).unwrap();
        assert!(read_seeds(&path).is_err());
        reth_fs_util::write(&path, b"# empty\n").unwrap();
        assert!(read_seeds(&path).is_err());
    }
}
