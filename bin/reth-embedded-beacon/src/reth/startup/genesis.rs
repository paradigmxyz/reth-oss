//! Concrete genesis/restart/archive launcher wiring keeps original genesis trust distinct.
//! No slot-zero input is represented as a current finalized weak-subjectivity checkpoint.

use super::*;
use reth_embedded_beacon_node::{import_coordinator::GenesisImportAnchor, VerifiedGenesis};
use std::sync::Arc;

mod routes;

/// Prepare only concrete genesis owners under the existing shared writer and checked topology.
pub(super) async fn prepare<E>(
    args: BeaconArgs,
    config: ChainConfig,
    devnet: Option<DevnetConfig>,
    shared: SharedDataStore,
    engine: RuntimeEngineAdapter<E>,
    listeners: PeerListenConfig,
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
    let network = if let Some(devnet) = devnet {
        PersistenceNetwork::from_devnet(devnet)?
    } else {
        PersistenceNetwork::from_config(config.clone())?
    };
    let identity = network.identity();
    if shared.identity() != identity || engine.config() != &config {
        eyre::bail!(
            "captured genesis, execution and storage owners have different network identities"
        );
    }
    // Availability retains original proof-checked sidecars in this exact captured writer.
    let availability_shared = shared.clone();
    let directory = shared.namespace_root();
    let limits = BootstrapDecodeLimits::default();
    let cache_bytes = args
        .effective_profile
        .as_ref()
        .ok_or_else(|| eyre::eyre!("missing checked embedded resource profile"))?
        .state_cache_bytes;
    let genesis_file = args.genesis.expect("checked original genesis artifact");
    let seed_file = args.bootnodes_file.expect("checked signed seed file");
    let verify_config = config.clone();
    let (repository, genesis, recovering, seeds) = tokio::task::spawn_blocking(move || {
        let repository = AdmittedStateRepository::open(
            shared,
            network,
            CompactStateLimits { hot_cache_schema_bytes: cache_bytes, ..Default::default() },
        )?;
        let original = read_original(&genesis_file, limits.max_state_bytes)?;
        let genesis = VerifiedGenesis::authenticate(verify_config, original, limits)?;
        let recovering = if let Some(saved) = repository.recover_genesis(limits)? {
            if saved.original_state_ssz().as_ref() != genesis.original_state_ssz().as_ref() ||
                saved.block_ssz().as_ref() != genesis.block_ssz().as_ref()
            {
                eyre::bail!("original genesis artifact differs from the recovered trust input");
            }
            true
        } else {
            false
        };
        let seeds = read_seeds(&seed_file)?;
        Ok::<_, eyre::Report>((repository, genesis, recovering, seeds))
    })
    .await??;
    let (availability, broker) =
        RetrievalAvailability::with_shared_store(config.clone(), availability_shared)?;
    let anchor = GenesisImportAnchor { genesis: genesis.clone(), decode_limits: limits };
    let import_limits = ImportLimits { fast_confirmation: args.fcr, ..ImportLimits::default() };
    let context = StartupContext {
        config,
        repository,
        availability,
        broker,
        directory,
        identity,
        listeners,
        seeds,
        limits: import_limits,
        experimental_sync: false,
    };
    if recovering || args.archive.is_some() {
        routes::resume(context, engine, genesis, anchor, args.archive).await
    } else {
        routes::fresh(context, engine, genesis, anchor).await
    }
}

/// Bound original trust input reads before full authentication on this blocking worker.
fn read_original(path: &Path, maximum: usize) -> eyre::Result<Arc<[u8]>> {
    let maximum = u64::try_from(maximum)?;
    if maximum == 0 || maximum > 512 * 1024 * 1024 || reth_fs_util::metadata(path)?.len() > maximum
    {
        eyre::bail!("original genesis artifact exceeds its bounded input limit");
    }
    let mut bytes = Vec::new();
    reth_fs_util::open(path)?.take(maximum + 1).read_to_end(&mut bytes)?;
    if bytes.is_empty() || bytes.len() as u64 > maximum {
        eyre::bail!("original genesis artifact is empty or exceeds its bounded input limit");
    }
    Ok(bytes.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn original_genesis_file_has_finite_bytes_before_authentication() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("original.ssz");
        reth_fs_util::write(&path, b"bounded untrusted bytes").unwrap();
        assert_eq!(read_original(&path, 64).unwrap().as_ref(), b"bounded untrusted bytes");
        assert!(read_original(&path, 0).is_err());
        assert!(read_original(&path, 8).is_err());
        assert!(read_original(&path, 512 * 1024 * 1024 + 1).is_err());
        reth_fs_util::write(&path, b"").unwrap();
        assert!(read_original(&path, 64).is_err());
    }
}
