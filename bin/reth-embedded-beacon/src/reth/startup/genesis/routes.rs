//! Separate original-history replay and fresh genesis owner startup.

use super::*;

/// Drive actual recovery or explicit signed archive replay before live handoff.
pub(super) async fn resume<E>(
    context: StartupContext,
    engine: RuntimeEngineAdapter<E>,
    genesis: VerifiedGenesis,
    anchor: GenesisImportAnchor,
    archive: Option<PathBuf>,
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
    let StartupContext {
        config,
        repository,
        availability,
        broker,
        directory,
        identity,
        listeners,
        seeds,
        limits: import_limits,
        experimental_sync: _,
    } = context;
    let mut startup = if let Some(archive) = archive {
        // Explicit original history resumes through its own progressive archive contract.
        ImportCoordinator::begin_genesis_archive(
            config.clone(),
            anchor,
            repository,
            engine,
            availability.clone(),
            import_limits,
            archive,
        )?
    } else {
        ImportCoordinator::begin_genesis_recovery(
            config.clone(),
            anchor,
            repository,
            engine,
            availability.clone(),
            import_limits,
            RecoveryLimits::default(),
        )?
    };
    let status = startup.anchor_status().await?;
    let peer_config = config.clone();
    let peer = tokio::task::spawn_blocking(move || {
        let peer = CheckpointPeerRuntime::open_genesis(
            directory,
            identity,
            peer_config,
            genesis,
            status,
            now()?,
            listeners,
        )?;
        for seed in &seeds {
            peer.add_bootstrap_seed(seed)?;
        }
        Ok::<_, eyre::Report>(peer)
    })
    .await??;
    Ok(boxed_future(|| LiveSync::finish_startup(peer, availability, broker, config, startup))
        .await?)
}

/// Open a fresh genuine genesis owner with the original sealed trust input.
pub(super) async fn fresh<E>(
    context: StartupContext,
    engine: RuntimeEngineAdapter<E>,
    genesis: VerifiedGenesis,
    anchor: GenesisImportAnchor,
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
    let StartupContext {
        config,
        repository,
        availability,
        broker,
        directory,
        identity,
        listeners,
        seeds,
        limits: import_limits,
        experimental_sync: _,
    } = context;
    // Keep concrete owner futures on the heap instead of growing this startup frame.
    let importer = boxed_future(|| {
        ImportCoordinator::start_genesis(
            config.clone(),
            anchor,
            repository,
            engine,
            availability.clone(),
            availability.clone(),
            import_limits,
        )
    })
    .await?;
    let status = importer.authenticated_status().await?;
    let peer_config = config.clone();
    let peer = tokio::task::spawn_blocking(move || {
        let peer = CheckpointPeerRuntime::open_genesis(
            directory,
            identity,
            peer_config,
            genesis,
            status,
            now()?,
            listeners,
        )?;
        for seed in &seeds {
            peer.add_bootstrap_seed(seed)?;
        }
        Ok::<_, eyre::Report>(peer)
    })
    .await??;
    Ok(boxed_future(|| LiveSync::new(peer, importer, availability, broker, config)).await?)
}
