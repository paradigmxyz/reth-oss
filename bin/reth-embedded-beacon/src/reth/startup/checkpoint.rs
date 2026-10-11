//! Distinct checkpoint acquisition, authenticated recovery and fresh owner startup routes.

use super::*;
use crate::reth::presentation::{observe_checkpoint_authentication, CheckpointWork};
use std::time::Instant;

mod admission;

use admission::CheckpointAdmission;

/// Restore original signed history through the real peer and availability owner.
pub(super) async fn recover<E>(
    context: StartupContext,
    engine: RuntimeEngineAdapter<E>,
    anchor: ImportAnchor,
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
        limits,
        experimental_sync,
    } = context;
    tracing::info!(target: "beacon::bootstrap", epoch = anchor.verified.checkpoint().epoch,
        "[beacon] Reauthenticating original signed history before restoring the canonical head");
    let checkpoint = anchor.verified.clone();
    let begin_recovery = match checkpoint_admission(&config, &anchor)? {
        CheckpointAdmission::Strict => ImportCoordinator::begin_recovery,
        CheckpointAdmission::Optimistic => ImportCoordinator::begin_recovery_optimistic,
    };
    // Both owners reauthenticate the exact published original transcript. The optimistic
    // route retains actual pending Engine results without forcing a VALID startup receipt.
    let mut startup = begin_recovery(
        config.clone(),
        anchor,
        repository,
        engine,
        availability.clone(),
        limits,
        RecoveryLimits::default(),
    )?;
    let status = startup.anchor_status().await?;
    let peer_config = config.clone();
    let peer = tokio::task::spawn_blocking(move || {
        let open = if experimental_sync {
            CheckpointPeerRuntime::open_experimental_hoodi_checkpoint
        } else {
            CheckpointPeerRuntime::open_authenticated
        };
        let peer = open(directory, identity, peer_config, checkpoint, status, now()?, listeners)?;
        for seed in &seeds {
            peer.add_bootstrap_seed(seed)?;
        }
        Ok::<_, eyre::Report>(peer)
    })
    .await??;
    Ok(boxed_future(|| LiveSync::finish_startup(peer, availability, broker, config, startup))
        .await?)
}

/// Open checked optimistic checkpoint owners while their single Engine owner catches up.
pub(super) async fn fresh<E>(
    context: StartupContext,
    engine: RuntimeEngineAdapter<E>,
    args: BeaconArgs,
    devnet: Option<DevnetConfig>,
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
    let StartupContext {
        config,
        repository,
        availability,
        broker,
        directory,
        identity,
        listeners,
        seeds,
        limits,
        experimental_sync,
    } = context;
    let anchor = download_anchor(&args, &config, devnet.as_ref(), output).await?;
    let checkpoint = anchor.verified.clone();
    let admission = checkpoint_admission(&config, &anchor)?;
    // An optimistic owner is the sole FCU producer. Never poll a parallel fixed-anchor
    // reconciliation loop that could retarget execution away from its advancing head.
    let receipt = admission
        .receipt(boxed_future(|| reconcile_checkpoint(&config, &anchor, &engine, output)))
        .await?;
    // Keep concrete owner futures on the heap instead of growing this startup frame.
    let importer = boxed_future(|| async {
        if let Some(receipt) = receipt {
            ImportCoordinator::start_reconciled(
                config.clone(),
                anchor,
                repository,
                engine,
                receipt,
                availability.clone(),
                availability.clone(),
                limits,
            )
            .await
        } else {
            ImportCoordinator::start_optimistic(
                config.clone(),
                anchor,
                repository,
                engine,
                availability.clone(),
                availability.clone(),
                limits,
            )
            .await
        }
    })
    .await?;
    let status = importer.authenticated_status().await?;
    let peer_config = config.clone();
    let peer = tokio::task::spawn_blocking(move || {
        let open = if experimental_sync {
            CheckpointPeerRuntime::open_experimental_hoodi_checkpoint
        } else {
            CheckpointPeerRuntime::open_authenticated
        };
        let peer = open(directory, identity, peer_config, checkpoint, status, now()?, listeners)?;
        for seed in &seeds {
            peer.add_bootstrap_seed(seed)?;
        }
        Ok::<_, eyre::Report>(peer)
    })
    .await??;
    Ok(boxed_future(|| LiveSync::new(peer, importer, availability, broker, config)).await?)
}

/// Download bounded original SSZ and run the unchanged full checkpoint replay verifier.
async fn download_anchor(
    args: &BeaconArgs,
    config: &ChainConfig,
    devnet: Option<&DevnetConfig>,
    output: OutputOptions,
) -> eyre::Result<ImportAnchor> {
    let max_age = args.checkpoint_max_age_epochs;
    let decode_limits = BootstrapDecodeLimits::default();
    Ok(match args.sync_mode {
        Some(SyncMode::Checkpoint) => {
            let source_url = args.checkpoint_url.as_deref().expect("checked checkpoint URL");
            let source = if let Some(devnet) = devnet {
                CheckpointSource::for_devnet(
                    source_url,
                    devnet,
                    decode_limits,
                    Duration::from_secs(120),
                )?
            } else {
                CheckpointSource::new(
                    source_url,
                    config.clone(),
                    decode_limits,
                    Duration::from_secs(120),
                )?
            };
            let inputs = source
                .download(args.trusted_checkpoint.expect("checked explicit checkpoint"))
                .await?;
            let verify_config = config.clone();
            let replay_slots = args.max_replay_slots;
            let work = CheckpointWork {
                epoch: inputs.trusted.epoch,
                state_bytes: inputs.state_ssz.len(),
                post_block_state_bytes: inputs.post_block_state_ssz.len(),
                block_bytes: inputs.block_ssz.len(),
            };
            let verification = tokio::task::spawn_blocking(move || {
                let verifier = SszCheckpointVerifier::new(
                    verify_config,
                    decode_limits,
                    CheckpointBlockFormat::Signed,
                )?;
                let started = Instant::now();
                tracing::info!(target: "beacon::bootstrap", epoch = inputs.trusted.epoch,
                    state_bytes = inputs.state_ssz.len(),
                    post_block_state_bytes = inputs.post_block_state_ssz.len(),
                    block_bytes = inputs.block_ssz.len(),
                    "[beacon] Authenticating downloaded checkpoint inputs");
                let proof = verifier.verify_checkpoint_replay(
                    &inputs.state_ssz,
                    &inputs.post_block_state_ssz,
                    &inputs.block_ssz,
                    inputs.trusted,
                    now()?,
                    max_age,
                    replay_slots,
                )?;
                tracing::info!(target: "beacon::bootstrap",
                    elapsed_ms = started.elapsed().as_millis(),
                    epoch = proof.verified().checkpoint().epoch, slot = proof.verified().slot(),
                    "[beacon] Completed downloaded checkpoint authentication");
                Ok::<_, eyre::Report>(ImportAnchor {
                    verified: proof.verified().clone(),
                    state_ssz: inputs.state_ssz,
                    block_ssz: inputs.block_ssz,
                    block_format: CheckpointBlockFormat::Signed,
                    max_age_epochs: max_age,
                    decode_limits,
                    replay: Some(ReplayAnchor {
                        proof,
                        post_block_state_ssz: inputs.post_block_state_ssz,
                    }),
                })
            });
            observe_checkpoint_authentication(verification, output, work).await??
        }
        Some(SyncMode::Genesis) => unreachable!("genesis startup has a distinct concrete owner"),
        None => eyre::bail!("missing checked embedded synchronization strategy"),
    })
}

/// Fresh and recovery owners select the authenticated original post-block schema uniformly.
fn checkpoint_admission(
    config: &ChainConfig,
    anchor: &ImportAnchor,
) -> eyre::Result<CheckpointAdmission> {
    // An epoch-boundary replay may upgrade after the original block. Its sealed proof selects
    // that actual post-block schema; the owner independently reauthenticates the original inputs.
    let fork = if let Some(replay) = &anchor.replay {
        replay.proof.post_block_fork()
    } else {
        config.fork_at_slot(anchor.verified.slot())?.fork
    };
    Ok(CheckpointAdmission::for_fork(fork))
}
