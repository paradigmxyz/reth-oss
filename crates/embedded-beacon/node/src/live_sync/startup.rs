//! Actual peer/broker event driving during bounded authenticated signed recovery.

use super::*;
use crate::import_coordinator::ImportStartup;

mod retrieval;
use retrieval::StartupRetrieval;

impl LiveSync {
    /// Drive real availability retrieval while the dedicated owner replays signed history.
    /// Checked newer peer finality is a data source only; no remote head is used as local proof.
    pub async fn finish_startup(
        mut peer: CheckpointPeerRuntime,
        availability: RetrievalAvailability,
        mut broker: RetrievalBroker,
        config: ChainConfig,
        startup: ImportStartup,
    ) -> Result<(Self, LiveSyncCancellation), LiveSyncError> {
        let digest = checked_config_digest(&config).map_err(|_| LiveSyncError::Configuration)?;
        if peer.config_digest()? != digest ||
            availability.config_digest() != digest ||
            broker.digest != digest
        {
            return Err(LiveSyncError::Configuration)
        }
        availability.bind_peer_identity(peer.local_enr()?.node_id().raw())?;
        peer.start().await?;
        let mut discovery_pending = discovery_scheduled(peer.request_discovery())?;
        let mut discovery_started = Instant::now();
        let mut targets = Vec::<Target>::new();
        let mut sidecars = StartupRetrieval::<RetrievalJob, SidecarRequestId>::default();
        let mut envelopes = StartupRetrieval::<EnvelopeRetrievalJob, EnvelopeRequestId>::default();
        let mut completed = None::<ImportCoordinator>;
        let mut ready = Box::pin(startup.finish());
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        let mut next_status = Instant::now();
        let importer = loop {
            // During replay, the worker supplies its exact failure through Ready. After
            // completion, a halted availability owner still prevents the live handoff.
            if completed.is_some() && availability.is_halted() {
                peer.shutdown();
                return Err(LiveSyncError::BrokerClosed)
            }
            sidecars.schedule(&mut targets, &mut peer, |job, peer, selected| {
                peer.request_sidecars(selected, job.request.clone())
            })?;
            envelopes.schedule(&mut targets, &mut peer, |job, peer, selected| {
                peer.request_envelopes(
                    selected,
                    EnvelopeRequest::Roots(vec![job.metadata.root()]),
                    vec![job.metadata],
                )
            })?;
            if Instant::now() >= next_status {
                tracing::info!(target: "beacon::replay", is_syncing = true, recovering = true,
                    peers = targets.len(), sidecar_request = sidecars.has_request(),
                    envelope_request = envelopes.has_request(),
                    recovery_jobs = usize::from(sidecars.has_work()) + usize::from(envelopes.has_work()),
                    "[beacon] Recovering authenticated signed history");
                next_status = Instant::now() + Duration::from_secs(30);
            }
            if completed.is_some() && !sidecars.has_request() && !envelopes.has_request() {
                break completed.take().expect("completed authenticated recovery")
            }
            let event = tokio::select! {
                result = ready.as_mut(), if completed.is_none() => StartupEvent::Ready(result),
                result = peer.next_event() => StartupEvent::Peer(result?),
                job = broker.receiver.recv() => StartupEvent::Retrieval(
                    job.ok_or(LiveSyncError::BrokerClosed)?
                ),
                job = broker.envelopes.recv() => StartupEvent::EnvelopeRetrieval(
                    job.ok_or(LiveSyncError::BrokerClosed)?
                ),
                _ = tick.tick() => StartupEvent::Tick,
            };
            match event {
                StartupEvent::Ready(result) => {
                    // A failed replay can leave its cancelled original retrieval in flight.
                    // Preserve that failure instead of replacing it with an ownership error.
                    let importer = result.inspect_err(|error| {
                        tracing::error!(target: "beacon::replay", %error,
                            "[beacon] Authenticated signed-history recovery failed");
                    })?;
                    sidecars.expire();
                    envelopes.expire();
                    if sidecars.has_work() || envelopes.has_work() {
                        return Err(LiveSyncError::Response)
                    }
                    // Cancelled receivers confer no data authority. Drain their exact private
                    // transport tokens before transferring this peer owner into live syncing.
                    completed = Some(importer);
                }
                StartupEvent::Retrieval(job) => {
                    if envelopes.has_work() {
                        return Err(LiveSyncError::Bounds)
                    }
                    sidecars.admit(job)?;
                }
                StartupEvent::EnvelopeRetrieval(job) => {
                    if sidecars.has_work() {
                        return Err(LiveSyncError::Bounds)
                    }
                    envelopes.admit(job)?;
                }
                StartupEvent::Peer(PeerRuntimeEvent::Transport(event)) => match event {
                    TransportEvent::PeerStatus {
                        peer,
                        status,
                        earliest_available_slot,
                        compatibility:
                            StatusCompatibility::KnownFinality |
                            StatusCompatibility::NeedsFinalizedRoot { .. },
                    } => {
                        if let Some(target) = targets.iter_mut().find(|target| target.peer == peer)
                        {
                            target.status = status;
                            target.earliest = earliest_available_slot;
                        } else if targets.len() < MAX_PEERS {
                            targets.push(Target {
                                peer,
                                status,
                                earliest: earliest_available_slot,
                                examined_root: None,
                            });
                        }
                    }
                    TransportEvent::Envelopes { peer, id, batch } => {
                        let Some(work) = envelopes.retire(peer, id) else {
                            tracing::debug!(target: "beacon::replay", %peer, ?id,
                                "[beacon] Discarded response without its current recovery request");
                            continue
                        };
                        let Some(work) = work else { continue };
                        let metadata = work.job.metadata;
                        let result = EnvelopeRequest::Roots(vec![metadata.root()])
                            .matches(&config, &batch, |root| {
                                (root == metadata.root()).then_some(&metadata)
                            })
                            .map(|()| batch)
                            .map_err(|_| RetrievalError::Association);
                        work.job.complete(result);
                    }
                    TransportEvent::EnvelopesUnavailable { peer: provider, id, code } => {
                        if envelopes.unavailable(provider, id)? {
                            targets.retain(|target| target.peer != provider);
                            if code == 3 {
                                peer.retire_peer(provider)?;
                            }
                            tracing::debug!(target: "beacon::replay", code,
                                remaining_providers = targets.len(),
                                "[beacon] Recovery envelope provider reported unavailable data");
                        } else {
                            tracing::debug!(target: "beacon::replay", peer = %provider, ?id,
                                "[beacon] Discarded response without its current recovery request");
                        }
                    }
                    TransportEvent::Sidecars { peer, id, batch } => {
                        let Some(work) = sidecars.retire(peer, id) else {
                            tracing::debug!(target: "beacon::replay", %peer, ?id,
                                "[beacon] Discarded response without its current recovery request");
                            continue
                        };
                        let Some(work) = work else { continue };
                        let result = work
                            .job
                            .request
                            .matches(&config, &batch)
                            .map(|()| batch)
                            .map_err(|_| RetrievalError::Association);
                        work.job.complete(result);
                    }
                    TransportEvent::SidecarsUnavailable { peer: provider, id, code } => {
                        if sidecars.unavailable(provider, id)? {
                            targets.retain(|target| target.peer != provider);
                            if code == 3 {
                                // The exact response retired its token; refill its connection slot.
                                peer.retire_peer(provider)?;
                            }
                            tracing::debug!(target: "beacon::replay", code,
                                remaining_providers = targets.len(),
                                "[beacon] Recovery sidecar provider reported unavailable data");
                        } else {
                            tracing::debug!(target: "beacon::replay", peer = %provider, ?id,
                                "[beacon] Discarded response without its current recovery request");
                        }
                    }
                    TransportEvent::Closed(closed) |
                    TransportEvent::Rejected { peer: closed, .. } => {
                        targets.retain(|target| target.peer != closed);
                        sidecars.disconnected(closed)?;
                        envelopes.disconnected(closed)?;
                    }
                    TransportEvent::BlockRequest { id, .. } => {
                        peer.respond_blocks_unavailable(id)?
                    }
                    TransportEvent::EnvelopeRequest { id, .. } => {
                        peer.respond_envelopes_unavailable(id)?
                    }
                    TransportEvent::SidecarRequest { id, .. } => {
                        peer.respond_sidecars_unavailable(id)?
                    }
                    TransportEvent::Listening(_) |
                    TransportEvent::DialFailed(_) |
                    TransportEvent::PeerPing { .. } |
                    TransportEvent::PeerMetadata { .. } |
                    TransportEvent::ControlUnavailable { .. } => {}
                    TransportEvent::Gossip { id, .. } => {
                        peer.report_gossip(
                            id,
                            reth_embedded_beacon_network::transport::GossipVerdict::Ignore,
                        )?;
                    }
                    TransportEvent::Blocks { .. } | TransportEvent::BlocksUnavailable { .. } => {
                        return Err(LiveSyncError::Response)
                    }
                },
                StartupEvent::Peer(PeerRuntimeEvent::DiscoveryComplete { .. }) => {
                    discovery_pending = false;
                }
                StartupEvent::Peer(_) => {}
                StartupEvent::Tick => {
                    if !discovery_pending && discovery_started.elapsed() >= DISCOVERY_INTERVAL {
                        discovery_pending = discovery_scheduled(peer.request_discovery())?;
                        discovery_started = Instant::now();
                    }
                    if sidecars.timed_out() || envelopes.timed_out() {
                        return Err(LiveSyncError::RequestTimeout)
                    }
                }
            }
        };
        let (mut driver, cancel) = Self::new(peer, importer, availability, broker, config).await?;
        driver.connected = targets.iter().map(|target| target.peer).collect();
        driver.targets = targets;
        driver.discovery_pending = discovery_pending;
        driver.discovery_started = discovery_started;
        driver.publish_status().await?;
        Ok((driver, cancel))
    }
}

enum StartupEvent {
    Ready(Result<ImportCoordinator, ImportError>),
    Peer(PeerRuntimeEvent),
    Retrieval(RetrievalJob),
    EnvelopeRetrieval(EnvelopeRetrievalJob),
    Tick,
}
