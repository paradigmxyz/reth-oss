//! Observational telemetry for the optional embedded beacon.
//! Measurements never grant consensus, execution or finality authority.

use super::presentation::{ForkBanner, HeadObservation};
use reth_embedded_beacon_node::{
    import_coordinator::{ImportCoordinator, Observation, ObservationId, ObservationRequest},
    live_sync::PeerObservationHandle,
};
use reth_metrics::{
    metrics::{self, Counter, Gauge, Histogram},
    Metrics,
};
use std::time::{Duration, Instant};

mod canonical;
mod imports;
mod network;
mod resource_metrics;
mod resources;
mod shared;
mod transport;

pub(super) use canonical::install;
pub(super) use resource_metrics::observe_resources;
pub(super) use resources::{ResourceCollector, StoragePaths};
pub(super) use shared::observe_shared;
pub(super) use transport::observe_transport;

/// Fixed-name gauges measure actual importer state; peer head advertisements remain hints.
#[derive(Metrics)]
#[metrics(scope = "embedded_beacon.consensus")]
struct ConsensusMetrics {
    /// One only while the latest owner observation completed within its deadline.
    observation_available: Gauge,
    /// The importer has closed admission after an uncertain or fatal failure.
    importer_halted: Gauge,
    /// The actual typed Engine adapter has closed admission.
    engine_closed: Gauge,
    /// Checked advertisements currently retained as retrieval hints.
    checked_targets: Gauge,
    /// Maximum advertised peer slot; this never identifies canonical chain state.
    target_head_slot: Gauge,
    /// Whether the actual peer driver is running.
    driver_running: Gauge,
    /// An actual reserved discovery query is outstanding.
    pending_discovery: Gauge,
    /// Original blocks held in the bounded ancestry queue.
    retained_ancestry_blocks: Gauge,
    /// Slot of the authenticated consensus head.
    head_slot: Gauge,
    /// Current slot derived from the configured network clock.
    current_slot: Gauge,
    /// Epoch of the native finalized checkpoint.
    finalized_epoch: Gauge,
    /// Epoch of the native justified checkpoint.
    justified_epoch: Gauge,
    /// Clock or peer-hint slots beyond the authenticated consensus head.
    sync_distance: Gauge,
    /// Whether the current published execution update was acknowledged.
    execution_acknowledged: Gauge,
    /// Whether the selected head still lacks execution verification.
    head_optimistic: Gauge,
    /// Currently connected checked consensus peers.
    connected_peers: Gauge,
    /// Retrieval requests retained by the bounded live owner.
    retrieval_requests: Gauge,
    /// Native imports currently owned by live synchronization.
    active_imports: Gauge,
    /// Next forward-retrieval slot, independently of the canonical head.
    retrieval_next_slot: Gauge,
    /// Importer command permits currently reserved.
    reserved_commands: Gauge,
    /// Original input bytes retained by importer reservations.
    reserved_input_bytes: Gauge,
    /// Original ancestry bytes retained by live synchronization.
    retained_ancestry_bytes: Gauge,
    /// Native states resident in the fork-choice graph.
    graph_hot_states: Gauge,
    /// Cold graph records retained for authenticated ancestry.
    graph_cold_entries: Gauge,
    /// Estimated resident schema bytes in hot graph states.
    graph_resident_schema_bytes: Gauge,
    /// Authenticated source states retained by the importer.
    source_retained_states: Gauge,
    /// Estimated schema bytes in the authenticated source cache.
    source_cached_schema_bytes: Gauge,
    /// Estimated source schema bytes resident in memory.
    source_resident_schema_bytes: Gauge,
    /// Authenticated head changes observed by this subscriber.
    observed_head_changes: Counter,
    /// Authenticated Fulu-to-Gloas transitions observed by the embedded terminal subscriber.
    observed_gloas_transitions: Counter,
    /// Owner observations that failed or exceeded their deadline.
    observation_errors: Counter,
    /// Original gossip deliveries observed by the live owner.
    gossip_received: Counter,
    /// Gossip deliveries accepted after native validation.
    gossip_accepted: Counter,
    /// Gossip deliveries rejected by native validation.
    gossip_rejected: Counter,
    /// Gossip deliveries ignored without acceptance.
    gossip_ignored: Counter,
    /// Time spent obtaining and recording an owner observation.
    observation_seconds: Histogram,
}

/// Poll the actual bounded owner without adding another database reader or import queue.
/// The launcher joins/drops this future with consensus and execution shutdown.
pub(super) async fn observe_consensus(
    importer: ImportCoordinator,
    peers: PeerObservationHandle,
    mut fork_banner: ForkBanner,
) {
    let metrics = ConsensusMetrics::default();
    let imports = imports::ImportMetrics::default();
    let network_metrics = network::NetworkMetrics::default();
    let mut tick = tokio::time::interval(Duration::from_secs(5));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut previous_head = None;
    let mut previous_gossip = [0u64; 4];
    let mut previous_execution = None;
    let mut previous_syncing = None;
    let mut next_status = Instant::now();
    loop {
        tick.tick().await;
        let started = Instant::now();
        metrics.importer_halted.set(u8::from(importer.is_halted()) as f64);
        let network = peers.snapshot();
        network_metrics.record(&network);
        metrics.connected_peers.set(network.connected_peers as f64);
        metrics.retrieval_requests.set(network.retrieval_requests as f64);
        metrics.active_imports.set(network.active_imports as f64);
        metrics.retrieval_next_slot.set(network.retrieval_next_slot as f64);
        metrics.checked_targets.set(network.checked_targets as f64);
        metrics.target_head_slot.set(network.target_head_slot as f64);
        metrics.driver_running.set(u8::from(network.running) as f64);
        metrics.pending_discovery.set(network.pending_discovery as f64);
        metrics.retained_ancestry_blocks.set(network.retained_ancestry_blocks as f64);
        match tokio::time::timeout(
            Duration::from_secs(2),
            importer.observe(ObservationRequest::Status),
        )
        .await
        {
            Ok(Ok(Observation::Status(owner))) => {
                metrics.observation_available.set(1.0);
                metrics.engine_closed.set(u8::from(owner.engine_closed) as f64);
                let network = peers.snapshot();
                let sync_distance = network.sync_distance(&owner);
                let is_syncing = network.is_syncing(&owner);
                imports.record(owner.imports, importer.queue_observation());
                let snapshot = owner.snapshot;
                metrics.head_slot.set(snapshot.head_slot as f64);
                metrics.current_slot.set(owner.current_slot as f64);
                metrics.finalized_epoch.set(snapshot.finalized_checkpoint.epoch as f64);
                metrics.justified_epoch.set(snapshot.justified_checkpoint.epoch as f64);
                metrics.sync_distance.set(sync_distance as f64);
                metrics.execution_acknowledged.set(u8::from(owner.execution_acknowledged) as f64);
                metrics.head_optimistic.set(u8::from(owner.head_optimistic) as f64);
                metrics.connected_peers.set(network.connected_peers as f64);
                metrics.retrieval_requests.set(network.retrieval_requests as f64);
                metrics.active_imports.set(network.active_imports as f64);
                metrics.retrieval_next_slot.set(network.retrieval_next_slot as f64);
                let (reserved_commands, reserved_bytes) = importer.reserved_resources();
                metrics.reserved_commands.set(reserved_commands as f64);
                metrics.reserved_input_bytes.set(reserved_bytes as f64);
                metrics.retained_ancestry_bytes.set(network.retained_ancestry_bytes as f64);
                metrics.graph_hot_states.set(owner.graph.hot_states as f64);
                metrics.graph_cold_entries.set(owner.graph.cold_entries as f64);
                metrics
                    .graph_resident_schema_bytes
                    .set(owner.graph.resident_hot_schema_bytes as f64);
                metrics.source_retained_states.set(owner.source.retained_states as f64);
                metrics.source_cached_schema_bytes.set(owner.source.cached_schema_bytes as f64);
                metrics.source_resident_schema_bytes.set(owner.source.resident_schema_bytes as f64);
                let actual_gossip = [
                    network.gossip_received,
                    network.gossip_accepted,
                    network.gossip_rejected,
                    network.gossip_ignored,
                ];
                for (index, counter) in [
                    &metrics.gossip_received,
                    &metrics.gossip_accepted,
                    &metrics.gossip_rejected,
                    &metrics.gossip_ignored,
                ]
                .into_iter()
                .enumerate()
                {
                    counter.increment(actual_gossip[index].saturating_sub(previous_gossip[index]));
                }
                previous_gossip = actual_gossip;
                if previous_head != Some(snapshot.head) {
                    metrics.observed_head_changes.increment(1);
                    tracing::info!(target: "beacon::consensus", slot = snapshot.head_slot,
                        root = %alloy_primitives::B256::from(snapshot.head.root), payload_status = ?snapshot.head.payload_status,
                        finalized_epoch = snapshot.finalized_checkpoint.epoch,
                        justified_epoch = snapshot.justified_checkpoint.epoch,
                        current_slot = owner.current_slot, peers = network.connected_peers,
                        execution_acknowledged = owner.execution_acknowledged,
                        optimistic = owner.head_optimistic, "[beacon] Observed authenticated consensus head");
                    previous_head = Some(snapshot.head);
                }
                let execution = (owner.execution_acknowledged, owner.head_optimistic);
                if previous_execution != Some(execution) {
                    tracing::info!(target: "beacon::consensus",
                        slot = snapshot.head_slot,
                        execution_acknowledged = owner.execution_acknowledged,
                        execution_optimistic = owner.head_optimistic,
                        engine_closed = owner.engine_closed,
                        "[beacon] Consensus execution status changed");
                    previous_execution = Some(execution);
                }
                if previous_syncing != Some(is_syncing) || Instant::now() >= next_status {
                    tracing::info!(target: "beacon::consensus",
                        is_syncing,
                        head_slot = snapshot.head_slot,
                        current_slot = owner.current_slot,
                        sync_distance,
                        peers = network.connected_peers,
                        active_imports = network.active_imports,
                        retrieval_next_slot = network.retrieval_next_slot,
                        retrieval_requests = network.retrieval_requests,
                        execution_acknowledged = owner.execution_acknowledged,
                        is_optimistic = owner.head_optimistic,
                        el_offline = owner.engine_closed,
                        "[beacon] Consensus synchronization status");
                    previous_syncing = Some(is_syncing);
                    next_status = Instant::now() + Duration::from_secs(30);
                }
                let head = HeadObservation {
                    root: snapshot.head.root,
                    slot: snapshot.head_slot,
                    execution_acknowledged: owner.execution_acknowledged,
                    engine_closed: owner.engine_closed,
                    optimistic: owner.head_optimistic,
                };
                if fork_banner.wants_header(head) &&
                    let Ok(Ok(Observation::Object(Some(header)))) = tokio::time::timeout(
                        Duration::from_secs(2),
                        importer.observe(ObservationRequest::Block {
                            id: ObservationId::Root(head.root),
                            header_only: true,
                        }),
                    )
                    .await &&
                    fork_banner.observe(head, &header)
                {
                    metrics.observed_gloas_transitions.increment(1);
                    tracing::info!(target: "beacon::consensus", fork = "gloas",
                        slot = head.slot, root = ?head.root,
                        "[beacon] Observed authenticated Gloas fork transition");
                    if tracing::enabled!(target: "beacon::consensus", tracing::Level::INFO) &&
                        let Err(error) = fork_banner.write(&mut std::io::stdout().lock())
                    {
                        tracing::debug!(target: "beacon::consensus", kind = ?error.kind(),
                            "[beacon] Fork banner output unavailable");
                    }
                }
                tracing::debug!(target: "beacon::consensus", ?network,
                    graph = ?owner.graph, source = ?owner.source, elapsed = ?started.elapsed(),
                    "[beacon] Measured bounded consensus owners");
            }
            outcome => {
                metrics.observation_available.set(0.0);
                metrics.observation_errors.increment(1);
                tracing::debug!(target: "beacon::consensus", ?outcome,
                    halted = importer.is_halted(), "[beacon] Consensus telemetry observation unavailable");
            }
        }
        metrics.observation_seconds.record(started.elapsed().as_secs_f64());
    }
}
