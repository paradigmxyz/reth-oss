//! Fixed-cardinality observations from the actual consensus network owner.

use reth_embedded_beacon_node::live_sync::PeerObservationSnapshot;
use reth_metrics::{
    metrics::{self, Counter, Gauge},
    Metrics,
};

#[derive(Metrics)]
#[metrics(scope = "embedded_beacon.network")]
pub(super) struct NetworkMetrics {
    /// Cumulative discovery completed since this owner started.
    discovery_completed_total: Counter,
    /// Cumulative discovery candidates since this owner started.
    discovery_candidates_total: Counter,
    /// Cumulative candidate rejected since this owner started.
    candidate_rejected_total: Counter,
    /// Cumulative status exchanges since this owner started.
    status_exchanges_total: Counter,
    /// Cumulative peers rejected since this owner started.
    peers_rejected_total: Counter,
    /// Cumulative peers closed since this owner started.
    peers_closed_total: Counter,
    /// Cumulative dial failures since this owner started.
    dial_failures_total: Counter,
    /// Cumulative block requests received since this owner started.
    block_requests_received_total: Counter,
    /// Cumulative envelope requests received since this owner started.
    envelope_requests_received_total: Counter,
    /// Cumulative sidecar requests received since this owner started.
    sidecar_requests_received_total: Counter,
    /// Cumulative block batches received since this owner started.
    block_batches_received_total: Counter,
    /// Cumulative envelope batches received since this owner started.
    envelope_batches_received_total: Counter,
    /// Cumulative sidecar batches received since this owner started.
    sidecar_batches_received_total: Counter,
    /// Cumulative blocks received since this owner started.
    blocks_received_total: Counter,
    /// Cumulative envelopes received since this owner started.
    envelopes_received_total: Counter,
    /// Cumulative sidecars received since this owner started.
    sidecars_received_total: Counter,
    /// Cumulative block bytes received since this owner started.
    block_bytes_received_total: Counter,
    /// Cumulative envelope bytes received since this owner started.
    envelope_bytes_received_total: Counter,
    /// Cumulative sidecar bytes received since this owner started.
    sidecar_bytes_received_total: Counter,
    /// Cumulative blocks unavailable since this owner started.
    blocks_unavailable_total: Counter,
    /// Cumulative envelopes unavailable since this owner started.
    envelopes_unavailable_total: Counter,
    /// Cumulative sidecars unavailable since this owner started.
    sidecars_unavailable_total: Counter,
    /// Cumulative pings received since this owner started.
    pings_received_total: Counter,
    /// Cumulative metadata received since this owner started.
    metadata_received_total: Counter,
    /// Cumulative control unavailable since this owner started.
    control_unavailable_total: Counter,
    /// Original gossip items currently waiting in the bounded driver queue.
    queued_gossip: Gauge,
    /// Whether the driver currently owns native gossip validation work.
    active_gossip: Gauge,
    /// Current measured retrieval jobs.
    retrieval_jobs: Gauge,
    /// Current measured proof jobs.
    proof_jobs: Gauge,
    /// Current measured history jobs.
    history_jobs: Gauge,
    /// Whether original custody data retrieval is currently active.
    custody_retrieving: Gauge,
    /// Whether the actual availability owner has deferred original data.
    deferred_data: Gauge,
    /// Whether the actual data-availability owner has halted.
    halted: Gauge,
    /// Whether the actual bounded history archive is configured.
    archive_enabled: Gauge,
}

impl NetworkMetrics {
    pub(super) fn record(&self, observation: &PeerObservationSnapshot) {
        let network = observation.network;
        self.discovery_completed_total.absolute(network.discovery_completed_total);
        self.discovery_candidates_total.absolute(network.discovery_candidates_total);
        self.candidate_rejected_total.absolute(network.candidate_rejected_total);
        self.status_exchanges_total.absolute(network.status_exchanges_total);
        self.peers_rejected_total.absolute(network.peers_rejected_total);
        self.peers_closed_total.absolute(network.peers_closed_total);
        self.dial_failures_total.absolute(network.dial_failures_total);
        self.block_requests_received_total.absolute(network.block_requests_received_total);
        self.envelope_requests_received_total.absolute(network.envelope_requests_received_total);
        self.sidecar_requests_received_total.absolute(network.sidecar_requests_received_total);
        self.block_batches_received_total.absolute(network.block_batches_received_total);
        self.envelope_batches_received_total.absolute(network.envelope_batches_received_total);
        self.sidecar_batches_received_total.absolute(network.sidecar_batches_received_total);
        self.blocks_received_total.absolute(network.blocks_received_total);
        self.envelopes_received_total.absolute(network.envelopes_received_total);
        self.sidecars_received_total.absolute(network.sidecars_received_total);
        self.block_bytes_received_total.absolute(network.block_bytes_received_total);
        self.envelope_bytes_received_total.absolute(network.envelope_bytes_received_total);
        self.sidecar_bytes_received_total.absolute(network.sidecar_bytes_received_total);
        self.blocks_unavailable_total.absolute(network.blocks_unavailable_total);
        self.envelopes_unavailable_total.absolute(network.envelopes_unavailable_total);
        self.sidecars_unavailable_total.absolute(network.sidecars_unavailable_total);
        self.pings_received_total.absolute(network.pings_received_total);
        self.metadata_received_total.absolute(network.metadata_received_total);
        self.control_unavailable_total.absolute(network.control_unavailable_total);
        self.queued_gossip.set(observation.queued_gossip as f64);
        self.active_gossip.set(observation.active_gossip as f64);
        let availability = observation.availability;
        self.retrieval_jobs.set(availability.retrieval_jobs as f64);
        self.proof_jobs.set(availability.proof_jobs as f64);
        self.history_jobs.set(availability.history_jobs as f64);
        self.custody_retrieving.set(u8::from(availability.custody_retrieving) as f64);
        self.deferred_data.set(u8::from(availability.deferred_data) as f64);
        self.halted.set(u8::from(availability.halted) as f64);
        self.archive_enabled.set(u8::from(availability.archive_enabled) as f64);
    }
}
