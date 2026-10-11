//! Bounded observational counters copied from the actual live owner.
//! These values confer no consensus, connection or recovery authority.

use super::availability::AvailabilityObservation;
use crate::import_coordinator::OwnerObservation;
use reth_embedded_beacon_network::transport::{GossipVerdict, TransportEvent};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};

mod network;

pub use network::NetworkObservationSnapshot;

/// Cloneable read-only network scheduling observations from the same actual owner.
#[derive(Clone, Debug, Default)]
pub struct PeerObservationHandle(Arc<Observations>);
impl PeerObservationHandle {
    /// Read current gauges; individual atomic fields can reflect adjacent owner turns.
    pub fn snapshot(&self) -> PeerObservationSnapshot {
        PeerObservationSnapshot {
            checked_targets: self.0.checked_targets.load(Ordering::Relaxed),
            connected_peers: self.0.connected_peers.load(Ordering::Relaxed),
            target_head_slot: self.0.target_head_slot.load(Ordering::Relaxed),
            running: self.0.running.load(Ordering::Relaxed) != 0,
            retrieval_requests: self.0.retrieval_requests.load(Ordering::Relaxed),
            active_imports: self.0.active_imports.load(Ordering::Relaxed),
            retrieval_next_slot: self.0.retrieval_next_slot.load(Ordering::Relaxed),
            pending_discovery: self.0.pending_discovery.load(Ordering::Relaxed),
            retained_ancestry_blocks: self.0.retained_ancestry_blocks.load(Ordering::Relaxed),
            retained_ancestry_bytes: self.0.retained_ancestry_bytes.load(Ordering::Relaxed),
            gossip_received: self.0.gossip_received.load(Ordering::Relaxed),
            gossip_accepted: self.0.gossip_accepted.load(Ordering::Relaxed),
            gossip_rejected: self.0.gossip_rejected.load(Ordering::Relaxed),
            gossip_ignored: self.0.gossip_ignored.load(Ordering::Relaxed),
            queued_gossip: self.0.queued_gossip.load(Ordering::Relaxed),
            active_gossip: self.0.active_gossip.load(Ordering::Relaxed),
            network: self.0.network.snapshot(),
            availability: AvailabilityObservation {
                retrieval_jobs: self.0.retrieval_jobs.load(Ordering::Relaxed),
                proof_jobs: self.0.proof_jobs.load(Ordering::Relaxed),
                history_jobs: self.0.history_jobs.load(Ordering::Relaxed),
                custody_retrieving: self.0.custody_retrieving.load(Ordering::Relaxed),
                deferred_data: self.0.deferred_data.load(Ordering::Relaxed),
                halted: self.0.availability_halted.load(Ordering::Relaxed),
                archive_enabled: self.0.archive_enabled.load(Ordering::Relaxed),
            },
        }
    }
    pub(super) fn update(
        &self,
        targets: usize,
        requests: usize,
        imports: bool,
        discovery: bool,
        blocks: usize,
        bytes: usize,
    ) {
        self.0.checked_targets.store(targets as u64, Ordering::Relaxed);
        self.0.retrieval_requests.store(requests as u64, Ordering::Relaxed);
        self.0.active_imports.store(u64::from(imports), Ordering::Relaxed);
        self.0.pending_discovery.store(u64::from(discovery), Ordering::Relaxed);
        self.0.retained_ancestry_blocks.store(blocks as u64, Ordering::Relaxed);
        self.0.retained_ancestry_bytes.store(bytes as u64, Ordering::Relaxed);
    }
    pub(super) fn connections(&self, connected: usize, hinted_slot: u64, running: bool) {
        self.0.connected_peers.store(connected as u64, Ordering::Relaxed);
        self.0.target_head_slot.store(hinted_slot, Ordering::Relaxed);
        self.0.running.store(u64::from(running), Ordering::Relaxed);
    }
    pub(super) fn transport(&self, event: &TransportEvent) {
        self.0.network.transport(event);
    }
    pub(super) fn discovery(&self, candidates: usize) {
        self.0.network.discovery(candidates);
    }
    pub(super) fn candidate_rejected(&self) {
        self.0.network.candidate_rejected();
    }
    pub(super) fn work(
        &self,
        queued_gossip: usize,
        active_gossip: bool,
        availability: AvailabilityObservation,
    ) {
        self.0.queued_gossip.store(queued_gossip as u64, Ordering::Relaxed);
        self.0.active_gossip.store(u64::from(active_gossip), Ordering::Relaxed);
        self.0.retrieval_jobs.store(availability.retrieval_jobs, Ordering::Relaxed);
        self.0.proof_jobs.store(availability.proof_jobs, Ordering::Relaxed);
        self.0.history_jobs.store(availability.history_jobs, Ordering::Relaxed);
        self.0.custody_retrieving.store(availability.custody_retrieving, Ordering::Relaxed);
        self.0.deferred_data.store(availability.deferred_data, Ordering::Relaxed);
        self.0.availability_halted.store(availability.halted, Ordering::Relaxed);
        self.0.archive_enabled.store(availability.archive_enabled, Ordering::Relaxed);
    }
    pub(super) fn received(&self) {
        self.0.gossip_received.fetch_add(1, Ordering::Relaxed);
    }
    pub(super) fn verdict(&self, verdict: reth_embedded_beacon_network::transport::GossipVerdict) {
        match verdict {
            GossipVerdict::Accept => &self.0.gossip_accepted,
            GossipVerdict::Reject => &self.0.gossip_rejected,
            GossipVerdict::Ignore => &self.0.gossip_ignored,
        }
        .fetch_add(1, Ordering::Relaxed);
    }
    pub(super) fn retrieval_cursor(&self, next_slot: u64) {
        self.0.retrieval_next_slot.store(next_slot, Ordering::Relaxed);
    }
}

/// Current actual scheduling counts and cumulative native gossip decisions.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PeerObservationSnapshot {
    /// Checked peer advertisements currently retained as retrieval targets.
    pub checked_targets: u64,
    /// Connected authenticated peers whose status completed; suppressed targets remain counted.
    pub connected_peers: u64,
    /// Maximum advertised target slot, a retrieval hint with no canonical authority.
    pub target_head_slot: u64,
    /// The actual driver is currently running; false after its IO shuts down.
    pub running: bool,
    /// Outstanding exact block, envelope and sidecar transport requests.
    pub retrieval_requests: u64,
    /// One active strict import owner, or zero.
    pub active_imports: u64,
    /// Next slot examined by forward retrieval; this is not the canonical head.
    pub retrieval_next_slot: u64,
    /// One actual reserved discovery query, or zero.
    pub pending_discovery: u64,
    /// Original blocks held by the bounded competing-branch ancestry queue.
    pub retained_ancestry_blocks: u64,
    /// Complete original SSZ bytes held by the ancestry queue.
    pub retained_ancestry_bytes: u64,
    /// Delivered wire observations; receipt grants no native validity.
    pub gossip_received: u64,
    /// Native validation and any required canonical publication completed successfully.
    pub gossip_accepted: u64,
    /// Definitively rejected native consensus messages.
    pub gossip_rejected: u64,
    /// Duplicate, missing-dependency or resource/infrastructure decisions.
    pub gossip_ignored: u64,
    /// Original gossip deliveries retained in the bounded queue.
    pub queued_gossip: u64,
    /// Native gossip validation futures currently running, either zero or one.
    pub active_gossip: u64,
    /// Actual received discovery and transport events, without per-peer labels.
    pub network: NetworkObservationSnapshot,
    /// Existing availability lease occupancy sampled by the live owner.
    pub availability: AvailabilityObservation,
}

impl PeerObservationSnapshot {
    /// Distance from the authenticated head to the clock or a peer retrieval hint.
    /// Peer hints confer no canonical authority.
    pub fn sync_distance(&self, owner: &OwnerObservation) -> u64 {
        owner.current_slot.max(self.target_head_slot).saturating_sub(owner.snapshot.head_slot)
    }

    /// Observational synchronization status shared by the Beacon API and telemetry.
    pub fn is_syncing(&self, owner: &OwnerObservation) -> bool {
        !self.running ||
            self.sync_distance(owner) > 2 ||
            self.retrieval_requests != 0 ||
            self.active_imports != 0 ||
            owner.head_optimistic ||
            !owner.execution_acknowledged ||
            owner.engine_closed
    }
}

#[derive(Debug, Default)]
struct Observations {
    checked_targets: AtomicU64,
    connected_peers: AtomicU64,
    target_head_slot: AtomicU64,
    running: AtomicU64,
    retrieval_requests: AtomicU64,
    active_imports: AtomicU64,
    retrieval_next_slot: AtomicU64,
    pending_discovery: AtomicU64,
    retained_ancestry_blocks: AtomicU64,
    retained_ancestry_bytes: AtomicU64,
    gossip_received: AtomicU64,
    gossip_accepted: AtomicU64,
    gossip_rejected: AtomicU64,
    gossip_ignored: AtomicU64,
    queued_gossip: AtomicU64,
    active_gossip: AtomicU64,
    network: network::NetworkObservations,
    retrieval_jobs: AtomicU64,
    proof_jobs: AtomicU64,
    history_jobs: AtomicU64,
    custody_retrieving: AtomicBool,
    deferred_data: AtomicBool,
    availability_halted: AtomicBool,
    archive_enabled: AtomicBool,
}
