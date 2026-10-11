//! Fixed-cardinality counts of actual live-driver network observations.
//! Received original SSZ is not wire bandwidth or a consensus/availability assertion.

use reth_embedded_beacon_network::transport::TransportEvent;
use std::sync::atomic::{AtomicU64, Ordering};

/// Actual transport and discovery events counted once when the live owner consumes them.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct NetworkObservationSnapshot {
    /// Completed signed discovery queries.
    pub discovery_completed_total: u64,
    /// Accepted or updated advertisements returned by discovery, not unique peers.
    pub discovery_candidates_total: u64,
    /// Discovery candidates rejected by bounded transport admission.
    pub candidate_rejected_total: u64,
    /// Completed authenticated status exchange events, including refreshes.
    pub status_exchanges_total: u64,
    /// Transport rejection events; no remote diagnostic text is retained.
    pub peers_rejected_total: u64,
    /// Final-connection close events.
    pub peers_closed_total: u64,
    /// Outbound dial failures before a status response.
    pub dial_failures_total: u64,
    /// Authenticated inbound block requests.
    pub block_requests_received_total: u64,
    /// Complete inbound block batches before native consensus validation.
    pub block_batches_received_total: u64,
    /// Complete original signed blocks received, including repeated retrievals.
    pub blocks_received_total: u64,
    /// Original received block SSZ bytes, excluding framing and compression.
    pub block_bytes_received_total: u64,
    /// Outbound block requests answered with an unavailable response.
    pub blocks_unavailable_total: u64,
    /// Authenticated inbound envelope requests.
    pub envelope_requests_received_total: u64,
    /// Complete envelope batches before builder, execution and consensus validation.
    pub envelope_batches_received_total: u64,
    /// Complete original signed envelopes received, including repeated retrievals.
    pub envelopes_received_total: u64,
    /// Original received envelope SSZ bytes, excluding framing and compression.
    pub envelope_bytes_received_total: u64,
    /// Outbound envelope requests answered with an unavailable response.
    pub envelopes_unavailable_total: u64,
    /// Authenticated inbound sidecar requests.
    pub sidecar_requests_received_total: u64,
    /// Structurally decoded sidecar batches before mandatory mathematical verification.
    pub sidecar_batches_received_total: u64,
    /// Complete original sidecars received, including repeated retrievals.
    pub sidecars_received_total: u64,
    /// Original received sidecar SSZ bytes, excluding framing and compression.
    pub sidecar_bytes_received_total: u64,
    /// Outbound sidecar requests answered with an unavailable response.
    pub sidecars_unavailable_total: u64,
    /// Authenticated remote ping response observations.
    pub pings_received_total: u64,
    /// Authenticated remote metadata response observations.
    pub metadata_received_total: u64,
    /// Authenticated control responses with a known unavailable code.
    pub control_unavailable_total: u64,
}

#[derive(Debug, Default)]
pub(super) struct NetworkObservations {
    discovery_completed_total: AtomicU64,
    discovery_candidates_total: AtomicU64,
    candidate_rejected_total: AtomicU64,
    status_exchanges_total: AtomicU64,
    peers_rejected_total: AtomicU64,
    peers_closed_total: AtomicU64,
    dial_failures_total: AtomicU64,
    block_requests_received_total: AtomicU64,
    block_batches_received_total: AtomicU64,
    blocks_received_total: AtomicU64,
    block_bytes_received_total: AtomicU64,
    blocks_unavailable_total: AtomicU64,
    envelope_requests_received_total: AtomicU64,
    envelope_batches_received_total: AtomicU64,
    envelopes_received_total: AtomicU64,
    envelope_bytes_received_total: AtomicU64,
    envelopes_unavailable_total: AtomicU64,
    sidecar_requests_received_total: AtomicU64,
    sidecar_batches_received_total: AtomicU64,
    sidecars_received_total: AtomicU64,
    sidecar_bytes_received_total: AtomicU64,
    sidecars_unavailable_total: AtomicU64,
    pings_received_total: AtomicU64,
    metadata_received_total: AtomicU64,
    control_unavailable_total: AtomicU64,
}

impl NetworkObservations {
    pub(super) fn snapshot(&self) -> NetworkObservationSnapshot {
        NetworkObservationSnapshot {
            discovery_completed_total: self.discovery_completed_total.load(Ordering::Relaxed),
            discovery_candidates_total: self.discovery_candidates_total.load(Ordering::Relaxed),
            candidate_rejected_total: self.candidate_rejected_total.load(Ordering::Relaxed),
            status_exchanges_total: self.status_exchanges_total.load(Ordering::Relaxed),
            peers_rejected_total: self.peers_rejected_total.load(Ordering::Relaxed),
            peers_closed_total: self.peers_closed_total.load(Ordering::Relaxed),
            dial_failures_total: self.dial_failures_total.load(Ordering::Relaxed),
            block_requests_received_total: self
                .block_requests_received_total
                .load(Ordering::Relaxed),
            block_batches_received_total: self.block_batches_received_total.load(Ordering::Relaxed),
            blocks_received_total: self.blocks_received_total.load(Ordering::Relaxed),
            block_bytes_received_total: self.block_bytes_received_total.load(Ordering::Relaxed),
            blocks_unavailable_total: self.blocks_unavailable_total.load(Ordering::Relaxed),
            envelope_requests_received_total: self
                .envelope_requests_received_total
                .load(Ordering::Relaxed),
            envelope_batches_received_total: self
                .envelope_batches_received_total
                .load(Ordering::Relaxed),
            envelopes_received_total: self.envelopes_received_total.load(Ordering::Relaxed),
            envelope_bytes_received_total: self
                .envelope_bytes_received_total
                .load(Ordering::Relaxed),
            envelopes_unavailable_total: self.envelopes_unavailable_total.load(Ordering::Relaxed),
            sidecar_requests_received_total: self
                .sidecar_requests_received_total
                .load(Ordering::Relaxed),
            sidecar_batches_received_total: self
                .sidecar_batches_received_total
                .load(Ordering::Relaxed),
            sidecars_received_total: self.sidecars_received_total.load(Ordering::Relaxed),
            sidecar_bytes_received_total: self.sidecar_bytes_received_total.load(Ordering::Relaxed),
            sidecars_unavailable_total: self.sidecars_unavailable_total.load(Ordering::Relaxed),
            pings_received_total: self.pings_received_total.load(Ordering::Relaxed),
            metadata_received_total: self.metadata_received_total.load(Ordering::Relaxed),
            control_unavailable_total: self.control_unavailable_total.load(Ordering::Relaxed),
        }
    }

    pub(super) fn discovery(&self, candidates: usize) {
        increment(&self.discovery_completed_total, 1);
        increment(&self.discovery_candidates_total, candidates as u64);
    }

    pub(super) fn candidate_rejected(&self) {
        increment(&self.candidate_rejected_total, 1);
    }

    pub(super) fn transport(&self, event: &TransportEvent) {
        match event {
            TransportEvent::PeerStatus { .. } => increment(&self.status_exchanges_total, 1),
            TransportEvent::Rejected { .. } => increment(&self.peers_rejected_total, 1),
            TransportEvent::Closed(_) => increment(&self.peers_closed_total, 1),
            TransportEvent::DialFailed(_) => increment(&self.dial_failures_total, 1),
            TransportEvent::BlockRequest { .. } => {
                increment(&self.block_requests_received_total, 1)
            }
            TransportEvent::Blocks { batch, .. } => {
                increment(&self.block_batches_received_total, 1);
                increment(&self.blocks_received_total, batch.blocks().len() as u64);
                increment(
                    &self.block_bytes_received_total,
                    batch
                        .blocks()
                        .iter()
                        .fold(0u64, |sum, block| sum.saturating_add(block.ssz().len() as u64)),
                );
            }
            TransportEvent::BlocksUnavailable { .. } => {
                increment(&self.blocks_unavailable_total, 1)
            }
            TransportEvent::EnvelopeRequest { .. } => {
                increment(&self.envelope_requests_received_total, 1)
            }
            TransportEvent::Envelopes { batch, .. } => {
                increment(&self.envelope_batches_received_total, 1);
                increment(&self.envelopes_received_total, batch.envelopes().len() as u64);
                increment(
                    &self.envelope_bytes_received_total,
                    batch.envelopes().iter().fold(0u64, |sum, envelope| {
                        sum.saturating_add(envelope.ssz().len() as u64)
                    }),
                );
            }
            TransportEvent::EnvelopesUnavailable { .. } => {
                increment(&self.envelopes_unavailable_total, 1)
            }
            TransportEvent::SidecarRequest { .. } => {
                increment(&self.sidecar_requests_received_total, 1)
            }
            TransportEvent::Sidecars { batch, .. } => {
                increment(&self.sidecar_batches_received_total, 1);
                increment(&self.sidecars_received_total, batch.sidecars().len() as u64);
                increment(
                    &self.sidecar_bytes_received_total,
                    batch
                        .sidecars()
                        .iter()
                        .fold(0u64, |sum, sidecar| sum.saturating_add(sidecar.ssz().len() as u64)),
                );
            }
            TransportEvent::SidecarsUnavailable { .. } => {
                increment(&self.sidecars_unavailable_total, 1)
            }
            TransportEvent::PeerPing { .. } => increment(&self.pings_received_total, 1),
            TransportEvent::PeerMetadata { .. } => increment(&self.metadata_received_total, 1),
            TransportEvent::ControlUnavailable { .. } => {
                increment(&self.control_unavailable_total, 1)
            }
            TransportEvent::Listening(_) | TransportEvent::Gossip { .. } => {}
        }
    }
}

fn increment(counter: &AtomicU64, amount: u64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
        Some(value.saturating_add(amount))
    });
}
