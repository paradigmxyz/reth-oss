//! Owned bounded gossip observations and single-use propagation callbacks.

use super::*;
use crate::{
    custody::CustodyParameters,
    gossip::{
        active_topics, decode_gossip, parse_topic, GossipKind, GossipTopic, GOSSIP_VALIDATION_TIMEOUT,
    },
};
use libp2p_gossipsub::{self as gossipsub, IdentTopic, MessageAcceptance, MessageId};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const MAX_GOSSIP_PENDING: usize = 16;
const MAX_GOSSIP_RETAINED_BYTES: u32 = 32 * 1024 * 1024;

/// Complete original raw bytes and decoded SSZ under one shared allocation reservation.
/// Possession is wire evidence only; native signatures, execution and DA remain mandatory.
#[derive(Clone, Debug)]
pub struct GossipInput {
    topic: GossipTopic,
    ssz: Arc<[u8]>,
    raw: Arc<[u8]>,
    _lease: Arc<GossipLease>,
}
impl GossipInput {
    /// Exact configured topic, schema and network context.
    pub const fn topic(&self) -> GossipTopic {
        self.topic
    }
    /// Complete SSZ without dropping the original raw wire representation.
    pub fn ssz(&self) -> &[u8] {
        &self.ssz
    }
    /// Original block-compressed Snappy, retained until validation resolves.
    pub fn raw(&self) -> &[u8] {
        &self.raw
    }
}
impl PartialEq for GossipInput {
    fn eq(&self, other: &Self) -> bool {
        self.topic == other.topic && self.ssz == other.ssz && self.raw == other.raw
    }
}
impl Eq for GossipInput {}

#[derive(Debug)]
struct GossipLease {
    _bytes: OwnedSemaphorePermit,
}

/// Opaque validation token scoped to one transport and never reused.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct GossipRequestId {
    owner: PeerId,
    sequence: u64,
}

/// Native validation decision controlling forwarding independently of imported authority.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GossipVerdict {
    /// Mandatory native gossip and import/proof checks passed.
    Accept,
    /// Definitive malformed or cryptographically invalid consensus input.
    Reject,
    /// Duplicate, missing dependency, local resource or infrastructure unavailability.
    Ignore,
}
impl GossipVerdict {
    const fn acceptance(self) -> MessageAcceptance {
        match self {
            Self::Accept => MessageAcceptance::Accept,
            Self::Reject => MessageAcceptance::Reject,
            Self::Ignore => MessageAcceptance::Ignore,
        }
    }
}

#[derive(Debug)]
pub(super) struct PendingGossip {
    source: PeerId,
    message: MessageId,
    started: tokio::time::Instant,
    _lease: Arc<GossipLease>,
}

impl StatusTransport {
    /// Resolve exactly one native validation callback. Unknown or reused tokens fail closed.
    /// Cache expiry means no forwarding, even for a valid object.
    pub fn report_gossip(
        &mut self,
        id: GossipRequestId,
        verdict: GossipVerdict,
    ) -> Result<bool, TransportError> {
        let pending = self.gossip_pending.remove(&id).ok_or(TransportError::Configuration)?;
        Ok(self.swarm.behaviour_mut().gossip.report_message_validation_result(
            &pending.message,
            &pending.source,
            verdict.acceptance(),
        ))
    }

    pub(super) fn handle_gossip(&mut self, event: gossipsub::Event) -> Option<TransportEvent> {
        let event = match event {
            gossipsub::Event::SlowPeer { peer_id, .. } => {
                // Native queue failure is a local capacity decision. Retire outstanding wire
                // channels and disconnect through the existing path without consensus blame.
                self.reject(peer_id);
                return Some(TransportEvent::Rejected {
                    peer: peer_id,
                    reason: StatusRejection::Capacity,
                })
            }
            event => event,
        };
        let gossipsub::Event::Message { propagation_source, message_id, message } = event else {
            return None
        };
        let topic = parse_topic(&self.config, message.topic.as_str()).ok();
        let admitted = self.checked_peers.contains(&propagation_source) &&
            topic.is_some_and(|topic| self.gossip_topics.contains(&topic.name())) &&
            self.gossip_pending.len() < MAX_GOSSIP_PENDING;
        if !admitted {
            self.swarm.behaviour_mut().gossip.report_message_validation_result(
                &message_id,
                &propagation_source,
                MessageAcceptance::Ignore,
            );
            return None
        }
        let length = match snap::raw::decompress_len(&message.data) {
            Ok(length)
                if length <= self.config.max_payload_size.min(crate::gossip::MAX_GOSSIP_BYTES) =>
            {
                length
            }
            _ => {
                self.swarm.behaviour_mut().gossip.report_message_validation_result(
                    &message_id,
                    &propagation_source,
                    MessageAcceptance::Reject,
                );
                return None
            }
        };
        let bytes =
            length.checked_add(message.data.len()).and_then(|bytes| u32::try_from(bytes).ok());
        let lease = bytes
            .and_then(|bytes| self.gossip_bytes.clone().try_acquire_many_owned(bytes.max(1)).ok());
        let Some(lease) = lease else {
            self.swarm.behaviour_mut().gossip.report_message_validation_result(
                &message_id,
                &propagation_source,
                MessageAcceptance::Ignore,
            );
            return None
        };
        let decoded = match decode_gossip(&message.data, self.config.max_payload_size) {
            Ok(decoded) => decoded,
            Err(_) => {
                self.swarm.behaviour_mut().gossip.report_message_validation_result(
                    &message_id,
                    &propagation_source,
                    MessageAcceptance::Reject,
                );
                return None
            }
        };
        let Some(sequence) = self.next_gossip_request.checked_add(1) else {
            self.swarm.behaviour_mut().gossip.report_message_validation_result(
                &message_id,
                &propagation_source,
                MessageAcceptance::Ignore,
            );
            return None
        };
        self.next_gossip_request = sequence;
        let id = GossipRequestId { owner: self.peer_id(), sequence };
        let lease = Arc::new(GossipLease { _bytes: lease });
        let input = GossipInput {
            topic: topic.expect("admitted configured topic"),
            ssz: Arc::from(decoded),
            raw: Arc::from(message.data),
            _lease: lease.clone(),
        };
        self.gossip_pending.insert(
            id,
            PendingGossip {
                source: propagation_source,
                message: message_id,
                started: tokio::time::Instant::now(),
                _lease: lease,
            },
        );
        Some(TransportEvent::Gossip { peer: propagation_source, id, input })
    }

    pub(super) fn update_gossip_topics(&mut self, slot: u64) -> Result<(), TransportError> {
        let mut topics =
            active_topics(&self.config, slot).map_err(|_| TransportError::Configuration)?;
        if let Some(columns) = self.custody_columns {
            let contexts = topics
                .iter()
                .filter(|topic| topic.fork >= Fork::Fulu && topic.kind == GossipKind::Block)
                .copied()
                .collect::<Vec<_>>();
            for context in contexts {
                topics.extend(
                    columns
                        .into_iter()
                        .map(|column| GossipTopic { kind: GossipKind::Column(column), ..context }),
                );
            }
        }
        let names = topics.iter().map(|topic| topic.name()).collect::<HashSet<_>>();
        for name in names.difference(&self.gossip_topics) {
            self.swarm
                .behaviour_mut()
                .gossip
                .subscribe(&IdentTopic::new(name.clone()))
                .map_err(|_| TransportError::Configuration)?;
        }
        for name in self.gossip_topics.difference(&names) {
            self.swarm.behaviour_mut().gossip.unsubscribe(&IdentTopic::new(name.clone()));
        }
        self.gossip_topics = names;
        Ok(())
    }
    /// Join the four deterministic groups of the actual signed discovery identity. Joining
    /// does not advertise retention; custody metadata requires a separate native proof receipt.
    pub(crate) fn join_custody_columns(&mut self, mut node_id: Root) -> Result<(), TransportError> {
        node_id.reverse();
        let columns = CustodyParameters::default()
            .get_custody_groups(node_id, 4)
            .map_err(|_| TransportError::Configuration)?;
        self.custody_columns = Some(columns.try_into().map_err(|_| TransportError::Configuration)?);
        self.update_gossip_topics(self.current_slot)
    }
    pub(super) fn expire_gossip(&mut self) {
        let expired = self
            .gossip_pending
            .iter()
            .filter(|(_, pending)| pending.started.elapsed() >= GOSSIP_VALIDATION_TIMEOUT)
            .map(|(id, _)| *id)
            .collect::<Vec<_>>();
        for id in expired {
            let _ = self.report_gossip(id, GossipVerdict::Ignore);
        }
    }
    pub(super) fn reject_gossip_peer(&mut self, peer: PeerId) {
        let ids = self
            .gossip_pending
            .iter()
            .filter(|(_, pending)| pending.source == peer)
            .map(|(id, _)| *id)
            .collect::<Vec<_>>();
        for id in ids {
            let _ = self.report_gossip(id, GossipVerdict::Ignore);
        }
    }
    pub(super) fn gossip_budget() -> Arc<Semaphore> {
        Arc::new(Semaphore::new(MAX_GOSSIP_RETAINED_BYTES as usize))
    }
}

#[cfg(test)]
mod tests;
