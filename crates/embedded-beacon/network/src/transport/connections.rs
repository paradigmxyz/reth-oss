//! Bounded exact connection ownership distinguishes retired RPC events from a new same-peer owner.

use super::*;
use libp2p::swarm::ConnectionId;
use std::collections::VecDeque;

#[derive(Default)]
pub(super) struct ConnectionInventory {
    active: HashMap<ConnectionId, PeerId>,
    retired: VecDeque<(ConnectionId, PeerId)>,
}

impl ConnectionInventory {
    pub(super) fn established(&mut self, connection: ConnectionId, peer: PeerId) -> bool {
        if self.active.get(&connection) == Some(&peer) {
            return true;
        }
        if self.active.len() >= MAX_PEERS ||
            self.active.values().filter(|known| **known == peer).count() >= 2
        {
            return false;
        }
        self.active.insert(connection, peer);
        self.retired.retain(|(known, _)| *known != connection);
        true
    }

    pub(super) fn closed(&mut self, connection: ConnectionId, peer: PeerId) -> bool {
        if self.active.get(&connection) == Some(&peer) {
            self.active.remove(&connection);
            self.remember(connection, peer);
        } else if !self.was_retired(connection, peer) {
            return false;
        }
        !self.active.values().any(|known| *known == peer)
    }

    pub(super) fn retire_peer(&mut self, peer: PeerId) {
        let owned = self
            .active
            .iter()
            .filter_map(|(connection, known)| (*known == peer).then_some(*connection))
            .collect::<Vec<_>>();
        for connection in owned {
            self.active.remove(&connection);
            self.remember(connection, peer);
        }
    }

    fn remember(&mut self, connection: ConnectionId, peer: PeerId) {
        self.retired.retain(|(known, _)| *known != connection);
        if self.retired.len() == MAX_PEERS {
            self.retired.pop_front();
        }
        self.retired.push_back((connection, peer));
    }

    fn was_retired(&self, connection: ConnectionId, peer: PeerId) -> bool {
        self.retired.iter().any(|entry| *entry == (connection, peer))
    }

    pub(super) fn clear(&mut self) {
        self.active.clear();
        self.retired.clear();
    }
}

impl StatusTransport {
    pub(super) fn admit_rpc_event<R, S>(
        &mut self,
        event: &request_response::Event<R, S>,
        original_peer: Option<PeerId>,
    ) -> Result<bool, TransportEvent> {
        let (peer, connection) = match event {
            request_response::Event::Message { peer, connection_id, .. } |
            request_response::Event::OutboundFailure { peer, connection_id, .. } |
            request_response::Event::InboundFailure { peer, connection_id, .. } |
            request_response::Event::ResponseSent { peer, connection_id, .. } => {
                (*peer, *connection_id)
            }
        };
        if let Some(owner) = self.connections.active.get(&connection) {
            if *owner != peer ||
                original_peer.is_some_and(|original| original != peer) ||
                (matches!(event, request_response::Event::OutboundFailure { .. }) &&
                    original_peer.is_none())
            {
                self.reject(peer);
                return Err(TransportEvent::Rejected { peer, reason: StatusRejection::Protocol });
            }
            return Ok(true);
        }
        if self.connections.was_retired(connection, peer) {
            // A partial disconnect may still own an exact original request on the other
            // connection. Preserve its ordinary failure/reply policy instead of leaking it.
            return Ok(original_peer == Some(peer));
        }
        // libp2p queues replies, requests and failures before reporting closure. A late
        // unowned callback has no authority on a non-active connection, even after finite
        // retirement history evicts its ID. Drop incoming channels instead of admitting them.
        if original_peer.is_none() {
            return Ok(false);
        }
        // A never-owned connection claiming a retained original still fails closed.
        self.reject(peer);
        Err(TransportEvent::Rejected { peer, reason: StatusRejection::Protocol })
    }

    pub(super) fn admit_status_event(
        &mut self,
        event: &request_response::Event<StatusAdvertisement, StatusReply>,
        schema_v2: bool,
    ) -> Result<bool, TransportEvent> {
        let owned = match event {
            request_response::Event::Message {
                message: Message::Response { request_id, .. },
                ..
            } |
            request_response::Event::OutboundFailure { request_id, .. } => {
                self.pending.get(&(schema_v2, *request_id)).copied()
            }
            _ => None,
        };
        self.admit_rpc_event(event, owned)
    }
}

#[cfg(test)]
mod tests;
