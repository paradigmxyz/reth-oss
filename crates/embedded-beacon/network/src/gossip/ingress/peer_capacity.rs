//! Authenticated connection reservations cap retained scored identities before allocation.
//!
//! Pending IDs remain charged until the exact established/failure event settles their handler.
//! Existing unexpired score entries are retained under the dependency's real scoring policy.

use super::*;
use std::collections::BTreeMap;

const MAX_SCORED_PEERS: usize = 4096;
const MAX_PENDING_SCORE_CONNECTIONS: usize = 24;

#[derive(Default)]
pub(super) struct PeerCapacity {
    pending: BTreeMap<ConnectionId, PeerId>,
}
#[derive(Debug, thiserror::Error)]
#[error("local gossip scored-peer capacity exhausted")]
struct ScoreCapacity;

impl PeerCapacity {
    pub(super) fn reserve(
        &mut self,
        inner: &mut InnerBehaviour,
        connection: ConnectionId,
        peer: PeerId,
    ) -> Result<(), ConnectionDenied> {
        if let Some(prior) = self.pending.get(&connection) {
            return if *prior == peer { Ok(()) } else { Err(ConnectionDenied::new(ScoreCapacity)) }
        }
        if self.pending.len() >= MAX_PENDING_SCORE_CONNECTIONS {
            return Err(ConnectionDenied::new(ScoreCapacity))
        }
        let unique = self.pending.values().copied().collect::<HashSet<_>>();
        // The new peer is already represented by a pending identity only once. Known scored
        // identities still pass the dependency query even at the reduced capacity limit.
        let remaining = MAX_SCORED_PEERS.saturating_sub(unique.len());
        if !unique.contains(&peer) && !inner.can_admit_score_peer(&peer, remaining) {
            return Err(ConnectionDenied::new(ScoreCapacity))
        }
        self.pending.insert(connection, peer);
        Ok(())
    }
    pub(super) fn release(&mut self, connection: ConnectionId) {
        self.pending.remove(&connection);
    }
    pub(super) fn settled(&mut self, event: &FromSwarm<'_>) {
        match event {
            FromSwarm::ConnectionEstablished(event) => self.release(event.connection_id),
            FromSwarm::DialFailure(event) => self.release(event.connection_id),
            FromSwarm::ListenFailure(event) => self.release(event.connection_id),
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use libp2p::{
        core::ConnectedPoint,
        swarm::{behaviour::ConnectionEstablished, ConnectionClosed, ListenError, ListenFailure},
    };

    fn inner() -> InnerBehaviour {
        crate::gossip::behaviour(Arc::new(ChainConfig::hoodi()), 3000 * 32).unwrap().inner
    }
    fn establish(
        inner: &mut InnerBehaviour,
        capacity: &mut PeerCapacity,
        peer: PeerId,
        id: ConnectionId,
    ) {
        let endpoint = ConnectedPoint::Dialer {
            address: "/ip4/127.0.0.1/tcp/30303".parse().unwrap(),
            role_override: Endpoint::Dialer,
            port_use: PortUse::Reuse,
        };
        capacity.reserve(inner, id, peer).unwrap();
        let address: Multiaddr = "/ip4/127.0.0.1/tcp/30303".parse().unwrap();
        let _handler = inner
            .handle_established_outbound_connection(
                id,
                peer,
                &address,
                Endpoint::Dialer,
                PortUse::Reuse,
            )
            .unwrap();
        inner.on_swarm_event(FromSwarm::ConnectionEstablished(ConnectionEstablished {
            peer_id: peer,
            connection_id: id,
            endpoint: &endpoint,
            failed_addresses: &[],
            other_established: 0,
        }));
        capacity.settled(&FromSwarm::ConnectionEstablished(ConnectionEstablished {
            peer_id: peer,
            connection_id: id,
            endpoint: &endpoint,
            failed_addresses: &[],
            other_established: 0,
        }));
        assert!(!capacity.pending.contains_key(&id));
    }
    #[test]
    fn actual_reservation_caps_connections_and_deduplicates_authenticated_pending_ids() {
        let mut inner = inner();
        let mut capacity = PeerCapacity::default();
        let peer = PeerId::random();
        for index in 0..MAX_PENDING_SCORE_CONNECTIONS {
            capacity.reserve(&mut inner, ConnectionId::new_unchecked(index), peer).unwrap();
        }
        assert_eq!(capacity.pending.values().copied().collect::<HashSet<_>>().len(), 1);
        assert!(capacity.reserve(&mut inner, ConnectionId::new_unchecked(24), peer).is_err());
        assert!(capacity
            .reserve(&mut inner, ConnectionId::new_unchecked(0), PeerId::random())
            .is_err());
        capacity.release(ConnectionId::new_unchecked(1));
        capacity.reserve(&mut inner, ConnectionId::new_unchecked(24), PeerId::random()).unwrap();
        let error = ListenError::Denied { cause: ConnectionDenied::new(ScoreCapacity) };
        let local = "/ip4/127.0.0.1/tcp/30303".parse().unwrap();
        let event = FromSwarm::ListenFailure(ListenFailure {
            local_addr: &local,
            send_back_addr: &local,
            error: &error,
            connection_id: ConnectionId::new_unchecked(24),
            peer_id: None,
        });
        capacity.settled(&event);
        assert!(!capacity.pending.contains_key(&ConnectionId::new_unchecked(24)));
    }
    #[test]
    fn actual_scored_capacity_denies_new_identity_without_erasing_known_reconnect() {
        let mut inner = inner();
        let mut capacity = PeerCapacity::default();
        let known = PeerId::random();
        establish(&mut inner, &mut capacity, known, ConnectionId::new_unchecked(0));
        for index in 1..MAX_SCORED_PEERS {
            establish(
                &mut inner,
                &mut capacity,
                PeerId::random(),
                ConnectionId::new_unchecked(index),
            );
        }
        assert!(capacity
            .reserve(&mut inner, ConnectionId::new_unchecked(MAX_SCORED_PEERS), PeerId::random())
            .is_err());
        capacity.reserve(&mut inner, ConnectionId::new_unchecked(MAX_SCORED_PEERS), known).unwrap();
        capacity.release(ConnectionId::new_unchecked(MAX_SCORED_PEERS));
        let endpoint = ConnectedPoint::Dialer {
            address: "/ip4/127.0.0.1/tcp/30303".parse().unwrap(),
            role_override: Endpoint::Dialer,
            port_use: PortUse::Reuse,
        };
        inner.on_swarm_event(FromSwarm::ConnectionClosed(ConnectionClosed {
            peer_id: known,
            connection_id: ConnectionId::new_unchecked(0),
            endpoint: &endpoint,
            cause: None,
            remaining_established: 0,
        }));
        // Native nonpositive scoring retention is preserved; reconnect is still admitted while
        // an unrelated new identity remains unable to consume the full score table.
        capacity.reserve(&mut inner, ConnectionId::new_unchecked(MAX_SCORED_PEERS), known).unwrap();
        assert!(capacity
            .reserve(
                &mut inner,
                ConnectionId::new_unchecked(MAX_SCORED_PEERS + 1),
                PeerId::random()
            )
            .is_err());
    }
}
