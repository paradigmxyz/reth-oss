//! Retired original request tokens cannot reject a new connection sharing the same peer identity.

use super::*;

impl StatusTransport {
    pub(super) fn failed_status_request(
        &mut self,
        schema_v2: bool,
        peer: PeerId,
        request_id: OutboundRequestId,
    ) -> Option<TransportEvent> {
        // libp2p allocates each outbound request ID. A missing exact original was already
        // retired by a real terminal event or explicit retirement, never by a new request.
        if *self.pending.get(&(schema_v2, request_id))? != peer {
            self.reject(peer);
            return Some(TransportEvent::Rejected { peer, reason: StatusRejection::Protocol });
        }
        self.pending.remove(&(schema_v2, request_id));
        self.reject(peer);
        Some(TransportEvent::Rejected { peer, reason: StatusRejection::Protocol })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_rpc::{ControlReply, ControlRequest};
    use libp2p::swarm::ConnectionId;

    #[derive(Clone, Copy)]
    enum Family {
        Status,
        Control,
        Block,
        Envelope,
        Sidecar,
    }

    impl Family {
        fn issue(self, owner: &mut StatusTransport, peer: PeerId) -> OutboundRequestId {
            match self {
                Self::Status => {
                    owner.refresh_status();
                    owner.pending.keys().next().unwrap().1
                }
                Self::Control => {
                    owner.request_metadata(peer).unwrap();
                    owner.control_pending.keys().next().unwrap().1
                }
                Self::Block => {
                    owner.request_blocks(peer, BlockRequest::Roots(vec![[7; 32]])).unwrap();
                    owner.block_pending.keys().next().unwrap().1
                }
                Self::Envelope => {
                    let block = crate::test_envelopes::wire_block(9, [1; 32]);
                    owner
                        .request_envelopes(
                            peer,
                            EnvelopeRequest::Roots(vec![block.root()]),
                            vec![EnvelopeBlockReference::from(&block)],
                        )
                        .unwrap();
                    owner.envelope_pending.keys().next().unwrap().1
                }
                Self::Sidecar => {
                    owner
                        .request_sidecars(
                            peer,
                            SidecarRequest::ColumnRoots(vec![
                                crate::availability_rpc::ColumnIdentifier {
                                    block_root: [7; 32],
                                    columns: vec![0],
                                },
                            ]),
                        )
                        .unwrap();
                    owner.sidecar_pending.keys().next().unwrap().1
                }
            }
        }

        fn retained(self, owner: &StatusTransport, id: OutboundRequestId) -> bool {
            match self {
                Self::Status => owner.pending.contains_key(&(true, id)),
                Self::Control => {
                    owner.control_pending.contains_key(&(ControlProtocol::MetadataV3, id))
                }
                Self::Block => owner.block_pending.contains_key(&(false, id)),
                Self::Envelope => owner.envelope_pending.contains_key(&(false, id)),
                Self::Sidecar => {
                    owner.sidecar_pending.contains_key(&(Protocol::DataColumnsByRootV1, id))
                }
            }
        }

        fn reply(
            self,
            owner: &mut StatusTransport,
            peer: PeerId,
            request_id: OutboundRequestId,
            connection_id: ConnectionId,
        ) -> Option<TransportEvent> {
            match self {
                Self::Status => {
                    let event = request_response::Event::Message {
                        peer,
                        connection_id,
                        message: Message::Response { request_id, response: StatusReply::Error },
                    };
                    match owner.admit_status_event(&event, true) {
                        Ok(false) => None,
                        Ok(true) => {
                            panic!("an unowned or foreign status response gained admission")
                        }
                        Err(rejected) => Some(rejected),
                    }
                }
                Self::Control => owner.handle_control_event(
                    ControlProtocol::MetadataV3,
                    request_response::Event::Message {
                        peer,
                        connection_id,
                        message: Message::Response { request_id, response: ControlReply::Error(3) },
                    },
                ),
                Self::Block => owner.handle_block_event(
                    false,
                    request_response::Event::Message {
                        peer,
                        connection_id,
                        message: Message::Response { request_id, response: BlockReply::Error(3) },
                    },
                ),
                Self::Envelope => owner.handle_envelope_event(
                    false,
                    request_response::Event::Message {
                        peer,
                        connection_id,
                        message: Message::Response {
                            request_id,
                            response: EnvelopeReply::Error(3),
                        },
                    },
                ),
                Self::Sidecar => owner.handle_sidecar_event(
                    Protocol::DataColumnsByRootV1,
                    request_response::Event::Message {
                        peer,
                        connection_id,
                        message: Message::Response { request_id, response: SidecarReply::Error(3) },
                    },
                ),
            }
        }
        fn fail(
            self,
            owner: &mut StatusTransport,
            peer: PeerId,
            request_id: OutboundRequestId,
            connection_id: ConnectionId,
        ) -> Option<TransportEvent> {
            let error = request_response::OutboundFailure::ConnectionClosed;
            match self {
                Self::Status => {
                    let event = request_response::Event::OutboundFailure {
                        peer,
                        connection_id,
                        request_id,
                        error,
                    };
                    match owner.admit_status_event(&event, true) {
                        Ok(true) => owner.failed_status_request(true, peer, request_id),
                        Ok(false) => None,
                        Err(rejected) => Some(rejected),
                    }
                }
                Self::Control => owner.handle_control_event(
                    ControlProtocol::MetadataV3,
                    request_response::Event::<ControlRequest, ControlReply>::OutboundFailure {
                        peer,
                        connection_id,
                        request_id,
                        error,
                    },
                ),
                Self::Block => owner.handle_block_event(
                    false,
                    request_response::Event::OutboundFailure {
                        peer,
                        connection_id,
                        request_id,
                        error,
                    },
                ),
                Self::Envelope => owner.handle_envelope_event(
                    false,
                    request_response::Event::OutboundFailure {
                        peer,
                        connection_id,
                        request_id,
                        error,
                    },
                ),
                Self::Sidecar => owner.handle_sidecar_event(
                    Protocol::DataColumnsByRootV1,
                    request_response::Event::OutboundFailure {
                        peer,
                        connection_id,
                        request_id,
                        error,
                    },
                ),
            }
        }
    }

    fn owner(peer: PeerId) -> StatusTransport {
        let config = crate::test_envelopes::config();
        let checkpoint = Checkpoint { epoch: 0, root: [1; 32] };
        let status = StatusV2 {
            status: Status {
                fork_digest: config.fork_digest(0).unwrap(),
                finalized_root: checkpoint.root,
                finalized_epoch: 0,
                head_root: [2; 32],
                head_slot: 12,
            },
            earliest_available_slot: 0,
        };
        let mut owner = StatusTransport::new(
            Keypair::generate_secp256k1(),
            config,
            12,
            checkpoint,
            Vec::new(),
            status,
        )
        .unwrap();
        assert!(owner.connections.established(ConnectionId::new_unchecked(1), peer));
        owner.peers.insert(peer);
        owner.checked_peers.insert(peer);
        owner
    }

    const FAMILIES: [Family; 5] =
        [Family::Status, Family::Control, Family::Block, Family::Envelope, Family::Sidecar];

    #[tokio::test]
    async fn retired_original_failure_cannot_consume_reconnected_same_peer_request() {
        for family in FAMILIES {
            let peer = PeerId::random();
            let mut owner = owner(peer);
            let retired = family.issue(&mut owner, peer);
            owner.retire_peer(peer);
            assert!(owner.connections.established(ConnectionId::new_unchecked(2), peer));
            owner.peers.insert(peer);
            owner.checked_peers.insert(peer);
            let current = family.issue(&mut owner, peer);
            assert_ne!(current, retired);
            let status = owner.local_status.clone();
            let finalized = owner.local_finalized;
            assert!(family
                .fail(&mut owner, peer, retired, ConnectionId::new_unchecked(1))
                .is_none());
            assert!(owner.peers.contains(&peer));
            assert!(owner.checked_peers.contains(&peer));
            assert_eq!(owner.local_status, status);
            assert_eq!(owner.local_finalized, finalized);
            assert_eq!(
                family.fail(&mut owner, peer, current, ConnectionId::new_unchecked(2)),
                Some(TransportEvent::Rejected { peer, reason: StatusRejection::Protocol })
            );
            assert!(!owner.checked_peers.contains(&peer));
            owner.shutdown();
        }
    }

    #[tokio::test]
    async fn retired_original_callbacks_survive_bounded_connection_history_eviction() {
        for family in FAMILIES {
            let peer = PeerId::random();
            let mut owner = owner(peer);
            let old = family.issue(&mut owner, peer);
            owner.retire_peer(peer);
            for index in 0..MAX_PEERS + 1 {
                let churn_peer = PeerId::random();
                assert!(owner
                    .connections
                    .established(ConnectionId::new_unchecked(100 + index), churn_peer));
                owner.connections.retire_peer(churn_peer);
            }
            assert!(owner.connections.established(ConnectionId::new_unchecked(2), peer));
            owner.peers.insert(peer);
            owner.checked_peers.insert(peer);
            let current = family.issue(&mut owner, peer);
            assert!(family.fail(&mut owner, peer, old, ConnectionId::new_unchecked(1)).is_none());
            assert!(family.reply(&mut owner, peer, old, ConnectionId::new_unchecked(1)).is_none());
            assert!(family.retained(&owner, current));
            assert!(owner.checked_peers.contains(&peer));
            // A callback claiming the current active connection still requires an exact token.
            assert_eq!(
                family.fail(&mut owner, peer, old, ConnectionId::new_unchecked(2)),
                Some(TransportEvent::Rejected { peer, reason: StatusRejection::Protocol })
            );
            owner.shutdown();
        }
    }

    #[tokio::test]
    async fn active_wrong_peer_response_keeps_the_other_original_reserved() {
        for family in FAMILIES {
            let actual = PeerId::random();
            let foreign = PeerId::random();
            let mut owner = owner(actual);
            let current = family.issue(&mut owner, actual);
            assert!(owner.connections.established(ConnectionId::new_unchecked(2), foreign));
            owner.peers.insert(foreign);
            owner.checked_peers.insert(foreign);
            assert_eq!(
                family.reply(&mut owner, foreign, current, ConnectionId::new_unchecked(2)),
                Some(TransportEvent::Rejected { peer: foreign, reason: StatusRejection::Protocol })
            );
            assert!(family.retained(&owner, current));
            assert!(owner.checked_peers.contains(&actual));
            owner.shutdown();
        }
    }
    #[tokio::test]
    async fn active_outbound_failure_with_wrong_peer_remains_rejected() {
        for family in FAMILIES {
            let actual = PeerId::random();
            let foreign = PeerId::random();
            let mut owner = owner(actual);
            let current = family.issue(&mut owner, actual);
            assert!(owner.connections.established(ConnectionId::new_unchecked(2), foreign));
            owner.peers.insert(foreign);
            owner.checked_peers.insert(foreign);
            assert_eq!(
                family.fail(&mut owner, foreign, current, ConnectionId::new_unchecked(2)),
                Some(TransportEvent::Rejected { peer: foreign, reason: StatusRejection::Protocol })
            );
            assert!(family.retained(&owner, current));
            assert!(owner.checked_peers.contains(&actual));
            assert_eq!(
                family.fail(&mut owner, actual, current, ConnectionId::new_unchecked(1)),
                Some(TransportEvent::Rejected { peer: actual, reason: StatusRejection::Protocol })
            );
            assert!(!family.retained(&owner, current));
            owner.shutdown();
        }
    }
}
