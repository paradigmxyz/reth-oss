//! Exact old connection events cannot consume a fresh same-peer request or grant wire authority.

use super::*;
use crate::control_rpc::{ControlReply, ControlRequest};
use tokio::time::timeout;

#[test]
fn exact_old_close_preserves_new_same_peer_connection_and_original_bounds() {
    let peer = PeerId::random();
    let old = ConnectionId::new_unchecked(1);
    let current = ConnectionId::new_unchecked(2);
    let mut inventory = ConnectionInventory::default();
    assert!(inventory.established(old, peer));
    inventory.retire_peer(peer);
    assert!(inventory.established(current, peer));
    assert!(!inventory.closed(old, peer));
    assert_eq!(inventory.active.get(&current), Some(&peer));
    assert!(inventory.was_retired(old, peer));
    assert!(inventory.closed(current, peer));
    assert!(inventory.active.is_empty());

    let mut inventory = ConnectionInventory::default();
    for index in 0..MAX_PEERS {
        assert!(inventory.established(ConnectionId::new_unchecked(index + 10), PeerId::random()));
    }
    assert!(!inventory.established(ConnectionId::new_unchecked(99), PeerId::random()));
    for index in 0..64 {
        let peer = PeerId::random();
        inventory.active.clear();
        assert!(inventory.established(ConnectionId::new_unchecked(index + 100), peer));
        inventory.retire_peer(peer);
        assert!(inventory.retired.len() <= MAX_PEERS);
    }
    inventory.clear();
    assert!(inventory.active.is_empty());
    assert!(inventory.retired.is_empty());

    assert!(inventory.established(old, peer));
    assert!(inventory.established(current, peer));
    assert!(!inventory.established(ConnectionId::new_unchecked(3), peer));
    assert!(!inventory.closed(old, peer));
    assert_eq!(inventory.active.get(&current), Some(&peer));
}

#[tokio::test]
async fn real_tcp_same_peer_reconnect_keeps_new_original_after_retired_rpc_events() {
    let mut server = super::super::envelope_tests::transport();
    let mut client = super::super::envelope_tests::transport();
    server.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()).unwrap();
    let TransportEvent::Listening(address) =
        timeout(Duration::from_secs(10), server.next_event()).await.unwrap()
    else {
        panic!("actual reconnect listener did not start");
    };
    let server_peer = server.peer_id();
    let client_peer = client.peer_id();
    client.dial(server_peer, address.clone()).unwrap();
    super::super::tests::exchange_connected(&mut server, &mut client, Some(0)).await;
    let block = crate::test_envelopes::wire_block(9, [1; 32]);
    let request = BlockRequest::Roots(vec![block.root()]);
    client.request_blocks(server_peer, request.clone()).unwrap();
    let old_outbound = client.block_pending.keys().next().unwrap().1;
    let old_server_connection =
        *server.connections.active.iter().find(|(_, peer)| **peer == client_peer).unwrap().0;
    let old_client_connection =
        *client.connections.active.iter().find(|(_, peer)| **peer == server_peer).unwrap().0;
    let old_inbound = timeout(Duration::from_secs(10), async {
        loop {
            tokio::select! {
                event = server.next_event() => match event {
                    TransportEvent::BlockRequest { peer, id, request: observed } => {
                        assert_eq!(peer, client_peer);
                        assert_eq!(observed, request);
                        break server.block_inbound[&id].request_id;
                    },
                    TransportEvent::PeerStatus { .. } => {},
                    other => panic!("actual old server request event: {other:?}"),
                },
                event = client.next_event() => {
                    assert!(matches!(event, TransportEvent::PeerStatus { .. }));
                },
            }
        }
    })
    .await
    .unwrap();
    // Keep the real old private channel to model an already queued incoming callback.
    let old_server_token = *server.block_inbound.keys().next().unwrap();
    let old_channel = server.block_inbound.remove(&old_server_token).unwrap().channel;
    server.retire_peer(client_peer);
    timeout(Duration::from_secs(10), async {
        let mut closed = [false; 2];
        while !closed.iter().all(|side| *side) {
            tokio::select! {
                event = server.next_event() => if let TransportEvent::Closed(peer) = event {
                    assert_eq!(peer, client_peer); closed[0] = true;
                },
                event = client.next_event() => if let TransportEvent::Closed(peer) = event {
                    assert_eq!(peer, server_peer); closed[1] = true;
                },
            }
        }
    })
    .await
    .unwrap();
    assert!(server.block_inbound.is_empty());
    assert!(client.block_pending.is_empty());

    client.dial(server_peer, address).unwrap();
    super::super::tests::exchange_connected(&mut server, &mut client, Some(0)).await;
    let current_original = client.request_blocks(server_peer, request.clone()).unwrap();
    let current_outbound = client.block_pending.keys().next().unwrap().1;
    let current_client_connection =
        *client.connections.active.iter().find(|(_, peer)| **peer == server_peer).unwrap().0;
    assert_ne!(old_outbound, current_outbound);
    assert_ne!(old_client_connection, current_client_connection);
    let current_inbound = timeout(Duration::from_secs(10), async {
        loop {
            tokio::select! {
                event = server.next_event() => match event {
                    TransportEvent::BlockRequest { peer, id, request: observed } => {
                        assert_eq!(peer, client_peer); assert_eq!(observed, request); break id;
                    },
                    TransportEvent::PeerStatus { .. } => {},
                    other => panic!("actual current server request event: {other:?}"),
                },
                event = client.next_event() => {
                    assert!(matches!(event, TransportEvent::PeerStatus { .. }));
                },
            }
        }
    })
    .await
    .unwrap();

    // Even after finite retirement history evicts both actual old connection IDs, queued
    // callbacks cannot consume the fresh same-peer request or make unowned bytes authoritative.
    for index in 0..MAX_PEERS + 1 {
        let churn_peer = PeerId::random();
        let churn_connection = ConnectionId::new_unchecked(10_000 + index);
        assert!(server.connections.established(churn_connection, churn_peer));
        server.connections.retire_peer(churn_peer);
        assert!(client.connections.established(churn_connection, churn_peer));
        client.connections.retire_peer(churn_peer);
    }
    assert!(!server.connections.was_retired(old_server_connection, client_peer));
    assert!(!client.connections.was_retired(old_client_connection, server_peer));
    let stale = request_response::Event::<StatusAdvertisement, StatusReply>::InboundFailure {
        peer: client_peer,
        connection_id: old_server_connection,
        request_id: old_inbound,
        error: request_response::InboundFailure::ConnectionClosed,
    };
    assert_eq!(server.admit_status_event(&stale, true), Ok(false));
    assert!(server
        .handle_control_event(
            ControlProtocol::MetadataV3,
            request_response::Event::<ControlRequest, ControlReply>::InboundFailure {
                peer: client_peer,
                connection_id: old_server_connection,
                request_id: old_inbound,
                error: request_response::InboundFailure::ConnectionClosed,
            },
        )
        .is_none());
    assert!(server
        .handle_block_event(
            false,
            request_response::Event::InboundFailure {
                peer: client_peer,
                connection_id: old_server_connection,
                request_id: old_inbound,
                error: request_response::InboundFailure::ConnectionClosed,
            }
        )
        .is_none());
    assert!(server
        .handle_envelope_event(
            false,
            request_response::Event::InboundFailure {
                peer: client_peer,
                connection_id: old_server_connection,
                request_id: old_inbound,
                error: request_response::InboundFailure::ConnectionClosed,
            }
        )
        .is_none());
    assert!(server
        .handle_sidecar_event(
            Protocol::DataColumnsByRootV1,
            request_response::Event::InboundFailure {
                peer: client_peer,
                connection_id: old_server_connection,
                request_id: old_inbound,
                error: request_response::InboundFailure::ConnectionClosed,
            },
        )
        .is_none());
    assert!(client
        .handle_block_event(
            false,
            request_response::Event::Message {
                peer: server_peer,
                connection_id: old_client_connection,
                message: Message::Response {
                    request_id: old_outbound,
                    response: BlockReply::Blocks(BlockBatch::serving(vec![block.clone()])),
                },
            }
        )
        .is_none());
    assert!(server
        .handle_block_event(
            false,
            request_response::Event::Message {
                peer: client_peer,
                connection_id: old_server_connection,
                message: Message::Request {
                    request_id: old_inbound,
                    request: request.clone(),
                    channel: old_channel
                },
            }
        )
        .is_none());
    let sent = request_response::Event::<StatusAdvertisement, StatusReply>::ResponseSent {
        peer: client_peer,
        connection_id: old_server_connection,
        request_id: old_inbound,
    };
    assert_eq!(server.admit_status_event(&sent, true), Ok(false));
    assert!(client.block_pending.contains_key(&(false, current_outbound)));
    assert!(server.block_inbound.contains_key(&current_inbound));
    assert!(server.checked_peers.contains(&client_peer));
    assert!(client.checked_peers.contains(&server_peer));

    server.respond_blocks(current_inbound, vec![block.clone()]).unwrap();
    timeout(Duration::from_secs(10), async {
        loop {
            tokio::select! {
                event = client.next_event() => match event {
                    TransportEvent::Blocks { peer, id, batch } => {
                        assert_eq!(peer, server_peer); assert_eq!(id, current_original);
                        assert_eq!(batch.blocks(), [block.clone()]); break;
                    },
                    TransportEvent::PeerStatus { .. } => {},
                    other => panic!("actual current client reply event: {other:?}"),
                },
                event = server.next_event() => {
                    assert!(matches!(event, TransportEvent::PeerStatus { .. }));
                },
            }
        }
    })
    .await
    .unwrap();
    assert!(client.block_pending.is_empty());
    assert!(server.block_inbound.is_empty());

    // A malformed reply for a genuinely active exact token remains a protocol rejection.
    client.request_blocks(server_peer, request).unwrap();
    let malformed_original = client.block_pending.keys().next().unwrap().1;
    let unrelated = crate::test_envelopes::wire_block(12, block.root());
    assert_eq!(
        client.handle_block_event(
            false,
            request_response::Event::Message {
                peer: server_peer,
                connection_id: current_client_connection,
                message: Message::Response {
                    request_id: malformed_original,
                    response: BlockReply::Blocks(BlockBatch::serving(vec![unrelated])),
                },
            }
        ),
        Some(TransportEvent::Rejected { peer: server_peer, reason: StatusRejection::Protocol })
    );
    server.shutdown();
    client.shutdown();
}
