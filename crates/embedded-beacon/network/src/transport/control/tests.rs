//! Real TCP/Noise and QUIC/TLS peers negotiate Ping and all three exact metadata schemas.

use super::*;
use crate::control_rpc::LocalMetadata;
use tokio::time::timeout;

async fn connected(quic: bool) -> (StatusTransport, StatusTransport) {
    let config = ChainConfig::hoodi();
    let slot = 50700 * 32;
    let checkpoint = Checkpoint { epoch: 100, root: [1; 32] };
    let status = StatusV2 {
        status: Status {
            fork_digest: config.fork_digest(slot / 32).unwrap(),
            finalized_root: checkpoint.root,
            finalized_epoch: checkpoint.epoch,
            head_root: [2; 32],
            head_slot: slot,
        },
        earliest_available_slot: 7,
    };
    let make = || {
        StatusTransport::new(
            Keypair::generate_secp256k1(),
            config.clone(),
            slot,
            checkpoint,
            Vec::new(),
            status.clone(),
        )
        .unwrap()
    };
    let mut server = make();
    let mut client = make();
    let address = if quic { "/ip4/127.0.0.1/udp/0/quic-v1" } else { "/ip4/127.0.0.1/tcp/0" };
    server.listen_on(address.parse().unwrap()).unwrap();
    let TransportEvent::Listening(address) =
        timeout(Duration::from_secs(10), server.next_event()).await.unwrap()
    else {
        panic!("actual authenticated listener")
    };
    client.dial(server.peer_id(), address).unwrap();
    super::super::tests::exchange_connected(&mut server, &mut client, Some(7)).await;
    (server, client)
}
async fn exchange_controls(quic: bool) {
    let (mut server, mut client) = connected(quic).await;
    let server_id = server.peer_id();
    for protocol in [
        ControlProtocol::Ping,
        ControlProtocol::MetadataV1,
        ControlProtocol::MetadataV2,
        ControlProtocol::MetadataV3,
    ] {
        client.request_control(server_id, protocol).unwrap();
        assert_eq!(client.control_pending.len(), 1);
        assert_eq!(client.request_ping(server_id), Err(TransportError::Capacity));
        let event = timeout(Duration::from_secs(10), async {
            loop {
                tokio::select! {
                    event = client.next_event() => {
                        match event {
                            TransportEvent::PeerPing {..} | TransportEvent::PeerMetadata {..} => break event,
                            TransportEvent::PeerStatus {..} => {},
                            other => panic!("actual client control event: {other:?}"),
                        }
                    }
                    event = server.next_event() => {
                        assert!(matches!(event, TransportEvent::PeerStatus {..}), "server event {event:?}");
                    }
                }
            }
        }).await.unwrap();
        if protocol == ControlProtocol::Ping {
            assert_eq!(event, TransportEvent::PeerPing { peer: server_id, seq_number: 0 });
        } else {
            assert_eq!(
                event,
                TransportEvent::PeerMetadata {
                    peer: server_id,
                    metadata: LocalMetadata::default().snapshot(protocol)
                }
            );
        }
        assert!(client.control_pending.is_empty());
        assert_eq!(server.checked_peers.len(), 1);
        assert_eq!(client.checked_peers.len(), 1);
    }
}
#[tokio::test]
async fn actual_tcp_noise_ping_and_all_metadata_versions() {
    exchange_controls(false).await;
}
#[tokio::test]
async fn actual_quic_tls_ping_and_all_metadata_versions() {
    exchange_controls(true).await;
}

#[tokio::test]
async fn authenticated_metadata_control_does_not_require_completed_bulk_status_gate() {
    let (mut server, mut client) = connected(false).await;
    server.checked_peers.clear();
    client.request_metadata(server.peer_id()).unwrap();
    let event = timeout(Duration::from_secs(10), async {
        loop {
            tokio::select! {
                event = client.next_event() => if matches!(event, TransportEvent::PeerMetadata { .. }) { break event },
                _ = server.next_event() => {},
            }
        }
    }).await.unwrap();
    assert_eq!(
        event,
        TransportEvent::PeerMetadata {
            peer: server.peer_id(),
            metadata: LocalMetadata::default().snapshot(ControlProtocol::MetadataV3)
        }
    );
    assert!(server.checked_peers.is_empty());
}

#[tokio::test]
async fn control_admission_requires_checked_outbound_peer_and_disconnect_releases_pending() {
    let (mut server, mut client) = connected(false).await;
    let unknown = Keypair::generate_secp256k1().public().to_peer_id();
    assert_eq!(client.request_ping(unknown), Err(TransportError::Configuration));
    assert!(client.control_pending.is_empty());
    client.request_ping(server.peer_id()).unwrap();
    assert_eq!(client.control_pending.len(), 1);
    client.reject(server.peer_id());
    assert!(client.control_pending.is_empty());
    assert!(!client.checked_peers.contains(&server.peer_id()));
    server.shutdown();
    client.shutdown();
    assert!(client.control_pending.is_empty());
}

#[tokio::test]
async fn status_updates_do_not_relabel_metadata_sequence_as_enr_or_head_progress() {
    let (mut server, _client) = connected(false).await;
    let mut next = server.local_status.clone();
    next.status.head_slot += 1;
    next.status.head_root = [3; 32];
    server
        .update_snapshot(server.current_slot + 1, server.local_finalized, Vec::new(), next)
        .unwrap();
    assert_eq!(server.local_metadata.seq_number(), 0);
    assert_eq!(
        server.local_metadata.snapshot(ControlProtocol::MetadataV3).custody_group_count,
        Some(0)
    );
}

#[tokio::test]
async fn simultaneous_bidirectional_metadata_preserves_one_outbound_owner_per_peer() {
    let (mut server, mut client) = connected(false).await;
    for protocol in
        [ControlProtocol::MetadataV1, ControlProtocol::MetadataV2, ControlProtocol::MetadataV3]
    {
        server.request_control(client.peer_id(), protocol).unwrap();
        client.request_control(server.peer_id(), protocol).unwrap();
        assert_eq!(server.control_pending.len(), 1);
        assert_eq!(client.control_pending.len(), 1);
        assert_eq!(server.request_ping(client.peer_id()), Err(TransportError::Capacity));
        assert_eq!(client.request_ping(server.peer_id()), Err(TransportError::Capacity));

        timeout(Duration::from_secs(10), async {
            let mut server_received = false;
            let mut client_received = false;
            while !server_received || !client_received {
                tokio::select! {
                    event = server.next_event() => match event {
                        TransportEvent::PeerMetadata { peer, metadata } => {
                            assert_eq!(peer, client.peer_id());
                            assert_eq!(metadata, client.local_metadata.snapshot(protocol));
                            server_received = true;
                        },
                        TransportEvent::PeerStatus { .. } => {},
                        other => panic!("actual simultaneous server control event: {other:?}"),
                    },
                    event = client.next_event() => match event {
                        TransportEvent::PeerMetadata { peer, metadata } => {
                            assert_eq!(peer, server.peer_id());
                            assert_eq!(metadata, server.local_metadata.snapshot(protocol));
                            client_received = true;
                        },
                        TransportEvent::PeerStatus { .. } => {},
                        other => panic!("actual simultaneous client control event: {other:?}"),
                    },
                }
            }
        })
        .await
        .unwrap();
        assert!(server.control_pending.is_empty());
        assert!(client.control_pending.is_empty());
        assert!(server.checked_peers.contains(&client.peer_id()));
        assert!(client.checked_peers.contains(&server.peer_id()));
    }
    server.shutdown();
    client.shutdown();
}
