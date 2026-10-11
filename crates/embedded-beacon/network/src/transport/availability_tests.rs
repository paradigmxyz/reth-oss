//! Real authenticated availability RPC retrieval; structural bytes grant no DA authority.
use super::*;
use crate::{
    availability_rpc::{
        tests::{config, protocol, sidecar_bytes, slot, wire_sidecar},
        BlobIdentifier, ColumnIdentifier,
    },
    envelope_rpc::EnvelopeBlockReference,
};
use reth_embedded_beacon_primitives::Fork;
use reth_embedded_beacon_state_transition::ssz::{
    auxiliary::{deneb, electra, fulu, gloas},
    gloas as gloas_block, Preset, Ssz,
};
use tokio::time::timeout;

fn transport() -> StatusTransport {
    let config = config();
    let checkpoint = Checkpoint { epoch: 0, root: [1; 32] };
    let status = StatusV2 {
        status: Status {
            fork_digest: config.fork_digest(8).unwrap(),
            finalized_root: checkpoint.root,
            finalized_epoch: 0,
            head_root: [2; 32],
            head_slot: 64,
        },
        earliest_available_slot: 0,
    };
    LocalStatusTransport::new(
        Keypair::generate_secp256k1(),
        config,
        64,
        checkpoint,
        Vec::new(),
        status,
    )
    .unwrap()
}

fn envelope_request() -> (EnvelopeRequest, Vec<EnvelopeBlockReference>) {
    let mut signed = gloas_block::SignedBeaconBlock::default_value(Preset::Minimal).unwrap();
    signed.message.slot = slot(Fork::Gloas);
    signed.message.parent_root = [1; 32];
    signed.message.body.signed_execution_payload_bid.message.slot = signed.message.slot;
    signed.message.body.signed_execution_payload_bid.message.parent_block_root = [1; 32];
    let block = WireBlock::from_ssz(signed.to_ssz(Preset::Minimal).unwrap(), &config()).unwrap();
    (EnvelopeRequest::Roots(vec![block.root()]), vec![EnvelopeBlockReference::from(&block)])
}

fn request(fork: Fork, roots: bool) -> SidecarRequest {
    let sidecar = wire_sidecar(fork, 0, 11);
    match (fork < Fork::Fulu, roots) {
        (true, false) => SidecarRequest::BlobRange { start_slot: slot(fork), count: 1 },
        (true, true) => SidecarRequest::BlobRoots(vec![
            BlobIdentifier { block_root: sidecar.block_root(), index: 0 },
            BlobIdentifier { block_root: sidecar.block_root(), index: 1 },
        ]),
        (false, false) => {
            SidecarRequest::ColumnRange { start_slot: slot(fork), count: 1, columns: vec![0, 1] }
        }
        (false, true) => SidecarRequest::ColumnRoots(vec![ColumnIdentifier {
            block_root: sidecar.block_root(),
            columns: vec![0, 1],
        }]),
    }
}

fn outside_range(fork: Fork) -> WireSidecar {
    let bytes = sidecar_bytes(fork, 0, 11);
    let bytes = match fork {
        Fork::Deneb => {
            let mut sidecar = deneb::BlobSidecar::from_ssz(&bytes, Preset::Minimal).unwrap();
            sidecar.signed_block_header.message.slot += 2;
            sidecar.to_ssz(Preset::Minimal).unwrap()
        }
        Fork::Electra => {
            let mut sidecar = electra::BlobSidecar::from_ssz(&bytes, Preset::Minimal).unwrap();
            sidecar.signed_block_header.message.slot += 2;
            sidecar.to_ssz(Preset::Minimal).unwrap()
        }
        Fork::Fulu => {
            let mut sidecar = fulu::DataColumnSidecar::from_ssz(&bytes, Preset::Minimal).unwrap();
            sidecar.signed_block_header.message.slot += 2;
            sidecar.to_ssz(Preset::Minimal).unwrap()
        }
        Fork::Gloas => {
            let mut sidecar = gloas::DataColumnSidecar::from_ssz(&bytes, Preset::Minimal).unwrap();
            sidecar.slot += 2;
            sidecar.to_ssz(Preset::Minimal).unwrap()
        }
        _ => panic!("sidecar schema required"),
    };
    WireSidecar::from_ssz(
        bytes,
        config().fork_digest((slot(fork) + 2) / 8).unwrap(),
        protocol(fork, false),
        &config(),
    )
    .unwrap()
}

async fn connect(listener: &mut StatusTransport, dialer: &mut StatusTransport, quic: bool) {
    listener
        .listen_on(
            if quic { "/ip4/127.0.0.1/udp/0/quic-v1" } else { "/ip4/127.0.0.1/tcp/0" }
                .parse()
                .unwrap(),
        )
        .unwrap();
    let TransportEvent::Listening(address) =
        timeout(Duration::from_secs(10), listener.next_event()).await.unwrap()
    else {
        panic!("listener unavailable")
    };
    dialer.dial(listener.peer_id(), address).unwrap();
    super::tests::exchange_connected(listener, dialer, Some(0)).await;
}

async fn exchange(fork: Fork, quic: bool) {
    let mut listener = transport();
    let mut dialer = transport();
    assert_eq!(
        dialer.request_sidecars(listener.peer_id(), request(fork, false)),
        Err(TransportError::Configuration)
    );
    connect(&mut listener, &mut dialer, quic).await;
    let peer = listener.peer_id();
    for roots in [false, true] {
        let request = request(fork, roots);
        let id = dialer.request_sidecars(peer, request.clone()).unwrap();
        assert_eq!(dialer.request_sidecars(peer, request.clone()), Err(TransportError::Capacity));
        assert_eq!(
            dialer.request_blocks(peer, BlockRequest::Roots(vec![[3; 32]])),
            Err(TransportError::Capacity)
        );
        let (envelope, references) = envelope_request();
        assert_eq!(
            dialer.request_envelopes(peer, envelope, references),
            Err(TransportError::Capacity)
        );
        timeout(Duration::from_secs(10), async {
            loop {
                tokio::select! {
                    event = listener.next_event() => {
                        let TransportEvent::SidecarRequest { peer: requester, id: inbound, request: actual } = event
                            else { panic!("server event {event:?}") };
                        assert_eq!(requester, dialer.peer_id());
                        assert_eq!(actual, request);
                        let unrelated = if roots { wire_sidecar(fork, 0, 99) } else { outside_range(fork) };
                        assert_eq!(listener.respond_sidecars(inbound, vec![unrelated]), Err(TransportError::Configuration));
                        assert!(listener.sidecar_inbound.contains_key(&inbound));
                        let mut foreign_config = config();
                        foreign_config.genesis_validators_root = [99; 32];
                        let foreign = WireSidecar::from_ssz(
                            sidecar_bytes(fork, 0, 11),
                            foreign_config.fork_digest(slot(fork) / 8).unwrap(),
                            protocol(fork, false), &foreign_config,
                        ).unwrap();
                        assert_eq!(listener.respond_sidecars(inbound, vec![foreign]), Err(TransportError::Configuration));
                        // An actual outbound token from the other transport cannot consume this channel.
                        assert_eq!(listener.respond_sidecars_unavailable(id), Err(TransportError::Configuration));
                        assert!(listener.sidecar_inbound.contains_key(&inbound));
                        listener.respond_sidecars(inbound, vec![wire_sidecar(fork, 0, 11), wire_sidecar(fork, 1, 11)]).unwrap();
                        assert_eq!(listener.respond_sidecars_unavailable(inbound), Err(TransportError::Configuration));
                    }
                    event = dialer.next_event() => {
                        let TransportEvent::Sidecars { peer: actual_peer, id: actual_id, batch } = event
                            else { panic!("client event {event:?}") };
                        assert_eq!(actual_peer, peer);
                        assert_eq!(actual_id, id);
                        assert_eq!(batch.sidecars(), [wire_sidecar(fork, 0, 11), wire_sidecar(fork, 1, 11)]);
                        assert_eq!(request.matches(&config(), &batch), Ok(()));
                        drop(batch);
                        break;
                    }
                }
            }
        }).await.unwrap();
        let unavailable = dialer.request_sidecars(peer, request.clone()).unwrap();
        timeout(Duration::from_secs(10), async {
            loop {
                tokio::select! {
                    event = listener.next_event() => {
                        let TransportEvent::SidecarRequest { id, .. } = event
                            else { panic!("server event {event:?}") };
                        listener.respond_sidecars_unavailable(id).unwrap();
                    }
                    event = dialer.next_event() => {
                        assert_eq!(event, TransportEvent::SidecarsUnavailable { peer, id: unavailable, code: 3 });
                        break;
                    }
                }
            }
        }).await.unwrap();
        let partial = dialer.request_sidecars(peer, request).unwrap();
        timeout(Duration::from_secs(10), async {
            loop {
                tokio::select! {
                    event = listener.next_event() => {
                        let TransportEvent::SidecarRequest { id, .. } = event
                            else { panic!("server event {event:?}") };
                        listener.respond_sidecars(id, Vec::new()).unwrap();
                    }
                    event = dialer.next_event() => {
                        let TransportEvent::Sidecars { peer: actual_peer, id, batch } = event
                            else { panic!("client event {event:?}") };
                        assert_eq!(actual_peer, peer);
                        assert_eq!(id, partial);
                        assert!(batch.sidecars().is_empty());
                        break;
                    }
                }
            }
        })
        .await
        .unwrap();
    }
    // Shutdown clears actual outbound state and an original private inbound channel.
    dialer.request_sidecars(peer, request(fork, true)).unwrap();
    timeout(Duration::from_secs(10), async {
        tokio::select! {
            event = listener.next_event() => {
                let TransportEvent::SidecarRequest { id, .. } = event
                    else { panic!("server event {event:?}") };
                assert!(listener.sidecar_inbound.contains_key(&id));
            }
            event = dialer.next_event() => panic!("unexpected client event {event:?}"),
        }
    })
    .await
    .unwrap();
    listener.shutdown();
    dialer.shutdown();
    assert!(listener.sidecar_inbound.is_empty());
    assert!(dialer.sidecar_pending.is_empty());
    assert!(listener.block_inbound.is_empty());
    assert!(listener.envelope_inbound.is_empty());
    assert!(dialer.block_pending.is_empty());
    assert!(dialer.envelope_pending.is_empty());
}

#[tokio::test]
async fn actual_tcp_noise_deneb_blob_range_and_roots() {
    exchange(Fork::Deneb, false).await;
}
#[tokio::test]
async fn actual_quic_tls_deneb_blob_range_and_roots() {
    exchange(Fork::Deneb, true).await;
}
#[tokio::test]
async fn actual_tcp_noise_electra_blob_range_and_roots() {
    exchange(Fork::Electra, false).await;
}
#[tokio::test]
async fn actual_quic_tls_electra_blob_range_and_roots() {
    exchange(Fork::Electra, true).await;
}
#[tokio::test]
async fn actual_tcp_noise_fulu_column_range_and_roots() {
    exchange(Fork::Fulu, false).await;
}
#[tokio::test]
async fn actual_quic_tls_fulu_column_range_and_roots() {
    exchange(Fork::Fulu, true).await;
}
#[tokio::test]
async fn actual_tcp_noise_gloas_column_range_and_roots() {
    exchange(Fork::Gloas, false).await;
}
#[tokio::test]
async fn actual_quic_tls_gloas_column_range_and_roots() {
    exchange(Fork::Gloas, true).await;
}

#[tokio::test]
async fn actual_checked_peers_share_one_global_outbound_block_envelope_sidecar_limit() {
    let mut dialer = transport();
    let mut listeners = Vec::new();
    for _ in 0..MAX_BLOCK_REQUESTS + 1 {
        let mut listener = transport();
        connect(&mut listener, &mut dialer, false).await;
        listeners.push(listener);
    }
    dialer.request_blocks(listeners[0].peer_id(), BlockRequest::Roots(vec![[3; 32]])).unwrap();
    let (envelope, references) = envelope_request();
    dialer.request_envelopes(listeners[1].peer_id(), envelope.clone(), references.clone()).unwrap();
    for listener in &listeners[2..MAX_BLOCK_REQUESTS] {
        dialer.request_sidecars(listener.peer_id(), request(Fork::Gloas, true)).unwrap();
    }
    let peer = listeners[MAX_BLOCK_REQUESTS].peer_id();
    assert_eq!(
        dialer.request_sidecars(peer, request(Fork::Gloas, true)),
        Err(TransportError::Capacity)
    );
    assert_eq!(
        dialer.request_blocks(peer, BlockRequest::Roots(vec![[3; 32]])),
        Err(TransportError::Capacity)
    );
    assert_eq!(dialer.request_envelopes(peer, envelope, references), Err(TransportError::Capacity));
    assert_eq!(
        dialer.block_pending.len() + dialer.envelope_pending.len() + dialer.sidecar_pending.len(),
        MAX_BLOCK_REQUESTS
    );
    dialer.shutdown();
    for listener in &mut listeners {
        listener.shutdown();
    }
    assert!(dialer.sidecar_pending.is_empty());
    assert!(dialer.block_pending.is_empty());
    assert!(dialer.envelope_pending.is_empty());
}

#[tokio::test]
async fn actual_inbound_sidecars_share_capacity_with_private_block_and_envelope_channels() {
    let mut listener = transport();
    let mut first = transport();
    let mut second = transport();
    let mut third = transport();
    connect(&mut listener, &mut first, false).await;
    // Subsequent connections reuse the already listening TCP address.
    let address = listener.swarm.listeners().next().unwrap().clone();
    second.dial(listener.peer_id(), address.clone()).unwrap();
    super::tests::exchange_connected(&mut listener, &mut second, Some(0)).await;
    third.dial(listener.peer_id(), address).unwrap();
    super::tests::exchange_connected(&mut listener, &mut third, Some(0)).await;
    first.request_blocks(listener.peer_id(), BlockRequest::Roots(vec![[3; 32]])).unwrap();
    let block_id = timeout(Duration::from_secs(10), async {
        tokio::select! {
            event = listener.next_event() => {
                let TransportEvent::BlockRequest { id, .. } = event else { panic!("server event {event:?}") };
                id
            }
            event = first.next_event() => panic!("unexpected client event {event:?}"),
        }
    }).await.unwrap();
    let (envelope, references) = envelope_request();
    second.request_envelopes(listener.peer_id(), envelope, references).unwrap();
    let envelope_id = timeout(Duration::from_secs(10), async {
        tokio::select! {
            event = listener.next_event() => {
                let TransportEvent::EnvelopeRequest { id, .. } = event else { panic!("server event {event:?}") };
                id
            }
            event = second.next_event() => panic!("unexpected client event {event:?}"),
        }
    }).await.unwrap();
    assert_eq!(
        listener.block_inbound.len() + listener.envelope_inbound.len(),
        MAX_INBOUND_BLOCK_REQUESTS
    );
    let id = third.request_sidecars(listener.peer_id(), request(Fork::Gloas, true)).unwrap();
    let peer = listener.peer_id();
    timeout(Duration::from_secs(10), async {
        tokio::select! {
            event = listener.next_event() => panic!("capacity must not expose a private channel: {event:?}"),
            event = third.next_event() => {
                assert_eq!(event, TransportEvent::SidecarsUnavailable { peer, id, code: 2 });
            }
        }
    }).await.unwrap();
    assert!(listener.sidecar_inbound.is_empty());
    assert!(third.sidecar_pending.is_empty());
    assert!(third.checked_peers.contains(&peer));
    listener.respond_blocks_unavailable(block_id).unwrap();
    listener.respond_envelopes_unavailable(envelope_id).unwrap();
    listener.shutdown();
    first.shutdown();
    second.shutdown();
    third.shutdown();
}

#[tokio::test]
async fn actual_connection_reset_clears_pending_and_original_private_sidecar_channels() {
    let mut listener = transport();
    let mut dialer = transport();
    connect(&mut listener, &mut dialer, false).await;
    let peer = listener.peer_id();
    dialer.request_sidecars(peer, request(Fork::Gloas, true)).unwrap();
    let inbound = timeout(Duration::from_secs(10), async {
        tokio::select! {
            event = listener.next_event() => {
                let TransportEvent::SidecarRequest { id, .. } = event else { panic!("server event {event:?}") };
                id
            }
            event = dialer.next_event() => panic!("unexpected client event {event:?}"),
        }
    }).await.unwrap();
    listener.reject(dialer.peer_id());
    assert!(listener.sidecar_inbound.is_empty());
    assert_eq!(listener.respond_sidecars_unavailable(inbound), Err(TransportError::Configuration));
    timeout(Duration::from_secs(10), async {
        loop {
            tokio::select! {
                _ = listener.next_event() => {},
                event = dialer.next_event() => {
                    let actual_peer = match event {
                        TransportEvent::Rejected { peer, .. } | TransportEvent::Closed(peer) => peer,
                        event => panic!("client event {event:?}"),
                    };
                    assert_eq!(actual_peer, peer);
                    assert!(dialer.sidecar_pending.is_empty());
                    assert!(dialer.sidecar_inbound.is_empty());
                    break;
                }
            }
        }
    })
    .await
    .unwrap();
    assert!(dialer.sidecar_pending.is_empty());
    assert!(!dialer.checked_peers.contains(&peer));
    assert_eq!(
        dialer.request_sidecars(peer, request(Fork::Gloas, true)),
        Err(TransportError::Configuration)
    );
    listener.shutdown();
    dialer.shutdown();
}
#[tokio::test]
async fn resource_unavailable_requires_explicit_retirement_to_replace_a_full_peer_pool() {
    let mut dialer = transport();
    let preserved_status = dialer.local_status.clone();
    let preserved_finalized = dialer.local_finalized;
    let preserved_roots = dialer.canonical_roots.clone();
    let refresh_after_test = tokio::time::Instant::now() + Duration::from_secs(60);
    dialer.next_status_refresh = refresh_after_test;
    let mut listeners = Vec::new();
    for _ in 0..MAX_PEERS {
        let mut listener = transport();
        connect(&mut listener, &mut dialer, false).await;
        listener.next_status_refresh = refresh_after_test;
        listeners.push(listener);
    }
    let listener = &mut listeners[0];
    let provider = listener.peer_id();
    assert_eq!(dialer.peers.len(), MAX_PEERS);
    assert!(!dialer.has_dial_capacity());
    let request = request(Fork::Fulu, true);
    let original = dialer.request_sidecars(provider, request.clone()).unwrap();
    timeout(Duration::from_secs(10), async {
        loop {
            tokio::select! {
                event = listener.next_event() => {
                    if let TransportEvent::SidecarRequest { id, .. } = event {
                        listener.respond_sidecars_unavailable(id).unwrap();
                    }
                },
                event = dialer.next_event() => {
                    if let TransportEvent::SidecarsUnavailable { peer, id, code } = event {
                        assert_eq!(peer, provider);
                        assert_eq!(id, original);
                        assert_eq!(code, 3);
                        break;
                    }
                },
            }
        }
    })
    .await
    .unwrap();
    assert!(dialer.sidecar_pending.is_empty());
    assert!(dialer.checked_peers.contains(&provider));
    assert_eq!(dialer.peers.len(), MAX_PEERS);
    assert!(!dialer.has_dial_capacity());
    // A request issued before retirement keeps its terminal Closed event for caller cleanup.
    dialer.request_sidecars(provider, request.clone()).unwrap();
    dialer.retire_peer(provider);
    assert!(dialer.sidecar_pending.is_empty());
    assert!(!dialer.checked_peers.contains(&provider));
    assert_eq!(dialer.peers.len(), MAX_PEERS - 1);
    assert!(dialer.has_dial_capacity());
    assert_eq!(
        dialer.request_sidecars(provider, request.clone()),
        Err(TransportError::Configuration)
    );
    timeout(Duration::from_secs(10), async {
        loop {
            tokio::select! {
                _ = listener.next_event() => {},
                event = dialer.next_event() => if let TransportEvent::Closed(peer) = event {
                    assert_eq!(peer, provider);
                    break;
                },
            }
        }
    })
    .await
    .unwrap();
    let mut replacement = transport();
    assert_eq!(
        dialer.request_sidecars(replacement.peer_id(), request),
        Err(TransportError::Configuration)
    );
    connect(&mut replacement, &mut dialer, false).await;
    assert_eq!(dialer.peers.len(), MAX_PEERS);
    assert!(!dialer.has_dial_capacity());
    assert!(dialer.checked_peers.contains(&replacement.peer_id()));
    assert_eq!(dialer.local_status, preserved_status);
    assert_eq!(dialer.local_finalized, preserved_finalized);
    assert_eq!(dialer.canonical_roots, preserved_roots);
    for listener in &mut listeners {
        listener.shutdown();
    }
    replacement.shutdown();
    dialer.shutdown();
}
