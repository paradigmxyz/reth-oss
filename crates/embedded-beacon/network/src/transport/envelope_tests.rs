//! Actual authenticated block-then-envelope exchanges over TCP/Noise and QUIC/TLS.

use super::*;
use crate::{
    envelope_rpc::EnvelopeBlockReference,
    test_envelopes::{config, signed_envelope, wire_block, wire_envelope},
};
use reth_embedded_beacon_state_transition::ssz::Preset;
use tokio::time::timeout;

pub(super) fn transport() -> StatusTransport {
    let config = config();
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
    LocalStatusTransport::new(
        Keypair::generate_secp256k1(),
        config,
        12,
        checkpoint,
        Vec::new(),
        status,
    )
    .unwrap()
}

async fn retrieved_references(
    listener: &mut StatusTransport,
    dialer: &mut StatusTransport,
    a: &WireBlock,
    b: &WireBlock,
) -> Vec<EnvelopeBlockReference> {
    let id = dialer
        .request_blocks(listener.peer_id(), BlockRequest::Range { start_slot: 9, count: 4 })
        .unwrap();
    timeout(Duration::from_secs(10), async {
        loop {
            tokio::select! {
                event = listener.next_event() => {
                    let TransportEvent::BlockRequest { peer, id, .. } = event else { panic!("server event {event:?}") };
                    assert_eq!(peer, dialer.peer_id());
                    listener.respond_blocks(id, vec![a.clone(), b.clone()]).unwrap();
                }
                event = dialer.next_event() => {
                    let TransportEvent::Blocks { peer, id: actual, batch } = event else { panic!("client event {event:?}") };
                    assert_eq!(peer, listener.peer_id());
                    assert_eq!(actual, id);
                    assert_eq!(batch.blocks(), [a.clone(), b.clone()]);
                    let references = batch.blocks().iter().map(EnvelopeBlockReference::from).collect();
                    // The coordinator can import or persist these blocks before dropping the batch.
                    // Only bounded small exact metadata survives into the envelope pending request.
                    drop(batch);
                    return references;
                }
            }
        }
    }).await.unwrap()
}

async fn exchange(quic: bool, roots: bool) {
    let mut listener = transport();
    let mut dialer = transport();
    let a = wire_block(9, [1; 32]);
    let b = wire_block(12, a.root());
    let request = if roots {
        EnvelopeRequest::Roots(vec![b.root(), a.root()])
    } else {
        EnvelopeRequest::Range { start_slot: 9, count: 4 }
    };
    let references = vec![EnvelopeBlockReference::from(&a), EnvelopeBlockReference::from(&b)];
    assert_eq!(
        dialer.request_envelopes(listener.peer_id(), request.clone(), references),
        Err(TransportError::Configuration)
    );
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
    let peer = listener.peer_id();
    dialer.dial(peer, address).unwrap();
    super::tests::exchange_connected(&mut listener, &mut dialer, Some(0)).await;
    let references = retrieved_references(&mut listener, &mut dialer, &a, &b).await;
    assert_eq!(
        dialer.request_envelopes(peer, EnvelopeRequest::Roots(vec![[9; 32]]), references.clone()),
        Err(TransportError::Configuration)
    );
    let id = dialer.request_envelopes(peer, request.clone(), references.clone()).unwrap();
    assert_eq!(
        dialer.request_envelopes(peer, request.clone(), references.clone()),
        Err(TransportError::Capacity)
    );
    assert_eq!(
        dialer.request_blocks(peer, BlockRequest::Roots(vec![a.root()])),
        Err(TransportError::Capacity)
    );
    timeout(Duration::from_secs(10), async {
        loop {
            tokio::select! {
                event = listener.next_event() => {
                    let TransportEvent::EnvelopeRequest { peer: requester, id, request: actual } = event
                        else { panic!("server event {event:?}") };
                    assert_eq!(requester, dialer.peer_id());
                    assert_eq!(actual, request);
                    let unrelated = wire_block(8, [1; 32]);
                    assert_eq!(listener.respond_envelopes(id, vec![wire_envelope(&unrelated)], &references),
                        Err(TransportError::Configuration));
                    assert!(listener.envelope_inbound.contains_key(&id));
                    let mut foreign_config = config();
                    foreign_config.genesis_validators_root = [9; 32];
                    let foreign_block = WireBlock::from_ssz(a.ssz().to_vec(), &foreign_config).unwrap();
                    let foreign_envelope = WireEnvelope::from_ssz(
                        signed_envelope(&a).to_ssz(Preset::Minimal).unwrap(), &foreign_block, &foreign_config,
                    ).unwrap();
                    assert_eq!(listener.respond_envelopes(id, vec![foreign_envelope], &references),
                        Err(TransportError::Configuration));
                    assert!(listener.envelope_inbound.contains_key(&id));
                    let foreign_id = EnvelopeRequestId { owner: dialer.peer_id(), sequence: id.sequence };
                    assert_eq!(listener.respond_envelopes_unavailable(foreign_id),
                        Err(TransportError::Configuration));
                    assert!(listener.envelope_inbound.contains_key(&id));
                    listener.respond_envelopes(id, vec![wire_envelope(&a), wire_envelope(&b)], &references).unwrap();
                    assert_eq!(listener.respond_envelopes_unavailable(id), Err(TransportError::Configuration));
                }
                event = dialer.next_event() => {
                    let TransportEvent::Envelopes { peer: actual_peer, id: actual_id, batch } = event
                        else { panic!("client event {event:?}") };
                    assert_eq!(actual_peer, peer);
                    assert_eq!(actual_id, id);
                    assert_eq!(batch.envelopes(), [wire_envelope(&a), wire_envelope(&b)]);
                    assert_eq!(request.matches(&config(), &batch,
                        |root| references.iter().find(|block| block.root() == root)), Ok(()));
                    drop(batch);
                    break;
                }
            }
        }
    }).await.unwrap();
    let unavailable = dialer.request_envelopes(peer, request.clone(), references.clone()).unwrap();
    timeout(Duration::from_secs(10), async {
        loop {
            tokio::select! {
                event = listener.next_event() => {
                    let TransportEvent::EnvelopeRequest { id, .. } = event else { panic!("server event {event:?}") };
                    listener.respond_envelopes_unavailable(id).unwrap();
                }
                event = dialer.next_event() => {
                    assert_eq!(event, TransportEvent::EnvelopesUnavailable { peer, id: unavailable, code: 3 });
                    break;
                }
            }
        }
    }).await.unwrap();
    // A pending RPC and its private inbound channel disappear on explicit cancellation/shutdown.
    dialer.request_envelopes(peer, request, references).unwrap();
    timeout(Duration::from_secs(10), async {
        tokio::select! {
            event = listener.next_event() => {
                let TransportEvent::EnvelopeRequest { id, .. } = event else { panic!("server event {event:?}") };
                assert!(listener.envelope_inbound.contains_key(&id));
            }
            event = dialer.next_event() => panic!("unexpected client event {event:?}"),
        }
    }).await.unwrap();
    listener.shutdown();
    dialer.shutdown();
    assert!(listener.block_inbound.is_empty());
    assert!(listener.envelope_inbound.is_empty());
    assert!(dialer.block_pending.is_empty());
    assert!(dialer.envelope_pending.is_empty());
}

#[tokio::test]
async fn actual_tcp_noise_range_envelopes_bind_original_retrieved_blocks() {
    exchange(false, false).await;
}

#[tokio::test]
async fn actual_quic_tls_root_envelopes_bind_original_retrieved_blocks() {
    exchange(true, true).await;
}

#[tokio::test]
async fn simultaneous_bidirectional_blocks_and_envelopes_preserve_original_owners() {
    let mut server = transport();
    let mut client = transport();
    server.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()).unwrap();
    let TransportEvent::Listening(address) =
        timeout(Duration::from_secs(10), server.next_event()).await.unwrap()
    else {
        panic!("actual simultaneous TCP listener did not start");
    };
    let server_peer = server.peer_id();
    let client_peer = client.peer_id();
    client.dial(server_peer, address).unwrap();
    super::tests::exchange_connected(&mut server, &mut client, Some(0)).await;
    let first = wire_block(9, [1; 32]);
    let last = wire_block(12, first.root());
    let blocks = vec![first.clone(), last.clone()];
    let envelopes = vec![wire_envelope(&first), wire_envelope(&last)];
    let references = blocks.iter().map(EnvelopeBlockReference::from).collect::<Vec<_>>();

    for roots in [false, true] {
        let request = if roots {
            BlockRequest::Roots(vec![first.root(), last.root()])
        } else {
            BlockRequest::Range { start_slot: 9, count: 4 }
        };
        let server_original = server.request_blocks(client_peer, request.clone()).unwrap();
        let client_original = client.request_blocks(server_peer, request.clone()).unwrap();
        assert_eq!(
            server.request_blocks(client_peer, request.clone()),
            Err(TransportError::Capacity)
        );
        assert_eq!(
            client.request_blocks(server_peer, request.clone()),
            Err(TransportError::Capacity)
        );
        timeout(Duration::from_secs(10), async {
            let mut completed = [false; 2];
            while !completed.iter().all(|received| *received) {
                let (side, event) = tokio::select! {
                    event = server.next_event() => (0, event),
                    event = client.next_event() => (1, event),
                };
                let (owner, remote, original) = if side == 0 {
                    (&mut server, client_peer, server_original)
                } else {
                    (&mut client, server_peer, client_original)
                };
                match event {
                    TransportEvent::BlockRequest { peer, id, request: observed } => {
                        assert_eq!(peer, remote);
                        assert_eq!(observed, request);
                        owner.respond_blocks(id, blocks.clone()).unwrap();
                    }
                    TransportEvent::Blocks { peer, id, batch } => {
                        assert_eq!(peer, remote);
                        assert_eq!(id, original);
                        assert_eq!(batch.blocks(), blocks.as_slice());
                        request.matches(batch.blocks()).unwrap();
                        assert!(!completed[side]);
                        completed[side] = true;
                    }
                    TransportEvent::PeerStatus { .. } => {}
                    other => panic!("actual simultaneous block event: {other:?}"),
                }
            }
        })
        .await
        .unwrap();
        assert!(server.block_pending.is_empty());
        assert!(client.block_pending.is_empty());
        assert!(server.block_inbound.is_empty());
        assert!(client.block_inbound.is_empty());

        let request = if roots {
            EnvelopeRequest::Roots(vec![first.root(), last.root()])
        } else {
            EnvelopeRequest::Range { start_slot: 9, count: 4 }
        };
        let server_original =
            server.request_envelopes(client_peer, request.clone(), references.clone()).unwrap();
        let client_original =
            client.request_envelopes(server_peer, request.clone(), references.clone()).unwrap();
        assert_eq!(
            server.request_envelopes(client_peer, request.clone(), references.clone()),
            Err(TransportError::Capacity)
        );
        assert_eq!(
            client.request_envelopes(server_peer, request.clone(), references.clone()),
            Err(TransportError::Capacity)
        );
        timeout(Duration::from_secs(10), async {
            let mut completed = [false; 2];
            while !completed.iter().all(|received| *received) {
                let (side, event) = tokio::select! {
                    event = server.next_event() => (0, event),
                    event = client.next_event() => (1, event),
                };
                let (owner, remote, original) = if side == 0 {
                    (&mut server, client_peer, server_original)
                } else {
                    (&mut client, server_peer, client_original)
                };
                match event {
                    TransportEvent::EnvelopeRequest { peer, id, request: observed } => {
                        assert_eq!(peer, remote);
                        assert_eq!(observed, request);
                        owner.respond_envelopes(id, envelopes.clone(), &references).unwrap();
                    }
                    TransportEvent::Envelopes { peer, id, batch } => {
                        assert_eq!(peer, remote);
                        assert_eq!(id, original);
                        assert_eq!(batch.envelopes(), envelopes.as_slice());
                        request
                            .matches(&config(), &batch, |root| {
                                references.iter().find(|block| block.root() == root)
                            })
                            .unwrap();
                        assert!(!completed[side]);
                        completed[side] = true;
                    }
                    TransportEvent::PeerStatus { .. } => {}
                    other => panic!("actual simultaneous envelope event: {other:?}"),
                }
            }
        })
        .await
        .unwrap();
        assert!(server.envelope_pending.is_empty());
        assert!(client.envelope_pending.is_empty());
        assert!(server.envelope_inbound.is_empty());
        assert!(client.envelope_inbound.is_empty());
        assert!(server.checked_peers.contains(&client_peer));
        assert!(client.checked_peers.contains(&server_peer));
    }
    server.shutdown();
    client.shutdown();
}
