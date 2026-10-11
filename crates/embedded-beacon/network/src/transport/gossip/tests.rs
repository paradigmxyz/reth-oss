//! Real TCP anonymous gossip with explicit forwarding and single-use callback controls.

use super::*;
use snap::raw::Encoder;

fn node() -> StatusTransport {
    let config = ChainConfig::mainnet();
    StatusTransport::new(
        Keypair::generate_secp256k1(),
        config.clone(),
        0,
        Checkpoint { root: [1; 32], epoch: 0 },
        vec![(0, [1; 32])],
        crate::StatusV2 {
            status: crate::Status {
                fork_digest: config.fork_digest(0).unwrap(),
                finalized_root: [1; 32],
                finalized_epoch: 0,
                head_root: [1; 32],
                head_slot: 0,
            },
            earliest_available_slot: 0,
        },
    )
    .unwrap()
}
async fn link(left: &mut StatusTransport, right: &mut StatusTransport) {
    left.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()).unwrap();
    let address = loop {
        if let TransportEvent::Listening(address) = left.next_event().await {
            break address
        }
    };
    right.dial(left.peer_id(), address).unwrap();
    let mut a = false;
    let mut b = false;
    while !a || !b {
        tokio::select! {
            event = left.next_event() => if let TransportEvent::PeerStatus { .. } = event { a = true; },
            event = right.next_event() => if let TransportEvent::PeerStatus { .. } = event { b = true; },
        }
    }
}
async fn meshes(a: &mut StatusTransport, b: &mut StatusTransport, c: &mut StatusTransport) {
    loop {
        if a.swarm.behaviour().gossip.all_mesh_peers().count() != 0 &&
            b.swarm.behaviour().gossip.all_mesh_peers().count() == 2 &&
            c.swarm.behaviour().gossip.all_mesh_peers().count() != 0
        {
            return
        }
        tokio::select! { _ = a.next_event() => {}, _ = b.next_event() => {}, _ = c.next_event() => {} }
    }
}
fn publish(node: &mut StatusTransport, data: &[u8]) {
    let topic = crate::gossip::active_topics(&node.config, 0).unwrap()[0];
    node.swarm
        .behaviour_mut()
        .gossip
        .publish(IdentTopic::new(topic.name()), Encoder::new().compress_vec(data).unwrap())
        .unwrap();
}
async fn receive(
    a: &mut StatusTransport,
    b: &mut StatusTransport,
    c: &mut StatusTransport,
) -> (GossipRequestId, GossipInput) {
    loop {
        tokio::select! {
            _ = a.next_event() => {},
            event = b.next_event() => if let TransportEvent::Gossip { id, input, .. } = event { return (id, input) },
            event = c.next_event() => assert!(!matches!(event, TransportEvent::Gossip { .. }), "unvalidated input propagated"),
        }
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_tcp_forwarding_requires_exact_single_use_validation_callback() {
    let mut a = node();
    let mut b = node();
    let mut c = node();
    tokio::time::timeout(Duration::from_secs(10), link(&mut a, &mut b)).await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), link(&mut b, &mut c)).await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), meshes(&mut a, &mut b, &mut c)).await.unwrap();
    // This transport control deliberately supplies bytes only, not a native-validation proof.
    publish(&mut a, b"first original wire object");
    let (id, input) = tokio::time::timeout(Duration::from_secs(5), receive(&mut a, &mut b, &mut c))
        .await
        .unwrap();
    assert_eq!(input.ssz(), b"first original wire object");
    assert_eq!(decode_gossip(input.raw(), 100).unwrap(), input.ssz());
    assert!(b.report_gossip(id, GossipVerdict::Reject).unwrap());
    assert_eq!(b.report_gossip(id, GossipVerdict::Accept), Err(TransportError::Configuration));
    assert_eq!(a.report_gossip(id, GossipVerdict::Accept), Err(TransportError::Configuration));
    publish(&mut a, b"second original wire object");
    let (id, _) = tokio::time::timeout(Duration::from_secs(5), receive(&mut a, &mut b, &mut c))
        .await
        .unwrap();
    assert!(b.report_gossip(id, GossipVerdict::Accept).unwrap());
    let input = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            tokio::select! {
                _ = a.next_event() => {}, _ = b.next_event() => {},
                event = c.next_event() => if let TransportEvent::Gossip { id, input, .. } = event {
                    c.report_gossip(id, GossipVerdict::Ignore).unwrap(); break input;
                },
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(input.ssz(), b"second original wire object");
    assert!(b.gossip_pending.is_empty());
    assert!(c.gossip_pending.is_empty());
}

#[tokio::test]
async fn real_discovery_identity_joins_exact_four_columns_with_configured_fork_overlap() {
    let mut node = node();
    let slot = ChainConfig::mainnet()
        .forks
        .activations()
        .iter()
        .find(|activation| activation.fork == Fork::Fulu)
        .unwrap()
        .epoch *
        32;
    node.join_custody_columns([0; 32]).unwrap();
    assert_eq!(node.custody_columns, Some([1, 17, 87, 102]));
    node.update_gossip_topics(slot).unwrap();
    let names = node
        .gossip_topics
        .iter()
        .map(|name| parse_topic(&node.config, name).unwrap())
        .collect::<Vec<_>>();
    let contexts = names
        .iter()
        .filter(|topic| topic.kind == GossipKind::Block && topic.fork >= Fork::Fulu)
        .count();
    let columns = names
        .iter()
        .filter_map(|topic| match topic.kind {
            GossipKind::Column(column) => Some(column),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(columns.len(), contexts * 4);
    assert!(columns.iter().all(|column| [1, 17, 87, 102].contains(column)));
    assert_eq!(node.local_metadata.seq_number(), 0);
    assert_eq!(
        node.local_metadata.snapshot(ControlProtocol::MetadataV3).custody_group_count,
        Some(0)
    );
}

#[tokio::test]
async fn delayed_validation_token_survives_thirty_seconds_and_expires_without_revoking_owned_input() {
    let mut node = node();
    let topic = active_topics(&node.config, 0).unwrap()[0];
    let ssz = Arc::<[u8]>::from(b"original consensus bytes".as_slice());
    let raw = Arc::<[u8]>::from(Encoder::new().compress_vec(&ssz).unwrap());
    let retained_bytes = (ssz.len() + raw.len()) as u32;
    let lease = Arc::new(GossipLease {
        _bytes: node.gossip_bytes.clone().try_acquire_many_owned(retained_bytes).unwrap(),
    });
    let input = GossipInput { topic, ssz, raw, _lease: lease.clone() };
    let id = GossipRequestId { owner: node.peer_id(), sequence: 1 };
    node.gossip_pending.insert(
        id,
        PendingGossip {
            source: PeerId::random(),
            message: MessageId::from(vec![1; 20]),
            started: tokio::time::Instant::now() - Duration::from_secs(30),
            _lease: lease,
        },
    );
    node.expire_gossip();
    assert!(node.gossip_pending.contains_key(&id));
    node.gossip_pending.get_mut(&id).unwrap().started =
        tokio::time::Instant::now() - GOSSIP_VALIDATION_TIMEOUT;
    node.expire_gossip();
    assert!(node.gossip_pending.is_empty());
    assert_eq!(node.report_gossip(id, GossipVerdict::Accept), Err(TransportError::Configuration));
    // Propagation expiry must not discard bytes still owned by an independent native import.
    assert_eq!(input.ssz(), b"original consensus bytes");
    assert_eq!(decode_gossip(input.raw(), 100).unwrap(), input.ssz());
    assert_eq!(
        node.gossip_bytes.available_permits(),
        (MAX_GOSSIP_RETAINED_BYTES - retained_bytes) as usize,
    );
    drop(input);
    assert_eq!(node.gossip_bytes.available_permits(), MAX_GOSSIP_RETAINED_BYTES as usize);
}
