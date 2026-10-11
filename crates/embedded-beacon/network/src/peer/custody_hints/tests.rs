//! Provider hints preserve signed identity, bounded membership and expiring advisory claims.

use super::*;
use crate::{
    discovery::{sign_local_enr, LocalAdvertisement},
    EndpointPolicy,
};
use discv5::enr::CombinedKey;
use reth_embedded_beacon_config::ChainConfig;
use std::net::{Ipv4Addr, SocketAddr};

fn record(key: &CombinedKey, sequence: u64, count: u64) -> DiscoveryRecord {
    let config = ChainConfig::hoodi();
    let slot = 50700 * 32;
    let advertisement = LocalAdvertisement {
        discovery_socket: SocketAddr::from((Ipv4Addr::LOCALHOST, 9050)),
        tcp_port: 9000,
        quic_port: 9001,
        attnets: [0; 8],
        syncnets: 0,
        custody_group_count: count,
    };
    let enr =
        sign_local_enr(key, sequence, &config, slot, &advertisement, EndpointPolicy::LoopbackOnly)
            .unwrap();
    DiscoveryRecord::verify(enr, &config, slot, EndpointPolicy::LoopbackOnly).unwrap()
}

fn metadata(sequence: u64, count: u64) -> PeerMetadata {
    PeerMetadata {
        seq_number: sequence,
        attnets: [0; 8],
        syncnets: Some(0),
        custody_group_count: Some(count),
    }
}

fn columns(record: &DiscoveryRecord, count: u64) -> Vec<u64> {
    let mut identity = record.node_id().raw();
    identity.reverse();
    CustodyParameters::default().get_custody_groups(identity, count).unwrap()
}

#[test]
fn signed_record_uses_complete_bound_node_identity_and_checked_status_only() {
    let key = CombinedKey::generate_secp256k1();
    let record = record(&key, 7, 4);
    let peer = record.peer_id();
    let requested = columns(&record, 4);
    let now = Instant::now();
    let mut hints = CustodyHints::default();
    hints.record(&record, now);
    assert_eq!(hints.overlap(peer, &requested, now), 0);
    hints.checked(peer, now);
    assert_eq!(hints.overlap(peer, &requested, now), 4);
    assert_eq!(hints.overlap(peer, &[128, u64::MAX], now), 0);
    assert_eq!(hints.overlap(PeerId::random(), &requested, now), 0);
    let absent = (0..128).filter(|column| !requested.contains(column)).collect::<Vec<_>>();
    assert_eq!(hints.overlap(peer, &absent, now), 0);
    hints.remove(peer);
    assert_eq!(hints.overlap(peer, &requested, now), 0);
    assert!(hints.peers.is_empty());
}

#[test]
fn metadata_updates_only_exact_checked_peer_with_monotonic_valid_claims() {
    let now = Instant::now();
    let mut hints = CustodyHints::default();
    let record = record(&CombinedKey::generate_secp256k1(), 7, 4);
    let peer = record.peer_id();
    let requested = columns(&record, 4);
    hints.record(&record, now);
    hints.metadata(peer, metadata(1, 128), now);
    assert!(hints.peers[&peer].metadata.is_none());
    hints.checked(peer, now);
    hints.metadata(PeerId::random(), metadata(1, 128), now);
    assert_eq!(hints.peers.len(), 1);
    hints.metadata(peer, metadata(1, 4), now);
    assert_eq!(hints.overlap(peer, &requested, now), 4);
    let absent = (0..128).find(|column| !requested.contains(column)).unwrap();
    for invalid in [metadata(0, 128), metadata(1, 128), metadata(2, 129)] {
        hints.metadata(peer, invalid, now);
        assert_eq!(hints.overlap(peer, &[absent], now), 0);
        assert_eq!(hints.peers[&peer].metadata, Some(metadata(1, 4)));
    }
    let mut invalid = metadata(2, 128);
    invalid.syncnets = Some(0xf0);
    hints.metadata(peer, invalid, now);
    invalid.syncnets = None;
    hints.metadata(peer, invalid, now);
    assert_eq!(hints.peers[&peer].metadata, Some(metadata(1, 4)));
    hints.metadata(peer, metadata(2, 128), now);
    assert_eq!(hints.overlap(peer, &[0, absent, 127], now), 3);
    hints.metadata(peer, metadata(3, 0), now);
    assert_eq!(hints.overlap(peer, &requested, now), 0);
}

#[test]
fn stale_signed_record_cannot_replace_newer_custody_or_independent_metadata() {
    let key = CombinedKey::generate_secp256k1();
    let original = record(&key, 9, 4);
    let peer = original.peer_id();
    let requested = columns(&original, 4);
    let now = Instant::now();
    let mut hints = CustodyHints::default();
    hints.record(&original, now);
    hints.checked(peer, now);
    hints.record(&record(&key, 8, 128), now);
    hints.record(&record(&key, 9, 128), now);
    assert_eq!(hints.peers[&peer].record_count, Some(4));
    hints.metadata(peer, metadata(1, 0), now);
    hints.record(&record(&key, 10, 128), now);
    assert_eq!(hints.overlap(peer, &requested, now), 0);
    assert_eq!(hints.peers[&peer].metadata, Some(metadata(1, 0)));
}

#[test]
fn expiry_keeps_active_identity_for_fresh_metadata_without_refreshing_old_data() {
    let original = record(&CombinedKey::generate_secp256k1(), 7, 4);
    let peer = original.peer_id();
    let requested = columns(&original, 4);
    let now = Instant::now();
    let expired = now + HINT_LIFETIME;
    let mut hints = CustodyHints::default();
    hints.record(&original, now);
    hints.checked(peer, now + HINT_LIFETIME - Duration::from_nanos(1));
    assert_eq!(hints.overlap(peer, &requested, expired), 0);
    assert_eq!(hints.overlap(peer, &requested, now - Duration::from_nanos(1)), 0);
    hints.metadata(peer, metadata(1, 4), expired);
    assert_eq!(hints.overlap(peer, &requested, expired), 4);
    assert_eq!(hints.peers[&peer].node_id, Some(original.node_id().raw()));
    hints.clear();
    assert!(hints.peers.is_empty());
}

#[test]
fn existing_peer_cap_unknown_identity_and_expired_eviction_are_preserved() {
    let now = Instant::now();
    let mut hints = CustodyHints::default();
    for _ in 0..MAX_PEERS {
        let peer = PeerId::random();
        hints.checked(peer, now);
        hints.metadata(peer, metadata(1, 4), now);
        assert_eq!(hints.overlap(peer, &[0, 1, 2, 3], now), 0);
    }
    let extra = PeerId::random();
    hints.checked(extra, now);
    hints.metadata(extra, metadata(1, 128), now);
    assert_eq!(hints.peers.len(), MAX_PEERS);
    assert_eq!(hints.overlap(extra, &[0, 127], now), 0);
    hints.checked(extra, now + HINT_LIFETIME);
    hints.metadata(extra, metadata(1, 128), now + HINT_LIFETIME);
    assert_eq!(hints.peers.len(), 1);
    assert_eq!(hints.overlap(extra, &[0, 127], now + HINT_LIFETIME), 2);
}
