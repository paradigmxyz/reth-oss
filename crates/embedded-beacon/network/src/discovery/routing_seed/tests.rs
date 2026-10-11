//! Genuine published ENR signature controls and signed routing-policy regressions.

use super::*;
use crate::discovery::{sign_local_enr, DiscoveryRecord, DiscoveryService, LocalAdvertisement};
use discv5::{enr::CombinedKey, ListenConfig};
use reth_embedded_beacon_config::ChainConfig;
use std::net::{Ipv4Addr, SocketAddr};

// Exact unchanged ENRs extracted from the official original inventory at the recorded commit.
const PUBLISHED: &str = include_str!("testdata/hoodi-bootstrap-210613b3.txt");
const CURRENT_SLOT: u64 = 54016 * 32;

fn record(ip: &str, port: u16) -> Enr {
    Enr::builder()
        .ip(ip.parse::<IpAddr>().unwrap())
        .udp4(port)
        .build(&CombinedKey::generate_secp256k1())
        .unwrap()
}

#[test]
fn original_official_udp_only_and_historical_fork_seeds_have_only_routing_authority() {
    let mut count = 0;
    let mut discovery_only = 0;
    for text in PUBLISHED.lines() {
        let routing = RoutingSeedRecord::parse(text, EndpointPolicy::Public).unwrap();
        assert!(routing.enr.verify());
        assert!(table_filter(EndpointPolicy::Public)(&routing.enr));
        assert!(DiscoveryRecord::parse(
            text,
            &ChainConfig::hoodi(),
            CURRENT_SLOT,
            EndpointPolicy::Public
        )
        .is_err());
        discovery_only += usize::from(routing.enr.get_raw_rlp("eth2").is_none());
        count += 1;
    }
    assert_eq!(count, 8);
    assert_eq!(discovery_only, 6);
}

#[tokio::test]
async fn actual_routing_table_admission_never_populates_the_consensus_candidate_queue() {
    let config = ChainConfig::hoodi();
    let key = CombinedKey::generate_secp256k1();
    let local = sign_local_enr(
        &key,
        1,
        &config,
        CURRENT_SLOT,
        &LocalAdvertisement {
            discovery_socket: SocketAddr::from(([8, 8, 8, 8], 9050)),
            tcp_port: 9000,
            quic_port: 9001,
            attnets: [0; 8],
            syncnets: 0,
            custody_group_count: 0,
        },
        EndpointPolicy::Public,
    )
    .unwrap();
    let mut service = DiscoveryService::new(
        config,
        CURRENT_SLOT,
        local.clone(),
        key,
        ListenConfig::Ipv4 { ip: Ipv4Addr::UNSPECIFIED, port: 9050 },
        EndpointPolicy::Public,
    )
    .unwrap();
    for text in PUBLISHED.lines() {
        service.add_bootstrap_seed(text).unwrap();
        assert!(service.add_seed(text).is_err());
        let enr = text.parse::<Enr>().unwrap();
        assert!(service.enqueue(enr).is_err());
        assert!(service.candidates.is_empty());
    }
    assert_eq!(service.add_bootstrap_seed(&local.to_base64()), Err(DiscoveryError::Record));
    assert!(service.events.is_none());
    assert!(service.next_candidate().is_none());
}

#[test]
fn invalid_signature_size_and_identity_never_enter_routing() {
    let mut text = PUBLISHED.lines().next().unwrap().as_bytes().to_vec();
    text[14] = if text[14] == b'A' { b'B' } else { b'A' };
    assert_eq!(
        RoutingSeedRecord::parse(std::str::from_utf8(&text).unwrap(), EndpointPolicy::Public)
            .unwrap_err(),
        DiscoveryError::Record
    );
    assert_eq!(
        RoutingSeedRecord::parse(&format!("enr:{}", "A".repeat(401)), EndpointPolicy::Public)
            .unwrap_err(),
        DiscoveryError::Record
    );
    let ed = Enr::builder()
        .ip("8.8.8.8".parse::<IpAddr>().unwrap())
        .udp4(9000)
        .build(&CombinedKey::generate_ed25519())
        .unwrap();
    assert_eq!(
        RoutingSeedRecord::parse(&ed.to_base64(), EndpointPolicy::Public).unwrap_err(),
        DiscoveryError::Record
    );
    assert!(!table_filter(EndpointPolicy::Public)(&ed));
}

#[test]
fn policy_is_identical_for_seed_insertion_and_discovery_routing_replies() {
    for ip in
        ["127.0.0.1", "10.1.2.3", "192.168.1.1", "169.254.1.1", "0.0.0.0", "224.0.0.1", "192.0.2.1"]
    {
        let enr = record(ip, 9000);
        assert_eq!(
            RoutingSeedRecord::parse(&enr.to_base64(), EndpointPolicy::Public).unwrap_err(),
            DiscoveryError::Endpoint
        );
        assert!(!table_filter(EndpointPolicy::Public)(&enr));
    }
    let loopback = record("127.0.0.1", 9000);
    assert!(RoutingSeedRecord::parse(&loopback.to_base64(), EndpointPolicy::LoopbackOnly).is_ok());
    assert!(table_filter(EndpointPolicy::LoopbackOnly)(&loopback));
    assert!(!table_filter(EndpointPolicy::Public)(&loopback));
    let private = record("10.1.2.3", 9000);
    assert!(RoutingSeedRecord::parse(&private.to_base64(), EndpointPolicy::PrivateNetwork).is_ok());
    assert!(!table_filter(EndpointPolicy::LoopbackOnly)(&private));
    assert!(!table_filter(EndpointPolicy::PrivateNetwork)(&record("8.8.8.8", 9000)));
}

#[test]
fn zero_missing_malformed_or_unpaired_udp_and_any_private_family_are_rejected() {
    let key = CombinedKey::generate_secp256k1();
    let zero = record("8.8.8.8", 0);
    assert_eq!(
        RoutingSeedRecord::parse(&zero.to_base64(), EndpointPolicy::Public).unwrap_err(),
        DiscoveryError::Endpoint
    );
    let missing = Enr::builder().ip("8.8.8.8".parse::<IpAddr>().unwrap()).build(&key).unwrap();
    assert_eq!(
        RoutingSeedRecord::parse(&missing.to_base64(), EndpointPolicy::Public).unwrap_err(),
        DiscoveryError::Endpoint
    );
    let unpaired = Enr::builder().udp4(9000).build(&key).unwrap();
    assert_eq!(
        RoutingSeedRecord::parse(&unpaired.to_base64(), EndpointPolicy::Public).unwrap_err(),
        DiscoveryError::Endpoint
    );
    let malformed = Enr::builder()
        .ip("8.8.8.8".parse::<IpAddr>().unwrap())
        .add_value("udp", &[1u8, 2, 3].as_slice())
        .build(&key)
        .unwrap();
    assert_eq!(
        RoutingSeedRecord::parse(&malformed.to_base64(), EndpointPolicy::Public).unwrap_err(),
        DiscoveryError::Record
    );
    let mixed = Enr::builder()
        .ip("8.8.8.8".parse::<IpAddr>().unwrap())
        .udp4(9000)
        .ip("::1".parse::<IpAddr>().unwrap())
        .udp6(9000)
        .build(&key)
        .unwrap();
    assert_eq!(
        RoutingSeedRecord::parse(&mixed.to_base64(), EndpointPolicy::Public).unwrap_err(),
        DiscoveryError::Endpoint
    );
    assert!(!table_filter(EndpointPolicy::Public)(&mixed));
}
