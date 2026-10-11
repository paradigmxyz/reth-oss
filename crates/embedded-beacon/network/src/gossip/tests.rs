//! Exact wire vectors and independent allocation/topic boundaries.

use super::*;
use snap::raw::Encoder;

fn message(topic: GossipTopic, bytes: &[u8]) -> Message {
    Message {
        source: None,
        data: Encoder::new().compress_vec(bytes).unwrap(),
        sequence_number: None,
        topic: TopicHash::from_raw(topic.name()),
    }
}
#[test]
fn message_identity_switches_at_actual_historical_schema() {
    let config = ChainConfig::mainnet();
    let phase0 = GossipTopic {
        fork: Fork::Phase0,
        digest: config.fork_digest(0).unwrap(),
        kind: GossipKind::Block,
    };
    let wire = message(phase0, b"original SSZ bytes");
    let mut preimage = vec![1, 0, 0, 0];
    preimage.extend_from_slice(b"original SSZ bytes");
    assert_eq!(
        gossip_message_id(&config, &wire),
        MessageId::from(sha256(&preimage)[..20].to_vec())
    );
    let altair = GossipTopic {
        fork: Fork::Altair,
        digest: config.fork_digest(74240).unwrap(),
        kind: GossipKind::Block,
    };
    let wire = message(altair, b"original SSZ bytes");
    let topic = altair.name();
    let mut preimage = vec![1, 0, 0, 0];
    preimage.extend_from_slice(&(topic.len() as u64).to_le_bytes());
    preimage.extend_from_slice(topic.as_bytes());
    preimage.extend_from_slice(b"original SSZ bytes");
    assert_eq!(
        gossip_message_id(&config, &wire),
        MessageId::from(sha256(&preimage)[..20].to_vec())
    );
}
#[test]
fn malformed_raw_snappy_uses_invalid_domain_without_framed_fallback() {
    let config = ChainConfig::mainnet();
    let topic = GossipTopic {
        fork: Fork::Phase0,
        digest: config.fork_digest(0).unwrap(),
        kind: GossipKind::Block,
    };
    let wire = Message {
        source: None,
        data: vec![0x80],
        sequence_number: None,
        topic: TopicHash::from_raw(topic.name()),
    };
    assert_eq!(decode_gossip(&wire.data, MAX_GOSSIP_BYTES), Err(GossipWireError::Compression));
    assert_eq!(
        gossip_message_id(&config, &wire),
        MessageId::from(sha256(&[0, 0, 0, 0, 0x80])[..20].to_vec())
    );
}
#[test]
fn allocation_claim_is_rejected_before_decoding_contents() {
    // A canonical raw Snappy length prefix declares 10MiB + 1 with no literal body.
    let declared = [0x81, 0x80, 0x80, 0x05];
    assert_eq!(decode_gossip(&declared, MAX_GOSSIP_BYTES), Err(GossipWireError::Oversized));
    let raw = Encoder::new().compress_vec(b"bounded bytes").unwrap();
    assert_eq!(decode_gossip(&raw, 3), Err(GossipWireError::Oversized));
    assert_eq!(decode_gossip(&raw, 100).unwrap(), b"bounded bytes");
}
#[test]
fn topics_reject_unknown_context_noncanonical_subnets_and_wrong_fork_objects() {
    let config = ChainConfig::mainnet();
    let digest = config.fork_digest(0).unwrap();
    let topic = GossipTopic { fork: Fork::Phase0, digest, kind: GossipKind::Block };
    assert_eq!(parse_topic(&config, &topic.name()).unwrap(), topic);
    assert_eq!(
        parse_topic(&config, "/eth2/ffffffff/beacon_block/ssz_snappy"),
        Err(GossipWireError::Topic)
    );
    let invalid = GossipTopic { kind: GossipKind::Envelope, ..topic };
    assert_eq!(parse_topic(&config, &invalid.name()), Err(GossipWireError::Topic));
    let attestation = GossipTopic { kind: GossipKind::Attestation(0), ..topic }.name();
    assert_eq!(
        parse_topic(&config, &attestation.replace("attestation_0", "attestation_00")),
        Err(GossipWireError::Topic)
    );
    let invalid = GossipTopic { kind: GossipKind::Attestation(64), ..topic };
    assert_eq!(parse_topic(&config, &invalid.name()), Err(GossipWireError::Topic));
}
#[test]
fn blob_parameter_transition_has_both_exact_digests_then_drops_old_mesh() {
    let config = ChainConfig::hoodi();
    let before = config.fork_digest(52479).unwrap();
    let after = config.fork_digest(52480).unwrap();
    assert_ne!(before, after);
    let topics = active_topics(&config, 52479 * 32).unwrap();
    assert!(topics.iter().any(|topic| topic.digest == before));
    assert!(topics.iter().any(|topic| topic.digest == after));
    assert!(active_topics(&config, 52483 * 32).unwrap().iter().all(|topic| topic.digest != before));
}
#[test]
fn configured_anonymous_behaviour_requires_native_verdicts() {
    let config = Arc::new(ChainConfig::hoodi());
    let behaviour = behaviour(config, 50700 * 32).unwrap();
    assert_eq!(behaviour.topics().count(), 2);
}

#[test]
fn message_cache_outlives_native_validation_deadline_at_heartbeat_boundary() {
    // An admission just before a heartbeat has one fewer complete interval before eviction.
    let earliest_eviction = GOSSIP_HEARTBEAT_INTERVAL * (GOSSIP_VALIDATION_HISTORY as u32 - 1);
    assert!(earliest_eviction > GOSSIP_VALIDATION_TIMEOUT);
    assert!(earliest_eviction < GOSSIP_VALIDATION_TIMEOUT + Duration::from_secs(2));
}
