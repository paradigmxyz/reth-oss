//! Cache budgets remain reserved until actual private-cache eviction evidence is observed.

use super::*;
use libp2p_gossipsub::IdentTopic;
use prometheus_client::metrics::histogram::Histogram;

fn message(tag: u8) -> Message {
    let config = ChainConfig::hoodi();
    let topic = active_topics(&config, 3000 * 32).unwrap().remove(0).name();
    Message {
        source: None,
        data: vec![tag; MAX_GOSSIP_BYTES],
        sequence_number: None,
        topic: IdentTopic::new(topic).hash(),
    }
}

#[test]
fn duplicate_and_elapsed_ttl_cannot_release_raw_bytes_without_completed_cache_shifts() {
    let ingress = BoundedTransform::new(Arc::new(ChainConfig::hoodi())).unwrap();
    for tag in 1..=3 {
        ingress.reserve(&message(tag)).unwrap();
    }
    ingress.settled();
    assert_eq!(ingress.reserve(&message(1)).unwrap_err().kind(), io::ErrorKind::WouldBlock);
    assert_eq!(ingress.state.lock().unwrap().entries.len(), 3);
    assert_eq!(ingress.reserve(&message(4)).unwrap_err().kind(), io::ErrorKind::WouldBlock);
    {
        let mut state = ingress.state.lock().unwrap();
        for entry in state.entries.values_mut() {
            entry.expires = Some(Instant::now().checked_sub(Duration::from_secs(1)).unwrap());
        }
    }
    assert_eq!(ingress.reserve(&message(4)).unwrap_err().kind(), io::ErrorKind::WouldBlock);
    ingress.completed_heartbeats(CACHE_HISTORY);
    assert_eq!(ingress.state.lock().unwrap().raw_bytes, 3 * MAX_GOSSIP_BYTES);
    ingress.completed_heartbeats(CACHE_HISTORY + 1);
    assert_eq!(ingress.state.lock().unwrap().raw_bytes, 0);
    ingress.reserve(&message(4)).unwrap();
    assert_eq!(ingress.state.lock().unwrap().entries.len(), 1);
}

#[test]
fn metric_sink_is_bounded_exact_unlabelled_and_monotone_retirement_fails_closed() {
    let mut registry = Registry::default();
    let completed = Histogram::new([0.0, 1.0]);
    registry.register("heartbeat_duration", "actual completed heartbeat", completed.clone());
    completed.observe(0.0);
    let mut sink = HeartbeatSink::default();
    encode(&mut sink, &registry).unwrap();
    assert_eq!(sink.completed, Some(1));
    let mut sink = HeartbeatSink::default();
    sink.write_str(
        "unrelated_heartbeat_duration_count 80\nheartbeat_duration_count{wrong=\"label\"} 9\n",
    )
    .unwrap();
    assert_eq!(sink.completed, None);
    sink.write_str("heartbeat_duration_co").unwrap();
    sink.write_str("unt 7\n").unwrap();
    assert_eq!(sink.completed, Some(7));
    assert!(sink.write_str("heartbeat_duration_count 8\n").is_err());
    let mut oversized = HeartbeatSink::default();
    assert!(oversized.write_str(&"x".repeat(MAX_METRICS_BYTES + 1)).is_err());
    let ingress = BoundedTransform::new(Arc::new(ChainConfig::hoodi())).unwrap();
    ingress.reserve(&message(1)).unwrap();
    ingress.completed_heartbeats(4);
    ingress.completed_heartbeats(3);
    assert_eq!(ingress.reserve(&message(2)).unwrap_err().kind(), io::ErrorKind::WouldBlock);
}

#[test]
fn duplicate_id_budget_stays_charged_for_full_post_handler_ttl_after_raw_retirement() {
    let ingress = BoundedTransform::new(Arc::new(ChainConfig::hoodi())).unwrap();
    ingress.reserve(&message(1)).unwrap();
    {
        let state = ingress.state.lock().unwrap();
        assert!(state.entries.values().all(|entry| entry.expires.is_none()));
    }
    let before_settlement = Instant::now();
    ingress.settled();
    ingress.completed_heartbeats(CACHE_HISTORY + 1);
    let state = ingress.state.lock().unwrap();
    assert_eq!(state.raw_bytes, 0);
    assert_eq!(state.entries.len(), 1);
    assert!(state
        .entries
        .values()
        .all(|entry| entry.expires.unwrap() >= before_settlement + ingress.ttl));
}

#[test]
fn accepted_raw_bytes_remain_charged_through_fresh_short_cache_history() {
    let ingress = BoundedTransform::new(Arc::new(ChainConfig::hoodi())).unwrap();
    let id = MessageId::from(vec![1; 20]);
    ingress.reserve_id(id.clone(), MAX_RAW_CACHE_BYTES, false).unwrap();
    ingress.settled();
    ingress.completed_heartbeats(87);
    ingress.validation_completed(&id, true, Some(87));
    ingress.completed_heartbeats(88);
    assert_eq!(ingress.state.lock().unwrap().raw_bytes, MAX_RAW_CACHE_BYTES);
    ingress.completed_heartbeats(87 + GOSSIP_CACHE_HISTORY as u64);
    assert_eq!(ingress.state.lock().unwrap().raw_bytes, MAX_RAW_CACHE_BYTES);
    ingress.completed_heartbeats(87 + GOSSIP_CACHE_HISTORY as u64 + 1);
    assert_eq!(ingress.state.lock().unwrap().raw_bytes, 0);
    assert_eq!(ingress.state.lock().unwrap().entries.len(), 1);
}

#[test]
fn definitive_cache_removal_releases_raw_bytes_but_preserves_duplicate_id_budget() {
    let ingress = BoundedTransform::new(Arc::new(ChainConfig::hoodi())).unwrap();
    let id = MessageId::from(vec![2; 20]);
    ingress.reserve_id(id.clone(), MAX_RAW_CACHE_BYTES, false).unwrap();
    ingress.settled();
    ingress.validation_completed(&id, false, Some(0));
    let state = ingress.state.lock().unwrap();
    assert_eq!(state.raw_bytes, 0);
    assert_eq!(state.entries.len(), 1);
    assert_eq!(state.entries.get(&id).unwrap().bytes, 0);
}

#[test]
fn accepted_raw_bytes_fail_closed_when_completed_shift_evidence_is_missing() {
    let ingress = BoundedTransform::new(Arc::new(ChainConfig::hoodi())).unwrap();
    let id = MessageId::from(vec![3; 20]);
    ingress.reserve_id(id.clone(), MAX_RAW_CACHE_BYTES, false).unwrap();
    ingress.settled();
    ingress.validation_completed(&id, true, None);
    ingress.completed_heartbeats(10_000);
    assert_eq!(ingress.state.lock().unwrap().raw_bytes, MAX_RAW_CACHE_BYTES);
    assert_eq!(
        ingress.reserve_id(MessageId::from(vec![4; 20]), 1, false).unwrap_err().kind(),
        io::ErrorKind::WouldBlock,
    );
}
