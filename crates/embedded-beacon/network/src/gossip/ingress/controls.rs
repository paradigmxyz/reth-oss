//! Control admission before private gossip caches or scoring mutate.
//!
//! The transport owns per-peer and global reservations. Capacity failures remove only the
//! affected ingress before upstream validation, while malformed published messages retain
//! the upstream invalid-message path. Accepted messages and controls are never re-encoded.
//! Real connection events and endpoints remain untouched. Dependency cleanup and score-record
//! capacity have separate owners; this helper does not claim a bound on all transport memory.

use super::InnerBehaviour;
use crate::gossip::{parse_topic, MAX_GOSSIP_WIRE_BYTES};
use libp2p::swarm::THandlerOutEvent;
use libp2p_gossipsub::{self as gossipsub, RawMessage, ValidationError};
use reth_embedded_beacon_config::ChainConfig;

const MAX_CONTROL_ACTIONS: usize = 16;
const MAX_CONTROL_IDS: usize = 1024;
const MAX_CONTROL_BYTES: usize = 64 * 1024;
const MAX_PRUNE_PEERS: usize = 16;
const LOCAL_PRUNE_BACKOFF_SECONDS: u64 = 60;
const MAX_REMOTE_PRUNE_BACKOFF_SECONDS: u64 = 3600;

/// Conservative original control material and metadata charged by the transport owner.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct ControlCharge {
    /// IDs across IHAVE, IWANT and IDONTWANT, each exactly 20 bytes.
    pub ids: usize,
    /// Original topic, ID and peer bytes plus fixed per-entry metadata allowance.
    pub bytes: usize,
    /// Control actions that may create an upstream response or backoff record.
    pub actions: usize,
    /// Minimum advertised backoff retention; the owner adds configured slack and ring cleanup.
    pub backoff_seconds: u64,
}

/// Explicit local ingress outcomes; none create native consensus authority.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct FilterStats {
    /// Published messages removed for local resource or duplicate admission.
    pub ignored_messages: usize,
    /// Published messages sent through the upstream malformed-envelope rejection path.
    pub invalid_messages: usize,
    /// Controls removed for local resource or structural admission.
    pub ignored_controls: usize,
}

/// Public associated handler event; its original controls need no private type imports.
pub(super) type HandlerEvent = THandlerOutEvent<InnerBehaviour>;

/// Filter before delegating to the pinned upstream behaviour.
///
/// Both callbacks must reserve against the actual authenticated propagation peer and the
/// same global owner. Message reservations must exist before the data transform succeeds.
/// A controls callback returning true must retain its charge through actual promise/backoff
/// cleanup, including peer churn. Resource failures cause no invalid-message score penalty.
pub(super) fn filter_event(
    event: HandlerEvent,
    config: &ChainConfig,
    mut admit_message: impl FnMut(&RawMessage) -> bool,
    mut admit_controls: impl FnMut(ControlCharge) -> bool,
) -> (HandlerEvent, FilterStats) {
    let mut stats = FilterStats::default();
    let HandlerEvent::Message { mut rpc, mut invalid_messages } = event else {
        return (event, stats)
    };
    rpc.messages.retain(|message| {
        if let Some(reason) = envelope_error(message, config) {
            invalid_messages.push((message.clone(), reason));
            stats.invalid_messages += 1;
            return false
        }
        if !admit_message(message) {
            stats.ignored_messages += 1;
            return false
        }
        true
    });
    if !rpc.control_msgs.is_empty() {
        let charge = control_charge(&rpc, config);
        if !charge.is_some_and(&mut admit_controls) {
            stats.ignored_controls = rpc.control_msgs.len();
            rpc.control_msgs.clear();
        }
    }
    (HandlerEvent::Message { rpc, invalid_messages }, stats)
}

fn envelope_error(message: &RawMessage, config: &ChainConfig) -> Option<ValidationError> {
    if message.source.is_some() {
        Some(ValidationError::MessageSourcePresent)
    } else if message.sequence_number.is_some() {
        Some(ValidationError::SequenceNumberPresent)
    } else if message.signature.is_some() {
        Some(ValidationError::SignaturePresent)
    } else if message.key.is_some() {
        // Anonymous upstream mode checks the other three fields but does not check key.
        // StrictNoSign requires all four absent. TransformFailed is its available generic
        // envelope-admission error; the original malformed object is rejected, never cached.
        Some(ValidationError::TransformFailed)
    } else if message.data.len() > MAX_GOSSIP_WIRE_BYTES {
        Some(ValidationError::MessageSizeTooLargeForTopic)
    } else if parse_topic(config, message.topic.as_str()).is_err() {
        Some(ValidationError::TransformFailed)
    } else {
        None
    }
}

#[allow(deprecated)]
fn control_charge(rpc: &gossipsub::Rpc, config: &ChainConfig) -> Option<ControlCharge> {
    if rpc.control_msgs.len() > MAX_CONTROL_ACTIONS {
        return None
    }
    // Upstream's public conversion exposes the protobuf control fields. Clone only controls,
    // avoiding copies of any payload or subscription. Original controls, not this inspection
    // copy, are delegated unchanged on success. Its transient allocation is wire-frame bounded.
    let inspected = gossipsub::Rpc {
        messages: Vec::new(),
        subscriptions: Vec::new(),
        control_msgs: rpc.control_msgs.clone(),
    }
    .into_protobuf();
    let control = inspected.control?;
    let mut charge = ControlCharge { actions: rpc.control_msgs.len(), ..Default::default() };
    let action_bytes = charge.actions.checked_mul(64)?;
    add_bytes(&mut charge, action_bytes)?;
    for entry in control.ihave {
        add_topic(&mut charge, config, entry.topic_id.as_deref())?;
        add_ids(&mut charge, &entry.message_ids)?;
    }
    for entry in control.iwant {
        add_ids(&mut charge, &entry.message_ids)?;
    }
    for entry in control.idontwant {
        add_ids(&mut charge, &entry.message_ids)?;
    }
    for entry in control.graft {
        record_backoff(&mut charge, None);
        add_topic(&mut charge, config, entry.topic_id.as_deref())?;
    }
    for entry in control.prune {
        record_backoff(&mut charge, entry.backoff);
        add_topic(&mut charge, config, entry.topic_id.as_deref())?;
        if entry.peers.len() > MAX_PRUNE_PEERS {
            return None
        }
        for peer in entry.peers {
            add_bytes(&mut charge, 64)?;
            add_bytes(&mut charge, peer.peer_id.as_ref().map_or(0, Vec::len))?;
            add_bytes(&mut charge, peer.signed_peer_record.as_ref().map_or(0, Vec::len))?;
        }
    }
    Some(charge)
}

fn record_backoff(charge: &mut ControlCharge, seconds: Option<u64>) {
    charge.backoff_seconds = charge
        .backoff_seconds
        .max(seconds.unwrap_or(LOCAL_PRUNE_BACKOFF_SECONDS).min(MAX_REMOTE_PRUNE_BACKOFF_SECONDS));
}

fn add_topic(charge: &mut ControlCharge, config: &ChainConfig, topic: Option<&str>) -> Option<()> {
    let topic = topic?;
    parse_topic(config, topic).ok()?;
    add_bytes(charge, topic.len().checked_add(64)?)
}

fn add_ids(charge: &mut ControlCharge, ids: &[Vec<u8>]) -> Option<()> {
    charge.ids = charge.ids.checked_add(ids.len())?;
    if charge.ids > MAX_CONTROL_IDS || ids.iter().any(|id| id.len() != 20) {
        return None
    }
    // Upstream may retain each ID in nested maps, outgoing requests and history entries.
    // This charge covers fixed metadata as well as the exact original ID bytes.
    add_bytes(charge, ids.len().checked_mul(128)?)
}

fn add_bytes(charge: &mut ControlCharge, bytes: usize) -> Option<()> {
    charge.bytes = charge.bytes.checked_add(bytes)?;
    (charge.bytes <= MAX_CONTROL_BYTES).then_some(())
}

#[cfg(test)]
#[allow(deprecated)]
mod tests {
    use super::*;
    use crate::gossip::active_topics;
    use libp2p_gossipsub::TopicHash;

    #[test]
    fn backoff_retention_caps_remote_input_without_weakening_local_prune() {
        let mut charge = ControlCharge::default();
        record_backoff(&mut charge, None);
        assert_eq!(charge.backoff_seconds, 60);
        record_backoff(&mut charge, Some(1));
        assert_eq!(charge.backoff_seconds, 60);
        record_backoff(&mut charge, Some(u64::MAX));
        assert_eq!(charge.backoff_seconds, 3600);
    }

    fn raw_message() -> RawMessage {
        let config = ChainConfig::hoodi();
        let topic = active_topics(&config, 0).unwrap().remove(0).name();
        RawMessage {
            source: None,
            data: vec![2, 1, 0, 7],
            sequence_number: None,
            topic: TopicHash::from_raw(topic),
            signature: None,
            key: None,
            validated: false,
        }
    }

    #[test]
    fn original_message_survives_successful_pre_handler_admission() {
        let config = ChainConfig::hoodi();
        let message = raw_message();
        let expected = message.clone();
        let event = HandlerEvent::Message {
            rpc: gossipsub::Rpc {
                messages: vec![message],
                subscriptions: Vec::new(),
                control_msgs: Vec::new(),
            },
            invalid_messages: Vec::new(),
        };
        let (event, stats) = filter_event(event, &config, |_| true, |_| true);
        let HandlerEvent::Message { rpc, invalid_messages } = event else { panic!() };
        assert_eq!(rpc.messages, vec![expected]);
        assert!(invalid_messages.is_empty());
        assert_eq!(stats, FilterStats::default());
    }

    #[test]
    fn resource_failure_is_ignored_before_cache_and_invalid_scoring() {
        let config = ChainConfig::hoodi();
        let event = HandlerEvent::Message {
            rpc: gossipsub::Rpc {
                messages: vec![raw_message()],
                subscriptions: Vec::new(),
                control_msgs: Vec::new(),
            },
            invalid_messages: Vec::new(),
        };
        let (event, stats) = filter_event(event, &config, |_| false, |_| true);
        let HandlerEvent::Message { rpc, invalid_messages } = event else { panic!() };
        assert!(rpc.messages.is_empty());
        assert!(invalid_messages.is_empty());
        assert_eq!(stats.ignored_messages, 1);
        assert_eq!(stats.invalid_messages, 0);
    }

    #[test]
    fn strict_no_sign_key_is_invalid_even_when_upstream_anonymous_mode_allows_it() {
        let config = ChainConfig::hoodi();
        let mut message = raw_message();
        message.key = Some(vec![9; 32]);
        let event = HandlerEvent::Message {
            rpc: gossipsub::Rpc {
                messages: vec![message],
                subscriptions: Vec::new(),
                control_msgs: Vec::new(),
            },
            invalid_messages: Vec::new(),
        };
        let (event, stats) = filter_event(event, &config, |_| panic!(), |_| true);
        let HandlerEvent::Message { rpc, invalid_messages } = event else { panic!() };
        assert!(rpc.messages.is_empty());
        assert_eq!(invalid_messages.len(), 1);
        assert!(matches!(invalid_messages[0].1, ValidationError::TransformFailed));
        assert_eq!(stats.invalid_messages, 1);
    }

    #[test]
    fn advertised_ids_are_exactly_twenty_bytes_and_metadata_is_charged() {
        let mut charge = ControlCharge::default();
        assert_eq!(add_ids(&mut charge, &[vec![7; 20], vec![8; 20]]), Some(()));
        assert_eq!(charge.ids, 2);
        assert_eq!(charge.bytes, 256);
        assert_eq!(add_ids(&mut charge, &[vec![9; 21]]), None);
        let mut oversized = ControlCharge::default();
        assert_eq!(add_ids(&mut oversized, &vec![vec![0; 20]; MAX_CONTROL_IDS + 1]), None);
    }
}
