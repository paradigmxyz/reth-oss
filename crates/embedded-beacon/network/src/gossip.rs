//! Bounded anonymous gossipsub wire admission with explicit native validation callbacks.
//!
//! Topic and message identifiers follow the pinned consensus networking revision. Wire
//! admission never creates consensus, execution or availability authority.

use libp2p_gossipsub::{self as gossipsub, IdentTopic, Message, MessageId, TopicHash};
use reth_embedded_beacon_config::ChainConfig;
use reth_embedded_beacon_primitives::{sha256, Fork};
use snap::raw::{decompress_len, Decoder};
use std::{collections::HashSet, sync::Arc, time::Duration};

/// Independent maximum decompressed allocation for one gossip object.
pub const MAX_GOSSIP_BYTES: usize = 10 * 1024 * 1024;
/// Maximum raw Snappy bytes including the block-compression bound.
pub const MAX_GOSSIP_WIRE_BYTES: usize = MAX_GOSSIP_BYTES + MAX_GOSSIP_BYTES / 6 + 32;
/// Validation owns a bounded wire token independently of a committed import receipt.
pub(crate) const GOSSIP_VALIDATION_TIMEOUT: Duration = Duration::from_secs(60);
/// Cache shifts and token expiry share one policy; two extra shifts cover boundary rounding.
const GOSSIP_HEARTBEAT_INTERVAL: Duration = Duration::from_millis(700);
const GOSSIP_CACHE_HISTORY: usize = 6;
const GOSSIP_VALIDATION_HISTORY: usize =
    GOSSIP_VALIDATION_TIMEOUT.as_millis().div_ceil(GOSSIP_HEARTBEAT_INTERVAL.as_millis()) as usize + 2;

/// The exact topic object whose consensus validation remains mandatory.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum GossipKind {
    /// A complete signed beacon block.
    Block,
    /// A signed aggregate and proof.
    Aggregate,
    /// An individual attestation on its subnet.
    Attestation(u64),
    /// A complete blob sidecar on its subnet.
    Blob(u64),
    /// A complete data-column sidecar on its subnet.
    Column(u64),
    /// A complete signed Gloas execution payload envelope.
    Envelope,
    /// A signed Gloas execution payload bid.
    Bid,
    /// A Gloas payload committee vote.
    PayloadAttestation,
    /// Signed Gloas proposer preferences.
    Preferences,
}

/// Exact configured schema and digest for one parsed topic.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GossipTopic {
    /// Native wire schema selected by the configured digest.
    pub fork: Fork,
    /// Topic network context including blob-parameter-only upgrades.
    pub digest: [u8; 4],
    /// Exact consensus message and optional subnet.
    pub kind: GossipKind,
}

impl GossipTopic {
    /// Render the canonical lowercase, unprefixed topic identifier.
    pub fn name(self) -> String {
        let name = match self.kind {
            GossipKind::Block => "beacon_block".to_owned(),
            GossipKind::Aggregate => "beacon_aggregate_and_proof".to_owned(),
            GossipKind::Attestation(subnet) => format!("beacon_attestation_{subnet}"),
            GossipKind::Blob(subnet) => format!("blob_sidecar_{subnet}"),
            GossipKind::Column(subnet) => format!("data_column_sidecar_{subnet}"),
            GossipKind::Envelope => "execution_payload".to_owned(),
            GossipKind::Bid => "execution_payload_bid".to_owned(),
            GossipKind::PayloadAttestation => "payload_attestation_message".to_owned(),
            GossipKind::Preferences => "proposer_preferences".to_owned(),
        };
        let [a, b, c, d] = self.digest;
        format!("/eth2/{a:02x}{b:02x}{c:02x}{d:02x}/{name}/ssz_snappy")
    }
}

/// Wire-only failure, before any native signature or state-transition decision.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum GossipWireError {
    /// Topic spelling, configured digest or fork-specific object is invalid.
    #[error("invalid consensus gossip topic")]
    Topic,
    /// Raw Snappy or decompressed allocation exceeded the bound.
    #[error("gossip allocation limit exceeded")]
    Oversized,
    /// Raw block Snappy decompression failed.
    #[error("invalid raw gossip Snappy")]
    Compression,
    /// Gossip behaviour or subscriptions could not be configured.
    #[error("invalid gossip configuration")]
    Configuration,
}

/// Decompress raw block Snappy only after inspecting its claimed allocation.
pub fn decode_gossip(bytes: &[u8], maximum: usize) -> Result<Vec<u8>, GossipWireError> {
    if bytes.len() > MAX_GOSSIP_WIRE_BYTES {
        return Err(GossipWireError::Oversized)
    }
    let length = decompress_len(bytes).map_err(|_| GossipWireError::Compression)?;
    if length > maximum.min(MAX_GOSSIP_BYTES) {
        return Err(GossipWireError::Oversized)
    }
    let mut output = vec![0; length];
    let actual =
        Decoder::new().decompress(bytes, &mut output).map_err(|_| GossipWireError::Compression)?;
    if actual != length {
        return Err(GossipWireError::Compression)
    }
    Ok(output)
}

/// Resolve all explicitly configured historical fork and blob-upgrade contexts.
/// This is used for message identifiers; subscription overlap is independently bounded.
pub fn parse_topic(config: &ChainConfig, name: &str) -> Result<GossipTopic, GossipWireError> {
    if name.len() > 128 {
        return Err(GossipWireError::Topic)
    }
    let parts = name.split('/').collect::<Vec<_>>();
    if parts.len() != 5 ||
        !parts[0].is_empty() ||
        parts[1] != "eth2" ||
        parts[2].len() != 8 ||
        parts[4] != "ssz_snappy" ||
        !parts[2].bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(GossipWireError::Topic)
    }
    let mut digest = [0; 4];
    for (index, byte) in digest.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&parts[2][index * 2..index * 2 + 2], 16)
            .map_err(|_| GossipWireError::Topic)?;
    }
    let epoch = context_epochs(config)
        .into_iter()
        .find(|epoch| config.fork_digest(*epoch).ok() == Some(digest))
        .ok_or(GossipWireError::Topic)?;
    let fork = config.fork_at_epoch(epoch).map_err(|_| GossipWireError::Configuration)?.fork;
    let kind = match parts[3] {
        "beacon_block" => GossipKind::Block,
        "beacon_aggregate_and_proof" => GossipKind::Aggregate,
        "execution_payload" if fork == Fork::Gloas => GossipKind::Envelope,
        "execution_payload_bid" if fork == Fork::Gloas => GossipKind::Bid,
        "payload_attestation_message" if fork == Fork::Gloas => GossipKind::PayloadAttestation,
        "proposer_preferences" if fork == Fork::Gloas => GossipKind::Preferences,
        name if name.starts_with("beacon_attestation_") => {
            GossipKind::Attestation(subnet(name, "beacon_attestation_", 64)?)
        }
        name if name.starts_with("blob_sidecar_") && (Fork::Deneb..Fork::Fulu).contains(&fork) => {
            GossipKind::Blob(subnet(
                name,
                "blob_sidecar_",
                if fork >= Fork::Electra { 9 } else { 6 },
            )?)
        }
        name if name.starts_with("data_column_sidecar_") && fork >= Fork::Fulu => {
            GossipKind::Column(subnet(name, "data_column_sidecar_", 128)?)
        }
        _ => return Err(GossipWireError::Topic),
    };
    Ok(GossipTopic { fork, digest, kind })
}

/// Derive the normative 20-byte content identity with historical Phase 0 isolation.
/// Invalid and oversized compression uses the invalid domain without allocating its claim.
pub fn gossip_message_id(config: &ChainConfig, message: &Message) -> MessageId {
    gossip_message_id_parts(config, &message.topic, &message.data)
}

fn gossip_message_id_parts(config: &ChainConfig, hash: &TopicHash, raw: &[u8]) -> MessageId {
    let topic = hash.as_str().as_bytes();
    let decoded = decode_gossip(raw, config.max_payload_size);
    let valid = decoded.is_ok();
    let data = decoded.as_deref().unwrap_or(raw);
    let phase0 = parse_topic(config, hash.as_str()).is_ok_and(|topic| topic.fork == Fork::Phase0);
    let mut input = Vec::with_capacity(4 + if phase0 { 0 } else { 8 + topic.len() } + data.len());
    input.extend_from_slice(if valid { &[1, 0, 0, 0] } else { &[0, 0, 0, 0] });
    if !phase0 {
        input.extend_from_slice(&(topic.len() as u64).to_le_bytes());
        input.extend_from_slice(topic);
    }
    input.extend_from_slice(data);
    MessageId::from(sha256(&input)[..20].to_vec())
}

/// Current and immediately overlapping network contexts, including BPO transitions.
/// The two-epoch overlap bounds retained topic and mesh metadata.
pub fn active_topics(
    config: &ChainConfig,
    current_slot: u64,
) -> Result<Vec<GossipTopic>, GossipWireError> {
    let epoch = current_slot / config.slots_per_epoch;
    let mut epochs = vec![epoch];
    for boundary in context_epochs(config) {
        if boundary.abs_diff(epoch) <= 2 {
            epochs.push(boundary);
            if boundary > 0 {
                epochs.push(boundary - 1);
            }
        }
    }
    let mut topics = Vec::new();
    let mut digests = HashSet::new();
    for epoch in epochs {
        let digest = config.fork_digest(epoch).map_err(|_| GossipWireError::Configuration)?;
        if !digests.insert(digest) {
            continue
        }
        let fork = config.fork_at_epoch(epoch).map_err(|_| GossipWireError::Configuration)?.fork;
        let mut kinds = vec![GossipKind::Block, GossipKind::Aggregate];
        if fork == Fork::Gloas {
            kinds.extend([
                GossipKind::Envelope,
                GossipKind::Bid,
                GossipKind::PayloadAttestation,
                GossipKind::Preferences,
            ]);
        }
        topics.extend(kinds.into_iter().map(|kind| GossipTopic { fork, digest, kind }));
    }
    Ok(topics)
}

mod ingress;
use ingress::BoundedTransform;
pub(crate) use ingress::GossipBehaviour;

/// Construct anonymous strict-no-sign meshes; every delivery awaits explicit validation.
pub(crate) fn behaviour(
    config: Arc<ChainConfig>,
    current_slot: u64,
) -> Result<GossipBehaviour, GossipWireError> {
    config.validate().map_err(|_| GossipWireError::Configuration)?;
    let ids = config.clone();
    let mut options = gossipsub::ConfigBuilder::default();
    options
        .validation_mode(gossipsub::ValidationMode::Anonymous)
        .validate_messages()
        .mesh_n(8)
        .mesh_n_low(6)
        .mesh_n_high(12)
        .gossip_lazy(6)
        .heartbeat_initial_delay(GOSSIP_HEARTBEAT_INTERVAL)
        .heartbeat_interval(GOSSIP_HEARTBEAT_INTERVAL)
        .prune_backoff(Duration::from_secs(60))
        .backoff_slack(1)
        .history_length(GOSSIP_CACHE_HISTORY)
        .validation_history_length(GOSSIP_VALIDATION_HISTORY)
        .history_gossip(3)
        .duplicate_cache_time(Duration::from_secs(
            config.seconds_per_slot * config.slots_per_epoch * 2,
        ))
        .max_transmit_size(MAX_GOSSIP_WIRE_BYTES + 1024)
        .max_messages_per_rpc(Some(16))
        .ingress_limits(gossipsub::IngressLimits {
            max_published_messages: 16,
            max_subscriptions: 256,
            max_controls: 16,
            max_message_ids: 1024,
            max_prune_peers: 16,
        })
        .connection_handler_queue_len(8)
        .max_ihave_length(1024)
        .max_ihave_messages(10)
        .message_id_fn(move |message| gossip_message_id(&ids, message));
    let filter = gossipsub::MaxCountSubscriptionFilter {
        filter: ConsensusSubscriptionFilter(config.clone()),
        max_subscribed_topics: 512,
        max_subscriptions_per_request: 256,
    };
    let ingress = BoundedTransform::new(config.clone())?;
    let mut registry = prometheus_client::registry::Registry::default();
    let mut behaviour = gossipsub::Behaviour::new_with_subscription_filter_and_transform(
        gossipsub::MessageAuthenticity::Anonymous,
        options.build().map_err(|_| GossipWireError::Configuration)?,
        filter,
        ingress.clone(),
    )
    .map_err(|_| GossipWireError::Configuration)?;
    let topics = active_topics(&config, current_slot)?;
    let scores = gossipsub::PeerScoreParams {
        topics: topics
            .iter()
            .map(|topic| {
                (IdentTopic::new(topic.name()).hash(), gossipsub::TopicScoreParams::default())
            })
            .collect(),
        ..Default::default()
    };
    behaviour
        .with_peer_score(scores, gossipsub::PeerScoreThresholds::default())
        .map_err(|_| GossipWireError::Configuration)?;
    // Disable per-topic metric labels. Only the fixed global histogram is cache-retirement
    // evidence; hostile topic churn cannot allocate metric families or forge a heartbeat.
    behaviour = behaviour.with_metrics(
        &mut registry,
        gossipsub::MetricsConfig {
            max_topics: 0,
            max_never_subscribed_topics: 0,
            ..Default::default()
        },
    );
    let mut behaviour = GossipBehaviour::new(behaviour, registry, ingress);
    for topic in topics {
        behaviour
            .subscribe(&IdentTopic::new(topic.name()))
            .map_err(|_| GossipWireError::Configuration)?;
    }
    Ok(behaviour)
}

pub(crate) struct ConsensusSubscriptionFilter(Arc<ChainConfig>);
impl gossipsub::TopicSubscriptionFilter for ConsensusSubscriptionFilter {
    fn can_subscribe(&mut self, topic: &TopicHash) -> bool {
        parse_topic(&self.0, topic.as_str()).is_ok()
    }
}

fn context_epochs(config: &ChainConfig) -> Vec<u64> {
    let mut epochs = config
        .forks
        .activations()
        .iter()
        .filter(|entry| entry.epoch != u64::MAX)
        .map(|entry| entry.epoch)
        .chain(config.blob_schedule.iter().map(|entry| entry.epoch))
        .collect::<Vec<_>>();
    epochs.sort_unstable();
    epochs.dedup();
    epochs
}
fn subnet(name: &str, prefix: &str, count: u64) -> Result<u64, GossipWireError> {
    let text = name.strip_prefix(prefix).ok_or(GossipWireError::Topic)?;
    let value = text.parse::<u64>().map_err(|_| GossipWireError::Topic)?;
    if value >= count || text != value.to_string() {
        return Err(GossipWireError::Topic)
    }
    Ok(value)
}

#[cfg(test)]
mod tests;
