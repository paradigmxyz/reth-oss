//! Bound raw message and duplicate-ID retention before the upstream private caches allocate.
//!
//! Cache retirement uses the actual completed heartbeat histogram, recorded after mcache.shift
//! by the pinned transport dependency. Elapsed time or pending polls confer no eviction evidence.

use super::*;
use libp2p::{
    core::{transport::PortUse, Endpoint},
    swarm::{
        ConnectionDenied, ConnectionId, FromSwarm, NetworkBehaviour, THandler, THandlerInEvent,
        THandlerOutEvent, ToSwarm,
    },
    Multiaddr, PeerId,
};
use libp2p_gossipsub::{DataTransform, RawMessage};
use prometheus_client::{encoding::text::encode, registry::Registry};
use std::{
    collections::HashMap,
    fmt::{self, Write},
    io,
    sync::Mutex,
    task::{Context, Poll},
    time::Instant,
};

const MAX_RAW_CACHE_BYTES: usize = 32 * 1024 * 1024;
const MAX_DUPLICATE_IDS: usize = 131_072;
const MAX_METRICS_BYTES: usize = 4 * 1024 * 1024;
const MAX_METRIC_LINE: usize = 1024;
const CACHE_HISTORY: u64 = GOSSIP_VALIDATION_HISTORY as u64;

pub(super) type InnerBehaviour = gossipsub::Behaviour<
    BoundedTransform,
    gossipsub::MaxCountSubscriptionFilter<ConsensusSubscriptionFilter>,
>;

/// The only active gossipsub owner; capacity is filtered before real peer scoring.
/// No wrapper method exposes a way to mutate its private heartbeat registry.
pub(crate) struct GossipBehaviour {
    inner: InnerBehaviour,
    registry: Registry,
    ingress: BoundedTransform,
    controls_budget: control_retention::ControlBudget,
    score_topics: HashSet<TopicHash>,
    peers_budget: peer_capacity::PeerCapacity,
}
impl GossipBehaviour {
    pub(super) fn new(
        inner: InnerBehaviour,
        registry: Registry,
        ingress: BoundedTransform,
    ) -> Self {
        Self {
            inner,
            registry,
            ingress,
            controls_budget: Default::default(),
            score_topics: HashSet::new(),
            peers_budget: Default::default(),
        }
    }
    #[cfg(test)]
    pub(crate) fn publish(
        &mut self,
        topic: impl Into<TopicHash>,
        data: impl Into<Vec<u8>>,
    ) -> Result<MessageId, gossipsub::PublishError> {
        let result = self.inner.publish(topic, data);
        self.ingress.settled();
        result
    }
    pub(crate) fn subscribe(
        &mut self,
        topic: &IdentTopic,
    ) -> Result<bool, gossipsub::SubscriptionError> {
        let hash = topic.hash();
        if parse_topic(&self.ingress.config, hash.as_str()).is_err() ||
            (!self.score_topics.contains(&hash) && self.score_topics.len() >= 512)
        {
            return Err(gossipsub::SubscriptionError::NotAllowed)
        }
        self.inner
            .set_topic_params(topic.clone(), gossipsub::TopicScoreParams::default())
            .map_err(|_| gossipsub::SubscriptionError::NotAllowed)?;
        self.score_topics.insert(hash);
        self.inner.subscribe(topic)
    }
    pub(crate) fn unsubscribe(&mut self, topic: &IdentTopic) -> bool {
        self.inner.unsubscribe(topic)
    }
    #[cfg(test)]
    pub(crate) fn topics(&self) -> impl Iterator<Item = &TopicHash> {
        self.inner.topics()
    }
    #[cfg(test)]
    pub(crate) fn all_mesh_peers(&self) -> impl Iterator<Item = &PeerId> {
        self.inner.all_mesh_peers()
    }
    pub(crate) fn report_message_validation_result(
        &mut self,
        id: &MessageId,
        peer: &PeerId,
        verdict: gossipsub::MessageAcceptance,
    ) -> bool {
        // Read completed shifts before synchronous validation moves a pending message into
        // the short accepted-message history. A failed observation cannot shorten a lease.
        let completed = self.observe_retirement();
        let accepted = matches!(verdict, gossipsub::MessageAcceptance::Accept);
        let found = self.inner.report_message_validation_result(id, peer, verdict);
        if found {
            self.ingress.validation_completed(id, accepted, completed);
        }
        found
    }
    fn observe_retirement(&mut self) -> Option<u64> {
        let mut sink = HeartbeatSink::default();
        if encode(&mut sink, &self.registry).is_ok() &&
            let Some(completed) = sink.completed
        {
            // The private registry has one unlabelled monotone counter. A reset, truncated
            // encoding or malformed counter grants no retirement and therefore fails closed.
            self.ingress.completed_heartbeats(completed);
            self.controls_budget.completed_heartbeats(completed);
            return Some(completed)
        }
        None
    }
}
impl fmt::Debug for GossipBehaviour {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GossipBehaviour")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}
#[derive(Clone)]
pub(crate) struct BoundedTransform {
    config: Arc<ChainConfig>,
    ttl: Duration,
    state: Arc<Mutex<IngressState>>,
}
#[derive(Default)]
struct IngressState {
    entries: HashMap<MessageId, Reservation>,
    raw_bytes: usize,
    completed: u64,
    poisoned: bool,
    pending: Vec<MessageId>,
}
struct Reservation {
    expires: Option<Instant>,
    after_heartbeat: u64,
    bytes: usize,
    claimed: bool,
}
impl BoundedTransform {
    pub(super) fn new(config: Arc<ChainConfig>) -> Result<Self, GossipWireError> {
        let seconds = config
            .seconds_per_slot
            .checked_mul(config.slots_per_epoch)
            .and_then(|seconds| seconds.checked_mul(2))
            .ok_or(GossipWireError::Configuration)?;
        Ok(Self {
            config,
            ttl: Duration::from_secs(seconds.max(120)),
            state: Arc::new(Mutex::new(IngressState::default())),
        })
    }
    fn reserve(&self, message: &Message) -> io::Result<()> {
        if message.data.len() > MAX_GOSSIP_WIRE_BYTES || message.topic.as_str().len() > 128 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "gossip wire size exceeded"))
        }
        let id = gossip_message_id_parts(&self.config, &message.topic, &message.data);
        self.reserve_id(id, message.data.len(), false)
    }
    fn admit_raw(&self, raw: &RawMessage) -> bool {
        let id = gossip_message_id_parts(&self.config, &raw.topic, &raw.data);
        self.reserve_id(id, raw.data.len(), true).is_ok()
    }
    fn reserve_id(&self, id: MessageId, bytes: usize, claimed: bool) -> io::Result<()> {
        let now = Instant::now();
        let mut state = self
            .state
            .lock()
            .map_err(|_| io::Error::new(io::ErrorKind::WouldBlock, "gossip cache unavailable"))?;
        if state.poisoned {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "gossip cache unavailable"))
        }
        // Upstream duplicate-cache insertion also prunes expired entries synchronously. Raw
        // bytes remain charged until actual cache shifts have separately been observed.
        state.entries.retain(|_, entry| {
            entry.expires.is_none_or(|expires| expires > now) || entry.bytes != 0
        });
        if state.entries.contains_key(&id) {
            // Ignore duplicates before upstream handling. A stalled heartbeat or alternate raw
            // encoding of the same decoded identity cannot create an uncharged event clone.
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "gossip duplicate reservation"))
        }
        if state.entries.len() >= MAX_DUPLICATE_IDS ||
            bytes > MAX_RAW_CACHE_BYTES.saturating_sub(state.raw_bytes)
        {
            // Inbound admission is filtered before the handler mutates scoring or caches.
            // Outbound capacity failure is a local publish error, never a peer penalty.
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "gossip cache capacity"))
        }

        let after_heartbeat = state.completed.checked_add(CACHE_HISTORY + 1).ok_or_else(|| {
            io::Error::new(io::ErrorKind::WouldBlock, "gossip cache time exceeded")
        })?;
        state.raw_bytes += bytes;
        state.pending.push(id.clone());
        state.entries.insert(id, Reservation { expires: None, after_heartbeat, bytes, claimed });
        Ok(())
    }
    fn settled(&self) {
        let Ok(mut state) = self.state.lock() else { return };
        let Some(expires) = Instant::now().checked_add(self.ttl) else {
            state.poisoned = true;
            return
        };
        // The dependency inserts duplicates synchronously after DataTransform returns. Starting
        // the reservation TTL only after its handler/publish turn completed is conservative.
        for id in std::mem::take(&mut state.pending) {
            if let Some(entry) = state.entries.get_mut(&id) {
                entry.expires = Some(expires);
            }
        }
    }
    fn completed_heartbeats(&self, completed: u64) {
        let Ok(mut state) = self.state.lock() else { return };
        if completed < state.completed {
            state.poisoned = true;
            return
        }
        state.completed = completed;
        let mut released = 0;
        for entry in state.entries.values_mut() {
            if entry.after_heartbeat <= completed {
                released += std::mem::take(&mut entry.bytes);
            }
        }
        state.raw_bytes = state.raw_bytes.saturating_sub(released);
    }
    fn validation_completed(&self, id: &MessageId, accepted: bool, completed: Option<u64>) {
        let Ok(mut state) = self.state.lock() else { return };
        if state.poisoned {
            return
        }
        let Some(entry) = state.entries.get_mut(id) else { return };
        if accepted {
            // Accept transfers the cached payload into a fresh short history. Missing or
            // overflowing completed-shift evidence pins the reservation rather than freeing it.
            entry.after_heartbeat = completed
                .and_then(|completed| completed.checked_add(GOSSIP_CACHE_HISTORY as u64 + 1))
                .unwrap_or(u64::MAX);
        } else {
            // A successful Ignore/Reject callback synchronously removed this original payload.
            let released = std::mem::take(&mut entry.bytes);
            state.raw_bytes = state.raw_bytes.saturating_sub(released);
        }
    }
}
impl DataTransform for BoundedTransform {
    fn inbound_transform(&self, raw: RawMessage) -> io::Result<Message> {
        let message = Message {
            source: raw.source,
            data: raw.data,
            sequence_number: raw.sequence_number,
            topic: raw.topic,
        };
        let id = gossip_message_id_parts(&self.config, &message.topic, &message.data);
        let mut state =
            self.state.lock().map_err(|_| io::Error::other("gossip cache owner unavailable"))?;
        let Some(entry) = state.entries.get_mut(&id).filter(|entry| entry.claimed) else {
            return Err(io::Error::other("gossip input lacked handler reservation"))
        };
        entry.claimed = false;
        Ok(message)
    }
    fn outbound_transform(&self, topic: &TopicHash, data: Vec<u8>) -> io::Result<Vec<u8>> {
        let message = Message { source: None, data, sequence_number: None, topic: topic.clone() };
        self.reserve(&message)?;
        Ok(message.data)
    }
}

/// Fixed line storage extracts one exact metric; the complete registry is never buffered.
#[derive(Default)]
struct HeartbeatSink {
    line: String,
    oversized_line: bool,
    total: usize,
    completed: Option<u64>,
}
impl Write for HeartbeatSink {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        self.total = self.total.checked_add(text.len()).ok_or(fmt::Error)?;
        if self.total > MAX_METRICS_BYTES {
            return Err(fmt::Error)
        }
        for fragment in text.split_inclusive('\n') {
            let content = fragment.strip_suffix('\n').unwrap_or(fragment);
            if !self.oversized_line &&
                self.line.len().saturating_add(content.len()) <= MAX_METRIC_LINE
            {
                self.line.push_str(content);
            } else {
                self.oversized_line = true;
            }
            if fragment.ends_with('\n') {
                if !self.oversized_line &&
                    let Some(value) = self.line.strip_prefix("heartbeat_duration_count ")
                {
                    if self.completed.is_some() {
                        return Err(fmt::Error)
                    }
                    self.completed = Some(value.parse::<u64>().map_err(|_| fmt::Error)?);
                }
                self.line.clear();
                self.oversized_line = false;
            }
        }
        Ok(())
    }
}

impl NetworkBehaviour for GossipBehaviour {
    type ConnectionHandler = <InnerBehaviour as NetworkBehaviour>::ConnectionHandler;
    type ToSwarm = <InnerBehaviour as NetworkBehaviour>::ToSwarm;
    fn handle_pending_inbound_connection(
        &mut self,
        id: ConnectionId,
        local: &Multiaddr,
        remote: &Multiaddr,
    ) -> Result<(), ConnectionDenied> {
        self.inner.handle_pending_inbound_connection(id, local, remote)
    }
    fn handle_established_inbound_connection(
        &mut self,
        id: ConnectionId,
        peer: PeerId,
        local: &Multiaddr,
        remote: &Multiaddr,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        self.peers_budget.reserve(&mut self.inner, id, peer)?;
        let result = self.inner.handle_established_inbound_connection(id, peer, local, remote);
        if result.is_err() {
            self.peers_budget.release(id);
        }
        result
    }
    fn handle_pending_outbound_connection(
        &mut self,
        id: ConnectionId,
        peer: Option<PeerId>,
        addresses: &[Multiaddr],
        role: Endpoint,
    ) -> Result<Vec<Multiaddr>, ConnectionDenied> {
        self.inner.handle_pending_outbound_connection(id, peer, addresses, role)
    }
    fn handle_established_outbound_connection(
        &mut self,
        id: ConnectionId,
        peer: PeerId,
        address: &Multiaddr,
        role: Endpoint,
        port: PortUse,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        self.peers_budget.reserve(&mut self.inner, id, peer)?;
        let result =
            self.inner.handle_established_outbound_connection(id, peer, address, role, port);
        if result.is_err() {
            self.peers_budget.release(id);
        }
        result
    }
    fn on_swarm_event(&mut self, event: FromSwarm<'_>) {
        self.inner.on_swarm_event(event);
        self.peers_budget.settled(&event);
    }
    fn on_connection_handler_event(
        &mut self,
        peer: PeerId,
        id: ConnectionId,
        event: THandlerOutEvent<Self>,
    ) {
        let (event, _stats) = controls::filter_event(
            event,
            &self.ingress.config,
            |raw| self.ingress.admit_raw(raw),
            |charge| self.controls_budget.reserve(peer, charge),
        );
        self.inner.on_connection_handler_event(peer, id, event);
        self.ingress.settled();
        self.controls_budget.settled();
    }
    fn poll(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<ToSwarm<Self::ToSwarm, THandlerInEvent<Self>>> {
        let event = self.inner.poll(cx);
        // A metrics observation is actual dependency execution evidence, independent of timer
        // readiness and event-loop starvation. Encoding is bounded and cannot grant authority.
        self.ingress.settled();
        let _ = self.observe_retirement();
        event
    }
}

mod control_retention;
mod controls;
mod peer_capacity;

#[cfg(test)]
mod tests;
