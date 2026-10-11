//! Dormant authenticated status transport over QUIC/TLS and TCP/Noise.
//! TCP negotiates Yamux first with bounded mplex fallback. No node startup is enabled.

use crate::{
    availability_rpc::{SidecarBatch, SidecarCodec, SidecarReply, SidecarRequest, WireSidecar},
    block_rpc::{BlockBatch, BlockCodec, BlockReply, BlockRequest, WireBlock},
    control_rpc::{ControlProtocol, LocalMetadata, PeerMetadata},
    encode_request,
    envelope_rpc::{
        EnvelopeBatch, EnvelopeBlockReference, EnvelopeCodec, EnvelopeReply, EnvelopeRequest,
        WireEnvelope,
    },
    validate_status, DecodeProgress, EndpointPolicy, HandshakeError, Limits, Protocol, Status,
    StatusCompatibility, StatusV2, StreamDecoder, WireError, WireMessage,
};
use async_trait::async_trait;
use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, StreamExt};
use libp2p::{
    connection_limits,
    core::{transport::ListenerId, ConnectedPoint},
    identity::{KeyType, Keypair},
    multiaddr::Protocol as AddressPart,
    noise,
    request_response::{
        self, Codec, InboundRequestId, Message, OutboundRequestId, ProtocolSupport, ResponseChannel,
    },
    swarm::{dial_opts::DialOpts, NetworkBehaviour, SwarmEvent},
    tcp, yamux, Multiaddr, PeerId, StreamProtocol, Swarm, SwarmBuilder,
};
use reth_embedded_beacon_config::ChainConfig;
use reth_embedded_beacon_primitives::{Checkpoint, Epoch, Fork, Root, Slot};
use std::{
    collections::{HashMap, HashSet},
    fmt, io,
    net::IpAddr,
    num::NonZeroU8,
    sync::Arc,
    time::Duration,
};

mod availability;
mod connections;
mod control;
mod gossip;
mod retired;
mod status_refresh;

#[cfg(test)]
mod listen_binding;

use availability::{sidecar_requests, InboundSidecarRequest, PendingSidecarRequest};
use control::control_requests;

pub use availability::SidecarRequestId;
pub use gossip::{GossipInput, GossipRequestId, GossipVerdict};

pub(crate) const MAX_PEERS: usize = 16;
const MAX_STREAMS: usize = 16;
const MAX_WORKERS_PER_CONNECTION: usize = 8;
const MAX_BLOCK_REQUESTS: usize = 4;
const MAX_INBOUND_BLOCK_REQUESTS: usize = 2;
const MAX_MUX_STREAMS: usize = 8;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const STATUS_REFRESH_INTERVAL: Duration = Duration::from_secs(4);
const STATUS_LIMITS: Limits = Limits { max_payload: 92, max_compressed: 139 };

/// Authenticated status exchanges using a dynamically updated trusted clock and finality.
/// Construction is dormant; explicit network admission remains the caller's responsibility.
pub struct StatusTransport {
    swarm: Swarm<StatusBehaviour>,
    config: Arc<ChainConfig>,
    current_slot: Slot,
    local_finalized: Checkpoint,
    canonical_roots: Vec<(Epoch, Root)>,
    local_status: StatusV2,
    peers: HashSet<PeerId>,
    connections: connections::ConnectionInventory,
    pending: HashMap<(bool, OutboundRequestId), PeerId>,
    checked_peers: HashSet<PeerId>,
    block_pending: HashMap<(bool, OutboundRequestId), PendingBlockRequest>,
    block_inbound: HashMap<BlockRequestId, InboundBlockRequest>,
    next_block_request: u64,
    envelope_pending: HashMap<(bool, OutboundRequestId), PendingEnvelopeRequest>,
    envelope_inbound: HashMap<EnvelopeRequestId, InboundEnvelopeRequest>,
    next_envelope_request: u64,
    sidecar_pending: HashMap<(Protocol, OutboundRequestId), PendingSidecarRequest>,
    sidecar_inbound: HashMap<SidecarRequestId, InboundSidecarRequest>,
    next_sidecar_request: u64,
    dialing: HashSet<PeerId>,
    policy: EndpointPolicy,
    listeners: HashSet<ListenerId>,
    next_status_refresh: tokio::time::Instant,
    local_metadata: LocalMetadata,
    control_pending: HashMap<(ControlProtocol, OutboundRequestId), PeerId>,
    gossip_pending: HashMap<GossipRequestId, gossip::PendingGossip>,
    gossip_bytes: Arc<tokio::sync::Semaphore>,
    gossip_topics: HashSet<String>,
    custody_columns: Option<[u64; 4]>,
    next_gossip_request: u64,
}

impl fmt::Debug for StatusTransport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StatusTransport")
            .field("peer_id", &self.peer_id())
            .field("peer_count", &self.peers.len())
            .field("pending_dials", &self.dialing.len())
            .finish_non_exhaustive()
    }
}

/// Construction or explicit local transport operation failure.
#[derive(Clone, Copy, Debug, thiserror::Error, PartialEq, Eq)]
pub enum TransportError {
    /// A key, configuration or trusted local status is inconsistent.
    #[error("invalid status transport configuration")]
    Configuration,
    /// The address is not an explicit permitted TCP or QUIC endpoint.
    #[error("peer endpoint is not permitted")]
    Address,
    /// The bounded peer or handshake request budget is exhausted.
    #[error("status transport capacity exhausted")]
    Capacity,
    /// The transport could not start listening.
    #[error("could not listen on local status endpoint")]
    Listen,
    /// The authenticated peer dial could not be scheduled.
    #[error("could not dial local status endpoint")]
    Dial,
}

/// A peer rejection without exposing remote error-message bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StatusRejection {
    /// The remote status conflicts with trusted local information.
    Handshake(HandshakeError),
    /// Negotiation, framing, timeout or stream IO failed.
    Protocol,
    /// The peer/stream budget was exceeded.
    Capacity,
}

/// Observations produced by the actual local transport.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TransportEvent {
    /// A validated local listener is ready at its assigned address.
    Listening(Multiaddr),
    /// A peer's status was checked; unknown finalized roots remain unverified.
    PeerStatus {
        /// Secp256k1 identity authenticated by Noise.
        peer: PeerId,
        /// The exact decoded peer status.
        status: Status,
        /// Availability exists only when the negotiated status schema advertises it.
        earliest_available_slot: Option<Slot>,
        /// Trusted known finality or a checkpoint still requiring validation.
        compatibility: StatusCompatibility,
    },
    /// A peer has been disconnected after a status/stream rejection.
    Rejected {
        /// Authenticated peer identity.
        peer: PeerId,
        /// The bounded reason classification.
        reason: StatusRejection,
    },
    /// The final connection to a peer has closed.
    Closed(PeerId),
    /// A checked peer requests blocks; the opaque token owns a bounded private response channel.
    BlockRequest {
        /// Authenticated peer with a completed checked status exchange.
        peer: PeerId,
        /// Token accepted only by the matching transport's response method.
        id: BlockRequestId,
        /// Exact bounded decoded request.
        request: BlockRequest,
    },
    /// Complete SSZ blocks matched to the original outbound request; consensus import is required.
    Blocks {
        /// Authenticated responder.
        peer: PeerId,
        /// Original transport request token.
        id: BlockRequestId,
        /// Immutable structurally checked blocks under a global retained-batch budget.
        batch: BlockBatch,
    },
    /// A bounded protocol error reports that the requested blocks cannot currently be served.
    BlocksUnavailable {
        /// Authenticated responder.
        peer: PeerId,
        /// Original transport request token.
        id: BlockRequestId,
        /// Exact known response code; remote text is never exposed.
        code: u8,
    },
    /// A checked peer requests original sidecars through an opaque private channel token.
    SidecarRequest {
        /// Authenticated requester.
        peer: PeerId,
        /// Token bound to the original private response channel.
        id: SidecarRequestId,
        /// Exact pinned request including commitment/column indices.
        request: SidecarRequest,
    },
    /// Structurally checked sidecars; mandatory candidate proof verification remains required.
    Sidecars {
        /// Authenticated responder.
        peer: PeerId,
        /// Original outbound request token.
        id: SidecarRequestId,
        /// Complete bytes under the shared retained-response reservation.
        batch: SidecarBatch,
    },
    /// A requested availability item cannot currently be served.
    SidecarsUnavailable {
        /// Authenticated responder.
        peer: PeerId,
        /// Original outbound request token.
        id: SidecarRequestId,
        /// Exact bounded protocol error code.
        code: u8,
    },
    /// A checked authenticated peer requests payload envelopes.
    EnvelopeRequest {
        /// Authenticated peer with a completed checked status exchange.
        peer: PeerId,
        /// Token owning only the original private response channel.
        id: EnvelopeRequestId,
        /// Exact bounded request for beacon slots or message roots.
        request: EnvelopeRequest,
    },
    /// Complete signed envelope SSZ linked to exact original beacon block metadata.
    /// Builder signature, EL validation, availability and consensus import remain required.
    Envelopes {
        /// Authenticated responder.
        peer: PeerId,
        /// Original transport request token.
        id: EnvelopeRequestId,
        /// Immutable complete envelopes under the shared global response budget.
        batch: EnvelopeBatch,
    },
    /// The requested envelopes are currently unavailable.
    EnvelopesUnavailable {
        /// Authenticated responder.
        peer: PeerId,
        /// Original transport request token.
        id: EnvelopeRequestId,
        /// Exact known response code; remote text is never exposed.
        code: u8,
    },
    /// Actual remote metadata sequence received through an authenticated Ping exchange.
    PeerPing {
        /// Authenticated remote peer identity.
        peer: PeerId,
        /// Remote metadata sequence, independent of ENR sequence.
        seq_number: u64,
    },
    /// Exact remote metadata advertisement, granting no availability authority.
    PeerMetadata {
        /// Authenticated remote peer identity.
        peer: PeerId,
        /// Complete negotiated schema fields; absent fields remain absent.
        metadata: PeerMetadata,
    },
    /// A remote control request failed with an exact known response code.
    ControlUnavailable {
        /// Authenticated remote peer identity.
        peer: PeerId,
        /// Error classification without remote text bytes.
        code: u8,
    },
    /// Complete original gossip input awaiting explicit native consensus validation.
    Gossip {
        /// Checked authenticated propagation source.
        peer: PeerId,
        /// Exact transport-scoped single-use validation token.
        id: GossipRequestId,
        /// Complete bounded original wire and SSZ; no imported authority.
        input: GossipInput,
    },
    /// An outbound local dial failed before a status was received.
    DialFailed(Option<PeerId>),
}

/// An opaque request token scoped to one transport instance and never reused.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct BlockRequestId {
    owner: PeerId,
    sequence: u64,
}

#[derive(Debug)]
struct PendingBlockRequest {
    peer: PeerId,
    id: BlockRequestId,
    request: BlockRequest,
}

#[derive(Debug)]
struct InboundBlockRequest {
    peer: PeerId,
    request_id: InboundRequestId,
    range: bool,
    request: BlockRequest,
    channel: ResponseChannel<BlockReply>,
}

/// An opaque envelope request token scoped to one transport instance and never reused.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct EnvelopeRequestId {
    owner: PeerId,
    sequence: u64,
}

#[derive(Debug)]
struct PendingEnvelopeRequest {
    peer: PeerId,
    id: EnvelopeRequestId,
    request: EnvelopeRequest,
    // Small metadata only: retaining the original BlockBatch would starve shared response permits.
    blocks: Vec<EnvelopeBlockReference>,
}

#[derive(Debug)]
struct InboundEnvelopeRequest {
    peer: PeerId,
    request_id: InboundRequestId,
    range: bool,
    request: EnvelopeRequest,
    channel: ResponseChannel<EnvelopeReply>,
}

/// Existing local experiment API; `new` continues to restrict endpoints to loopback.
pub type LocalStatusTransport = StatusTransport;

impl StatusTransport {
    /// Build a dormant local transport. The supplied private key is never logged.
    /// The finalized root in `local_status` must equal the trusted checkpoint.
    pub fn new(
        key: Keypair,
        config: ChainConfig,
        current_slot: Slot,
        local_finalized: Checkpoint,
        canonical_roots: Vec<(Epoch, Root)>,
        local_status: StatusV2,
    ) -> Result<Self, TransportError> {
        Self::with_policy(
            key,
            config,
            current_slot,
            local_finalized,
            canonical_roots,
            local_status,
            EndpointPolicy::LoopbackOnly,
        )
    }

    /// Build a dormant transport with an explicit public or test-only routing policy.
    /// A caller must enforce full node readiness before invoking public listen or dial operations.
    #[allow(clippy::too_many_arguments)]
    pub fn with_policy(
        key: Keypair,
        config: ChainConfig,
        current_slot: Slot,
        local_finalized: Checkpoint,
        canonical_roots: Vec<(Epoch, Root)>,
        local_status: StatusV2,
        policy: EndpointPolicy,
    ) -> Result<Self, TransportError> {
        if key.key_type() != KeyType::Secp256k1 ||
            local_status.status.finalized_epoch != local_finalized.epoch ||
            local_status.status.finalized_root != local_finalized.root ||
            local_status.status.head_slot > current_slot ||
            canonical_roots.len() > 4096 ||
            validate_status(
                &config,
                current_slot,
                local_finalized,
                &canonical_roots,
                &local_status,
            ) != Ok(StatusCompatibility::KnownFinality)
        {
            return Err(TransportError::Configuration);
        }
        let config = Arc::new(config);
        let requests_v1 = status_requests(config.clone(), Protocol::StatusV1);
        let requests_v2 = status_requests(config.clone(), Protocol::StatusV2);
        let blocks = BlockCodec::new(config.clone());
        let blocks_range = block_requests(blocks.clone(), Protocol::BlocksByRangeV2);
        let envelopes = EnvelopeCodec::new(config.clone(), &blocks);
        let sidecars = SidecarCodec::new(config.clone(), &blocks);
        let blocks_root = block_requests(blocks, Protocol::BlocksByRootV2);
        let envelopes_range =
            envelope_requests(envelopes.clone(), Protocol::PayloadEnvelopesByRangeV1);
        let envelopes_root = envelope_requests(envelopes, Protocol::PayloadEnvelopesByRootV1);
        let sidecars_blob_range =
            sidecar_requests(sidecars.clone(), Protocol::BlobSidecarsByRangeV1);
        let sidecars_blob_root = sidecar_requests(sidecars.clone(), Protocol::BlobSidecarsByRootV1);
        let sidecars_column_range =
            sidecar_requests(sidecars.clone(), Protocol::DataColumnsByRangeV1);
        let sidecars_column_root = sidecar_requests(sidecars, Protocol::DataColumnsByRootV1);
        // Connection limits reject excess pending connections before Noise authentication.
        // Eight workers per connection bound tiny decoded requests to 128 across 16 connections.
        // Blocks and envelopes share two global decoding/retention and two write reservations.
        let connections = connection_limits::Behaviour::new(
            connection_limits::ConnectionLimits::default()
                .with_max_pending_incoming(Some(8))
                .with_max_pending_outgoing(Some(16))
                .with_max_established_incoming(Some(8))
                .with_max_established_outgoing(Some(16))
                .with_max_established(Some(16))
                .with_max_established_per_peer(Some(2)),
        );
        let behaviour = StatusBehaviour {
            requests_v1,
            requests_v2,
            ping: control_requests(ControlProtocol::Ping),
            metadata_v1: control_requests(ControlProtocol::MetadataV1),
            metadata_v2: control_requests(ControlProtocol::MetadataV2),
            metadata_v3: control_requests(ControlProtocol::MetadataV3),
            gossip: crate::gossip::behaviour(config.clone(), current_slot)
                .map_err(|_| TransportError::Configuration)?,
            blocks_range,
            blocks_root,
            envelopes_range,
            envelopes_root,
            sidecars_blob_range,
            sidecars_blob_root,
            sidecars_column_range,
            sidecars_column_root,
            connections,
        };
        let swarm = SwarmBuilder::with_existing_identity(key)
            .with_tokio()
            .with_tcp(
                tcp::Config::default().nodelay(true),
                noise::Config::new,
                (bounded_yamux, bounded_mplex),
            )
            .map_err(|_| TransportError::Configuration)?
            .with_quic_config(|mut config| {
                config.handshake_timeout = REQUEST_TIMEOUT;
                config.max_concurrent_stream_limit = MAX_MUX_STREAMS as u32;
                config.max_stream_data = 64 * 1024;
                config.max_connection_data = 512 * 1024;
                config
            })
            .with_behaviour(|_| behaviour)
            .map_err(|_| TransportError::Configuration)?
            .with_swarm_config(|config| {
                config
                    .with_idle_connection_timeout(Duration::from_secs(15))
                    .with_max_negotiating_inbound_streams(MAX_WORKERS_PER_CONNECTION)
            })
            .with_connection_timeout(REQUEST_TIMEOUT)
            .build();
        Ok(Self {
            swarm,
            config: config.clone(),
            current_slot,
            local_finalized,
            canonical_roots,
            local_status,
            peers: HashSet::new(),
            connections: connections::ConnectionInventory::default(),
            pending: HashMap::new(),
            checked_peers: HashSet::new(),
            block_pending: HashMap::new(),
            block_inbound: HashMap::new(),
            next_block_request: 0,
            envelope_pending: HashMap::new(),
            envelope_inbound: HashMap::new(),
            next_envelope_request: 0,
            sidecar_pending: HashMap::new(),
            sidecar_inbound: HashMap::new(),
            next_sidecar_request: 0,
            dialing: HashSet::new(),
            policy,
            listeners: HashSet::new(),
            next_status_refresh: tokio::time::Instant::now() + STATUS_REFRESH_INTERVAL,
            local_metadata: LocalMetadata::default(),
            control_pending: HashMap::new(),
            gossip_pending: HashMap::new(),
            gossip_bytes: Self::gossip_budget(),
            gossip_topics: crate::gossip::active_topics(&config, current_slot)
                .map_err(|_| TransportError::Configuration)?
                .iter()
                .map(|topic| topic.name())
                .collect(),
            custody_columns: None,
            next_gossip_request: 0,
        })
    }

    /// Atomically advance trusted clock/finality/head inputs after consensus validation.
    /// Conflicting known finalized roots and regressions are rejected before mutation.
    /// A fork digest change closes existing peers. Outbound requests use the exact active status
    /// version.
    pub fn update_snapshot(
        &mut self,
        current_slot: Slot,
        finalized: Checkpoint,
        canonical_roots: Vec<(Epoch, Root)>,
        local_status: StatusV2,
    ) -> Result<(), TransportError> {
        self.validate_snapshot(current_slot, finalized, &canonical_roots, &local_status)?;
        let changed = local_status.status.fork_digest != self.local_status.status.fork_digest;
        if changed {
            for peer in self.peers.iter().chain(&self.dialing).copied().collect::<Vec<_>>() {
                self.reject(peer);
            }
        }
        self.update_gossip_topics(current_slot)?;
        self.current_slot = current_slot;
        self.local_finalized = finalized;
        self.canonical_roots = canonical_roots;
        self.local_status = local_status;
        Ok(())
    }

    pub(super) fn validate_snapshot(
        &self,
        current_slot: Slot,
        finalized: Checkpoint,
        canonical_roots: &[(Epoch, Root)],
        local_status: &StatusV2,
    ) -> Result<(), TransportError> {
        if current_slot < self.current_slot ||
            finalized.epoch < self.local_finalized.epoch ||
            local_status.status.finalized_epoch != finalized.epoch ||
            local_status.status.finalized_root != finalized.root ||
            local_status.status.head_slot > current_slot ||
            canonical_roots.len() > 4096 ||
            (finalized.epoch == self.local_finalized.epoch &&
                finalized.root != self.local_finalized.root) ||
            canonical_roots.iter().any(|(epoch, root)| {
                (*epoch == self.local_finalized.epoch && *root != self.local_finalized.root) ||
                    self.canonical_roots
                        .iter()
                        .any(|(old_epoch, old_root)| epoch == old_epoch && root != old_root)
            }) ||
            validate_status(&self.config, current_slot, finalized, canonical_roots, local_status) !=
                Ok(StatusCompatibility::KnownFinality)
        {
            return Err(TransportError::Configuration);
        }
        Ok(())
    }

    /// Whether the separate peer, dial and outbound request budgets permit another candidate.
    pub fn has_dial_capacity(&self) -> bool {
        self.peers.len() + self.dialing.len() < MAX_PEERS && self.pending.len() < MAX_STREAMS
    }

    /// Close all listeners and connections; no pending requests or candidates are retained.
    pub fn shutdown(&mut self) {
        for listener in self.listeners.drain() {
            self.swarm.remove_listener(listener);
        }
        for peer in self.peers.iter().chain(&self.dialing).copied().collect::<Vec<_>>() {
            self.reject(peer);
        }
        self.connections.clear();
        self.pending.clear();
        self.control_pending.clear();
        self.checked_peers.clear();
        self.block_pending.clear();
        self.block_inbound.clear();
        self.envelope_pending.clear();
        self.sidecar_pending.clear();
        self.sidecar_inbound.clear();
        self.envelope_inbound.clear();
    }

    /// The identity authenticated on every transport connection.
    pub fn peer_id(&self) -> PeerId {
        *self.swarm.local_peer_id()
    }

    /// Begin listening on a permitted explicit TCP or QUIC endpoint; port zero is allowed.
    pub fn listen_on(&mut self, address: Multiaddr) -> Result<(), TransportError> {
        validate_address(&address, true, self.policy)?;
        if self.listeners.len() >= 4 {
            return Err(TransportError::Capacity);
        }
        let listener = self.swarm.listen_on(address).map_err(|_| TransportError::Listen)?;
        self.listeners.insert(listener);
        Ok(())
    }

    /// Dial one explicit endpoint and require its expected authenticated identity.
    pub fn dial(&mut self, peer: PeerId, address: Multiaddr) -> Result<(), TransportError> {
        self.dial_addresses(peer, vec![address])
    }

    /// Dial at most four signed literal endpoints, trying QUIC before TCP, one attempt at a time.
    /// The expected peer identity applies to every fallback; DNS and relays are rejected.
    pub fn dial_addresses(
        &mut self,
        peer: PeerId,
        mut addresses: Vec<Multiaddr>,
    ) -> Result<(), TransportError> {
        if addresses.is_empty() || addresses.len() > 4 {
            return Err(TransportError::Address);
        }
        for address in &addresses {
            validate_address(address, false, self.policy)?;
        }
        if !self.has_dial_capacity() {
            return Err(TransportError::Capacity);
        }
        if self.peers.contains(&peer) || self.dialing.contains(&peer) {
            return Err(TransportError::Dial);
        }
        addresses.sort_by_key(|address| !address.iter().any(|part| part == AddressPart::QuicV1));
        addresses.dedup();
        self.swarm
            .dial(
                DialOpts::peer_id(peer)
                    .addresses(addresses)
                    .override_dial_concurrency_factor(
                        NonZeroU8::new(1).ok_or(TransportError::Configuration)?,
                    )
                    .build(),
            )
            .map_err(|_| TransportError::Dial)?;
        self.dialing.insert(peer);
        Ok(())
    }

    /// Request at most sixteen blocks from a peer whose exact status exchange already passed.
    pub fn request_blocks(
        &mut self,
        peer: PeerId,
        request: BlockRequest,
    ) -> Result<BlockRequestId, TransportError> {
        request.validate_outbound().map_err(|_| TransportError::Configuration)?;
        if !self.checked_peers.contains(&peer) {
            return Err(TransportError::Configuration);
        }
        if self.block_pending.len() + self.envelope_pending.len() + self.sidecar_pending.len() >=
            MAX_BLOCK_REQUESTS ||
            self.envelope_pending.values().any(|pending| pending.peer == peer) ||
            self.sidecar_pending.values().any(|pending| pending.peer == peer) ||
            self.block_pending.values().any(|pending| pending.peer == peer)
        {
            return Err(TransportError::Capacity);
        }
        let id = self.allocate_block_id()?;
        let range = matches!(request, BlockRequest::Range { .. });
        let request_id = self.blocks_mut(range).send_request(&peer, request.clone());
        self.block_pending.insert((range, request_id), PendingBlockRequest { peer, id, request });
        Ok(id)
    }

    /// Serve structurally checked retained signed blocks only on the original private channel.
    /// Invalid provider data cannot consume the token or produce a peer response.
    pub fn respond_blocks(
        &mut self,
        id: BlockRequestId,
        blocks: Vec<WireBlock>,
    ) -> Result<(), TransportError> {
        let pending = self.block_inbound.get(&id).ok_or(TransportError::Configuration)?;
        pending.request.matches(&blocks).map_err(|_| TransportError::Configuration)?;
        for block in &blocks {
            if self
                .config
                .fork_at_slot(block.slot())
                .map_err(|_| TransportError::Configuration)?
                .fork !=
                block.fork() ||
                self.config
                    .fork_digest(block.slot() / self.config.slots_per_epoch)
                    .map_err(|_| TransportError::Configuration)? !=
                    block.context()
            {
                return Err(TransportError::Configuration);
            }
        }
        let pending = self.block_inbound.remove(&id).ok_or(TransportError::Configuration)?;
        self.blocks_mut(pending.range)
            .send_response(pending.channel, BlockReply::Blocks(BlockBatch::serving(blocks)))
            .map_err(|_| TransportError::Configuration)
    }

    /// Return a bounded unavailable response using an existing private request token.
    pub fn respond_blocks_unavailable(&mut self, id: BlockRequestId) -> Result<(), TransportError> {
        let pending = self.block_inbound.remove(&id).ok_or(TransportError::Configuration)?;
        self.blocks_mut(pending.range)
            .send_response(pending.channel, BlockReply::Error(3))
            .map_err(|_| TransportError::Configuration)
    }

    /// Request bounded envelopes after blocks have been imported or durably retained.
    /// References contain exact immutable decoded block metadata, no SSZ bytes or batch permits.
    pub fn request_envelopes(
        &mut self,
        peer: PeerId,
        request: EnvelopeRequest,
        blocks: Vec<EnvelopeBlockReference>,
    ) -> Result<EnvelopeRequestId, TransportError> {
        request
            .validate_expected(&self.config, &blocks)
            .map_err(|_| TransportError::Configuration)?;
        if !self.checked_peers.contains(&peer) {
            return Err(TransportError::Configuration);
        }
        if self.block_pending.len() + self.envelope_pending.len() + self.sidecar_pending.len() >=
            MAX_BLOCK_REQUESTS ||
            self.block_pending.values().any(|pending| pending.peer == peer) ||
            self.envelope_pending.values().any(|pending| pending.peer == peer) ||
            self.sidecar_pending.values().any(|pending| pending.peer == peer)
        {
            return Err(TransportError::Capacity);
        }
        let id = self.allocate_envelope_id()?;
        let range = matches!(request, EnvelopeRequest::Range { .. });
        let request_id = self.envelopes_mut(range).send_request(&peer, request.clone());
        self.envelope_pending
            .insert((range, request_id), PendingEnvelopeRequest { peer, id, request, blocks });
        Ok(id)
    }

    /// Serve complete signed envelopes using exact metadata from retained decoded beacon blocks.
    /// Wrong root, parent, slot or context cannot consume the original inbound token.
    pub fn respond_envelopes(
        &mut self,
        id: EnvelopeRequestId,
        envelopes: Vec<WireEnvelope>,
        blocks: &[EnvelopeBlockReference],
    ) -> Result<(), TransportError> {
        let pending = self.envelope_inbound.get(&id).ok_or(TransportError::Configuration)?;
        pending
            .request
            .validate_serving(&self.config, blocks)
            .map_err(|_| TransportError::Configuration)?;
        let batch = EnvelopeBatch::serving(envelopes);
        pending
            .request
            .matches(&self.config, &batch, |root| blocks.iter().find(|block| block.root() == root))
            .map_err(|_| TransportError::Configuration)?;
        let pending = self.envelope_inbound.remove(&id).ok_or(TransportError::Configuration)?;
        self.envelopes_mut(pending.range)
            .send_response(pending.channel, EnvelopeReply::Envelopes(batch))
            .map_err(|_| TransportError::Configuration)
    }

    /// Report unavailable envelope history through the original bounded private response channel.
    pub fn respond_envelopes_unavailable(
        &mut self,
        id: EnvelopeRequestId,
    ) -> Result<(), TransportError> {
        let pending = self.envelope_inbound.remove(&id).ok_or(TransportError::Configuration)?;
        self.envelopes_mut(pending.range)
            .send_response(pending.channel, EnvelopeReply::Error(3))
            .map_err(|_| TransportError::Configuration)
    }
    /// Drive actual IO until an observation is available. A dialer immediately sends status.
    /// This method must be polled for handshakes, response deadlines and disconnects to progress.
    pub async fn next_event(&mut self) -> TransportEvent {
        loop {
            // Persist the deadline in the owner: unrelated IO/tick cancellation must not
            // postpone periodic head hints indefinitely or create additional status jobs.
            self.expire_gossip();
            let observation = tokio::select! {
                event = self.swarm.select_next_some() => event,
                _ = tokio::time::sleep_until(self.next_status_refresh) => {
                    self.refresh_status();
                    continue;
                }
            };
            let event = match observation {
                SwarmEvent::Behaviour(StatusBehaviourEvent::Gossip(event)) => {
                    if let Some(observation) = self.handle_gossip(event) {
                        return observation
                    }
                    continue;
                }
                SwarmEvent::Behaviour(StatusBehaviourEvent::Ping(event)) => {
                    if let Some(observation) =
                        self.handle_control_event(ControlProtocol::Ping, event)
                    {
                        return observation
                    }
                    continue;
                }
                SwarmEvent::Behaviour(StatusBehaviourEvent::MetadataV1(event)) => {
                    if let Some(observation) =
                        self.handle_control_event(ControlProtocol::MetadataV1, event)
                    {
                        return observation
                    }
                    continue;
                }
                SwarmEvent::Behaviour(StatusBehaviourEvent::MetadataV2(event)) => {
                    if let Some(observation) =
                        self.handle_control_event(ControlProtocol::MetadataV2, event)
                    {
                        return observation
                    }
                    continue;
                }
                SwarmEvent::Behaviour(StatusBehaviourEvent::MetadataV3(event)) => {
                    if let Some(observation) =
                        self.handle_control_event(ControlProtocol::MetadataV3, event)
                    {
                        return observation
                    }
                    continue;
                }
                SwarmEvent::Behaviour(StatusBehaviourEvent::EnvelopesRange(event)) => {
                    if let Some(observation) = self.handle_envelope_event(true, event) {
                        return observation;
                    }
                    continue;
                }
                SwarmEvent::Behaviour(StatusBehaviourEvent::EnvelopesRoot(event)) => {
                    if let Some(observation) = self.handle_envelope_event(false, event) {
                        return observation;
                    }
                    continue;
                }
                SwarmEvent::Behaviour(StatusBehaviourEvent::BlocksRange(event)) => {
                    if let Some(observation) = self.handle_block_event(true, event) {
                        return observation;
                    }
                    continue;
                }
                SwarmEvent::Behaviour(StatusBehaviourEvent::BlocksRoot(event)) => {
                    if let Some(observation) = self.handle_block_event(false, event) {
                        return observation;
                    }
                    continue;
                }
                SwarmEvent::Behaviour(StatusBehaviourEvent::SidecarsBlobRange(event)) => {
                    if let Some(observation) =
                        self.handle_sidecar_event(Protocol::BlobSidecarsByRangeV1, event)
                    {
                        return observation
                    }
                    continue;
                }
                SwarmEvent::Behaviour(StatusBehaviourEvent::SidecarsBlobRoot(event)) => {
                    if let Some(observation) =
                        self.handle_sidecar_event(Protocol::BlobSidecarsByRootV1, event)
                    {
                        return observation
                    }
                    continue;
                }
                SwarmEvent::Behaviour(StatusBehaviourEvent::SidecarsColumnRange(event)) => {
                    if let Some(observation) =
                        self.handle_sidecar_event(Protocol::DataColumnsByRangeV1, event)
                    {
                        return observation
                    }
                    continue;
                }
                SwarmEvent::Behaviour(StatusBehaviourEvent::SidecarsColumnRoot(event)) => {
                    if let Some(observation) =
                        self.handle_sidecar_event(Protocol::DataColumnsByRootV1, event)
                    {
                        return observation
                    }
                    continue;
                }
                event => event,
            };
            let (schema_v2, event) = match event {
                SwarmEvent::Behaviour(StatusBehaviourEvent::RequestsV2(event)) => {
                    (true, SwarmEvent::Behaviour(StatusBehaviourEvent::RequestsV1(event)))
                }
                event => (false, event),
            };
            if let SwarmEvent::Behaviour(StatusBehaviourEvent::RequestsV1(status_event)) = &event {
                match self.admit_status_event(status_event, schema_v2) {
                    Ok(true) => {}
                    Ok(false) => continue,
                    Err(rejected) => return rejected,
                }
            }
            match event {
                SwarmEvent::NewListenAddr { address, .. } => {
                    return TransportEvent::Listening(address)
                }
                SwarmEvent::ConnectionEstablished { peer_id, connection_id, endpoint, .. } => {
                    self.dialing.remove(&peer_id);
                    if self.peers.len() >= MAX_PEERS && !self.peers.contains(&peer_id) {
                        self.reject(peer_id);
                        return TransportEvent::Rejected {
                            peer: peer_id,
                            reason: StatusRejection::Capacity,
                        };
                    }
                    if !self.connections.established(connection_id, peer_id) {
                        self.reject(peer_id);
                        return TransportEvent::Rejected {
                            peer: peer_id,
                            reason: StatusRejection::Capacity,
                        };
                    }
                    self.peers.insert(peer_id);
                    if matches!(endpoint, ConnectedPoint::Dialer { .. }) {
                        if self.pending.len() >= MAX_STREAMS {
                            self.reject(peer_id);
                            return TransportEvent::Rejected {
                                peer: peer_id,
                                reason: StatusRejection::Capacity,
                            };
                        }
                        let schema_v2 = self
                            .config
                            .fork_at_slot(self.current_slot)
                            .is_ok_and(|fork| fork.fork >= Fork::Fulu);
                        let status = StatusAdvertisement::from_local(&self.local_status);
                        let request = self.requests_mut(schema_v2).send_request(&peer_id, status);
                        self.pending.insert((schema_v2, request), peer_id);
                    }
                }
                SwarmEvent::Behaviour(StatusBehaviourEvent::RequestsV1(
                    request_response::Event::Message { peer, message, .. },
                )) => {
                    match message {
                        Message::Request { request, channel, .. } => {
                            match self.check(&request) {
                                Ok(compatibility) => {
                                    let reply = StatusReply::Status(
                                        StatusAdvertisement::from_local(&self.local_status),
                                    );
                                    if self
                                        .requests_mut(schema_v2)
                                        .send_response(channel, reply)
                                        .is_err()
                                    {
                                        self.reject(peer);
                                        return TransportEvent::Rejected {
                                            peer,
                                            reason: StatusRejection::Protocol,
                                        };
                                    }
                                    self.checked_peers.insert(peer);
                                    return TransportEvent::PeerStatus {
                                        peer,
                                        status: request.status,
                                        earliest_available_slot: request.earliest_available_slot,
                                        compatibility,
                                    };
                                }
                                Err(reason) => {
                                    // Rejected claims cannot enter the peer-status observation
                                    // path.
                                    drop(channel);
                                    self.reject(peer);
                                    return TransportEvent::Rejected {
                                        peer,
                                        reason: StatusRejection::Handshake(reason),
                                    };
                                }
                            }
                        }
                        Message::Response { request_id, response } => {
                            if self.pending.remove(&(schema_v2, request_id)) != Some(peer) {
                                self.reject(peer);
                                return TransportEvent::Rejected {
                                    peer,
                                    reason: StatusRejection::Protocol,
                                };
                            }
                            match response {
                                StatusReply::Status(status) => match self.check(&status) {
                                    Ok(compatibility) => {
                                        self.checked_peers.insert(peer);
                                        return TransportEvent::PeerStatus {
                                            peer,
                                            status: status.status,
                                            earliest_available_slot: status.earliest_available_slot,
                                            compatibility,
                                        }
                                    }
                                    Err(reason) => {
                                        self.reject(peer);
                                        return TransportEvent::Rejected {
                                            peer,
                                            reason: StatusRejection::Handshake(reason),
                                        };
                                    }
                                },
                                StatusReply::Error => {
                                    self.reject(peer);
                                    return TransportEvent::Rejected {
                                        peer,
                                        reason: StatusRejection::Protocol,
                                    };
                                }
                            }
                        }
                    }
                }
                SwarmEvent::Behaviour(StatusBehaviourEvent::RequestsV1(
                    request_response::Event::OutboundFailure { peer, request_id, .. },
                )) => {
                    if let Some(event) = self.failed_status_request(schema_v2, peer, request_id) {
                        return event;
                    }
                }
                SwarmEvent::Behaviour(StatusBehaviourEvent::RequestsV1(
                    request_response::Event::InboundFailure { peer, .. },
                )) => {
                    self.reject(peer);
                    return TransportEvent::Rejected { peer, reason: StatusRejection::Protocol };
                }
                SwarmEvent::ConnectionClosed { peer_id, connection_id, .. } => {
                    if !self.connections.closed(connection_id, peer_id) {
                        continue;
                    }
                    self.reject_gossip_peer(peer_id);
                    self.peers.remove(&peer_id);
                    self.pending.retain(|_, peer| *peer != peer_id);
                    self.checked_peers.remove(&peer_id);
                    self.control_pending.retain(|_, peer| *peer != peer_id);
                    self.block_pending.retain(|_, pending| pending.peer != peer_id);
                    self.block_inbound.retain(|_, pending| pending.peer != peer_id);
                    self.envelope_pending.retain(|_, pending| pending.peer != peer_id);
                    self.envelope_inbound.retain(|_, pending| pending.peer != peer_id);
                    self.sidecar_pending.retain(|_, pending| pending.peer != peer_id);
                    self.sidecar_inbound.retain(|_, pending| pending.peer != peer_id);
                    return TransportEvent::Closed(peer_id);
                }
                SwarmEvent::ListenerClosed { listener_id, .. } => {
                    self.listeners.remove(&listener_id);
                }
                SwarmEvent::OutgoingConnectionError { peer_id, .. } => {
                    if let Some(peer) = peer_id {
                        self.dialing.remove(&peer);
                    }
                    return TransportEvent::DialFailed(peer_id);
                }
                _ => {}
            }
        }
    }

    fn allocate_block_id(&mut self) -> Result<BlockRequestId, TransportError> {
        self.next_block_request =
            self.next_block_request.checked_add(1).ok_or(TransportError::Capacity)?;
        Ok(BlockRequestId { owner: self.peer_id(), sequence: self.next_block_request })
    }

    fn blocks_mut(&mut self, range: bool) -> &mut request_response::Behaviour<BlockCodec> {
        if range {
            &mut self.swarm.behaviour_mut().blocks_range
        } else {
            &mut self.swarm.behaviour_mut().blocks_root
        }
    }

    fn handle_block_event(
        &mut self,
        range: bool,
        event: request_response::Event<BlockRequest, BlockReply>,
    ) -> Option<TransportEvent> {
        let owned = match &event {
            request_response::Event::Message {
                message: Message::Response { request_id, .. },
                ..
            } |
            request_response::Event::OutboundFailure { request_id, .. } => {
                self.block_pending.get(&(range, *request_id)).map(|pending| pending.peer)
            }
            request_response::Event::InboundFailure { request_id, .. } => self
                .block_inbound
                .values()
                .find(|pending| pending.range == range && pending.request_id == *request_id)
                .map(|pending| pending.peer),
            _ => None,
        };
        match self.admit_rpc_event(&event, owned) {
            Ok(true) => {}
            Ok(false) => return None,
            Err(rejected) => return Some(rejected),
        }
        match event {
            request_response::Event::Message { peer, message, .. } => match message {
                Message::Request { request_id, request, channel } => {
                    if !self.checked_peers.contains(&peer) {
                        drop(channel);
                        self.reject(peer);
                        return Some(TransportEvent::Rejected {
                            peer,
                            reason: StatusRejection::Protocol,
                        });
                    }
                    if self.block_inbound.len() +
                        self.envelope_inbound.len() +
                        self.sidecar_inbound.len() >=
                        MAX_INBOUND_BLOCK_REQUESTS
                    {
                        let _ = self.blocks_mut(range).send_response(channel, BlockReply::Error(2));
                        return None;
                    }
                    let Ok(id) = self.allocate_block_id() else {
                        drop(channel);
                        self.reject(peer);
                        return Some(TransportEvent::Rejected {
                            peer,
                            reason: StatusRejection::Capacity,
                        });
                    };
                    self.block_inbound.insert(
                        id,
                        InboundBlockRequest {
                            peer,
                            request_id,
                            range,
                            request: request.clone(),
                            channel,
                        },
                    );
                    Some(TransportEvent::BlockRequest { peer, id, request })
                }
                Message::Response { request_id, response } => {
                    let Some(pending) = self
                        .block_pending
                        .remove(&(range, request_id))
                        .filter(|pending| pending.peer == peer)
                    else {
                        self.reject(peer);
                        return Some(TransportEvent::Rejected {
                            peer,
                            reason: StatusRejection::Protocol,
                        });
                    };
                    match response {
                        BlockReply::Blocks(batch)
                            if pending.request.matches(batch.blocks()).is_ok() =>
                        {
                            Some(TransportEvent::Blocks { peer, id: pending.id, batch })
                        }
                        BlockReply::Error(code) => {
                            Some(TransportEvent::BlocksUnavailable { peer, id: pending.id, code })
                        }
                        _ => {
                            self.reject(peer);
                            Some(TransportEvent::Rejected {
                                peer,
                                reason: StatusRejection::Protocol,
                            })
                        }
                    }
                }
            },
            request_response::Event::OutboundFailure { peer, request_id, .. } => {
                // Only this exact active outbound token can reject a current connection.
                self.block_pending.remove(&(range, request_id))?;
                self.reject(peer);
                Some(TransportEvent::Rejected { peer, reason: StatusRejection::Protocol })
            }
            request_response::Event::InboundFailure { peer, request_id, .. } => {
                self.block_inbound.retain(|_, pending| {
                    pending.peer != peer ||
                        pending.range != range ||
                        pending.request_id != request_id
                });
                self.reject(peer);
                Some(TransportEvent::Rejected { peer, reason: StatusRejection::Protocol })
            }
            request_response::Event::ResponseSent { .. } => None,
        }
    }

    fn allocate_envelope_id(&mut self) -> Result<EnvelopeRequestId, TransportError> {
        self.next_envelope_request =
            self.next_envelope_request.checked_add(1).ok_or(TransportError::Capacity)?;
        Ok(EnvelopeRequestId { owner: self.peer_id(), sequence: self.next_envelope_request })
    }

    fn envelopes_mut(&mut self, range: bool) -> &mut request_response::Behaviour<EnvelopeCodec> {
        if range {
            &mut self.swarm.behaviour_mut().envelopes_range
        } else {
            &mut self.swarm.behaviour_mut().envelopes_root
        }
    }
    fn handle_envelope_event(
        &mut self,
        range: bool,
        event: request_response::Event<EnvelopeRequest, EnvelopeReply>,
    ) -> Option<TransportEvent> {
        let owned = match &event {
            request_response::Event::Message {
                message: Message::Response { request_id, .. },
                ..
            } |
            request_response::Event::OutboundFailure { request_id, .. } => {
                self.envelope_pending.get(&(range, *request_id)).map(|pending| pending.peer)
            }
            request_response::Event::InboundFailure { request_id, .. } => self
                .envelope_inbound
                .values()
                .find(|pending| pending.range == range && pending.request_id == *request_id)
                .map(|pending| pending.peer),
            _ => None,
        };
        match self.admit_rpc_event(&event, owned) {
            Ok(true) => {}
            Ok(false) => return None,
            Err(rejected) => return Some(rejected),
        }
        match event {
            request_response::Event::Message { peer, message, .. } => match message {
                Message::Request { request_id, request, channel } => {
                    if !self.checked_peers.contains(&peer) {
                        drop(channel);
                        self.reject(peer);
                        return Some(TransportEvent::Rejected {
                            peer,
                            reason: StatusRejection::Protocol,
                        });
                    }
                    if self.block_inbound.len() +
                        self.envelope_inbound.len() +
                        self.sidecar_inbound.len() >=
                        MAX_INBOUND_BLOCK_REQUESTS
                    {
                        let _ = self
                            .envelopes_mut(range)
                            .send_response(channel, EnvelopeReply::Error(2));
                        return None;
                    }
                    let Ok(id) = self.allocate_envelope_id() else {
                        drop(channel);
                        self.reject(peer);
                        return Some(TransportEvent::Rejected {
                            peer,
                            reason: StatusRejection::Capacity,
                        });
                    };
                    self.envelope_inbound.insert(
                        id,
                        InboundEnvelopeRequest {
                            peer,
                            request_id,
                            range,
                            request: request.clone(),
                            channel,
                        },
                    );
                    Some(TransportEvent::EnvelopeRequest { peer, id, request })
                }
                Message::Response { request_id, response } => {
                    let Some(pending) = self
                        .envelope_pending
                        .remove(&(range, request_id))
                        .filter(|pending| pending.peer == peer)
                    else {
                        self.reject(peer);
                        return Some(TransportEvent::Rejected {
                            peer,
                            reason: StatusRejection::Protocol,
                        });
                    };
                    match response {
                        EnvelopeReply::Envelopes(batch)
                            if pending
                                .request
                                .matches(&self.config, &batch, |root| {
                                    pending.blocks.iter().find(|block| block.root() == root)
                                })
                                .is_ok() =>
                        {
                            Some(TransportEvent::Envelopes { peer, id: pending.id, batch })
                        }
                        EnvelopeReply::Error(code) => Some(TransportEvent::EnvelopesUnavailable {
                            peer,
                            id: pending.id,
                            code,
                        }),
                        _ => {
                            self.reject(peer);
                            Some(TransportEvent::Rejected {
                                peer,
                                reason: StatusRejection::Protocol,
                            })
                        }
                    }
                }
            },
            request_response::Event::OutboundFailure { peer, request_id, .. } => {
                // Only this exact active outbound token can reject a current connection.
                self.envelope_pending.remove(&(range, request_id))?;
                self.reject(peer);
                Some(TransportEvent::Rejected { peer, reason: StatusRejection::Protocol })
            }
            request_response::Event::InboundFailure { peer, request_id, .. } => {
                self.envelope_inbound.retain(|_, pending| {
                    pending.peer != peer ||
                        pending.range != range ||
                        pending.request_id != request_id
                });
                self.reject(peer);
                Some(TransportEvent::Rejected { peer, reason: StatusRejection::Protocol })
            }
            request_response::Event::ResponseSent { .. } => None,
        }
    }

    fn requests_mut(&mut self, schema_v2: bool) -> &mut request_response::Behaviour<StatusCodec> {
        if schema_v2 {
            &mut self.swarm.behaviour_mut().requests_v2
        } else {
            &mut self.swarm.behaviour_mut().requests_v1
        }
    }

    fn check(&self, status: &StatusAdvertisement) -> Result<StatusCompatibility, HandshakeError> {
        let expects_v2 = self
            .config
            .fork_at_slot(self.current_slot)
            .map_err(|_| HandshakeError::Configuration)?
            .fork >=
            Fork::Fulu;
        if expects_v2 != status.earliest_available_slot.is_some() {
            return Err(HandshakeError::StatusVersion);
        }
        validate_status(
            &self.config,
            self.current_slot,
            self.local_finalized,
            &self.canonical_roots,
            &StatusV2 {
                status: status.status.clone(),
                earliest_available_slot: status.earliest_available_slot.unwrap_or(0),
            },
        )
    }

    fn reject(&mut self, peer: PeerId) {
        self.connections.retire_peer(peer);
        self.reject_gossip_peer(peer);
        self.pending.retain(|_, pending_peer| *pending_peer != peer);
        self.checked_peers.remove(&peer);
        self.control_pending.retain(|_, pending| *pending != peer);
        self.block_pending.retain(|_, pending| pending.peer != peer);
        self.block_inbound.retain(|_, pending| pending.peer != peer);
        self.envelope_pending.retain(|_, pending| pending.peer != peer);
        self.sidecar_pending.retain(|_, pending| pending.peer != peer);
        self.sidecar_inbound.retain(|_, pending| pending.peer != peer);
        self.envelope_inbound.retain(|_, pending| pending.peer != peer);
        self.peers.remove(&peer);
        self.dialing.remove(&peer);
        let _ = self.swarm.disconnect_peer_id(peer);
    }

    /// Retire one provider connection and its transport reservations so another can be dialed.
    /// This grants no data validity and applies no ban. Established connections still emit
    /// `Closed` when the underlying transport finishes closing, preserving caller token cleanup.
    pub fn retire_peer(&mut self, peer: PeerId) {
        self.reject(peer);
    }
}

#[derive(NetworkBehaviour)]
struct StatusBehaviour {
    // Admission must run before either request handler records an established connection.
    // Otherwise a later denial leaves request bookkeeping for a connection the swarm never
    // accepted.
    connections: connection_limits::Behaviour,
    requests_v1: request_response::Behaviour<StatusCodec>,
    requests_v2: request_response::Behaviour<StatusCodec>,
    ping: request_response::Behaviour<crate::control_rpc::ControlCodec>,
    metadata_v1: request_response::Behaviour<crate::control_rpc::ControlCodec>,
    metadata_v2: request_response::Behaviour<crate::control_rpc::ControlCodec>,
    metadata_v3: request_response::Behaviour<crate::control_rpc::ControlCodec>,
    gossip: crate::gossip::GossipBehaviour,
    blocks_range: request_response::Behaviour<BlockCodec>,
    blocks_root: request_response::Behaviour<BlockCodec>,
    envelopes_range: request_response::Behaviour<EnvelopeCodec>,
    envelopes_root: request_response::Behaviour<EnvelopeCodec>,
    sidecars_blob_range: request_response::Behaviour<SidecarCodec>,
    sidecars_blob_root: request_response::Behaviour<SidecarCodec>,
    sidecars_column_range: request_response::Behaviour<SidecarCodec>,
    sidecars_column_root: request_response::Behaviour<SidecarCodec>,
}

fn bounded_yamux() -> yamux::Config {
    let mut config = yamux::Config::default();
    config.set_max_num_streams(MAX_MUX_STREAMS);
    config
}

fn bounded_mplex() -> libp2p_mplex::Config {
    let mut config = libp2p_mplex::Config::new();
    config
        .set_max_num_streams(MAX_MUX_STREAMS)
        .set_max_buffer_size(1)
        .set_max_buffer_behaviour(libp2p_mplex::MaxBufferBehaviour::ResetStream)
        .set_split_send_size(1024);
    config
}

fn status_requests(
    config: Arc<ChainConfig>,
    protocol: Protocol,
) -> request_response::Behaviour<StatusCodec> {
    request_response::Behaviour::with_codec(
        StatusCodec { config },
        [(StreamProtocol::new(protocol.id()), ProtocolSupport::Full)],
        request_response::Config::default()
            .with_request_timeout(REQUEST_TIMEOUT)
            .with_max_concurrent_streams(2),
    )
}

fn block_requests(
    codec: BlockCodec,
    protocol: Protocol,
) -> request_response::Behaviour<BlockCodec> {
    request_response::Behaviour::with_codec(
        codec,
        [(StreamProtocol::new(protocol.id()), ProtocolSupport::Full)],
        request_response::Config::default()
            .with_request_timeout(Duration::from_secs(10))
            // Inbound serving and our own outbound response share this finite worker budget.
            .with_max_concurrent_streams(2),
    )
}

fn envelope_requests(
    codec: EnvelopeCodec,
    protocol: Protocol,
) -> request_response::Behaviour<EnvelopeCodec> {
    request_response::Behaviour::with_codec(
        codec,
        [(StreamProtocol::new(protocol.id()), ProtocolSupport::Full)],
        request_response::Config::default()
            .with_request_timeout(Duration::from_secs(10))
            // Inbound serving and our own outbound response share this finite worker budget.
            .with_max_concurrent_streams(2),
    )
}

#[derive(Clone, Debug)]
struct StatusCodec {
    config: Arc<ChainConfig>,
}

#[derive(Clone, Debug)]
enum StatusReply {
    Status(StatusAdvertisement),
    Error,
}

#[derive(Clone, Debug)]
struct StatusAdvertisement {
    status: Status,
    earliest_available_slot: Option<Slot>,
}

impl StatusAdvertisement {
    fn from_local(status: &StatusV2) -> Self {
        Self {
            status: status.status.clone(),
            earliest_available_slot: Some(status.earliest_available_slot),
        }
    }
}

#[async_trait]
impl Codec for StatusCodec {
    type Protocol = StreamProtocol;
    type Request = StatusAdvertisement;
    type Response = StatusReply;

    async fn read_request<T>(
        &mut self,
        protocol: &StreamProtocol,
        io: &mut T,
    ) -> io::Result<StatusAdvertisement>
    where
        T: AsyncRead + Unpin + Send,
    {
        let protocol = status_protocol(protocol)?;
        let decoder = StreamDecoder::request(protocol, STATUS_LIMITS, 1).map_err(wire_io)?;
        match read_message(io, decoder).await? {
            WireMessage::Request(bytes) => decode_status(protocol, &bytes),
            _ => Err(wire_io(WireError::Protocol)),
        }
    }

    async fn read_response<T>(
        &mut self,
        protocol: &StreamProtocol,
        io: &mut T,
    ) -> io::Result<StatusReply>
    where
        T: AsyncRead + Unpin + Send,
    {
        let protocol = status_protocol(protocol)?;
        let decoder =
            StreamDecoder::response(protocol, STATUS_LIMITS, 1, &self.config).map_err(wire_io)?;
        match read_message(io, decoder).await? {
            WireMessage::Response(chunk) if chunk.code == 0 => {
                Ok(StatusReply::Status(decode_status(protocol, &chunk.payload)?))
            }
            WireMessage::Response(_) => Ok(StatusReply::Error),
            _ => Err(wire_io(WireError::Protocol)),
        }
    }

    async fn write_request<T>(
        &mut self,
        protocol: &StreamProtocol,
        io: &mut T,
        request: StatusAdvertisement,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        let wire = encode_status(status_protocol(protocol)?, &request)?;
        io.write_all(&wire).await?;
        io.close().await
    }

    async fn write_response<T>(
        &mut self,
        protocol: &StreamProtocol,
        io: &mut T,
        response: StatusReply,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        match response {
            StatusReply::Status(status) => {
                let wire = encode_status(status_protocol(protocol)?, &status)?;
                io.write_all(&[0]).await?;
                io.write_all(&wire).await?;
            }
            StatusReply::Error => {
                io.write_all(&[1]).await?;
                io.write_all(&encode_request(b"invalid status", STATUS_LIMITS).map_err(wire_io)?)
                    .await?;
            }
        }
        io.close().await
    }
}

async fn read_message<T: AsyncRead + Unpin>(
    io: &mut T,
    mut decoder: StreamDecoder,
) -> io::Result<WireMessage> {
    let mut buffer = [0; 64];
    loop {
        match decoder.next_message().map_err(wire_io)? {
            DecodeProgress::Message(message) => return Ok(message),
            DecodeProgress::Finished => return Err(wire_io(WireError::Truncated)),
            DecodeProgress::NeedMore => {}
        }
        let read = io.read(&mut buffer).await?;
        if read == 0 {
            decoder.finish_input();
        } else {
            decoder.push(&buffer[..read]).map_err(wire_io)?;
        }
    }
}

fn status_protocol(protocol: &StreamProtocol) -> io::Result<Protocol> {
    match Protocol::from_id(protocol.as_ref()).map_err(wire_io)? {
        protocol @ (Protocol::StatusV1 | Protocol::StatusV2) => Ok(protocol),
        _ => Err(wire_io(WireError::Protocol)),
    }
}

fn encode_status(protocol: Protocol, status: &StatusAdvertisement) -> io::Result<Vec<u8>> {
    let payload = match protocol {
        Protocol::StatusV1 => status.status.encode().to_vec(),
        Protocol::StatusV2 => StatusV2 {
            status: status.status.clone(),
            earliest_available_slot: status
                .earliest_available_slot
                .ok_or_else(|| wire_io(WireError::Ssz))?,
        }
        .encode()
        .to_vec(),
        _ => return Err(wire_io(WireError::Protocol)),
    };
    encode_request(&payload, STATUS_LIMITS).map_err(wire_io)
}

fn decode_status(protocol: Protocol, bytes: &[u8]) -> io::Result<StatusAdvertisement> {
    match protocol {
        Protocol::StatusV1 => Ok(StatusAdvertisement {
            status: Status::decode(bytes).map_err(wire_io)?,
            earliest_available_slot: None,
        }),
        Protocol::StatusV2 => {
            let status = StatusV2::decode(bytes).map_err(wire_io)?;
            Ok(StatusAdvertisement::from_local(&status))
        }
        _ => Err(wire_io(WireError::Protocol)),
    }
}

fn wire_io(error: WireError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}

fn validate_address(
    address: &Multiaddr,
    allow_zero: bool,
    policy: EndpointPolicy,
) -> Result<(), TransportError> {
    let mut parts = address.iter();
    let ip = match parts.next() {
        Some(AddressPart::Ip4(ip)) => IpAddr::V4(ip),
        Some(AddressPart::Ip6(ip)) => IpAddr::V6(ip),
        _ => return Err(TransportError::Address),
    };
    let permitted = if allow_zero { policy.permits_bind(ip) } else { policy.permits(ip) };
    if !permitted {
        return Err(TransportError::Address);
    }
    match parts.next() {
        Some(AddressPart::Tcp(port)) if allow_zero || port != 0 => {}
        Some(AddressPart::Udp(port)) if allow_zero || port != 0 => {
            if parts.next() != Some(AddressPart::QuicV1) {
                return Err(TransportError::Address);
            }
        }
        _ => return Err(TransportError::Address),
    }
    if parts.next().is_some() {
        return Err(TransportError::Address);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::timeout;

    pub(super) fn transport(config: ChainConfig, current_slot: Slot) -> LocalStatusTransport {
        let checkpoint = Checkpoint { epoch: 100, root: [1; 32] };
        let status = StatusV2 {
            status: Status {
                fork_digest: config.fork_digest(current_slot / 32).unwrap(),
                finalized_root: checkpoint.root,
                finalized_epoch: checkpoint.epoch,
                head_root: [2; 32],
                head_slot: current_slot,
            },
            earliest_available_slot: 7,
        };
        LocalStatusTransport::new(
            Keypair::generate_secp256k1(),
            config,
            current_slot,
            checkpoint,
            Vec::new(),
            status,
        )
        .unwrap()
    }

    async fn listen(transport: &mut LocalStatusTransport) -> Multiaddr {
        transport.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()).unwrap();
        let event = timeout(Duration::from_secs(10), transport.next_event()).await.unwrap();
        let TransportEvent::Listening(address) = event else {
            panic!("unexpected event {event:?}");
        };
        address
    }

    async fn exchange(current_slot: Slot, availability: Option<Slot>) {
        let config = ChainConfig::hoodi();
        let mut listener = transport(config.clone(), current_slot);
        let mut dialer = transport(config, current_slot);
        let listener_id = listener.peer_id();
        let address = listen(&mut listener).await;
        dialer.dial(listener_id, address).unwrap();
        exchange_connected(&mut listener, &mut dialer, availability).await;
    }

    pub(super) async fn exchange_connected(
        listener: &mut StatusTransport,
        dialer: &mut StatusTransport,
        availability: Option<Slot>,
    ) {
        let listener_id = listener.peer_id();
        let dialer_id = dialer.peer_id();
        timeout(Duration::from_secs(10), async {
            let mut listener_received = false;
            let mut dialer_received = false;
            // Each side sends its own status request. An inbound observation alone must not
            // leave the other private handshake unresolved when the caller stops polling it.
            while !listener_received || !dialer_received ||
                !listener.pending.is_empty() || !dialer.pending.is_empty()
            {
                tokio::select! {
                    event = listener.next_event() => {
                        // A previously retired connection can emit another terminal stream
                        // event after Closed. It cannot satisfy this exact new handshake.
                        if matches!(&event, TransportEvent::Closed(peer) | TransportEvent::Rejected { peer, .. }
                            if *peer != dialer_id && !listener.peers.contains(peer))
                        {
                            continue;
                        }
                        let TransportEvent::PeerStatus { peer, status, earliest_available_slot, compatibility } = event
                            else { panic!("listener event {event:?}"); };
                        assert_eq!(peer, dialer_id);
                        assert_eq!(status, dialer.local_status.status);
                        assert_eq!(earliest_available_slot, availability);
                        assert_eq!(compatibility, StatusCompatibility::KnownFinality);
                        listener_received = true;
                    }
                    event = dialer.next_event() => {
                        if matches!(&event, TransportEvent::Closed(peer) | TransportEvent::Rejected { peer, .. }
                            if *peer != listener_id && !dialer.peers.contains(peer))
                        {
                            continue;
                        }
                        let TransportEvent::PeerStatus { peer, status, earliest_available_slot, compatibility } = event
                            else { panic!("dialer event {event:?}"); };
                        assert_eq!(peer, listener_id);
                        assert_eq!(status, listener.local_status.status);
                        assert_eq!(earliest_available_slot, availability);
                        assert_eq!(compatibility, StatusCompatibility::KnownFinality);
                        dialer_received = true;
                    }
                }
            }
        }).await.unwrap();
    }

    #[tokio::test]
    async fn actual_tcp_noise_yamux_status_v2_exchange() {
        exchange(50700 * 32, Some(7)).await;
    }

    #[tokio::test]
    async fn actual_tcp_noise_yamux_status_v1_preserves_unknown_availability() {
        exchange(3000 * 32, None).await;
    }

    #[tokio::test]
    async fn actual_quic_tls_status_exchange_authenticates_both_peers() {
        let config = ChainConfig::hoodi();
        let mut listener = transport(config.clone(), 50700 * 32);
        let mut dialer = transport(config, 50700 * 32);
        listener.listen_on("/ip4/127.0.0.1/udp/0/quic-v1".parse().unwrap()).unwrap();
        let TransportEvent::Listening(address) =
            timeout(Duration::from_secs(10), listener.next_event()).await.unwrap()
        else {
            panic!("listener unavailable")
        };
        assert!(address.iter().any(|part| part == AddressPart::QuicV1));
        dialer.dial(listener.peer_id(), address).unwrap();
        exchange_connected(&mut listener, &mut dialer, Some(7)).await;
    }

    #[tokio::test]
    async fn actual_tcp_noise_mplex_only_peer_uses_fallback() {
        let config = ChainConfig::hoodi();
        let mut listener = transport(config.clone(), 50700 * 32);
        let mut dialer = transport(config, 50700 * 32);
        let shared_blocks = BlockCodec::new(listener.config.clone());
        let shared_envelopes = EnvelopeCodec::new(listener.config.clone(), &shared_blocks);
        let shared_sidecars = SidecarCodec::new(listener.config.clone(), &shared_blocks);
        let behaviour = StatusBehaviour {
            requests_v1: status_requests(listener.config.clone(), Protocol::StatusV1),
            requests_v2: status_requests(listener.config.clone(), Protocol::StatusV2),
            ping: control_requests(ControlProtocol::Ping),
            metadata_v1: control_requests(ControlProtocol::MetadataV1),
            metadata_v2: control_requests(ControlProtocol::MetadataV2),
            metadata_v3: control_requests(ControlProtocol::MetadataV3),
            gossip: crate::gossip::behaviour(listener.config.clone(), listener.current_slot)
                .unwrap(),
            blocks_range: block_requests(shared_blocks.clone(), Protocol::BlocksByRangeV2),
            blocks_root: block_requests(shared_blocks, Protocol::BlocksByRootV2),
            envelopes_range: envelope_requests(
                shared_envelopes.clone(),
                Protocol::PayloadEnvelopesByRangeV1,
            ),
            envelopes_root: envelope_requests(shared_envelopes, Protocol::PayloadEnvelopesByRootV1),
            sidecars_blob_range: sidecar_requests(
                shared_sidecars.clone(),
                Protocol::BlobSidecarsByRangeV1,
            ),
            sidecars_blob_root: sidecar_requests(
                shared_sidecars.clone(),
                Protocol::BlobSidecarsByRootV1,
            ),
            sidecars_column_range: sidecar_requests(
                shared_sidecars.clone(),
                Protocol::DataColumnsByRangeV1,
            ),
            sidecars_column_root: sidecar_requests(shared_sidecars, Protocol::DataColumnsByRootV1),
            connections: connection_limits::Behaviour::new(
                connection_limits::ConnectionLimits::default(),
            ),
        };
        listener.swarm = SwarmBuilder::with_existing_identity(Keypair::generate_secp256k1())
            .with_tokio()
            .with_tcp(tcp::Config::default(), noise::Config::new, bounded_mplex)
            .unwrap()
            .with_behaviour(|_| behaviour)
            .unwrap()
            .build();
        let address = listen(&mut listener).await;
        dialer.dial(listener.peer_id(), address).unwrap();
        exchange_connected(&mut listener, &mut dialer, Some(7)).await;
    }

    #[tokio::test]
    async fn fork_clock_update_closes_old_handlers_then_negotiates_new_status_schema() {
        let config = ChainConfig::hoodi();
        let mut listener = transport(config.clone(), 50687 * 32);
        let mut dialer = transport(config, 50687 * 32);
        let address = listen(&mut listener).await;
        dialer.dial(listener.peer_id(), address.clone()).unwrap();
        exchange_connected(&mut listener, &mut dialer, None).await;
        for node in [&mut listener, &mut dialer] {
            let mut status = node.local_status.clone();
            status.status.fork_digest = node.config.fork_digest(50688).unwrap();
            status.status.head_slot = 50688 * 32;
            node.update_snapshot(50688 * 32, node.local_finalized, Vec::new(), status).unwrap();
        }
        timeout(Duration::from_secs(10), async {
            let mut listener_closed = false;
            let mut dialer_closed = false;
            while !listener_closed || !dialer_closed {
                tokio::select! {
                    event = listener.next_event(), if !listener_closed => { listener_closed |= matches!(event, TransportEvent::Closed(_)); }
                    event = dialer.next_event(), if !dialer_closed => { dialer_closed |= matches!(event, TransportEvent::Closed(_)); }
                }
            }
        }).await.unwrap();
        dialer.dial(listener.peer_id(), address).unwrap();
        exchange_connected(&mut listener, &mut dialer, Some(7)).await;
    }

    #[tokio::test]
    async fn trusted_snapshot_rejects_clock_finality_and_history_regressions_atomically() {
        let mut node = transport(ChainConfig::hoodi(), 50700 * 32);
        let old = node.local_status.clone();
        assert_eq!(
            node.update_snapshot(50700 * 32 - 1, node.local_finalized, Vec::new(), old.clone()),
            Err(TransportError::Configuration)
        );
        let mut status = old.clone();
        status.status.finalized_root = [9; 32];
        assert_eq!(
            node.update_snapshot(
                50700 * 32,
                Checkpoint { epoch: 100, root: [9; 32] },
                Vec::new(),
                status
            ),
            Err(TransportError::Configuration)
        );
        assert_eq!(
            node.update_snapshot(
                50700 * 32,
                node.local_finalized,
                vec![(100, [9; 32])],
                old.clone()
            ),
            Err(TransportError::Configuration)
        );
        assert_eq!(node.local_status, old);
        assert_eq!(node.current_slot, 50700 * 32);
        assert!(validate_address(
            &"/ip4/8.8.8.8/udp/9000/quic-v1".parse().unwrap(),
            false,
            EndpointPolicy::Public
        )
        .is_ok());
        assert!(validate_address(
            &"/ip4/127.0.0.1/tcp/9000".parse().unwrap(),
            false,
            EndpointPolicy::Public
        )
        .is_err());
        assert!(validate_address(
            &"/ip4/8.8.8.8/udp/9000".parse().unwrap(),
            false,
            EndpointPolicy::Public
        )
        .is_err());
        assert!(validate_address(
            &"/ip4/8.8.8.8/tcp/9000/p2p-circuit".parse().unwrap(),
            false,
            EndpointPolicy::Public
        )
        .is_err());
    }

    #[tokio::test]
    async fn different_network_is_rejected_over_authenticated_wire() {
        let config = ChainConfig::hoodi();
        let mut other = config.clone();
        other.genesis_validators_root = [9; 32];
        let mut listener = transport(config, 50700 * 32);
        let mut dialer = transport(other, 50700 * 32);
        let address = listen(&mut listener).await;
        dialer.dial(listener.peer_id(), address).unwrap();
        timeout(Duration::from_secs(10), async {
            loop {
                tokio::select! {
                    event = listener.next_event() => {
                        assert_eq!(event, TransportEvent::Rejected { peer: dialer.peer_id(),
                            reason: StatusRejection::Handshake(HandshakeError::ForkDigest) });
                        break;
                    }
                    _ = dialer.next_event() => {}
                }
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn noise_requires_expected_remote_peer_identity() {
        let mut listener = transport(ChainConfig::hoodi(), 50700 * 32);
        let mut dialer = transport(ChainConfig::hoodi(), 50700 * 32);
        let address = listen(&mut listener).await;
        let wrong_peer = Keypair::generate_secp256k1().public().to_peer_id();
        dialer.dial(wrong_peer, address).unwrap();
        timeout(Duration::from_secs(10), async {
            loop {
                tokio::select! {
                    event = dialer.next_event() => {
                        assert_eq!(event, TransportEvent::DialFailed(Some(wrong_peer)));
                        break;
                    }
                    event = listener.next_event() => {
                        assert!(!matches!(event, TransportEvent::PeerStatus { .. }));
                    }
                }
            }
        })
        .await
        .unwrap();
        assert!(dialer.dialing.is_empty());
    }

    #[tokio::test]
    async fn explicit_local_address_and_pending_dial_budgets_are_enforced() {
        let mut local = transport(ChainConfig::hoodi(), 50700 * 32);
        assert_eq!(
            local.listen_on("/ip4/0.0.0.0/tcp/9000".parse().unwrap()),
            Err(TransportError::Address)
        );
        assert_eq!(
            local.listen_on("/dns4/localhost/tcp/9000".parse().unwrap()),
            Err(TransportError::Address)
        );
        assert_eq!(
            local.listen_on("/ip6/::1/tcp/0/ws".parse().unwrap()),
            Err(TransportError::Address)
        );
        for _ in 0..MAX_PEERS {
            let peer = Keypair::generate_secp256k1().public().to_peer_id();
            local.dial(peer, "/ip4/127.0.0.1/tcp/65530".parse().unwrap()).unwrap();
        }
        let peer = Keypair::generate_secp256k1().public().to_peer_id();
        assert_eq!(
            local.dial(peer, "/ip4/127.0.0.1/tcp/65530".parse().unwrap()),
            Err(TransportError::Capacity)
        );
        assert_eq!(local.dialing.len(), MAX_PEERS);
    }

    #[tokio::test]
    async fn silent_tcp_connections_are_bounded_before_noise_authentication() {
        let mut listener = transport(ChainConfig::hoodi(), 50700 * 32);
        let address = listen(&mut listener).await;
        let port = address
            .iter()
            .find_map(|part| match part {
                AddressPart::Tcp(port) => Some(port),
                _ => None,
            })
            .unwrap();
        let mut silent_connections = Vec::new();
        for _ in 0..8 {
            silent_connections
                .push(tokio::net::TcpStream::connect(("127.0.0.1", port)).await.unwrap());
            let event =
                timeout(Duration::from_secs(2), listener.swarm.select_next_some()).await.unwrap();
            assert!(matches!(event, SwarmEvent::IncomingConnection { .. }));
        }
        silent_connections.push(tokio::net::TcpStream::connect(("127.0.0.1", port)).await.unwrap());
        let event =
            timeout(Duration::from_secs(2), listener.swarm.select_next_some()).await.unwrap();
        assert!(matches!(
            event,
            SwarmEvent::IncomingConnectionError {
                peer_id: None,
                error: libp2p::swarm::ListenError::Denied { .. },
                ..
            }
        ));
        assert!(listener.peers.is_empty());
        assert_eq!(silent_connections.len(), 9);
    }

    #[test]
    fn malformed_local_status_cannot_start_transport() {
        let config = ChainConfig::hoodi();
        let local = StatusV2 {
            status: Status {
                fork_digest: [0; 4],
                finalized_root: [1; 32],
                finalized_epoch: 100,
                head_root: [2; 32],
                head_slot: 50700 * 32,
            },
            earliest_available_slot: 0,
        };
        assert!(matches!(
            LocalStatusTransport::new(
                Keypair::generate_secp256k1(),
                config,
                50700 * 32,
                Checkpoint { epoch: 100, root: [1; 32] },
                Vec::new(),
                local
            ),
            Err(TransportError::Configuration)
        ));
    }
    async fn actual_block_exchange(quic: bool, roots: bool) {
        let config = crate::test_blocks::config();
        let make = || {
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
                config.clone(),
                12,
                checkpoint,
                Vec::new(),
                status,
            )
            .unwrap()
        };
        let mut listener = make();
        let mut dialer = make();
        listener
            .listen_on(
                if quic { "/ip4/127.0.0.1/udp/0/quic-v1" } else { "/ip4/127.0.0.1/tcp/0" }
                    .parse()
                    .unwrap(),
            )
            .unwrap();
        let TransportEvent::Listening(address) = listener.next_event().await else {
            panic!("listener unavailable")
        };
        let peer = listener.peer_id();
        let a = crate::test_blocks::wire_block(9, [1; 32]);
        let b = crate::test_blocks::wire_block(12, a.root());
        let request = if roots {
            BlockRequest::Roots(vec![a.root(), b.root()])
        } else {
            BlockRequest::Range { start_slot: 9, count: 4 }
        };
        assert_eq!(
            dialer.request_blocks(peer, request.clone()),
            Err(TransportError::Configuration)
        );
        dialer.dial(peer, address).unwrap();
        exchange_connected(&mut listener, &mut dialer, None).await;
        let id = dialer.request_blocks(peer, request.clone()).unwrap();
        assert_eq!(dialer.request_blocks(peer, request.clone()), Err(TransportError::Capacity));
        timeout(Duration::from_secs(10), async {
            loop {
                tokio::select! {
                    event = listener.next_event() => {
                        let TransportEvent::BlockRequest { peer: requester, id, request: actual } = event
                            else { panic!("server event {event:?}") };
                        assert_eq!(requester, dialer.peer_id());
                        assert_eq!(actual, request);
                        // A provider cannot answer a real request with an unrelated block.
                        assert_eq!(listener.respond_blocks(id,
                            vec![crate::test_blocks::wire_block(8, [1; 32])]),
                            Err(TransportError::Configuration));
                        listener.respond_blocks(id, vec![a.clone(), b.clone()]).unwrap();
                        assert_eq!(listener.respond_blocks_unavailable(id),
                            Err(TransportError::Configuration));
                    },
                    event = dialer.next_event() => {
                        let TransportEvent::Blocks { peer: actual, id: actual_id, batch } = event
                            else { panic!("client event {event:?}") };
                        assert_eq!(actual, peer);
                        assert_eq!(actual_id, id);
                        assert_eq!(batch.blocks(), [a.clone(), b.clone()]);
                        break;
                    }
                }
            }
        }).await.unwrap();
        listener.shutdown();
        dialer.shutdown();
        assert!(dialer.block_pending.is_empty());
        assert!(listener.block_inbound.is_empty());
    }

    #[tokio::test]
    async fn actual_tcp_range_response_uses_same_authenticated_transport() {
        actual_block_exchange(false, false).await;
    }

    #[tokio::test]
    async fn actual_quic_roots_response_uses_same_authenticated_transport() {
        actual_block_exchange(true, true).await;
    }
}

#[cfg(test)]
mod envelope_tests;

#[cfg(test)]
mod availability_tests;
