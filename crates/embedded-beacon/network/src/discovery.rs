//! Signed, bounded discovery v5.1 and a queue of untrusted dial candidates.
//! Construction is dormant. This module does not start the node or import consensus data.

use crate::{
    decode_custody_group_count, encode_custody_group_count, DiscoveryCompatibility,
    DiscoveryFieldError, DiscoveryForkFields, EndpointPolicy, EnrForkId,
};
use bytes::Bytes;
use discv5::{
    enr::{CombinedKey, CombinedPublicKey, EnrPublicKey, NodeId},
    ConfigBuilder, Discv5, Enr, Event, ListenConfig,
};
use libp2p::{identity, multiaddr::Protocol, Multiaddr, PeerId};
use reth_embedded_beacon_config::ChainConfig;
use reth_embedded_beacon_primitives::Slot;
use std::{
    collections::VecDeque,
    fmt,
    future::Future,
    net::{IpAddr, SocketAddr},
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::mpsc;

mod routing_seed;

pub use routing_seed::RoutingSeedRecord;

const MAX_ENR_TEXT: usize = 404;
const MAX_CANDIDATES: usize = 32;
const MAX_QUERY_RESULTS: usize = 16;
const QUERY_TIMEOUT: Duration = Duration::from_secs(8);
const QUERY_INTERVAL: Duration = Duration::from_secs(10);

/// Discovery failure without including remote records, addresses or key material.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum DiscoveryError {
    /// A signature, secp256k1 identity, encoding or field is invalid.
    #[error("invalid signed discovery record")]
    Record,
    /// A trusted network or local advertisement is inconsistent.
    #[error("invalid discovery configuration")]
    Configuration,
    /// An endpoint is forbidden by the explicit routing policy.
    #[error("discovery endpoint is not permitted")]
    Endpoint,
    /// The queue or query admission budget is exhausted.
    #[error("discovery capacity exhausted")]
    Capacity,
    /// UDP service startup, event-channel or query operation failed.
    #[error("discovery service operation failed")]
    Service,
    /// Durable local record persistence failed before advertisement.
    #[error("discovery record persistence failed")]
    Persistence,
    /// The service has not been explicitly started.
    #[error("discovery service is dormant")]
    Dormant,
}

impl From<DiscoveryFieldError> for DiscoveryError {
    fn from(_: DiscoveryFieldError) -> Self {
        Self::Record
    }
}

/// Local subscription and actual socket advertisements. Custody is supplied by the DA service.
#[derive(Clone, Debug)]
pub struct LocalAdvertisement {
    /// Dedicated discovery UDP socket; it must differ from the QUIC UDP port.
    pub discovery_socket: SocketAddr,
    /// Actual TCP listener port.
    pub tcp_port: u16,
    /// Actual QUIC listener port.
    pub quic_port: u16,
    /// SSZ attestation `BitVector[64]`, one bit per actual subscription.
    pub attnets: [u8; 8],
    /// SSZ sync `BitVector[4]`, one bit per actual subscription.
    pub syncnets: u8,
    /// Actual custody group count; zero advertises no custody and does not imply readiness.
    pub custody_group_count: u64,
}

/// Sign a complete advertisement with an existing secp256k1 identity and persisted ENR sequence.
/// The caller must persist the key and sequence before publishing this record.
pub fn sign_local_enr(
    key: &CombinedKey,
    persisted_sequence: u64,
    config: &ChainConfig,
    current_slot: Slot,
    advertisement: &LocalAdvertisement,
    policy: EndpointPolicy,
) -> Result<Enr, DiscoveryError> {
    if !matches!(key, CombinedKey::Secp256k1(_)) ||
        !policy.permits(advertisement.discovery_socket.ip()) ||
        advertisement.discovery_socket.port() == 0 ||
        advertisement.tcp_port == 0 ||
        advertisement.quic_port == 0 ||
        advertisement.discovery_socket.port() == advertisement.quic_port ||
        advertisement.syncnets & 0xf0 != 0
    {
        return Err(DiscoveryError::Configuration);
    }
    let fields =
        DiscoveryForkFields::from_config(config, current_slot / config.slots_per_epoch.max(1))?;
    let eth2 = fields.eth2.to_ssz();
    let custody = encode_custody_group_count(advertisement.custody_group_count);
    let mut builder = Enr::builder();
    builder
        .seq(persisted_sequence)
        .ip(advertisement.discovery_socket.ip())
        .add_value("eth2", &eth2.as_slice())
        .add_value("attnets", &advertisement.attnets.as_slice())
        .add_value("syncnets", &[advertisement.syncnets].as_slice());
    if advertisement.discovery_socket.is_ipv4() {
        builder
            .udp4(advertisement.discovery_socket.port())
            .tcp4(advertisement.tcp_port)
            .add_value("quic", &advertisement.quic_port);
    } else {
        builder
            .udp6(advertisement.discovery_socket.port())
            .tcp6(advertisement.tcp_port)
            .add_value("quic6", &advertisement.quic_port);
    }
    if let Some(digest) = fields.next_fork_digest {
        builder.add_value("nfd", &digest.as_slice()).add_value("cgc", &custody.as_slice());
    }
    builder.build(key).map_err(|_| DiscoveryError::Record)
}

/// A verified signed advertisement. Availability, liveness and consensus remain untrusted.
#[derive(Clone, Debug)]
pub struct DiscoveryRecord {
    enr: Enr,
    peer_id: PeerId,
    addresses: Vec<Multiaddr>,
    /// Current network and upcoming schedule compatibility.
    pub compatibility: DiscoveryCompatibility,
    /// Claimed custody count, never proof of custody.
    pub custody_group_count: Option<u64>,
}

impl DiscoveryRecord {
    /// Parse a bounded text ENR, then check its signature, identity, fields and endpoint policy.
    pub fn parse(
        text: &str,
        config: &ChainConfig,
        current_slot: Slot,
        policy: EndpointPolicy,
    ) -> Result<Self, DiscoveryError> {
        if text.len() > MAX_ENR_TEXT || !text.starts_with("enr:") {
            return Err(DiscoveryError::Record);
        }
        let enr = text.parse::<Enr>().map_err(|_| DiscoveryError::Record)?;
        Self::verify(enr, config, current_slot, policy)
    }

    /// Verify a decoded record from the bounded discovery library.
    pub fn verify(
        enr: Enr,
        config: &ChainConfig,
        current_slot: Slot,
        policy: EndpointPolicy,
    ) -> Result<Self, DiscoveryError> {
        if !enr.verify() || !matches!(enr.public_key(), CombinedPublicKey::Secp256k1(_)) {
            return Err(DiscoveryError::Record);
        }
        let epoch = current_slot / config.slots_per_epoch.max(1);
        let expected = DiscoveryForkFields::from_config(config, epoch)?;
        let eth2 = EnrForkId::from_ssz(&field(&enr, "eth2")?)?;
        let next_fork_digest = match enr.get_raw_rlp("nfd") {
            Some(_) => Some(
                field(&enr, "nfd")?.as_slice().try_into().map_err(|_| DiscoveryError::Record)?,
            ),
            None => None,
        };
        let compatibility =
            expected.compare(DiscoveryForkFields { eth2, next_fork_digest }, epoch)?;
        let custody_group_count = match enr.get_raw_rlp("cgc") {
            Some(_) => Some(decode_custody_group_count(&field(&enr, "cgc")?)?),
            None if expected.next_fork_digest.is_some() => return Err(DiscoveryError::Record),
            None => None,
        };
        if enr.get_raw_rlp("attnets").is_some() && field(&enr, "attnets")?.len() != 8 {
            return Err(DiscoveryError::Record);
        }
        if enr.get_raw_rlp("syncnets").is_some() {
            let bytes = field(&enr, "syncnets")?;
            if bytes.len() != 1 || bytes[0] & 0xf0 != 0 {
                return Err(DiscoveryError::Record);
            }
        }
        let public = identity::secp256k1::PublicKey::try_from_bytes(&enr.public_key().encode())
            .map_err(|_| DiscoveryError::Record)?;
        let peer_id = identity::PublicKey::from(public).to_peer_id();
        let mut quic = Vec::with_capacity(2);
        let mut tcp = Vec::with_capacity(2);
        let mut discovery = false;
        for (ip, udp, tcp_port, quic_key) in [
            (enr.ip4().map(IpAddr::V4), enr.udp4(), enr.tcp4(), "quic"),
            (enr.ip6().map(IpAddr::V6), enr.udp6(), enr.tcp6(), "quic6"),
        ] {
            let Some(ip) = ip else { continue };
            if !policy.permits(ip) {
                return Err(DiscoveryError::Endpoint);
            }
            discovery |= udp.is_some_and(|port| port != 0);
            if let Some(port) = enr.get_decodable::<u16>(quic_key) {
                let port = port.map_err(|_| DiscoveryError::Record)?;
                if port == 0 || Some(port) == udp {
                    return Err(DiscoveryError::Endpoint);
                }
                let mut address = ip_address(ip);
                address.push(Protocol::Udp(port));
                address.push(Protocol::QuicV1);
                quic.push(address);
            }
            if let Some(port) = tcp_port {
                if port == 0 {
                    return Err(DiscoveryError::Endpoint);
                }
                let mut address = ip_address(ip);
                address.push(Protocol::Tcp(port));
                tcp.push(address);
            }
        }
        if !discovery || (quic.is_empty() && tcp.is_empty()) {
            return Err(DiscoveryError::Endpoint);
        }
        quic.extend(tcp);
        Ok(Self { enr, peer_id, addresses: quic, compatibility, custody_group_count })
    }

    /// libp2p identity derived from the same compressed public key signed in the ENR.
    pub const fn peer_id(&self) -> PeerId {
        self.peer_id
    }
    /// Signed discovery node identifier; it is distinct from the libp2p peer identifier.
    pub const fn node_id(&self) -> NodeId {
        self.enr.node_id()
    }
    /// Signed record sequence for replacement and persistence.
    pub const fn sequence(&self) -> u64 {
        self.enr.seq()
    }
    /// Explicit QUIC-first addresses, at most four, without relay or DNS indirection.
    pub fn addresses(&self) -> &[Multiaddr] {
        &self.addresses
    }
    /// The signature-verified ENR for persistence or insertion into discovery.
    pub const fn enr(&self) -> &Enr {
        &self.enr
    }
}

fn field(enr: &Enr, key: &str) -> Result<Vec<u8>, DiscoveryError> {
    enr.get_decodable::<Bytes>(key)
        .ok_or(DiscoveryError::Record)?
        .map(|bytes| bytes.to_vec())
        .map_err(|_| DiscoveryError::Record)
}

fn ip_address(ip: IpAddr) -> Multiaddr {
    match ip {
        IpAddr::V4(ip) => Protocol::Ip4(ip).into(),
        IpAddr::V6(ip) => Protocol::Ip6(ip).into(),
    }
}

/// One bounded discovery query, which can be polled alongside transport IO.
/// Admission remains reserved until the query timeout even after cancellation.
pub type DiscoveryQuery = Pin<Box<dyn Future<Output = Result<Vec<Enr>, DiscoveryError>> + Send>>;

/// A dormant UDP service with explicit start, bounded sessions, query rate and candidate queue.
/// Signed local-record changes are returned for durable persistence by the caller.
pub struct DiscoveryService {
    service: Discv5,
    key: CombinedKey,
    config: Arc<ChainConfig>,
    current_slot: Slot,
    policy: EndpointPolicy,
    events: Option<mpsc::Receiver<Event>>,
    candidates: VecDeque<DiscoveryRecord>,
    next_query: Instant,
}

impl fmt::Debug for DiscoveryService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DiscoveryService")
            .field("candidate_count", &self.candidates.len())
            .field("started", &self.events.is_some())
            .finish_non_exhaustive()
    }
}

impl DiscoveryService {
    /// Build from an existing signed local ENR and matching persisted identity.
    /// No UDP socket or tasks are started during construction.
    pub fn new(
        config: ChainConfig,
        current_slot: Slot,
        enr: Enr,
        key: CombinedKey,
        listen: ListenConfig,
        policy: EndpointPolicy,
    ) -> Result<Self, DiscoveryError> {
        if DiscoveryRecord::verify(enr.clone(), &config, current_slot, policy)?.compatibility !=
            DiscoveryCompatibility::Matching
        {
            return Err(DiscoveryError::Configuration);
        }
        let key_copy = match &key {
            CombinedKey::Secp256k1(key) => CombinedKey::Secp256k1(key.clone()),
            CombinedKey::Ed25519(_) => return Err(DiscoveryError::Configuration),
        };
        let listen_matches = match &listen {
            ListenConfig::Ipv4 { ip, port } => {
                policy.permits_bind((*ip).into()) &&
                    enr.ip4().is_some() &&
                    enr.udp4() == Some(*port)
            }
            ListenConfig::Ipv6 { ip, port } => {
                policy.permits_bind((*ip).into()) &&
                    enr.ip6().is_some() &&
                    enr.udp6() == Some(*port)
            }
            ListenConfig::DualStack { ipv4, ipv4_port, ipv6, ipv6_port } => {
                policy.permits_bind((*ipv4).into()) &&
                    policy.permits_bind((*ipv6).into()) &&
                    enr.ip4().is_some() &&
                    enr.udp4() == Some(*ipv4_port) &&
                    enr.ip6().is_some() &&
                    enr.udp6() == Some(*ipv6_port)
            }
            ListenConfig::FromSockets { .. } => false,
        };
        if !listen_matches {
            return Err(DiscoveryError::Configuration);
        }
        let mut builder = ConfigBuilder::new(listen);
        builder
            .enable_packet_filter()
            .request_timeout(Duration::from_secs(1))
            .query_peer_timeout(Duration::from_secs(2))
            .query_timeout(QUERY_TIMEOUT)
            .query_parallelism(2)
            .session_cache_capacity(64)
            .max_nodes_response(16)
            .incoming_bucket_limit(8)
            .table_filter(routing_seed::table_filter(policy))
            .disable_enr_update()
            .auto_nat_listen_duration(None);
        if policy == EndpointPolicy::Public {
            builder.ip_limit();
        }
        let service = Discv5::new(enr, key_copy, builder.build())
            .map_err(|_| DiscoveryError::Configuration)?;
        Ok(Self {
            service,
            key,
            config: Arc::new(config),
            current_slot,
            policy,
            events: None,
            candidates: VecDeque::new(),
            next_query: Instant::now(),
        })
    }

    /// Start actual discovery IO. Full node readiness is an independent gate.
    pub async fn start(&mut self) -> Result<(), DiscoveryError> {
        if self.events.is_some() {
            return Err(DiscoveryError::Service);
        }
        self.service.start().await.map_err(|_| DiscoveryError::Service)?;
        match self.service.event_stream().await {
            Ok(events) => {
                self.events = Some(events);
                Ok(())
            }
            Err(_) => {
                self.service.shutdown();
                Err(DiscoveryError::Service)
            }
        }
    }

    /// Shut down UDP tasks and discard pending candidates.
    pub fn shutdown(&mut self) {
        self.service.shutdown();
        self.events = None;
        self.candidates.clear();
    }

    /// Signed local advertisement, including the monotonically updated sequence.
    pub fn local_enr(&self) -> Enr {
        self.service.local_enr()
    }

    /// Advance the trusted wall clock and atomically sign all changed fork advertisements.
    /// Regressing the clock is rejected. Persistence runs before atomic publication to discovery
    /// peers.
    pub fn update_clock(
        &mut self,
        current_slot: Slot,
        persist: impl FnOnce(&Enr) -> Result<(), DiscoveryError>,
    ) -> Result<Enr, DiscoveryError> {
        if current_slot < self.current_slot {
            return Err(DiscoveryError::Configuration);
        }
        let fields = DiscoveryForkFields::from_config(
            &self.config,
            current_slot / self.config.slots_per_epoch,
        )?;
        let local = self.service.external_enr();
        let mut published = local.write();
        let mut record = published.clone();
        let eth2 = fields.eth2.to_ssz();
        let next = fields.next_fork_digest;
        if field(&record, "eth2")? != eth2 ||
            next.is_some_and(|digest| {
                field(&record, "nfd").ok().as_deref() != Some(digest.as_slice())
            })
        {
            let mut entries: Vec<(&str, &[u8])> = vec![("eth2", eth2.as_slice())];
            if let Some(digest) = &next {
                entries.push(("nfd", digest.as_slice()));
            }
            record
                .remove_insert(std::iter::empty::<&str>(), entries.into_iter(), &self.key)
                .map_err(|_| DiscoveryError::Record)?;
        }
        if record != *published {
            persist(&record).map_err(|_| DiscoveryError::Persistence)?;
            *published = record.clone();
        }
        self.current_slot = current_slot;
        self.candidates.clear();
        Ok(record)
    }

    /// Persist the newly signed custody advertisement before publishing it to discovery.
    /// The runtime caller supplies only a custody count derived from actual native coverage.
    pub fn update_custody(
        &mut self,
        count: u64,
        persist: impl FnOnce(&Enr) -> Result<(), DiscoveryError>,
    ) -> Result<(), DiscoveryError> {
        if !matches!(count, 0 | 4) {
            return Err(DiscoveryError::Configuration)
        }
        let fork = self
            .config
            .fork_at_slot(self.current_slot)
            .map_err(|_| DiscoveryError::Configuration)?
            .fork;
        if fork < reth_embedded_beacon_primitives::Fork::Fulu {
            return if count == 0 { Ok(()) } else { Err(DiscoveryError::Configuration) }
        }
        let bytes = encode_custody_group_count(count);
        let local = self.service.external_enr();
        let mut published = local.write();
        if field(&published, "cgc")?.as_slice() == bytes.as_slice() {
            return Ok(())
        }
        let mut record = published.clone();
        record
            .remove_insert(
                std::iter::empty::<&str>(),
                std::iter::once(("cgc", bytes.as_slice())),
                &self.key,
            )
            .map_err(|_| DiscoveryError::Record)?;
        persist(&record).map_err(|_| DiscoveryError::Persistence)?;
        *published = record;
        Ok(())
    }
    /// Add one verified seed; mismatched network identities never enter the routing table.
    pub fn add_seed(&self, text: &str) -> Result<(), DiscoveryError> {
        let record = DiscoveryRecord::parse(text, &self.config, self.current_slot, self.policy)?;
        self.service.add_enr(record.enr).map_err(|_| DiscoveryError::Service)
    }

    /// Insert a signature-verified routing seed, without creating a consensus dial candidate.
    /// UDP-only or historical-fork records can route discovery queries. Every returned peer
    /// still passes the current strict fork, transport and later status-handshake admission.
    pub fn add_bootstrap_seed(&self, text: &str) -> Result<(), DiscoveryError> {
        let record = RoutingSeedRecord::parse(text, self.policy)?;
        if record.node_id() == self.service.local_enr().node_id() {
            return Err(DiscoveryError::Record);
        }
        self.service.add_enr(record.enr).map_err(|_| DiscoveryError::Service)
    }

    /// Perform one bounded FINDNODE query and queue at most sixteen validated advertisements.
    /// Admission stays occupied through the upstream timeout even if this future is cancelled.
    pub async fn discover(&mut self, target: NodeId) -> Result<usize, DiscoveryError> {
        let query = self.start_query(target)?;
        self.finish_query(query.await?)
    }

    /// Reserve one query slot synchronously, returning a future independent of service borrowing.
    pub fn start_query(&mut self, target: NodeId) -> Result<DiscoveryQuery, DiscoveryError> {
        if self.events.is_none() {
            return Err(DiscoveryError::Dormant);
        }
        if self.candidates.len() >= MAX_CANDIDATES || Instant::now() < self.next_query {
            return Err(DiscoveryError::Capacity);
        }
        self.next_query = Instant::now() + QUERY_INTERVAL;
        let config = self.config.clone();
        let slot = self.current_slot;
        let policy = self.policy;
        let predicate = Box::new(move |enr: &Enr| {
            DiscoveryRecord::verify(enr.clone(), &config, slot, policy).is_ok()
        });
        let query = self.service.find_node_predicate(target, predicate, MAX_QUERY_RESULTS);
        Ok(Box::pin(async move {
            query
                .await
                .map(|records| records.into_iter().take(MAX_QUERY_RESULTS).collect())
                .map_err(|_| DiscoveryError::Service)
        }))
    }

    /// Queue completed results after verifying them against the latest trusted clock snapshot.
    /// Oversized result batches are rejected rather than silently entering the queue.
    pub fn finish_query(&mut self, records: Vec<Enr>) -> Result<usize, DiscoveryError> {
        if self.events.is_none() {
            return Err(DiscoveryError::Dormant);
        }
        if records.len() > MAX_QUERY_RESULTS {
            return Err(DiscoveryError::Capacity);
        }
        let mut added = 0;
        for enr in records {
            if self.enqueue(enr).is_ok() {
                added += 1;
            }
        }
        Ok(added)
    }

    /// Process at most thirty-two queued UDP events, returning one current verified candidate.
    /// This nonblocking drain bounds work and allocations per scheduler turn.
    pub fn next_candidate(&mut self) -> Option<DiscoveryRecord> {
        for _ in 0..MAX_CANDIDATES {
            let event = self.events.as_mut()?.try_recv().ok();
            match event {
                Some(Event::Discovered(enr) | Event::SessionEstablished(enr, _)) => {
                    let _ = self.enqueue(enr);
                }
                Some(_) => {}
                None => break,
            }
        }
        self.choose_candidate()
    }

    fn enqueue(&mut self, enr: Enr) -> Result<(), DiscoveryError> {
        if enr.node_id() == self.service.local_enr().node_id() {
            return Err(DiscoveryError::Record);
        }
        let record = DiscoveryRecord::verify(enr, &self.config, self.current_slot, self.policy)?;
        if let Some(existing) = self.candidates.iter_mut().find(|old| old.peer_id == record.peer_id)
        {
            if record.sequence() > existing.sequence() {
                *existing = record;
            }
            return Ok(());
        }
        if self.candidates.len() == MAX_CANDIDATES {
            return Err(DiscoveryError::Capacity);
        }
        self.candidates.push_back(record);
        Ok(())
    }

    fn choose_candidate(&mut self) -> Option<DiscoveryRecord> {
        // Prefer claimed full custody within the bounded, signature-verified queue. This is
        // only a routing hint: status admission and every requested column proof stay mandatory.
        if let Some(index) =
            self.candidates.iter().position(|record| record.custody_group_count == Some(128))
        {
            return self.candidates.remove(index);
        }
        self.candidates.pop_front()
    }
}

impl Drop for DiscoveryService {
    fn drop(&mut self) {
        self.service.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use discv5::enr::EnrKey;
    use std::net::{Ipv4Addr, UdpSocket};
    use tokio::time::timeout;

    fn advertisement(port: u16) -> LocalAdvertisement {
        LocalAdvertisement {
            discovery_socket: SocketAddr::from((Ipv4Addr::LOCALHOST, port)),
            tcp_port: 9000,
            quic_port: 9001,
            attnets: [0; 8],
            syncnets: 0,
            custody_group_count: 0,
        }
    }

    fn signed(key: &CombinedKey, port: u16) -> Enr {
        sign_local_enr(
            key,
            12,
            &ChainConfig::hoodi(),
            2048 * 32,
            &advertisement(port),
            EndpointPolicy::LoopbackOnly,
        )
        .unwrap()
    }

    fn discovery(port: u16) -> DiscoveryService {
        let key = CombinedKey::generate_secp256k1();
        let enr = signed(&key, port);
        DiscoveryService::new(
            ChainConfig::hoodi(),
            2048 * 32,
            enr,
            key,
            ListenConfig::Ipv4 { ip: Ipv4Addr::LOCALHOST, port },
            EndpointPolicy::LoopbackOnly,
        )
        .unwrap()
    }

    #[tokio::test]
    async fn wildcard_bind_never_rewrites_signed_advertised_endpoint_or_public_dial_policy() {
        let key = CombinedKey::generate_secp256k1();
        let mut ad = advertisement(9050);
        ad.discovery_socket.set_ip("8.8.8.8".parse().unwrap());
        let record =
            sign_local_enr(&key, 12, &ChainConfig::hoodi(), 2048 * 32, &ad, EndpointPolicy::Public)
                .unwrap();
        let service = DiscoveryService::new(
            ChainConfig::hoodi(),
            2048 * 32,
            record.clone(),
            key,
            ListenConfig::Ipv4 { ip: Ipv4Addr::UNSPECIFIED, port: 9050 },
            EndpointPolicy::Public,
        )
        .unwrap();
        assert_eq!(service.local_enr(), record);
        let checked = DiscoveryRecord::verify(
            service.local_enr(),
            &ChainConfig::hoodi(),
            2048 * 32,
            EndpointPolicy::Public,
        )
        .unwrap();
        assert_eq!(
            checked.addresses(),
            &[
                "/ip4/8.8.8.8/udp/9001/quic-v1".parse::<Multiaddr>().unwrap(),
                "/ip4/8.8.8.8/tcp/9000".parse::<Multiaddr>().unwrap(),
            ]
        );
        assert!(!EndpointPolicy::Public.permits(Ipv4Addr::UNSPECIFIED.into()));
        assert!(!EndpointPolicy::Public.permits(Ipv4Addr::LOCALHOST.into()));
    }

    #[test]
    fn signed_identity_and_quic_first_addresses_share_one_secp_key() {
        let key = CombinedKey::generate_secp256k1();
        let enr = signed(&key, 9050);
        let record = DiscoveryRecord::parse(
            &enr.to_base64(),
            &ChainConfig::hoodi(),
            2048 * 32,
            EndpointPolicy::LoopbackOnly,
        )
        .unwrap();
        assert_eq!(record.sequence(), 12);
        assert_eq!(record.custody_group_count, Some(0));
        let public =
            identity::secp256k1::PublicKey::try_from_bytes(&key.public().encode()).unwrap();
        assert_eq!(record.peer_id(), identity::PublicKey::from(public).to_peer_id());
        assert_eq!(
            record.addresses(),
            &[
                "/ip4/127.0.0.1/udp/9001/quic-v1".parse::<Multiaddr>().unwrap(),
                "/ip4/127.0.0.1/tcp/9000".parse::<Multiaddr>().unwrap(),
            ]
        );
        assert!(record.enr().verify());
    }

    #[test]
    fn malformed_signatures_unsigned_identity_and_wrong_genesis_are_rejected() {
        let key = CombinedKey::generate_secp256k1();
        let enr = signed(&key, 9050);
        let mut changed = ChainConfig::hoodi();
        changed.genesis_validators_root = [9; 32];
        assert!(DiscoveryRecord::verify(
            enr.clone(),
            &changed,
            2048 * 32,
            EndpointPolicy::LoopbackOnly
        )
        .is_err());
        let mut text = enr.to_base64().into_bytes();
        text[14] = if text[14] == b'A' { b'B' } else { b'A' };
        assert_eq!(
            DiscoveryRecord::parse(
                std::str::from_utf8(&text).unwrap(),
                &ChainConfig::hoodi(),
                2048 * 32,
                EndpointPolicy::LoopbackOnly
            )
            .unwrap_err(),
            DiscoveryError::Record
        );
        assert_eq!(
            DiscoveryRecord::parse(
                &format!("enr:{}", "A".repeat(401)),
                &ChainConfig::hoodi(),
                2048 * 32,
                EndpointPolicy::LoopbackOnly
            )
            .unwrap_err(),
            DiscoveryError::Record
        );
        let ed_key = CombinedKey::generate_ed25519();
        let ed_record = Enr::builder().build(&ed_key).unwrap();
        assert_eq!(
            DiscoveryRecord::verify(
                ed_record,
                &ChainConfig::hoodi(),
                2048 * 32,
                EndpointPolicy::LoopbackOnly
            )
            .unwrap_err(),
            DiscoveryError::Record
        );
        let different_key = CombinedKey::generate_secp256k1();
        assert!(DiscoveryService::new(
            ChainConfig::hoodi(),
            2048 * 32,
            enr,
            different_key,
            ListenConfig::Ipv4 { ip: Ipv4Addr::LOCALHOST, port: 9050 },
            EndpointPolicy::LoopbackOnly
        )
        .is_err());
    }

    #[test]
    fn required_fulu_fields_and_bitvector_lengths_fail_closed() {
        let key = CombinedKey::generate_secp256k1();
        for (name, value) in [
            ("cgc", vec![0]),
            ("nfd", vec![1; 3]),
            ("attnets", vec![1; 7]),
            ("syncnets", vec![0x10]),
        ] {
            let mut enr = signed(&key, 9050);
            enr.insert(name, &value.as_slice(), &key).unwrap();
            assert_eq!(
                DiscoveryRecord::verify(
                    enr,
                    &ChainConfig::hoodi(),
                    2048 * 32,
                    EndpointPolicy::LoopbackOnly
                )
                .unwrap_err(),
                DiscoveryError::Record
            );
        }
        let mut enr = signed(&key, 9050);
        enr.remove_insert(std::iter::once("cgc"), std::iter::empty::<(&str, &[u8])>(), &key)
            .unwrap();
        assert_eq!(
            DiscoveryRecord::verify(
                enr,
                &ChainConfig::hoodi(),
                2048 * 32,
                EndpointPolicy::LoopbackOnly
            )
            .unwrap_err(),
            DiscoveryError::Record
        );
        let mut enr = signed(&key, 9050);
        enr.insert("nfd", &[9u8; 4].as_slice(), &key).unwrap();
        let record = DiscoveryRecord::verify(
            enr,
            &ChainConfig::hoodi(),
            2048 * 32,
            EndpointPolicy::LoopbackOnly,
        )
        .unwrap();
        assert_eq!(record.compatibility, DiscoveryCompatibility::UpcomingForkDifference);
    }

    #[test]
    fn public_endpoint_policy_does_not_admit_private_or_relay_endpoints() {
        let key = CombinedKey::generate_secp256k1();
        let mut ad = advertisement(9050);
        for ip in [
            "0.0.0.0",
            "127.0.0.1",
            "10.1.2.3",
            "192.168.1.1",
            "169.254.1.1",
            "100.64.1.1",
            "198.18.0.1",
            "192.0.2.1",
            "224.0.0.1",
            "::1",
            "fc00::1",
            "fe80::1",
            "2001:db8::1",
        ] {
            ad.discovery_socket.set_ip(ip.parse().unwrap());
            assert!(sign_local_enr(
                &key,
                1,
                &ChainConfig::hoodi(),
                2048 * 32,
                &ad,
                EndpointPolicy::Public
            )
            .is_err());
        }
        for ip in ["8.8.8.8", "2606:4700:4700::1111"] {
            ad.discovery_socket.set_ip(ip.parse().unwrap());
            let record = sign_local_enr(
                &key,
                1,
                &ChainConfig::hoodi(),
                2048 * 32,
                &ad,
                EndpointPolicy::Public,
            )
            .unwrap();
            assert!(DiscoveryRecord::verify(
                record,
                &ChainConfig::hoodi(),
                2048 * 32,
                EndpointPolicy::Public
            )
            .is_ok());
        }
        ad.discovery_socket = SocketAddr::from((Ipv4Addr::LOCALHOST, 9001));
        assert!(sign_local_enr(
            &key,
            1,
            &ChainConfig::hoodi(),
            2048 * 32,
            &ad,
            EndpointPolicy::LoopbackOnly
        )
        .is_err());
    }

    #[tokio::test]
    async fn signed_fork_update_is_persisted_before_publication_and_survives_restore() {
        let mut service = discovery(9050);
        let old = service.local_enr();
        assert_eq!(
            service.update_clock(50688 * 32, |_| Err(DiscoveryError::Persistence)),
            Err(DiscoveryError::Persistence)
        );
        assert_eq!(service.local_enr(), old);
        assert_eq!(service.current_slot, 2048 * 32);
        let mut saved = None;
        let changed = service
            .update_clock(50688 * 32, |record| {
                saved = Some(record.clone());
                Ok(())
            })
            .unwrap();
        assert_eq!(saved, Some(changed.clone()));
        assert_eq!(changed.seq(), old.seq() + 1);
        assert_eq!(field(&changed, "eth2").unwrap()[..4], [0xe2, 0xab, 0xcc, 0xa4]);
        assert_eq!(field(&changed, "nfd").unwrap(), [0xae, 0x9f, 0x70, 0xa0]);
        assert!(service.update_clock(50688 * 32 - 1, |_| Ok(())).is_err());
        let key = match &service.key {
            CombinedKey::Secp256k1(key) => CombinedKey::Secp256k1(key.clone()),
            _ => unreachable!(),
        };
        service.shutdown();
        let mut restored = DiscoveryService::new(
            ChainConfig::hoodi(),
            50688 * 32,
            changed,
            key,
            ListenConfig::Ipv4 { ip: Ipv4Addr::LOCALHOST, port: 9050 },
            EndpointPolicy::LoopbackOnly,
        )
        .unwrap();
        let updated = restored.update_clock(52480 * 32, |_| Ok(())).unwrap();
        assert_eq!(updated.seq(), old.seq() + 2);
    }

    #[tokio::test]
    async fn candidate_queue_is_bounded_deduplicated_and_newer_signed_sequence_replaces() {
        let mut service = discovery(9050);
        let key = CombinedKey::generate_secp256k1();
        let first = signed(&key, 9051);
        service.enqueue(first.clone()).unwrap();
        service.enqueue(first.clone()).unwrap();
        assert_eq!(service.candidates.len(), 1);
        let mut newer = first;
        newer.insert("attnets", &[1u8; 8].as_slice(), &key).unwrap();
        service.enqueue(newer.clone()).unwrap();
        assert_eq!(service.candidates[0].enr(), &newer);
        for _ in 1..MAX_CANDIDATES {
            service.enqueue(signed(&CombinedKey::generate_secp256k1(), 9052)).unwrap();
        }
        assert_eq!(
            service.enqueue(signed(&CombinedKey::generate_secp256k1(), 9053)),
            Err(DiscoveryError::Capacity)
        );
        assert_eq!(service.candidates.len(), MAX_CANDIDATES);
    }

    #[tokio::test]
    async fn actual_udp_discovery_authenticates_sessions_and_query_admission_is_bounded() {
        let reservation_a = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let reservation_b = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let mut a = discovery(reservation_a.local_addr().unwrap().port());
        let mut b = discovery(reservation_b.local_addr().unwrap().port());
        let b_record = b.local_enr();
        a.add_seed(&b_record.to_base64()).unwrap();
        b.add_seed(&a.local_enr().to_base64()).unwrap();
        drop(reservation_a);
        drop(reservation_b);
        a.start().await.unwrap();
        b.start().await.unwrap();
        timeout(Duration::from_secs(12), a.discover(b_record.node_id())).await.unwrap().unwrap();
        assert!(a.service.connected_peers() > 0);
        let record = a.next_candidate().expect("a verified UDP peer must become a dial candidate");
        assert_eq!(record.node_id(), b_record.node_id());
        assert_eq!(a.discover(NodeId::random()).await, Err(DiscoveryError::Capacity));
        a.shutdown();
        assert_eq!(a.discover(NodeId::random()).await, Err(DiscoveryError::Dormant));
    }

    #[tokio::test]
    async fn full_custody_candidates_precede_fifo_fallback_without_granting_admission() {
        let mut service = discovery(9050);
        let mut records = Vec::new();
        for (port, custody) in
            [(9051, Some(4)), (9052, None), (9053, Some(128)), (9054, Some(129)), (9055, Some(128))]
        {
            let key = CombinedKey::generate_secp256k1();
            let mut record = signed(&key, port);
            if let Some(custody) = custody {
                record
                    .insert("cgc", &encode_custody_group_count(custody).as_slice(), &key)
                    .unwrap();
            }
            service.enqueue(record.clone()).unwrap();
            records.push(record);
        }
        let key = CombinedKey::generate_secp256k1();
        let mut malformed = signed(&key, 9056);
        malformed.insert("cgc", &[0u8].as_slice(), &key).unwrap();
        assert_eq!(service.enqueue(malformed), Err(DiscoveryError::Record));
        assert_eq!(service.candidates.len(), records.len());
        for index in [2, 4, 0, 1, 3] {
            let selected = service.choose_candidate().unwrap();
            assert_eq!(selected.enr(), &records[index]);
            assert_eq!(selected.node_id(), records[index].node_id());
        }
        assert!(service.choose_candidate().is_none());
    }
}
