//! Concrete discovery-to-transport scheduling with one secp256k1 identity.
//! This dormant peer foundation supplies status observations, never block import authorization.

use crate::{
    block_rpc::{BlockRequest, WireBlock},
    discovery::{DiscoveryError, DiscoveryQuery, DiscoveryService},
    envelope_rpc::{EnvelopeBlockReference, EnvelopeRequest, WireEnvelope},
    transport::{
        BlockRequestId, EnvelopeRequestId, StatusTransport, TransportError, TransportEvent,
    },
    EndpointPolicy, StatusV2,
};
use discv5::{enr::CombinedKey, Enr, ListenConfig};
use libp2p::{identity, Multiaddr, PeerId};
use reth_embedded_beacon_config::ChainConfig;
use reth_embedded_beacon_primitives::{Checkpoint, Epoch, Root, Slot};
use std::{
    fmt,
    time::{Duration, Instant},
};

mod custody_hints;
use custody_hints::CustodyHints;

/// Caller-supplied clock and consensus inputs from already validated local state.
#[derive(Clone, Debug)]
pub struct TrustedStatusSnapshot {
    /// Current trusted wall-clock slot, independent of sync head.
    pub current_slot: Slot,
    /// Already validated local finalized checkpoint.
    pub finalized: Checkpoint,
    /// At most 4096 known finalized epoch/root pairs.
    pub canonical_roots: Vec<(Epoch, Root)>,
    /// Actual local head, finality and earliest available block advertisement.
    pub status: StatusV2,
}

/// Bounded peer-runtime failure without key or remote record material.
#[derive(Debug, thiserror::Error)]
pub enum PeerRuntimeError {
    /// A shared identity or trusted snapshot is inconsistent.
    #[error("inconsistent peer runtime identity")]
    Identity,
    /// Discovery admission, persistence or actual UDP operation failed.
    #[error(transparent)]
    Discovery(#[from] DiscoveryError),
    /// Authenticated transport admission or operation failed.
    #[error(transparent)]
    Transport(#[from] TransportError),
}

/// Actual peer observations. Discovery completion does not imply liveness or validated consensus.
#[derive(Clone, Debug)]
pub enum PeerRuntimeEvent {
    /// An authenticated transport observation, with its precise finality trust classification.
    Transport(TransportEvent),
    /// One discovery query completed; accepted signed advertisements entered the bounded queue.
    DiscoveryComplete {
        /// Number accepted or updated in the candidate queue.
        candidate_count: usize,
    },
    /// A signed candidate could not be scheduled under current transport admission.
    CandidateRejected {
        /// Peer identity derived from its signed record.
        peer: PeerId,
        /// Bounded admission reason.
        reason: TransportError,
    },
    /// A bounded scheduler turn elapsed; the caller can apply a new trusted clock snapshot.
    Tick,
}

/// UDP discovery and QUIC/TCP dialing share one persisted identity and exact network configuration.
/// Constructor/start/listen are deliberately separate; no launcher or readiness flag is enabled.
pub struct PeerRuntime {
    discovery: DiscoveryService,
    transport: StatusTransport,
    query: Option<DiscoveryQuery>,
    dial_families: dial_addresses::DialFamilies,
    custody_hints: CustodyHints,
}

impl fmt::Debug for PeerRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PeerRuntime")
            .field("peer_id", &self.peer_id())
            .field("query_pending", &self.query.is_some())
            .finish_non_exhaustive()
    }
}

impl PeerRuntime {
    /// Build both dormant services from one persisted key and already signed local record.
    /// Private key bytes are immediately zeroed by the identity decoder and are never logged.
    pub fn new(
        config: ChainConfig,
        snapshot: TrustedStatusSnapshot,
        local_enr: Enr,
        key: CombinedKey,
        listen: ListenConfig,
        policy: EndpointPolicy,
    ) -> Result<Self, PeerRuntimeError> {
        if !matches!(key, CombinedKey::Secp256k1(_)) {
            return Err(PeerRuntimeError::Identity);
        }
        let mut secret_bytes = key.encode();
        let secret = identity::secp256k1::SecretKey::try_from_bytes(&mut secret_bytes)
            .map_err(|_| PeerRuntimeError::Identity)?;
        let transport_key = identity::Keypair::from(identity::secp256k1::Keypair::from(secret));
        let mut transport = StatusTransport::with_policy(
            transport_key,
            config.clone(),
            snapshot.current_slot,
            snapshot.finalized,
            snapshot.canonical_roots,
            snapshot.status,
            policy,
        )?;
        transport.join_custody_columns(local_enr.node_id().raw())?;
        let dial_families = dial_addresses::DialFamilies::from_listen(&listen);
        let discovery =
            DiscoveryService::new(config, snapshot.current_slot, local_enr, key, listen, policy)?;
        Ok(Self {
            discovery,
            transport,
            query: None,
            dial_families,
            custody_hints: CustodyHints::default(),
        })
    }

    /// The secp256k1 identity used by the signed ENR, Noise and QUIC/TLS.
    pub fn peer_id(&self) -> PeerId {
        self.transport.peer_id()
    }

    /// Read the signed local record for durable persistence and explicit endpoint inspection.
    pub fn local_enr(&self) -> Enr {
        self.discovery.local_enr()
    }

    /// Start only the actual discovery UDP service after the caller's independent readiness gate.
    pub async fn start_discovery(&mut self) -> Result<(), PeerRuntimeError> {
        self.discovery.start().await?;
        Ok(())
    }

    /// Start one explicit QUIC or TCP listener after the caller's independent readiness gate.
    pub fn listen_on(&mut self, address: Multiaddr) -> Result<(), PeerRuntimeError> {
        self.transport.listen_on(address)?;
        Ok(())
    }

    /// Admit a signature-checked seed on the same trusted network.
    pub fn add_seed(&self, text: &str) -> Result<(), PeerRuntimeError> {
        self.discovery.add_seed(text)?;
        Ok(())
    }

    /// Admit only signed discovery routing data; no peer or consensus authority is granted.
    pub fn add_bootstrap_seed(&self, text: &str) -> Result<(), PeerRuntimeError> {
        self.discovery.add_bootstrap_seed(text)?;
        Ok(())
    }

    /// Schedule at most one actual discovery query without blocking authenticated transport IO.
    pub fn request_discovery(&mut self) -> Result<(), PeerRuntimeError> {
        if self.query.is_some() {
            return Err(DiscoveryError::Capacity.into());
        }
        self.query = Some(self.discovery.start_query(discv5::enr::NodeId::random())?);
        Ok(())
    }

    /// Validate all consensus inputs before changing advertisements, persist a signed ENR first,
    /// then apply the same clock/finality snapshot to transport without an intervening await.
    pub fn update_snapshot(
        &mut self,
        snapshot: TrustedStatusSnapshot,
        persist: impl FnOnce(&Enr) -> Result<(), DiscoveryError>,
    ) -> Result<(), PeerRuntimeError> {
        self.transport.validate_snapshot(
            snapshot.current_slot,
            snapshot.finalized,
            &snapshot.canonical_roots,
            &snapshot.status,
        )?;
        self.discovery.update_clock(snapshot.current_slot, persist)?;
        // Completion rechecks records against this new clock. Keep the reserved query alive
        // across ordinary head/clock publication so the event owner receives its completion.
        self.transport.update_snapshot(
            snapshot.current_slot,
            snapshot.finalized,
            snapshot.canonical_roots,
            snapshot.status,
        )?;
        Ok(())
    }

    /// Publish custody only after the owner proves retained coverage; signed ENR durability
    /// precedes the independent metadata sequence change without an intervening await.
    pub fn update_custody(
        &mut self,
        count: u64,
        persist: impl FnOnce(&Enr) -> Result<(), DiscoveryError>,
    ) -> Result<(), PeerRuntimeError> {
        let metadata = self.transport.prepare_custody_metadata(count)?;
        self.discovery.update_custody(count, persist)?;
        self.transport.commit_custody_metadata(metadata);
        Ok(())
    }
    /// Drive at most four candidate admissions, then concurrently poll transport, discovery query
    /// completion and a 100ms scheduler turn. Candidate queues and all connection classes are
    /// capped.
    pub async fn next_event(&mut self) -> Result<PeerRuntimeEvent, PeerRuntimeError> {
        for _ in 0..4 {
            if !self.transport.has_dial_capacity() {
                break;
            }
            let Some(candidate) = self.discovery.next_candidate() else { break };
            // Single-family nodes retain only usable signed endpoints from dual-stack ENRs.
            let addresses = self.dial_families.retain(candidate.addresses());
            if addresses.is_empty() {
                continue
            }
            if let Err(reason) = self.transport.dial_addresses(candidate.peer_id(), addresses) {
                return Ok(PeerRuntimeEvent::CandidateRejected {
                    peer: candidate.peer_id(),
                    reason,
                });
            }
            self.custody_hints.record(&candidate, Instant::now());
        }
        let event: Result<PeerRuntimeEvent, PeerRuntimeError> = tokio::select! {
            event = self.transport.next_event() => Ok(PeerRuntimeEvent::Transport(event)),
            result = async {
                match &mut self.query {
                    Some(query) => query.await,
                    None => std::future::pending().await,
                }
            } => {
                self.query = None;
                let candidate_count = self.discovery.finish_query(result?)?;
                Ok(PeerRuntimeEvent::DiscoveryComplete { candidate_count })
            },
            () = tokio::time::sleep(Duration::from_millis(100)) => Ok(PeerRuntimeEvent::Tick),
        };
        let event = event?;
        if let PeerRuntimeEvent::Transport(transport) = &event {
            match transport {
                TransportEvent::PeerStatus { peer, .. } => {
                    self.custody_hints.checked(*peer, Instant::now());
                    match self.transport.request_metadata(*peer) {
                        Ok(()) | Err(TransportError::Capacity) => {}
                        Err(error) => return Err(error.into()),
                    }
                }
                TransportEvent::PeerMetadata { peer, metadata } => {
                    self.custody_hints.metadata(*peer, *metadata, Instant::now());
                }
                TransportEvent::Closed(peer) |
                TransportEvent::Rejected { peer, .. } |
                TransportEvent::DialFailed(Some(peer)) => {
                    self.custody_hints.remove(*peer);
                }
                _ => {}
            }
        }
        Ok(event)
    }

    /// Schedule bounded block retrieval only after a checked authenticated status exchange.
    pub fn request_blocks(
        &mut self,
        peer: PeerId,
        request: BlockRequest,
    ) -> Result<BlockRequestId, PeerRuntimeError> {
        Ok(self.transport.request_blocks(peer, request)?)
    }

    /// Resolve only the exact original gossip delivery after native validation.
    pub fn report_gossip(
        &mut self,
        id: crate::transport::GossipRequestId,
        verdict: crate::transport::GossipVerdict,
    ) -> Result<bool, PeerRuntimeError> {
        Ok(self.transport.report_gossip(id, verdict)?)
    }

    /// Serve retained signed blocks on their original private transport response channel.
    pub fn respond_blocks(
        &mut self,
        id: BlockRequestId,
        blocks: Vec<WireBlock>,
    ) -> Result<(), PeerRuntimeError> {
        Ok(self.transport.respond_blocks(id, blocks)?)
    }

    /// Report unavailable history through a bounded response instead of fabricating blocks.
    pub fn respond_blocks_unavailable(
        &mut self,
        id: BlockRequestId,
    ) -> Result<(), PeerRuntimeError> {
        Ok(self.transport.respond_blocks_unavailable(id)?)
    }

    /// Retrieve bounded Gloas envelopes linked to original decoded block metadata.
    pub fn request_envelopes(
        &mut self,
        peer: PeerId,
        request: EnvelopeRequest,
        blocks: Vec<EnvelopeBlockReference>,
    ) -> Result<EnvelopeRequestId, PeerRuntimeError> {
        Ok(self.transport.request_envelopes(peer, request, blocks)?)
    }

    /// Serve exact signed envelopes on their original authenticated response channel.
    pub fn respond_envelopes(
        &mut self,
        id: EnvelopeRequestId,
        envelopes: Vec<WireEnvelope>,
        blocks: &[EnvelopeBlockReference],
    ) -> Result<(), PeerRuntimeError> {
        Ok(self.transport.respond_envelopes(id, envelopes, blocks)?)
    }

    /// Report unavailable envelope history without supplying asserted payloads.
    pub fn respond_envelopes_unavailable(
        &mut self,
        id: EnvelopeRequestId,
    ) -> Result<(), PeerRuntimeError> {
        Ok(self.transport.respond_envelopes_unavailable(id)?)
    }
    /// Retrieve candidate-bound original blobs or columns using the existing authenticated
    /// transport.
    pub fn request_sidecars(
        &mut self,
        peer: PeerId,
        request: crate::availability_rpc::SidecarRequest,
    ) -> Result<crate::transport::SidecarRequestId, PeerRuntimeError> {
        Ok(self.transport.request_sidecars(peer, request)?)
    }

    /// Serve retained verified original sidecars on their actual private response channel.
    pub fn respond_sidecars(
        &mut self,
        id: crate::transport::SidecarRequestId,
        sidecars: Vec<crate::availability_rpc::WireSidecar>,
    ) -> Result<(), PeerRuntimeError> {
        Ok(self.transport.respond_sidecars(id, sidecars)?)
    }

    /// Report missing data without manufacturing availability evidence.
    pub fn respond_sidecars_unavailable(
        &mut self,
        id: crate::transport::SidecarRequestId,
    ) -> Result<(), PeerRuntimeError> {
        Ok(self.transport.respond_sidecars_unavailable(id)?)
    }

    /// Shut down actual UDP tasks, listeners, authenticated connections and the query future.
    pub fn shutdown(&mut self) {
        self.query = None;
        self.discovery.shutdown();
        self.transport.shutdown();
        self.custody_hints.clear();
    }

    /// Release one provider's connection budget without granting validity or banning its identity.
    /// The ordinary terminal transport events remain available through `next_event`.
    pub fn retire_peer(&mut self, peer: PeerId) {
        self.custody_hints.remove(peer);
        self.transport.retire_peer(peer);
    }

    /// Rank requested columns using expiring, identity-bound remote custody claims only.
    /// A positive overlap never proves availability, signatures or consensus validity.
    pub fn custody_overlap(&self, peer: PeerId, columns: &[u64]) -> usize {
        self.custody_hints.overlap(peer, columns, Instant::now())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        custody::CustodyParameters,
        discovery::{sign_local_enr, DiscoveryRecord, LocalAdvertisement},
        EnrForkId, Status, StatusCompatibility,
    };
    use std::net::{Ipv4Addr, TcpListener, UdpSocket};
    use tokio::time::timeout;

    fn snapshot(slot: Slot) -> TrustedStatusSnapshot {
        TrustedStatusSnapshot {
            current_slot: slot,
            finalized: Checkpoint { epoch: 100, root: [1; 32] },
            canonical_roots: Vec::new(),
            status: StatusV2 {
                status: Status {
                    fork_digest: ChainConfig::hoodi().fork_digest(slot / 32).unwrap(),
                    finalized_root: [1; 32],
                    finalized_epoch: 100,
                    head_root: [2; 32],
                    head_slot: slot,
                },
                earliest_available_slot: 7,
            },
        }
    }

    fn runtime() -> (PeerRuntime, Vec<UdpSocket>, TcpListener) {
        let udp = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let quic = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let tcp = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let ad = LocalAdvertisement {
            discovery_socket: udp.local_addr().unwrap(),
            tcp_port: tcp.local_addr().unwrap().port(),
            quic_port: quic.local_addr().unwrap().port(),
            attnets: [0; 8],
            syncnets: 0,
            custody_group_count: 0,
        };
        let key = CombinedKey::generate_secp256k1();
        let config = ChainConfig::hoodi();
        let snapshot = snapshot(50700 * 32);
        let enr = sign_local_enr(
            &key,
            8,
            &config,
            snapshot.current_slot,
            &ad,
            EndpointPolicy::LoopbackOnly,
        )
        .unwrap();
        let runtime = PeerRuntime::new(
            config,
            snapshot,
            enr,
            key,
            ListenConfig::Ipv4 { ip: Ipv4Addr::LOCALHOST, port: ad.discovery_socket.port() },
            EndpointPolicy::LoopbackOnly,
        )
        .unwrap();
        (runtime, vec![udp, quic], tcp)
    }

    #[tokio::test]
    async fn actual_scheduler_preserves_query_through_clock_updates_and_authenticates_quic_identity(
    ) {
        let (mut a, a_udp, a_tcp) = runtime();
        let (mut b, b_udp, b_tcp) = runtime();
        a.add_seed(&b.local_enr().to_base64()).unwrap();
        b.add_seed(&a.local_enr().to_base64()).unwrap();
        let a_port = a_udp[1].local_addr().unwrap().port();
        let b_port = b_udp[1].local_addr().unwrap().port();
        drop(a_udp);
        drop(b_udp);
        drop(a_tcp);
        drop(b_tcp);
        a.listen_on(format!("/ip4/127.0.0.1/udp/{a_port}/quic-v1").parse().unwrap()).unwrap();
        b.listen_on(format!("/ip4/127.0.0.1/udp/{b_port}/quic-v1").parse().unwrap()).unwrap();
        a.start_discovery().await.unwrap();
        b.start_discovery().await.unwrap();
        a.request_discovery().unwrap();
        a.update_snapshot(snapshot(50700 * 32), |_| Ok(())).unwrap();
        a.update_snapshot(snapshot(50700 * 32 + 1), |_| Ok(())).unwrap();
        assert!(a.query.is_some());
        assert!(matches!(
            a.request_discovery(),
            Err(PeerRuntimeError::Discovery(DiscoveryError::Capacity))
        ));
        timeout(Duration::from_secs(15), async {
            let mut a_status = false;
            let mut b_status = false;
            let mut queried = false;
            while !a_status || !b_status || !queried {
                tokio::select! {
                    event = a.next_event() => match event.unwrap() {
                        PeerRuntimeEvent::Transport(TransportEvent::PeerStatus { peer, compatibility, .. }) => {
                            assert_eq!(peer, b.peer_id()); assert_eq!(compatibility, StatusCompatibility::KnownFinality); a_status = true;
                        }
                        PeerRuntimeEvent::DiscoveryComplete { candidate_count } => { assert!(candidate_count > 0); queried = true; }
                        _ => {},
                    },
                    event = b.next_event() => {
                        if let PeerRuntimeEvent::Transport(TransportEvent::PeerStatus { peer, compatibility, .. }) = event.unwrap() {
                            assert_eq!(peer, a.peer_id()); assert_eq!(compatibility, StatusCompatibility::KnownFinality); b_status = true;
                        }
                    },
                }
            }
        }).await.unwrap();
        a.shutdown();
        b.shutdown();
        assert!(matches!(
            a.request_discovery(),
            Err(PeerRuntimeError::Discovery(DiscoveryError::Dormant))
        ));
    }

    #[tokio::test]
    async fn invalid_consensus_snapshot_cannot_publish_new_fork_record_or_call_persistence() {
        let (mut node, _udp, _tcp) = runtime();
        let previous = node.local_enr();
        let mut changed = snapshot(52480 * 32);
        changed.finalized.root = [9; 32];
        changed.status.status.finalized_root = [9; 32];
        let mut persisted = false;
        assert!(matches!(
            node.update_snapshot(changed, |_| {
                persisted = true;
                Ok(())
            }),
            Err(PeerRuntimeError::Transport(TransportError::Configuration))
        ));
        assert!(!persisted);
        assert_eq!(node.local_enr(), previous);
        node.update_snapshot(snapshot(52480 * 32), |record| {
            persisted = true;
            assert_eq!(record.seq(), previous.seq() + 1);
            let bytes = record.get_decodable::<bytes::Bytes>("eth2").unwrap().unwrap();
            assert_eq!(EnrForkId::from_ssz(&bytes).unwrap().fork_digest, [0xae, 0x9f, 0x70, 0xa0]);
            Ok(())
        })
        .unwrap();
        assert!(persisted);
    }

    #[tokio::test]
    async fn cancelled_query_keeps_admission_reserved_and_dormant_construct_binds_keys() {
        let (mut node, udp, tcp) = runtime();
        assert!(matches!(
            node.request_discovery(),
            Err(PeerRuntimeError::Discovery(DiscoveryError::Dormant))
        ));
        drop(udp);
        drop(tcp);
        node.start_discovery().await.unwrap();
        node.request_discovery().unwrap();
        node.update_snapshot(snapshot(50700 * 32), |_| Ok(())).unwrap();
        assert!(node.query.is_some());
        // Explicit cancellation still preserves the service's upstream query reservation.
        drop(node.query.take());
        assert!(node.query.is_none());
        assert!(matches!(
            node.request_discovery(),
            Err(PeerRuntimeError::Discovery(DiscoveryError::Capacity))
        ));
        let different_key = CombinedKey::generate_secp256k1();
        assert!(matches!(
            PeerRuntime::new(
                ChainConfig::hoodi(),
                snapshot(50700 * 32),
                node.local_enr(),
                different_key,
                ListenConfig::Ipv4 {
                    ip: Ipv4Addr::LOCALHOST,
                    port: node.local_enr().udp4().unwrap()
                },
                EndpointPolicy::LoopbackOnly
            ),
            Err(PeerRuntimeError::Discovery(DiscoveryError::Configuration))
        ));
    }
    #[tokio::test]
    async fn custody_publication_persists_enr_before_independent_metadata_and_failed_write_is_inert(
    ) {
        let (mut node, _udp, _tcp) = runtime();
        node.update_snapshot(snapshot(52480 * 32), |_| Ok(())).unwrap();
        let old = node.local_enr();
        assert!(matches!(
            node.update_custody(4, |_| Err(DiscoveryError::Persistence)),
            Err(PeerRuntimeError::Discovery(DiscoveryError::Persistence))
        ));
        assert_eq!(node.local_enr(), old);
        let zero = node.transport.prepare_custody_metadata(0).unwrap();
        assert_eq!(zero.seq_number(), 0);
        let mut persisted = None;
        node.update_custody(4, |record| {
            persisted = Some(record.clone());
            Ok(())
        })
        .unwrap();
        assert_eq!(persisted, Some(node.local_enr()));
        assert_eq!(node.local_enr().seq(), old.seq() + 1);
        let four = node.transport.prepare_custody_metadata(4).unwrap();
        assert_eq!(four.seq_number(), 1);
        assert_eq!(
            four.snapshot(crate::control_rpc::ControlProtocol::MetadataV3).custody_group_count,
            Some(4)
        );
        node.update_custody(4, |_| panic!("unchanged custody cannot republish ENR")).unwrap();
        assert_eq!(node.transport.prepare_custody_metadata(4).unwrap().seq_number(), 1);
        node.update_custody(0, |_| Ok(())).unwrap();
        assert_eq!(node.transport.prepare_custody_metadata(0).unwrap().seq_number(), 2);
        assert_eq!(node.local_enr().seq(), old.seq() + 2);
    }

    #[tokio::test]
    async fn actual_tcp_metadata_updates_exact_signed_provider_hint_and_disconnect_evicts_it() {
        let (mut client, client_udp, client_tcp) = runtime();
        let (mut server, server_udp, server_tcp) = runtime();
        drop((client_udp, client_tcp, server_udp, server_tcp));
        server.update_custody(4, |_| Ok(())).unwrap();
        let record = DiscoveryRecord::verify(
            server.local_enr(),
            &ChainConfig::hoodi(),
            50700 * 32,
            EndpointPolicy::LoopbackOnly,
        )
        .unwrap();
        client.custody_hints.record(&record, Instant::now());
        let mut node_id = record.node_id().raw();
        node_id.reverse();
        let columns = CustodyParameters::default().get_custody_groups(node_id, 4).unwrap();
        assert_eq!(client.custody_overlap(server.peer_id(), &columns), 0);
        server.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()).unwrap();
        let address = timeout(Duration::from_secs(10), async {
            loop {
                match server.next_event().await.unwrap() {
                    PeerRuntimeEvent::Transport(TransportEvent::Listening(address)) => {
                        break address
                    }
                    PeerRuntimeEvent::Tick => {}
                    other => panic!("unexpected real listener event: {other:?}"),
                }
            }
        })
        .await
        .unwrap();
        client.transport.dial(server.peer_id(), address).unwrap();
        let metadata = timeout(Duration::from_secs(10), async {
            loop {
                tokio::select! {
                    event = client.next_event() => match event.unwrap() {
                        PeerRuntimeEvent::Transport(TransportEvent::PeerMetadata { peer, metadata }) => {
                            assert_eq!(peer, server.peer_id());
                            break metadata;
                        },
                        PeerRuntimeEvent::Transport(TransportEvent::Rejected { .. }) => panic!("unexpected client rejection"),
                        _ => {},
                    },
                    event = server.next_event() => {
                        assert!(!matches!(event.unwrap(), PeerRuntimeEvent::Transport(TransportEvent::Rejected { .. })));
                    },
                }
            }
        }).await.unwrap();
        assert_eq!(metadata.custody_group_count, Some(4));
        assert_eq!(client.custody_overlap(server.peer_id(), &columns), 4);
        assert_eq!(client.custody_overlap(PeerId::random(), &columns), 0);
        server.retire_peer(client.peer_id());
        timeout(Duration::from_secs(10), async {
            loop {
                tokio::select! {
                    event = client.next_event() => {
                        if let PeerRuntimeEvent::Transport(TransportEvent::Closed(peer) | TransportEvent::Rejected { peer, .. }) = event.unwrap() {
                            assert_eq!(peer, server.peer_id());
                            break;
                        }
                    },
                    _ = server.next_event() => {},
                }
            }
        }).await.unwrap();
        assert_eq!(client.custody_overlap(server.peer_id(), &columns), 0);
        server.shutdown();
        client.shutdown();
    }
}

mod dial_addresses;
